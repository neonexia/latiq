//! latiq-sdk — a reusable Rust client for Latiq's Data/Query + Admin/Control gRPC
//! (the **CLI/SDK** audience — gRPC, never MCP; the SDK is not an agent,
//! invariants 1 & 8). The Python SDK (`sdk/python`) is a thin PyO3 wrapper over it.
//!
//! Two ways to get a cluster, **one wire shape** (calls always ride gRPC):
//!   - `Latiq::connect("local", root)` — start a control-plane + pond-node
//!     in-process on free loopback ports, backed by `root` (default
//!     `~/.latiq/local`). Behaviour matches remote mode exactly.
//!   - `Latiq::connect("<url>", _)` — `<url>` is a remote control-plane endpoint
//!     (the `LATIQ_SERVER` semantics the CLI uses).
//!
//! Routing: pond create/resolve via the **control plane**, pond list via
//! **admin**, and data ops (query/describe/drop) via the **Data/Stream front
//! door** — the greeter forwards by pond (never node-direct, which is unroutable
//! behind a k8s LB). Reads stream over `ReadArrow` → Arrow `RecordBatch`es.
use anyhow::{anyhow, Context, Result};
use arrow::buffer::Buffer;
use arrow::ipc::reader::StreamDecoder;
use arrow::record_batch::RecordBatch;
use latiq_control_plane::{serve_control_plane, Registry};
use latiq_pond_node::{run_pond_node, PondNodeConfig};
use latiq_proto::v1::admin_client::AdminClient;
use latiq_proto::v1::control_client::ControlClient;
use latiq_proto::v1::data_client::DataClient;
use latiq_proto::v1::stream_client::StreamClient;
use latiq_proto::v1::*;
use std::collections::{BTreeMap, HashMap};
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;
use tonic::transport::Channel;
use tonic::Request;

type RtClient<T> = Result<T>;

/// A live Latiq client. Holds a tokio runtime + the control-plane endpoint; for
/// `"local"` it also owns the in-process servers (kept alive until drop).
pub struct Latiq {
    rt: Arc<tokio::runtime::Runtime>,
    control_endpoint: String,
    /// The Data+Stream front door: the in-process node (embedded) or the query
    /// gateway / control front door (remote). Data ops dial THIS and rely on the
    /// greeter to forward by pond — never node-direct (unroutable behind a LB).
    data_endpoint: String,
    /// Long-lived gRPC channels, created once. tonic's `Channel` is cheap to
    /// clone and multiplexes concurrent RPCs over ONE HTTP/2 connection, and
    /// `connect_lazy` reconnects on its own. Dialing per call instead (the old
    /// behaviour) opened a fresh TCP connection for every query, which collapses
    /// with `transport error` under sustained concurrent load.
    control_channel: Channel,
    data_channel: Channel,
    identity: String,
    /// Keeps the embedded control-plane + pond-node alive (None in remote mode).
    _local: Option<LocalCluster>,
}

/// One pond's metadata (from create/get/list).
#[derive(Debug, Clone)]
pub struct PondInfo {
    pub pond_id: String,
    pub name: String,
    pub node_id: String,
    pub tier: String,
    pub description: String,
}

/// A handle to a pond: metadata + SQL. `db.get_pond("x").query("SELECT …")`.
pub struct Pond<'a> {
    latiq: &'a Latiq,
    pub info: PondInfo,
}

impl Pond<'_> {
    /// Run SQL. Reads stream → Arrow batches (uncapped); writes return no rows.
    pub fn query(&self, sql: &str) -> Result<Vec<RecordBatch>> {
        self.latiq.query(&self.info.name, sql)
    }
    pub fn describe(&self) -> Result<serde_json::Value> {
        self.latiq.describe_pond(&self.info.name)
    }
    /// Explain a query plan (no execution).
    pub fn explain(&self, sql: &str) -> Result<serde_json::Value> {
        self.latiq.explain(&self.info.name, sql)
    }
    /// The pond's DuckLake snapshot history (who wrote what, when) — a read.
    pub fn snapshots(&self) -> Result<Vec<RecordBatch>> {
        self.latiq.snapshots(&self.info.name)
    }
    /// Load a curated dataset (by name, from `Latiq::list_datasets`) into this pond.
    pub fn load_dataset(&self, dataset: &str) -> Result<serde_json::Value> {
        self.latiq.load_dataset(&self.info.name, dataset)
    }
    /// Describe an external catalog's tables (attached transiently on this pond).
    /// `set`: runtime config + credentials (e.g. `{"token": "…"}`); never stored.
    pub fn describe_catalog(
        &self,
        catalog: &str,
        set: HashMap<String, String>,
    ) -> Result<serde_json::Value> {
        self.latiq.describe_catalog(&self.info.name, catalog, set)
    }
    /// Pull a subset of an external catalog into a pond table. `query` is the
    /// materialization SQL (e.g. `CREATE TABLE us AS SELECT … FROM lake.s.orders`).
    pub fn pull_catalog(
        &self,
        catalog: &str,
        query: &str,
        set: HashMap<String, String>,
    ) -> Result<serde_json::Value> {
        self.latiq
            .pull_catalog(&self.info.name, catalog, query, set)
    }
    pub fn name(&self) -> &str {
        &self.info.name
    }
    pub fn id(&self) -> &str {
        &self.info.pond_id
    }
    pub fn tier(&self) -> &str {
        &self.info.tier
    }
    pub fn description(&self) -> &str {
        &self.info.description
    }
}

impl Latiq {
    /// Connect. `server == "local"` starts an in-process cluster backed by `root`
    /// (default `~/.latiq/local`); any other value is a remote control-plane URL.
    pub fn connect(server: &str, root: Option<PathBuf>) -> Result<Self> {
        Self::connect_with(server, root, None)
    }

    /// `query_gateway`: the Data/Stream front door when it differs from `server`
    /// (e.g. nginx exposes Control/Admin and Data/Stream on separate addresses).
    /// `None` → reuse `server` (unified front door). Ignored for `"local"`.
    pub fn connect_with(
        server: &str,
        root: Option<PathBuf>,
        query_gateway: Option<&str>,
    ) -> Result<Self> {
        let rt = Arc::new(
            tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .worker_threads(2)
                .build()?,
        );
        if server == "local" {
            let root = root.unwrap_or_else(default_local_root);
            let local = rt.block_on(LocalCluster::start(&rt, &root))?;
            let control_endpoint = local.control_endpoint.clone();
            let data_endpoint = local.data_endpoint.clone();
            // `connect_lazy` registers with the reactor, so it must be built
            // inside the runtime context.
            let _guard = rt.enter();
            let control_channel = lazy_channel(&control_endpoint)?;
            let data_channel = lazy_channel(&data_endpoint)?;
            drop(_guard);
            Ok(Self {
                rt,
                control_endpoint,
                data_endpoint,
                control_channel,
                data_channel,
                identity: "sdk".into(),
                _local: Some(local),
            })
        } else {
            let control_endpoint = normalize_endpoint(server);
            let data_endpoint = query_gateway
                .map(normalize_endpoint)
                .unwrap_or_else(|| control_endpoint.clone());
            rt.block_on(wait_connectable(&control_endpoint))
                .with_context(|| format!("connecting to control plane at {server}"))?;
            // `connect_lazy` registers with the reactor, so it must be built
            // inside the runtime context.
            let _guard = rt.enter();
            let control_channel = lazy_channel(&control_endpoint)?;
            let data_channel = lazy_channel(&data_endpoint)?;
            drop(_guard);
            Ok(Self {
                rt,
                control_endpoint,
                data_endpoint,
                control_channel,
                data_channel,
                identity: "sdk".into(),
                _local: None,
            })
        }
    }

    /// The identity attributed to this client's writes (default `sdk`).
    pub fn with_identity(mut self, identity: impl Into<String>) -> Self {
        self.identity = identity.into();
        self
    }

    /// The control-plane endpoint this client is bound to.
    pub fn server(&self) -> &str {
        &self.control_endpoint
    }

    /// The Data+Stream front door this client sends queries to — the in-process
    /// node (embedded) or the query gateway (remote).
    pub fn query_gateway(&self) -> &str {
        &self.data_endpoint
    }

    /// Allocate a pond and return a handle. `description` is agent-discovery text.
    pub fn create_pond(
        &self,
        name: Option<&str>,
        tier: &str,
        description: &str,
    ) -> Result<Pond<'_>> {
        let info = self.rt.block_on(async {
            let mut c = self.control().await?;
            let r = c
                .create_pond_assignment(CreatePondAssignmentRequest {
                    name: name.unwrap_or_default().to_string(),
                    owner_identity: self.identity.clone(),
                    policy_json: "{}".into(),
                    tier: tier.to_string(),
                    extensions: vec![],
                    description: description.to_string(),
                })
                .await?
                .into_inner();
            let pond = c
                .get_pond_info(GetPondInfoRequest {
                    pond_ref: r.pond_id.clone(),
                })
                .await?
                .into_inner()
                .pond;
            Self::info_from_msg(pond, r.pond_id)
        })?;
        Ok(Pond { latiq: self, info })
    }

    /// Fetch a pond's metadata and return a handle (one round-trip).
    pub fn get_pond(&self, pond: &str) -> Result<Pond<'_>> {
        let info = self.rt.block_on(async {
            let mut c = self.control().await?;
            let resp = c
                .get_pond_info(GetPondInfoRequest {
                    pond_ref: pond.to_string(),
                })
                .await
                .map_err(|s| anyhow!("pond '{pond}': {}", s.message()))?
                .into_inner();
            Self::info_from_msg(resp.pond, pond.to_string())
        })?;
        Ok(Pond { latiq: self, info })
    }

    fn info_from_msg(msg: Option<PondInfoMsg>, fallback_id: String) -> Result<PondInfo> {
        let m = msg.ok_or_else(|| anyhow!("pond not found"))?;
        Ok(PondInfo {
            pond_id: if m.pond_id.is_empty() {
                fallback_id
            } else {
                m.pond_id
            },
            name: m.name,
            // PondInfoMsg carries node_endpoint, not node_id; node_id comes via list.
            node_id: String::new(),
            tier: m.tier,
            description: m.description,
        })
    }

    /// List ponds keyed by name (admin metadata read; works if nodes are down).
    pub fn list_ponds(&self) -> Result<BTreeMap<String, PondInfo>> {
        self.rt.block_on(async {
            let mut a = self.admin().await?;
            let resp = a.pond_list(PondListRequest {}).await?.into_inner();
            Ok(resp
                .ponds
                .into_iter()
                .map(|p| {
                    (
                        p.name.clone(),
                        PondInfo {
                            pond_id: p.pond_id,
                            name: p.name,
                            node_id: p.node_id,
                            tier: p.tier,
                            description: p.description,
                        },
                    )
                })
                .collect())
        })
    }

    /// Describe a pond's schema (node-direct).
    pub fn describe_pond(&self, pond: &str) -> Result<serde_json::Value> {
        self.rt.block_on(async {
            let mut d = self.data().await?;
            let resp = d
                .describe_pond(self.with_id(DescribePondRequest {
                    pond: pond.to_string(),
                }))
                .await?
                .into_inner();
            parse_json(&resp.json)
        })
    }

    /// Drop a pond and all its data (`confirm` must be true).
    pub fn drop_pond(&self, pond: &str, confirm: bool) -> Result<()> {
        self.rt.block_on(async {
            let mut d = self.data().await?;
            d.drop_pond(self.with_id(DropPondRequest {
                pond: pond.to_string(),
                confirm,
            }))
            .await?;
            Ok(())
        })
    }

    /// Run SQL against `pond`. One verb: reads stream over `ReadArrow` and return
    /// Arrow batches (uncapped); writes go unary (attributed/snapshotted
    /// server-side) and return no rows. The client classifies by statement —
    /// callers don't pick read vs write.
    pub fn query(&self, pond: &str, sql: &str) -> Result<Vec<RecordBatch>> {
        decode_ipc(&self.query_ipc(pond, sql)?)
    }

    /// The single read-or-write path, returning the read's Arrow IPC stream bytes
    /// (schema + batches) — for FFI consumers (Python `pyarrow.ipc.open_stream`)
    /// and for `query` to decode. Reads stream over `ReadArrow` (uncapped); writes
    /// execute unary and return empty bytes (writes yield no rows; their snapshot
    /// is recorded server-side). `query` and `query_ipc` share this so the
    /// classify + dispatch logic lives in one place.
    pub fn query_ipc(&self, pond: &str, sql: &str) -> Result<Vec<u8>> {
        self.rt.block_on(async {
            if !latiq_engine::is_read_only(sql) {
                let mut d = self.data().await?;
                d.write_query(self.with_id(QueryRequest {
                    pond: pond.to_string(),
                    sql: sql.to_string(),
                }))
                .await
                .map_err(|s| anyhow!("write: {}", s.message()))?;
                return Ok(Vec::new());
            }
            let mut sc = self.stream().await?;
            let mut streaming = sc
                .read_arrow(self.with_id(QueryRequest {
                    pond: pond.to_string(),
                    sql: sql.to_string(),
                }))
                .await
                .map_err(|s| anyhow!("read: {}", s.message()))?
                .into_inner();
            let mut out = Vec::new();
            while let Some(chunk) = streaming.message().await? {
                out.extend_from_slice(&chunk.ipc);
            }
            Ok(out)
        })
    }

    /// Explain a query plan against `pond` (no execution). Returns the plan JSON.
    pub fn explain(&self, pond: &str, sql: &str) -> Result<serde_json::Value> {
        self.rt.block_on(async {
            let mut d = self.data().await?;
            let resp = d
                .explain_query(self.with_id(QueryRequest {
                    pond: pond.to_string(),
                    sql: sql.to_string(),
                }))
                .await
                .map_err(|s| anyhow!("explain: {}", s.message()))?
                .into_inner();
            parse_json(&resp.json)
        })
    }

    /// `pond`'s DuckLake snapshot history as Arrow batches (a read on
    /// `<pond>.snapshots()`). `pond` must be the pond NAME (the catalog name).
    pub fn snapshots(&self, pond: &str) -> Result<Vec<RecordBatch>> {
        decode_ipc(&self.snapshots_ipc(pond)?)
    }

    /// `pond`'s snapshot history as Arrow IPC bytes (the Python boundary).
    pub fn snapshots_ipc(&self, pond: &str) -> Result<Vec<u8>> {
        let sql = format!(
            "SELECT * FROM {}.snapshots() ORDER BY snapshot_id",
            quote_ident(pond)
        );
        self.query_ipc(pond, &sql)
    }

    /// Curated datasets (control-plane metadata), keyed by name. `query`: `""` =
    /// all, `"#tag"`, `"prefix*"`, or a substring.
    pub fn list_datasets(&self, query: &str) -> Result<serde_json::Value> {
        self.rt.block_on(async {
            let mut c = self.control().await?;
            let resp = c
                .list_datasets(ListDatasetsRequest {
                    query: query.to_string(),
                })
                .await?
                .into_inner();
            let mut map = serde_json::Map::new();
            for ds in resp.datasets {
                let tables: Vec<_> = ds
                    .tables
                    .into_iter()
                    .map(|t| {
                        serde_json::json!({
                            "table_name": t.table_name, "source_uri": t.source_uri, "format": t.format,
                        })
                    })
                    .collect();
                map.insert(
                    ds.name,
                    serde_json::json!({
                        "description": ds.description, "tags": ds.tags, "tables": tables,
                        "created_by": ds.created_by, "created_at": ds.created_at,
                    }),
                );
            }
            Ok(serde_json::Value::Object(map))
        })
    }

    /// External catalogs (control-plane metadata), keyed by name.
    pub fn list_catalogs(&self, query: &str) -> Result<serde_json::Value> {
        self.rt.block_on(async {
            let mut c = self.control().await?;
            let resp = c
                .list_catalogs(ListCatalogsRequest {
                    query: query.to_string(),
                })
                .await?
                .into_inner();
            let mut map = serde_json::Map::new();
            for cat in resp.catalogs {
                map.insert(
                    cat.name,
                    serde_json::json!({
                        "type": cat.r#type, "params": cat.params, "description": cat.description,
                        "tags": cat.tags, "created_by": cat.created_by, "created_at": cat.created_at,
                    }),
                );
            }
            Ok(serde_json::Value::Object(map))
        })
    }

    /// Load a curated dataset into `pond` (copies its files in; one schema per
    /// dataset). Returns the load summary.
    pub fn load_dataset(&self, pond: &str, dataset: &str) -> Result<serde_json::Value> {
        self.rt.block_on(async {
            let mut d = self.data().await?;
            let resp = d
                .load_dataset(self.with_id(LoadDatasetRequest {
                    pond: pond.to_string(),
                    dataset: dataset.to_string(),
                }))
                .await
                .map_err(|s| anyhow!("load_dataset: {}", s.message()))?
                .into_inner();
            parse_json(&resp.json)
        })
    }

    /// Describe an external catalog's tables (attached transiently on `pond`).
    /// `set` carries runtime config + credentials; never stored.
    pub fn describe_catalog(
        &self,
        pond: &str,
        catalog: &str,
        set: HashMap<String, String>,
    ) -> Result<serde_json::Value> {
        self.rt.block_on(async {
            let mut d = self.data().await?;
            let resp = d
                .catalog_describe(self.with_id(CatalogDescribeRequest {
                    pond: pond.to_string(),
                    catalog: catalog.to_string(),
                    params: set,
                }))
                .await
                .map_err(|s| anyhow!("describe_catalog: {}", s.message()))?
                .into_inner();
            parse_json(&resp.json)
        })
    }

    /// Pull a subset of an external catalog into a pond table. `query` is the
    /// materialization SQL. `set` carries runtime config + credentials; never stored.
    pub fn pull_catalog(
        &self,
        pond: &str,
        catalog: &str,
        query: &str,
        set: HashMap<String, String>,
    ) -> Result<serde_json::Value> {
        self.rt.block_on(async {
            let mut d = self.data().await?;
            let resp = d
                .catalog_pull(self.with_id(CatalogPullRequest {
                    pond: pond.to_string(),
                    catalog: catalog.to_string(),
                    query: query.to_string(),
                    params: set,
                }))
                .await
                .map_err(|s| anyhow!("pull_catalog: {}", s.message()))?
                .into_inner();
            parse_json(&resp.json)
        })
    }

    // ── client helpers ──────────────────────────────────────────────

    async fn control(&self) -> RtClient<ControlClient<Channel>> {
        Ok(ControlClient::new(self.control_channel.clone()))
    }

    async fn admin(&self) -> RtClient<AdminClient<Channel>> {
        Ok(AdminClient::new(self.control_channel.clone()))
    }

    /// A Data gRPC client on the front door. The greeter forwards by pond — we do
    /// NOT resolve the owner node directly (its address is unroutable behind a LB).
    async fn data(&self) -> RtClient<DataClient<Channel>> {
        Ok(DataClient::new(self.data_channel.clone()))
    }

    /// A Stream gRPC client on the front door (served alongside Data on the same
    /// endpoint). Reads ride `ReadArrow`; the greeter forwards by pond.
    async fn stream(&self) -> RtClient<StreamClient<Channel>> {
        Ok(StreamClient::new(self.data_channel.clone()))
    }

    /// Attach the (relaxed) identity header data ops carry.
    fn with_id<T>(&self, msg: T) -> Request<T> {
        let mut r = Request::new(msg);
        if let Ok(v) = self.identity.parse() {
            r.metadata_mut().insert("latiq-agent-id", v);
        }
        r
    }
}

/// An in-process control-plane + pond-node, alive for the life of a local `Latiq`.
struct LocalCluster {
    control_endpoint: String,
    data_endpoint: String,
}

impl LocalCluster {
    async fn start(rt: &Arc<tokio::runtime::Runtime>, root: &Path) -> Result<Self> {
        std::fs::create_dir_all(root.join("ponds"))?;
        let cp_port = free_port()?;
        let (data_port, mcp_port) = (free_port()?, free_port()?);
        let cp_addr: SocketAddr = format!("127.0.0.1:{cp_port}").parse()?;
        let control_endpoint = format!("http://127.0.0.1:{cp_port}");

        // Control plane (Control + Admin on one addr) over a persistent registry.
        let registry = Registry::open(Some(&root.join("registry.duckdb")))
            .map_err(|e| anyhow!("open registry: {e}"))?;
        rt.spawn(async move {
            let _ = serve_control_plane(cp_addr, registry).await;
        });
        wait_connectable(&control_endpoint).await?;

        // Pond node: self-registers with the control plane, serves Data gRPC.
        let cfg = PondNodeConfig {
            node_id: "local".into(),
            mcp_addr: format!("127.0.0.1:{mcp_port}").parse()?,
            data_addr: format!("127.0.0.1:{data_port}").parse()?,
            internal_endpoint: format!("http://127.0.0.1:{data_port}"),
            control_endpoint: control_endpoint.clone(),
            data_dir: root.join("ponds"),
            metrics_addr: None,
            // The embedded stack is in-process and single-user: relaxed identity.
            auth: None,
        };
        rt.spawn(async move {
            let _ = run_pond_node(cfg).await;
        });
        wait_for_active_node(&control_endpoint).await?;
        // The node registers as `active` (heartbeat) BEFORE its Data/Stream gRPC
        // server is necessarily accepting connections — so also wait for the data
        // port to be dialable, or the first query races and fails "data plane
        // unreachable".
        let data_endpoint = format!("http://127.0.0.1:{data_port}");
        wait_connectable(&data_endpoint).await?;
        Ok(Self {
            control_endpoint,
            data_endpoint,
        })
    }
}

fn default_local_root() -> PathBuf {
    let home = std::env::var_os("HOME").map(PathBuf::from);
    home.unwrap_or_else(|| PathBuf::from("."))
        .join(".latiq")
        .join("local")
}

fn free_port() -> Result<u16> {
    let l = std::net::TcpListener::bind("127.0.0.1:0")?;
    Ok(l.local_addr()?.port())
}

/// Accept `grpc://`, `http(s)://`, or a bare `host:port` and produce an
/// `http(s)://` endpoint tonic can dial.
fn normalize_endpoint(server: &str) -> String {
    let s = server.trim();
    if let Some(rest) = s.strip_prefix("grpc://") {
        format!("http://{rest}")
    } else if s.starts_with("http://") || s.starts_with("https://") {
        s.to_string()
    } else {
        format!("http://{s}")
    }
}

/// Quote a SQL identifier (double-quote, escaping internal quotes) so a pond name
/// is safe to interpolate, e.g. into `<pond>.snapshots()`.
fn quote_ident(s: &str) -> String {
    format!("\"{}\"", s.replace('"', "\"\""))
}

/// Decode an Arrow IPC stream (schema + batches) into RecordBatches. Empty bytes
/// (a write, or an empty read with no payload) → an empty Vec.
fn decode_ipc(ipc: &[u8]) -> Result<Vec<RecordBatch>> {
    if ipc.is_empty() {
        return Ok(Vec::new());
    }
    let mut decoder = StreamDecoder::new();
    let mut buf = Buffer::from_vec(ipc.to_vec());
    let mut batches = Vec::new();
    while !buf.is_empty() {
        match decoder
            .decode(&mut buf)
            .map_err(|e| anyhow!("arrow ipc: {e}"))?
        {
            Some(batch) => batches.push(batch),
            None => break,
        }
    }
    Ok(batches)
}

fn parse_json(s: &str) -> Result<serde_json::Value> {
    serde_json::from_str(s).map_err(|e| anyhow!("decode response: {e}"))
}

async fn wait_connectable(endpoint: &str) -> Result<()> {
    for _ in 0..200 {
        if let Ok(ch) = Channel::from_shared(endpoint.to_string()) {
            if ch.connect().await.is_ok() {
                return Ok(());
            }
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    Err(anyhow!("endpoint never became connectable: {endpoint}"))
}

async fn wait_for_active_node(control_endpoint: &str) -> Result<()> {
    for _ in 0..400 {
        if let Ok(mut c) = AdminClient::connect(control_endpoint.to_string()).await {
            if let Ok(resp) = c.list_nodes(ListNodesRequest {}).await {
                if resp.into_inner().nodes.iter().any(|n| n.state == "active") {
                    return Ok(());
                }
            }
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    Err(anyhow!("local pond node did not register in time"))
}

/// A lazily-connecting, auto-reconnecting channel to `endpoint`. Built once per
/// endpoint and cloned per RPC: tonic multiplexes concurrent calls over one
/// HTTP/2 connection, so a busy client no longer opens a TCP connection per query.
fn lazy_channel(endpoint: &str) -> Result<Channel> {
    Ok(Channel::from_shared(endpoint.to_string())
        .with_context(|| format!("invalid endpoint {endpoint}"))?
        .connect_lazy())
}
