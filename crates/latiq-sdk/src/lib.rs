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
use std::collections::BTreeMap;
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
            Ok(Self {
                rt,
                control_endpoint,
                data_endpoint,
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
            Ok(Self {
                rt,
                control_endpoint,
                data_endpoint,
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

    // ── client helpers ──────────────────────────────────────────────

    async fn control(&self) -> RtClient<ControlClient<Channel>> {
        ControlClient::connect(self.control_endpoint.clone())
            .await
            .map_err(|e| {
                anyhow!(
                    "control plane unreachable at {}: {e}",
                    self.control_endpoint
                )
            })
    }

    async fn admin(&self) -> RtClient<AdminClient<Channel>> {
        AdminClient::connect(self.control_endpoint.clone())
            .await
            .map_err(|e| {
                anyhow!(
                    "control plane unreachable at {}: {e}",
                    self.control_endpoint
                )
            })
    }

    /// A Data gRPC client on the front door. The greeter forwards by pond — we do
    /// NOT resolve the owner node directly (its address is unroutable behind a LB).
    async fn data(&self) -> RtClient<DataClient<Channel>> {
        DataClient::connect(self.data_endpoint.clone())
            .await
            .map_err(|e| anyhow!("data plane unreachable at {}: {e}", self.data_endpoint))
    }

    /// A Stream gRPC client on the front door (served alongside Data on the same
    /// endpoint). Reads ride `ReadArrow`; the greeter forwards by pond.
    async fn stream(&self) -> RtClient<StreamClient<Channel>> {
        StreamClient::connect(self.data_endpoint.clone())
            .await
            .map_err(|e| anyhow!("stream plane unreachable at {}: {e}", self.data_endpoint))
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
        };
        rt.spawn(async move {
            let _ = run_pond_node(cfg).await;
        });
        wait_for_active_node(&control_endpoint).await?;
        Ok(Self {
            control_endpoint,
            data_endpoint: format!("http://127.0.0.1:{data_port}"),
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
