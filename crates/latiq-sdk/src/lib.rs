// Copyright 2026 Neonexia
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

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
    /// The OAuth bearer token presented on every request, when the deployment
    /// requires one. Held BESIDE the channels, never in them: a `Channel` is
    /// shared and cached, while tonic metadata is per request.
    token: Option<String>,
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
    /// Whether this pond records OpenLineage events (into the pond's own
    /// `lineage/` directory on its node). Fixed at creation.
    pub lineage: bool,
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
    /// Plan a query without executing it: `estimated_rows`, one
    /// `scan_operations` entry per table read, derived `warnings`/`suggestions`,
    /// and `raw_plan`. Every number is the optimiser ESTIMATING.
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

/// The bearer token a client presents: an explicit value wins outright — a
/// BLANK one included, which means "explicitly no token" and must NOT fall back
/// to `$LATIQ_TOKEN`. `e2e/sdk/test_auth.py`'s `anon_db` builds its deliberately
/// anonymous client that way, and an env fallback there would silently turn the
/// negative auth tests green. Blank resolves to `None` in either direction: an
/// empty `Authorization: Bearer ` is rejected as malformed rather than absent.
fn resolve_token(explicit: Option<&str>, from_env: Option<String>) -> Option<String> {
    explicit
        .map(str::to_string)
        .or(from_env)
        .map(|t| t.trim().to_string())
        .filter(|t| !t.is_empty())
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
        Self::connect_with_token(server, root, query_gateway, None)
    }

    /// As `connect_with`, plus the OAuth bearer token this client presents.
    ///
    /// `token = None` falls back to `$LATIQ_TOKEN`, so a notebook or a job can be
    /// handed a credential without threading it through the call. Either way a
    /// blank value means "no token": an empty `Authorization: Bearer ` header is
    /// worse than none, since it is rejected as malformed rather than as absent.
    /// Against a deployment with no issuer configured the token is simply unused.
    pub fn connect_with_token(
        server: &str,
        root: Option<PathBuf>,
        query_gateway: Option<&str>,
        token: Option<&str>,
    ) -> Result<Self> {
        let token = resolve_token(token, std::env::var("LATIQ_TOKEN").ok());
        let rt = Arc::new(
            tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .worker_threads(2)
                .build()?,
        );
        if server == "local" {
            let root = root.unwrap_or_else(default_local_root);
            let auth = BearerAuth {
                identity: DEFAULT_IDENTITY.into(),
                token: token.clone(),
            };
            let local = rt.block_on(LocalCluster::start(&rt, &root, auth))?;
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
                identity: DEFAULT_IDENTITY.into(),
                token,
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
                identity: DEFAULT_IDENTITY.into(),
                token,
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
    ///
    /// **Eager**: this returns only once the pond's storage exists on the node
    /// the control plane placed it on. If that node is unreachable the call
    /// fails, the assignment is rolled back, and the same name is free to try
    /// again — you never get a handle to a pond that does not exist.
    ///
    /// `lineage` records OpenLineage provenance for every query (written as JSONL
    /// into the pond's own `lineage/` directory on its node; agents read it with
    /// the `get_lineage` MCP tool). The choice is made here and is **fixed for the
    /// pond's lifetime and cannot be enabled later** — turning it on later would
    /// leave a hole at the start of the record. Off (`false`) costs nothing.
    pub fn create_pond(
        &self,
        name: Option<&str>,
        tier: &str,
        description: &str,
        lineage: bool,
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
                    lineage,
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
            // PondInfoMsg now carries the owning node's id as well as its
            // endpoint (#89), so describe agrees with list instead of leaving
            // this blank.
            node_id: m.node_id,
            tier: m.tier,
            description: m.description,
            lineage: m.lineage,
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
                            lineage: p.lineage,
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
                .describe_pond(DescribePondRequest {
                    pond: pond.to_string(),
                })
                .await?
                .into_inner();
            parse_json(&resp.json)
        })
    }

    /// Drop a pond and all its data (`confirm` must be true).
    pub fn drop_pond(&self, pond: &str, confirm: bool) -> Result<()> {
        self.rt.block_on(async {
            let mut d = self.data().await?;
            d.drop_pond(DropPondRequest {
                pond: pond.to_string(),
                confirm,
            })
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
                d.write_query(QueryRequest {
                    pond: pond.to_string(),
                    sql: sql.to_string(),
                    // The node's default. The SDK does not surface a
                    // per-statement timeout yet; 0 is "unset" on the wire.
                    timeout_ms: 0,
                })
                .await
                .map_err(|s| anyhow!("write: {}", s.message()))?;
                return Ok(Vec::new());
            }
            let mut sc = self.stream().await?;
            let mut streaming = sc
                .read_arrow(QueryRequest {
                    pond: pond.to_string(),
                    sql: sql.to_string(),
                    // The node's default. The SDK does not surface a
                    // per-statement timeout yet; 0 is "unset" on the wire.
                    timeout_ms: 0,
                })
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

    /// Plan a query against `pond` without executing it. Returns the plan
    /// JSON: `estimated_rows` (the result size the optimiser expects), one
    /// `scan_operations` entry per table read (`table`, `scan_type`,
    /// `estimated_rows_scanned`, `source`), derived `warnings`/`suggestions`,
    /// and `raw_plan`. These are ESTIMATES — nothing runs, and there is no time
    /// or byte figure because the plan predicts neither.
    pub fn explain(&self, pond: &str, sql: &str) -> Result<serde_json::Value> {
        self.rt.block_on(async {
            let mut d = self.data().await?;
            let resp = d
                .explain_query(QueryRequest {
                    pond: pond.to_string(),
                    sql: sql.to_string(),
                    // The node's default. The SDK does not surface a
                    // per-statement timeout yet; 0 is "unset" on the wire.
                    timeout_ms: 0,
                })
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
                .load_dataset(LoadDatasetRequest {
                    pond: pond.to_string(),
                    dataset: dataset.to_string(),
                })
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
                .catalog_describe(CatalogDescribeRequest {
                    pond: pond.to_string(),
                    catalog: catalog.to_string(),
                    params: set,
                })
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
                .catalog_pull(CatalogPullRequest {
                    pond: pond.to_string(),
                    catalog: catalog.to_string(),
                    query: query.to_string(),
                    params: set,
                })
                .await
                .map_err(|s| anyhow!("pull_catalog: {}", s.message()))?
                .into_inner();
            parse_json(&resp.json)
        })
    }

    // ── client helpers ──────────────────────────────────────────────

    /// This client's identity headers, installed on every client below.
    fn auth(&self) -> BearerAuth {
        BearerAuth {
            identity: self.identity.clone(),
            token: self.token.clone(),
        }
    }

    async fn control(&self) -> RtClient<ControlClient<Authed>> {
        Ok(ControlClient::with_interceptor(
            self.control_channel.clone(),
            self.auth(),
        ))
    }

    async fn admin(&self) -> RtClient<AdminClient<Authed>> {
        Ok(AdminClient::with_interceptor(
            self.control_channel.clone(),
            self.auth(),
        ))
    }

    /// A Data gRPC client on the front door. The greeter forwards by pond — we do
    /// NOT resolve the owner node directly (its address is unroutable behind a LB).
    async fn data(&self) -> RtClient<DataClient<Authed>> {
        Ok(DataClient::with_interceptor(
            self.data_channel.clone(),
            self.auth(),
        ))
    }

    /// A Stream gRPC client on the front door (served alongside Data on the same
    /// endpoint). Reads ride `ReadArrow`; the greeter forwards by pond.
    async fn stream(&self) -> RtClient<StreamClient<Authed>> {
        Ok(StreamClient::with_interceptor(
            self.data_channel.clone(),
            self.auth(),
        ))
    }
}

/// The identity this client presents on EVERY request: the CLAIMED leaf, plus
/// the bearer token that actually proves who we are where one is configured.
///
/// An interceptor rather than a per-call-site wrapper, and that is the whole
/// point. The wrapper it replaces had to be remembered at each of a dozen call
/// sites, and `list_ponds` — the SDK's one Admin call, on the surface least like
/// the others — was the one that forgot, so a fully-tokened client was refused
/// by the control plane. Here there is nothing to remember: a new RPC carries
/// both headers because it cannot be issued any other way.
///
/// Both still ride per-request metadata; the shared, cached `Channel` carries
/// neither.
#[derive(Clone)]
struct BearerAuth {
    identity: String,
    token: Option<String>,
}

impl tonic::service::Interceptor for BearerAuth {
    // The signature is tonic's, so the `Err` type is not ours to box.
    #[allow(clippy::result_large_err)]
    fn call(&mut self, mut req: Request<()>) -> Result<Request<()>, tonic::Status> {
        if let Ok(v) = self.identity.parse() {
            req.metadata_mut().insert("latiq-agent-id", v);
        }
        if let Some(t) = self.token.as_deref() {
            if let Ok(v) = format!("Bearer {t}").parse() {
                req.metadata_mut().insert("authorization", v);
            }
        }
        Ok(req)
    }
}

/// The claimed leaf the SDK presents when the caller sets none.
const DEFAULT_IDENTITY: &str = "sdk";

/// What every SDK gRPC client rides: a cached channel plus the identity headers.
type Authed = tonic::service::interceptor::InterceptedService<Channel, BearerAuth>;

/// An in-process control-plane + pond-node, alive for the life of a local `Latiq`.
struct LocalCluster {
    control_endpoint: String,
    data_endpoint: String,
}

impl LocalCluster {
    /// `auth` is only used by the readiness probe below; the embedded control
    /// plane and node are started with no verifier (relaxed identity).
    async fn start(
        rt: &Arc<tokio::runtime::Runtime>,
        root: &Path,
        auth: BearerAuth,
    ) -> Result<Self> {
        std::fs::create_dir_all(root.join("ponds"))?;
        let cp_port = free_port()?;
        let (data_port, mcp_port) = (free_port()?, free_port()?);
        let cp_addr: SocketAddr = format!("127.0.0.1:{cp_port}").parse()?;
        let control_endpoint = format!("http://127.0.0.1:{cp_port}");

        // Control plane (Control + Admin on one addr) over a persistent registry.
        let registry = Registry::open(Some(&root.join("registry.duckdb")))
            .map_err(|e| anyhow!("open registry: {e}"))?;
        rt.spawn(async move {
            // Embedded: no issuer, so identity stays relaxed (claimed).
            let _ = serve_control_plane(cp_addr, registry, None).await;
        });
        wait_connectable(&control_endpoint).await?;

        // Pond node: self-registers with the control plane, serves Data gRPC.
        let cfg = PondNodeConfig {
            node_id: "local".into(),
            mcp_addr: format!("127.0.0.1:{mcp_port}").parse()?,
            data_addr: format!("127.0.0.1:{data_port}").parse()?,
            internal_endpoint: format!("http://127.0.0.1:{data_port}"),
            // Embedded: the client dials this very process on loopback, so the
            // derived URL is already the one it reaches.
            public_mcp_url: None,
            control_endpoint: control_endpoint.clone(),
            data_dir: root.join("ponds"),
            metrics_addr: None,
            // The embedded stack is in-process and single-user: relaxed identity.
            auth: None,
            // The embedded stack has no configuration surface for a backend and
            // often no network at all; lineage-enabled ponds still write their
            // own files, which is what `get_lineage` reads.
            lineage_backend_url: None,
            // The shipped default/maximum; the embedded cluster exposes no
            // knob for them.
            timeouts: latiq_common::QueryTimeouts::default(),
        };
        rt.spawn(async move {
            let _ = run_pond_node(cfg).await;
        });
        wait_for_active_node(&control_endpoint, auth).await?;
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

/// Poll the registry until the embedded node reports `active`. `auth` is the
/// SAME interceptor every other client gets: `list_nodes` is an Admin RPC, and
/// an Admin RPC without the identity headers is the defect this module's
/// interceptor exists to make impossible — the embedded control plane happens
/// not to verify today, which is exactly how such a call survives review.
async fn wait_for_active_node(control_endpoint: &str, auth: BearerAuth) -> Result<()> {
    for _ in 0..400 {
        if let Ok(ch) = Channel::from_shared(control_endpoint.to_string()) {
            let mut c = AdminClient::with_interceptor(ch.connect_lazy(), auth.clone());
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

/// The token-resolution rules, pinned where they are pure (no server, no env
/// mutation, so no cross-test races): `resolve_token` takes the environment's
/// value as an argument precisely so this can be a unit test.
#[cfg(test)]
mod token_resolution {
    use super::resolve_token;

    #[test]
    fn an_explicit_token_wins_over_the_environment() {
        assert_eq!(
            resolve_token(Some("explicit"), Some("from-env".into())),
            Some("explicit".to_string())
        );
    }

    #[test]
    fn no_explicit_token_falls_back_to_the_environment() {
        assert_eq!(
            resolve_token(None, Some("from-env".into())),
            Some("from-env".to_string())
        );
        assert_eq!(resolve_token(None, None), None);
    }

    #[test]
    fn a_blank_explicit_token_means_anonymous_and_ignores_the_environment() {
        // The property `e2e/sdk/test_auth.py`'s `anon_db` relies on: asking for
        // no token must produce no token even when `$LATIQ_TOKEN` holds a valid
        // one, or every negative auth test would pass while authenticated.
        for blank in ["", "   ", "\t\n"] {
            assert_eq!(
                resolve_token(Some(blank), Some("a-valid-token".into())),
                None,
                "an explicitly blank token ({blank:?}) must not fall back to $LATIQ_TOKEN"
            );
        }
    }

    #[test]
    fn a_blank_environment_token_is_no_token_rather_than_an_empty_bearer() {
        // `LATIQ_TOKEN=` in a compose file or a `.env` is "unset", not a header.
        assert_eq!(resolve_token(None, Some("".into())), None);
        assert_eq!(resolve_token(None, Some("  ".into())), None);
        // …and a real one is trimmed, not passed through with its whitespace.
        assert_eq!(
            resolve_token(None, Some(" tok \n".into())),
            Some("tok".to_string())
        );
    }
}

/// Structural guards on how this file builds gRPC clients.
///
/// A unit module rather than an integration test: both assertions are
/// `include_str!` greps over this very file with no runtime dependency at all,
/// and an integration binary would statically link a bundled DuckDB (~130-160
/// MB) to run two string searches. See `crates/latiq/tests/CLAUDE.md` rule 1.
///
/// The identity headers (`latiq-agent-id` and, where a deployment configures an
/// issuer, `authorization: Bearer …`) are attached by the `BearerAuth`
/// interceptor installed in the four client helpers. That makes CONSTRUCTION the
/// only place a request can lose them: an interceptor cannot be forgotten at a
/// call site, but a client built straight from a raw `Channel` has no
/// interceptor at all and would send neither header.
///
/// This is the shape of bug that shipped once already — `list_ponds` issued the
/// SDK's one Admin call without the wrapper that carried the token, so a
/// fully-tokened client was refused by the control plane. The wrapper is gone;
/// these tests are what keep its replacement from being routed around. The
/// behavioural half already exists: `crates/latiq/tests/admin.rs` drives Data,
/// Stream and Admin against an authenticated stack.
#[cfg(test)]
mod client_construction {
    /// Every way tonic exposes to build a client from a channel.
    /// `with_interceptor` is the only one that installs `BearerAuth`; the others
    /// hand back a bare client that would send no identity at all.
    const UNAUTHENTICATED_CONSTRUCTORS: &[&str] = &["::new(", "::connect(", "::with_origin("];

    const CLIENTS: &[&str] = &["ControlClient", "AdminClient", "DataClient", "StreamClient"];

    #[test]
    fn every_grpc_client_is_built_with_the_auth_interceptor() {
        let src = include_str!("lib.rs");
        for client in CLIENTS {
            for ctor in UNAUTHENTICATED_CONSTRUCTORS {
                let pattern = format!("{client}{ctor}");
                assert!(
                    !src.contains(&pattern),
                    "`{pattern}` builds a client with no interceptor, so it would send \
                     neither `latiq-agent-id` nor the bearer token. Every gRPC client \
                     must come from the helper that installs `BearerAuth` — see the \
                     `list_ponds` regression in crates/latiq/tests/admin.rs."
                );
            }
            // …and each client really is built, the authenticated way, at least
            // once: a guard that passes because nothing matches is no guard.
            //
            // Deliberately NOT pinned to exactly one. The invariant is "every
            // construction is authenticated", not "there is only one construction" —
            // `AdminClient` legitimately has two (the `admin()` helper and the
            // embedded readiness probe). Pinning the count would make a future
            // honest builder look like a violation, and the pressure would be to
            // relax this test rather than to route the new builder correctly.
            let authed = src.matches(&format!("{client}::with_interceptor")).count();
            assert!(
                authed >= 1,
                "no authenticated `{client}` builder found — the assertions above \
                 would then be passing vacuously"
            );
        }
    }

    /// The interceptor is only worth anything if it attaches both headers.
    #[test]
    fn the_interceptor_attaches_both_identity_headers() {
        let src = include_str!("lib.rs");
        let body = src
            .split_once("impl tonic::service::Interceptor for BearerAuth")
            .expect("the SDK's interceptor is named BearerAuth")
            .1;
        assert!(
            body.contains("latiq-agent-id"),
            "the claimed leaf is missing"
        );
        assert!(
            body.contains("authorization"),
            "the bearer token is missing — this is exactly the list_ponds bug"
        );
    }
}
