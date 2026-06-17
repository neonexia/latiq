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
//! Routing mirrors the CLI: pond create/resolve via the **control plane**, pond
//! list via **admin**, and data ops (describe/drop/read/write) **node-direct**
//! against the owning node's Data gRPC.
use anyhow::{anyhow, Context, Result};
use latiq_control_plane::{serve_control_plane, Registry};
use latiq_pond_node::{run_pond_node, PondNodeConfig};
use latiq_proto::v1::admin_client::AdminClient;
use latiq_proto::v1::control_client::ControlClient;
use latiq_proto::v1::data_client::DataClient;
use latiq_proto::v1::*;
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
    identity: String,
    /// Keeps the embedded control-plane + pond-node alive (None in remote mode).
    _local: Option<LocalCluster>,
}

/// One pond's identity, returned by create/list.
#[derive(Debug, Clone)]
pub struct Pond {
    pub pond_id: String,
    pub name: String,
    pub node_id: String,
    pub tier: String,
}

impl Latiq {
    /// Connect. `server == "local"` starts an in-process cluster backed by `root`
    /// (default `~/.latiq/local`); any other value is a remote control-plane URL.
    pub fn connect(server: &str, root: Option<PathBuf>) -> Result<Self> {
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
            Ok(Self {
                rt,
                control_endpoint,
                identity: "sdk".into(),
                _local: Some(local),
            })
        } else {
            let control_endpoint = normalize_endpoint(server);
            rt.block_on(wait_connectable(&control_endpoint))
                .with_context(|| format!("connecting to control plane at {server}"))?;
            Ok(Self {
                rt,
                control_endpoint,
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

    /// Allocate a pond (pure control-plane op; the registry assigns a node).
    pub fn create_pond(&self, name: Option<&str>, tier: &str) -> Result<Pond> {
        self.rt.block_on(async {
            let mut c = self.control().await?;
            let r = c
                .create_pond_assignment(CreatePondAssignmentRequest {
                    name: name.unwrap_or_default().to_string(),
                    owner_identity: self.identity.clone(),
                    policy_json: "{}".into(),
                    tier: tier.to_string(),
                    extensions: vec![],
                })
                .await?
                .into_inner();
            let info = c
                .get_pond_info(GetPondInfoRequest {
                    pond_ref: r.pond_id.clone(),
                })
                .await?
                .into_inner()
                .pond;
            Ok(Pond {
                pond_id: r.pond_id,
                name: info.as_ref().map(|p| p.name.clone()).unwrap_or_default(),
                node_id: String::new(),
                tier: info.map(|p| p.tier).unwrap_or_default(),
            })
        })
    }

    /// List ponds (control-plane metadata read; works even if nodes are down).
    pub fn list_ponds(&self) -> Result<Vec<Pond>> {
        self.rt.block_on(async {
            let mut a = self.admin().await?;
            let resp = a.pond_list(PondListRequest {}).await?.into_inner();
            Ok(resp
                .ponds
                .into_iter()
                .map(|p| Pond {
                    pond_id: p.pond_id,
                    name: p.name,
                    node_id: p.node_id,
                    tier: p.tier,
                })
                .collect())
        })
    }

    /// Describe a pond's schema (node-direct).
    pub fn describe_pond(&self, pond: &str) -> Result<serde_json::Value> {
        self.rt.block_on(async {
            let mut d = self.data_for(pond).await?;
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
            let mut d = self.data_for(pond).await?;
            d.drop_pond(self.with_id(DropPondRequest {
                pond: pond.to_string(),
                confirm,
            }))
            .await?;
            Ok(())
        })
    }

    /// Run a read-only query; returns the `{columns, rows, …}` result as JSON.
    pub fn read(&self, pond: &str, sql: &str) -> Result<serde_json::Value> {
        self.query(pond, sql, false)
    }

    /// Run a write (INSERT/UPDATE/DELETE/DDL), attributed to this client.
    pub fn write(&self, pond: &str, sql: &str) -> Result<serde_json::Value> {
        self.query(pond, sql, true)
    }

    fn query(&self, pond: &str, sql: &str, write: bool) -> Result<serde_json::Value> {
        self.rt.block_on(async {
            let mut d = self.data_for(pond).await?;
            let req = self.with_id(QueryRequest {
                pond: pond.to_string(),
                sql: sql.to_string(),
            });
            let resp = if write {
                d.write_query(req).await
            } else {
                d.read_query(req).await
            }?
            .into_inner();
            parse_json(&resp.json)
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

    /// Resolve `pond`'s owning node via the control plane, then dial its Data gRPC
    /// directly (the control plane is never in the data path).
    async fn data_for(&self, pond: &str) -> RtClient<DataClient<Channel>> {
        let mut c = self.control().await?;
        let loc = c
            .get_pond_location(GetPondLocationRequest {
                pond_ref: pond.to_string(),
            })
            .await
            .map_err(|s| anyhow!("pond '{pond}': {}", s.message()))?
            .into_inner();
        DataClient::connect(loc.node_endpoint.clone())
            .await
            .map_err(|e| anyhow!("pond node unreachable at {}: {e}", loc.node_endpoint))
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
        Ok(Self { control_endpoint })
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
