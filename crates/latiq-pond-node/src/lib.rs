//! latiq-pond-node — wires the agent (MCP) + data (gRPC) inbound surfaces to one
//! `AgentOps` (engine + storage + gRPC ControlPlane client); registers with the
//! control plane and heartbeats.
pub mod data_service;
pub mod forward_client;
pub mod grpc_control;
pub mod stream_service;
pub mod wire;

pub use data_service::DataService;
pub use forward_client::GrpcForwarder;
pub use grpc_control::GrpcControlPlane;
pub use stream_service::StreamService;

use latiq_agent_core::{AgentConfig, AgentOps};
use latiq_auth::Verifier;
use latiq_engine_duckdb::DuckEngine;
use latiq_mcp::serve_mcp;
use latiq_proto::v1::control_client::ControlClient;
use latiq_proto::v1::data_server::DataServer;
use latiq_proto::v1::stream_server::StreamServer;
use latiq_proto::v1::{HeartbeatRequest, RegisterNodeRequest};
use latiq_storage::LocalFs;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use tonic::transport::Server;

pub struct PondNodeConfig {
    pub node_id: String,
    /// Address to serve the agent MCP surface on.
    pub mcp_addr: SocketAddr,
    /// Address to serve the Data/Query gRPC surface on (CLI/SDK).
    pub data_addr: SocketAddr,
    /// Internal endpoint advertised to the control plane (single-node routing).
    pub internal_endpoint: String,
    /// Control-plane Control gRPC endpoint, e.g. `http://127.0.0.1:9090`.
    pub control_endpoint: String,
    /// Local-FS root for pond storage.
    pub data_dir: PathBuf,
    /// Address to serve Prometheus `/metrics` on (None = metrics off).
    pub metrics_addr: Option<SocketAddr>,
    /// When set, every surface on this node requires a verified bearer token.
    /// `None` keeps the relaxed identity of the embedded and dev paths.
    pub auth: Option<latiq_auth::AuthConfig>,
}

/// Install the standard + optional DuckDB extensions into the local cache so a
/// node can run offline. Used to **bake** extensions into the container image at
/// build time (`latiq warm-extensions`): INSTALL hits the network here, once, so
/// the runtime node never needs it. Errors if a required extension can't install.
pub fn warm_extensions() -> Result<(), Box<dyn std::error::Error>> {
    latiq_engine_duckdb::ensure_standard_extensions()?;
    latiq_engine_duckdb::warm_optional_extensions();
    Ok(())
}

/// Periodically refresh this node's resource + load gauges. The per-pond
/// in-flight gauge is maintained inline in AgentOps; this samples the node-level
/// gauges + process CPU/memory.
pub fn spawn_node_collector(ops: Arc<AgentOps>) {
    tokio::spawn(async move {
        let mut sampler = latiq_metrics::ProcessSampler::new();
        loop {
            tokio::time::sleep(Duration::from_secs(5)).await;
            latiq_metrics::record_process_gauges(&mut sampler);
            metrics::gauge!("latiq_inflight_queries").set(ops.inflight().len() as f64);
            metrics::gauge!("latiq_node_open_ponds").set(ops.open_pond_count() as f64);
        }
    });
}

/// Build the pond node's `AgentOps` (gRPC control + local storage + DuckDB
/// engine) and register the node with the control plane.
pub async fn build_ops(
    node_id: &str,
    mcp_endpoint: &str,
    internal_endpoint: &str,
    control_endpoint: &str,
    data_dir: &std::path::Path,
) -> anyhow::Result<Arc<AgentOps>> {
    let mut reg = ControlClient::connect(control_endpoint.to_string()).await?;
    reg.register_node(RegisterNodeRequest {
        node_id: node_id.to_string(),
        mcp_endpoint: mcp_endpoint.to_string(),
        internal_endpoint: internal_endpoint.to_string(),
        capacity: 100,
    })
    .await?;

    let control = Arc::new(GrpcControlPlane::connect(control_endpoint.to_string()).await?);
    let storage = Arc::new(LocalFs::new(data_dir));
    let engine = Arc::new(DuckEngine::new());
    // Forward requests for ponds this node doesn't own to the owning node. The
    // node's own `internal_endpoint` is the identity it compares against, so a
    // pond whose registry endpoint matches runs locally; everything else forwards.
    let ops = AgentOps::new(control, storage, engine, AgentConfig::default()).with_forwarding(
        internal_endpoint.to_string(),
        Arc::new(GrpcForwarder::new()),
    );
    Ok(Arc::new(ops))
}

/// Serve the Data/Query gRPC surface on `addr`. `verifier` is built once at
/// startup and shared — never per request.
pub async fn serve_data(
    addr: SocketAddr,
    ops: Arc<AgentOps>,
    verifier: Option<Arc<Verifier>>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // Data (unary JSON, CLI/SDK) + Stream (server-streaming Arrow, SDK + the
    // node-to-node read forward) share one port — both plain tonic. Both get the
    // same verifier: a surface left unauthenticated is the whole node's auth.
    Server::builder()
        .add_service(DataServer::new(
            DataService::new(ops.clone()).with_verifier(verifier.clone()),
        ))
        .add_service(StreamServer::new(
            StreamService::new(ops).with_verifier(verifier),
        ))
        .serve(addr)
        .await?;
    Ok(())
}

/// Run a pond node: register with the control plane, start a heartbeat loop, and
/// serve the agent MCP surface + the Data/Query gRPC surface (blocks).
pub async fn run_pond_node(cfg: PondNodeConfig) -> anyhow::Result<()> {
    // Latiq is built on the ducklake + httpfs extensions — ensure they load BEFORE
    // we register or serve, so a node that can't function never joins the cluster.
    // Fail fast with a clear error rather than degrading per pond.
    tokio::task::spawn_blocking(latiq_engine_duckdb::ensure_standard_extensions)
        .await
        .map_err(|e| anyhow::anyhow!("extension check failed to run: {e}"))?
        .map_err(|e| {
            anyhow::anyhow!(
                "Latiq requires the ducklake and httpfs DuckDB extensions but could \
                 not load them (bake them into the deployment image): {e}"
            )
        })?;

    // Built ONCE, before anything serves: a bad auth config must stop the node
    // loudly rather than let it come up accepting unauthenticated callers.
    let verifier = match cfg.auth.clone() {
        Some(auth) => Some(Arc::new(Verifier::new(auth).map_err(|e| {
            anyhow::anyhow!("auth is configured but the verifier could not be built: {e}")
        })?)),
        None => None,
    };

    let mcp_endpoint = format!("http://{}/mcp", cfg.mcp_addr);
    let ops = build_ops(
        &cfg.node_id,
        &mcp_endpoint,
        &cfg.internal_endpoint,
        &cfg.control_endpoint,
        &cfg.data_dir,
    )
    .await?;

    // Warm the OPTIONAL extension cache once, in the background — the dev stand-in
    // for image-baking (a no-op when the image is pre-baked). Best-effort: keeps
    // per-pond LOADs download-free without blocking serving or the create path.
    tokio::task::spawn_blocking(|| {
        tracing::info!("warming optional DuckDB extension cache");
        latiq_engine_duckdb::warm_optional_extensions();
    });

    // Prometheus /metrics + the gauge collector (if a metrics port is configured).
    if let Some(metrics_addr) = cfg.metrics_addr {
        let handle = latiq_metrics::init_recorder();
        metrics::gauge!("latiq_build_info", "version" => env!("CARGO_PKG_VERSION")).set(1.0);
        spawn_node_collector(ops.clone());
        tokio::spawn(async move {
            if let Err(e) = latiq_metrics::serve_metrics(metrics_addr, handle).await {
                eprintln!("metrics server error: {e}");
            }
        });
    }

    // Heartbeat loop (reconnects on failure so a control-plane blip doesn't
    // silently drop the node from the cluster).
    let hb_endpoint = cfg.control_endpoint.clone();
    let hb_node = cfg.node_id.clone();
    tokio::spawn(async move {
        let mut client: Option<ControlClient<tonic::transport::Channel>> = None;
        loop {
            tokio::time::sleep(Duration::from_secs(10)).await;
            if client.is_none() {
                client = ControlClient::connect(hb_endpoint.clone()).await.ok();
            }
            if let Some(c) = client.as_mut() {
                if c.heartbeat(HeartbeatRequest {
                    node_id: hb_node.clone(),
                    pond_count: 0,
                })
                .await
                .is_err()
                {
                    client = None; // drop + reconnect next tick
                }
            }
        }
    });

    // Serve both surfaces concurrently.
    let data_ops = ops.clone();
    let data_addr = cfg.data_addr;
    let data_verifier = verifier.clone();
    tokio::spawn(async move {
        if let Err(e) = serve_data(data_addr, data_ops, data_verifier).await {
            eprintln!("data gRPC server error: {e}");
        }
    });

    println!(
        "pond-node '{}' serving MCP at {mcp_endpoint} and Data gRPC at http://{}",
        cfg.node_id, cfg.data_addr
    );
    println!("  pond storage: {}", cfg.data_dir.display());
    serve_mcp(cfg.mcp_addr, ops)
        .await
        .map_err(|e| anyhow::anyhow!("mcp server error: {e}"))?;
    Ok(())
}
