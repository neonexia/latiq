//! latiq-pond-node — wires the MCP surface to agent-core + engine + storage and
//! a gRPC ControlPlane client; registers with the control plane and heartbeats.
pub mod grpc_control;

pub use grpc_control::GrpcControlPlane;

use latiq_agent_core::{AgentConfig, AgentOps};
use latiq_engine_duckdb::DuckEngine;
use latiq_mcp::serve_mcp;
use latiq_proto::v1::control_client::ControlClient;
use latiq_proto::v1::{HeartbeatRequest, RegisterNodeRequest};
use latiq_storage::LocalFs;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

pub struct PondNodeConfig {
    pub node_id: String,
    /// Address to serve the agent MCP surface on.
    pub mcp_addr: SocketAddr,
    /// Internal endpoint advertised to the control plane (single-node routing).
    pub internal_endpoint: String,
    /// Control-plane Control gRPC endpoint, e.g. `http://127.0.0.1:9090`.
    pub control_endpoint: String,
    /// Local-FS root for pond storage.
    pub data_dir: PathBuf,
}

/// Run a pond node: register with the control plane, start a heartbeat loop, and
/// serve the MCP surface (blocks until the server stops).
pub async fn run_pond_node(cfg: PondNodeConfig) -> anyhow::Result<()> {
    // Register this node with the control plane.
    let mut reg = ControlClient::connect(cfg.control_endpoint.clone()).await?;
    let mcp_endpoint = format!("http://{}/mcp", cfg.mcp_addr);
    reg.register_node(RegisterNodeRequest {
        node_id: cfg.node_id.clone(),
        mcp_endpoint: mcp_endpoint.clone(),
        internal_endpoint: cfg.internal_endpoint.clone(),
        capacity: 100,
    })
    .await?;

    // Build the agent operations: gRPC control + local storage + DuckDB engine.
    let control = Arc::new(GrpcControlPlane::connect(cfg.control_endpoint.clone()).await?);
    let storage = Arc::new(LocalFs::new(&cfg.data_dir));
    let engine = Arc::new(DuckEngine::new());
    let ops = Arc::new(AgentOps::new(
        control,
        storage,
        engine,
        AgentConfig::default(),
    ));

    // Heartbeat loop.
    let hb_endpoint = cfg.control_endpoint.clone();
    let hb_node = cfg.node_id.clone();
    tokio::spawn(async move {
        let mut c = match ControlClient::connect(hb_endpoint).await {
            Ok(c) => c,
            Err(_) => return,
        };
        loop {
            tokio::time::sleep(Duration::from_secs(10)).await;
            let _ = c
                .heartbeat(HeartbeatRequest {
                    node_id: hb_node.clone(),
                    pond_count: 0,
                })
                .await;
        }
    });

    println!("pond-node '{}' serving MCP at {mcp_endpoint}", cfg.node_id);
    serve_mcp(cfg.mcp_addr, ops)
        .await
        .map_err(|e| anyhow::anyhow!("mcp server error: {e}"))?;
    Ok(())
}
