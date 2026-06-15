//! latiq-control-plane — registry + Control/Admin gRPC surfaces.
pub mod admin_service;
pub mod control_service;
pub mod dataset_convert;
pub mod error;
pub mod migrations;
pub mod registry;
pub use error::ControlPlaneError;
pub use registry::Registry;

use std::net::SocketAddr;
use std::time::Duration;
use tonic::transport::Server;

/// Heartbeat liveness: nodes beat every 10s; the reaper downs a node after this
/// long without one (3 missed beats), sweeping every `REAP_INTERVAL`.
const NODE_TTL_SECS: u32 = 30;
const REAP_INTERVAL: Duration = Duration::from_secs(10);

/// Periodically refresh the control-plane gauges from the registry + this
/// process's CPU/memory. Counters (allocations, reaped) are incremented inline.
pub fn spawn_system_collector(registry: Registry) {
    tokio::spawn(async move {
        let mut sampler = latiq_metrics::ProcessSampler::new();
        loop {
            tokio::time::sleep(Duration::from_secs(5)).await;
            latiq_metrics::record_process_gauges(&mut sampler);
            if let Ok(nodes) = registry.list_nodes() {
                let active = nodes.iter().filter(|n| n.state == "active").count();
                metrics::gauge!("latiq_nodes", "state" => "active").set(active as f64);
                metrics::gauge!("latiq_nodes", "state" => "down")
                    .set((nodes.len() - active) as f64);
            }
            if let Ok(ponds) = registry.list_ponds() {
                metrics::gauge!("latiq_ponds_total").set(ponds.len() as f64);
                let mut by_tier: std::collections::HashMap<String, usize> = Default::default();
                for p in &ponds {
                    *by_tier.entry(p.tier.clone()).or_default() += 1;
                }
                for (tier, n) in by_tier {
                    metrics::gauge!("latiq_ponds", "tier" => tier).set(n as f64);
                }
            }
        }
    });
}

/// Periodically mark nodes whose heartbeat went stale as `down` (so placement
/// stops picking them). A node's next heartbeat/register revives it.
pub fn spawn_node_reaper(registry: Registry) {
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(REAP_INTERVAL).await;
            match registry.reap_stale_nodes(NODE_TTL_SECS) {
                Ok(n) if n > 0 => {
                    tracing::warn!(downed = n, ttl_secs = NODE_TTL_SECS, "reaped stale nodes")
                }
                Ok(_) => {}
                Err(e) => tracing::warn!(error = %e, "node reaper sweep failed"),
            }
        }
    });
}

/// Serve BOTH the Control gRPC (pond nodes) and Admin gRPC (operators/CLI) on a
/// single `addr`. tonic routes by service name, so one port carries both.
pub async fn serve_control_plane(
    addr: SocketAddr,
    registry: Registry,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    spawn_node_reaper(registry.clone());
    let control = latiq_proto::v1::control_server::ControlServer::new(
        control_service::ControlService::new(registry.clone()),
    );
    let admin =
        latiq_proto::v1::admin_server::AdminServer::new(admin_service::AdminService::new(registry));
    Server::builder()
        .add_service(control)
        .add_service(admin)
        .serve(addr)
        .await?;
    Ok(())
}

/// Serve the Control gRPC surface on `addr` until the server is shut down.
pub async fn serve_control(
    addr: SocketAddr,
    registry: Registry,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let svc = latiq_proto::v1::control_server::ControlServer::new(
        control_service::ControlService::new(registry),
    );
    Server::builder().add_service(svc).serve(addr).await?;
    Ok(())
}

/// Serve the Admin gRPC surface on `addr` until the server is shut down.
pub async fn serve_admin(
    addr: SocketAddr,
    registry: Registry,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let svc =
        latiq_proto::v1::admin_server::AdminServer::new(admin_service::AdminService::new(registry));
    Server::builder().add_service(svc).serve(addr).await?;
    Ok(())
}
