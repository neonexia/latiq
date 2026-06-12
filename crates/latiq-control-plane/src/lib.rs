//! latiq-control-plane — registry + Control/Admin gRPC surfaces.
pub mod admin_service;
pub mod control_service;
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
