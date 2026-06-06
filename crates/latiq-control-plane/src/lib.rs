//! latiq-control-plane — registry + Control/Admin gRPC surfaces.
pub mod admin_service;
pub mod control_service;
pub mod error;
pub mod migrations;
pub mod registry;
pub use error::ControlPlaneError;
pub use registry::Registry;

use std::net::SocketAddr;
use tonic::transport::Server;

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
