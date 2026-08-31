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

/// RFC 9728's well-known path. Duplicated from the MCP surface (which actually
/// serves the document) rather than depended on: the control plane has no reason
/// to pull in the agent crate for one string.
const PROTECTED_RESOURCE_PATH: &str = "/.well-known/oauth-protected-resource";

/// The protected-resource metadata URL advertised in the Admin surface's
/// `WWW-Authenticate` challenge.
///
/// ASSUMPTION: it is derived from the address the Admin surface itself binds, at
/// the standard well-known path. Nothing serves that document on the control
/// plane today — identity v0 publishes it on the pond node's MCP surface, and
/// the control plane has no HTTP surface to hang it off — so what the challenge
/// buys an operator's CLI right now is the RFC 6750 signal that a bearer token
/// is required, plus the resource it is required for. Deriving it here rather
/// than adding a flag keeps the URL correct by construction the day the control
/// plane grows that endpoint.
fn protected_resource_url(addr: &str) -> String {
    format!("http://{addr}{PROTECTED_RESOURCE_PATH}")
}

/// Build the Admin surface's token verifier, if an issuer is configured. A bad
/// config is a startup failure, never a silent downgrade to unauthenticated:
/// the operator asked for verification, so refusing to serve is the only safe
/// answer.
fn build_verifier(
    auth: Option<latiq_auth::AuthConfig>,
) -> Result<Option<std::sync::Arc<latiq_auth::Verifier>>, Box<dyn std::error::Error + Send + Sync>>
{
    match auth {
        None => Ok(None),
        Some(cfg) => Ok(Some(std::sync::Arc::new(latiq_auth::Verifier::new(cfg)?))),
    }
}

/// Serve BOTH the Control gRPC (pond nodes) and Admin gRPC (operators/CLI) on a
/// single `addr`. tonic routes by service name, so one port carries both.
///
/// `auth` configures the **Admin** surface as an OAuth 2.1 resource server;
/// `None` keeps the relaxed (claimed) identity path. Control gRPC is the
/// internal pond-node → control-plane channel and is not covered here.
pub async fn serve_control_plane(
    addr: SocketAddr,
    registry: Registry,
    auth: Option<latiq_auth::AuthConfig>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // Built ONCE, before anything binds, so a bad config fails startup loudly.
    let verifier = build_verifier(auth)?;
    let metadata_url = verifier
        .as_ref()
        .map(|_| protected_resource_url(&addr.to_string()));
    spawn_node_reaper(registry.clone());
    let control = latiq_proto::v1::control_server::ControlServer::new(
        control_service::ControlService::new(registry.clone()),
    );
    let admin = latiq_proto::v1::admin_server::AdminServer::new(
        admin_service::AdminService::new(registry)
            .with_verifier(verifier)
            .with_metadata_url(metadata_url.as_deref()),
    );
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
    auth: Option<latiq_auth::AuthConfig>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let verifier = build_verifier(auth)?;
    let metadata_url = verifier
        .as_ref()
        .map(|_| protected_resource_url(&addr.to_string()));
    let svc = latiq_proto::v1::admin_server::AdminServer::new(
        admin_service::AdminService::new(registry)
            .with_verifier(verifier)
            .with_metadata_url(metadata_url.as_deref()),
    );
    Server::builder().add_service(svc).serve(addr).await?;
    Ok(())
}
