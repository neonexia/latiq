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

//! The control plane's **one** outbound client: `Data.MaterializePond` on a pond
//! node.
//!
//! Every other gRPC edge in the system points *at* the control plane (nodes call
//! Control, operators call Admin). This one points away from it, and it exists
//! for exactly one reason: **the control plane places the pond, so the control
//! plane is what makes it real** (root `CLAUDE.md` invariant 3). It never touches
//! a node's filesystem — it has none — it asks the owner to, over the same
//! idempotent node-to-node RPC eager allocation already used.
//!
//! It is deliberately narrow. Nothing here reads or writes pond DATA, and
//! nothing here is reachable from a query: `materialize` is called from
//! `CreatePondAssignment` and from nowhere else. Widening this file is how the
//! control plane ends up in the query path.

use latiq_proto::v1::data_client::DataClient;
use latiq_proto::v1::MaterializePondRequest;
use std::collections::HashMap;
use std::time::Duration;
use tokio::sync::Mutex;
use tonic::metadata::MetadataValue;
use tonic::transport::{Channel, Endpoint};
use tonic::{Code, Request};

/// How long to wait for a TCP+HTTP/2 connection to a pond node.
///
/// Short on purpose: an unreachable owner is the failure this whole path exists
/// to surface *now* rather than at the caller's first write, and a create that
/// hangs for a minute is barely better than one that lies. On a refused port
/// (the usual "node is gone") the error arrives immediately and this is never
/// reached; it bounds the black-hole case — a host that swallows SYNs.
pub const CONNECT_TIMEOUT: Duration = Duration::from_secs(5);

/// How long to wait for `MaterializePond` itself once connected.
///
/// Generous relative to the connect budget because the work is real: creating
/// the pond directory and opening a DuckDB/DuckLake instance for it (~80ms on
/// loopback, but a cold node under load can be far slower). This is a
/// backstop against a wedged node, not a latency target.
pub const MATERIALIZE_TIMEOUT: Duration = Duration::from_secs(30);

/// What the control plane replays onto the node hop.
///
/// Both fields are the CALLER's, never the control plane's own: there is no
/// control-plane service identity and there must not be one, or the node would
/// be materialising ponds on the authority of whoever can reach the control
/// plane rather than on the authority of the token the caller presented.
#[derive(Default, Clone, Debug)]
pub struct CallerAuth {
    /// The claimed leaf (`latiq-agent-id`) — attribution only, never authority.
    pub agent_id: Option<String>,
    /// The caller's verbatim `authorization` header, so the node re-verifies it
    /// on its own authority. `None` when the control plane has no auth
    /// configured — see `ControlService::replaying_bearer`.
    pub bearer: Option<String>,
}

/// Ensure a pond's storage exists on the node that owns it.
///
/// A trait so the orchestration in `control_service` can be tested against a
/// node that fails, a node that is slow, or a node that concurrently deletes the
/// registry row — none of which a real pond node can be talked into on demand.
#[tonic::async_trait]
pub trait NodeMaterializer: Send + Sync {
    /// `Err` carries a human-readable cause that is embedded verbatim in the
    /// error the caller sees, so it must name the failure, not just its kind.
    async fn materialize(
        &self,
        endpoint: &str,
        pond_id: &str,
        caller: &CallerAuth,
    ) -> Result<(), String>;
}

/// The deployed materializer: `Data.MaterializePond` over gRPC, one cached
/// channel per node endpoint.
#[derive(Default)]
pub struct GrpcNodeMaterializer {
    /// One channel per pond-node endpoint. tonic channels multiplex concurrent
    /// RPCs, so caching + cloning is the cheap, correct reuse pattern — the same
    /// one `latiq-pond-node`'s `GrpcForwarder` uses for the node-to-node hop.
    ///
    /// The lock is NOT held across `connect()`: a single unreachable node would
    /// otherwise stall every concurrent create in the cluster for the whole
    /// connect timeout. Two racing connects to the same endpoint is a wasted
    /// handshake and nothing worse.
    clients: Mutex<HashMap<String, DataClient<Channel>>>,
}

impl GrpcNodeMaterializer {
    pub fn new() -> Self {
        Self::default()
    }

    async fn client(&self, endpoint: &str) -> Result<DataClient<Channel>, String> {
        if let Some(c) = self.clients.lock().await.get(endpoint) {
            return Ok(c.clone());
        }
        let channel = Endpoint::from_shared(endpoint.to_string())
            .map_err(|e| format!("the registry holds an unusable endpoint '{endpoint}': {e}"))?
            .connect_timeout(CONNECT_TIMEOUT)
            .timeout(MATERIALIZE_TIMEOUT)
            .connect()
            .await
            .map_err(|e| format!("could not reach the node at {endpoint}: {e}"))?;
        let c = DataClient::new(channel);
        self.clients
            .lock()
            .await
            .insert(endpoint.to_string(), c.clone());
        Ok(c)
    }
}

#[tonic::async_trait]
impl NodeMaterializer for GrpcNodeMaterializer {
    async fn materialize(
        &self,
        endpoint: &str,
        pond_id: &str,
        caller: &CallerAuth,
    ) -> Result<(), String> {
        let mut client = self.client(endpoint).await?;
        let mut req = Request::new(MaterializePondRequest {
            pond: pond_id.to_string(),
        });
        if let Some(agent) = caller.agent_id.as_deref() {
            if let Ok(v) = MetadataValue::try_from(agent) {
                req.metadata_mut().insert("latiq-agent-id", v);
            }
        }
        if let Some(bearer) = caller.bearer.as_deref() {
            if let Ok(v) = MetadataValue::try_from(bearer) {
                req.metadata_mut().insert("authorization", v);
            }
        }
        match client.materialize_pond(req).await {
            Ok(_) => Ok(()),
            Err(status) => {
                // A node that died after we cached its channel: drop the entry so
                // the next create reconnects instead of replaying a dead one.
                // (tonic reconnects on its own; this only stops a permanently
                // moved node from being retried through a stale entry for ever.)
                if status.code() == Code::Unavailable {
                    self.clients.lock().await.remove(endpoint);
                }
                Err(status.message().to_string())
            }
        }
    }
}
