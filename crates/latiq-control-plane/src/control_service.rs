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

//! Control gRPC service (pond-nodes call this).
use crate::error::{to_status, to_status_traced, ControlPlaneError};
use crate::node_client::{CallerAuth, NodeMaterializer};
use crate::registry::Registry;
use latiq_proto::v1::control_server::Control;
use latiq_proto::v1::*;
use std::sync::Arc;
use tonic::{Request, Response, Status};

/// The internal node-facing surface: registration, heartbeats, and the routing
/// and registry lookups a node needs to serve a pond. Not an external surface —
/// no CLI, SDK or agent calls this.
///
/// It is also where **every** pond in the system is created — `AgentOps::
/// allocate_pond` (agents over MCP, clients over Data gRPC) and the direct
/// `CreatePondAssignment` calls from `latiq pond create` and the SDK all land on
/// `create_pond_assignment`. That is why materialisation lives here: one
/// implementation of "a create that returns success means a pond that can accept
/// data", rather than four that drift.
pub struct ControlService {
    pub registry: Registry,
    /// How this control plane reaches a pond node to materialise a pond it just
    /// placed. `None` = the pre-eager behaviour (write the row, let the owner's
    /// lazy `ensure_pond` catch up), which is a **test-only** configuration:
    /// every production entry point (`serve_control_plane`, `serve_control`)
    /// passes one.
    materializer: Option<Arc<dyn NodeMaterializer>>,
    /// Replay the caller's `authorization` header onto the node hop.
    ///
    /// Gated exactly like the node-to-node forwarder's: only a control plane
    /// that was configured with an issuer replays a bearer token. An
    /// unauthenticated control plane must not capture whatever `authorization`
    /// header a client happens to send (one meant for an upstream gateway, say)
    /// and present it on an internal channel.
    replay_bearer: bool,
}

impl ControlService {
    /// `materializer` is a required argument rather than a builder step on
    /// purpose: forgetting to wire it does not fail, it silently reverts every
    /// create path to lazy allocation, which is invisible until a pond is
    /// created on a node that is down. A caller has to write `None` and mean it.
    pub fn new(registry: Registry, materializer: Option<Arc<dyn NodeMaterializer>>) -> Self {
        Self {
            registry,
            materializer,
            replay_bearer: false,
        }
    }

    /// Whether this control plane replays the caller's bearer token on the node
    /// hop — true iff it was configured with an issuer.
    pub fn replaying_bearer(mut self, yes: bool) -> Self {
        self.replay_bearer = yes;
        self
    }

    /// The identity the node hop carries. Both halves are the caller's own; the
    /// control plane asserts nothing of its own here.
    fn caller_auth<T>(&self, req: &Request<T>, owner_identity: &str) -> CallerAuth {
        let md = req.metadata();
        let header = |k: &str| md.get(k).and_then(|v| v.to_str().ok()).map(str::to_string);
        CallerAuth {
            // The header when the caller sent one (agents, the Data surface, a
            // node forwarding on someone's behalf); otherwise the identity the
            // create names as the pond's owner, which is the same string the
            // CLI/SDK put in `--agent-id`.
            agent_id: header("latiq-agent-id")
                .or_else(|| Some(owner_identity.to_string()).filter(|s| !s.is_empty())),
            bearer: if self.replay_bearer {
                header("authorization")
            } else {
                None
            },
            // The node hop dropped the trace entirely, so a `pond create` — one
            // caller action that becomes a Control call here and a
            // MaterializePond over there — could not be followed across the two
            // processes. That is the single most useful trace in the system to
            // have, because it is the one place the control plane is allowed to
            // touch a node at all. Minted when the caller sent none, since an
            // id we propagate IS a correlation even if we invented it.
            traceparent: Some(crate::trace_meta::trace_of(req).traceparent()),
        }
    }

    /// Give the registry row back after the owner could not materialise the
    /// pond, and turn that into the error the caller sees.
    ///
    /// Compensation can itself fail — the row was deleted underneath us, or the
    /// registry is unwell — which leaves an ORPHAN: a pond name that resolves
    /// with no storage anywhere. That state is what `latiq pond forget` exists
    /// for, so it is said out loud in both directions: an `error!` an operator
    /// can grep for with the pond id in it, and a different `suggest` for the
    /// caller, who must not be told to retry a name that may still be taken.
    fn compensate(
        &self,
        pond_id: &str,
        name: &str,
        owner: &str,
        cause: String,
        trace_id: Option<String>,
    ) -> Status {
        let compensated = match self.registry.drop_pond(pond_id) {
            Ok(()) => true,
            Err(e) => {
                tracing::error!(
                    pond = %pond_id,
                    name = %name,
                    owner = %owner,
                    cause = %cause,
                    error = %e,
                    "ORPHANED REGISTRY ROW: this pond could not be materialised and its \
                     assignment could not be rolled back; remove it with `latiq pond forget`"
                );
                false
            }
        };
        // Stamped, unlike the pure registry reads around it: this is the ONE
        // control-plane failure that happened on another process, so the id is
        // the only way its caller reaches the node's side of the story.
        to_status_traced(
            ControlPlaneError::AllocationNotMaterialized {
                name: name.to_string(),
                owner: owner.to_string(),
                cause,
                compensated,
            },
            trace_id,
        )
    }
}

#[tonic::async_trait]
impl Control for ControlService {
    async fn register_node(
        &self,
        req: Request<RegisterNodeRequest>,
    ) -> Result<Response<RegisterNodeResponse>, Status> {
        let r = req.into_inner();
        self.registry
            .register_node(
                &r.node_id,
                &r.mcp_endpoint,
                &r.internal_endpoint,
                r.capacity,
            )
            .map_err(to_status)?;
        Ok(Response::new(RegisterNodeResponse {}))
    }

    async fn heartbeat(
        &self,
        req: Request<HeartbeatRequest>,
    ) -> Result<Response<HeartbeatResponse>, Status> {
        let r = req.into_inner();
        self.registry
            .heartbeat(&r.node_id, r.pond_count)
            .map_err(to_status)?;
        Ok(Response::new(HeartbeatResponse {}))
    }

    /// Create a pond: **place it, then make it real, then give the placement
    /// back if it could not be made real.**
    ///
    /// The one create path in the system, and the one place eager allocation is
    /// implemented. The control plane does not touch storage — it has no access
    /// to a node's filesystem — it calls the owning node's idempotent
    /// `Data.MaterializePond` and waits. That is the explicit, narrow exception
    /// to invariant 3 (root `CLAUDE.md`): the control plane drives pond
    /// LIFECYCLE, and is still never in the query path.
    ///
    /// It has to be here rather than in `AgentOps` because `AgentOps` is not on
    /// every create path: `latiq pond create` and the SDK's `create_pond` call
    /// this RPC directly, and while materialisation lived in the node they
    /// stayed lazy — the two paths humans use were the two that could still
    /// report "created" for a pond on an unreachable node.
    async fn create_pond_assignment(
        &self,
        req: Request<CreatePondAssignmentRequest>,
    ) -> Result<Response<CreatePondAssignmentResponse>, Status> {
        let caller = self.caller_auth(&req, &req.get_ref().owner_identity);
        // The id the node hop will carry, kept so a failure can name it too — a
        // caller told "the node could not materialise this" needs the id to find
        // out WHY on that node.
        let trace_id = caller
            .traceparent
            .as_deref()
            .and_then(latiq_common::TraceContext::parse)
            .map(|c| c.trace_id().to_string());
        let r = req.into_inner();
        let name = if r.name.is_empty() {
            None
        } else {
            Some(r.name)
        };
        let tier = if r.tier.is_empty() {
            "medium"
        } else {
            r.tier.as_str()
        };
        // Placement first: the registry picks the owner and reserves the name,
        // and only then is there something to materialise. Everything after this
        // point owns the row and must not return an error without giving it back.
        let pond = self
            .registry
            .create_pond(
                name,
                &r.owner_identity,
                &r.policy_json,
                tier,
                &r.extensions,
                &r.description,
                r.lineage,
            )
            .map_err(to_status)?;
        let endpoint = match self.registry.get_pond_location(&pond.pond_id) {
            Ok((_pid, endpoint)) => endpoint,
            // The owning node's row vanished between the placement and this
            // read. Nothing exists anywhere, so the row goes back rather than
            // becoming a pond name no node will ever serve.
            Err(e) => {
                return Err(self.compensate(
                    &pond.pond_id,
                    &pond.name,
                    &pond.node_id,
                    format!("the registry can no longer say where it was placed: {e}"),
                    trace_id,
                ))
            }
        };
        if let Some(m) = &self.materializer {
            if let Err(cause) = m.materialize(&endpoint, &pond.pond_id, &caller).await {
                return Err(self.compensate(&pond.pond_id, &pond.name, &endpoint, cause, trace_id));
            }
        }
        Ok(Response::new(CreatePondAssignmentResponse {
            pond_id: pond.pond_id,
            assigned_node_endpoint: endpoint,
        }))
    }

    async fn get_pond_location(
        &self,
        req: Request<GetPondLocationRequest>,
    ) -> Result<Response<GetPondLocationResponse>, Status> {
        let (pond_id, node_endpoint) = self
            .registry
            .get_pond_location(&req.into_inner().pond_ref)
            .map_err(to_status)?;
        Ok(Response::new(GetPondLocationResponse {
            pond_id,
            node_endpoint,
        }))
    }

    async fn drop_pond_assignment(
        &self,
        req: Request<DropPondAssignmentRequest>,
    ) -> Result<Response<DropPondAssignmentResponse>, Status> {
        self.registry
            .drop_pond(&req.into_inner().pond_id)
            .map_err(to_status)?;
        Ok(Response::new(DropPondAssignmentResponse {}))
    }

    async fn list_ponds(
        &self,
        _req: Request<ListPondsRequest>,
    ) -> Result<Response<ListPondsResponse>, Status> {
        let rows = self.registry.list_ponds().map_err(to_status)?;
        let mut ponds = Vec::with_capacity(rows.len());
        for row in rows {
            // N+1 list-then-detail read: skip a pond dropped between the list and
            // its pond_info lookup instead of failing the whole call (review #9).
            let (row, created_at, policy_json, endpoint) =
                match self.registry.pond_info(&row.pond_id) {
                    Ok(info) => info,
                    Err(ControlPlaneError::PondNotFound(_)) => continue,
                    Err(e) => return Err(to_status(e)),
                };
            ponds.push(PondInfoMsg {
                pond_id: row.pond_id,
                name: row.name,
                owner: row.owner_identity,
                created_at,
                policy_json,
                // The owner's identity (what a node routes on) alongside its
                // address (what a node dials) — never one standing in for the
                // other (#89).
                node_id: row.node_id,
                node_endpoint: endpoint.unwrap_or_default(),
                tier: row.tier,
                extensions: row.extensions,
                description: row.description,
                lineage: row.lineage,
            });
        }
        Ok(Response::new(ListPondsResponse { ponds }))
    }

    async fn get_pond_info(
        &self,
        req: Request<GetPondInfoRequest>,
    ) -> Result<Response<GetPondInfoResponse>, Status> {
        let (row, created_at, policy_json, endpoint) = self
            .registry
            .pond_info(&req.into_inner().pond_ref)
            .map_err(to_status)?;
        Ok(Response::new(GetPondInfoResponse {
            pond: Some(PondInfoMsg {
                pond_id: row.pond_id,
                name: row.name,
                owner: row.owner_identity,
                created_at,
                policy_json,
                // The owner's identity (what a node routes on) alongside its
                // address (what a node dials) — never one standing in for the
                // other (#89).
                node_id: row.node_id,
                node_endpoint: endpoint.unwrap_or_default(),
                tier: row.tier,
                extensions: row.extensions,
                description: row.description,
                lineage: row.lineage,
            }),
        }))
    }

    async fn get_dataset(
        &self,
        req: Request<GetDatasetRequest>,
    ) -> Result<Response<GetDatasetResponse>, Status> {
        let d = self
            .registry
            .get_dataset(&req.into_inner().name)
            .map_err(to_status)?;
        Ok(Response::new(GetDatasetResponse {
            dataset: Some(crate::dataset_convert::dataset_to_msg(d)),
        }))
    }

    async fn list_datasets(
        &self,
        req: Request<ListDatasetsRequest>,
    ) -> Result<Response<ListDatasetsResponse>, Status> {
        let datasets = self
            .registry
            .list_datasets(&req.into_inner().query)
            .map_err(to_status)?
            .into_iter()
            .map(crate::dataset_convert::dataset_to_msg)
            .collect();
        Ok(Response::new(ListDatasetsResponse { datasets }))
    }

    async fn get_catalog(
        &self,
        req: Request<GetCatalogRequest>,
    ) -> Result<Response<GetCatalogResponse>, Status> {
        let c = self
            .registry
            .get_catalog(&req.into_inner().name)
            .map_err(to_status)?;
        Ok(Response::new(GetCatalogResponse {
            catalog: Some(crate::dataset_convert::catalog_to_msg(c)),
        }))
    }

    async fn list_catalogs(
        &self,
        req: Request<ListCatalogsRequest>,
    ) -> Result<Response<ListCatalogsResponse>, Status> {
        let catalogs = self
            .registry
            .list_catalogs(&req.into_inner().query)
            .map_err(to_status)?
            .into_iter()
            .map(crate::dataset_convert::catalog_to_msg)
            .collect();
        Ok(Response::new(ListCatalogsResponse { catalogs }))
    }
}
