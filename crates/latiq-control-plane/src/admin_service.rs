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

//! Admin gRPC service (the latiq CLI calls this).
//!
//! With an issuer configured this surface is an OAuth 2.1 resource server: an
//! `authorization: Bearer <jwt>` is required and verified, and `latiq-agent-id`
//! is only the claimed leaf. Without one identity stays relaxed (claimed,
//! default anonymous) — the embedded and dev path. Verification only: nothing
//! here gates on WHO the caller is (no authorization in identity v0).
//!
//! Every handler records its action on the `latiq::access` trace target, with
//! the same fields as `AgentOps::audit`, so operator actions (create a dataset,
//! set policy, change a tier) and agent actions land in one searchable stream.
use crate::error::{to_status, ControlPlaneError};
use crate::registry::Registry;
use latiq_common::Identity;
use latiq_proto::v1::admin_server::Admin;
use latiq_proto::v1::*;
use std::time::Instant;
use tonic::{Request, Response, Status};

pub struct AdminService {
    pub registry: Registry,
    verifier: Option<std::sync::Arc<latiq_auth::Verifier>>,
    /// The `WWW-Authenticate` value handed back on a rejection, built once.
    challenge: Option<String>,
}

impl AdminService {
    pub fn new(registry: Registry) -> Self {
        Self {
            registry,
            verifier: None,
            challenge: None,
        }
    }

    /// Require verified bearer tokens on this surface. `None` keeps the relaxed
    /// (embedded / dev) path.
    pub fn with_verifier(mut self, verifier: Option<std::sync::Arc<latiq_auth::Verifier>>) -> Self {
        self.verifier = verifier;
        self
    }

    /// The RFC 9728 protected-resource metadata URL to advertise on a rejection.
    /// See `serve_control_plane` for what the control plane passes here.
    pub fn with_metadata_url(mut self, metadata_url: Option<&str>) -> Self {
        self.challenge = metadata_url.map(latiq_auth::metadata::challenge_header);
        self
    }

    /// `Unauthenticated`, carrying the RFC 9728 challenge when we have one.
    ///
    /// gRPC has no 401, but a tonic `Status` carries trailing metadata — so the
    /// same `www-authenticate` value the MCP surface returns on its 401 rides
    /// along here. Without it an operator whose CLI is turned away knows only
    /// THAT a token is required, never which authorization server issues one.
    fn unauthenticated(&self, message: &'static str) -> Status {
        let mut status = Status::unauthenticated(message);
        if let Some(value) = self.challenge.as_deref().and_then(|c| c.parse().ok()) {
            status.metadata_mut().insert("www-authenticate", value);
        }
        status
    }

    /// Identity from gRPC metadata. With a verifier configured, an
    /// `authorization: Bearer <jwt>` header is REQUIRED and verified;
    /// `latiq-agent-id` then supplies only the claimed leaf. Without one,
    /// identity stays relaxed (claimed, default anonymous).
    ///
    /// A REJECTION is itself an audited event: on a surface whose whole job is
    /// recording who did what, "someone tried and was turned away" is exactly
    /// the line an operator wants, and a `debug!` is off in every default
    /// configuration. So both failure paths emit an access record before
    /// returning, naming the RPC that was targeted.
    async fn identity_of<T>(
        &self,
        req: &Request<T>,
        op: &'static str,
        started: Instant,
    ) -> Result<Identity, Status> {
        let claimed = req
            .metadata()
            .get("latiq-agent-id")
            .and_then(|v| v.to_str().ok());
        let Some(verifier) = self.verifier.as_ref() else {
            return Ok(Identity::claimed(claimed));
        };
        let token = req
            .metadata()
            .get("authorization")
            .and_then(|v| v.to_str().ok())
            .and_then(latiq_auth::bearer);
        let Some(token) = token else {
            // The identity fields are empty by construction — that is the
            // record: an attempt with nothing behind it.
            audit(
                &Identity::claimed(claimed),
                op,
                None,
                "rejected: no token",
                ERROR,
                started,
            );
            return Err(self.unauthenticated("a bearer token is required"));
        };
        verifier.verify(token, claimed).await.map_err(|e| {
            // Logged in full here, summarised on the wire: the detail is for the
            // operator, and an unauthenticated caller must not be able to probe
            // our issuer list or key endpoints by reading error text.
            tracing::debug!(error = %e, "bearer token rejected");
            audit(
                &Identity::claimed(claimed),
                op,
                None,
                "rejected: invalid token",
                ERROR,
                started,
            );
            self.unauthenticated("the bearer token was rejected")
        })
    }

    /// The `created_by` to persist on a registry row. A verified subject is
    /// authority; the request's own `created_by` is a client claim, so it only
    /// stands when nothing was verified. Same rule as DuckLake commit
    /// attribution — an audit trail that knows better than the durable row it
    /// describes is a trail with a hole in it.
    fn created_by(identity: &Identity, claimed: String) -> String {
        if identity.verified && !identity.subject.is_empty() {
            return identity.subject.clone();
        }
        match claimed.trim() {
            "" => "anonymous".to_string(),
            _ => claimed,
        }
    }
}

/// The `outcome` field's two values. An audit record that does not say whether
/// the action LANDED is worse than none: a rejected `dataset_remove` would read
/// byte-identically to a real one.
const OK: &str = "ok";
const ERROR: &str = "error";

/// `ok`/`error` for a completed handler body.
fn outcome<T, E>(res: &Result<T, E>) -> &'static str {
    if res.is_ok() {
        OK
    } else {
        ERROR
    }
}

/// One access record for an operator action, on the SAME `latiq::access` target
/// and with the SAME field names as `AgentOps::audit` — so `op=`/`subject=`
/// greps find operator and agent actions alike — plus `outcome`. The control
/// plane holds no `AgentOps`, so this is a local twin rather than a shared call;
/// keep the shared fields identical. `pond` is `-` where the action is not about
/// one pond.
///
/// As there, `subject=`/`issuer=` are only meaningful together with
/// `verified=true`; `agent=` is the caller's own claim and carries no authority.
fn audit(
    identity: &Identity,
    op: &'static str,
    pond: Option<&str>,
    summary: &str,
    outcome: &str,
    started: Instant,
) {
    tracing::info!(
        target: "latiq::access",
        agent = %identity.agent_id,          // CLAIMED. never authority.
        subject = %identity.subject,         // verified, or "" when not
        issuer = %identity.issuer,
        verified = identity.verified,        // scopes subject/issuer, NOT agent
        op,
        pond = pond.unwrap_or("-"),
        duration_ms = started.elapsed().as_millis() as u64,
        summary,
        outcome,                             // ok | error — did it LAND?
        "access",
    );
}

#[tonic::async_trait]
impl Admin for AdminService {
    async fn list_nodes(
        &self,
        req: Request<ListNodesRequest>,
    ) -> Result<Response<ListNodesResponse>, Status> {
        let started = Instant::now();
        let identity = self.identity_of(&req, "list_nodes", started).await?;
        let res = self.registry.list_nodes().map_err(to_status);
        audit(&identity, "list_nodes", None, "", outcome(&res), started);
        let nodes = res?.into_iter().map(node_info).collect();
        Ok(Response::new(ListNodesResponse { nodes }))
    }

    async fn describe_node(
        &self,
        req: Request<DescribeNodeRequest>,
    ) -> Result<Response<DescribeNodeResponse>, Status> {
        let started = Instant::now();
        let identity = self.identity_of(&req, "describe_node", started).await?;
        let node_id = req.into_inner().node_id;
        let res = self.registry.describe_node(&node_id).map_err(to_status);
        audit(
            &identity,
            "describe_node",
            None,
            &format!("node={node_id}"),
            outcome(&res),
            started,
        );
        Ok(Response::new(DescribeNodeResponse {
            node: Some(node_info(res?)),
        }))
    }

    async fn policy_get(
        &self,
        req: Request<PolicyGetRequest>,
    ) -> Result<Response<PolicyGetResponse>, Status> {
        let started = Instant::now();
        let identity = self.identity_of(&req, "policy_get", started).await?;
        let res = self.registry.policy_get().map_err(to_status);
        audit(&identity, "policy_get", None, "", outcome(&res), started);
        Ok(Response::new(PolicyGetResponse {
            policy_json: res?.to_string(),
        }))
    }

    async fn policy_set(
        &self,
        req: Request<PolicySetRequest>,
    ) -> Result<Response<PolicySetResponse>, Status> {
        let started = Instant::now();
        let identity = self.identity_of(&req, "policy_set", started).await?;
        let r = req.into_inner();
        let res = self
            .registry
            .policy_set(&r.key, &r.value)
            .map_err(to_status);
        // The value is operator-supplied policy, not a secret, and knowing what
        // a setting was changed TO is the point of recording the change.
        audit(
            &identity,
            "policy_set",
            None,
            &format!("key={} value={}", r.key, r.value),
            outcome(&res),
            started,
        );
        res?;
        Ok(Response::new(PolicySetResponse {}))
    }

    async fn pond_list(
        &self,
        req: Request<PondListRequest>,
    ) -> Result<Response<PondListResponse>, Status> {
        let started = Instant::now();
        let identity = self.identity_of(&req, "pond_list", started).await?;
        let listed = self.registry.list_ponds().map_err(to_status);
        audit(&identity, "pond_list", None, "", outcome(&listed), started);
        let rows = listed?;
        let mut ponds = Vec::with_capacity(rows.len());
        for row in rows {
            // N+1 list-then-detail read: skip a pond dropped between the list and
            // its pond_info lookup instead of failing the whole call (review #9).
            let (row, created_at, _policy, _endpoint) = match self.registry.pond_info(&row.pond_id)
            {
                Ok(info) => info,
                Err(ControlPlaneError::PondNotFound(_)) => continue,
                Err(e) => return Err(to_status(e)),
            };
            ponds.push(PondSummary {
                pond_id: row.pond_id,
                name: row.name,
                owner: row.owner_identity,
                created_at,
                node_id: row.node_id,
                tier: row.tier,
                description: row.description,
                lineage: row.lineage,
            });
        }
        Ok(Response::new(PondListResponse { ponds }))
    }

    async fn pond_set_tier(
        &self,
        req: Request<PondSetTierRequest>,
    ) -> Result<Response<PondSetTierResponse>, Status> {
        let started = Instant::now();
        let identity = self.identity_of(&req, "pond_set_tier", started).await?;
        let r = req.into_inner();
        if r.pond.trim().is_empty() {
            audit(
                &identity,
                "pond_set_tier",
                None,
                "rejected: pond is required",
                ERROR,
                started,
            );
            return Err(Status::invalid_argument("pond is required"));
        }
        let res = self
            .registry
            .set_pond_tier(&r.pond, &r.tier)
            .map_err(to_status);
        audit(
            &identity,
            "pond_set_tier",
            Some(&r.pond),
            &format!("tier={}", r.tier),
            outcome(&res),
            started,
        );
        res?;
        Ok(Response::new(PondSetTierResponse {
            pond: r.pond,
            tier: r.tier,
        }))
    }

    async fn dataset_add(
        &self,
        req: Request<DatasetAddRequest>,
    ) -> Result<Response<DatasetAddResponse>, Status> {
        let started = Instant::now();
        let identity = self.identity_of(&req, "dataset_add", started).await?;
        let Some(d) = req.into_inner().dataset else {
            audit(
                &identity,
                "dataset_add",
                None,
                "rejected: dataset is required",
                ERROR,
                started,
            );
            return Err(Status::invalid_argument("dataset is required"));
        };
        let row = crate::registry::DatasetRow {
            name: d.name,
            description: d.description,
            tags: d.tags,
            tables: d
                .tables
                .into_iter()
                .map(crate::dataset_convert::dataset_table_from_msg)
                .collect(),
            created_by: Self::created_by(&identity, d.created_by),
            created_at: String::new(),
        };
        let dataset_name = row.name.clone();
        let res = self.registry.add_dataset(&row).map_err(to_status);
        audit(
            &identity,
            "dataset_add",
            None,
            &format!("dataset={dataset_name}"),
            outcome(&res),
            started,
        );
        Ok(Response::new(DatasetAddResponse { name: res? }))
    }

    async fn dataset_remove(
        &self,
        req: Request<DatasetRemoveRequest>,
    ) -> Result<Response<DatasetRemoveResponse>, Status> {
        let started = Instant::now();
        let identity = self.identity_of(&req, "dataset_remove", started).await?;
        let name = req.into_inner().name;
        let res = self.registry.remove_dataset(&name).map_err(to_status);
        audit(
            &identity,
            "dataset_remove",
            None,
            &format!("dataset={name}"),
            outcome(&res),
            started,
        );
        res?;
        Ok(Response::new(DatasetRemoveResponse {}))
    }

    async fn dataset_list(
        &self,
        req: Request<DatasetListRequest>,
    ) -> Result<Response<DatasetListResponse>, Status> {
        let started = Instant::now();
        let identity = self.identity_of(&req, "dataset_list", started).await?;
        let query = req.into_inner().query;
        let res = self.registry.list_datasets(&query).map_err(to_status);
        audit(
            &identity,
            "dataset_list",
            None,
            &format!("query={query}"),
            outcome(&res),
            started,
        );
        let datasets = res?
            .into_iter()
            .map(crate::dataset_convert::dataset_to_msg)
            .collect();
        Ok(Response::new(DatasetListResponse { datasets }))
    }

    async fn catalog_add(
        &self,
        req: Request<CatalogAddRequest>,
    ) -> Result<Response<CatalogAddResponse>, Status> {
        let started = Instant::now();
        let identity = self.identity_of(&req, "catalog_add", started).await?;
        let Some(c) = req.into_inner().catalog else {
            audit(
                &identity,
                "catalog_add",
                None,
                "rejected: catalog is required",
                ERROR,
                started,
            );
            return Err(Status::invalid_argument("catalog is required"));
        };
        if !latiq_common::catalog::is_known_type(&c.r#type) {
            audit(
                &identity,
                "catalog_add",
                None,
                &format!("rejected: unknown catalog type {}", c.r#type),
                ERROR,
                started,
            );
            return Err(Status::invalid_argument(format!(
                "unknown catalog type '{}' (supported: iceberg)",
                c.r#type
            )));
        }
        // Allowlist the params at registration — credentials never persist.
        let incoming: std::collections::BTreeMap<String, String> = c.params.into_iter().collect();
        let (kept, dropped) = latiq_common::catalog::filter_params(&c.r#type, &incoming);
        let row = crate::registry::CatalogRow {
            name: c.name,
            r#type: c.r#type,
            params: kept,
            description: c.description,
            tags: c.tags,
            created_by: Self::created_by(&identity, c.created_by),
            created_at: String::new(),
        };
        let catalog_name = row.name.clone();
        let catalog_type = row.r#type.clone();
        let res = self.registry.add_catalog(&row).map_err(to_status);
        // Params are already credential-filtered above, but the record still
        // names only the catalog and its type — never the locator values.
        audit(
            &identity,
            "catalog_add",
            None,
            &format!("catalog={catalog_name} type={catalog_type}"),
            outcome(&res),
            started,
        );
        Ok(Response::new(CatalogAddResponse {
            name: res?,
            dropped_params: dropped,
        }))
    }

    async fn catalog_remove(
        &self,
        req: Request<CatalogRemoveRequest>,
    ) -> Result<Response<CatalogRemoveResponse>, Status> {
        let started = Instant::now();
        let identity = self.identity_of(&req, "catalog_remove", started).await?;
        let name = req.into_inner().name;
        let res = self.registry.remove_catalog(&name).map_err(to_status);
        audit(
            &identity,
            "catalog_remove",
            None,
            &format!("catalog={name}"),
            outcome(&res),
            started,
        );
        res?;
        Ok(Response::new(CatalogRemoveResponse {}))
    }

    async fn catalog_list(
        &self,
        req: Request<CatalogListRequest>,
    ) -> Result<Response<CatalogListResponse>, Status> {
        let started = Instant::now();
        let identity = self.identity_of(&req, "catalog_list", started).await?;
        let query = req.into_inner().query;
        let res = self.registry.list_catalogs(&query).map_err(to_status);
        audit(
            &identity,
            "catalog_list",
            None,
            &format!("query={query}"),
            outcome(&res),
            started,
        );
        let catalogs = res?
            .into_iter()
            .map(crate::dataset_convert::catalog_to_msg)
            .collect();
        Ok(Response::new(CatalogListResponse { catalogs }))
    }
}

fn node_info(n: crate::registry::NodeRow) -> NodeInfo {
    NodeInfo {
        node_id: n.node_id,
        mcp_endpoint: n.mcp_endpoint,
        state: n.state,
        pond_count: n.pond_count,
        last_heartbeat: n.last_heartbeat,
        heartbeat_age_seconds: n.heartbeat_age_seconds.max(0) as u64,
    }
}
