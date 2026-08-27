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
}

impl AdminService {
    pub fn new(registry: Registry) -> Self {
        Self {
            registry,
            verifier: None,
        }
    }

    /// Require verified bearer tokens on this surface. `None` keeps the relaxed
    /// (embedded / dev) path.
    pub fn with_verifier(mut self, verifier: Option<std::sync::Arc<latiq_auth::Verifier>>) -> Self {
        self.verifier = verifier;
        self
    }

    /// Identity from gRPC metadata. With a verifier configured, an
    /// `authorization: Bearer <jwt>` header is REQUIRED and verified;
    /// `latiq-agent-id` then supplies only the claimed leaf. Without one,
    /// identity stays relaxed (claimed, default anonymous).
    async fn identity_of<T>(&self, req: &Request<T>) -> Result<Identity, Status> {
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
            return Err(Status::unauthenticated("a bearer token is required"));
        };
        verifier.verify(token, claimed).await.map_err(|e| {
            // Logged in full here, summarised on the wire: the detail is for the
            // operator, and an unauthenticated caller must not be able to probe
            // our issuer list or key endpoints by reading error text.
            tracing::debug!(error = %e, "bearer token rejected");
            Status::unauthenticated("the bearer token was rejected")
        })
    }
}

/// One access record for an operator action, on the SAME `latiq::access` target
/// and with the SAME field names as `AgentOps::audit` — so `op=`/`subject=`
/// greps find operator and agent actions alike. The control plane holds no
/// `AgentOps`, so this is a local twin rather than a shared call; keep the
/// fields identical. `pond` is `-` where the action is not about one pond.
///
/// As there, `subject=`/`issuer=` are only meaningful together with
/// `verified=true`; `agent=` is the caller's own claim and carries no authority.
fn audit(
    identity: &Identity,
    op: &'static str,
    pond: Option<&str>,
    summary: &str,
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
        let identity = self.identity_of(&req).await?;
        let res = self.registry.list_nodes().map_err(to_status);
        audit(&identity, "list_nodes", None, "", started);
        let nodes = res?.into_iter().map(node_info).collect();
        Ok(Response::new(ListNodesResponse { nodes }))
    }

    async fn describe_node(
        &self,
        req: Request<DescribeNodeRequest>,
    ) -> Result<Response<DescribeNodeResponse>, Status> {
        let started = Instant::now();
        let identity = self.identity_of(&req).await?;
        let node_id = req.into_inner().node_id;
        let res = self.registry.describe_node(&node_id).map_err(to_status);
        audit(
            &identity,
            "describe_node",
            None,
            &format!("node={node_id}"),
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
        let identity = self.identity_of(&req).await?;
        let res = self.registry.policy_get().map_err(to_status);
        audit(&identity, "policy_get", None, "", started);
        Ok(Response::new(PolicyGetResponse {
            policy_json: res?.to_string(),
        }))
    }

    async fn policy_set(
        &self,
        req: Request<PolicySetRequest>,
    ) -> Result<Response<PolicySetResponse>, Status> {
        let started = Instant::now();
        let identity = self.identity_of(&req).await?;
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
        let identity = self.identity_of(&req).await?;
        let listed = self.registry.list_ponds().map_err(to_status);
        audit(&identity, "pond_list", None, "", started);
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
            });
        }
        Ok(Response::new(PondListResponse { ponds }))
    }

    async fn pond_set_tier(
        &self,
        req: Request<PondSetTierRequest>,
    ) -> Result<Response<PondSetTierResponse>, Status> {
        let started = Instant::now();
        let identity = self.identity_of(&req).await?;
        let r = req.into_inner();
        if r.pond.trim().is_empty() {
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
        let identity = self.identity_of(&req).await?;
        let d = req
            .into_inner()
            .dataset
            .ok_or_else(|| Status::invalid_argument("dataset is required"))?;
        let row = crate::registry::DatasetRow {
            name: d.name,
            description: d.description,
            tags: d.tags,
            tables: d
                .tables
                .into_iter()
                .map(crate::dataset_convert::dataset_table_from_msg)
                .collect(),
            created_by: if d.created_by.is_empty() {
                "anonymous".into()
            } else {
                d.created_by
            },
            created_at: String::new(),
        };
        let dataset_name = row.name.clone();
        let res = self.registry.add_dataset(&row).map_err(to_status);
        audit(
            &identity,
            "dataset_add",
            None,
            &format!("dataset={dataset_name}"),
            started,
        );
        Ok(Response::new(DatasetAddResponse { name: res? }))
    }

    async fn dataset_remove(
        &self,
        req: Request<DatasetRemoveRequest>,
    ) -> Result<Response<DatasetRemoveResponse>, Status> {
        let started = Instant::now();
        let identity = self.identity_of(&req).await?;
        let name = req.into_inner().name;
        let res = self.registry.remove_dataset(&name).map_err(to_status);
        audit(
            &identity,
            "dataset_remove",
            None,
            &format!("dataset={name}"),
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
        let identity = self.identity_of(&req).await?;
        let query = req.into_inner().query;
        let res = self.registry.list_datasets(&query).map_err(to_status);
        audit(
            &identity,
            "dataset_list",
            None,
            &format!("query={query}"),
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
        let identity = self.identity_of(&req).await?;
        let c = req
            .into_inner()
            .catalog
            .ok_or_else(|| Status::invalid_argument("catalog is required"))?;
        if !latiq_common::catalog::is_known_type(&c.r#type) {
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
            created_by: if c.created_by.is_empty() {
                "anonymous".into()
            } else {
                c.created_by
            },
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
        let identity = self.identity_of(&req).await?;
        let name = req.into_inner().name;
        let res = self.registry.remove_catalog(&name).map_err(to_status);
        audit(
            &identity,
            "catalog_remove",
            None,
            &format!("catalog={name}"),
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
        let identity = self.identity_of(&req).await?;
        let query = req.into_inner().query;
        let res = self.registry.list_catalogs(&query).map_err(to_status);
        audit(
            &identity,
            "catalog_list",
            None,
            &format!("query={query}"),
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
