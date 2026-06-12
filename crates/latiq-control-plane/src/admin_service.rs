//! Admin gRPC service (the latiq CLI calls this).
use crate::error::ControlPlaneError;
use crate::registry::Registry;
use latiq_proto::v1::admin_server::Admin;
use latiq_proto::v1::*;
use tonic::{Request, Response, Status};

pub struct AdminService {
    pub registry: Registry,
}

impl AdminService {
    pub fn new(registry: Registry) -> Self {
        Self { registry }
    }
}

fn to_status(e: ControlPlaneError) -> Status {
    match e {
        ControlPlaneError::PondNotFound(m) | ControlPlaneError::NodeNotFound(m) => {
            Status::not_found(m)
        }
        ControlPlaneError::NameConflict(m) => Status::already_exists(m),
        ControlPlaneError::Storage(m) => Status::internal(m),
    }
}

#[tonic::async_trait]
impl Admin for AdminService {
    async fn list_nodes(
        &self,
        _req: Request<ListNodesRequest>,
    ) -> Result<Response<ListNodesResponse>, Status> {
        let nodes = self
            .registry
            .list_nodes()
            .map_err(to_status)?
            .into_iter()
            .map(node_info)
            .collect();
        Ok(Response::new(ListNodesResponse { nodes }))
    }

    async fn describe_node(
        &self,
        req: Request<DescribeNodeRequest>,
    ) -> Result<Response<DescribeNodeResponse>, Status> {
        let n = self
            .registry
            .describe_node(&req.into_inner().node_id)
            .map_err(to_status)?;
        Ok(Response::new(DescribeNodeResponse {
            node: Some(node_info(n)),
        }))
    }

    async fn policy_get(
        &self,
        _req: Request<PolicyGetRequest>,
    ) -> Result<Response<PolicyGetResponse>, Status> {
        let policy = self.registry.policy_get().map_err(to_status)?;
        Ok(Response::new(PolicyGetResponse {
            policy_json: policy.to_string(),
        }))
    }

    async fn policy_set(
        &self,
        req: Request<PolicySetRequest>,
    ) -> Result<Response<PolicySetResponse>, Status> {
        let r = req.into_inner();
        self.registry
            .policy_set(&r.key, &r.value)
            .map_err(to_status)?;
        Ok(Response::new(PolicySetResponse {}))
    }

    async fn audit_tail(
        &self,
        req: Request<AuditTailRequest>,
    ) -> Result<Response<AuditTailResponse>, Status> {
        let limit = req.into_inner().limit.max(1);
        let entries = self
            .registry
            .audit_tail(limit)
            .map_err(to_status)?
            .into_iter()
            .map(audit_entry)
            .collect();
        Ok(Response::new(AuditTailResponse { entries }))
    }

    async fn pond_list(
        &self,
        _req: Request<PondListRequest>,
    ) -> Result<Response<PondListResponse>, Status> {
        let rows = self.registry.list_ponds().map_err(to_status)?;
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
            });
        }
        Ok(Response::new(PondListResponse { ponds }))
    }

    async fn audit_search(
        &self,
        req: Request<AuditSearchRequest>,
    ) -> Result<Response<AuditSearchResponse>, Status> {
        let r = req.into_inner();
        let since = if r.since.is_empty() {
            "1970-01-01".to_string()
        } else {
            r.since
        };
        let entries = self
            .registry
            .audit_search(&r.identity, &since)
            .map_err(to_status)?
            .into_iter()
            .map(audit_entry)
            .collect();
        Ok(Response::new(AuditSearchResponse { entries }))
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

fn audit_entry(a: crate::registry::AuditRow) -> AuditEntry {
    AuditEntry {
        ts: a.ts,
        agent_identity: a.agent_identity,
        verified: a.verified,
        operation: a.operation,
        pond_id: a.pond_id.unwrap_or_default(),
        duration_ms: a.duration_ms,
    }
}
