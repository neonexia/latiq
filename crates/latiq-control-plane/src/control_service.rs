//! Control gRPC service (pond-nodes call this).
use crate::error::ControlPlaneError;
use crate::registry::{AuditInsert, Registry};
use latiq_proto::v1::control_server::Control;
use latiq_proto::v1::*;
use tonic::{Request, Response, Status};

pub struct ControlService {
    pub registry: Registry,
}

impl ControlService {
    pub fn new(registry: Registry) -> Self {
        Self { registry }
    }
}

fn to_status(e: ControlPlaneError) -> Status {
    match e {
        ControlPlaneError::NameConflict(m) => Status::already_exists(m),
        ControlPlaneError::PondNotFound(m) => Status::not_found(m),
        // No node available to host the pond is an availability/precondition
        // failure, NOT "pond not found" — collapsing them mislabels allocate-with-
        // no-node as a missing pond (review #13).
        ControlPlaneError::NodeNotFound(m) => Status::failed_precondition(m),
        ControlPlaneError::Storage(m) => Status::internal(m),
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

    async fn create_pond_assignment(
        &self,
        req: Request<CreatePondAssignmentRequest>,
    ) -> Result<Response<CreatePondAssignmentResponse>, Status> {
        let r = req.into_inner();
        let name = if r.name.is_empty() {
            None
        } else {
            Some(r.name)
        };
        let pond = self
            .registry
            .create_pond(name, &r.owner_identity, &r.policy_json)
            .map_err(to_status)?;
        let (_pid, endpoint) = self
            .registry
            .get_pond_location(&pond.pond_id)
            .map_err(to_status)?;
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
            let (row, created_at, policy_json) = match self.registry.pond_info(&row.pond_id) {
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
            });
        }
        Ok(Response::new(ListPondsResponse { ponds }))
    }

    async fn get_pond_info(
        &self,
        req: Request<GetPondInfoRequest>,
    ) -> Result<Response<GetPondInfoResponse>, Status> {
        let (row, created_at, policy_json) = self
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
            }),
        }))
    }

    async fn record_audit(
        &self,
        req: Request<RecordAuditRequest>,
    ) -> Result<Response<RecordAuditResponse>, Status> {
        let r = req.into_inner();
        self.registry
            .record_audit(AuditInsert {
                agent_identity: r.agent_identity,
                identity_verified: r.identity_verified,
                operation: r.operation,
                pond_id: if r.pond_id.is_empty() {
                    None
                } else {
                    Some(r.pond_id)
                },
                request_summary: if r.request_summary_json.is_empty() {
                    None
                } else {
                    Some(r.request_summary_json)
                },
                result_summary: if r.result_summary_json.is_empty() {
                    None
                } else {
                    Some(r.result_summary_json)
                },
                duration_ms: r.duration_ms,
            })
            .map_err(to_status)?;
        Ok(Response::new(RecordAuditResponse {}))
    }
}
