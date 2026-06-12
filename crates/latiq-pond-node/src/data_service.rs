//! The Data/Query gRPC surface — a second inbound adapter onto `AgentOps`, for
//! the CLI and SDK (NOT agents). Identity comes from the `latiq-agent-id` gRPC
//! metadata key (relaxed, default anonymous). Errors map to a tonic `Status`
//! whose code derives from the ErrorKind and whose `details` carry the
//! JSON-encoded `ErrorEnvelope` (so the client can render kind/suggest/see).
use crate::wire::query_value;
use latiq_agent_core::{AgentError, AgentOps};
use latiq_common::{ErrorKind, Identity};
use latiq_proto::v1::data_server::Data;
use latiq_proto::v1::*;
use std::sync::Arc;
use tonic::{Code, Request, Response, Status};

pub struct DataService {
    ops: Arc<AgentOps>,
}

impl DataService {
    pub fn new(ops: Arc<AgentOps>) -> Self {
        Self { ops }
    }
}

/// Relaxed identity from gRPC metadata (`latiq-agent-id`); default anonymous.
pub(crate) fn identity_of<T>(req: &Request<T>) -> Identity {
    let claimed = req
        .metadata()
        .get("latiq-agent-id")
        .and_then(|v| v.to_str().ok());
    Identity::claimed(claimed)
}

pub(crate) fn to_status(e: AgentError) -> Status {
    let env = e.into_envelope();
    let code = match env.kind {
        ErrorKind::PondNotFound => Code::NotFound,
        ErrorKind::NameConflict => Code::AlreadyExists,
        ErrorKind::QueryTimeout => Code::DeadlineExceeded,
        ErrorKind::QueryCancelled => Code::Cancelled,
        ErrorKind::Storage | ErrorKind::Internal => Code::Internal,
        // ParseError / InvalidValue / MissingArgument / ReadOnlyViolation /
        // WriteToReservedSchema / ResultCapExceeded / UriNotAllowed
        _ => Code::InvalidArgument,
    };
    let details = serde_json::to_vec(&env).unwrap_or_default();
    Status::with_details(code, env.message.clone(), details.into())
}

fn json_resp(value: serde_json::Value) -> Response<JsonResponse> {
    Response::new(JsonResponse {
        json: value.to_string(),
    })
}

#[tonic::async_trait]
impl Data for DataService {
    async fn allocate_pond(
        &self,
        req: Request<AllocatePondRequest>,
    ) -> Result<Response<AllocatePondResponse>, Status> {
        let id = identity_of(&req);
        let r = req.into_inner();
        let name = if r.name.is_empty() {
            None
        } else {
            Some(r.name)
        };
        let policy = if r.policy_json.is_empty() {
            "{}".to_string()
        } else {
            r.policy_json
        };
        let res = self
            .ops
            .allocate_pond(&id, name, &policy)
            .await
            .map_err(to_status)?;
        Ok(Response::new(AllocatePondResponse {
            pond_id: res.pond_id,
            pond_name: res.pond_name,
        }))
    }

    async fn drop_pond(
        &self,
        req: Request<DropPondRequest>,
    ) -> Result<Response<DropPondResponse>, Status> {
        let id = identity_of(&req);
        let r = req.into_inner();
        self.ops
            .drop_pond(&id, &r.pond, r.confirm)
            .await
            .map_err(to_status)?;
        Ok(Response::new(DropPondResponse {}))
    }

    async fn describe_pond(
        &self,
        req: Request<DescribePondRequest>,
    ) -> Result<Response<JsonResponse>, Status> {
        let id = identity_of(&req);
        let r = req.into_inner();
        let d = self
            .ops
            .describe_pond(&id, &r.pond)
            .await
            .map_err(to_status)?;
        Ok(json_resp(serde_json::to_value(d).unwrap_or_default()))
    }

    async fn read_query(
        &self,
        req: Request<QueryRequest>,
    ) -> Result<Response<JsonResponse>, Status> {
        let id = identity_of(&req);
        let r = req.into_inner();
        // Reads ride the Arrow internal hop, collected to JSON here at the edge.
        let qr = self
            .ops
            .read_collected(&id, &r.pond, &r.sql)
            .await
            .map_err(to_status)?;
        Ok(json_resp(query_value(qr)))
    }

    async fn write_query(
        &self,
        req: Request<QueryRequest>,
    ) -> Result<Response<JsonResponse>, Status> {
        let id = identity_of(&req);
        let r = req.into_inner();
        let qr = self
            .ops
            .write_query(&id, &r.pond, &r.sql)
            .await
            .map_err(to_status)?;
        Ok(json_resp(query_value(qr)))
    }

    async fn explain_query(
        &self,
        req: Request<QueryRequest>,
    ) -> Result<Response<JsonResponse>, Status> {
        let id = identity_of(&req);
        let r = req.into_inner();
        let er = self
            .ops
            .explain_query(&id, &r.pond, &r.sql)
            .await
            .map_err(to_status)?;
        Ok(json_resp(serde_json::to_value(er).unwrap_or_default()))
    }
}
