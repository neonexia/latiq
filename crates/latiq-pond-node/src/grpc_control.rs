//! `GrpcControlPlane` — implements agent-core's `ControlPlane` over a tonic
//! Control client, so a pond-node (separate process) reaches the control-plane
//! registry remotely. `ControlClient<Channel>` is cheaply cloneable, so each
//! call clones it (tonic RPC methods take `&mut self`).
use latiq_agent_core::{AgentError, ControlPlane};
use latiq_agent_core::{AuditRecord, PondInfo};
use latiq_proto::v1::control_client::ControlClient;
use latiq_proto::v1::*;
use tonic::transport::Channel;
use tonic::Code;

#[derive(Clone)]
pub struct GrpcControlPlane {
    client: ControlClient<Channel>,
}

impl GrpcControlPlane {
    pub async fn connect(control_endpoint: String) -> anyhow::Result<Self> {
        let client = ControlClient::connect(control_endpoint).await?;
        Ok(Self { client })
    }

    pub fn client(&self) -> ControlClient<Channel> {
        self.client.clone()
    }
}

fn status_err(s: tonic::Status) -> AgentError {
    match s.code() {
        Code::NotFound => AgentError::pond_not_found(s.message()),
        Code::AlreadyExists => AgentError::name_conflict(s.message()),
        // FailedPrecondition = no pond node available to host the request (an
        // availability error, not a missing pond) — keep it distinct from NotFound.
        Code::FailedPrecondition => {
            AgentError::internal(format!("no pond node available: {}", s.message()))
        }
        _ => AgentError::internal(s.message().to_string()),
    }
}

fn to_info(m: PondInfoMsg) -> PondInfo {
    PondInfo {
        pond_id: m.pond_id,
        name: m.name,
        owner: m.owner,
        created_at: m.created_at,
        policy_json: m.policy_json,
    }
}

#[async_trait::async_trait]
impl ControlPlane for GrpcControlPlane {
    async fn create_pond(
        &self,
        name: Option<String>,
        owner: &str,
        policy_json: &str,
    ) -> Result<PondInfo, AgentError> {
        let mut c = self.client.clone();
        let created = c
            .create_pond_assignment(CreatePondAssignmentRequest {
                name: name.unwrap_or_default(),
                owner_identity: owner.to_string(),
                policy_json: policy_json.to_string(),
            })
            .await
            .map_err(status_err)?
            .into_inner();
        let info = c
            .get_pond_info(GetPondInfoRequest {
                pond_ref: created.pond_id.clone(),
            })
            .await
            .map_err(status_err)?
            .into_inner()
            .pond
            .ok_or_else(|| AgentError::internal("control plane returned no pond info"))?;
        Ok(to_info(info))
    }

    async fn resolve_pond(&self, pond_ref: &str) -> Result<String, AgentError> {
        let mut c = self.client.clone();
        let loc = c
            .get_pond_location(GetPondLocationRequest {
                pond_ref: pond_ref.to_string(),
            })
            .await
            .map_err(status_err)?
            .into_inner();
        Ok(loc.pond_id)
    }

    async fn list_ponds(&self) -> Result<Vec<PondInfo>, AgentError> {
        let mut c = self.client.clone();
        let resp = c
            .list_ponds(ListPondsRequest {})
            .await
            .map_err(status_err)?
            .into_inner();
        Ok(resp.ponds.into_iter().map(to_info).collect())
    }

    async fn pond_info(&self, pond_ref: &str) -> Result<PondInfo, AgentError> {
        let mut c = self.client.clone();
        let resp = c
            .get_pond_info(GetPondInfoRequest {
                pond_ref: pond_ref.to_string(),
            })
            .await
            .map_err(status_err)?
            .into_inner();
        resp.pond
            .map(to_info)
            .ok_or_else(|| AgentError::pond_not_found(pond_ref))
    }

    async fn drop_pond(&self, pond_id: &str) -> Result<(), AgentError> {
        let mut c = self.client.clone();
        c.drop_pond_assignment(DropPondAssignmentRequest {
            pond_id: pond_id.to_string(),
        })
        .await
        .map_err(status_err)?;
        Ok(())
    }

    async fn record_audit(&self, rec: AuditRecord) {
        let mut c = self.client.clone();
        let _ = c
            .record_audit(RecordAuditRequest {
                agent_identity: rec.agent_identity,
                identity_verified: rec.verified,
                operation: rec.operation,
                pond_id: rec.pond_id.unwrap_or_default(),
                request_summary_json: rec.request_summary.unwrap_or_default(),
                result_summary_json: String::new(),
                duration_ms: rec.duration_ms,
            })
            .await;
    }
}
