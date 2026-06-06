//! In-process `ControlPlane` backed directly by the control-plane `Registry`.
//! Used for single-process operation and tests; M6 adds a gRPC-client impl.
use crate::control::ControlPlane;
use crate::error::AgentError;
use crate::types::{AuditRecord, PondInfo};
use latiq_common::ErrorKind;
use latiq_control_plane::registry::{AuditInsert, PondRow};
use latiq_control_plane::{ControlPlaneError, Registry};

pub struct RegistryControlPlane {
    registry: Registry,
}

impl RegistryControlPlane {
    pub fn new(registry: Registry) -> Self {
        Self { registry }
    }
}

fn cp_err(e: ControlPlaneError) -> AgentError {
    match e {
        ControlPlaneError::NameConflict(n) => AgentError::name_conflict(&n),
        ControlPlaneError::PondNotFound(r) => AgentError::pond_not_found(&r),
        ControlPlaneError::NodeNotFound(m) => AgentError::new(
            ErrorKind::Internal,
            format!("no pond node available: {m}"),
            "Ensure a pond node is registered with the control plane.",
            "latiq://troubleshooting",
        ),
        ControlPlaneError::Storage(m) => AgentError::internal(m),
    }
}

fn to_info(row: PondRow, created_at: String, policy_json: String) -> PondInfo {
    PondInfo {
        pond_id: row.pond_id,
        name: row.name,
        owner: row.owner_identity,
        created_at,
        policy_json,
    }
}

#[async_trait::async_trait]
impl ControlPlane for RegistryControlPlane {
    async fn create_pond(
        &self,
        name: Option<String>,
        owner: &str,
        policy_json: &str,
    ) -> Result<PondInfo, AgentError> {
        let row = self
            .registry
            .create_pond(name, owner, policy_json)
            .map_err(cp_err)?;
        let (row, created_at, policy) = self.registry.pond_info(&row.pond_id).map_err(cp_err)?;
        Ok(to_info(row, created_at, policy))
    }

    async fn resolve_pond(&self, pond_ref: &str) -> Result<String, AgentError> {
        let (row, _, _) = self.registry.pond_info(pond_ref).map_err(cp_err)?;
        Ok(row.pond_id)
    }

    async fn list_ponds(&self) -> Result<Vec<PondInfo>, AgentError> {
        let rows = self.registry.list_ponds().map_err(cp_err)?;
        let mut out = Vec::with_capacity(rows.len());
        for row in rows {
            let (row, created_at, policy) =
                self.registry.pond_info(&row.pond_id).map_err(cp_err)?;
            out.push(to_info(row, created_at, policy));
        }
        Ok(out)
    }

    async fn pond_info(&self, pond_ref: &str) -> Result<PondInfo, AgentError> {
        let (row, created_at, policy) = self.registry.pond_info(pond_ref).map_err(cp_err)?;
        Ok(to_info(row, created_at, policy))
    }

    async fn drop_pond(&self, pond_id: &str) -> Result<(), AgentError> {
        self.registry.drop_pond(pond_id).map_err(cp_err)
    }

    async fn record_audit(&self, rec: AuditRecord) {
        let _ = self.registry.record_audit(AuditInsert {
            agent_identity: rec.agent_identity,
            identity_verified: rec.verified,
            operation: rec.operation,
            pond_id: rec.pond_id,
            request_summary: rec.request_summary,
            result_summary: None,
            duration_ms: rec.duration_ms,
        });
    }
}
