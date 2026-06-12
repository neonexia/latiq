//! The control-plane operations AgentOps depends on. Abstracted as a trait so
//! it can be backed in-process by the Registry (now) or a gRPC client (M6).
use crate::error::AgentError;
use crate::types::{AuditRecord, PondInfo};

#[async_trait::async_trait]
pub trait ControlPlane: Send + Sync {
    async fn create_pond(
        &self,
        name: Option<String>,
        owner: &str,
        policy_json: &str,
        tier: &str,
    ) -> Result<PondInfo, AgentError>;

    /// Resolve a pond ref (id or name) to its pond_id, erroring if absent.
    async fn resolve_pond(&self, pond_ref: &str) -> Result<String, AgentError>;

    async fn list_ponds(&self) -> Result<Vec<PondInfo>, AgentError>;

    async fn pond_info(&self, pond_ref: &str) -> Result<PondInfo, AgentError>;

    async fn drop_pond(&self, pond_id: &str) -> Result<(), AgentError>;

    /// Fire-and-forget audit write (errors are swallowed by the impl).
    async fn record_audit(&self, rec: AuditRecord);
}
