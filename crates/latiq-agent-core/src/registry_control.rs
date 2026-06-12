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

fn to_info(
    row: PondRow,
    created_at: String,
    policy_json: String,
    node_endpoint: Option<String>,
) -> PondInfo {
    PondInfo {
        pond_id: row.pond_id,
        name: row.name,
        owner: row.owner_identity,
        created_at,
        policy_json,
        node_endpoint,
        tier: row.tier,
    }
}

#[async_trait::async_trait]
impl ControlPlane for RegistryControlPlane {
    async fn create_pond(
        &self,
        name: Option<String>,
        owner: &str,
        policy_json: &str,
        tier: &str,
    ) -> Result<PondInfo, AgentError> {
        let row = self
            .registry
            .create_pond(name, owner, policy_json, tier)
            .map_err(cp_err)?;
        let (row, created_at, policy, endpoint) =
            self.registry.pond_info(&row.pond_id).map_err(cp_err)?;
        Ok(to_info(row, created_at, policy, endpoint))
    }

    async fn resolve_pond(&self, pond_ref: &str) -> Result<String, AgentError> {
        let (row, _, _, _) = self.registry.pond_info(pond_ref).map_err(cp_err)?;
        Ok(row.pond_id)
    }

    async fn list_ponds(&self) -> Result<Vec<PondInfo>, AgentError> {
        let rows = self.registry.list_ponds().map_err(cp_err)?;
        let mut out = Vec::with_capacity(rows.len());
        for row in rows {
            // This is a list-then-detail (N+1) read: a pond dropped between the
            // list and its pond_info lookup must be skipped, not fail the whole
            // call (review #9). Other errors still propagate.
            match self.registry.pond_info(&row.pond_id) {
                Ok((row, created_at, policy, endpoint)) => {
                    out.push(to_info(row, created_at, policy, endpoint))
                }
                Err(ControlPlaneError::PondNotFound(_)) => continue,
                Err(e) => return Err(cp_err(e)),
            }
        }
        Ok(out)
    }

    async fn pond_info(&self, pond_ref: &str) -> Result<PondInfo, AgentError> {
        let (row, created_at, policy, endpoint) =
            self.registry.pond_info(pond_ref).map_err(cp_err)?;
        Ok(to_info(row, created_at, policy, endpoint))
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    fn node_backed_registry() -> Registry {
        let reg = Registry::open(None).unwrap();
        reg.register_node("node-a", "http://n:8080/mcp", "http://n:9092", 1000)
            .unwrap();
        reg
    }

    // list_ponds is a list-then-detail (N+1) read: a pond dropped between the
    // list snapshot and its per-row pond_info lookup must be skipped, not blow up
    // the whole call (review #9). Drops run concurrently with repeated lists; every
    // list must return Ok regardless of which ponds raced away mid-iteration.
    #[tokio::test]
    async fn list_ponds_tolerates_concurrent_drop() {
        let registry = node_backed_registry();
        let cp = Arc::new(RegistryControlPlane::new(registry.clone()));

        let mut ids = Vec::new();
        for i in 0..40 {
            let info = cp
                .create_pond(Some(format!("p{i}")), "agent-x", "{}", "medium")
                .await
                .unwrap();
            ids.push(info.pond_id);
        }

        // Dropper: tear the ponds down one by one, concurrently with the lister.
        let dropper = {
            let cp = cp.clone();
            tokio::spawn(async move {
                for id in ids {
                    let _ = cp.drop_pond(&id).await;
                    tokio::task::yield_now().await;
                }
            })
        };

        // Lister: hammer list_ponds while drops are in flight. Before the fix, a
        // drop landing between list_ponds() and pond_info() returned PondNotFound
        // and failed the whole call.
        let lister = {
            let cp = cp.clone();
            tokio::spawn(async move {
                for _ in 0..200 {
                    cp.list_ponds()
                        .await
                        .expect("list_ponds must tolerate a concurrent drop");
                    tokio::task::yield_now().await;
                }
            })
        };

        dropper.await.unwrap();
        lister.await.unwrap();

        // All ponds dropped → an empty, still-successful list.
        assert!(cp.list_ponds().await.unwrap().is_empty());
    }
}
