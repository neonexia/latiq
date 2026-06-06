//! AgentOps — the protocol-neutral agent operations. Composes ControlPlane +
//! PondStorage + QueryEngine. Engine calls (blocking DuckDB) run on the blocking
//! pool; cancellation flows through the in-flight registry's AbortToken.
use crate::control::ControlPlane;
use crate::error::AgentError;
use crate::inflight::InFlightRegistry;
use crate::types::{AllocateResult, AuditRecord, DescribeResult, PondInfo};
use latiq_common::Identity;
use latiq_common::PondId;
use latiq_engine::{ExplainResult, QueryEngine, QueryResult};
use latiq_storage::PondStorage;
use std::sync::Arc;
use std::time::Instant;

#[derive(Debug, Clone)]
pub struct AgentConfig {
    pub inline_row_cap: usize,
}

impl Default for AgentConfig {
    fn default() -> Self {
        Self {
            inline_row_cap: 10_000,
        }
    }
}

#[derive(Clone)]
pub struct AgentOps {
    control: Arc<dyn ControlPlane>,
    storage: Arc<dyn PondStorage>,
    engine: Arc<dyn QueryEngine>,
    inflight: InFlightRegistry,
    config: AgentConfig,
}

impl AgentOps {
    pub fn new(
        control: Arc<dyn ControlPlane>,
        storage: Arc<dyn PondStorage>,
        engine: Arc<dyn QueryEngine>,
        config: AgentConfig,
    ) -> Self {
        Self {
            control,
            storage,
            engine,
            inflight: InFlightRegistry::new(),
            config,
        }
    }

    pub fn inflight(&self) -> &InFlightRegistry {
        &self.inflight
    }

    fn parse_id(pond_id: &str) -> Result<PondId, AgentError> {
        PondId::parse(pond_id).map_err(|e| AgentError::internal(format!("bad pond id: {e}")))
    }

    pub async fn allocate_pond(
        &self,
        identity: &Identity,
        name: Option<String>,
        policy_json: &str,
    ) -> Result<AllocateResult, AgentError> {
        let info = self
            .control
            .create_pond(name, &identity.agent_id, policy_json)
            .await?;
        let pid = Self::parse_id(&info.pond_id)?;
        let loc = self
            .storage
            .create_pond(pid)
            .map_err(|e| AgentError::internal(format!("storage: {e}")))?;

        let engine = self.engine.clone();
        let loc2 = loc.clone();
        let init = tokio::task::spawn_blocking(move || engine.init_pond(&loc2))
            .await
            .map_err(|e| AgentError::internal(format!("join: {e}")))?;
        if let Err(e) = init {
            // Compensate: roll back registry + storage.
            let _ = self.control.drop_pond(&info.pond_id).await;
            let _ = self.storage.drop_pond(pid);
            return Err(e.into());
        }
        self.audit(identity, "allocate_pond", Some(&info.pond_id), None, 0)
            .await;
        Ok(AllocateResult {
            pond_id: info.pond_id,
            pond_name: info.name,
        })
    }

    pub async fn describe_pond(
        &self,
        identity: &Identity,
        pond_ref: &str,
    ) -> Result<DescribeResult, AgentError> {
        let info = self.control.pond_info(pond_ref).await?;
        let pid = Self::parse_id(&info.pond_id)?;
        let loc = self
            .storage
            .pond_location(pid)
            .map_err(|e| AgentError::internal(format!("storage: {e}")))?;
        let engine = self.engine.clone();
        let loc2 = loc.clone();
        let schema = tokio::task::spawn_blocking(move || engine.describe_schema(&loc2))
            .await
            .map_err(|e| AgentError::internal(format!("join: {e}")))??;
        self.audit(identity, "describe_pond", Some(&info.pond_id), None, 0)
            .await;
        Ok(DescribeResult { pond: info, schema })
    }

    pub async fn list_ponds(&self, identity: &Identity) -> Result<Vec<PondInfo>, AgentError> {
        let ponds = self.control.list_ponds().await?;
        self.audit(identity, "list_ponds", None, None, 0).await;
        Ok(ponds)
    }

    pub async fn drop_pond(&self, identity: &Identity, pond_ref: &str) -> Result<(), AgentError> {
        let pond_id = self.control.resolve_pond(pond_ref).await?;
        self.inflight.cancel_for_pond(&pond_id);
        self.control.drop_pond(&pond_id).await?;
        let _ = self.storage.drop_pond(Self::parse_id(&pond_id)?);
        self.audit(identity, "drop_pond", Some(&pond_id), None, 0)
            .await;
        Ok(())
    }

    pub async fn read_query(
        &self,
        identity: &Identity,
        pond_ref: &str,
        sql: &str,
    ) -> Result<QueryResult, AgentError> {
        let res = self.run_query(pond_ref, sql, identity, false).await?;
        Ok(res)
    }

    pub async fn write_query(
        &self,
        identity: &Identity,
        pond_ref: &str,
        sql: &str,
    ) -> Result<QueryResult, AgentError> {
        let res = self.run_query(pond_ref, sql, identity, true).await?;
        Ok(res)
    }

    async fn run_query(
        &self,
        pond_ref: &str,
        sql: &str,
        identity: &Identity,
        write: bool,
    ) -> Result<QueryResult, AgentError> {
        let pond_id = self.control.resolve_pond(pond_ref).await?;
        let pid = Self::parse_id(&pond_id)?;
        let loc = self
            .storage
            .pond_location(pid)
            .map_err(|e| AgentError::internal(format!("storage: {e}")))?;
        let (op_id, token) = self.inflight.register(Some(pond_id.clone()));

        let engine = self.engine.clone();
        let loc2 = loc.clone();
        let sql2 = sql.to_string();
        let identity2 = identity.clone();
        let t0 = Instant::now();
        let out = tokio::task::spawn_blocking(move || {
            if write {
                engine.write_query(&loc2, &sql2, &identity2, token)
            } else {
                engine.read_query(&loc2, &sql2, token)
            }
        })
        .await
        .map_err(|e| AgentError::internal(format!("join: {e}")));
        self.inflight.complete(&op_id);

        let result = out?;
        let qr = result?;
        if !write && qr.rows.len() > self.config.inline_row_cap {
            return Err(AgentError::result_cap_exceeded(
                qr.rows.len(),
                self.config.inline_row_cap,
            ));
        }
        let op = if write { "write_query" } else { "read_query" };
        self.audit(
            identity,
            op,
            Some(&pond_id),
            Some(redact_sql(sql)),
            t0.elapsed().as_millis() as u64,
        )
        .await;
        Ok(qr)
    }

    pub async fn explain_query(
        &self,
        identity: &Identity,
        pond_ref: &str,
        sql: &str,
    ) -> Result<ExplainResult, AgentError> {
        let pond_id = self.control.resolve_pond(pond_ref).await?;
        let pid = Self::parse_id(&pond_id)?;
        let loc = self
            .storage
            .pond_location(pid)
            .map_err(|e| AgentError::internal(format!("storage: {e}")))?;
        let engine = self.engine.clone();
        let loc2 = loc.clone();
        let sql2 = sql.to_string();
        let res = tokio::task::spawn_blocking(move || engine.explain_query(&loc2, &sql2))
            .await
            .map_err(|e| AgentError::internal(format!("join: {e}")))??;
        self.audit(
            identity,
            "explain_query",
            Some(&pond_id),
            Some(redact_sql(sql)),
            0,
        )
        .await;
        Ok(res)
    }

    async fn audit(
        &self,
        identity: &Identity,
        operation: &str,
        pond_id: Option<&str>,
        request_summary: Option<String>,
        duration_ms: u64,
    ) {
        self.control
            .record_audit(AuditRecord {
                agent_identity: identity.agent_id.clone(),
                verified: identity.verified,
                operation: operation.to_string(),
                pond_id: pond_id.map(|s| s.to_string()),
                request_summary,
                duration_ms,
            })
            .await;
    }
}

/// Minimal SQL-shape redaction for the audit log: collapse quoted string and
/// numeric literals to `?`. (A full parser-based redactor is future work.)
fn redact_sql(sql: &str) -> String {
    let mut out = String::with_capacity(sql.len());
    let mut chars = sql.chars().peekable();
    while let Some(c) = chars.next() {
        match c {
            '\'' => {
                // consume to the closing quote
                for n in chars.by_ref() {
                    if n == '\'' {
                        break;
                    }
                }
                out.push('?');
            }
            d if d.is_ascii_digit() => {
                while matches!(chars.peek(), Some(n) if n.is_ascii_digit() || *n == '.') {
                    chars.next();
                }
                out.push('?');
            }
            other => out.push(other),
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn redaction_collapses_literals() {
        assert_eq!(
            redact_sql("SELECT * FROM events WHERE id = 47 AND sev = 'high'"),
            "SELECT * FROM events WHERE id = ? AND sev = ?"
        );
    }
}
