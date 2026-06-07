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
        let pid = Self::parse_id(&pond_id)?;
        // Evict the cached engine instance (closing its connection to the catalog)
        // BEFORE deleting the files out from under it. Best-effort: a pond that was
        // never queried has no location/instance to forget.
        if let Ok(loc) = self.storage.pond_location(pid) {
            self.engine.forget_pond(&loc);
        }
        let _ = self.storage.drop_pond(pid);
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

/// Minimal SQL-shape redaction for the audit log: drop comments (so literals
/// hidden in them can't leak), then collapse quoted-string and numeric *literals*
/// to `?`. Identifiers that merely contain digits (`t1`, `events2`) are preserved.
/// (A full parser-based redactor is future work.)
fn redact_sql(sql: &str) -> String {
    let decommented = strip_sql_comments(sql);
    let mut out = String::with_capacity(decommented.len());
    let mut chars = decommented.chars().peekable();
    // Whether the previously emitted char was part of an identifier — a digit
    // right after one (e.g. the `1` in `t1`) is part of that identifier, not a
    // numeric literal, so it must not be collapsed.
    let mut prev_ident = false;
    while let Some(c) = chars.next() {
        match c {
            '\'' => {
                // String literal: consume to the closing quote, treating `''` as
                // an escaped quote (not the terminator).
                while let Some(n) = chars.next() {
                    if n == '\'' {
                        if chars.peek() == Some(&'\'') {
                            chars.next();
                        } else {
                            break;
                        }
                    }
                }
                out.push('?');
                prev_ident = false;
            }
            d if d.is_ascii_digit() && !prev_ident => {
                // Numeric literal: collapse the digit/decimal run.
                while matches!(chars.peek(), Some(n) if n.is_ascii_digit() || *n == '.') {
                    chars.next();
                }
                out.push('?');
                prev_ident = false;
            }
            other => {
                prev_ident = other.is_ascii_alphanumeric() || other == '_';
                out.push(other);
            }
        }
    }
    out
}

/// Strip SQL comments (`-- … EOL` and `/* … */`), leaving string literals — and
/// any `--`/`/*` *inside* them — untouched.
fn strip_sql_comments(sql: &str) -> String {
    let mut out = String::with_capacity(sql.len());
    let mut chars = sql.chars().peekable();
    while let Some(c) = chars.next() {
        match c {
            '\'' => {
                // Copy the whole string literal verbatim (with `''` escapes).
                out.push('\'');
                while let Some(n) = chars.next() {
                    out.push(n);
                    if n == '\'' {
                        if chars.peek() == Some(&'\'') {
                            out.push(chars.next().unwrap());
                        } else {
                            break;
                        }
                    }
                }
            }
            '-' if chars.peek() == Some(&'-') => {
                // Line comment: drop to (and keep) the newline.
                for n in chars.by_ref() {
                    if n == '\n' {
                        out.push('\n');
                        break;
                    }
                }
            }
            '/' if chars.peek() == Some(&'*') => {
                chars.next(); // consume '*'
                let mut prev = '\0';
                for n in chars.by_ref() {
                    if prev == '*' && n == '/' {
                        break;
                    }
                    prev = n;
                }
                out.push(' '); // keep a token separator
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

    #[test]
    fn redaction_preserves_identifiers_with_digits() {
        // t1/t2/events2 are identifiers, not literals — they must survive.
        assert_eq!(
            redact_sql("SELECT t1.x FROM events2 AS t1 JOIN t2 ON t1.id = 5"),
            "SELECT t1.x FROM events2 AS t1 JOIN t2 ON t1.id = ?"
        );
    }

    #[test]
    fn redaction_strips_line_comment_so_literals_do_not_leak() {
        let r = redact_sql("SELECT 1 -- pw is 'hunter2'\nFROM t");
        assert!(!r.contains("hunter2"), "literal leaked via comment: {r}");
        assert!(!r.contains("pw is"), "comment text leaked: {r}");
        assert_eq!(r, "SELECT ? \nFROM t");
    }

    #[test]
    fn redaction_strips_block_comment() {
        let r = redact_sql("SELECT /* secret 'hunter2' */ id FROM t");
        assert!(!r.contains("hunter2"), "literal leaked via block comment: {r}");
        assert!(!r.contains("secret"));
        // The block comment becomes whitespace; the statement shape survives.
        assert_eq!(r.split_whitespace().collect::<Vec<_>>(), ["SELECT", "id", "FROM", "t"]);
    }

    #[test]
    fn redaction_handles_escaped_quote_and_keeps_dashes_in_strings() {
        // `''` escape inside a literal, and a `--` that lives inside a string must
        // NOT be treated as a comment — the whole literal collapses to one `?`.
        assert_eq!(
            redact_sql("INSERT INTO t VALUES ('it''s -- not a comment')"),
            "INSERT INTO t VALUES (?)"
        );
    }
}
