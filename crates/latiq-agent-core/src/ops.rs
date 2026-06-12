//! AgentOps — the protocol-neutral agent operations. Composes ControlPlane +
//! PondStorage + QueryEngine. Engine calls (blocking DuckDB) run on the blocking
//! pool; cancellation flows through the in-flight registry's AbortToken.
use crate::arrow::ArrowReadStream;
use crate::control::ControlPlane;
use crate::error::AgentError;
use crate::forward::Forwarder;
use crate::inflight::InFlightRegistry;
use crate::types::{AllocateResult, AuditRecord, DescribeResult, PondInfo};
use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use latiq_common::ErrorKind;
use latiq_common::Identity;
use latiq_common::PondId;
use latiq_common::QueryMeta;
use latiq_common::{PondTier, ResourceLimits};
use latiq_engine::{ArrowSink, ExplainResult, QueryEngine, QueryResult};
use latiq_storage::PondStorage;
use std::ops::ControlFlow;
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::{mpsc, oneshot};
use tokio_stream::wrappers::ReceiverStream;
use tokio_stream::StreamExt;
use tracing::info;

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
    /// This node's own internal endpoint (registered with the control plane).
    /// `None` in single-node/in-process setups, where forwarding never applies.
    self_endpoint: Option<String>,
    /// Delegate for ponds owned by a different node. `None` = single-node: every
    /// pond is local, so the behavior is exactly as before forwarding existed.
    forwarder: Option<Arc<dyn Forwarder>>,
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
            self_endpoint: None,
            forwarder: None,
        }
    }

    /// Enable node-to-node forwarding: requests for ponds owned by a node other
    /// than `self_endpoint` are delegated to `forwarder`. Without this, all ponds
    /// are treated as local (single-node behavior).
    pub fn with_forwarding(mut self, self_endpoint: String, forwarder: Arc<dyn Forwarder>) -> Self {
        self.self_endpoint = Some(self_endpoint);
        self.forwarder = Some(forwarder);
        self
    }

    pub fn inflight(&self) -> &InFlightRegistry {
        &self.inflight
    }

    /// If this node isn't the pond's owner (and forwarding is configured), return
    /// the delegate + owner endpoint to forward to. `None` → handle locally. A
    /// pond with no live owner (`node_endpoint == None`) also runs locally, so the
    /// local error path (e.g. storage/availability) surfaces normally.
    fn forward_target<'a>(&'a self, info: &'a PondInfo) -> Option<(&'a dyn Forwarder, &'a str)> {
        let fwd = self.forwarder.as_ref()?;
        let me = self.self_endpoint.as_deref()?;
        let owner = info.node_endpoint.as_deref()?;
        if owner == me {
            return None;
        }
        Some((fwd.as_ref(), owner))
    }

    fn parse_id(pond_id: &str) -> Result<PondId, AgentError> {
        PondId::parse(pond_id).map_err(|e| AgentError::internal(format!("bad pond id: {e}")))
    }

    pub async fn allocate_pond(
        &self,
        identity: &Identity,
        name: Option<String>,
        policy_json: &str,
        tier: &str,
    ) -> Result<AllocateResult, AgentError> {
        let info = self
            .control
            .create_pond(name, &identity.agent_id, policy_json, tier)
            .await?;
        // The control plane may place the pond on a different node than the one
        // that received this call. In that case, don't eagerly create storage here
        // (it would orphan files on the wrong node) — the owning node materializes
        // it lazily on first use (ensure_pond). Single-node: owner == self, so this
        // is skipped and the eager init below runs as before.
        if self.forward_target(&info).is_some() {
            self.audit(identity, "allocate_pond", Some(&info.pond_id), None, 0)
                .await;
            return Ok(AllocateResult {
                pond_id: info.pond_id,
                pond_name: info.name,
            });
        }
        let pid = Self::parse_id(&info.pond_id)?;
        let mut loc = self
            .storage
            .create_pond(pid)
            .map_err(|e| AgentError::internal(format!("storage: {e}")))?;
        loc.catalog_name = info.name.clone();
        loc.limits = tier_limits(&info.tier);

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
        if let Some((fwd, owner)) = self.forward_target(&info) {
            info!(
                op = "describe_pond",
                pond = pond_ref,
                owner,
                "forwarding to owner node"
            );
            return fwd.describe(owner, identity, pond_ref).await;
        }
        info!(op = "describe_pond", pond = pond_ref, "processing locally");
        let pid = Self::parse_id(&info.pond_id)?;
        // ensure_pond materializes storage on first touch; attach under the
        // pond's registry name so introspection is scoped to this catalog.
        let mut loc = self
            .storage
            .ensure_pond(pid)
            .map_err(|e| AgentError::internal(format!("storage: {e}")))?;
        loc.catalog_name = info.name.clone();
        loc.limits = tier_limits(&info.tier);
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

    pub async fn drop_pond(
        &self,
        identity: &Identity,
        pond_ref: &str,
        confirm: bool,
    ) -> Result<(), AgentError> {
        // drop_pond deletes the pond and ALL its data — require explicit confirm.
        // Every surface plumbs this flag; enforcing it here keeps the gate
        // consistent across MCP and the Data gRPC.
        if !confirm {
            return Err(AgentError::new(
                ErrorKind::MissingArgument,
                format!("drop_pond deletes pond '{pond_ref}' and all its data; set confirm=true to proceed"),
                "Re-issue drop_pond with confirm=true once you're certain.",
                "latiq://guidance",
            ));
        }
        let info = self.control.pond_info(pond_ref).await?;
        // Owned by another node → forward the drop so the owner evicts its engine
        // instance and deletes the files it actually holds.
        if let Some((fwd, owner)) = self.forward_target(&info) {
            info!(
                op = "drop_pond",
                pond = pond_ref,
                owner,
                "forwarding to owner node"
            );
            return fwd.drop_pond(owner, identity, pond_ref, confirm).await;
        }
        info!(op = "drop_pond", pond = pond_ref, "processing locally");
        let pond_id = info.pond_id.clone();
        let pid = Self::parse_id(&pond_id)?;
        // Tombstone the pond + cancel its in-flight ops. begin_drop also makes any
        // query that registers from here on get a pre-cancelled token, so one that
        // slipped past resolve_pond can't run against files we're about to delete.
        self.inflight.begin_drop(&pond_id);
        if let Err(e) = self.control.drop_pond(&pond_id).await {
            // Registry drop failed: the pond still exists — clear the tombstone so
            // it stays usable instead of permanently rejecting queries.
            self.inflight.end_drop(&pond_id);
            return Err(e);
        }
        // Evict the cached engine instance (closing its connection to the catalog)
        // BEFORE deleting the files out from under it. Best-effort: a pond that was
        // never queried has no location/instance to forget.
        if let Ok(loc) = self.storage.pond_location(pid) {
            self.engine.forget_pond(&loc);
        }
        let _ = self.storage.drop_pond(pid);
        self.inflight.end_drop(&pond_id);
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

    /// Stream a read as Arrow batches. Local: drive the engine's `read_arrow` on
    /// the blocking pool, delivering the schema then batches over channels
    /// (bounded mpsc → backpressure). Remote: forward to the owning node. The
    /// schema is resolved before returning, so an empty result still carries
    /// columns and a pre-stream error (parse / pond-not-found) surfaces here
    /// rather than mid-stream.
    pub async fn read_arrow(
        &self,
        identity: &Identity,
        pond_ref: &str,
        sql: &str,
    ) -> Result<ArrowReadStream, AgentError> {
        let info = self.control.pond_info(pond_ref).await?;
        if let Some((fwd, owner)) = self.forward_target(&info) {
            info!(
                op = "read_arrow",
                pond = pond_ref,
                owner,
                "forwarding to owner node"
            );
            return fwd.read_arrow(owner, identity, pond_ref, sql).await;
        }
        info!(op = "read_arrow", pond = pond_ref, "processing locally");
        let pond_id = info.pond_id.clone();
        let pid = Self::parse_id(&pond_id)?;
        let mut loc = self
            .storage
            .ensure_pond(pid)
            .map_err(|e| AgentError::internal(format!("storage: {e}")))?;
        loc.catalog_name = info.name.clone();
        loc.limits = tier_limits(&info.tier);
        let (op_id, token) = self.inflight.register(Some(pond_id));

        let (schema_tx, schema_rx) = oneshot::channel::<Result<SchemaRef, AgentError>>();
        let (batch_tx, batch_rx) = mpsc::channel::<Result<RecordBatch, AgentError>>(4);

        let engine = self.engine.clone();
        let inflight = self.inflight.clone();
        let sql2 = sql.to_string();
        tokio::task::spawn_blocking(move || {
            let mut sink = ChannelSink {
                schema_tx: Some(schema_tx),
                batch_tx,
            };
            let res = engine.read_arrow(&loc, &sql2, token, &mut sink);
            if let Err(e) = res {
                let ae = AgentError::from(e);
                // Deliver the error on whichever channel is still open: the schema
                // oneshot (no batches produced yet) or the batch stream.
                if let Some(stx) = sink.schema_tx.take() {
                    let _ = stx.send(Err(ae));
                } else {
                    let _ = sink.batch_tx.blocking_send(Err(ae));
                }
            }
            inflight.complete(&op_id);
        });

        let schema = schema_rx
            .await
            .map_err(|_| AgentError::internal("arrow read produced no schema"))??;
        Ok(ArrowReadStream {
            schema,
            batches: Box::pin(ReceiverStream::new(batch_rx)),
        })
    }

    /// Read via the Arrow hop, then collect the batches into the neutral
    /// `{columns, rows}` `QueryResult` the JSON edges (Data gRPC, MCP) return —
    /// bounded by the inline cap. So MCP/CLI reads ride the same Arrow internal
    /// transport (no double-materialize on a forward) and only convert to JSON
    /// once here, at the edge.
    pub async fn read_collected(
        &self,
        identity: &Identity,
        pond_ref: &str,
        sql: &str,
    ) -> Result<QueryResult, AgentError> {
        let stream = self.read_arrow(identity, pond_ref, sql).await?;
        let columns: Vec<String> = stream
            .schema
            .fields()
            .iter()
            .map(|f| f.name().to_string())
            .collect();
        let mut rows: Vec<Vec<serde_json::Value>> = Vec::new();
        let mut batches = stream.batches;
        while let Some(b) = batches.next().await {
            append_batch_rows(&b?, &columns, &mut rows)?;
            if rows.len() > self.config.inline_row_cap {
                return Err(AgentError::result_cap_exceeded(
                    rows.len(),
                    self.config.inline_row_cap,
                ));
            }
        }
        let n = rows.len() as u64;
        Ok(QueryResult {
            columns,
            rows,
            meta: QueryMeta {
                rows: n,
                ..Default::default()
            },
        })
    }

    async fn run_query(
        &self,
        pond_ref: &str,
        sql: &str,
        identity: &Identity,
        write: bool,
    ) -> Result<QueryResult, AgentError> {
        let info = self.control.pond_info(pond_ref).await?;
        // The CLI sends every statement through write_query (it doesn't parse SQL;
        // the engine classifies it), and forwarding happens *before* execution — so
        // at this point we can't honestly say read vs write. Log a neutral "query".
        // Owned by another node → forward and relay. The owner audits + snapshots;
        // we just return its result, so attribution stays on the node that ran it.
        if let Some((fwd, owner)) = self.forward_target(&info) {
            info!(
                op = "query",
                pond = pond_ref,
                owner,
                "forwarding to owner node"
            );
            return if write {
                fwd.write(owner, identity, pond_ref, sql).await
            } else {
                fwd.read(owner, identity, pond_ref, sql).await
            };
        }
        info!(op = "query", pond = pond_ref, "processing locally");
        let pond_id = info.pond_id.clone();
        let pid = Self::parse_id(&pond_id)?;
        // ensure_pond materializes storage on first touch; attach the catalog
        // under the pond's registry name so callers query `<pond>.snapshots()`.
        let mut loc = self
            .storage
            .ensure_pond(pid)
            .map_err(|e| AgentError::internal(format!("storage: {e}")))?;
        loc.catalog_name = info.name.clone();
        loc.limits = tier_limits(&info.tier);
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
        let info = self.control.pond_info(pond_ref).await?;
        if let Some((fwd, owner)) = self.forward_target(&info) {
            info!(
                op = "explain_query",
                pond = pond_ref,
                owner,
                "forwarding to owner node"
            );
            return fwd.explain(owner, identity, pond_ref, sql).await;
        }
        info!(op = "explain_query", pond = pond_ref, "processing locally");
        let pid = Self::parse_id(&info.pond_id)?;
        // ensure_pond materializes storage on first touch; attach under the
        // pond's registry name so the plan resolves names in this catalog.
        let mut loc = self
            .storage
            .ensure_pond(pid)
            .map_err(|e| AgentError::internal(format!("storage: {e}")))?;
        loc.catalog_name = info.name.clone();
        loc.limits = tier_limits(&info.tier);
        let engine = self.engine.clone();
        let loc2 = loc.clone();
        let sql2 = sql.to_string();
        let res = tokio::task::spawn_blocking(move || engine.explain_query(&loc2, &sql2))
            .await
            .map_err(|e| AgentError::internal(format!("join: {e}")))??;
        self.audit(
            identity,
            "explain_query",
            Some(info.pond_id.as_str()),
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

/// Map a pond's tier name to its resource caps (unknown/empty → medium).
fn tier_limits(tier: &str) -> Option<ResourceLimits> {
    Some(PondTier::parse(tier).unwrap_or_default().limits())
}

/// Convert one Arrow `RecordBatch` to positional JSON rows aligned to `columns`.
/// Uses Arrow's JSON writer (column-keyed objects), then reshapes to arrays in
/// column order — a missing key (a null cell) becomes JSON null.
fn append_batch_rows(
    batch: &RecordBatch,
    columns: &[String],
    out: &mut Vec<Vec<serde_json::Value>>,
) -> Result<(), AgentError> {
    let mut buf = Vec::new();
    let mut w = arrow::json::ArrayWriter::new(&mut buf);
    w.write(batch)
        .map_err(|e| AgentError::internal(format!("arrow->json: {e}")))?;
    w.finish()
        .map_err(|e| AgentError::internal(format!("arrow->json: {e}")))?;
    let objs: Vec<serde_json::Map<String, serde_json::Value>> = serde_json::from_slice(&buf)
        .map_err(|e| AgentError::internal(format!("arrow->json parse: {e}")))?;
    for mut obj in objs {
        let mut row = Vec::with_capacity(columns.len());
        for c in columns {
            row.push(obj.remove(c).unwrap_or(serde_json::Value::Null));
        }
        out.push(row);
    }
    Ok(())
}

/// Bridges the engine's blocking Arrow output to async channels: the schema once
/// (oneshot), then batches (bounded mpsc → backpressure). A closed channel
/// (consumer dropped) returns `Break`, which stops the engine promptly.
struct ChannelSink {
    schema_tx: Option<oneshot::Sender<Result<SchemaRef, AgentError>>>,
    batch_tx: mpsc::Sender<Result<RecordBatch, AgentError>>,
}

impl ArrowSink for ChannelSink {
    fn schema(&mut self, schema: SchemaRef) -> ControlFlow<()> {
        if let Some(tx) = self.schema_tx.take() {
            if tx.send(Ok(schema)).is_err() {
                return ControlFlow::Break(()); // receiver gone
            }
        }
        ControlFlow::Continue(())
    }
    fn batch(&mut self, batch: RecordBatch) -> ControlFlow<()> {
        match self.batch_tx.blocking_send(Ok(batch)) {
            Ok(()) => ControlFlow::Continue(()),
            Err(_) => ControlFlow::Break(()),
        }
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
        assert!(
            !r.contains("hunter2"),
            "literal leaked via block comment: {r}"
        );
        assert!(!r.contains("secret"));
        // The block comment becomes whitespace; the statement shape survives.
        assert_eq!(
            r.split_whitespace().collect::<Vec<_>>(),
            ["SELECT", "id", "FROM", "t"]
        );
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
