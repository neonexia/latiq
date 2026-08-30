//! The pond's OpenLineage trail — one `RunEvent` pair per query, written by the
//! node that ran it.
//!
//! Shaped like [`crate::access`] on purpose: a single emitter, called from the
//! **public** ops methods beside `self.audit(...)`, so every producer says the
//! same thing about the same operation. Where it differs from the access trail,
//! it differs deliberately:
//!
//! - **Opt-in per pond.** `AgentOps::emit_lineage` returns before this module is
//!   entered when the pond did not opt in, so a pond without lineage pays no
//!   event, no formatting and no directory lookup. That is the whole
//!   justification for the flag: "emits nothing" would not be worth a flag.
//! - **Local only.** A forwarded op returns before the emit exactly as it
//!   returns before the audit — the owner ran the query, so the owner records
//!   it. Emitting on both sides would duplicate the run under two different
//!   pond-local snapshot ids, only one of them real.
//! - **Emitted once, from the public method.** `read_collected` runs
//!   `read_arrow_local`; an emitter in the local halves would record that read
//!   twice, under two different ops.
//! - **Never fails a query.** Nothing here returns a `Result` and nothing here
//!   unwraps: the writer swallows and warns, and the worst case is a lost event.
//!
//! Two absences worth naming, so the next reader does not read them as
//! oversights:
//!
//! - **No `parent` / `latiq_parent_claim` facet.** `latiq-lineage` has the
//!   UUIDv5 derivation ready, but **no transport carries a workflow id today**,
//!   and identity-shaped context arrives in the transport and never in a
//!   tool/RPC argument (invariant 9). Inventing a SQL-level or argument-level
//!   workflow id here would be a design violation, so the facet stays absent
//!   until a transport field exists to fill it.
//! - **No `processing_engine` facet.** It requires the DuckDB version, and this
//!   crate is protocol- and engine-neutral (invariant 5): there is no
//!   engine-version accessor on the `QueryEngine` trait to read one from. It
//!   belongs here the moment there is.
//!
//! Datasets come from `QueryMeta.tables_touched`, which **nothing populates
//! today** — task 4 fills it from DuckDB's bound plan. Until then most events
//! carry no datasets, and that is expected: an invented input is worse than a
//! missing one.
use latiq_common::{Identity, QueryMeta};
use latiq_lineage::event::{
    dataset_namespace, facets, job_name, DurationMeaning, EventType, Outcome, JOB_NAMESPACE,
};
use latiq_lineage::{Dataset, Job, LineageWriter, Run, RunEvent};

use crate::error::AgentError;
use crate::types::PondInfo;

/// `nodeId` for a node with no advertised endpoint — the single-node and
/// in-process setups, where forwarding never applies and there is exactly one
/// node for a consumer to attribute the run to. A constant rather than an empty
/// string so the field is never ambiguous with "we forgot to record it".
pub(crate) const IN_PROCESS_NODE: &str = "in-process";

/// Everything one operation knows about itself at the point the public op
/// method is about to return. A struct rather than ten arguments so a call site
/// cannot silently transpose two of them.
pub(crate) struct QueryRecord<'a> {
    pub identity: &'a Identity,
    pub info: &'a PondInfo,
    /// The op as the CALLER invoked it (`read_query`, `write_query`,
    /// `read_arrow`) — the same name the access trail records, never the
    /// internal hop it rode.
    pub op: &'a str,
    /// Raw SQL. Redacted here, once, on the way into the facet.
    pub sql: &'a str,
    pub duration_ms: u64,
    /// What `duration_ms` measured — `establishment` for a stream, whose
    /// completion is not observable server-side.
    pub meaning: DurationMeaning,
    /// `None` when the op succeeded.
    pub error: Option<&'a AgentError>,
    /// `None` where there is no result to describe yet (a stream at
    /// establishment).
    pub meta: Option<&'a QueryMeta>,
}

/// Buffer this operation's `START` and terminal event. Both go in one
/// `record_all` so a reader never finds one without the other in the directory.
pub(crate) fn record(writer: &LineageWriter, node_id: &str, rec: QueryRecord<'_>) {
    let info = rec.info;
    let is_write = rec.op == "write_query";

    // One flat list today (see the module doc). A write's tables are what it
    // produced; a read's are what it consumed.
    let tables: &[String] = rec.meta.map(|m| m.tables_touched.as_slice()).unwrap_or(&[]);
    let snapshot = rec.meta.and_then(|m| m.snapshot_id);
    let datasets: Vec<Dataset> = tables
        .iter()
        .map(|t| {
            let ds = Dataset::new(dataset_namespace(&info.pond_id), t.clone());
            // The DuckLake snapshot rides the standard dataset-version facet;
            // there is no top-level snapshot field in the spec.
            match snapshot {
                Some(id) => ds.with(facets::dataset_version(id)),
                None => ds,
            }
        })
        .collect();

    let job = || {
        Job::new(
            JOB_NAMESPACE,
            job_name(&info.name, rec.op, tables.first().map(String::as_str)),
        )
        .with(facets::sql(&crate::ops::redact_sql(rec.sql)))
        .with(facets::job_type("QUERY"))
        .with(facets::pond(&info.pond_id, &info.name, node_id))
    };

    // The run id is minted once and shared: a START and its terminal event are
    // one run, and a consumer joins them on nothing else.
    let base = Run::new().with(facets::identity(
        rec.identity.verified,
        &rec.identity.subject,
        non_empty(&rec.identity.issuer),
        // CLAIMED, and stamped `agentIdVerified: false` by `latiq-lineage`.
        Some(rec.identity.agent_id.as_str()),
    ));

    // START carries inputs only: the outputs do not exist yet, and their
    // version facet would be the snapshot BEFORE the write rather than the one
    // the write produced.
    let mut start = RunEvent::new(EventType::Start, base.clone(), job());
    if !is_write {
        start = start.with_inputs(datasets.clone());
    }

    let (event_type, outcome) = terminal(rec.error);
    let mut run = base.with(facets::query(
        rec.op,
        outcome,
        rec.duration_ms,
        rec.meaning,
        crate::trace::current_trace_id().as_deref(),
    ));
    if let Some(e) = rec.error {
        run = run.with(facets::error_message(&e.envelope().message));
    }
    let mut terminal_event = RunEvent::new(event_type, run, job());
    terminal_event = if is_write {
        terminal_event.with_outputs(datasets)
    } else {
        terminal_event.with_inputs(datasets)
    };

    writer.record_all(&[start, terminal_event]);
}

/// The terminal state. A cancelled query `ABORT`s: it neither completed nor
/// failed, and a consumer that cannot tell those apart cannot tell a user who
/// hit Ctrl-C from a broken query.
fn terminal(error: Option<&AgentError>) -> (EventType, Outcome) {
    match error {
        None => (EventType::Complete, Outcome::Ok),
        Some(e) => match e.envelope().kind {
            latiq_common::ErrorKind::QueryCancelled | latiq_common::ErrorKind::QueryTimeout => {
                (EventType::Abort, Outcome::Cancelled)
            }
            _ => (EventType::Fail, Outcome::Error),
        },
    }
}

/// `subject`/`issuer` are empty strings when the identity is not verified;
/// `null` says "not present" where `""` would read as a real, blank issuer.
fn non_empty(s: &str) -> Option<&str> {
    (!s.is_empty()).then_some(s)
}
