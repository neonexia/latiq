// Copyright 2026 Neonexia
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

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
//!
//! Datasets come from `QueryMeta.inputs`/`outputs`, which the engine fills from
//! its bound plan. An operation whose plan did not resolve carries no datasets
//! rather than guessed ones: an invented input is worse than a missing one.
use latiq_common::{DatasetRef, Identity, QueryMeta};
use latiq_lineage::event::{
    dataset_namespace, facets, job_name, rfc3339_millis_ago, DurationMeaning, EventType, Outcome,
    JOB_NAMESPACE,
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
    /// The version of the engine that ran it, e.g. `v1.5.3`. Read from the
    /// engine (this crate is engine-neutral and cannot know it), and passed in
    /// rather than looked up so the emitter stays a pure function of the record.
    pub engine_version: &'a str,
}

/// Buffer this operation's `START` and terminal event. Both go in one call so a
/// reader never finds one without the other in the directory.
///
/// Buffering only — **no filesystem access happens here**. Returns whether a
/// batch has come due, which the caller answers by flushing on a blocking pool:
/// the fsync belongs anywhere except the async worker the query ran on.
#[must_use = "a due batch must be flushed, or events sit in memory until shutdown"]
pub(crate) fn record(writer: &LineageWriter, node_id: &str, rec: QueryRecord<'_>) -> bool {
    let info = rec.info;

    // What the statement read and what it wrote are two different edges of the
    // graph, and the engine's plan tells them apart: an `INSERT INTO a SELECT
    // FROM b` that reported one flat list would make `b` look written.
    let empty: &[DatasetRef] = &[];
    let (inputs, outputs) = match rec.meta {
        Some(m) => (m.inputs.as_slice(), m.outputs.as_slice()),
        None => (empty, empty),
    };
    // A write's outputs have no version in the plan — the snapshot they got is
    // the one the write COMMITTED, which only the result knows.
    let produced = rec.meta.and_then(|m| m.snapshot_id);
    let to_datasets = |refs: &[DatasetRef], fallback: Option<i64>| -> Vec<Dataset> {
        refs.iter()
            .map(|r| {
                // Only a pond's own tables get the pond namespace; an external
                // source keeps the standard scheme it arrived with, because
                // that is what another tool's lineage joins on.
                let namespace = r
                    .namespace
                    .clone()
                    .unwrap_or_else(|| dataset_namespace(&info.pond_id));
                let mut ds = Dataset::new(namespace, r.name.clone());
                // The DuckLake snapshot rides the standard dataset-version
                // facet; there is no top-level snapshot field in the spec.
                if let Some(id) = r.version.or(fallback) {
                    ds = ds.with(facets::dataset_version(id));
                }
                // The columns, on the standard schema facet, when the engine
                // had them cheaply — a pond table. An external dataset's are
                // empty and stay absent: a dataset with no `fields` reads as
                // "not stated", and a guessed one would read as fact.
                if !r.fields.is_empty() {
                    ds = ds.with(facets::schema(
                        r.fields
                            .iter()
                            .map(|f| (f.name.as_str(), f.type_name.as_str())),
                    ));
                }
                ds
            })
            .collect()
    };
    let input_datasets = to_datasets(inputs, None);
    let output_datasets = to_datasets(outputs, produced);

    // Built ONCE and cloned: the job is identical on both events of a run, and
    // rebuilding it would re-run `redact_sql` (a full char-by-char rescan) and
    // re-serialize three facet bodies for one query.
    //
    // The job's target is what the operation produced, or failing that what it
    // read: a write's job is about the table it writes, not the one it scans.
    let target = outputs.first().or(inputs.first()).map(|d| d.name.as_str());
    let job = Job::new(JOB_NAMESPACE, job_name(&info.name, rec.op, target))
        .with(facets::sql(&crate::ops::redact_sql(rec.sql)))
        .with(facets::job_type("QUERY"))
        .with(facets::pond(&info.pond_id, &info.name, node_id));

    // The run id is minted once and shared: a START and its terminal event are
    // one run, and a consumer joins them on nothing else.
    let base = Run::new()
        .with(facets::identity(
            rec.identity.verified,
            &rec.identity.subject,
            non_empty(&rec.identity.issuer),
            // CLAIMED, and stamped `agentIdVerified: false` by `latiq-lineage`.
            Some(rec.identity.agent_id.as_str()),
        ))
        // WHAT ran the query, read from the engine itself: a consumer comparing
        // two runs of the same job needs to know the engine changed under them.
        .with(facets::processing_engine(rec.engine_version));

    // The START is stamped with when the operation BEGAN, not with now. Both
    // events are built here, after the op finished, so a `now` on the START
    // would put the beginning of the run at its end — every consumer that
    // derives a duration from START -> terminal (which is why the spec wants
    // the pair at all) would report ~0 ms, and contradict `latiq_query`'s
    // `durationMs` on the very same events.
    //
    // It carries inputs only: the outputs do not exist yet, and a version facet
    // on them would name the snapshot BEFORE the write.
    let start = RunEvent::at(
        rfc3339_millis_ago(rec.duration_ms),
        EventType::Start,
        base.clone(),
        job.clone(),
    )
    .with_inputs(input_datasets.clone());

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
    // The terminal event carries both sides: an `INSERT … SELECT` read one
    // dataset and wrote another, and an edge needs both ends.
    let terminal_event = RunEvent::new(event_type, run, job)
        .with_inputs(input_datasets)
        .with_outputs(output_datasets);

    writer.buffer_all(&[start, terminal_event])
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
