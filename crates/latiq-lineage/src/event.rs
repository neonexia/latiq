//! The OpenLineage `RunEvent` and the facets Latiq emits.
//!
//! Written by hand, not taken from a crate: the only OpenLineage crate on
//! crates.io is 0.0.x, single-source, and does not state which spec version it
//! targets — for a compliance surface that is a worse dependency than 120 lines.
//!
//! Two spec rules drive the design, and both are hard rejection causes in real
//! consumers: **`run.runId` must be a UUID**, and **every facet must carry
//! `_producer` and `_schemaURL`**. The second is why a facet body can only be
//! built through [`Facet::stamp`] — there is no public path that skips it.

use std::collections::BTreeMap;

use serde::{Serialize, Serializer};
use serde_json::{Map, Value};
use uuid::Uuid;

/// URI identifying us as the producer of this metadata. Deliberately carries
/// **no version**: consumers key on `_producer` as an opaque producer identity,
/// so bumping it every release would make each Latiq release look like a
/// different producer. Do not "update" it at release time.
pub const PRODUCER: &str = "https://github.com/neonexia/latiq";

/// The core spec version every event here conforms to. `spec/README.md` records
/// the upstream tag the vendored copy came from.
pub const SCHEMA_URL: &str = "https://openlineage.io/spec/2-0-2/OpenLineage.json#/$defs/RunEvent";

/// One job namespace per deployment, **not** per pond. Marquez treats a
/// namespace as a top-level object with its own lifecycle; ponds are ephemeral,
/// so a namespace per pond would litter every consumer with dead namespaces.
pub const JOB_NAMESPACE: &str = "latiq";

/// Arbitrary but **fixed** — a v5 namespace only has to be constant, and this
/// one must never change: it is what makes a caller's claimed workflow id map
/// to the same `parent.run.runId` on every event, forever. Changing it splits
/// every existing workflow graph in two.
const LATIQ_PARENT_NAMESPACE: Uuid = Uuid::from_u128(0xe2adba64_917e_49e4_9882_278323268fed);

const SPEC_FACETS: &str = "https://openlineage.io/spec/facets";

/// Where our custom facet schemas are identified from. `{version}` is a
/// **facet-schema** version, not a release: it appears in the git ref and in
/// the path, and it changes **only when that facet's fields change**. It must
/// not be bumped at release time — `_schemaURL` identifies a facet's *shape* to
/// a consumer, so floating it with the crate version would make every Latiq
/// release look like a new facet type downstream.
const LATIQ_FACETS: &str = "https://raw.githubusercontent.com/neonexia/latiq/lineage-facets-{version}/crates/latiq-lineage/spec/facets/{version}";

// Each facet versions independently — a change to `latiq_query`'s fields must
// not renumber, and so invalidate, the other three.
const IDENTITY_FACET_VERSION: &str = "1-0-0";
const POND_FACET_VERSION: &str = "1-0-0";
const QUERY_FACET_VERSION: &str = "1-0-0";
const PARENT_CLAIM_FACET_VERSION: &str = "1-0-0";

// ---------------------------------------------------------------- envelope

/// The run-state transition. The spec requires exactly one `START` and one of
/// `COMPLETE`/`ABORT`/`FAIL` per run — a lone `START` leaves a run stuck in
/// `RUNNING` forever in every consumer, so callers emit both.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum EventType {
    Start,
    Running,
    Complete,
    Abort,
    Fail,
    Other,
}

/// A single OpenLineage `RunEvent`.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct RunEvent {
    /// RFC-3339 **with an explicit offset**. A naive local timestamp is invalid.
    pub event_time: String,
    pub producer: &'static str,
    #[serde(rename = "schemaURL")]
    pub schema_url: &'static str,
    pub event_type: EventType,
    pub run: Run,
    pub job: Job,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub inputs: Vec<Dataset>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub outputs: Vec<Dataset>,
}

impl RunEvent {
    /// Stamps `eventTime` with *now*. A START and its terminal event are
    /// therefore two calls, which is what the spec wants — they differ in time,
    /// type and facets, and share only the run id.
    pub fn new(event_type: EventType, run: Run, job: Job) -> Self {
        Self::at(now_rfc3339(), event_type, run, job)
    }

    /// As [`RunEvent::new`], with `event_time` supplied by the caller.
    ///
    /// A producer that only learns an operation happened once it has FINISHED
    /// needs this: stamping both events with *now* would place the START at the
    /// end of the run, so every consumer computing a duration from START →
    /// COMPLETE would report ~0 ms — and disagree with `latiq_query.durationMs`
    /// on the same event pair. Backdate the START instead; see
    /// [`rfc3339_millis_ago`].
    pub fn at(event_time: impl Into<String>, event_type: EventType, run: Run, job: Job) -> Self {
        Self {
            event_time: event_time.into(),
            producer: PRODUCER,
            schema_url: SCHEMA_URL,
            event_type,
            run,
            job,
            inputs: Vec::new(),
            outputs: Vec::new(),
        }
    }

    pub fn with_inputs(mut self, inputs: Vec<Dataset>) -> Self {
        self.inputs = inputs;
        self
    }

    pub fn with_outputs(mut self, outputs: Vec<Dataset>) -> Self {
        self.outputs = outputs;
        self
    }
}

/// One query execution. The id is minted here — provenance a caller can
/// fabricate is worthless — and reused across that query's START and terminal
/// events.
#[derive(Debug, Clone, Serialize)]
pub struct Run {
    #[serde(rename = "runId")]
    run_id: Uuid,
    #[serde(skip_serializing_if = "BTreeMap::is_empty")]
    facets: BTreeMap<&'static str, Value>,
}

impl Run {
    #[allow(clippy::new_without_default)] // minting a random id is not a Default
    pub fn new() -> Self {
        Self {
            run_id: Uuid::new_v4(),
            facets: BTreeMap::new(),
        }
    }

    pub fn run_id(&self) -> Uuid {
        self.run_id
    }

    pub fn with(mut self, facet: Facet) -> Self {
        self.facets.insert(facet.key, facet.body);
        self
    }
}

/// The stable, recurring thing: a pond and what it operates on. Never the run.
#[derive(Debug, Clone, Serialize)]
pub struct Job {
    pub namespace: String,
    pub name: String,
    #[serde(skip_serializing_if = "BTreeMap::is_empty")]
    facets: BTreeMap<&'static str, Value>,
}

impl Job {
    pub fn new(namespace: impl Into<String>, name: impl Into<String>) -> Self {
        Self {
            namespace: namespace.into(),
            name: name.into(),
            facets: BTreeMap::new(),
        }
    }

    pub fn with(mut self, facet: Facet) -> Self {
        self.facets.insert(facet.key, facet.body);
        self
    }
}

/// An input or output dataset. Both sides use the same shape; the spec's
/// `inputFacets`/`outputFacets` are not emitted (nothing we have belongs there).
#[derive(Debug, Clone, Serialize)]
pub struct Dataset {
    pub namespace: String,
    pub name: String,
    #[serde(skip_serializing_if = "BTreeMap::is_empty")]
    facets: BTreeMap<&'static str, Value>,
}

impl Dataset {
    pub fn new(namespace: impl Into<String>, name: impl Into<String>) -> Self {
        Self {
            namespace: namespace.into(),
            name: name.into(),
            facets: BTreeMap::new(),
        }
    }

    pub fn with(mut self, facet: Facet) -> Self {
        self.facets.insert(facet.key, facet.body);
        self
    }
}

// ------------------------------------------------------------------ facets

/// A facet body plus the key it must hang under. The key travels with the body
/// so a caller cannot file `SQLJobFacet` under `parent`.
#[derive(Debug, Clone)]
pub struct Facet {
    key: &'static str,
    body: Value,
}

impl Facet {
    /// The only way to build a facet: serialize the body and stamp the two
    /// mandatory base fields onto it. Private, so every facet in the crate goes
    /// through it and none can be emitted without them.
    fn stamp<T: Serialize>(key: &'static str, schema_url: String, body: &T) -> Self {
        let mut object = match serde_json::to_value(body) {
            Ok(Value::Object(map)) => map,
            // Unreachable for the structs below (all serialize to objects), and
            // not worth an error type on a hot path: an empty body still
            // carries the two required fields, so the event stays valid.
            other => {
                tracing::warn!(
                    facet = key,
                    ?other,
                    "lineage facet did not serialize to an object"
                );
                Map::new()
            }
        };
        object.insert("_producer".into(), Value::String(PRODUCER.into()));
        object.insert("_schemaURL".into(), Value::String(schema_url));
        Self {
            key,
            body: Value::Object(object),
        }
    }

    pub fn key(&self) -> &'static str {
        self.key
    }
}

impl Serialize for Facet {
    /// Serializes as the bare body, so a test (or a sink) can look at a facet
    /// without reconstructing the event around it.
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        self.body.serialize(serializer)
    }
}

/// What a `latiq_query` facet's `durationMs` actually measured. A streaming
/// read has no completion time at emission — both its events fire when the
/// stream is established — and silently calling that "duration" would be a lie.
#[derive(Debug, Clone, Copy, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum DurationMeaning {
    Completion,
    Establishment,
}

/// How the operation ended.
#[derive(Debug, Clone, Copy, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum Outcome {
    Ok,
    Error,
    Cancelled,
}

/// The caller's opaque workflow labels. **Claimed, never verified** — only the
/// agent environment knows what a workflow is, so we record what it says and
/// never let it carry authority.
#[derive(Debug, Clone)]
pub struct ParentClaim {
    pub workflow_id: String,
    pub step_id: Option<String>,
}

/// The parent run id for a claimed workflow: a UUIDv5 under a fixed Latiq
/// namespace. The spec requires `parent.run.runId` to be a UUID and callers
/// supply arbitrary strings; deriving keeps the same workflow pointing at the
/// same parent across processes and restarts, where a minted id would not.
pub fn parent_run_id(workflow_id: &str) -> Uuid {
    Uuid::new_v5(&LATIQ_PARENT_NAMESPACE, workflow_id.as_bytes())
}

/// Facet constructors. One per facet we emit; standard facets wherever the spec
/// has one, so a consumer that has never heard of Latiq still gets the SQL, the
/// parent, the dataset version and the error.
pub mod facets {
    use super::*;

    fn spec_url(version: &str, name: &str) -> String {
        format!("{SPEC_FACETS}/{version}/{name}.json#/$defs/{name}")
    }

    fn latiq_url(name: &str, version: &str) -> String {
        format!(
            "{}/{name}.json#/$defs/{name}",
            LATIQ_FACETS.replace("{version}", version)
        )
    }

    /// `ParentRunFacet` — the claimed workflow, as a derived UUID plus a job
    /// reference. The raw claim rides in [`parent_claim`] beside it.
    pub fn parent(claim: &ParentClaim, job_namespace: &str, job_name: &str) -> Facet {
        Facet::stamp(
            "parent",
            spec_url("1-0-1", "ParentRunFacet"),
            &serde_json::json!({
                "run": { "runId": parent_run_id(&claim.workflow_id) },
                "job": { "namespace": job_namespace, "name": job_name },
            }),
        )
    }

    /// `latiq_parent_claim` — the caller's strings verbatim. Emitted alongside
    /// `parent` because the derived UUID is lossy and provenance must never
    /// silently fabricate: a reader can see exactly what was claimed.
    pub fn parent_claim(claim: &ParentClaim) -> Facet {
        Facet::stamp(
            "latiq_parent_claim",
            latiq_url("LatiqParentClaimFacet", PARENT_CLAIM_FACET_VERSION),
            &serde_json::json!({
                "workflowId": claim.workflow_id,
                "stepId": claim.step_id,
                "verified": false,
            }),
        )
    }

    /// `SQLJobFacet`. The query must already be redacted by the caller — this
    /// crate does not inspect SQL. `dialect` is not in the 1-0-1 schema but is
    /// permitted (facets allow additional properties) and tells a consumer how
    /// to parse what it is looking at.
    pub fn sql(query: &str) -> Facet {
        Facet::stamp(
            "sql",
            spec_url("1-0-1", "SQLJobFacet"),
            &serde_json::json!({ "query": query, "dialect": "duckdb" }),
        )
    }

    /// `DatasetVersionDatasetFacet` — the DuckLake snapshot id. The spec types
    /// `datasetVersion` as a **string**, so the `i64` is stringified; there is
    /// no top-level snapshot field to put it in.
    pub fn dataset_version(snapshot_id: i64) -> Facet {
        Facet::stamp(
            "version",
            spec_url("1-0-1", "DatasetVersionDatasetFacet"),
            &serde_json::json!({ "datasetVersion": snapshot_id.to_string() }),
        )
    }

    /// `ErrorMessageRunFacet`. `programmingLanguage` is required by the schema.
    pub fn error_message(message: &str) -> Facet {
        Facet::stamp(
            "errorMessage",
            spec_url("1-0-1", "ErrorMessageRunFacet"),
            &serde_json::json!({ "message": message, "programmingLanguage": "RUST" }),
        )
    }

    /// `JobTypeJobFacet`. `processingType` and `integration` are required;
    /// `jobType` is the Latiq operation class, e.g. `QUERY`.
    pub fn job_type(job_type: &str) -> Facet {
        Facet::stamp(
            "jobType",
            spec_url("2-0-3", "JobTypeJobFacet"),
            &serde_json::json!({
                "processingType": "BATCH",
                "integration": "LATIQ",
                "jobType": job_type,
            }),
        )
    }

    /// `ProcessingEngineRunFacet` — the DuckDB version that executed the query.
    pub fn processing_engine(duckdb_version: &str) -> Facet {
        Facet::stamp(
            "processing_engine",
            spec_url("1-1-1", "ProcessingEngineRunFacet"),
            &serde_json::json!({
                "name": "duckdb",
                "version": duckdb_version,
                "openlineageAdapterVersion": env!("CARGO_PKG_VERSION"),
            }),
        )
    }

    /// `latiq_identity` — who Latiq attributed the run to. `agentIdVerified` is
    /// hard-coded false and is **not** a parameter: the agent id is a claimed
    /// leaf that rides an HTTP header, and authority only ever comes from the
    /// verified subject. Making it settable would invite a caller to assert it.
    pub fn identity(
        verified: bool,
        subject: &str,
        issuer: Option<&str>,
        agent_id: Option<&str>,
    ) -> Facet {
        Facet::stamp(
            "latiq_identity",
            latiq_url("LatiqIdentityFacet", IDENTITY_FACET_VERSION),
            &serde_json::json!({
                "verified": verified,
                "subject": subject,
                "issuer": issuer,
                "agentId": agent_id,
                "agentIdVerified": false,
            }),
        )
    }

    /// `latiq_pond` — on the **job**, because the pond is part of what makes a
    /// job the recurring thing it is.
    pub fn pond(pond_id: &str, pond_name: &str, node_id: &str) -> Facet {
        Facet::stamp(
            "latiq_pond",
            latiq_url("LatiqPondFacet", POND_FACET_VERSION),
            &serde_json::json!({ "pondId": pond_id, "pondName": pond_name, "nodeId": node_id }),
        )
    }

    /// `latiq_query` — the operation, how it ended, and how long it took.
    pub fn query(
        op: &str,
        outcome: Outcome,
        duration_ms: u64,
        duration_meaning: DurationMeaning,
        trace_id: Option<&str>,
    ) -> Facet {
        Facet::stamp(
            "latiq_query",
            latiq_url("LatiqQueryFacet", QUERY_FACET_VERSION),
            &serde_json::json!({
                "op": op,
                "outcome": outcome,
                "durationMs": duration_ms,
                "durationMeaning": duration_meaning,
                "traceId": trace_id,
            }),
        )
    }
}

// ----------------------------------------------------------- our namings

/// The dataset namespace for a pond's own tables. Our convention — the spec is
/// silent on DuckDB/DuckLake. It is the *pond id*, not the name, because names
/// are mutable and a namespace is how a consumer joins across events.
///
/// External sources keep their **standard** schemes (`s3://bucket`, `file`)
/// untouched: those are the identifiers another tool's lineage joins on, and
/// rewriting them would make Latiq's events unjoinable.
pub fn dataset_namespace(pond_id: &str) -> String {
    format!("ducklake://{pond_id}")
}

/// The job name. Our convention, again because the spec is silent: a job must
/// **recur across runs**, so this may never contain the run id, a timestamp or
/// the SQL — those change every execution and would make every run its own job.
///
/// `{pond}.{op}.{target}` when a dataset resolves (the written table, or the
/// first input read), `{pond}.{op}` when nothing does.
pub fn job_name(pond_name: &str, op: &str, target: Option<&str>) -> String {
    match target {
        Some(target) => format!("{pond_name}.{op}.{target}"),
        None => format!("{pond_name}.{op}"),
    }
}

/// RFC-3339 in UTC with millisecond precision and an explicit `Z`.
fn now_rfc3339() -> String {
    chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Millis, true)
}

/// An `eventTime` `millis` milliseconds in the past, in the same format as
/// *now*. This is how a caller that measured an operation's duration recovers
/// the moment it STARTED — the wall clock is sampled once, here, so the two
/// events of a run cannot be stamped from two different reads of it.
pub fn rfc3339_millis_ago(millis: u64) -> String {
    let now = chrono::Utc::now();
    let started = now
        .checked_sub_signed(chrono::TimeDelta::milliseconds(millis as i64))
        // Only reachable for an absurd duration; `now` is a truthful fallback
        // and still a valid timestamp, which is what compliance requires.
        .unwrap_or(now);
    started.to_rfc3339_opts(chrono::SecondsFormat::Millis, true)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn event_time_is_rfc3339_with_an_explicit_offset() {
        // A naive local timestamp is invalid OpenLineage, and the mistake is
        // invisible until a consumer rejects the event.
        let now = now_rfc3339();
        assert!(
            now.ends_with('Z'),
            "expected an explicit UTC offset, got {now}"
        );
        chrono::DateTime::parse_from_rfc3339(&now).expect("parses as RFC-3339");
    }

    #[test]
    fn a_backdated_event_time_is_still_rfc3339_and_really_earlier() {
        // The START of a run is stamped by subtracting the measured duration:
        // if that produced a naive or a same-instant timestamp, every consumer
        // would report a zero-length run for a query that took a minute.
        let started = rfc3339_millis_ago(5_000);
        assert!(
            started.ends_with('Z'),
            "expected an explicit UTC offset, got {started}"
        );
        let parsed = chrono::DateTime::parse_from_rfc3339(&started).expect("parses as RFC-3339");
        let delta = chrono::Utc::now()
            .signed_duration_since(parsed)
            .num_milliseconds();
        assert!(
            (4_900..=6_000).contains(&delta),
            "expected ~5s in the past, got {delta}ms"
        );
    }

    #[test]
    fn job_name_never_carries_per_run_detail() {
        assert_eq!(
            job_name("orders", "write_query", Some("main.orders")),
            "orders.write_query.main.orders"
        );
        assert_eq!(job_name("orders", "read_query", None), "orders.read_query");
    }
}
