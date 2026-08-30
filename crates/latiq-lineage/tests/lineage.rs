//! What this crate has to get right: the events are *really* OpenLineage (a
//! consumer we have never seen must accept them), and the writer never lets
//! lineage hurt a query — it cannot tear a file, cannot escape the pond
//! directory, cannot grow without bound, and cannot fail upwards.
//!
//! One binary on purpose: these tests share the fixture event and the schema
//! registry, and neither half needs a subscriber of its own.

use std::collections::BTreeSet;
use std::fs;
use std::path::Path;

use latiq_lineage::event::{facets, DurationMeaning, EventType, Outcome};
use latiq_lineage::{Dataset, Job, LineageWriter, ParentClaim, Run, RunEvent};
use serde_json::{json, Value};

// ---------------------------------------------------------------- fixtures

const WORKFLOW: &str = "orders-refresh/nightly";
const POND: &str = "pond-8812";

fn parent_claim() -> ParentClaim {
    ParentClaim {
        workflow_id: WORKFLOW.to_string(),
        step_id: Some("step-3".to_string()),
    }
}

fn run_facets(run: Run, outcome: Outcome, error: Option<&str>) -> Run {
    let claim = parent_claim();
    let run = run
        .with(facets::parent(
            &claim,
            latiq_lineage::event::JOB_NAMESPACE,
            "orders.write",
        ))
        .with(facets::parent_claim(&claim))
        .with(facets::identity(
            true,
            "svc-agent@example.com",
            Some("https://issuer.example.com/"),
            Some("planner-7"),
        ))
        .with(facets::processing_engine("1.4.0"))
        .with(facets::query(
            "write_query",
            outcome,
            42,
            DurationMeaning::Completion,
            Some("trace-abc"),
        ));
    match error {
        Some(msg) => run.with(facets::error_message(msg)),
        None => run,
    }
}

fn job() -> Job {
    Job::new(
        latiq_lineage::event::JOB_NAMESPACE,
        latiq_lineage::event::job_name("orders", "write_query", Some("main.orders")),
    )
    .with(facets::sql("INSERT INTO orders VALUES (?)"))
    .with(facets::job_type("QUERY"))
    .with(facets::pond(POND, "orders", "node-1"))
}

fn dataset(name: &str, snapshot: i64) -> Dataset {
    Dataset::new(latiq_lineage::event::dataset_namespace(POND), name)
        .with(facets::dataset_version(snapshot))
}

/// A write that succeeded: every facet this crate can emit except the error one.
fn write_event() -> RunEvent {
    RunEvent::new(
        EventType::Complete,
        run_facets(Run::new(), Outcome::Ok, None),
        job(),
    )
    .with_inputs(vec![dataset("orders.main.staging", 11)])
    .with_outputs(vec![dataset("orders.main.orders", 12)])
}

/// A read: an input, no output, and an external source under its own scheme.
fn read_event() -> RunEvent {
    RunEvent::new(
        EventType::Start,
        run_facets(Run::new(), Outcome::Ok, None),
        job(),
    )
    .with_inputs(vec![
        dataset("orders.main.orders", 12),
        Dataset::new("s3://bucket", "prefix/part.parquet"),
    ])
}

/// A failed write. It still resolved its datasets, so this is the one fixture
/// that carries every facet the crate can emit at once.
fn fail_event() -> RunEvent {
    RunEvent::new(
        EventType::Fail,
        run_facets(
            Run::new(),
            Outcome::Error,
            Some("Table with name x does not exist"),
        ),
        job(),
    )
    .with_inputs(vec![dataset("orders.main.staging", 11)])
    .with_outputs(vec![dataset("orders.main.orders", 11)])
}

// ------------------------------------------------------------ schema setup

const CORE_URI: &str = "https://openlineage.io/spec/2-0-2/OpenLineage.json";
const CORE: &str = include_str!("../spec/OpenLineage-2-0-2.json");

/// Every facet key this crate emits, paired with the schema that defines it.
/// Adding a facet to `event.rs` without vendoring its schema fails
/// `lineage_every_facet_carries_producer_and_schema_url`'s completeness check.
fn facet_schemas() -> Vec<(&'static str, &'static str)> {
    vec![
        (
            "parent",
            include_str!("../spec/facets/ParentRunFacet-1-0-1.json"),
        ),
        ("sql", include_str!("../spec/facets/SQLJobFacet-1-0-1.json")),
        (
            "version",
            include_str!("../spec/facets/DatasetVersionDatasetFacet-1-0-1.json"),
        ),
        (
            "errorMessage",
            include_str!("../spec/facets/ErrorMessageRunFacet-1-0-1.json"),
        ),
        (
            "jobType",
            include_str!("../spec/facets/JobTypeJobFacet-2-0-3.json"),
        ),
        (
            "processing_engine",
            include_str!("../spec/facets/ProcessingEngineRunFacet-1-1-1.json"),
        ),
        (
            "latiq_identity",
            include_str!("../spec/facets/1-0-0/LatiqIdentityFacet.json"),
        ),
        (
            "latiq_pond",
            include_str!("../spec/facets/1-0-0/LatiqPondFacet.json"),
        ),
        (
            "latiq_query",
            include_str!("../spec/facets/1-0-0/LatiqQueryFacet.json"),
        ),
        (
            "latiq_parent_claim",
            include_str!("../spec/facets/1-0-0/LatiqParentClaimFacet.json"),
        ),
    ]
}

/// The vendored core spec, registered under the absolute URI the facet schemas
/// `$ref`. `jsonschema` is built without its HTTP retriever, so an unregistered
/// reference fails the test instead of quietly reaching the network.
fn core_registry() -> jsonschema::Registry<'static> {
    let core: Value = serde_json::from_str(CORE).expect("vendored core schema parses");
    jsonschema::Registry::new()
        .add(CORE_URI, jsonschema::Resource::from_contents(core))
        .expect("core schema URI is valid")
        .prepare()
        .expect("registry builds")
}

/// Formats are checked: `runId`'s `uuid` and `eventTime`'s `date-time` are two
/// of the constraints most likely to be got wrong, and both are format-only.
fn validator(registry: &jsonschema::Registry<'_>, schema: Value) -> jsonschema::Validator {
    jsonschema::options()
        .should_validate_formats(true)
        .with_registry(registry)
        .build(&schema)
        .expect("schema compiles")
}

fn assert_valid(v: &jsonschema::Validator, instance: &Value, what: &str) {
    let errors: Vec<String> = v.iter_errors(instance).map(|e| format!("{e}")).collect();
    assert!(
        errors.is_empty(),
        "{what} is not valid OpenLineage: {errors:?}\ninstance: {instance:#}"
    );
}

/// Every facet in the event, as (owner, key, payload). Owner names where it
/// hung so a failure says which side of the event is wrong.
fn all_facets(event: &Value) -> Vec<(String, String, Value)> {
    let mut out = Vec::new();
    let mut collect = |owner: &str, holder: &Value| {
        if let Some(map) = holder.get("facets").and_then(Value::as_object) {
            for (k, v) in map {
                out.push((owner.to_string(), k.clone(), v.clone()));
            }
        }
    };
    collect("run", &event["run"]);
    collect("job", &event["job"]);
    for (side, key) in [("input", "inputs"), ("output", "outputs")] {
        for ds in event[key].as_array().unwrap_or(&Vec::new()) {
            collect(side, ds);
        }
    }
    out
}

// -------------------------------------------------------------- the tests

#[test]
fn lineage_event_validates_against_the_vendored_schema() {
    // The whole point of compliance: a RunEvent must satisfy the real spec, not
    // our reading of it. Envelope and facets are validated separately rather
    // than by resolving cross-file $refs, because the core schema constrains
    // facets only as additionalProperties -> BaseFacet, so the envelope stands
    // alone and each facet's own schema is the only thing that checks its body.
    let registry = core_registry();
    let envelope = validator(
        &registry,
        json!({ "$ref": format!("{CORE_URI}#/$defs/RunEvent") }),
    );

    let mut seen_facets = BTreeSet::new();
    for (what, event) in [
        ("write COMPLETE", write_event()),
        ("read START", read_event()),
        ("FAIL", fail_event()),
    ] {
        let value = serde_json::to_value(&event).expect("event serializes");
        assert_valid(&envelope, &value, what);

        for (owner, key, payload) in all_facets(&value) {
            let (_, schema) = facet_schemas()
                .into_iter()
                .find(|(k, _)| *k == key)
                .unwrap_or_else(|| panic!("facet `{key}` on {owner} has no vendored schema"));
            let v = validator(
                &registry,
                serde_json::from_str(schema).expect("facet schema parses"),
            );
            // Facet files are keyed by the facet name, e.g. {"sql": {...}}.
            assert_valid(
                &v,
                &json!({ key.clone(): payload }),
                &format!("{owner} facet `{key}`"),
            );
            seen_facets.insert(key);
        }
    }

    // Anti-vacuity: had the events carried no facets, every loop above would
    // have passed without validating anything.
    let expected: BTreeSet<String> = facet_schemas()
        .into_iter()
        .map(|(k, _)| k.to_string())
        .collect();
    assert_eq!(
        seen_facets, expected,
        "the fixtures must exercise every facet this crate emits"
    );
}

#[test]
fn lineage_every_facet_carries_producer_and_schema_url() {
    // Missing `_producer`/`_schemaURL` is one of only two hard rejection causes.
    // The schema test alone cannot catch it, because a facet we forget to stamp
    // would more likely be absent (and so silently valid) than present-and-wrong.
    let value = serde_json::to_value(fail_event()).expect("event serializes");
    let facets = all_facets(&value);
    assert!(
        facets.len() >= 10,
        "expected the fully-populated event to carry every facet; got {:?}",
        facets
            .iter()
            .map(|(o, k, _)| format!("{o}.{k}"))
            .collect::<Vec<_>>()
    );
    for (owner, key, payload) in facets {
        for field in ["_producer", "_schemaURL"] {
            let uri = payload
                .get(field)
                .and_then(Value::as_str)
                .unwrap_or_else(|| panic!("{owner} facet `{key}` is missing {field}: {payload:#}"));
            assert!(
                uri.starts_with("https://"),
                "{owner} facet `{key}` {field} must be an absolute URI, got {uri:?}"
            );
        }
    }
}

#[test]
fn lineage_claimed_workflow_id_becomes_a_stable_uuid() {
    // parent.run.runId must be a UUID; callers hand us opaque strings. The
    // mapping has to be stable, or the same workflow becomes a different parent
    // on every event and the graph never assembles.
    let claim = parent_claim();
    let first = serde_json::to_value(facets::parent(&claim, "latiq", "orders.write")).unwrap();
    let second = serde_json::to_value(facets::parent(&claim, "latiq", "orders.write")).unwrap();
    let derived = first["run"]["runId"]
        .as_str()
        .expect("parent runId is a string");
    assert_eq!(
        derived, second["run"]["runId"],
        "the same claim must map to the same UUID"
    );
    assert_eq!(
        derived,
        latiq_lineage::event::parent_run_id(WORKFLOW).to_string(),
        "the derivation must be the published one"
    );
    assert_ne!(
        derived,
        latiq_lineage::event::parent_run_id("orders-refresh/hourly").to_string(),
        "distinct workflows must not collide"
    );

    // The raw string is provenance, not a hash: it survives verbatim.
    let claimed = serde_json::to_value(facets::parent_claim(&claim)).unwrap();
    assert_eq!(claimed["workflowId"], json!(WORKFLOW));
    assert_eq!(claimed["stepId"], json!("step-3"));
    assert_eq!(
        claimed["verified"],
        json!(false),
        "a parent is claimed, never verified"
    );
}

// ------------------------------------------------------------- the writer

fn jsonl_files(dir: &Path) -> Vec<String> {
    let mut names: Vec<String> = fs::read_dir(dir)
        .expect("lineage dir readable")
        .map(|e| {
            e.expect("dir entry")
                .file_name()
                .to_string_lossy()
                .into_owned()
        })
        .collect();
    names.sort();
    names
}

fn events_in(dir: &Path) -> Vec<Value> {
    let mut out = Vec::new();
    for name in jsonl_files(dir) {
        let body = fs::read_to_string(dir.join(&name)).expect("event file readable");
        for line in body.lines() {
            out.push(
                serde_json::from_str(line)
                    .unwrap_or_else(|e| panic!("torn record in {name}: {e} in line {line:?}")),
            );
        }
    }
    out
}

#[test]
fn lineage_writer_batches_and_renames_into_place() {
    // A reader (the MCP tool in task 6) globs this directory while the writer is
    // running, so it must never observe a half-written file — hence write-then-
    // rename, and hence no temp file may survive a flush. The names must also
    // sort chronologically, which is the only reason the millis prefix exists.
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("lineage");
    fs::create_dir(&path).expect("lineage dir");

    let writer = LineageWriter::with_limits(path.to_str().unwrap(), 2, 64);
    writer.record(&write_event());
    assert!(
        jsonl_files(&path).is_empty(),
        "below the batch size nothing should have been written yet"
    );

    writer.record(&read_event());
    let first_batch = jsonl_files(&path);
    assert_eq!(
        first_batch.len(),
        1,
        "the batch should have landed as one file"
    );

    writer.record_all(&[fail_event(), write_event()]);
    let names = jsonl_files(&path);
    assert_eq!(
        names.len(),
        2,
        "the second batch should be a second file: {names:?}"
    );
    assert!(
        names.iter().all(|n| n.ends_with(".jsonl")),
        "no temp file may remain after a flush: {names:?}"
    );
    assert!(
        names[0] == first_batch[0],
        "sorting names must put the older batch first, got {names:?}"
    );
    let millis: Vec<u64> = names
        .iter()
        .map(|n| n.split('-').next().unwrap().parse().expect("millis prefix"))
        .collect();
    assert!(
        millis[0] <= millis[1] && millis[0] > 1_700_000_000_000,
        "names must start with a real unix-millis prefix: {names:?}"
    );

    let events = events_in(&path);
    assert_eq!(
        events.len(),
        4,
        "every recorded event must be readable back"
    );
    assert_eq!(
        events[1]["eventType"],
        json!("START"),
        "records must keep their order within the batch"
    );
}

#[test]
fn lineage_writer_buffering_alone_never_touches_the_filesystem() {
    // `buffer_all` exists so an async caller can keep the fsync off its worker
    // thread: it must therefore do NO io itself, and it must report when a
    // batch is due so the caller knows to flush somewhere it is allowed to
    // block. Both halves matter -- a `buffer_all` that never said "due" would
    // be silently correct here and lose every event in production.
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().to_path_buf();
    let writer = LineageWriter::with_limits(path.to_str().unwrap(), 2, 64);

    assert!(
        !writer.buffer_all(&[write_event()]),
        "one event is below the batch size"
    );
    assert!(
        writer.buffer_all(&[read_event()]),
        "the second event brings a batch due"
    );
    assert!(
        jsonl_files(&path).is_empty(),
        "buffering must write nothing at all, due batch or not: {:?}",
        jsonl_files(&path)
    );

    writer.flush();
    assert_eq!(
        events_in(&path).len(),
        2,
        "everything buffered lands once the caller flushes"
    );
}

#[test]
fn lineage_writer_retries_a_batch_that_failed_to_write() {
    // Regression pin (c9b5c4d): a failed batch was DISCARDED, so a transient
    // write error cost events outright — and with batch 64 < cap 10 000 the
    // buffer could never fill, making the documented drop-oldest policy
    // unreachable.
    // A failed batch goes back in the buffer instead of being discarded, so a
    // transient failure (a brief ENOSPC, an EIO) costs latency rather than
    // events — and so the capacity bound is guarding something real rather
    // than a buffer that can never fill.
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("not-created-yet");
    let writer = LineageWriter::with_limits(path.to_str().unwrap(), 1000, 64);

    writer.record(&write_event());
    writer.record(&read_event());
    writer.flush(); // fails: the directory does not exist
    assert!(!path.exists(), "nothing can have been written yet");

    fs::create_dir(&path).expect("lineage dir appears");
    writer.flush();

    let events = events_in(&path);
    assert_eq!(
        events.len(),
        2,
        "both buffered events must survive the failed attempt and land on the retry"
    );
    assert_eq!(
        events
            .iter()
            .map(|e| e["eventType"].as_str().expect("eventType"))
            .collect::<Vec<_>>(),
        vec!["COMPLETE", "START"],
        "a requeued batch must keep its order, not reverse or interleave"
    );
}

#[test]
fn lineage_writer_flushes_on_drop() {
    // Events recorded before a shutdown are exactly the ones an incident
    // investigation wants; a batch-size-only flush would lose them.
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().to_path_buf();
    {
        let writer = LineageWriter::with_limits(path.to_str().unwrap(), 1000, 1000);
        writer.record(&write_event());
        assert!(
            jsonl_files(&path).is_empty(),
            "still far below the batch size"
        );
    }
    let events = events_in(&path);
    assert_eq!(events.len(), 1, "the buffered event must land on drop");
    assert_eq!(events[0]["eventType"], json!("COMPLETE"));
}

#[test]
fn lineage_writer_never_propagates_a_failure() {
    // Emission must never fail a query, so an unwritable directory has to be a
    // warn-and-drop. record()/flush() return () precisely so this cannot be
    // ignored by accident; what is proven here is that they do not panic.
    //
    // The unwritable path is a directory *under a regular file*: unlike a
    // chmod'd directory it fails with ENOTDIR for root too, so the test cannot
    // quietly stop proving anything when CI runs as root in a container.
    let dir = tempfile::tempdir().expect("tempdir");
    let blocker = dir.path().join("not-a-dir");
    fs::write(&blocker, b"").expect("blocker file");
    let path = blocker.join("lineage");

    let writer = LineageWriter::with_limits(path.to_str().unwrap(), 1, 64);
    writer.record(&write_event());
    writer.flush();
    drop(writer);

    assert!(
        !path.exists(),
        "the write should have failed — if it succeeded this test proves nothing"
    );
}

#[test]
fn lineage_writer_refuses_a_directory_outside_the_pond() {
    // A PondLocation from a producer that predates the lineage_dir field
    // deserializes with lineage_dir: "". Joining that with a filename would
    // scatter events across the process CWD, outside any pond and outside
    // drop_pond's reach, so the constructor refuses anything not absolute.
    let cwd_before: BTreeSet<_> = fs::read_dir(".")
        .expect("cwd readable")
        .map(|e| e.expect("entry").file_name())
        .collect();

    for bad in ["", "lineage", "./lineage"] {
        let writer = LineageWriter::with_limits(bad, 1, 64);
        assert!(
            !writer.is_enabled(),
            "{bad:?} must not be accepted as a lineage dir"
        );
        writer.record(&write_event());
        writer.flush();
    }
    let cwd_after: BTreeSet<_> = fs::read_dir(".")
        .expect("cwd readable")
        .map(|e| e.expect("entry").file_name())
        .collect();
    assert_eq!(
        cwd_before, cwd_after,
        "a rejected writer must not touch the filesystem"
    );

    // Anti-vacuity: the same events through an absolute dir do land, so the
    // rejections above are about the path and not about a writer that is inert.
    let dir = tempfile::tempdir().expect("tempdir");
    let good = LineageWriter::with_limits(dir.path().to_str().unwrap(), 1, 64);
    assert!(good.is_enabled());
    good.record(&write_event());
    assert_eq!(events_in(dir.path()).len(), 1);
}

#[test]
fn lineage_writer_bounds_its_buffer() {
    // If flushing keeps failing the buffer would grow without bound, and
    // lineage must never be able to OOM a node. Oldest is dropped: the events
    // nearest the failure are the ones worth keeping.
    let dir = tempfile::tempdir().expect("tempdir");
    // A batch size above the cap means nothing ever flushes on its own, which is
    // the shape of the failure this bound exists for.
    let writer = LineageWriter::with_limits(dir.path().to_str().unwrap(), 1000, 3);
    for i in 0..10 {
        let mut event = write_event();
        event.job.name = format!("job-{i}");
        writer.record(&event);
    }
    writer.flush();

    let events = events_in(dir.path());
    assert_eq!(events.len(), 3, "the buffer must hold at most its capacity");
    let names: Vec<&str> = events
        .iter()
        .map(|e| e["job"]["name"].as_str().unwrap())
        .collect();
    assert_eq!(
        names,
        vec!["job-7", "job-8", "job-9"],
        "the newest events must be the survivors"
    );
}
