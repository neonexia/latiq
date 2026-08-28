//! AgentOps end-to-end over the in-process ControlPlane (Registry) + DuckEngine
//! + TempFs storage. Exercises the full agent loop through the public ops.
use latiq_agent_core::{AgentConfig, AgentOps, RegistryControlPlane};
use latiq_common::Identity;
use latiq_control_plane::Registry;
use latiq_engine_duckdb::DuckEngine;
use latiq_storage::TempFs;
use std::sync::Arc;

fn ops() -> AgentOps {
    let registry = Registry::open(None).unwrap();
    // A pond node must exist for the control plane to place ponds.
    registry
        .register_node(
            "node-a",
            "http://127.0.0.1:8080/mcp",
            "http://127.0.0.1:9092",
            100,
        )
        .unwrap();
    let control = Arc::new(RegistryControlPlane::new(registry));
    let storage = Arc::new(TempFs::new());
    let engine = Arc::new(DuckEngine::new());
    AgentOps::new(control, storage, engine, AgentConfig::default())
}

#[tokio::test]
async fn full_agent_loop() {
    let ops = ops();
    let id = Identity::claimed(Some("agent-loop"));

    let alloc = ops
        .allocate_pond(&id, Some("incident-9".into()), "{}", "medium", &[])
        .await
        .unwrap();
    assert_eq!(alloc.pond_name, "incident-9");

    ops.write_query(
        &id,
        "incident-9",
        "CREATE TABLE events(id INTEGER, sev VARCHAR)",
    )
    .await
    .unwrap();
    let w = ops
        .write_query(
            &id,
            "incident-9",
            "INSERT INTO events VALUES (1,'high'),(2,'critical')",
        )
        .await
        .unwrap();
    assert!(w.meta.snapshot_id.is_some());

    let r = ops
        .read_query(&id, "incident-9", "SELECT id, sev FROM events ORDER BY id")
        .await
        .unwrap();
    assert_eq!(r.rows.len(), 2);
    assert_eq!(r.rows[1][1], serde_json::json!("critical"));

    // Attribution visible via native DuckLake snapshots.
    let attr = ops
        .read_query(
            &id,
            "incident-9",
            "SELECT DISTINCT author FROM ducklake_snapshots('incident-9')",
        )
        .await
        .unwrap();
    let authors: Vec<_> = attr.rows.iter().filter_map(|row| row[0].as_str()).collect();
    assert!(authors.contains(&"agent-loop"), "got {authors:?}");

    let desc = ops.describe_pond(&id, "incident-9").await.unwrap();
    assert_eq!(desc.pond.name, "incident-9");
    assert!(desc.schema.tables.iter().any(|t| t.name == "events"));

    let ponds = ops.list_ponds(&id).await.unwrap();
    assert_eq!(ponds.len(), 1);

    ops.drop_pond(&id, "incident-9", true).await.unwrap();
    assert!(ops.describe_pond(&id, "incident-9").await.is_err());
}

#[tokio::test]
async fn read_arrow_streams_rows_locally() {
    use tokio_stream::StreamExt;
    let ops = ops();
    let id = Identity::claimed(Some("a"));
    ops.allocate_pond(&id, Some("ar".into()), "{}", "medium", &[])
        .await
        .unwrap();
    ops.write_query(
        &id,
        "ar",
        "CREATE TABLE t AS SELECT * FROM range(3000) r(i)",
    )
    .await
    .unwrap();

    let stream = ops.read_arrow(&id, "ar", "SELECT i FROM t").await.unwrap();
    // Schema is known up front, even before the first batch.
    assert_eq!(stream.schema.field(0).name(), "i");
    let mut rows = 0;
    let mut batches = stream.batches;
    while let Some(b) = batches.next().await {
        rows += b.unwrap().num_rows();
    }
    assert_eq!(rows, 3000, "all rows streamed across batches");

    // read_arrow rejects writes too (the error surfaces before the schema), and
    // for the read-only reason — a bare `is_err()` would also be satisfied by a
    // vanished pond or a SQL parse failure, i.e. by the guard being gone.
    // (`ArrowReadStream` is not `Debug`, so `expect_err` is not available.)
    let Err(err) = ops.read_arrow(&id, "ar", "INSERT INTO t VALUES (1)").await else {
        panic!("the streaming read path must refuse a write");
    };
    assert_eq!(
        err.envelope().kind,
        latiq_common::ErrorKind::ReadOnlyViolation
    );
}

#[tokio::test]
async fn pond_lifecycle_drop_requires_confirm() {
    let ops = ops();
    let id = Identity::claimed(Some("agent-loop"));
    ops.allocate_pond(&id, Some("keepme".into()), "{}", "medium", &[])
        .await
        .unwrap();

    // Without confirm the destructive drop is refused with a structured error...
    let err = ops
        .drop_pond(&id, "keepme", false)
        .await
        .expect_err("drop without confirm must be refused");
    assert_eq!(
        err.envelope().kind,
        latiq_common::ErrorKind::MissingArgument
    );

    // ...and the pond is untouched.
    assert!(ops.describe_pond(&id, "keepme").await.is_ok());

    // With confirm it actually drops.
    ops.drop_pond(&id, "keepme", true).await.unwrap();
    assert!(ops.describe_pond(&id, "keepme").await.is_err());
}

#[tokio::test]
async fn lazy_materialize_pond_assigned_without_eager_storage() {
    // Mirrors the CLI `pond create` path: the control plane assigns a node in the
    // registry, but NO storage is provisioned up front. The first query must
    // materialize the pond's storage on touch and succeed.
    let registry = Registry::open(None).unwrap();
    registry
        .register_node(
            "node-a",
            "http://127.0.0.1:8080/mcp",
            "http://127.0.0.1:9092",
            100,
        )
        .unwrap();
    // Registry-only allocation — deliberately NOT ops.allocate_pond (no storage).
    registry
        .create_pond(Some("lazy".into()), "agent-x", "{}", "medium", &[], "")
        .unwrap();

    let control = Arc::new(RegistryControlPlane::new(registry));
    let storage = Arc::new(TempFs::new());
    let engine = Arc::new(DuckEngine::new());
    let ops = AgentOps::new(control, storage, engine, AgentConfig::default());
    let id = Identity::claimed(Some("agent-x"));

    ops.write_query(&id, "lazy", "CREATE TABLE t(x INTEGER)")
        .await
        .unwrap();
    ops.write_query(&id, "lazy", "INSERT INTO t VALUES (1),(2)")
        .await
        .unwrap();
    let r = ops
        .read_query(&id, "lazy", "SELECT count(*) AS n FROM t")
        .await
        .unwrap();
    assert_eq!(r.rows[0][0], serde_json::json!(2));
}

#[tokio::test]
async fn read_query_rejects_writes_with_structured_error() {
    let ops = ops();
    let id = Identity::claimed(None);
    ops.allocate_pond(&id, Some("p".into()), "{}", "medium", &[])
        .await
        .unwrap();
    let err = ops
        .read_query(&id, "p", "INSERT INTO t VALUES (1)")
        .await
        .unwrap_err();
    assert_eq!(
        err.envelope().kind,
        latiq_common::ErrorKind::ReadOnlyViolation
    );
}

#[tokio::test]
async fn unknown_pond_is_pond_not_found() {
    let ops = ops();
    let id = Identity::claimed(None);
    let err = ops.read_query(&id, "ghost", "SELECT 1").await.unwrap_err();
    assert_eq!(err.envelope().kind, latiq_common::ErrorKind::PondNotFound);
}

/// The `none` (uncapped) tier is an operator grant, not something a workload can
/// assign itself: an uncapped pond can starve every other pond on its node. It is
/// rejected on the allocate path — which is the agent (MCP) and SDK surface — and
/// set afterwards over Admin instead.
#[tokio::test]
async fn pond_lifecycle_allocate_rejects_the_uncapped_tier() {
    let ops = ops();
    let id = Identity::claimed(Some("greedy-agent"));

    let err = ops
        .allocate_pond(&id, Some("grabby".into()), "{}", "none", &[])
        .await
        .expect_err("an agent must not be able to allocate an uncapped pond");
    let msg = err.envelope().message.to_lowercase();
    assert!(msg.contains("none"), "unhelpful message: {msg}");
    assert!(
        msg.contains("set-tier") || msg.contains("operator"),
        "the error should point at the operator path: {msg}"
    );

    // The alias is refused too, so it can't be smuggled in under another name --
    // and refused for the SAME reason. A bare `is_err()` here would be satisfied
    // by `uncapped` being rejected as an UNKNOWN tier, which is exactly the
    // confusion this case exists to rule out: the alias must resolve to the
    // uncapped tier and then be refused as an operator grant.
    let err = ops
        .allocate_pond(&id, Some("grabby2".into()), "{}", "uncapped", &[])
        .await
        .expect_err("the `uncapped` alias must be refused too");
    let msg = err.envelope().message.to_lowercase();
    assert!(
        msg.contains("none"),
        "the alias must be refused as the uncapped tier, not as an unknown one: {msg}"
    );
    assert!(
        msg.contains("set-tier") || msg.contains("operator"),
        "the error should point at the operator path: {msg}"
    );

    // A normal tier still allocates.
    ops.allocate_pond(&id, Some("fine".into()), "{}", "large", &[])
        .await
        .unwrap();
}
