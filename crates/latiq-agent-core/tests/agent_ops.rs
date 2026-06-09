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
        .allocate_pond(&id, Some("incident-9".into()), "{}")
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
async fn pond_lifecycle_drop_requires_confirm() {
    let ops = ops();
    let id = Identity::claimed(Some("agent-loop"));
    ops.allocate_pond(&id, Some("keepme".into()), "{}")
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
        .create_pond(Some("lazy".into()), "agent-x", "{}")
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
    ops.allocate_pond(&id, Some("p".into()), "{}")
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
