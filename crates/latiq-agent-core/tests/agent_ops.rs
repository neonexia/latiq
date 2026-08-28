//! AgentOps end-to-end over the in-process ControlPlane (Registry), DuckEngine
//! and TempFs storage. Exercises the full agent loop through the public ops,
//! plus the M7 success criteria (`mod m7`) and the forwarding decision
//! (`mod forwarding`).
//!
//! One binary, not three: each integration binary statically links a bundled
//! DuckDB (~130-160 MB). The submodules keep each group's fixtures to itself —
//! `mod forwarding` in particular brings a fake ControlPlane whose `ops()` must
//! not be confused with the real one here.
//!
//! `tests/access_trail.rs` stays a separate binary on purpose: it installs a
//! process-global `tracing` subscriber, and `tracing` caches callsite interest
//! process-wide, so it cannot share a binary with tests that install none.
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

/// M7 success criteria: query-by-URI ingestion (local file, hermetic) and
/// concurrent multi-agent writes with correct per-identity attribution.
mod m7 {
    use super::ops;
    use latiq_common::Identity;

    #[tokio::test]
    async fn query_by_uri_local_csv_ingestion() {
        let ops = ops();
        let id = Identity::claimed(Some("ingestor"));
        ops.allocate_pond(&id, Some("ing".into()), "{}", "medium", &[])
            .await
            .unwrap();

        let dir = tempfile::tempdir().unwrap();
        let csv = dir.path().join("data.csv");
        std::fs::write(&csv, "id,name\n1,alice\n2,bob\n3,carol\n").unwrap();

        let sql = format!(
            "CREATE TABLE people AS SELECT * FROM read_csv('{}', header=true)",
            csv.display()
        );
        ops.write_query(&id, "ing", &sql).await.unwrap();

        let r = ops
            .read_query(&id, "ing", "SELECT count(*) AS n FROM people")
            .await
            .unwrap();
        assert_eq!(r.rows[0][0], serde_json::json!(3));
    }

    #[tokio::test]
    async fn concurrent_multi_agent_writes_are_consistent_and_attributed() {
        let ops = ops();
        let setup = Identity::claimed(Some("setup"));
        ops.allocate_pond(&setup, Some("shared".into()), "{}", "medium", &[])
            .await
            .unwrap();
        ops.write_query(
            &setup,
            "shared",
            "CREATE TABLE log(agent VARCHAR, n INTEGER)",
        )
        .await
        .unwrap();

        let n_agents = 4;
        let per = 3;
        let mut handles = vec![];
        for a in 0..n_agents {
            let ops2 = ops.clone();
            handles.push(tokio::spawn(async move {
                let id = Identity::claimed(Some(&format!("agent-{a}")));
                for i in 0..per {
                    let sql = format!("INSERT INTO log VALUES ('agent-{a}', {i})");
                    // DuckLake uses optimistic concurrency; retry on conflict.
                    let mut attempts = 0;
                    loop {
                        match ops2.write_query(&id, "shared", &sql).await {
                            Ok(_) => break,
                            Err(e) => {
                                attempts += 1;
                                assert!(
                                    attempts < 50,
                                    "write kept conflicting: {}",
                                    e.envelope().message
                                );
                                tokio::time::sleep(std::time::Duration::from_millis(15)).await;
                            }
                        }
                    }
                }
            }));
        }
        for h in handles {
            h.await.unwrap();
        }

        let total = ops
            .read_query(&setup, "shared", "SELECT count(*) AS n FROM log")
            .await
            .unwrap();
        assert_eq!(
            total.rows[0][0],
            serde_json::json!(n_agents * per),
            "all concurrent writes must be durable"
        );

        let authors = ops
            .read_query(
                &setup,
                "shared",
                "SELECT count(DISTINCT author) AS a FROM ducklake_snapshots('shared') WHERE author LIKE 'agent-%'",
            )
            .await
            .unwrap();
        assert_eq!(
            authors.rows[0][0],
            serde_json::json!(n_agents),
            "each agent's writes must be attributed to it"
        );
    }
}

/// Forwarding decision in AgentOps: a request for a pond owned by another node
/// is delegated to the `Forwarder`; a pond owned by *this* node (or with no live
/// owner) runs locally. Uses a fake ControlPlane (to pin the owner endpoint) and
/// a recording Forwarder (to observe delegation) — no real cluster needed.
mod forwarding {
    use latiq_agent_core::{
        AgentConfig, AgentError, AgentOps, ArrowReadStream, ControlPlane, DescribeResult,
        Forwarder, PondInfo,
    };
    use latiq_common::{Identity, QueryMeta};
    use latiq_engine::{ExplainResult, QueryResult, SchemaSummary};
    use latiq_engine_duckdb::DuckEngine;
    use latiq_storage::TempFs;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};

    const PID: &str = "00000000-0000-0000-0000-000000000001";

    /// Every pond resolves to a fixed owner endpoint (or none).
    struct FixedOwner {
        endpoint: Option<String>,
    }

    #[async_trait::async_trait]
    impl ControlPlane for FixedOwner {
        async fn create_pond(
            &self,
            _: Option<String>,
            _: &str,
            _: &str,
            _: &str,
            _: &[String],
        ) -> Result<PondInfo, AgentError> {
            unreachable!("not exercised")
        }
        async fn resolve_pond(&self, pond_ref: &str) -> Result<String, AgentError> {
            Ok(pond_ref.to_string())
        }
        async fn list_ponds(&self) -> Result<Vec<PondInfo>, AgentError> {
            Ok(vec![])
        }
        async fn pond_info(&self, pond_ref: &str) -> Result<PondInfo, AgentError> {
            Ok(PondInfo {
                pond_id: PID.to_string(),
                name: pond_ref.to_string(),
                owner: "owner".to_string(),
                created_at: String::new(),
                policy_json: "{}".to_string(),
                node_endpoint: self.endpoint.clone(),
                tier: "medium".to_string(),
                extensions: vec![],
                description: String::new(),
            })
        }
        async fn drop_pond(&self, _: &str) -> Result<(), AgentError> {
            Ok(())
        }
        async fn list_datasets(
            &self,
            _: &str,
        ) -> Result<Vec<latiq_agent_core::DatasetInfo>, AgentError> {
            Ok(vec![])
        }
        async fn get_dataset(&self, r: &str) -> Result<latiq_agent_core::DatasetInfo, AgentError> {
            Err(AgentError::dataset_not_found(r))
        }
        async fn list_catalogs(
            &self,
            _: &str,
        ) -> Result<Vec<latiq_agent_core::CatalogInfo>, AgentError> {
            Ok(vec![])
        }
        async fn get_catalog(&self, r: &str) -> Result<latiq_agent_core::CatalogInfo, AgentError> {
            Err(AgentError::internal(format!("no catalog {r}")))
        }
    }

    #[derive(Default)]
    struct RecordingForwarder {
        reads: AtomicUsize,
        writes: AtomicUsize,
        pulls: AtomicUsize,
        describes: AtomicUsize,
        last_endpoint: Mutex<String>,
        last_pond: Mutex<String>,
        last_sql: Mutex<String>,
    }

    impl RecordingForwarder {
        fn note(&self, endpoint: &str, pond: &str, sql: &str) {
            *self.last_endpoint.lock().unwrap() = endpoint.to_string();
            *self.last_pond.lock().unwrap() = pond.to_string();
            *self.last_sql.lock().unwrap() = sql.to_string();
        }
    }

    fn sentinel(tag: &str) -> QueryResult {
        QueryResult {
            columns: vec![tag.to_string()],
            rows: vec![],
            meta: QueryMeta::default(),
        }
    }

    #[async_trait::async_trait]
    impl Forwarder for RecordingForwarder {
        async fn read(
            &self,
            e: &str,
            _: &Identity,
            p: &str,
            s: &str,
        ) -> Result<QueryResult, AgentError> {
            self.reads.fetch_add(1, Ordering::SeqCst);
            self.note(e, p, s);
            Ok(sentinel("forwarded_read"))
        }
        async fn write(
            &self,
            e: &str,
            _: &Identity,
            p: &str,
            s: &str,
        ) -> Result<QueryResult, AgentError> {
            self.writes.fetch_add(1, Ordering::SeqCst);
            self.note(e, p, s);
            Ok(sentinel("forwarded_write"))
        }
        async fn read_arrow(
            &self,
            e: &str,
            _: &Identity,
            p: &str,
            s: &str,
        ) -> Result<ArrowReadStream, AgentError> {
            self.reads.fetch_add(1, Ordering::SeqCst);
            self.note(e, p, s);
            Ok(ArrowReadStream {
                schema: std::sync::Arc::new(arrow::datatypes::Schema::empty()),
                batches: Box::pin(tokio_stream::iter(Vec::new())),
            })
        }
        async fn explain(
            &self,
            e: &str,
            _: &Identity,
            p: &str,
            s: &str,
        ) -> Result<ExplainResult, AgentError> {
            self.note(e, p, s);
            Ok(ExplainResult {
                estimated_rows: 0,
                estimated_bytes: 0,
                estimated_duration_ms: 0,
                scan_operations: vec![],
                warnings: vec![],
                suggestions: vec![],
                raw_plan: "forwarded".to_string(),
            })
        }
        async fn describe(
            &self,
            e: &str,
            _: &Identity,
            p: &str,
        ) -> Result<DescribeResult, AgentError> {
            self.note(e, p, "");
            Ok(DescribeResult {
                pond: PondInfo {
                    pond_id: PID.to_string(),
                    name: p.to_string(),
                    owner: "owner".to_string(),
                    created_at: String::new(),
                    policy_json: "{}".to_string(),
                    node_endpoint: Some(e.to_string()),
                    tier: "medium".to_string(),
                    extensions: vec![],
                    description: String::new(),
                },
                schema: SchemaSummary::default(),
            })
        }
        async fn drop_pond(
            &self,
            e: &str,
            _: &Identity,
            p: &str,
            _: bool,
        ) -> Result<(), AgentError> {
            self.note(e, p, "");
            Ok(())
        }
        async fn catalog_pull(
            &self,
            e: &str,
            _: &Identity,
            p: &str,
            catalog: &str,
            q: &str,
            _params: std::collections::BTreeMap<String, String>,
        ) -> Result<latiq_agent_core::PullResult, AgentError> {
            self.pulls.fetch_add(1, Ordering::SeqCst);
            self.note(e, p, q);
            Ok(latiq_agent_core::PullResult {
                catalog: catalog.to_string(),
                query: q.to_string(),
            })
        }
        async fn catalog_describe(
            &self,
            e: &str,
            _: &Identity,
            p: &str,
            _catalog: &str,
            _params: std::collections::BTreeMap<String, String>,
        ) -> Result<Vec<(String, String)>, AgentError> {
            self.describes.fetch_add(1, Ordering::SeqCst);
            self.note(e, p, "");
            Ok(vec![("main".to_string(), "forwarded_table".to_string())])
        }
    }

    fn ops_with(owner: Option<&str>, self_ep: &str, fwd: Arc<RecordingForwarder>) -> AgentOps {
        let control = Arc::new(FixedOwner {
            endpoint: owner.map(|s| s.to_string()),
        });
        AgentOps::new(
            control,
            Arc::new(TempFs::new()),
            Arc::new(DuckEngine::new()),
            AgentConfig::default(),
        )
        .with_forwarding(self_ep.to_string(), fwd)
    }

    #[tokio::test]
    async fn forwarding_read_delegates_to_owner() {
        let fwd = Arc::new(RecordingForwarder::default());
        let ops = ops_with(
            Some("http://owner:9092"),
            "http://greeter:9092",
            fwd.clone(),
        );
        let r = ops
            .read_query(&Identity::claimed(Some("a")), "pond-x", "SELECT 1")
            .await
            .unwrap();
        assert_eq!(fwd.reads.load(Ordering::SeqCst), 1);
        assert_eq!(*fwd.last_endpoint.lock().unwrap(), "http://owner:9092");
        assert_eq!(*fwd.last_pond.lock().unwrap(), "pond-x");
        assert_eq!(*fwd.last_sql.lock().unwrap(), "SELECT 1");
        // The sentinel proves the result came from the forwarder, not a local run.
        assert_eq!(r.columns, vec!["forwarded_read".to_string()]);
    }

    #[tokio::test]
    async fn forwarding_write_delegates_to_owner() {
        let fwd = Arc::new(RecordingForwarder::default());
        let ops = ops_with(
            Some("http://owner:9092"),
            "http://greeter:9092",
            fwd.clone(),
        );
        let r = ops
            .write_query(
                &Identity::claimed(Some("a")),
                "pond-x",
                "CREATE TABLE t(i INT)",
            )
            .await
            .unwrap();
        assert_eq!(fwd.writes.load(Ordering::SeqCst), 1);
        assert_eq!(r.columns, vec!["forwarded_write".to_string()]);
    }

    #[tokio::test]
    async fn forwarding_skipped_when_self_owns() {
        // self_endpoint == owner → no forward; the read runs locally (DuckDB).
        let fwd = Arc::new(RecordingForwarder::default());
        let ops = ops_with(Some("http://me:9092"), "http://me:9092", fwd.clone());
        let r = ops
            .read_query(&Identity::claimed(Some("a")), "pond-x", "SELECT 1 AS one")
            .await
            .unwrap();
        assert_eq!(
            fwd.reads.load(Ordering::SeqCst),
            0,
            "must not forward to self"
        );
        assert_eq!(r.rows[0][0], serde_json::json!(1));
    }

    #[tokio::test]
    async fn forwarding_catalog_pull_delegates_to_owner() {
        let fwd = Arc::new(RecordingForwarder::default());
        let ops = ops_with(
            Some("http://owner:9092"),
            "http://greeter:9092",
            fwd.clone(),
        );
        let r = ops
            .catalog_pull(
                &Identity::claimed(Some("a")),
                "pond-x",
                "lake",
                "CREATE TABLE t AS SELECT 1",
                std::collections::BTreeMap::new(),
            )
            .await
            .unwrap();
        assert_eq!(fwd.pulls.load(Ordering::SeqCst), 1);
        assert_eq!(*fwd.last_endpoint.lock().unwrap(), "http://owner:9092");
        assert_eq!(*fwd.last_pond.lock().unwrap(), "pond-x");
        assert_eq!(r.catalog, "lake");
    }

    #[tokio::test]
    async fn forwarding_catalog_describe_delegates_to_owner() {
        let fwd = Arc::new(RecordingForwarder::default());
        let ops = ops_with(
            Some("http://owner:9092"),
            "http://greeter:9092",
            fwd.clone(),
        );
        let tables = ops
            .catalog_describe(
                &Identity::claimed(Some("a")),
                "pond-x",
                "lake",
                std::collections::BTreeMap::new(),
            )
            .await
            .unwrap();
        assert_eq!(fwd.describes.load(Ordering::SeqCst), 1);
        assert_eq!(*fwd.last_endpoint.lock().unwrap(), "http://owner:9092");
        assert_eq!(
            tables,
            vec![("main".to_string(), "forwarded_table".to_string())]
        );
    }

    #[tokio::test]
    async fn forwarding_skipped_when_no_live_owner() {
        // node_endpoint == None (owning node gone) → run locally, surface real errors
        // rather than forwarding to nowhere.
        let fwd = Arc::new(RecordingForwarder::default());
        let ops = ops_with(None, "http://me:9092", fwd.clone());
        let r = ops
            .read_query(&Identity::claimed(Some("a")), "pond-x", "SELECT 1 AS one")
            .await
            .unwrap();
        assert_eq!(fwd.reads.load(Ordering::SeqCst), 0);
        assert_eq!(r.rows[0][0], serde_json::json!(1));
    }
}
