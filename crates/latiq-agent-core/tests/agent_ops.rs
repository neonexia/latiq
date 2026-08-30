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
        .allocate_pond(&id, Some("incident-9".into()), "{}", "medium", &[], false)
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
    ops.allocate_pond(&id, Some("ar".into()), "{}", "medium", &[], false)
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
async fn read_arrow_cancel_reaches_a_producer_parked_on_a_stalled_consumer() {
    // Regression pin. The engine holds a read-only transaction open across the
    // whole batch stream, so it also holds a pooled connection and a pinned
    // DuckLake snapshot. Cancellation works by interrupting DuckDB — but a
    // producer parked in the bounded sink channel is not in DuckDB, so the
    // interrupt reaches nothing and a client that stays connected while it
    // stops reading pins that snapshot for as long as it likes. The send must
    // therefore watch the abort token itself.
    use std::time::Duration;
    let ops = ops();
    let id = Identity::claimed(Some("a"));
    let alloc = ops
        .allocate_pond(&id, Some("stall".into()), "{}", "medium", &[], false)
        .await
        .unwrap();
    ops.write_query(
        &id,
        "stall",
        // Comfortably more batches than the channel's capacity, so the producer
        // cannot drain into it and finish on its own.
        "CREATE TABLE t AS SELECT * FROM range(50000) r(i)",
    )
    .await
    .unwrap();

    // Held, NOT drained: dropping it instead would exercise the "receiver gone"
    // path, which always worked and is not the bug.
    let _stream = ops
        .read_arrow(&id, "stall", "SELECT i FROM t")
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(250)).await;
    // Anti-vacuity: the operation really is still running (parked in the sink),
    // so what the cancel below unblocks is that park and not an already
    // finished read.
    assert!(
        !ops.inflight().is_empty(),
        "the producer should still be in flight, parked on the full channel"
    );

    ops.inflight().cancel_for_pond(&alloc.pond_id);
    // The blocking task only reaches `complete` — and so only releases the
    // transaction and the connection — once the SEND observes the cancel.
    let unparked = tokio::time::timeout(Duration::from_secs(10), async {
        while !ops.inflight().is_empty() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await;
    assert!(
        unparked.is_ok(),
        "a cancelled read stayed parked in the sink: its transaction, pooled \
         connection and pinned snapshot are held by a consumer that stopped reading"
    );
}

#[tokio::test]
async fn pond_lifecycle_drop_requires_confirm() {
    let ops = ops();
    let id = Identity::claimed(Some("agent-loop"));
    ops.allocate_pond(&id, Some("keepme".into()), "{}", "medium", &[], false)
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
        .create_pond(
            Some("lazy".into()),
            "agent-x",
            "{}",
            "medium",
            &[],
            "",
            false,
        )
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
    ops.allocate_pond(&id, Some("p".into()), "{}", "medium", &[], false)
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
        .allocate_pond(&id, Some("grabby".into()), "{}", "none", &[], false)
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
        .allocate_pond(&id, Some("grabby2".into()), "{}", "uncapped", &[], false)
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
    ops.allocate_pond(&id, Some("fine".into()), "{}", "large", &[], false)
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
        ops.allocate_pond(&id, Some("ing".into()), "{}", "medium", &[], false)
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
        ops.allocate_pond(&setup, Some("shared".into()), "{}", "medium", &[], false)
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
        /// The pond's lineage opt-in, as the registry would report it.
        lineage: bool,
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
            _: bool,
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
                lineage: self.lineage,
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
        lineages: AtomicUsize,
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
            Ok(ArrowReadStream::new(
                std::sync::Arc::new(arrow::datatypes::Schema::empty()),
                Box::pin(tokio_stream::iter(Vec::new())),
            ))
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
                    lineage: false,
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
        async fn get_lineage(
            &self,
            e: &str,
            _: &Identity,
            p: &str,
            limit: usize,
            since: Option<&str>,
            before: Option<&str>,
        ) -> Result<latiq_agent_core::LineagePage, AgentError> {
            self.lineages.fetch_add(1, Ordering::SeqCst);
            // The bounds are recorded in `last_sql` (the fake's free-text slot)
            // so a test can prove they crossed the hop rather than being
            // silently dropped — a forward that lost `before` would page for
            // ever without it.
            self.note(e, p, &format!("{limit}|{since:?}|{before:?}"));
            Ok(latiq_agent_core::LineagePage {
                pond_id: PID.to_string(),
                pond_name: p.to_string(),
                lineage_dir: "/owner/ponds/x/lineage".to_string(),
                events: vec![serde_json::json!({"eventTime": "2026-08-14T10:00:00.000Z"})],
                truncated: false,
                malformed_lines: 0,
                unreadable_files: 0,
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
        ops_with_lineage(owner, self_ep, fwd, false).0
    }

    /// As `ops_with`, with the pond's lineage opt-in chosen and the storage kept
    /// so a test can look for the events on disk.
    fn ops_with_lineage(
        owner: Option<&str>,
        self_ep: &str,
        fwd: Arc<RecordingForwarder>,
        lineage: bool,
    ) -> (AgentOps, Arc<TempFs>) {
        let control = Arc::new(FixedOwner {
            endpoint: owner.map(|s| s.to_string()),
            lineage,
        });
        let storage = Arc::new(TempFs::new());
        let ops = AgentOps::new(
            control,
            storage.clone(),
            Arc::new(DuckEngine::new()),
            AgentConfig::default(),
        )
        .with_forwarding(self_ep.to_string(), fwd);
        (ops, storage)
    }

    #[tokio::test]
    async fn lineage_forwarded_op_is_recorded_once_by_the_owner() {
        // The owner runs the query, so the owner emits: a greeter that also
        // emitted would double the run in every consumer, with two different
        // pond-local snapshot ids and only one of them real.
        let fwd = Arc::new(RecordingForwarder::default());
        let (greeter, greeter_storage) = ops_with_lineage(
            Some("http://owner:9092"),
            "http://greeter:9092",
            fwd.clone(),
            true,
        );
        // Give the greeter this pond's storage, lineage directory and all: a
        // greeter that emitted would then leave real files here, so the
        // emptiness below is a decision and not a missing directory.
        latiq_storage::PondStorage::ensure_pond(
            greeter_storage.as_ref(),
            latiq_common::PondId::parse(PID).unwrap(),
            true,
        )
        .unwrap();
        greeter
            .write_query(
                &Identity::claimed(Some("a")),
                "pond-x",
                "CREATE TABLE t(i INT)",
            )
            .await
            .unwrap();
        greeter.flush_lineage();
        assert_eq!(fwd.writes.load(Ordering::SeqCst), 1, "it did forward");
        assert_eq!(
            greeter.lineage_writer_count(),
            0,
            "the node that only relayed the write must record nothing"
        );
        assert!(super::lineage::events_in(&greeter_storage, PID).is_empty());

        // Anti-vacuity: the same pond, same SQL, owned by this node — the
        // events appear, so the silence above is the forward and not a pond
        // that never emits.
        let (owner, owner_storage) =
            ops_with_lineage(Some("http://me:9092"), "http://me:9092", fwd.clone(), true);
        owner
            .write_query(
                &Identity::claimed(Some("a")),
                "pond-x",
                "CREATE TABLE t(i INT)",
            )
            .await
            .unwrap();
        owner.flush_lineage();
        assert_eq!(
            super::lineage::events_in(&owner_storage, PID).len(),
            2,
            "the owner records the write it actually ran, once"
        );
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
    async fn forwarding_get_lineage_delegates_to_owner_with_its_bounds_intact() {
        // A pond's events are FILES on the node that ran its queries, so a peer
        // has nothing of its own to answer with — behind a load-balancing
        // gateway an agent lands on a peer most of the time, and a local read
        // there would answer an honest question with an empty page.
        let fwd = Arc::new(RecordingForwarder::default());
        let ops = ops_with(
            Some("http://owner:9092"),
            "http://greeter:9092",
            fwd.clone(),
        );
        let page = ops
            .get_lineage(
                &Identity::claimed(Some("a")),
                "pond-x",
                7,
                Some("2026-08-14T09:00:00Z"),
                Some("2026-08-14T11:00:00Z"),
            )
            .await
            .unwrap();
        assert_eq!(fwd.lineages.load(Ordering::SeqCst), 1);
        assert_eq!(*fwd.last_endpoint.lock().unwrap(), "http://owner:9092");
        assert_eq!(*fwd.last_pond.lock().unwrap(), "pond-x");
        assert_eq!(
            *fwd.last_sql.lock().unwrap(),
            "7|Some(\"2026-08-14T09:00:00Z\")|Some(\"2026-08-14T11:00:00Z\")",
            "limit and BOTH bounds must cross the hop, or paging loops on the owner"
        );
        assert_eq!(
            page.lineage_dir, "/owner/ponds/x/lineage",
            "the owner's answer is returned as-is, directory included"
        );
    }

    #[tokio::test]
    async fn forwarding_get_lineage_rejects_a_zero_limit_before_the_hop() {
        // `limit: 0` is a caller mistake with one right answer everywhere; the
        // owner would refuse it identically, so a peer hop to earn the same
        // error is pure latency.
        let fwd = Arc::new(RecordingForwarder::default());
        let ops = ops_with(
            Some("http://owner:9092"),
            "http://greeter:9092",
            fwd.clone(),
        );
        let err = ops
            .get_lineage(&Identity::claimed(Some("a")), "pond-x", 0, None, None)
            .await
            .expect_err("zero is refused");
        assert_eq!(err.envelope().kind, latiq_common::ErrorKind::InvalidValue);
        assert_eq!(
            fwd.lineages.load(Ordering::SeqCst),
            0,
            "and it never reached the owner"
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

/// Lineage emission from the query path: a pond that opted in leaves a
/// spec-compliant OpenLineage trail in its own directory, and a pond that did
/// not pays nothing for it.
///
/// The events are read back off disk rather than through a fake sink, because
/// the file in the pond's `lineage/` directory is the artefact task 6's MCP
/// tool and task 7's HTTP sink both read — a test against an in-memory
/// interception would not prove the thing that ships.
mod lineage {
    use latiq_agent_core::{AgentConfig, AgentOps, RegistryControlPlane};
    use latiq_common::{Identity, PondId};
    use latiq_control_plane::Registry;
    use latiq_engine_duckdb::DuckEngine;
    use latiq_storage::{PondStorage, TempFs};
    use serde_json::{json, Value};
    use std::collections::BTreeSet;
    use std::sync::Arc;

    /// As the file-level `ops()`, but keeping the storage handle: the events
    /// live in the pond's own directory, so a test has to be able to find it.
    pub(super) fn ops_with_storage() -> (AgentOps, Arc<TempFs>) {
        let registry = Registry::open(None).unwrap();
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
        let ops = AgentOps::new(control, storage.clone(), engine, AgentConfig::default());
        (ops, storage)
    }

    /// The pond's lineage directory, or `None` once (or before) the pond has
    /// no storage on this node at all.
    pub(super) fn lineage_dir(storage: &TempFs, pond_id: &str) -> Option<std::path::PathBuf> {
        let pid = PondId::parse(pond_id).expect("pond id parses");
        let loc = storage.pond_location(pid).ok()?;
        Some(std::path::PathBuf::from(loc.lineage_dir))
    }

    /// Every event the pond has on disk, in file order (which is chronological,
    /// per the writer's name format) and then line order within a file.
    pub(super) fn events_in(storage: &TempFs, pond_id: &str) -> Vec<Value> {
        let Some(dir) = lineage_dir(storage, pond_id) else {
            return Vec::new(); // no storage for this pond on this node
        };
        let Ok(entries) = std::fs::read_dir(&dir) else {
            return Vec::new(); // no lineage directory at all
        };
        let mut names: Vec<String> = entries
            .map(|e| {
                e.expect("dir entry")
                    .file_name()
                    .to_string_lossy()
                    .into_owned()
            })
            .filter(|n| n.ends_with(".jsonl"))
            .collect();
        names.sort();
        let mut out = Vec::new();
        for name in names {
            let body = std::fs::read_to_string(dir.join(&name)).expect("event file readable");
            for line in body.lines() {
                out.push(
                    serde_json::from_str(line)
                        .unwrap_or_else(|e| panic!("torn record in {name}: {e}")),
                );
            }
        }
        out
    }

    /// One facet body, e.g. `facet(e, "run", "latiq_identity")`.
    pub(super) fn facet<'a>(event: &'a Value, owner: &str, key: &str) -> &'a Value {
        &event[owner]["facets"][key]
    }

    fn job_name(event: &Value) -> &str {
        event["job"]["name"].as_str().expect("job name is a string")
    }

    fn event_type(event: &Value) -> &str {
        event["eventType"].as_str().expect("eventType is a string")
    }

    /// The events of one operation: a job name uniquely identifies the op here
    /// (`{pond}.{op}[.{target}]`), and the START/terminal pair share a run id.
    fn events_for_op<'a>(events: &'a [Value], job_prefix: &str) -> Vec<&'a Value> {
        events
            .iter()
            .filter(|e| job_name(e).starts_with(job_prefix))
            .collect()
    }

    #[tokio::test]
    async fn lineage_disabled_pond_emits_nothing_and_does_no_work() {
        // The point of the opt-in. "Emits nothing" and "costs nothing" are
        // different claims and only the second justifies the flag: a pond
        // without lineage must never even resolve a writer, which is what the
        // per-pond directory lookup and every event allocation hang off.
        let (ops, storage) = ops_with_storage();
        let id = Identity::claimed(Some("agent-a"));
        let off = ops
            .allocate_pond(&id, Some("quiet".into()), "{}", "medium", &[], false)
            .await
            .unwrap();
        ops.write_query(&id, "quiet", "CREATE TABLE t(i INTEGER)")
            .await
            .unwrap();
        ops.read_query(&id, "quiet", "SELECT * FROM t")
            .await
            .unwrap();
        ops.flush_lineage();

        assert_eq!(
            ops.lineage_writer_count(),
            0,
            "a pond without lineage must not cause a writer to be built"
        );
        assert!(
            !lineage_dir(&storage, &off.pond_id)
                .expect("the pond has storage")
                .exists(),
            "a pond without lineage must have no lineage directory"
        );
        assert!(events_in(&storage, &off.pond_id).is_empty());

        // Anti-vacuity: the same ops, the same queries, with the flag on — so
        // the emptiness above is the flag and not an emitter that never runs.
        let on = ops
            .allocate_pond(&id, Some("loud".into()), "{}", "medium", &[], true)
            .await
            .unwrap();
        ops.write_query(&id, "loud", "CREATE TABLE t(i INTEGER)")
            .await
            .unwrap();
        ops.flush_lineage();
        assert_eq!(ops.lineage_writer_count(), 1, "exactly the opted-in pond");
        assert_eq!(
            events_in(&storage, &on.pond_id).len(),
            2,
            "the opted-in pond records a START and a terminal event"
        );
    }

    #[tokio::test]
    async fn lineage_write_records_start_and_complete_with_the_verified_subject() {
        // A lone START leaves a permanently-RUNNING run in every consumer, so
        // both events are required. And the identity facet has to keep the
        // verified subject distinguishable from the claimed leaf: authority
        // only ever comes from the verified field.
        let (ops, storage) = ops_with_storage();
        let id = Identity::verified(
            "svc-orchestrator",
            "https://idp.example/realms/latiq",
            Some("planner-7"),
        );
        let pond = ops
            .allocate_pond(&id, Some("ledger".into()), "{}", "medium", &[], true)
            .await
            .unwrap();
        ops.write_query(&id, "ledger", "CREATE TABLE t(i INTEGER, sev VARCHAR)")
            .await
            .unwrap();
        ops.write_query(&id, "ledger", "INSERT INTO t VALUES (1,'high')")
            .await
            .unwrap();
        ops.flush_lineage();

        let events = events_in(&storage, &pond.pond_id);
        let writes = events_for_op(&events, "ledger.write_query");
        assert_eq!(
            writes.len(),
            4,
            "two writes, each a START and a terminal event: {events:#?}"
        );
        let types: Vec<&str> = writes.iter().map(|e| event_type(e)).collect();
        assert_eq!(types, vec!["START", "COMPLETE", "START", "COMPLETE"]);

        // The pair of one operation shares a run id, and two operations do not.
        let run_ids: Vec<&Value> = writes.iter().map(|e| &e["run"]["runId"]).collect();
        assert_eq!(
            run_ids[0], run_ids[1],
            "a START and its terminal event are one run"
        );
        assert_ne!(run_ids[1], run_ids[2], "two writes are two runs");

        let insert = writes[3];
        let ident = facet(insert, "run", "latiq_identity");
        assert_eq!(ident["subject"], json!("svc-orchestrator"));
        assert_eq!(ident["issuer"], json!("https://idp.example/realms/latiq"));
        assert_eq!(ident["verified"], json!(true));
        assert_eq!(
            ident["agentId"],
            json!("planner-7"),
            "the claimed leaf is recorded, and is not the subject"
        );
        assert_eq!(
            ident["agentIdVerified"],
            json!(false),
            "the leaf is claimed on every path"
        );

        let pond_facet = facet(insert, "job", "latiq_pond");
        assert_eq!(pond_facet["pondId"], json!(pond.pond_id));
        assert_eq!(pond_facet["pondName"], json!("ledger"));
        assert_eq!(insert["job"]["namespace"], json!("latiq"));

        let query = facet(insert, "run", "latiq_query");
        assert_eq!(query["op"], json!("write_query"));
        assert_eq!(query["outcome"], json!("ok"));
        assert_eq!(query["durationMeaning"], json!("completion"));

        // The SQL rides the standard SQLJobFacet, redacted exactly as the
        // access trail redacts it — provenance must not become a literal leak.
        let sql = facet(insert, "job", "sql")["query"]
            .as_str()
            .expect("sql facet carries the query");
        assert_eq!(sql, "INSERT INTO t VALUES (?,?)");
    }

    #[tokio::test]
    async fn lineage_failed_query_emits_a_fail_event_with_an_error_facet() {
        // A store that records only successes tells you what worked, never what
        // was attempted — the same reason the access trail records `outcome`.
        let (ops, storage) = ops_with_storage();
        let id = Identity::claimed(Some("agent-a"));
        let pond = ops
            .allocate_pond(&id, Some("broken".into()), "{}", "medium", &[], true)
            .await
            .unwrap();
        ops.read_query(&id, "broken", "SELECT * FROM nope")
            .await
            .expect_err("the table does not exist");
        ops.flush_lineage();

        let events = events_in(&storage, &pond.pond_id);
        assert_eq!(
            events.len(),
            2,
            "a failed query is still a run: START and a terminal event"
        );
        assert_eq!(event_type(&events[0]), "START");
        assert_eq!(
            event_type(&events[1]),
            "FAIL",
            "the terminal event must say the run failed, not COMPLETE"
        );
        assert_eq!(
            facet(&events[1], "run", "latiq_query")["outcome"],
            json!("error")
        );
        let err = facet(&events[1], "run", "errorMessage");
        assert_eq!(err["programmingLanguage"], json!("RUST"));
        let message = err["message"].as_str().expect("an error message");
        assert!(
            message.to_lowercase().contains("nope"),
            "the facet must carry the real failure, got {message:?}"
        );
    }

    #[tokio::test]
    async fn lineage_read_is_recorded_once_as_the_op_the_caller_invoked() {
        // `read_collected` runs `read_arrow_local`, so an emitter placed in the
        // local halves rather than the public methods would record this read
        // twice, under two different ops. Pins that.
        let (ops, storage) = ops_with_storage();
        let id = Identity::claimed(Some("agent-a"));
        let pond = ops
            .allocate_pond(&id, Some("once".into()), "{}", "medium", &[], true)
            .await
            .unwrap();
        ops.write_query(&id, "once", "CREATE TABLE t AS SELECT 1 AS i")
            .await
            .unwrap();
        // `read_collected` — the path that rides `read_arrow_local`, and so the
        // one where a misplaced emitter would double-record. (`read_query`
        // takes the non-Arrow `run_query` path and is covered above.)
        ops.read_collected(&id, "once", "SELECT i FROM t")
            .await
            .unwrap();
        ops.flush_lineage();

        let events = events_in(&storage, &pond.pond_id);
        assert_eq!(
            events_for_op(&events, "once.read_query").len(),
            2,
            "exactly one START/terminal pair for the read: {events:#?}"
        );
        assert!(
            events_for_op(&events, "once.read_arrow").is_empty(),
            "the internal Arrow hop must not appear as an operation of its own"
        );
        assert_eq!(
            events.len(),
            4,
            "one write and one read, two events each — nothing else emitted"
        );
    }

    #[tokio::test]
    async fn lineage_events_carry_the_datasets_the_query_touched() {
        // Without datasets an event says a query happened, not what it read or
        // produced — which is the whole point of lineage. Two things are pinned
        // here because each has its own way of silently emitting nothing:
        //
        //  * a WRITE's target must land in `outputs`, and its inputs must NOT
        //    be filed as outputs (that would reverse every edge);
        //  * `read_collected` — the CLI/SDK read path — rides the Arrow hop,
        //    which returns batches and not a meta. If the engine's meta is not
        //    carried across that hop, this path emits dataset-less events even
        //    though extraction worked perfectly.
        let (ops, storage) = ops_with_storage();
        let id = Identity::claimed(Some("agent-a"));
        let pond = ops
            .allocate_pond(&id, Some("graph".into()), "{}", "medium", &[], true)
            .await
            .unwrap();
        ops.write_query(&id, "graph", "CREATE TABLE src(i INTEGER)")
            .await
            .unwrap();
        ops.write_query(&id, "graph", "INSERT INTO src VALUES (1)")
            .await
            .unwrap();
        ops.write_query(&id, "graph", "CREATE TABLE dst(i INTEGER)")
            .await
            .unwrap();
        ops.write_query(&id, "graph", "INSERT INTO dst SELECT i FROM src")
            .await
            .unwrap();
        ops.read_collected(&id, "graph", "SELECT i FROM dst")
            .await
            .unwrap();
        ops.flush_lineage();

        let events = events_in(&storage, &pond.pond_id);
        let namespace = format!("ducklake://{}", pond.pond_id);
        let names = |e: &Value, side: &str| -> Vec<String> {
            e[side]
                .as_array()
                .unwrap_or(&Vec::new())
                .iter()
                .map(|d| {
                    assert_eq!(
                        d["namespace"],
                        json!(namespace),
                        "a pond table must be namespaced by the pond it lives in"
                    );
                    d["name"].as_str().expect("a dataset name").to_string()
                })
                .collect()
        };

        // The INSERT … SELECT: one dataset read, a different one written.
        // Selected by its SQL, because `CREATE TABLE dst` is deliberately the
        // same JOB (same pond, same op, same target) and shares the job name.
        let insert: Vec<&Value> = events
            .iter()
            .filter(|e| {
                facet(e, "job", "sql")["query"] == json!("INSERT INTO dst SELECT i FROM src")
            })
            .collect();
        assert_eq!(insert.len(), 2, "the insert is one START/terminal pair");
        let terminal = insert
            .iter()
            .find(|e| event_type(e) == "COMPLETE")
            .expect("the write completed");
        assert_eq!(names(terminal, "outputs"), vec!["graph.main.dst"]);
        assert_eq!(names(terminal, "inputs"), vec!["graph.main.src"]);
        let start = insert
            .iter()
            .find(|e| event_type(e) == "START")
            .expect("the write started");
        assert_eq!(
            names(start, "inputs"),
            vec!["graph.main.src"],
            "a START knows what it is about to read"
        );
        assert!(
            start["outputs"].as_array().is_none_or(|o| o.is_empty()),
            "a START must not claim outputs that do not exist yet: {start:#}"
        );
        // The written dataset carries the snapshot the write PRODUCED.
        let produced = terminal["outputs"][0]["facets"]["version"]["datasetVersion"].clone();
        assert!(
            produced.as_str().is_some_and(|v| v.parse::<i64>().is_ok()),
            "a written dataset must name the snapshot it produced, got {produced}"
        );

        // The collected read — the path that had no meta to carry.
        let read = events_for_op(&events, "graph.read_query");
        assert_eq!(read.len(), 2, "one read, one event pair: {events:#?}");
        for event in &read {
            assert_eq!(
                names(event, "inputs"),
                vec!["graph.main.dst"],
                "read_collected must report what it read: {event:#}"
            );
        }
        // The read happened after that write and nothing wrote since, so the
        // version it reports must be exactly the one the write produced —
        // which is what makes the two events joinable into an edge.
        assert_eq!(
            read[1]["inputs"][0]["facets"]["version"]["datasetVersion"], produced,
            "the read must name the snapshot it actually read"
        );
    }

    #[tokio::test]
    async fn lineage_a_failed_write_still_names_its_intended_target() {
        // A write that fails AFTER binding — a constraint violation, a full
        // disk, a cancel — is exactly the event where a reader needs to know
        // what it was aiming at. The result carries no meta (there is no
        // result), so the datasets have to come from the plan instead.
        let (ops, storage) = ops_with_storage();
        let id = Identity::claimed(Some("agent-a"));
        let pond = ops
            .allocate_pond(&id, Some("strict".into()), "{}", "medium", &[], true)
            .await
            .unwrap();
        ops.write_query(&id, "strict", "CREATE TABLE t(i INTEGER)")
            .await
            .unwrap();
        ops.write_query(&id, "strict", "CREATE TABLE src(v VARCHAR)")
            .await
            .unwrap();
        ops.write_query(&id, "strict", "INSERT INTO src VALUES ('not a number')")
            .await
            .unwrap();
        // Binds fine (both tables exist and the cast type-checks); fails when
        // it RUNS, on the value.
        ops.write_query(&id, "strict", "INSERT INTO t SELECT v::INTEGER FROM src")
            .await
            .expect_err("the conversion must fail at execution");
        ops.flush_lineage();

        let events = events_in(&storage, &pond.pond_id);
        let failed = events
            .iter()
            .find(|e| event_type(e) == "FAIL")
            .expect("the failed write produced a FAIL event");
        let empty = Vec::new();
        let outputs: Vec<&str> = failed["outputs"]
            .as_array()
            .unwrap_or(&empty)
            .iter()
            .map(|d| d["name"].as_str().expect("a dataset name"))
            .collect();
        assert_eq!(
            outputs,
            vec!["strict.main.t"],
            "a FAIL event must still name the table the write meant to touch: {failed:#}"
        );
        let inputs: Vec<&str> = failed["inputs"]
            .as_array()
            .unwrap_or(&empty)
            .iter()
            .map(|d| d["name"].as_str().expect("a dataset name"))
            .collect();
        assert_eq!(
            inputs,
            vec!["strict.main.src"],
            "and what it was reading from: {failed:#}"
        );
    }

    #[tokio::test]
    async fn lineage_read_arrow_records_at_establishment() {
        // The stream's completion is unobservable on both paths (see
        // `read_arrow`'s audit-timing doc), so its events fire when the stream
        // is established and say so — a `completion` label here would be a lie
        // about what the duration measured.
        use tokio_stream::StreamExt;
        let (ops, storage) = ops_with_storage();
        let id = Identity::claimed(Some("agent-a"));
        let pond = ops
            .allocate_pond(&id, Some("streamy".into()), "{}", "medium", &[], true)
            .await
            .unwrap();
        ops.write_query(&id, "streamy", "CREATE TABLE t AS SELECT 1 AS i")
            .await
            .unwrap();
        let stream = ops
            .read_arrow(&id, "streamy", "SELECT i FROM t")
            .await
            .unwrap();
        ops.flush_lineage();

        let events = events_in(&storage, &pond.pond_id);
        let reads = events_for_op(&events, "streamy.read_arrow");
        assert_eq!(
            reads.len(),
            2,
            "both events fire at establishment, before a single batch is drained"
        );
        let query = facet(reads[1], "run", "latiq_query");
        assert_eq!(query["op"], json!("read_arrow"));
        assert_eq!(
            query["durationMeaning"],
            json!("establishment"),
            "the duration measured establishment, not the life of the stream"
        );

        // Draining afterwards must not add a third event.
        let mut batches = stream.batches;
        while let Some(b) = batches.next().await {
            b.unwrap();
        }
        ops.flush_lineage();
        assert_eq!(
            events_in(&storage, &pond.pond_id).len(),
            events.len(),
            "consuming the stream must not emit again"
        );
    }

    #[tokio::test]
    async fn lineage_start_event_is_stamped_when_the_query_began() {
        // Both events are built after the query finished, so a naive `now` on
        // the START would place the beginning of the run at its end: every
        // consumer deriving a duration from START -> COMPLETE (which is the
        // reason the spec wants the pair) would report ~0 ms for a query that
        // took a third of a second, and contradict `latiq_query.durationMs` on
        // the very same events.
        let (ops, storage) = ops_with_storage();
        let id = Identity::claimed(Some("agent-a"));
        let pond = ops
            .allocate_pond(&id, Some("slow".into()), "{}", "medium", &[], true)
            .await
            .unwrap();
        // ~300 ms in a debug build: comfortably longer than the millisecond
        // granularity of `eventTime`, so a stamped-at-the-end START cannot pass
        // this by accident.
        ops.read_query(
            &id,
            "slow",
            "SELECT count(DISTINCT i) FROM range(3000000) t(i)",
        )
        .await
        .unwrap();
        ops.flush_lineage();

        let events = events_in(&storage, &pond.pond_id);
        assert_eq!(events.len(), 2, "one read, one event pair");
        let reported = facet(&events[1], "run", "latiq_query")["durationMs"]
            .as_i64()
            .expect("durationMs is a number");
        assert!(
            reported >= 100,
            "the query must actually be slow or this test proves nothing, got {reported}ms"
        );

        let at = |e: &Value| {
            chrono::DateTime::parse_from_rfc3339(
                e["eventTime"].as_str().expect("eventTime is a string"),
            )
            .expect("eventTime is RFC-3339 with an explicit offset")
        };
        let spanned = at(&events[1])
            .signed_duration_since(at(&events[0]))
            .num_milliseconds();
        assert!(
            (spanned - reported).abs() <= 25,
            "START -> terminal must span the measured duration ({reported}ms), spanned {spanned}ms"
        );
    }

    #[tokio::test]
    async fn lineage_a_due_batch_reaches_disk_with_nobody_calling_flush() {
        // Queries never flush inline (the write fsyncs, and the query is on an
        // async worker), so a due batch is handed to the blocking pool and not
        // awaited. If that hand-off were dropped, events would sit in memory
        // until shutdown and every reader — the MCP tool, the sink — would see
        // an empty pond on a busy node. No flush_lineage() call in this test on
        // purpose: the point is that nobody has to make one.
        let (ops, storage) = ops_with_storage();
        let id = Identity::claimed(Some("agent-a"));
        let pond = ops
            .allocate_pond(&id, Some("busy".into()), "{}", "medium", &[], true)
            .await
            .unwrap();
        // The default batch is 64 events = 32 operations; go past it.
        for i in 0..40 {
            ops.write_query(&id, "busy", &format!("CREATE TABLE t{i}(i INTEGER)"))
                .await
                .unwrap();
        }

        // The write lands on another thread, so wait for it — bounded, and a
        // failure here means it never happened rather than that it was slow.
        let mut landed = 0;
        for _ in 0..100 {
            landed = events_in(&storage, &pond.pond_id).len();
            if landed >= 64 {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }
        assert!(
            landed >= 64,
            "a due batch must reach disk on its own; only {landed} events did"
        );
    }

    #[tokio::test]
    async fn lineage_concurrent_emits_share_one_cached_writer() {
        // What this pins: a pond gets ONE cached writer however many emits race
        // to create it (the location is resolved outside the registry lock, so
        // several can build one and only one may be installed), and no emit
        // loses an event to the contention. It does not distinguish "keep the
        // winner" from "clobber the winner" — both leave one entry — because
        // that difference is not observable through the public surface; the
        // `or_insert` is what makes it the winner.
        let (ops, storage) = ops_with_storage();
        let id = Identity::claimed(Some("agent-a"));
        let pond = ops
            .allocate_pond(&id, Some("racy".into()), "{}", "medium", &[], true)
            .await
            .unwrap();
        let mut tasks = Vec::new();
        for i in 0..16 {
            let ops = ops.clone();
            let id = id.clone();
            tasks.push(tokio::spawn(async move {
                ops.write_query(&id, "racy", &format!("CREATE TABLE t{i}(i INTEGER)"))
                    .await
            }));
        }
        for task in tasks {
            task.await.expect("no panic").expect("the write succeeds");
        }
        ops.flush_lineage();

        assert_eq!(
            ops.lineage_writer_count(),
            1,
            "one pond, one writer, however many emits raced for it"
        );
        assert_eq!(
            events_in(&storage, &pond.pond_id).len(),
            32,
            "every concurrent write is recorded exactly once"
        );
    }

    #[tokio::test]
    async fn lineage_dropping_the_pond_evicts_its_writer() {
        // The writer is keyed by pond and lives for the process; without
        // eviction the map leaks an entry per dropped pond, and a writer that
        // outlived its pond would later flush into a deleted directory.
        let (ops, storage) = ops_with_storage();
        let id = Identity::claimed(Some("agent-a"));
        let pond = ops
            .allocate_pond(&id, Some("ephemeral".into()), "{}", "medium", &[], true)
            .await
            .unwrap();
        ops.write_query(&id, "ephemeral", "CREATE TABLE t(i INTEGER)")
            .await
            .unwrap();
        // Captured before the drop: afterwards the pond has no location to
        // resolve the path from.
        let dir = lineage_dir(&storage, &pond.pond_id).expect("the pond has storage");
        assert_eq!(
            ops.lineage_writer_count(),
            1,
            "the query must have built a writer, or the eviction below proves nothing"
        );

        ops.drop_pond(&id, "ephemeral", true).await.unwrap();
        assert_eq!(
            ops.lineage_writer_count(),
            0,
            "dropping the pond must evict its writer"
        );
        assert!(
            !dir.exists(),
            "the pond's files, lineage included, are reaped with it"
        );
    }

    // ------------------------------------------------------- compliance

    const CORE_URI: &str = "https://openlineage.io/spec/2-0-2/OpenLineage.json";
    // The vendored schemas live in `latiq-lineage/spec/` and stay a single
    // copy: this reaches across to them rather than duplicating the files,
    // because two copies of a spec drift and only one of them would be wrong.
    const CORE: &str = include_str!("../../latiq-lineage/spec/OpenLineage-2-0-2.json");

    /// The schema for every facet the PRODUCTION emitter can attach. A facet
    /// that appears on a real event without an entry here fails the test —
    /// which is the point: `latiq-lineage`'s own compliance test walks a
    /// fixture it builds itself and so cannot see what `ops.rs` actually emits.
    fn facet_schemas() -> Vec<(&'static str, &'static str)> {
        vec![
            (
                "sql",
                include_str!("../../latiq-lineage/spec/facets/SQLJobFacet-1-0-1.json"),
            ),
            (
                "jobType",
                include_str!("../../latiq-lineage/spec/facets/JobTypeJobFacet-2-0-3.json"),
            ),
            (
                "errorMessage",
                include_str!("../../latiq-lineage/spec/facets/ErrorMessageRunFacet-1-0-1.json"),
            ),
            (
                "version",
                include_str!(
                    "../../latiq-lineage/spec/facets/DatasetVersionDatasetFacet-1-0-1.json"
                ),
            ),
            (
                "processing_engine",
                include_str!("../../latiq-lineage/spec/facets/ProcessingEngineRunFacet-1-1-1.json"),
            ),
            (
                "latiq_identity",
                include_str!("../../latiq-lineage/spec/facets/1-0-0/LatiqIdentityFacet.json"),
            ),
            (
                "latiq_pond",
                include_str!("../../latiq-lineage/spec/facets/1-0-0/LatiqPondFacet.json"),
            ),
            (
                "latiq_query",
                include_str!("../../latiq-lineage/spec/facets/1-0-0/LatiqQueryFacet.json"),
            ),
        ]
    }

    fn core_registry() -> jsonschema::Registry<'static> {
        let core: Value = serde_json::from_str(CORE).expect("vendored core schema parses");
        jsonschema::Registry::new()
            .add(CORE_URI, jsonschema::Resource::from_contents(core))
            .expect("core schema URI is valid")
            .prepare()
            .expect("registry builds")
    }

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

    #[tokio::test]
    async fn lineage_events_from_the_query_path_are_valid_and_fully_stamped() {
        // Missing `_producer`/`_schemaURL` is one of only two hard OpenLineage
        // rejection causes, and a fixture-based test cannot catch a facet the
        // PRODUCTION emitter forgets to stamp. So: take events off the disk of
        // a pond that ran real queries, and hold them to the real spec.
        let (ops, storage) = ops_with_storage();
        let id = Identity::verified("svc-orchestrator", "https://idp.example", Some("planner-7"));
        let pond = ops
            .allocate_pond(&id, Some("compliant".into()), "{}", "medium", &[], true)
            .await
            .unwrap();
        ops.write_query(&id, "compliant", "INSERT INTO absent VALUES (1)")
            .await
            .expect_err("no such table — so the error facet is exercised too");
        ops.write_query(&id, "compliant", "CREATE TABLE t AS SELECT 1 AS i")
            .await
            .unwrap();
        ops.read_query(&id, "compliant", "SELECT i FROM t")
            .await
            .unwrap();
        ops.flush_lineage();

        let registry = core_registry();
        let envelope = validator(
            &registry,
            json!({ "$ref": format!("{CORE_URI}#/$defs/RunEvent") }),
        );

        let events = events_in(&storage, &pond.pond_id);
        assert!(
            events.len() >= 6,
            "three operations must have produced three event pairs, got {}",
            events.len()
        );
        let mut seen: BTreeSet<String> = BTreeSet::new();
        for event in &events {
            assert_valid(&envelope, event, "an event from the query path");
            let facets = all_facets(event);
            assert!(
                facets.len() >= 4,
                "an emitted event carried almost no facets: {event:#}"
            );
            for (owner, key, payload) in facets {
                for field in ["_producer", "_schemaURL"] {
                    let uri = payload
                        .get(field)
                        .and_then(Value::as_str)
                        .unwrap_or_else(|| {
                            panic!("{owner} facet `{key}` is missing {field}: {payload:#}")
                        });
                    assert!(
                        uri.starts_with("https://"),
                        "{owner} facet `{key}` {field} must be an absolute URI, got {uri:?}"
                    );
                }
                let (_, schema) = facet_schemas()
                    .into_iter()
                    .find(|(k, _)| *k == key)
                    .unwrap_or_else(|| {
                        panic!("emitted facet `{key}` on {owner} has no vendored schema")
                    });
                let v = validator(
                    &registry,
                    serde_json::from_str(schema).expect("facet schema parses"),
                );
                assert_valid(
                    &v,
                    &json!({ key.clone(): payload }),
                    &format!("{owner} facet `{key}`"),
                );
                seen.insert(key);
            }
        }
        // Anti-vacuity, and a completeness pin: these are the facets the
        // emitter is supposed to stamp on every run, plus the error facet the
        // failed write above exists to produce.
        for required in [
            "latiq_identity",
            "latiq_pond",
            "latiq_query",
            "processing_engine",
            "sql",
            "jobType",
            "errorMessage",
        ] {
            assert!(
                seen.contains(required),
                "the query path never emitted the `{required}` facet; saw {seen:?}"
            );
        }
    }
}
