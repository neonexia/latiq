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
        ops_with_sink(None)
    }

    /// `ops_with_storage`, with the optional OpenLineage HTTP backend attached.
    pub(super) fn ops_with_sink(
        sink: Option<Arc<dyn latiq_lineage::EventSink>>,
    ) -> (AgentOps, Arc<TempFs>) {
        let (ops, storage, _registry) = ops_with_registry(sink);
        (ops, storage)
    }

    /// `ops_with_sink`, keeping the registry too — registering an external
    /// catalog is an operator act, and `AgentOps` has no method for it.
    pub(super) fn ops_with_registry(
        sink: Option<Arc<dyn latiq_lineage::EventSink>>,
    ) -> (AgentOps, Arc<TempFs>, Registry) {
        let registry = Registry::open(None).unwrap();
        registry
            .register_node(
                "node-a",
                "http://127.0.0.1:8080/mcp",
                "http://127.0.0.1:9092",
                100,
            )
            .unwrap();
        let control = Arc::new(RegistryControlPlane::new(registry.clone()));
        let storage = Arc::new(TempFs::new());
        let engine = Arc::new(DuckEngine::new());
        let mut ops = AgentOps::new(control, storage.clone(), engine, AgentConfig::default());
        if let Some(sink) = sink {
            ops = ops.with_lineage_sink(sink);
        }
        (ops, storage, registry)
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

    /// Every event the pond has on disk as the RAW LINE it was written as, in
    /// the same order as `events_in`. The byte form matters where `events_in`'s
    /// parsed form does not: what a backend receives has to be what the files
    /// hold, and a `Value` comparison would hide a re-serialization.
    pub(super) fn lines_in(storage: &TempFs, pond_id: &str) -> Vec<String> {
        let Some(dir) = lineage_dir(storage, pond_id) else {
            return Vec::new();
        };
        let Ok(entries) = std::fs::read_dir(&dir) else {
            return Vec::new();
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
        names
            .iter()
            .flat_map(|name| {
                std::fs::read_to_string(dir.join(name))
                    .expect("event file readable")
                    .lines()
                    .map(str::to_string)
                    .collect::<Vec<_>>()
            })
            .collect()
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
    async fn lineage_datasets_carry_their_columns_and_the_job_name_says_the_pond_once() {
        // Both defects a live Marquez surfaced, on the same events because they
        // are the same event:
        //
        //  * every dataset came back with `"fields": []`, so a table was a node
        //    you could click into and learn nothing from;
        //  * every job was `shop.write_query.shop.main.…` — the pond once as
        //    the pond and once as the DuckLake catalog alias, which IS the pond
        //    name.
        let (ops, storage) = ops_with_storage();
        let id = Identity::claimed(Some("agent-a"));
        let pond = ops
            .allocate_pond(&id, Some("shop".into()), "{}", "medium", &[], true)
            .await
            .unwrap();
        ops.write_query(
            &id,
            "shop",
            "CREATE TABLE orders(id INTEGER, customer VARCHAR, amount DECIMAL(10,2))",
        )
        .await
        .unwrap();
        ops.write_query(&id, "shop", "INSERT INTO orders VALUES (1,'ada',9.99)")
            .await
            .unwrap();
        ops.write_query(
            &id,
            "shop",
            "CREATE TABLE customer_totals AS \
             SELECT customer, sum(amount) AS total FROM orders GROUP BY customer",
        )
        .await
        .unwrap();
        ops.flush_lineage();

        let events = events_in(&storage, &pond.pond_id);
        let ctas = events
            .iter()
            .find(|e| {
                event_type(e) == "COMPLETE"
                    && facet(e, "job", "sql")["query"]
                        .as_str()
                        .is_some_and(|q| q.contains("customer_totals"))
            })
            .expect("the CTAS completed");

        // The name a consumer keys the job on: the pond, the op, and the table
        // WITHOUT the catalog segment that only repeats the pond.
        assert_eq!(
            ctas["job"]["name"],
            json!("shop.write_query.main.customer_totals"),
            "the pond must appear once in a job name: {ctas:#}"
        );
        // The DATASET name is unchanged and still fully qualified — Marquez
        // keys datasets on it, and it is correct as it is.
        assert_eq!(
            ctas["outputs"][0]["name"],
            json!("shop.main.customer_totals"),
            "only the job name loses the catalog, never the dataset"
        );

        let columns = |ds: &Value| -> Value { ds["facets"]["schema"]["fields"].clone() };
        assert_eq!(
            columns(&ctas["outputs"][0]),
            json!([
                { "name": "customer", "type": "VARCHAR" },
                { "name": "total", "type": "DECIMAL(38,2)" },
            ]),
            "the table the run produced must carry its columns: {ctas:#}"
        );
        assert_eq!(
            columns(&ctas["inputs"][0]),
            json!([
                { "name": "id", "type": "INTEGER" },
                { "name": "customer", "type": "VARCHAR" },
                { "name": "amount", "type": "DECIMAL(10,2)" },
            ]),
            "and so must the table it read: {ctas:#}"
        );
        // Every facet carries the two mandatory base fields — a facet that
        // skipped `Facet::stamp` would be rejected by a real consumer.
        let schema_facet = &ctas["outputs"][0]["facets"]["schema"];
        assert!(
            schema_facet["_producer"].is_string()
                && schema_facet["_schemaURL"]
                    .as_str()
                    .is_some_and(|u| u.contains("SchemaDatasetFacet")),
            "the schema facet must identify itself: {schema_facet:#}"
        );

        // An EXTERNAL dataset carries no `schema` facet at all — not an empty
        // one. A consumer reads a missing facet as "not stated" and an empty
        // `fields` as "this table has no columns", and only the first is true.
        let tmp = tempfile::tempdir().unwrap();
        let parquet = tmp.path().join("probe.parquet");
        let parquet = parquet.display();
        ops.write_query(
            &id,
            "shop",
            &format!("COPY (SELECT 1 AS id) TO '{parquet}' (FORMAT PARQUET)"),
        )
        .await
        .unwrap();
        ops.read_query(
            &id,
            "shop",
            &format!("SELECT * FROM read_parquet('{parquet}')"),
        )
        .await
        .unwrap();
        ops.flush_lineage();
        let events = events_in(&storage, &pond.pond_id);
        let external = events
            .iter()
            .find(|e| {
                event_type(e) == "COMPLETE"
                    && e["inputs"][0]["namespace"] == json!("file")
                    && facet(e, "job", "sql")["query"]
                        .as_str()
                        .is_some_and(|q| q.contains("read_parquet"))
            })
            .expect("the parquet read is in the trail");
        assert!(
            external["inputs"][0]["facets"].get("schema").is_none(),
            "an external dataset's columns are not ours to state: {external:#}"
        );

        // A read carries the input's columns too, on both events of the run.
        ops.read_query(&id, "shop", "SELECT customer FROM customer_totals")
            .await
            .unwrap();
        ops.flush_lineage();
        let events = events_in(&storage, &pond.pond_id);
        let read: Vec<&Value> = events
            .iter()
            .filter(|e| {
                facet(e, "job", "sql")["query"] == json!("SELECT customer FROM customer_totals")
            })
            .collect();
        assert_eq!(read.len(), 2, "one read, one event pair");
        for event in &read {
            assert_eq!(
                event["job"]["name"],
                json!("shop.read_query.main.customer_totals")
            );
            assert_eq!(
                columns(&event["inputs"][0]),
                json!([
                    { "name": "customer", "type": "VARCHAR" },
                    { "name": "total", "type": "DECIMAL(38,2)" },
                ]),
                "a read's input must say what it read: {event:#}"
            );
        }
    }

    /// A local DuckLake catalog with one table, as an operator would register
    /// it: `(metadata_path, data_path)`. A throwaway in-memory DuckDB builds
    /// it, so it is a real external source rather than a fixture — the plan has
    /// to bind against something that is genuinely attached.
    fn seed_ducklake(dir: &std::path::Path) -> (String, String) {
        let meta = dir.join("meta.duckdb");
        let data = dir.join("data");
        std::fs::create_dir_all(&data).unwrap();
        let conn = duckdb::Connection::open_in_memory().unwrap();
        conn.execute_batch(&format!(
            "INSTALL ducklake; LOAD ducklake;
             ATTACH 'ducklake:{}' AS ext (DATA_PATH '{}');
             CREATE TABLE ext.widgets AS
               SELECT * FROM (VALUES (1,'gear',9.99),(2,'bolt',0.99)) t(id,name,price);",
            meta.display(),
            data.display(),
        ))
        .unwrap();
        (meta.display().to_string(), data.display().to_string())
    }

    #[tokio::test]
    async fn lineage_catalog_pull_names_the_external_source_and_the_pond_table() {
        // The pull is the ONE op whose input is not in the pond, and the edge
        // with the most provenance value: the catalog is detached before the
        // call returns, so after this nothing in the pond — not the catalog,
        // not the snapshots — remembers where its rows came from. If the pull
        // emitted no event, "how did this table get here" would be answerable
        // for every table except the imported ones.
        //
        // Both sides are asserted, because each fails on its own: an input left
        // under the pond's namespace would claim the lakehouse's table as ours
        // and make our events unjoinable with the source's own lineage, and a
        // missing output loses the other end of the edge entirely.
        let tmp = tempfile::tempdir().unwrap();
        let (metadata_path, data_path) = seed_ducklake(tmp.path());
        let (ops, storage, registry) = ops_with_registry(None);
        registry
            .add_catalog(&latiq_control_plane::registry::CatalogRow {
                name: "ext".into(),
                r#type: "ducklake".into(),
                params: std::collections::BTreeMap::from([
                    ("metadata_path".to_string(), metadata_path.clone()),
                    ("data_path".to_string(), data_path),
                ]),
                description: "local ducklake".into(),
                tags: vec![],
                created_by: String::new(),
                created_at: String::new(),
            })
            .unwrap();

        let id = Identity::claimed(Some("agent-a"));
        let pond = ops
            .allocate_pond(&id, Some("shop".into()), "{}", "medium", &[], true)
            .await
            .unwrap();
        ops.catalog_pull(
            &id,
            "shop",
            "ext",
            "CREATE TABLE cheap AS SELECT id, name FROM ext.main.widgets WHERE price < 10",
            std::collections::BTreeMap::new(),
        )
        .await
        .unwrap();
        ops.flush_lineage();

        let events = events_in(&storage, &pond.pond_id);
        let pull = events_for_op(&events, "shop.catalog_pull");
        assert_eq!(
            pull.len(),
            2,
            "one pull, one START/terminal pair: {events:#?}"
        );
        let terminal = pull
            .iter()
            .find(|e| event_type(e) == "COMPLETE")
            .expect("the pull completed");

        // The external side keeps the SOURCE's own locator as its namespace —
        // the alias `ext` is a pond-local registry name nobody else can join on.
        assert_eq!(
            terminal["inputs"],
            json!([{
                "namespace": format!("ducklake:{metadata_path}"),
                "name": "main.widgets",
                "facets": terminal["inputs"][0]["facets"],
            }]),
            "the pull must name the external table it read: {terminal:#}"
        );
        // Free of charge from a DuckLake source: the snapshot it was read at.
        let read_at = &terminal["inputs"][0]["facets"]["version"]["datasetVersion"];
        assert!(
            read_at.as_str().is_some_and(|v| v.parse::<i64>().is_ok()),
            "an external DuckLake input names the snapshot it was read at, got {read_at}"
        );
        // …and the table it produced is a pond table, namespaced by its pond.
        assert_eq!(
            terminal["outputs"][0]["namespace"],
            json!(format!("ducklake://{}", pond.pond_id))
        );
        assert_eq!(terminal["outputs"][0]["name"], json!("shop.main.cheap"));

        let start = pull
            .iter()
            .find(|e| event_type(e) == "START")
            .expect("the pull started");
        assert_eq!(
            start["inputs"][0]["name"],
            json!("main.widgets"),
            "a START knows what it is about to read"
        );
        assert!(
            start["outputs"].as_array().is_none_or(|o| o.is_empty()),
            "a START must not claim a table that does not exist yet: {start:#}"
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
                "schema",
                include_str!("../../latiq-lineage/spec/facets/SchemaDatasetFacet-1-1-1.json"),
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
            "schema",
        ] {
            assert!(
                seen.contains(required),
                "the query path never emitted the `{required}` facet; saw {seen:?}"
            );
        }
    }

    // ------------------------------------------------ the optional HTTP sink

    /// A minimal HTTP/1.1 receiver that records every POST body verbatim.
    ///
    /// A hand-rolled `TcpListener` rather than a web framework on purpose: the
    /// assertion below is about BYTES, and anything that parsed the JSON and
    /// handed back a re-serialized form would hide the exact drift the test
    /// exists to catch. It answers 200 and keeps the connection open, because
    /// `reqwest` pools and will send several events down one socket.
    async fn capture_server() -> (String, Arc<std::sync::Mutex<Vec<String>>>) {
        capture_server_with("HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n", None).await
    }

    /// `capture_server`, answering with `response` after an optional delay.
    /// The delay is what makes the drain test deterministic (without a drain,
    /// the events are provably still in flight when the assertion runs) and
    /// `response` is what lets one server stand in for a backend that rejects
    /// everything.
    async fn capture_server_with(
        response: &'static str,
        delay: Option<std::time::Duration>,
    ) -> (String, Arc<std::sync::Mutex<Vec<String>>>) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind capture server");
        let port = listener.local_addr().expect("local addr").port();
        let bodies: Arc<std::sync::Mutex<Vec<String>>> =
            Arc::new(std::sync::Mutex::new(Vec::new()));
        let accepted = bodies.clone();
        tokio::spawn(async move {
            while let Ok((mut stream, _)) = listener.accept().await {
                let bodies = accepted.clone();
                tokio::spawn(async move {
                    use tokio::io::{AsyncReadExt, AsyncWriteExt};
                    let mut buf: Vec<u8> = Vec::new();
                    let mut chunk = [0u8; 4096];
                    loop {
                        // Headers, then exactly Content-Length bytes of body.
                        let head_end = loop {
                            if let Some(i) = buf.windows(4).position(|w| w == b"\r\n\r\n") {
                                break i + 4;
                            }
                            match stream.read(&mut chunk).await {
                                Ok(0) | Err(_) => return,
                                Ok(n) => buf.extend_from_slice(&chunk[..n]),
                            }
                        };
                        let head = String::from_utf8_lossy(&buf[..head_end]).to_lowercase();
                        let len: usize = head
                            .lines()
                            .find_map(|l| l.strip_prefix("content-length:"))
                            .and_then(|v| v.trim().parse().ok())
                            .unwrap_or(0);
                        while buf.len() < head_end + len {
                            match stream.read(&mut chunk).await {
                                Ok(0) | Err(_) => return,
                                Ok(n) => buf.extend_from_slice(&chunk[..n]),
                            }
                        }
                        let body =
                            String::from_utf8_lossy(&buf[head_end..head_end + len]).into_owned();
                        bodies.lock().expect("capture lock").push(body);
                        buf.drain(..head_end + len);
                        if let Some(delay) = delay {
                            tokio::time::sleep(delay).await;
                        }
                        if stream.write_all(response.as_bytes()).await.is_err() {
                            return;
                        }
                    }
                });
            }
        });
        (format!("http://127.0.0.1:{port}/api/v1/lineage"), bodies)
    }

    /// Wait until the receiver has `want` bodies, or give up. The POSTs happen
    /// on a task nobody awaits — that is the whole design — so a test has to
    /// wait for them, and a test that just slept would be flaky in the
    /// direction that hides a bug.
    async fn wait_for_posts(
        bodies: &Arc<std::sync::Mutex<Vec<String>>>,
        want: usize,
    ) -> Vec<String> {
        for _ in 0..200 {
            let got = bodies.lock().expect("capture lock").clone();
            if got.len() >= want {
                return got;
            }
            tokio::time::sleep(std::time::Duration::from_millis(25)).await;
        }
        bodies.lock().expect("capture lock").clone()
    }

    /// An endpoint that accepts the connection and then never answers — the
    /// worst case for a sink, and the one a plain refused connection does not
    /// exercise at all. The listener is returned so the caller keeps it alive.
    async fn hung_endpoint() -> (String, tokio::net::TcpListener) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind hung endpoint");
        let port = listener.local_addr().expect("local addr").port();
        (format!("http://127.0.0.1:{port}/api/v1/lineage"), listener)
    }

    /// A port nothing is listening on. Bound and released, so it is a port the
    /// OS just confirmed is free rather than one a hard-coded guess might hit.
    async fn dead_endpoint() -> String {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind to find a free port");
        let port = listener.local_addr().expect("local addr").port();
        drop(listener);
        format!("http://127.0.0.1:{port}/api/v1/lineage")
    }

    #[tokio::test]
    async fn lineage_sink_failure_never_fails_a_query() {
        // A sink that can break queries is worse than no sink. The endpoint is
        // dead, so every POST fails connecting — and the queries must still
        // return their answers, and the pond's own files must still hold the
        // events. `submitted()` is the anti-vacuity guard: without it a
        // `with_lineage_sink` that quietly dropped the sink on the floor would
        // pass this test while proving nothing.
        let sink =
            Arc::new(latiq_lineage::HttpSink::new(&dead_endpoint().await).expect("valid url"));
        let (ops, storage) = ops_with_sink(Some(sink.clone() as Arc<dyn latiq_lineage::EventSink>));
        let id = Identity::claimed(Some("agent-a"));
        let pond = ops
            .allocate_pond(&id, Some("deadsink".into()), "{}", "medium", &[], true)
            .await
            .expect("allocate is unaffected by a dead lineage backend");
        ops.write_query(&id, "deadsink", "CREATE TABLE t AS SELECT 42 AS i")
            .await
            .expect("the write must succeed with the backend down");
        let read = ops
            .read_query(&id, "deadsink", "SELECT i FROM t")
            .await
            .expect("the read must succeed with the backend down");
        assert_eq!(
            read.rows[0][0],
            json!(42),
            "the query must return its real answer, not a degraded one"
        );

        ops.flush_lineage();
        let events = events_in(&storage, &pond.pond_id);
        assert!(
            events.len() >= 4,
            "the local files must still hold both operations' events, got {}",
            events.len()
        );
        let complete: Vec<&Value> = events
            .iter()
            .filter(|e| e["eventType"] == json!("COMPLETE"))
            .collect();
        assert_eq!(
            complete.len(),
            2,
            "both operations must have completed locally despite the dead sink"
        );
        assert!(
            sink.submitted() >= 4,
            "the sink must really have been wired and really have been handed \
             every event -- otherwise the survival above proves nothing; got {}",
            sink.submitted()
        );
    }

    #[tokio::test]
    async fn lineage_sink_posts_the_event_verbatim() {
        // Byte-identical to what the files hold and to what `get_lineage`
        // returns. If the wire form and the stored form can drift, "OpenLineage
        // compliant" means nothing: the events a consumer validated against the
        // spec would not be the events its backend received.
        let (url, bodies) = capture_server().await;
        let (ops, storage) = ops_with_sink(Some(Arc::new(
            latiq_lineage::HttpSink::new(&url).expect("valid url"),
        ) as Arc<dyn latiq_lineage::EventSink>));
        let id = Identity::verified("svc-a", "https://idp.example", Some("planner-7"));
        let pond = ops
            .allocate_pond(&id, Some("posted".into()), "{}", "medium", &[], true)
            .await
            .unwrap();
        ops.write_query(&id, "posted", "CREATE TABLE t AS SELECT 1 AS i")
            .await
            .unwrap();
        ops.read_query(&id, "posted", "SELECT i FROM t")
            .await
            .unwrap();
        ops.flush_lineage();

        let mut stored = lines_in(&storage, &pond.pond_id);
        assert!(
            stored.len() >= 4,
            "two operations must have written two event pairs, got {}",
            stored.len()
        );
        let mut posted = wait_for_posts(&bodies, stored.len()).await;
        stored.sort();
        posted.sort();
        assert_eq!(
            posted, stored,
            "every posted body must be byte-identical to the line stored in the pond"
        );

        // ... and to what the agent reads back, which is the form a consumer
        // actually validates. The reader hands lines on verbatim, so this ties
        // the wire form to the tool's answer rather than to the file format.
        let page = ops
            .get_lineage(&id, "posted", 500, None, None)
            .await
            .expect("the pond records lineage");
        let mut returned: Vec<String> = page
            .events
            .iter()
            .map(|e| serde_json::to_string(e).expect("re-serializable"))
            .collect();
        let mut posted_values: Vec<String> = posted
            .iter()
            .map(|b| {
                serde_json::to_string(
                    &serde_json::from_str::<Value>(b).expect("a posted body is JSON"),
                )
                .expect("re-serializable")
            })
            .collect();
        returned.sort();
        posted_values.sort();
        assert_eq!(
            posted_values, returned,
            "the backend must receive exactly the events get_lineage returns"
        );
    }

    #[tokio::test]
    async fn lineage_sink_survives_a_backend_that_rejects_every_event() {
        // Connection-refused is the easy failure. A backend that is UP, accepts
        // the body and answers 500 exercises a different branch — the response
        // path rather than the transport one — and it is the shape a
        // misconfigured Marquez actually takes. Nothing is retried, so the
        // events are gone from the backend and intact on disk.
        let (url, bodies) = capture_server_with(
            "HTTP/1.1 500 Internal Server Error\r\nContent-Length: 0\r\n\r\n",
            None,
        )
        .await;
        let sink = Arc::new(latiq_lineage::HttpSink::new(&url).expect("valid url"));
        let (ops, storage) = ops_with_sink(Some(sink.clone() as Arc<dyn latiq_lineage::EventSink>));
        let id = Identity::claimed(Some("agent-a"));
        let pond = ops
            .allocate_pond(&id, Some("rejected".into()), "{}", "medium", &[], true)
            .await
            .unwrap();
        ops.write_query(&id, "rejected", "CREATE TABLE t AS SELECT 7 AS i")
            .await
            .expect("a 500 from the backend must not fail the write");
        let read = ops
            .read_query(&id, "rejected", "SELECT i FROM t")
            .await
            .expect("a 500 from the backend must not fail the read");
        assert_eq!(
            read.rows[0][0],
            json!(7),
            "the query returns its real answer"
        );

        ops.flush_lineage();
        let stored = lines_in(&storage, &pond.pond_id);
        assert!(
            stored.len() >= 4,
            "the local files must hold both operations' events, got {}",
            stored.len()
        );
        // The backend really did receive and reject them: without this the test
        // would pass against a sink that posted nothing at all.
        let posted = wait_for_posts(&bodies, stored.len()).await;
        assert!(
            posted.len() >= 4,
            "every event must have been offered to the backend, got {}",
            posted.len()
        );
        assert_eq!(
            sink.dropped(),
            0,
            "a rejected POST is discarded by the poster, never counted as a queue overflow"
        );
    }

    #[tokio::test]
    async fn lineage_sink_survives_a_backend_that_never_answers() {
        // The failure the per-request timeout exists for, and the one a refused
        // connection cannot show: the socket connects, the body is accepted,
        // and nothing ever comes back. The queries must return at their normal
        // speed regardless — the whole design is that nobody awaits the POST.
        let (url, _listener) = hung_endpoint().await;
        let sink = Arc::new(latiq_lineage::HttpSink::new(&url).expect("valid url"));
        let (ops, storage) = ops_with_sink(Some(sink.clone() as Arc<dyn latiq_lineage::EventSink>));
        let id = Identity::claimed(Some("agent-a"));

        // Comfortably under the sink's own 10s per-POST timeout, so a query
        // that waited on the backend could not fit inside this.
        let work = tokio::time::timeout(std::time::Duration::from_secs(5), async {
            let pond = ops
                .allocate_pond(&id, Some("hung".into()), "{}", "medium", &[], true)
                .await
                .unwrap();
            ops.write_query(&id, "hung", "CREATE TABLE t AS SELECT 5 AS i")
                .await
                .expect("write");
            let read = ops
                .read_query(&id, "hung", "SELECT i FROM t")
                .await
                .expect("read");
            assert_eq!(read.rows[0][0], json!(5));
            pond
        })
        .await
        .expect("a hung backend must not delay the query path");

        ops.flush_lineage();
        assert!(
            lines_in(&storage, &work.pond_id).len() >= 4,
            "the local files must be complete while the backend hangs"
        );
        assert!(
            sink.submitted() >= 4,
            "anti-vacuity: the sink must really have been handed the events it is hanging on"
        );
    }

    #[tokio::test]
    async fn lineage_sink_drain_posts_the_backlog_before_the_node_exits() {
        // What SIGTERM would otherwise throw away. The backend answers slowly,
        // so at the moment the queries return the backlog is provably still in
        // flight; `drain` is the only thing that gets it out before the process
        // ends. Without the drain seam this asserts a race and fails.
        let (url, bodies) = capture_server_with(
            "HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n",
            Some(std::time::Duration::from_millis(200)),
        )
        .await;
        let sink = Arc::new(latiq_lineage::HttpSink::new(&url).expect("valid url"));
        let (ops, storage) = ops_with_sink(Some(sink.clone() as Arc<dyn latiq_lineage::EventSink>));
        let id = Identity::claimed(Some("agent-a"));
        let pond = ops
            .allocate_pond(&id, Some("draining".into()), "{}", "medium", &[], true)
            .await
            .unwrap();
        ops.write_query(&id, "draining", "CREATE TABLE t AS SELECT 1 AS i")
            .await
            .unwrap();
        ops.read_query(&id, "draining", "SELECT i FROM t")
            .await
            .unwrap();
        ops.flush_lineage();

        let stored = lines_in(&storage, &pond.pond_id);
        assert!(stored.len() >= 4, "two operations, two event pairs");
        let in_flight = bodies.lock().expect("capture lock").len();
        assert!(
            in_flight < stored.len(),
            "the backlog must still be in flight when the shutdown begins, or this test \
             cannot tell a drain from luck: {in_flight} of {} already posted",
            stored.len()
        );

        // The node's shutdown budget, as `shutdown_lineage` passes it.
        ops.drain_lineage_sink(std::time::Duration::from_secs(5))
            .await;
        let posted = bodies.lock().expect("capture lock").clone();
        assert_eq!(
            posted.len(),
            stored.len(),
            "the drain must post the whole backlog before the node exits"
        );
        let mut posted = posted;
        let mut stored = stored;
        posted.sort();
        stored.sort();
        assert_eq!(
            posted, stored,
            "and post it verbatim, exactly as `submit` would have"
        );
    }
}
