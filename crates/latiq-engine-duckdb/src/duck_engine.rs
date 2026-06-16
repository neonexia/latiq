//! `DuckEngine` — the DuckDB + DuckLake implementation of `QueryEngine`.
//!
//! **One DuckDB instance per pond** (spec §5, Decision 3): a single connection
//! per pond, reused across queries and guarded by a mutex. This is what makes
//! concurrent multi-agent writes correct — all commits to a pond's DuckLake
//! catalog go through one DuckDB handle, so DuckLake's transactional model
//! serializes them (instead of independent instances racing on the catalog file).
//!
//! Cancellation uses the spike-confirmed `Connection::interrupt_handle()`: a
//! watcher thread interrupts the running statement when the `AbortToken` is
//! cancelled, and exits when the operation completes.
use crate::exec::{run_explain, run_read, run_read_arrow, run_write};
use crate::instance::PondInstance;
use latiq_common::Identity;
use latiq_engine::{
    AbortToken, ArrowSink, EngineError, ExplainResult, QueryEngine, QueryResult, SchemaSummary,
    TableInfo,
};
use latiq_storage::PondLocation;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

#[derive(Default)]
pub struct DuckEngine {
    /// Per-pond DuckDB instances, keyed by catalog URI. Each is mutex-guarded so
    /// queries on a pond serialize through its single connection.
    instances: Mutex<HashMap<String, Arc<Mutex<PondInstance>>>>,
}

/// Lock a mutex, recovering the guard if a prior holder panicked. A poisoned
/// pond/instances mutex must not brick the pond — the DuckDB connection (or map)
/// is still usable, and the next statement either succeeds or returns a normal
/// engine error.
fn lock_recover<T>(m: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    m.lock().unwrap_or_else(|e| e.into_inner())
}

impl DuckEngine {
    pub fn new() -> Self {
        Self::default()
    }

    /// Get the pond's instance, opening (and caching) it on first use.
    fn instance(&self, loc: &PondLocation) -> Result<Arc<Mutex<PondInstance>>, EngineError> {
        let mut map = lock_recover(&self.instances);
        if let Some(inst) = map.get(&loc.catalog_uri) {
            return Ok(inst.clone());
        }
        let inst = Arc::new(Mutex::new(PondInstance::open(loc)?));
        map.insert(loc.catalog_uri.clone(), inst.clone());
        Ok(inst)
    }

    /// Run a blocking engine operation with an interrupt watcher bound to `abort`.
    /// An `INTERRUPT` error is normalized to `Cancelled`.
    fn run_with_abort<T>(
        inst: &PondInstance,
        abort: &AbortToken,
        f: impl FnOnce(&PondInstance) -> Result<T, EngineError>,
    ) -> Result<T, EngineError> {
        let handle = inst.conn.interrupt_handle();
        let abort = abort.clone();
        let done = Arc::new(AtomicBool::new(false));
        let done_w = done.clone();
        let watcher = std::thread::spawn(move || loop {
            if abort.is_cancelled() {
                handle.interrupt();
                break;
            }
            if done_w.load(Ordering::Relaxed) {
                break;
            }
            std::thread::sleep(Duration::from_millis(10));
        });

        let result = f(inst);
        done.store(true, Ordering::Relaxed);
        let _ = watcher.join();

        match result {
            Err(EngineError::Engine(ref m)) if m.to_uppercase().contains("INTERRUPT") => {
                Err(EngineError::Cancelled)
            }
            other => other,
        }
    }
}

impl QueryEngine for DuckEngine {
    fn init_pond(&self, loc: &PondLocation) -> Result<(), EngineError> {
        // Opening the instance attaches the pond's DuckLake catalog (creating the
        // catalog file on first open) and validates it's usable. No Latiq objects
        // are created on top — the pond is pure DuckLake.
        let _ = self.instance(loc)?;
        Ok(())
    }

    fn read_query(
        &self,
        loc: &PondLocation,
        sql: &str,
        abort: AbortToken,
    ) -> Result<QueryResult, EngineError> {
        let inst = self.instance(loc)?;
        let guard = lock_recover(&inst);
        Self::run_with_abort(&guard, &abort, |i| run_read(i, sql))
    }

    fn read_arrow(
        &self,
        loc: &PondLocation,
        sql: &str,
        abort: AbortToken,
        sink: &mut dyn ArrowSink,
    ) -> Result<(), EngineError> {
        let inst = self.instance(loc)?;
        let guard = lock_recover(&inst);
        Self::run_with_abort(&guard, &abort, |i| run_read_arrow(i, sql, &abort, sink))
    }

    fn write_query(
        &self,
        loc: &PondLocation,
        sql: &str,
        identity: &Identity,
        abort: AbortToken,
    ) -> Result<QueryResult, EngineError> {
        let inst = self.instance(loc)?;
        let guard = lock_recover(&inst);
        Self::run_with_abort(&guard, &abort, |i| {
            run_write(i, sql, identity, &loc.catalog_name)
        })
    }

    fn explain_query(&self, loc: &PondLocation, sql: &str) -> Result<ExplainResult, EngineError> {
        let inst = self.instance(loc)?;
        let guard = lock_recover(&inst);
        run_explain(&guard, sql)
    }

    fn open_pond_count(&self) -> usize {
        lock_recover(&self.instances).len()
    }

    fn forget_pond(&self, loc: &PondLocation) {
        // Drop the cached instance so its DuckDB connection (and the open handle to
        // the pond's catalog file) is closed before storage deletes those files.
        // No-op if the pond was never opened. The Arc is dropped when the last
        // in-flight query on it finishes, closing the connection then.
        let mut map = lock_recover(&self.instances);
        map.remove(&loc.catalog_uri);
    }

    fn describe_schema(&self, loc: &PondLocation) -> Result<SchemaSummary, EngineError> {
        let inst = self.instance(loc)?;
        let guard = lock_recover(&inst);
        // Native DuckDB catalog introspection on the attached pond catalog — no
        // Latiq view in between. (This lives in the DuckDB adapter, so using
        // duckdb_tables() here is fine; a DataFusion adapter would use its own.)
        // Scope to this pond's catalog (its name); escape `'` for the literal.
        let cat = loc.catalog_name.replace('\'', "''");
        let res = run_read(
            &guard,
            &format!(
                "SELECT table_name AS name, estimated_size AS row_count, comment \
                 FROM duckdb_tables() WHERE database_name = '{cat}'"
            ),
        )?;
        let tables = res
            .rows
            .iter()
            .map(|r| TableInfo {
                name: r.first().and_then(|v| v.as_str()).unwrap_or("").to_string(),
                columns: vec![],
                row_count_estimate: r.get(1).and_then(|v| v.as_u64()).unwrap_or(0),
                comment: r.get(2).and_then(|v| v.as_str()).map(|s| s.to_string()),
            })
            .collect();
        Ok(SchemaSummary { tables })
    }

    fn pull_catalog(
        &self,
        loc: &PondLocation,
        catalog_type: &str,
        alias: &str,
        params: &std::collections::BTreeMap<String, String>,
        query: &str,
    ) -> Result<(), EngineError> {
        let inst = self.instance(loc)?;
        let guard = lock_recover(&inst);
        let plan = crate::attachers::plan(catalog_type, alias, params)?;
        attach_catalog(&guard.conn, &plan)?;
        // Run the pull query (a CREATE TABLE … in the pond's default catalog),
        // then tear the attachment down regardless of the outcome.
        let ran = guard
            .conn
            .execute_batch(query)
            .map_err(|e| EngineError::Engine(format!("pull query: {e}")));
        teardown_catalog(&guard.conn, &plan);
        ran
    }

    fn describe_catalog(
        &self,
        loc: &PondLocation,
        catalog_type: &str,
        alias: &str,
        params: &std::collections::BTreeMap<String, String>,
    ) -> Result<Vec<(String, String)>, EngineError> {
        let inst = self.instance(loc)?;
        let guard = lock_recover(&inst);
        let plan = crate::attachers::plan(catalog_type, alias, params)?;
        attach_catalog(&guard.conn, &plan)?;
        let cat = alias.replace('\'', "''");
        let listed = run_read(
            &guard,
            &format!(
                "SELECT table_schema, table_name FROM information_schema.tables \
                 WHERE table_catalog = '{cat}' ORDER BY table_schema, table_name"
            ),
        );
        teardown_catalog(&guard.conn, &plan);
        let res = listed?;
        Ok(res
            .rows
            .iter()
            .map(|r| {
                (
                    r.first().and_then(|v| v.as_str()).unwrap_or("").to_string(),
                    r.get(1).and_then(|v| v.as_str()).unwrap_or("").to_string(),
                )
            })
            .collect())
    }
}

/// LOAD the type's extensions, create its secrets, and ATTACH it.
fn attach_catalog(
    conn: &duckdb::Connection,
    plan: &crate::attachers::AttachPlan,
) -> Result<(), EngineError> {
    // If any step fails AFTER a CREATE SECRET ran (e.g. ATTACH errors on a bad
    // endpoint), the credential would otherwise linger on the reused per-pond
    // connection. Tear down on error so no secret survives a failed attach —
    // Latiq stores zero credentials (invariant 6).
    match attach_catalog_inner(conn, plan) {
        Ok(()) => Ok(()),
        Err(e) => {
            teardown_catalog(conn, plan);
            Err(e)
        }
    }
}

fn attach_catalog_inner(
    conn: &duckdb::Connection,
    plan: &crate::attachers::AttachPlan,
) -> Result<(), EngineError> {
    for s in &plan.load {
        conn.execute_batch(s)
            .map_err(|e| EngineError::Engine(format!("catalog extensions: {e}")))?;
    }
    for (_, sql) in &plan.secrets {
        conn.execute_batch(sql)
            .map_err(|e| EngineError::Engine(format!("catalog secret: {e}")))?;
    }
    conn.execute_batch(&plan.attach)
        .map_err(|e| EngineError::Engine(format!("attach: {e}")))?;
    Ok(())
}

/// Best-effort DETACH + drop the plan's secrets (run regardless of pull outcome).
fn teardown_catalog(conn: &duckdb::Connection, plan: &crate::attachers::AttachPlan) {
    for s in plan.teardown() {
        let _ = conn.execute_batch(&s);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use latiq_common::PondId;
    use latiq_storage::{PondStorage, TempFs};
    use std::time::{Duration, Instant};

    #[test]
    fn failed_attach_does_not_leak_the_secret() {
        // Regression: attach_catalog creates the credential secret BEFORE ATTACH.
        // If ATTACH fails, the secret must NOT linger on the reused pond connection
        // (Latiq stores zero credentials — invariant 6).
        let fs = TempFs::new();
        let loc = fs.create_pond(PondId::new()).unwrap();
        let inst = PondInstance::open(&loc).unwrap();
        let plan = crate::attachers::AttachPlan {
            alias: "leaktest".into(),
            load: vec![],
            secrets: vec![(
                "leak_sec".into(),
                "CREATE OR REPLACE SECRET leak_sec (TYPE s3, KEY_ID 'k', SECRET 's')".into(),
            )],
            // A ducklake attach under a non-existent directory fails after the
            // secret is created.
            attach: "ATTACH 'ducklake:/nonexistent_dir_xyz/meta.duckdb' AS leaktest \
                     (DATA_PATH '/nonexistent_dir_xyz/data')"
                .into(),
        };
        assert!(
            attach_catalog(&inst.conn, &plan).is_err(),
            "attach should fail on a bad path"
        );
        let n: i64 = inst
            .conn
            .query_row(
                "SELECT count(*) FROM duckdb_secrets() WHERE name='leak_sec'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(n, 0, "secret must not survive a failed attach");
    }

    #[test]
    fn cancels_long_running_query_and_recovers() {
        let fs = TempFs::new();
        let loc = fs.create_pond(PondId::new()).unwrap();
        let eng = DuckEngine::new();
        eng.init_pond(&loc).unwrap();

        let abort = AbortToken::new();
        let abort2 = abort.clone();
        std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(200));
            abort2.cancel();
        });
        let t0 = Instant::now();
        let res = eng.read_query(
            &loc,
            "SELECT count(*) FROM range(100000000000) t1, range(1000) t2",
            abort,
        );
        assert!(
            matches!(res, Err(EngineError::Cancelled)),
            "expected Cancelled, got {res:?}"
        );
        assert!(
            t0.elapsed() < Duration::from_secs(5),
            "abort must be prompt, took {:?}",
            t0.elapsed()
        );

        let ok = eng
            .read_query(&loc, "SELECT 1 AS x", AbortToken::new())
            .unwrap();
        assert_eq!(ok.rows[0][0], serde_json::json!(1));
    }

    fn instance_count(eng: &DuckEngine) -> usize {
        eng.instances.lock().unwrap().len()
    }

    #[test]
    fn recovers_from_poisoned_mutex() {
        use std::panic::{catch_unwind, AssertUnwindSafe};
        let fs = TempFs::new();
        let loc = fs.create_pond(PondId::new()).unwrap();
        let eng = DuckEngine::new();
        eng.init_pond(&loc).unwrap();

        // Poison the instances map: panic while holding its guard.
        let r = catch_unwind(AssertUnwindSafe(|| {
            let _g = eng.instances.lock().unwrap();
            panic!("boom while holding the instances lock");
        }));
        assert!(r.is_err());

        // Poison the per-pond instance mutex too (the query-path lock).
        let inst = eng.instance(&loc).unwrap();
        let r = catch_unwind(AssertUnwindSafe(|| {
            let _g = inst.lock().unwrap();
            panic!("boom while holding the pond instance lock");
        }));
        assert!(r.is_err());

        // Both mutexes are poisoned, yet the pond must still be queryable —
        // lock_recover recovers the guard instead of bricking the engine.
        let ok = eng
            .read_query(&loc, "SELECT 1 AS x", AbortToken::new())
            .unwrap();
        assert_eq!(ok.rows[0][0], serde_json::json!(1));
    }

    #[test]
    fn read_arrow_streams_batches_and_rejects_writes() {
        use arrow::datatypes::SchemaRef;
        use arrow::record_batch::RecordBatch;
        use std::ops::ControlFlow;

        let fs = TempFs::new();
        let loc = fs.create_pond(PondId::new()).unwrap();
        let eng = DuckEngine::new();
        eng.init_pond(&loc).unwrap();
        eng.write_query(
            &loc,
            "CREATE TABLE t AS SELECT * FROM range(2500) r(i)",
            &Identity::claimed(Some("a")),
            AbortToken::new(),
        )
        .unwrap();

        #[derive(Default)]
        struct Collector {
            first_col: Option<String>,
            rows: usize,
            batches: usize,
        }
        impl ArrowSink for Collector {
            fn schema(&mut self, s: SchemaRef) -> ControlFlow<()> {
                self.first_col = Some(s.field(0).name().clone());
                ControlFlow::Continue(())
            }
            fn batch(&mut self, b: RecordBatch) -> ControlFlow<()> {
                self.rows += b.num_rows();
                self.batches += 1;
                ControlFlow::Continue(())
            }
        }

        let mut c = Collector::default();
        eng.read_arrow(
            &loc,
            "SELECT i FROM t ORDER BY i",
            AbortToken::new(),
            &mut c,
        )
        .unwrap();
        assert_eq!(c.rows, 2500, "all rows streamed");
        assert!(c.batches >= 1, "schema + at least one batch");
        assert_eq!(c.first_col.as_deref(), Some("i"));

        // read_arrow rejects writes, like read_query.
        let mut c2 = Collector::default();
        assert!(matches!(
            eng.read_arrow(&loc, "INSERT INTO t VALUES (1)", AbortToken::new(), &mut c2),
            Err(EngineError::ReadOnlyViolation)
        ));
    }

    #[test]
    fn forget_pond_evicts_cached_instance() {
        let fs = TempFs::new();
        let loc = fs.create_pond(PondId::new()).unwrap();
        let eng = DuckEngine::new();
        eng.init_pond(&loc).unwrap(); // opens + caches the instance
        assert_eq!(
            instance_count(&eng),
            1,
            "init_pond should cache an instance"
        );

        eng.forget_pond(&loc);
        assert_eq!(
            instance_count(&eng),
            0,
            "forget_pond must evict the cached instance (else it leaks the connection)"
        );

        // Idempotent: forgetting an unknown / already-forgotten pond is a no-op.
        eng.forget_pond(&loc);
        assert_eq!(instance_count(&eng), 0);

        // The pond is still usable afterward — a new query re-opens it lazily.
        let ok = eng
            .read_query(&loc, "SELECT 1 AS x", AbortToken::new())
            .unwrap();
        assert_eq!(ok.rows[0][0], serde_json::json!(1));
        assert_eq!(instance_count(&eng), 1, "query should re-open the instance");
    }
}
