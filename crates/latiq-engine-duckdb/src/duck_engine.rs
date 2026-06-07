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
use crate::exec::{run_explain, run_read, run_write};
use crate::instance::PondInstance;
use crate::latiq_schema::create_latiq_schema;
use latiq_common::Identity;
use latiq_engine::{
    AbortToken, EngineError, ExplainResult, QueryEngine, QueryResult, SchemaSummary, TableInfo,
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
        let inst = self.instance(loc)?;
        let guard = lock_recover(&inst);
        create_latiq_schema(&guard.conn)
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

    fn write_query(
        &self,
        loc: &PondLocation,
        sql: &str,
        identity: &Identity,
        abort: AbortToken,
    ) -> Result<QueryResult, EngineError> {
        let inst = self.instance(loc)?;
        let guard = lock_recover(&inst);
        Self::run_with_abort(&guard, &abort, |i| run_write(i, sql, identity))
    }

    fn explain_query(&self, loc: &PondLocation, sql: &str) -> Result<ExplainResult, EngineError> {
        let inst = self.instance(loc)?;
        let guard = lock_recover(&inst);
        run_explain(&guard, sql)
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
        let res = run_read(
            &guard,
            "SELECT name, row_count, comment FROM _latiq.tables_summary",
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
}

#[cfg(test)]
mod tests {
    use super::*;
    use latiq_common::PondId;
    use latiq_storage::{PondStorage, TempFs};
    use std::time::{Duration, Instant};

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
