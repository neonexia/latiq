//! `DuckEngine` — the DuckDB + DuckLake implementation of `QueryEngine`.
//!
//! One DuckDB instance (connection) per query, per spec §5/§6. Cancellation
//! uses the spike-confirmed `Connection::interrupt_handle()`: a watcher thread
//! interrupts the running statement when the `AbortToken` is cancelled, and
//! stops promptly when the query finishes (so it never lingers).
use crate::exec::{run_explain, run_read, run_write};
use crate::instance::PondInstance;
use crate::latiq_schema::create_latiq_schema;
use latiq_common::Identity;
use latiq_engine::{
    AbortToken, EngineError, ExplainResult, QueryEngine, QueryResult, SchemaSummary, TableInfo,
};
use latiq_storage::PondLocation;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

pub struct DuckEngine;

impl DuckEngine {
    pub fn new() -> Self {
        Self
    }

    /// Run a blocking engine operation with an interrupt watcher bound to `abort`.
    /// The watcher interrupts the connection on cancellation and exits when the
    /// operation completes. An `INTERRUPT` error is normalized to `Cancelled`.
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

impl Default for DuckEngine {
    fn default() -> Self {
        Self::new()
    }
}

impl QueryEngine for DuckEngine {
    fn init_pond(&self, loc: &PondLocation) -> Result<(), EngineError> {
        let inst = PondInstance::open(loc)?;
        create_latiq_schema(&inst.conn)
    }

    fn read_query(
        &self,
        loc: &PondLocation,
        sql: &str,
        abort: AbortToken,
    ) -> Result<QueryResult, EngineError> {
        let inst = PondInstance::open(loc)?;
        Self::run_with_abort(&inst, &abort, |i| run_read(i, sql))
    }

    fn write_query(
        &self,
        loc: &PondLocation,
        sql: &str,
        identity: &Identity,
        abort: AbortToken,
    ) -> Result<QueryResult, EngineError> {
        let inst = PondInstance::open(loc)?;
        Self::run_with_abort(&inst, &abort, |i| run_write(i, sql, identity))
    }

    fn explain_query(&self, loc: &PondLocation, sql: &str) -> Result<ExplainResult, EngineError> {
        let inst = PondInstance::open(loc)?;
        run_explain(&inst, sql)
    }

    fn describe_schema(&self, loc: &PondLocation) -> Result<SchemaSummary, EngineError> {
        let inst = PondInstance::open(loc)?;
        let res = run_read(
            &inst,
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

        // Pond remains usable afterwards (resources reclaimed).
        let ok = eng
            .read_query(&loc, "SELECT 1 AS x", AbortToken::new())
            .unwrap();
        assert_eq!(ok.rows[0][0], serde_json::json!(1));
    }
}
