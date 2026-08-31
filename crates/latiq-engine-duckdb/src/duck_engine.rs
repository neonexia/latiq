// Copyright 2026 Neonexia
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! `DuckEngine` — the DuckDB + DuckLake implementation of `QueryEngine`.
//!
//! **One DuckDB instance (database) per pond** (spec §5, Decision 3), reached
//! through a *serialized writer* connection plus a *bounded pool of read
//! connections* to that same database. All commits to a pond's DuckLake catalog
//! still go through one writer handle, so DuckLake's transactional model
//! serializes them (instead of independent instances racing on the catalog file),
//! while reads run concurrently rather than queueing behind the writer.
//!
//! The invariant that matters is unchanged: one *database* per pond, so the tier's
//! `memory_limit`/`threads` caps stay instance-global and one process owns the
//! catalog file. Only the connection count varies — never the instance count.
//!
//! Cancellation uses the spike-confirmed `Connection::interrupt_handle()`: a
//! watcher thread interrupts the running statement when the `AbortToken` is
//! cancelled, and exits when the operation completes.
use crate::exec::{
    annotate_schemas, in_read_txn, referenced_tables, run_explain, run_read, run_read_arrow,
    run_write,
};
use crate::instance::PondInstance;
use latiq_common::{DatasetRef, Identity, QueryMeta};
use latiq_engine::{
    AbortToken, ArrowSink, EngineError, ExplainResult, QueryEngine, QueryResult, SchemaSummary,
    TableInfo,
};
use latiq_storage::PondLocation;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::time::Duration;

#[derive(Default)]
pub struct DuckEngine {
    /// Per-pond engine resources, keyed by catalog URI.
    ponds: Mutex<HashMap<String, Arc<Pond>>>,
    /// The linked DuckDB's version, asked once (see `version`).
    version: std::sync::OnceLock<String>,
}

/// What the bound plan said a statement reads and writes, or `None` when the
/// pond did not opt into lineage.
///
/// The `Option` is the opt-in, made unmissable: extraction costs a second bind
/// (~380 µs against a 2.16 ms query, ~14–18% on LOCAL tables — much more on
/// remote files, where the second bind repeats the glob and schema sniff; see
/// `exec::referenced_tables`), and not paying it for a pond that records no
/// lineage is the entire justification for the per-pond flag. Every call site
/// therefore goes through here rather than deciding for itself.
type PlanDatasets = Option<(Vec<DatasetRef>, Vec<DatasetRef>)>;

/// Runs inside the caller's `run_with_abort`, so the extra bind is **abortable**
/// — a remote glob at bind time can take as long as the query, and an
/// uncancellable one would leave a client's cancel waiting on work it cannot
/// see.
fn plan_datasets(loc: &PondLocation, inst: &PondInstance, sql: &str) -> PlanDatasets {
    loc.lineage
        .then(|| referenced_tables(inst, sql, &loc.catalog_name))
}

/// Fill in the columns of the pond's own datasets, in one lookup.
///
/// Gated by the same `Option` as the extraction itself, so a pond without
/// lineage does not pay for it — and called at a point each path chooses: an
/// output's columns exist only *after* the statement ran, and an input's must
/// be read inside the read's own transaction to describe what the rows came
/// from. Best-effort throughout (`exec::annotate_schemas` warns and returns).
fn annotate(loc: &PondLocation, inst: &PondInstance, datasets: &mut PlanDatasets) {
    if let Some((inputs, outputs)) = datasets {
        annotate_schemas(inst, &loc.catalog_name, inputs, outputs);
    }
}

/// Attach what the plan found to the statement's meta. A no-op for a pond
/// without lineage, so `tables_touched` stays empty exactly where nothing asked
/// for it.
fn apply_datasets(meta: &mut QueryMeta, datasets: PlanDatasets) {
    if let Some((inputs, outputs)) = datasets {
        meta.set_datasets(inputs, outputs);
    }
}

/// Re-file a dataset that a transient `ATTACH` put under the catalog's local
/// `alias` in the SOURCE's own namespace.
///
/// The alias is a pond-local name — the operator's registry entry, mounted for
/// the duration of one pull — so `ext.main.widgets` says nothing another tool's
/// lineage can join on, and leaving `namespace` empty would hand the table the
/// *pond's* namespace and claim the lakehouse's data as ours. Anything not
/// under the alias (the pond table the pull creates) is left exactly as it is.
fn externalize(mut ds: DatasetRef, alias: &str, namespace: &str) -> DatasetRef {
    if let Some(rest) = ds.name.strip_prefix(&format!("{alias}.")) {
        ds.name = rest.to_string();
        ds.namespace = Some(namespace.to_string());
    }
    ds
}

/// One pond's engine resources. Still **one DuckDB database per pond**
/// (invariant 7) — tier `memory_limit`/`threads` caps stay instance-global and
/// one process owns the catalog file — but reached through two handles:
///
/// * `writer` — writes/DDL and session-scoped work, serialized. One writer per
///   pond is what keeps this pond's DuckLake commits ordered.
/// * `reads` — a bounded pool of additional connections to that same database,
///   so concurrent reads run concurrently instead of queueing behind one handle.
///
/// Measured: reads no longer block the writer (it keeps ~98% of its solo rate
/// under read load) and shared-pond read throughput rises ~2.5x at a 4-thread
/// tier. Per-pond parallelism is still capped by the tier — as intended.
struct Pond {
    /// The tier caps this instance was opened with. A pond can be re-tiered after
    /// creation, and `memory_limit`/`threads` are applied when the instance is
    /// opened — so if the resolved limits no longer match, the instance is stale
    /// and must be re-opened for the new caps to take effect.
    limits: Option<latiq_common::ResourceLimits>,
    writer: Mutex<PondInstance>,
    /// Clone source for growing the read pool. Never runs queries, so growing the
    /// pool never has to wait behind a long-running write.
    source: Mutex<PondInstance>,
    reads: ReadPool,
}

/// A bounded, lazily-grown pool of read connections to one pond's database.
/// Bounded because unbounded growth would mean one DuckDB connection per
/// in-flight agent request; when every connection is busy a reader waits, which
/// is backpressure at the pond's tier, not an error.
struct ReadPool {
    state: Mutex<PoolState>,
    returned: Condvar,
    max: usize,
}

#[derive(Default)]
struct PoolState {
    idle: Vec<PondInstance>,
    created: usize,
}

/// A checked-out read connection, returned to the pool on drop.
struct ReadGuard<'a> {
    pool: &'a ReadPool,
    inst: Option<PondInstance>,
    reusable: bool,
}

impl std::ops::Deref for ReadGuard<'_> {
    type Target = PondInstance;
    fn deref(&self) -> &PondInstance {
        self.inst.as_ref().expect("checked out")
    }
}

impl Drop for ReadGuard<'_> {
    fn drop(&mut self) {
        if let Some(inst) = self.inst.take() {
            let mut st = lock_recover(&self.pool.state);
            if self.reusable {
                st.idle.push(inst);
            } else {
                // Errored/cancelled: discard rather than hand a connection with a
                // stale interrupt or open transaction to the next reader.
                st.created -= 1;
            }
            drop(st);
            self.pool.returned.notify_one();
        }
    }
}

/// Lock a mutex, recovering the guard if a prior holder panicked. A poisoned
/// pond/instances mutex must not brick the pond — the DuckDB connection (or map)
/// is still usable, and the next statement either succeeds or returns a normal
/// engine error.
fn lock_recover<T>(m: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    m.lock().unwrap_or_else(|e| e.into_inner())
}

impl Pond {
    fn open(loc: &PondLocation) -> Result<Self, EngineError> {
        let writer = PondInstance::open(loc)?;
        let source = writer.clone_for_read()?;
        // Read concurrency allowed per pond. Scaled off the tier's core budget —
        // measurably still worth ~2x the serial rate at 2x cores — and clamped so
        // a tiny pond keeps some concurrency and a big one can't hoard handles.
        // The ceiling must stay above 2x the largest tier's cores, or the top
        // tiers clamp to the same value and a bigger tier buys no read
        // parallelism. More concurrency costs neighbour isolation, which is the
        // trade a larger tier is meant to make — never a default-tier pond.
        // No limits = the `none` tier: the engine is using the whole host, so the
        // read pool sizes off the host too. Defaulting to a fixed number here
        // would pin an uncapped pond to a mid-tier pond's read concurrency, so
        // uncapping would raise DuckDB's thread budget while quietly leaving
        // concurrent readers queued.
        let cores = loc
            .limits
            .as_ref()
            .map(|l| l.cores as usize)
            .unwrap_or_else(|| {
                std::thread::available_parallelism()
                    .map(|n| n.get())
                    .unwrap_or(4)
            });
        Ok(Self {
            limits: loc.limits,
            writer: Mutex::new(writer),
            source: Mutex::new(source),
            reads: ReadPool {
                state: Mutex::new(PoolState::default()),
                returned: Condvar::new(),
                max: (cores * 2).clamp(4, 32),
            },
        })
    }

    /// Check out a read connection, growing the pool up to `max`, else waiting
    /// for one to come back.
    fn checkout_read(&self) -> Result<ReadGuard<'_>, EngineError> {
        let mut st = lock_recover(&self.reads.state);
        loop {
            if let Some(inst) = st.idle.pop() {
                return Ok(ReadGuard {
                    pool: &self.reads,
                    inst: Some(inst),
                    reusable: true,
                });
            }
            if st.created < self.reads.max {
                st.created += 1;
                drop(st); // clone outside the pool lock
                return match lock_recover(&self.source).clone_for_read() {
                    Ok(inst) => Ok(ReadGuard {
                        pool: &self.reads,
                        inst: Some(inst),
                        reusable: true,
                    }),
                    Err(e) => {
                        lock_recover(&self.reads.state).created -= 1;
                        Err(e)
                    }
                };
            }
            st = self
                .reads
                .returned
                .wait(st)
                .unwrap_or_else(|e| e.into_inner());
        }
    }
}

impl DuckEngine {
    pub fn new() -> Self {
        Self::default()
    }

    /// Get the pond's resources, opening (and caching) them on first use.
    fn pond(&self, loc: &PondLocation) -> Result<Arc<Pond>, EngineError> {
        let mut map = lock_recover(&self.ponds);
        if let Some(p) = map.get(&loc.catalog_uri) {
            if p.limits == loc.limits {
                return Ok(p.clone());
            }
            // Re-tiered: drop the cached instance so the next open applies the new
            // caps. In-flight queries hold their own Arc and finish under the old
            // ones (same lifetime rule as forget_pond).
            map.remove(&loc.catalog_uri);
        }
        let p = Arc::new(Pond::open(loc)?);
        map.insert(loc.catalog_uri.clone(), p.clone());
        Ok(p)
    }

    /// Run a read on a pooled connection. The connection is dropped rather than
    /// reused unless the read succeeded.
    ///
    /// Not reusable *until proven otherwise*: `f` can panic (a cell conversion,
    /// a sink, an encoder), and an assignment after the call is skipped entirely
    /// when it does. `ReadGuard::drop` still runs during the unwind, so a
    /// connection left mid-transaction — with a `ROLLBACK` that the pending
    /// interrupt may have refused — would otherwise be pooled and wedge every
    /// later reader of this pond. The `Err` path and the panic path must reach
    /// the same conclusion.
    fn with_read<T>(
        &self,
        loc: &PondLocation,
        f: impl FnOnce(&PondInstance) -> Result<T, EngineError>,
    ) -> Result<T, EngineError> {
        let pond = self.pond(loc)?;
        let mut g = pond.checkout_read()?;
        g.reusable = false;
        let out = f(&g);
        g.reusable = out.is_ok();
        out
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
    fn version(&self) -> String {
        // Asked of the linked library itself (an in-memory connection needs no
        // pond and no extensions), then cached: a hard-coded string would be
        // wrong the first time the bundled DuckDB is bumped, and the lineage
        // trail would claim a version that never ran anything.
        self.version
            .get_or_init(|| {
                duckdb::Connection::open_in_memory()
                    .and_then(|c| c.query_row("SELECT version()", [], |r| r.get::<_, String>(0)))
                    // An engine that cannot say its version must not break a
                    // query over it: the facet is simply less specific.
                    .unwrap_or_default()
            })
            .clone()
    }

    fn init_pond(&self, loc: &PondLocation) -> Result<(), EngineError> {
        // Opening the instance attaches the pond's DuckLake catalog (creating the
        // catalog file on first open) and validates it's usable. No Latiq objects
        // are created on top — the pond is pure DuckLake.
        let _ = self.pond(loc)?;
        Ok(())
    }

    fn read_query(
        &self,
        loc: &PondLocation,
        sql: &str,
        abort: AbortToken,
    ) -> Result<QueryResult, EngineError> {
        // The read-only guard first, so a write submitted here is rejected
        // before the binder is handed a statement this surface does not accept
        // — and before it pays for provenance it is going to discard.
        if !latiq_engine::is_read_only(sql) {
            return Err(EngineError::ReadOnlyViolation);
        }
        self.with_read(loc, |i| {
            // Both under ONE abort watcher: the extra bind is real work (a
            // remote glob can dominate the query) and must be interruptible.
            Self::run_with_abort(i, &abort, |i| {
                // One read-only transaction over both, so the version the plan
                // records for an input is the snapshot the rows actually came
                // from (`exec::in_read_txn`).
                in_read_txn(i, |i| {
                    // Before the run, not after: extraction binds the
                    // statement, and a statement that has already run may no
                    // longer bind (a dropped table, a table the same batch
                    // created). Same order on every path, so provenance does
                    // not depend on the statement.
                    let mut datasets = plan_datasets(loc, i, sql);
                    // Inside this transaction, so the columns recorded are the
                    // ones the rows came from.
                    annotate(loc, i, &mut datasets);
                    let mut res = run_read(i, sql)?;
                    apply_datasets(&mut res.meta, datasets);
                    Ok(res)
                })
            })
        })
    }

    fn read_arrow(
        &self,
        loc: &PondLocation,
        sql: &str,
        abort: AbortToken,
        sink: &mut dyn ArrowSink,
    ) -> Result<QueryMeta, EngineError> {
        // Guard first, as in `read_query` — same reason.
        if !latiq_engine::is_read_only(sql) {
            return Err(EngineError::ReadOnlyViolation);
        }
        self.with_read(loc, |i| {
            // One abort watcher over both, as in `read_query`. The transaction
            // stays open across the whole batch stream — a correctness gain, in
            // that the stream is now snapshot-consistent rather than resolving
            // the catalog per statement — and a cancellation mid-stream unwinds
            // through `in_read_txn`, which rolls back before the connection is
            // discarded.
            //
            // That makes the sink's backpressure part of this transaction's
            // lifetime: a consumer that stops reading holds a DuckLake snapshot
            // pinned, not just a pool connection. `ArrowSink::batch` must
            // therefore stay cancellation-responsive — see `ChannelSink` in
            // `latiq-agent-core`, whose send wakes on the abort token.
            Self::run_with_abort(i, &abort, |i| {
                in_read_txn(i, |i| {
                    let mut datasets = plan_datasets(loc, i, sql);
                    annotate(loc, i, &mut datasets);
                    let mut meta = run_read_arrow(i, sql, &abort, sink)?;
                    apply_datasets(&mut meta, datasets);
                    Ok(meta)
                })
            })
        })
    }

    fn write_query(
        &self,
        loc: &PondLocation,
        sql: &str,
        identity: &Identity,
        abort: AbortToken,
    ) -> Result<QueryResult, EngineError> {
        let pond = self.pond(loc)?;
        let guard = lock_recover(&pond.writer);
        // One abort watcher over the extraction AND the write. The extraction
        // holds the pond's writer mutex — deliberately, because it must happen
        // before the write (a `DROP TABLE t` no longer binds once it has run,
        // so extracting afterwards loses the output of exactly the statements
        // whose output matters most) and moving it outside the lock would let
        // another write land in between and change what it resolves against.
        // The cost is that a slow bind delays other writers to this pond; being
        // abortable is what keeps that bounded.
        Self::run_with_abort(&guard, &abort, |i| {
            let mut datasets = plan_datasets(loc, i, sql);
            let mut res = run_write(i, sql, identity, &loc.catalog_name)?;
            // AFTER the statement: a `CREATE TABLE … AS`'s target has no
            // columns to describe until it exists, and a dropped table has none
            // to describe at all — which is why a failed write records none
            // either: the `QueryEngine::plan_datasets` recovery path below
            // recovers the datasets and deliberately not their columns.
            annotate(loc, i, &mut datasets);
            apply_datasets(&mut res.meta, datasets);
            Ok(res)
        })
    }

    fn explain_query(&self, loc: &PondLocation, sql: &str) -> Result<ExplainResult, EngineError> {
        self.with_read(loc, |i| run_explain(i, sql))
    }

    fn plan_datasets(&self, loc: &PondLocation, sql: &str) -> Option<QueryMeta> {
        if !loc.lineage {
            return None;
        }
        // A READ connection, not the writer: this runs after a write already
        // failed, and making the recovery of its provenance queue ahead of the
        // next write would charge every other writer for one failure. There is
        // no abort token here — the operation is over — so this bind is not
        // interruptible; it is bounded by being on the failure path only.
        let (inputs, outputs) = self
            .with_read(loc, |i| Ok(referenced_tables(i, sql, &loc.catalog_name)))
            .ok()?;
        if inputs.is_empty() && outputs.is_empty() {
            return None;
        }
        let mut meta = QueryMeta::default();
        meta.set_datasets(inputs, outputs);
        Some(meta)
    }

    fn open_pond_count(&self) -> usize {
        lock_recover(&self.ponds).len()
    }

    fn forget_pond(&self, loc: &PondLocation) {
        // Drop the cached instance so its DuckDB connection (and the open handle to
        // the pond's catalog file) is closed before storage deletes those files.
        // No-op if the pond was never opened. The Arc is dropped when the last
        // in-flight query on it finishes, closing the connection then.
        let mut map = lock_recover(&self.ponds);
        map.remove(&loc.catalog_uri);
    }

    fn describe_schema(&self, loc: &PondLocation) -> Result<SchemaSummary, EngineError> {
        // Native DuckDB catalog introspection on the attached pond catalog — no
        // Latiq view in between. (This lives in the DuckDB adapter, so using
        // duckdb_tables() here is fine; a DataFusion adapter would use its own.)
        // Scope to this pond's catalog (its name); escape `'` for the literal.
        let cat = loc.catalog_name.replace('\'', "''");
        let res = self.with_read(loc, |i| {
            run_read(
                i,
                &format!(
                    "SELECT table_name AS name, estimated_size AS row_count, comment \
                     FROM duckdb_tables() WHERE database_name = '{cat}'"
                ),
            )
        })?;
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
    ) -> Result<QueryMeta, EngineError> {
        // Session-scoped ATTACH/DETACH (+ a transient secret) and, for a pull, a
        // write into the pond — must run on the writer connection, not a pooled
        // read one whose session state other readers would then observe.
        let pond = self.pond(loc)?;
        let guard = lock_recover(&pond.writer);
        let plan = crate::attachers::plan(catalog_type, alias, params)?;
        attach_catalog(&guard.conn, &plan)?;
        // Extracted while the catalog is still ATTACHED and before the pull
        // runs — the only window where both sides bind: afterwards the external
        // tables are detached and the pull's own target already exists. Gated
        // on the pond's lineage flag like every other path, so a pond that did
        // not opt in pays nothing for the second bind.
        let mut datasets = plan_datasets(loc, &guard, query);
        // Run the pull query (a CREATE TABLE … in the pond's default catalog),
        // then tear the attachment down regardless of the outcome.
        let ran = guard
            .conn
            .execute_batch(query)
            .map_err(|e| EngineError::Engine(format!("pull query: {e}")));
        teardown_catalog(&guard.conn, &plan);
        ran?;
        // The pull's own target is a pond table, and it exists now. The source
        // side is external and is skipped, exactly as an `s3://` input is.
        annotate(loc, &guard, &mut datasets);
        let mut meta = QueryMeta::default();
        apply_datasets(
            &mut meta,
            datasets.map(|(inputs, outputs)| {
                let f = |ds: Vec<DatasetRef>| {
                    ds.into_iter()
                        .map(|d| externalize(d, &plan.alias, &plan.namespace))
                        .collect()
                };
                (f(inputs), f(outputs))
            }),
        );
        Ok(meta)
    }

    fn describe_catalog(
        &self,
        loc: &PondLocation,
        catalog_type: &str,
        alias: &str,
        params: &std::collections::BTreeMap<String, String>,
    ) -> Result<Vec<(String, String)>, EngineError> {
        // Session-scoped ATTACH/DETACH (+ a transient secret) and, for a pull, a
        // write into the pond — must run on the writer connection, not a pooled
        // read one whose session state other readers would then observe.
        let pond = self.pond(loc)?;
        let guard = lock_recover(&pond.writer);
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
        let loc = fs.create_pond(PondId::new(), false).unwrap();
        let inst = PondInstance::open(&loc).unwrap();
        let plan = crate::attachers::AttachPlan {
            alias: "leaktest".into(),
            namespace: "ducklake:/nonexistent_dir_xyz/meta.duckdb".into(),
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
        let loc = fs.create_pond(PondId::new(), false).unwrap();
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
        eng.ponds.lock().unwrap().len()
    }

    #[test]
    fn recovers_from_poisoned_mutex() {
        use std::panic::{catch_unwind, AssertUnwindSafe};
        let fs = TempFs::new();
        let loc = fs.create_pond(PondId::new(), false).unwrap();
        let eng = DuckEngine::new();
        eng.init_pond(&loc).unwrap();

        // Poison the pond map: panic while holding its guard.
        let r = catch_unwind(AssertUnwindSafe(|| {
            let _g = eng.ponds.lock().unwrap();
            panic!("boom while holding the ponds lock");
        }));
        assert!(r.is_err());

        // Poison the per-pond writer mutex AND the read-pool state — both are on
        // the query path now.
        let pond = eng.pond(&loc).unwrap();
        let r = catch_unwind(AssertUnwindSafe(|| {
            let _g = pond.writer.lock().unwrap();
            panic!("boom while holding the pond writer lock");
        }));
        assert!(r.is_err());
        let r = catch_unwind(AssertUnwindSafe(|| {
            let _g = pond.reads.state.lock().unwrap();
            panic!("boom while holding the read pool lock");
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
        let loc = fs.create_pond(PondId::new(), false).unwrap();
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

    // ---- read connection pool -------------------------------------------
    // Reads run on pooled connections to the SAME database while writes stay on
    // the serialized writer. These pin the properties that makes safe.

    #[test]
    fn pooled_read_sees_writes_committed_on_the_writer() {
        // THE correctness question for a multi-connection pond: a read on a
        // different connection must observe committed writes. Checked on a fresh
        // pooled connection AND on a reused one.
        let fs = TempFs::new();
        let loc = fs.create_pond(PondId::new(), false).unwrap();
        let eng = DuckEngine::new();
        let id = Identity::claimed(Some("writer"));
        eng.write_query(&loc, "CREATE TABLE t(i INTEGER)", &id, AbortToken::new())
            .unwrap();
        for n in 1..=3 {
            eng.write_query(
                &loc,
                &format!("INSERT INTO t VALUES ({n})"),
                &id,
                AbortToken::new(),
            )
            .unwrap();
            // Unqualified name also proves the pooled connection re-applied `USE`.
            let got = eng
                .read_query(&loc, "SELECT count(*) AS c FROM t", AbortToken::new())
                .unwrap();
            assert_eq!(
                got.rows[0][0],
                serde_json::json!(n),
                "pooled read must see writes committed on the writer"
            );
        }
    }

    #[test]
    fn readers_do_not_hold_the_writer_lock() {
        // The Scenario B fix: a checked-out reader must not block a write. If
        // reads still took the writer mutex this would deadlock.
        let fs = TempFs::new();
        let loc = fs.create_pond(PondId::new(), false).unwrap();
        let eng = DuckEngine::new();
        let id = Identity::claimed(Some("w"));
        eng.write_query(&loc, "CREATE TABLE t(i INTEGER)", &id, AbortToken::new())
            .unwrap();
        let pond = eng.pond(&loc).unwrap();
        let held = pond.checkout_read().unwrap(); // a reader is in flight
        eng.write_query(&loc, "INSERT INTO t VALUES (1)", &id, AbortToken::new())
            .expect("a write must proceed while a read connection is checked out");
        drop(held);
    }

    #[test]
    fn pool_hands_out_distinct_connections_and_respects_its_bound() {
        let fs = TempFs::new();
        let loc = fs.create_pond(PondId::new(), false).unwrap();
        let eng = DuckEngine::new();
        eng.init_pond(&loc).unwrap();
        let pond = eng.pond(&loc).unwrap();

        // Concurrent checkouts grow the pool rather than queueing on one handle.
        let a = pond.checkout_read().unwrap();
        let b = pond.checkout_read().unwrap();
        assert_eq!(lock_recover(&pond.reads.state).created, 2);
        drop(a);
        drop(b);
        // Returned, not leaked — and reused rather than re-cloned.
        assert_eq!(lock_recover(&pond.reads.state).idle.len(), 2);
        let c = pond.checkout_read().unwrap();
        assert_eq!(
            lock_recover(&pond.reads.state).created,
            2,
            "reuse, not grow"
        );
        drop(c);

        // The bound is honored: never more connections than `max`.
        let held: Vec<_> = (0..pond.reads.max)
            .map(|_| pond.checkout_read().unwrap())
            .collect();
        assert_eq!(lock_recover(&pond.reads.state).created, pond.reads.max);
        drop(held);
    }

    #[test]
    fn a_panicking_read_does_not_return_its_connection_to_the_pool() {
        // `g.reusable = out.is_ok()` after the call is SKIPPED when `f` panics,
        // so a panic (a cell conversion, a sink, an encoder) used to pool a
        // connection that may still be mid-transaction with a stale interrupt —
        // wedging every later reader of the pond. The `Err` path and the panic
        // path must reach the same conclusion, which is why the guard is now
        // cleared before `f` runs rather than set after it.
        let fs = TempFs::new();
        let loc = fs.create_pond(PondId::new(), false).unwrap();
        let eng = DuckEngine::new();
        eng.init_pond(&loc).unwrap();
        let pond = eng.pond(&loc).unwrap();

        let panicked = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            eng.with_read(&loc, |_| -> Result<(), EngineError> {
                panic!("a sink blew up mid-read")
            })
        }));
        assert!(
            panicked.is_err(),
            "the panic must propagate, not be absorbed"
        );
        let st = lock_recover(&pond.reads.state);
        assert!(
            st.idle.is_empty(),
            "a connection whose read panicked must not be handed to the next reader"
        );
        assert_eq!(
            st.created, 0,
            "and it must be accounted as discarded, or the pool leaks its bound"
        );
    }

    #[test]
    fn pooled_connections_are_utc_like_the_primary() {
        // Session state is not inherited by a clone; if we failed to re-apply it,
        // pooled reads would render timestamps in the host timezone.
        let fs = TempFs::new();
        let loc = fs.create_pond(PondId::new(), false).unwrap();
        let eng = DuckEngine::new();
        let tz = eng
            .read_query(
                &loc,
                "SELECT current_setting('TimeZone') AS z",
                AbortToken::new(),
            )
            .unwrap();
        assert_eq!(tz.rows[0][0], serde_json::json!("UTC"));
    }

    #[test]
    fn concurrent_reads_on_one_pond_all_succeed() {
        // Many readers on a shared pond — the product's "agents share a pond" case.
        use std::sync::Arc as StdArc;
        let fs = TempFs::new();
        let loc = fs.create_pond(PondId::new(), false).unwrap();
        let eng = StdArc::new(DuckEngine::new());
        eng.write_query(
            &loc,
            "CREATE TABLE t AS SELECT i FROM range(0,50000) tbl(i)",
            &Identity::claimed(Some("w")),
            AbortToken::new(),
        )
        .unwrap();
        let hs: Vec<_> = (0..12)
            .map(|_| {
                let (e, l) = (eng.clone(), loc.clone());
                std::thread::spawn(move || {
                    e.read_query(&l, "SELECT count(*) AS c FROM t", AbortToken::new())
                        .map(|r| r.rows[0][0].clone())
                })
            })
            .collect();
        for h in hs {
            assert_eq!(h.join().unwrap().unwrap(), serde_json::json!(50000));
        }
    }

    #[test]
    fn retiering_a_pond_reopens_it_with_the_new_caps() {
        // A pond can be re-tiered after creation. Limits are applied when the
        // instance opens, so a cached instance must be re-opened when the
        // resolved limits change — otherwise the new tier silently does nothing.
        use latiq_common::ResourceLimits;
        let fs = TempFs::new();
        let mut loc = fs.create_pond(PondId::new(), false).unwrap();
        loc.limits = Some(ResourceLimits {
            memory_bytes: 1024 * 1024 * 1024,
            cores: 2,
        });
        let eng = DuckEngine::new();
        let threads = |l: &latiq_storage::PondLocation| -> String {
            eng.read_query(
                l,
                "SELECT current_setting('threads')::VARCHAR AS t",
                AbortToken::new(),
            )
            .unwrap()
            .rows[0][0]
                .as_str()
                .unwrap()
                .to_string()
        };
        assert_eq!(threads(&loc), "2");
        assert_eq!(instance_count(&eng), 1);

        // Re-tier upward: same pond (same catalog uri), new caps.
        loc.limits = Some(ResourceLimits {
            memory_bytes: 16 * 1024 * 1024 * 1024,
            cores: 8,
        });
        assert_eq!(threads(&loc), "8", "new tier must reach the engine");
        assert_eq!(instance_count(&eng), 1, "re-opened in place, not leaked");

        // Unchanged limits must NOT churn the instance — the pool would be lost.
        let pond_before = eng.pond(&loc).unwrap();
        let _ = threads(&loc);
        assert!(
            Arc::ptr_eq(&pond_before, &eng.pond(&loc).unwrap()),
            "identical limits must reuse the cached instance"
        );
    }

    #[test]
    fn an_uncapped_pond_sizes_its_read_pool_off_the_host() {
        // The `none` tier means "engine defaults" — DuckDB takes the whole host.
        // The read pool must follow, or uncapping raises the thread budget while
        // leaving concurrent readers queued behind a mid-tier-sized pool.
        let fs = TempFs::new();
        let mut loc = fs.create_pond(PondId::new(), false).unwrap();
        loc.limits = None; // the `none` tier
        let eng = DuckEngine::new();
        let uncapped = eng.pond(&loc).unwrap().reads.max;

        let mut medium = fs.create_pond(PondId::new(), false).unwrap();
        medium.limits = Some(latiq_common::ResourceLimits {
            memory_bytes: 4 * 1024 * 1024 * 1024,
            cores: 4,
        });
        let capped = eng.pond(&medium).unwrap().reads.max;

        let host = std::thread::available_parallelism().map_or(4, |n| n.get());
        assert_eq!(uncapped, (host * 2).clamp(4, 32));
        if host > 4 {
            assert!(
                uncapped > capped,
                "uncapped ({uncapped}) must allow more concurrent reads than \
                 medium ({capped}) on a {host}-core host"
            );
        }
    }

    #[test]
    fn forget_pond_evicts_cached_instance() {
        let fs = TempFs::new();
        let loc = fs.create_pond(PondId::new(), false).unwrap();
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
