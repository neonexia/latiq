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

//! Query execution against an attached pond instance: read (SELECT),
//! write (txn-wrapped + native DuckLake attribution), and explain.
use crate::instance::PondInstance;
use duckdb::types::ValueRef;
use latiq_common::{DatasetField, DatasetRef, Identity, QueryMeta};
use latiq_engine::{AbortToken, ArrowSink, EngineError, ExplainResult, QueryResult, SqlShape};
use std::time::Instant;

/// Convert a single DuckDB cell to a JSON value for the neutral result. Owns the
/// value first so complex types (LIST/STRUCT/MAP/ARRAY) materialize, then maps
/// recursively to real nested JSON — never DuckDB's Arrow Debug repr.
fn cell_to_json(v: ValueRef<'_>) -> serde_json::Value {
    value_to_json(&v.to_owned())
}

fn value_to_json(v: &duckdb::types::Value) -> serde_json::Value {
    use duckdb::types::Value as V;
    use serde_json::Value as J;
    match v {
        V::Null => J::Null,
        V::Boolean(b) => J::Bool(*b),
        V::TinyInt(i) => J::from(*i),
        V::SmallInt(i) => J::from(*i),
        V::Int(i) => J::from(*i),
        V::BigInt(i) => J::from(*i),
        V::UTinyInt(i) => J::from(*i),
        V::USmallInt(i) => J::from(*i),
        V::UInt(i) => J::from(*i),
        V::UBigInt(i) => J::from(*i),
        // i128 doesn't fit serde_json::Number; keep full precision as a string
        // when it overflows i64 (never silently truncate via `as i64`).
        V::HugeInt(i) => match i64::try_from(*i) {
            Ok(x) => J::from(x),
            Err(_) => J::String(i.to_string()),
        },
        V::Float(f) => J::from(*f),
        V::Double(f) => J::from(*f),
        V::Decimal(d) => J::String(d.to_string()),
        V::Text(s) => J::String(s.clone()),
        V::Enum(s) => J::String(s.clone()),
        // Temporal types: ISO strings, not Debug (which printed e.g. "Date32(18817)").
        V::Date32(days) => {
            let (y, m, d) = civil_from_days(*days as i64);
            J::String(format!("{y:04}-{m:02}-{d:02}"))
        }
        V::Timestamp(unit, t) => {
            let (secs, frac_us) = split_unit_since_epoch(*unit, *t);
            let (y, mo, d) = civil_from_days(secs.div_euclid(86_400));
            let tod = secs.rem_euclid(86_400);
            let (h, mi, s) = (tod / 3600, (tod % 3600) / 60, tod % 60);
            J::String(if frac_us > 0 {
                format!("{y:04}-{mo:02}-{d:02} {h:02}:{mi:02}:{s:02}.{frac_us:06}")
            } else {
                format!("{y:04}-{mo:02}-{d:02} {h:02}:{mi:02}:{s:02}")
            })
        }
        V::Time64(unit, t) => {
            let (secs, frac_us) = split_unit_since_epoch(*unit, *t);
            let (h, mi, s) = (secs / 3600, (secs % 3600) / 60, secs % 60);
            J::String(if frac_us > 0 {
                format!("{h:02}:{mi:02}:{s:02}.{frac_us:06}")
            } else {
                format!("{h:02}:{mi:02}:{s:02}")
            })
        }
        V::Interval {
            months,
            days,
            nanos,
        } => serde_json::json!({ "months": months, "days": days, "nanos": nanos }),
        V::Blob(b) => J::String(
            b.iter()
                .map(|byte| format!("{byte:02x}"))
                .collect::<String>(),
        ),
        V::List(items) | V::Array(items) => J::Array(items.iter().map(value_to_json).collect()),
        V::Struct(fields) => J::Object(
            fields
                .iter()
                .map(|(k, val)| (k.clone(), value_to_json(val)))
                .collect(),
        ),
        V::Map(entries) => J::Object(
            entries
                .iter()
                .map(|(k, val)| (map_key(k), value_to_json(val)))
                .collect(),
        ),
        V::Union(inner) => value_to_json(inner),
    }
}

/// Render a MAP key as a JSON object key (JSON keys must be strings).
fn map_key(k: &duckdb::types::Value) -> String {
    use duckdb::types::Value as V;
    match k {
        V::Text(s) | V::Enum(s) => s.clone(),
        other => value_to_json(other).to_string(),
    }
}

/// Split a value expressed in `unit` into whole seconds + microsecond fraction.
fn split_unit_since_epoch(unit: duckdb::types::TimeUnit, v: i64) -> (i64, u32) {
    use duckdb::types::TimeUnit;
    let (secs, frac_us) = match unit {
        TimeUnit::Second => (v, 0),
        TimeUnit::Millisecond => (v.div_euclid(1_000), (v.rem_euclid(1_000) * 1_000) as u32),
        TimeUnit::Microsecond => (v.div_euclid(1_000_000), v.rem_euclid(1_000_000) as u32),
        TimeUnit::Nanosecond => (
            v.div_euclid(1_000_000_000),
            (v.rem_euclid(1_000_000_000) / 1_000) as u32,
        ),
    };
    (secs, frac_us)
}

/// Civil date (year, month, day) from days since 1970-01-01 (Hinnant's algorithm).
fn civil_from_days(z: i64) -> (i64, u32, u32) {
    let z = z + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1_460 + doe / 36_524 - doe / 146_096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = (doy - (153 * mp + 2) / 5 + 1) as u32;
    let m = if mp < 10 { mp + 3 } else { mp - 9 } as u32;
    (y + if m <= 2 { 1 } else { 0 }, m, d)
}

/// The pond's current max snapshot id, or `None` if the catalog has none. Used to
/// detect — authoritatively, from DuckLake — whether a statement actually created
/// a snapshot (i.e. changed data), instead of guessing read-vs-write from the SQL
/// text. `set_commit_message` on a transaction that changes nothing produces no
/// snapshot (verified), so a read run through the write path is a harmless no-op.
fn max_snapshot(inst: &PondInstance, cat_quoted: &str) -> Option<i64> {
    inst.conn
        .query_row(
            &format!("SELECT max(snapshot_id) FROM {cat_quoted}.snapshots()"),
            [],
            |r| r.get::<_, Option<i64>>(0),
        )
        .ok()
        .flatten()
}

/// An open `BEGIN TRANSACTION READ ONLY`, rolled back on drop unless committed.
///
/// Drop-based, not `ROLLBACK` at each error site, because these connections are
/// **pooled and reused**: one missed early return leaves an open transaction
/// that wedges the connection for every later reader. `?` inside the
/// transaction must be as safe as an explicit error path, and only `Drop` makes
/// that true for paths nobody has written yet.
struct ReadTxn<'a> {
    inst: &'a PondInstance,
    open: bool,
}

impl<'a> ReadTxn<'a> {
    fn begin(inst: &'a PondInstance) -> Result<Self, EngineError> {
        inst.conn
            // OUR statement, so OUR failure: not classified as the caller's.
            .execute_batch("BEGIN TRANSACTION READ ONLY")
            .map_err(|e| EngineError::Engine(e.to_string()))?;
        Ok(Self { inst, open: true })
    }

    fn commit(mut self) -> Result<(), EngineError> {
        self.inst
            .conn
            .execute_batch("COMMIT")
            .map_err(|e| EngineError::Engine(e.to_string()))?;
        // Cleared only on success. A failed COMMIT most likely aborted the
        // transaction itself, in which case Drop's ROLLBACK is a no-op — this
        // is defensive, not a claim about what DuckDB does with a failed
        // commit, and it costs one refused statement on a connection the
        // caller is about to discard anyway.
        self.open = false;
        Ok(())
    }
}

impl Drop for ReadTxn<'_> {
    fn drop(&mut self) {
        if self.open {
            // Best effort: on the cancellation path the statement was
            // interrupted, so this ROLLBACK may itself be refused. The caller
            // discards a connection whose read errored, so a transaction we
            // could not close never reaches another reader.
            let _ = self.inst.conn.execute_batch("ROLLBACK");
        }
    }
}

/// Run `f` inside a read-only transaction on `inst`.
///
/// The transaction is the whole point: everything `f` does — provenance
/// extraction and the read itself — resolves against **one** pinned catalog
/// snapshot. Unbracketed they are separate implicit transactions, so a commit
/// landing between them makes the version recorded for an input describe a
/// state the query never read, which is precisely the claim that version is
/// there to make.
///
/// There is no separate snapshot accessor: each DuckLake scan already carries
/// its own `snapshot.snapshot_id` in the bound plan (see [`scan_datasets`]),
/// and *because* the extraction now runs inside this transaction, that value
/// **is** the one the read observed. Measured across 17 read shapes, every
/// input from this pond's catalog arrives versioned that way, so asking
/// `current_snapshot()` as well would have been a second question with the
/// same answer, charged to every lineage read.
///
/// Measured (200 iterations, 200k-row aggregate, release): plain read
/// 1.94–1.99 ms, bracketed 1.68–1.72 ms. One transaction amortises the
/// per-statement catalog-snapshot resolution that auto-commit repeats, so the
/// bracket is a saving, not a cost — which is why `read_query`/`read_arrow`
/// take one for every pond, not only a lineage pond's.
///
/// Scope: the two `QueryEngine` read paths. `describe_schema`,
/// `describe_catalog` and `explain_query` deliberately stay unbracketed —
/// single-statement introspection with no version to record and nothing to keep
/// consistent across statements.
///
/// The bracket's integrity depends on the read guard refusing transaction
/// control: a `COMMIT` inside the user's SQL ends *this* transaction, and a
/// following `BEGIN` leaves a fresh one for [`ReadTxn::commit`] to close
/// without complaint. `latiq_engine::is_read_only` rejects those keywords for
/// that reason (verified reachable before it did).
///
/// Pinning is **lazy** — taken at the first catalog-touching statement, not at
/// `BEGIN` — so whichever of `f`'s statements runs first establishes it.
pub fn in_read_txn<T>(
    inst: &PondInstance,
    f: impl FnOnce(&PondInstance) -> Result<T, EngineError>,
) -> Result<T, EngineError> {
    let txn = ReadTxn::begin(inst)?;
    // Any error here — including a cancellation — drops `txn` and rolls back.
    let out = f(inst)?;
    txn.commit()?;
    Ok(out)
}

/// Prepare a caller's statement, with the two things `Connection::prepare` alone
/// gets wrong for an agent.
///
/// **Empty is a parse error, said in words.** DuckDB accepts a blank statement
/// and returns nothing at all, so a caller that sent an empty string would get
/// an empty result set and no hint that it never asked anything. And the read
/// guard no longer catches it: blank text is `Unrecognized`, not a write.
///
/// **Failures are classified by DuckDB's error class**, never by the fact that
/// it was `prepare` that failed — DuckDB binds some statements here and defers
/// others to execution, so the call site says nothing about what went wrong.
fn prepare<'a>(inst: &'a PondInstance, sql: &str) -> Result<duckdb::Statement<'a>, EngineError> {
    if sql.trim().is_empty() {
        return Err(EngineError::Parse(
            "The statement is empty — there is nothing to run.".into(),
        ));
    }
    inst.conn
        .prepare(sql)
        .map_err(|e| crate::errclass::classify(&e))
}

/// Execute a statement and materialize its result rows aligned to column names.
/// Works for any statement — a SELECT yields its rows; a write/DDL executes and
/// yields DuckDB's summary row (which write callers drop). Multi-statement input
/// executes every statement and returns the last one's result.
fn materialize(
    inst: &PondInstance,
    sql: &str,
) -> Result<(Vec<String>, Vec<Vec<serde_json::Value>>), EngineError> {
    let mut stmt = prepare(inst, sql)?;
    let mut rows = stmt.query([]).map_err(|e| crate::errclass::classify(&e))?;
    let mut out: Vec<Vec<serde_json::Value>> = Vec::new();
    let mut columns: Vec<String> = Vec::new();
    let mut have_columns = false;
    while let Some(row) = rows.next().map_err(|e| crate::errclass::classify(&e))? {
        let stmt_ref = row.as_ref();
        if !have_columns {
            columns = stmt_ref
                .column_names()
                .iter()
                .map(|s| s.to_string())
                .collect();
            have_columns = true;
        }
        let mut cells = Vec::with_capacity(columns.len());
        for i in 0..columns.len() {
            cells.push(cell_to_json(
                row.get_ref(i).map_err(|e| crate::errclass::classify(&e))?,
            ));
        }
        out.push(cells);
    }
    if !have_columns {
        // Zero rows: column names are available from the executed statement.
        columns = rows
            .as_ref()
            .map(|s| s.column_names().iter().map(|c| c.to_string()).collect())
            .unwrap_or_default();
    }
    Ok((columns, out))
}

/// Stream a read-only query's results as Arrow batches into `sink` (schema first,
/// then batches), using DuckDB's native Arrow output — no per-cell JSON
/// conversion, nothing buffered here. `abort` stops the stream between batches
/// (the interrupt handle covers cancellation inside a fetch).
pub fn run_read_arrow(
    inst: &PondInstance,
    sql: &str,
    abort: &AbortToken,
    sink: &mut dyn ArrowSink,
) -> Result<QueryMeta, EngineError> {
    if latiq_engine::classify(sql) == SqlShape::Write {
        return Err(EngineError::ReadOnlyViolation);
    }
    let t0 = Instant::now();
    let mut stmt = prepare(inst, sql)?;
    let arrow = stmt
        .query_arrow([])
        .map_err(|e| crate::errclass::classify(&e))?;
    // A meta even for a stream: it is how the streamed read's provenance (and
    // its row count) reaches the caller, which otherwise sees only batches.
    let mut meta = QueryMeta::default();
    // Schema is available even for an empty result, so downstream IPC/JSON always
    // has columns.
    if sink.schema(arrow.get_schema()).is_break() {
        meta.duration_ms = t0.elapsed().as_millis() as u64;
        return Ok(meta);
    }
    for batch in arrow {
        if abort.is_cancelled() {
            return Err(EngineError::Cancelled);
        }
        meta.rows += batch.num_rows() as u64;
        if sink.batch(batch).is_break() {
            break;
        }
    }
    meta.duration_ms = t0.elapsed().as_millis() as u64;
    Ok(meta)
}

/// Run a read-only query, materializing rows aligned to column names.
///
/// The `is_read_only` guard here is the read surface's "won't mutate" contract.
/// It is a text heuristic (interim) — the authoritative, engine-enforced version
/// (a read-only transaction) is deferred to the authorization work, since that is
/// where read-only *enforcement* as a permission belongs. Its remaining failure
/// mode is benign: at worst it rejects an unusual read, never lets a write slip
/// through unattributed.
///
/// (The engine read paths now call this inside [`in_read_txn`], where DuckDB
/// *does* refuse a write — "transaction is launched in read-only mode". Demoting
/// the heuristic to a fast-fail hint in front of that enforcement is a real
/// improvement and a deliberately separate change; this one does not touch it.)
pub fn run_read(inst: &PondInstance, sql: &str) -> Result<QueryResult, EngineError> {
    if latiq_engine::classify(sql) == SqlShape::Write {
        return Err(EngineError::ReadOnlyViolation);
    }
    let t0 = Instant::now();
    let (columns, rows) = materialize(inst, sql)?;
    let meta = QueryMeta {
        rows: rows.len() as u64,
        duration_ms: t0.elapsed().as_millis() as u64,
        ..Default::default()
    };
    Ok(QueryResult {
        columns,
        rows,
        meta,
    })
}

/// Run a statement in a transaction with native DuckLake attribution.
///
/// We do NOT pre-classify the SQL as read-vs-write. Every statement runs inside
/// `BEGIN … COMMIT` with `set_commit_message` issued LAST (so a user-supplied one
/// can't override it, and a trailing comment or embedded `;` can't shift our
/// COMMIT/attribution). DuckLake creates a snapshot only when data actually
/// changed, so a read run through this path is a harmless no-op — no snapshot, no
/// attribution — and simply returns its rows. Whether a write happened is decided
/// **authoritatively** by whether the pond's snapshot id advanced, never by
/// scanning the SQL text. Any failure rolls the transaction back (the per-pond
/// connection is reused, so a dangling open transaction would wedge the pond).
pub fn run_write(
    inst: &PondInstance,
    sql: &str,
    identity: &Identity,
    catalog: &str,
) -> Result<QueryResult, EngineError> {
    let t0 = Instant::now();
    // Attribution is a DuckLake method on THIS pond's catalog (named after the
    // pond), so qualify + quote the catalog name.
    let cat = crate::instance::quote_ident(catalog);
    // Used ONLY for our own framing — BEGIN, `set_commit_message`, COMMIT.
    // Those are not the caller's SQL, so a failure in one is ours and stays
    // `Engine` (→ `internal`, "report to your operator"), which for a
    // mis-plumbed catalog name is exactly the right advice. The caller's
    // statement goes through `materialize`, which classifies.
    let exec = |s: &str| {
        inst.conn
            .execute_batch(s)
            .map_err(|e| EngineError::Engine(e.to_string()))
    };
    let rollback = || {
        let _ = inst.conn.execute_batch("ROLLBACK");
    };

    let before = max_snapshot(inst, &cat);
    exec("BEGIN")?;
    // Execute + materialize the user's statement (executes writes and DDL too).
    let (columns, rows) = match materialize(inst, sql) {
        Ok(v) => v,
        Err(e) => {
            rollback();
            return Err(e);
        }
    };
    // The author is the strongest identity we have: the verified subject when
    // the caller authenticated, the claimed leaf otherwise. The claimed leaf is
    // always recorded separately so history distinguishes the two — a bare
    // `verified` must never sit next to a claimed value.
    //
    // Accepted v0 trade-off: `author` is the BARE subject, so subjects from two
    // different issuers collide when an operator groups history by `author`. The
    // issuer is recorded in `commit_extra_info` — group by the pair to be exact.
    // Qualifying the author (`iss#sub`) would churn the format for every reader,
    // so it waits for a deliberate decision, not a drive-by change.
    let author = if identity.verified {
        &identity.subject
    } else {
        &identity.agent_id
    };
    let author = author.replace('\'', "''");
    // Built with serde_json (never hand-concatenated) then escaped as one SQL
    // literal: every value here is caller- or token-supplied.
    let extra = serde_json::json!({
        "agent_id": identity.agent_id,
        "issuer": identity.issuer,
        "verified": identity.verified,
    })
    .to_string()
    .replace('\'', "''");
    let call = format!(
        "CALL {cat}.set_commit_message('{author}', 'write_query', extra_info => '{extra}')"
    );
    if let Err(e) = exec(&call) {
        rollback();
        return Err(e);
    }
    if let Err(e) = exec("COMMIT") {
        rollback();
        return Err(e);
    }
    let after = max_snapshot(inst, &cat);
    let snapshot_id = if after > before { after } else { None };

    if snapshot_id.is_some() {
        // A snapshot advanced → this was a write. Return the write shape (no rows +
        // the new snapshot id); DuckDB's summary row is not meaningful to callers.
        Ok(QueryResult {
            columns: vec![],
            rows: vec![],
            meta: QueryMeta {
                snapshot_id,
                duration_ms: t0.elapsed().as_millis() as u64,
                ..Default::default()
            },
        })
    } else {
        // Nothing changed → this was a read routed here. Return its rows.
        let n = rows.len() as u64;
        Ok(QueryResult {
            columns,
            rows,
            meta: QueryMeta {
                rows: n,
                duration_ms: t0.elapsed().as_millis() as u64,
                ..Default::default()
            },
        })
    }
}

// ------------------------------------------------------------- provenance
//
// What a statement reads and writes, taken from DuckDB's **bound plan**
// (`json_serialize_plan`). Everything below is best-effort: it must never fail
// a query and must never execute one.

/// Largest plan JSON we will parse. The plan scales with the number of
/// *literals*, not tables — a 1000-element `IN` list serializes to ~338 KB —
/// so a cap is what keeps a pathological query from paying a multi-megabyte
/// JSON parse for provenance. Over the cap we record nothing, and say so.
///
/// Counted in **bytes** (`strlen`), not characters: DuckDB's `length()` counts
/// characters, so a plan full of non-ASCII literals would cross the boundary at
/// up to ~4x this and make the constant's name a lie.
const MAX_PLAN_JSON_BYTES: usize = 512 * 1024;

/// The datasets a statement reads and writes, as `(inputs, outputs)`, from the
/// **bound** plan. `pond` is the pond's catalog name, used only to make the
/// diagnostics below identifiable.
///
/// Bound, not parsed: `SELECT * FROM v` where `v` joins two tables resolves to
/// those two base tables here, where the parse tree only ever says `v`. Writes
/// resolve too (`json_serialize_sql` refuses them outright).
///
/// **Cost — and where the published figure does not apply.** This is a SECOND
/// bind of the statement (~380 µs against a 2.16 ms query, ~14–18%). That
/// measurement was taken on **local tables only**. DuckDB's binder globs and
/// sniffs files at bind time — `json_serialize_plan` on `read_csv('missing')`
/// comes back with an `io` error, not a plan — so a query over
/// `read_parquet('s3://…/*.parquet')` pays the remote listing and schema sniff
/// **twice**. Enabling lineage on a pond that reads from object storage is
/// therefore materially more expensive than the local figure suggests.
///
/// Three properties this must keep, in order of how badly a regression hurts:
///
/// 1. **It never fails a query.** No `Result`, no panic, no unwrap:
///    unparseable SQL, an unknown table, a renamed plan key and a plan too big
///    to parse all yield no datasets. They do not, however, yield SILENCE —
///    see [`PlanSkip`].
/// 2. **It never executes the statement.** `json_serialize_plan` binds and
///    plans; it does not run. That is what disqualified `EXPLAIN ANALYZE` and
///    profiling, and it is pinned by a test.
/// 3. **The SQL is a bound parameter**, cast explicitly (a bare `?` is
///    rejected) — never concatenated into the extraction query.
pub fn referenced_tables(
    inst: &PondInstance,
    sql: &str,
    pond: &str,
) -> (Vec<DatasetRef>, Vec<DatasetRef>) {
    let mut inputs = Vec::new();
    let mut outputs = Vec::new();
    match serialized_plan(inst, sql) {
        Ok(plan) => {
            walk_plan(&plan, &mut inputs, &mut outputs);
            if inputs.is_empty() && outputs.is_empty() {
                // Legitimate for `SELECT 1` — and the exact shape a renamed
                // plan key would also take, which is why it is not silent.
                tracing::debug!(pond, "lineage: the plan named no datasets");
            }
        }
        Err(skip) => tracing::warn!(
            pond,
            reason = skip.reason(),
            "lineage: no provenance for this statement"
        ),
    }
    (dedup(inputs), dedup(outputs))
}

/// Fill in the columns of every dataset that lives in **this pond's own
/// catalog**, in ONE lookup for all of them.
///
/// One query, not one per table: the datasets of a statement are all in the
/// same `information_schema`, so a per-table round trip would charge a join
/// over five tables five times for information one query already returns.
/// Everything not in the pond's catalog — an `s3://` object, a Parquet file, a
/// transiently attached catalog — is skipped rather than guessed: we do not
/// have its columns cheaply, and a wrong schema is worse than an absent one.
///
/// **When to call it matters on each side.** An output's columns only exist
/// *after* the statement ran (a `CREATE TABLE … AS` has no target to describe
/// before it); an input's must be read inside the read's own transaction, or
/// the columns recorded may not be the ones the rows came from.
///
/// Best-effort, like [`referenced_tables`]: a failure logs why and leaves the
/// datasets exactly as they were. It must never fail a query.
pub fn annotate_schemas(
    inst: &PondInstance,
    catalog: &str,
    inputs: &mut [DatasetRef],
    outputs: &mut [DatasetRef],
) {
    // `{catalog}.{schema}.{table}`, and only for a dataset the pond owns (an
    // external one carries its own namespace).
    let qualified = |d: &DatasetRef| -> Option<(String, String)> {
        if d.namespace.is_some() {
            return None;
        }
        let rest = d.name.strip_prefix(catalog)?.strip_prefix('.')?;
        let (schema, table) = rest.split_once('.')?;
        (!table.contains('.')).then(|| (schema.to_string(), table.to_string()))
    };
    let wanted: std::collections::BTreeSet<(String, String)> = inputs
        .iter()
        .chain(outputs.iter())
        .filter_map(&qualified)
        .collect();
    if wanted.is_empty() {
        return;
    }
    // Escaped as SQL literals, never concatenated raw: these names come from
    // DuckDB's own plan, but they originate in caller SQL and a quote in an
    // identifier must not be able to end the literal.
    let lit = |s: &str| format!("'{}'", s.replace('\'', "''"));
    let pairs = wanted
        .iter()
        .map(|(s, t)| format!("({}, {})", lit(s), lit(t)))
        .collect::<Vec<_>>()
        .join(", ");
    let query = format!(
        "SELECT table_schema, table_name, column_name, data_type \
         FROM information_schema.columns \
         WHERE table_catalog = {} AND (table_schema, table_name) IN ({pairs}) \
         ORDER BY table_schema, table_name, ordinal_position",
        lit(catalog)
    );
    let mut by_table: std::collections::HashMap<(String, String), Vec<DatasetField>> =
        std::collections::HashMap::new();
    let collected = inst.conn.prepare(&query).and_then(|mut stmt| {
        let rows = stmt.query_map([], |r| {
            Ok((
                r.get::<_, String>(0)?,
                r.get::<_, String>(1)?,
                r.get::<_, String>(2)?,
                r.get::<_, String>(3)?,
            ))
        })?;
        for row in rows {
            let (schema, table, column, ty) = row?;
            by_table
                .entry((schema, table))
                .or_default()
                .push(DatasetField {
                    name: column,
                    type_name: ty,
                });
        }
        Ok(())
    });
    if let Err(e) = collected {
        // Loud for the same reason `referenced_tables` is: an event whose
        // datasets lost their columns looks exactly like one whose tables never
        // had any. The message is DuckDB's own and quotes no user SQL.
        tracing::warn!(pond = catalog, error = %e, "lineage: no column schema for this statement");
        return;
    }
    for ds in inputs.iter_mut().chain(outputs.iter_mut()) {
        if let Some(key) = qualified(ds) {
            if let Some(fields) = by_table.get(&key) {
                ds.fields = fields.clone();
            }
        }
    }
}

/// Why an extraction produced nothing.
///
/// The distinction is the whole point: "this statement touched no datasets" and
/// "we could not find out what it touched" look identical in an event, and this
/// feature's own thesis is that silently under-reporting provenance is the
/// worst failure mode there is. Every variant is logged by
/// [`referenced_tables`], so a pond whose events lost their datasets says so in
/// the node's log instead of looking complete.
#[derive(Debug)]
enum PlanSkip {
    /// The plan was larger than [`MAX_PLAN_JSON_BYTES`] and was never parsed.
    OverCap,
    /// DuckDB would not bind or serialize the statement — a syntax error, an
    /// unknown table, a missing file. Carries the plan's `error_type` only:
    /// binder messages quote the SQL back, and the SQL is redacted everywhere
    /// else it is recorded, so it must not leak through a log line here.
    NotPlanned(String),
    /// The extraction query itself failed — an interrupted statement, a
    /// connection in a bad state.
    Unavailable,
    /// The plan came back as something that is not JSON at all: a serialisation
    /// change, and the loudest reason of the four.
    Unreadable,
}

impl PlanSkip {
    fn reason(&self) -> String {
        match self {
            Self::OverCap => format!("plan larger than {MAX_PLAN_JSON_BYTES} bytes"),
            Self::NotPlanned(kind) => format!("duckdb would not plan it ({kind})"),
            Self::Unavailable => "the extraction query did not run".into(),
            Self::Unreadable => "the plan was not JSON".into(),
        }
    }
}

/// The bound plan as JSON, or why there is none. `json_serialize_plan` reports
/// failure **in band** — it returns `{"error":true,…}` for a syntax error or a
/// missing table rather than raising — so that shape is handled here.
fn serialized_plan(inst: &PondInstance, sql: &str) -> Result<serde_json::Value, PlanSkip> {
    // The cap is applied INSIDE DuckDB so an oversized plan is never carried
    // across the boundary and never handed to serde_json. `strlen` is BYTES;
    // `length` would count characters (verified: `length('héllo')` = 5,
    // `strlen('héllo')` = 6).
    let query = format!(
        "SELECT CASE WHEN strlen(p) <= {MAX_PLAN_JSON_BYTES} THEN p END \
         FROM (SELECT json_serialize_plan(?::VARCHAR)::VARCHAR AS p)"
    );
    let json: Option<String> = inst
        .conn
        .query_row(&query, [sql], |r| r.get(0))
        .map_err(|_| PlanSkip::Unavailable)?;
    let json = json.ok_or(PlanSkip::OverCap)?;
    let value: serde_json::Value = serde_json::from_str(&json).map_err(|_| PlanSkip::Unreadable)?;
    if value.get("error").and_then(serde_json::Value::as_bool) == Some(true) {
        return Err(PlanSkip::NotPlanned(
            value
                .get("error_type")
                .and_then(serde_json::Value::as_str)
                .unwrap_or("unknown")
                .to_string(),
        ));
    }
    Ok(value)
}

/// Walk every node of the plan, collecting scans as inputs and write/DDL
/// targets as outputs. Recursive over the whole document rather than only
/// `children`, because the operators we care about hang off several different
/// keys and a missed branch is a silently under-reported lineage.
fn walk_plan(
    node: &serde_json::Value,
    inputs: &mut Vec<DatasetRef>,
    outputs: &mut Vec<DatasetRef>,
) {
    match node {
        serde_json::Value::Object(obj) => {
            match obj.get("type").and_then(serde_json::Value::as_str) {
                Some("LOGICAL_GET") => inputs.extend(scan_datasets(obj)),
                // The write targets: the table entry sits under `table_info`…
                Some("LOGICAL_INSERT" | "LOGICAL_UPDATE" | "LOGICAL_DELETE") => {
                    outputs.extend(obj.get("table_info").and_then(entry_dataset));
                }
                // …and under `info` for DDL.
                Some("LOGICAL_CREATE_TABLE" | "LOGICAL_CREATE_VIEW" | "LOGICAL_DROP") => {
                    outputs.extend(obj.get("info").and_then(entry_dataset));
                }
                // `COPY … TO` is the pond's export path, and its target is a
                // real output: without this the event shows what the export
                // read and nothing it produced, so the edge that leaves the
                // pond is missing while the event still looks complete. The
                // target keeps its standard scheme, exactly as an external
                // input does — `file_path` is the whole destination for a
                // partitioned write too (it is then a directory).
                Some("LOGICAL_COPY_TO_FILE") => {
                    outputs.extend(
                        obj.get("file_path")
                            .and_then(serde_json::Value::as_str)
                            .map(DatasetRef::external),
                    );
                }
                _ => {}
            }
            for value in obj.values() {
                walk_plan(value, inputs, outputs);
            }
        }
        serde_json::Value::Array(items) => {
            for item in items {
                walk_plan(item, inputs, outputs);
            }
        }
        _ => {}
    }
}

/// What a `LOGICAL_GET` reads. **The key names differ per table function**, and
/// they are serialisation internals with no stability guarantee — a DuckDB
/// upgrade that renames one would silently under-report, which is exactly the
/// failure mode that disqualified the C API. `lineage_plan_key_names_still_*`
/// in `tests/engine_e2e.rs` fails loudly when that happens.
///
/// Note which catalog is which: for `ducklake_scan` the GET's own
/// `catalog_name`/`schema_name` name the *table function* (`system.main`), and
/// the table is in `function_data`. Reading the outer pair would file every
/// pond table under `system`.
fn scan_datasets(get: &serde_json::Map<String, serde_json::Value>) -> Vec<DatasetRef> {
    let Some(fd) = get.get("function_data").and_then(|v| v.as_object()) else {
        return Vec::new();
    };
    let text = |k: &str| fd.get(k).and_then(serde_json::Value::as_str);
    // ducklake_scan — and its snapshot, which is the version this input was
    // read at, free of charge.
    if let (Some(c), Some(s), Some(t)) = (
        text("catalog_name"),
        text("schema_name"),
        text("table_name"),
    ) {
        let mut ds = DatasetRef::table(c, s, t);
        ds.version = fd
            .get("snapshot")
            .and_then(|s| s.get("snapshot_id"))
            .and_then(serde_json::Value::as_i64);
        return vec![ds];
    }
    // Core `seq_scan` (a temp table, or a plain attached DuckDB catalog).
    if let (Some(c), Some(s), Some(t)) = (text("catalog"), text("schema"), text("table")) {
        return vec![DatasetRef::table(c, s, t)];
    }
    // `read_parquet` / `read_csv` and friends: external files, which keep their
    // standard scheme so another tool's lineage can join on them.
    fd.get("files")
        .and_then(serde_json::Value::as_array)
        .map(|files| {
            files
                .iter()
                .filter_map(serde_json::Value::as_str)
                .map(DatasetRef::external)
                .collect()
        })
        .unwrap_or_default()
}

/// A catalog entry (`TABLE_ENTRY`, `VIEW_ENTRY`, a `DROP_INFO`) as a dataset.
/// The name lives under a different key per entry kind — `table` for a table,
/// `view_name` for a view, `name` for a drop.
fn entry_dataset(entry: &serde_json::Value) -> Option<DatasetRef> {
    let obj = entry.as_object()?;
    let text = |k: &str| obj.get(k).and_then(serde_json::Value::as_str);
    let name = text("table")
        .or_else(|| text("view_name"))
        .or_else(|| text("name"))?;
    Some(DatasetRef::table(text("catalog")?, text("schema")?, name))
}

/// Same dataset twice (an `UPDATE`'s target, a self-join) is one dataset — but
/// the **version** is part of its identity: a time-travel self-join
/// (`FROM a AT (VERSION => 1) JOIN a`) reads two genuinely different states of
/// one table, and collapsing them would report a single snapshot for both.
fn dedup(mut datasets: Vec<DatasetRef>) -> Vec<DatasetRef> {
    let mut seen = std::collections::HashSet::new();
    datasets.retain(|d| seen.insert((d.namespace.clone(), d.name.clone(), d.version)));
    datasets
}

/// Wrap DuckDB `EXPLAIN`, returning the raw plan text. Richer estimate parsing
/// is a later refinement (Slice 0+ surfaces the plan; estimates are coarse).
pub fn run_explain(inst: &PondInstance, sql: &str) -> Result<ExplainResult, EngineError> {
    // `EXPLAIN ANALYZE <stmt>` EXECUTES the statement in DuckDB — refuse it so
    // the "plan only" tool can never run a write. Plain `EXPLAIN` does not run.
    if sql.trim_start().to_lowercase().starts_with("analyze") {
        return Err(EngineError::Parse(
            "EXPLAIN ANALYZE executes the statement; use read_query/write_query to run it".into(),
        ));
    }
    let explain_sql = format!("EXPLAIN {sql}");
    let mut stmt = inst
        .conn
        .prepare(&explain_sql)
        .map_err(|e| crate::errclass::classify(&e))?;
    let mut rows = stmt.query([]).map_err(|e| crate::errclass::classify(&e))?;
    let mut plan = String::new();
    while let Some(row) = rows.next().map_err(|e| crate::errclass::classify(&e))? {
        let ncols = row.as_ref().column_names().len();
        for i in 0..ncols {
            if let Ok(ValueRef::Text(t)) = row.get_ref(i) {
                plan.push_str(&String::from_utf8_lossy(t));
                plan.push('\n');
            }
        }
    }
    Ok(ExplainResult {
        estimated_rows: 0,
        estimated_bytes: 0,
        estimated_duration_ms: 0,
        scan_operations: vec![],
        warnings: vec![],
        suggestions: vec![],
        raw_plan: plan,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::instance::PondInstance;
    use latiq_common::PondId;
    use latiq_storage::{PondStorage, TempFs};

    fn pond() -> (TempFs, PondInstance) {
        let fs = TempFs::new();
        let loc = fs.create_pond(PondId::new(), false).unwrap();
        let inst = PondInstance::open(&loc).unwrap();
        (fs, inst)
    }

    #[test]
    fn write_then_read_with_attribution() {
        let (_fs, inst) = pond();
        let id = Identity::claimed(Some("agent-test"));
        run_write(
            &inst,
            "CREATE TABLE events(id INTEGER, sev VARCHAR)",
            &id,
            "pond",
        )
        .unwrap();
        run_write(
            &inst,
            "INSERT INTO events VALUES (1,'high'),(2,'low')",
            &id,
            "pond",
        )
        .unwrap();
        let res = run_read(&inst, "SELECT id, sev FROM events ORDER BY id").unwrap();
        assert_eq!(res.columns, vec!["id", "sev"]);
        assert_eq!(res.rows.len(), 2);
        assert_eq!(res.rows[0][0], serde_json::json!(1));
        let attr = run_read(
            &inst,
            "SELECT author FROM pond.snapshots() WHERE author = 'agent-test'",
        )
        .unwrap();
        assert!(
            !attr.rows.is_empty(),
            "expected attribution rows for agent-test"
        );
    }

    #[test]
    fn lineage_skip_reason_distinguishes_why_there_is_no_provenance() {
        // "This statement touched nothing" and "we could not find out what it
        // touched" are the same empty result to a caller, and this feature's
        // whole thesis is that silently under-reporting provenance is the worst
        // failure mode. The classification below is what the warn/debug lines
        // in `referenced_tables` are built on, so it is pinned here rather than
        // in a log-capture binary (each of those statically links DuckDB).
        let (_fs, inst) = pond();
        let id = Identity::claimed(Some("a"));
        run_write(&inst, "CREATE TABLE a(id INTEGER, v VARCHAR)", &id, "pond").unwrap();

        assert!(
            serialized_plan(&inst, "SELECT * FROM a").is_ok(),
            "a statement that plans must not be reported as a skip"
        );
        assert!(
            matches!(
                serialized_plan(&inst, "SELEC bogus"),
                Err(PlanSkip::NotPlanned(ref kind)) if kind == "parser"
            ),
            "a syntax error is DuckDB refusing to plan, and says which kind"
        );
        assert!(
            matches!(
                serialized_plan(&inst, "SELECT * FROM nope"),
                Err(PlanSkip::NotPlanned(ref kind)) if kind == "catalog"
            ),
            "an unknown table is a catalog error, not the same as a syntax error"
        );
        // Over the cap: the same shape the byte-cap test exercises end to end.
        let huge = format!("SELECT * FROM a WHERE a.v = '{}'", "x".repeat(600_000));
        assert!(
            matches!(serialized_plan(&inst, &huge), Err(PlanSkip::OverCap)),
            "an oversized plan is skipped deliberately, not a DuckDB failure"
        );
        // Every reason is a real sentence an operator can act on — a blank one
        // would make the log line useless.
        for skip in [
            PlanSkip::OverCap,
            PlanSkip::NotPlanned("io".into()),
            PlanSkip::Unavailable,
            PlanSkip::Unreadable,
        ] {
            assert!(skip.reason().len() > 10, "empty reason for {skip:?}");
        }
    }

    #[test]
    fn read_transaction_pins_the_plan_to_what_the_read_returns_and_leaves_none_open() {
        // The version recorded for an input comes from the bound plan, and is
        // the observed one only because the extraction runs inside the same
        // transaction as the read. Both must therefore name the snapshot the
        // rows came from, and the transaction must not outlive the call.
        let (_fs, inst) = pond();
        let id = Identity::claimed(Some("a"));
        run_write(&inst, "CREATE TABLE t(i INTEGER)", &id, "pond").unwrap();
        run_write(&inst, "INSERT INTO t VALUES (1)", &id, "pond").unwrap();
        let latest: i64 = inst
            .conn
            .query_row("SELECT max(snapshot_id) FROM pond.snapshots()", [], |r| {
                r.get(0)
            })
            .unwrap();
        let (rows, inputs) = in_read_txn(&inst, |i| {
            let (inputs, _) = referenced_tables(i, "SELECT * FROM t", "pond");
            Ok((run_read(i, "SELECT * FROM t")?.rows.len(), inputs))
        })
        .unwrap();
        assert_eq!(rows, 1);
        assert_eq!(
            inputs[0].version,
            Some(latest),
            "the input must carry the snapshot the transaction read at"
        );
        // The connection is not left mid-transaction: this write would fail
        // inside one, both because it is read-only and because COMMIT never ran.
        run_write(&inst, "INSERT INTO t VALUES (2)", &id, "pond").unwrap();
    }

    #[test]
    fn read_rejects_writes() {
        let (_fs, inst) = pond();
        assert!(matches!(
            run_read(&inst, "INSERT INTO t VALUES (1)"),
            Err(EngineError::ReadOnlyViolation)
        ));
    }

    #[test]
    fn read_zero_rows_has_columns() {
        let (_fs, inst) = pond();
        run_write(
            &inst,
            "CREATE TABLE events(id INTEGER, sev VARCHAR)",
            &Identity::claimed(Some("a")),
            "pond",
        )
        .unwrap();
        let res = run_read(&inst, "SELECT id FROM events WHERE 1=0").unwrap();
        assert_eq!(res.columns, vec!["id"]);
        assert!(res.rows.is_empty());
    }

    fn snapshot_count(inst: &PondInstance) -> i64 {
        inst.conn
            .query_row("SELECT count(*) FROM pond.snapshots()", [], |r| r.get(0))
            .unwrap()
    }

    /// The pond's largest snapshot id, or -1 when it has none. `run_write`
    /// decides "was this a write?" by whether this advanced, so a rollback test
    /// that only counted rows would miss a snapshot landing without its data.
    fn max_snapshot_id(inst: &PondInstance) -> i64 {
        max_snapshot(inst, "\"pond\"").unwrap_or(-1)
    }

    /// A statement that fails *inside* the transaction must roll the whole
    /// transaction back: no partial rows, no snapshot, and — the failure mode
    /// that actually hurts — no transaction left open on the pooled connection.
    ///
    /// The failure is a cast that binds and fails at execution, so the first
    /// statement of the batch really has run and really has rows to lose by the
    /// time the second one dies. Anything rejected at prepare time would leave
    /// nothing to roll back and prove nothing.
    #[test]
    fn write_rollback_undoes_a_partial_write_and_leaves_the_pond_usable() {
        let (_fs, inst) = pond();
        let id = Identity::claimed(Some("agent-test"));
        run_write(&inst, "CREATE TABLE t(i INTEGER)", &id, "pond").unwrap();
        run_write(&inst, "CREATE TABLE src(s VARCHAR)", &id, "pond").unwrap();
        run_write(&inst, "INSERT INTO src VALUES ('nope')", &id, "pond").unwrap();
        let snapshot_before = max_snapshot_id(&inst);

        let err = run_write(
            &inst,
            "INSERT INTO t VALUES (42); INSERT INTO t SELECT CAST(s AS INTEGER) FROM src",
            &id,
            "pond",
        )
        .expect_err("a runtime cast failure must fail the write");
        // Why it failed, not merely that it did. This test used to assert
        // `EngineError::Engine` — i.e. that a cast DuckDB rejects at RUN time
        // is indistinguishable from a crash of ours — which is the D7 bug
        // itself: the caller reached `internal` + "retry, report to your
        // operator" for a value it could have fixed. The failing statement is
        // the caller's and DuckDB classes it `Conversion Error`, so that is
        // what must come out, still carrying DuckDB's own message.
        match &err {
            EngineError::Conversion(msg) => assert!(
                msg.contains("Conversion Error") && msg.contains("nope"),
                "expected the failing cast's own message, got: {msg}"
            ),
            other => panic!("expected EngineError::Conversion, got {other:?}"),
        }

        // Consistency: the 42 that DID execute is not visible, and no snapshot
        // was published for a transaction that never completed.
        let rows = run_read(&inst, "SELECT count(*) AS c FROM t").unwrap();
        assert_eq!(
            rows.rows[0][0],
            serde_json::json!(0),
            "the partial write must not be visible after the rollback"
        );
        assert_eq!(
            max_snapshot_id(&inst),
            snapshot_before,
            "a failed write must not advance the pond's snapshot"
        );

        // The connection is not wedged: without the ROLLBACK the transaction
        // stays open and this write's own BEGIN is refused.
        let ok = run_write(&inst, "INSERT INTO t VALUES (7)", &id, "pond").unwrap();
        assert!(
            ok.meta.snapshot_id.is_some(),
            "the pond must still take writes after a rolled-back one"
        );
        let rows = run_read(&inst, "SELECT i FROM t").unwrap();
        assert_eq!(rows.rows, vec![vec![serde_json::json!(7)]]);
    }

    /// The second rollback arm: the user's statement succeeded and only our
    /// `set_commit_message` failed. Forced by naming a catalog the instance does
    /// not have — a mis-plumbed catalog name is exactly how this arm is reached
    /// in production, and the pond must not keep a write it could not attribute.
    #[test]
    fn write_rollback_when_attribution_cannot_be_recorded() {
        let (_fs, inst) = pond();
        let id = Identity::claimed(Some("agent-test"));
        run_write(&inst, "CREATE TABLE t(i INTEGER)", &id, "pond").unwrap();
        let snapshot_before = max_snapshot_id(&inst);

        let err = run_write(&inst, "INSERT INTO t VALUES (1)", &id, "not_this_catalog")
            .expect_err("attribution against an unknown catalog must fail the write");
        // Still `Engine`, deliberately, even though DuckDB calls it a `Catalog
        // Error`: the statement that failed is OURS, not the caller's, so the
        // caller cannot fix it by looking up table names and `internal` +
        // "report to your operator" is the honest answer. This is the boundary
        // of the class-based classification — it applies to caller SQL only.
        match &err {
            EngineError::Engine(msg) => assert!(
                msg.contains("set_commit_message"),
                "expected the failing attribution call's message, got: {msg}"
            ),
            other => panic!("expected EngineError::Engine, got {other:?}"),
        }

        let rows = run_read(&inst, "SELECT count(*) AS c FROM t").unwrap();
        assert_eq!(
            rows.rows[0][0],
            serde_json::json!(0),
            "an unattributable write must be rolled back, not kept"
        );
        assert_eq!(max_snapshot_id(&inst), snapshot_before);
        run_write(&inst, "INSERT INTO t VALUES (2)", &id, "pond")
            .expect("the pond must still take writes");
    }

    /// The `COMMIT` arm, and the one documented way to reach it: caller SQL that
    /// closes our bracket itself (`crates/latiq-engine-duckdb/CLAUDE.md` — the
    /// write path deliberately does not scan SQL, so this is reachable by
    /// contract, not by accident). Our `COMMIT` then finds no transaction.
    ///
    /// Both halves of that contract are pinned here because they differ in what
    /// survives: a caller `ROLLBACK` loses the write, a caller `COMMIT` keeps it
    /// but lands it **unattributed** (`author IS NULL` — the documented tell).
    /// Either way the error must surface and the connection must stay usable.
    #[test]
    fn write_commit_failure_surfaces_and_does_not_wedge_the_pond() {
        let (_fs, inst) = pond();
        let id = Identity::claimed(Some("agent-test"));
        run_write(&inst, "CREATE TABLE t(i INTEGER)", &id, "pond").unwrap();

        for (sql, expect_rows) in [
            ("INSERT INTO t VALUES (1); ROLLBACK", 0),
            ("INSERT INTO t VALUES (5); COMMIT", 1),
        ] {
            let err = match run_write(&inst, sql, &id, "pond") {
                Ok(r) => panic!("expected `{sql}` to fail at COMMIT, got {r:?}"),
                Err(e) => e,
            };
            let EngineError::Engine(msg) = &err else {
                panic!("expected EngineError::Engine for `{sql}`, got {err:?}");
            };
            assert!(
                msg.contains("no transaction is active"),
                "expected the failure to come from OUR commit finding no \
                 transaction (`{sql}`), got: {msg}"
            );
            let rows = run_read(&inst, "SELECT count(*) AS c FROM t").unwrap();
            assert_eq!(
                rows.rows[0][0],
                serde_json::json!(expect_rows),
                "wrong surviving row count for `{sql}`"
            );
        }
        // The caller's own COMMIT landed a snapshot with no author — the
        // documented tell that our attribution never reached it.
        let unattributed = run_read(
            &inst,
            "SELECT count(*) AS c FROM pond.snapshots() WHERE author IS NULL AND snapshot_id > 0",
        )
        .unwrap();
        assert_eq!(
            unattributed.rows[0][0],
            serde_json::json!(1),
            "caller-committed work must land exactly once, unattributed"
        );
        // Not wedged: the next write commits normally and IS attributed.
        let ok = run_write(&inst, "INSERT INTO t VALUES (9)", &id, "pond").unwrap();
        let sid = ok.meta.snapshot_id.expect("a normal write still commits");
        let author = run_read(
            &inst,
            &format!("SELECT author FROM pond.snapshots() WHERE snapshot_id = {sid}"),
        )
        .unwrap();
        assert_eq!(author.rows[0][0], serde_json::json!("agent-test"));
    }

    #[test]
    fn write_with_trailing_comment_does_not_wedge_the_pond() {
        let (_fs, inst) = pond();
        let id = Identity::claimed(Some("agent-test"));
        run_write(&inst, "CREATE TABLE t(id INTEGER)", &id, "pond").unwrap();
        // A trailing line comment must NOT comment out our COMMIT/attribution or
        // leave a dangling transaction on the reused connection.
        run_write(&inst, "INSERT INTO t VALUES (1) --trailing", &id, "pond").unwrap();
        // The pond is still usable (would error mid-transaction if wedged).
        run_write(&inst, "INSERT INTO t VALUES (2)", &id, "pond").unwrap();
        let n = run_read(&inst, "SELECT count(*) AS c FROM t").unwrap();
        assert_eq!(n.rows[0][0], serde_json::json!(2));
        // Attribution still recorded for the commented write's identity.
        let a = run_read(
            &inst,
            "SELECT count(*) AS c FROM pond.snapshots() WHERE author='agent-test'",
        )
        .unwrap();
        assert!(a.rows[0][0].as_i64().unwrap() >= 2);
    }

    #[test]
    fn write_query_with_a_select_creates_no_snapshot() {
        let (_fs, inst) = pond();
        let id = Identity::claimed(Some("agent-test"));
        run_write(&inst, "CREATE TABLE t(id INTEGER)", &id, "pond").unwrap();
        let before = snapshot_count(&inst);
        // A SELECT routed to write_query must not pollute snapshot history.
        let res = run_write(&inst, "SELECT 1 AS x", &id, "pond").unwrap();
        assert_eq!(res.rows[0][0], serde_json::json!(1));
        assert_eq!(
            snapshot_count(&inst),
            before,
            "SELECT must not add a snapshot"
        );
    }

    #[test]
    fn from_first_shorthand_is_a_read_not_a_write() {
        let (_fs, inst) = pond();
        let id = Identity::claimed(Some("agent-test"));
        run_write(&inst, "CREATE TABLE t(id INTEGER)", &id, "pond").unwrap();
        run_write(&inst, "INSERT INTO t VALUES (1),(2)", &id, "pond").unwrap();
        let before = snapshot_count(&inst);
        // DuckDB's `FROM t` shorthand is `SELECT * FROM t` — must read, and routed
        // through write_query must NOT create a snapshot (the reported bug: it ran
        // as a write, returning no rows and polluting history).
        let r = run_read(&inst, "FROM t ORDER BY id").unwrap();
        assert_eq!(r.rows.len(), 2);
        assert_eq!(r.rows[0][0], serde_json::json!(1));
        let w = run_write(&inst, "FROM t", &id, "pond").unwrap();
        assert_eq!(w.rows.len(), 2, "FROM via write path must return rows");
        assert_eq!(
            snapshot_count(&inst),
            before,
            "a FROM-first read must not add a snapshot"
        );
    }

    #[test]
    fn commented_read_through_write_path_returns_rows_and_no_snapshot() {
        // The exact #53 failure: a leading comment made the old keyword heuristic
        // treat this SELECT as a write → empty rows + a spurious snapshot. With
        // authoritative (snapshot-advance) detection it returns rows and adds no
        // snapshot — no text classification involved.
        let (_fs, inst) = pond();
        let id = Identity::claimed(Some("agent-test"));
        run_write(&inst, "CREATE TABLE t(id INTEGER)", &id, "pond").unwrap();
        run_write(&inst, "INSERT INTO t VALUES (10)", &id, "pond").unwrap();
        let before = snapshot_count(&inst);
        let res = run_write(&inst, "-- fetch it\nSELECT id FROM t", &id, "pond").unwrap();
        assert_eq!(res.rows.len(), 1, "commented SELECT must return its rows");
        assert_eq!(res.rows[0][0], serde_json::json!(10));
        assert_eq!(
            snapshot_count(&inst),
            before,
            "a commented read must not add a snapshot"
        );
    }

    #[test]
    fn string_literal_write_word_read_through_write_path_is_a_read() {
        // The other #53 case: a write keyword inside a string literal. Authoritative
        // detection doesn't care about the text — nothing changed, so no snapshot.
        let (_fs, inst) = pond();
        let id = Identity::claimed(Some("agent-test"));
        run_write(&inst, "CREATE TABLE t(note VARCHAR)", &id, "pond").unwrap();
        run_write(&inst, "INSERT INTO t VALUES (' update me ')", &id, "pond").unwrap();
        let before = snapshot_count(&inst);
        let res = run_write(
            &inst,
            "SELECT note FROM t WHERE note = ' update me '",
            &id,
            "pond",
        )
        .unwrap();
        assert_eq!(
            res.rows.len(),
            1,
            "SELECT with 'update' in a literal is a read"
        );
        assert_eq!(snapshot_count(&inst), before, "must not add a snapshot");
    }

    #[test]
    fn multi_statement_write_persists_and_adds_one_snapshot() {
        // A multi-statement write is one transaction → one snapshot, and every
        // statement executes (was supported by the old execute_batch path).
        let (_fs, inst) = pond();
        let id = Identity::claimed(Some("agent-test"));
        let before = snapshot_count(&inst);
        run_write(
            &inst,
            "CREATE TABLE m(x INTEGER); INSERT INTO m VALUES (1),(2),(3)",
            &id,
            "pond",
        )
        .unwrap();
        assert_eq!(
            snapshot_count(&inst) - before,
            1,
            "one transaction must add exactly one snapshot"
        );
        let n = run_read(&inst, "SELECT count(*) AS c FROM m").unwrap();
        assert_eq!(
            n.rows[0][0],
            serde_json::json!(3),
            "all statements must run"
        );
    }

    #[test]
    fn read_rejects_cte_that_writes() {
        let (_fs, inst) = pond();
        run_write(
            &inst,
            "CREATE TABLE t(id INTEGER)",
            &Identity::claimed(None),
            "pond",
        )
        .unwrap();
        assert!(matches!(
            run_read(&inst, "WITH c AS (SELECT 1) INSERT INTO t SELECT * FROM c"),
            Err(EngineError::ReadOnlyViolation)
        ));
    }

    #[test]
    fn explain_refuses_analyze_and_does_not_execute() {
        let (_fs, inst) = pond();
        let id = Identity::claimed(None);
        run_write(&inst, "CREATE TABLE t(id INTEGER)", &id, "pond").unwrap();
        run_write(&inst, "INSERT INTO t VALUES (1),(2)", &id, "pond").unwrap();
        assert!(run_explain(&inst, "ANALYZE DELETE FROM t").is_err());
        // The rows must still be there — ANALYZE was refused, not executed.
        let n = run_read(&inst, "SELECT count(*) AS c FROM t").unwrap();
        assert_eq!(n.rows[0][0], serde_json::json!(2));
    }

    #[test]
    fn temporal_values_render_as_iso_strings_not_debug() {
        let (_fs, inst) = pond();
        let res = run_read(
            &inst,
            "SELECT DATE '2021-07-01' AS d, \
             TIMESTAMP '2021-07-01 13:45:06' AS ts, \
             TIME '13:45:06' AS t",
        )
        .unwrap();
        assert_eq!(res.rows[0][0], serde_json::json!("2021-07-01"));
        assert_eq!(res.rows[0][1], serde_json::json!("2021-07-01 13:45:06"));
        assert_eq!(res.rows[0][2], serde_json::json!("13:45:06"));
    }

    #[test]
    fn nested_types_render_as_json_not_arrow_debug() {
        let (_fs, inst) = pond();
        let res = run_read(
            &inst,
            "SELECT [1,2,3] AS l, {'x': 1, 'y': 'hi'} AS s, MAP {'k': [10,20]} AS m",
        )
        .unwrap();
        assert_eq!(res.rows[0][0], serde_json::json!([1, 2, 3]));
        assert_eq!(res.rows[0][1], serde_json::json!({"x": 1, "y": "hi"}));
        assert_eq!(res.rows[0][2], serde_json::json!({"k": [10, 20]}));
    }

    /// Every `value_to_json` arm, pinned to the **exact** JSON we emit — value
    /// and shape both, because a consumer parses these and a `Decimal` that
    /// silently became a JSON number (or an `Interval` that became a string)
    /// is a wire-format break, not a cosmetic one.
    ///
    /// Invariant 10: this asserts *our* conversion, never DuckDB's semantics.
    /// Each case is one column of one real query, so the arm is reached the way
    /// a caller reaches it. Only the exactly-representable float values are used
    /// (1.5, 2.5) so the assertion is about our mapping, not float printing.
    #[test]
    fn result_encoding_covers_every_value_arm_with_an_exact_shape() {
        let (_fs, inst) = pond();
        // (label, SQL expression, the JSON we promise for it)
        let cases: Vec<(&str, &str, serde_json::Value)> = vec![
            ("NULL", "NULL::INTEGER", serde_json::Value::Null),
            ("BOOLEAN", "true", serde_json::json!(true)),
            ("BOOLEAN false", "false", serde_json::json!(false)),
            ("TINYINT", "(-8)::TINYINT", serde_json::json!(-8)),
            ("SMALLINT", "(-16)::SMALLINT", serde_json::json!(-16)),
            ("INTEGER", "(-32)::INTEGER", serde_json::json!(-32)),
            ("BIGINT", "(-64)::BIGINT", serde_json::json!(-64)),
            ("UTINYINT", "255::UTINYINT", serde_json::json!(255)),
            ("USMALLINT", "65535::USMALLINT", serde_json::json!(65535)),
            (
                "UINTEGER",
                "4294967295::UINTEGER",
                serde_json::json!(4294967295u32),
            ),
            // Above i64::MAX: u64 has its own serde_json::Number, so this must
            // stay a number with full precision (unlike HugeInt, which cannot).
            (
                "UBIGINT",
                "18446744073709551615::UBIGINT",
                serde_json::json!(18446744073709551615u64),
            ),
            ("FLOAT", "1.5::FLOAT", serde_json::json!(1.5)),
            ("DOUBLE", "2.5::DOUBLE", serde_json::json!(2.5)),
            // A STRING, not a number: DECIMAL is exact and JSON numbers are not,
            // so rendering 3.14 as a float would hand the consumer a value that
            // is no longer the one stored.
            ("DECIMAL", "3.14::DECIMAL(5,2)", serde_json::json!("3.14")),
            (
                "DECIMAL wide",
                "123456789012345678.90::DECIMAL(38,2)",
                serde_json::json!("123456789012345678.90"),
            ),
            // An OBJECT of the three independent components, not a string: a
            // month is not a fixed number of days, so any single scalar would
            // have to invent a calendar.
            (
                "INTERVAL",
                "INTERVAL 1 MONTH + INTERVAL 2 DAY + INTERVAL 3 SECOND",
                serde_json::json!({"months": 1, "days": 2, "nanos": 3_000_000_000i64}),
            ),
            // Lowercase hex, no prefix, no separators.
            ("BLOB", "'ab'::BLOB", serde_json::json!("6162")),
            (
                "BLOB non-ascii",
                "'\\xFF\\x00'::BLOB",
                serde_json::json!("ff00"),
            ),
            ("ENUM", "'x'::ENUM('x','y')", serde_json::json!("x")),
            ("VARCHAR", "'hi'", serde_json::json!("hi")),
            // TimeUnit: only Microsecond was ever exercised. Second carries no
            // fraction; Nanosecond truncates to microseconds (our format's
            // resolution) rather than rounding or overflowing.
            (
                "TIMESTAMP_S",
                "'2021-07-01 13:45:06'::TIMESTAMP_S",
                serde_json::json!("2021-07-01 13:45:06"),
            ),
            (
                "TIMESTAMP_MS",
                "'2021-07-01 13:45:06.123'::TIMESTAMP_MS",
                serde_json::json!("2021-07-01 13:45:06.123000"),
            ),
            (
                "TIMESTAMP_NS",
                "'2021-07-01 13:45:06.123456789'::TIMESTAMP_NS",
                serde_json::json!("2021-07-01 13:45:06.123456"),
            ),
            // The same arms, reached through the nested containers: a LIST and a
            // STRUCT must map their elements with the identical rules, not fall
            // back to a Debug rendering.
            (
                "LIST of DECIMAL",
                "[1.10::DECIMAL(4,2), 2.20::DECIMAL(4,2)]",
                serde_json::json!(["1.10", "2.20"]),
            ),
            (
                "STRUCT of BOOLEAN/BLOB/FLOAT",
                "{'b': false, 'raw': 'ab'::BLOB, 'f': 1.5::FLOAT}",
                serde_json::json!({"b": false, "raw": "6162", "f": 1.5}),
            ),
        ];
        // Anti-vacuity: the loop must actually run, and this count is what makes
        // a case silently dropped from the table a failing test.
        assert_eq!(cases.len(), 25, "the value-arm table lost or gained a case");
        for (label, expr, expected) in &cases {
            let res = run_read(&inst, &format!("SELECT {expr} AS v"))
                .unwrap_or_else(|e| panic!("{label}: query failed: {e:?}"));
            assert_eq!(res.rows.len(), 1, "{label}: expected exactly one row");
            assert_eq!(&res.rows[0][0], expected, "{label}: wrong JSON encoding");
        }
    }

    #[test]
    fn hugeint_beyond_i64_is_returned_as_string_not_truncated() {
        let (_fs, inst) = pond();
        // i64::MAX + 1 as HUGEINT must round-trip with full precision (string),
        // never wrap to a wrong i64.
        let res = run_read(&inst, "SELECT 9223372036854775808::HUGEINT AS x").unwrap();
        assert_eq!(res.rows[0][0], serde_json::json!("9223372036854775808"));
    }
}
