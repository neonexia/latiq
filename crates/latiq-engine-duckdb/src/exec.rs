//! Query execution against an attached pond instance: read (SELECT),
//! write (txn-wrapped + native DuckLake attribution), and explain.
use crate::instance::PondInstance;
use duckdb::types::ValueRef;
use latiq_common::{Identity, QueryMeta};
use latiq_engine::{is_read_only, AbortToken, ArrowSink, EngineError, ExplainResult, QueryResult};
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

/// Execute a statement and materialize its result rows aligned to column names.
/// Works for any statement — a SELECT yields its rows; a write/DDL executes and
/// yields DuckDB's summary row (which write callers drop). Multi-statement input
/// executes every statement and returns the last one's result.
fn materialize(
    inst: &PondInstance,
    sql: &str,
) -> Result<(Vec<String>, Vec<Vec<serde_json::Value>>), EngineError> {
    let mut stmt = inst
        .conn
        .prepare(sql)
        .map_err(|e| EngineError::Parse(e.to_string()))?;
    let mut rows = stmt
        .query([])
        .map_err(|e| EngineError::Engine(e.to_string()))?;
    let mut out: Vec<Vec<serde_json::Value>> = Vec::new();
    let mut columns: Vec<String> = Vec::new();
    let mut have_columns = false;
    while let Some(row) = rows
        .next()
        .map_err(|e| EngineError::Engine(e.to_string()))?
    {
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
                row.get_ref(i)
                    .map_err(|e| EngineError::Engine(e.to_string()))?,
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
) -> Result<(), EngineError> {
    if !is_read_only(sql) {
        return Err(EngineError::ReadOnlyViolation);
    }
    let mut stmt = inst
        .conn
        .prepare(sql)
        .map_err(|e| EngineError::Parse(e.to_string()))?;
    let arrow = stmt
        .query_arrow([])
        .map_err(|e| EngineError::Engine(e.to_string()))?;
    // Schema is available even for an empty result, so downstream IPC/JSON always
    // has columns.
    if sink.schema(arrow.get_schema()).is_break() {
        return Ok(());
    }
    for batch in arrow {
        if abort.is_cancelled() {
            return Err(EngineError::Cancelled);
        }
        if sink.batch(batch).is_break() {
            break;
        }
    }
    Ok(())
}

/// Run a read-only query, materializing rows aligned to column names.
///
/// The `is_read_only` guard here is the read surface's "won't mutate" contract.
/// It is a text heuristic (interim) — the authoritative, engine-enforced version
/// (a read-only transaction) is deferred to the authorization work, since that is
/// where read-only *enforcement* as a permission belongs. Its remaining failure
/// mode is benign: at worst it rejects an unusual read, never lets a write slip
/// through unattributed.
pub fn run_read(inst: &PondInstance, sql: &str) -> Result<QueryResult, EngineError> {
    if !is_read_only(sql) {
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
        .map_err(|e| EngineError::Parse(e.to_string()))?;
    let mut rows = stmt
        .query([])
        .map_err(|e| EngineError::Engine(e.to_string()))?;
    let mut plan = String::new();
    while let Some(row) = rows
        .next()
        .map_err(|e| EngineError::Engine(e.to_string()))?
    {
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

    #[test]
    fn hugeint_beyond_i64_is_returned_as_string_not_truncated() {
        let (_fs, inst) = pond();
        // i64::MAX + 1 as HUGEINT must round-trip with full precision (string),
        // never wrap to a wrong i64.
        let res = run_read(&inst, "SELECT 9223372036854775808::HUGEINT AS x").unwrap();
        assert_eq!(res.rows[0][0], serde_json::json!("9223372036854775808"));
    }
}
