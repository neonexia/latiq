//! Query execution against an attached pond instance: read (SELECT),
//! write (txn-wrapped + native DuckLake attribution), and explain.
use crate::instance::PondInstance;
use duckdb::types::ValueRef;
use latiq_common::{Identity, QueryMeta};
use latiq_engine::{EngineError, ExplainResult, QueryResult};
use std::time::Instant;

/// Convert a single DuckDB cell to a JSON value for the neutral result.
fn cell_to_json(v: ValueRef<'_>) -> serde_json::Value {
    use serde_json::Value;
    match v {
        ValueRef::Null => Value::Null,
        ValueRef::Boolean(b) => Value::Bool(b),
        ValueRef::TinyInt(i) => Value::from(i),
        ValueRef::SmallInt(i) => Value::from(i),
        ValueRef::Int(i) => Value::from(i),
        ValueRef::BigInt(i) => Value::from(i),
        // i128 doesn't fit serde_json::Number; keep full precision as a string
        // when it overflows i64 (never silently truncate via `as i64`).
        ValueRef::HugeInt(i) => match i64::try_from(i) {
            Ok(v) => Value::from(v),
            Err(_) => Value::String(i.to_string()),
        },
        ValueRef::UTinyInt(i) => Value::from(i),
        ValueRef::USmallInt(i) => Value::from(i),
        ValueRef::UInt(i) => Value::from(i),
        ValueRef::UBigInt(i) => Value::from(i),
        ValueRef::Float(f) => Value::from(f),
        ValueRef::Double(f) => Value::from(f),
        ValueRef::Text(t) => Value::String(String::from_utf8_lossy(t).into_owned()),
        ValueRef::Timestamp(_, _) | ValueRef::Date32(_) | ValueRef::Time64(_, _) => {
            Value::String(format!("{v:?}"))
        }
        other => Value::String(format!("{other:?}")),
    }
}

/// Heuristic: is this SQL a read-only statement (safe for `read_query`)?
///
/// DuckDB exposes no statement-type introspection, so this is a careful
/// heuristic: the statement must start with a read keyword AND not contain any
/// data-modifying keyword (which catches `WITH … INSERT`, `EXPLAIN ANALYZE …`,
/// side-effecting `PRAGMA`/`CALL`, etc.). It errs toward rejecting ambiguous
/// statements — a wrongly-rejected read is recoverable; a write slipping through
/// the read path is not.
fn is_read_only(sql: &str) -> bool {
    let s = sql.trim_start().to_lowercase();
    let starts_read = s.starts_with("select")
        || s.starts_with("with")
        || s.starts_with("describe")
        || s.starts_with("show")
        || s.starts_with("explain")
        || s.starts_with("pragma");
    if !starts_read {
        return false;
    }
    // Word-boundary scan for data-modifying / side-effecting keywords.
    let normalized = format!(" {} ", s.replace(['\n', '\t', '(', ')', ','], " "));
    const WRITE_KEYWORDS: &[&str] = &[
        " insert ",
        " update ",
        " delete ",
        " create ",
        " drop ",
        " alter ",
        " truncate ",
        " attach ",
        " detach ",
        " copy ",
        " call ",
        " install ",
        " load ",
        " replace ",
        " analyze ",
        " vacuum ",
        " checkpoint ",
    ];
    !WRITE_KEYWORDS.iter().any(|w| normalized.contains(w))
}

/// Whether a statement is a no-op for the write path (a read routed to
/// write_query): run it without creating a snapshot/attribution.
fn is_read_for_write(sql: &str) -> bool {
    is_read_only(sql)
}

/// Run a read-only query, materializing rows aligned to column names.
pub fn run_read(inst: &PondInstance, sql: &str) -> Result<QueryResult, EngineError> {
    if !is_read_only(sql) {
        return Err(EngineError::ReadOnlyViolation);
    }
    let t0 = Instant::now();
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
    let meta = QueryMeta {
        rows: out.len() as u64,
        duration_ms: t0.elapsed().as_millis() as u64,
        ..Default::default()
    };
    Ok(QueryResult {
        columns,
        rows: out,
        meta,
    })
}

/// Run a write/DDL statement and stamp native DuckLake attribution.
///
/// The user's SQL is executed as its OWN statement (not concatenated into a
/// single `BEGIN; … ; COMMIT;` string), so a trailing comment or embedded `;`
/// in user SQL cannot comment out / shift our COMMIT or attribution call. Our
/// `set_commit_message` is issued LAST so a user-supplied one can't override it,
/// and any failure rolls the transaction back (the per-pond connection is
/// reused, so a dangling open transaction would wedge the pond).
pub fn run_write(
    inst: &PondInstance,
    sql: &str,
    identity: &Identity,
) -> Result<QueryResult, EngineError> {
    // A read routed to write_query: run it without creating a snapshot or
    // attribution (no history pollution), returning its rows gracefully.
    if is_read_for_write(sql) {
        return run_read(inst, sql);
    }

    let t0 = Instant::now();
    let exec = |s: &str| {
        inst.conn
            .execute_batch(s)
            .map_err(|e| EngineError::Engine(e.to_string()))
    };
    let rollback = || {
        let _ = inst.conn.execute_batch("ROLLBACK");
    };

    exec("BEGIN")?;
    if let Err(e) = exec(sql) {
        rollback();
        return Err(e);
    }
    let agent = identity.agent_id.replace('\'', "''");
    let extra = format!("{{\"verified\":{}}}", identity.verified);
    let call =
        format!("CALL pond.set_commit_message('{agent}', 'write_query', extra_info => '{extra}')");
    if let Err(e) = exec(&call) {
        rollback();
        return Err(e);
    }
    if let Err(e) = exec("COMMIT") {
        rollback();
        return Err(e);
    }

    let snapshot_id: Option<i64> = inst
        .conn
        .query_row("SELECT max(snapshot_id) FROM pond.snapshots()", [], |r| {
            r.get(0)
        })
        .ok();
    let meta = QueryMeta {
        snapshot_id,
        duration_ms: t0.elapsed().as_millis() as u64,
        ..Default::default()
    };
    Ok(QueryResult {
        columns: vec![],
        rows: vec![],
        meta,
    })
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
        let loc = fs.create_pond(PondId::new()).unwrap();
        let inst = PondInstance::open(&loc).unwrap();
        (fs, inst)
    }

    #[test]
    fn write_then_read_with_attribution() {
        let (_fs, inst) = pond();
        let id = Identity::claimed(Some("agent-test"));
        run_write(&inst, "CREATE TABLE events(id INTEGER, sev VARCHAR)", &id).unwrap();
        run_write(&inst, "INSERT INTO events VALUES (1,'high'),(2,'low')", &id).unwrap();
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
        run_write(&inst, "CREATE TABLE t(id INTEGER)", &id).unwrap();
        // A trailing line comment must NOT comment out our COMMIT/attribution or
        // leave a dangling transaction on the reused connection.
        run_write(&inst, "INSERT INTO t VALUES (1) --trailing", &id).unwrap();
        // The pond is still usable (would error mid-transaction if wedged).
        run_write(&inst, "INSERT INTO t VALUES (2)", &id).unwrap();
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
        run_write(&inst, "CREATE TABLE t(id INTEGER)", &id).unwrap();
        let before = snapshot_count(&inst);
        // A SELECT routed to write_query must not pollute snapshot history.
        let res = run_write(&inst, "SELECT 1 AS x", &id).unwrap();
        assert_eq!(res.rows[0][0], serde_json::json!(1));
        assert_eq!(
            snapshot_count(&inst),
            before,
            "SELECT must not add a snapshot"
        );
    }

    #[test]
    fn read_rejects_cte_that_writes() {
        let (_fs, inst) = pond();
        run_write(
            &inst,
            "CREATE TABLE t(id INTEGER)",
            &Identity::claimed(None),
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
        run_write(&inst, "CREATE TABLE t(id INTEGER)", &id).unwrap();
        run_write(&inst, "INSERT INTO t VALUES (1),(2)", &id).unwrap();
        assert!(run_explain(&inst, "ANALYZE DELETE FROM t").is_err());
        // The rows must still be there — ANALYZE was refused, not executed.
        let n = run_read(&inst, "SELECT count(*) AS c FROM t").unwrap();
        assert_eq!(n.rows[0][0], serde_json::json!(2));
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
