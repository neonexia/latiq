//! Query execution against an attached pond instance: read (SELECT),
//! write (txn-wrapped + native DuckLake attribution), and explain.
use crate::instance::PondInstance;
use crate::latiq_schema::writes_reserved_schema;
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
        ValueRef::HugeInt(i) => Value::from(i as i64),
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
fn is_read_only(sql: &str) -> bool {
    let s = sql.trim_start().to_lowercase();
    s.starts_with("select")
        || s.starts_with("with")
        || s.starts_with("describe")
        || s.starts_with("show")
        || s.starts_with("explain")
        || s.starts_with("pragma")
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

/// Run a write/DDL statement inside a transaction, stamping native DuckLake
/// attribution via `set_commit_message`. Records the resulting snapshot id.
pub fn run_write(
    inst: &PondInstance,
    sql: &str,
    identity: &Identity,
) -> Result<QueryResult, EngineError> {
    if writes_reserved_schema(sql) {
        return Err(EngineError::ReservedSchemaWrite);
    }
    let t0 = Instant::now();
    let agent = identity.agent_id.replace('\'', "''");
    let extra = format!("{{\"verified\":{}}}", identity.verified);
    let trimmed = sql.trim().trim_end_matches(';');
    let batch = format!(
        "BEGIN; {trimmed}; CALL pond.set_commit_message('{agent}', 'write_query', extra_info => '{extra}'); COMMIT;"
    );
    inst.conn
        .execute_batch(&batch)
        .map_err(|e| EngineError::Engine(e.to_string()))?;
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
    use crate::latiq_schema::create_latiq_schema;
    use latiq_common::PondId;
    use latiq_storage::{PondStorage, TempFs};

    fn pond() -> (TempFs, PondInstance) {
        let fs = TempFs::new();
        let loc = fs.create_pond(PondId::new()).unwrap();
        let inst = PondInstance::open(&loc).unwrap();
        create_latiq_schema(&inst.conn).unwrap();
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
            "SELECT author FROM _latiq.attribution WHERE author = 'agent-test'",
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
    fn rejects_reserved_schema_write() {
        let (_fs, inst) = pond();
        assert!(matches!(
            run_write(
                &inst,
                "INSERT INTO _latiq.attribution VALUES (1)",
                &Identity::claimed(None)
            ),
            Err(EngineError::ReservedSchemaWrite)
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
}
