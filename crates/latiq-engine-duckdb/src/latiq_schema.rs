//! The reserved `_latiq` schema: read-only views over DuckLake + DuckDB catalog.
//! Pure DuckLake — no Latiq-side store (spec §9).
use duckdb::Connection;
use latiq_engine::EngineError;

/// Create the `_latiq` schema + views on a freshly-attached pond instance.
///
/// Column list for `pond.snapshots()` is per spike findings (m1-spike-findings.md, Probe B):
///   snapshot_id, author, commit_message
/// (No `snapshot_time` column — it was not present in the confirmed output.)
pub fn create_latiq_schema(conn: &Connection) -> Result<(), EngineError> {
    let sql = r#"
        CREATE SCHEMA IF NOT EXISTS _latiq;
        CREATE OR REPLACE VIEW _latiq.snapshots AS
            SELECT snapshot_id, author, commit_message
            FROM pond.snapshots();
        CREATE OR REPLACE VIEW _latiq.attribution AS
            SELECT snapshot_id, author, commit_message
            FROM pond.snapshots();
        CREATE OR REPLACE VIEW _latiq.tables_summary AS
            SELECT table_name AS name, estimated_size AS row_count, comment
            FROM duckdb_tables()
            WHERE database_name = 'pond';
        CREATE OR REPLACE VIEW _latiq.sources AS
            SELECT NULL::VARCHAR AS name WHERE 1=0;
    "#;
    conn.execute_batch(sql)
        .map_err(|e| EngineError::Engine(format!("create _latiq: {e}")))
}

/// Returns true if the SQL writes to the reserved `_latiq` schema. Conservative
/// substring/keyword check for Slice 0+ (a full parser comes later).
pub fn writes_reserved_schema(sql: &str) -> bool {
    let lower = sql.to_lowercase();
    let writes = [
        "insert into",
        "update",
        "delete from",
        "drop",
        "create",
        "alter",
        "truncate",
    ];
    writes.iter().any(|w| lower.contains(w)) && lower.contains("_latiq")
}

#[cfg(test)]
mod tests {
    use super::*;
    use latiq_common::PondId;
    use latiq_storage::{PondStorage, TempFs};

    use crate::instance::PondInstance;

    #[test]
    fn detects_reserved_writes() {
        assert!(writes_reserved_schema(
            "INSERT INTO _latiq.attribution VALUES (1)"
        ));
        assert!(writes_reserved_schema("DROP VIEW _latiq.snapshots"));
        assert!(!writes_reserved_schema("SELECT * FROM _latiq.snapshots"));
        assert!(!writes_reserved_schema("INSERT INTO events VALUES (1)"));
    }

    #[test]
    fn snapshots_view_is_queryable() {
        let fs = TempFs::new();
        let loc = fs.create_pond(PondId::new()).unwrap();
        let inst = PondInstance::open(&loc).unwrap();
        create_latiq_schema(&inst.conn).unwrap();
        // Must not error — validates the view SQL compiles against the real DuckLake catalog.
        inst.conn
            .execute_batch("SELECT * FROM _latiq.snapshots LIMIT 0")
            .expect("_latiq.snapshots view should be queryable");
    }
}
