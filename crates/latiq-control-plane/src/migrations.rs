//! Forward-only schema migrations for the control-plane registry.
use crate::error::ControlPlaneError;
use duckdb::Connection;

/// Ordered DDL migrations. Index 0 → version 1, etc. Append-only; never edit
/// a shipped migration (add a new one).
pub const MIGRATIONS: &[&str] = &[
    // v1: initial schema
    r#"
    CREATE TABLE nodes (
        node_id           VARCHAR PRIMARY KEY,
        mcp_endpoint      VARCHAR NOT NULL,
        internal_endpoint VARCHAR NOT NULL,
        capacity          UINTEGER NOT NULL,
        pond_count        UINTEGER NOT NULL DEFAULT 0,
        state             VARCHAR NOT NULL DEFAULT 'active',
        last_heartbeat    TIMESTAMP NOT NULL DEFAULT current_timestamp
    );
    CREATE TABLE ponds (
        pond_id        VARCHAR PRIMARY KEY,
        name           VARCHAR NOT NULL UNIQUE,
        owner_identity VARCHAR NOT NULL,
        node_id        VARCHAR NOT NULL,
        policy_json    VARCHAR NOT NULL DEFAULT '{}',
        created_at     TIMESTAMP NOT NULL DEFAULT current_timestamp,
        state          VARCHAR NOT NULL DEFAULT 'active'
    );
    CREATE TABLE policy (
        key   VARCHAR PRIMARY KEY,
        value VARCHAR NOT NULL
    );
    CREATE TABLE audit_log (
        audit_id          VARCHAR PRIMARY KEY,
        ts                TIMESTAMP NOT NULL DEFAULT current_timestamp,
        agent_identity    VARCHAR NOT NULL,
        identity_verified BOOLEAN NOT NULL,
        operation         VARCHAR NOT NULL,
        pond_id           VARCHAR,
        request_summary   VARCHAR,
        result_summary    VARCHAR,
        duration_ms       UBIGINT NOT NULL DEFAULT 0
    );
    INSERT INTO policy(key, value) VALUES
        ('default_pond_lifetime_seconds', '3600'),
        ('query_timeout_seconds', '30');
    "#,
    // v2: per-pond resource tier (small/medium/large/x-large). Nullable with a
    // default so existing rows read as 'medium'; the engine maps it to caps.
    "ALTER TABLE ponds ADD COLUMN tier VARCHAR DEFAULT 'medium';",
    // v3: per-pond optional DuckDB extensions, stored comma-separated (empty =
    // none). The engine LOADs them from the deployment image on pond open.
    "ALTER TABLE ponds ADD COLUMN extensions VARCHAR DEFAULT '';",
];

pub fn run_migrations(conn: &Connection) -> Result<(), ControlPlaneError> {
    conn.execute_batch(
        "CREATE TABLE IF NOT EXISTS _latiq_schema_version (version INTEGER NOT NULL);",
    )?;
    let current: i64 = conn
        .query_row(
            "SELECT coalesce(max(version), 0) FROM _latiq_schema_version",
            [],
            |r| r.get(0),
        )
        .unwrap_or(0);
    for (i, ddl) in MIGRATIONS.iter().enumerate() {
        let version = (i + 1) as i64;
        if version > current {
            conn.execute_batch(ddl)?;
            conn.execute(
                "INSERT INTO _latiq_schema_version(version) VALUES (?)",
                [version],
            )?;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn migrations_are_idempotent() {
        let conn = Connection::open_in_memory().unwrap();
        run_migrations(&conn).unwrap();
        run_migrations(&conn).unwrap(); // second run is a no-op
        let v: i64 = conn
            .query_row("SELECT max(version) FROM _latiq_schema_version", [], |r| {
                r.get(0)
            })
            .unwrap();
        assert_eq!(v, MIGRATIONS.len() as i64);
        let n: i64 = conn
            .query_row("SELECT count(*) FROM policy", [], |r| r.get(0))
            .unwrap();
        assert_eq!(n, 2);
    }
}
