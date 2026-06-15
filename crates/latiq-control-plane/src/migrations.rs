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
    // v4: the dataset catalog — named, namespaced external tables operators curate
    // and agents/clients load into ponds. `ref` is the full "<namespace>.<name>".
    // Seeds the built-in samples under the `latiq.sample` namespace.
    r#"
    CREATE TABLE datasets (
        ref          VARCHAR PRIMARY KEY,
        namespace    VARCHAR NOT NULL,
        name         VARCHAR NOT NULL,
        description  VARCHAR NOT NULL DEFAULT '',
        created_by   VARCHAR NOT NULL DEFAULT 'anonymous',
        created_at   TIMESTAMP NOT NULL DEFAULT current_timestamp
    );
    CREATE TABLE dataset_tables (
        ref          VARCHAR NOT NULL,
        table_name   VARCHAR NOT NULL,
        source_uri   VARCHAR NOT NULL,
        format       VARCHAR NOT NULL DEFAULT 'auto',
        PRIMARY KEY (ref, table_name)
    );
    CREATE TABLE dataset_tags (
        ref          VARCHAR NOT NULL,
        tag          VARCHAR NOT NULL,
        PRIMARY KEY (ref, tag)
    );
    INSERT INTO datasets(ref, namespace, name, description, created_by) VALUES
      ('latiq.sample.startrek','latiq.sample','startrek','Star Trek Season 1 scripts — CSV, ~2 KB','latiq'),
      ('latiq.sample.holdings','latiq.sample','holdings','Example stock holdings — CSV, ~300 B','latiq'),
      ('latiq.sample.tpch','latiq.sample','tpch','TPC-H scale 0.01 — 8 tables, Parquet, a few MB','latiq'),
      ('latiq.sample.taxi','latiq.sample','taxi','NYC yellow-taxi, Apr 2019 — Parquet, ~127 MB (large)','latiq');
    INSERT INTO dataset_tables(ref, table_name, source_uri) VALUES
      ('latiq.sample.startrek','startrek','https://blobs.duckdb.org/data/Star_Trek-Season_1.csv'),
      ('latiq.sample.holdings','holdings','https://duckdb.org/data/holdings.csv'),
      ('latiq.sample.tpch','lineitem','https://shell.duckdb.org/data/tpch/0_01/parquet/lineitem.parquet'),
      ('latiq.sample.tpch','orders','https://shell.duckdb.org/data/tpch/0_01/parquet/orders.parquet'),
      ('latiq.sample.tpch','customer','https://shell.duckdb.org/data/tpch/0_01/parquet/customer.parquet'),
      ('latiq.sample.tpch','part','https://shell.duckdb.org/data/tpch/0_01/parquet/part.parquet'),
      ('latiq.sample.tpch','supplier','https://shell.duckdb.org/data/tpch/0_01/parquet/supplier.parquet'),
      ('latiq.sample.tpch','partsupp','https://shell.duckdb.org/data/tpch/0_01/parquet/partsupp.parquet'),
      ('latiq.sample.tpch','nation','https://shell.duckdb.org/data/tpch/0_01/parquet/nation.parquet'),
      ('latiq.sample.tpch','region','https://shell.duckdb.org/data/tpch/0_01/parquet/region.parquet'),
      ('latiq.sample.taxi','taxi','https://blobs.duckdb.org/data/taxi_2019_04.parquet');
    INSERT INTO dataset_tags(ref, tag) VALUES
      ('latiq.sample.startrek','sample'),
      ('latiq.sample.holdings','sample'),
      ('latiq.sample.tpch','sample'),('latiq.sample.tpch','tpch'),
      ('latiq.sample.taxi','sample');
    "#,
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
