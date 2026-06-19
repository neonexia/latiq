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
    // v4: the data catalog — two first-class, separate concepts.
    //
    //  * CATALOG — an external attachable source (iceberg/ducklake/duckdb/…).
    //    Registered with a `type` + opaque `params` (locator metadata; the
    //    per-type attacher allowlists them — never credentials). Pulled from
    //    transiently: attach → query → detach. Its tables are *discovered*.
    //  * DATASET — a simple file (parquet/csv) or set of files, living in the
    //    built-in `latiq` catalog. `load`ed into a pond (copied), one table each.
    //
    // Seeds the built-in samples as datasets.
    r#"
    CREATE TABLE catalogs (
        name         VARCHAR PRIMARY KEY,
        type         VARCHAR NOT NULL,
        params_json  VARCHAR NOT NULL DEFAULT '{}',
        description  VARCHAR NOT NULL DEFAULT '',
        created_by   VARCHAR NOT NULL DEFAULT 'anonymous',
        created_at   TIMESTAMP NOT NULL DEFAULT current_timestamp
    );
    CREATE TABLE catalog_tags (
        catalog VARCHAR NOT NULL,
        tag     VARCHAR NOT NULL,
        PRIMARY KEY (catalog, tag)
    );
    CREATE TABLE datasets (
        name         VARCHAR PRIMARY KEY,
        description  VARCHAR NOT NULL DEFAULT '',
        created_by   VARCHAR NOT NULL DEFAULT 'anonymous',
        created_at   TIMESTAMP NOT NULL DEFAULT current_timestamp
    );
    CREATE TABLE dataset_tables (
        dataset      VARCHAR NOT NULL,
        table_name   VARCHAR NOT NULL,
        source_uri   VARCHAR NOT NULL,
        format       VARCHAR NOT NULL DEFAULT 'auto',
        PRIMARY KEY (dataset, table_name)
    );
    CREATE TABLE dataset_tags (
        dataset VARCHAR NOT NULL,
        tag     VARCHAR NOT NULL,
        PRIMARY KEY (dataset, tag)
    );
    INSERT INTO datasets(name, description, created_by) VALUES
      ('startrek','Star Trek Season 1 scripts — CSV, ~2 KB','latiq'),
      ('holdings','Example stock holdings — CSV, ~300 B','latiq'),
      ('tpch','TPC-H scale 0.01 — 8 tables, Parquet, a few MB','latiq'),
      ('taxi','NYC yellow-taxi, Apr 2019 — Parquet, ~127 MB (large)','latiq');
    INSERT INTO dataset_tables(dataset, table_name, source_uri) VALUES
      ('startrek','startrek','https://blobs.duckdb.org/data/Star_Trek-Season_1.csv'),
      ('holdings','holdings','https://duckdb.org/data/holdings.csv'),
      ('tpch','lineitem','https://shell.duckdb.org/data/tpch/0_01/parquet/lineitem.parquet'),
      ('tpch','orders','https://shell.duckdb.org/data/tpch/0_01/parquet/orders.parquet'),
      ('tpch','customer','https://shell.duckdb.org/data/tpch/0_01/parquet/customer.parquet'),
      ('tpch','part','https://shell.duckdb.org/data/tpch/0_01/parquet/part.parquet'),
      ('tpch','supplier','https://shell.duckdb.org/data/tpch/0_01/parquet/supplier.parquet'),
      ('tpch','partsupp','https://shell.duckdb.org/data/tpch/0_01/parquet/partsupp.parquet'),
      ('tpch','nation','https://shell.duckdb.org/data/tpch/0_01/parquet/nation.parquet'),
      ('tpch','region','https://shell.duckdb.org/data/tpch/0_01/parquet/region.parquet'),
      ('taxi','taxi','https://blobs.duckdb.org/data/taxi_2019_04.parquet');
    INSERT INTO dataset_tags(dataset, tag) VALUES
      ('startrek','sample'),
      ('holdings','sample'),
      ('tpch','sample'),('tpch','tpch'),
      ('taxi','sample');
    "#,
    // v3: drop the audit_log table. Access auditing moved out of the registry —
    // each access is now a structured trace on the node's `latiq::access` log
    // target (operators grep the log files), so there is no audit store.
    r#"
    DROP TABLE IF EXISTS audit_log;
    "#,
    // Optional agent-discovery description (what the pond is for, so other agents
    // can find it). Nullable + default so existing rows read as empty.
    "ALTER TABLE ponds ADD COLUMN description VARCHAR DEFAULT '';",
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
