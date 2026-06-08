//! One DuckDB instance per pond: loads extensions, ATTACHes the pond's DuckLake
//! catalog as `pond`. The instance owns exactly this pond's catalog (no cross-pond).
use duckdb::Connection;
use latiq_engine::EngineError;
use latiq_storage::PondLocation;

/// The Slice 0+ extension allowlist (public file sources only).
pub const EXTENSIONS: &[&str] = &["ducklake", "httpfs", "parquet", "json"];

pub struct PondInstance {
    pub conn: Connection,
}

impl PondInstance {
    /// Open a DuckDB instance with the pond's DuckLake catalog attached as `pond`.
    pub fn open(loc: &PondLocation) -> Result<Self, EngineError> {
        let conn = Connection::open_in_memory().map_err(|e| EngineError::Engine(e.to_string()))?;
        // Load extensions (INSTALL may need network the first time; LOAD is local once installed).
        for ext in EXTENSIONS {
            conn.execute_batch(&format!("INSTALL {ext}; LOAD {ext};"))
                .map_err(|e| EngineError::Engine(format!("load {ext}: {e}")))?;
        }
        // ATTACH the pond's DuckLake catalog.
        // Exact syntax confirmed in spike findings (m1-spike-findings.md, Probe A):
        //   ATTACH 'ducklake:duckdb:<catalog_path>' AS pond (DATA_PATH '<data_path>');
        // The catalog_uri already contains the full 'ducklake:duckdb:<path>' prefix.
        conn.execute_batch(&format!(
            "ATTACH '{}' AS pond (DATA_PATH '{}');",
            loc.catalog_uri, loc.data_path
        ))
        .map_err(|e| EngineError::Engine(format!("attach: {e}")))?;
        // Make `pond` the default catalog so unqualified table names resolve there.
        conn.execute_batch("USE pond;").ok();
        Ok(Self { conn })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use latiq_common::PondId;
    use latiq_storage::{PondStorage, TempFs};

    #[test]
    fn opens_attaches_and_round_trips() {
        let fs = TempFs::new();
        let loc = fs.create_pond(PondId::new()).unwrap();
        let inst = PondInstance::open(&loc).unwrap();
        inst.conn
            .execute_batch("CREATE TABLE t(id INTEGER); INSERT INTO t VALUES (1),(2);")
            .unwrap();
        let n: i64 = inst
            .conn
            .query_row("SELECT count(*) FROM t", [], |r| r.get(0))
            .unwrap();
        assert_eq!(n, 2);
    }
}
