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

/// Quote a SQL identifier (the catalog alias), doubling embedded `"` so any pond
/// name — dashes, spaces, reserved words — is a valid catalog name.
pub fn quote_ident(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

impl PondInstance {
    /// Open a DuckDB instance with the pond's DuckLake catalog attached as `pond`.
    pub fn open(loc: &PondLocation) -> Result<Self, EngineError> {
        let conn = Connection::open_in_memory().map_err(|e| EngineError::Engine(e.to_string()))?;
        // Per-pond resource caps from its tier (instance-global in DuckDB, and we
        // run one instance per pond — invariant 7). Caps, not reservations.
        if let Some(lim) = &loc.limits {
            conn.execute_batch(&format!(
                "SET memory_limit='{}MiB'; SET threads={};",
                lim.memory_bytes / (1024 * 1024),
                lim.threads.max(1),
            ))
            .map_err(|e| EngineError::Engine(format!("set resource limits: {e}")))?;
        }
        // Load extensions (INSTALL may need network the first time; LOAD is local once installed).
        for ext in EXTENSIONS {
            conn.execute_batch(&format!("INSTALL {ext}; LOAD {ext};"))
                .map_err(|e| EngineError::Engine(format!("load {ext}: {e}")))?;
        }
        // ATTACH the pond's DuckLake catalog under the pond's name, so callers
        // query `<pond>.snapshots()` / `<pond>.main.<table>`. The alias is quoted
        // (and embedded quotes doubled) so any pond name is a valid identifier.
        // Syntax per spike findings (m1-spike-findings.md, Probe A); catalog_uri
        // already carries the full 'ducklake:duckdb:<path>' prefix.
        let alias = quote_ident(&loc.catalog_name);
        conn.execute_batch(&format!(
            "ATTACH '{}' AS {alias} (DATA_PATH '{}');",
            loc.catalog_uri, loc.data_path
        ))
        .map_err(|e| EngineError::Engine(format!("attach: {e}")))?;
        // Make it the default catalog so unqualified table names resolve there.
        conn.execute_batch(&format!("USE {alias};")).ok();
        Ok(Self { conn })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use latiq_common::PondId;
    use latiq_storage::{PondStorage, TempFs};

    #[test]
    fn applies_resource_limits_to_the_instance() {
        use latiq_common::ResourceLimits;
        let fs = TempFs::new();
        let mut loc = fs.create_pond(PondId::new()).unwrap();
        loc.limits = Some(ResourceLimits {
            memory_bytes: 512 * 1024 * 1024,
            threads: 1,
        });
        let inst = PondInstance::open(&loc).unwrap();
        // Our SET plumbing landed on the instance (DuckDB enforces from here —
        // invariant 10: test our integration, not DuckDB's enforcement).
        let threads: String = inst
            .conn
            .query_row("SELECT current_setting('threads')::VARCHAR", [], |r| {
                r.get(0)
            })
            .unwrap();
        assert_eq!(threads, "1");
        let mem: String = inst
            .conn
            .query_row("SELECT current_setting('memory_limit')::VARCHAR", [], |r| {
                r.get(0)
            })
            .unwrap();
        assert!(mem.contains("512"), "memory_limit not applied: {mem}");
    }

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
