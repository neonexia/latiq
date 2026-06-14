//! One DuckDB instance per pond: loads extensions, ATTACHes the pond's DuckLake
//! catalog as `pond`. The instance owns exactly this pond's catalog (no cross-pond).
use duckdb::Connection;
use latiq_engine::EngineError;
use latiq_storage::PondLocation;

/// Standard extensions loaded on every pond. `parquet`/`json` are statically
/// linked into the binary (duckdb cargo features) so they need no `LOAD`;
/// `ducklake`/`httpfs` have no cargo feature and are loaded from the deployment
/// image. `INSTALL` here self-heals the required set on a cold cache.
const STANDARD_LOAD: &[&str] = &["ducklake", "httpfs"];

/// Best-effort: ensure the loadable standard set + the optional allowlist are
/// present in the local extension cache, so pond opens are pure `LOAD`. Run once
/// at node startup — the dev stand-in for image-baking (in production the image
/// is pre-baked, making this a fast no-op). Network happens here, **never** in
/// the pond-create path. Individual failures (offline, already present) are
/// non-fatal.
pub fn warm_extension_cache() {
    let Ok(conn) = Connection::open_in_memory() else {
        return;
    };
    let names = STANDARD_LOAD
        .iter()
        .copied()
        .chain(latiq_common::extensions::OPTIONAL.iter().copied());
    for ext in names {
        let _ = conn.execute_batch(&format!("INSTALL {ext};"));
    }
}

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
                // The core budget maps to DuckDB's instance-global `threads`.
                "SET memory_limit='{}MiB'; SET threads={};",
                lim.memory_bytes / (1024 * 1024),
                lim.cores.max(1),
            ))
            .map_err(|e| EngineError::Engine(format!("set resource limits: {e}")))?;
        }
        // Standard set: required, self-healing (INSTALL is a no-op once cached;
        // parquet/json are statically linked, so they're omitted here).
        for ext in STANDARD_LOAD {
            conn.execute_batch(&format!("INSTALL {ext}; LOAD {ext};"))
                .map_err(|e| EngineError::Engine(format!("load {ext}: {e}")))?;
        }
        // Per-pond optional extensions: LOAD-only from the image. autoinstall off
        // so a missing one fails fast — never a download in the pond path.
        if !loc.extensions.is_empty() {
            conn.execute_batch("SET autoinstall_known_extensions=false;")
                .map_err(|e| EngineError::Engine(e.to_string()))?;
            for ext in &loc.extensions {
                conn.execute_batch(&format!("LOAD {ext};")).map_err(|e| {
                    EngineError::Engine(format!(
                        "extension '{ext}' is not available on this node — bake it \
                         into the deployment image and upgrade Latiq ({e})"
                    ))
                })?;
            }
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
            cores: 1,
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

    #[test]
    fn loads_a_baked_optional_extension() {
        // Simulate image-baking: install the extension into the cache first.
        warm_extension_cache();
        Connection::open_in_memory()
            .unwrap()
            .execute_batch("INSTALL icu;")
            .unwrap();
        let fs = TempFs::new();
        let mut loc = fs.create_pond(PondId::new()).unwrap();
        loc.extensions = vec!["icu".to_string()];
        let inst = PondInstance::open(&loc).unwrap();
        // Our LOAD plumbing landed it on the instance (test our integration, not
        // DuckDB's enforcement — invariant 10).
        let loaded: bool = inst
            .conn
            .query_row(
                "SELECT loaded FROM duckdb_extensions() WHERE extension_name='icu'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert!(loaded, "icu should be LOADed on the pond");
    }

    #[test]
    fn missing_optional_extension_fails_fast() {
        // autoinstall is off → LOADing an unavailable extension errors (no
        // download), so a cache miss never silently downloads in the pond path.
        let fs = TempFs::new();
        let mut loc = fs.create_pond(PondId::new()).unwrap();
        loc.extensions = vec!["definitely_not_an_extension_xyz".to_string()];
        let err = match PondInstance::open(&loc) {
            Ok(_) => panic!("expected open to fail for a missing extension"),
            Err(e) => e,
        };
        let msg = format!("{err:?}");
        assert!(
            msg.contains("not available") || msg.contains("definitely_not_an_extension_xyz"),
            "expected a fail-fast extension error, got: {msg}"
        );
    }
}
