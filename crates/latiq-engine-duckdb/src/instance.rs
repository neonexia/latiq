//! One DuckDB instance per pond: loads extensions, ATTACHes the pond's DuckLake
//! catalog as `pond`. The instance owns exactly this pond's catalog (no cross-pond).
use duckdb::Connection;
use latiq_engine::EngineError;
use latiq_storage::PondLocation;

/// **Required** standard extensions, loaded on every pond. The whole of Latiq is
/// built on these — `ducklake` is the catalog format, `httpfs` reads remote
/// sources, and `icu` provides timezone-aware TIMESTAMPTZ arithmetic (DuckLake's
/// snapshots/maintenance use `TIMESTAMP WITH TIME ZONE`, and patterns like
/// `now() - INTERVAL '1 week'` fail to bind without it). A node that can't load
/// them cannot function. `parquet`/`json` are statically linked into the binary
/// (duckdb cargo features), so they're always present and omitted here; these
/// have no cargo feature and load from the deployment image.
const STANDARD_LOAD: &[&str] = &["ducklake", "httpfs", "icu"];

/// Ensure the required standard extensions ([`STANDARD_LOAD`]) install and load.
/// The node calls this at **startup** and refuses to serve if it fails — Latiq is
/// useless without ducklake/httpfs, so this is a hard, fail-fast check rather than
/// a per-pond surprise. `INSTALL` may hit the network on a cold cache (in
/// production the image is pre-baked, making it a no-op).
pub fn ensure_standard_extensions() -> Result<(), EngineError> {
    let conn = Connection::open_in_memory().map_err(|e| EngineError::Engine(e.to_string()))?;
    for ext in STANDARD_LOAD {
        conn.execute_batch(&format!("INSTALL {ext}; LOAD {ext};"))
            .map_err(|e| EngineError::Engine(format!("required extension '{ext}': {e}")))?;
    }
    Ok(())
}

/// Best-effort: install the **optional** allowlist into the local cache so
/// per-pond `LOAD`s don't download. Run once at node startup — the dev stand-in
/// for image-baking. Failures (offline, already present) are non-fatal; a pond
/// requesting an extension that didn't warm simply fails fast on open.
pub fn warm_optional_extensions() {
    let Ok(conn) = Connection::open_in_memory() else {
        return;
    };
    for ext in latiq_common::extensions::OPTIONAL {
        let _ = conn.execute_batch(&format!("INSTALL {ext};"));
    }
}

pub struct PondInstance {
    pub conn: Connection,
    /// Quoted catalog alias for this pond. Kept so [`PondInstance::clone_for_read`]
    /// can re-apply `USE` — that is *session* state and is not inherited by a
    /// cloned connection (the `ATTACH` itself is database-level and is shared).
    alias: String,
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
        // Pin the session timezone. With icu loaded, DuckDB otherwise defaults the
        // TimeZone setting to the HOST OS zone, making TIMESTAMPTZ rendering and
        // wall-clock results host-dependent (different per node / after a TZ
        // change). UTC makes results deterministic across the cluster.
        conn.execute_batch("SET TimeZone='UTC';")
            .map_err(|e| EngineError::Engine(format!("set timezone: {e}")))?;
        // Force httpfs to download whole remote files instead of HTTP range reads.
        // Some CDNs (e.g. Cloudflare in front of shell.duckdb.org, which serves the
        // curated datasets) don't honor range requests cleanly, so a ranged footer
        // read returns wrong bytes → "No magic bytes found at end of file". Whole-
        // file download is correct for the small curated-dataset/pull sources.
        conn.execute_batch("SET force_download=true;")
            .map_err(|e| EngineError::Engine(format!("set force_download: {e}")))?;
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
        Ok(Self { conn, alias })
    }

    /// A second connection to the **same already-opened database**, for concurrent
    /// reads (`Connection::try_clone` = "creates a new connection to the
    /// already-opened database").
    ///
    /// This keeps invariant 7 intact: still **one DuckDB database per pond**, so
    /// `memory_limit`/`threads` tier caps stay instance-global and one process
    /// still owns the catalog file — only the *connection* multiplies, which is
    /// what lets reads run concurrently instead of serializing behind one handle.
    ///
    /// Database-level state (the `ATTACH`, loaded extensions) is shared with the
    /// clone; **session** state is not inherited and must be re-applied here, or
    /// the clone resolves unqualified names against the wrong catalog and renders
    /// timestamps in the host timezone.
    pub fn clone_for_read(&self) -> Result<Self, EngineError> {
        let conn = self
            .conn
            .try_clone()
            .map_err(|e| EngineError::Engine(format!("clone read connection: {e}")))?;
        // Same determinism guarantees the primary gets at open().
        conn.execute_batch("SET TimeZone='UTC'; SET force_download=true;")
            .map_err(|e| EngineError::Engine(format!("read connection settings: {e}")))?;
        conn.execute_batch(&format!("USE {};", self.alias))
            .map_err(|e| EngineError::Engine(format!("read connection USE: {e}")))?;
        Ok(Self {
            conn,
            alias: self.alias.clone(),
        })
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
        // (`inet`, not `icu` — icu is now a STANDARD extension, always loaded.)
        Connection::open_in_memory()
            .unwrap()
            .execute_batch("INSTALL inet;")
            .unwrap();
        let fs = TempFs::new();
        let mut loc = fs.create_pond(PondId::new()).unwrap();
        loc.extensions = vec!["inet".to_string()];
        let inst = PondInstance::open(&loc).unwrap();
        // Our LOAD plumbing landed it on the instance (test our integration, not
        // DuckDB's enforcement — invariant 10).
        let loaded: bool = inst
            .conn
            .query_row(
                "SELECT loaded FROM duckdb_extensions() WHERE extension_name='inet'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert!(loaded, "inet should be LOADed on the pond");
    }

    #[test]
    fn pond_session_timezone_is_utc() {
        // Results must be deterministic regardless of the host OS timezone.
        let fs = TempFs::new();
        let loc = fs.create_pond(PondId::new()).unwrap();
        let inst = PondInstance::open(&loc).unwrap();
        let tz: String = inst
            .conn
            .query_row("SELECT current_setting('TimeZone')", [], |r| r.get(0))
            .unwrap();
        assert_eq!(tz, "UTC");
    }

    #[test]
    fn standard_pond_supports_timestamptz_arithmetic() {
        // icu is standard, so timezone-aware TIMESTAMPTZ math (e.g. DuckLake's
        // `expire_snapshots(older_than => now() - INTERVAL '1 week')`) binds on a
        // pond with NO explicitly-requested extensions.
        let fs = TempFs::new();
        let loc = fs.create_pond(PondId::new()).unwrap();
        let inst = PondInstance::open(&loc).unwrap();
        let ok: bool = inst
            .conn
            .query_row("SELECT (now() - INTERVAL '7 days') < now()", [], |r| {
                r.get(0)
            })
            .expect("TIMESTAMPTZ - INTERVAL must bind (icu loaded)");
        assert!(ok);
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
