// Copyright 2026 Neonexia
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! One DuckDB instance per pond: loads extensions, ATTACHes the pond's DuckLake
//! catalog as `pond`. The instance owns exactly this pond's catalog (no cross-pond).
//!
//! **No `INSTALL` on a request path.** Downloading an extension is an unbounded
//! wait on an external host, and every path that reaches DuckDB from a caller —
//! opening a pond, the transient attach behind `pull_catalog` — is a path where
//! an agent is sitting on the call. So there are exactly two places an `INSTALL`
//! may run, and both are before the node serves anything:
//!
//! - [`ensure_standard_extensions`] — once per process, memoized, at node
//!   startup (`run_pond_node_until` awaits it before registering).
//! - [`warm_extension_cache`] — `latiq warm-extensions` at image-build time, and
//!   the node's background warm at startup.
//!
//! Everywhere else `autoinstall_known_extensions` is **off** and the SQL is
//! `LOAD` only, so a cache miss is an immediate, actionable failure naming
//! `latiq warm-extensions` instead of a silent download mid-query.
use duckdb::Connection;
use latiq_engine::EngineError;
use latiq_storage::PondLocation;
use std::path::Path;
use std::sync::OnceLock;

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
/// a per-pond surprise.
///
/// **This is the one place an `INSTALL` may reach the network in a serving
/// process, and it is deliberate.** It runs before the node registers or serves,
/// which is the acceptable moment to download: bounded, once, with nobody
/// waiting on a query. It is kept as `INSTALL; LOAD` rather than a bare verify
/// because `pip install latiq` (SDK / `LocalCluster` / `dev.sh`) has no baked
/// image to verify against — refusing to download here would mean a wheel that
/// cannot start a node at all. A pre-baked image makes it a local no-op.
///
/// The result is **memoized**: `PondInstance::open` calls it so a fresh process
/// (a test, an embedded cluster) still bootstraps itself once, and on a node
/// that started properly the per-pond call is then a boolean check — never an
/// `INSTALL` on a request path.
pub fn ensure_standard_extensions() -> Result<(), EngineError> {
    static STANDARD_READY: OnceLock<Result<(), String>> = OnceLock::new();
    STANDARD_READY
        .get_or_init(|| {
            let conn = Connection::open_in_memory().map_err(|e| e.to_string())?;
            for ext in STANDARD_LOAD {
                conn.execute_batch(&format!("INSTALL {ext}; LOAD {ext};"))
                    .map_err(|e| format!("required extension '{ext}': {e}"))?;
            }
            Ok(())
        })
        .clone()
        .map_err(EngineError::Engine)
}

/// Every extension a `LOAD` site in this crate can reach for, in one list —
/// what a complete node cache has to contain:
///
/// - [`STANDARD_LOAD`] — loaded into every pond by [`PondInstance::open`].
/// - `OPTIONAL` — what a pond may request; `open` `LOAD`s it with autoinstall
///   **off**, so a cache miss fails the pond rather than downloading.
/// - `CATALOG_DRIVEN` — what a *catalog type* needs (`iceberg`, plus the `avro`
///   DuckDB pulls in when iceberg loads); `attachers.rs` `LOAD`s it for the
///   transient attach behind `pull_catalog`.
///
/// `parquet`/`json` are statically linked into the binary and need no cache.
pub fn warmable_extensions() -> Vec<&'static str> {
    let mut out: Vec<&'static str> = STANDARD_LOAD.to_vec();
    for ext in latiq_common::extensions::OPTIONAL
        .iter()
        .chain(latiq_common::extensions::CATALOG_DRIVEN.iter())
    {
        if !out.contains(ext) {
            out.push(ext);
        }
    }
    out
}

/// What [`warm_extension_cache`] tried, and what it could not deliver.
///
/// The warm used to be `let _ = INSTALL …` — right for a node that is merely
/// topping up its cache, but it made a gap invisible until some pond or
/// `pull_catalog` failed much later, far from the cause. The report is what lets
/// the node **warn** with the names and the build-time command **fail**.
#[derive(Debug, Default, Clone)]
pub struct WarmReport {
    /// Every extension the warm was supposed to deliver ([`warmable_extensions`]).
    pub attempted: Vec<&'static str>,
    /// `(extension, why)` for each one that could not be installed, or that
    /// still would not `LOAD` from the cache afterwards.
    pub failed: Vec<(&'static str, String)>,
}

impl WarmReport {
    /// Every warmable extension is installed **and** loads offline.
    ///
    /// An empty `attempted` is deliberately **not** complete: a warm that
    /// examined nothing has proved nothing, and would otherwise let a build
    /// whose extension list evaporated report success.
    pub fn is_complete(&self) -> bool {
        !self.attempted.is_empty() && self.failed.is_empty()
    }

    /// One line naming what is missing and why — the text the node warns with
    /// and `latiq warm-extensions` fails with.
    pub fn failure_summary(&self) -> String {
        if self.attempted.is_empty() {
            return "warmed 0 extensions: there was nothing to warm, which cannot be right — \
                    a node with an empty extension cache cannot open a pond"
                .to_string();
        }
        let what = self
            .failed
            .iter()
            .map(|(ext, why)| format!("{ext} ({why})"))
            .collect::<Vec<_>>()
            .join("; ");
        format!(
            "{} of {} DuckDB extensions are not usable offline on this node: {what}. \
             Every LOAD site runs with autoinstall off, so these will fail the pond or the \
             catalog attach that needs them rather than downloading. Re-run \
             `latiq warm-extensions` with network access (the container image bakes the same \
             step in at build time).",
            self.failed.len(),
            self.attempted.len(),
        )
    }
}

/// Install every extension in [`warmable_extensions`] into the local DuckDB
/// cache, then **prove** each one loads back out of it with autoinstall off.
///
/// This is the only warm path: `latiq warm-extensions` at image-build time (which
/// fails the build on an incomplete report) and the node's background warm at
/// startup (which warns). Installing hits the network here, on purpose — that is
/// the whole point of doing it here and not on a request path.
///
/// The verify pass is not a formality. `INSTALL iceberg` installs *only*
/// `iceberg`; `LOAD iceberg` then pulls in `avro` — so "the install succeeded"
/// never meant "the node can load it offline", which is exactly how an image
/// shipped claiming extensions it did not have.
///
/// `extension_directory` overrides DuckDB's default cache location
/// (`~/.duckdb/extensions/…`). Production passes `None`; the guard test passes a
/// scratch directory so the ambient cache cannot mask a miss.
pub fn warm_extension_cache(extension_directory: Option<&Path>) -> WarmReport {
    let attempted = warmable_extensions();
    let mut failed: Vec<(&'static str, String)> = Vec::new();

    let open = |autoinstall: bool| -> Result<Connection, String> {
        let conn = Connection::open_in_memory().map_err(|e| e.to_string())?;
        if let Some(dir) = extension_directory {
            conn.execute_batch(&format!(
                "SET extension_directory='{}';",
                dir.display().to_string().replace('\'', "''")
            ))
            .map_err(|e| e.to_string())?;
        }
        if !autoinstall {
            conn.execute_batch("SET autoinstall_known_extensions=false;")
                .map_err(|e| e.to_string())?;
        }
        Ok(conn)
    };

    // Install pass — the network one.
    let installer = match open(true) {
        Ok(c) => c,
        Err(e) => {
            return WarmReport {
                failed: attempted.iter().map(|ext| (*ext, e.clone())).collect(),
                attempted,
            }
        }
    };
    for ext in &attempted {
        if let Err(e) = installer.execute_batch(&format!("INSTALL {ext};")) {
            failed.push((ext, format!("INSTALL failed: {e}")));
        }
    }

    // Verify pass — a fresh connection with autoinstall OFF, i.e. the same
    // conditions every LOAD site runs under. Anything that only worked because
    // DuckDB reached out is a failure here.
    match open(false) {
        Ok(verifier) => {
            for ext in &attempted {
                if failed.iter().any(|(f, _)| f == ext) {
                    continue;
                }
                if let Err(e) = verifier.execute_batch(&format!("LOAD {ext};")) {
                    failed.push((ext, format!("installed, but will not LOAD offline: {e}")));
                }
            }
        }
        Err(e) => failed.push((
            "<verify>",
            format!("could not open a verify connection: {e}"),
        )),
    }

    WarmReport { attempted, failed }
}

/// The error every `LOAD` site raises on a cache miss. Autoinstall is off there,
/// so this is "not in the node's extension cache" and no retry changes it: the
/// message has to name the extension AND the operator action for both
/// deployments we ship — the image bakes extensions at build time
/// (`latiq warm-extensions`), while a `pip install latiq` node warms them at
/// startup and cannot if that first start had no network.
///
/// (The `kind` this maps to is still `internal`, so the envelope's `retryable`
/// says `as_is` — advice that can never work for an operator-only fix. Fixing
/// that needs a new `ErrorKind`; until then the message carries the whole
/// burden, which is why it is one string shared by every site.)
pub fn extension_not_cached(what: &str, err: &dyn std::fmt::Display) -> EngineError {
    EngineError::Engine(format!(
        "{what} is not cached on this node, and nothing the caller sends changes this — \
         extension downloads are disabled while serving requests. An operator restores it by \
         running `latiq warm-extensions` on the node with network access (the container image \
         bakes the same step in at build time), then restarting the node. Underlying error: {err}"
    ))
}

/// One pond's open DuckDB database, with its DuckLake catalog attached and its
/// tier's caps applied. The caps are instance-global in DuckDB, which is why the
/// instance — not the connection — is the unit of per-pond isolation.
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
        // Opening a pond is a REQUEST path (materialize, first query), so nothing
        // from here on may download: autoinstall off before the first LOAD, for
        // every pond, not just one that requested extensions. It is a global
        // DuckDB setting, so `clone_for_read` connections inherit it, and it is
        // what lets `attachers.rs` emit LOAD-only SQL.
        conn.execute_batch("SET autoinstall_known_extensions=false;")
            .map_err(|e| EngineError::Engine(format!("disable extension autoinstall: {e}")))?;
        // The standard set comes out of the node's cache. The memoized startup
        // check is what put it there (a no-op after the node's own call at
        // startup; the bootstrap for a fresh process that never made one).
        ensure_standard_extensions()?;
        for ext in STANDARD_LOAD {
            conn.execute_batch(&format!("LOAD {ext};"))
                .map_err(|e| extension_not_cached(&format!("required extension '{ext}'"), &e))?;
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
        // Per-pond optional extensions: LOAD-only from the node's cache, under
        // the autoinstall-off setting applied above.
        for ext in &loc.extensions {
            conn.execute_batch(&format!("LOAD {ext};")).map_err(|e| {
                extension_not_cached(&format!("the pond requested extension '{ext}', which"), &e)
            })?;
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
        let mut loc = fs.create_pond(PondId::new(), false).unwrap();
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
        let loc = fs.create_pond(PondId::new(), false).unwrap();
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
        let mut loc = fs.create_pond(PondId::new(), false).unwrap();
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

    /// **The offline claim, tested where it is made, against a cache that cannot
    /// be lying.** `deploy/Dockerfile` bakes extensions with
    /// `latiq warm-extensions` "so nodes start without network", and every LOAD
    /// site — `PondInstance::open`'s standard set, a pond's requested
    /// extensions, `attachers.rs` for the transient attach behind
    /// `pull_catalog` — now runs with autoinstall **off**. So the whole
    /// deployment claim reduces to: after a warm, does every warmable extension
    /// load with no network?
    ///
    /// The warm goes into a **scratch `extension_directory`** rather than the
    /// developer's `~/.duckdb`: against the ambient cache, an extension the warm
    /// forgot still loads (someone installed it months ago) and the test passes
    /// while a fresh image ships broken. That is the failure #120 found for
    /// `avro` — DuckDB autoinstalls it when `iceberg` LOADs, so "INSTALL
    /// iceberg succeeded" never meant "iceberg loads offline".
    ///
    /// This asserts OUR warm set is complete, not DuckDB's dependency
    /// resolution (invariant 10). It needs the network, once, like the image
    /// build it stands in for.
    #[test]
    fn a_warmed_cache_loads_every_extension_we_ever_load_without_the_network() {
        let scratch = tempfile::tempdir().unwrap();
        let report = warm_extension_cache(Some(scratch.path()));

        // Nothing may be missing — and a warm that examined nothing is not a
        // pass. `warm_extension_cache` verifies with autoinstall off in a fresh
        // connection, which is the only way to prove nothing downloaded.
        assert!(
            report.is_complete(),
            "a scratch cache warmed by `latiq warm-extensions` is incomplete: {}",
            report.failure_summary()
        );

        // The set really is every LOAD site's needs, not an empty list that
        // trivially satisfies the assertion above.
        for required in ["ducklake", "httpfs", "icu", "iceberg", "avro"] {
            assert!(
                report.attempted.contains(&required),
                "'{required}' is LOADed somewhere but is not warmed: {:?}",
                report.attempted
            );
        }
        for t in latiq_common::catalog::TYPES {
            for ext in t.required_extensions {
                assert!(
                    report.attempted.contains(ext),
                    "catalog type '{}' needs '{ext}', which the warm does not cover",
                    t.name
                );
            }
        }
        for ext in latiq_common::extensions::OPTIONAL.iter() {
            assert!(
                report.attempted.contains(ext),
                "a pond may request '{ext}', which the warm does not cover"
            );
        }
        assert!(
            report.attempted.len() >= 6,
            "the warm covered only {:?}",
            report.attempted
        );
    }

    /// A report is a claim about work done, so "nothing attempted" must not read
    /// as success — that is what would let a build whose extension list
    /// evaporated pass `latiq warm-extensions` and ship an empty cache.
    #[test]
    fn an_empty_warm_is_not_a_successful_one() {
        assert!(!WarmReport::default().is_complete());
        assert!(
            WarmReport::default().failure_summary().contains("0"),
            "the summary must say nothing was warmed: {}",
            WarmReport::default().failure_summary()
        );
        let one_failed = WarmReport {
            attempted: vec!["spatial", "fts"],
            failed: vec![("spatial", "INSTALL failed: offline".into())],
        };
        assert!(!one_failed.is_complete());
        let msg = one_failed.failure_summary();
        assert!(
            msg.contains("spatial") && msg.contains("latiq warm-extensions"),
            "a warm failure must name the extension and the fix: {msg}"
        );
        assert!(WarmReport {
            attempted: vec!["spatial"],
            failed: vec![],
        }
        .is_complete());
    }

    /// The property the whole change rests on: a pond's connection cannot
    /// download, whatever SQL later runs on it. `attachers.rs` emits LOAD-only
    /// statements *because* of this setting, and DuckDB would otherwise
    /// autoinstall a known extension from a plain query.
    #[test]
    fn a_pond_connection_can_never_download_an_extension() {
        let fs = TempFs::new();
        let loc = fs.create_pond(PondId::new(), false).unwrap();
        let inst = PondInstance::open(&loc).unwrap();
        let read = inst.clone_for_read().unwrap();
        for conn in [&inst.conn, &read.conn] {
            let autoinstall: String = conn
                .query_row(
                    "SELECT current_setting('autoinstall_known_extensions')::VARCHAR",
                    [],
                    |r| r.get(0),
                )
                .unwrap();
            assert_eq!(
                autoinstall, "false",
                "a pond connection would download an extension mid-query"
            );
        }
    }

    #[test]
    fn pond_session_timezone_is_utc() {
        // Results must be deterministic regardless of the host OS timezone.
        let fs = TempFs::new();
        let loc = fs.create_pond(PondId::new(), false).unwrap();
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
        let loc = fs.create_pond(PondId::new(), false).unwrap();
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
        let mut loc = fs.create_pond(PondId::new(), false).unwrap();
        loc.extensions = vec!["definitely_not_an_extension_xyz".to_string()];
        let err = match PondInstance::open(&loc) {
            Ok(_) => panic!("expected open to fail for a missing extension"),
            Err(e) => e,
        };
        // An operator, not the caller, is the only one who can fix this — so the
        // message must name the extension, say it is the NODE's cache that is
        // missing it, and name the command that fixes it. A bare DuckDB
        // "Extension ... not found" leaves whoever reads it with nothing to do.
        let msg = format!("{err:?}");
        for needle in [
            "definitely_not_an_extension_xyz",
            "not cached on this node",
            "latiq warm-extensions",
        ] {
            assert!(
                msg.contains(needle),
                "the missing-extension error must be actionable — no {needle:?} in: {msg}"
            );
        }
    }
}
