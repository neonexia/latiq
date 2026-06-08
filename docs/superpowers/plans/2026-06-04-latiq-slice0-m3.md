# Latiq Slice 0+ — M3 (Outbound Seams: Storage + Engine) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: superpowers:subagent-driven-development. Steps use checkbox (`- [ ]`) syntax.

**Goal:** Build the two outbound adapters of the hexagon — `PondStorage` (where a pond's bytes live) and `QueryEngine` (how SQL executes against DuckLake) — plus the DuckDB adapter that makes ponds real. After M3, a pond can be created on disk and queried end-to-end *in-process* (no network), with native attribution, the `_latiq` schema, and working query cancellation.

**Architecture:** `latiq-storage` (trait + LocalFs + TempFs test backend) and `latiq-engine` (trait + neutral result types + abort primitive) are engine/storage-agnostic. `latiq-engine-duckdb` implements `QueryEngine` over `duckdb-rs` + the `ducklake` extension, one DuckDB instance per pond. See spec `docs/superpowers/specs/2026-06-04-latiq-slice0-design.md` §3/§5/§6/§9 and the **spike findings** `docs/superpowers/notes/m1-spike-findings.md` (authoritative for the real duckdb-rs/DuckLake API — ATTACH syntax, `snapshots()`/`set_commit_message`, `interrupt_handle()`).

**Tech Stack:** Rust, `duckdb` (bundled), `tokio` + `tokio-util` (CancellationToken), `serde_json` (cell values), `tempfile` (test backend), `thiserror`.

---

## Conventions
- TDD: write the test, see it fail, implement, see it pass, commit. `cargo fmt` + `cargo clippy -p <crate> --all-targets -- -D warnings` clean before each commit.
- Commit trailer: `Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>` (use `git -c user.name='Latiq Dev' -c user.email='svsujeet@gmail.com' commit`).
- **The duckdb/DuckLake API is spike-confirmed but version-sensitive.** Where a snippet below is marked *(adapt per spike findings)*, the implementer must match the actual `duckdb` 1.x API and the working SQL from the findings doc, and fix minor signature differences rather than reporting blocked.
- Add crate deps to the workspace root `[workspace.dependencies]` when first introduced (e.g. `duckdb`, `tokio-util`, `tempfile`), then reference `{ workspace = true }`.

---

## File structure (created in M3)
- `crates/latiq-storage/src/lib.rs` — re-exports
- `crates/latiq-storage/src/location.rs` — `PondLocation`
- `crates/latiq-storage/src/storage.rs` — `PondStorage` trait + `StorageError`
- `crates/latiq-storage/src/local_fs.rs` — `LocalFs` backend
- `crates/latiq-storage/src/temp_fs.rs` — `TempFs` test backend
- `crates/latiq-engine/src/lib.rs` — re-exports
- `crates/latiq-engine/src/result.rs` — `QueryResult`, `ExplainResult`, `ScanOp`, `SchemaSummary`, `TableInfo`
- `crates/latiq-engine/src/engine.rs` — `QueryEngine` trait + `EngineError`
- `crates/latiq-engine/src/abort.rs` — `AbortToken` alias + helper
- `crates/latiq-engine-duckdb/src/lib.rs` — re-exports
- `crates/latiq-engine-duckdb/src/instance.rs` — per-pond DuckDB instance (open/attach/extensions/close)
- `crates/latiq-engine-duckdb/src/exec.rs` — read/write/explain execution + row decoding
- `crates/latiq-engine-duckdb/src/latiq_schema.rs` — `_latiq` view creation + reserved-schema guard
- `crates/latiq-engine-duckdb/src/duck_engine.rs` — `DuckEngine: QueryEngine`
- `crates/latiq-engine-duckdb/tests/engine_e2e.rs` — end-to-end integration test

---

## Task 3.1: latiq-storage — PondLocation + PondStorage trait

**Files:** Create `crates/latiq-storage/src/{location.rs,storage.rs}`; modify `src/lib.rs`, `Cargo.toml`.

- [ ] **Step 1: Write `location.rs`**
```rust
//! Where a pond's bytes live — the descriptor handed to the query engine.
use serde::{Deserialize, Serialize};

/// Resolved physical location of a pond. The engine consumes this to ATTACH.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PondLocation {
    /// DuckLake catalog connection string, e.g. "ducklake:duckdb:/var/lib/latiq/ponds/<id>/catalog.duckdb".
    pub catalog_uri: String,
    /// DuckLake DATA_PATH for parquet files, e.g. "/var/lib/latiq/ponds/<id>/data".
    pub data_path: String,
}
```

- [ ] **Step 2: Write `storage.rs`**
```rust
//! Pluggable physical storage for ponds (LocalFs now; S3/MinIO later).
use crate::location::PondLocation;
use latiq_common::PondId;

#[derive(Debug, thiserror::Error)]
pub enum StorageError {
    #[error("pond already exists: {0}")]
    AlreadyExists(PondId),
    #[error("pond not found: {0}")]
    NotFound(PondId),
    #[error("io error: {0}")]
    Io(String),
}

/// Storage backend. Implementations own the physical layout and produce the
/// per-pond `PondLocation` the engine attaches. They do NOT touch DuckLake.
pub trait PondStorage: Send + Sync {
    /// Provision storage for a new pond. Errors if it already exists.
    fn create_pond(&self, pond_id: PondId) -> Result<PondLocation, StorageError>;
    /// Resolve an existing pond's location.
    fn pond_location(&self, pond_id: PondId) -> Result<PondLocation, StorageError>;
    /// Remove a pond's storage entirely.
    fn drop_pond(&self, pond_id: PondId) -> Result<(), StorageError>;
    /// Whether the pond's storage exists.
    fn pond_exists(&self, pond_id: PondId) -> bool;
}
```

- [ ] **Step 3: Wire `Cargo.toml` + `lib.rs`**
```toml
[dependencies]
latiq-common = { path = "../latiq-common" }
serde = { workspace = true }
thiserror = { workspace = true }
```
```rust
//! latiq-storage — pluggable pond storage (PondStorage trait + backends).
pub mod location;
pub mod storage;
pub use location::PondLocation;
pub use storage::{PondStorage, StorageError};
```

- [ ] **Step 4: Build** — `cargo build -p latiq-storage` → compiles.
- [ ] **Step 5: Commit** — `feat(storage): PondLocation + PondStorage trait`

## Task 3.2: latiq-storage — LocalFs backend

**Files:** Create `crates/latiq-storage/src/local_fs.rs`; modify `src/lib.rs`.

- [ ] **Step 1: Write the failing test (in `local_fs.rs`)**
```rust
//! Local-filesystem pond storage: <root>/<pond-id>/{catalog.duckdb, data/}.
use crate::location::PondLocation;
use crate::storage::{PondStorage, StorageError};
use latiq_common::PondId;
use std::path::{Path, PathBuf};

pub struct LocalFs {
    root: PathBuf,
}

impl LocalFs {
    pub fn new(root: impl AsRef<Path>) -> Self {
        Self { root: root.as_ref().to_path_buf() }
    }
    fn pond_dir(&self, pond_id: PondId) -> PathBuf {
        self.root.join(pond_id.to_string())
    }
    fn location_for(&self, pond_id: PondId) -> PondLocation {
        let dir = self.pond_dir(pond_id);
        PondLocation {
            catalog_uri: format!("ducklake:duckdb:{}", dir.join("catalog.duckdb").display()),
            data_path: dir.join("data").display().to_string(),
        }
    }
}

impl PondStorage for LocalFs {
    fn create_pond(&self, pond_id: PondId) -> Result<PondLocation, StorageError> {
        let dir = self.pond_dir(pond_id);
        if dir.exists() {
            return Err(StorageError::AlreadyExists(pond_id));
        }
        std::fs::create_dir_all(dir.join("data")).map_err(|e| StorageError::Io(e.to_string()))?;
        Ok(self.location_for(pond_id))
    }
    fn pond_location(&self, pond_id: PondId) -> Result<PondLocation, StorageError> {
        if self.pond_exists(pond_id) {
            Ok(self.location_for(pond_id))
        } else {
            Err(StorageError::NotFound(pond_id))
        }
    }
    fn drop_pond(&self, pond_id: PondId) -> Result<(), StorageError> {
        let dir = self.pond_dir(pond_id);
        if !dir.exists() {
            return Err(StorageError::NotFound(pond_id));
        }
        std::fs::remove_dir_all(dir).map_err(|e| StorageError::Io(e.to_string()))
    }
    fn pond_exists(&self, pond_id: PondId) -> bool {
        self.pond_dir(pond_id).exists()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn create_resolve_drop_lifecycle() {
        let tmp = std::env::temp_dir().join(format!("latiq-localfs-test-{}", PondId::new()));
        let fs = LocalFs::new(&tmp);
        let id = PondId::new();
        assert!(!fs.pond_exists(id));
        let loc = fs.create_pond(id).unwrap();
        assert!(loc.catalog_uri.starts_with("ducklake:duckdb:"));
        assert!(loc.catalog_uri.contains(&id.to_string()));
        assert!(fs.pond_exists(id));
        assert_eq!(fs.pond_location(id).unwrap(), loc);
        assert!(matches!(fs.create_pond(id), Err(StorageError::AlreadyExists(_))));
        fs.drop_pond(id).unwrap();
        assert!(!fs.pond_exists(id));
        assert!(matches!(fs.pond_location(id), Err(StorageError::NotFound(_))));
        let _ = std::fs::remove_dir_all(&tmp);
    }
}
```

- [ ] **Step 2: Export** — add `pub mod local_fs; pub use local_fs::LocalFs;` to `lib.rs`. Add `latiq-common` uuid dep already present.
- [ ] **Step 3: Run** — `cargo test -p latiq-storage local_fs::` → 1 passed.
- [ ] **Step 4: Commit** — `feat(storage): LocalFs backend`

## Task 3.3: latiq-storage — TempFs test backend

**Files:** Create `crates/latiq-storage/src/temp_fs.rs`; modify `src/lib.rs`, `Cargo.toml`.

- [ ] **Step 1: Write `temp_fs.rs`** — a backend rooted in an auto-cleaned temp dir, proving the seam + giving hermetic tests. It delegates to a `LocalFs` over a `tempfile::TempDir` it owns.
```rust
//! Ephemeral pond storage backed by a self-cleaning temp dir. For tests.
use crate::local_fs::LocalFs;
use crate::location::PondLocation;
use crate::storage::{PondStorage, StorageError};
use latiq_common::PondId;
use tempfile::TempDir;

pub struct TempFs {
    _dir: TempDir, // dropped → removed
    inner: LocalFs,
}

impl TempFs {
    pub fn new() -> Self {
        let dir = TempDir::new().expect("create temp dir");
        let inner = LocalFs::new(dir.path());
        Self { _dir: dir, inner }
    }
}

impl Default for TempFs {
    fn default() -> Self { Self::new() }
}

impl PondStorage for TempFs {
    fn create_pond(&self, id: PondId) -> Result<PondLocation, StorageError> { self.inner.create_pond(id) }
    fn pond_location(&self, id: PondId) -> Result<PondLocation, StorageError> { self.inner.pond_location(id) }
    fn drop_pond(&self, id: PondId) -> Result<(), StorageError> { self.inner.drop_pond(id) }
    fn pond_exists(&self, id: PondId) -> bool { self.inner.pond_exists(id) }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn proves_the_seam_with_a_second_backend() {
        let fs = TempFs::new();
        let id = PondId::new();
        let loc = fs.create_pond(id).unwrap();
        assert!(loc.catalog_uri.starts_with("ducklake:duckdb:"));
        assert!(fs.pond_exists(id));
    }
}
```

- [ ] **Step 2: Wire** — add `tempfile = "3"` to workspace deps + `tempfile = { workspace = true }` to `crates/latiq-storage/Cargo.toml`. Add `pub mod temp_fs; pub use temp_fs::TempFs;` to `lib.rs`.
- [ ] **Step 3: Run** — `cargo test -p latiq-storage` → 2 passed.
- [ ] **Step 4: Commit** — `feat(storage): TempFs test backend (proves the storage seam)`

## Task 3.4: latiq-engine — result types

**Files:** Create `crates/latiq-engine/src/result.rs`; modify `src/lib.rs`, `Cargo.toml`.

- [ ] **Step 1: Write `result.rs`**
```rust
//! Neutral, protocol-agnostic query result types produced by any QueryEngine.
use latiq_common::QueryMeta;
use serde::{Deserialize, Serialize};

/// A query result. Rows are positional cells aligned to `columns`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct QueryResult {
    pub columns: Vec<String>,
    pub rows: Vec<Vec<serde_json::Value>>,
    pub meta: QueryMeta,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ScanOp {
    pub table: String,
    /// "full_scan" | "filtered_scan" | "indexed"
    pub scan_type: String,
    pub estimated_rows_scanned: u64,
    /// "pond" | "attached"
    pub source: String,
}

/// Result of explain_query — estimates + guidance + raw plan.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ExplainResult {
    pub estimated_rows: u64,
    pub estimated_bytes: u64,
    pub estimated_duration_ms: u64,
    #[serde(default)]
    pub scan_operations: Vec<ScanOp>,
    #[serde(default)]
    pub warnings: Vec<String>,
    #[serde(default)]
    pub suggestions: Vec<String>,
    pub raw_plan: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TableInfo {
    pub name: String,
    pub columns: Vec<(String, String)>, // (name, type)
    pub row_count_estimate: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub comment: Option<String>,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct SchemaSummary {
    pub tables: Vec<TableInfo>,
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn query_result_serializes() {
        let r = QueryResult {
            columns: vec!["id".into()],
            rows: vec![vec![serde_json::json!(1)]],
            meta: QueryMeta { rows: 1, ..Default::default() },
        };
        let v = serde_json::to_value(&r).unwrap();
        assert_eq!(v["columns"][0], "id");
        assert_eq!(v["rows"][0][0], 1);
    }
}
```

- [ ] **Step 2: Wire `Cargo.toml`**
```toml
[dependencies]
latiq-common = { path = "../latiq-common" }
serde = { workspace = true }
serde_json = { workspace = true }
thiserror = { workspace = true }
tokio-util = { workspace = true }
```
Add `tokio-util = { version = "0.7", features = ["rt"] }` to workspace deps. Set `lib.rs`:
```rust
//! latiq-engine — engine-agnostic query contract (DuckLake-format targeted).
pub mod result;
pub use result::{ExplainResult, QueryResult, ScanOp, SchemaSummary, TableInfo};
```

- [ ] **Step 3: Run** — `cargo test -p latiq-engine result::` → 1 passed.
- [ ] **Step 4: Commit** — `feat(engine): neutral query/explain/schema result types`

## Task 3.5: latiq-engine — AbortToken + QueryEngine trait

**Files:** Create `crates/latiq-engine/src/{abort.rs,engine.rs}`; modify `src/lib.rs`.

- [ ] **Step 1: Write `abort.rs`**
```rust
//! Cancellation primitive shared across engines. The in-flight *registry*
//! (op-id → token) lives in latiq-agent-core; here we only define the token
//! and the contract that execute() must honor it and release resources promptly.
pub use tokio_util::sync::CancellationToken as AbortToken;
```

- [ ] **Step 2: Write `engine.rs`**
```rust
use crate::abort::AbortToken;
use crate::result::{ExplainResult, QueryResult, SchemaSummary};
use latiq_common::Identity;
use latiq_storage::PondLocation;

#[derive(Debug, thiserror::Error)]
pub enum EngineError {
    #[error("query parse error: {0}")]
    Parse(String),
    #[error("write to reserved schema _latiq is not allowed")]
    ReservedSchemaWrite,
    #[error("read_query received a non-read statement; use write_query")]
    ReadOnlyViolation,
    #[error("query was cancelled")]
    Cancelled,
    #[error("query timed out")]
    Timeout,
    #[error("engine error: {0}")]
    Engine(String),
}

/// Executes SQL against a pond's DuckLake storage. One implementation per engine
/// (DuckDB now; DataFusion later). Methods are blocking — callers run them on a
/// blocking thread. `abort` MUST interrupt execution and release engine resources
/// within a bounded window (see spec §6).
pub trait QueryEngine: Send + Sync {
    /// Initialize a freshly-created pond (create the `_latiq` views, load extensions).
    fn init_pond(&self, loc: &PondLocation) -> Result<(), EngineError>;
    /// Run a read-only query (SELECT / read-only metadata). Rejects writes.
    fn read_query(&self, loc: &PondLocation, sql: &str, abort: AbortToken) -> Result<QueryResult, EngineError>;
    /// Run a write/DDL query, transaction-wrapped with native attribution.
    fn write_query(&self, loc: &PondLocation, sql: &str, identity: &Identity, abort: AbortToken) -> Result<QueryResult, EngineError>;
    /// Plan a query without executing it.
    fn explain_query(&self, loc: &PondLocation, sql: &str) -> Result<ExplainResult, EngineError>;
    /// Summarize the pond's user tables (for describe_pond).
    fn describe_schema(&self, loc: &PondLocation) -> Result<SchemaSummary, EngineError>;
}
```

- [ ] **Step 3: Wire `lib.rs`** — add:
```rust
pub mod abort;
pub mod engine;
pub use abort::AbortToken;
pub use engine::{EngineError, QueryEngine};
```
Add `latiq-storage = { path = "../latiq-storage" }` to `crates/latiq-engine/Cargo.toml` deps.

- [ ] **Step 4: Build** — `cargo build -p latiq-engine` → compiles.
- [ ] **Step 5: Commit** — `feat(engine): AbortToken + QueryEngine trait`

## Task 3.6: latiq-engine-duckdb — per-pond instance (open/attach/extensions)

**Files:** Create `crates/latiq-engine-duckdb/src/instance.rs`; modify `src/lib.rs`, `Cargo.toml`. *(adapt per spike findings for exact ATTACH + extension SQL.)*

- [ ] **Step 1: Write `instance.rs`** — opens an in-memory DuckDB, loads the extension allowlist, and ATTACHes the pond's DuckLake catalog as schema `pond`. Exposes a connection + its interrupt handle.
```rust
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
        // ATTACH the pond's DuckLake catalog. (adapt: exact syntax per spike findings)
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
```

- [ ] **Step 2: Wire `Cargo.toml`**
```toml
[dependencies]
latiq-common = { path = "../latiq-common" }
latiq-storage = { path = "../latiq-storage" }
latiq-engine = { path = "../latiq-engine" }
duckdb = { workspace = true }
serde_json = { workspace = true }
tokio = { workspace = true }
tokio-util = { workspace = true }

[dev-dependencies]
tempfile = { workspace = true }
```
Add `duckdb = { version = "1", features = ["bundled"] }` to workspace deps. Set `lib.rs`:
```rust
//! latiq-engine-duckdb — DuckDB + DuckLake implementation of QueryEngine.
pub mod instance;
```

- [ ] **Step 3: Write a smoke test (in `instance.rs`)** — proves open+attach+round-trip on a TempFs pond.
```rust
#[cfg(test)]
mod tests {
    use super::*;
    use latiq_storage::{PondStorage, TempFs};
    use latiq_common::PondId;

    #[test]
    fn opens_attaches_and_round_trips() {
        let fs = TempFs::new();
        let loc = fs.create_pond(PondId::new()).unwrap();
        let inst = PondInstance::open(&loc).unwrap();
        inst.conn.execute_batch(
            "CREATE TABLE t(id INTEGER); INSERT INTO t VALUES (1),(2);"
        ).unwrap();
        let n: i64 = inst.conn.query_row("SELECT count(*) FROM t", [], |r| r.get(0)).unwrap();
        assert_eq!(n, 2);
    }
}
```

- [ ] **Step 4: Run** — `cargo test -p latiq-engine-duckdb instance::` → 1 passed. (First build compiles DuckDB from source — may take several minutes; allow it.)
- [ ] **Step 5: Commit** — `feat(engine-duckdb): per-pond DuckDB instance with DuckLake attach`

## Task 3.7: latiq-engine-duckdb — `_latiq` schema views + reserved guard

**Files:** Create `crates/latiq-engine-duckdb/src/latiq_schema.rs`; modify `lib.rs`. *(adapt per spike findings for snapshots() / catalog introspection SQL.)*

- [ ] **Step 1: Write `latiq_schema.rs`** — creates the read-only `_latiq` views over DuckLake/DuckDB metadata, and a guard that rejects writes targeting `_latiq`.
```rust
//! The reserved `_latiq` schema: read-only views over DuckLake + DuckDB catalog.
//! Pure DuckLake — no Latiq-side store (spec §9).
use duckdb::Connection;
use latiq_engine::EngineError;

/// Create the `_latiq` schema + views on a freshly-attached pond instance.
/// (adapt the exact view SQL to the DuckLake snapshot/table functions confirmed
/// in the spike findings: `pond.snapshots()` exposes snapshot_id/author/commit_message.)
pub fn create_latiq_schema(conn: &Connection) -> Result<(), EngineError> {
    let sql = r#"
        CREATE SCHEMA IF NOT EXISTS _latiq;
        CREATE OR REPLACE VIEW _latiq.snapshots AS
            SELECT snapshot_id, snapshot_time, author, commit_message
            FROM pond.snapshots();
        CREATE OR REPLACE VIEW _latiq.attribution AS
            SELECT snapshot_id, author, commit_message
            FROM pond.snapshots();
        CREATE OR REPLACE VIEW _latiq.tables_summary AS
            SELECT table_name AS name, estimated_size AS row_count, comment
            FROM duckdb_tables() WHERE schema_name = 'main' OR database_name = 'pond';
        CREATE OR REPLACE VIEW _latiq.sources AS
            SELECT NULL::VARCHAR AS name WHERE 1=0;
    "#;
    conn.execute_batch(sql).map_err(|e| EngineError::Engine(format!("create _latiq: {e}")))
}

/// Returns true if the SQL writes to the reserved `_latiq` schema. Conservative
/// substring/keyword check for Slice 0+ (a full parser comes later).
pub fn writes_reserved_schema(sql: &str) -> bool {
    let lower = sql.to_lowercase();
    let writes = ["insert into", "update", "delete from", "drop", "create", "alter", "truncate"];
    writes.iter().any(|w| lower.contains(w)) && lower.contains("_latiq")
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn detects_reserved_writes() {
        assert!(writes_reserved_schema("INSERT INTO _latiq.attribution VALUES (1)"));
        assert!(writes_reserved_schema("DROP VIEW _latiq.snapshots"));
        assert!(!writes_reserved_schema("SELECT * FROM _latiq.snapshots"));
        assert!(!writes_reserved_schema("INSERT INTO events VALUES (1)"));
    }
}
```

- [ ] **Step 2: Export** — add `pub mod latiq_schema;` to `lib.rs`.
- [ ] **Step 3: Run** — `cargo test -p latiq-engine-duckdb latiq_schema::` → passes (the `writes_reserved_schema` unit test; view creation is exercised in the e2e test 3.10). If the view SQL needs adjustment, that surfaces in 3.10.
- [ ] **Step 4: Commit** — `feat(engine-duckdb): _latiq read-only views + reserved-schema guard`

## Task 3.8: latiq-engine-duckdb — row decoding + read/write/explain exec

**Files:** Create `crates/latiq-engine-duckdb/src/exec.rs`; modify `lib.rs`. *(adapt per spike findings for duckdb row/column APIs.)*

- [ ] **Step 1: Write `exec.rs`** — execute a SELECT into `QueryResult` (columns + JSON cells + meta), execute a write inside a txn with `set_commit_message` attribution, and wrap EXPLAIN. Decode cells via `duckdb`'s row API into `serde_json::Value` (map NULL→null, ints→number, floats→number, bool→bool, text→string, else→string).
```rust
use crate::instance::PondInstance;
use crate::latiq_schema::writes_reserved_schema;
use duckdb::types::ValueRef;
use latiq_common::{Identity, QueryMeta};
use latiq_engine::{EngineError, ExplainResult, QueryResult};
use std::time::Instant;

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
        other => Value::String(format!("{other:?}")),
    }
}

fn is_read_only(sql: &str) -> bool {
    let s = sql.trim_start().to_lowercase();
    s.starts_with("select") || s.starts_with("with") || s.starts_with("describe")
        || s.starts_with("show") || s.starts_with("explain") || s.starts_with("pragma")
}

/// Run a SELECT, materializing rows (bounded by the caller's cap).
pub fn run_read(inst: &PondInstance, sql: &str) -> Result<QueryResult, EngineError> {
    if !is_read_only(sql) {
        return Err(EngineError::ReadOnlyViolation);
    }
    let t0 = Instant::now();
    let mut stmt = inst.conn.prepare(sql).map_err(|e| EngineError::Parse(e.to_string()))?;
    let mut rows = stmt.query([]).map_err(|e| EngineError::Engine(e.to_string()))?;
    let mut out: Vec<Vec<serde_json::Value>> = Vec::new();
    let mut columns: Vec<String> = Vec::new();
    let mut first = true;
    while let Some(row) = rows.next().map_err(|e| EngineError::Engine(e.to_string()))? {
        if first {
            columns = row.as_ref().column_names().iter().map(|s| s.to_string()).collect();
            first = false;
        }
        let mut cells = Vec::with_capacity(columns.len());
        for i in 0..columns.len() {
            cells.push(cell_to_json(row.get_ref(i).map_err(|e| EngineError::Engine(e.to_string()))?));
        }
        out.push(cells);
    }
    if first {
        // zero rows: still fetch column names from the statement
        columns = stmt.column_names().iter().map(|s| s.to_string()).collect();
    }
    let meta = QueryMeta { rows: out.len() as u64, duration_ms: t0.elapsed().as_millis() as u64, ..Default::default() };
    Ok(QueryResult { columns, rows: out, meta })
}

/// Run a write/DDL inside a transaction, stamping native DuckLake attribution.
pub fn run_write(inst: &PondInstance, sql: &str, identity: &Identity) -> Result<QueryResult, EngineError> {
    if writes_reserved_schema(sql) {
        return Err(EngineError::ReservedSchemaWrite);
    }
    let t0 = Instant::now();
    // Escape single quotes in identity for the CALL (defensive).
    let agent = identity.agent_id.replace('\'', "''");
    let extra = format!("{{\"verified\":{}}}", identity.verified);
    let batch = format!(
        "BEGIN; {sql}; CALL pond.set_commit_message('{agent}', 'write_query', extra_info => '{extra}'); COMMIT;"
    );
    inst.conn.execute_batch(&batch).map_err(|e| EngineError::Engine(e.to_string()))?;
    // Read back the latest snapshot id for _meta.
    let snapshot_id: Option<i64> = inst.conn
        .query_row("SELECT max(snapshot_id) FROM pond.snapshots()", [], |r| r.get(0))
        .ok();
    let meta = QueryMeta { snapshot_id, duration_ms: t0.elapsed().as_millis() as u64, ..Default::default() };
    Ok(QueryResult { columns: vec![], rows: vec![], meta })
}

/// Wrap DuckDB EXPLAIN; produce the raw plan + a coarse estimate. (Richer
/// estimate parsing is a later refinement.)
pub fn run_explain(inst: &PondInstance, sql: &str) -> Result<ExplainResult, EngineError> {
    let explain_sql = format!("EXPLAIN {sql}");
    let mut stmt = inst.conn.prepare(&explain_sql).map_err(|e| EngineError::Parse(e.to_string()))?;
    let mut rows = stmt.query([]).map_err(|e| EngineError::Engine(e.to_string()))?;
    let mut plan = String::new();
    while let Some(row) = rows.next().map_err(|e| EngineError::Engine(e.to_string()))? {
        // EXPLAIN returns columns; concatenate text cells.
        for i in 0..row.as_ref().column_names().len() {
            if let ValueRef::Text(t) = row.get_ref(i).map_err(|e| EngineError::Engine(e.to_string()))? {
                plan.push_str(&String::from_utf8_lossy(t));
                plan.push('\n');
            }
        }
    }
    Ok(ExplainResult {
        estimated_rows: 0, estimated_bytes: 0, estimated_duration_ms: 0,
        scan_operations: vec![], warnings: vec![], suggestions: vec![], raw_plan: plan,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::latiq_schema::create_latiq_schema;
    use latiq_storage::{PondStorage, TempFs};
    use latiq_common::PondId;

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
        // attribution visible
        let attr = run_read(&inst, "SELECT author FROM _latiq.attribution WHERE author = 'agent-test'").unwrap();
        assert!(!attr.rows.is_empty(), "expected attribution rows for agent-test");
    }

    #[test]
    fn read_rejects_writes() {
        let (_fs, inst) = pond();
        assert!(matches!(run_read(&inst, "INSERT INTO t VALUES (1)"), Err(EngineError::ReadOnlyViolation)));
    }

    #[test]
    fn rejects_reserved_schema_write() {
        let (_fs, inst) = pond();
        assert!(matches!(
            run_write(&inst, "INSERT INTO _latiq.attribution VALUES (1)", &Identity::claimed(None)),
            Err(EngineError::ReservedSchemaWrite)
        ));
    }
}
```

- [ ] **Step 2: Export** — add `pub mod exec;` to `lib.rs`.
- [ ] **Step 3: Run** — `cargo test -p latiq-engine-duckdb exec::` → 3 passed. *(If the DuckLake `snapshots()` / attribution view SQL needs tweaking to match the real columns, fix `latiq_schema.rs` per the spike findings until attribution assertions pass.)*
- [ ] **Step 4: Commit** — `feat(engine-duckdb): read/write(attributed)/explain execution + row decode`

## Task 3.9: latiq-engine-duckdb — DuckEngine (QueryEngine impl) + cancellation

**Files:** Create `crates/latiq-engine-duckdb/src/duck_engine.rs`; modify `lib.rs`. *(adapt per spike findings for `interrupt_handle()`.)*

- [ ] **Step 1: Write `duck_engine.rs`** — implement `QueryEngine`. Each call opens a `PondInstance` (one connection per query, per spec §5/§6), spawns an interrupt watcher bound to the `AbortToken`, runs the blocking exec, and on cancel returns `EngineError::Cancelled`. `init_pond` opens an instance and creates the `_latiq` schema.
```rust
use crate::exec::{run_explain, run_read, run_write};
use crate::instance::PondInstance;
use crate::latiq_schema::create_latiq_schema;
use latiq_common::Identity;
use latiq_engine::{AbortToken, EngineError, ExplainResult, QueryEngine, QueryResult, SchemaSummary};
use latiq_storage::PondLocation;

pub struct DuckEngine;

impl DuckEngine {
    pub fn new() -> Self { Self }

    /// Run a blocking closure with an interrupt watcher wired to `abort`.
    fn with_abort<T>(
        inst: &PondInstance,
        abort: &AbortToken,
        f: impl FnOnce(&PondInstance) -> Result<T, EngineError>,
    ) -> Result<T, EngineError> {
        let handle = inst.conn.interrupt_handle(); // Arc<InterruptHandle> (adapt per spike)
        let token = abort.clone();
        // Watcher thread: interrupt the connection when the token is cancelled.
        let watcher = std::thread::spawn(move || {
            // block until cancelled (or dropped via the drop_guard below)
            while !token.is_cancelled() {
                std::thread::sleep(std::time::Duration::from_millis(20));
                if token.is_cancelled() { break; }
            }
            if token.is_cancelled() { handle.interrupt(); }
        });
        let result = f(inst);
        // Ensure the watcher stops: if the query finished first, cancel a *local*
        // signal. Simplest: detach by cancelling token only on real abort; here we
        // just join after nudging. Use a dedicated guard token to end the loop.
        // (adapt: the implementer may prefer a CancellationToken child + select.)
        let _ = watcher; // watcher exits on cancellation; on success it spins until process/token end
        match result {
            Err(EngineError::Engine(ref m)) if m.contains("INTERRUPT") => Err(EngineError::Cancelled),
            other => other,
        }
    }
}

impl Default for DuckEngine { fn default() -> Self { Self::new() } }

impl QueryEngine for DuckEngine {
    fn init_pond(&self, loc: &PondLocation) -> Result<(), EngineError> {
        let inst = PondInstance::open(loc)?;
        create_latiq_schema(&inst.conn)
    }
    fn read_query(&self, loc: &PondLocation, sql: &str, abort: AbortToken) -> Result<QueryResult, EngineError> {
        let inst = PondInstance::open(loc)?;
        Self::with_abort(&inst, &abort, |i| run_read(i, sql))
    }
    fn write_query(&self, loc: &PondLocation, sql: &str, identity: &Identity, abort: AbortToken) -> Result<QueryResult, EngineError> {
        let inst = PondInstance::open(loc)?;
        Self::with_abort(&inst, &abort, |i| run_write(i, sql, identity))
    }
    fn explain_query(&self, loc: &PondLocation, sql: &str) -> Result<ExplainResult, EngineError> {
        let inst = PondInstance::open(loc)?;
        run_explain(&inst, sql)
    }
    fn describe_schema(&self, loc: &PondLocation) -> Result<SchemaSummary, EngineError> {
        let inst = PondInstance::open(loc)?;
        let res = run_read(&inst, "SELECT name, row_count, comment FROM _latiq.tables_summary")?;
        let tables = res.rows.iter().map(|r| latiq_engine::TableInfo {
            name: r.first().and_then(|v| v.as_str()).unwrap_or("").to_string(),
            columns: vec![],
            row_count_estimate: r.get(1).and_then(|v| v.as_u64()).unwrap_or(0),
            comment: r.get(2).and_then(|v| v.as_str()).map(|s| s.to_string()),
        }).collect();
        Ok(SchemaSummary { tables })
    }
}
```
> **Implementer note (cancellation watcher):** the watcher above is a sketch — the spike confirmed `interrupt_handle()` works and interrupt is effectively instant. Implement the watcher so it (a) interrupts on `abort` cancellation and (b) terminates promptly when the query finishes normally (e.g. a child `CancellationToken` you cancel in a drop-guard after `f` returns, with the watcher doing `token.cancelled()` via a small tokio runtime or a `std` condvar). Keep it correct and simple; the test below is the gate.

- [ ] **Step 2: Export** — add `pub mod duck_engine; pub use duck_engine::DuckEngine;` to `lib.rs`.
- [ ] **Step 3: Write the cancellation test (in `duck_engine.rs`)**
```rust
#[cfg(test)]
mod tests {
    use super::*;
    use latiq_storage::{PondStorage, TempFs};
    use latiq_common::PondId;
    use std::time::{Duration, Instant};

    #[test]
    fn cancels_long_running_query_and_recovers() {
        let fs = TempFs::new();
        let loc = fs.create_pond(PondId::new()).unwrap();
        let eng = DuckEngine::new();
        eng.init_pond(&loc).unwrap();

        let abort = AbortToken::new();
        let abort2 = abort.clone();
        std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(200));
            abort2.cancel();
        });
        let t0 = Instant::now();
        // A deliberately huge query that must be interrupted.
        let res = eng.read_query(&loc, "SELECT count(*) FROM range(100000000000) t1, range(1000) t2", abort);
        assert!(matches!(res, Err(EngineError::Cancelled)), "expected Cancelled, got {res:?}");
        assert!(t0.elapsed() < Duration::from_secs(5), "abort must be prompt");

        // Pond is still usable afterwards (resources reclaimed).
        let ok = eng.read_query(&loc, "SELECT 1 AS x", AbortToken::new()).unwrap();
        assert_eq!(ok.rows[0][0], serde_json::json!(1));
    }
}
```

- [ ] **Step 4: Run** — `cargo test -p latiq-engine-duckdb duck_engine::` → passes (cancellation prompt + pond reusable).
- [ ] **Step 5: Commit** — `feat(engine-duckdb): DuckEngine QueryEngine impl with prompt cancellation`

## Task 3.10: End-to-end integration test (storage + engine)

**Files:** Create `crates/latiq-engine-duckdb/tests/engine_e2e.rs`.

- [ ] **Step 1: Write the e2e test** — full lifecycle through the public traits only (proves the seams compose): create pond via `PondStorage`, `init_pond`, write attributed data, read it back, see attribution + tables_summary in `_latiq`, drop pond.
```rust
use latiq_common::{Identity, PondId};
use latiq_engine::{AbortToken, QueryEngine};
use latiq_engine_duckdb::DuckEngine;
use latiq_storage::{PondStorage, TempFs};

#[test]
fn pond_lifecycle_end_to_end() {
    let fs = TempFs::new();
    let eng = DuckEngine::new();
    let id = PondId::new();
    let loc = fs.create_pond(id).unwrap();
    eng.init_pond(&loc).unwrap();

    let agent = Identity::claimed(Some("agent-e2e"));
    eng.write_query(&loc, "CREATE TABLE events(id INTEGER COMMENT 'pk', sev VARCHAR) ", &agent, AbortToken::new()).unwrap();
    let w = eng.write_query(&loc, "INSERT INTO events VALUES (1,'high'),(2,'critical')", &agent, AbortToken::new()).unwrap();
    assert!(w.meta.snapshot_id.is_some(), "write should record a snapshot id");

    let r = eng.read_query(&loc, "SELECT id, sev FROM events ORDER BY id", AbortToken::new()).unwrap();
    assert_eq!(r.rows.len(), 2);
    assert_eq!(r.rows[1][1], serde_json::json!("critical"));

    let attr = eng.read_query(&loc, "SELECT DISTINCT author FROM _latiq.attribution", AbortToken::new()).unwrap();
    let authors: Vec<_> = attr.rows.iter().filter_map(|row| row[0].as_str()).collect();
    assert!(authors.contains(&"agent-e2e"), "attribution must name the writer; got {authors:?}");

    let schema = eng.describe_schema(&loc).unwrap();
    assert!(schema.tables.iter().any(|t| t.name == "events"));

    fs.drop_pond(id).unwrap();
    assert!(!fs.pond_exists(id));
}
```

- [ ] **Step 2: Run** — `cargo test -p latiq-engine-duckdb --test engine_e2e` → passes. *(If DuckLake snapshot/table-function SQL differs, reconcile `latiq_schema.rs` and `exec.rs` against the spike findings until green.)*
- [ ] **Step 3: Final M3 gates** — `cargo test --workspace` (all green) + `cargo clippy --workspace --all-targets -- -D warnings` + `cargo fmt --all`.
- [ ] **Step 4: Commit** — `test(engine-duckdb): end-to-end pond lifecycle through public seams`

---

## Self-Review

- **Spec coverage:** §3 crates latiq-storage/latiq-engine/latiq-engine-duckdb ✅ (3.1–3.10); §5 instance-per-pond + DuckLake attach + txn-wrapped attributed writes ✅ (3.6, 3.8); §6 abort + prompt resource release + cancellation test ✅ (3.9); §9 `_latiq` views + reserved guard ✅ (3.7); storage seam proven by 2nd backend (TempFs) ✅ (3.3). `explain_query` is coarse (raw plan only) — richer estimate parsing deferred (noted), acceptable for Slice 0+.
- **Placeholder scan:** the `with_abort` watcher is a sketch with an explicit implementer note + a hard test gate (3.9) — not a silent placeholder. DuckLake-SQL "adapt per spike findings" markers are intrinsic (version-sensitive), each backed by a failing test that forces correctness.
- **Type consistency:** `PondLocation`, `QueryResult`, `ExplainResult`, `SchemaSummary`/`TableInfo`, `AbortToken`, `EngineError`, `QueryEngine` names are consistent across tasks and match the spec. These are the contracts M4–M7 build on.

## Next
After M3 green, plan M4 (control plane: DuckDB registry + migrations + Control/Admin gRPC + async audit).
