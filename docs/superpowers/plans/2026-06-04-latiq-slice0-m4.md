# Latiq Slice 0+ — M4 (Control Plane) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: superpowers:subagent-driven-development. Steps use checkbox (`- [ ]`).

**Goal:** The control-plane process: a DuckDB-backed registry (nodes, ponds, policy, audit) with a migrations framework, the **Control gRPC** surface (pond-nodes call it) and the **Admin gRPC** surface (the CLI calls it), and async audit ingestion. After M4, `latiq control-plane` can run and be driven over both gRPC surfaces (verified by in-process integration tests).

**Architecture:** `latiq-control-plane` owns a `Registry` (an `Arc<Mutex<duckdb::Connection>>` — the CP process is the sole writer, so single-writer DuckDB is the happy path; ops are sync + short, never held across `.await`). Two tonic services from `latiq-proto` (`Control`, `Admin`) wrap the `Registry`. See spec §4 (contracts + schema) and `docs/superpowers/notes/m1-spike-findings.md`.

**Tech Stack:** Rust, `duckdb` (cached), `tonic`/`prost` (cached), `tokio`, `serde_json`, `thiserror`.

---

## Conventions
- TDD; `cargo fmt` + `cargo clippy -p latiq-control-plane --all-targets -- -D warnings` clean before each commit.
- Commit trailer `Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>` via `git -c user.name='Latiq Dev' -c user.email='svsujeet@gmail.com' commit`. Use `git add -A` (new files).
- Builds are fast (duckdb + proto already compiled). Each task should run quickly.
- IDs/time: use `latiq_common::PondId` for pond ids. For timestamps, store as DuckDB `TIMESTAMP` via SQL `now()` / `current_timestamp` (do NOT call `Date::now()` in Rust).

---

## File structure (created in M4)
- `crates/latiq-control-plane/src/lib.rs` — re-exports + `serve` entrypoints
- `crates/latiq-control-plane/src/error.rs` — `ControlPlaneError`
- `crates/latiq-control-plane/src/migrations.rs` — schema migration runner + the v1 schema SQL
- `crates/latiq-control-plane/src/registry.rs` — `Registry` (DuckDB-backed) + domain methods + row structs
- `crates/latiq-control-plane/src/control_service.rs` — `ControlService: control_server::Control`
- `crates/latiq-control-plane/src/admin_service.rs` — `AdminService: admin_server::Admin`
- `crates/latiq-control-plane/tests/grpc_integration.rs` — start both servers in-process, drive via clients

---

## Task 4.1: Cargo wiring + error type + migrations framework

**Files:** Modify `Cargo.toml`; create `src/error.rs`, `src/migrations.rs`; modify `src/lib.rs`.

- [ ] **Step 1: Set `crates/latiq-control-plane/Cargo.toml`**
```toml
[package]
name = "latiq-control-plane"
version.workspace = true
edition.workspace = true

[dependencies]
latiq-common = { path = "../latiq-common" }
latiq-proto = { path = "../latiq-proto" }
duckdb = { workspace = true }
tonic = { workspace = true }
prost = { workspace = true }
tokio = { workspace = true }
serde_json = { workspace = true }
thiserror = { workspace = true }

[dev-dependencies]
tempfile = { workspace = true }
```

- [ ] **Step 2: Write `src/error.rs`**
```rust
//! Control-plane error type.
#[derive(Debug, thiserror::Error)]
pub enum ControlPlaneError {
    #[error("pond name already exists: {0}")]
    NameConflict(String),
    #[error("pond not found: {0}")]
    PondNotFound(String),
    #[error("node not found: {0}")]
    NodeNotFound(String),
    #[error("storage error: {0}")]
    Storage(String),
}

impl From<duckdb::Error> for ControlPlaneError {
    fn from(e: duckdb::Error) -> Self {
        ControlPlaneError::Storage(e.to_string())
    }
}
```

- [ ] **Step 3: Write `src/migrations.rs`** — a minimal forward-only migration runner: a `_latiq_schema_version(version INTEGER)` table; apply each migration whose index > current version, in order, then record the new version.
```rust
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
];

pub fn run_migrations(conn: &Connection) -> Result<(), ControlPlaneError> {
    conn.execute_batch(
        "CREATE TABLE IF NOT EXISTS _latiq_schema_version (version INTEGER NOT NULL);",
    )?;
    let current: i64 = conn
        .query_row("SELECT coalesce(max(version), 0) FROM _latiq_schema_version", [], |r| r.get(0))
        .unwrap_or(0);
    for (i, ddl) in MIGRATIONS.iter().enumerate() {
        let version = (i + 1) as i64;
        if version > current {
            conn.execute_batch(ddl)?;
            conn.execute("INSERT INTO _latiq_schema_version(version) VALUES (?)", [version])?;
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
        let v: i64 = conn.query_row("SELECT max(version) FROM _latiq_schema_version", [], |r| r.get(0)).unwrap();
        assert_eq!(v, MIGRATIONS.len() as i64);
        let n: i64 = conn.query_row("SELECT count(*) FROM policy", [], |r| r.get(0)).unwrap();
        assert_eq!(n, 2);
    }
}
```

- [ ] **Step 4: Set `src/lib.rs`**
```rust
//! latiq-control-plane — registry + Control/Admin gRPC surfaces.
pub mod admin_service;
pub mod control_service;
pub mod error;
pub mod migrations;
pub mod registry;
pub use error::ControlPlaneError;
pub use registry::Registry;
```
(The `admin_service`/`control_service`/`registry` modules are created in later tasks; for this task, create empty placeholder files `registry.rs`, `control_service.rs`, `admin_service.rs` each containing only a doc comment so the crate compiles, OR add the `pub mod` lines as each module lands. To keep this task self-contained, create the three files now with just a `//! placeholder` doc comment.)

- [ ] **Step 5: Run** — `cargo test -p latiq-control-plane migrations::` → 1 passed.
- [ ] **Step 6: Commit** — `feat(control-plane): error type + forward-only migrations framework`

## Task 4.2: Registry — domain operations over DuckDB

**Files:** Replace `src/registry.rs`.

- [ ] **Step 1: Write `registry.rs`** — `Registry` wraps `Arc<Mutex<Connection>>`; opens a DuckDB at a path (or in-memory) and runs migrations. Methods (all sync, lock-and-go):
  - `open(path: Option<&Path>) -> Result<Registry>` (in-memory if None)
  - `register_node(node_id, mcp_endpoint, internal_endpoint, capacity)` (upsert)
  - `heartbeat(node_id, pond_count)` — updates pond_count + last_heartbeat; `NodeNotFound` if absent
  - `list_nodes() -> Vec<NodeRow>` / `describe_node(node_id) -> NodeRow`
  - `create_pond(name: Option<String>, owner_identity, policy_json) -> PondRow` — generates `PondId` if name None (use the id string as the name too); enforce UNIQUE name → `NameConflict`; pick the single active node as `node_id` (first node; error `NodeNotFound` if none registered)
  - `get_pond_location(pond_ref) -> (pond_id, node_endpoint)` — resolve by id OR name; join ponds→nodes for the node's `internal_endpoint`; `PondNotFound` if missing
  - `drop_pond(pond_id)` — delete; `PondNotFound` if absent
  - `policy_get() -> serde_json::Value` (all key/values as a JSON object) / `policy_set(key, value)` (upsert)
  - `record_audit(entry: AuditInsert)` — insert one row; generate `audit_id` (a `PondId::new().to_string()` works as a UUID source)
  - `audit_tail(limit) -> Vec<AuditRow>` (most recent first) / `audit_search(identity, since) -> Vec<AuditRow>`
  
  Define `NodeRow`, `PondRow`, `AuditInsert`, `AuditRow` structs (plain data; serde derive). Use parameterized queries (`?` placeholders) everywhere — never string-interpolate values.

```rust
//! DuckDB-backed control-plane registry. The control-plane process is the sole
//! writer, so a single connection behind a Mutex is correct (single-writer).
use crate::error::ControlPlaneError;
use crate::migrations::run_migrations;
use duckdb::Connection;
use latiq_common::PondId;
use serde::{Deserialize, Serialize};
use std::path::Path;
use std::sync::{Arc, Mutex};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeRow {
    pub node_id: String,
    pub mcp_endpoint: String,
    pub internal_endpoint: String,
    pub capacity: u32,
    pub pond_count: u32,
    pub state: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PondRow {
    pub pond_id: String,
    pub name: String,
    pub owner_identity: String,
    pub node_id: String,
}

#[derive(Debug, Clone)]
pub struct AuditInsert {
    pub agent_identity: String,
    pub identity_verified: bool,
    pub operation: String,
    pub pond_id: Option<String>,
    pub request_summary: Option<String>,
    pub result_summary: Option<String>,
    pub duration_ms: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuditRow {
    pub ts: String,
    pub agent_identity: String,
    pub verified: bool,
    pub operation: String,
    pub pond_id: Option<String>,
    pub duration_ms: u64,
}

#[derive(Clone)]
pub struct Registry {
    conn: Arc<Mutex<Connection>>,
}

impl Registry {
    pub fn open(path: Option<&Path>) -> Result<Self, ControlPlaneError> {
        let conn = match path {
            Some(p) => Connection::open(p)?,
            None => Connection::open_in_memory()?,
        };
        run_migrations(&conn)?;
        Ok(Self { conn: Arc::new(Mutex::new(conn)) })
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, Connection> {
        self.conn.lock().expect("registry mutex poisoned")
    }

    pub fn register_node(&self, node_id: &str, mcp: &str, internal: &str, capacity: u32) -> Result<(), ControlPlaneError> {
        let c = self.lock();
        c.execute(
            "INSERT INTO nodes(node_id, mcp_endpoint, internal_endpoint, capacity)
             VALUES (?,?,?,?)
             ON CONFLICT (node_id) DO UPDATE SET
               mcp_endpoint=excluded.mcp_endpoint,
               internal_endpoint=excluded.internal_endpoint,
               capacity=excluded.capacity,
               last_heartbeat=current_timestamp",
            duckdb::params![node_id, mcp, internal, capacity],
        )?;
        Ok(())
    }

    pub fn heartbeat(&self, node_id: &str, pond_count: u32) -> Result<(), ControlPlaneError> {
        let c = self.lock();
        let n = c.execute(
            "UPDATE nodes SET pond_count=?, last_heartbeat=current_timestamp WHERE node_id=?",
            duckdb::params![pond_count, node_id],
        )?;
        if n == 0 { return Err(ControlPlaneError::NodeNotFound(node_id.to_string())); }
        Ok(())
    }

    pub fn list_nodes(&self) -> Result<Vec<NodeRow>, ControlPlaneError> {
        let c = self.lock();
        let mut stmt = c.prepare("SELECT node_id, mcp_endpoint, internal_endpoint, capacity, pond_count, state FROM nodes ORDER BY node_id")?;
        let rows = stmt.query_map([], |r| Ok(NodeRow {
            node_id: r.get(0)?, mcp_endpoint: r.get(1)?, internal_endpoint: r.get(2)?,
            capacity: r.get(3)?, pond_count: r.get(4)?, state: r.get(5)?,
        }))?;
        Ok(rows.collect::<Result<_, _>>()?)
    }

    pub fn describe_node(&self, node_id: &str) -> Result<NodeRow, ControlPlaneError> {
        self.list_nodes()?.into_iter().find(|n| n.node_id == node_id)
            .ok_or_else(|| ControlPlaneError::NodeNotFound(node_id.to_string()))
    }

    pub fn create_pond(&self, name: Option<String>, owner_identity: &str, policy_json: &str) -> Result<PondRow, ControlPlaneError> {
        let pond_id = PondId::new().to_string();
        let name = name.unwrap_or_else(|| pond_id.clone());
        let c = self.lock();
        // pick the single active node
        let node_id: String = c
            .query_row("SELECT node_id FROM nodes WHERE state='active' ORDER BY node_id LIMIT 1", [], |r| r.get(0))
            .map_err(|_| ControlPlaneError::NodeNotFound("no active node registered".into()))?;
        // name uniqueness
        let exists: i64 = c.query_row("SELECT count(*) FROM ponds WHERE name=?", duckdb::params![name], |r| r.get(0))?;
        if exists > 0 { return Err(ControlPlaneError::NameConflict(name)); }
        c.execute(
            "INSERT INTO ponds(pond_id, name, owner_identity, node_id, policy_json) VALUES (?,?,?,?,?)",
            duckdb::params![pond_id, name, owner_identity, node_id, policy_json],
        )?;
        Ok(PondRow { pond_id, name, owner_identity: owner_identity.to_string(), node_id })
    }

    pub fn get_pond_location(&self, pond_ref: &str) -> Result<(String, String), ControlPlaneError> {
        let c = self.lock();
        c.query_row(
            "SELECT p.pond_id, n.internal_endpoint FROM ponds p JOIN nodes n ON n.node_id=p.node_id
             WHERE p.pond_id=? OR p.name=? LIMIT 1",
            duckdb::params![pond_ref, pond_ref],
            |r| Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?)),
        ).map_err(|_| ControlPlaneError::PondNotFound(pond_ref.to_string()))
    }

    pub fn drop_pond(&self, pond_id: &str) -> Result<(), ControlPlaneError> {
        let c = self.lock();
        let n = c.execute("DELETE FROM ponds WHERE pond_id=? OR name=?", duckdb::params![pond_id, pond_id])?;
        if n == 0 { return Err(ControlPlaneError::PondNotFound(pond_id.to_string())); }
        Ok(())
    }

    pub fn policy_get(&self) -> Result<serde_json::Value, ControlPlaneError> {
        let c = self.lock();
        let mut stmt = c.prepare("SELECT key, value FROM policy ORDER BY key")?;
        let rows = stmt.query_map([], |r| Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?)))?;
        let mut map = serde_json::Map::new();
        for kv in rows { let (k, v) = kv?; map.insert(k, serde_json::Value::String(v)); }
        Ok(serde_json::Value::Object(map))
    }

    pub fn policy_set(&self, key: &str, value: &str) -> Result<(), ControlPlaneError> {
        let c = self.lock();
        c.execute(
            "INSERT INTO policy(key,value) VALUES (?,?) ON CONFLICT (key) DO UPDATE SET value=excluded.value",
            duckdb::params![key, value],
        )?;
        Ok(())
    }

    pub fn record_audit(&self, e: AuditInsert) -> Result<(), ControlPlaneError> {
        let audit_id = PondId::new().to_string();
        let c = self.lock();
        c.execute(
            "INSERT INTO audit_log(audit_id, agent_identity, identity_verified, operation, pond_id, request_summary, result_summary, duration_ms)
             VALUES (?,?,?,?,?,?,?,?)",
            duckdb::params![audit_id, e.agent_identity, e.identity_verified, e.operation, e.pond_id, e.request_summary, e.result_summary, e.duration_ms],
        )?;
        Ok(())
    }

    pub fn audit_tail(&self, limit: u32) -> Result<Vec<AuditRow>, ControlPlaneError> {
        let c = self.lock();
        let mut stmt = c.prepare(
            "SELECT ts::VARCHAR, agent_identity, identity_verified, operation, pond_id, duration_ms
             FROM audit_log ORDER BY ts DESC LIMIT ?")?;
        let rows = stmt.query_map(duckdb::params![limit], audit_row)?;
        Ok(rows.collect::<Result<_, _>>()?)
    }

    pub fn audit_search(&self, identity: &str, since: &str) -> Result<Vec<AuditRow>, ControlPlaneError> {
        let c = self.lock();
        let mut stmt = c.prepare(
            "SELECT ts::VARCHAR, agent_identity, identity_verified, operation, pond_id, duration_ms
             FROM audit_log WHERE agent_identity=? AND ts >= CAST(? AS TIMESTAMP) ORDER BY ts DESC")?;
        let rows = stmt.query_map(duckdb::params![identity, since], audit_row)?;
        Ok(rows.collect::<Result<_, _>>()?)
    }
}

fn audit_row(r: &duckdb::Row<'_>) -> duckdb::Result<AuditRow> {
    Ok(AuditRow {
        ts: r.get(0)?, agent_identity: r.get(1)?, verified: r.get(2)?,
        operation: r.get(3)?, pond_id: r.get(4)?, duration_ms: r.get(5)?,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn reg() -> Registry { Registry::open(None).unwrap() }

    #[test]
    fn node_and_pond_lifecycle() {
        let r = reg();
        r.register_node("node-a", "http://n:8080/mcp", "http://n:9092", 100).unwrap();
        r.heartbeat("node-a", 3).unwrap();
        assert_eq!(r.list_nodes().unwrap().len(), 1);
        let p = r.create_pond(Some("incident-1".into()), "agent-x", "{}").unwrap();
        assert_eq!(p.name, "incident-1");
        let (pid, endpoint) = r.get_pond_location("incident-1").unwrap();
        assert_eq!(pid, p.pond_id);
        assert_eq!(endpoint, "http://n:9092");
        assert!(matches!(r.create_pond(Some("incident-1".into()), "y", "{}"), Err(ControlPlaneError::NameConflict(_))));
        r.drop_pond(&p.pond_id).unwrap();
        assert!(matches!(r.get_pond_location("incident-1"), Err(ControlPlaneError::PondNotFound(_))));
    }

    #[test]
    fn create_pond_without_node_errors() {
        let r = reg();
        assert!(matches!(r.create_pond(None, "x", "{}"), Err(ControlPlaneError::NodeNotFound(_))));
    }

    #[test]
    fn policy_and_audit() {
        let r = reg();
        r.policy_set("query_timeout_seconds", "60").unwrap();
        assert_eq!(r.policy_get().unwrap()["query_timeout_seconds"], "60");
        r.record_audit(AuditInsert {
            agent_identity: "agent-x".into(), identity_verified: false, operation: "read_query".into(),
            pond_id: Some("p1".into()), request_summary: Some("SELECT ?".into()), result_summary: None, duration_ms: 12,
        }).unwrap();
        let tail = r.audit_tail(10).unwrap();
        assert_eq!(tail.len(), 1);
        assert_eq!(tail[0].operation, "read_query");
        assert_eq!(r.audit_search("agent-x", "1970-01-01").unwrap().len(), 1);
    }
}
```

- [ ] **Step 2: Run** — `cargo test -p latiq-control-plane registry::` → 3 passed. *(Adapt any duckdb param/cast detail that the compiler/test surfaces — e.g. `ts::VARCHAR` cast, `ON CONFLICT` syntax — DuckDB supports these, but fix minor differences until green.)*
- [ ] **Step 3: Commit** — `feat(control-plane): DuckDB-backed Registry (nodes/ponds/policy/audit)`

## Task 4.3: Control gRPC service

**Files:** Replace `src/control_service.rs`.

- [ ] **Step 1: Write `control_service.rs`** — implement `latiq_proto::v1::control_server::Control` over a `Registry`. Map registry errors to `tonic::Status` (NotFound → `Status::not_found`, NameConflict → `Status::already_exists`, others → `Status::internal`). Each RPC unpacks the request, calls the registry, builds the response.
```rust
//! Control gRPC service (pond-nodes call this).
use crate::error::ControlPlaneError;
use crate::registry::{AuditInsert, Registry};
use latiq_proto::v1::control_server::Control;
use latiq_proto::v1::*;
use tonic::{Request, Response, Status};

pub struct ControlService {
    pub registry: Registry,
}

impl ControlService {
    pub fn new(registry: Registry) -> Self { Self { registry } }
}

fn to_status(e: ControlPlaneError) -> Status {
    match e {
        ControlPlaneError::NameConflict(m) => Status::already_exists(m),
        ControlPlaneError::PondNotFound(m) | ControlPlaneError::NodeNotFound(m) => Status::not_found(m),
        ControlPlaneError::Storage(m) => Status::internal(m),
    }
}

#[tonic::async_trait]
impl Control for ControlService {
    async fn register_node(&self, req: Request<RegisterNodeRequest>) -> Result<Response<RegisterNodeResponse>, Status> {
        let r = req.into_inner();
        self.registry.register_node(&r.node_id, &r.mcp_endpoint, &r.internal_endpoint, r.capacity).map_err(to_status)?;
        Ok(Response::new(RegisterNodeResponse {}))
    }
    async fn heartbeat(&self, req: Request<HeartbeatRequest>) -> Result<Response<HeartbeatResponse>, Status> {
        let r = req.into_inner();
        self.registry.heartbeat(&r.node_id, r.pond_count).map_err(to_status)?;
        Ok(Response::new(HeartbeatResponse {}))
    }
    async fn create_pond_assignment(&self, req: Request<CreatePondAssignmentRequest>) -> Result<Response<CreatePondAssignmentResponse>, Status> {
        let r = req.into_inner();
        let name = if r.name.is_empty() { None } else { Some(r.name) };
        let pond = self.registry.create_pond(name, &r.owner_identity, &r.policy_json).map_err(to_status)?;
        let (_pid, endpoint) = self.registry.get_pond_location(&pond.pond_id).map_err(to_status)?;
        Ok(Response::new(CreatePondAssignmentResponse { pond_id: pond.pond_id, assigned_node_endpoint: endpoint }))
    }
    async fn get_pond_location(&self, req: Request<GetPondLocationRequest>) -> Result<Response<GetPondLocationResponse>, Status> {
        let (pond_id, node_endpoint) = self.registry.get_pond_location(&req.into_inner().pond_ref).map_err(to_status)?;
        Ok(Response::new(GetPondLocationResponse { pond_id, node_endpoint }))
    }
    async fn drop_pond_assignment(&self, req: Request<DropPondAssignmentRequest>) -> Result<Response<DropPondAssignmentResponse>, Status> {
        self.registry.drop_pond(&req.into_inner().pond_id).map_err(to_status)?;
        Ok(Response::new(DropPondAssignmentResponse {}))
    }
    async fn record_audit(&self, req: Request<RecordAuditRequest>) -> Result<Response<RecordAuditResponse>, Status> {
        let r = req.into_inner();
        self.registry.record_audit(AuditInsert {
            agent_identity: r.agent_identity, identity_verified: r.identity_verified, operation: r.operation,
            pond_id: if r.pond_id.is_empty() { None } else { Some(r.pond_id) },
            request_summary: if r.request_summary_json.is_empty() { None } else { Some(r.request_summary_json) },
            result_summary: if r.result_summary_json.is_empty() { None } else { Some(r.result_summary_json) },
            duration_ms: r.duration_ms,
        }).map_err(to_status)?;
        Ok(Response::new(RecordAuditResponse {}))
    }
}
```

- [ ] **Step 2: Build** — `cargo build -p latiq-control-plane` → compiles. *(Adapt to the exact generated proto type/module names from `latiq-proto` — e.g. `control_server::Control`, message field names — fix mismatches.)*
- [ ] **Step 3: Commit** — `feat(control-plane): Control gRPC service over the registry`

## Task 4.4: Admin gRPC service

**Files:** Replace `src/admin_service.rs`.

- [ ] **Step 1: Write `admin_service.rs`** — implement `admin_server::Admin` over a `Registry`, mapping rows to proto messages.
```rust
//! Admin gRPC service (the latiq CLI calls this).
use crate::error::ControlPlaneError;
use crate::registry::Registry;
use latiq_proto::v1::admin_server::Admin;
use latiq_proto::v1::*;
use tonic::{Request, Response, Status};

pub struct AdminService { pub registry: Registry }
impl AdminService { pub fn new(registry: Registry) -> Self { Self { registry } } }

fn to_status(e: ControlPlaneError) -> Status {
    match e {
        ControlPlaneError::PondNotFound(m) | ControlPlaneError::NodeNotFound(m) => Status::not_found(m),
        ControlPlaneError::NameConflict(m) => Status::already_exists(m),
        ControlPlaneError::Storage(m) => Status::internal(m),
    }
}

#[tonic::async_trait]
impl Admin for AdminService {
    async fn list_nodes(&self, _req: Request<ListNodesRequest>) -> Result<Response<ListNodesResponse>, Status> {
        let nodes = self.registry.list_nodes().map_err(to_status)?.into_iter().map(|n| NodeInfo {
            node_id: n.node_id, mcp_endpoint: n.mcp_endpoint, state: n.state, pond_count: n.pond_count,
        }).collect();
        Ok(Response::new(ListNodesResponse { nodes }))
    }
    async fn describe_node(&self, req: Request<DescribeNodeRequest>) -> Result<Response<DescribeNodeResponse>, Status> {
        let n = self.registry.describe_node(&req.into_inner().node_id).map_err(to_status)?;
        Ok(Response::new(DescribeNodeResponse { node: Some(NodeInfo {
            node_id: n.node_id, mcp_endpoint: n.mcp_endpoint, state: n.state, pond_count: n.pond_count,
        })}))
    }
    async fn policy_get(&self, _req: Request<PolicyGetRequest>) -> Result<Response<PolicyGetResponse>, Status> {
        let policy = self.registry.policy_get().map_err(to_status)?;
        Ok(Response::new(PolicyGetResponse { policy_json: policy.to_string() }))
    }
    async fn policy_set(&self, req: Request<PolicySetRequest>) -> Result<Response<PolicySetResponse>, Status> {
        let r = req.into_inner();
        self.registry.policy_set(&r.key, &r.value).map_err(to_status)?;
        Ok(Response::new(PolicySetResponse {}))
    }
    async fn audit_tail(&self, req: Request<AuditTailRequest>) -> Result<Response<AuditTailResponse>, Status> {
        let limit = req.into_inner().limit.max(1);
        let entries = self.registry.audit_tail(limit).map_err(to_status)?.into_iter().map(audit_entry).collect();
        Ok(Response::new(AuditTailResponse { entries }))
    }
    async fn audit_search(&self, req: Request<AuditSearchRequest>) -> Result<Response<AuditSearchResponse>, Status> {
        let r = req.into_inner();
        let since = if r.since.is_empty() { "1970-01-01".to_string() } else { r.since };
        let entries = self.registry.audit_search(&r.identity, &since).map_err(to_status)?.into_iter().map(audit_entry).collect();
        Ok(Response::new(AuditSearchResponse { entries }))
    }
}

fn audit_entry(a: crate::registry::AuditRow) -> AuditEntry {
    AuditEntry {
        ts: a.ts, agent_identity: a.agent_identity, verified: a.verified,
        operation: a.operation, pond_id: a.pond_id.unwrap_or_default(), duration_ms: a.duration_ms,
    }
}
```

- [ ] **Step 2: Build** — `cargo build -p latiq-control-plane` → compiles.
- [ ] **Step 3: Commit** — `feat(control-plane): Admin gRPC service over the registry`

## Task 4.5: serve entrypoints + gRPC integration test

**Files:** Modify `src/lib.rs` (add `serve_control` / `serve_admin`); create `tests/grpc_integration.rs`.

- [ ] **Step 1: Add serve functions to `src/lib.rs`**
```rust
use std::net::SocketAddr;
use tonic::transport::Server;

/// Serve the Control gRPC surface on `addr` until the process exits.
pub async fn serve_control(addr: SocketAddr, registry: Registry) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let svc = latiq_proto::v1::control_server::ControlServer::new(control_service::ControlService::new(registry));
    Server::builder().add_service(svc).serve(addr).await?;
    Ok(())
}

/// Serve the Admin gRPC surface on `addr` until the process exits.
pub async fn serve_admin(addr: SocketAddr, registry: Registry) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let svc = latiq_proto::v1::admin_server::AdminServer::new(admin_service::AdminService::new(registry));
    Server::builder().add_service(svc).serve(addr).await?;
    Ok(())
}
```

- [ ] **Step 2: Write `tests/grpc_integration.rs`** — start both services on ephemeral ports (share one in-memory `Registry`), then drive them with generated clients: register a node via Control, create a pond assignment, get its location; list nodes + set/get policy + record/tail audit via Admin.
```rust
use latiq_control_plane::{serve_admin, serve_control, Registry};
use latiq_proto::v1::admin_client::AdminClient;
use latiq_proto::v1::control_client::ControlClient;
use latiq_proto::v1::*;

#[tokio::test]
async fn control_and_admin_surfaces_work() {
    let registry = Registry::open(None).unwrap();

    let control_addr = "127.0.0.1:0".parse().unwrap();
    let listener = tokio::net::TcpListener::bind(control_addr).await.unwrap();
    let control_port = listener.local_addr().unwrap().port();
    drop(listener); // free the port for the server (use a fixed-but-ephemeral pattern)

    // Bind concrete addrs.
    let c_addr: std::net::SocketAddr = format!("127.0.0.1:{control_port}").parse().unwrap();
    let a_addr: std::net::SocketAddr = "127.0.0.1:0".parse().unwrap();
    let a_listener = tokio::net::TcpListener::bind(a_addr).await.unwrap();
    let admin_port = a_listener.local_addr().unwrap().port();
    drop(a_listener);
    let a_addr: std::net::SocketAddr = format!("127.0.0.1:{admin_port}").parse().unwrap();

    let r1 = registry.clone();
    let r2 = registry.clone();
    tokio::spawn(async move { serve_control(c_addr, r1).await.unwrap(); });
    tokio::spawn(async move { serve_admin(a_addr, r2).await.unwrap(); });
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;

    let mut control = ControlClient::connect(format!("http://127.0.0.1:{control_port}")).await.unwrap();
    let mut admin = AdminClient::connect(format!("http://127.0.0.1:{admin_port}")).await.unwrap();

    control.register_node(RegisterNodeRequest {
        node_id: "node-a".into(), mcp_endpoint: "http://n:8080/mcp".into(),
        internal_endpoint: "http://n:9092".into(), capacity: 100,
    }).await.unwrap();

    let created = control.create_pond_assignment(CreatePondAssignmentRequest {
        name: "incident-1".into(), owner_identity: "agent-x".into(), policy_json: "{}".into(),
    }).await.unwrap().into_inner();
    assert_eq!(created.assigned_node_endpoint, "http://n:9092");

    let loc = control.get_pond_location(GetPondLocationRequest { pond_ref: "incident-1".into() }).await.unwrap().into_inner();
    assert_eq!(loc.pond_id, created.pond_id);

    let nodes = admin.list_nodes(ListNodesRequest {}).await.unwrap().into_inner();
    assert_eq!(nodes.nodes.len(), 1);

    admin.policy_set(PolicySetRequest { key: "query_timeout_seconds".into(), value: "45".into() }).await.unwrap();
    let pol = admin.policy_get(PolicyGetRequest {}).await.unwrap().into_inner();
    assert!(pol.policy_json.contains("\"45\""));

    control.record_audit(RecordAuditRequest {
        agent_identity: "agent-x".into(), identity_verified: false, operation: "read_query".into(),
        pond_id: created.pond_id.clone(), request_summary_json: "{}".into(), result_summary_json: "{}".into(), duration_ms: 5,
    }).await.unwrap();
    let tail = admin.audit_tail(AuditTailRequest { limit: 10 }).await.unwrap().into_inner();
    assert_eq!(tail.entries.len(), 1);
    assert_eq!(tail.entries[0].operation, "read_query");
}
```

- [ ] **Step 3: Run** — `cargo test -p latiq-control-plane --test grpc_integration` → passes. *(Adapt the generated client/server type paths to the real `latiq-proto` module names. The port dance frees an ephemeral port then rebinds; if flaky, bind the server directly to `127.0.0.1:0` and read back the addr via a oneshot channel instead.)*
- [ ] **Step 4: M4 gates** — `cargo test -p latiq-control-plane` (all green) + `cargo clippy -p latiq-control-plane --all-targets -- -D warnings` + `cargo fmt --all`.
- [ ] **Step 5: Commit** — `feat(control-plane): serve entrypoints + gRPC integration test`

---

## Self-Review
- **Spec coverage:** §4 registry schema (nodes/ponds/policy/audit) ✅; migrations framework ✅; Control gRPC (all 6 RPCs) ✅; Admin gRPC (all 6 RPCs) ✅; name-uniqueness + allocate placement ✅; audit insert/tail/search ✅. Async audit *ingestion on the node side* is M5/M6 (the node enqueues); the CP `record_audit` write is here.
- **Placeholder scan:** "adapt to generated proto type paths" notes are intrinsic to codegen; each task has a build/test gate. No TODO/stub left behind (the 3 placeholder module files in 4.1 are replaced in 4.2–4.4).
- **Type consistency:** `Registry`, `NodeRow`/`PondRow`/`AuditRow`/`AuditInsert`, `ControlService`/`AdminService`, `serve_control`/`serve_admin` consistent; proto messages match `latiq-proto` (Task 2.6).

## Next
After M4 green: M5 (agent-core + MCP server + latiq-client).
