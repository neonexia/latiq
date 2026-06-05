# Latiq Slice 0+ — M1 (Spike) + M2 (Workspace & Kernel) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** De-risk the load-bearing dependencies (rmcp Streamable-HTTP, DuckLake-via-duckdb-rs, cancellation) with a throwaway spike, then stand up the Cargo workspace, the shared kernel crate, the gRPC contracts, and the binary skeleton.

**Architecture:** Hexagonal workspace (see `docs/superpowers/specs/2026-06-04-latiq-slice0-design.md`). This plan covers only M1 + M2; M3–M7 are planned after the spike's findings land.

**Tech Stack:** Rust, `tokio`, `rmcp` (MCP Streamable-HTTP), `tonic`/`prost` (gRPC), `duckdb` (duckdb-rs, `bundled` feature) + `ducklake` extension, `clap`, `serde`, `uuid`, `thiserror`.

---

## Conventions for this plan

- Rust edition 2021. Run `cargo fmt` and `cargo clippy --all-targets -- -D warnings` before each commit.
- Commit messages end with the Co-Authored-By trailer used in the repo's first commit.
- The spike (M1) is **exploratory, not TDD** — its tasks are *probes*: write a scratch program, run it, observe, and **record the working API pattern + surprises** in `docs/superpowers/notes/m1-spike-findings.md`. The spike code is throwaway and lives under `spike/` (a standalone crate, not a workspace member).

---

# Milestone 1 — Spike (de-risk gate)

**Deliverable:** `docs/superpowers/notes/m1-spike-findings.md` containing, for each probe: the exact crate version pinned, the working code snippet, and any surprises/blockers. The `spike/` crate itself is throwaway.

### Task 1.1: Scratch crate scaffold

**Files:**
- Create: `spike/Cargo.toml`
- Create: `spike/src/main.rs`
- Create: `docs/superpowers/notes/m1-spike-findings.md`
- Modify: `.gitignore` (add `/target` and `/spike/target`)

- [ ] **Step 1: Create `.gitignore`**

```gitignore
/target
/spike/target
**/*.rs.bk
.DS_Store
```

- [ ] **Step 2: Create `spike/Cargo.toml`** (pin the latest published versions; record the exact versions resolved in the findings doc)

```toml
[package]
name = "spike"
version = "0.0.0"
edition = "2021"
publish = false

[dependencies]
tokio = { version = "1", features = ["full"] }
duckdb = { version = "1", features = ["bundled"] }
rmcp = { version = "*", features = ["server", "transport-streamable-http-server"] }
anyhow = "1"
serde_json = "1"
# NOTE: pin `rmcp` and `duckdb` to the actual latest versions at execution time;
# adjust feature names to match the crate's real feature set and record them.
```

- [ ] **Step 3: Create `spike/src/main.rs` placeholder**

```rust
fn main() {
    println!("latiq spike — run individual probes from the plan");
}
```

- [ ] **Step 4: Create the findings doc skeleton**

```markdown
# M1 Spike Findings

Resolved versions: rmcp=?, duckdb=?, ducklake=?

## Probe A — DuckLake round-trip via duckdb-rs
## Probe B — Native attribution (set_commit_message)
## Probe C — rmcp Streamable-HTTP server (JSON tool call)
## Probe D — rmcp SSE response + progress notification
## Probe E — Query cancellation (Connection::interrupt)

## Surprises / blockers
## Decisions for M3+
```

- [ ] **Step 5: Verify it builds**

Run: `cd spike && cargo build`
Expected: compiles (resolving real versions). If a feature name is wrong, fix it and **record the correct feature flags** in the findings doc.

- [ ] **Step 6: Commit**

```bash
git add .gitignore spike docs/superpowers/notes/m1-spike-findings.md
git commit -m "spike: scaffold scratch crate for M1 de-risk probes"
```

### Task 1.2: Probe A — DuckLake round-trip via duckdb-rs

**Goal:** Confirm `duckdb-rs` can load the `ducklake` extension, ATTACH a DuckDB-backed catalog, and round-trip CREATE/INSERT/SELECT.

- [ ] **Step 1: Write the probe** (replace `main.rs`'s body)

```rust
use duckdb::Connection;

fn main() -> anyhow::Result<()> {
    let dir = std::env::temp_dir().join("latiq-spike-a");
    std::fs::create_dir_all(dir.join("data"))?;
    let catalog = dir.join("catalog.duckdb");
    let data = dir.join("data");

    let conn = Connection::open_in_memory()?;
    // ducklake is fetched from the extension repo on INSTALL (needs network the first time).
    conn.execute_batch("INSTALL ducklake; LOAD ducklake;")?;
    conn.execute_batch(&format!(
        "ATTACH 'ducklake:duckdb:{}' AS pond (DATA_PATH '{}');",
        catalog.display(), data.display()
    ))?;
    conn.execute_batch(
        "CREATE TABLE pond.events(id INTEGER, sev VARCHAR);
         INSERT INTO pond.events VALUES (1,'high'),(2,'low');",
    )?;
    let n: i64 = conn.query_row("SELECT count(*) FROM pond.events", [], |r| r.get(0))?;
    println!("row count = {n}");
    assert_eq!(n, 2);
    Ok(())
}
```

- [ ] **Step 2: Run it**

Run: `cd spike && cargo run`
Expected: prints `row count = 2`.

- [ ] **Step 3: Record findings.** In `m1-spike-findings.md` Probe A, paste the working ATTACH syntax (the `ducklake:duckdb:` prefix may differ — record what actually worked), and note whether `INSTALL ducklake` required network / whether the extension is autoloadable. If it fails offline, record the mitigation (pre-stage the extension, or `custom_extension_repository`).

- [ ] **Step 4: Commit**

```bash
git add spike/src/main.rs docs/superpowers/notes/m1-spike-findings.md
git commit -m "spike: probe A — DuckLake round-trip via duckdb-rs"
```

### Task 1.3: Probe B — Native attribution via set_commit_message

**Goal:** Confirm an agent identity set with `set_commit_message` is readable from snapshot metadata (this underpins `_latiq.attribution`).

- [ ] **Step 1: Write the probe** (append after the Probe A insert, before `Ok(())`)

```rust
    conn.execute_batch(
        "BEGIN;
         INSERT INTO pond.events VALUES (3,'critical');
         CALL pond.set_commit_message('agent-spike', 'write_query', extra_info => '{\"verified\":false}');
         COMMIT;",
    )?;
    // Try both documented call forms; keep whichever returns the author.
    let mut stmt = conn.prepare("SELECT snapshot_id, author, commit_message FROM pond.snapshots()")?;
    let rows = stmt.query_map([], |r| Ok((r.get::<_, i64>(0)?, r.get::<_, Option<String>>(1)?, r.get::<_, Option<String>>(2)?)))?;
    for row in rows { println!("snapshot = {:?}", row?); }
```

- [ ] **Step 2: Run it**

Run: `cd spike && cargo run`
Expected: at least one snapshot row shows `author = Some("agent-spike")`, `commit_message = Some("write_query")`.

- [ ] **Step 3: Record findings.** Record the exact working form of the snapshots table function (`pond.snapshots()` vs `ducklake_snapshots('pond')`), the exact column names for author/message/extra_info, and confirm `set_commit_message` requires the explicit `BEGIN…COMMIT` wrapper. **If author is not populated**, record it as a blocker and flag the attribution design for revisit.

- [ ] **Step 4: Commit**

```bash
git add spike/src/main.rs docs/superpowers/notes/m1-spike-findings.md
git commit -m "spike: probe B — native attribution via set_commit_message"
```

### Task 1.4: Probe C — rmcp Streamable-HTTP server, JSON tool call

**Goal:** Confirm `rmcp` can serve a Streamable-HTTP MCP endpoint with one tool returning a single JSON `CallToolResult`.

- [ ] **Step 1: Write a minimal rmcp server** in `spike/src/bin/probe_c.rs` exposing one `echo` tool over Streamable-HTTP, following the rmcp crate's current server example (the API shape — `#[tool]` macros vs builder — must be read from the pinned rmcp version's docs/examples and adapted).

- [ ] **Step 2: Run the server**

Run: `cd spike && cargo run --bin probe_c`
Expected: server binds an HTTP port (record which).

- [ ] **Step 3: Drive it with a raw MCP `tools/call`** using curl (initialize → tools/call), confirming a single `application/json` `CallToolResult` comes back.

Run (adapt the path/headers to what rmcp expects, record them):
```bash
curl -sN -X POST http://127.0.0.1:<port>/mcp \
  -H 'Content-Type: application/json' -H 'Accept: application/json, text/event-stream' \
  -d '{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"echo","arguments":{"msg":"hi"}}}'
```
Expected: a JSON-RPC result containing the echoed content.

- [ ] **Step 4: Record findings.** Record rmcp's server construction pattern (the exact types/macros), the endpoint path, required headers, and how tool handlers return content blocks + set `structuredContent`/`isError`.

- [ ] **Step 5: Commit**

```bash
git add spike/src/bin/probe_c.rs docs/superpowers/notes/m1-spike-findings.md
git commit -m "spike: probe C — rmcp streamable-http json tool call"
```

### Task 1.5: Probe D — SSE response + progress notification

**Goal:** Confirm a tool handler can return a `text/event-stream` response that emits a `notifications/progress` event before the final result (underpins SSE for `read_query`/`write_query`).

- [ ] **Step 1: Extend `probe_c.rs`** with a `slow_echo` tool that, given a `progressToken`, sleeps in a loop and sends progress notifications via rmcp's notification/peer handle, then returns the final result. (Read the rmcp version's API for sending progress from within a handler; record it.)

- [ ] **Step 2: Drive it with curl, observing SSE frames**

Run:
```bash
curl -sN -X POST http://127.0.0.1:<port>/mcp \
  -H 'Content-Type: application/json' -H 'Accept: text/event-stream' \
  -d '{"jsonrpc":"2.0","id":2,"method":"tools/call","params":{"name":"slow_echo","arguments":{},"_meta":{"progressToken":"p2"}}}'
```
Expected: one or more `data: {…notifications/progress…}` frames, then a final `data: {…result…}` frame, then stream close.

- [ ] **Step 3: Record findings.** Record how rmcp selects SSE vs JSON, how to send progress notifications from a handler, and whether a client disconnect surfaces to the handler (test by killing curl mid-stream — does the server observe it? This is the disconnect→abort signal).

- [ ] **Step 4: Commit**

```bash
git add spike/src/bin/probe_c.rs docs/superpowers/notes/m1-spike-findings.md
git commit -m "spike: probe D — SSE response + progress notification"
```

### Task 1.6: Probe E — Query cancellation via Connection::interrupt

**Goal:** Confirm a long DuckDB query on a `spawn_blocking` thread can be aborted from another thread and resources reclaimed.

- [ ] **Step 1: Write `spike/src/bin/probe_e.rs`** that starts a deliberately slow query (e.g. a large `range()` cross join) on a blocking thread, calls `interrupt()` from the main thread after ~200ms, and asserts the query returns an interrupted error promptly.

```rust
use duckdb::Connection;
use std::sync::Arc;
use std::time::{Duration, Instant};

fn main() -> anyhow::Result<()> {
    let conn = Arc::new(Connection::open_in_memory()?);
    let interrupter = conn.clone();
    let t0 = Instant::now();
    let handle = std::thread::spawn(move || {
        // Heavy query; should be interrupted, not complete.
        conn.execute_batch("SELECT count(*) FROM range(100000000000) t1, range(1000) t2;")
    });
    std::thread::sleep(Duration::from_millis(200));
    interrupter.interrupt(); // record the exact method name from duckdb-rs
    let res = handle.join().unwrap();
    println!("elapsed={:?} result_is_err={}", t0.elapsed(), res.is_err());
    assert!(res.is_err(), "expected interrupted error");
    assert!(t0.elapsed() < Duration::from_secs(5));
    Ok(())
}
```

- [ ] **Step 2: Run it**

Run: `cd spike && cargo run --bin probe_e`
Expected: prints a small elapsed time and `result_is_err=true`.

- [ ] **Step 3: Record findings.** Record the exact duckdb-rs interrupt API (method name, what it's called on — `Connection` vs an `InterruptHandle`), the observed abort latency, and confirm the discard-connection-on-cancel approach is viable.

- [ ] **Step 4: Write the spike summary.** Fill the "Surprises / blockers" and "Decisions for M3+" sections of the findings doc. Explicitly answer: does anything force a design change to §5/§6/§7 of the spec?

- [ ] **Step 5: Commit**

```bash
git add spike/src/bin/probe_e.rs docs/superpowers/notes/m1-spike-findings.md
git commit -m "spike: probe E — query cancellation + findings summary"
```

**M1 GATE:** Do not start M3 planning until the findings doc is complete and any spec-affecting surprises are reconciled. M2 (below) has no dependency on spike findings and may proceed in parallel.

---

# Milestone 2 — Workspace & kernel

**Deliverable:** A compiling Cargo workspace with the 10 crates, a tested `latiq-common` kernel, compiling gRPC contracts in `latiq-proto`, and a `latiq` binary whose `--help` lists the subcommands. No business logic yet.

### Task 2.1: Cargo workspace + 10 crate stubs

**Files:**
- Create: `Cargo.toml` (workspace root)
- Create: `crates/{latiq,latiq-common,latiq-proto,latiq-agent-core,latiq-mcp,latiq-engine,latiq-engine-duckdb,latiq-storage,latiq-pond-node,latiq-control-plane}/Cargo.toml` + `src/lib.rs` (or `src/main.rs` for `latiq`)

- [ ] **Step 1: Create the workspace root `Cargo.toml`**

```toml
[workspace]
resolver = "2"
members = [
  "crates/latiq",
  "crates/latiq-common",
  "crates/latiq-proto",
  "crates/latiq-agent-core",
  "crates/latiq-mcp",
  "crates/latiq-engine",
  "crates/latiq-engine-duckdb",
  "crates/latiq-storage",
  "crates/latiq-pond-node",
  "crates/latiq-control-plane",
]

[workspace.package]
version = "0.1.0"
edition = "2021"
license = "Apache-2.0"

[workspace.dependencies]
tokio = { version = "1", features = ["full"] }
serde = { version = "1", features = ["derive"] }
serde_json = "1"
thiserror = "1"
anyhow = "1"
uuid = { version = "1", features = ["v4", "serde"] }
tonic = "0.12"
prost = "0.13"
clap = { version = "4", features = ["derive"] }
```

- [ ] **Step 2: Create each library crate stub.** For each of the 9 lib crates, create `crates/<name>/Cargo.toml`:

```toml
[package]
name = "<name>"
version.workspace = true
edition.workspace = true

[dependencies]
```

and `crates/<name>/src/lib.rs`:

```rust
//! <name> — see docs/superpowers/specs/2026-06-04-latiq-slice0-design.md
```

- [ ] **Step 3: Create the `latiq` binary crate.** `crates/latiq/Cargo.toml`:

```toml
[package]
name = "latiq"
version.workspace = true
edition.workspace = true

[[bin]]
name = "latiq"
path = "src/main.rs"

[dependencies]
clap = { workspace = true }
```

and `crates/latiq/src/main.rs`:

```rust
fn main() {
    println!("latiq");
}
```

- [ ] **Step 4: Build the workspace**

Run: `cargo build`
Expected: all 10 crates compile.

- [ ] **Step 5: Commit**

```bash
git add Cargo.toml Cargo.lock crates
git commit -m "feat: scaffold 10-crate hexagonal workspace"
```

### Task 2.2: latiq-common — PondId (UUID newtype)

**Files:**
- Modify: `crates/latiq-common/Cargo.toml`
- Create: `crates/latiq-common/src/id.rs`
- Modify: `crates/latiq-common/src/lib.rs`

- [ ] **Step 1: Write the failing test** in `crates/latiq-common/src/id.rs`

```rust
//! Strongly-typed identifiers.
use serde::{Deserialize, Serialize};
use std::fmt;
use uuid::Uuid;

/// Opaque pond identifier (UUIDv4).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct PondId(Uuid);

impl PondId {
    pub fn new() -> Self { Self(Uuid::new_v4()) }
    pub fn parse(s: &str) -> Result<Self, uuid::Error> { Ok(Self(Uuid::parse_str(s)?)) }
}

impl fmt::Display for PondId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result { write!(f, "{}", self.0) }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrips_through_string() {
        let id = PondId::new();
        let parsed = PondId::parse(&id.to_string()).unwrap();
        assert_eq!(id, parsed);
    }

    #[test]
    fn rejects_garbage() {
        assert!(PondId::parse("not-a-uuid").is_err());
    }
}
```

- [ ] **Step 2: Wire deps + module.** Set `crates/latiq-common/Cargo.toml` deps:

```toml
[dependencies]
serde = { workspace = true }
uuid = { workspace = true }
```

and `crates/latiq-common/src/lib.rs`:

```rust
//! latiq-common — shared kernel (ids, identity, errors, results, config).
pub mod id;
pub use id::PondId;
```

- [ ] **Step 3: Run the tests**

Run: `cargo test -p latiq-common id::`
Expected: 2 passed.

- [ ] **Step 4: Commit**

```bash
git add crates/latiq-common
git commit -m "feat(common): PondId newtype with string roundtrip"
```

### Task 2.3: latiq-common — Identity (relaxed, verified flag)

**Files:**
- Create: `crates/latiq-common/src/identity.rs`
- Modify: `crates/latiq-common/src/lib.rs`

- [ ] **Step 1: Write the test + type** in `crates/latiq-common/src/identity.rs`

```rust
//! Agent identity. Slice 0+ is relaxed: identity is *claimed*, never verified.
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Identity {
    pub agent_id: String,
    pub verified: bool,
}

impl Identity {
    /// Build a claimed (unverified) identity, defaulting to "anonymous" when absent/empty.
    pub fn claimed(header: Option<&str>) -> Self {
        let agent_id = match header.map(str::trim) {
            Some(s) if !s.is_empty() => s.to_string(),
            _ => "anonymous".to_string(),
        };
        Self { agent_id, verified: false }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn uses_header_when_present() {
        let id = Identity::claimed(Some("agent-incident-bot"));
        assert_eq!(id.agent_id, "agent-incident-bot");
        assert!(!id.verified);
    }

    #[test]
    fn defaults_to_anonymous() {
        assert_eq!(Identity::claimed(None).agent_id, "anonymous");
        assert_eq!(Identity::claimed(Some("   ")).agent_id, "anonymous");
    }
}
```

- [ ] **Step 2: Export it.** Add to `crates/latiq-common/src/lib.rs`:

```rust
pub mod identity;
pub use identity::Identity;
```

- [ ] **Step 3: Run the tests**

Run: `cargo test -p latiq-common identity::`
Expected: 2 passed.

- [ ] **Step 4: Commit**

```bash
git add crates/latiq-common
git commit -m "feat(common): relaxed claimed Identity with anonymous fallback"
```

### Task 2.4: latiq-common — ErrorEnvelope (trelisdb-aligned)

**Files:**
- Create: `crates/latiq-common/src/error.rs`
- Modify: `crates/latiq-common/src/lib.rs`

- [ ] **Step 1: Write the test + types** in `crates/latiq-common/src/error.rs`

```rust
//! Agent-facing structured error envelope (adopted from trelisdb).
//! Philosophy: 80% of errors recoverable from `suggest` alone; 20% fetch `see`.
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct Location {
    pub line: u32,
    pub column: u32,
    pub byte: u32,
}

/// Closed taxonomy of error kinds (serialized as snake_case strings).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ErrorKind {
    PondNotFound,
    NameConflict,
    ParseError,
    InvalidValue,
    MissingArgument,
    WriteToReservedSchema,
    ResultCapExceeded,
    ReadOnlyViolation,
    UriNotAllowed,
    QueryTimeout,
    QueryCancelled,
    Storage,
    Internal,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ErrorEnvelope {
    pub kind: ErrorKind,
    /// One sentence on what went wrong. No "suggest" text here.
    pub message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub location: Option<Location>,
    /// Copy-paste-ready corrected example — the immediate retry path.
    pub suggest: String,
    /// `latiq://` resource URI + anchor — the deeper learning path.
    pub see: String,
}

impl ErrorEnvelope {
    pub fn new(
        kind: ErrorKind,
        message: impl Into<String>,
        suggest: impl Into<String>,
        see: impl Into<String>,
    ) -> Self {
        Self { kind, message: message.into(), location: None, suggest: suggest.into(), see: see.into() }
    }

    pub fn with_location(mut self, loc: Location) -> Self {
        self.location = Some(loc);
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn serializes_kind_as_snake_case() {
        let e = ErrorEnvelope::new(
            ErrorKind::PondNotFound,
            "Pond 'incident-001' does not exist.",
            "Call list_ponds to see available ponds.",
            "latiq://troubleshooting/pond-not-found",
        );
        let v = serde_json::to_value(&e).unwrap();
        assert_eq!(v["kind"], "pond_not_found");
        assert!(v.get("location").is_none(), "location omitted when None");
    }

    #[test]
    fn includes_location_when_set() {
        let e = ErrorEnvelope::new(ErrorKind::ParseError, "bad SQL", "fix it", "latiq://dialect")
            .with_location(Location { line: 1, column: 8, byte: 7 });
        let v = serde_json::to_value(&e).unwrap();
        assert_eq!(v["location"]["line"], 1);
    }
}
```

- [ ] **Step 2: Wire serde_json dev-dep + export.** Add to `crates/latiq-common/Cargo.toml`:

```toml
serde_json = { workspace = true }
```

and to `crates/latiq-common/src/lib.rs`:

```rust
pub mod error;
pub use error::{ErrorEnvelope, ErrorKind, Location};
```

- [ ] **Step 3: Run the tests**

Run: `cargo test -p latiq-common error::`
Expected: 2 passed.

- [ ] **Step 4: Commit**

```bash
git add crates/latiq-common
git commit -m "feat(common): trelisdb-aligned ErrorEnvelope + closed kind taxonomy"
```

### Task 2.5: latiq-common — QueryMeta + Warning (the _meta envelope)

**Files:**
- Create: `crates/latiq-common/src/meta.rs`
- Modify: `crates/latiq-common/src/lib.rs`

- [ ] **Step 1: Write the test + types** in `crates/latiq-common/src/meta.rs`

```rust
//! The `_meta` envelope carried on every query response ("every response carries signal").
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WarningKind { Performance, Portability, SchemaHygiene, ResultHygiene }

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Warning {
    pub kind: WarningKind,
    pub message: String,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct QueryMeta {
    pub rows: u64,
    pub rows_affected: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub snapshot_id: Option<i64>,
    pub duration_ms: u64,
    pub bytes_scanned: u64,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub tables_touched: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub warnings: Vec<Warning>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub hint: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn omits_empty_optional_fields() {
        let m = QueryMeta { rows: 10, duration_ms: 5, ..Default::default() };
        let v = serde_json::to_value(&m).unwrap();
        assert_eq!(v["rows"], 10);
        assert!(v.get("snapshot_id").is_none());
        assert!(v.get("warnings").is_none(), "empty warnings omitted");
    }

    #[test]
    fn serializes_warning_kind_snake_case() {
        let w = Warning { kind: WarningKind::Performance, message: "full scan".into() };
        assert_eq!(serde_json::to_value(&w).unwrap()["kind"], "performance");
    }
}
```

- [ ] **Step 2: Export it.** Add to `crates/latiq-common/src/lib.rs`:

```rust
pub mod meta;
pub use meta::{QueryMeta, Warning, WarningKind};
```

- [ ] **Step 3: Run the tests**

Run: `cargo test -p latiq-common meta::`
Expected: 2 passed.

- [ ] **Step 4: Commit**

```bash
git add crates/latiq-common
git commit -m "feat(common): QueryMeta + Warning envelope"
```

### Task 2.6: latiq-proto — Control + Admin gRPC contracts

**Files:**
- Modify: `crates/latiq-proto/Cargo.toml`
- Create: `crates/latiq-proto/build.rs`
- Create: `crates/latiq-proto/proto/latiq/v1/control.proto`
- Create: `crates/latiq-proto/proto/latiq/v1/admin.proto`
- Modify: `crates/latiq-proto/src/lib.rs`

- [ ] **Step 1: Write `control.proto`** (the pond-node → control-plane surface from spec §4)

```proto
syntax = "proto3";
package latiq.v1;

service Control {
  rpc RegisterNode(RegisterNodeRequest) returns (RegisterNodeResponse);
  rpc Heartbeat(HeartbeatRequest) returns (HeartbeatResponse);
  rpc CreatePondAssignment(CreatePondAssignmentRequest) returns (CreatePondAssignmentResponse);
  rpc GetPondLocation(GetPondLocationRequest) returns (GetPondLocationResponse);
  rpc DropPondAssignment(DropPondAssignmentRequest) returns (DropPondAssignmentResponse);
  rpc RecordAudit(RecordAuditRequest) returns (RecordAuditResponse);
}

message RegisterNodeRequest { string node_id = 1; string mcp_endpoint = 2; string internal_endpoint = 3; uint32 capacity = 4; }
message RegisterNodeResponse { }
message HeartbeatRequest { string node_id = 1; uint32 pond_count = 2; }
message HeartbeatResponse { }
message CreatePondAssignmentRequest { string name = 1; string owner_identity = 2; string policy_json = 3; }
message CreatePondAssignmentResponse { string pond_id = 1; string assigned_node_endpoint = 2; }
message GetPondLocationRequest { string pond_ref = 1; } // id or name
message GetPondLocationResponse { string pond_id = 1; string node_endpoint = 2; }
message DropPondAssignmentRequest { string pond_id = 1; }
message DropPondAssignmentResponse { }
message RecordAuditRequest {
  string agent_identity = 1; bool identity_verified = 2; string operation = 3;
  string pond_id = 4; string request_summary_json = 5; string result_summary_json = 6; uint64 duration_ms = 7;
}
message RecordAuditResponse { }
```

- [ ] **Step 2: Write `admin.proto`** (the CLI → control-plane surface)

```proto
syntax = "proto3";
package latiq.v1;

service Admin {
  rpc ListNodes(ListNodesRequest) returns (ListNodesResponse);
  rpc DescribeNode(DescribeNodeRequest) returns (DescribeNodeResponse);
  rpc PolicyGet(PolicyGetRequest) returns (PolicyGetResponse);
  rpc PolicySet(PolicySetRequest) returns (PolicySetResponse);
  rpc AuditTail(AuditTailRequest) returns (AuditTailResponse);
  rpc AuditSearch(AuditSearchRequest) returns (AuditSearchResponse);
}

message NodeInfo { string node_id = 1; string mcp_endpoint = 2; string state = 3; uint32 pond_count = 4; }
message ListNodesRequest { }
message ListNodesResponse { repeated NodeInfo nodes = 1; }
message DescribeNodeRequest { string node_id = 1; }
message DescribeNodeResponse { NodeInfo node = 1; }
message PolicyGetRequest { }
message PolicyGetResponse { string policy_json = 1; }
message PolicySetRequest { string key = 1; string value = 2; }
message PolicySetResponse { }
message AuditEntry { string ts = 1; string agent_identity = 2; bool verified = 3; string operation = 4; string pond_id = 5; uint64 duration_ms = 6; }
message AuditTailRequest { uint32 limit = 1; }
message AuditTailResponse { repeated AuditEntry entries = 1; }
message AuditSearchRequest { string identity = 1; string since = 2; }
message AuditSearchResponse { repeated AuditEntry entries = 1; }
```

- [ ] **Step 3: Write `build.rs`**

```rust
fn main() {
    tonic_build::configure()
        .build_server(true)
        .build_client(true)
        .compile_protos(
            &["proto/latiq/v1/control.proto", "proto/latiq/v1/admin.proto"],
            &["proto"],
        )
        .expect("compile protos");
}
```

- [ ] **Step 4: Set `crates/latiq-proto/Cargo.toml`**

```toml
[package]
name = "latiq-proto"
version.workspace = true
edition.workspace = true

[dependencies]
tonic = { workspace = true }
prost = { workspace = true }

[build-dependencies]
tonic-build = "0.12"
```

and `crates/latiq-proto/src/lib.rs`:

```rust
//! Generated gRPC contracts for the Control and Admin surfaces.
pub mod v1 {
    tonic::include_proto!("latiq.v1");
}
```

- [ ] **Step 5: Build and verify codegen**

Run: `cargo build -p latiq-proto`
Expected: compiles; generated `Control`/`Admin` server+client traits exist.

- [ ] **Step 6: Add a smoke test** in `crates/latiq-proto/src/lib.rs`

```rust
#[cfg(test)]
mod tests {
    #[test]
    fn types_are_generated() {
        // Construction proves codegen wired up.
        let _ = super::v1::CreatePondAssignmentRequest {
            name: "incident-001".into(),
            owner_identity: "agent".into(),
            policy_json: "{}".into(),
        };
    }
}
```

Run: `cargo test -p latiq-proto`
Expected: 1 passed.

- [ ] **Step 7: Commit**

```bash
git add crates/latiq-proto
git commit -m "feat(proto): Control + Admin gRPC contracts with tonic codegen"
```

### Task 2.7: latiq binary — clap subcommand skeleton

**Files:**
- Modify: `crates/latiq/Cargo.toml`
- Modify: `crates/latiq/src/main.rs`

- [ ] **Step 1: Write the CLI skeleton** in `crates/latiq/src/main.rs`

```rust
//! latiq — single binary. Server roles (control-plane, pond-node) + admin CLI.
use clap::{Parser, Subcommand};

#[derive(Parser)]
#[command(name = "latiq", version, about = "Agent-native data pond")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Run the control-plane server.
    ControlPlane,
    /// Run a pond-node server.
    PondNode,
    /// Operator: node administration.
    #[command(subcommand)]
    Node(NodeCmd),
    /// Operator: policy administration.
    #[command(subcommand)]
    Policy(PolicyCmd),
    /// Operator: audit access.
    #[command(subcommand)]
    Audit(AuditCmd),
}

#[derive(Subcommand)]
enum NodeCmd { List, Describe { node_id: String } }
#[derive(Subcommand)]
enum PolicyCmd { Show, Set { key: String, value: String } }
#[derive(Subcommand)]
enum AuditCmd { Tail, Search { identity: String } }

fn main() {
    let cli = Cli::parse();
    match cli.command {
        Command::ControlPlane => println!("control-plane: not yet implemented (M4)"),
        Command::PondNode => println!("pond-node: not yet implemented (M6)"),
        Command::Node(_) | Command::Policy(_) | Command::Audit(_) =>
            println!("admin CLI: not yet implemented (M4/M6)"),
    }
}
```

- [ ] **Step 2: Verify the subcommands parse**

Run: `cargo run -p latiq -- --help`
Expected: help text lists `control-plane`, `pond-node`, `node`, `policy`, `audit`.

Run: `cargo run -p latiq -- control-plane`
Expected: prints `control-plane: not yet implemented (M4)`.

- [ ] **Step 3: Commit**

```bash
git add crates/latiq
git commit -m "feat(cli): clap subcommand skeleton for server roles + admin"
```

### Task 2.8: CI workflow

**Files:**
- Create: `.github/workflows/ci.yml`

- [ ] **Step 1: Write the CI workflow**

```yaml
name: ci
on: [push, pull_request]
jobs:
  build:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: dtolnay/rust-toolchain@stable
        with: { components: rustfmt, clippy }
      - uses: Swatinem/rust-cache@v2
      - run: cargo fmt --all -- --check
      - run: cargo clippy --workspace --all-targets -- -D warnings
      - run: cargo test --workspace
```

- [ ] **Step 2: Verify the gates locally**

Run: `cargo fmt --all -- --check && cargo clippy --workspace --all-targets -- -D warnings && cargo test --workspace`
Expected: all pass (the `spike/` crate is excluded — it's not a workspace member).

- [ ] **Step 3: Commit**

```bash
git add .github/workflows/ci.yml
git commit -m "ci: fmt + clippy + test gate"
```

---

## Self-Review (completed)

- **Spec coverage (M1+M2 scope):** spike covers all four §13 spike-confirmed assumptions + attribution (§9) + cancellation (§6); workspace matches §3 crate layout exactly (10 crates); `latiq-common` covers Identity (§8 relaxed), ErrorEnvelope (§8), QueryMeta (§8), PondId (§8 addressing); `latiq-proto` covers the §4 Control + Admin method lists; binary covers the §4 subcommand surface. M3–M7 are intentionally out of this plan (gated on M1).
- **Placeholder scan:** the only "not yet implemented" strings are in the binary skeleton and are deliberate, labeled with the milestone that fills them. The spike's "adapt to the pinned version" notes are intrinsic to a de-risk spike, not placeholders for omitted plan content.
- **Type consistency:** `PondId`, `Identity`, `ErrorEnvelope/ErrorKind/Location`, `QueryMeta/Warning/WarningKind` names are used consistently; proto message/field names match spec §4.

## Next planning step

After M1's findings doc is complete and reconciled against the spec, write `docs/superpowers/plans/2026-06-04-latiq-slice0-m3plus.md` (outbound seams → control plane → surface → wiring → integration), using the spike-confirmed APIs.
