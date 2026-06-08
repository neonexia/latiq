# Latiq Slice 0+ — M5 (Inbound Surface: agent-core + MCP + client) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: superpowers:subagent-driven-development. Steps use checkbox (`- [ ]`). (Note: in practice the controller has been implementing M3–M4 directly due to subagent socket-drop flakiness; either path is fine — TDD + commit per task.)

**Goal:** The agent-facing inbound surface. `latiq-agent-core` holds protocol-neutral agent operations (allocate/describe/list/drop pond, read/write/explain query) + the in-flight/abort registry, decoupled from the control-plane via a `ControlPlane` trait. `latiq-mcp` exposes those ops as 7 MCP tools over rmcp Streamable-HTTP (dual text+structuredContent encoding, relaxed identity, SSE for queries, cancel→abort). `latiq-client` is an rmcp client wrapper for the CLI + integration tests.

**Architecture:** Hexagon core = `AgentOps` over `Arc<dyn ControlPlane>` + `Arc<dyn PondStorage>` + `Arc<dyn QueryEngine>`. The `ControlPlane` trait abstracts registry access: impl'd in-process over `Registry` now (`RegistryControlPlane`), over a gRPC client in M6. Engine calls (blocking DuckDB) run via `spawn_blocking`. See spec §3/§6/§8/§10 + spike findings (rmcp `StreamableHttpService`, `#[tool_router]`, `structuredContent` must be set explicitly, progress via `Peer::notify_progress`).

**Tech Stack:** Rust, `rmcp` (server + client, `transport-streamable-http-server`/client), `axum` 0.8, `async-trait`, `tokio`, `serde`/`serde_json`, `schemars`.

---

## Conventions
- TDD; `cargo fmt` + `cargo clippy -p <crate> --all-targets -- -D warnings` clean before commit. Commit trailer as in prior milestones. `git add -A`.
- Builds are fast (heavy deps cached). rmcp/axum/schemars compile on first use (~1–2 min once).

---

## Prereq: extend Registry + ControlPlane needs

### Task 5.0: Registry — add `list_ponds` + `pond_info`
**Files:** modify `crates/latiq-control-plane/src/registry.rs`.

- [ ] **Step 1:** Add a `PondInfo`-bearing query. Add to `Registry`:
```rust
pub fn list_ponds(&self) -> Result<Vec<PondRow>, ControlPlaneError> {
    let c = self.lock();
    let mut stmt = c.prepare(
        "SELECT pond_id, name, owner_identity, node_id FROM ponds ORDER BY created_at")?;
    let rows = stmt.query_map([], |r| Ok(PondRow {
        pond_id: r.get(0)?, name: r.get(1)?, owner_identity: r.get(2)?, node_id: r.get(3)?,
    }))?;
    Ok(rows.collect::<Result<_, _>>()?)
}

pub fn pond_info(&self, pond_ref: &str) -> Result<(PondRow, String, String), ControlPlaneError> {
    // returns (PondRow, created_at, policy_json)
    let c = self.lock();
    c.query_row(
        "SELECT pond_id, name, owner_identity, node_id, created_at::VARCHAR, policy_json
         FROM ponds WHERE pond_id=? OR name=? LIMIT 1",
        duckdb::params![pond_ref, pond_ref],
        |r| Ok((
            PondRow { pond_id: r.get(0)?, name: r.get(1)?, owner_identity: r.get(2)?, node_id: r.get(3)? },
            r.get::<_, String>(4)?, r.get::<_, String>(5)?,
        )),
    ).map_err(|_| ControlPlaneError::PondNotFound(pond_ref.to_string()))
}
```
- [ ] **Step 2:** Add tests `list_ponds_returns_all` + `pond_info_resolves_by_name`. Run `cargo test -p latiq-control-plane registry::` → 5 passed.
- [ ] **Step 3:** Commit `feat(control-plane): registry list_ponds + pond_info`.

---

## latiq-agent-core

### Task 5.1: agent-core — types, error, ControlPlane trait
**Files:** `crates/latiq-agent-core/Cargo.toml`; `src/{lib,error,control,types}.rs`.

- [ ] **Step 1: Cargo.toml**
```toml
[dependencies]
latiq-common = { path = "../latiq-common" }
latiq-storage = { path = "../latiq-storage" }
latiq-engine = { path = "../latiq-engine" }
async-trait = "1"
serde = { workspace = true }
serde_json = { workspace = true }
tokio = { workspace = true }
thiserror = { workspace = true }
```
Add `async-trait = "1"` to workspace deps.

- [ ] **Step 2: `src/error.rs`** — `AgentError` carrying an `ErrorEnvelope` (from latiq-common). Provide constructors that build the envelope (kind/message/suggest/see). Map `latiq_engine::EngineError` → `AgentError` (ReadOnlyViolation→read_only_violation, ReservedSchemaWrite→write_to_reserved_schema, Cancelled→query_cancelled, Timeout→query_timeout, Parse→parse_error, Engine→internal). Provide `pub fn envelope(&self) -> &ErrorEnvelope`.

- [ ] **Step 3: `src/types.rs`** — neutral op result/info structs:
```rust
use serde::{Deserialize, Serialize};
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PondInfo { pub pond_id: String, pub name: String, pub owner: String,
    pub created_at: String, pub policy_json: String }
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AllocateResult { pub pond_id: String, pub pond_name: String }
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuditRecord { pub agent_identity: String, pub verified: bool, pub operation: String,
    pub pond_id: Option<String>, pub request_summary: Option<String>, pub duration_ms: u64 }
```

- [ ] **Step 4: `src/control.rs`** — the trait agent-core depends on:
```rust
use crate::error::AgentError;
use crate::types::{AuditRecord, PondInfo};
#[async_trait::async_trait]
pub trait ControlPlane: Send + Sync {
    async fn create_pond(&self, name: Option<String>, owner: &str, policy_json: &str) -> Result<PondInfo, AgentError>;
    async fn resolve_pond(&self, pond_ref: &str) -> Result<String, AgentError>; // -> pond_id
    async fn list_ponds(&self) -> Result<Vec<PondInfo>, AgentError>;
    async fn pond_info(&self, pond_ref: &str) -> Result<PondInfo, AgentError>;
    async fn drop_pond(&self, pond_id: &str) -> Result<(), AgentError>;
    async fn record_audit(&self, rec: AuditRecord);
}
```
- [ ] **Step 5: `lib.rs`** re-exports. Build + commit `feat(agent-core): error envelope, neutral types, ControlPlane trait`.

### Task 5.2: agent-core — in-flight/abort registry
**Files:** `src/inflight.rs`.
- [ ] In-flight registry mapping a generated op-id → `AbortToken`. `register() -> (OpId, AbortToken)`, `cancel(op_id)`, `complete(op_id)`, `cancel_for_pond(pond_id)` (drop_pond authority — store pond_id alongside). `Arc<Mutex<HashMap<..>>>`. Unit test: register → cancel marks token cancelled; complete removes. Commit `feat(agent-core): in-flight/abort registry`.

### Task 5.3: agent-core — AgentOps
**Files:** `src/ops.rs`.
- [ ] `AgentConfig { inline_row_cap: usize (default 10_000) }`. `AgentOps { control, storage, engine, inflight, config }` (`Arc<dyn ...>`). Methods (async):
  - `allocate_pond(identity, name, policy_json)`: control.create_pond → storage.create_pond(PondId::parse(pond_id)) → spawn_blocking engine.init_pond(loc); on engine failure → control.drop_pond (compensate) + error. Returns `AllocateResult`.
  - `describe_pond(identity, pond_ref)`: control.pond_info + spawn_blocking engine.describe_schema(loc). Returns `{ pond_info, schema_summary }`.
  - `list_ponds(identity)`: control.list_ponds.
  - `drop_pond(identity, pond_ref, confirm)`: resolve → inflight.cancel_for_pond → control.drop_pond → storage.drop_pond.
  - `read_query(identity, pond_ref, sql, abort)`: resolve → storage.pond_location → register inflight → spawn_blocking engine.read_query(loc, sql, abort) → enforce inline_row_cap (rows > cap → ResultCapExceeded) → fill `_meta` (rows, duration, snapshot via result) → audit. complete inflight.
  - `write_query` / `explain_query`: analogous (explain has no abort).
  - For each op, build an `AuditRecord` (redacted SQL shape: replace literals — for M5, store the operation + a coarse summary; full redaction can be minimal) and call control.record_audit (fire-and-forget; ignore errors).
- [ ] Tests using a `RegistryControlPlane` (see 5.4) + `DuckEngine` + `TempFs`: allocate → write → read (rows + attribution) → describe (schema has table) → list (1 pond) → drop. Plus inline-cap test (insert > cap rows, read errors). Commit `feat(agent-core): AgentOps (allocate/describe/list/drop/read/write/explain)`.

### Task 5.4: agent-core test support — RegistryControlPlane
**Files:** `src/registry_control.rs` (gated behind `#[cfg(any(test, feature = "registry-control"))]` OR always-on with a `latiq-control-plane` dep).
- [ ] Implement `ControlPlane` over `latiq_control_plane::Registry` (in-process). This is the impl used in M5 tests and `latiq dev`-style single-process; M6 adds the gRPC-client impl. Add `latiq-control-plane = { path = "../latiq-control-plane" }` dep. `create_pond` maps to registry.create_pond; `resolve_pond`/`pond_info` via registry.pond_info; `list_ponds` via registry.list_ponds; `record_audit` via registry.record_audit (map errors to AgentError). Commit `feat(agent-core): RegistryControlPlane (in-process ControlPlane impl)`.

---

## latiq-mcp

### Task 5.5: latiq-mcp — rmcp server with the 7 tools
**Files:** `crates/latiq-mcp/Cargo.toml`; `src/{lib,server,encode,identity}.rs`. Reference `spike/src/bin/probe_c.rs` for the rmcp API.
- [ ] **Cargo.toml**: `rmcp` (server + transport-streamable-http-server), `axum` 0.8, `schemars` 0.8, `serde`, `serde_json`, `tokio`, `latiq-agent-core`, `latiq-common`.
- [ ] **`encode.rs`**: build a `CallToolResult` with BOTH `content: [text(json_string)]` and `structured_content: Some(json)` (spike: structuredContent must be set explicitly). Helper `ok_result(value)` and `err_result(envelope)` (isError=true). The result object shape per spec §8: `{ rows, columns, statement, status, _meta }` for queries; lifecycle tools return their natural JSON.
- [ ] **`identity.rs`**: extract `Identity::claimed` from the `X-Latiq-Agent-Id` header (rmcp exposes request parts/extensions — read from the http request; if not reachable in a tool handler, accept an `agent_id` tool arg fallback for M5 and note it). Relaxed: default anonymous.
- [ ] **`server.rs`**: `LatiqServer { ops: Arc<AgentOps> }` with `#[tool_router]`; 7 tools:
  - `allocate_pond { name?, tags? }`, `describe_pond { pond }`, `list_ponds {}`, `drop_pond { pond, confirm? }` — JSON results.
  - `read_query { pond, sql }`, `write_query { pond, sql }` — create an `AbortToken`, call ops, encode; (SSE/progress optional in M5 — wire `notify_progress` if a progress token is present, else plain).
  - `explain_query { pond, sql }`.
  Each tool: extract identity, call the matching `AgentOps` method, map `AgentError` → `err_result(envelope)`. Tool descriptions = mini-tutorials (concise). `serve_mcp(addr, ops)` builds `StreamableHttpService` + axum router `nest_service("/mcp", ...)` and serves.
- [ ] Build. Commit `feat(mcp): rmcp Streamable-HTTP server exposing the 7 agent tools`.

### Task 5.6: latiq-client — rmcp client wrapper
**Files:** `crates/latiq-client/Cargo.toml`; `src/lib.rs`.
- [ ] `rmcp` client features. `LatiqClient::connect(endpoint, agent_id)` → does the MCP handshake (sets the `X-Latiq-Agent-Id` header on the transport). Methods mirroring the tools: `allocate_pond`, `list_ponds`, `describe_pond`, `drop_pond`, `query` (read), `write`, `explain` — each calls the tool and returns the decoded `structuredContent` (serde_json::Value) or a structured error. Commit `feat(client): rmcp MCP client wrapper for the agent surface`.

### Task 5.7: M5 integration test — server + client round-trip
**Files:** `crates/latiq-mcp/tests/mcp_e2e.rs` (or a new `tests` crate).
- [ ] Start `serve_mcp` on an ephemeral port over an `AgentOps` built from `RegistryControlPlane` (in-memory Registry, a node pre-registered) + `DuckEngine` + `TempFs`. Use `LatiqClient` to: allocate a pond, write rows, read them back (assert rows + attribution), describe (schema has table), list (1 pond), drop. Assert structured-error path on a bad pond name. Commit `test(mcp): end-to-end server+client agent loop`.
- [ ] M5 gates: `cargo test --workspace` green, clippy clean, fmt.

---

## Self-Review
- **Spec coverage:** §10 7 tools ✅; §8 dual encoding + relaxed identity + error envelope ✅; §6 abort registry + cancel_for_pond (drop authority) ✅; §10a `latiq-client` ✅. SSE/progress is wired opportunistically in M5 (full SSE polish can follow); inline cap enforced. `latiq://` resources are minimal/deferred to M6/M7 polish (the `see` links may not all resolve yet — acceptable, noted).
- **Open decisions recorded:** identity extraction from HTTP headers inside rmcp tool handlers — if the rmcp version doesn't expose request headers to handlers, M5 falls back to an `agent_id` tool argument and M6 revisits header extraction at the axum layer. Control-plane access via `ControlPlane` trait (in-process now, gRPC in M6).
- **Type consistency:** `ControlPlane`, `AgentOps`, `PondInfo`, `AllocateResult`, `AgentError`/`ErrorEnvelope`, `LatiqClient` consistent.

## Next
M6: gRPC `ControlPlane` impl + pond-node wiring + `latiq` binary (server roles + admin CLI + agent client CLI) + `dev.sh` + run-everything; then M7 ingestion + integration + `docs/usage.md`.
