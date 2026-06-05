# Latiq Slice 0+ — Design Spec

**Date:** 2026-06-04
**Status:** Approved (brainstormed end-to-end; ready for implementation planning)
**Scope:** The first buildable, demoable vertical slice of Latiq M1. Later slices (federation/catalogs, multi-node topology, OIDC/rate-limiting, OpenTelemetry) get their own spec→plan cycles.

References: `docs/product_spec.md`, `docs/m1_design.md`. This spec **supersedes** the M1 design doc wherever they differ (noted inline).

---

## 1. What Slice 0+ is

A two-process Latiq deployment where agents, over MCP-over-HTTP, allocate ponds, write and read SQL against DuckLake storage, ingest public external files directly into a pond, and see their writes attributed — with an admin CLI over a separate gRPC surface. No federation, no OIDC verification, no multi-node proxying.

### In scope
- MCP-over-HTTP agent surface with **7 tools**: `allocate_pond`, `describe_pond`, `list_ponds`, `drop_pond`, `read_query`, `write_query`, `explain_query`.
- **DuckLake storage per pond**, catalog backed by **DuckDB**; one DuckDB instance per pond.
- **Implicit query-by-URI ingestion** via a fixed extension allowlist (`parquet`, `csv`, `json`, `httpfs`) — read public/anonymous files and `INSERT` / `CREATE TABLE AS SELECT` directly into a pond. No credentials on this path.
- **Reserved `_latiq` schema** — read-only views, sourced purely from DuckLake/DuckDB catalog.
- **Native write attribution** via DuckLake `set_commit_message`.
- **Minimal topology split**: separate `control-plane` and `pond-node` processes (single pond node), both metadata stores backed by **DuckDB**. `dev.sh` starts both and prints endpoints.
- **Admin CLI + Admin gRPC** (no federation): `node list/describe`, `policy show/set`, `audit tail/search`.
- **Relaxed claimed identity** (any agent id accepted, `verified:false`), audit log of every operation.
- Engine-agnostic **query cancellation + prompt resource release**, graceful shutdown.

### Out of scope (later slices)
External catalogs + credential store + federation; Flight SQL proxy hops; OIDC verification + identity allow-listing; rate limiting; OpenTelemetry; multi-node Docker Compose; MCP Prompts; the full `latiq://` recipe set (only a minimal subset ships here, see §9). Governance *features* (retention/RBAC/erasure enforcement) — only the *seam* ships.

---

## 2. Core principles (defended)

1. **The agent is the customer.** Agent features live on the MCP surface; operator features on the Admin/CLI surface. They never overlap.
2. **Hard separation of MCP vs Admin** — enforced structurally: `latiq-pond-node` (MCP) does not depend on admin code; admin lives in the control-plane + binary crates.
3. **Pure DuckLake — nothing on top.** Latiq adds **no parallel store** that shadows what DuckLake tracks (data, snapshots, attribution). Anything relied upon must be in the DuckLake **spec** (honored by both `duckdb`+`ducklake` and `datafusion-ducklake`). *Governance/policy metadata is a different plane (the control-plane registry) and is not "on top of DuckLake."*
4. **One pond, one node.** Cross-pond joins are impossible by construction (a pond's DuckDB instance only attaches its own catalog).
5. **Make it boring.** Predictable behavior, clear errors, good defaults.
6. **Three swap seams designed from day one** (§3): agent API surface, query engine, pond storage.

---

## 3. Architecture — ports & adapters (hexagonal)

Three trait seams, each with one live adapter now and room for more:

| Seam | Trait | Live adapter (now) | Future adapters |
|---|---|---|---|
| Agent API surface (inbound) | agent-core ops | MCP-over-HTTP | A2A, … |
| Query engine (outbound) | `QueryEngine` (targets DuckLake format) | DuckDB + `ducklake` | DataFusion (`datafusion-ducklake`) |
| Pond storage (outbound) | `PondStorage` | LocalFs + InMemory/Temp (tests) | S3 / MinIO / RustFS |

**Protocol-neutral core.** Agent operations (`allocate_pond`, …, `explain_query`) live in `latiq-agent-core` and return rich structured results (rows + `_meta` + warnings + structured errors). MCP is one inbound adapter onto this core; A2A would be another. The **in-flight/abort registry lives in the core** (cancellation is protocol-agnostic).

### Crate layout (Cargo workspace)
```
crates/
  latiq                 # binary: clap dispatch (control-plane | pond-node) + admin CLI subcommands
  latiq-common          # Identity, ErrorEnvelope, Location, IDs (UUIDv4), config, neutral Result/_meta types
  latiq-proto           # tonic gRPC: Control service + Admin service (build.rs)
  latiq-agent-core      # protocol-neutral agent ops + in-flight/abort registry + result/_meta/warnings assembly
  latiq-mcp             # MCP-over-HTTP surface adapter (rmcp) → agent-core
  latiq-engine          # QueryEngine trait + DuckLake-format contract + BatchSink + abort layer
  latiq-engine-duckdb   # DuckDB + ducklake adapter (per-pond instance manager)
  latiq-storage         # PondStorage trait; LocalFs + InMemory/Temp backends
  latiq-pond-node       # wires surfaces + agent-core + engine(duckdb) + storage(localfs) + Control-gRPC client
  latiq-control-plane   # registry/routing/audit (DuckDB) + Control gRPC + Admin gRPC + migrations
```

### Tech stack
`tokio`, `rmcp` (MCP Streamable-HTTP), `tonic` (gRPC), `duckdb-rs` (DuckDB + `ducklake` extension), `clap` (CLI). DuckLake spec **v1.0** (2026-04-13).

---

## 4. Topology & process wiring

Two processes, real gRPC between them. **No `latiq dev` single-process mode** — `dev.sh` launches `latiq control-plane` + `latiq pond-node` over loopback gRPC and prints the MCP HTTP + gRPC endpoints. Fast integration tests spin both servers up as tokio tasks in one test process (a test harness, not a shipped mode).

- **Control plane** — sole writer to its DuckDB metadata file (pond nodes reach it only via gRPC → DuckDB single-writer happy path). Two gRPC surfaces:
  - **Control gRPC** (pond-node → control-plane): `register_node`, `heartbeat`, `create_pond_assignment`, `get_pond_location`, `drop_pond_assignment`, `record_audit`.
  - **Admin gRPC** (CLI → control-plane): `list_nodes`, `describe_node`, `policy_get`, `policy_set`, `audit_tail`, `audit_search`.
- **Pond node** — owns ponds on local disk, terminates MCP. No proxy hops (single node).

**Allocate consistency rule:** control plane reserves name+id authoritatively (enforces name uniqueness) → pond node creates the physical pond (storage + engine init + `_latiq` views) → on physical-create failure the node calls `drop_pond_assignment` to compensate. Registry is the source of truth for *existence*; disk follows.

**Audit write path:** async/non-blocking. Node enqueues entries to a bounded in-memory channel; a background task ships them via `record_audit`; on overflow, drop-with-counter (never block the query path).

### Control-plane DuckDB schema (with migrations framework from day 1)
- `nodes(node_id, mcp_endpoint, internal_endpoint, capacity, last_heartbeat, state)`
- `ponds(pond_id UUID, name UNIQUE, owner_identity, node_id, created_at, policy JSON, tags, state)`
- `policy(key, value)` — default-pond-lifetime, query-timeout (rate-limit deferred)
- `audit_log(audit_id, ts, agent_identity, identity_verified, operation, pond_id, request_summary JSON, result_summary JSON, duration_ms)` — SQL **shape** recorded, literals redacted to `?`, results never logged.

The registry is the designated **governance plane** — it evolves (via migrations) to hold policy/RBAC/retention/erasure policy later. Division of labor: **DuckLake provides enforcement mechanisms** (snapshot expiry, vacuum/cleanup, encryption-at-rest); **Latiq's registry owns policy** (what retention, who, what's erasable) and drives DuckLake operations.

---

## 5. Storage & engine model

- **One DuckDB instance per pond**: each pond is a DuckDB handle that attaches **only** its own DuckLake catalog, with its own connection pool and `memory_limit`. `drop_pond` closes the instance. Per-pond memory caps trivial; cross-pond joins structurally impossible.
- **Per-pond on-disk layout** (LocalFs):
  ```
  <pond-id>/
    catalog.duckdb     # DuckLake catalog DB (DuckDB-backed)
    data/              # Parquet data files (→ s3:// when storage scales)
  ```
  No Latiq side-store (principle 3).
- **`PondStorage` descriptor** provides, per pond: the **catalog connection string** (`ducklake:duckdb:.../catalog.duckdb` now; `ducklake:postgres:…` later) and the **DATA_PATH** (local dir now; `s3://…` + secrets later). DuckLake already decouples these, so `PondStorage` is a *location + credential provider*, not a storage reimplementation. The InMemory/Temp backend proves the seam and gives hermetic tests.
- **Write path wraps in an explicit transaction** to attach attribution:
  ```sql
  BEGIN;
    <user write SQL>;
    CALL pond.set_commit_message(<identity>, 'write_query', extra_info => '<json>');
  COMMIT;
  ```
  The transaction is the unit the abort layer rolls back.
- **Concurrency:** rely on DuckLake OCC + snapshot isolation + auto-retry (5 default). Conflicts surface in `_meta`.

---

## 6. Cancellation, abort, graceful shutdown

A first-class, **engine-agnostic abort layer** in `latiq-agent-core`:

- **In-flight registry** keyed by internal op-id (each surface maps its own request-id onto it) → holds an `AbortToken`.
- `QueryEngine::execute(sql, abort_token) -> BatchStream`. **Contract:** after `abort` fires, engine-side memory/handles for that query are released within a **bounded window**.
  - DuckDB adapter: `Connection::interrupt()` from the control thread → blocking exec returns abort error → drop statement/result → **discard the connection** (don't return to pool; re-create lazily). `memory_limit` bounds reclaimed budget.
  - DataFusion adapter (future): drop the stream/task; `Drop` tears down operator + spill state deterministically.
- **All cancel sources funnel into one `abort(op_id)`:** MCP `notifications/cancelled`, client disconnect (detected via SSE write failure), `drop_pond` (authoritative), query timeout (default 30s), SIGTERM.
- **Graceful shutdown sequence:** stop accepting new MCP calls (503) → `abort` all in-flight + await within drain timeout → `CHECKPOINT`/close DuckLake instances → deregister node from control plane → exit.
- **Cancellation test (engine-agnostic):** start a heavy query, abort, assert connection count returns to baseline and a fresh query on the same pond succeeds promptly.

---

## 7. MCP transport

- **SSE (`text/event-stream`) for `read_query` / `write_query`** — emits `notifications/progress` during execution (keepalive + progress + prompt disconnect→abort detection), then the single bounded `CallToolResult`.
- **Single-JSON (`application/json`) for `explain_query` + lifecycle + admin tools** — sub-millisecond, no value in SSE.
- The query result is **bounded** by the inline cap (default 10k rows / 1MB). Streaming is internal/future-SDK; the agent boundary is one bounded MCP result. (This supersedes the M1 doc's "JSON-Lines over HTTP chunked transfer" wording.)
- **MCP cancel routing:** a `notifications/cancelled` POST (possibly on a different connection) is matched by request-id to the in-flight op and triggers `abort`.

---

## 8. Agent contract

Conventions adopted from `trelisdb` (the user's existing MCP database) for cross-stack consistency.

- **Identity (relaxed):** capture claimed identity best-effort from standard sources (`X-Latiq-Agent-Id` header now; opportunistically an *unverified* bearer-token `sub` if present); default to `anonymous` when absent; **never reject**; always `verified:false`. This string flows into DuckLake `set_commit_message` and the audit log. Real OIDC verification + allow-listing → M2.
- **Error model — `ErrorEnvelope`:**
  ```rust
  struct ErrorEnvelope {
    kind: String,               // snake_case, closed taxonomy
    message: String,            // one sentence; no "suggest" text here
    location: Option<Location>, // {line,column,byte} for SQL parse errors
    suggest: String,            // copy-paste-ready corrected example — the retry path
    see: String,                // latiq:// resource URI + anchor — the learning path
  }
  ```
  Philosophy: *80% recoverable from `suggest` alone; 20% fetch `see`.* Latiq `kind` taxonomy (adapt as needed): `pond_not_found`, `name_conflict`, `parse_error`, `invalid_value`, `missing_argument`, `write_to_reserved_schema`, `result_cap_exceeded`, `read_only_violation` (write SQL via read_query), `uri_not_allowed`, `query_timeout`, `query_cancelled`, `storage`, `internal`.
  **Return path:** tool failures → `CallToolResult { isError: true, structuredContent: <envelope>, content: [text mirror] }`. JSON-RPC errors reserved for protocol-level (unknown tool / bad params).
- **Result encoding (dual, trelisdb-aligned):** a single JSON result object placed in **both** `content[0].text` (stringified) and `structuredContent`:
  ```jsonc
  { "rows": [...], "columns": [...], "statement": "read_query", "status": "ok",
    "_meta": { "rows": 842, "rows_affected": 0, "snapshot_id": 12, "duration_ms": 31,
               "bytes_scanned": 1048576, "tables_touched": ["events"],
               "warnings": [...], "hint": "..." } }
  ```
  **Latiq divergence from trelisdb:** the `_meta` envelope lives *inside* the result (trelisdb keeps metrics external) — "every response carries forward signal" is core to Latiq's thrift/self-correct UX. Inline cap exceeded → `result_cap_exceeded` error advising narrow/aggregate/`CREATE TABLE AS SELECT`.
- **Pond addressing:** tools accept `pond` as UUID or human name; control-plane registry resolves and enforces uniqueness + validated charset.

---

## 9. The `_latiq` schema (pure DuckLake-derived)

Read-only views created per pond at allocation; never backed by a Latiq store:
- `snapshots` → DuckLake `snapshots()` (snapshot_id, snapshot_time, schema_version, author, commit_message, commit_extra_info).
- `attribution` → `ducklake_snapshot_changes` (snapshot_id, author, commit_message, extra_info) — "who wrote what" natively.
- `tables_summary` → DuckLake/DuckDB table catalog (name, row_count, comment, last_modified).
- `sources` → DuckDB attached-DB list — **zero rows in Slice 0+** (no federation).

`_latiq.*` is read-only: objects are views *and* the node rejects any parsed SQL whose write target is `_latiq` (`write_to_reserved_schema`).

**`pond_info` is NOT in `_latiq`** — it's operational metadata owned by the registry; `describe_pond` serves it.

**Minimal `latiq://` resources** ship so `see` links resolve: `latiq://guidance` + a small `latiq://troubleshooting/*` set keyed to the error taxonomy. The full recipe set is a later slice.

---

## 10. The 7 tools (annotations + behavior)

| Tool | Annotations | Transport | Notes |
|---|---|---|---|
| `allocate_pond` | not read-only, not destructive, not idempotent | JSON | name optional (CP generates); creates `_latiq` views |
| `describe_pond` | read-only, idempotent | JSON | serves `pond_info` from registry + schema summary |
| `list_ponds` | read-only, idempotent | JSON | all ponds (single global scope) |
| `drop_pond` | not read-only, destructive, idempotent | JSON | authoritative — aborts in-flight queries on the pond |
| `read_query` | read-only, idempotent | **SSE** | SELECT / read-only metadata only; else → `read_only_violation` |
| `write_query` | not read-only, destructive, not idempotent | **SSE** | INSERT/UPDATE/DELETE/DDL/CTAS; txn-wrapped + attribution |
| `explain_query` | read-only, idempotent | JSON | wraps DuckDB `EXPLAIN` → estimated rows/bytes/duration, scan ops, warnings, suggestions, raw plan |

Tool descriptions are mini-tutorials (doc §4a principle 1): what / when-vs-alternatives / concrete SQL example / do-don't pair / `see` cross-reference.

**Implicit query-by-URI:** `read_query`/`write_query` accept DuckDB file-source SQL (`'s3://…parquet'`, `read_csv(…)`, `read_json(…)`) validated against the extension allowlist (`httpfs`,`parquet`,`csv`,`json`). **Public/anonymous only** — credentialed DB sources → `uri_not_allowed` (deferred to federation slice).

---

## 11. Testing strategy

- **Unit tests per crate**; the engine seam validated against both `LocalFs` and `InMemory` storage.
- **Cancellation/resource-release test** (§6) — engine-agnostic.
- **Two-process integration harness** (servers as tokio tasks over loopback gRPC).
- **Success-criterion test:** N agents writing concurrently to one pond → consistent state + correct per-identity attribution + conflict-and-retry verified (doc §14.3).
- **End-to-end:** allocate → ingest public file → cross-table query → attribution visible in `_latiq`; the "30-min demo" walkthrough script + quickstart doc.

---

## 12. Build order (milestones)

De-risk-first, then bottom-up through the seams, integration last.

- **M1 — Spike (throwaway, de-risk gate).** Prove: rmcp Streamable-HTTP boots; `duckdb-rs` loads `ducklake` + ATTACH + CRUD round-trip; `set_commit_message` author shows in `snapshots()`; SSE response + progress notification; `Connection::interrupt()` aborts a running query. Deliverable = confirmed APIs + surprises.
- **M2 — Workspace + kernel.** Workspace + 10 crates stubbed; `latiq-common`; `latiq-proto` (Control+Admin); binary clap skeleton; CI green.
- **M3 — Outbound seams.** `latiq-storage` (LocalFs + InMemory); `latiq-engine` (trait + DuckLake contract + BatchSink + abort layer); `latiq-engine-duckdb` (instance mgr, extension allowlist, txn writes + attribution, interrupt, `_latiq` views, explain). Cancellation test.
- **M4 — Control plane.** DuckDB registry + migrations; Control gRPC + Admin gRPC; async audit ingestion.
- **M5 — Inbound surface.** `latiq-agent-core` (neutral ops + abort registry + result/`_meta`/warnings/errors); `latiq-mcp` (rmcp server, 7 tools + descriptions + annotations, relaxed identity, SSE for queries, dual encoding, cancel→abort, minimal `latiq://` resources).
- **M6 — Pond node + processes.** Wire everything; node registration/heartbeat; allocate consistency; graceful shutdown; `latiq` binary; `dev.sh`; YAML configs.
- **M7 — Integration & success criteria.** Query-by-URI ingestion; integration harness; concurrent-multi-agent correctness + attribution test; cancellation E2E; demo walkthrough + quickstart docs.

---

## 13. Risks & open items

- **DataFusion engine parity (future):** `datafusion-ducklake` is pre-1.0 — read + basic INSERT + snapshots only; **no UPDATE/DELETE, no time-travel, cannot *set* commit author.** The engine swap is real work and write-attribution parity is a known gap to design around later. The `QueryEngine` seam makes it *possible and bounded*, not small.
- **Erasure bug to track:** DuckLake `cleanup_old_files` reportedly broken with a Postgres catalog (issue #586) — relevant only when catalog moves to Postgres (scaling slice).
- **Spike-confirmed assumptions:** exact `rmcp` progress/notification API; `duckdb-rs` extension-load ergonomics; `set_commit_message` transactional behavior. M1 resolves these before the plan's foundation is committed.
- **Inline cap / timeouts / pool sizing / progress cadence** are tunable defaults (doc §15) — start with doc values, adjust from observed behavior.
