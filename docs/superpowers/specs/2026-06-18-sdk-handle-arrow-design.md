# SDK handle API + Arrow streaming reads + pond description — design

**Date:** 2026-06-18
**Status:** Approved (shape) — pending written review.
**Builds on:** `2026-06-12-arrow-streaming-design.md` (the `ReadArrow`/`Stream`
service already streams Arrow IPC and is **already served on the node's
`data_addr`** alongside the Data service). This spec **reuses that stream as the
SDK's read transport** (the SDK collects batches into a `pyarrow.Table`
client-side), reshapes the SDK into a **handle-centric** API, and threads a pond
**`description`** end-to-end.

**Goal:** Make the Latiq SDK feel like an SDK — the pond is an object, SQL is the
verb, results are Arrow — not a thin transcript of the CLI.

**Key simplification (vs. an earlier draft):** the SDK does **not** need a new
unary Arrow-IPC response, nor any CLI/forward/proto change. `ReadArrow` already
exists, is uncapped (streaming), already does raw-IPC passthrough on the forward,
and is served on the same endpoint the SDK already dials. SDK reads call
`ReadArrow` and `.read_all()` into a Table; SDK writes stay on unary `WriteQuery`.
The 10k inline cap stays on the **CLI/MCP** unary JSON edges (unchanged).

## Audiences / invariants (unchanged)
SDK = CLI/SDK audience → **gRPC, never MCP** (invariants 1 & 8). MCP stays JSON.
The CLI does **not** depend on `latiq-sdk` (confirmed) — changing SDK methods has
zero CLI blast radius. Encoding stays out of `latiq-agent-core` (invariant 5):
IPC serialization lives in the pond-node adapter, next to `wire.rs`.

---

## Surface (the agreed Python API)

```python
db = latiq.connect(server="local", root=None)          # or server="grpc://host:port"

work = db.create_pond(name="work", tier="medium",
                      description="raw clickstream events, 2024 H1")   # → Pond handle
work = db.get_pond(pond="work")        # → handle; metadata pre-fetched as attributes
db.list_ponds()                        # → {"work": {id, tier, node, description, …}, …}
db.drop_pond(pond="work", confirm=True)

# the handle: metadata attributes + SQL (SQL is the focus)
work.name; work.id; work.tier; work.node; work.description
tbl = work.query(sql="SELECT * FROM t")    # → pyarrow.Table
work.describe()                            # structured table/column schema (≠ description)
```

**Rules locked with the user:**
1. All params keyword-capable (`name=`, `tier=`, `pond=`, `sql=`, `confirm=`).
2. `list_ponds()` returns a **dict keyed by pond name**, value = info object incl. `description`.
3. `create_pond` takes `tier` + `description` (agent-discovery metadata). The **CLI**
   gains `--description` too; `pond list`/`describe` show it.
4. `get_pond(pond=)` returns a handle with metadata **pre-fetched as attributes**
   (one round-trip) plus `query`. (Renamed from `pond()`.)
5. `drop_pond(pond=, confirm=True)` stays on `db`.
6. No `ponds()` iterator.
7. `query()` returns a **`pyarrow.Table`** (Arrow IPC on the wire). Streaming
   (unbounded `RecordBatchReader`) is a later slice over the existing `ReadArrow`.

Removed from the public surface: `db.query(name, …)`, `db.describe_pond(name)`,
`db.list_ponds()`-as-list. Everything data-related is reached through a handle;
`db` only mints/derives handles and drops ponds.

---

## Slice 1 — Arrow IPC on the unary Data edge (point 7 / "B")

**Proto** (`crates/latiq-proto/proto/latiq/v1/data.proto`): `ReadQuery`/`WriteQuery`
return a new message instead of `JsonResponse`:
```proto
message QueryResult {
  bytes  arrow_ipc = 1;   // Arrow IPC stream: schema + batches, capped by inline_row_cap
  string meta_json = 2;   // {statement, status, snapshot_id?, row_count, truncated}
}
```
`ExplainQuery`/`DescribePond`/dataset/catalog stay on `JsonResponse` (plans/schema,
not tables). `meta_json` preserves what the CLI needs today — the **write
`snapshot_id`** (attribution) and statement/status — which raw Arrow can't carry.

**pond-node** (`data_service.rs` + a new collector next to `wire.rs`): the read
handler drives the existing `AgentOps` Arrow funnel (`read_arrow`), collects
batches up to the **10k `inline_row_cap`** (unchanged), encodes them with
`arrow::ipc::writer::StreamWriter` (schema once + batches) into `arrow_ipc`, and
emits `meta_json`. Writes return `meta_json` (snapshot/status) + a small/empty
`arrow_ipc` (count row if the engine yields one). The **node-to-node read forward**
re-hydration (`query_result_from_json`) moves to the IPC shape (or reuses the
already-Arrow `ReadArrow` forward).

**CLI** (`main.rs` `run_query` / `print_table_result` / `print_json_result`):
decode `arrow_ipc` → render the table; `--format json` decodes IPC → JSON for
display (CLI already deps `arrow`). Write detection reads `meta_json.snapshot_id`.

**Out of scope here:** MCP (`latiq-mcp` read handler stays JSON), streaming writes.

## Slice 2 — pond `description` metadata (end to end)

- **Proto:** add `description` to `CreatePondAssignmentRequest`, `PondInfoMsg`
  (control), and `PondSummary` (admin `pond_list`).
- **Registry:** append migration `ALTER TABLE ponds ADD COLUMN description VARCHAR
  DEFAULT '';` to `MIGRATIONS`; add `description` to `PondRow` + the create/list/
  info SQL.
- **Control plane:** thread `description` through `create_pond_assignment`,
  `list_ponds`, `get_pond_info`; **admin** `pond_list` → `PondSummary.description`.
- **agent-core:** add `description` to the neutral `PondInfo` so `describe_pond`
  surfaces it.
- **CLI:** `pond create --description`; show it in `pond list` (table+json) and
  `pond describe`.

## Routing — front door + forwarding (NOT node-direct)

**The current SDK `data_for` is node-direct and is broken behind a k8s LB:**
`get_pond_location` returns the owner **pod's internal address**, unroutable from
an external client. The data/stream path must hit a **front door** and let the
**greeter** forward by pond (the same model the CLI's `LATIQ_QUERY_GATEWAY` and
MCP agents already use). The greeter forwarding for `ReadArrow` and all Data ops
already exists (`StreamService::read_arrow` → `AgentOps::read_arrow` →
`Forwarder`; zero-touch raw-IPC passthrough).

- **`server="local"` (embedded):** the single in-process node *is* the front door
  — dial it directly; no LB, no `get_pond_location`.
- **`server="grpc://lb:port"` (remote):** dial that front door for Control/Admin
  **and** Data/Stream; the greeter forwards by pond. **Remove the
  `get_pond_location`→node-direct hop for queries/describe/drop.**
- **`connect(query_gateway=…)`** optional override for deployments where
  Data/Stream is a *separate* LB address from Control/Admin (control-plane and
  pond-nodes are different upstreams). Defaults to `server` when unified.

`describe_pond`/`drop_pond` are Data ops too — they ride the same front door +
forwarding, not node-direct.

## Slice 3 — SDK handle redesign (depends on 1 & 2)

- **`latiq-sdk` (Rust):** rename data struct `Pond`→`PondInfo` (carries
  `description`); add a `Pond<'a>` handle (borrows `&Latiq`, holds cached
  `PondInfo`) with `query(sql) -> Vec<RecordBatch>`/Arrow, `describe()`, and
  metadata accessors. `create_pond`/`get_pond` return the handle; `list_ponds()`
  returns a map keyed by name; `drop_pond` on `Latiq`. **Replace node-direct
  `data_for` with front-door routing** (above): reads call `Stream::ReadArrow` on
  the front door + collect IPC → batches; writes call unary `WriteQuery`.
- **`sdk/python` (PyO3):** `create_pond`/`get_pond` return `PyPond` (with
  `name/id/tier/node/description` attributes + `query`/`describe`); `list_ponds()`
  → Python `dict`; `drop_pond` on `PyDatabase`. `query` returns a `pyarrow.Table`
  (decode IPC via `arrow`/`pyo3-arrow`, or hand the IPC bytes to `pyarrow.ipc` on
  the Python side — pick in the plan). Named-arg signatures throughout.
- Update `tests/test_sdk.py`, `tests/embedded.rs`, and `README.md` (kill the stale
  `read`/`write` surface line).

---

## Testing
- **S1:** engine Arrow fidelity already covered; add node-edge test that `ReadQuery`
  returns decodable IPC with correct schema/rows + `meta_json`; write returns
  `snapshot_id`. CLI render regression (table + `--format json`).
- **S2:** registry migration test (description round-trips create→list→info);
  control/admin round-trip; CLI `--description` shown in list/describe.
- **S3:** `embedded.rs` + `test_sdk.py` exercise handle ergonomics: `create_pond`
  returns a handle, attributes populated, `query` → Arrow Table, `list_ponds` dict
  keyed by name with description, `drop_pond` on db.

Gate each slice: `cargo fmt --all` + `cargo clippy --workspace --all-targets -D
warnings` + `cargo test --workspace`; Python via `.venv/bin/python -m pytest`.

## MCP stays JSON + capped (not streamed) — rationale
Streaming Arrow is the **SDK/CLI (program)** surface, not MCP. MCP's client is an
**LLM agent**: it can't decode Arrow IPC, and dumping large result sets into a
context window is an anti-pattern — the 10k JSON cap is a guardrail, not a wart.
MCP tool calls also return a single `CallToolResult`, not a server-stream of
batches (SSE carries progress notifications, not result chunks). MCP already
drives the same `AgentOps::read_arrow` funnel internally (no double-materialize);
it just collects to JSON+cap at the edge. The right pattern for an agent facing
large data is "keep it in the pond, query summaries," not "stream it in."

**MCP roadmap watch (revisit if/when it lands):** streamed reads are "On the
Horizon" in the MCP roadmap via *streamed results* (incremental output) and
*reference-based results* (client pulls large payloads on demand instead of
polluting context) — SEP-1391 (Long-Running Operations), SEP-1686 (Tasks). When
standardized, the Latiq mapping is **reference-based**: a tool returns a
pond/table reference the agent queries — still not raw Arrow into the model.

## Out of scope (later)
Streaming `query(stream=True) → RecordBatchReader` over `ReadArrow` (unbounded);
dataset/catalog SDK methods (separate design after this); `.pyi` stubs, nightly
wheel CI, PyPI publish; Node SDK.
