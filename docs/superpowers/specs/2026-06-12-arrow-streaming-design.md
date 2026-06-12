# Arrow streaming for reads — design

**Date:** 2026-06-12
**Status:** Approved (shape). Supersedes the "SDK Arrow/Flight" deferral in the
multi-node forwarding spec.
**Goal:** Stream query results as Arrow batches end-to-end — DuckDB → owner →
greeter → client — so large results aren't buffered, the node-to-node hop stops
double-materializing, and an SDK gets Arrow → pandas via standard Arrow tooling.

## Decisions (locked)

- **Stream Arrow IPC over our OWN gRPC** — no `arrow-flight`. A server-streaming
  RPC carries Arrow IPC byte chunks. The *data* is standard Arrow (decodable by
  `pyarrow.ipc`, arrow-rs, etc.); only the transport framing is ours. Rationale:
  both hops are internal/ours, and `arrow-flight@58` forces tonic 0.14 alongside
  our tonic 0.12 (build bloat) for a protocol we don't need. **One tonic version.**
- **One mechanism, two roles.** The same streaming RPC is the SDK's external
  transport *and* the internal node-to-node forward for reads.
- **Arrow internally everywhere; JSON only at the MCP/CLI edge.** The node-to-node
  hop is Arrow for reads on every surface — the double-materialize is gone.
- **Greeter passthrough is zero-touch.** When forwarding for a streaming client,
  the greeter relays the owner's raw IPC byte chunks **without decoding** to
  RecordBatch — cheaper than decode/re-encode.
- **Writes unchanged** — unary status/snapshot response; nothing to stream.
- **Server + standard-Arrow client this pass.** The "SDK" is a thin client over
  our gRPC stream that decodes Arrow IPC (`pyarrow.ipc.open_stream` → `.to_pandas()`).
  Verify with a Rust client test (always-on) + an optional pyarrow smoke.
- **Trade-off accepted:** generic Flight tools (ADBC/JDBC) won't connect — only
  our SDK / Arrow-IPC-aware clients. Fine: it's internal.

## Data flow

```
 SDK (gRPC + pyarrow.ipc) ─ReadArrow(pond,sql)─▶ nginx ─▶ greeter (streaming RPC server)
                                                              │ owns pond?
                                                  local ──────┼────── remote
                                                              │             │ relay raw IPC chunks (no decode)
                                          engine.read_arrow → IPC bytes      ▼
                                          (DuckDB native Arrow)         owner streaming RPC → engine.read_arrow
                                                              │             │
                                                              ▼             ▼
                                          ◀──── Arrow IPC byte chunks (streamed, nothing buffered) ────
```
- **Streaming client (SDK):** receives IPC chunks → Arrow → pandas. Zero
  materialization on the nodes, **no row cap**.
- **Data gRPC (CLI) / MCP:** the read handler drives the *same* `AgentOps`
  Arrow stream, **collects** it into the existing `{columns, rows, _meta}` JSON
  **once at the edge**, bounded by the 10k inline cap, and returns the unary
  response as today.

## Components

### 1. Engine: streaming Arrow read
- Enable the `duckdb` crate's `arrow` feature.
- Add `QueryEngine::read_arrow(loc, sql, abort) -> Result<RecordBatchReader>` —
  returns DuckDB's native Arrow batches lazily (a blocking iterator of
  `arrow::record_batch::RecordBatch`). The engine stays sync/blocking, as today.
- The read-only guard (`is_read_only`) still applies; this path is reads only.
- Move `is_read_only` into `latiq-engine` as a neutral free function (it's a
  SQL-prefix heuristic, not DuckDB-specific) so both the engine and the CLI/core
  can classify without depending on the DuckDB impl crate.

### 2. agent-core: Arrow read funnel
- `AgentOps::read_arrow(identity, pond, sql) -> Result<RecordBatchStream>` where
  `RecordBatchStream = Pin<Box<dyn Stream<Item = Result<RecordBatch, AgentError>> + Send>>`.
  Local: drive `engine.read_arrow` on `spawn_blocking`, forwarding batches over a
  bounded `tokio::mpsc` exposed as a `Stream` (backpressure). Remote: delegate to
  the `Forwarder`.
- `Forwarder` gains `read_arrow(endpoint, identity, pond, sql) -> Result<RecordBatchStream>`.
- **Invariant note:** agent-core gains a dependency on the **`arrow` data crate**
  only — Arrow is a data representation (like the `serde_json::Value` it already
  returns), *not* a transport. **`arrow-flight` (the gRPC transport) stays out of
  agent-core**, in the pond-node adapter. Invariant 5 holds.

### 3. pond-node: streaming RPC server + Arrow forwarder
- New proto RPC on our own (tonic 0.12) stack: `rpc ReadArrow(QueryRequest)
  returns (stream ArrowChunk)` where `ArrowChunk { bytes ipc = 1; }`. Put it on a
  dedicated `Stream` service (own port) so it's cleanly separable.
- Server `do_read_arrow`: resolve owner via `AgentOps`.
  - **Local:** drive `AgentOps::read_arrow` (RecordBatch stream) → encode with
    `arrow_ipc::writer::StreamWriter` (schema once, then batches) → emit `ArrowChunk`s.
  - **Remote:** open a `ReadArrow` stream to the owner and **relay the raw
    `ArrowChunk` bytes through** — no decode/re-encode (zero-touch passthrough).
- `GrpcForwarder::read_arrow` (for the JSON edges that need typed batches): open
  the owner's `ReadArrow` stream and decode IPC → `RecordBatch` stream.
- Data `ReadQuery` / MCP `read_query` handlers: call `AgentOps::read_arrow`,
  collect batches → the existing JSON shape (cap-bounded), return unary. The
  Arrow→`{columns, rows}` collector lives next to `wire.rs`, paired with the JSON
  encoder.
- `AgentOps` exposes owner resolution (`locate(pond) -> Local | Remote(endpoint)`)
  so the passthrough server can relay bytes without going through a typed stream.
- Serve the Stream service on `cfg.stream_addr` (= data port + 2). MCP stays data + 1.

### 4. nginx: front-door the stream port
- Third upstream `latiq_stream` over all nodes' stream ports; gRPC (`grpc_pass`,
  http2), spread (forwarding handles ownership). Gateway stream port printed by
  `dev.sh`; documented as the SDK endpoint.

### 5. CLI: route reads to the read path
- `latiq query` classifies via the neutral `is_read_only`: reads → `ReadQuery`
  (Arrow-backed), writes → `WriteQuery`. Still one `query` command. Bonus: fixes
  the `op="query"` log to read/write accurately.
- (Optional, same pass) a `latiq query --arrow` smoke that pulls via the stream
  RPC — or rely on the pyarrow/Rust client tests.

## Cap & resources
- **Stream passthrough (SDK): uncapped** — the reason to stream.
- **JSON edges (Data/MCP): keep the 10k inline cap** (unary, must buffer).
- Per-pond memory/CPU limits remain **deferred**; a huge server-side aggregation
  on the owner is still unbounded (same as today) — noted, not fixed here.

## Cancellation, errors, attribution
- Cancellation: the existing `AbortToken` flows into `read_arrow`; dropping the
  client stream cancels the owner's work (drop propagates through the channel /
  RPC). Best-effort across the hop, as with the JSON forward.
- Errors mid-stream surface as a gRPC `Status` (envelope in details, as elsewhere);
  the JSON edges map them to `ErrorEnvelope`. Pre-stream errors (pond not found,
  parse) behave as today.
- Reads aren't attributed (no snapshot); writes (unchanged) keep native attribution.

## Dependencies
- Enable the `duckdb` crate's `arrow` feature (native Arrow output; pulls `arrow`
  58.3, already in the tree).
- Add `arrow` (with `ipc`) at **58.3** to engine + pond-node for IPC encode/decode
  and `RecordBatch`. **No `arrow-flight`** → single tonic 0.12. (A spike confirmed
  arrow-flight@58 forces tonic 0.14; avoided entirely by streaming IPC over our
  own RPC.)

## Testing
- Engine: `read_arrow` yields batches; schema/row fidelity vs the JSON path
  (don't assert DuckDB semantics — assert our conversion). `is_read_only` moved,
  still correct.
- agent-core: `read_arrow` local + forwarded (fake forwarder) — batches delivered,
  backpressure, cancellation drops the stream.
- Full-stack (`tests/arrow_stream.rs`): 2 nodes; `ReadArrow` a **> 10k-row**
  result through the **non-owner** node — proves streaming past the cap + raw-chunk
  forwarding passthrough. A Rust streaming-client test decoding IPC (always-on) and
  an optional pyarrow smoke (gated, needs Python).
- Regression: existing Data/MCP read tests still pass (now Arrow-backed at the edge).

Gate: `cargo fmt` + `cargo clippy --workspace --all-targets -D warnings` +
`cargo test --workspace`.

## Out of scope
- The Arrow **Flight** protocol / ADBC-JDBC interop — we stream IPC over our own
  RPC instead (revisit only if generic Flight clients become a requirement).
- A packaged Python SDK — later (a thin gRPC + `pyarrow.ipc` client suffices now).
- Streaming **writes** and per-pond resource limits — separate passes.
