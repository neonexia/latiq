# latiq-agent-core — CLAUDE.md

The **protocol-neutral core** of the hexagon. Holds `AgentOps` (allocate/describe/list/drop pond, read/write/explain query), the `ControlPlane` trait, the neutral result/error types, the in-flight/abort registry, the `latiq::access` emitter, and the ambient bearer task-local.

## Invariants (do not drift)
- **NO transport/protocol types here.** No `rmcp`, `tonic`/gRPC, `axum`, HTTP, or MCP types. If you're importing a protocol crate, you're in the wrong crate — it belongs in an inbound adapter (`latiq-mcp`, the Data-gRPC adapter).
- **Every external surface is an inbound adapter onto `AgentOps`.** Adding MCP/gRPC/A2A support = a new adapter crate that calls these methods. Never special-case a protocol in here.
- **`ControlPlane` is a trait** (in-process `RegistryControlPlane` for single-process/tests; `GrpcControlPlane` over the wire). `AgentOps` depends on the trait, never on a concrete transport.
- **Results are neutral** (`QueryResult { columns, rows, _meta }`, `ErrorEnvelope`). Adapters encode them; the core never knows about JSON-vs-protobuf.
- **The core consumes an `Identity`; adapters extract it from their transport.** Both modes exist: verified (`latiq-auth` produced it from a token — `subject`/`issuer` set, `verified: true`) and relaxed (`Identity::claimed`, default anonymous, the no-issuer default). Never branch on the transport in here; `agent_id` is claimed in BOTH modes and must never carry authority.
- **`bearer` is the ambient caller token** (`with_bearer` / `current_bearer`), scoped per request by each adapter and read back by the forwarder so the owning node re-verifies. Protocol-neutral by design: a bare `String` task-local, no transport types — and set ONLY when a verifier is configured.
- **`access::record` is the one `latiq::access` emitter.** Same fields from every producer (`AgentOps` and the pond node's Data/Stream adapter both call it), including `outcome` (`ok`/`error`) — audit failures and rejections, not only successes.
- **Engine calls are blocking** → run via `spawn_blocking`. Don't hold a std mutex guard across `.await`.

## Tests
Unit: in `src/`. Cross-component (AgentOps over RegistryControlPlane + DuckEngine + TempFs): `tests/agent_ops.rs` — the crate's one general integration binary, with `mod m7` (M7 success criteria) and `mod forwarding` (the delegation decision, over fakes) inside it. `tests/access_trail.rs` is a separate binary ONLY because it installs a process-global `tracing` subscriber; nothing else earns one, since every binary statically links a bundled DuckDB. These are the in-process feature tests; full-stack/surface tests live in `crates/latiq/tests/`.
