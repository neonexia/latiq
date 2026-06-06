# latiq-agent-core — CLAUDE.md

The **protocol-neutral core** of the hexagon. Holds `AgentOps` (allocate/describe/list/drop pond, read/write/explain query), the `ControlPlane` trait, the neutral result/error types, and the in-flight/abort registry.

## Invariants (do not drift)
- **NO transport/protocol types here.** No `rmcp`, `tonic`/gRPC, `axum`, HTTP, or MCP types. If you're importing a protocol crate, you're in the wrong crate — it belongs in an inbound adapter (`latiq-mcp`, the Data-gRPC adapter).
- **Every external surface is an inbound adapter onto `AgentOps`.** Adding MCP/gRPC/A2A support = a new adapter crate that calls these methods. Never special-case a protocol in here.
- **`ControlPlane` is a trait** (in-process `RegistryControlPlane` for single-process/tests; `GrpcControlPlane` over the wire). `AgentOps` depends on the trait, never on a concrete transport.
- **Results are neutral** (`QueryResult { columns, rows, _meta }`, `ErrorEnvelope`). Adapters encode them; the core never knows about JSON-vs-protobuf.
- **Identity is relaxed** (`Identity::claimed`, default anonymous). The core consumes an `Identity`; adapters extract it from their transport.
- **Engine calls are blocking** → run via `spawn_blocking`. Don't hold a std mutex guard across `.await`.

## Tests
Unit: in `src/`. Cross-component (AgentOps over RegistryControlPlane + DuckEngine + TempFs): `tests/agent_ops.rs`, `tests/m7.rs`. These are the in-process feature tests; full-stack/surface tests live in `crates/latiq/tests/`.
