# latiq-pond-node — CLAUDE.md

Wires a running pond node: hosts the agent + data inbound adapters over one `AgentOps`, plus `GrpcControlPlane` (the `ControlPlane` trait over the Control gRPC client), node registration, and heartbeat.

## Invariants
- **One `AgentOps`, multiple inbound surfaces.** The pond node serves **MCP** (agents) AND the **Data/Query gRPC** (CLI/SDK) over the *same* `AgentOps` instance. Both are thin adapters; neither holds business logic.
- **This node owns storage + engine** — so allocate/drop/read/write/explain run here. It reaches the control plane (registry) only through `GrpcControlPlane` (Control gRPC). It must not embed the registry.
- **`GrpcControlPlane` maps gRPC ↔ the neutral `ControlPlane` trait.** Keep its error mapping consistent with `RegistryControlPlane` (e.g. node-not-found is an internal/availability error, NOT `pond_not_found`).
- **Heartbeat must survive blips** — reconnect + re-register on failure; don't silently let the node go stale. Report real `pond_count`.
- **Graceful shutdown** (target): stop accepting → abort in-flight (the in-flight registry) → checkpoint DuckLake → deregister → exit.

## Tests
Full two-process behavior (this node ↔ control plane over real gRPC, driven by a client) lives in the full-stack harness at `crates/latiq/tests/` — that's the only place `GrpcControlPlane`/`run_pond_node` get exercised, so keep it covered.
