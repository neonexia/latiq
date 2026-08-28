# latiq-pond-node — CLAUDE.md

Wires a running pond node: hosts the agent + data inbound adapters over one `AgentOps`, plus `GrpcControlPlane` (the `ControlPlane` trait over the Control gRPC client), node registration, and heartbeat.

## Invariants
- **One `AgentOps`, multiple inbound surfaces.** The pond node serves **MCP** (agents) AND the **Data/Query gRPC** (CLI/SDK) over the *same* `AgentOps` instance. Both are thin adapters; neither holds business logic.
- **This node owns storage + engine** — so allocate/drop/read/write/explain run here. It reaches the control plane (registry) only through `GrpcControlPlane` (Control gRPC). It must not embed the registry.
- **`GrpcControlPlane` maps gRPC ↔ the neutral `ControlPlane` trait.** Keep its error mapping consistent with `RegistryControlPlane` (e.g. node-not-found is an internal/availability error, NOT `pond_not_found`).
- **One verifier for both surfaces, built before anything binds** — a bad auth config fails startup, never degrades silently to unauthenticated. Data/Stream rejections are `Unauthenticated` + the `www-authenticate` challenge, and are recorded on `latiq::access` with `outcome=error`.
- **The forwarder replays the caller's own token** (`latiq_agent_core::current_bearer`) so the owning node re-verifies from scratch. NEVER add a trusted internal "already verified" header — the boundary would become the network, not the token. Keep `Unauthenticated` from the owner as `Unauthenticated` across the hop; collapsing it to `Internal` makes it unactionable.
- **`--advertise-addr` ≠ `--public-mcp-url`.** The first is the internal address peers forward to; the second is what agents dial (the gateway's URL behind a gateway) and is what gets published as the RFC 9728 `resource` + in the challenge, for the Data surface too.
- **Heartbeat must survive blips** — reconnect + re-register on failure; don't silently let the node go stale. Report real `pond_count`.
- **Graceful shutdown** (target): stop accepting → abort in-flight (the in-flight registry) → checkpoint DuckLake → deregister → exit.

## Tests
Full two-process behavior (this node ↔ control plane over real gRPC, driven by a client) lives in the full-stack harness at `crates/latiq/tests/` — that's the only place `GrpcControlPlane`/`run_pond_node` get exercised, so keep it covered.
