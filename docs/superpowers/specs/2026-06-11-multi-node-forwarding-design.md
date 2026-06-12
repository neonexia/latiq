# Multi-node request forwarding — design

**Date:** 2026-06-11
**Status:** Approved (shape), internals owned by implementer.
**Scope this pass:** node-to-node forwarding + multi-node test harness + nginx dev front door.
**Deferred (later passes):** SDK Arrow/Flight SQL transport; per-pond resource limits.

## Goal

Let any external client reach a single front door (a load balancer) without knowing
which node owns its pond. Whichever node greets a request resolves the owning node
and, if it isn't local, forwards the request to the owner and relays the answer back.
The client never sees the hop.

## Why (the production truth)

In k8s, an outside client cannot address an individual pod — it reaches a Service
(the front door). Only pods inside the cluster can address each other one-to-one
(the node-to-node hop). So the honest model is: **outside → in always goes through
the front door; inside, nodes talk directly.** The earlier "client resolves and
dials the node directly" idea only worked for local dev and is dropped.

## Shape

```
 Agent (MCP/HTTP) ┐
 CLI  (gRPC)      ├─▶ nginx front door ─▶ greeter node ─┬─ owns pond? ─▶ run locally (today's path)
 SDK  (Flight)*   ┘                                     └─ no ─▶ forward to owner node ─▶ relay back
```
`*` SDK/Flight is a later pass.

- **One forwarding mechanism, shared by all surfaces.** It lives in the core ops
  layer (`AgentOps`), so the inbound adapters (MCP, Data gRPC) stay dumb and unchanged.
  This mirrors the existing `ControlPlane` trait: the core already abstracts "a remote
  thing" (the registry); "a peer node" is the same pattern. Invariant 5 holds — the
  forwarding trait carries only neutral types; the gRPC implementation lives in the
  pond-node adapter layer.
- **Invariant 3 holds.** The control plane is consulted only to *resolve* the owner
  (a registry read, same class as today's `pond_info`); data flows node→node directly.
- **Invariant 7 holds.** A pond is still owned by exactly one node; forwarding never
  creates a second owner.
- **Results stay native per surface.** MCP/CLI = JSON (this pass). SDK = Arrow batches
  end-to-end via Flight SQL with DuckDB's native Arrow output — no JSON in that path
  (later pass). The forwarding *brain* is shared; only the pipe and result wrapping
  differ by surface.

## Components (this pass)

### 1. Resolve the owning node (no extra RPC)
`PondInfo` (agent-core) gains `node_endpoint: Option<String>`. The registry's
pond-info / pond-location read already joins `nodes`, so `GrpcControlPlane.pond_info`
fills it from `GetPondLocationResponse.node_endpoint` (already on the wire, currently
discarded). `run_query`/`describe`/`drop` already call `pond_info`, so they get the
endpoint for free.

### 2. Forwarding strategy in the core
- New trait `Forwarder` in `latiq-agent-core` (neutral signatures): `forward_read`,
  `forward_write`, `forward_explain`, `forward_describe`, `forward_drop` — each takes
  `(endpoint: &str, &Identity, pond, sql?)` and returns the same result types
  `AgentOps` returns.
- `AgentOps` gains `self_endpoint: Option<String>` + `forwarder: Option<Arc<dyn Forwarder>>`.
  Both `None` → today's single-node behavior (existing tests untouched).
- In each pond-specific op: resolve owner; if `forwarder` set and
  `owner != self_endpoint`, delegate to the forwarder; else run locally.

### 3. gRPC forwarder (pond-node adapter)
`GrpcForwarder` in `latiq-pond-node`: a small connection pool
(`Mutex<HashMap<endpoint, DataClient<Channel>>>`, lazy connect, reuse channel).
Each forward calls the peer's existing Data gRPC RPC with `latiq-agent-id` metadata
and re-hydrates `JsonResponse.json` into the result type. To avoid encoder drift, the
Data service's result encoder and this decoder share one serde-shaped representation
(single canonical wire shape). Endpoint comparison is verbatim string equality on the
registered `internal_endpoint`.

### 4. Multi-node test harness
`tests/common/mod.rs`: `start_stack()` → `start_stack_n(n_nodes)` returning the control
endpoint + a `Vec` of per-node `{node_id, mcp_endpoint, data_endpoint, internal_endpoint}`.
Each node is built with `self_endpoint` + a `GrpcForwarder` so forwarding is live.
`start_stack()` stays as `start_stack_n(1)` for existing tests.

### 5. nginx dev front door + `dev.sh --nodes N`
- `./dev.sh --nodes N` (default 1). `N == 1` → today's behavior, **no nginx** (keeps the
  common path dependency-free). `N > 1` → start control plane + N nodes + nginx.
- Nodes bind consecutive ports from the data base (data = base + 2k, mcp = data + 1).
- A generated `nginx.conf` (written under `$ROOT`) fronts two upstreams:
  - **MCP** (HTTP) over all node MCP ports — **`ip_hash`** for agent session affinity
    (the MCP session lives on the greeter; a later request must return to it).
  - **Data gRPC** (`grpc_pass`, http2) over all node data ports — round-robin (spread
    requests across greeters; forwarding handles correctness).
- Preflight: nginx installed? front-door ports free? Banner shows the front door +
  each node. Cleanup stops nginx + all nodes.
- Requires `nginx` for multi-node (documented; `brew install nginx`).

### 6. CLI through the front door (opt-in)
`LATIQ_GATEWAY` (data gRPC front-door address). When set, the CLI sends data ops there
(skips resolve+dial); pond create/list still go to the control plane. Unset → today's
resolve-and-dial path. Lets you drive everything through nginx for manual verification
without breaking existing single-node flows.

## Testing (proof)

New e2e file `crates/latiq/tests/forwarding.rs` (feature prefix `forwarding_`):
- `forwarding_read_happy` — 2 nodes; create pond (find owner via control plane); send a
  read to the **non-owner** node's Data gRPC; assert correct rows.
- `forwarding_write_then_read_consistent` — write via non-owner, read via owner (and
  vice-versa); same data both ways.
- `forwarding_local_unchanged` — hit the owner directly; unchanged behavior (regression).
- `forwarding_describe` / `forwarding_drop` — describe and drop forwarded correctly.
- `forwarding_pond_not_found` — error envelope still propagates across the hop.

Determinism: never rely on random assignment landing where we want — always resolve the
owner via the control plane first, then deliberately target a different node.

Gate before each PR: `cargo fmt --all && cargo clippy --workspace --all-targets -- -D
warnings && cargo test --workspace`.

## Non-goals (this pass)

- SDK Flight SQL / Arrow transport (separate later pass; design reserved above).
- Per-pond resource limits (separate later pass).
- nginx in the Rust tests (the harness exercises forwarding in-process; nginx is dev-only).
- Production k8s manifests (dev nginx only).
