# latiq-control-plane — CLAUDE.md

The control plane: a DuckDB-backed registry (nodes, ponds, policy) + the **Control gRPC** (pond nodes call it) and **Admin gRPC** (operators) surfaces.

## Invariants
- **NEVER in the query data path.** This process holds *metadata* — registry, routing, policy. It must not execute or proxy pond queries. Queries live on the pond node.
- **Sole writer to its registry.** Single `Arc<Mutex<Connection>>` (DuckDB single-writer happy path). Pond nodes reach it only via Control gRPC, never the file.
- **Two gRPC surfaces, distinct audiences:** Control gRPC = internal (pond nodes); Admin gRPC = operators (the CLI). Don't merge them.
- **Pond metadata reads (list/describe) belong on the Admin gRPC** so operators can inspect ponds even when pond nodes are down (split-by-ownership). Mutations (allocate/drop) are driven by the pond node via Control gRPC.
- **No audit store here.** Access auditing is not a registry capability — each access is a structured trace on the `latiq::access` log target (redacted SQL shape via `redact_sql` in `latiq-agent-core`; operators grep log files). The registry holds no audit table and the Admin gRPC exposes no audit RPC. The Admin surface emits its own records (it holds no `AgentOps`, so it has a local twin of the emitter): **keep its field names and `outcome` values identical** to `latiq_agent_core::access` or the one trail stops being greppable. Every op records both outcomes, rejected calls included.
- **Admin is verified like every other surface** when an issuer is configured (`authorization: Bearer` metadata, `Unauthenticated` + the `www-authenticate` challenge on rejection) — operators are not exempt. A row's `created_by` is the **verified subject** when there is one; the request's own `created_by` is a client claim and only stands when nothing was verified.
- **Migrations are forward-only and append-only.** Never edit a shipped migration; add a new one. DuckDB note: use `now()` (not bare `current_timestamp`) in `UPDATE … SET`.
- **Postgres is the scale path** behind the same gRPC contract — keep SQL portable-ish; don't leak DuckDB-only quirks into the contract.

## Tests
Unit (registry, migrations) + `tests/grpc_integration.rs` (Control + Admin over real loopback gRPC). Surface e2e: `crates/latiq/tests/admin.rs`.
