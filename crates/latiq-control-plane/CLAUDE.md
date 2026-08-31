# latiq-control-plane — CLAUDE.md

The control plane: a DuckDB-backed registry (nodes, ponds, policy) + the **Control gRPC** (pond nodes call it) and **Admin gRPC** (operators) surfaces.

## Invariants
- **NEVER in the query data path.** This process holds *metadata* — registry, routing, policy. It must not execute or proxy pond queries. Queries live on the pond node.
- **Sole writer to its registry.** Single `Arc<Mutex<Connection>>` (DuckDB single-writer happy path). Pond nodes reach it only via Control gRPC, never the file.
- **Two gRPC surfaces, distinct audiences:** Control gRPC = internal (pond nodes); Admin gRPC = operators (the CLI). Don't merge them.
- **Pond metadata reads (list/describe) belong on the Admin gRPC** so operators can inspect ponds even when pond nodes are down (split-by-ownership). Mutations (allocate/drop) are driven by the pond node via Control gRPC.
- **No audit store here.** Access auditing is not a registry capability — each access is a structured trace on the `latiq::access` log target (redacted SQL shape via `redact_sql` in `latiq-agent-core`; operators grep log files). The registry holds no audit table and the Admin gRPC exposes no audit RPC. The Admin surface emits its own records (it holds no `AgentOps`, so it has a local twin of the emitter): **keep its field names and `outcome` values identical** to `latiq_agent_core::access` or the one trail stops being greppable. Every op records both outcomes, rejected calls included.
- **Admin is verified like every other surface** when an issuer is configured (`authorization: Bearer` metadata, `Unauthenticated` + the `www-authenticate` challenge on rejection) — operators are not exempt. A row's `created_by` is the **verified subject** when there is one; the request's own `created_by` is a client claim and only stands when nothing was verified.
- **`ponds.lineage` is the per-pond opt-in, and it is fixed for the pond's lifetime.** A `BOOLEAN DEFAULT FALSE` column added by the last migration in `migrations.rs`; existing rows read as false, which is correct — nothing was recorded for them. It is set at `create_pond_assignment` and there is **no update path**: enabling it later would leave a hole at the start of the record that reads as "nothing happened". The registry stores the flag only — the events are files on the pond node (invariant 3), and the control plane never sees one. It rides `PondRow` out through both Control gRPC (the node needs it to provision `lineage/` and to gate emission) and Admin gRPC (`pond list`/`describe`, so a caller can tell whether `get_lineage` will have anything to say).
- **Migrations are forward-only and append-only.** Never edit a shipped migration; add a new one. DuckDB note: use `now()` (not bare `current_timestamp`) in `UPDATE … SET`.
- **Postgres is the scale path** behind the same gRPC contract — keep SQL portable-ish; don't leak DuckDB-only quirks into the contract.

## Tests
Unit (registry, migrations) + `tests/grpc_integration.rs` (Control + Admin over real loopback gRPC). Surface e2e: `crates/latiq/tests/admin.rs`.
