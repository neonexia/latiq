# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Current state

Implementation has begun. The repo is a **Cargo workspace** (10 crates under `crates/`) — the M1 spike and M2 (workspace + kernel) of "Slice 0+" are done; M3–M7 are not yet built.

**Read these first, in order:**
- `docs/superpowers/specs/2026-06-04-latiq-slice0-design.md` — **the authoritative design** for the current build (Slice 0+). Supersedes `docs/m1_design.md` where they differ.
- `docs/superpowers/plans/2026-06-04-latiq-slice0-m1-m2.md` — the executed M1+M2 plan.
- `docs/superpowers/notes/m1-spike-findings.md` — spike-confirmed crate APIs (rmcp `StreamableHttpService`, DuckLake `set_commit_message` attribution, `interrupt_handle()` cancellation, the rmcp client-disconnect gap).
- `docs/product_spec.md`, `docs/m1_design.md` — original product vision + full M1 design (background).

**What exists now:** `latiq-common` (Identity, ErrorEnvelope/ErrorKind, QueryMeta, PondId — all tested), `latiq-proto` (Control + Admin gRPC, tonic codegen), the `latiq` binary (clap subcommand skeleton), CI, and a throwaway `spike/` crate (NOT a workspace member — exploratory only). The other crates are empty stubs awaiting M3+.

**Build/test commands** (run from repo root):
- `cargo build` / `cargo test --workspace` — workspace excludes `spike/`.
- `cargo clippy --workspace --all-targets -- -D warnings` and `cargo fmt --all` — CI gates; keep both green.
- `latiq-proto` codegen needs **`protoc`** on PATH (`brew install protobuf`).
- The intended target is a **single binary** (`latiq`) serving roles via subcommand (`control-plane`, `pond-node`) — don't introduce a second language or multi-binary layout. M3+ continues per the spec's build order (§12).

## What Latiq is

A data system whose customer is an AI agent, not a human. Agents allocate ephemeral workspaces called **ponds**, write/query data with SQL, attach admin-curated external data sources ("catalogs"), and collaborate with other agents in a shared pond. Operators administer the deployment but never touch the agent surface.

## Architecture (from `m1_design.md`)

One binary, three roles via subcommand:

```
latiq control-plane    # stateful registry (Postgres; SQLite in dev)
latiq pond-node        # hosts ponds, terminates agent MCP calls, executes queries
latiq dev              # both roles in one process, for development
latiq <admin command>  # CLI client for operators (catalog/credential/node/audit ...)
```

Three tiers:

- **Control plane** — routing table (pond → node), pond registry, catalog registry, audit log, OIDC verification config, node health. **Never in the data path.** Two gRPC surfaces: *Control gRPC* (pond nodes call it) and *Admin gRPC* (the CLI calls it). These are distinct surfaces with distinct auth.
- **Pond nodes** — each is simultaneously *owner* (ponds on its local disk), *proxy* (forwards queries for ponds it doesn't own), and *MCP gateway* (terminates agent HTTP). Storage engine is **DuckDB + DuckLake**; pond data lives at `/var/lib/latiq/ponds/<pond-id>/` (`catalog.sqlite`, `data/` Parquet, `metadata.json`).
- **Load balancer** — standard L7 (nginx/envoy). No Latiq-specific logic; pond nodes route internally.

Three protocols:

- **MCP-over-HTTP (Streamable HTTP)** — the only agent-facing surface, versioned at `/mcp/v1/`. Targets the 2026-07-28 MCP spec. Requests carry `Mcp-Method` / `Mcp-Name` headers so the LB can route without parsing the body.
- **Internal Flight SQL over gRPC** — pond-node-to-pond-node proxy hops only. **Not exposed externally in M1** (that ships with the Python SDK in M2). Arrow batches are converted to JSON Lines at the pond node edge before reaching the agent.
- **Control/Admin gRPC** — internal + operator surfaces described above.

The load-bearing flow is the agent query (`m1_design.md` §3): LB → any pond node A → A validates identity → A asks control plane where the pond lives (no caching in M1) → A executes locally or proxies to owner B over Flight SQL → Arrow batches stream back → A converts to JSON Lines over HTTP chunked transfer → final `{"_meta": {...}}` frame → async audit write.

## The agent MCP surface (10 tools)

- Lifecycle: `allocate_pond`, `describe_pond`, `list_ponds`, `drop_pond`
- Query: `read_query` (SELECT only), `write_query` (INSERT/UPDATE/DELETE/DDL/CTAS), `explain_query` (cost estimation, read-only planner)
- Catalog: `list_catalogs`, `attach_catalog`, `detach_catalog`

`read_query`/`write_query` are split so MCP tool annotations are static and accurate (auto-approve reads, require confirmation for destructive writes). Plus MCP **Prompts** (parameterized SOPs) and **Resources** (`latiq://` recipes/troubleshooting/reference). Metadata is exposed not as tools but as SQL views in a reserved per-pond `_latiq` schema (`pond_info`, `snapshots`, `attribution`, `tables_summary`, `sources`) — writes to `_latiq.*` are blocked.

## Non-negotiable constraints

When implementing, these four principles (`m1_design.md` §16) override convenience. If a change is in tension, resolve in favor of the agent:

1. **The agent is the customer.** Agent-serving features go in the MCP surface; operator features go in the Admin API / CLI. Never blur them.
2. **Hard separation between MCP and Admin surfaces.** Different transports, different auth, different audit trails. An agent cannot register catalogs, manage credentials, or configure nodes. An admin never appears as an agent.
3. **One pond, one node.** Cross-catalog joins *inside* a pond are the feature. Cross-pond joins, distributed query, and multi-node ponds are explicitly out — don't add them.
4. **Make it boring.** M1 is the predictable floor (clear errors, good defaults). Cleverness waits for M2/M3.

Additional invariants worth holding onto:

- **Agents never see credentials, URIs, or connection strings.** Admins register catalogs + credentials (Vault first) via CLI; agents pick from a curated menu by name. Credentials are fetched from the store at attach time and discarded, never persisted by Latiq.
- **Identity is mandatory; verification is optional.** OIDC verification is admin-toggleable; when off, the `X-Latiq-Agent-Id` header is trusted and audited as `verified: false`. Every operation produces an audit entry.
- **Audit records SQL shape, not content.** Literal values become `?` placeholders; parameters are counted not logged; query results never enter the audit log.
- **MCP surface is versioned and stable within `/mcp/v1/`.** Additions allowed; renames/removals are not.

## MCP UX is the product

`m1_design.md` §4a is required reading for anything touching the agent surface. The prose in tool descriptions, errors, warnings, and resources is read by LLMs far more than by humans. Concretely: tool descriptions are mini-tutorials with concrete SQL examples and do/don't pairs; errors carry `what_failed` / `why` / `try` (and `did_you_mean` / `example` when relevant); suboptimal-but-successful operations return success with `warnings`; every query response carries a `_meta` block. Write directly and declaratively — no hedging, no apologetic errors, no aspirational docs for unbuilt features.

## Scope discipline

Out of M1 (don't build these without an explicit decision): Python SDK / externally-exposed Flight SQL, streaming ingestion (Kafka/CDC), Kubernetes/multi-host production deployment, cross-pond joins, per-pond ACLs / column-level security, disk quotas, live credential rotation, management UI, DataFusion engine. See `m1_design.md` §1 and §17 for the full deferred list and the milestone each maps to.
