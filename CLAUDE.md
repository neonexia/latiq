# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

**Read first, in order:**
- `docs/product.md` - Product Spec for latiq
- `docs/dev.md` — how to build, run, and manually test the current system (`./dev.sh` + CLI).
- Per-crate `crates/*/CLAUDE.md` — local invariants for that crate.
- `docs/superpowers/notes/m1-spike-findings.md` — spike-confirmed crate APIs.

## What Latiq is

An agent-native data system. Agents allocate ephemeral **ponds** (DuckLake workspaces- https://ducklake.select/docs/stable/), write/read SQL, collaborate, and release them. Operators administer the deployment; humans/programs (CLI, SDK) drive it programmatically. DuckLake is the storage spec; DuckDB is the M1 engine.

## Surfaces & audiences (the spine of the design)

**Three external surfaces, three distinct audiences. Do not blur them.**

| Surface | Audience | Lives on | Carries |
|---|---|---|---|
| **MCP-over-HTTP** | **Agents only** (frontier LLMs) | pond node | the agent tools + resources + prompts + guidance |
| **Data/Query gRPC** | **CLI + SDK** (not agents) | pond node | allocate / drop / read / write / explain |
| **Admin gRPC** | **Operators** | control plane | node / policy / audit + pond list/describe (metadata reads) |

Plus one internal surface: **Control gRPC** (pond-node → control-plane; routing/registry/audit writes).

## Design invariants (DO NOT DRIFT)

1. **MCP is the agent layer ONLY.** The CLI and SDK are **not agents** and must **never** use MCP. `latiq-client` (the MCP client) is for agent-simulation + MCP integration tests only.
2. **CLI/SDK speak gRPC.** Data ops (allocate/drop/read/write/explain) → **Data/Query gRPC on the pond node**. Metadata reads (pond list/describe) + admin (node/policy/audit) → **Admin gRPC on the control plane**.
3. **The control plane is NEVER in the query data path.** Queries execute on the **pond node** only. The control plane holds the registry/routing/policy/audit — metadata, never data.
4. **Split by ownership.** The pond node owns storage + engine (so allocate/drop/queries go there). The control plane owns the registry (so pure metadata reads go there, and work even when pond nodes are down).
5. **`latiq-agent-core` is PROTOCOL-NEUTRAL.** No MCP / gRPC / HTTP / transport types may appear in `latiq-agent-core`. Every surface (MCP, Data gRPC, future A2A) is an **inbound adapter** that maps its protocol onto `AgentOps`. **A new surface is a new adapter, never a change to the core.**
6. **Pure DuckLake — nothing on top.** Attribution rides DuckLake's native `set_commit_message`; callers read history via native `pond.snapshots()` and tables/columns via `SHOW TABLES`/`information_schema`. **No Latiq objects in the pond catalog** (no `_latiq` schema, views, or macros) and no shadow store of pond data/snapshots/attribution. (The DuckDB adapter may use `duckdb_tables()` *internally* for `describe_schema`; governance/policy metadata in the control-plane registry is a *different plane*. Both allowed.)
7. **One DuckDB instance per pond** (mutex-guarded, reused across queries) — the unit of **resource isolation** (per-pond memory/CPU caps live on the instance; DuckDB's `memory_limit`/`threads` are instance-global) and of concurrency ownership (one process owns each catalog file; independent instances racing on one catalog lose writes). Never go back to instance-per-query.
8. **Hard separation of surfaces.** Agents (MCP) cannot do admin; operators (Admin gRPC) are not agents; data clients (Data gRPC) are not agents. Different transports, different audiences, different audit attribution.
9. **Identity is relaxed in M1** (claimed, default `anonymous`, `verified:false`), carried by a header (MCP) / gRPC metadata (gRPC). OIDC verification is M2.
10. **Don't test DuckDB; test our integration with it.** DuckDB is a production engine. Test *our* code and *our* boundary: cell→JSON conversion (incl. nested/temporal types), the read/write/explain guards, cancellation + prompt resource release, concurrency correctness, attribution plumbing (native `pond.snapshots()`). Never assert DuckDB SQL semantics.
11. **Single binary** (`latiq`) for all roles. `protoc` required to build (`brew install protobuf`).
12. **Make it boring.** Predictable behavior, structured errors (`kind`/`message`/`suggest`/`see`), good defaults. Cleverness waits for later slices.

## Crates (`crates/`)

- `latiq` — the single binary: server roles (`control-plane`, `pond-node`) + the CLI (gRPC client; **not** an MCP client).
- `latiq-common` — kernel: `Identity`, `ErrorEnvelope`/`ErrorKind`, `QueryMeta`, `PondId`.
- `latiq-proto` — gRPC contracts: Control, Admin, and **Data/Query** services (tonic codegen).
- `latiq-agent-core` — **protocol-neutral** `AgentOps` + `ControlPlane` trait + in-flight/abort registry. No transport types (invariant 5).
- `latiq-mcp` — **inbound adapter**: MCP-over-HTTP (rmcp) → `AgentOps`. Agent-only.
- *(M8)* the Data/Query gRPC **inbound adapter** → `AgentOps` (in `latiq-pond-node` or its own crate).
- `latiq-client` — MCP client. **Agent-sim / MCP tests only** (invariant 1).
- `latiq-engine` (`QueryEngine` trait) + `latiq-engine-duckdb` (DuckDB/DuckLake adapter, instance-per-pond).
- `latiq-storage` — `PondStorage`: LocalFs + TempFs.
- `latiq-control-plane` — DuckDB registry + migrations + Control/Admin gRPC. Sole writer to its registry; never in the query path.
- `latiq-pond-node` — wires surfaces + `AgentOps` + engine + storage + `GrpcControlPlane`; node registration/heartbeat.

## Test taxonomy (so we can run targeted tests per change)

Tests are categorized by **layer** and **surface/feature** so a given change runs a known subset.

**Layers:**
- **Unit** (`#[test]` in `src/`) — pure logic, per crate. Run: `cargo test -p <crate> --lib`.
- **Crate integration** (`crates/<crate>/tests/*.rs`) — that crate's public API over real deps (e.g. engine lifecycle, gRPC round-trip). Run: `cargo test -p <crate> --test '*'`.
- **Full-stack e2e** (`crates/latiq/tests/<surface>.rs`) — the whole stack in-process via the harness, one file per surface. Run: `cargo test -p latiq --test <surface>`.

**Conventions (keep these so targeting works):**
- One e2e file per **surface**: `tests/mcp.rs`, `tests/query_grpc.rs`, `tests/admin.rs`. (`tests/common/mod.rs` = the shared harness.)
- Test fn names start with the **feature**: `pond_lifecycle_*`, `sql_read_write_*`, `attribution_*`, `result_encoding_*`, `inline_cap_*`, `cancellation_*`, `concurrency_*`, `ingestion_*`, `audit_*`, `policy_*`, `error_contract_*`. Both a `_happy` and the relevant `_edge`/error cases exist for every feature.
- **Run a feature across surfaces:** `cargo test <feature_prefix>` (name filter), e.g. `cargo test attribution`.
- **Run a surface:** `cargo test -p latiq --test query_grpc`.
- Every feature add/change ships with its tests **in the same commit** (interleaved, not deferred).

**Common targets:**
- Engine change → `cargo test -p latiq-engine-duckdb`
- MCP surface change → `cargo test -p latiq-mcp && cargo test -p latiq --test mcp`
- Data gRPC change → `cargo test -p latiq --test query_grpc`
- Control plane change → `cargo test -p latiq-control-plane && cargo test -p latiq --test admin`
- Everything → `cargo test --workspace`

## Build commands

- `cargo build` / `cargo test --workspace` (excludes `spike/`); first build compiles DuckDB from source (slow once).
- `cargo clippy --workspace --all-targets -- -D warnings` and `cargo fmt --all` — keep green (run manually before pushing). CI is **nightly only** (`.github/workflows/nightly.yml`: fmt+clippy+test, iceberg/MinIO catalog e2e, and a dockerized 3-node cluster scale-out), not per-PR, to bound GitHub usage (#28). `release-images.yml` publishes the single-binary image to GHCR; `deploy/` holds the Dockerfile, cluster compose, and the public `latiq-up.sh`.

## Scope / deferrals (later slices)

External-source **credentials** + federation, OIDC verification, rate limiting, OpenTelemetry, multi-node + proxy hops, Arrow **Flight SQL streaming** for large result sets (M1 Data gRPC is unary + bounded by the inline cap), Kubernetes, DataFusion engine. Don't build these without an explicit decision. (External catalogs themselves **shipped** — pull-only/transient, no stored creds; datasets + catalogs are in `docs/dataset.md`.)
