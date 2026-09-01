# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository. It holds the **technical + design invariants** for writing code.

**Product spec + positioning lives in [`docs/product.md`](docs/product.md)** — what Latiq is, who it's for, why, and what's shipped/next. **Consult it when in doubt about product intent or scope; don't reload it every call.** In one line: agents allocate ephemeral **ponds** (DuckLake workspaces — https://ducklake.select/docs/stable/ — on DuckDB, the M1 engine), read/write SQL, collaborate, and release them.

**Read first, in order:**
- `docs/dev.md` — how to build, run, and manually test the current system (`./dev.sh` + CLI).
- Per-crate `crates/*/CLAUDE.md` — local invariants for that crate.
- Per-area `e2e/CLAUDE.md`, `deploy/CLAUDE.md` — the nightly e2e suite and deployment/packaging (+ `docs/releasing.md` for publishing).

The non-obvious, confirmed facts about our two load-bearing dependencies live with the code that has to obey them: **duckdb-rs/DuckLake** (ATTACH form, `Connection` is not `Send`, `interrupt_handle()`, native `set_commit_message` attribution) in `crates/latiq-engine-duckdb/CLAUDE.md`; **rmcp** (required features, axum as a direct dep, client disconnects never reaching the handler) in `crates/latiq-mcp/CLAUDE.md`.

## Surfaces & audiences (the spine of the design)

**Three external surfaces, three distinct audiences. Do not blur them.**

| Surface | Audience | Lives on | Carries |
|---|---|---|---|
| **MCP-over-HTTP** | **Agents only** (frontier LLMs) | pond node | the agent tools + resources + prompts + guidance |
| **Data/Query gRPC** | **CLI + SDK** (not agents) | pond node | allocate / drop / read / write / explain |
| **Admin gRPC** | **Operators** | control plane | node / policy + pond list/describe (metadata reads) |

Plus one internal surface: **Control gRPC** (pond-node → control-plane; routing/registry writes). **Access auditing** is a structured `latiq::access` log trace — no audit table, no audit RPC (details in product.md).

## Design invariants (DO NOT DRIFT)

1. **MCP is the agent layer ONLY.** The CLI and SDK are **not agents** and must **never** use MCP. `latiq-client` (the MCP client) is for agent-simulation + MCP integration tests only.
2. **CLI/SDK speak gRPC.** Data ops (allocate/drop/read/write/explain) → **Data/Query gRPC on the pond node**. Metadata reads (pond list/describe) + admin (node/policy) → **Admin gRPC on the control plane**.
3. **The control plane is NEVER in the query data path.** Queries execute on the **pond node** only. The control plane holds the registry/routing/policy — metadata, never data.
4. **Split by ownership.** The pond node owns storage + engine (so allocate/drop/queries go there). The control plane owns the registry (so pure metadata reads go there, and work even when pond nodes are down).
5. **`latiq-agent-core` and `latiq-lineage` are PROTOCOL-NEUTRAL.** No MCP / gRPC / HTTP / transport types may appear in them. Every surface (MCP, Data gRPC, future A2A) is an **inbound adapter** that maps its protocol onto `AgentOps`. **A new surface is a new adapter, never a change to the core.** The one deliberate exception is `latiq-lineage`'s OpenLineage HTTP sink — HTTP by definition — isolated behind the **`http-sink` Cargo feature** that only `latiq-pond-node` enables (outside dev-dependencies): with the feature off the crate does not even depend on `reqwest`, so Cargo enforces the neutrality of the rest. What the neutral code sees is `EventSink`, a trait over `&str`.
6. **Pure DuckLake — nothing on top.** Attribution rides DuckLake's native `set_commit_message`; callers read history via native `pond.snapshots()` and tables/columns via `SHOW TABLES`/`information_schema`. **No Latiq objects in the pond catalog** (no `_latiq` schema, views, or macros) and no shadow store of pond data/snapshots/attribution. (The DuckDB adapter may use `duckdb_tables()` *internally* for `describe_schema`; governance/policy metadata in the control-plane registry is a *different plane*. Both allowed.)
7. **One DuckDB instance per pond** (mutex-guarded, reused across queries) — the unit of **resource isolation** (per-pond memory/CPU caps live on the instance; DuckDB's `memory_limit`/`threads` are instance-global) and of concurrency ownership (one process owns each catalog file; independent instances racing on one catalog lose writes). Never go back to instance-per-query.
8. **Hard separation of surfaces.** Agents (MCP) cannot do admin; operators (Admin gRPC) are not agents; data clients (Data gRPC) are not agents. Different transports, different audiences, different attribution.
9. **Identity always arrives in the transport, never in a tool/RPC argument** — `latiq-agent-id` (claimed leaf) and `Authorization: Bearer` (verified principal) as HTTP headers (MCP) / gRPC metadata. Configure an issuer and every surface verifies (OAuth 2.1 resource server, `latiq-auth`); configure none and identity stays relaxed (claimed, default `anonymous`, `verified:false`) — the default, and what the SDK/`dev.sh`/tests run. **Authority only ever comes from a verified field**; the claimed leaf is attribution. Authorization (who may reach what) is NOT built — see `docs/identity.md`.
10. **Don't test DuckDB; test our integration with it.** DuckDB is a production engine. Test *our* code and *our* boundary: cell→JSON conversion (incl. nested/temporal types), the read/write/explain guards, cancellation + prompt resource release, concurrency correctness, attribution plumbing (native `pond.snapshots()`). Never assert DuckDB SQL semantics.
11. **Single binary** (`latiq`) for all roles. `protoc` required to build (`brew install protobuf`).
12. **Make it boring.** Predictable behavior, structured errors (`kind`/`message`/`suggest`/`see`), good defaults. Cleverness waits for later slices.

## Crates (`crates/`)

- `latiq` — the single binary: server roles (`control-plane`, `pond-node`) + the CLI (gRPC client; **not** an MCP client).
- `latiq-common` — kernel: `Identity`, `ErrorEnvelope`/`ErrorKind`, `QueryMeta`, `PondId`.
- `latiq-proto` — gRPC contracts: Control, Admin, and **Data/Query** services (tonic codegen).
- `latiq-agent-core` — **protocol-neutral** `AgentOps` + `ControlPlane` trait + in-flight/abort registry. No transport types (invariant 5).
- `latiq-auth` — **protocol-neutral** OAuth 2.1 token verification: multi-issuer JWKS cache + claim validation + RFC 9728 metadata. Takes a token string, returns an `Identity`; adapters extract the carrier.
- `latiq-mcp` — **inbound adapter**: MCP-over-HTTP (rmcp) → `AgentOps`. Agent-only.
- the Data/Query gRPC **inbound adapter** → `AgentOps` (shipped — `latiq-pond-node/src/data_service.rs` + `stream_service.rs`).
- `latiq-client` — MCP client. **Agent-sim / MCP tests only** (invariant 1).
- `latiq-engine` (`QueryEngine` trait) + `latiq-engine-duckdb` (DuckDB/DuckLake adapter, instance-per-pond).
- `latiq-lineage` — **protocol-neutral** OpenLineage (core spec `2-0-2`): events + facets, the batching JSONL writer into the pond's own `lineage/` dir, the reader `get_lineage` pages, and the optional HTTP sink behind the `http-sink` feature. Opt-in per pond; a lineage failure never reaches a query (`docs/lineage.md`).
- `latiq-storage` — `PondStorage`: LocalFs + TempFs.
- `latiq-control-plane` — DuckDB registry + migrations + Control/Admin gRPC. Sole writer to its registry; never in the query path.
- `latiq-pond-node` — wires surfaces + `AgentOps` + engine + storage + `GrpcControlPlane`; node registration/heartbeat.

## Test taxonomy (so we can run targeted tests per change)

Tests are categorized by **layer** and **surface/feature** so a given change runs a known subset.

**Before adding or deleting a test, read [`crates/latiq/tests/CLAUDE.md`](crates/latiq/tests/CLAUDE.md)** — the conventions for *what a test must assert* (assert why not that, no vacuous guards, don't add test binaries, label regression pins). This section says where a test goes; that file says whether it should exist.

**Layers:**
- **Unit** (`#[test]` in `src/`) — pure logic, per crate. Run: `cargo test -p <crate> --lib`.
- **Crate integration** (`crates/<crate>/tests/*.rs`) — that crate's public API over real deps (e.g. engine lifecycle, gRPC round-trip). Run: `cargo test -p <crate> --test '*'`.
- **Full-stack e2e** (`crates/latiq/tests/<surface>.rs`) — the whole stack in-process via the harness, one file per surface. Run: `cargo test -p latiq --test <surface>`.

**Conventions (keep these so targeting works):**
- One e2e file per **surface**: `tests/mcp.rs`, `tests/query_grpc.rs`, `tests/admin.rs`. (`tests/common/mod.rs` = the shared harness.)
- Test fn names start with the **feature**: `pond_lifecycle_*`, `sql_read_write_*`, `attribution_*`, `result_encoding_*`, `inline_cap_*`, `cancellation_*`, `concurrency_*`, `ingestion_*`, `policy_*`, `error_contract_*`. Both a `_happy` and the relevant `_edge`/error cases exist for every feature.
- **Run a feature across surfaces:** `cargo test <feature_prefix>` (name filter), e.g. `cargo test attribution`.
- **Run a surface:** `cargo test -p latiq --test query_grpc`.
- Every feature add/change ships with its tests **in the same commit** (interleaved, not deferred).

**Common targets:**
- Engine change → `cargo test -p latiq-engine-duckdb`
- MCP surface change → `cargo test -p latiq-mcp && cargo test -p latiq --test mcp`
- Data gRPC change → `cargo test -p latiq --test query_grpc`
- Control plane change → `cargo test -p latiq-control-plane && cargo test -p latiq --test admin`
- Lineage change → `cargo test -p latiq-lineage && cargo test -p latiq-agent-core --test agent_ops` (the emitter + writer registry live there; `cargo test lineage` adds the surface e2e in `crates/latiq/tests/mcp.rs`)
- Everything → `cargo test --workspace`

## Build commands

- `cargo build` / `cargo test --workspace` (excludes `spike/`); first build compiles DuckDB from source (slow once).
- `cargo clippy --workspace --all-targets -- -D warnings` and `cargo fmt --all` — keep green (run manually before pushing). **Lint against the same toolchain CI uses**: CI is `dtolnay/rust-toolchain@stable`, i.e. *today's* stable, which is routinely ahead of a local toolchain that hasn't been `rustup update`d. New clippy releases add lints, so **local green does not imply CI green** — this has already cost a red nightly (`result_large_err` firing on tonic-generated code under a stable four releases newer than the local one). Before pushing: `rustup update stable && cargo +stable clippy --workspace --all-targets -- -D warnings`. CI is **nightly only** (`.github/workflows/nightly.yml`), not per-PR, to bound GitHub usage (#28): fmt+clippy+test, the iceberg/MinIO catalog e2e, a dockerized cluster scale-out, the **`e2e/` end-to-end suite** (SDK + MCP agent harness + perf, against a gatewayed multi-node cluster), and a **test-gated + change-gated versioned publish** (PyPI wheel + GHCR image — `deploy/CLAUDE.md`, `docs/releasing.md`). Those checks live in **one reusable workflow** (`.github/workflows/verify.yml`) which both `nightly.yml` and `release.yml` (a `v*` tag) call, so a tagged release runs exactly what the nightly runs; **no publishing job may skip it** (#55). **`deploy/` is the single home for deployment artifacts** — `deploy/docker-compose.yml` (the clone-free user deployment), `deploy/cluster/` (the multi-node CI/dev stack: pond nodes behind an **nginx gateway** = the single MCP + Data/Stream front door), `deploy/iceberg-minio/` (the catalog fixture), both Dockerfiles and the CLI installer. Start at `deploy/README.md`.

## Scope / deferrals

The deferral list (identity/auth, rate limiting, OTLP, k8s, Flight SQL streaming, DataFusion, …) lives in product.md *What's next*. **Don't build any of it without an explicit decision.** Coding notes: M1 Data gRPC is unary, bounded by the inline cap (Flight SQL streaming deferred); external catalogs **shipped** (pull-only/transient, no stored creds — see `docs/dataset.md`).
