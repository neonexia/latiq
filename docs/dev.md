# Latiq — Developer Guide

> **Status:** Slice 0+ (M1–M11) complete and runnable. This is the hands-on guide for building Latiq, starting the dev stack, and driving it manually through the **CLI** (the gRPC surfaces). Agents drive the separate **MCP** surface; an SDK is a later slice. Federation/catalogs, OIDC, rate-limiting, and OpenTelemetry are deferred.

## What you're running

Latiq is a two-process stack plus a CLI, all from one `latiq` binary:

- **control-plane** — the registry (nodes, ponds, policy, audit). Serves **Control gRPC** (pond nodes call it) and **Admin gRPC** (operators). Never in the query path.
- **pond-node** — owns storage + the DuckDB/DuckLake engine. Serves **MCP-over-HTTP** (agents only) and the **Data/Query gRPC** (CLI/SDK). Allocate/drop/read/write/explain run here.
- **`latiq` CLI** — a **gRPC client, not an agent.** Data ops → the pond node's Data gRPC; `pond list` + operator commands → the control plane's Admin gRPC.

The three surfaces have three distinct audiences and must not be blurred: **MCP = agents**, **Data gRPC = CLI/SDK**, **Admin gRPC = operators**.

---

## Prerequisites

- **Rust** (stable, 1.90+) — `cargo --version`.
- **protobuf compiler** `protoc` (the gRPC build needs it) — `brew install protobuf` on macOS.
- The first build **compiles DuckDB from source** (the `bundled` feature) — a few minutes once, then cached.

```bash
git clone git@github.com:neonexia/latiq.git
cd latiq
cargo build              # first build is slow (DuckDB); later builds are fast
```

### Dev build / check commands

```bash
cargo build                                                   # debug build of everything
cargo build -p latiq                                          # just the binary
cargo build --release                                         # optimized build
cargo test --workspace                                        # full test suite
cargo test -p latiq --test query_grpc                         # one surface's e2e tests
cargo test attribution                                        # one feature across surfaces (name filter)
cargo clippy --workspace --all-targets -- -D warnings         # lints (keep green)
cargo fmt --all                                               # format (keep green)
```

The binary lands at `target/debug/latiq` (or `target/release/latiq`).

---

## Start the stack

### Quick start (one command)

```bash
./dev.sh
```

Builds `latiq`, starts the control-plane and one pond-node, and prints the endpoints + example commands. Leave it running; open a second terminal for the CLI. `Ctrl+C` stops both.

Endpoints it brings up:

```
MCP (agents only):    http://127.0.0.1:8080/mcp
Data gRPC (CLI/SDK):  127.0.0.1:8081
Control gRPC:         127.0.0.1:9090
Admin gRPC (ops):     127.0.0.1:9091
```

Runtime artifacts land in `./latiq-cp.duckdb` (registry) and `./latiq-data/` (pond storage) — both gitignored. Override the locations with `LATIQ_DB` / `LATIQ_DATA`:

```bash
LATIQ_DB=/tmp/cp.duckdb LATIQ_DATA=/tmp/ponds ./dev.sh
```

### Manual start (two terminals)

Run the roles yourself to control their flags:

```bash
# Terminal 1 — control plane (registry + Control/Admin gRPC)
cargo run -p latiq -- control-plane --db ./latiq-cp.duckdb

# Terminal 2 — pond node (MCP for agents + Data gRPC for CLI/SDK; registers with the CP)
cargo run -p latiq -- pond-node --data-dir ./latiq-data
```

### Server options

**`latiq control-plane`**

| Flag | Default | Meaning |
|---|---|---|
| `--control-addr` | `127.0.0.1:9090` | Control gRPC bind (pond nodes connect here) |
| `--admin-addr` | `127.0.0.1:9091` | Admin gRPC bind (operators connect here) |
| `--db` | in-memory | DuckDB registry file; omit for an ephemeral in-memory registry |

**`latiq pond-node`**

| Flag | Default | Meaning |
|---|---|---|
| `--node-id` | `node-1` | This node's id in the registry |
| `--mcp-addr` | `127.0.0.1:8080` | MCP-over-HTTP bind (agents) |
| `--data-addr` | `127.0.0.1:8081` | Data/Query gRPC bind (CLI/SDK) |
| `--control` | `http://127.0.0.1:9090` | Control plane endpoint to register with |
| `--data-dir` | `./latiq-data` | Pond storage root |

---

## Drive it from the CLI

The CLI is a **gRPC client.** Connection options come from two flag groups:

- **Data ops** (`pond create/describe/drop`, `query`, `write`, `explain`) accept `--endpoint` (default `http://127.0.0.1:8081`) and `--agent-id <name>` — the identity your writes are attributed to (default `anonymous`).
- **Admin / metadata** (`pond list`, `node`, `policy`, `audit`) accept `--admin` (default `http://127.0.0.1:9091`).

> `pond list` reads the **control plane**, so it works even when the pond node is down. The other data ops need the pond node.

The examples below call `latiq` directly. Get it on your PATH once:

```bash
cargo install --path crates/latiq    # installs `latiq` into ~/.cargo/bin
```

Or, without installing, use the built binary (`target/debug/latiq <cmd>`) or `cargo run -q -p latiq -- <cmd>` in place of `latiq` below.

### Pond lifecycle

```bash
latiq pond create --name demo                         # allocate a pond (--name optional; auto-named if omitted)
latiq pond list                                       # discover ponds (control plane)
latiq pond describe demo                              # metadata + table summary
latiq pond drop demo --confirm                        # DESTRUCTIVE — requires --confirm
```

`pond drop` without `--confirm` is refused with a structured error and leaves the pond intact — the confirm flag is the destructive-op gate.

### SQL: write / read / explain

```bash
# Write (DDL + insert), attributed to an identity
latiq write --pond demo --agent-id alice "CREATE TABLE events(id INTEGER, sev VARCHAR)"
latiq write --pond demo --agent-id alice "INSERT INTO events VALUES (1,'high'),(2,'critical')"

# Read it back (rows + a _meta envelope)
latiq query --pond demo "SELECT id, sev FROM events ORDER BY id"

# Native DuckLake attribution, exposed via the _latiq schema
latiq query --pond demo "SELECT snapshot_id, author, commit_message FROM _latiq.attribution"

# Plan a query without running it
latiq explain --pond demo "SELECT * FROM events WHERE sev = 'critical'"
```

The SQL is a positional argument; `--pond` is required. Successful results print as pretty JSON (`columns`, `rows`, `statement`, `status`, `_meta`). Errors print the structured envelope:

```
error [pond_not_found]: Pond 'ghost' does not exist.
  suggest: Call list_ponds to see available ponds, or allocate_pond to create one.
  see: latiq://troubleshooting/pond-not-found
```

### Query-by-URI ingestion (public files)

The pond node loads DuckDB's `httpfs`, `parquet`, and `json` extensions, so you can read public/anonymous files straight into a pond — no catalog needed (credentialed/federated sources are a later slice):

```bash
latiq write --pond demo "CREATE TABLE t AS SELECT * FROM read_csv('https://example.com/data.csv')"
latiq query --pond demo "SELECT count(*) FROM 's3://some-public-bucket/file.parquet'"
```

### Targeting a non-default stack

```bash
latiq query --pond demo --endpoint http://127.0.0.1:18081 "SELECT 1"
latiq pond list --admin http://127.0.0.1:19091
```

---

## Operator CLI (Admin gRPC)

A separate surface from the agent MCP path — `--admin` (default `http://127.0.0.1:9091`).

```bash
latiq node list                       # registered pond nodes (id, state, pond_count, mcp_endpoint)
latiq node describe node-1            # one node, pretty JSON
latiq policy show                     # deployment policy (defaults seeded)
latiq policy set query_timeout_seconds 45
latiq audit tail --limit 20           # recent ops: identity, verified, operation, pond, duration (SQL shape redacted)
latiq audit search alice              # audit for one identity
```

---

## The agent surface (MCP) — for reference

Agents (frontier LLMs), not the CLI, point an MCP client at:

```
http://127.0.0.1:8080/mcp     (Streamable HTTP transport)
```

Tools: `allocate_pond`, `describe_pond`, `list_ponds`, `drop_pond`, `read_query`, `write_query`, `explain_query` — plus `latiq://` resources and prompt SOPs for guidance. Results carry both a text block and `structuredContent`; errors set `isError` with the structured envelope. Identity is relaxed (Slice 0+): pass an optional `agent_id` argument; header-based/OIDC identity is a later slice. To exercise this surface programmatically in tests, use `latiq-client` (the agent-sim MCP client) — never from the CLI.

---

## What works now vs. later

**Now (Slice 0+ / M1–M11):** pond lifecycle, SQL read/write with native attribution, `explain`, the `_latiq` schema (`snapshots`, `attribution`, `tables_summary`, `sources`), query-by-URI ingestion of public files, query cancellation + prompt resource release, the completed MCP agent surface (tools + resources + prompts), the Data and Admin gRPC surfaces, and an audit log.

**Later slices:** external catalogs + credentials + federation, OIDC verification, Arrow Flight SQL streaming for large results, rate limiting, OpenTelemetry, multi-node, an SDK.

---

## Cleanup

```bash
# stop dev.sh with Ctrl+C, then:
rm -rf ./latiq-data ./latiq-cp.duckdb
```
