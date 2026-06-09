# Latiq — Developer Guide

> **Status:** Slice 0+ (M1–M11) complete and runnable. This is the hands-on guide for building Latiq, starting the dev stack, and driving it manually through the **CLI** (the gRPC surfaces). Agents drive the separate **MCP** surface; an SDK is a later slice. Federation/catalogs, OIDC, rate-limiting, and OpenTelemetry are deferred.

## What you're running

Latiq is a two-process stack plus a CLI, all from one `latiq` binary:

- **control plane** (`latiq serve`) — the registry (nodes, ponds, policy, audit). Serves **Control gRPC** (pond nodes) and **Admin gRPC** (operators/CLI) on one port. Never in the query path.
- **pond node** (`latiq node add`) — owns storage + the DuckDB/DuckLake engine. Serves **MCP-over-HTTP** (agents only) and the **Data/Query gRPC** (CLI/SDK). Queries run here.
- **`latiq` CLI** — a **gRPC client, not an agent.** Its single entry point is the **control plane**: the CP assigns a node for new ponds and resolves which node hosts an existing pond, then the CLI runs data ops **node-direct** (the control plane is never in the data path).

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

Builds `latiq`, starts the control plane (`serve`) and one pond node (`node add`), waits until each is actually listening, and prints the endpoints + examples. Leave it running; open a second terminal for the CLI. `Ctrl+C` stops both.

Endpoints it brings up:

```
Control plane (CLI):  127.0.0.1:9090   (Control + Admin gRPC, one port)
Pond node Data gRPC:  127.0.0.1:8081
MCP (agents only):    http://127.0.0.1:8082/mcp
Root:                 ./.latiq-dev
```

Runtime artifacts land under `./.latiq-dev/` (registry at `registry.duckdb`, pond storage under `ponds/`) — gitignored.

`dev.sh` preflights the ports and aborts (naming the culprit) if one is taken, so a stale stack fails loudly instead of producing confusing gRPC errors. Override via flags (`./dev.sh --help`); MCP is always the Data port + 1:

```bash
./dev.sh --cp-port 19090 --data-port 18081 --root /tmp/latiq-dev
```

### Manual start (two terminals)

```bash
# Terminal 1 — control plane (Control + Admin gRPC on one port)
cargo run -p latiq -- serve --port 9090 --root ~/.latiq

# Terminal 2 — pond node (Data gRPC + MCP; registers with the control plane)
cargo run -p latiq -- node add --port 8081 --root ~/.latiq --control http://127.0.0.1:9090
```

### Server options

**`latiq serve`** (control plane)

| Flag | Default | Meaning |
|---|---|---|
| `--port` | `9090` | Control + Admin gRPC port (one port, both services) |
| `--root` | `~/.latiq` | Data root; registry at `<root>/registry.duckdb` |

**`latiq node add`** (pond node)

| Flag | Default | Meaning |
|---|---|---|
| `--node-id` | `node-1` | This node's id in the registry |
| `--port` | `8081` | Data/Query gRPC port; MCP (agents) is served on `port + 1` |
| `--root` | `~/.latiq` | Data root; pond storage under `<root>/ponds` |
| `--control` | `http://127.0.0.1:9090` | Control plane to register with |

---

## Drive it from the CLI

The CLI is a **gRPC client whose one entry point is the control plane.** Every command takes `--control` (default `http://127.0.0.1:9090`) and `--agent-id <name>` (the identity your writes are attributed to; default `anonymous`). You never pass a node address — the CLI resolves the node via the control plane and connects to it directly for data ops.

The examples below call `latiq` directly. For dev, put the build output on your PATH — every `cargo build` refreshes the binary in place, nothing to reinstall:

```bash
export PATH="$PWD/target/debug:$PATH"     # or target/release after `cargo build --release`
```

### Pond lifecycle

```bash
latiq pond create --name demo          # control plane picks a node; --name optional (auto-named)
latiq pond list                        # discover ponds (control-plane registry)
latiq pond describe demo               # metadata + table summary
latiq pond drop demo --confirm         # DESTRUCTIVE — requires --confirm
```

`pond drop` without `--confirm` is refused with a structured error and leaves the pond intact.

### Run SQL — one `query` command

`query` runs any statement: DDL/DML are attributed to your identity; a plain `SELECT` runs as a read (no snapshot). The pond's storage materializes on first touch.

```bash
latiq query --pond demo --agent-id alice "CREATE TABLE events(id INTEGER, sev VARCHAR)"
latiq query --pond demo --agent-id alice "INSERT INTO events VALUES (1,'high'),(2,'critical')"
latiq query --pond demo "SELECT id, sev FROM events ORDER BY id"

# Native DuckLake — history/attribution (who wrote what); nothing layered on top
latiq query --pond demo "SELECT snapshot_id, author, commit_message FROM pond.snapshots()"

# Catalog introspection is standard SQL (engine-portable)
latiq query --pond demo "SHOW TABLES"
latiq query --pond demo "SELECT column_name, data_type FROM information_schema.columns WHERE table_name='events'"
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
latiq query --pond demo "CREATE TABLE t AS SELECT * FROM read_csv('https://example.com/data.csv')"
latiq query --pond demo "SELECT count(*) FROM 's3://some-public-bucket/file.parquet'"
```

### Targeting a non-default control plane

```bash
latiq pond create --name demo --control http://127.0.0.1:19090
latiq query --pond demo --control http://127.0.0.1:19090 "SELECT 1"
```

---

## Operator CLI (node admin)

Inspect registered pond nodes (control plane); `--control` default `http://127.0.0.1:9090`.

```bash
latiq node list                # registered pond nodes (id, state, pond_count, mcp_endpoint)
latiq node describe node-1     # one node, pretty JSON
```

(Policy and audit commands were trimmed from the CLI for now — the Admin gRPC still serves them; commands return when needed.)

---

## The agent surface (MCP) — for reference

Agents (frontier LLMs), not the CLI, point an MCP client at the node's MCP endpoint (the Data port + 1):

```
http://127.0.0.1:8082/mcp     (Streamable HTTP transport)
```

Tools: `allocate_pond`, `describe_pond`, `list_ponds`, `drop_pond`, `read_query`, `write_query`, `explain_query` — plus `latiq://` resources and prompt SOPs for guidance. Results carry both a text block and `structuredContent`; errors set `isError` with the structured envelope. Identity is relaxed (Slice 0+): pass an optional `agent_id` argument; header-based/OIDC identity is a later slice. To exercise this surface programmatically in tests, use `latiq-client` (the agent-sim MCP client) — never from the CLI.

---

## What works now vs. later

**Now (Slice 0+ / M1–M11):** pond lifecycle, SQL read/write with native attribution, `explain`, native DuckLake metadata (`pond.snapshots()` for history/attribution; `SHOW TABLES` / `information_schema` for catalog introspection — nothing layered on top), query-by-URI ingestion of public files, query cancellation + prompt resource release, the completed MCP agent surface (tools + resources + prompts), the Data and Admin gRPC surfaces, and an audit log.

**Later slices:** external catalogs + credentials + federation, OIDC verification, Arrow Flight SQL streaming for large results, rate limiting, OpenTelemetry, multi-node, an SDK.

---

## Cleanup

```bash
# stop dev.sh with Ctrl+C, then:
rm -rf ./.latiq-dev
```
