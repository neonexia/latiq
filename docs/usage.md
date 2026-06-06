# Latiq — Usage & Manual Testing Guide

> **Status:** Slice 0+ (milestones M1–M6 complete). This covers what you can run and test **today**: a two-process Latiq stack (control-plane + one pond-node) driven by the `latiq` CLI. Federation/catalogs, OIDC, rate-limiting, and OpenTelemetry are later slices.

## What Latiq is (for testers)

An agent allocates a **pond** (a lightweight DuckLake workspace), writes and reads SQL in it, and its writes are attributed to its identity — all over MCP. Operators run the system and inspect it over a separate admin surface. The `latiq` binary is everything: the two server roles, the operator CLI, and an agent client CLI.

---

## Prerequisites

- **Rust** (stable, 1.90+). `cargo --version`.
- **protobuf compiler** `protoc` (for the gRPC build): `brew install protobuf` (macOS).
- First build compiles **DuckDB from source** (the `bundled` feature) — expect a few minutes once; it's cached after.

```bash
git clone git@github.com:neonexia/latiq.git
cd latiq
cargo build           # first build is slow (DuckDB); subsequent builds are fast
```

---

## Quick start (one command)

```bash
./dev.sh
```

This builds `latiq`, starts the **control-plane** (Control gRPC `:9090`, Admin gRPC `:9091`) and a **pond-node** (MCP `:8080`), and prints the endpoints + example commands. Leave it running; open a second terminal for the CLI. `Ctrl+C` stops the stack.

Runtime artifacts land in `./latiq-cp.duckdb` (registry) and `./latiq-data/` (pond storage) — both gitignored. Override with `LATIQ_DB` / `LATIQ_DATA`.

---

## Manual start (two terminals)

If you prefer to run the roles yourself:

```bash
# Terminal 1 — control plane (registry + Control/Admin gRPC)
cargo run -p latiq -- control-plane --db ./latiq-cp.duckdb

# Terminal 2 — pond node (serves the agent MCP surface, registers with the CP)
cargo run -p latiq -- pond-node --data-dir ./latiq-data
```

Defaults: control-plane `--control-addr 127.0.0.1:9090 --admin-addr 127.0.0.1:9091`; pond-node `--node-id node-1 --mcp-addr 127.0.0.1:8080 --control http://127.0.0.1:9090`.

---

## Agent client CLI (connect to the server, issue queries)

These speak the **agent MCP surface** at `http://127.0.0.1:8080/mcp` (acting as an agent). Use `--agent-id <name>` to set the identity your writes are attributed to (defaults to `anonymous`).

```bash
BIN="cargo run -q -p latiq --"

# Allocate a pond
$BIN pond create --name demo

# Write data (DDL + insert), attributed to an agent identity
$BIN write --pond demo --agent-id alice "CREATE TABLE events(id INTEGER, sev VARCHAR)"
$BIN write --pond demo --agent-id alice "INSERT INTO events VALUES (1,'high'),(2,'critical')"

# Read it back (returns rows + a _meta envelope)
$BIN query --pond demo "SELECT id, sev FROM events ORDER BY id"

# See who wrote what — native DuckLake attribution, exposed via _latiq
$BIN query --pond demo "SELECT snapshot_id, author, commit_message FROM _latiq.attribution"

# Plan a query without running it
$BIN explain --pond demo "SELECT * FROM events WHERE sev = 'critical'"

# Discover ponds / inspect one
$BIN pond list
$BIN pond describe demo

# Drop it
$BIN pond drop demo
```

Successful results print as pretty JSON (`rows`, `columns`, `statement`, `status`, `_meta`). Errors print the structured envelope:

```
error [pond_not_found]: Pond 'ghost' does not exist.
  suggest: Call list_ponds to see available ponds, or allocate_pond to create one.
  see: latiq://troubleshooting/pond-not-found
```

### Ingesting public files (query-by-URI)

The pond node loads DuckDB's `httpfs`, `parquet`, and `json` extensions, so you can read public/anonymous files directly into a pond — no catalog needed (federation/credentialed sources are a later slice):

```bash
$BIN write --pond demo "CREATE TABLE t AS SELECT * FROM read_csv('https://example.com/data.csv')"
$BIN query --pond demo "SELECT count(*) FROM 's3://some-public-bucket/file.parquet'"
```

---

## Operator CLI (admin surface)

These speak the **Admin gRPC** at `127.0.0.1:9091` — a separate surface from the agent MCP path.

```bash
$BIN node list                       # registered pond nodes
$BIN node describe node-1
$BIN policy show                     # deployment policy (defaults seeded)
$BIN policy set query_timeout_seconds 45
$BIN audit tail --limit 20           # recent operations (SQL shape redacted, identities, durations)
$BIN audit search alice              # audit for one identity
```

---

## Connecting your own MCP client / agent framework

Point any MCP client (or a framework's built-in MCP client) at:

```
http://127.0.0.1:8080/mcp     (Streamable HTTP transport)
```

Tools exposed: `allocate_pond`, `describe_pond`, `list_ponds`, `drop_pond`, `read_query`, `write_query`, `explain_query`. Results carry both a text content block and `structuredContent`; errors set `isError` with the structured envelope. **Identity (Slice 0+):** pass an optional `agent_id` argument on any tool call (relaxed; defaults to `anonymous`). Header-based / OIDC identity arrives in a later slice.

---

## What works now vs. later

**Now (Slice 0+):** pond lifecycle, SQL read/write with native attribution, `explain`, the `_latiq` schema (`snapshots`, `attribution`, `tables_summary`, `sources`), query-by-URI ingestion of public files, query cancellation, the operator admin surface, and an audit log.

**Later slices:** external catalogs + credentials + federation, OIDC verification, rate limiting, OpenTelemetry, multi-node, MCP Prompts, the full `latiq://` recipe set.

---

## Cleanup

```bash
# stop dev.sh with Ctrl+C, then:
rm -rf ./latiq-data ./latiq-cp.duckdb
```
