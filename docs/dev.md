# Latiq — Developer Guide

> **Status:** Slice 0+ (M1–M11) complete and runnable, plus **multi-node request forwarding** (a front door over N nodes), **Arrow streaming reads** (the SDK path), and **per-pond resource tiers**. This is the hands-on guide for building Latiq, starting the dev stack, and driving it manually through the **CLI** (the gRPC surfaces). Agents drive the separate **MCP** surface; a packaged SDK is still a later slice (the Arrow stream is standard `pyarrow`-decodable today). **OAuth 2.1 token verification** ships too, off by default — see *Auth mode* below. Rate-limiting, OpenTelemetry, disk quotas, and authorization (who may reach which pond) are deferred.

## What you're running

Latiq is a two-process stack plus a CLI, all from one `latiq` binary:

- **control plane** (`latiq serve`) — the registry (nodes, ponds, policy). Serves **Control gRPC** (pond nodes) and **Admin gRPC** (operators/CLI) on one port. Never in the query path.
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
Control plane (CLI):  127.0.0.1:51400   (Control + Admin gRPC, one port)
Pond node Data gRPC:  127.0.0.1:51401
MCP (agents only):    http://127.0.0.1:51402/mcp
Root:                 ~/.latiq
```

Runtime artifacts land under `~/.latiq/` (registry at `registry.duckdb`, pond storage under `ponds/`) — the same default the CLI uses. Pass `--root /tmp/latiq-dev` for throwaway state.

`dev.sh` tracks its child PIDs under `<root>/dev.pids` and **self-cleans**: a normal start first sweeps any survivors from a prior run before the port preflight, so a stack that died hard (SIGKILL / closed terminal, which the `Ctrl+C` trap can't catch) no longer leaves orphaned nodes/nginx that block the next run. To tear a stale stack down without starting a new one:

```bash
./dev.sh --down                          # or: ./dev.sh --down --root /tmp/latiq-dev
```

After the sweep it preflights the ports and aborts (naming the culprit) if one is *still* taken — e.g. a process it doesn't manage — so conflicts fail loudly instead of producing confusing gRPC errors. Override ports via flags (`./dev.sh --help`); MCP is always the Data port + 1:

```bash
./dev.sh --server-port 41400 --data-port 41401 --root /tmp/latiq-dev
```

### Multiple nodes behind a front door

`--nodes N` starts the control plane plus N pond nodes and puts an **nginx front door** in front of them (needs `nginx` — `brew install nginx`):

```bash
./dev.sh --nodes 3
```

Node *i* binds Data port `data-port + 2*i` and MCP `+1`; the front door listens just past them and load-balances both surfaces. The banner prints the gateway and an `export LATIQ_QUERY_GATEWAY=…` line. With it set, the CLI sends data ops to the gateway instead of resolving the owning node — whichever node nginx picks resolves the pond's owner and **forwards** the request there, relaying the result back. This is the same single-front-door model agents use over MCP, and it mirrors production (k8s): outside clients reach a Service/LB, never an individual pod; node-to-node hops are internal.

```bash
export LATIQ_QUERY_GATEWAY=http://127.0.0.1:51405      # printed by dev.sh
latiq pond create --name demo                    # control plane picks a node
latiq query -p demo "SELECT 1"                   # via the gateway → forwarded to the owner
```

The MCP upstream is sticky (`ip_hash`) so a streamable-HTTP session stays on its greeter; the Data gRPC upstream is spread, since forwarding makes node choice irrelevant to correctness. `--nodes 1` (the default) keeps the single-node path with **no nginx dependency**.

### Auth mode (optional, debugging only)

The dev stack runs **unauthenticated** — identity is claimed, default `anonymous`.
That is the default everywhere: `cargo test`, the embedded SDK, and a plain
`docker compose up`. Token verification is opt-in by configuration.

```bash
./dev.sh --auth            # needs Docker
```

Starts a Keycloak container (realm imported from `deploy/cluster/keycloak-realm.json`),
waits for it, and exports `LATIQ_AUTH_ISSUER` + `LATIQ_AUTH_AUDIENCE` so the control
plane and every node it starts verify tokens on **all** surfaces — MCP, Data/Stream,
and Admin. The banner prints a `client_credentials` curl that mints a token; the CLI
and SDK pick up `$LATIQ_TOKEN` automatically (same as `--token`). Without a token
every call is refused: `401` + `WWW-Authenticate` on MCP, `Unauthenticated` carrying
the same challenge on gRPC.

This is for debugging by hand. Auth is otherwise exercised only by the **nightly**,
in containers (`docker compose --env-file auth.env up`, which also brings up an
in-network Keycloak). See [`docs/identity.md`](identity.md) for the design.

### Manual start (two terminals)

```bash
# Terminal 1 — control plane (Control + Admin gRPC on one port)
cargo run -p latiq -- serve --port 51400 --root ~/.latiq

# Terminal 2 — pond node (Data gRPC + MCP; registers at $LATIQ_SERVER, default :51400)
cargo run -p latiq -- node add --port 51401 --root ~/.latiq
```

### Server options

**`latiq serve`** (control plane)

| Flag | Default | Meaning |
|---|---|---|
| `--port` | `51400` | Control + Admin gRPC port (one port, both services) |
| `--root` | `~/.latiq` | Data root; registry at `<root>/registry.duckdb` |
| `--auth-issuer` | — | Trusted OIDC issuer (`$LATIQ_AUTH_ISSUER`). Repeatable / comma-separated for several IdPs. Any value turns verification on for this process; none = relaxed claimed identity |
| `--auth-audience` | — | The `aud` this deployment expects (`$LATIQ_AUTH_AUDIENCE`). **Required** whenever an issuer is set |
| `--auth-jwks-uri` | derived from the issuer | Explicit JWKS URL; only valid with exactly one issuer (split-horizon deployments) |

**`latiq node add`** (pond node)

| Flag | Default | Meaning |
|---|---|---|
| `--node-id` | `node-1` | This node's id in the registry |
| `--port` | `51401` | Data/Query gRPC port; MCP (agents) is served on `port + 1` |
| `--root` | `~/.latiq` | Data root; pond storage under `<root>/ponds` |
| `--advertise-addr` | `127.0.0.1:<port>` | The node's **internal** `host:port`, advertised to the control plane so peer nodes can forward pond requests. Agents never dial it |
| `--public-mcp-url` | derived from `--advertise-addr` | The URL **agents** dial, e.g. `https://latiq.example.com/mcp` (`$LATIQ_PUBLIC_MCP_URL`). Published as the RFC 9728 `resource` and in the 401 challenge — behind a gateway this must be the *gateway's* URL, or conforming clients reject the metadata document |
| `--auth-issuer` / `--auth-audience` / `--auth-jwks-uri` | — | As `latiq serve` above; turns verification on for MCP and Data/Stream |
| `--lineage-backend-url` | — | Also POST every lineage event to an OpenLineage receiver (`$LATIQ_LINEAGE_BACKEND_URL`) — the **full** endpoint, e.g. `http://marquez:5000/api/v1/lineage`. Validated at startup; additive, never in the query path, no credentials sent |

`--root` defaults to `~/.latiq` (pass `--root /data` to override); the node registers with the control plane at `$LATIQ_SERVER` (default `http://127.0.0.1:51400`). So `LATIQ_SERVER=… latiq serve --root /data` needs nothing more.

---

## Drive it from the CLI

The CLI is a **gRPC client whose one entry point is the control plane.** Its address comes from the `LATIQ_SERVER` env var (default `http://127.0.0.1:51400`) — set it once, there's no per-command flag. `--agent-id <name>` (the identity your writes are attributed to) lives only on the commands that record one: `query` and `pond create`. You never pass a node address — the CLI resolves the node via the control plane and connects to it directly for data ops. Set `$LATIQ_QUERY_GATEWAY` (a multi-node front door, see above) to send data ops there instead and let the greeter node forward. Against a deployment started with `--auth-issuer`, set `$LATIQ_TOKEN` (or pass the global `--token`) — the CLI presents it on every request, Admin as well as data ops; it is simply unused where no issuer is configured.

For dev, put the build output on your PATH (every `cargo build` refreshes it in place) and export the control address only if it's not the default:

```bash
export PATH="$PWD/target/debug:$PATH"           # or target/release after `cargo build --release`
export LATIQ_SERVER=http://127.0.0.1:51400      # only if you changed --server-port
```

### Pond lifecycle

```bash
latiq pond create --name demo                 # control plane picks a node; --name optional (auto-named)
latiq pond create --name big --tier large     # resource tier (default medium); caps the pond's memory + CPU
latiq pond create --name geo --extensions spatial,fts  # load DuckDB extensions on the pond
latiq pond create --name audited --lineage    # record OpenLineage provenance (fixed at creation; see below)
latiq pond list                               # discover ponds (control-plane registry)
latiq pond describe demo                       # metadata (incl. tier) + table summary
latiq pond drop demo --confirm                 # DESTRUCTIVE — requires --confirm
latiq pond forget stranded --confirm           # operator recovery: drop the RECORD of a pond whose node is gone
```

**Extensions** are baked into the deployment image. The **required standard** set is always loaded on every pond: `parquet`/`json` are statically linked into the binary, and `ducklake` (the catalog format) + `httpfs` (remote reads) load from the image — Latiq is built on these, so a node **ensures them at startup and refuses to serve if it can't load them**. The **optional** set (`spatial`, `fts`, `icu`, `inet`) is requested per pond and `LOAD`ed on open — never installed in the create path; a requested extension that isn't present fails fast. A node warms the optional cache once at startup (the dev stand-in for image-baking). Community/unsigned extensions are rejected.

**Resource tiers** cap a pond's DuckDB instance (memory + threads) — caps, not reservations:

| Tier | memory_limit | threads | concurrent reads |
|---|---|---|---|
| x-small | 512 MB | 1 | 4 |
| small | 1 GB | 2 | 4 |
| **medium** (default) | 4 GB | 4 | 8 |
| large | 16 GB | 8 | 16 |
| x-large | 32 GB | 16 | 32 |
| `none` | — | — | host (up to 32) |

`none` applies **no caps at all** — the engine's own defaults govern the pond
(DuckDB: every core, ~80% of RAM). It is operator-only and cannot be requested
when a pond is created: `latiq pond set-tier <pond> --tier none`. Re-tiering takes
effect on the pond's next query.

"Concurrent reads" is how many queries run at once on one pond (its read-connection
pool). Readers past that wait briefly for a connection.

`pond drop` without `--confirm` is refused with a structured error and leaves the pond intact.

**A pond whose node is gone.** A pond is owned by exactly one node, and a node
serves a pond only when the registry names it as the owner. If the owning node's
registration is gone, every request for that pond is refused with
`pond_unavailable` rather than served — a node with no claim to the pond would
otherwise create an empty one of the same name and answer out of it. `pond drop`
cannot help there either: it forwards to the owner, and there is none. `latiq
pond forget <pond> --confirm` (Admin gRPC, the operator surface) removes the
**registry record** so the name is free again. **It deletes no data:** whatever
that pond had on the departed node's disk is still there, now orphaned, and
reclaiming it is a manual job on that host. It is refused while the owning node
is registered and active — then the pond is servable and `pond drop` is the verb
that deletes the data properly.

### Run SQL — one `query` command

`query` runs any statement: DDL/DML are attributed to your identity; a plain `SELECT` runs as a read (no snapshot). The pond's storage materializes on first touch.

Latiq manages the transaction on the write path — send plain statements (several in one call is fine) and leave out `BEGIN`/`COMMIT`/`ROLLBACK`/`START TRANSACTION`. The author is recorded just before Latiq's own commit, so your `COMMIT` ends the transaction first and the write lands in history with no author. Reads reject transaction control outright; writes don't police it.

```bash
latiq query --pond demo --agent-id alice "CREATE TABLE events(id INTEGER, sev VARCHAR)"
latiq query --pond demo --agent-id alice "INSERT INTO events VALUES (1,'high'),(2,'critical')"
latiq query --pond demo "SELECT id, sev FROM events ORDER BY id"

# Native DuckLake — history/attribution (the catalog is named after the pond).
# `author` is the verified subject where the caller authenticated, the claimed agent
# id otherwise; `commit_extra_info` carries the evidence (agent_id, issuer, verified).
# Read both — `author` alone can't tell a verified writer from one claiming the name.
latiq query --pond demo "SELECT snapshot_id, author, commit_message, commit_extra_info FROM demo.snapshots()"

# Catalog introspection is standard SQL (engine-portable)
latiq query --pond demo "SHOW TABLES"
latiq query --pond demo "SELECT column_name, data_type FROM information_schema.columns WHERE table_name='events'"
```

The SQL is a positional argument; `--pond` (`-p`) is required. Read results print as a **table** by default (`--format json` / `-f json` for raw `{columns, rows, statement, status, _meta}`); writes print `ok (snapshot N)`. Most flags have short forms — `-p` pond, `-n` name, `-a` agent-id, `-f` format. Errors print the structured envelope:

```
error [pond_not_found]: Pond 'ghost' does not exist.
  suggest: Call list_ponds to see available ponds, or allocate_pond to create one.
  see: latiq://troubleshooting/pond-not-found
```

### Lineage (OpenLineage, opt-in per pond)

A pond allocated with `--lineage` records an OpenLineage event pair (a `START` plus a `COMPLETE`/`FAIL`/`ABORT`) for **every** query — reads included — as batched JSONL in its own `lineage/` directory. The flag is chosen at creation and **fixed for the pond's lifetime**; ponds without it pay nothing.

```bash
latiq pond create --name audited --lineage
latiq query --pond audited --agent-id alice "CREATE TABLE t(id INTEGER)"
latiq query --pond audited --agent-id alice "INSERT INTO t VALUES (1),(2)"
latiq query --pond audited "SELECT count(*) FROM t"
latiq pond list -f json                       # each pond's `lineage` flag (describe reports it too)
ls ~/.latiq/ponds/<pond-id>/lineage/          # the event files: {unix-millis}-{uuid}.jsonl
```

**Reading it back is `get_lineage`, an agent tool** — "where did this data come from?" is an agent question, so it lives on MCP and there is deliberately no CLI command for it (invariant 1: the CLI is not an agent). The Data gRPC `GetLineage` RPC exists only so a node that doesn't own the pond can forward to the one that does — the events are files on the node that ran the query. Point an MCP client at the node (`http://127.0.0.1:51402/mcp`, see *The agent surface* below) and call `get_lineage {pond:'audited'}` — newest first, events returned verbatim, with the pond's `lineage_dir` in the response; `limit`/`since`/`before` page it (`latiq-client` is the agent-sim client to drive it from a test).

A raw `ls` may show nothing right after a query: events are written a batch at a time (64) and on shutdown. `get_lineage` flushes that pond's buffer before reading, so it always sees the query you just ran. To also POST every event to an OpenLineage backend, start the node with `--lineage-backend-url http://localhost:5000/api/v1/lineage` (`$LATIQ_LINEAGE_BACKEND_URL`) — additive, never in the query path, no credentials sent.

### Query-by-URI ingestion (public files)

The pond node loads DuckDB's `httpfs`, `parquet`, and `json` extensions, so you can read public/anonymous files straight into a pond — no catalog needed (credentialed/federated sources are a later slice):

```bash
latiq query --pond demo "CREATE TABLE t AS SELECT * FROM read_csv('https://example.com/data.csv')"
latiq query --pond demo "SELECT count(*) FROM 's3://some-public-bucket/file.parquet'"
```

### Datasets & catalogs

Latiq has two ways to get external data into a pond: **datasets** (simple files
you copy in) and **catalogs** (external databases you pull from once). Full guide:
[`docs/dataset.md`](dataset.md).

```bash
# datasets — simple files in the built-in `latiq` catalog
latiq dataset list                                 # samples are seeded (tpch, …)
latiq dataset add sales --table sales=https://example.com/sales.parquet --tag finance
latiq dataset load tpch -p demo                     # copy all 8 TPC-H tables into `demo`

# catalogs — external (iceberg/…); credentials ride in at pull, never stored
latiq catalog add lake --type iceberg \
  --set endpoint=https://polaris.acme/api/catalog --set warehouse=prod --tag prod
latiq catalog describe lake -p demo --set token="$BEARER"
latiq catalog pull lake -p demo --set token="$BEARER" \
  --query "CREATE TABLE us AS SELECT * FROM lake.sales.orders WHERE region='us'"
```

`dataset add`/`catalog add` are operator actions; loading/pulling are available to
anyone. A `--set token=…` at `add` is **dropped** (credentials never persist) — pass
it at pull/describe. Credentialed identity integration: issue #26.

### Targeting a non-default control plane

```bash
export LATIQ_SERVER=http://127.0.0.1:41400
latiq pond create --name demo
latiq query --pond demo "SELECT 1"
```

---

## Arrow streaming (for the SDK / large reads)

Reads stream end-to-end as **Arrow batches** — DuckDB → owning node → (greeter, if
forwarded) → client — so large results aren't buffered. Internally every read
rides this Arrow hop (the CLI's `query` routes `SELECT`s to it); the MCP/CLI
surfaces just collect it back to JSON at the edge (bounded by the 10k inline cap),
while an SDK can pull the **raw Arrow stream, uncapped**.

The transport is a server-streaming gRPC RPC — `latiq.v1.Stream/ReadArrow` — that
carries **Arrow IPC** chunks. It is **not** the Flight protocol, but the payload is
standard Arrow, so any Arrow library decodes it. It **shares the Data gRPC port**
(and the multi-node front door), so there's no extra endpoint: point a client at
the same address the CLI uses.

A Python client is just gRPC + `pyarrow` (no Latiq SDK package needed yet):

```python
# ReadArrow(QueryRequest{pond, sql}) → stream of ArrowChunk{ipc: bytes}
import pyarrow as pa
buf = b"".join(chunk.ipc for chunk in stub.ReadArrow(QueryRequest(pond="demo", sql="SELECT * FROM t")))
table = pa.ipc.open_stream(buf).read_all()   # → pa.Table
df = table.to_pandas()
```

(Concatenating then decoding is the simple form; a streaming client feeds each
`chunk.ipc` to an incremental `pa.ipc` reader to avoid buffering.)

## Operator CLI (node admin)

Inspect registered pond nodes (control plane, at `$LATIQ_SERVER`).

```bash
latiq node list                # id, state, pond_count, heartbeat age, mcp_endpoint
latiq node describe node-1     # one node, pretty JSON (incl. last_heartbeat)
```

**Node liveness.** Pond nodes heartbeat the control plane every 10s. A **reaper**
on the control plane flips a node to `down` after **30s** without a heartbeat
(3 missed beats); the node's next heartbeat/restart revives it to `active`.
Placement (`pond create`) only picks `active` nodes, so a dead node stops
receiving ponds automatically. `pond_count` is computed live from assignments.

### `latiq stats` — system snapshot

A one-shot health view: node states + heartbeat age, pond totals, ponds by tier.

```bash
latiq stats              # rendered dashboard (color on a TTY)
latiq stats -f json      # raw snapshot for scripts
```

```
 ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  latiq · system snapshot
 ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

   nodes  2 total · 1 active · 1 down
   ponds  5  ·  large 1 · medium 4

   NODE        STATE   PONDS  LAST BEAT    ENDPOINT
   node-0      active     3        0s ago  http://127.0.0.1:51402/mcp
   node-1      down       2       39s ago  http://127.0.0.1:51404/mcp
```

(Per-node CPU/memory and per-pond query metrics are the next pass — a Prometheus
`/metrics` endpoint. This snapshot is registry state only.)

(Policy commands were trimmed from the CLI for now — the Admin gRPC still serves them; commands return when needed. Access auditing is not a registry capability — each access is a structured trace on the `latiq::access` log target (the pond node and the Admin surface both write it, with identical fields); operators grep the log files. Records carry `outcome=ok|error`, so failures and rejected calls show up too.)

---

## Observability (logging + metrics)

> Full posture, metric reference, PromQL, and Prometheus/Grafana setup: **[`docs/obs.md`](obs.md)**.

**Logging.** Both server roles use `tracing`. Level is `$RUST_LOG` (default `info`,
e.g. `RUST_LOG=latiq_agent_core=debug`). Format is human-readable by default, or
structured JSON for log aggregators:

```bash
LATIQ_LOG_FORMAT=json latiq serve …      # JSON lines for Loki/ELK/Datadog
```

**Metrics (Prometheus).** Each process serves `GET /metrics` on **its port + 1000**
(control plane `51400→52400`, a node `51401→52401`), overridable with
`--metrics-port`. `dev.sh` writes a ready-to-use scrape config (60s = per-minute)
and prints the endpoints:

```bash
./dev.sh --nodes 3
prometheus --config.file=~/.latiq/prometheus.yml     # path printed by dev.sh
curl -s http://127.0.0.1:52401/metrics | grep latiq_
```

Counters give **over time** (`rate(latiq_pond_queries_total[1m])`,
`increase(latiq_pond_errors_total[1m])` — per minute over any range); gauges give
the **latest** snapshot. The set:

| Metric | Type | Labels | Where |
|---|---|---|---|
| `latiq_nodes` / `latiq_ponds` / `latiq_ponds_total` | gauge | `state` / `tier` / — | control plane |
| `latiq_pond_allocations_total` / `latiq_nodes_reaped_total` | counter | — | control plane |
| `latiq_process_cpu_percent` / `latiq_process_memory_bytes` | gauge | — | both |
| `latiq_node_open_ponds` / `latiq_inflight_queries` | gauge | — | node |
| `latiq_pond_inflight_queries` | gauge | `pond` | node — live per-pond load |
| `latiq_pond_queries_total` | counter | `pond`, `op` | node — load over time |
| `latiq_pond_errors_total` | counter | `pond`, `kind` | node — errors over time |

The operator runs Prometheus (scrape + 1-day retention) and Grafana; Latiq stores
no history. (Distributed-trace spans + OTLP export are a later add-on.)

## The agent surface (MCP) — for reference

Agents (frontier LLMs), not the CLI, point an MCP client at the node's MCP endpoint (the Data port + 1):

```
http://127.0.0.1:51402/mcp     (Streamable HTTP transport)
```

Tools: `allocate_pond`, `describe_pond`, `list_ponds`, `drop_pond`, `read_query`, `write_query`, `explain_query`, `get_lineage` (the pond's own provenance) — plus `latiq://` resources and prompt SOPs for guidance. Results carry both a text block and `structuredContent`; every tool also declares an `outputSchema` (generated from the type it serializes, so it cannot drift) describing that success shape. Errors set `isError` with the structured envelope, which is deliberately OUTSIDE the declared schema — conforming MCP clients skip output-schema validation on an error result. **Identity arrives in the transport, never in a tool argument** — the claimed leaf is the `latiq-agent-id` HTTP header, and a verified principal is `Authorization: Bearer`. With no issuer configured (the dev default) identity stays claimed, default `anonymous`. To exercise this surface programmatically in tests, use `latiq-client` (the agent-sim MCP client) — never from the CLI.

---

## What works now vs. later

**Now (Slice 0+ / M1–M11 + forwarding + Arrow streaming + tiers):** pond lifecycle, SQL read/write with native attribution, `explain`, native DuckLake metadata (`pond.snapshots()` for history/attribution; `SHOW TABLES` / `information_schema` for catalog introspection — nothing layered on top), query-by-URI ingestion of public files, query cancellation + prompt resource release, the completed MCP agent surface (tools + resources + prompts), the Data and Admin gRPC surfaces, structured access traces (`latiq::access` log target, with `outcome`), **OAuth 2.1 authentication across all three surfaces** (opt-in via `--auth-issuer`; the caller's token is replayed on the node hop and re-verified by the owner), **multi-node forwarding behind a front door** (any node greets, resolves the owner, forwards), **Arrow streaming reads** (`Stream/ReadArrow`, Arrow IPC over our own gRPC — uncapped for the SDK; MCP/CLI collect to JSON at the edge), **per-pond resource tiers** (`--tier`; caps the pond's DuckDB memory + threads), and **opt-in per-pond OpenLineage lineage** (`--lineage`; JSONL in the pond's own `lineage/`, read over MCP with `get_lineage`, optional HTTP backend).

**Later slices:** authorization (pond ownership + grants bound to the verified subject), rate limiting, OpenTelemetry, node-liveness reaping (a crashed node currently stays `active`), disk quotas, a packaged SDK (and, if generic Flight/ADBC interop is ever needed, the Flight protocol on top of the existing Arrow stream).

---

## Cleanup

```bash
# stop dev.sh with Ctrl+C, then (removes the registry + all pond storage):
rm -rf ~/.latiq
```
