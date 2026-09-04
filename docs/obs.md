# Latiq — Observability

> **Posture:** Latiq *emits* the three signals; the operator's stack *stores and
> renders* them. Latiq ships no built-in dashboards, alerting, or time-series
> storage — it exposes standard interfaces (a Prometheus `/metrics` endpoint,
> `tracing` logs) and you point your existing Prometheus / Grafana / Loki at them.
> This keeps Latiq boring and fits whatever monitoring stack you already run.

| Signal | What Latiq does | Where it's stored / viewed |
|---|---|---|
| **Logs** | structured `tracing` (text or JSON), with `trace_id` + `op`/`pond` fields | your log aggregator (Loki/ELK/Datadog) |
| **Metrics** | a Prometheus `/metrics` endpoint per process (pull) | Prometheus (scrape + retention) + Grafana |
| **Traces** | a `trace_id` per request, propagated across the node hop, in the logs | correlate by `trace_id` in your log stack (OTLP export is a future add-on) |

---

## 1. Logging

Both server roles (`latiq serve`, `latiq node add`) use `tracing`. Client CLI
commands stay quiet.

- **Level:** `$RUST_LOG` (default `info`). E.g. `RUST_LOG=latiq_agent_core=debug,info`.
- **Format:** human-readable by default; **JSON** for aggregators:
  ```bash
  LATIQ_LOG_FORMAT=json latiq serve …
  ```
- **Key fields:** every request logs `op` (read_query / write_query / read_arrow /
  describe_pond / drop_pond), `pond`, and — when multi-node — whether it was
  `forwarding to owner node` or `processing locally` (with `owner`). The enclosing
  span carries `trace_id`.

Ship stdout to your aggregator (k8s: the container log → Fluent Bit/Vector →
Loki/ELK). Nothing Latiq-specific to configure.

### Access auditing (the `latiq::access` trail)

There is **no audit table** — every access is a structured `tracing` event on the
**`latiq::access`** target (one per operation: allocate/describe/list/drop pond,
read/write/explain, dataset load, catalog pull/describe, and the Admin surface's
operator actions — pure registry browsing, `list_datasets`/`list_catalogs` and
their `get_*`, is deliberately not audited: no pond, no identity). Each carries
`agent` (the caller's **claim**), `subject`/`issuer` (verified, empty when not),
`verified`, `op`, `pond`, `trace_id`, `duration_ms`, a redacted `summary` (SQL
shape with literals replaced by `?`), and `outcome` (`ok`/`error` — failures and
rejected calls are recorded too, not only successes). Operators grep the log files (or
query them in their log stack); with `LATIQ_LOG_FORMAT=json` the fields are
structured.

To ask *who* did something, filter on `subject=` **together with**
`verified=true`: `agent=` is the caller's own claim and carries no authority.
A streaming read (`read_arrow`) is recorded when the stream is **established**,
not when it finishes — so `duration_ms` there measures establishment and
`outcome` says whether the read started.

To follow **one request**, filter on `trace_id=`. A request that lands on a node
which does not own the pond is forwarded, and it is the **owner** that records
it (the greeter returns before its own audit, so attribution stays on the node
that ran the op) — under the trace id the client's request carried, which the
greeter propagates. Without that field the record sits on a node the client
never dialled with nothing tying it back to the request that caused it.
`trace_id="-"` only where there is no trace to inherit: an Admin call whose
caller sent no `traceparent`, and auth rejections (recorded at the door, before
the handler enters the scope).

Note that the **op can change across the hop**: an agent's `read_query` reaches
the owner as `read_arrow`, because a forwarded read rides the Arrow stream. The
id is what joins the two records; the op name is not.

```bash
# tail just the access trail
RUST_LOG=latiq::access=info latiq node add …
# or filter JSON logs by the target
jq 'select(.target == "latiq::access")'
# everything agent-x did
jq 'select(.target == "latiq::access" and .fields.agent == "agent-x")'
```

## 2. Tracing (request correlation)

Latiq speaks **W3C Trace Context**. Send a standard `traceparent` — an HTTP
header on MCP, gRPC metadata on Data/Stream/Admin — and every record Latiq writes
for that request carries its `trace_id` (32 hex digits). Send none and one is
minted at the edge. One spelling on every surface, so an existing collector or
agent orchestrator needs no adapter.

The context is a task-local for the whole request, and is stamped back onto every
outbound hop: the node-to-node forward, the node → control-plane call, and the
control plane → node call that materialises a pond. So one request's spans share
a `trace_id` across processes — and so do the access record the owner writes and
the OpenLineage events it emits.

An inbound `traceparent` is **attribution-grade, not authority-grade**: it is
recorded and propagated, never consulted for an access decision, and a malformed
one is replaced rather than refused (a trace id is not worth failing a query
over). The span id is always Latiq's own, so a caller cannot forge our spans.

**The id comes back to the caller**, which is what makes it usable from the agent
side rather than only from the log stack:

- `_meta.trace_id` on every query result.
- `trace_id` on the `ErrorEnvelope` of every failed call — an agent that cannot
  cite the id of its own failed request cannot ask anyone about it.
- `run.facets.latiq_query.traceId` on every OpenLineage event.

The response says who ran it: `_meta.served_by` on every query result names the
node that **actually executed** the statement (its advertised internal endpoint,
or `in-process` where a node advertises none). On a forwarded request that is the
OWNER, not the node the client dialled — the cheapest way to see, from the client
side, which node did the work. The Arrow stream carries the same value on its
first chunk.

**Correlate a request across nodes** by the `trace_id` — the one on the span, on
the access record, on the response's `_meta`, and on the envelope of a failure.
It is `served_by`'s complement, not the same thing: `served_by` says WHICH node
ran one statement, `trace_id` gathers EVERY record of one request wherever it
landed.

```bash
# every record of one request, on every node, whatever the op was called there
jq 'select(.fields.trace_id == "4bf92f3577b34da6a3ce929d0e0e4736")'
# or by the enclosing span, which carries it too
jq 'select(.spans[]?.trace_id == "4bf92f3577b34da6a3ce929d0e0e4736")'
```

Start from an id you were handed: `_meta.trace_id` on a slow result, or
`trace_id` on the error an agent is stuck on.

OTLP export to a collector (Jaeger/Tempo) is a planned add-on; today the trace id
lives in the structured logs.

## 3. Metrics (Prometheus)

Each process serves `GET /metrics` (Prometheus text) on **its main port + 1000**
(control plane `51400 → 52400`; a pond node `51401 → 52401`). Override with
`--metrics-port`.

**Counters** give *over time* — `rate()` / `increase()` over any window (a day at
per-minute resolution if you scrape every 60s). **Gauges** give the *latest*
snapshot, refreshed by a 5s in-process collector.

### Metric reference

| Metric | Type | Labels | Process | Meaning |
|---|---|---|---|---|
| `latiq_nodes` | gauge | `state` | control plane | Nodes by liveness state (`active`/`down`) |
| `latiq_ponds` | gauge | `tier` | control plane | Ponds by resource tier |
| `latiq_ponds_total` | gauge | — | control plane | Total ponds |
| `latiq_pond_allocations_total` | counter | — | control plane | Ponds allocated (lifetime) |
| `latiq_nodes_reaped_total` | counter | — | control plane | Reaper down-markings |
| `latiq_process_cpu_percent` | gauge | — | both | Process CPU% (can exceed 100% across cores) |
| `latiq_process_memory_bytes` | gauge | — | both | Process resident memory |
| `latiq_node_open_ponds` | gauge | — | pond node | DuckDB instances open on the node |
| `latiq_inflight_queries` | gauge | — | pond node | In-flight ops on the node (all ponds) |
| `latiq_pond_inflight_queries` | gauge | `pond` | pond node | **Live** query load on an owned pond |
| `latiq_pond_queries_total` | counter | `pond`, `op` | pond node | Query load **over time** |
| `latiq_pond_query_duration_seconds` | histogram | `pond`, `op` | pond node | Query wall-clock latency (engine exec) → p50/p95/p99 |
| `latiq_pond_errors_total` | counter | `pond`, `kind` | pond node | Errors **over time**, by `ErrorKind` |
| `latiq_forwarded_total` | counter | `op` | pond node | Ops forwarded to another node (multi-node path). `op` is the op as the **caller** invoked it — the same name the `latiq::access` trail records, so a spike here is greppable there. Allocation is absent on purpose: a pond placed on another node is never dialled. |
| `latiq_build_info` | gauge | `version` | both | Always 1; carries the version label |

> **Cardinality:** the `pond` label is intentional (per-pond visibility). Series
> grow with pond count — fine at the target scale; a future flag can drop the
> label if a deployment has very many ponds.

### Useful PromQL

```promql
# Per-pond query rate (per second, 5-minute window)
rate(latiq_pond_queries_total[5m])

# p95 query latency per pond (5-minute window)
histogram_quantile(0.95, sum by (pond, le) (rate(latiq_pond_query_duration_seconds_bucket[5m])))

# Forwarding rate by op — how much traffic crosses node boundaries (multi-node)
sum by (op) (rate(latiq_forwarded_total[5m]))

# Errors per minute over the last day, by pond + kind
increase(latiq_pond_errors_total[1m])

# Cluster: active vs down nodes
latiq_nodes{state="active"}   /   sum(latiq_nodes)

# Busiest ponds right now
topk(5, latiq_pond_inflight_queries)

# Per-node memory
latiq_process_memory_bytes
```

## 4. Running Prometheus

`dev.sh` writes a ready-to-use config at `<root>/prometheus.yml` (scrape targets =
every process's metrics port, 60s interval) and prints the path:

```bash
./dev.sh --nodes 3
prometheus --config.file=~/.latiq/prometheus.yml \
           --storage.tsdb.retention.time=1d        # keep ≥ your dashboard window
```

For a real deployment, see [`deploy/prometheus.example.yml`](../deploy/prometheus.example.yml)
— a static-target template. In Kubernetes, prefer the Prometheus Operator + a
`ServiceMonitor` selecting the metrics port instead of static targets. Point
Grafana at Prometheus and chart the PromQL above; point your log pipeline at the
JSON logs and correlate by `trace_id`.

## 5. What's deferred
- **OTLP trace export** (push spans to a collector) — today traces live in the logs.
- **Per-pond CPU/memory** — DuckDB doesn't expose it; CPU/memory is per-node.
- **Shipped Grafana dashboards** — we expose the metrics; dashboards are yours.
