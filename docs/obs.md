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

## 2. Tracing (request correlation)

Each request gets a `trace_id` (from an incoming `latiq-trace-id` gRPC metadata
header, or freshly generated at the edge). It is set as a task-local span field
for the whole request and **propagated across the node-to-node forward hop** (the
greeter stamps `latiq-trace-id` on its call to the owner). So one request's spans
share a `trace_id` across nodes.

**Correlate a request across nodes** by that id in your log stack:
```bash
# in a JSON log stream
jq 'select(.spans[]?.trace_id == "16d7e16e-a6bc-484b-954f-a611ba1e1ada")'
```

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
| `latiq_pond_errors_total` | counter | `pond`, `kind` | pond node | Errors **over time**, by `ErrorKind` |
| `latiq_build_info` | gauge | `version` | both | Always 1; carries the version label |

> **Cardinality:** the `pond` label is intentional (per-pond visibility). Series
> grow with pond count — fine at the target scale; a future flag can drop the
> label if a deployment has very many ponds.

### Useful PromQL

```promql
# Per-pond query rate (per second, 5-minute window)
rate(latiq_pond_queries_total[5m])

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
