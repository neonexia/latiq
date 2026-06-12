# Observability: structured logging + tracing spans + Prometheus metrics

**Date:** 2026-06-12
**Goal:** Industry-standard logging (JSON), distributed tracing (spans + a trace
ID across the forward hop), and Prometheus metrics (`/metrics` per process) so
operators get per-node CPU/memory, system state, and per-pond query load/errors —
"latest" *and* over time (Prometheus retains the 1-day/per-minute history).

## Decisions (locked)
- **Logging:** keep `tracing`; add a **JSON formatter** toggled by
  `LATIQ_LOG_FORMAT=json|pretty` (default `pretty`). `RUST_LOG` still controls level.
- **Tracing:** `#[instrument]` spans, a **request/trace id** propagated node→node
  via gRPC metadata and included on log lines. **No OTLP** (can bolt on later).
- **Metrics:** Prometheus pull. Each process serves **`/metrics`** on a dedicated
  port. Counters give over-time (`rate`/`increase`); gauges give latest. Prometheus
  (operator-run) does the per-minute/1-day retention.

## Metric set

### Control plane (`/metrics`)
| Metric | Type | Labels | Description |
|---|---|---|---|
| `latiq_nodes` | gauge | `state` | Nodes by liveness state |
| `latiq_ponds` | gauge | `tier` | Ponds by tier |
| `latiq_ponds_total` | gauge | — | Total ponds |
| `latiq_pond_allocations_total` | counter | — | Ponds allocated (lifetime) |
| `latiq_nodes_reaped_total` | counter | — | Reaper down-markings |
| `latiq_process_cpu_percent` | gauge | — | Control-plane process CPU% |
| `latiq_process_memory_bytes` | gauge | — | Control-plane process RSS |
| `latiq_build_info` | gauge | `version` | Always 1; carries version |

### Pond node (`/metrics`)
| Metric | Type | Labels | Description |
|---|---|---|---|
| `latiq_process_cpu_percent` | gauge | — | Node process CPU% |
| `latiq_process_memory_bytes` | gauge | — | Node process RSS |
| `latiq_node_open_ponds` | gauge | — | DuckDB instances open on this node |
| `latiq_inflight_queries` | gauge | — | In-flight ops on this node (all ponds) |
| `latiq_pond_inflight_queries` | gauge | `pond` | Queries running *now* on an owned pond (latest load) |
| `latiq_pond_queries_total` | counter | `pond`, `op` | Query load over time (`rate[1m]`) |
| `latiq_pond_errors_total` | counter | `pond`, `kind` | Errors over time (`increase[1m]`), by `ErrorKind` |
| `latiq_build_info` | gauge | `version` | Always 1 |

Per-pond labels are intentional (the user wants per-pond). Cardinality grows with
pond count — acceptable for the target scale; noted as a future opt-out if needed.

## Components

### 1. metrics plumbing (`latiq-metrics` or inline)
- `metrics` facade + `metrics-exporter-prometheus` (`PrometheusBuilder` →
  `PrometheusHandle`; we `handle.render()` the text). No tonic in this crate
  (hyper-free path — we serve the rendered text ourselves via axum), so no
  version-skew risk.
- A tiny `serve_metrics(addr, handle, collector)` that serves `GET /metrics`
  (axum) returning `handle.render()`.

### 2. Collector (periodic gauge sampler)
A background task (every ~5s) that sets the gauges:
- **Both processes:** `sysinfo` → `process_cpu_percent`, `process_memory_bytes`.
- **Pond node:** `inflight_queries` (in-flight registry len), `node_open_ponds`
  (engine instance count — new `QueryEngine::open_pond_count()`),
  `pond_inflight_queries{pond}` (per-pond in-flight, from the registry).
- **Control plane:** `nodes{state}`, `ponds{tier}`, `ponds_total` (registry counts).

### 3. Counter call sites (agent-core)
On the **owner** (local execution path, not forward): `pond_queries_total{pond,op}++`
on each executed query; `pond_errors_total{pond,kind}++` on a returned error. The
`metrics` facade is observability (like `tracing`), so agent-core may depend on it
without breaking invariant 5 (no transport types).

### 4. Logging + spans
- `init_tracing()` gains the JSON layer (env-toggled).
- `#[instrument]` (or manual spans) on the inbound adapters + AgentOps ops, with a
  `trace_id` field. The forwarder injects the trace id into gRPC metadata
  (`latiq-trace-id`); the receiving service extracts it into its span. So one
  request's spans across nodes share a trace id (correlate in the log stack).

### 5. Surfaces / config
- `serve` + `node add` gain `--metrics-port` (default: main port + 1000; e.g.
  CP 51400→52400, node data 51401→52401). A metrics server starts there.
- `dev.sh`: print each process's `/metrics` URL + write a ready-to-scrape
  `prometheus.yml` under the root (targets = the metrics ports). No container
  orchestration in dev.sh (operator runs Prometheus/Grafana; we hand them the config).

## Testing
- metrics: a unit that records a counter/gauge and asserts `render()` contains the
  series; the collector sets process gauges (non-zero memory).
- e2e: `/metrics` endpoint returns 200 + `latiq_` series after a query runs
  (`latiq_pond_queries_total` present for the pond).
- logging: JSON format toggles (a line parses as JSON when `LATIQ_LOG_FORMAT=json`).
- Don't assert exact CPU numbers (environment-dependent) — assert the series exist.

Gate: `fmt` + `clippy --workspace --all-targets -D warnings` + `cargo test --workspace`.

## Out of scope
- OTLP trace export (collector push) — later toggle.
- Grafana dashboards as JSON (we expose metrics; dashboards are the operator's).
- Per-pond CPU/memory (DuckDB doesn't expose it; CPU/mem is per-node).
