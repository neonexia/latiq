# Nightly E2E suite — full deploy, agent harness, Arrow/pandas interop, perf

**Date:** 2026-06-19
**Status:** Approved (shape). Decisions locked with the user.
**Goal:** A nightly CI suite that exercises **every external surface** of a real,
multi-node, dockerized Latiq deployment the way a **user or an agent** would —
functional + result-streaming (Arrow *and* JSON) + Arrow↔pandas interop + mid-size
perf — and publishes the SDK/images publicly.

## Locked decisions
- **Distribution: public** — `latiq` → **PyPI** (name is free), images → **public
  GHCR**, and (caveat below) `latiq-sdk` → crates.io.
- **Agent harness: deterministic** — a TS harness using the **Vercel AI SDK MCP
  client** with a *scripted* tool-call sequence (no live LLM): exercises every MCP
  tool/resource/prompt, deterministic, no API key/cost, CI-stable.
- **Topology: a real nginx gateway** front-doors Data+Stream+MCP across nodes —
  validates Slice B's front-door/forwarding (k8s-LB) fix and matches prod.

## Crate-publish caveat (don't assume trivial)
`latiq-sdk` is **not a leaf**: embedded mode pulls in `latiq-control-plane` +
`latiq-pond-node` (the whole server). Publishing it to **crates.io requires
publishing the entire ~8-crate tree publicly**. The **PyPI wheel is
self-contained** (server statically linked) and is the clean external artifact.
**Lead with PyPI + public images; treat crates.io as a deliberate later step**
(or offer a git-dependency for Rust consumers instead).

## Existing infra we build on (do NOT rebuild)
- `deploy/cluster/docker-compose.yml`: control-plane + pond-node-1/2/3 (+`scale`
  profile) + Prometheus + MinIO + Iceberg-REST + a `cli` container.
- `deploy/cluster/scale_out_test.sh`: 2→3-node scale-out via the CLI.
- `.github/workflows/nightly.yml`: fmt+clippy+test, iceberg/minio e2e, cluster
  scale-out. `release-images.yml`: GHCR `ghcr.io/neonexia/latiq` on `v*` tags.

## Cluster port layout (per node, `node add --port 51401`)
- **Data + Stream gRPC** → `:51401` (one tonic server hosts both `Data` + `Stream`).
- **MCP HTTP** → `:51402` (port + 1).
- **Metrics** → `:52401` (port + 1000).
- Control + Admin gRPC → `control-plane:51400` (host-exposed today).

## The gateway (new)
An **nginx** service in the compose, host-exposed, front-dooring across nodes:
- `grpc_pass` upstream `latiq_data` = `pond-node-{1,2,3}:51401` (Data **and**
  Stream — same port; HTTP/2; round-robin → greeter forwards by pond) → host
  **`51500`**.
- `proxy_pass` upstream `latiq_mcp` = `pond-node-{1,2,3}:51402` (MCP HTTP) → host
  **`51510`**.
- Control/Admin stay node-less on `control-plane:51400`.

Clients now know **one address per plane** and never a pod IP — exactly the SDK's
front-door contract.

## Three drivers — each "like a real user/agent", against the live 3-node cluster

| Driver | Lang | Audience | Endpoint(s) | Exercises |
|---|---|---|---|---|
| **SDK e2e** | Python (`latiq` wheel + `pyarrow`+`pandas`) | SDK/data | control `:51400` + gateway `:51500` | all SDK surfaces, multi-node forwarding, **Arrow→pandas**, uncapped Arrow stream |
| **Agent harness** | TypeScript (Vercel AI SDK MCP client) | agents | MCP gateway `:51510` | every MCP tool + `latiq://` resources + prompt SOPs |
| **CLI** | shell (in-network `cli` container) | operators | control `:51400` | node/pond/policy, datasets, catalogs, **JSON** result path |

## Coverage matrix (no corners cut)
Each surface × every column it can do:

| | functional lifecycle | Arrow stream | JSON | attribution/snapshots | error contract | multi-node forwarding |
|---|---|---|---|---|---|---|
| SDK (Python) | ✓ | ✓ (query→Table) | — (SDK is Arrow) | ✓ (writes commit) | ✓ (drop-confirm, bad SQL) | ✓ (ponds on N nodes via gateway) |
| MCP (agent) | ✓ | — (agent JSON) | ✓ (tool results) | ✓ (`pond.snapshots()`) | ✓ (read-only guard) | ✓ (gateway → greeter) |
| CLI (operator) | ✓ | — | ✓ (`--format json`) | ✓ | ✓ | ✓ (scale-out) |

**Arrow interop proof:** the SDK pulls a non-trivial query → `pyarrow.Table` →
`.to_pandas()` → a pandas `groupby().agg()` → assert it **equals the same
aggregate computed in SQL**. Proves Arrow types + values survive the wire into the
pandas ecosystem.

**Multi-node proof:** allocate enough ponds that random placement spreads them
across nodes 1–3; write+read each **through the gateway** (a non-owner greeter
forwards) — proves forwarding + the gateway + Slice B routing together.

## Perf (mid-size)
A perf driver (Python SDK): per run, against the cluster —
- **Ingest** ~500k–1M rows into a pond (batched INSERT / `COPY` from a generated
  parquet), record rows/s.
- **Read throughput**: stream a large (>10k-row, past the JSON cap) result via
  Arrow; record rows/s + that it streams uncapped.
- **Query latency**: N point/aggregate queries across ponds on different nodes;
  record p50/p95.
- **Assert** sane absolute thresholds (generous, to catch gross regressions, not
  microbenchmark noise) **and** print the numbers for trend-watching. No silent
  truncation — log the row counts/timings.

## CI wiring (new nightly jobs)
Add to `nightly.yml` (nightly + `workflow_dispatch`):
1. **`build-artifacts`**: build the dev image (`ghcr.io/neonexia/latiq:dev`) and
   the `latiq` wheel (maturin, manylinux for CI); cache/upload for downstream jobs.
2. **`e2e-cluster`**: bring up compose **+ gateway** (3 nodes); run the **SDK e2e**
   pytest (functional + Arrow/pandas + streaming + multi-node) against the
   gateway; dump logs + teardown on failure.
3. **`e2e-agent`**: same cluster; run the **TS Vercel MCP harness** over every tool.
4. **`perf`**: same cluster; run the perf driver; assert thresholds + record.
Each job dumps `docker compose logs` on failure (operators grep the `latiq::access`
trail too).

## Publishing (Phase 4, public)
- Add **`LICENSE`** (Apache-2.0, matching the workspace) + make the repo public
  (user action / explicit authorization).
- **`publish-pypi.yml`** on `v*` tags: build manylinux + macOS/arm64 abi3 wheels
  (maturin) → **PyPI** (`latiq`) via trusted publishing (OIDC) — no stored token.
- Ensure GHCR images are **public**.
- **crates.io**: documented runbook to publish the dependency tree in order
  (`latiq-common` → … → `latiq-sdk`); flagged as deliberate, not auto-on-tag.

## Phasing (each a PR, gated)
1. **Gateway + Python SDK e2e** — nginx in compose; `e2e/sdk/` pytest driver
   (functional + Arrow/pandas + streaming + multi-node); `e2e-cluster` nightly job.
2. **TS Vercel agent harness** — `e2e/agent/` (Vercel AI SDK MCP client, scripted);
   `e2e-agent` nightly job.
3. **Perf suite** — `e2e/perf/`; `perf` nightly job.
4. **Publishing** — LICENSE, PyPI workflow, public GHCR, crates.io runbook.

## Test layout
- `e2e/sdk/` — Python pytest (remote-mode SDK against the gateway).
- `e2e/agent/` — TS harness (package.json, Vercel AI SDK, tsx runner).
- `e2e/perf/` — Python perf driver + a tiny results reporter.
- `deploy/cluster/` — gateway service + `nginx.conf` added here.
- Drivers run on the CI host against host-exposed gateway/control ports (or as
  compose services on the network — pick per driver in the plan).

## Out of scope (later)
Live-LLM agent job (optional, key-gated), Flight-SQL streaming, k8s manifests,
multi-arch perf, private-registry fallback. crates.io publish execution (runbook
only this pass).
