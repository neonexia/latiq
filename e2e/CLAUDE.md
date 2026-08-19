# e2e — CLAUDE.md

The **nightly end-to-end suite**: it exercises a real, multi-node, dockerized
deployment over **every external surface, the way a user or an agent would** — not
unit tests of our code. Lives in `.github/workflows/nightly.yml`'s `e2e-suite` job.

## Three drivers (one per audience)
- **`sdk/`** — Python (`latiq` wheel + `pyarrow`/`pandas`). All SDK surfaces, **Arrow→pandas analysis asserted == SQL**, uncapped Arrow streaming past the 10k cap, multi-node placement + greeter forwarding.
- **`agent/`** — TypeScript, **Vercel AI SDK MCP client** (the same client an `ai` agent uses), driven by a *scripted* sequence (no live LLM → deterministic, no API key). Every MCP tool + read-only guard + structured-error contract; resources + prompt SOPs via the raw MCP SDK client (the AI SDK client doesn't surface those).
- **`perf/`** — Python SDK. `run_perf.py` is the nightly smoke gate (mid-size write/read/pandas
  throughput + aggregate p50/p95 + cross-node fan, recorded + floored). `read_bench.py` +
  `report.py` are a **characterization** benchmark, not a gate: read concurrency on a shared
  pond vs a pond per reader, a mixed reader+writer case, noisy-neighbour isolation, and a soak
  (RSS/fd/latency drift), rendered to a self-contained HTML report (`--baseline` gives
  before/after). Run it **manually** via the `Read benchmark (manual)` workflow, or locally —
  it needs a quiet machine to mean anything, so it is deliberately out of the nightly. Run it
  before/after any change to the engine's concurrency model. **Build the wheel `--release`** —
  a debug wheel measures the compiler, not the engine.

## Two modes, same assertions
- **REMOTE** (CI): set `LATIQ_CONTROL` + `LATIQ_GATEWAY` (+ `LATIQ_MCP` for the agent harness) → drives the dockerized cluster through the gateway. Proves multi-node + forwarding + the front door.
- **EMBEDDED** (local, no docker): unset → an in-process single-node cluster (`connect("local")`). Validates the call + Arrow/pandas logic. Multi-node-only tests self-skip. Run: `pytest e2e/sdk -v`; `LATIQ_MCP=http://127.0.0.1:51402/mcp npm --prefix e2e/agent test` against a local `dev.sh` node.

## Gotchas (paid for in CI)
- **MCP sessions are node-local** (rmcp's in-memory session manager): the gateway's MCP upstream **must be sticky** (`ip_hash`) or round-robin breaks the session (`session not found`). The agent harness caught this; the Data/Stream gateway stays round-robin (stateless + forwarding).
- The cluster tests use a wheel **built fresh from this repo** (not PyPI). `verify-published` (a separate nightly job) installs **from PyPI** to prove `pip install latiq` works for a real user.
- `load_dataset` pulls a curated dataset from its **source URL** (real user flow, network dependency) — datasets load into **their own schema** (`tpch.nation`, not `nation`).
- Don't bloat: `e2e/.gitignore` keeps `node_modules/`, `__pycache__/`, `dist/`, venvs out of git (we already paid for build-artifact bloat once — #48/#50).
