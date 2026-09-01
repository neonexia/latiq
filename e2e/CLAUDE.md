# e2e — CLAUDE.md

The **nightly end-to-end suite**: it exercises a real, multi-node, dockerized
deployment over **every external surface, the way a user or an agent would** — not
unit tests of our code. Lives in `.github/workflows/nightly.yml`'s `e2e-suite` job.

The shared rules for writing any test (which layer earns it, assert *why*, no vacuous guards, what belongs up here vs. in Rust) live in **`crates/latiq/tests/CLAUDE.md`** — read it first; the guidance below is the e2e-specific half.

## Three drivers (one per audience)
- **`sdk/`** — Python (`latiq` wheel + `pyarrow`/`pandas`). All SDK surfaces, **Arrow→pandas analysis asserted == SQL**, uncapped Arrow streaming past the 10k cap, multi-node placement + greeter forwarding.
- **`agent/`** — TypeScript, **Vercel AI SDK MCP client** (the same client an `ai` agent uses), driven by a *scripted* sequence (no live LLM → deterministic, no API key). Every MCP tool **except `get_lineage`** + read-only guard + structured-error contract; resources + prompt SOPs via the raw MCP SDK client (the AI SDK client doesn't surface those). **Lineage is a known e2e gap:** nothing here allocates a pond with `lineage: true` or calls `get_lineage`, so the tool is covered only by the in-process Rust tests (`crates/latiq/tests/mcp.rs`, `latiq-agent-core`'s `tests/agent_ops.rs`) and never against the dockerized cluster — where forwarding to the pond's *owner* (the only node holding the event files) is the part e2e would actually prove.
- **`perf/`** — Python SDK. `run_perf.py` is the nightly smoke gate (mid-size write/read/pandas
  throughput + aggregate p50/p95 + cross-node fan, recorded + floored). `read_bench.py` +
  `report.py` are a **characterization** benchmark, not a gate: read concurrency on a shared
  pond vs a pond per reader, a mixed reader+writer case, noisy-neighbour isolation, and a soak
  (RSS/fd/latency drift), rendered to a self-contained HTML report (`--baseline` gives
  before/after). Run it **manually** via the `Read benchmark (manual)` workflow, or locally —
  it needs a quiet machine to mean anything, so it is deliberately out of the nightly. Run it
  before/after any change to the engine's concurrency model. **Build the wheel `--release`** —
  a debug wheel measures the compiler, not the engine.

The **wheel's own** tests (`sdk/python/tests/`) are a fourth, cheaper thing and deliberately not here: they need nothing but `pip install latiq` (they drive `connect("local")`), and they cover the Python API itself — notably the `lineage` allocation flag, which `e2e/sdk` never sets. CI runs them **against an installed wheel** in `verify.yml`'s `e2e-suite` (pre-publish, wheel built from this repo) and again in the post-publish jobs against the wheel PyPI actually serves.

## Three modes, same assertions
- **REMOTE** (CI): set `LATIQ_CONTROL` + `LATIQ_GATEWAY` (+ `LATIQ_MCP` for the agent harness) → drives the dockerized cluster through the gateway. Proves multi-node + forwarding + the front door.
- **EMBEDDED** (local, no docker): unset → an in-process single-node cluster (`connect("local")`). Validates the call + Arrow/pandas logic. Multi-node-only tests self-skip. Run: `pytest e2e/sdk -v`; `LATIQ_MCP=http://127.0.0.1:51402/mcp npm --prefix e2e/agent test` against a local `dev.sh` node.
- **AUTH** (nightly, container-only): `cd deploy/cluster && docker compose --env-file auth.env up -d` → the same cluster with token verification on, plus an in-network Keycloak. The runners (`auth-tests-sdk`, `auth-tests-agent`) run **inside the compose network**, so servers and clients share ONE issuer URL (`http://keycloak:8080/realms/latiq`) — no host-vs-container split. `docker compose --env-file auth.env run --rm auth-tests-sdk` (build the wheel into `dist/` first). The auth tests self-skip when `LATIQ_AUTH_ISSUER` is unset, so REMOTE/EMBEDDED runs are untouched. They test the **integration** with a real OIDC provider (discovery, `client_credentials`, array `aud`, verified identity reaching DuckLake attribution) — verification *logic* is the Rust suite's job, against its fake IdP. `./dev.sh --nodes 2 --auth` gives the same shape locally (issuer `http://localhost:8080/realms/latiq`) — including with the `$LATIQ_TOKEN` its banner tells you to export: the negative tests build their anonymous client with `token=""` (explicitly no token, which never consults the environment) rather than asserting nobody exported one.

Because the modes differ in what they skip, **every CI invocation of `pytest e2e/sdk` passes `--latiq-mode=<embedded|remote|auth>`** (`e2e/sdk/conftest.py`). Without it a run that skipped *everything* still exits 0 — the failure mode that kept the iceberg e2e green for weeks against a test target that no longer existed. The flag is checked against a per-mode table of **test names** (what must have passed; what may skip), not a count, so adding a test never edits it and no number ever needs bumping. Adding a test that *skips* does edit it — deliberately.

## Gotchas (paid for in CI)
- **MCP sessions are node-local** (rmcp's in-memory session manager): the gateway's MCP upstream **must be sticky** (`ip_hash`) or round-robin breaks the session (`session not found`). The agent harness caught this; the Data/Stream gateway stays round-robin (stateless + forwarding).
- The cluster tests use a wheel **built fresh from this repo** (not PyPI). `verify-published` (a separate nightly job) installs **from PyPI** to prove `pip install latiq` works for a real user.
- `load_dataset` pulls a curated dataset from its **source URL** (real user flow, network dependency) — datasets load into **their own schema** (`tpch.nation`, not `nation`).
- **Never put prose inside a compose `command: >-` scalar.** Folding joins the lines with spaces, so a leading `#` commented out the *entire* command: `auth-tests-sdk` ran `sh -c '# … pip install … && pytest …'`, installed nothing, ran nothing, and exited 0 — the nightly's `SDK auth e2e` step was green while proving nothing. Comments belong above the key, in YAML.
- Don't bloat: `e2e/.gitignore` keeps `node_modules/`, `__pycache__/`, `dist/`, venvs out of git (we already paid for build-artifact bloat once — #48/#50).
