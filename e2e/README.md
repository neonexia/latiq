# Latiq end-to-end suites

These exercise a **real deployment** the way a user or an agent would — not unit
tests of our code, but the whole stack over its external surfaces. They are the
nightly CI suite (see `.github/workflows/nightly.yml`).

| Dir | Driver | Audience | What it proves |
|---|---|---|---|
| `sdk/` | Python `latiq` wheel + `pyarrow`/`pandas` | SDK / data | all SDK surfaces, multi-node + greeter forwarding via the gateway, **Arrow→pandas analysis**, uncapped Arrow streaming |
| `agent/` | TypeScript, Vercel AI SDK MCP client | agents | every MCP tool (lifecycle, read/write/explain, datasets, catalogs) + read-only guard + structured errors + `latiq://` resources + prompt SOPs |
| `perf/` | Python perf driver | — | mid-size write/read/pandas throughput + aggregate-query latency + cross-node fan, recorded + floored |

## Topology
Clients hit the **gateway** (`deploy/cluster`, nginx) — a single front door per
plane, exactly like a k8s LB. The greeter node forwards each request to the pond's
owner, so a pond on a node that isn't even in the gateway's upstream pool still
works. Control/Admin gRPC is on the control plane (`:51400`); Data+Stream gRPC and
MCP HTTP are front-doored on `:51500` / `:51510`.

## SDK suite (`sdk/`)
Runs in two modes, **same assertions**:

```bash
# REMOTE — against the dockerized cluster (what CI does):
cd deploy/cluster
LATIQ_IMAGE=latiq:nightly docker compose --profile scale up -d \
  control-plane pond-node-1 pond-node-2 pond-node-3 gateway
cd ../..
maturin build -m sdk/python/Cargo.toml -o dist && pip install dist/*.whl -r e2e/sdk/requirements.txt
LATIQ_CONTROL=http://localhost:51400 LATIQ_GATEWAY=http://localhost:51500 \
  pytest e2e/sdk -v --latiq-mode=remote

# EMBEDDED — in-process single node, no docker (validates the test logic):
pytest e2e/sdk -v --latiq-mode=embedded   # multi-node/forwarding tests self-skip
```

`--latiq-mode` is optional locally and **required in CI**: since most of this
suite is conditional, a run that skipped every test would otherwise exit 0. The
flag names what the invocation must prove, and `e2e/sdk/conftest.py` fails the run
if a test that mode requires did not pass, or if a test skipped that the mode does
not sanction. It is a table of test names, not a count, so adding a test never
touches it.

The wheel's own Python-API tests live in **`sdk/python/tests/`** (not here): they
need only `pip install latiq`, so CI runs them against the installed wheel —
including the published one, post-publish.
