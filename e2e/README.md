# Latiq end-to-end suites

These exercise a **real deployment** the way a user or an agent would — not unit
tests of our code, but the whole stack over its external surfaces. They are the
nightly CI suite (see `.github/workflows/nightly.yml`).

| Dir | Driver | Audience | What it proves |
|---|---|---|---|
| `sdk/` | Python `latiq` wheel + `pyarrow`/`pandas` | SDK / data | all SDK surfaces, multi-node + greeter forwarding via the gateway, **Arrow→pandas analysis**, uncapped Arrow streaming |
| `agent/` | TypeScript, Vercel AI SDK MCP client | agents | every MCP tool + `latiq://` resources + prompt SOPs *(phase 2)* |
| `perf/` | Python perf driver | — | mid-size ingest + read throughput + query latency *(phase 3)* |

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
LATIQ_CONTROL=http://localhost:51400 LATIQ_GATEWAY=http://localhost:51500 pytest e2e/sdk -v

# EMBEDDED — in-process single node, no docker (validates the test logic):
pytest e2e/sdk -v        # multi-node/forwarding tests self-skip
```
