# Latiq

An **agent-native data system**. Agents allocate ephemeral **ponds** (DuckLake
workspaces), write and read SQL, collaborate in them, and release them. Humans and
programs drive it the same way through an SDK; operators administer the deployment.

It speaks to three audiences over three surfaces — pick the one that's you.

## 🤖 Agents — over MCP (no SDK)

Run a cluster (no repo clone — pull the published images), then point any MCP host
(Claude Desktop, the Vercel AI SDK, …) at the gateway's MCP endpoint:

```bash
curl -O https://raw.githubusercontent.com/neonexia/latiq/main/deploy/docker-compose.yml
docker compose up -d                       # or:  podman compose up -d
# control plane + pond nodes + gateway; MCP endpoint: http://localhost:51510/mcp
```

The agent gets tools (`allocate_pond`, `read_query`, `write_query`, `load_dataset`,
…), `latiq://` guidance resources, and prompt SOPs. The gateway forwards each call
to the pond's owning node — agents never see a node address.

## 🐍 Programs — the Python SDK

```bash
pip install latiq
```
```python
import latiq

# Embedded: a real single-node Latiq in-process (great for notebooks / CI).
db = latiq.connect(server="local")
# Or connect to a running cluster (point at the gateway):
# db = latiq.connect(server="grpc://host:51400", query_gateway="grpc://host:51500")

work = db.create_pond(name="work", description="raw events 2024")
work.query(sql="CREATE TABLE t(id INT, region VARCHAR)")
work.query(sql="INSERT INTO t VALUES (1,'east'),(2,'west')")
table = work.query(sql="SELECT * FROM t")     # → pyarrow.Table
df = table.to_pandas()                          # straight into the pandas ecosystem
```

Reads stream back as Arrow (uncapped); writes are attributed and snapshotted. See
[`sdk/python/README.md`](sdk/python/README.md).

## 🛠 Operators — deploy + CLI

The whole system is one binary; the role is the command (`serve`, `node add`, or
the CLI). Everything deployment-shaped lives in [`deploy/`](deploy/README.md) —
the compose above, the multi-node cluster, the Dockerfiles. Install the admin CLI
(`latiq-admin` — a small client-only build, no server/DuckDB) and point it at the
control plane:

```bash
pipx install latiq-admin      # a native `latiq` on PATH; pip works too
export LATIQ_SERVER=http://your-control-plane:51400
latiq stats          # nodes, ponds, tiers   ·   latiq pond list · latiq dataset list
```

`pip install latiq` (the SDK wheel) also puts a `latiq` on PATH — the *full*
build, which can additionally run the servers (`latiq serve`, `latiq node add`).
Two builds of one CLI: install `latiq-admin` to drive a cluster, `latiq` to run
one, and only one of them into a given environment.

## How it fits together

| Surface | Audience | Endpoint | Carries |
|---|---|---|---|
| **MCP-over-HTTP** | agents | gateway `:51510/mcp` | tools + resources + prompts |
| **Data/Query + Stream gRPC** | SDK / CLI | gateway `:51500` | allocate / read / write / stream |
| **Admin gRPC** | operators | control plane `:51400` | nodes / policy / pond metadata |

Clients hit **one address per surface**; the **gateway** spreads requests across
pond nodes and the **greeter** forwards each to the pond's owner — so it scales
from a laptop compose to a k8s cluster unchanged.

## Develop

```bash
./dev.sh                 # local control plane + pond node(s) (+ gateway for --nodes>1)
cargo test --workspace   # unit + crate + full-stack e2e
```

See [`docs/dev.md`](docs/dev.md), the per-crate `crates/*/CLAUDE.md`, and the
end-to-end suites in [`e2e/`](e2e/README.md). Releasing/publishing:
[`docs/releasing.md`](docs/releasing.md).
