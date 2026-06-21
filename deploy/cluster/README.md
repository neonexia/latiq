# Latiq cluster (Docker / Podman)

A full multi-node Latiq deployment: a **control plane** + **pond nodes** behind an
**nginx gateway** (the single MCP + Data/Stream front door), with **Prometheus**
scraping every node and a local **MinIO + Iceberg REST** catalog. This is what the
nightly CI (`.github/workflows/nightly.yml`) exercises, and the compose external
users run (agents → the MCP endpoint; SDK/CLI → the gRPC endpoints).

> Runs under **Docker** or **Podman** (`podman compose`). The compose auto-uses
> whichever is running; in scripts override with `LATIQ_COMPOSE`.

## Image

Every role is the one `latiq` binary; the role is the command:

| Role | Command |
|---|---|
| Control plane | `latiq serve --bind 0.0.0.0 --port 51400 --root /var/lib/latiq` |
| Pond node | `latiq node add --bind 0.0.0.0 --port 51401 --advertise-addr <host>:51401 …` |
| CLI | `latiq <pond/query/stats/…>` (with `LATIQ_SERVER` set) |

`--bind 0.0.0.0` makes a containerized role reachable; `--advertise-addr` is the
host:port the control plane stores for **query forwarding** — it must be the
node's container/pod hostname, or forwarding lands on the wrong host. Build the
image with `docker build -f ../Dockerfile -t latiq:dev ..` (DuckDB compiles from
source the first time — slow; extensions are baked in via `latiq warm-extensions`).

## Bring it up

```bash
# from this directory; pin a published tag or use a locally-built image
LATIQ_IMAGE=ghcr.io/neonexia/latiq:latest docker compose up -d \
    control-plane pond-node-1 pond-node-2 prometheus
docker compose ps
```

- Control + Admin gRPC (CLI/operators): `localhost:51400`
- Prometheus: `localhost:9090` (scrapes `control-plane:52400`, `pond-node-N:52401`)
- MinIO console: `localhost:9001` (`admin`/`password`), Iceberg REST: `localhost:8181`

`pond-node-3` is behind the `scale` profile (not started by default) so the
scale-out test can add it at runtime.

## Use the CLI

The CLI resolves a pond's **owning node directly** via the control plane, so it
must run where the `pond-node-N` hostnames resolve — i.e. **inside the network**:

```bash
docker compose run --rm --no-deps -T cli pond create --name demo
docker compose run --rm --no-deps -T cli query --pond demo "CREATE TABLE t AS SELECT 42 AS n"
docker compose run --rm --no-deps -T cli query --pond demo "SELECT * FROM t"
docker compose run --rm --no-deps -T cli stats
```

A native CLI on your laptop works too: point it at the control plane with
`export LATIQ_SERVER=http://localhost:51400` (it reaches pond nodes through the
gateway / forwarding). Or run it in-network: `docker compose run --rm cli <args>`.

## Scale-out e2e

`scale_out_test.sh` proves dynamic scale-up against the running cluster: it adds
`pond-node-3` at runtime, allocates ponds (placement is random across active
nodes), asserts at least one lands on node-3, and runs a query on a node-3 pond
(routed node-direct) — the same flow as the nightly CI job.

```bash
LATIQ_IMAGE=latiq:nightly docker compose up -d control-plane pond-node-1 pond-node-2 prometheus
./scale_out_test.sh        # needs jq
docker compose --profile scale down -v
```

## Tear down

```bash
docker compose --profile scale down -v   # include the scaled node + volumes
```
