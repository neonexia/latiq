# Deploying Latiq

Everything needed to build, ship and run Latiq lives here. The whole system is
**one binary** (`latiq`) and the role is the command — `serve` is the control
plane, `node add` is a pond node, anything else is the CLI — so every deployment
below is the same image, started differently.

## Which one do I want?

| I want to… | Use | Needs a clone? |
|---|---|---|
| **try Latiq / run it for real** | [`docker-compose.yml`](docker-compose.yml) | no |
| **just the admin CLI**, against a cluster someone else runs | [`install.sh`](install.sh) | no |
| **hack on Latiq locally** (no containers) | `./dev.sh` in the repo root | yes |
| **a multi-node cluster** — scale-out, metrics, auth, catalogs | [`cluster/`](cluster/README.md) | yes |
| **an Iceberg/MinIO catalog to test against** | [`iceberg-minio/`](iceberg-minio/README.md) | yes |
| **Kubernetes** | *not built yet* — see below | — |

---

## Trying it out — `docker-compose.yml`

The lean, self-contained deployment: a control plane, two pond nodes, and the
nginx **gateway** that is the single front door. It references only **published
images** — no build step, no inline configs, no file mounts — so it runs without
cloning anything:

```bash
curl -O https://raw.githubusercontent.com/neonexia/latiq/main/deploy/docker-compose.yml
docker compose up -d        # or:  podman compose up -d
```

Then:

- **Agents (MCP, no SDK):** point any MCP host at `http://localhost:51510/mcp`.
- **Programs (Python SDK):** `pip install latiq`, then
  `latiq.connect("grpc://localhost:51400", query_gateway="grpc://localhost:51500")`.
- **Operators (CLI):** `export LATIQ_SERVER=http://localhost:51400 && latiq stats`.

Pin a version with `LATIQ_IMAGE` / `LATIQ_GATEWAY_IMAGE` (both default to
`:nightly`). Stop with `docker compose down`, add `-v` to wipe pond data.

> **Apple Silicon / ARM:** the published images are `linux/amd64` today. Until
> multi-arch images land, run with emulation:
> `DOCKER_DEFAULT_PLATFORM=linux/amd64 docker compose up -d`.

This file is what the nightly's `verify-deployment` job runs against the
just-published image and wheel — the "the shipped thing actually works" gate. It
is deliberately kept lean: no profiles, no mounts, nothing that only makes sense
inside this repo.

## The admin CLI — `install.sh`

A small **client-only** `latiq` (no server, no bundled DuckDB) for driving a
cluster you did not start:

```bash
curl -fsSL https://raw.githubusercontent.com/neonexia/latiq/main/deploy/install.sh | sh
export LATIQ_SERVER=http://your-control-plane:51400
latiq stats                 # nodes, ponds, tiers
latiq pond list
```

macOS + Linux, arm64 + x86_64. Installs to `~/.local/bin` (`LATIQ_BIN_DIR` to
change that). The prebuilt binaries currently come from the `cli-latest` release
of `neonexia/latiq-deploy`, which the nightly publishes — override with
`LATIQ_RELEASE_REPO` / `LATIQ_RELEASE_TAG`.

## The multi-node cluster — `cluster/`

The stack CI and contributors run: control plane + pond nodes + gateway, plus
**profiles** for the things only this repo needs — `test` (Prometheus, MinIO,
Iceberg REST), `scale` (a third node for the scale-out e2e), `tools` (an
in-network CLI), `auth` (Keycloak + the in-network auth test runners). It mounts
`nginx.conf`, `prometheus.yml` and `keycloak-realm.json` from the working tree,
so it needs the repo.

```bash
cd deploy/cluster
LATIQ_IMAGE=ghcr.io/neonexia/latiq:nightly docker compose up -d
```

Full instructions, the CLI recipes and the scale-out test:
[`cluster/README.md`](cluster/README.md). Auth mode:
[`../CLAUDE.md`](CLAUDE.md).

## The Iceberg/MinIO fixture — `iceberg-minio/`

A real Iceberg REST catalog on MinIO, so Latiq's external-catalog attacher can be
exercised end to end. `./iceberg-minio/up.sh` brings it up and seeds a table; the
matching test is `crates/latiq/tests/catalogs_iceberg.rs` (`#[ignore]`d — it needs
this harness). See [`iceberg-minio/README.md`](iceberg-minio/README.md).

## Images — `Dockerfile`, `gateway.Dockerfile`

- **`Dockerfile`** → `ghcr.io/neonexia/latiq`. The single binary, all roles.
  DuckDB compiles from source the first time (slow); the runtime stage bakes the
  DuckDB extensions in with `latiq warm-extensions` so nodes start without
  network. Build from the **repo root**:
  `docker build -f deploy/Dockerfile -t ghcr.io/neonexia/latiq:dev .`
- **`gateway.Dockerfile`** → `ghcr.io/neonexia/latiq-gateway`. nginx with
  `cluster/nginx.conf` baked in — the one gateway-config source of truth. It
  exists so `docker-compose.yml` can stay pure images with no mounts. Build:
  `docker build -f deploy/gateway.Dockerfile -t ghcr.io/neonexia/latiq-gateway:dev deploy/cluster`

Both are published by the nightly and by `release-images.yml`. Both GHCR packages
must be **public** for the clone-free path to work.

`prometheus.example.yml` is a scrape config for pointing a host-native Prometheus
at a `./dev.sh` cluster; the containerised one is `cluster/prometheus.yml`.

## Podman

- **`docker-compose.yml` is runtime-agnostic by construction** — published images
  and port mappings only, no bind mounts, no `configs:`. `podman compose up -d`
  is the supported second runtime, and the compose is kept mount-free
  specifically to keep it that way. Note that CI only exercises the Docker path,
  so Podman is supported by design rather than by a green build.
- **`iceberg-minio/` is verified green under Podman.** `up.sh` auto-detects the
  runtime (Docker if its daemon is up, else `podman compose`); override with
  `LATIQ_COMPOSE`.
- **`cluster/` is Docker-first and not verified under Podman.** Two things to know
  before trying: it bind-mounts config files (`./nginx.conf`, `./prometheus.yml`,
  `./keycloak-realm.json`, and `../..:/repo` for the auth runners), which on an
  SELinux host needs a `:z`/`:Z` suffix that is not there; and it leans on
  `depends_on: { condition: service_healthy }`, which `podman-compose` has
  historically supported unevenly. Use `podman compose` (the Docker-compatible
  front end) rather than `podman-compose` if you try it.

## Kubernetes

**There are no k8s manifests yet** — issue
[#23](https://github.com/neonexia/latiq/issues/23) tracks them. The design is
settled (tiers map to pod sizing, per-pod scraping via a `PodMonitor`, the
gateway becomes a Service/Ingress) and nothing about the front-door + forwarding
model changes, but none of it is written. Until then the compose above is the
supported deployment.

---

Publishing and versioning: [`../docs/releasing.md`](../docs/releasing.md).
Invariants for anyone changing these files: [`CLAUDE.md`](CLAUDE.md).
