# Deploying Latiq

Everything needed to build, ship and run Latiq lives here. The whole system is
**one binary** (`latiq`) and the role is the command — `serve` is the control
plane, `node add` is a pond node, anything else is the CLI — so every deployment
below is the same image, started differently.

## Which one do I want?

| I want to… | Use | Needs a clone? |
|---|---|---|
| **try Latiq / run it for real** | [`docker-compose.yml`](docker-compose.yml) | no |
| **just the admin CLI**, against a cluster someone else runs | `pipx install latiq-admin` | no |
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

Both images default to the **`0.1.1` release** — a version someone decided to
ship, not whatever main built last night. `LATIQ_IMAGE` / `LATIQ_GATEWAY_IMAGE`
override that: a newer release to move forward, or `:nightly` to track main. Stop
with `docker compose down`, add `-v` to wipe pond data.

**Apple Silicon / ARM64 works with no flags.** Both images are published as
multi-arch manifest lists (`linux/amd64` + `linux/arm64`), so the commands above
are the whole story on an M-series Mac — no `DOCKER_DEFAULT_PLATFORM`, no
`--platform`, no emulation, under Docker *and* Podman (#66). The arm64 image is
built on a native arm64 runner, so it is a real arm64 binary, not an emulated
amd64 one.

> The **wheels** were the same gap, one channel over, and are fixed in CI but not
> yet on PyPI (#108). Every build now produces manylinux `x86_64` + `aarch64` and
> macOS `arm64` wheels for both `latiq` and `latiq-admin`, and CI installs and
> runs them on arm64 — but PyPI still serves only the amd64-only `latiq` 0.1.0
> (PyPI versions are immutable; uploads are gated off while 0.1.x stabilises).
> **Until the next version is published**, `pip install latiq` on an arm64 host
> has nothing to resolve: drive the cluster from an amd64 client, or use the MCP
> endpoint.

This file is what the nightly's `verify-deployment` job runs against the
just-published image and wheel — the "the shipped thing actually works" gate —
and `verify-deployment-arm64` runs the same compose from the same images on a
native arm64 runner. It is deliberately kept lean: no profiles, no mounts,
nothing that only makes sense inside this repo.

## The admin CLI — `pip install latiq-admin`

A small **client-only** `latiq` (no server, no bundled DuckDB) for driving a
cluster you did not start:

```bash
pipx install latiq-admin    # or: pip install latiq-admin
export LATIQ_SERVER=http://your-control-plane:51400
latiq stats                 # nodes, ponds, tiers
latiq pond list
```

The wheel contains the **native executable**, not Python — pip is only the
delivery mechanism, the way it is for any other packaged CLI. `pipx` is the
recommended form because it gets its own environment; plain `pip` works. Pin with
`latiq-admin==0.1.1`, upgrade with `pipx upgrade latiq-admin`, remove with
`pipx uninstall latiq-admin`.

This replaced a `curl … | sh` installer and four cross-compiled binaries on a
rolling GitHub release. PyPI already solves what that hand-rolled: platform
selection, version pinning, upgrade, uninstall — and nothing has to pipe an
unsigned script into a shell.

**Two builds of one CLI.** `pip install latiq` (the SDK wheel) also puts a
`latiq` on PATH: the *full* build, which can run the servers too (`latiq serve`,
`latiq node add`). Install `latiq-admin` to **drive** a cluster and `latiq` to
**run** one — and only one of the two into any single environment, since they
install the same command name. In the lean build the server roles still parse and
fail with an error naming `pip install latiq`, rather than claiming the command
does not exist.

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
matching test is `crates/latiq/tests/admin.rs::catalogs_iceberg` (`#[ignore]`d — it needs
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

Both are published by the nightly (`:nightly`) and by `release.yml` on a `v*` tag
(`:<version>`, plus `:latest` from 1.0.0 on). Both GHCR packages
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
