# deploy — CLAUDE.md

How Latiq is packaged and run. The whole system is the **one `latiq` binary**; the
role is the command (`serve` = control plane, `node add` = pond node, else the CLI).
`ENTRYPOINT ["latiq"]`; compose/k8s pick the role.

## The cluster compose (`deploy/cluster/`)
Control plane + pond nodes behind an **nginx gateway** — the single front door:
- **Data + Stream gRPC → `:51500`** — round-robin across nodes (stateless; the
  greeter forwards each request to the pond's owner, so a pond on a node *not* in
  the gateway pool still works).
- **MCP HTTP → `:51510`** — **must be sticky** (`ip_hash` in `nginx.conf`): MCP
  Streamable-HTTP sessions are **node-local** (rmcp's in-memory session manager),
  so a round-robined request lands on a node without the session → `session not
  found`. Cross-node pond ops still forward.
- Control + Admin gRPC → control plane `:51400` (node-less).

**Two composes, on purpose.**
- `cluster/docker-compose.yml` — the **repo/CI/dev** stack: mounts `./nginx.conf`,
  and carries the `test`/`scale`/`tools` **profiles** (Prometheus + MinIO +
  Iceberg-REST for internal testing; a 3rd node for scale-out; an in-network `cli`).
- `latiq-compose.yml` — the **minimal external-user** deployment: control plane +
  2 pond nodes + gateway, **no profiles**, **pure images + ports** (no inline
  configs, no mounts) so it runs clone-free and **identically under Docker AND
  Podman**. Mirrored to the public `neonexia/latiq-deploy` repo:
  `curl -O https://raw.githubusercontent.com/neonexia/latiq-deploy/main/docker-compose.yml && docker compose up -d` (or `podman compose up -d`).

**The gateway image.** So the user compose stays pure-images, the gateway is a
published image — `latiq-gateway` (`deploy/gateway.Dockerfile` = `nginx` + baked
`cluster/nginx.conf`, the **one** gateway-config source). Built + pushed alongside
`latiq` by the nightly publish + `release-images.yml`. **Both** GHCR packages
(`latiq`, `latiq-gateway`) must be **public** for anonymous pulls.

Keep the two composes in sync when the topology changes (manual for now). The
nightly's `verify-deployment` job runs the **user** compose against the
**published** images + PyPI wheel — the real "shipped thing works" gate.

`--advertise-addr` must be the node's **routable** hostname (compose service / k8s
pod) — the control plane stores it and forwarding dials it; a wrong value lands
forwarding on the wrong host.

## Auth mode (`auth` profile — nightly + container only)
Identity v0 (OAuth bearer verification on **all three** surfaces: MCP, Data/Stream
gRPC, Admin gRPC) is exercised **only** in containers, **only** in the nightly.
`./dev.sh`, `cargo test`, and a plain `docker compose up` are unchanged and
unauthenticated.

One compose file, driven by env — no override file:
```bash
cd deploy/cluster
docker compose --env-file auth.env up -d     # auth cluster + Keycloak
docker compose --env-file auth.env run --rm auth-tests-sdk
docker compose --env-file auth.env run --rm auth-tests-agent
```
`auth.env` sets **`COMPOSE_PROFILES=auth`** (Compose reads it from an env file like
any other setting, so no `--profile` flag) *and* `LATIQ_AUTH_ISSUER` /
`LATIQ_AUTH_AUDIENCE`. Two mechanisms, two jobs: **`profiles:`** gates whether a
service *starts* (keycloak, `auth-tests-*`); **`${VAR:-default}`** injects the
issuer/audience into the **existing** control-plane + pond-node definitions — a
profile can't do that. Without the env file the issuer renders **empty**, and the
binary normalizes a blank issuer to "auth off".

**Everything is in-network, on purpose.** `keycloak:8080` resolves via Docker DNS
for the servers *and* for the two one-shot test runners (`auth-tests-sdk` on
`python:3.11-slim` → the wheel from `/repo/dist` + `pytest e2e/sdk/test_auth.py`;
`auth-tests-agent` on `node:22-slim` → `npm ci && npm test` in `e2e/agent`), so
there is **one** issuer URL and no host-vs-container address split to reconcile.
`KC_HOSTNAME_URL` pins the `iss` claim to that same URL. Keycloak publishes **no**
ports (add `ports: ["8080:8080"]` by hand if you want the admin console).

`keycloak-realm.json` carries realm `latiq` + confidential client `latiq-agent`
(service accounts → `client_credentials`) and — **essential** — an
`oidc-audience-mapper` adding `aud: latiq`. Keycloak emits no custom `aud` by
default, and without the mapper every token fails the audience check as an opaque
rejection. Verified token: `iss=http://keycloak:8080/realms/latiq`,
`aud=["latiq","account"]`.

`latiq-compose.yml` is **deliberately untouched** by all of this: the external-user
deployment stays pure images + ports, no profiles, no mounts.

## Lineage backend (`LATIQ_LINEAGE_BACKEND_URL` — optional, pond node only)
`--lineage-backend-url` / **`LATIQ_LINEAGE_BACKEND_URL`** on `latiq node add` is the
**full** OpenLineage endpoint to POST to (`http://marquez:5000/api/v1/lineage`), not a
base URL. Unset in every compose today; add it per pond-node service when lineage must
outlive its pond — dropping a pond destroys its local trail, and the backend is the
durability answer. Validated **once at startup**, so a typo stops the node instead of
warning on every query forever. Additive: ponds allocated with `--lineage` always write
their own files, and a backend that is down, hung or 500-ing can never affect a query.
**No credentials are sent** — a plaintext `http://` URL to a non-loopback host warns
once at startup (bodies carry pond and table names, redacted SQL, and the caller's
subject/issuer) and is allowed anyway: a Marquez on a private network is legitimate.
Watch `latiq_lineage_sink_events_dropped` on the node's `/metrics` — nothing awaits a
POST, so it is the only signal a backend has stopped keeping up.

## Deployment tiers
1. **Dev** → `./dev.sh` (control plane + N nodes; nginx front door when `--nodes>1`).
2. **External** → the published compose + GHCR image. Agents → the MCP endpoint
   (no SDK); programs → `pip install latiq` pointed at the gRPC endpoint; operators → CLI.
3. **Enterprise** → the same image on **k8s** (later, #23) — the gateway becomes a
   Service/Ingress; the front-door + forwarding model is unchanged.

## Publishing
See `docs/releasing.md`. We ship **binaries only** for now — the `latiq` **wheel →
PyPI** and the **image → GHCR**, one version each, from the **test-gated +
change-gated** nightly publish (inert unless `PUBLISH_NIGHTLY=true` + a PyPI
trusted publisher). **No Rust crates, no public repo** until we open-source
(crates.io would make the source public; a wheel/image is a binary). `#55` tracks
the open-source readiness checklist.
