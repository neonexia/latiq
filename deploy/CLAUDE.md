# deploy — CLAUDE.md

How Latiq is packaged and run. The whole system is the **one `latiq` binary**; the
role is the command (`serve` = control plane, `node add` = pond node, else the CLI).
`ENTRYPOINT ["latiq"]`; compose/k8s pick the role.

**`deploy/` is the single home for deployment artifacts** — the user compose, the
cluster compose, the fixtures, both Dockerfiles. `README.md`
here is the human front door (which file is which); this file is the invariants.
Nothing deployment-shaped should live outside this directory or in another repo.

## The cluster compose (`deploy/cluster/`)
Control plane + pond nodes behind an **nginx gateway** — the single front door:
- **Data + Stream gRPC → `:51500`** — round-robin across nodes (stateless; the
  greeter forwards each request to the pond's owner, so a pond on a node *not* in
  the gateway pool still works).
- **MCP HTTP → `:51510`** — **must be sticky** (`ip_hash` in `nginx.conf`): MCP
  Streamable-HTTP sessions are **node-local** (rmcp's in-memory session manager),
  so a round-robined request lands on a node without the session → `session not
  found`. Cross-node pond ops still forward. It also **must** carry the client's
  `Host` through (`proxy_set_header Host $host`) and that host **must** match
  `LATIQ_PUBLIC_MCP_URL` on every node: rmcp's DNS-rebinding guard is
  loopback-only by default, and the node widens it to that URL's host — bare,
  since `$host` drops the port and rmcp matches a port-qualified entry only
  against that exact port. A mismatch is `403 Forbidden: Host header is not
  allowed` on every JSON-RPC POST while discovery still answers, so it reads as
  an auth failure and is not one. Found by the auth e2e, whose runners dial
  `gateway:51510` from inside the network rather than `localhost`.
- Control + Admin gRPC → control plane `:51400` (node-less).

**Two composes, on purpose.**
- `cluster/docker-compose.yml` — the **repo/CI/dev** stack: mounts `./nginx.conf`,
  and carries the `test`/`scale`/`tools` **profiles** (Prometheus + MinIO +
  Iceberg-REST for internal testing; a 3rd node for scale-out; an in-network `cli`).
- `docker-compose.yml` (repo root of `deploy/`) — the **minimal external-user**
  deployment: control plane + 2 pond nodes + gateway, **no profiles**, **pure
  images + ports** (no inline configs, no mounts) so it runs clone-free and
  **identically under Docker AND Podman**. It is fetched directly from this repo:
  `curl -O https://raw.githubusercontent.com/neonexia/latiq/main/deploy/docker-compose.yml && docker compose up -d` (or `podman compose up -d`).
  **Keep it mount-free** — a single bind mount both breaks the clone-free path and
  is the most likely thing to break Podman.

**The CLI ships as a wheel, not a shell installer.** There is no `install.sh` and
no rolling `cli-latest` release; both were deleted along with the four
cross-compiled binaries that fed them. PyPI already does platform selection,
version pinning, upgrade and uninstall — everything the installer hand-rolled —
and nothing has to pipe an unsigned script into a shell. **If you are adding an
install path, it is a wheel; do not reintroduce `curl | sh`.**

**Two wheels, two builds of one CLI, and they are not interchangeable:**
- **`latiq`** (`sdk/python/`) — the SDK wheel, default features. Its
  `[project.scripts]` entry point runs the **full** CLI, server roles included,
  through PyO3 (`_cli_main` → `latiq::run_from_args`). `pip install latiq &&
  latiq serve` works because `latiq-sdk` already linked the control plane and the
  pond node — the wheel gained an argument parser, not a server.
- **`latiq-admin`** (`sdk/admin/`) — maturin `bindings = "bin"` +
  `no-default-features`: the native client-only executable in the wheel's scripts
  dir, no DuckDB, no server. `latiq serve` here fails with
  `server_role_unavailable` (`crates/latiq/src/lib.rs`), which names
  `pip install latiq` — *not* clap's "unrecognized subcommand", which would tell
  an operator the command does not exist when the truth is that this build cannot
  run it. **`serve`/`node add` must keep PARSING in the lean build** (pinned by
  `error_contract_client_only_build_parses_the_server_roles`).

Both install the same command name, so one or the other per environment.

**One clap layer serves both**, which is why `crates/latiq` is a library with a
two-line `src/main.rs`: the image runs the binary, the wheel calls
`run_from_args`. Adding a command to one adds it to both, by construction.

**Six places carry the version**, and `release.yml`'s `meta` job fails the release
before anything builds if any disagrees with the tag: `Cargo.toml`
(`[workspace.package]`, inherited by every crate), **`sdk/python/Cargo.toml`**
(hard-coded, *not* inherited — maturin reads the wheel version from
`pyproject.toml`, so nothing else ever looks at it and it drifted to a stale
`0.1.0` unnoticed), `sdk/python/pyproject.toml`, `sdk/admin/pyproject.toml`, the
**two image pins in `deploy/docker-compose.yml`**, and the git tag itself.
**Add a new one and add it to `meta`** — an unchecked place is how a release
ships an artifact at a version that is not the tag.

**The gateway image.** So the user compose stays pure-images, the gateway is a
published image — `latiq-gateway` (`deploy/gateway.Dockerfile` = `nginx` + baked
`cluster/nginx.conf`, the **one** gateway-config source). Built + pushed alongside
`latiq` by the nightly publish + `release.yml`. **Both** GHCR packages
(`latiq`, `latiq-gateway`) must be **public** for anonymous pulls.

**Both images are multi-arch (`linux/amd64` + `linux/arm64`), and that is not
optional** (#66). Apple Silicon is what most developers evaluate on; an
amd64-only manifest fails at `docker pull`/`podman pull`, before a line of our
code runs, and no amount of documentation makes that an acceptable first
experience. Three things keep it true, and all three are load-bearing:

- **One place builds them** — `.github/workflows/images.yml`, called by BOTH
  `nightly.yml` and `release.yml` the way `verify.yml` is. Never inline a
  `docker/build-push-action` step into a publishing workflow again: the amd64-only
  images shipped precisely because four copies of that step, in two files, all
  omitted `platforms:` and so silently inherited the runner's arch.
- **`latiq` builds on NATIVE runners, one per arch** (`ubuntu-latest` +
  `ubuntu-24.04-arm`, free for public repos), pushed by digest and merged with
  `docker buildx imagetools create`. It compiles DuckDB from source — ~21 min
  native (observed), hours under QEMU. **Do not "simplify" this into a single
  `platforms: linux/amd64,linux/arm64` job**; that is the emulated build, and it
  will time out. The gateway image, being `FROM nginx` + one `COPY` with nothing
  to execute, *is* a single multi-platform job, correctly.
- **The published manifest is asserted, not assumed** — `images.yml`'s
  `manifests` job re-reads every published tag of both images from ghcr.io and
  fails unless `linux/amd64` and `linux/arm64` are both really in the index
  (attestation manifests, which carry `unknown/unknown`, are filtered out so they
  cannot pad the list). And `verify-deployment-arm64` / `verify-release-arm64`
  bring the **user compose** up from the published images on a native arm64
  runner, with no `--platform` and no `DOCKER_DEFAULT_PLATFORM`, asserting the
  running containers' `Architecture` is `arm64` — because with binfmt registered
  an amd64 image runs under qemu-user and would pass every other check. The
  amd64-only state was invisible for two releases exactly because the only
  "verify user path" job ran on `ubuntu-latest`.

**The wheels are multi-platform too, for the same reason and by the same
pattern** (#108 — the sibling of #66, one channel over). `latiq` 0.1.0 is a
single `manylinux_2_28_x86_64` file on PyPI with no sdist, so `pip install latiq`
fails outright on Apple Silicon; both wheel jobs in both workflows passed
`target: x86_64` on `ubuntu-latest`. `.github/workflows/wheels.yml` is the wheel
counterpart of `images.yml`:
- **Both wheels × three platforms**, one job each on a **native** runner —
  manylinux `x86_64` (`ubuntu-latest`), manylinux `aarch64`
  (`ubuntu-24.04-arm`), macOS `arm64` (`macos-14`). The `latiq` wheel compiles
  DuckDB, so QEMU is out here as well. Builds are `cp39-abi3`: one wheel per
  platform, not per Python version. The manylinux `before-script-linux` derives
  protoc's arch from `uname -m` — a hard-coded `linux-x86_64` zip would put an
  unrunnable protoc on PATH in the aarch64 container.
- **The built set is asserted** by `.github/scripts/assert-wheel-platforms.sh`
  (every platform present, every filename at the published version, and never
  zero files examined), and the publish jobs run the same script in `pypi` mode
  against the file list PyPI serves afterwards.
- **Both wheels are pip-installed and driven on arm64** (`verify-linux-arm64`,
  `verify-macos-arm64`) — from what the run built, so the coverage survives
  `PUBLISH_PYPI_WHEELS` being off.
- **No sdist**, deliberately: it would need protoc + a Rust toolchain + a
  ~20-minute DuckDB compile on the user's machine, on platforms we never build or
  test. Cover a platform by building a wheel for it — matrix row *and*
  `EXPECTED_TAGS` entry, together. (`docs/releasing.md`, *Why there is no sdist*.)
- **The upload stays in the callers.** PyPI trusted publishing matches on the
  workflow filename, so `wheels.yml` builds and `nightly.yml` / `release.yml`
  upload, from a run artifact. Moving `gh-action-pypi-publish` into the reusable
  workflow would invalidate all four configured publishers.

The arm64 *image* verification jobs still drive the cluster with the image's own
CLI and the MCP agent harness rather than the SDK: their subject is the images,
and the wheel jobs above already cover arm64 pip installs without depending on a
PyPI upload having happened.

**Building the image locally on Apple Silicon needs a properly sized VM.** A
default `podman machine` (2 GiB) cannot do it: it thrashes to a load average of
31 with 26 MB free and no swap while still compiling `syn`/`tokio`, long before
DuckDB's C++ compile starts. Give the machine ≳8 GiB (`podman machine set
--memory 8192`) — or just pull the published image, which is the point of all of
the above.

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
service *starts* (keycloak); **`${VAR:-default}`** injects the
issuer/audience into the **existing** control-plane + pond-node definitions — a
profile can't do that. Without the env file the issuer renders **empty**, and the
binary normalizes a blank issuer to "auth off".

**The two runners sit in their OWN profile (`auth-tests`), and must.** A profile
is what `up` starts, so an `auth`-profiled runner is launched *eagerly* by
`docker compose --env-file auth.env up -d` — the moment `gateway` comes up,
seconds before Keycloak has imported its realm. That phantom run fails
(`fetch failed`: connection refused on the token endpoint), races the real
`run --rm` over the same bind-mounted `/repo` (two `npm ci` into one
`node_modules`), and surfaces in the on-failure log dump looking exactly like the
real step failing — which is how it read for the whole of run 33557528403.
`docker compose run <svc>` enables the target service's own profiles, so the
explicit invocations above are unaffected — and do **not** "help" it along with
`--profile auth-tests`: that flag *replaces* `COMPOSE_PROFILES` from the env
file rather than adding to it, which un-defines `keycloak` and fails the project
outright (`service "auth-tests-sdk" depends on undefined service "keycloak"`).
Name the service and let Compose work it out.

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

`docker-compose.yml` is **deliberately untouched** by all of this: the external-user
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
See `docs/releasing.md`. We ship **binaries only** — **two wheels → PyPI**
(`latiq`, `latiq-admin`) and the **`latiq` + `latiq-gateway` images → GHCR**.
**No Rust crates** (crates.io is out of scope; the crates are not a product), and
**no loose release-asset binaries** — that channel is gone.

**Three reusable workflows, for the same reason.** `verify.yml` holds every
check; **`images.yml`** holds every image build (multi-arch, native-per-arch,
manifest asserted); **`wheels.yml`** holds every wheel build (multi-platform,
native-per-platform, `dist/` asserted, installed on arm64). All three publish
paths call all three, so none can drift into running a different suite, pushing a
different manifest, or building a different set of wheels.

**PyPI uploads are gated on `PUBLISH_PYPI_WHEELS`** (a repo variable, off today)
in *both* paths and for *both* wheels. The wheels are still built and every
wheel-based test still runs — `verify.yml`'s e2e installs from `dist/`, the auth
suite installs by path — so the gate costs no coverage; only the upload stops.
The reason it exists: pre-1.0 we re-cut tags to stabilise the deployment, and
**a PyPI version is immutable** — burned on first upload, never reusable, so it
cannot be re-cut with everything else. A gated upload emits a `::warning::` and a
job-summary block, and the post-publish jobs that install *from PyPI* skip rather
than silently resolve an older version and report it as this run's.

Two publish paths, both **test-gated on the same reusable workflow**
(`.github/workflows/verify.yml` — refactored out of the nightly for exactly this
reason, #55):
- **`nightly.yml`** — rolling + change-gated. Wheels `0.1.0.devYYYYMMDDHHMM`,
  image `:nightly` / `:nightly-<stamp>`. Inert unless `PUBLISH_NIGHTLY=true`.
- **`release.yml`** — a `v<x>.<y>.<z>` tag. GitHub release + both wheels + images
  `:<version>`. `latest` only moves from 1.0.0 on.

**`latiq-admin` publishes from its own job** in both files (`publish-admin` /
`wheel-admin`), gated on `PUBLISH_ADMIN_WHEEL`. That split is load-bearing:
PyPI trusted publishing matches on the **workflow filename**, so `latiq-admin`
needs its own publisher entry per workflow, and a missing one must not be able to
wedge the `latiq` wheel that already works. The `latiq` upload completes before
the admin job starts. Off, the job **skips visibly**; on, it must pass.

**Never add a publishing step that does not `needs:` the verify job**, and never
let a publish be *verified* by anything weaker than the artifact itself: both
`verify-admin-cli` jobs assert on a resolved binary and an exact version string,
because a check that only inspects an exit status goes green having run nothing.
