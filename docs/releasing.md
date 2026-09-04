# Releasing Latiq

Latiq ships **binaries, not source packages**. Four artifacts, one version:

| Artifact | Where | Who consumes it |
|---|---|---|
| **`latiq` wheel** | PyPI (`pip install latiq`) | the Python SDK, the embedded/standalone mode, **and the full `latiq` CLI incl. server roles** |
| **`ghcr.io/neonexia/latiq`** | GHCR | the server (all roles) — agents point at its MCP endpoint, SDK/CLI at its gRPC endpoints |
| **`ghcr.io/neonexia/latiq-gateway`** | GHCR | nginx + the baked front-door config, so the user compose is pure images |
| **`latiq-admin` wheel** | PyPI (`pipx install latiq-admin`) | operators — the lean client-only `latiq` (no server, no DuckDB) |

Both wheels install a command called `latiq`; they are two builds of one CLI, so
one or the other goes into a given environment, not both. The admin wheel carries
a **native executable** (maturin `bindings = "bin"`) — pip is the delivery
mechanism, nothing about it is Python.

This replaced a `curl … | sh` installer plus four cross-compiled binaries on a
rolling `cli-latest` GitHub release. **Do not reintroduce that channel.** PyPI
already does platform selection, pinning, upgrade and uninstall, and it does not
require piping an unsigned script into a shell.

We do **not** publish the Rust crates to crates.io. The `latiq-*` crates are
internal structure, not a product surface; nothing is gained by versioning them
for third parties.

There are **two publish paths**, and both are gated on **the same** verification.

---

## The gate: `.github/workflows/verify.yml`

One reusable workflow (`on: workflow_call`) holds every check:

- `checks` — `cargo fmt --all --check`, `cargo clippy --workspace --all-targets -D warnings`, `cargo test --workspace`
- `iceberg` — the Iceberg REST + MinIO external-catalog e2e
- `cluster-scale-out` — a real dockerised cluster, add a node, allocate, query
- `e2e-suite` — the `e2e/` suite (Python SDK, the MCP agent harness, perf) against a gatewayed 3-node cluster, plus the embedded `pip install` path
- `auth-e2e` — identity v0 against a Keycloak-authenticated cluster

It is called by `nightly.yml` and by `release.yml`, so **a tagged release runs
exactly what the nightly runs**. It used to be inlined in the nightly only —
which is how [#55](https://github.com/neonexia/latiq/issues/55) happened: the old
`release-images.yml` published a GHCR image on any `v*` tag with **no test step
at all**. That workflow is deleted.

Rule: **a publishing job must always `needs:` the verify job.** Adding a check to
`verify.yml` gates both paths at once; inlining a check into one caller does not.

---

## Path 1 — the nightly (rolling, unversioned)

`.github/workflows/nightly.yml`, 07:00 UTC and on `workflow_dispatch`.

1. `verify` runs.
2. `gate` decides whether to publish at all: only if the repo variable
   **`PUBLISH_NIGHTLY=true`** *and* there are commits since the last `nightly-*`
   tag (`force_publish: true` on a manual run bypasses the change check, never
   the tests).
3. `publish` (`needs: [gate, verify]`) builds the manylinux x86_64 wheel in-job,
   uploads it to PyPI, pushes both images, then tags the commit `nightly-<stamp>`
   — the marker the next run's change-gate reads.
4. `publish-admin` (`needs: [gate, publish]`, and gated on
   **`PUBLISH_ADMIN_WHEEL=true`**) stamps `sdk/admin/pyproject.toml` to the same
   version and uploads the `latiq-admin` wheel to PyPI. It is a **separate job**
   so that a missing trusted publisher for the new wheel cannot block the `latiq`
   wheel, which is already published by the time it starts.
5. `verify-published`, `verify-deployment` and `verify-admin-cli` prove the
   *published* artifacts work for an external user: the wheel's Python API, the
   `latiq serve` console script it now installs, the images through the user
   compose, and the `latiq-admin` CLI installed from PyPI.

Nightly versions are **development versions** and are meant to be disposable:
wheel `0.1.0.devYYYYMMDDHHMM`, images `:nightly` and `:nightly-<stamp>`. They
never move `:latest`, and they never create a GitHub release.

## Path 2 — a versioned release (what a user pins)

`.github/workflows/release.yml`, on a `v<major>.<minor>.<patch>` tag push (or a
manual run with that tag selected as the ref).

### Cutting one

```bash
# 1. Bump the version in ALL THREE places (they must match the tag exactly).
#    - Cargo.toml               → [workspace.package] version
#    - sdk/python/pyproject.toml → [project] version   (the `latiq` wheel)
#    - sdk/admin/pyproject.toml  → [project] version   (the `latiq-admin` wheel)
$EDITOR Cargo.toml sdk/python/pyproject.toml sdk/admin/pyproject.toml
cargo build --workspace          # refresh Cargo.lock

# 2. Point the published compose at the version you are about to ship.
#    Not checked by `meta` (it is not a package version) and easy to forget —
#    leave it and the release exists but nothing directs a user to it.
$EDITOR deploy/docker-compose.yml   # LATIQ_IMAGE / LATIQ_GATEWAY_IMAGE defaults

# 3. Land it on main through the normal PR flow.

# 4. Tag the merged commit and push the tag.
git checkout main && git pull
git tag v0.2.0
git push origin v0.2.0
```

The workflow fails fast (in `meta`, before anything is built) if the tag is
malformed or if **any** of the three versions in the tree disagrees with it.
Nothing is rewritten at release time on purpose: the tagged tree must be exactly
what was published, byte for byte. **If you add a fourth place a version lives,
add it to `meta`** — an unchecked one is how a release ships an artifact at a
version that is not the tag.

### What the tag runs, in order

```
meta  (parse v0.2.0, check Cargo.toml + BOTH pyproject.toml agree, decide prerelease)
  └─ verify  (fmt + clippy + workspace tests, iceberg, cluster scale-out,
              e2e suite, auth e2e)          ←── THE GATE
       ├─ release          → GitHub release for the tag, --generate-notes
       │    ├─ wheel       → `latiq` → PyPI (trusted publishing) + attached to the release
       │    └─ wheel-admin → `latiq-admin` → PyPI + attached to the release
       │         └─ verify-admin-cli → pip install latiq-admin==0.2.0 from PyPI,
       │                               drive the binary it puts on PATH
       ├─ images           → ghcr.io/neonexia/latiq{,-gateway}:0.2.0 (+ :latest)
       └─ verify-release   → user compose + published images + `pip install latiq==0.2.0`,
                             the `latiq serve` console script, SDK + MCP agent suites
```

If `verify` fails, **nothing publishes** — no release, no wheel, no image. There
is no `continue-on-error` and no `if: always()` on any publishing job.

`wheel-admin` is a separate job from `wheel`, and deliberately so: `latiq-admin`
is its own PyPI project needing its own trusted-publisher entry, and until that
exists the new wheel must not be able to wedge the one that already works. It is
gated on **`PUBLISH_ADMIN_WHEEL=true`** — off, it *skips* (visible in the run's
job list) rather than reporting a publish it did not do.

### Prerelease and `latest`

`v0.*` is marked a **prerelease** on GitHub and does **not** move the `:latest`
image tag. Pre-1.0 means the API can break between minors, and an unpinned
`docker pull ...:latest` should not silently pick that up. From `v1.0.0` the
release is marked latest and `:latest` moves with it. Until then the published
compose pins an explicit release version (`0.1.0` today), overridable via
`LATIQ_IMAGE` / `LATIQ_GATEWAY_IMAGE`. **Bump those defaults as part of cutting a
release** — otherwise a new version ships that the compose never points anyone
at.

### Verifying it worked

The `verify-release` job already does the real check — it installs the published
wheel from PyPI and runs the SDK + MCP suites against the published images
through the user compose. If it is green, the release is good. By hand:

```bash
gh release view v0.2.0 --repo neonexia/latiq      # notes + 2 assets (both wheels)
docker pull ghcr.io/neonexia/latiq:0.2.0          # anonymous — proves the package is public
pip download latiq==0.2.0 --no-deps -d /tmp/x     # PyPI has the SDK wheel
pipx install latiq-admin==0.2.0 && latiq --version # → `latiq 0.2.0`, the operator CLI
```

### Rolling back a bad release

Published artifacts are effectively immutable — **roll forward**, don't try to
un-ship.

1. **Stop the bleeding first**, in this order of impact:
   - GHCR: repoint `:latest` at the last good version —
     `docker buildx imagetools create -t ghcr.io/neonexia/latiq:latest ghcr.io/neonexia/latiq:0.1.9`
     (same for `latiq-gateway`). Do **not** delete the bad tag; anything pinned to
     it breaks, and deleting does not fix anyone who already pulled.
   - PyPI: **yank** the version (`https://pypi.org/manage/project/latiq/` →
     Releases → Yank). A yanked version stays installable if explicitly pinned but
     is never resolved for a plain `pip install latiq`. **Do not delete it** —
     PyPI never lets that version number be reused.
   - GitHub: mark the release a prerelease (`gh release edit v0.2.0 --prerelease`)
     so it loses the "Latest" badge, and note the problem in the release body.
   - The operator CLI: **yank `latiq-admin`** the same way as `latiq`
     (`https://pypi.org/manage/project/latiq-admin/`). It is a normal PyPI
     project, so it rolls back exactly like the SDK wheel — there is no rolling
     asset bucket to repair any more.
2. **Fix forward**: land the fix, bump to the next patch version, tag it. The tag
   path re-runs the full suite, so the replacement is verified.
3. Never re-point an existing tag at a new commit. Cut `v0.2.1`.

If the failure happened *mid-release* (say `verify` was green, images pushed, and
the PyPI upload flaked), re-run the workflow from the Actions UI with the same
tag: every step is idempotent except PyPI, which refuses to re-upload a version
that already exists. If the wheel did land and the run failed after it, the
re-run's PyPI step is the one thing you must skip or accept as a failure.

---

## One-time setup

Both publish paths are **inert** until these are done.

1. **PyPI trusted publishers.** PyPI matches on the *workflow filename*, so
   **each publishing workflow needs its own entry**, on **each project** — a
   single entry for `nightly.yml` will **not** authorise `release.yml`, and an
   entry on `latiq` will **not** authorise a `latiq-admin` upload. A missing one
   fails the PyPI step with an OIDC error. Two projects × two workflows = **four
   publishers** in total (Owner `neonexia`, Repository `latiq`, Environment
   blank):

   | On | Workflow filename |
   |---|---|
   | https://pypi.org/manage/project/latiq/settings/publishing/ | `nightly.yml` |
   | https://pypi.org/manage/project/latiq/settings/publishing/ | `release.yml` |
   | https://pypi.org/manage/project/latiq-admin/settings/publishing/ | `nightly.yml` |
   | https://pypi.org/manage/project/latiq-admin/settings/publishing/ | `release.yml` |

   `latiq-admin` does not exist on PyPI yet. Create it with a **pending
   publisher** (same page, before any upload: https://pypi.org/manage/account/publishing/)
   so the first CI upload can claim the name — otherwise the name has to be
   claimed by a manual upload first.
2. **Enable the admin wheel**: repo → *Settings* → *Secrets and variables* →
   *Actions* → *Variables* → **`PUBLISH_ADMIN_WHEEL` = `true`**, once step 1's
   `latiq-admin` publishers exist. Until it is set, `publish-admin` /
   `wheel-admin` and their verification jobs **skip** — visibly, in the run's job
   list — and the `latiq` wheel and images publish exactly as before. Read this
   as the switch that turns the operator-CLI channel on for the first time; it
   is deliberately separate from `PUBLISH_NIGHTLY` so a not-yet-configured new
   channel cannot fail a publish path that works.
3. **Enable the nightly publish**: same place → **`PUBLISH_NIGHTLY` = `true`**.
   The tagged-release path does not read this variable and is always live.
4. **GHCR** needs no secret (built-in `GITHUB_TOKEN`), but both packages —
   `latiq` and `latiq-gateway` — must be set **public** in the org's *Packages*
   settings, or the anonymous-pull verification jobs and every external user
   fail.
5. **CLI binaries**: nothing to configure, and nothing to keep. The CLI ships as
   the `latiq-admin` wheel; the `cli-latest` rolling release, the four
   cross-compiled assets and `deploy/install.sh` are gone, and with them the
   `LATIQ_DEPLOY_TOKEN` secret that once pushed assets to `neonexia/latiq-deploy`
   (safe to delete). Assets already published under `cli-latest` stay resolvable
   for anyone running an old installer; they are simply never refreshed again.

---

## Housekeeping: the lineage facet tag

`crates/latiq-lineage/src/event.rs` stamps our custom OpenLineage facets with

```
https://raw.githubusercontent.com/neonexia/latiq/lineage-facets-1-0-0/crates/latiq-lineage/spec/facets/1-0-0/...
```

That URL **404s today** — the `lineage-facets-1-0-0` ref does not exist. Now that
the repo is public, creating it once makes every emitted `_schemaURL` resolve to
the real schema for downstream consumers (Marquez et al.):

```bash
# from a commit that contains crates/latiq-lineage/spec/facets/1-0-0/
git tag lineage-facets-1-0-0
git push origin lineage-facets-1-0-0
```

Read `crates/latiq-lineage/CLAUDE.md` before touching this. The rule it encodes:
**that version is the facet's *shape*, not a release version.** It moves only
when a facet's fields change, and each facet versions independently. Never bump
it at release time — floating it with the crate version would make every Latiq
release look like a brand-new facet type to every downstream consumer. So this
tag is created **once**, and a new `lineage-facets-<v>` tag is created only when
a facet schema actually changes. It is not part of the release flow, and
`release.yml` deliberately ignores any tag that is not `v*`.
