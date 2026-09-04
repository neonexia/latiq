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
for third parties. We also publish **no sdist** — see *Why there is no sdist*.

Each wheel ships for **three platforms**: manylinux `x86_64`, manylinux
`aarch64`, and macOS `arm64`. `pip install` on an Apple Silicon Mac is a
first-class path, not an afterthought — it was broken for 0.1.0 and that is what
`wheels.yml` exists to prevent.

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

## The images: `.github/workflows/images.yml`

The same principle, applied to the artifacts rather than the checks. Both GHCR
images are built by **one** reusable workflow that `nightly.yml` and
`release.yml` call with a list of tag names, because the two paths must push the
*same* manifest.

They are **multi-arch manifest lists — `linux/amd64` + `linux/arm64`** — and that
is the whole point of the file existing ([#66](https://github.com/neonexia/latiq/issues/66)).
Before it, four copies of a `docker/build-push-action` step across two workflows
all omitted `platforms:`, so every published image silently inherited the
`ubuntu-latest` runner's arch and `docker pull` failed outright on Apple Silicon.

- `latiq` is built **once per arch on a native runner** (`ubuntu-latest` +
  `ubuntu-24.04-arm`), pushed by digest with no tag, and merged with
  `docker buildx imagetools create`. It compiles DuckDB from source — ~21 minutes
  native, hours under QEMU. **Do not collapse this into a single
  `platforms: linux/amd64,linux/arm64` job.**
- `latiq-gateway` (`FROM nginx` + one `COPY`, nothing executes) *is* a single
  multi-platform job.
- The `manifests` job then re-reads **every published tag of both images from
  ghcr.io** and fails unless both platforms are really in the index. A
  `platforms:` input we passed is not evidence; what the registry serves is.

`verify-deployment-arm64` (nightly) and `verify-release-arm64` (release) close
the loop: the **user compose**, from the **published images**, on a native arm64
runner, with no `--platform` and no `DOCKER_DEFAULT_PLATFORM`, asserting the
containers really are `arm64` and driving the MCP agent harness through the
gateway. They also run the literal `podman pull` from the bug report. The
amd64-only images survived two releases precisely because the only "verify user
path" job ran on `ubuntu-latest`.

## The wheels: `.github/workflows/wheels.yml`

The same principle again, for the PyPI channel — and it exists for the same bug,
one channel over ([#108](https://github.com/neonexia/latiq/issues/108)). `latiq`
0.1.0 has exactly **one** file on PyPI:

```
$ curl -s https://pypi.org/pypi/latiq/0.1.0/json | jq -r '.urls[].filename'
latiq-0.1.0-cp39-abi3-manylinux_2_28_x86_64.whl
```

No aarch64 wheel, no macOS wheel, no sdist — so on an Apple Silicon Mac
`pip install latiq` has nothing to resolve *and* no source fallback, and fails
outright. `latiq-admin`, which since [#102](https://github.com/neonexia/latiq/issues/102)
is the only non-Docker way to get the operator CLI, was about to inherit it. The
cause was one line in four copies: both wheel jobs in both workflows passed
`target: x86_64` and ran on `ubuntu-latest`.

**Three platforms, one build job each, all native runners:**

| Platform tag | Runner | Why native |
|---|---|---|
| `manylinux_2_28_x86_64` | `ubuntu-latest` | — |
| `manylinux_2_28_aarch64` | `ubuntu-24.04-arm` | the `latiq` wheel compiles DuckDB (~21 min native, hours under QEMU) |
| `macosx_*_arm64` | `macos-14` | the machine in the bug report |

Both wheels are `cp39-abi3` (`latiq-admin` is `py3-none`, a native binary), so it
is **one wheel per platform, not per Python version** — six wheels per run.

Two things make this more than a matrix:

- **`collect` asserts the built set.** `.github/scripts/assert-wheel-platforms.sh`
  reads the actual `dist/` filenames and fails unless every platform above is
  present for *both* projects, every file carries the version being published,
  and — explicitly — unless it examined more than zero files. The **callers** run
  the same script in `pypi` mode after an upload, against the filenames the PyPI
  JSON API reports: what `pip install` will actually resolve from, read back from
  the published thing. Run it against the broken release and it reproduces the
  bug (`assert-wheel-platforms.sh pypi latiq 0.1.0` → two missing platforms).
- **`verify-linux-arm64` and `verify-macos-arm64` install and run them.** Not "an
  aarch64 tag exists": `pip install` the wheel (no `--platform`, no index — pip
  refuses a wheel this machine's tags do not support), then `latiq --version`,
  `import latiq`, `latiq serve` proved by a `latiq pond list` round-trip against
  it, `sdk/python/tests`, and the admin wheel's lean-build refusal. These use the
  wheels the run **built**, not PyPI, so the coverage does not depend on
  `PUBLISH_PYPI_WHEELS` being on.

**The build moved into `wheels.yml`; the upload did not.** PyPI trusted
publishing matches on the *workflow filename*, so `pypa/gh-action-pypi-publish`
stays in `nightly.yml` / `release.yml`, which is what the configured publishers
authorise. Those jobs download the wheels as a run artifact (`wheels-latiq`,
`wheels-latiq-admin`) instead of building them.

### Why there is no sdist

Deliberate, not an omission. An sdist would be a fallback for platforms we do not
build wheels for — but `pip install latiq` from source needs **`protoc` and a
Rust toolchain on the installing machine** plus a ~20-minute DuckDB compile, and
we do not build or test that path on any platform where it would be the *only*
option (Windows, musl/Alpine, Intel macOS). A build that fails twenty minutes in
with a `protoc` error is a worse answer than pip's immediate *"no matching
distribution found"*, and shipping one would let us believe a platform is covered
when it is not.

So: **cover platforms by building wheels for them.** Adding one means adding a
row to `wheels.yml`'s matrix *and* an entry to `EXPECTED_TAGS` in
`assert-wheel-platforms.sh` — the assertion is worth nothing if it only knows
about the platforms we already build. Revisit the sdist only if we decide to
support a platform we are not willing to run a build job for, and then only with
a CI job that installs *from the sdist* in a clean environment.

Not covered today, and known: **Intel macOS** (GitHub's x86_64 macOS runners are
being retired and Apple Silicon is the audience), **Windows**, and **musl**.

---

## Path 1 — the nightly (rolling, unversioned)

`.github/workflows/nightly.yml`, 07:00 UTC and on `workflow_dispatch`.

1. `verify` runs.
2. `gate` decides whether to publish at all: only if the repo variable
   **`PUBLISH_NIGHTLY=true`** *and* there are commits since the last `nightly-*`
   tag (`force_publish: true` on a manual run bypasses the change check, never
   the tests).
3. `wheels` (`needs: [gate, verify]`, `wheels.yml`) builds **both** wheels for
   **all three platforms** and verifies the set; `publish` (`needs: [gate,
   wheels]`) downloads the `latiq` wheels and uploads them to PyPI **if
   `PUBLISH_PYPI_WHEELS=true`** (see *PyPI is gated*, below), then asserts PyPI
   serves every platform. `images` (`needs: [gate, verify]`, `images.yml`) pushes both
   multi-arch images in parallel, and `mark-published` tags the commit
   `nightly-<stamp>` — the marker the next run's change-gate reads — only after
   **both** have succeeded. That tag used to be the last step of `publish`; with
   the images in their own job, tagging from `publish` would mark the commit
   published while the arm64 image was still building, and the next nightly
   would skip it.
4. `publish-admin` (`needs: [gate, wheels, publish]`, and gated on
   **`PUBLISH_ADMIN_WHEEL=true`**) uploads the `latiq-admin` wheels — built and
   version-stamped in `wheels.yml`, at the same version as the `latiq` wheel and
   the image from this run. It is a **separate job**
   so that a missing trusted publisher for the new wheel cannot block the `latiq`
   wheel, which is already published by the time it starts.
5. `verify-published`, `verify-deployment` and `verify-admin-cli` prove the
   *published* artifacts work for an external user: the wheel's Python API, the
   `latiq serve` console script it now installs, the images through the user
   compose, and the `latiq-admin` CLI installed from PyPI.

   Wheel versions are stamped **inside `wheels.yml`**, once per build job: each
   platform builds on its own runner, so a single caller-side `sed` would reach
   one of six builds.

Nightly versions are **development versions** and are meant to be disposable:
wheel `<base>.devYYYYMMDDHHMM`, images `:nightly` and `:nightly-<stamp>`. They
never move `:latest`, and they never create a GitHub release.

---

## PyPI is gated, and PyPI is immutable

**`PUBLISH_PYPI_WHEELS`** (a repo variable, off by default) is the switch for
*every* PyPI upload — both wheels, both paths. It is off deliberately while the
0.1.x deployment is being stabilised: the images are being re-cut repeatedly and
the wheels should not follow them onto a channel that cannot be undone.

Two things follow, and both matter:

- **The wheels are still built, and every wheel-based test still runs.**
  `verify.yml`'s e2e installs the wheel from `dist/` and the auth suite installs
  it by path, so gating the upload costs no coverage. Only the *upload* stops.
- **A skipped upload must never look like a publish.** Each gated step emits a
  `::warning::` and writes a job-summary block naming the version and the switch,
  and the post-publish jobs that install *from PyPI* (`verify-published`,
  `verify-admin-cli`, the wheel half of `verify-deployment` / `verify-release`)
  are skipped rather than left to resolve an *older* version and report it as
  this run's.

**Before turning `PUBLISH_PYPI_WHEELS` on, check that the multi-platform build
is green.** This is not a formality: 0.1.0 burned a version number on a wheel set
that is unusable on Apple Silicon, and it can never be re-cut
([#108](https://github.com/neonexia/latiq/issues/108)). The first run with the
gate on must have a green `wheels` job — which means `collect` found all three
platforms for both projects, and both arm64 verification jobs installed and ran
what it built. The publish jobs then re-assert the same thing against PyPI
itself, so a first upload that lands amd64-only fails the run rather than
becoming the next version nobody can install.

**PyPI versions are immutable.** A version number is burned on first upload; a
re-upload is rejected and deleting the release never frees the number for reuse.
So a changed wheel always needs a **new version number** — there is no republish.
(Concretely: the `latiq` **0.1.0** wheel on PyPI predates the CLI-as-wheels work,
carries no `latiq` console script, is amd64-only, and can never be replaced. Only
a later version can fix it — and it is worth **yanking** independently of that,
since it is the version a plain `pip install latiq` resolves today.)
`latiq-admin` has never been published at all.

This is the one asymmetry between the channels: GHCR tags, GitHub releases and
their assets can all be rewritten. PyPI cannot.

## Path 2 — a versioned release (what a user pins)

`.github/workflows/release.yml`, on a `v<major>.<minor>.<patch>` tag push (or a
manual run with that tag selected as the ref).

### Cutting one

```bash
# 1. Bump the version in ALL FOUR declarations (they must match the tag exactly).
#    - Cargo.toml                → [workspace.package] version (every crate
#                                  inherits it with `version.workspace = true`)
#    - sdk/python/Cargo.toml     → [package] version — hard-coded, NOT inherited
#    - sdk/python/pyproject.toml → [project] version   (the `latiq` wheel)
#    - sdk/admin/pyproject.toml  → [project] version   (the `latiq-admin` wheel)
$EDITOR Cargo.toml sdk/python/Cargo.toml sdk/python/pyproject.toml sdk/admin/pyproject.toml
cargo update --workspace                  # refresh Cargo.lock
(cd sdk/python && cargo update --workspace)   # …and sdk/python's own lock

# 2. Point the published compose at the version you are about to ship.
#    Leave it and the release exists but nothing directs a user to it. `meta`
#    checks this too now — a mismatch fails the release before anything builds.
$EDITOR deploy/docker-compose.yml   # LATIQ_IMAGE / LATIQ_GATEWAY_IMAGE defaults

# 3. Land it on main through the normal PR flow.

# 4. Tag the merged commit and push the tag.
git checkout main && git pull
git tag v0.2.0
git push origin v0.2.0
```

The workflow fails fast (in `meta`, before anything is built) if the tag is
malformed, if **any** of the four version declarations disagrees with it, or if
the compose's two image pins are not that version. Nothing is rewritten at
release time on purpose: the tagged tree must be exactly what was published, byte
for byte. **If you add a fifth place a version lives, add it to `meta`** — an
unchecked one is how a release ships an artifact at a version that is not the
tag, which is exactly how `sdk/python/Cargo.toml` sat at `0.1.0` unnoticed (only
`pyproject.toml` feeds maturin, so nothing ever read it).

### Re-cutting a pre-1.0 tag is a sanctioned workflow, not an incident

While 0.1.x is being stabilised, **force-moving a `v0.1.z` tag to re-run the
whole gated pipeline is expected** — the version is not something anyone has
pinned in production yet, and re-running the tag is how a deployment fix gets
proven end to end.

```bash
git tag -f v0.1.1 && git push -f origin v0.1.1
# or: Actions → Release → Run workflow, selecting the same tag as the ref
```

Everything except PyPI takes this happily: the GHCR tags are overwritten by the
new manifest, the GitHub release is reused, and `gh release upload --clobber`
replaces the assets. PyPI is the exception, which is why `PUBLISH_PYPI_WHEELS`
exists — see *PyPI is gated, and PyPI is immutable* above. From 1.0.0 this stops:
a released tag is then something people pin, and the answer is `v1.0.1`.

### What the tag runs, in order

```
meta  (parse v0.2.0, check all four version declarations + the compose pins,
       decide prerelease, compute the image tags)
  └─ verify  (fmt + clippy + workspace tests, iceberg, cluster scale-out,
              e2e suite, auth e2e)          ←── THE GATE
       ├─ wheels (wheels.yml)  → both wheels × {manylinux x86_64, manylinux
       │                         aarch64, macOS arm64}; the built set asserted,
       │                         then installed + run on linux arm64 and macOS
       │                         arm64
       ├─ release              → GitHub release for the tag, --generate-notes
       │    ├─ wheel           → `latiq` (from `wheels`) → PyPI if
       │    │                     PUBLISH_PYPI_WHEELS, then PyPI's own file list
       │    │                     asserted; always attached to the release
       │    └─ wheel-admin     → `latiq-admin`, same rules
       │         └─ verify-admin-cli → pip install latiq-admin==0.2.0 from PyPI,
       │                               drive the binary it puts on PATH
       ├─ images (images.yml)  → ghcr.io/neonexia/latiq{,-gateway}:0.2.0 (+ :latest),
       │                         amd64 + arm64, manifest asserted from the registry
       ├─ verify-release       → user compose + published images (+ the wheel from
       │                         PyPI when one was published), SDK + MCP suites
       └─ verify-release-arm64 → the same user compose + images on a NATIVE arm64
                                 runner, no --platform, plus the podman pull
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
compose pins an explicit release version (`0.1.1` today), overridable via
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
# Both platforms are really in the manifest list (what #66 was):
docker buildx imagetools inspect ghcr.io/neonexia/latiq:0.2.0
docker buildx imagetools inspect ghcr.io/neonexia/latiq-gateway:0.2.0
# Only if PUBLISH_PYPI_WHEELS was on for the run:
pip download latiq==0.2.0 --no-deps -d /tmp/x     # PyPI has the SDK wheel
pipx install latiq-admin==0.2.0 && latiq --version # → `latiq 0.2.0`, the operator CLI
# Every platform is really there (what #108 was) — the same check CI runs:
.github/scripts/assert-wheel-platforms.sh pypi latiq 0.2.0
.github/scripts/assert-wheel-platforms.sh pypi latiq-admin 0.2.0
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

   These four are still correct after [#108](https://github.com/neonexia/latiq/issues/108)
   moved the wheel *builds* into the reusable `wheels.yml`: the
   `pypa/gh-action-pypi-publish` step deliberately **stayed** in `nightly.yml`
   and `release.yml`, precisely because PyPI matches the filename of the workflow
   the upload runs in. Do not move it — a publisher entry for `wheels.yml` would
   have to be added first, and the two existing ones would stop authorising
   anything.

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
3b. **Enable PyPI uploads**: same place → **`PUBLISH_PYPI_WHEELS` = `true`**.
   Off today, on purpose, while 0.1.x is being stabilised by re-cutting tags:
   images and GitHub releases can be rewritten, a PyPI version never can. With it
   off both wheels are still built and every wheel-based test still runs; the
   uploads and the post-publish jobs that install from PyPI are skipped, loudly
   (a `::warning::` plus a job-summary block). Turn it on for the first release
   whose wheel is meant to be permanent.
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
