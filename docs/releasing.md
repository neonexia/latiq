# Releasing Latiq

We ship **two** artifacts for the two consumers we have today — the **Python SDK**
and **agents (MCP)**:

1. **`latiq` on PyPI** — the Python wheel (`pip install latiq`). A compiled binary;
   **no source is published.**
2. **`ghcr.io/neonexia/latiq`** — the single-binary container image (agents point
   at its MCP endpoint; the SDK/CLI point at its gRPC endpoints).

We do **not** publish the Rust crates and the repo stays **private** — both are
deferred to "when we open-source." (Publishing to crates.io would make the source
public, since a crate *is* its source; a wheel/image is a binary, so it doesn't.)

## How nightly publishing works (`.github/workflows/nightly.yml`)

Every night (and on `workflow_dispatch`):

1. The test + e2e jobs run (`checks`, `iceberg`, `cluster-scale-out`, `e2e-suite`).
2. **`gate`** decides whether to publish: only if **`PUBLISH_NIGHTLY=true`** AND
   there are **new commits since the last `nightly-*` tag**. Otherwise it stops.
3. **`build-wheels`** runs only if the gate said yes *and every test job is green*
   (it `needs` all of them) — builds the wheel for Linux (manylinux x86_64) and
   macOS (arm64), stamped with one version.
4. **`publish`** uploads the wheels to **PyPI** (trusted publishing, no stored
   token) and builds + pushes the **image** to GHCR with the **same version**, then
   tags the commit `nightly-<stamp>` (the marker the next run's change-gate reads).

So: **unchanged → nothing publishes; changed + tests green → both artifacts publish,
same version.** A failing test publishes nothing.

### Versioning
One version per publish, identical across wheel + image:
- Nightly: wheel `0.1.0.devYYYYMMDDHHMM`, image `nightly-YYYYMMDDHHMM` (+ moving
  `nightly`). Base `0.1.0` comes from `sdk/python/pyproject.toml`.
- Real release: push a `v0.2.0` git tag → `release-images.yml` publishes the image
  `0.2.0`. (Bump the base version in `pyproject.toml` + `Cargo.toml` first.)

## One-time setup to turn publishing on

The nightly publish chain is **inert** until you do this — `gate` short-circuits
while `PUBLISH_NIGHTLY` is unset.

1. **PyPI project + trusted publisher** (no token to store):
   - Log in to https://pypi.org → create/claim the project name **`latiq`**.
   - Project → *Settings* → *Publishing* → *Add a trusted publisher* (GitHub):
     - Owner: `neonexia` · Repository: `latiq`
     - Workflow filename: `nightly.yml`
     - Environment: *(leave blank)*
   - (For real-release wheels via `v*` tags, also add a trusted publisher for the
     release workflow if/when we add one.)
2. **Enable it**: repo → *Settings* → *Secrets and variables* → *Actions* →
   *Variables* → add **`PUBLISH_NIGHTLY` = `true`**.
3. GHCR needs nothing — it uses the built-in `GITHUB_TOKEN`. Make the
   `latiq` package **public** in the org's *Packages* settings if external users
   should pull it without auth.

That's it. The next nightly (or a manual *Run workflow*) with a code change will
publish `latiq` to PyPI and the image to GHCR.

## Deferred (when we open-source)
- Rust crates → crates.io (requires publishing the whole crate tree's source +
  per-crate metadata).
- Repo → public (+ add `LICENSE`, drop internal notes under `.claude/`, and slim
  the git history that still carries the #48 build-artifact blobs).
- k8s manifests.
