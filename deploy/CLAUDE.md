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
