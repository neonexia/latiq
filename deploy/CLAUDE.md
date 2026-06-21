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

**Lean by default.** A bare `docker compose up` = control plane + 2 pond nodes +
gateway (what an agent/SDK user needs). Profiles gate the rest: `test` =
Prometheus + MinIO + Iceberg-REST (internal testing/obs only), `scale` =
pond-node-3 (scale-out test), `tools` = the in-network `cli` helper.

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
