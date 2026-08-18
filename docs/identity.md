# Latiq — Identity, Authorization, and Isolation

*Design note. Not yet a spec — this is the discussion that has to settle before the
authorization slice is built. Nothing here is implemented except where marked
**today**.*

---

## Where we are today

```rust
pub struct Identity {
    pub agent_id: String,
    pub verified: bool,   // always false in M1
}
```

- The agent **claims** an id. On the MCP surface it arrives as an optional
  `agent_id` tool argument; on gRPC as metadata. Absent or blank → `anonymous`.
- Nothing verifies it. Any caller can assert any id.
- The id is used for exactly two things: **attribution** (it rides DuckLake's
  native `set_commit_message`, so `pond.snapshots()` shows who wrote what) and
  the **`latiq::access` trail** (`agent`, `verified`, `op`, `pond`, `duration_ms`,
  redacted SQL shape).
- It is used for **nothing else**. There are no pond ACLs. There are no
  per-principal catalog grants — the catalog "allowlist" in the code is a
  *parameter* allowlist that strips credential-shaped keys at registration, not
  an access control list. Any agent that can reach the gateway can allocate a
  pond, read any pond, and pull from any registered catalog.

This is a defensible M1 posture for a single trusted deployment. It does not
survive the production topology the product spec describes.

---

## The problem

Production agents don't arrive one at a time. A workflow starts and spawns a
graph — parallel branches, sequential stages, potentially hundreds of agents over
one run. Multiple workflows share a cluster on purpose, because a busy cluster is
an efficiently used one.

That breaks a flat agent identity in three specific ways.

**1. Grants can't be written against agent instances.** Nobody is going to add
three hundred ephemeral ids to an access list — and the ids don't exist until the
workflow is already running. Any scheme where an operator enumerates agents is
dead on arrival.

**2. The grant has to precede and outlive the agent.** An agent in stage three
needs access to a pond created in stage one by an agent that has already exited.
Authority therefore belongs to something with the run's lifetime, not the agent's.

**3. Attribution and authorization want different granularities.** Authorization
wants coarse and stable: *this workflow, in this tenant, may read this catalog*.
Attribution wants fine and disposable: *agent 147 of stage 2 wrote this snapshot*.
Collapsing both onto one string forces you to choose, and either choice is wrong.

---

## Proposal: a principal hierarchy

Replace the flat id with a chain. Four levels, each optional below the first, so
the simple cases stay simple.

```
tenant  →  workflow (run)  →  stage  →  agent instance
```

| Level | Lifetime | Example | Role |
|---|---|---|---|
| `tenant` | permanent | `acme-risk` | billing, hard isolation boundary |
| `workflow` | one run, minutes to hours | `wf-incident/run-8812` | **the principal grants bind to** |
| `stage` | part of a run | `stage-2-enrich` | optional scoping, readable history |
| `agent` | seconds to minutes | `agent-147` | attribution, revocation, rate limiting |

**The rule that falls out of it:**

> **Authorization binds to the tenant and the workflow. Attribution records the
> whole chain.**

Everything else follows. Catalog grants are written against a tenant or a
workflow *role* — never an agent id. Pond ACLs are owned by the run. The access
trail and the DuckLake commit message carry the full chain, so a 300-agent
history is readable and one misbehaving agent is individually revocable and
individually rate-limited without touching the grant model.

### Wire shape

The chain should be one structured claim, not four headers to be assembled by
each adapter. Something like:

```
latiq-principal: tenant=acme-risk; workflow=wf-incident/run-8812; stage=2-enrich; agent=147
```

`latiq-agent-core` stays protocol-neutral (invariant 5) — each inbound adapter
parses its transport's carrier into the same `Principal` type. `Identity` becomes
`Principal` with `agent_id` retained as the leaf, which keeps the existing
attribution and access-trail code working through the transition.

**Open:** on MCP the id is currently a *tool argument*, which means the model
itself can type anything into it. That's fine for claimed identity and
unacceptable for verified identity — a verified principal must arrive out of band
(HTTP header or transport credential), never as a tool parameter the model
controls. This is a breaking change to the MCP tool schemas and should happen
before the tools have wide adoption.

---

## Verification: where does the token come from?

Three options, not mutually exclusive.

**A. Workflow-issued token (favored).** The orchestrator authenticates once per
run against the IdP and receives a token scoped to the run. Agents inherit it and
add their leaf id in-band. Matches how workflow engines already handle secrets;
one authentication per run rather than per agent; revoking a run revokes every
agent in it. Weakness: an agent can spoof its *leaf* id — acceptable, since the
leaf is attribution, not authority.

**B. Per-agent minted token.** The harness mints a short-lived, narrowly-scoped
token per agent from the run's credential. Strongest attribution and per-agent
revocation. Cost: a minting service and hundreds of token issuances per run.

**C. Workload identity (SPIFFE / k8s ServiceAccount).** In a cluster-resident
deployment, the agent pod already has a verifiable identity. Latiq maps the
workload identity to a tenant and reads the workflow from the request. Zero new
credential plumbing where it applies; says nothing about agents that share a pod.

A reasonable path: **C for the tenant, A for the run, in-band for the leaf.**
Each level verified by the cheapest mechanism that can verify it.

---

## Pond authorization

Today: no ACLs at all.

Proposed model, deliberately small:

- Every pond has an **owner principal**, set at allocation. Default: the
  allocating agent's *workflow*, not the agent.
- Three modes: `read`, `write`, `admin` (describe/drop). Grants are held against
  a tenant, a workflow, or a workflow role.
- Default visibility: **same workflow gets `write`, same tenant gets nothing.**
  Cross-workflow sharing is an explicit grant. This is the safe default for a
  cluster running several workflows at once, and it's the case the current
  system gets wrong.
- Pond discovery (`list_ponds`) filters to what the caller can see. Today it
  lists everything, which is both a leak and a scaling problem once a cluster
  runs many concurrent workflows.

**Open:** can an agent grant access to a pond its workflow owns, or is granting
an operator action? Agent-granting is ergonomic and matches "the agent is the
customer"; operator-granting is what a compliance team will ask for. A middle
path — agents may grant within their tenant, operators may grant across tenants
— is probably right.

---

## Catalog authorization

Today a registered catalog is reachable by anyone who can reach the gateway.
Credentials are not stored, which is a genuine mitigation: the caller must supply
the credential at pull time, so the catalog registration alone grants nothing.
That's the reason this is not yet urgent — and also the reason the design must
land before the identity-bearer integration replaces caller-supplied credentials.

Once Latiq can present the caller's identity to the source, **the registration
becomes the grant**, and it needs an owner. Grants should bind to tenant or
workflow role. The `describe`/`pull` path should filter the catalog menu to what
the principal may use — a catalog an agent can't pull from shouldn't appear in the
list at all, both for governance and to keep the menu small enough for a model to
reason about.

---

## Isolation is three problems

They get conflated constantly. They have different mechanisms and different
owners.

**Access isolation** — which principals may touch which pond and which catalog.
Policy in the control plane registry, enforced on the pond node. Not built.

**Resource isolation** — one pond's scan not starving its neighbors. **Today:**
per-pond tiers (small/medium/large/x-large) map to hard `memory_limit` and
`threads` caps on the pond's DuckDB instance. This is the piece that already
works, and it's the direct answer to "won't this starve my agents?"

**Placement** — which node a new pond lands on. Not built; ponds are placed
without regard to load or tier. On a cluster deliberately kept busy this is the
gap that turns into a noisy-neighbor incident, and it's the one that costs the
least to fix: tier-aware binpacking against the per-node metrics already emitted
(`latiq_node_open_ponds`, `latiq_inflight_queries`, `latiq_process_memory_bytes`).

---

## Pond lifecycle — an ownership problem, not an identity one, but it lands here

`default_pond_lifetime_seconds` (3600) exists in the policy table and **nothing
reads it.** There is no pond reaper. Node liveness is reaped on a 30s TTL; ponds
are not reaped at all.

With a 300-agent graph this becomes acute, because "the agent drops the pond when
done" has no meaning when there are three hundred of them:

- **Who drops it?** If any agent may, one finishing branch tears down the
  workspace a parallel branch is still writing to.
- **What if nobody does?** The run dies mid-flight and the pond leaks forever.

Proposal: the pond's owner is the **run**; `drop` requires `admin` on the owning
principal; every pond carries a TTL that a control-plane reaper enforces; and the
orchestrator can extend it with a heartbeat while the run is live. A leaked pond
then costs one lifetime, not forever.

---

## What to decide now vs. later

**Now, because retrofitting is expensive:**

1. **The principal is a chain, not a string.** Getting `Principal` into
   `latiq-common` and threaded through the adapters is cheap today and invasive
   after OIDC ships against a flat id.
2. **The carrier moves out of the tool arguments** on MCP. Breaking change; do it
   before the tool surface has adopters.
3. **Attribution carries run and stage**, not just the agent. A one-line change to
   the commit message today; unreadable history forever if it's skipped. This is
   the cheapest high-value item on the list.
4. **Pond ownership defaults to the workflow.** Changes the meaning of `owner` in
   the registry — decide before ACLs are written against it.

**Later, safely:**

- Verification mechanism (A/B/C above) — the hierarchy is what constrains the
  design; the token source can change without reshaping the model.
- Grant management UX (CLI verbs, whether agents may grant).
- Rate limiting per principal — needs the hierarchy, nothing else.
- Cross-tenant sharing, column-level policy, masked queries.

---

## Open questions

- Is `tenant` real for us in the near term, or is one deployment one tenant until
  a managed offering exists? Carrying the level in the type costs nothing; making
  it load-bearing costs a lot.
- Does a workflow's identity come from the orchestrator, or does Latiq issue a run
  handle at first pond allocation and let the orchestrator pass it down? The
  second is self-contained but puts Latiq in the run-registry business.
- What happens to a pond when its run ends but another workflow holds a grant on
  it? Handoff, copy, or refuse.
- How does an operator revoke a run mid-flight, and what should the several
  hundred agents holding open queries see when it happens?
