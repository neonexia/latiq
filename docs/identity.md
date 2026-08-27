# Latiq — Identity (v0: authentication)

*Design note for the identity slice. **Authorization is explicitly out of scope
here** — see [Authorization is deferred](#authorization-is-deferred-on-purpose).
Nothing below is implemented except where marked **today**.*

---

## Where we are today

```rust
pub struct Identity {
    pub agent_id: String,
    pub verified: bool,   // always false
}
```

- The caller **claims** an id. On MCP it arrives as an optional `agent_id` *tool
  argument* (`crates/latiq-mcp/src/server.rs`); on Data gRPC as the
  `latiq-agent-id` metadata key (`crates/latiq-pond-node/src/data_service.rs`).
  Absent or blank → `anonymous`.
- Nothing verifies it. Any caller can assert any id.
- It is used for exactly two things: **attribution** (it rides DuckLake's native
  `set_commit_message`, so `pond.snapshots()` shows who wrote what) and the
  **`latiq::access` trail** (`crates/latiq-agent-core/src/ops.rs`).
- There are no pond ACLs and no per-principal catalog grants. Any caller that
  can reach the gateway can allocate a pond, read any pond, and pull from any
  registered catalog.

Defensible for a single trusted deployment. It does not survive contact with an
enterprise, for one blunt reason: **an enterprise cannot onboard a service that
has no relationship with its identity provider.**

---

## The strategic call

There is **no settled standard for multi-agent authorization.** Principal
hierarchies, delegation chains, agent-to-agent credentials — all of it is in
motion. Meanwhile every enterprise that would deploy Latiq already runs a
centralized IdP (Okta, Auth0, Entra) and already knows how to issue tokens to
workloads.

And the agents that exist *today* are overwhelmingly **single agents and loop
agents**, not the hundred-node graphs a hierarchy would be designed for. Those
agents carry a **stable** subject — a delegated human via the authorization-code
flow, or a service account via client credentials. A stable subject has none of
the "the grant can't precede the agent" problems that motivate a hierarchy.

> **So: adopt the standard that exists, invent nothing, and let the multi-agent
> identity story mature in the industry rather than in our codebase.**

Concretely, that means the earlier proposal in this document's history — a
four-level `tenant → workflow → stage → agent` principal chain of our own design
— is **withdrawn**. What it got right (that attribution wants finer granularity
than authority) survives as *recorded data*, not as type structure. See
[`lineage.md`](lineage.md), which is where that granularity now lives.

---

## v0 — authenticate, don't authorize

MCP already specifies this, so we implement a spec rather than a design: the MCP
server is an **OAuth 2.1 resource server**. The same verification then applies to
our other surfaces, because the hard part (validating a token) is transport-
independent.

### The flow

1. An unauthenticated request gets `401` with a `WWW-Authenticate` header
   carrying `resource_metadata=…`.
2. That points at `/.well-known/oauth-protected-resource`, which advertises the
   **trusted authorization servers** the operator configured.
3. The client obtains a token from the IdP directly. **Latiq is never in that
   exchange**, holds no client secret, and stores no credential.
4. The client retries with `Authorization: Bearer …`.
5. Latiq verifies the token **locally** against the IdP's JWKS: signature,
   issuer, expiry, and — critically — **audience**, so a token minted for some
   other service cannot be replayed at us. No token passthrough.

JWKS is fetched once and cached with a refresh on unknown `kid`. Verification is
offline after that: no IdP round-trip on the request path, which matters because
this sits in front of every query.

### One verifier, three carriers

The verifier belongs in `latiq-agent-core` and knows nothing about transports
(invariant 5). Each inbound adapter extracts its own carrier and hands over a
token string:

| Surface | Carrier |
|---|---|
| MCP-over-HTTP | `Authorization: Bearer` header |
| Data / Stream gRPC | `authorization` gRPC metadata |
| Admin gRPC | `authorization` gRPC metadata |

Covering only MCP would leave the front door locked and a side door open. The
cost of covering all three is small precisely because the verifier is shared.

### The identity type

`Identity` keeps its name and gains fields. Its central property is that **each
field knows whether it was verified**:

```rust
pub struct Identity {
    /// The IdP's `sub`. Verified when `verified` is true.
    pub subject: String,
    /// Issuer (`iss`) of the token that produced `subject`. Empty when unverified.
    /// Carried separately so subjects from different issuers cannot collide.
    pub issuer: String,
    pub verified: bool,

    /// The leaf agent instance. ALWAYS claimed — never verified, never authority.
    pub agent_id: String,
}
```

*(An earlier draft called this `Principal`. The rename was dropped: it touches 12
source files and ~30 test call sites for no behaviour change, and `Identity` is an
honest name for these four fields.)*

Two rules, and they are the whole model:

> **Authority may only ever come from a verified field.**
> **Everything else is recorded as claimed and never load-bearing.**

The leaf `agent_id` stays claimed on purpose. An agent instance inside a process
that already holds the run's token can always assert whatever leaf id it likes;
pretending otherwise would be theatre. The leaf is attribution, not authority —
and once authorization arrives, it binds to `subject`, which *is* verified.

Workflow and step labels are **not** in `Principal`. They belong to lineage, are
always claimed, and live in the lineage event
([`lineage.md`](lineage.md#the-run-scope-question)).

### Unauthenticated mode stays

If the operator configures no authorization server, Latiq behaves exactly as it
does today: claimed identity, `verified: false`, default `anonymous`. This is not
a loophole to close — it is the embedded SDK, the single-process case, `./dev.sh`,
and every test in the repo. Auth is **opt-in by configuration**, and turning it on
is what an enterprise deployment does.

### The MCP breaking change

Identity moves **out of the tool arguments**. Today the model itself types
`agent_id` into a tool parameter, which is fine for a claimed value and
unacceptable for a verified one — a verified principal must arrive out of band,
in the transport, where the model cannot reach it. This is a breaking change to
every MCP tool schema and it gets more expensive every day the current surface
ships. It is the **first** thing to do, not the last.

---

## Authorization is deferred, on purpose

v0 ships **authentication only**. Verified identity flows into attribution, the
`latiq::access` trail, and lineage. It does **not** yet gate anything: any caller
holding a valid token from a trusted issuer can still reach any pond.

That is a real gap and this document does not pretend otherwise. It is deferred
because it is genuinely separable — nothing in v0 has to be redesigned to add it
later, since ACLs will bind to `subject`, which v0 already establishes and
verifies. Sequencing authn first gets us enterprise-compatible **now**; bundling
them delays both.

The authorization slice (its own design note, its own issue) has to settle at
least:

- **Pond ownership.** Owner is the allocating `subject`. Same subject full
  access; anything cross-subject an explicit grant. `list_ponds` filters to what
  the caller may see — today it lists everything, which is both a leak and a
  scaling problem.
- **Catalog grants.** Today a registered catalog is reachable by anyone who can
  reach the gateway. Credentials are not stored, which is a genuine mitigation —
  the caller supplies the credential at pull time, so registration alone grants
  nothing. That changes the moment Latiq can present the caller's identity to the
  source: **the registration becomes the grant** and needs an owner.
- **Who may grant** — agent, operator, or agents-within-a-boundary.
- **Group and role claims.** Enterprise tokens carry them; binding grants to a
  group is far more usable than binding to individual subjects, and it is the
  natural first thing to reach for once ACLs exist.
- **Pond lifecycle.** `default_pond_lifetime_seconds` (3600) exists in the policy
  table and **nothing reads it**; there is no pond reaper. Ponds leak. Release
  authority is an ownership question, so it lands with authorization.

---

## Isolation is three problems

Conflated constantly; different mechanisms, different owners.

**Access isolation** — which principals may touch which pond and catalog. Policy
in the control-plane registry, enforced on the pond node. Not built; deferred
with authorization above.

**Resource isolation** — one pond's scan not starving its neighbours. **Today:**
per-pond tiers map to hard `memory_limit` and `threads` caps on the pond's DuckDB
instance, plus a per-pond read-connection pool sized off the tier. This is the
piece that already works.

**Placement** — which node a new pond lands on. Not built; ponds are placed
without regard to load or tier. Tier-aware binpacking against the per-node
metrics already emitted (`latiq_node_open_ponds`, `latiq_inflight_queries`,
`latiq_process_memory_bytes`) is the cheap fix.

---

## Open questions

- **Multiple issuers.** One trusted AS is simple; several (a workforce IdP plus a
  workload IdP) is realistic. The metadata document supports a list — the
  question is whether subjects from different issuers can collide, which is why
  `issuer` is carried alongside `subject` rather than folded into it.
- **Token lifetime vs. long queries.** A token that expires mid-scan: do we
  validate once at admission (favoured — the operation was authorized when it
  started) or re-check during execution?
- **Dynamic client registration** (RFC 7591) is optional in the MCP spec and many
  enterprise IdPs disable it. Do we require pre-registered clients in v0?
- **Does the gateway verify, or the node?** Verifying at the nginx front door is
  conventional and centralizes JWKS; verifying on the node keeps the security
  boundary inside our own code and survives someone bypassing the gateway. Doing
  it on the node is the safer default; doing it at both is not wrong.
