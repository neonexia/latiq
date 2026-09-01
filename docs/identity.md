# Latiq — Identity (v0: authentication)

*Design note for the identity slice. Authentication is **shipped**; **authorization
is explicitly out of scope** — see [Authorization is deferred](#authorization-is-deferred-on-purpose).
Everything below describes the system as it is, unless it is under
["What is still open"](#what-is-still-open).*

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
server is an **OAuth 2.1 resource server**. The same verification applies to our
other surfaces, because the hard part (validating a token) is transport-
independent.

### The flow

1. An unauthenticated request gets `401` with a `WWW-Authenticate` header
   carrying `resource_metadata=…`.
2. That points at `/.well-known/oauth-protected-resource`, an RFC 9728 document
   advertising the **trusted authorization servers** the operator configured.
3. The client obtains a token from the IdP directly. **Latiq is never in that
   exchange**, holds no client secret, and stores no credential.
4. The client retries with `Authorization: Bearer …`.
5. Latiq verifies the token **locally** against the IdP's JWKS: signature,
   algorithm, issuer, `exp`/`nbf`, and — critically — **audience**, so a token
   minted for some other service cannot be replayed at us. No token passthrough.

### One verifier, three carriers

`latiq-auth` knows nothing about transports (invariant 5): it takes a token
string and returns an `Identity`. Each inbound adapter extracts its own carrier.

| Surface | Carrier | Rejection |
|---|---|---|
| MCP-over-HTTP | `Authorization: Bearer` header | HTTP `401` + `WWW-Authenticate` |
| Data / Stream gRPC | `authorization` gRPC metadata | `Unauthenticated` + `www-authenticate` metadata |
| Admin gRPC | `authorization` gRPC metadata | `Unauthenticated` + `www-authenticate` metadata |

Covering only MCP would leave the front door locked and a side door open. The
cost of covering all three is small precisely because the verifier is shared.

gRPC has no 401, but a tonic `Status` carries trailing metadata, so the *same*
challenge string rides along — an operator's CLI gets the RFC 6750 signal that a
token is required, and the resource it is required for. The metadata document
itself is served on the pond node's MCP surface (the one HTTP surface we have);
the Admin surface's challenge derives a URL at the standard path on the control
plane's own address, which nothing serves yet.

On MCP, verification happens in a **layer in front of the router**, not inside
the tool handlers. So *every* JSON-RPC method is covered — `initialize`,
`tools/list`, `resources/read` included, not only tool calls. A handler-only
check would let an unauthenticated caller complete the handshake, enumerate the
tool catalogue, read every `latiq://` resource and allocate an rmcp session per
request; and it would answer a forged or expired token with a JSON-RPC error
inside HTTP 200, which no MCP client can act on, because client-side re-auth keys
off a real 401. The well-known path is the one route exempt from the layer, or
discovery would be impossible.

### The identity type

`Identity` keeps its name and gains fields. Its central property is that **each
field knows whether it was verified**:

```rust
#[non_exhaustive]
pub struct Identity {
    /// The claimed leaf agent instance. Never verified. Attribution only.
    pub agent_id: String,
    /// The IdP's `sub`. Empty unless `verified`.
    pub subject: String,
    /// The `iss` of the token that produced `subject`. Empty unless `verified`.
    /// Carried separately so subjects from different issuers cannot collide.
    pub issuer: String,
    pub verified: bool,
}
```

Two rules, and they are the whole model:

> **Authority may only ever come from a verified field.**
> **Everything else is recorded as claimed and never load-bearing.**

The type enforces the second half structurally. It derives `Serialize` but **not
`Deserialize`**: an `Identity` is produced by the verifier or by
`Identity::claimed()`, never parsed from a wire payload — otherwise
attacker-controlled JSON could mint a fully-verified principal.

The leaf `agent_id` stays claimed on purpose. An agent instance inside a process
that already holds the run's token can always assert whatever leaf id it likes;
pretending otherwise would be theatre. The leaf is attribution, not authority —
and once authorization arrives, it binds to `subject`, which *is* verified. On a
verified caller that supplied no leaf, `agent_id` falls back to the subject:
`anonymous` would be a lie.

Workflow and step labels are **not** in `Identity`. They belong to lineage, are
always claimed, and live in the lineage event
([`lineage.md`](lineage.md#the-run-scope-question)).

### Verification

Multiple issuers from the start: `AuthConfig { audience, issuers }`, configured
as `--auth-issuer` (repeatable, or a comma-separated `$LATIQ_AUTH_ISSUER`) plus
`--auth-audience`. A workforce IdP for operators and a workload IdP for agents
are both legitimate, and the RFC 9728 document advertises all of them. One
audience across all issuers: the audience names *us*, not who vouched for the
caller. Each issuer gets its **own** JWKS cache — two IdPs may legitimately
publish the same `kid`, and a shared map would let either one's key satisfy the
other's tokens.

The parts worth stating explicitly:

- **Algorithms are an asymmetric-only allowlist**, checked before any key lookup,
  never taken from the token header. With a symmetric alg the verifier would hold
  a signing secret, which a resource server must not — and that is exactly what
  algorithm confusion reaches for, feeding a public key back as an HMAC secret.
  A JWK that declares its own `alg` is honoured too, so a caller cannot quietly
  downgrade the issuer's policy.
- **The `iss` is read unverified for exactly one purpose**: choosing whose keys
  to check the signature against. An issuer that is not configured is rejected
  before any key lookup, and validation still pins both issuer and audience — so
  a token claiming an issuer it was not signed by is checked against that
  issuer's real keys and fails.
- **`exp` and `nbf` are both validated**, with an explicit 30s leeway rather than
  the library's inherited 60s: enough for ordinary NTP drift, half the window in
  which an expired token still works.
- **A blank `sub` is rejected.** Present is not the same as usable — an empty
  subject would become an empty DuckLake commit author.
- **Config is validated at startup**, and a bad config is a startup failure, never
  a silent downgrade to unauthenticated. A plaintext `http` `jwks_uri` is refused
  unless the host is loopback (tests and `./dev.sh --auth` run a fake IdP there):
  on a real network it is a total auth bypass, since anyone on-path substitutes
  signing keys and mints arbitrary identities. The check runs on the parsed host
  the HTTP client will actually dial, because hand-rolled authority splitting
  disagrees with the WHATWG parser in ways an attacker can use.

#### `--auth-allow-insecure-jwks` — a test/development escape, never production

There is exactly one deployment shape the loopback exemption does not cover: an
IdP running as a **container on a private network**, reached by service name.
`deploy/cluster/`'s auth profile is that shape — Keycloak is at
`http://keycloak:8080/realms/latiq/protocol/openid-connect/certs`, which is
neither loopback nor able to present a certificate any client would trust. The
guard refused it, and the containerised auth e2e could not start at all.

The answer is **not** to widen the guard — "private network" is not a property a
process can verify, and a rule that tried would be exactly the kind of
hand-rolled host check this guard exists to replace. Instead there is an explicit
opt-out:

```
--auth-allow-insecure-jwks     (env: LATIQ_AUTH_ALLOW_INSECURE_JWKS)
```

- **Off by default**, and an empty env var (compose passes every variable through)
  means off. A value that is neither true nor false is a startup **error**, not a
  silent `false`.
- It relaxes the plaintext-http-to-a-non-loopback-host arm and **nothing else**:
  an unsupported scheme, an empty host and a missing authority are still refused.
- When set, every node `warn!`s on **every** startup, naming the URI and the
  consequence. Enabling it quietly is not possible.
- Set in **`deploy/cluster/auth.env`** only — the CI auth-e2e stack. It is not in
  `deploy/docker-compose.yml` (the user-facing deployment) and not in `dev.sh`,
  whose fake IdP is on loopback and needs no escape.

The risk it accepts is total: an attacker who can intercept the JWKS fetch serves
their own signing keys and mints any identity, including one that claims to be an
operator. In production the IdP is reached over **https** and this stays unset.

### The JWKS cache is on the request path

`kid` selects the key, so the cache lookup is the **first** thing an
unauthenticated caller reaches — before any signature is checked. A naive
refetch-on-miss therefore hands an attacker one outbound request to the
customer's IdP per attacker request: Latiq becomes an amplifier pointed at the
IdP it depends on. It is hardened accordingly:

- **Single-flight** — concurrent misses queue on one guard, so exactly one of
  them fetches and the rest ride its result rather than being rejected.
- **A refresh floor stamped on completion** (~1 fetch/minute), so a flood of
  bogus `kid`s costs the IdP one fetch while a genuine key rotation is still
  picked up within the interval. Stamped on *completion*, not entry: on entry,
  every request concurrent with the very first fetch would see the floor as
  already running and be rejected, so a cold start would refuse nearly every
  valid token.
- **Failure backoff** — a shorter floor after a failed fetch, doubling to the
  success interval, so a transient IdP blip clears in about a second while a
  sustained outage does not turn into a retry storm. A suppressed refresh reports
  the IdP as unavailable rather than the token as bad; sending the operator
  hunting for a bad token during an IdP outage is its own outage.
- **Connect/request timeouts, a bounded redirect policy, and a body cap**, so a
  `jwks_uri` misconfigured to point at a log file or an object-store listing
  cannot be read into memory.

Keys marked `use: "enc"`, or declaring a key-management algorithm, are skipped —
importing one would let a token be verified against a key its issuer never
intended to sign anything with. One unusable key does not poison the set.

Verification is offline after the fetch: no IdP round-trip on the request path,
which matters because this sits in front of every query.

### The public MCP URL is not the advertise address

Two settings, easily conflated, and getting them confused is the subtlest failure
in this slice:

- **`--advertise-addr`** is the node's **internal** address — what the node
  registers with the control plane so *peer nodes* can forward pond requests to
  it. Agents never dial it.
- **`--public-mcp-url`** (`$LATIQ_PUBLIC_MCP_URL`) is what **agents** dial.
  Behind a gateway — the shipped topology — that is the *gateway's* URL, not the
  node's.

The public URL is published as the RFC 9728 `resource` identifier and is what the
401 challenge points at. A conforming client compares the `resource` it discovers
against the URL it dialled and refuses on any origin difference, so publishing
the node's own address behind a gateway fails the client before it ever asks for
a token. Resolution order is: the configured `--public-mcp-url`, then a URL
derived from `--advertise-addr`, then the bound socket — and only the first is
right behind a gateway. (The bound socket is a last resort precisely because
every compose file we ship binds `0.0.0.0`, which no client can match.) A
configured value is validated, not trusted: a relative or hostless URL fails
startup rather than breaking discovery with an error that points nowhere near the
config.

### Cross-node forwarding replays the token

A pond is owned by one node, and any node may greet a request. On the hop, the
forwarder **replays the caller's original `Authorization` header** and the owning
node verifies it from scratch.

There is deliberately **no internal "already verified" header**. A header the
owner trusts without checking is exactly the trust laundering this design
forbids: anything that can reach the internal channel could then assert any
identity, and the security boundary would silently become the network perimeter
rather than the token. Re-verifying costs a cached-key signature check. Nor is
the claimed leaf re-injected on its own — that would hand the owner a *claimed*
identity, dropping subject/issuer and corrupting its attribution.

The token rides as a task-local in `latiq-agent-core` (`with_bearer` /
`current_bearer`), which is protocol-neutral by construction: a bare `String`, no
transport types. Every inbound adapter scopes it from its own carrier and the
forwarder reads it back. It is captured **only when a verifier is configured** —
a node that never opted into auth must not start capturing whatever
`authorization` header a client happens to send (one meant for an upstream
gateway, say) and replaying it to a peer. An `Unauthenticated` from the owner
stays `Unauthenticated` across the hop rather than collapsing to `Internal`, so
the code the caller sees remains actionable.

### Attribution

The DuckLake commit **author** is the verified `subject` when the caller
authenticated, and the claimed leaf otherwise. The claimed leaf, the issuer, and
the `verified` flag are always recorded alongside it in **`commit_extra_info`**
(built with `serde_json`, never hand-concatenated). A bare `verified` must never
sit next to a claimed value, so history can always tell provenance from
assertion:

```sql
SELECT snapshot_id, author, commit_message, commit_extra_info
FROM ducklake_snapshots('<pond>') ORDER BY snapshot_id DESC;
```

**Read both columns.** `author` alone cannot distinguish a verified writer from
one merely claiming that name.

The author is recorded *inside the transaction Latiq owns*, immediately before
the commit. Caller SQL that does its own `COMMIT` (or `BEGIN`/`ROLLBACK`/`START
TRANSACTION`) closes that transaction first, and the snapshot lands with no
author at all — identity never reaches history. The write path does **not**
police this (the read path rejects transaction control for an unrelated reason:
it must not close the read bracket), so it is stated as guidance on every
agent-facing surface rather than enforced by scanning SQL.

Accepted v0 trade-off: `author` is the **bare** subject, so subjects from two
different issuers collide when an operator groups history by `author`. The issuer
is in `commit_extra_info` — group by the pair to be exact. Qualifying the author
(`iss#sub`) would churn the format for every reader, so it waits for a deliberate
decision.

The same rule governs the control-plane registry: a row's `created_by` is the
verified subject when there is one, and the client's claim only stands when
nothing was verified.

### The access trail

Every audited operation emits a `latiq::access` record carrying the claimed
`agent`, the verified `subject`/`issuer`, `verified`, the op, the pond, the
duration, a redacted SQL shape — and **`outcome`** (`ok` / `error`).

`outcome` is not optional. An audit record that does not say whether the action
*landed* is worse than none: a refused `drop_pond` would read byte-identically to
a real one. Failures and auth rejections are recorded too, not only successes — a
rejected Data/Stream call otherwise left no trace at all, so an operator grepping
one stream saw a complete picture of operator activity and a partial one of
everything else. A rejection carries only the caller's claim, because there is no
verified identity to record.

To ask *who* did something, filter on `subject=` **together with**
`verified=true`. `agent=` is the caller's own claim.

**`read_arrow` records at stream establishment**, deliberately. Establishment is
the moment the access is authorized and rows begin to flow, and it is reached
exactly once, on the server, before any byte reaches the client. Completion is
not: when a stream ends — and whether that end is observed at all — is controlled
by the consumer, and a consumer that drops mid-stream is noticed in different
places on the local and forwarded paths. An audit record must not be contingent
on the behaviour of the party being audited. A read held open for an hour would
also be invisible for that hour, so "who is reading this pond right now" could
not be answered at the one time it matters. The cost is paid in two fields:
`duration_ms` measures establishment, and `outcome` says whether the read
*started*. The non-streaming `read_collected` runs entirely server-side, so it
records at completion instead.

**Deliberately unaudited:** catalog and dataset *browsing* — `list_datasets`,
`get_dataset`, `list_catalogs`, `get_catalog`. They are registry metadata reads
that touch no pond and take no identity at all. Everything that touches a pond or
moves data is audited, `load_dataset` and `catalog_pull` / `catalog_describe`
included.

### Unauthenticated mode stays — and is the default

If the operator configures no issuer, Latiq behaves as it always did: claimed
identity, `verified: false`, default `anonymous`. This is not a loophole to
close — it is the embedded SDK, the single-process case, `./dev.sh`, every test
in the repo, and a plain `docker compose up`. Auth is **opt-in by configuration**,
and turning it on is what an enterprise deployment does.

`./dev.sh --auth` runs the local stack against a Keycloak in Docker for
debugging; auth is otherwise exercised only by the nightly, in containers
(`docker compose --env-file auth.env up`). The CLI and SDK present
`$LATIQ_TOKEN` (or `--token`) on every request, Admin as well as data ops, and
simply do not use it against a deployment with no issuer.

---

## Authorization is deferred, on purpose

v0 ships **authentication only**. Verified identity flows into attribution, the
`latiq::access` trail, and registry ownership fields. It does **not** yet gate
anything: any caller holding a valid token from a trusted issuer can still reach
any pond, and any registered catalog.

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

## What is still open

- **Authorization**, above. The big one.
- **Token lifetime vs. long queries.** A token that expires mid-scan: today it is
  validated once at admission (the operation was authorized when it started).
  Whether anything should re-check during execution is undecided.
- **Dynamic client registration** (RFC 7591) is optional in the MCP spec and many
  enterprise IdPs disable it. Latiq advertises no registration endpoint and is
  never in the token exchange, so clients are registered at the IdP out of band.
  Whether that is the long-term answer is not settled.
- **No protected-resource document on the control plane.** The Admin surface
  challenges with a URL at the standard path on its own address, but nothing
  serves it there — the control plane has no HTTP surface to hang it off. An
  operator's CLI gets the "a token is required" signal without a fetchable
  document.
- **Author qualification.** Whether `author` should become `iss#sub` once more
  than one issuer is common, or stay the bare subject with the issuer in
  `commit_extra_info`.
- **Where to verify.** Today the node verifies, which keeps the security boundary
  inside our own code and survives someone bypassing the gateway. Verifying at
  the nginx front door as well would centralize JWKS and is not wrong; it is
  simply not what is built.
