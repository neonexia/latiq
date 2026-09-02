# Latiq — Product Spec

*The agent-native data lake. Sell to agents, not people.*

---

## What Latiq is

Latiq is a data system built for AI agents, not for people. Traditional data systems are administered by humans for humans: provisioned by an admin, populated by a data team, queried by an analyst, governed by a committee. Their lifecycle is measured in fiscal years. Their setup takes weeks.

Agents work differently. A workflow kicks off, spawns a graph of agents — sometimes hundreds, in parallel stages — and those agents need a shared workspace they can create on intent, fill with the data they need, query at the speed of thought, read each other's work in, and dispose of when the work is done. They don't need a database, a data warehouse, or a data lake. They need a *pond*: small enough to spin up in seconds, smart enough to pull in existing enterprise data, capable enough to handle real analytical work.

Latiq is that. An operator installs Latiq once, into the cluster where the agents already run. From that point on, agents allocate ponds, write data, query data, collaborate with the other agents in their workflow, pull from enterprise sources the operator has registered, and release ponds when finished. The operator sets the boundaries. The agents do the work.

---

## Where Latiq runs — and why that's the point

The dominant assumption in data infrastructure is that analytics happens *somewhere else*. You produce data here; you ship it to a warehouse there; you ask questions of the warehouse and wait. That arrangement was built for humans, who ask questions occasionally and can tolerate a wait.

Agents can't. An agent in a loop asks constantly, about data it produced seconds ago, and has to act on the answer before its next tool call. Round-tripping that to an external warehouse doesn't make it slow — it makes it impossible, which is why agents today do retrieval and hope rather than analysis. And every such round trip copies working data out of the boundary the agent was granted, into a third-party processor, where it has to be governed all over again.

**Latiq inverts the arrangement: the analytics runs where the agent runs.** Not as a metaphor — as a deployment fact, in one of two topologies.

**Embedded — in the agent's own process.** `latiq.connect(server="local")` gives a real single-node Latiq in-process. Same memory, same lifetime, no network at all. This is the single-agent case, the notebook, the CI run, and the local harness.

**Cluster-resident — in the agent cluster.** The production shape. Enterprise agents don't run on a laptop terminal; they run as workloads on a cluster. Latiq deploys into that same cluster: same network, same trust boundary, no egress, latency in milliseconds rather than internet round trips. One Latiq deployment serves every workflow and every agent on that cluster.

What makes cluster residency possible is a deliberate architectural choice: **Latiq scales out, it does not distribute processing.** There is no shuffle, no exchange operator, no distributed planner. A pond is owned by exactly one node, and every query for that pond executes on that node. Multiple nodes exist so that many ponds can be served at once and the deployment can grow — not so that one query can be split across machines. The gateway is a single front door; the control plane holds routing and policy and is never in the query data path.

That choice is what keeps a node small enough to live next to the agents rather than in a platform of its own. It's a feature, and the boundary it implies is stated plainly under *Declared limits* below.

The compute story follows from this, and it's narrower than "free analytics": you are not paying to move the data. No extract pipeline, no second copy in a vendor's storage, no egress, no always-on warehouse waiting for a question. And agent harness workloads are CPU-and-memory shaped and mostly idle between model calls — which is exactly the shape of compute an embedded analytical engine wants, on exactly the nodes that already have it.

---

## The shape of the workload

A single agent with a scratch table is the easy case. The case Latiq is designed for is the production one:

- A workflow starts and spawns a **graph of agents** — parallel branches, sequential stages, sometimes hundreds of agents over the life of one run.
- Those agents **share a working set**. Stage two reads what stage one's forty parallel agents wrote. The pond is the workflow's shared memory, and it outlives every individual agent in the graph.
- **Multiple workflows run concurrently on the same cluster**, because a cluster you keep busy is a cluster you're using well. Latiq is shared infrastructure for all of them, under one management plane.

Three consequences run through the rest of this spec:

**The pond belongs to the run, not to an agent.** Agents are the ephemeral things. If any agent can drop a pond, a 300-agent graph either leaks ponds forever or tears down a workspace its siblings are still using. Pond ownership, TTL, and release authority belong to the workflow run.

**Scale is measured in working sets, not in agents.** Three hundred agents can share one pond on one node. Node count tracks concurrent ponds and data size, decoupled from fleet size — which is a very different cost curve from per-agent infrastructure.

**Authority is the run's, not the agent's.** Nobody is going to pre-register three hundred ephemeral agent identities on an access list. So authority comes from one verified principal — the subject in the run's token, issued by the enterprise's own IdP — while each agent's leaf id rides along as claimed attribution and never carries authority. Latiq authenticates that principal today; deciding what it may *reach* is the next slice, and both have their own note: [`docs/identity.md`](identity.md).

---

## Who it's for

**The agent is the customer.** This isn't marketing language; it's a design constraint. The interfaces are written for AI agents to read and use directly. Tool descriptions teach agents how to use the system well. Errors suggest next actions, not just diagnoses. `latiq://` guidance resources teach dialect, schema design, and recovery from conflicts. The system trusts agents with capability and gives them what they need to succeed.

**Operators are the supporting audience.** Platform teams install Latiq into the agent cluster, register the data sources agents may use, set policy, and watch it run. They never touch the agent-facing surface; they have their own CLI and their existing observability stack.

**Programs are a third audience.** Notebooks, CI, harnesses, and framework glue drive Latiq through a Python SDK over gRPC — the same operations, no MCP involved.

**Three surfaces, three audiences, no overlap:**

| Surface | Audience | Carries |
|---|---|---|
| MCP-over-HTTP (gateway) | agents | tools + `latiq://` resources + prompts |
| Data/Query + Stream gRPC (gateway) | SDK, CLI | allocate / read / write / explain / stream |
| Admin gRPC (control plane) | operators | nodes / policy / pond + catalog metadata |

Agents can't perform admin operations; operators don't get caught in agent workflows; the SDK is not an agent and never speaks MCP.

---

## What it does (the agent experience)

An agent connected to Latiq can do the following, using only SQL and a handful of tool calls:

**Create a workspace.** "Give me a pond called `incident-2026-001` for analyzing this outage." Done in milliseconds. No tickets, no provisioning queue, no schema-up-front decisions.

Allocation is **eager and holistic**: it returns only once the pond's storage really exists on the node the control plane placed it on, so a pond a caller was told it has is a pond that can accept data. The **control plane** is what makes it real — it places the pond and then asks the owning node to materialize it — so every create path is eager through one mechanism: the agent's `allocate_pond`, `latiq pond create`, and the SDK alike. (This is the one narrow exception to "the control plane is never in the data path": it drives pond *lifecycle*, never a query, and it touches no storage itself.) On a multi-node deployment that costs a second hop — control plane, then the owning node — and it means allocation now *fails* when that node is unreachable, instead of succeeding and failing later at the agent's first write. The failure names the node and says the assignment was rolled back, so the agent knows the name is free and can simply retry. We would rather be slower and honest here than fast and wrong: the alternative hands an agent a pond id that nothing is behind.

**Bring in curated data.** Operators pre-register two kinds of source. A **dataset** is a curated file or set of files (parquet/CSV) that `load_dataset` copies into the pond under its own schema. A **catalog** is an external database or lakehouse — Iceberg today — that the agent `pull`s from: a transient attach, one query, detach, with the result materialized into the pond. Agents pick from a described menu and never see a credential or a connection string.

**Then work locally.** This is the important half. Latiq does not proxy live queries to external systems — a catalog is a tap you open, pull through, and close. Everything after that runs on the pond, on the node, next to the agent. Pushdown means the pull downloads only the columns and row groups the query touches, not the whole table.

**Combine data freely.** The agents' own working data, pulled enterprise subsets, and loaded datasets all coexist as ordinary tables in one pond. One SQL query joins across all of them.

**Plan before running.** Before an expensive query, the agent can ask Latiq to explain it — what it will scan, what it will cost — refine, and only run when it's satisfied. This makes agents thrifty rather than greedy, and it works because the estimate is a local call, not a network round trip.

**Collaborate with the rest of the graph.** Multiple agents in one pond is the common case, not the edge case. Writes serialize and conflicts auto-retry. Every write is attributed to the identity that made it, riding DuckLake's native commit metadata — the author is the verified subject where the caller authenticated, with the claimed agent id and the issuer in `commit_extra_info` — so history is readable through ordinary SQL against `pond.snapshots()`, and a reader can tell a verified writer from one merely claiming a name. No Latiq objects in the pond catalog. Agents coordinate by reading each other's work, not by interrupting each other.

**Trace where data came from.** A pond can be allocated with lineage on, and every query in it then records a standard [OpenLineage](https://openlineage.io) event pair — what it read, what it wrote, which DuckLake snapshot it saw, how long it took, and the identity behind it (the verified subject where the caller authenticated, the claimed agent id either way). Agents read it back mid-run with `get_lineage`, on the node that ran the queries; no web UI, no external system, and nothing added to the pond's catalog. It is off by default and fixed at allocation, because a pond that does not want it should pay nothing for it.

**Discover what's available.** Agents can list ponds, inspect schemas, and decide whether to join an existing collaboration or start fresh. Column and table comments — which the guidance resources push agents to write — make that discovery natural.

**Stream results.** Reads come back as Arrow. Inline tool results are capped so an agent isn't drowned in rows; the SDK path streams uncapped for programs that want the whole set.

**Release the workspace.** When the work is done, drop the pond. Storage reclaimed, access trail preserved.

---

## What it does (the operator experience)

**Install once, into the agent cluster.** One binary, one role per command (`serve`, `node add`, or the CLI). A compose deployment runs control plane, pond nodes, and an nginx gateway — the single MCP and Data/Stream front door — and the same topology scales from a laptop to a cluster unchanged. The admin CLI installs as a small client-only build with no engine in it.

**Register the data agents may use.** `latiq dataset add` for curated files; `latiq catalog add` for external sources, with a type and locator params. Credential-shaped params are *dropped* at registration by a per-type allowlist — Latiq stores no credentials, ever. Credentials ride in at pull time, build a temporary secret for that one operation, and are dropped on detach.

**Size ponds.** Each pond has a resource tier (x-small / small / medium / large / x-large) that maps to hard `memory_limit` and `threads` caps on its DuckDB instance, and to how many reads it runs at once. This is what keeps one workflow's scan from starving its neighbors on a busy shared cluster. A tier can be changed after creation (`latiq pond set-tier`), and an operator can opt a pond out of capping entirely with the `none` tier — the engine's own defaults then govern it. `none` is operator-only: a pond cannot ask for it at allocation.

**Watch it run.** Every process serves a Prometheus `/metrics` endpoint: ponds by tier, per-pond query rate, p95 latency, errors by kind, in-flight load, cross-node forwarding, node liveness. Logs are structured `tracing`, JSON on request, with a `trace_id` propagated across the node hop so one request correlates across the cluster. Latiq ships no dashboards and stores no time series — you point your existing Prometheus, Grafana, and log pipeline at it.

**Connect it to your identity provider.** Latiq is an OAuth 2.1 resource server. Point it at the issuers you already run (Okta, Auth0, Entra, Keycloak) with an audience, and every surface — MCP, Data/Stream gRPC, Admin gRPC — requires a valid bearer token. Latiq is never in the token exchange, holds no client secret, and stores no credential: it verifies tokens locally against the IdP's published keys. Agents discover where to authenticate through the standard protected-resource metadata document. Configure no issuer and identity stays claimed-only — which is the embedded case, the dev stack, and a plain compose deployment. Authentication does not yet *gate* anything beyond requiring a valid token; per-pond authorization is the next slice ([`docs/identity.md`](identity.md)).

**Point lineage at a backend, if it must outlive the pond.** Lineage events live in the pond's own directory and are reaped **with** the pond — dropping a pond destroys its provenance, which is exactly the post-mortem history someone will want afterwards. That trade is deliberate: Latiq is not a lineage archive, and building one would mean a reaper, an orphan store, and a retention policy we have no basis to choose. The escape hatch is `--lineage-backend-url`, an OpenLineage-compatible receiver (Marquez or anything else that speaks the standard) that every event is also posted to, byte-identical to what the pond stores. A backend that is down, slow or dead can never fail or delay a query — and delivery to it is best-effort by design: a failed POST is dropped rather than retried, and a full queue sheds its oldest events. The pond's own files keep everything regardless. Three `latiq_lineage_sink_*` gauges on `/metrics` say whether the backend is keeping up. See [`docs/lineage.md`](lineage.md).

**Read the access trail.** Every operation that touches a pond or moves data emits a structured event on the `latiq::access` target carrying the claimed agent, the verified subject and issuer, whether it was verified, the operation, the pond, the duration, a redacted SQL shape with literals replaced, and the outcome — successes, failures, and rejected calls alike. (Pure registry browsing — listing datasets and catalogs — is not audited: it touches no pond and carries no identity.) There is no audit table and no audit RPC by design — the trail lives in your log stack, where you already search, retain, and alert.

---

## What makes it different

**Analytics is co-resident with the agents.** Embedded in the process, or deployed in the same cluster — not in a warehouse across the internet. Latency low enough to sit inside an agent's reasoning loop, and no copy of the working set outside the boundary the agents already run in.

**Scale-out, not distributed processing.** One owner per pond, one engine instance per pond, queries always local to the owner. Growth comes from more ponds on more nodes under one management plane, not from splitting a query across machines. This is why a node is small enough to live where the agents live.

**Lifecycle is the workflow's, not the operator's.** Agents allocate and release ponds; operators set boundaries and stay out of the way. The mental model is closer to "memory a workflow allocates" than "an asset someone administers."

**Interfaces are written for AI agents.** Tool descriptions are mini-tutorials. Errors are structured — kind, message, suggestion, reference — and suggest next actions with examples and fuzzy-matched alternatives. Every query response carries forward signal: what was scanned, what was touched, whether it could have been better. The surface treats agents as colleagues, not as untrusted clients.

**Federation by curation, then locality.** Operators publish a curated menu of sources; agents pull the subset they need and work on it locally. Governance sits at the menu, not at every query, and no credential ever reaches an agent.

**Multi-agent collaboration is the base case.** Attribution on every write, automatic conflict handling, discoverable history through native DuckLake snapshots. Built for a graph of agents sharing a workspace, not for one agent with a database.

**Hard separation of concerns.** Three surfaces, three audiences, distinct identities and trails. An agent cannot escalate to admin; an admin cannot appear as an agent; the SDK is not an agent. This makes each surface simpler and the security story far easier to reason about.

---

## Declared limits

Stating these is what makes the rest credible.

**A pond fits on one node.** No distributed execution means a pond's working set is bounded by its node's memory and disk. This is by design: ponds are task-scoped working sets, and heavy reduction stays pushed down to the source at pull time. A workflow whose working set genuinely exceeds a node is not a Latiq workload.

**External sources are pulled, not queried live.** Latiq is not a federated query engine and does not want to be. If the answer requires scanning a petabyte in place, scan it in place and pull the result.

**Result sets are capped on the agent path.** Inline tool results are bounded so an agent isn't flooded; large results are for the SDK's streaming path, or for `CREATE TABLE AS SELECT` and a follow-up query.

**Provenance is not tamper-proof.** Lineage records what happened, and the identity facet tells a verified subject from a claimed one. But `write_query` runs arbitrary SQL by design, and DuckDB SQL can write files — so an agent *can* today forge or overwrite its own pond's events. The per-pond sandbox that closes this is tracked as [#79](https://github.com/neonexia/latiq/issues/79) and is a prerequisite for beta. M1 assumes trusted agents, consistent with the rest of the posture.

**Callers are authenticated, not yet authorized.** Latiq verifies enterprise IdP tokens on every surface, and attribution and the access trail record the verified subject. But it does not yet gate *what* a verified caller may reach: any valid token from a trusted issuer can allocate a pond, read any pond, and use any registered catalog. Pond ownership and grants are the next slice — see [`docs/identity.md`](identity.md).

---

## Feature status & roadmap

Where each feature stands — shipped, next, later, with releases and status — lives in **[`docs/roadmap.md`](roadmap.md)**, kept separate from this spec on purpose: positioning is stable, feature status evolves.

---

## Why we think this works

Four bets, each testable.

**Bet 1: AI agents are a real customer category.** Not an interface for human users, but distinct entities whose ergonomics, error tolerance, and trust model differ. If this is wrong, Latiq is a niche product. If it's right, agent-native infrastructure becomes a category.

**Bet 2: Lightweight, lifecycle-driven workspaces beat provision-once infrastructure for agent workloads.** A pond a workflow creates and discards is a fundamentally different product from a lake an enterprise stands up for a multi-year initiative. If this is right, "pond" becomes a primitive the way "container" did.

**Bet 3: Federation by curation is the right governance boundary.** Operators decide what's in scope; agents choose among curated sources and pull what they need. If this is wrong, enterprises won't let agents touch real data.

**Bet 4: Co-residency beats remote analytics for agent workloads.** Analytics in the agent's process or the agent's cluster wins on the only three axes that matter here — latency inside the reasoning loop, no working-set copy outside the trust boundary, and no pipeline to build or pay for. If this is wrong, agents keep using remote warehouses badly. If it's right, this is the reason a team picks Latiq over pointing an agent at the warehouse they already have.

Bet 4 is the newest and the one to pressure-test first, because it's the one a skeptical platform team will challenge directly.

---

## What success looks like

At six months:

- Open-source momentum on GitHub: stars, contributors, third-party integrations
- At least one major agent framework or workflow orchestrator with first-class Latiq integration
- A workflow-scale demo — a real agent graph sharing a pond — that gets shared without our pushing it
- An RFC process for the agent-facing API with non-Latiq contributors participating
- Early design partners running Latiq in the same cluster as their agents

Revenue, paying customers, managed offerings — all later. Six months is for proving the category exists and that Latiq owns the right shape of it.

---

## Four phrases worth tattooing on the team

- **The agent is the customer.**
- **The pond is a workflow primitive, not an admin artifact.**
- **Scale out, don't distribute.**
- **Make it boring.** Predictable, well-documented, rock-solid.

If a decision is hard, one of these four is in tension with something else. Resolve in favor of the agent.
