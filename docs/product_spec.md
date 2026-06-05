# Latiq — Product Spec

*The agent-native data lake. Sell to agents, not people.*

---

## What Latiq is

Latiq is a data system built for AI agents, not for people. Traditional data systems are administered by humans for humans: provisioned by an admin, populated by a data team, queried by an analyst, governed by a committee. Their lifecycle is measured in fiscal years. Their setup takes weeks.

Agents work differently. A reasoning agent — or a team of agents collaborating on a task — needs a workspace they can create on intent, fill with the data they need, query at the speed of thought, share with peers, and dispose of when the work is done. They don't need a database, a data warehouse, or a data lake. They need a *pond*: small enough to spin up in seconds, smart enough to plug into existing enterprise data, capable enough to handle real analytical work.

Latiq is that. An operator installs Latiq once. From that point on, agents allocate their own ponds, write data, query data, collaborate with other agents, plug into enterprise databases the operator has registered, and release ponds when finished. The operator sets the boundaries. The agents do the work.

---

## Who it's for

**The agent is the customer.** This isn't marketing language; it's a design constraint. The interfaces are written for AI agents to read and use directly. Tool descriptions teach agents how to use the system well. Errors suggest next actions, not just diagnoses. The system trusts agents with capability and gives them what they need to succeed.

**Operators are the supporting audience.** Admins, platform teams, and SREs install Latiq, register the external data sources agents should be allowed to use, configure identity verification, and watch the system run. They never use the agent-facing surface; they have their own command-line tool and observability stack.

**Two clear separations:**
- Agents talk to Latiq through one interface (MCP, the emerging standard for AI agent tooling)
- Operators talk to Latiq through a different interface (a CLI command-line tool)
- These two surfaces don't overlap. Agents can't perform admin operations; operators don't get caught in agent workflows.

---

## What it does (the agent experience)

An agent connected to Latiq can do the following, using only SQL and a handful of simple tool calls:

**Create a workspace.** "Give me a pond called `incident-2026-001` for analyzing this outage." Done in milliseconds. No tickets, no provisioning queue, no schema-up-front decisions.

**Plug into existing enterprise data.** The operator has pre-registered the company's CRM database, data warehouse, and analytical tables. The agent picks from a curated list ("Production CRM — customers, orders, leads"), attaches what it needs to its pond, and queries that data using normal SQL. No credentials, no connection strings, no extract-transform-load pipelines. The data isn't copied — the agent queries it where it lives.

**Combine data freely.** The agent's own working data, the CRM database, the warehouse, and ad-hoc public files (a Parquet file on a public bucket, a CSV at a URL) all coexist as queryable sources. One SQL query can join across all of them. This is the moment when an agent stops feeling like a chatbot with database access and starts feeling like a colleague with the full toolkit.

**Plan before running.** Before issuing an expensive query, the agent can ask Latiq to estimate the cost. "This would scan 2 million rows and take 4 seconds — consider filtering on the date column first." The agent refines, asks again, and only runs when it's happy. This makes agents thrifty rather than greedy.

**Collaborate with other agents.** Multiple agents can work in the same pond. Each agent's writes are attributed to its identity. Other agents can see who wrote what and when. Conflicts between concurrent writes are handled automatically. The pond becomes a shared workspace where agents coordinate by reading each other's work, not by interrupting each other.

**Discover what's available.** Agents can list existing ponds in the deployment, look at their schemas, and decide whether to join an existing collaboration or start fresh. Agent-friendly descriptions and column comments — encouraged by Latiq's guidance — make this discovery natural.

**Release the workspace.** When the work is done, "drop pond" tears it down. Storage reclaimed, audit trail preserved.

---

## What it does (the operator experience)

An operator running Latiq has a simple, focused job: register the data sources the organization wants agents to use, configure identity verification, set policy limits, and watch the system.

**Install once.** Latiq ships as a single binary. One command in development; a Docker Compose configuration for multi-machine simulation; future Kubernetes support for production scale.

**Register catalogs.** A catalog is a named, curated connection to external data — a database, a data warehouse, a set of files in object storage. The operator gives it a name, a description (what's in it, what it's for), and connects it to credentials stored in the organization's existing secrets infrastructure (HashiCorp Vault, etc.). Agents never see credentials or connection strings; they see a curated menu.

**Control who can use what.** Each catalog has an allow-list of agent identities that can attach it. The operator grants and revokes access through the CLI. Restricted catalogs don't appear to agents who can't use them.

**Configure identity verification.** Optional but recommended for production. Latiq integrates with the organization's existing identity provider (Keycloak, Auth0, Okta, Google Workspace, etc.) and verifies agent identities at the boundary. When disabled (for development or trusted networks), Latiq still tracks claimed identities for audit purposes — identity is mandatory for accountability even when verification is optional.

**Set policy limits.** Default lifetime for ponds, query timeouts, rate limits per agent identity. Defaults work; customization is per-deployment.

**Monitor through standard tools.** Latiq emits the full set of operational signals — metrics, traces, structured logs — through OpenTelemetry, the open observability standard. Operators connect their existing Grafana, Datadog, Honeycomb, or whatever they run, and Latiq fits into their existing dashboards and alerting.

**Audit everything.** Every action — every pond creation, every query, every catalog attachment, every admin operation — produces an audit log entry tied to the identity that performed it. Operators can search, export, and analyze the audit log. Sensitive data (literal values in SQL, query results) is never recorded; only the shape of operations.

---

## What makes it different

Five things that distinguish Latiq from any database, data warehouse, or data lake aimed at humans.

**Lifecycle is the agent's, not the operator's.** Agents allocate and release ponds. Operators set the boundaries (rate limits, allowed catalogs, identity policy) and otherwise stay out of the way. The mental model is closer to "a process that an agent spawns" than "an asset that someone administers."

**Interfaces are written for AI agents.** Tool descriptions are mini-tutorials, not API docs. Errors suggest next actions, with examples and fuzzy-matched alternatives. Warnings teach agents to do better next time without failing the current call. Every query response carries forward signal — how much data was scanned, which tables were touched, whether the query could have been better. The surface treats agents as colleagues, not as untrusted clients.

**Federation by curation.** The operator builds a curated menu of enterprise data sources; agents pick from it. This is the right boundary for governance — operators decide what data is in scope; agents decide which of those sources to use for a given task. Agents query enterprise data without ever seeing credentials or connection details.

**Multi-agent collaboration is a first-class concern.** Multiple agents in one pond is the common case, not the edge case. Writes are attributed. Conflicts are handled. Other agents' work is discoverable through standard SQL queries against a reserved metadata schema. Agents can pair up, hand off work, divide and conquer — all without coordination overhead Latiq has to specifically handle.

**Hard separation of concerns.** Two surfaces, two audiences. Agents and operators have completely distinct interfaces, identities, audit trails. An agent cannot escalate to admin operations; an admin cannot accidentally appear as an agent. This separation makes both surfaces simpler and the security story far easier to reason about.

---

## What it doesn't do (in M1)

Setting clear expectations:

- **No streaming ingestion.** Agents load data into ponds via SQL — `INSERT`, `CREATE TABLE AS SELECT` from attached catalogs, or implicit reads of public files. Native streaming connectors (Kafka, change-data-capture) come in M2.

- **No Python SDK.** Agents talk to Latiq through MCP only. Frameworks like LangGraph, CrewAI, AutoGen integrate by using their built-in MCP clients. A Python SDK with high-performance streaming arrives in M2 for code that processes very large result sets.

- **No multi-machine production deployment out of the box.** M1 ships with a single-binary developer mode and a Docker Compose deployment for simulating multi-node topologies on one machine. Production Kubernetes deployment ships in M2.

- **No full federation governance.** M1 supports admin-curated catalogs as the primary federation mechanism. Richer governance — per-pond ACLs, column-level security, masked queries — comes in M2 and M3.

- **No disk quotas.** M1 trusts the operator to provision adequate disk; M2 adds quota enforcement.

- **No management UI.** The CLI is the surface for M1. A web UI is a future product.

These aren't permanent constraints; they're scope discipline. Each one has a clear plan for when it shows up.

---

## The bigger story

The world of AI agents is moving from "agents that chat" to "agents that do work." Work means producing artifacts, transforming data, collaborating with other agents, and reaching into the systems the rest of the organization runs.

Traditional data infrastructure wasn't built for this. Spinning up a database for a 20-minute reasoning task is absurd; setting up a data warehouse pipeline for a one-off analysis is overkill; asking an agent to maintain credentials and connection strings is a security disaster.

Latiq fits the shape of agent work. Lightweight when the work is light, durable when the work matters. Curated when governance is needed, exploratory when the source is public. Collaborative when agents pair up, isolated when they don't. Built for the way agents actually operate — not retrofitted from a tool designed for humans.

**The premise that all of this rests on:** agents are real customers now. Their constraints, ergonomics, and failure modes are different from human users' constraints. Building for them means starting from those differences, not adapting human tools and hoping the seams don't show.

Latiq starts there.

---

## What we're shipping in M1

The first release of Latiq, targeted at 90 days from project start, includes:

- The core pond primitive — allocate, query, collaborate, release
- Admin-curated catalog system with credential-store integration (Vault)
- Cross-catalog query (combine pond data with attached enterprise data in one SQL query)
- Cost estimation (the `explain_query` capability)
- Multi-agent collaboration with attribution and conflict handling
- MCP surface with tools, resources, and prompts following current MCP best practices
- Admin CLI for catalog registration, credential management, audit access, policy
- Single-binary distribution with developer mode and Docker Compose multi-node simulation
- OpenTelemetry observability with metrics, traces, and structured logs
- Optional OIDC identity verification with major identity providers
- Audit log of every operation
- Rate limiting per agent identity

The goal for the first six months after launch is open-source momentum — frameworks adopting Latiq, agent communities trying it, the demo getting shared without our pushing it. Revenue and enterprise contracts come later. M1 is for proving the shape is right.

---

## Why we think this works

Three bets, each testable:

**Bet 1: AI agents are a real customer category.** Not just an interface for human users, but distinct entities whose ergonomics, error tolerance, and trust model are different. If this bet is wrong, Latiq is a niche product. If it's right, agent-native infrastructure becomes a category — and Latiq is the data system within it.

**Bet 2: Lightweight, lifecycle-driven workspaces beat heavyweight, provision-once infrastructure for agent workloads.** A pond an agent creates for a 30-minute task and discards is fundamentally a different product than a data lake an enterprise stands up for a multi-year initiative. If this bet is wrong, agents end up using existing data warehouses badly. If it's right, "pond" becomes a primitive in the way "container" became a primitive after Docker.

**Bet 3: Federation by curation is the right governance boundary.** Operators decide what enterprise data is in scope; agents decide which curated sources to use for a task. If this bet is wrong, enterprises won't let agents touch real data. If it's right, this is the model that makes agentic data access tractable for compliance teams.

The 90-day M1 is built to test bet 2 directly and bet 1 indirectly. Bet 3 plays out over months as enterprises start adopting.

---

## What success looks like

At six months:

- Open-source momentum on GitHub: stars, contributors, third-party integrations
- At least one major agent framework with first-class Latiq integration
- The first incident-response or analytical demo gets shared organically — not pushed by us
- Working group / RFC process for the agent-facing API with non-Latiq contributors participating
- Early conversations with enterprise design partners for M2-onwards

Revenue, paying customers, managed offerings — all later. Six months is for proving the category exists and that Latiq owns the right shape of it.

---

## Three phrases worth tattooing on the team

- **The agent is the customer.**
- **The lake is an agent primitive, not an admin artifact.**
- **Make it boring.** (For M1 specifically — predictable, well-documented, rock-solid. The magic comes in M2 and M3.)

If a decision is hard, one of these three is in tension with something else. Resolve in favor of the agent.
