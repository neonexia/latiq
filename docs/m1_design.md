# Latiq M1 — Architecture & Design (v1.0)

*Agent-native data pond. Smart-proxy topology, DuckLake + DuckDB engine, single-node ponds with control-plane discovery. MCP-over-HTTP for agents, gRPC Admin API + CLI for operators. Single binary. 90 days to a shippable product that demonstrates the agent value proposition end-to-end.*

---

## 0. Implementation status & deltas (2026-06)

> **This is the original M1 design vision.** The shipped slice realizes its spine
> — agent-native ponds, DuckLake+DuckDB, control-plane discovery, smart-proxy
> forwarding, MCP for agents / gRPC for operators, single binary — but several
> concrete choices below have changed. **Where this document and the deltas here
> disagree, the deltas win.** `docs/dev.md` is the hands-on runbook for what's
> actually built; `CLAUDE.md` holds the binding invariants.

**Built and runnable:** pond lifecycle (create/describe/list/drop); SQL read/write
with native attribution; `explain_query`; the MCP agent surface (tools, resources,
prompt SOPs); Data + Admin gRPC; query-by-URI for public files; sample datasets;
audit log; **multi-node request forwarding behind a front door**; **Arrow streaming
reads**; cancellation + prompt-resource release.

**Architectural deltas from the body of this doc:**

- **No Flight SQL — anywhere.** The node-to-node proxy hop *and* the SDK read path
  stream **Arrow IPC over our own server-streaming gRPC** (`latiq.v1.Stream/ReadArrow`),
  sharing the Data gRPC port. We skipped Flight because `arrow-flight` at the Arrow
  version DuckDB pulls forces a second `tonic` major into the build for a protocol
  we don't need internally. The *data* is still standard Arrow (any `pyarrow`/arrow
  client decodes it); only the framing is ours. (§2, §3, §6.2 below describe Flight
  — read "Arrow IPC over gRPC" instead.)
- **No `_latiq` schema (§8).** Ponds are **pure DuckLake** — history/attribution via
  native `pond.snapshots()`, tables/columns via `SHOW TABLES` / `information_schema`.
  Attribution rides DuckLake's native `set_commit_message`; there are no Latiq
  objects in the pond catalog and no shadow store.
- **Forwarding model is real and uniform.** Any node can greet a request (behind an
  nginx front door in dev; a k8s Service in prod). It resolves the pond's owner from
  the registry and forwards over gRPC if it isn't local — so clients hit one
  endpoint and never address a node directly. Reads forward as the Arrow stream;
  writes/describe/drop forward unary. The control plane is consulted only to
  *resolve* the owner, never for data.
- **Control plane is DuckDB-backed** in this slice (not Postgres). Same gRPC
  contract; Postgres remains the scale path.
- **Subcommands:** `latiq serve` (control plane), `latiq node add` (pond node),
  plus `pond` / `query` / `dataset` / `node` CLI verbs — not `control-plane` /
  `pond-node` / `dev` as written in §2/§12.
- **Identity is relaxed:** a claimed `agent_id` (MCP arg / `latiq-agent-id` gRPC
  metadata), default `anonymous`, `verified:false`. OIDC verification (§7) is
  deferred. Cross-hop identity propagation carries the claim; no signing yet.
- **Result handling:** the unary JSON path (MCP/CLI) is bounded by the inline cap
  (default 10k rows); the **Arrow stream path is uncapped** (the streaming answer to
  large results, ahead of a packaged SDK).

**Designed here but not yet built:** admin-curated catalogs + credentials/Vault
(§5, §11), OIDC (§7), per-identity rate limiting (§13a), OpenTelemetry (§13), the
Docker Compose harness (§12), and per-pond resource limits. These remain the
roadmap, not the current surface.

---

## 1. M1 scope

A complete, deployable system that lets agents allocate ponds, write data, query with SQL, share state with other agents, attach admin-curated external catalogs, and release ponds — over MCP, with optional federated identity, with attribution and audit. Operators administer the system through a CLI; agents never see admin operations.

**In M1:**
- Pond lifecycle: create, describe, list, drop
- ANSI SQL surface (SELECT, INSERT, UPDATE, DELETE, CREATE/DROP TABLE)
- Query planning via `explain_query` so agents can estimate cost before running
- Admin-curated external catalogs: operators register catalogs via CLI; agents attach them to ponds via MCP
- Implicit query-by-URI for public, anonymous file sources (Parquet, CSV, JSON on S3/HTTP/etc.)
- Credential store integration (Vault first) — credentials are admin-managed; agents never see secrets and never reference credential names directly
- Multi-agent shared state with attribution
- MCP-over-HTTP for direct agents (Streamable HTTP transport, current spec)
- MCP Prompts as parameterized SOPs for common workflows
- Admin API (gRPC) exposed only to the CLI — separate surface from MCP
- Single-binary distribution; one binary serves both server roles (control plane + pond node) via subcommand
- Optional OIDC token verification (admin-toggleable)
- Mandatory identity for audit (verified or claimed)
- Audit log of every operation, with SQL shape recorded but values redacted
- Per-identity rate limiting at the MCP layer
- Configurable query timeout
- Reserved `_latiq` schema with metadata views
- Local filesystem storage
- Two deployment shapes: single-binary dev mode (one process), Docker Compose for multi-pond simulation on one machine

**Out of M1 (deferred):**
- Python SDK with Flight SQL — M2 (without SDK in M1, large result sets are handled server-side via SQL aggregation/CTAS, not by streaming millions of rows to agents)
- Streaming ingestion (Kafka, CDC) — M2
- Multi-node Kubernetes deployment — M2 / M3
- Full federation across the enterprise estate (live, governed, fine-grained) — M3
- Pond migration between nodes — M3+
- Distributed/object storage — when needed
- Multi-tenancy beyond single global scope — M2/M3
- Disk quotas — M2
- Live credential rotation propagation to in-flight connections — v2
- Real authorization model (per-pond ACLs, column security) — M2/M3
- Management UI
- DataFusion engine — parallel track post-launch

**Principles for M1:**
- **The agent is the customer.** MCP surface serves agents only; admin operations live elsewhere.
- **Hard separation between agent surface and admin surface.** MCP exposes only what agents need. The CLI / Admin API handles everything operators need.
- **One pond, one node.** Single-node ponds with cross-catalog joins. Cross-pond joins are not a feature.
- **Make it boring.** M1 is the floor; magic comes later.

---

## 2. System topology

Two components (same binary, different roles), three protocols, one agent endpoint.

```
                    ┌─────────────────────┐
   Agents (LLMs) ──▶│   Load Balancer     │
   (MCP/HTTP)       └──────────┬──────────┘
                               │
                  ┌────────────┼────────────┐
                  ▼            ▼            ▼
            ┌─────────┐  ┌─────────┐  ┌─────────┐
            │ Pond    │  │ Pond    │  │ Pond    │
            │ Node A  │◀─│ Node B  │─▶│ Node C  │
            │  (MCP)  │  │  (MCP)  │  │  (MCP)  │
            └────┬────┘  └────┬────┘  └────┬────┘
                 │  internal Flight SQL (proxy hops, mTLS)
                 │            │            │
                 │  control-plane gRPC (registry + audit)
                 ▼            ▼            ▼
                       ┌─────────────┐
                       │ Control     │◀── Admin gRPC ──── CLI (latiq)
                       │ Plane       │
                       │ (Postgres)  │
                       └─────────────┘
```

### One binary, two roles

Latiq ships as a **single binary** that serves both roles via subcommand:

```
latiq control-plane    # run the control plane
latiq pond-node        # run a pond node
latiq dev              # run both in one process, for development
latiq <admin command>  # CLI client for admin operations
```

Operators install one binary. Dev mode runs both roles in one process. Production runs them as separate processes on as many hosts as needed.

### Tier 1 — Control plane

Stateful but lightly loaded. Backed by Postgres (SQLite for dev).

**Responsibilities:**
- Routing table: which pond lives on which node
- Pond registry: pond ID, name, owning identity, creation time, policy, tags
- **Catalog registry**: admin-curated external catalogs (name, description, type, URI, credential_ref, allowed_identities)
- Audit log: every operation, every identity, every result
- Identity verification: OIDC token validation against configured issuers (when enabled)
- Node health: pond nodes register and heartbeat here

**Not in the data path.** The control plane never sees query traffic. Pond nodes ask it for routing and catalog information; agents never call it directly for queries.

**Two protocol surfaces on the control plane:**
- **Control gRPC** — pond nodes call this for routing lookups, catalog metadata, audit writes, identity-issuer config
- **Admin gRPC** — the `latiq` CLI calls this for admin operations (catalog registration, credential config, policy, audit queries)

The Admin gRPC surface is separate from the Control gRPC surface; the CLI is the only legitimate client. Authentication is via an admin OIDC issuer (when OIDC is enabled) or a local admin token (when disabled). Admins can never be agents and vice versa — the surfaces don't overlap.

### Tier 2 — Pond nodes

Each node hosts N ponds. Each node has three roles:

1. **Owner** for the ponds physically stored on its local disk
2. **Proxy** for queries against ponds owned by other nodes
3. **MCP gateway** terminating agent HTTP connections

**Surfaces per pond node:**
- **MCP-over-HTTP** (agent-facing, behind LB) — terminates agent calls, serves or proxies as needed
- **Internal Flight SQL over gRPC** (between pond nodes) — proxy hops for queries on ponds the receiving node doesn't own; private surface, mTLS in production
- **Internal gRPC** (to control plane) — for pond create/drop, routing lookups, catalog metadata fetches, audit writes

Flight SQL is used internally in M1 but is not exposed as an external surface. When the Python SDK ships in M2, a separate Flight SQL endpoint will be exposed externally on its own port — same protocol, public auth, public API stability.

**Pond data layout on disk:**
```
/var/lib/latiq/ponds/
  <pond-id>/
    catalog.sqlite           # DuckLake SQL catalog
    data/                    # Parquet files
    metadata.json            # Latiq-owned: creator, policy, tags
```

### Tier 3 — The load balancer

Standard enterprise L7 LB (nginx, envoy, cloud LB). Round-robin or least-connections across pond nodes. Terminates TLS. Forwards MCP-over-HTTP traffic to the pond node pool.

**No Latiq-specific logic in the LB.** Pond nodes handle routing internally. The LB is just a fan-out point and an enterprise integration point.

### What lives where

| Concern | Where |
|---|---|
| Pond lifecycle (allocate, drop, list, describe) | Control plane handles, pond nodes execute |
| Catalog registration (admin) | Control plane via Admin API + CLI |
| Catalog attachment to pond (agent) | Pond node, via MCP |
| Query execution | Pond node that owns the pond |
| Query routing (agent → owning node) | Pond node that received the call, via control plane lookup |
| Identity verification | Pond node (when enabled) |
| Audit log writes | Control plane |
| Audit log queries | Control plane |
| Attribution metadata in data | Pond node, stamped into DuckLake snapshots |

---

## 3. Request flow: agent query

This is the load-bearing flow. Walking through it explicitly:

1. Agent sends `POST /mcp/v1/tools/call` with `{"name": "read_query", "arguments": {"pond": "abc-123", "sql": "SELECT ..."}}` to the load balancer
2. LB forwards to pond node A (any node)
3. A validates the identity claim:
   - If OIDC enabled: verifies JWT signature, extracts identity
   - If OIDC disabled: reads `X-Latiq-Agent-Id` header, accepts as claimed identity
4. A looks up where pond `abc-123` lives. **M1 has no caching** — A always asks the control plane fresh on each call. (Caching with invalidation is M2 work.)
5. A calls control plane via gRPC: `get_pond_location("abc-123")` → "node B"
6. A opens (or reuses) an internal Flight SQL connection to B
7. A submits the query to B via Flight SQL `Execute`, with identity carried in gRPC metadata
8. B trusts A's assertion (no re-validation), executes the query through DuckDB+DuckLake
9. B streams Arrow record batches back to A over Flight SQL
10. A converts each batch to JSON Lines and streams over HTTP chunked transfer to the agent
11. After the last batch, A appends a metadata frame: `{"_meta": {"rows": N, "rows_affected": M, "snapshot_id": S, "duration_ms": D, "bytes_scanned": B, "tables_touched": [...]}}`
12. A writes one row to the audit log via the control plane (async, non-blocking)

**If A is itself the owner of pond `abc-123`**, steps 5-9 collapse: A executes directly against its local DuckDB instance, no proxy hop. Same JSON streaming back to the agent.

Note: Flight SQL is used **internally** between pond nodes in M1 — it's the right wire protocol for streaming Arrow data between trusted nodes. We do not expose Flight SQL externally to agents or SDKs in M1; that ships when the Python SDK does in M2, on a separate external port.

**Latency targets:**
- Local case (A owns the pond): P50 under 50ms for small queries
- Proxy case (A proxies to B): P50 under 80ms for small queries
- Stream first-byte: under 100ms in both cases

---

## 4. The agent-facing MCP surface

Versioned at `/mcp/v1/`. Standard MCP semantics per the 2026-07-28 specification.

**Transport.** Streamable HTTP. Older HTTP+SSE-only transport is not supported. Every request carries the required `Mcp-Method` and `Mcp-Name` headers so the L7 load balancer can route on operation type without parsing the JSON-RPC body. Servers reject requests where headers and body disagree.

**Primitives in use.** Tools, Resources, Prompts. We do not use Sampling (deprecated in the spec) or Roots (filesystem context, not relevant). Elicitation is used only via the Multi-Round-Trip pattern for confirmation flows (see below).

**Tool annotations.** Every tool ships with MCP standard annotations so clients can apply differential policy (auto-approve safe operations, require explicit confirmation for destructive ones).

### Tools

Ten tools. Lifecycle (4), query (3), and catalog (3). `query` is deliberately split into `read_query` and `write_query` so MCP tool annotations are static and accurate — clients can auto-approve reads and require confirmation for writes. `explain_query` is the read-only planner.

#### `allocate_pond`
```
annotations: { readOnlyHint: false, destructiveHint: false, idempotentHint: false }
input:
  name: string (optional; control plane generates if omitted)
  policy: { lifetime_seconds?: int, tags?: [string] }
  attach_catalogs: [string] (optional; names of admin-registered catalogs to attach immediately)
output:
  pond_id: string (UUID)
  pond_name: string
  created_at: timestamp
  attached_catalogs: [{name, source_type, schemas}]  // populated if attach_catalogs was provided
```

Allocates a new pond. Optionally attaches one or more admin-registered catalogs in the same call — common pattern for agents that know upfront what data they need ("create a pond for analyzing CRM data, attach the `crm` and `events` catalogs"). Catalog attach errors don't fail the allocation — the pond is created, any catalog attach failures are reported in the response with structured errors.

#### `describe_pond`
```
annotations: { readOnlyHint: true, idempotentHint: true }
input: pond_id (or pond_name)
output:
  pond_id, pond_name, owner, created_at, policy, tags
  schema_summary: { tables: [{name, columns, row_count_estimate, comment}] }
```

#### `list_ponds`
```
annotations: { readOnlyHint: true, idempotentHint: true }
input: filter: { owner?, tag?, name_prefix? } (optional)
output: ponds: [{pond_id, pond_name, owner, created_at, tags}]
```
M1: returns all ponds in the deployment (single global scope).

#### `drop_pond`
```
annotations: { readOnlyHint: false, destructiveHint: true, idempotentHint: true }
input: pond_id (or pond_name), confirm: bool
output: released_at: timestamp
```
Authoritative — kills in-flight queries on this pond with a structured error.

If the pond contains data and `confirm: true` was not provided, the tool returns a Multi-Round-Trip `InputRequiredResult` describing the pond contents and asking for explicit confirmation. The client gathers the answer and re-issues with the echoed `requestState` — any pond node can pick up the resumed call (state is in the payload, not the server).

#### `read_query`
```
annotations: { readOnlyHint: true, idempotentHint: true }
input:
  pond: pond_id or pond_name
  sql: string (ANSI SQL preferred, DuckDB extensions tolerated; must be SELECT or a read-only metadata statement like DESCRIBE / SHOW)
  parameters: [value] (optional)
output: streaming JSON Lines via HTTP chunked transfer
  - one JSON object per row
  - final object is {"_meta": {...}}
```

Reads only. If the SQL is not a SELECT or read-only metadata statement, the tool returns a structured error directing the agent to `write_query`.

#### `write_query`
```
annotations: { readOnlyHint: false, destructiveHint: true, idempotentHint: false }
input:
  pond: pond_id or pond_name
  sql: string (ANSI SQL with documented DuckDB extensions tolerated; INSERT, UPDATE, DELETE, CREATE/DROP/ALTER TABLE, CREATE TABLE AS SELECT)
  parameters: [value] (optional)
output: streaming JSON Lines via HTTP chunked transfer
  - row stream (for RETURNING clauses) or empty
  - final object is {"_meta": {...}}
```

Writes and DDL. Tool annotation is `destructiveHint: true` because `write_query` *can* delete data (DELETE, DROP TABLE) — the annotation reflects worst-case capability, which lets MCP clients require user/policy approval by default for any call.

Additional in-tool safety: a `write_query` issuing destructive DML (DELETE, DROP TABLE, TRUNCATE) that would affect more rows than a configurable threshold (default 1000) returns an `InputRequiredResult` asking the agent to confirm. The MCP annotation triggers client-level policy; this triggers in-tool policy. Both layers, both useful.

**Inline result cap** (both query tools): configurable, default 10,000 rows or 1MB. Queries exceeding it return a structured error advising the agent to narrow with WHERE/LIMIT, aggregate server-side, or materialize into a pond table with `CREATE TABLE ... AS SELECT` for further analysis. (The Python SDK in M2 will allow streaming larger result sets directly.)

#### `explain_query`
```
annotations: { readOnlyHint: true, idempotentHint: true }
input:
  pond: pond_id or pond_name
  sql: string (any SELECT or DML)
  parameters: [value] (optional)
output:
  estimated_rows: int
  estimated_bytes: int
  estimated_duration_ms: int (rough cost estimate)
  scan_operations: [
    { table: string, type: "full_scan" | "filtered_scan" | "indexed",
      estimated_rows_scanned: int, source: "pond" | "attached" }
  ]
  joins: [{ tables: [string], type: string, estimated_rows: int }]
  warnings: [string]  -- e.g., "Full scan on events (1.2M rows). Consider WHERE on occurred_at."
  suggestions: [string]  -- e.g., "Filter on severity reduces scan to ~12K rows."
  raw_plan: string (DuckDB EXPLAIN output, for agents that want it)
```

Plans the query without executing it. Agents call this before `read_query` or `write_query` to reason about cost.

Returns the estimated work the query would do, warnings about heavy operations (full scans, large joins, cross-product accidents), and suggestions for narrowing. Wraps DuckDB's `EXPLAIN` with structured output and added guidance.

Common pattern:
1. Agent forms a query
2. Agent calls `explain_query` → sees "estimated 2.4M rows, full scan, suggest WHERE on date column"
3. Agent refines the query
4. Agent calls `explain_query` again → sees "estimated 8K rows"
5. Agent calls `read_query`

This is one of the most important tools in the surface. It's what makes agents thrifty rather than greedy.

#### `list_catalogs`
```
annotations: { readOnlyHint: true, idempotentHint: true }
input: filter: { type?, name_prefix? } (optional)
output:
  catalogs: [
    {
      name: string,            // alias to use in attach_catalog and in SQL
      description: string,     // admin-written, human/agent-readable purpose
      type: string,            // "postgres" | "snowflake" | "iceberg" | "parquet_dir" | etc.
      read_only: bool
    }
  ]
```

Returns the admin-curated catalogs visible to the calling identity. Each catalog has a name, a description (why this catalog exists, what's in it), and a type. Connection details, credentials, and URIs are *not* exposed — agents see only what they need to decide whether to attach.

Catalogs are registered by admins via the `latiq catalog register` CLI command (§5). Agents discover them here.

#### `attach_catalog`
```
annotations: { readOnlyHint: false, destructiveHint: false, idempotentHint: true }
input:
  pond: pond_id or pond_name
  catalog: string (name from list_catalogs)
output:
  catalog_name: string          // the alias now valid in SQL on this pond
  source_type: string
  schemas: [
    { schema: string, tables: [{name, columns: [{name, type}], estimated_rows}] }
  ]
```

Attaches an admin-registered catalog to a pond. Agents pass only the catalog *name*. Latiq handles credential resolution, connection setup, and the DuckDB `ATTACH` behind the scenes.

Once attached, agents query the catalog with three-part naming:

```sql
SELECT * FROM <catalog>.<schema>.<table> WHERE ...
```

For example, after `attach_catalog(pond, catalog: "crm")`:

```sql
SELECT * FROM crm.public.customers WHERE region = 'NA';
SELECT c.name, o.total
FROM crm.public.customers c
JOIN crm.public.orders o ON o.customer_id = c.id;
```

**Cross-catalog joins work.** Agents can join pond-local tables with attached external tables in one SQL query — this is the heart of the value proposition. Pull data in by reference, join with local work, materialize only what's worth keeping.

**Materializing into the pond:**

```sql
CREATE TABLE my_customers AS
  SELECT * FROM crm.public.customers WHERE region = 'NA';
```

That's a pond-local table now. The catalog can be detached without losing the local copy.

**Authorization:** the calling identity must be in the catalog's allowed-identities list (set by the admin at registration). If not, the call returns a structured error. Restricted catalogs are filtered from `list_catalogs` for identities that aren't allowed — agents don't see what they can't use.

#### `detach_catalog`
```
annotations: { readOnlyHint: false, destructiveHint: true, idempotentHint: true }
input:
  pond: pond_id or pond_name
  catalog: string (name)
output:
  detached_at: timestamp
```

Detaches a catalog from a pond. Materialized pond-local tables (created with `CREATE TABLE AS SELECT`) are unaffected. The catalog alias becomes invalid; subsequent queries against `<catalog>.*` fail until reattached.

### Implicit query-by-URI (public file reads)

For raw, public, anonymous file sources — a Parquet file an agent wants to peek at, a CSV on a public bucket — the agent can issue SQL against the URI directly without attaching:

```sql
SELECT * FROM 's3://bucket/data.parquet' LIMIT 10;
SELECT count(*) FROM read_json('https://example.com/feed.json');
SELECT * FROM read_csv('https://example.com/data.csv');
```

This is standard DuckDB syntax and works through `read_query` / `write_query`. The pond node validates the URI against the loaded-extensions allowlist. **No credentials are supported on this path** — credentialed access requires an admin-registered catalog.

**This path is for public, anonymous files only.** Databases (Postgres, Snowflake, etc.) cannot be queried by URI — they must be admin-registered as catalogs and attached. The pond node returns a structured error if the agent tries.

Admins can disable implicit query-by-URI entirely via config for strict-governance environments.

**When to use which:**

- *Implicit query-by-URI*: public files, one-shot reads, exploratory peeks at anonymous data
- *Admin-registered catalogs*: anything credentialed, anything enterprise (databases, warehouses, lake formats), anything the org wants to govern

### Prompts

MCP Prompts are parameterized standard operating procedures — workflow templates an agent invokes to get a structured guide for a common task. Different from Resources (static context to read) and Tools (operations to invoke): Prompts are *invokable procedures that return guidance*.

M1 ships these prompts:

#### `setup_multi_agent_pond`
```
parameters:
  pond_name: string
  expected_writers: [string] (agent identities that will write)
  domain: string (e.g., "incident response", "research analysis")
output: structured workflow guide covering:
  - pond allocation with appropriate tags
  - initial schema design with collaborative comments
  - attribution lookup pattern
  - conflict-handling guidance for this specific writer count
```

#### `discover_existing_pond`
```
parameters:
  search_term: string
  intent: "read" | "write" | "extend"
output: structured workflow covering:
  - list_ponds + filter strategy
  - describe_pond on candidates
  - _latiq.tables_summary read pattern
  - intent-specific next steps (e.g., for "extend": how to add tables without conflicting)
```

#### `design_collaborative_schema`
```
parameters:
  domain: string
  expected_tables: [string] (optional)
output: structured guide covering:
  - naming conventions for cross-agent legibility
  - COMMENT clause usage patterns with examples
  - type choices tuned to the domain
  - how to evolve the schema as the work progresses
```

#### `recover_from_conflict`
```
parameters:
  pond_id: string
  failed_operation: string (the SQL that conflicted)
output: structured recovery guide covering:
  - reading the current snapshot state
  - identifying the conflicting writer via _latiq.attribution
  - retry strategy options (back off, re-plan, coordinate)
```

Prompts return structured text (not tool calls) — they teach the agent how to compose the right tool calls. Adding a prompt is much cheaper than adding a tool; we'll add more as we observe what agents actually need help with.

### Resources

Static context. Read-only. The same recipe / troubleshooting taxonomy from §4a (MCP UX principles), exposed at `latiq://` URIs.

- `latiq://ponds` — listing of all ponds (live data)
- `latiq://ponds/{pond_id}/schema` — schema of a specific pond
- `latiq://dialect` — ANSI SQL contract, supported types, compatibility notes
- `latiq://guidance` — top-level guidance, points to recipes
- `latiq://recipes/*` — scenario-based guides (see §4a)
- `latiq://troubleshooting/*` — problem-keyed action guides (see §4a)

### What's deliberately not exposed

- **No streaming subscribe.** Agents poll for changes.
- **No cross-pond join syntax.** Queries are scoped to one pond. To share data across ponds, agents copy via SQL (read from one, write to another).
- **No multi-statement transactions across calls.** Each query call is one statement. Atomicity within a single call only.

---

## 4a. MCP UX principles

The MCP surface is the product. Agents are LLMs reasoning over tool calls and resource reads; the surface either makes them effective or it doesn't. The principles below are not style preferences — they're how we instantiate "the agent is the customer" in concrete prose. Every tool description, error message, warning, and resource body should be written against these principles.

### Principle 1 — Tool descriptions are mini-tutorials, not API docs

A tool description should teach an LLM how to use the tool well in one read, not just enumerate parameters. Every tool description includes:

- **What it does**, in one sentence, agent-relevant
- **When to use it** vs. alternatives in the surface
- **A concrete example** showing the most common correct usage
- **A do/don't pair** when there's a common pitfall
- **Cross-references** to relevant resources for deeper guidance

Example shape for `read_query`:

> Run a read-only SQL query against a pond. Pond must exist; use `allocate_pond` or `list_ponds` first. For writes/DDL, use `write_query`.
>
> Latiq prefers ANSI SQL; DuckDB extensions are tolerated but reduce portability — prefer ANSI when writing code other agents will read.
>
> **Example — discover schema, then query:**
> ```sql
> -- Discover tables and their purposes:
> SELECT name, comment FROM _latiq.tables_summary;
>
> -- Query with selective WHERE on a documented column:
> SELECT id, severity, raised_by FROM alerts
> WHERE severity IN ('high', 'critical')
> ORDER BY id DESC LIMIT 100;
> ```
>
> **Do:** Call `explain_query` first if you're unsure of cost. Read `_latiq.tables_summary` to discover what's available. Include WHERE clauses on selective columns.
> **Don't:** Issue unbounded `SELECT *` on large tables. The inline result cap will refuse results over ~10K rows — narrow first, or aggregate server-side.
>
> Results stream back as JSON Lines. Final line is `{"_meta": {...}}` with query stats. See `latiq://recipes/query-metadata` for how to use them.

### Principle 2 — Errors are next-action suggestions, not just diagnostics

Every error response includes three structured fields beyond the standard error code:

- `what_failed` — concise statement of the immediate failure
- `why` — the underlying reason, in agent-actionable terms
- `try` — a list of concrete next actions, ordered from most likely useful

When relevant, errors include:
- `did_you_mean` — fuzzy-matched alternatives (table names, pond names, column names)
- `example` — a corrected version of the request

Example: agent calls `read_query` against a non-existent pond `incident-001`:

```json
{
  "error": {
    "code": "POND_NOT_FOUND",
    "what_failed": "Pond 'incident-001' does not exist.",
    "why": "No pond with this ID or name is registered in the deployment.",
    "try": [
      "Call list_ponds to see available ponds.",
      "Call allocate_pond if you want to create a new one.",
      "Check the spelling of the pond name."
    ],
    "did_you_mean": ["incident-2026-001", "incident-response"],
    "example": {
      "tool": "list_ponds",
      "arguments": {"name_prefix": "incident"}
    }
  }
}
```

This is more bytes than a traditional API error. That's fine. The agent makes one fewer call to recover, the LLM uses fewer tokens reasoning about what went wrong, and the experience feels like collaboration rather than rejection.

### Principle 3 — Suggestions over errors when the operation can proceed

When something the agent did was suboptimal but the operation succeeded, return success with `warnings`. The query ran; it could have been better.

Examples of warning categories:
- **Performance**: "Full table scan on `events` (1.2M rows, 800ms). Consider adding WHERE on `occurred_at` which is the most selective indexed column."
- **Portability**: "Query uses DuckDB-specific function `list_value()`. Prefer ANSI `ARRAY[...]` for portability."
- **Schema hygiene**: "Created table `data` with no column comments. Other agents discovering this pond won't know what the columns mean. Consider adding COMMENT clauses."
- **Result hygiene**: "Result set hit the inline cap (10,000 rows). 23,847 rows total. Narrow your query with WHERE/LIMIT, aggregate server-side (GROUP BY, count, sum), or materialize into a pond table with CREATE TABLE AS SELECT for further analysis."

Warnings are advisory, structured, and aggregated in the `_meta.warnings` array of the query response. They don't fail the call. They teach over time.

### Principle 4 — Every response carries forward signal

The `_meta` field on every query response is part of the UX, not just instrumentation. It includes:

- `rows`, `rows_affected`, `snapshot_id`, `duration_ms`, `bytes_scanned`, `tables_touched`
- `warnings` (described above)
- `attribution` — which identity produced the snapshot this query saw or wrote
- `hint` (optional) — a single recommended next action when the agent's pattern suggests one (e.g., after a CREATE TABLE: "Other agents can now discover this table via `_latiq.tables_summary`.")

Agents read `_meta` to self-correct. Frameworks surface it in their traces. Humans debug with it. It's cheap to populate and high-leverage to consume.

### Principle 5 — Resources are recipes, not reference docs

The `latiq://` resource namespace is where agents go to learn patterns. Each resource is a focused, scenario-based guide written for LLM consumption — short, concrete, example-heavy.

Resource taxonomy for M1:

**Reference (small, stable):**
- `latiq://dialect` — ANSI SQL contract, supported types, function compatibility notes
- `latiq://guidance` — top-level guidance, points to recipes
- `latiq://ponds` — listing of all ponds (live data)
- `latiq://ponds/{pond_id}/schema` — pond schema (live data)

**Recipes (scenario-based, prose with SQL examples):**
- `latiq://recipes/schema-design` — authoring tables agents will collaborate on (comments, naming, types)
- `latiq://recipes/multi-agent-coordination` — sharing state, attribution, conflict patterns
- `latiq://recipes/attribution-lookup` — "who wrote this row/table" patterns
- `latiq://recipes/large-results` — handling large result sets without the SDK (narrow / aggregate / materialize patterns)
- `latiq://recipes/query-metadata` — using `_meta` to self-correct
- `latiq://recipes/data-ingestion-m1` — how to load data via SQL in M1 (M2 will add native ingestion)

**Troubleshooting (problem-keyed, action-oriented):**
- `latiq://troubleshooting/conflicts` — what to do when writes conflict and retry
- `latiq://troubleshooting/timeouts` — breaking up slow queries
- `latiq://troubleshooting/pond-not-found` — discovery and recovery
- `latiq://troubleshooting/permission-denied` — when auth is on and an operation fails

Each recipe and troubleshooting entry is structured the same way:

> **When you'd use this:** one-sentence scenario
> **The pattern:** 3-7 lines of SQL or pseudocode
> **Why it works:** brief explanation grounded in Latiq's semantics
> **What to watch for:** common mistakes
> **Related:** links to other resources

### Principle 6 — Voice is direct, declarative, collaborative

Write to the agent, not about it. Use second person ("You'll want to..."). Avoid hedging ("might possibly want to consider..."). Be specific ("Add a WHERE clause on `occurred_at`" not "narrow your query").

Tone is collegial — Latiq is a system the agent works *with*, not a service the agent submits requests to. The MCP surface should read like a helpful senior engineer pairing with the agent, not like an API gateway returning rejections.

### Principle 7 — Prose is for LLMs first, humans second

These responses, descriptions, and resources will be read by LLMs vastly more often than by humans. That changes how we write:

- **Concrete examples beat abstract prose.** Show the SQL; don't describe it.
- **Explicit do/don't pairs beat single-sided guidance.** LLMs use contrast to anchor.
- **Structured fields beat freeform paragraphs.** JSON keys with consistent meaning let LLMs route reasoning.
- **Repeat the important things.** Token-efficient prose isn't the goal; clarity is. LLMs handle repetition better than ambiguity.
- **Avoid hedge words.** "Generally," "typically," "sometimes" — cut them. Be definite where Latiq is definite.

Humans reading these will find them unusually direct. That's correct. The audience is an LLM agent making a decision in milliseconds.

### Principle 8 — The MCP surface is versioned and stable

Within a major version (`/mcp/v1/`), tool names, parameter names, error codes, resource URIs, and the structure of `_meta` are stable. Additions are allowed; removals and renames are not. Agents pin their assumptions to the surface; we don't break them mid-version.

Breaking changes wait for `/mcp/v2/`. M1 does not need a deprecation policy or migration story — just stability within v1.

### What we won't do

Three temptations to resist:

- **Verbose meta-explanation.** Don't preface tool descriptions with "This tool allows you to..." — just say what it does.
- **Aspirational guidance.** Don't document features that don't exist yet. Recipes describe M1's actual surface. Federation, ingestion, and promotion get their own recipes when they ship.
- **Apologetic errors.** "We're sorry, but..." wastes tokens. State the failure and the next action.

The MCP surface should feel like a system that knows what it is and helps the agent do its job. Clear, specific, opinionated.

---

## 5. The Admin API and `latiq` CLI

The Admin API is a separate gRPC surface on the control plane, distinct from the MCP surface. The `latiq` CLI is the primary (and in M1, only) client. The Admin API is how operators configure the deployment; agents never touch it.

**Authentication.** The CLI authenticates with the control plane via:
- A local admin token when OIDC is disabled (stored in `~/.latiq/credentials` or `LATIQ_ADMIN_TOKEN` env var)
- An admin OIDC issuer + token when OIDC is enabled (separate from agent OIDC config)

Admin identity is logged in the audit log alongside the operation.

### Admin operations exposed via CLI

#### Catalog management

```
latiq catalog register \
  --name crm \
  --description "Production CRM (Postgres) — customers, orders, leads" \
  --type postgres \
  --uri "host=db.prod.internal port=5432 dbname=crm" \
  --credential prod-crm \
  --read-only \
  --allow-identity "*"

latiq catalog list
latiq catalog describe crm
latiq catalog update crm --description "..."
latiq catalog grant crm --identity "agent-incident-response"
latiq catalog revoke crm --identity "agent-incident-response"
latiq catalog unregister crm
```

A registered catalog persists in the control plane's catalog registry. Agents see it in `list_catalogs` (filtered by allowed identities) and attach it via `attach_catalog`.

#### Credential management

```
latiq credential add prod-crm --vault-path "secret/data/crm/db"
latiq credential add s3-public --type none
latiq credential list
latiq credential rotate prod-crm  # forces refetch from store on next attach
latiq credential remove prod-crm
```

Credentials reference entries in the configured credential store (§11). Latiq stores no credential material itself.

#### Node management

```
latiq node list
latiq node describe pond-node-7
latiq node drain pond-node-7    # stops new pond assignments, existing ponds keep serving
latiq node remove pond-node-7   # only after drain + ponds migrated (M3) or accepted as lost
```

#### Identity issuer management

```
latiq issuer add enterprise-keycloak \
  --url "https://auth.enterprise.com/realms/agents" \
  --audience latiq \
  --required-claims agent_id
latiq issuer list
latiq issuer remove enterprise-keycloak
```

#### Policy management

```
latiq policy show
latiq policy set default-pond-lifetime --seconds 3600
latiq policy set rate-limit-rps --identity "*" --value 100
latiq policy set query-timeout --seconds 30
latiq policy set implicit-uri-queries --enabled true
```

#### Audit access

```
latiq audit tail
latiq audit search --identity "agent-foo" --since 1h
latiq audit export --since 24h --format json
```

### What the Admin API is *not*

- **It is not exposed to agents.** Different gRPC surface, different authentication, different observability. A misrouted agent call to the Admin API gets `PERMISSION_DENIED`.
- **It is not a separate binary in M1.** The `latiq` binary serves both server roles (`latiq control-plane`, `latiq pond-node`) and the CLI (`latiq catalog ...`, `latiq node ...`, etc.). One install, multiple roles via subcommand.
- **It is not a UI.** No web console in M1. CLI is the surface; UI is a future product.

---

## 6. Internal protocol surfaces

Two internal surfaces, both off-limits to agents and to the CLI.

### 6.1 Control plane gRPC (pond nodes only)

Pond nodes call these. Mutually-authenticated via mTLS in production.

- `register_node(node_id, endpoint, capacity)` — node registers on startup
- `heartbeat(node_id, current_state)` — node liveness + capacity update
- `get_pond_location(pond_id) → node_endpoint` — for proxy routing
- `get_catalog(catalog_name, requesting_identity) → catalog_details` — pond node fetches catalog metadata + credential reference at attach time
- `record_audit(audit_entry)` — async log writes
- `create_pond_assignment(pond_id, owner_identity, policy) → assigned_node` — control plane decides placement and tells caller

### 6.2 Pond-node-to-pond-node Flight SQL (proxy hops)

When pond node A proxies a query to pond node B, the wire protocol is **Flight SQL over gRPC**. Flight SQL is the right choice for this hop — standardized service definition, native streaming, type-preserving Arrow transport, well-supported error semantics. We use it internally even though we don't expose it externally in M1.

Operations used:

- `Execute` — A submits SQL to B, B streams Arrow record batches back
- `ExecuteUpdate` — for DML, single response with affected row count and metadata
- `GetTables`, `GetSchemas` — used by the pond node to introspect when needed

Identity propagation: A's Flight calls carry the original agent identity in gRPC metadata. B trusts A's assertion (see §13a for the security trade-off and configurable strict mode).

**Why Flight SQL internally but not externally in M1.** Internally, Flight SQL is a private surface — pond nodes only, mTLS, network controlled by the operator. We get the protocol's value (streaming, Arrow, standardized service) without the costs of a public API commitment (client SDKs, auth design, version stability across external consumers). When M2 ships the Python SDK, we expose Flight SQL on a separate external port — same protocol, promoted from private to public. The internal surface having been validated by M1 traffic dramatically accelerates that M2 work.

**No agent ever sees Flight SQL in M1.** Agents speak MCP-over-HTTP. The pond node terminates the agent's MCP call, opens a Flight SQL connection to the owning pond node (if not itself), streams Arrow batches back over Flight, converts each batch to JSON Lines, and streams to the agent over HTTP chunked transfer. Two protocols stitched at the edge of the pond node.

---

## 7. Identity, auth, audit

### Auth modes

**Disabled (dev mode default):**
- No JWT validation
- Agent supplies `X-Latiq-Agent-Id` header with a claimed identity string
- Audit log records the claim with `verified: false`
- Suitable for dev, evaluation, single-tenant trusted environments

**Enabled (admin opt-in):**
- One or more OIDC issuers configured in Latiq config
- Agent supplies `Authorization: Bearer <jwt>`
- Pond node validates JWT signature, expiration, issuer, audience
- Identity extracted from `sub` claim (and optional configured claims)
- Audit log records with `verified: true`
- Validation happens at the receiving pond node; internal proxy hops trust

### Audit log

Every operation writes to the audit log via the control plane:

```sql
audit_log (
  audit_id: uuid,
  timestamp: timestamp,
  agent_identity: string,
  identity_verified: bool,
  operation: string,
  pond_id: uuid,
  request_summary: jsonb,   -- redacted SQL, parameter counts
  result_summary: jsonb,    -- rows affected, snapshot_id, error if any
  duration_ms: int
)
```

Audit is mandatory; auth is optional. Even unverified identities produce audit records.

### Redaction policy

Audit logs record SQL **shape**, not SQL **content**. Concretely:

- Literal values in the SQL are replaced with `?` placeholders before logging (`SELECT * FROM events WHERE id = 47` → `SELECT * FROM events WHERE id = ?`)
- Parameters from parameterized queries are counted but never logged (`request_summary` records `parameter_count: 3`, not the values)
- Error messages from the engine are sanitized to remove literal values before being placed in `result_summary`
- Query results never enter the audit log under any circumstance

This makes the audit log forensically useful (we can answer "what shape of query did this agent run?") without making it a data-exfiltration target.

### Attribution in data

Every write is tagged with the producing agent identity. The implementation uses DuckLake snapshot metadata where the format supports identity-tagged commits, or a Latiq-maintained `_latiq.attribution` table (in the same SQLite catalog as DuckLake's catalog tables) that maps `snapshot_id → identity → table → timestamp`. From the agent's perspective the surface is the same: query `_latiq.attribution` to see who wrote what. The engineering team picks the cleaner implementation path based on DuckLake's actual snapshot-metadata capabilities at build time.

---

## 8. The reserved `_latiq` schema

Every pond, on creation, has a `_latiq` schema pre-created with read-only views. Agents query it via standard SQL; we don't need custom MCP tools for metadata.

**Views:**

```sql
_latiq.pond_info
  -- single row: pond_id, name, owner, created_at, policy, tags

_latiq.snapshots
  -- one row per DuckLake snapshot: snapshot_id, created_at, identity, operation

_latiq.attribution
  -- table_name, snapshot_id, identity, written_at
  -- joined view making "who wrote this table" easy

_latiq.tables_summary
  -- table_name, row_count, byte_size, last_modified, comment

_latiq.sources
  -- one row per attached source (catalog):
  --   name: the catalog alias used in SQL (catalog.schema.table)
  --   source_type: "postgres" | "mysql" | "snowflake" | "iceberg" | "parquet_dir" | etc.
  --   uri: connection target (credentials redacted)
  --   credential_name: which admin-registered credential was used (or NULL for public)
  --   read_only: bool
  --   attached_by: agent identity
  --   attached_at: timestamp
  --   schemas_visible: array of schemas exposed by this catalog
```

**Why views, not tools:** agents already speak SQL. Standard `SELECT * FROM _latiq.attribution WHERE table_name = 'events'` is more agent-native than a custom `get_attribution` tool. The `_latiq` schema is documented and stable.

**Writes to `_latiq.*` are blocked** by the pond node — it's a reserved namespace.

**Operational metrics are not exposed to agents.** Performance trends, query latency percentiles, error rates, disk headroom — all live in the OpenTelemetry stream emitted to the ops backend (see §13). Agents stay focused on their own task; capacity and health decisions belong to operators and their dashboards.

---

## 9. Guidance to agents

The `latiq://guidance` MCP resource returns a structured document agents read on demand. Key topics:

**SQL dialect:** "Latiq speaks ANSI SQL. DuckDB-specific functions work but reduce portability. When writing portable agent code, prefer ANSI constructs."

**Self-describing schemas:** "When creating tables, add column comments and table comments. Other agents discovering your pond rely on them."

```sql
CREATE TABLE events (
  id INTEGER COMMENT 'event primary key',
  severity VARCHAR COMMENT 'one of: low, medium, high, critical',
  occurred_at TIMESTAMP COMMENT 'event timestamp in UTC'
) COMMENT 'incident events stream from monitoring';
```

**Attribution:** "Your writes are tagged with your agent identity. To see who wrote what, query `_latiq.attribution`."

**Large results:** "For result sets over ~10K rows, narrow the query (WHERE/LIMIT), aggregate server-side (GROUP BY, count, sum), or materialize into a pond table with `CREATE TABLE ... AS SELECT` for further analysis. The MCP inline cap exists to keep the LLM context bounded; the M2 Python SDK will allow streaming larger result sets."

**Coordination:** "Multiple agents in the same pond write through DuckLake's transactional model. Conflicts auto-retry; expect occasional snapshot bumps. If you need strict ordering, coordinate at the application layer."

This is the place where "agents speak SQL, we federate the rest" gets operationalized. The guidance lives in one resource, written for LLM consumption, kept up to date.

---

## 10. Concurrency model

Inside a pond:

- **Reads never block.** Snapshot isolation per query. Each query sees the latest committed snapshot at start.
- **Writes serialize at commit.** DuckLake's transactional model: each write transaction operates on its starting snapshot, commits a new snapshot. Conflicting writes auto-retry.
- **Conflict semantics:** two writers on the same row → second retries against the new snapshot. Two writers on different rows → both commit, two snapshots produced.

**Connection pooling:** each pond has a small DuckDB connection pool on its owning node (default 4-8). Connections check out per query, return after.

**Query timeout:** configurable per deployment, default 30 seconds. Killed cleanly with a structured error.

**Drop is authoritative:** dropping a pond kills in-flight queries with a structured error indicating the pond was dropped.

---

## 11. External sources and credentials

Agents allocate ponds and need to put data in them — or query data that lives elsewhere without copying it. DuckDB's catalog model is the right mental anchor: external sources are attached as **catalogs**, and queries reach into them with three-part naming (`catalog.schema.table`). Cross-catalog joins work natively, so a pond can have its own native catalog plus several attached external catalogs, and a single SQL query can join across all of them.

**Admins curate; agents consume.** External sources are registered by operators via the CLI (`latiq catalog register`). Agents discover registered catalogs via `list_catalogs`, attach them to ponds via `attach_catalog`, and query them via SQL. **Agents never see URIs, never see credentials, never register catalogs themselves.** This separation is structural to M1 — different surfaces, different identities, different audit trails.

Two paths for getting data into a pond:

1. **Write data directly with SQL** — `INSERT`, `CREATE TABLE`, `CREATE TABLE AS SELECT FROM <attached>.<schema>.<table>` (materializes into the pond)
2. **Attach an admin-registered catalog** and query by reference; or use implicit query-by-URI for public anonymous files

This section covers how the second path works architecturally.

### Catalog registry

The control plane maintains a catalog registry — registered by admins, queried by pond nodes when an agent calls `attach_catalog`.

Each registered catalog has:
- `name` — the alias agents use in SQL
- `description` — admin-written, agent-readable purpose
- `type` — postgres, mysql, snowflake, iceberg, delta, parquet_dir, etc.
- `uri` — connection string or path (not exposed to agents)
- `credential_ref` — name of an entry in the credential store (not exposed to agents)
- `read_only` — bool (default true)
- `allowed_identities` — list of identity patterns that can attach this catalog; `"*"` means anyone

The registry is queried in two flows:
- Agent calls `list_catalogs` → control plane returns registered catalogs filtered by the calling identity's allow list
- Agent calls `attach_catalog(pond, name)` → pond node calls `get_catalog(name, identity)` on the control plane, gets the full catalog details, resolves credentials, ATTACHes via DuckDB

### Loaded extensions

Each pond node loads a fixed set of DuckDB extensions on startup. The set is configurable by the admin but bounded — we don't load arbitrary extensions at runtime. M1 default extension set:

- `httpfs` — HTTP, HTTPS, S3, GCS, Azure Blob access
- `parquet` — Parquet read/write
- `json` — JSON read/write
- `iceberg` — Iceberg table reads
- `delta` — Delta Lake reads
- `postgres` — Postgres database attach (formerly `postgres_scanner`)
- `mysql` — MySQL database attach (formerly `mysql_scanner`)
- `aws` — AWS credential helpers

Snowflake and Databricks support depends on the maturity of their DuckDB extensions at M1 ship time; we'll include them if stable, document the gap if not.

### Credential store integration

Agents never see credentials. Admins register credentials in an external credential store; agents reference them by name.

**Supported credential stores in M1:**

- **HashiCorp Vault** — primary integration. Latiq is a Vault client. Admin configures a Vault address, mount point, and auth method (token, AppRole, Kubernetes auth, etc.). Credentials are fetched at attach time.
- **Environment variables** — for dev mode and simple deployments. Admin maps credential names to env vars in Latiq config.
- **Static config file** — for testing and air-gapped environments. Discouraged for production.

The credential resolution flow:

1. Agent calls `attach_catalog(pond, catalog: "crm")` — agent only knows the catalog name
2. Pond node calls control plane: `get_catalog("crm", identity)` → returns URI + credential_ref + connection details
3. Pond node looks up the `credential_ref` (e.g., `"prod-crm"`) in its credential resolver
4. Credential resolver calls the configured backend (Vault, etc.) using its own service identity
5. Backend returns the credential payload (e.g., `{access_key, secret_key, region}` for S3 / `{user, password, host}` for Postgres)
6. Pond node uses the credential to ATTACH the source via DuckDB, then **discards** the credential value (it's not cached at the Latiq layer)
7. Subsequent queries against the attached catalog reuse the established connection (database sources) or re-resolve the credential as needed (object storage signed URLs)

Credentials are never persisted in Latiq's catalog registry, audit log, or pond data. They live in the credential store; Latiq fetches on demand and discards.

### Credential lifecycle

The credential's lifecycle is managed by the external store, not by Latiq. If the credential rotates or is revoked in Vault, subsequent attach calls will see the new value or fail. Existing attachments with cached connections may continue to work until the connection drops, at which point re-establishment will use the current credential.

This is fine for M1. Live credential propagation to in-flight connections is a complex problem we defer to v2.

### Allowlist enforcement

Admin config can restrict which catalogs are visible to which identities. M1 ships a simple version:

- Per-catalog `allowed_identities` list set at registration (`latiq catalog register --allow-identity "*"` or `--allow-identity "agent-foo"`)
- `latiq catalog grant <name> --identity <pattern>` adds an identity to a registered catalog's allow list
- `latiq catalog revoke <name> --identity <pattern>` removes one
- Filtered at `list_catalogs` — agents see only catalogs they're allowed to attach

Richer policy (per-pond source restrictions, per-tenant credential isolation, fine-grained column-level masking) is M2/M3 work.

### Why this design

Three reasons:

1. **Agents don't handle secrets.** This is non-negotiable. Agents are LLM-driven; we cannot trust an LLM with a credential it might leak in a response or log line.

2. **Credentials live in the enterprise's existing secret infrastructure.** Building a Latiq-native secret store would duplicate Vault badly. Integrating with Vault means Latiq fits into existing enterprise practice.

3. **The credential boundary is the admin's, not the agent's.** Admins register what's available; agents choose from the menu. This matches how enterprises actually want to operate.

---

## 12. Deployment

Two deployment shapes for M1: single-binary dev mode and Docker Compose for simulating multi-pond-node topology on one machine. Multi-host Kubernetes deployment is M2/M3.

### Dev mode (single binary, single process)

```
$ latiq dev
```

Control plane + one pond node running in one process. SQLite for the registry. All ports on localhost. No auth (OIDC disabled by default in dev mode). No load balancer. No mTLS. The CLI talks to localhost.

```
$ latiq catalog register --name sample --type parquet_dir --uri "/data/samples/"
$ latiq pond list   # uses MCP under the hood; admin can see all ponds
```

Suitable for a developer evaluating Latiq, framework authors integrating, demos, and tests. One binary, one process, run-anywhere.

### Docker Compose (multi-pond-node simulation on one machine)

For developers and operators who want to exercise the smart-proxy topology — multiple pond nodes, control plane separation, cross-node query routing — without standing up a Kubernetes cluster.

Ships as `docker-compose.yml` in the repo. Brings up:

- 1 Postgres container (control plane registry)
- 1 Latiq control-plane container
- 3 Latiq pond-node containers (configurable)
- 1 nginx container acting as the L7 load balancer
- 1 OpenTelemetry collector container (optional)

```
$ docker compose up
$ latiq --endpoint http://localhost:8080 catalog register --name sample ...
$ # agents now hit http://localhost:8080/mcp/v1 ; LB distributes across the 3 pond nodes
```

This is the demo topology. It's also the M1 success-criterion harness — the concurrent-multi-agent test in §14 runs against Docker Compose, not against the single-binary dev mode.

### Production-ish single-host (not M1 ship-target, but supported)

Operators can run the single binary in two roles on separate machines without Docker:

```
host-1$ latiq control-plane --config /etc/latiq/control-plane.yaml
host-2$ latiq pond-node --config /etc/latiq/pond-node.yaml --control-plane host-1:9090
host-3$ latiq pond-node --config /etc/latiq/pond-node.yaml --control-plane host-1:9090
```

This works but is unsupported as a deployment shape — we don't ship init scripts, systemd units, or Helm charts in M1. Operators willing to wire it up themselves can; we're not advertising it.

### Multi-host Kubernetes — deferred

The Helm chart, the StatefulSet for pond nodes, the load balancer integration, the credential-store integration in cluster — all M2/M3 work. Latiq runs on Kubernetes in M2 because that's where enterprises deploy things; M1 ships with Docker Compose because that's enough to prove the architecture and run the demo.

### Configuration

One YAML config per process. Required overrides documented. Defaults work for dev mode.

```yaml
# control-plane.yaml
listen:
  mcp: ":8080"        # not used; pond nodes terminate MCP
  control_grpc: ":9090"
  admin_grpc: ":9091"
storage:
  postgres_url: "..." # or sqlite for dev
oidc:
  enabled: false      # set true and configure issuers in prod
otel:
  endpoint: "..."
```

```yaml
# pond-node.yaml
listen:
  mcp: ":8080"
  internal_grpc: ":9092"
control_plane_endpoint: "..."
data_dir: "/var/lib/latiq/ponds"
extensions:
  - httpfs
  - parquet
  - postgres_scanner
  # ...
credential_store:
  type: vault
  address: "..."
otel:
  endpoint: "..."
```

### Distribution

- **Single statically-linked Rust binary** — `latiq`, served via subcommand (`latiq control-plane`, `latiq pond-node`, `latiq dev`, `latiq catalog register`, etc.)
- **Container images** — same binary, packaged for Docker / Kubernetes
- **Homebrew formula** — for developers on macOS
- **`curl | sh` installer** — for Linux installs without package managers

---

## 13. Observability

Latiq emits **OpenTelemetry** as the unified observability signal. Both the control plane and pond nodes export metrics, traces, and structured logs over OTLP (gRPC or HTTP) to whatever OpenTelemetry collector the operator configures. Latiq ships no built-in dashboards, no alerting rules, no metrics aggregation — those live in the operator's existing observability stack (Grafana, Tempo, Loki, Honeycomb, Datadog, whatever they run).

**Metrics emitted (illustrative, not exhaustive):**

- `latiq.pond.count` — total ponds per node
- `latiq.pond.allocation.duration_ms` — histogram
- `latiq.query.duration_ms` — histogram, with attributes (operation type, pond_id, owning_node)
- `latiq.query.bytes_scanned` — histogram
- `latiq.query.error_count` — counter, with attributes (error code)
- `latiq.proxy_hop.count` — counter (queries that required A→B proxy)
- `latiq.conflict.retry.count` — counter (DuckLake snapshot retries)
- `latiq.duckdb.connection_pool.saturation` — gauge per pond
- `latiq.disk.bytes_used` — gauge per node
- `latiq.audit.write.duration_ms` — histogram
- `latiq.attach_catalog.duration_ms` — histogram
- `latiq.credential.fetch.duration_ms` — histogram

**Traces:** every MCP call is a root span. Proxy hops, control plane lookups, DuckDB query execution, catalog attaches, and audit writes are child spans. Trace context propagates through the proxy hop via gRPC metadata. An operator can follow a single agent's request end-to-end.

**Logs:** structured JSON to stdout, also exported via OTLP. One log line per significant event: agent call received, identity verified, query planned, query executed, audit recorded, error raised.

**What ops teams build on top:**

- Dashboards for capacity, latency, error rates
- Alerts for the obvious things — disk headroom, error rate spikes, query latency degradation, node down
- Cost attribution by identity or tag
- SLO tracking

We provide no opinion on those. Operators bring their existing stack and point the OTLP collector at it.

**Why OpenTelemetry, not Prometheus directly:**

OTel is the broader contract — metrics, traces, and logs in one protocol. It maps to Prometheus on the metrics side (most Prometheus stacks ingest OTLP natively now), to any tracing backend on the traces side, and to any log aggregator on the logs side. Choosing OTel doesn't force the operator into one stack; choosing Prometheus alone would have.

**What's not in this signal:** anything agent-facing. Operational metrics serve operators, not agents. Agents reason about their work through query metadata (`_meta`), the `_latiq` schema, and MCP responses — not through dashboards.

---

## 13a. Security posture

Latiq is a data system that agents write to and read from. That makes it a vector for several classes of attack we need to address explicitly, even when our M1 response is "acknowledge and defer."

### Rate limiting

Per-identity rate limiting at the MCP layer is M1 scope. Configurable token-bucket per identity, applied at the receiving pond node, with sane defaults (e.g., 100 requests/second, 10 concurrent queries per identity). Exceeding the limit returns 429 with a structured `try` action: "Wait N seconds and retry, or request a higher limit from your administrator."

Rate limits are not a quota system (disk quotas are deferred to M2). They exist to bound runaway agent loops — an agent stuck in a retry storm should hit a limit and back off, not consume the cluster.

### Prompt injection (acknowledged, not solved)

Latiq is a vector for prompt injection by virtue of being a data store: Agent A writes data containing instructions intended to manipulate Agent B; Agent B reads that data; if Agent B is a credulous LLM, it follows the injected instructions.

This is an agent-framework concern, not a data-layer concern. Latiq's position:

- We never inject pond data into anything other than `read_query` results. Tool descriptions, error messages, warnings, `_meta` fields, and resource bodies are free of stored data. The injection surface is bounded to where the agent is explicitly asking for stored data.
- We document the vector at `latiq://troubleshooting/prompt-injection` so framework authors know to treat `read_query` results as untrusted input (which they should already).
- We do not scan, sanitize, or filter stored data. Doing so would be unreliable, would impose a content policy on agents we don't want to impose, and would create a false sense of security.

The agent framework is responsible for treating data read from any external source — Latiq included — as adversarial input. We make this expectation explicit in our docs.

### Identity propagation across proxy hops

When pond node A proxies a request to pond node B, A validates the identity and asserts it to B via gRPC metadata on the Flight SQL call (a `latiq-proxy-identity` metadata field, plus a signature using a deployment-wide shared secret). B trusts A's assertion. This is the right trade-off for performance but it does mean a compromised pond node could forge any identity to its peers. Mitigations:

- Internal pond-node-to-pond-node Flight SQL traffic uses mTLS in production (only authenticated pond nodes can participate)
- The proxy-identity assertion is signed with a deployment-wide secret rotated regularly
- Audit log records both the requesting identity and the proxying node, so forged-identity incidents are forensically detectable

If the threat model requires zero trust between pond nodes, the deployment can disable trust mode and require full re-validation at every hop. Configurable trade-off.

### Identity verification weak points

When OIDC is enabled, Latiq verifies JWT signatures at every relevant boundary. We do not implement live revocation — a revoked token works until its expiry. Token TTLs should be short (15 minutes recommended); enterprises that need live revocation should configure their IdP for it and accept the latency cost.

When OIDC is disabled, the `X-Latiq-Agent-Id` header is unverified and trusted. This is appropriate for trusted networks (dev environments, internal-only deployments behind a corporate VPN) and inappropriate for anything else. The mode is admin-toggled and clearly logged.

### Secrets

Latiq stores no agent secrets. The OIDC token lives in the request, the identity is extracted, the token is not persisted. The control plane's Postgres connection string and any IdP signing-key cache live in standard secret-management surfaces (env vars, k8s secrets, vault integration via env vars).

### What's out of scope for M1

- Live token revocation
- Per-pond ACLs and column-level security (M2/M3)
- Data-at-rest encryption beyond what the filesystem provides (M2)
- Network policy enforcement (deployment concern, not product concern)
- WAF/IDS at the LB layer (deployment concern)

We document these gaps explicitly in security docs so enterprises evaluating M1 know what they're getting.

---

## 14. Success criteria for M1

All must hold to ship:

1. **Pond allocation latency.** P50 under 100ms, P99 under 500ms.
2. **Query latency.** P50 under 50ms (local case) / 80ms (proxy case) for small queries.
3. **Concurrent multi-agent correctness.** 10 agents writing concurrently to one pond produces consistent state with correct attribution. Conflict-and-retry verified.
4. **Identity verification.** OIDC verification works against Keycloak, Auth0, and Google (or three equivalents) out of the box.
5. **Framework integration.** At least one major agent framework (LangGraph, CrewAI, AutoGen, LlamaIndex) has a working Latiq integration consuming the MCP surface directly through its built-in MCP client. Without an M1 SDK, this validates that the MCP surface alone is rich enough for real framework use.
6. **The demo runs end-to-end.** Multi-agent collaboration in a single pond — agents collaborate on the Docker Compose deployment, attach an admin-registered catalog, query across local and external tables. Recorded and shareable.
7. **The docs work.** Developer goes from "I've heard of Latiq" to "agents reading and writing to a pond" in under 30 minutes — single binary install, `latiq dev`, walk through allocating and querying.

---

## 15. Open questions for engineering

Deferred to implementation:

1. **DuckLake catalog choice per pond:** SQLite per pond is the default, but DuckDB-as-catalog has performance advantages. Benchmark before locking in.
2. **Connection pool sizing:** default 4-8 DuckDB connections per pond is a guess. Tune from observed workloads.
3. **JSON streaming chunk size:** trade-off between time-to-first-byte and total throughput. Default 1000 rows per chunk, revisit.
4. **Heartbeat interval and node-health timeout:** default 10s heartbeat, 30s timeout, revisit under load.
5. **Error semantics mid-stream:** when a stream breaks (timeout, node crash, drop), how does the agent learn? Deferred until we hit it in practice.
6. **Disk-full behavior at allocation time:** deferred to M2 quota work.
7. **Inline result cap defaults:** 10K rows / 1MB is a starting point. Adjust based on real agent workload profiles.
8. **CLI ↔ Admin API auth in OIDC-disabled mode:** local token in `~/.latiq/credentials` vs unix-socket trust vs env var. Pick the simplest correct thing.

---

## 16. Four principles to defend

The implementation will face pressure. These four principles are the test:

**The agent is the customer.** Features that serve human admins go in the Admin API / CLI surface. Features that serve agents go in the MCP surface. The two never overlap.

**Hard separation between MCP and Admin surfaces.** An agent cannot register a catalog, manage credentials, or configure nodes — those are operator concerns. An admin uses the CLI; agents use MCP. Different transports, different auth, different audit trails. Don't blur the line "for convenience."

**One pond, one node.** Cross-pond joins, distributed query, multi-node ponds — all tar pits. Single-node ponds with cross-catalog joins inside them are what makes M1 shippable. Defend the rule.

**Make it boring.** M1 is not the place to be clever. Predictable behavior, clear errors, good defaults. The magic comes in M2 and M3. M1's job is to be the floor those build on.

---

## 17. What this doesn't cover

Out of scope for this document:

- M2 design (Python SDK with Flight SQL, streaming ingestion — Kafka, S3 bulk, CDC, Kubernetes deployment)
- M3 design (full federation through expanded extension support, governance UI, advanced ACLs)
- DataFusion parallel-track design (post-Alpha)
- GTM plan, OSS launch strategy
- Detailed protobuf schemas for internal gRPC and Admin API
- Full MCP tool descriptions in production form
- CLI man pages and admin runbook

These follow once M1 is real.
