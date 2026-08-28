# Latiq — Lineage (OpenLineage events, agent-queryable by default)

*Design note. Pairs with [`identity.md`](identity.md) — lineage is where the
identity chain that authorization deliberately does **not** model gets recorded.
Nothing below is implemented yet; the shape is settled (see
[Decisions](#decisions)).*

---

## Why

An agent that reads a table needs to answer "where did this come from, and who
put it here?" — and it needs to answer it **itself**, mid-run, not by asking a
human to open a web UI.

That is the requirement that shapes everything here: **Latiq often runs fully
contained inside the agent's environment**, with no network path to a lineage
backend. Lineage that exists only as an emitted event is, in that deployment,
lineage nobody can read. So the local, queryable store is the *default* and the
external backend is the *option* — not the other way round.

We emit **[OpenLineage](https://openlineage.io)** events because it is the
existing standard for exactly this, and inventing a schema here would repeat the
mistake identity.md just backed out of.

---

## Not the access trail

Two things that look similar and are not:

| | `latiq::access` trail (**today**) | Lineage (**this note**) |
|---|---|---|
| Reader | operators, in their log stack | **agents**, in SQL, mid-run |
| Question | "who did what, when?" | "where did this data come from?" |
| Shape | one structured log line per op | dataset-level in/out graph |
| Store | none — it's a log trace | a queryable sidecar catalog |

The access trail stays exactly as it is: a structured trace, no audit table, no
audit RPC, as `product.md` describes. Lineage does not replace it and does not
make it a store — different reader, different question.

---

## Storage: a sidecar catalog, never a table in the pond

The obvious design is a lineage table inside the pond, so agents can just query
it. **It is the wrong design**, and not primarily because of invariant 6.

Writing lineage into the pond means every read becomes a write, and that costs
three specific things:

1. **It destroys attribution.** A logged `SELECT` produces a DuckLake
   **snapshot**. `pond.snapshots()` — the thing attribution is built on — fills
   with our bookkeeping instead of the agent's actual data changes. Lineage would
   make history unreadable, which is the opposite of its purpose.
2. **It re-serializes reads.** Writes take the pond's writer mutex. Reads
   currently take a pooled cloned connection *precisely* so they don't serialize
   (`crates/latiq-engine-duckdb/src/duck_engine.rs`). Routing a lineage write
   into the read path puts every read back behind that mutex and undoes the
   read-concurrency work.
3. **It breaks invariant 6** — `_latiq` objects visible in the agent's
   `SHOW TABLES`, in a catalog we promise is pure DuckLake.

**Instead:** Latiq keeps a **separate, node-owned DuckDB/DuckLake database** and
`ATTACH`es it **read-only** into the agent's session as `lineage`. The agent gets
exactly the ergonomics it wanted —

```sql
SELECT * FROM lineage.events WHERE pond = 'pond-8812' ORDER BY event_time DESC;
```

— with zero pond snapshots, no contention with the writer mutex, and writes that
can be batched and asynchronous because they are off the query's critical path.
Invariant 6 stays literally true: nothing Latiq-owned lives in the *pond's*
catalog. **Invariant 6's wording should be amended to say this explicitly**,
otherwise "agents query lineage in SQL" reads like a violation to the next person.

Read-only is load-bearing: an agent must not be able to rewrite its own
provenance.

### Sinks

- **Local sidecar** — always on. The default, and the only one that works in a
  contained environment.
- **OpenLineage HTTP backend** (Marquez or any compatible receiver) —
  configurable, additive. When set, events go to both.

Emission must never fail a query. A sink that is down drops events with a
`WARN`; it does not propagate.

---

## The run-scope question

The awkward part, and it resolves into OpenLineage's own model rather than a
choice we have to make.

Two constraints that look contradictory:

- The run id must be **Latiq-minted** — it is provenance, and provenance a caller
  can fabricate is worthless.
- Only the **agent environment** knows what a workflow *is*: long-running or
  short, a graph of steps, each step an agent launched many times.

OpenLineage already separates these, because it has a run **and** a
**parent-run facet**:

| OpenLineage concept | Latiq mapping | Trust |
|---|---|---|
| **Job** | the pond + the target dataset — the stable, recurring thing | derived |
| **Run** | **one query execution**, id minted by Latiq | **ours, unforgeable** |
| **Parent run** | the caller's `workflow_id` / `step_id`, opaque | **claimed, never verified** |

So we guarantee what we can guarantee, and we record what only the environment
knows — labelled as claimed, never load-bearing for authorization. When the
environment supplies a parent, the graph assembles correctly across hundreds of
agents. When it doesn't — today's single and loop agents — lineage is still
complete per query, just flat.

Same discipline as identity: **verify what's verifiable, record the rest as
claimed, and never let a claim carry authority.** It also means we do not have to
guess the shape of workflows now, and nothing needs retrofitting when graph
agents arrive.

**Explicitly rejected: scoping the run to the pond's lifetime.** A pond can
outlive a workflow or be shared by several. It is a good *job* name and a wrong
*run* boundary.

---

## What an event carries

Sketch, not a schema:

- **Run** — Latiq run id, event type (`START` / `COMPLETE` / `FAIL`), event time,
  duration.
- **Job** — pond id, pond name, target dataset.
- **Inputs / outputs** — datasets read and written, with the DuckLake
  **snapshot id** as the dataset version. This is where being pure DuckLake pays
  off: the version is native, not something we maintain.
- **Identity** (from [`identity.md`](identity.md)) — `subject`, `issuer`,
  `verified`, and the claimed leaf `agent_id`. **`verified` must be carried into
  the event**, so a reader can tell provenance from assertion.
- **Parent run** — claimed workflow / step labels, if supplied.
- **SQL** — redacted the same way the access trail redacts it (literals
  replaced). The existing `redact_sql` applies.
- **Trace id** — the existing `latiq-trace-id`, so a lineage row joins to the
  logs.

Extracting inputs and outputs is the real work: DuckDB's `EXPLAIN` /
`json_serialize_sql` can surface referenced tables, and we already run an explain
path. Getting it *approximately* right (tables touched, not column-level) is
worth far more than getting it perfectly right later.

---

## Decisions

Settled before implementation. Recorded here because an unanswered question in a
design note becomes an unexamined assumption in the code.

**One sidecar per pond.** Not one per node. The deciding argument is the one this
note anticipated: authorization does not exist yet, so a node-wide sidecar would
show every pond's provenance to every agent, and any filtering would be advisory
rather than enforced. Per-pond makes the isolation structural — an agent attached
to its pond can only see its own lineage. The costs are real and accepted: many
small files, and no cross-pond question ("what read this dataset anywhere?") until
a node-wide view is added deliberately, behind authorization.

**Reads emit as well as writes.** "Which agent read this before the bad decision"
is a question people genuinely ask, and reads are where agents spend most of their
time — a write-only graph answers who *produced* a dataset but never who
*consumed* it. Emission is asynchronous and off the query's critical path, so the
cost is disk and volume rather than latency. If volume turns out to hurt, the
lever is a sampling or opt-out setting, not a redesign.

**Lineage is reaped with the pond.** No TTL, no reaper, no unbounded growth — the
sidecar's lifecycle is the pond's. **The cost is stated plainly: dropping a pond
destroys its provenance, which is exactly the post-mortem history someone will
want afterwards.** The escape hatch is the HTTP sink: an operator who needs
lineage to outlive its pond configures an OpenLineage backend, and durability
becomes that backend's job rather than ours. That is the right split — we are not
a lineage archive, and building one would mean a reaper, an orphan store, and a
retention policy we have no basis to choose yet.

---

## Still open

- **Column-level lineage** — a stated OpenLineage facet, and much harder than
  table-level. Table-level first; column-level only if agents ask for it.
- **MCP surface** — is lineage just the attached `lineage` catalog, or also a
  tool/resource? The attached catalog needs no new tool, which argues for starting
  there and adding a tool only if agents do not find it.
- **Cross-pond lineage** — deliberately impossible under the per-pond decision
  above. If it is ever wanted, it arrives with authorization, as a filtered
  node-wide view rather than by widening the sidecar.
