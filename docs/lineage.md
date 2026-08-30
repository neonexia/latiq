# Latiq — Lineage (OpenLineage events, per pond, opt-in)

*Design note, describing **what shipped**. Pairs with [`identity.md`](identity.md)
— lineage is where the identity chain that authorization deliberately does not
model gets recorded.*

---

## Why

An agent that reads a table needs to answer "where did this come from, and who
put it here?" — and it needs to answer it **itself**, mid-run, not by asking a
human to open a web UI.

That is the requirement that shapes everything here: **Latiq often runs fully
contained inside the agent's environment**, with no network path to a lineage
backend. Lineage that exists only as an emitted event is, in that deployment,
lineage nobody can read. So the local trail is the *default* and the external
backend is the *option* — not the other way round.

We emit **[OpenLineage](https://openlineage.io)** events (core spec `2-0-2`)
because it is the existing standard for exactly this, and inventing a schema
here would repeat the mistake identity.md backed out of.

---

## Not the access trail

Two things that look similar and are not:

| | `latiq::access` trail | Lineage |
|---|---|---|
| Reader | operators, in their log stack | **agents**, mid-run |
| Question | "who did what, when?" | "where did this data come from?" |
| Shape | one structured log line per op | dataset-level in/out graph |
| Store | none — it's a log trace | JSONL files in the pond's own directory |
| Scope | always on, every op | **opt-in per pond**, queries only |

The access trail is unchanged: a structured trace, no audit table, no audit RPC,
as `product.md` describes. Lineage does not replace it and does not make it a
store — different reader, different question.

---

## What shipped

**Opt in at allocation, off by default.** `allocate_pond(..., lineage=true)`.
It is fixed for the pond's lifetime: an existing pond cannot be switched on, and
`describe_pond` reports the flag so a caller can tell whether `get_lineage` will
have anything to say. A pond that did not opt in pays nothing — no event, no
formatting, no directory lookup, no writer.

**One event pair per query.** A `START` and a terminal `COMPLETE` / `FAIL` /
`ABORT` (a cancelled or timed-out query aborts; a consumer that cannot tell that
from a failure cannot tell Ctrl-C from a bug). The `START` is stamped with when
the operation *began*, not with now, so a consumer deriving duration from the
pair gets the real number. Reads emit as well as writes: "which agent read this
before the bad decision" is a question people genuinely ask, and a write-only
graph answers who produced a dataset but never who consumed it.

**Recorded on the node that ran it.** A forwarded operation records on the owner,
exactly as the access trail does — emitting on both sides would duplicate the run
under two pond-local snapshot ids, only one of them real.

**Inputs and outputs from DuckDB's bound plan**, kept apart: an
`INSERT INTO a SELECT FROM b` that reported one flat list would make `b` look
written. Table-level, not column-level. A statement whose plan did not resolve
carries **no** datasets rather than guessed ones — an invented input is worse
than a missing one.

**Versions are native DuckLake snapshots**, on the standard
`datasetVersion` facet. A read reports the exact snapshot it observed; a write
reports the snapshot it committed. This is where being pure DuckLake pays off:
the version is the engine's, not something we maintain.

**Identity, verified or claimed.** The `latiq_identity` facet carries `subject`,
`issuer`, `verified`, and the claimed leaf `agentId` stamped
`agentIdVerified: false`. A reader can always tell provenance from assertion.

**Files in the pond's own `lineage/` directory.** Batched JSONL, written to a
temp file, `fsync`ed, and renamed into place, so a reader globbing `*.jsonl`
sees either nothing or a whole batch — never a torn record. Names carry a
zero-padded unix-millis prefix, so sorting names sorts by time. The buffer is
bounded (10 000 events per pond) and a failed batch is retried; nothing here can
fail a query, and every failure below `record()` is a `warn!`.

**`get_lineage(pond, limit, since?, before?)`** on the MCP surface, forwarded to
the pond's owning node. Newest first, events returned **verbatim** — not
round-tripped through our own struct, so an event written by a different build
of Latiq keeps every field it was recorded with. `since` is an inclusive lower
bound, `before` an exclusive upper one, and a page is never cut in the middle of
one `eventTime`, so walking `before` backwards visits every event exactly once.
Malformed lines and unreadable batch files are **counted and reported**: a short
answer must never look like a complete one.

**An optional OpenLineage HTTP backend.** `--lineage-backend-url` /
`LATIQ_LINEAGE_BACKEND_URL` on `latiq node add` — the full endpoint to POST to,
e.g. `http://marquez:5000/api/v1/lineage`, validated once at startup so a typo
stops the node instead of warning on every query forever. When set, every event
goes to both. **No credentials are sent**; a backend that needs auth is a later,
explicit decision rather than a scheme invented here.

Three properties of the sink, in the order they matter:

1. **It can never fail, slow or block a query.** Submitting is one `try_send`
   onto a bounded queue; every POST happens on a background task nobody awaits,
   modelled on the node's heartbeat loop (tolerates a dead endpoint, recovers
   without supervision). A dead, hung, TLS-failing, DNS-failing or 500-ing
   backend is invisible to the query path. A full queue **drops** with a
   warning; it never grows and it never waits.
2. **The posted bytes are the stored bytes.** An event is serialized exactly
   once and that same string goes to the file buffer and to the sink, so what a
   backend receives is byte-identical to what the pond's files hold and to what
   `get_lineage` returns. If the wire form and the stored form could drift,
   "OpenLineage compliant" would mean nothing.
3. **Failure logging is rate-limited to the transitions.** A node whose backend
   is down posts on every query; without this it would drown the log it shares
   with the access trail.

**A flush on shutdown.** The writer flushes on `Drop`, so a graceful teardown
loses nothing — but SIGTERM ends the process rather than dropping anything, and
up to one batch per pond would go with it. That batch is the last few queries
before the node went down, which is exactly the window an incident asks about,
so the node catches SIGTERM/Ctrl-C and flushes. This is the node's *only*
shutdown work today; the full sequence (stop accepting → abort in-flight →
checkpoint → deregister) is still a target, and this is where it goes.

---

## The run-scope question

Two constraints that look contradictory:

- The run id must be **Latiq-minted** — it is provenance, and provenance a
  caller can fabricate is worthless.
- Only the **agent environment** knows what a workflow *is*.

OpenLineage already separates these, because it has a run **and** a parent-run
facet:

| OpenLineage concept | Latiq mapping | Trust |
|---|---|---|
| **Job** | the pond + the op + the target dataset — the stable, recurring thing | derived |
| **Run** | **one query execution**, id minted by Latiq | **ours, unforgeable** |
| **Parent run** | the caller's workflow / step, opaque | **claimed, never verified** |

Same discipline as identity: verify what's verifiable, record the rest as
claimed, and never let a claim carry authority.

**The parent facet is not emitted yet**, and that absence is deliberate rather
than an oversight: **no transport carries a workflow id today**, and
identity-shaped context arrives in the transport and never in a tool or RPC
argument (invariant 9). Inventing a SQL-level or argument-level workflow id
would be a design violation. The UUIDv5 derivation is written and tested; it
stays unused until a transport field exists to fill it.

**Explicitly rejected: scoping the run to the pond's lifetime.** A pond can
outlive a workflow or be shared by several. It is a good *job* name and a wrong
*run* boundary.

---

## Cut: the SQL catalog over the events

An earlier draft of this note designed a per-pond `lineage.duckdb` sidecar
holding a view over the event files, attached `READ_ONLY` into the agent's
session so an agent could write `SELECT * FROM lineage.events WHERE …`. **None
of that shipped, and it was cut deliberately** — publish compliant OpenLineage
first, optimise access later. It is recorded here so the next reader knows it
was a decision and not an omission.

What the cut removes: the sidecar catalog, the view, the seeded sentinel event a
`CREATE VIEW` over an empty glob needs, the pinned column list (`SELECT *` over a
glob re-binds column count, order *and types* per query), the read-only attach,
an extra attached database per open pond — and any need to amend invariant 6,
since nothing Latiq-owned goes into the pond catalog either way.

**The accepted cost, plainly.** An agent that wants to filter or aggregate its
lineage has two options, and neither is a view:

- pull events through `get_lineage` and do it itself, or
- run its own `read_json_auto` over the pond's lineage directory — which
  `get_lineage` hands back as `lineage_dir` for exactly this reason. The catch
  worth knowing up front: the facets map differs per event, so DuckDB's inferred
  struct will unify across whatever files exist and the schema can shift between
  two runs of the same query.

A view over these same files is purely additive. Nothing here forecloses it.

---

## What lineage does not promise

**An agent can rewrite its own provenance.** `write_query` executes arbitrary
SQL by design (#53), and DuckDB SQL can write files — so an agent can today
forge events, overwrite real ones, or delete the directory, and it can read the
path out of `get_lineage`'s own response.
**[#79](https://github.com/neonexia/latiq/issues/79)** tracks the per-pond
sandbox that fixes it, and is a prerequisite for beta rather than for this
slice. M1 assumes trusted agents, consistent with the rest of the posture. The
claim "an agent cannot rewrite its own provenance" is **not true today** and
must not be written down as though it were.

**Dropping a pond destroys its provenance.** Lineage is reaped with the pond —
no TTL, no reaper, no orphan store, no retention policy we have no basis to
choose yet. The cost is real and it is the obvious one: the post-mortem history
someone wants is gone at exactly the moment they think to ask for it. **The HTTP
sink is the durability answer**: an operator who needs lineage to outlive its
pond configures a backend, and durability becomes that backend's job rather than
ours. We are not a lineage archive.

**Per-pond, so no cross-pond questions.** "What read this dataset anywhere?" has
no answer from inside Latiq. That is the price of making isolation structural
while authorization does not exist: a node-wide store would show every pond's
provenance to every agent, and any filtering would be advisory rather than
enforced. If it is ever wanted it arrives *with* authorization, as a filtered
node-wide read, not by widening a pond's trail.

**Table-level, not column-level.** Column-level lineage is a stated OpenLineage
facet and much harder. Getting tables approximately right is worth far more now
than getting columns perfectly right later.

---

## Where it lives

| Piece | Code |
|---|---|
| Events + facets, the file writer, the reader, the HTTP sink | `crates/latiq-lineage` |
| The emitter (one pair per op, on the local path only) | `crates/latiq-agent-core/src/lineage.rs` |
| Inputs/outputs from the bound plan | `crates/latiq-engine-duckdb` |
| `get_lineage` (neutral) and the writer registry | `crates/latiq-agent-core/src/ops.rs` |
| The `get_lineage` MCP tool | `crates/latiq-mcp` |
| `--lineage-backend-url`, the sink, the shutdown flush | `crates/latiq-pond-node`, `crates/latiq/src/main.rs` |
| Vendored OpenLineage + Latiq facet schemas | `crates/latiq-lineage/spec/` |

`latiq-lineage` is protocol-neutral like `latiq-agent-core` that depends on it
(invariant 5). The HTTP sink is the one deliberate exception — it is HTTP by
definition — and it sits behind the `http-sink` Cargo feature that only
`latiq-pond-node` enables. With the feature off the crate does not even depend
on `reqwest`, so the neutrality of the rest is enforced by Cargo rather than by
a reviewer noticing. What the writer and the core see is `EventSink`, a trait
over `&str` with no transport in it.
