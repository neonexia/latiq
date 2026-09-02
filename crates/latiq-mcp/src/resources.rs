// Copyright 2026 Neonexia
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Static MCP resources (`latiq://…` guidance/recipes/troubleshooting) and the
//! prompt SOPs. These exist so a frontier agent can read patterns directly and
//! so error `see` links resolve to a real body. Prose is written for LLMs.
use rmcp::model::{
    AnnotateAble, GetPromptResult, Prompt, PromptMessage, PromptMessageRole, RawResource,
    ReadResourceResult, Resource, ResourceContents,
};
use serde_json::{Map, Value};

struct Res {
    uri: &'static str,
    name: &'static str,
    desc: &'static str,
    body: &'static str,
}

const RESOURCES: &[Res] = &[
    Res {
        uri: "latiq://guidance",
        name: "Latiq guidance",
        desc: "Top-level guidance for working in a pond",
        body: "# Working in a Latiq pond\n\n\
- **SQL dialect:** Latiq speaks ANSI SQL on DuckDB. DuckDB-specific functions work but reduce portability — prefer ANSI when other agents will read your code.\n\
- **Self-describing schemas:** when you CREATE TABLE, add column and table COMMENTs. Other agents discovering your pond rely on them. See latiq://recipes/schema-design.\n\
- **Attribution:** your writes are tagged with your agent identity. To see who wrote what: `SELECT author, commit_message, commit_extra_info FROM ducklake_snapshots('<pond>')`. `author` is the identity; `commit_extra_info` carries the evidence for it (issuer/subject when the caller was verified) — read BOTH, because an unverified caller can claim any author.\n\
- **Latiq owns the transaction:** send plain statements — multi-statement SQL is fine, but never `BEGIN`/`COMMIT`/`ROLLBACK`/`START TRANSACTION`. Latiq commits your write itself and records the author just before committing; your own `COMMIT` ends that transaction first, so the change lands in history with NO author.\n\
- **Discover:** `SHOW TABLES` lists tables (and `information_schema.columns` for columns); list_ponds + describe_pond find existing work to join.\n\
- **External data:** to bring outside data in, use list_datasets + load_dataset (curated public files), or list_catalogs → describe_catalog → pull_catalog (external databases/lakehouses like iceberg — you pull a subset into the pond, then work there). See latiq://recipes/external-data.\n\
- **Identity:** who you are arrives in the TRANSPORT, never in a tool argument — no tool takes an agent id, so don't try to set one. The `Authorization: Bearer` token is the verified principal (`subject` + `issuer`); the `latiq-agent-id` header is a CLAIM and carries no authority. On a deployment with no issuer configured nothing is verified: you get `verified: false` and a null `subject` wherever identity is reported. Read that as \"nobody proved it\", not \"nobody did it\".\n\
- **Provenance:** a pond allocated with `lineage: true` records an OpenLineage event pair for every query; read it with get_lineage (newest first), and check the `lineage` flag in describe_pond to know whether a pond has it. It is chosen at allocation and CANNOT be turned on later, so ask for it when you allocate. It is a working record, not tamper-proof evidence — the events are files in the pond, and dropping the pond destroys them. See latiq://recipes/lineage.\n\
- **Large results:** results are capped (~10k rows). Narrow with WHERE/LIMIT, aggregate server-side, or materialize with CREATE TABLE AS SELECT. See latiq://recipes/large-results.\n\
- **Plan first:** call explain_query before an expensive query to estimate cost, then refine.\n\
- **Collaboration:** multiple agents in one pond is the common case. Writes serialize; conflicts auto-retry. See latiq://troubleshooting/conflicts.",
    },
    Res {
        uri: "latiq://dialect",
        name: "SQL dialect",
        desc: "The SQL contract Latiq exposes",
        body: "# Latiq SQL dialect\n\n\
Latiq runs ANSI SQL on a DuckDB engine over DuckLake storage.\n\n\
- **read_query** accepts SELECT and read-only metadata (SHOW/DESCRIBE). Writes are rejected — use write_query.\n\
- **write_query** accepts INSERT/UPDATE/DELETE and DDL (CREATE/DROP/ALTER, CREATE TABLE AS SELECT).\n\
- **Transaction control is Latiq's.** Don't send `BEGIN`/`COMMIT`/`ROLLBACK`/`START TRANSACTION`: read_query rejects them, and in write_query they cut short the transaction Latiq attributes your write in (nothing rejects them there — it silently costs you the author). Several plain statements in one call are fine; they commit together as one snapshot.\n\
- Your tables live in the pond's default schema; query them directly (you can also `CREATE SCHEMA` for more).\n\
- Snapshots/history/attribution are native DuckLake — `SELECT snapshot_id, author, commit_message, commit_extra_info FROM ducklake_snapshots('<pond>')` (`commit_extra_info` is where verified-vs-claimed shows up). List tables/columns with `SHOW TABLES` / `information_schema`.\n\
- Prefer ANSI constructs; DuckDB extensions are tolerated but reduce portability.",
    },
    Res {
        uri: "latiq://recipes/schema-design",
        name: "Recipe: schema design",
        desc: "Authoring tables other agents can collaborate on",
        body: "# Recipe — schema design for collaboration\n\n\
**When:** you're the first agent creating tables in a pond.\n\n\
**Pattern:**\n```sql\nCREATE TABLE events (\n  id INTEGER,           -- event primary key\n  severity VARCHAR,     -- one of: low, medium, high, critical\n  occurred_at TIMESTAMP -- event time in UTC\n);\n```\n\
**Why it works:** comments are visible via `SHOW TABLES` / `information_schema.columns`, so the next agent understands your schema without asking.\n\
**Watch for:** vague table/column names; missing comments; types that don't match the domain.",
    },
    Res {
        uri: "latiq://recipes/large-results",
        name: "Recipe: large results",
        desc: "Handling results larger than the inline cap",
        body: "# Recipe — large results\n\n\
**When:** a read_query returns `result_cap_exceeded` or you expect many rows.\n\n\
**The pattern (pick one):**\n\
1. **Narrow:** add a WHERE on a selective column and/or LIMIT.\n\
2. **Aggregate server-side:** `SELECT severity, count(*) FROM events GROUP BY severity`.\n\
3. **Materialize:** `CREATE TABLE hot AS SELECT * FROM events WHERE severity='critical'` then query the smaller table.\n\
**Why:** the inline cap keeps your context bounded; the M2 SDK will stream large sets.",
    },
    Res {
        uri: "latiq://recipes/data-ingestion-m1",
        name: "Recipe: data ingestion",
        desc: "Loading data into a pond with SQL",
        body: "# Recipe — ingest data (M1)\n\n\
**Public files (no credentials):** read CSV/Parquet/JSON by URL directly in write_query:\n```sql\nCREATE TABLE raw AS SELECT * FROM read_csv('https://example.com/data.csv');\nINSERT INTO raw SELECT * FROM 's3://public-bucket/more.parquet';\n```\n\
For curated/registered sources (incl. external lakehouses like iceberg) use list_datasets/load_dataset and list_catalogs -> describe_catalog -> pull_catalog — see latiq://recipes/external-data. Credentials for those ride in at pull time and are never stored.\n\
**Own data:** `INSERT INTO t VALUES (...)` or `CREATE TABLE t AS SELECT ...`.",
    },
    Res {
        uri: "latiq://recipes/external-data",
        name: "Recipe: external data (datasets & catalogs)",
        desc: "Bring outside data into a pond — curated files or external catalogs",
        body: "# Recipe — bring external data into a pond\n\n\
Latiq has two paths. Everything ends up as tables IN your pond — external catalogs are never queried live.\n\n\
## Datasets — curated public files (copy in)\n\
A dataset loads into a SCHEMA named after it — query its tables as `<dataset>.<table>`.\n\
```\nlist_datasets {query?}            # discover; e.g. query='#sample' or 'tpch'\nload_dataset {pond, dataset:'tpch'}   # -> schema 'tpch'; returns schema-qualified tables\nread_query {pond, sql:'SELECT count(*) FROM tpch.orders'}\n```\n\n\
## Catalogs — external databases/lakehouses (pull a subset in)\n\
An operator registers a catalog (iceberg today). You discover its tables, then pull what you need:\n\
```\nlist_catalogs {query?}                                  # find a catalog, e.g. 'lake'\ndescribe_catalog {pond, catalog, set:{token:'<bearer>'}} # list its tables (transient attach)\npull_catalog {pond, catalog, query:'CREATE TABLE us AS SELECT id,total FROM lake.sales.orders WHERE region=''us''', set:{token:'<bearer>'}}\nread_query {pond, sql:'SELECT * FROM us LIMIT 10'}\n```\n\
**Credentials** ride in via `set` (e.g. `{token: '<bearer>'}`) on describe/pull only — used once, never stored. \
Write the pull `query` as a CREATE TABLE that names the catalog; DuckDB downloads only the columns/rows you SELECT. \
**Don't** try to query `lake.…` outside a pull — attach is transient. **Do** describe_catalog first so you SELECT real table names.",
    },
    Res {
        uri: "latiq://recipes/attribution-lookup",
        name: "Recipe: attribution lookup",
        desc: "Who wrote what in a pond",
        body: "# Recipe — attribution lookup\n\n\
Every write is tagged with the writing agent's identity (native DuckLake commit metadata).\n```sql\nSELECT snapshot_id, author, commit_message, commit_extra_info FROM ducklake_snapshots('<pond>') ORDER BY snapshot_id DESC;\n```\n\
Use this to coordinate: see who created a table before extending it.\n\
**How a write loses its author:** Latiq brackets your statement in its own transaction and records the author immediately before committing. SQL that does its own `COMMIT` (or `BEGIN`/`ROLLBACK`/`START TRANSACTION`) ends that bracket first, so the snapshot appears here with no author and nobody can tell who made the change. Nothing stops you — just send plain statements.\n\
**Always read `commit_extra_info` alongside `author`.** `author` alone cannot tell a VERIFIED writer from one merely claiming that name — the evidence (issuer/subject, and whether the identity was verified) lives in `commit_extra_info`. Where the deployment configures no issuer there is nothing to verify, so every author is a claim and no subject is recorded; that is the default, not a fault.\n\
**You cannot choose the identity you write under.** It comes from the transport — the bearer token (verified `subject`/`issuer`) and the `latiq-agent-id` header (a claimed leaf) — and no tool takes it as an argument.",
    },
    Res {
        uri: "latiq://recipes/lineage",
        name: "Recipe: lineage lookup",
        desc: "Where a table came from — the pond's OpenLineage trail",
        body: "# Recipe — lineage lookup\n\n\
**When:** you need the provenance of a table: what a run read, what it wrote, who ran it, and which snapshot it produced.\n\n\
**First, the pond must be recording.** Lineage is opt-in at allocation and FIXED for the pond's lifetime:\n\
```\nallocate_pond {name:'audited', lineage:true}\n```\n\
`describe_pond` reports `lineage`. Calling get_lineage on a pond without it returns an error, not an empty list — 'we were not recording' and 'nothing happened' are different answers, and only one of them means the data appeared from nowhere.\n\n\
**Read it, newest first:**\n\
```\nget_lineage {pond:'audited'}                 # the newest 50 events\n```\n\
**Page backwards** while `truncated` is true, using the OLDEST `eventTime` you received as the next `before` (exclusive — it never repeats or skips an event, because a page is cut on a timestamp boundary; the one exception is a FULL page whose events all share a single `eventTime`, which is returned uncut, so raise `limit` if a pond records more than that in one millisecond):\n\
```\nget_lineage {pond:'audited', limit:50}\nget_lineage {pond:'audited', limit:50, before:'<oldest eventTime from the last page>'}\n... until truncated is false\n```\n\
**Catch up** instead with `since`, which is the opposite bound and is INCLUSIVE — pass the newest `eventTime` you already have and that one event comes back with anything newer:\n\
```\nget_lineage {pond:'audited', since:'2026-08-14T10:00:00Z'}\n```\n\
`malformed_lines` and `unreadable_files` are non-zero when the page is missing events that were recorded — a short answer never pretends to be a complete one.\n\
Each operation records a START and a terminal (COMPLETE / FAIL / ABORT) event sharing one `run.runId`. \
Standard facets carry the SQL shape (`job.facets.sql`, literals redacted), the engine, the error message on a failure, each dataset's DuckLake snapshot (`inputs[].facets.version`), and its columns (`inputs[]`/`outputs[].facets.schema.fields` — name + the engine's own type). A dataset OUTSIDE the pond (an s3 object, a Parquet file, another catalog) carries no `schema` facet: those columns are not ours to state, and absent means unknown rather than empty. \
Latiq's own facets carry the caller (`run.facets.latiq_identity`), the pond (`job.facets.latiq_pond`), and the outcome + duration (`run.facets.latiq_query`, on the terminal event only).\n\
The events are canonical OpenLineage 2-0-2: hand them to any OpenLineage consumer unchanged.\n\n\
**Reading the identity facet.** Verified caller:\n\
```json\n{ \"agentId\": \"analytics-agent-7\", \"agentIdVerified\": false,\n  \"issuer\": \"https://idp.example/realms/latiq\",\n  \"subject\": \"d6d75715-…\", \"verified\": true }\n```\n\
`subject`/`issuer` come from the caller's bearer token; `agentId` is the claimed leaf header and `agentIdVerified` says whether that leaf itself was backed by the token. Where no issuer is configured there is nothing to verify, so `verified` is false and `subject` and `issuer` are **null** while `agentId` still names the caller — a null subject means the deployment proved nothing, not that nobody ran the query. Check `verified` before you attribute anything to `subject`.\n\n\
**Complements attribution, not a replacement.** `ducklake_snapshots('<pond>')` says who committed a snapshot; lineage says which run produced it and what that run read. See latiq://recipes/attribution-lookup.\n\n\
**To filter or aggregate the WHOLE trail** rather than page it, query the files directly — get_lineage returns their directory as `lineage_dir`:\n\
```sql\nSELECT job.name, run.facets.latiq_query.outcome, count(*)\nFROM read_json_auto('<lineage_dir>/*.jsonl')\nGROUP BY 1, 2;\n```\n\
**Watch for:** the facets present differ per event, so DuckDB's inferred struct type can shift between queries — SELECT the fields you need, and don't rely on a stable schema across runs. Only `*.jsonl` files are complete; a `.tmp-` file is a batch still being written.\n\n\
**What lineage does NOT promise.**\n\
- It is a record, not proof. The events are ordinary files under `lineage_dir` in the pond, and write_query runs arbitrary SQL — so anything that can write in the pond can also add, overwrite or remove events. Use it to understand what happened; do not present it as evidence nobody could have altered.\n\
- It starts at allocation. Nothing is recorded for a pond that was allocated without `lineage`, and it cannot be switched on afterwards — you would have to redo the work in a new pond.\n\
- It dies with the pond. drop_pond removes the lineage directory along with the data; read what you need first.\n\
- It covers queries, not intent. The SQL shape is recorded with literals redacted, so the trail shows what ran, not the values it ran on.",
    },
    Res {
        uri: "latiq://troubleshooting",
        name: "Troubleshooting index",
        desc: "Problem-keyed recovery guides",
        body: "# Troubleshooting\n\n\
- latiq://troubleshooting/pond-not-found — the pond id/name doesn't resolve.\n\
- latiq://troubleshooting/pond-unavailable — the pond exists but no node is serving it, or allocate_pond could not create one on the node it was assigned to.\n\
- latiq://troubleshooting/large-results — results exceeded the inline cap.\n\
- latiq://troubleshooting/timeouts — a query ran too long.\n\
- latiq://troubleshooting/conflicts — concurrent writes conflicted.\n\
- latiq://troubleshooting/read-only-violation — a write was sent to read_query.",
    },
    Res {
        uri: "latiq://troubleshooting/pond-not-found",
        name: "Troubleshooting: pond not found",
        desc: "Recover from a missing pond",
        body: "# Pond not found\n\n\
The pond id or name doesn't exist in this deployment.\n\
- Call **list_ponds** to see what exists (names + ids).\n\
- Call **allocate_pond** to create a new one.\n\
- Check spelling; pond refs accept either the UUID or the human name.",
    },
    Res {
        uri: "latiq://troubleshooting/pond-unavailable",
        name: "Troubleshooting: pond unavailable",
        desc: "The pond exists but no node is serving it",
        body: "# Pond unavailable\n\n\
The pond is still in the registry — its name resolves and list_ponds shows it — but the node that owns it is no longer registered, so nothing can reach its files. This is NOT the same as a missing pond, and it is not something you can fix from here:\n\
- **Do not** allocate a replacement under the same intent and assume the data moved. It did not; the old pond's tables are on a node this deployment cannot see.\n\
- An empty answer would have been a plausible lie, which is why you get an error instead.\n\
- Ask an operator. They can bring the node back (`latiq node list`), or, if it is gone for good, remove the stale record with `latiq pond forget <pond> --confirm` — which deletes the registry row only, never the data on the departed node.\n\
- Work in a different pond in the meantime: **list_ponds**, or **allocate_pond** for a fresh one.\n\n\
## The same error from allocate_pond\n\n\
Allocation is eager: the control plane picks a node, and that node must create the pond's storage before you are told you have one. If it cannot be reached, **the pond was not created** — and the message says so, along with what happened to the assignment:\n\
- **'the assignment has been rolled back'** — nothing was left behind and the name is free. Retry allocate_pond, the same name and all. If it keeps failing, that node is down and only an operator can bring it back; use a different pond in the meantime.\n\
- **'may still exist'** — the rollback itself failed, so a registry row with that name may survive with no storage behind it. Retry under a DIFFERENT name, and tell an operator: `latiq pond forget <pond> --confirm` removes the stranded record.\n\n\
You are not seeing a half-created pond either way. The eagerness is the point: the alternative is a pond id that works until your first write.",
    },
    Res {
        uri: "latiq://troubleshooting/large-results",
        name: "Troubleshooting: large results",
        desc: "Results exceeded the inline cap",
        body: "# Result cap exceeded\n\n\
Your read returned more rows than the inline cap (~10k). Narrow with WHERE/LIMIT, aggregate server-side (GROUP BY/count/sum), or materialize with CREATE TABLE AS SELECT and query the smaller table. See latiq://recipes/large-results.",
    },
    Res {
        uri: "latiq://troubleshooting/timeouts",
        name: "Troubleshooting: timeouts",
        desc: "Break up slow queries",
        body: "# Query timeout\n\n\
Your statement ran past the timeout in effect for it and was stopped. The error names two numbers: the timeout that was APPLIED, and the maximum this node allows.\n\n\
**How the timeout is decided.** `read_query` and `write_query` take an optional `timeout_ms`. Omit it and the node's default applies. Ask for more than the node's maximum and you are CLAMPED to that maximum — the query still runs, it is never refused — so read `_meta.timeout_ms` on every successful result to see what was actually in effect.\n\n\
**Three levers, in order of cost:**\n\
1. **Retry with a larger `timeout_ms`**, up to the node's maximum. Cheapest when the work is genuinely large and you simply under-asked.\n\
2. **Narrow the query** — a WHERE on a selective column, a LIMIT, fewer columns, or aggregate server-side (GROUP BY/count/sum) instead of scanning. Call explain_query first to find the heavy operation.\n\
3. **If it already timed out AT the maximum**, a larger `timeout_ms` is not available: the work is too large for this pond's tier. Ask an operator to re-tier the pond.\n\n\
`query_timeout` and `query_cancelled` are different: the first is the node's deadline, the second is somebody asking for the query to stop. Only the first is fixed by asking for more time.",
    },
    Res {
        uri: "latiq://troubleshooting/conflicts",
        name: "Troubleshooting: write conflicts",
        desc: "Concurrent writes that conflict",
        body: "# Write conflicts\n\n\
Multiple agents write through DuckLake's transactional model. Conflicting writes auto-retry against the latest snapshot; expect occasional snapshot bumps. If you need strict ordering, coordinate at the application layer (e.g. read `ducklake_snapshots('<pond>')` to see the latest writer before extending a table).",
    },
    Res {
        uri: "latiq://troubleshooting/read-only-violation",
        name: "Troubleshooting: read-only violation",
        desc: "A write was sent to read_query",
        body: "# Read-only violation\n\n\
read_query only runs SELECT and read-only metadata statements. For INSERT/UPDATE/DELETE/DDL, use **write_query** — your writes there are attributed to your identity.",
    },
];

pub fn list_resources() -> Vec<Resource> {
    RESOURCES
        .iter()
        .map(|r| {
            let mut raw = RawResource::new(r.uri, r.name);
            raw.description = Some(r.desc.to_string());
            raw.mime_type = Some("text/markdown".to_string());
            raw.no_annotation()
        })
        .collect()
}

pub fn read_resource(uri: &str) -> Option<ReadResourceResult> {
    RESOURCES
        .iter()
        .find(|r| r.uri == uri)
        .map(|r| ReadResourceResult::new(vec![ResourceContents::text(r.body, r.uri)]))
}

struct PromptDef {
    name: &'static str,
    desc: &'static str,
}

const PROMPTS: &[PromptDef] = &[
    PromptDef {
        name: "setup_multi_agent_pond",
        desc: "Workflow to set up a pond for several agents to collaborate in (args: pond_name, domain)",
    },
    PromptDef {
        name: "discover_existing_pond",
        desc: "Workflow to find and join an existing pond (args: search_term, intent)",
    },
    PromptDef {
        name: "design_collaborative_schema",
        desc: "Workflow to design tables other agents can read and extend (args: domain)",
    },
    PromptDef {
        name: "recover_from_conflict",
        desc: "Workflow to recover after a write conflict (args: pond_id)",
    },
];

pub fn list_prompts() -> Vec<Prompt> {
    PROMPTS
        .iter()
        .map(|p| Prompt::new(p.name, Some(p.desc), None))
        .collect()
}

fn arg<'a>(args: &'a Map<String, Value>, key: &str, default: &'a str) -> &'a str {
    args.get(key).and_then(Value::as_str).unwrap_or(default)
}

pub fn get_prompt(name: &str, args: &Map<String, Value>) -> Option<GetPromptResult> {
    let text = match name {
        "setup_multi_agent_pond" => format!(
            "Set up a pond named '{pond}' for {domain} where several agents will collaborate:\n\
1. allocate_pond with name='{pond}'.\n\
2. Design a self-describing schema (COMMENT every table and column) — see latiq://recipes/schema-design.\n\
3. Establish an attribution-lookup habit: agents read `ducklake_snapshots('<pond>')` to see who wrote what — and none of them wrap SQL in BEGIN/COMMIT/ROLLBACK, which ends Latiq's own transaction early and leaves the write with no author.\n\
4. Coordinate writes; conflicts auto-retry (latiq://troubleshooting/conflicts).",
            pond = arg(args, "pond_name", "shared"),
            domain = arg(args, "domain", "the task")
        ),
        "discover_existing_pond" => format!(
            "Find an existing pond related to '{term}' (intent: {intent}):\n\
1. list_ponds and scan names/owners.\n\
2. describe_pond on candidates to inspect tables.\n\
3. read_query `SHOW TABLES` (and `information_schema.columns`) to understand the data.\n\
4. For intent=extend: add new tables without colliding with existing ones; comment them.",
            term = arg(args, "search_term", ""),
            intent = arg(args, "intent", "read")
        ),
        "design_collaborative_schema" => format!(
            "Design a schema for {domain} that other agents can read and extend:\n\
- Use clear, ANSI types; name tables/columns for cross-agent legibility.\n\
- COMMENT every table and column (visible via `information_schema.columns`).\n\
- Prefer additive evolution; don't rename columns others may depend on.\n\
See latiq://recipes/schema-design.",
            domain = arg(args, "domain", "the domain")
        ),
        "recover_from_conflict" => format!(
            "Recover from a write conflict in pond '{pond}':\n\
1. Re-read the current state: `SELECT max(snapshot_id) FROM ducklake_snapshots('<pond>')`.\n\
2. Identify the conflicting writer via `ducklake_snapshots('<pond>')`.\n\
3. Re-plan your write against the latest snapshot and retry (writes auto-retry, but re-check assumptions) — retry as plain statements; a hand-rolled BEGIN/COMMIT does not help and costs you the author on the write.\n\
See latiq://troubleshooting/conflicts.",
            pond = arg(args, "pond_id", "the pond")
        ),
        _ => return None,
    };
    Some(GetPromptResult::new(vec![PromptMessage::new_text(
        PromptMessageRole::User,
        text,
    )]))
}
