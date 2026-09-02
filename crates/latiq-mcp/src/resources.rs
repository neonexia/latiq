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
    AnnotateAble, GetPromptResult, Prompt, PromptArgument, PromptMessage, PromptMessageRole,
    RawResource, ReadResourceResult, Resource, ResourceContents,
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
- **Self-describing schemas:** after you CREATE TABLE, send `COMMENT ON TABLE`/`COMMENT ON COLUMN` statements. A `--` comment inside the DDL is lexical — the parser discards it and nothing is stored, so the next agent sees nothing. See latiq://recipes/schema-design.\n\
- **Attribution:** your writes are tagged with your agent identity. To see who wrote what: `SELECT author, commit_message, commit_extra_info FROM ducklake_snapshots('<pond>')`. `author` is the identity; `commit_extra_info` carries the evidence for it (issuer/subject when the caller was verified) — read BOTH, because an unverified caller can claim any author.\n\
- **Latiq owns the transaction:** send plain statements — multi-statement SQL is fine, but never `BEGIN`/`COMMIT`/`ROLLBACK`/`START TRANSACTION`. Latiq commits your write itself and records the author just before committing; your own `COMMIT` ends that transaction first, so the change lands in history with NO author.\n\
- **Discover:** `SHOW TABLES` lists tables, `DESCRIBE <table>` its columns and types, and `SELECT column_name, comment FROM duckdb_columns() WHERE table_name='<table>'` the column comments; list_ponds + describe_pond find existing work to join.\n\
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
- Snapshots/history/attribution are native DuckLake — `SELECT snapshot_id, author, commit_message, commit_extra_info FROM ducklake_snapshots('<pond>')` (`commit_extra_info` is where verified-vs-claimed shows up). List tables/columns with `SHOW TABLES` / `DESCRIBE <table>` / `information_schema.columns`; a column's stored COMMENT comes back from `duckdb_columns()`.\n\
- Prefer ANSI constructs; DuckDB extensions are tolerated but reduce portability.\n\n\
## Values, types and constraints\n\n\
An `invalid_value` error is about the DATA in your statement, not its syntax — the statement parsed and the names resolved.\n\
- **Type conversion:** a literal or column is not convertible to the type it is used as (`Conversion Error: Could not convert string 'notanint' to INT32`). Quoted text is not coerced into a numeric column because it looks numeric. Check the target with `DESCRIBE <table>` and pass the right type, or CAST explicitly: `CAST('7' AS INTEGER)`.\n\
- **Constraints:** the value is well-typed but breaks a rule on the table — primary key, unique, not null, check (`Constraint Error: Duplicate key …`). Read the conflicting row first (`SELECT * FROM t WHERE <key> = <value>`), then correct the value, UPDATE the existing row, or use `INSERT OR REPLACE` / `ON CONFLICT`.\n\n\
Neither is fixed by retrying the same statement, and neither is a `parse_error`: if your statement had a syntax problem you would have been told `parse_error` with DuckDB's `Parser Error` text.",
    },
    Res {
        uri: "latiq://recipes/schema-design",
        name: "Recipe: schema design",
        desc: "Authoring tables other agents can collaborate on",
        // The SQL block below is EXECUTED by
        // `crates/latiq/tests/mcp.rs::mcp_resources_schema_design_recipe_sql_actually_stores_comments`,
        // which reads it out of this body and asserts the comments are
        // readable afterwards. It taught the `-- …` form for months; that form
        // stores nothing, and four documents repeated the claim because nobody
        // ran it. Keep the block runnable as-is.
        body: "# Recipe — schema design for collaboration\n\n\
**When:** you're the first agent creating tables in a pond.\n\n\
**Pattern:**\n```sql\nCREATE TABLE events (\n  id INTEGER,\n  severity VARCHAR,\n  occurred_at TIMESTAMP\n);\nCOMMENT ON TABLE events IS 'One row per observed event.';\nCOMMENT ON COLUMN events.id IS 'event primary key';\nCOMMENT ON COLUMN events.severity IS 'one of: low, medium, high, critical';\nCOMMENT ON COLUMN events.occurred_at IS 'event time in UTC';\n```\n\
Send it as one write_query — several plain statements in one call are fine, and they commit as one snapshot.\n\
**Why it works:** `COMMENT ON` STORES the text in the catalog, so the next agent reads your intent without asking:\n\
```sql\nSELECT column_name, comment FROM duckdb_columns() WHERE table_name='events';\nSELECT comment FROM duckdb_tables() WHERE table_name='events';\n```\n\
(`information_schema.columns` carries the same text in its `column_comment` column.)\n\
**A `--` comment inside the DDL stores NOTHING.** It is lexical: the parser discards it, `duckdb_columns().comment` stays NULL, and the next agent sees an undocumented table. Only `COMMENT ON` persists.\n\
**Watch for:** vague table/column names; a CREATE TABLE with no COMMENT ON statements after it; types that don't match the domain.",
    },
    Res {
        uri: "latiq://recipes/large-results",
        name: "Recipe: large results",
        desc: "Handling results larger than the inline cap",
        body: "# Recipe — large results\n\n\
**When:** a read_query returns `result_cap_exceeded` or you expect many rows.\n\n\
**First, plan it:** call **explain_query** on the statement. It does not execute, so it costs you nothing, and it shows which operation is heavy before you spend a result on finding out.\n\n\
**Then (pick one):**\n\
1. **Narrow:** add a WHERE on a selective column and/or LIMIT.\n\
2. **Aggregate server-side:** `SELECT severity, count(*) FROM events GROUP BY severity`.\n\
3. **Materialize:** `CREATE TABLE hot AS SELECT * FROM events WHERE severity='critical'` then query the smaller table.\n\
**Why:** the inline cap keeps your context bounded. It is a limit on THIS surface, not on Latiq — the SDK reads the same query unbounded as an Arrow stream (`Stream/ReadArrow`), so a full extract is a job for whoever holds the SDK, not something to page into your context.",
    },
    Res {
        // The `-m1` in this URI names a milestone that is long past. It is kept
        // anyway: a served URI is a link agents and error `see` fields hold, and
        // renaming it would 404 for them. The heading and body are current; only
        // the URI is historical.
        uri: "latiq://recipes/data-ingestion-m1",
        name: "Recipe: data ingestion",
        desc: "Loading data into a pond with SQL",
        body: "# Recipe — ingest data with SQL\n\n\
**Files by URL:** read CSV/Parquet/JSON straight into a table from write_query:\n```sql\nCREATE TABLE raw AS SELECT * FROM read_csv('https://example.com/data.csv');\nINSERT INTO raw SELECT * FROM 's3://public-bucket/more.parquet';\n```\n\
**No credentials are attached to these reads.** A source that needs authentication will fail as `source_unavailable` — for those use pull_catalog, whose `set:{…}` credentials are used once and never stored (latiq://recipes/external-data). \
**And the address is not restricted.** Latiq does not inspect or allow-list the path in your SQL, so the node will read whatever it can reach — including its own local files. That is a known gap (issue #79), not a sandbox: name only sources you were asked to use, and never read that a path worked as permission to read it.\n\
For curated/registered sources (incl. external lakehouses like iceberg) use list_datasets/load_dataset and list_catalogs -> describe_catalog -> pull_catalog — see latiq://recipes/external-data.\n\
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
        // `dataset_not_found`'s `see` has always pointed here, and until the
        // guard below existed, nothing served it: an agent that followed the
        // link got `resource_not_found` and spent a call learning nothing.
        uri: "latiq://datasets",
        name: "Datasets",
        desc: "The curated public files this deployment can load into a pond",
        body: "# Datasets\n\n\
A **dataset** is a curated file this deployment already knows how to fetch — you name it, Latiq loads it into your pond. Nothing is queried live: `load_dataset` copies the data in, and from then on it is ordinary pond data.\n\n\
```\nlist_datasets {}                        # everything registered here\nlist_datasets {query:'tpch'}            # filter by name/tag; '#sample' finds the small ones\nload_dataset {pond:'p', dataset:'tpch'} # -> a SCHEMA named after the dataset\nread_query {pond:'p', sql:'SELECT count(*) FROM tpch.orders'}\n```\n\n\
**A dataset lands in its own schema**, so its tables are `<dataset>.<table>` — `tpch.orders`, not `orders`. `describe_pond` shows them after the load.\n\n\
**`dataset_not_found` means the reference is not registered in THIS deployment.** The catalogue differs per deployment and is an operator's to extend, so:\n\
- Call **list_datasets** and use a name from the answer — do not guess, and do not retry the same reference.\n\
- If what you need is a URL rather than a registered dataset, read it directly instead: `write_query {sql:\"CREATE TABLE raw AS SELECT * FROM read_csv('https://…')\"}` (latiq://recipes/data-ingestion-m1).\n\
- For an external database or lakehouse, that is a **catalog**, not a dataset: list_catalogs → describe_catalog → pull_catalog (latiq://recipes/external-data).",
    },
    Res {
        uri: "latiq://troubleshooting",
        name: "Troubleshooting index",
        desc: "Problem-keyed recovery guides",
        body: "# Troubleshooting\n\n\
Every page here is keyed by the `kind` on the error envelope you received — match the kind, don't browse.\n\n\
- latiq://troubleshooting/pond-not-found — `pond_not_found`: the pond id/name doesn't resolve.\n\
- latiq://troubleshooting/pond-unavailable — `pond_unavailable`: the pond exists but no node is serving it, or allocate_pond could not create one on the node it was assigned to.\n\
- latiq://troubleshooting/catalog-error — `catalog_error`: a table/column/function in your SQL doesn't exist, or already does.\n\
- latiq://troubleshooting/source-unavailable — `source_unavailable`: a URL or path in your SQL could not be read.\n\
- latiq://troubleshooting/large-results — `result_cap_exceeded`: results exceeded the inline cap.\n\
- latiq://troubleshooting/timeouts — `query_timeout` and `query_cancelled`: a query was stopped before it finished.\n\
- latiq://troubleshooting/unauthenticated — `unauthenticated`: your token was missing, expired or rejected.\n\
- latiq://troubleshooting/internal — `internal` and `storage`: Latiq itself failed. Nothing in your SQL fixes it.\n\
- latiq://troubleshooting/conflicts — concurrent writes conflicted. No error kind: Latiq retries these for you.\n\
- latiq://troubleshooting/read-only-violation — `read_only_violation`: a write was sent to read_query.",
    },
    Res {
        uri: "latiq://troubleshooting/catalog-error",
        name: "Troubleshooting: catalog error",
        desc: "A name in your SQL doesn't resolve — or already exists",
        body: "# Catalog error (`catalog_error`)\n\n\
Your statement is valid SQL. A **name** in it does not match this pond: a table, column, schema or function that isn't there — or one that is there when you asked to create it.\n\n\
## The name doesn't exist\n\
`Catalog Error: Table with name nope does not exist!`, `Binder Error: Referenced column \"qty\" not found`.\n\
1. **Look, don't guess:** `read_query {sql:'SHOW TABLES'}`, `read_query {sql:'DESCRIBE orders'}`, or **describe_pond** for the whole schema in one call.\n\
2. Ponds are separate — a table in another pond is not visible here. `list_ponds` if you may be in the wrong one.\n\
3. If it genuinely isn't there, create it with **write_query** (`CREATE TABLE …`, or `CREATE TABLE … AS SELECT …`), or bring the data in with load_dataset / pull_catalog (latiq://recipes/external-data).\n\n\
## The name already exists\n\
`Catalog Error: Table with name t already exists!` — from `CREATE TABLE t …`.\n\
- **Do not retry the same statement.** It will fail identically forever; this is not a transient error.\n\
- Choose: a different name, `CREATE TABLE IF NOT EXISTS t …` (keep what's there), `CREATE OR REPLACE TABLE t …` (discard what's there — destructive), or `INSERT INTO t …` if you meant to add rows to it.\n\
- Another agent may have created it since you last looked. `DESCRIBE t` before you replace anything: ponds are shared, and CREATE OR REPLACE destroys someone else's table without asking.\n\n\
## Functions\n\
`Binder Error: No function matches the given name and argument types` — the function name or its argument types are wrong, not the SQL. See latiq://dialect.\n\n\
This is never a syntax problem (that arrives as `parse_error`) and never something an operator can fix for you.",
    },
    Res {
        uri: "latiq://troubleshooting/source-unavailable",
        name: "Troubleshooting: source unavailable",
        desc: "A URL or path in your SQL could not be read",
        body: "# Source unavailable (`source_unavailable`)\n\n\
The statement named a data source outside the pond — a URL, an object-store path, a file — and the engine could not read it: `IO Error: Could not connect to server …`, `HTTP Error: … (404)`.\n\n\
**Nothing in Latiq is broken, and this is not the pond's storage.** The address is yours, in your SQL, so the fix is too:\n\
1. **Check the address** — spelling, scheme, host, bucket, the file actually being there. `read_csv('http://127.0.0.1:9/none.csv')` fails for the obvious reason.\n\
2. **Check it is reachable from the NODE**, not from you — the node's network decides, and it is not yours. Your laptop's localhost and your VPN's private hosts are not the node's. A refused URI (blocked by policy rather than unreachable) arrives as `uri_not_allowed` instead.\n\
3. **Credentials:** Latiq attaches none to a URL in your SQL, so anything requiring authentication fails here. For those use **pull_catalog** with `set:{…}` (used once, never stored) — see latiq://recipes/external-data.\n\
4. **Retry once, not repeatedly.** A transient network fault is worth one retry; a second identical failure is the source, and repeating it will not change that.\n\n\
Once the data is in the pond it can't fail this way again: `CREATE TABLE raw AS SELECT * FROM read_csv('<url>')` copies it in, and later queries read the pond.",
    },
    Res {
        uri: "latiq://troubleshooting/pond-not-found",
        name: "Troubleshooting: pond not found",
        desc: "Recover from a missing pond",
        body: "# Pond not found (`pond_not_found`)\n\n\
The pond id or name doesn't exist in this deployment.\n\
- Call **list_ponds** to see what exists (names + ids).\n\
- Call **allocate_pond** to create a new one.\n\
- Check spelling; pond refs accept either the UUID or the human name.",
    },
    Res {
        uri: "latiq://troubleshooting/pond-unavailable",
        name: "Troubleshooting: pond unavailable",
        desc: "The pond exists but no node is serving it",
        body: "# Pond unavailable (`pond_unavailable`)\n\n\
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
        body: "# Result cap exceeded (`result_cap_exceeded`)\n\n\
Your read returned more rows than the inline cap (~10k). Narrow with WHERE/LIMIT, aggregate server-side (GROUP BY/count/sum), or materialize with CREATE TABLE AS SELECT and query the smaller table. See latiq://recipes/large-results.",
    },
    Res {
        uri: "latiq://troubleshooting/timeouts",
        name: "Troubleshooting: timeouts",
        desc: "Break up slow queries",
        body: "# Query stopped (`query_timeout`, `query_cancelled`)\n\n\
Your statement ran past the timeout in effect for it and was stopped. The error names two numbers: the timeout that was APPLIED, and the maximum this node allows.\n\n\
**How the timeout is decided.** `read_query` and `write_query` take an optional `timeout_ms`. Omit it and the node's default applies. Ask for more than the node's maximum and you are CLAMPED to that maximum — the query still runs, it is never refused — so read `_meta.timeout_ms` on every successful result to see what was actually in effect.\n\n\
**Three levers, in order of cost:**\n\
1. **Retry with a larger `timeout_ms`**, up to the node's maximum. Cheapest when the work is genuinely large and you simply under-asked.\n\
2. **Narrow the query** — a WHERE on a selective column, a LIMIT, fewer columns, or aggregate server-side (GROUP BY/count/sum) instead of scanning. Call explain_query first to find the heavy operation.\n\
3. **If it already timed out AT the maximum**, a larger `timeout_ms` is not available: the work is too large for this pond's tier. Ask an operator to re-tier the pond.\n\n\
`query_timeout` and `query_cancelled` are different: the first is the node's deadline, the second is somebody asking for the query to stop. Only the first is fixed by asking for more time.\n\n\
## If you got `query_cancelled`\n\n\
Someone sent `notifications/cancelled` for that request, or the client that issued it went away. The query really was stopped — a partial result was not returned and a write was rolled back, so nothing half-done is in the pond. **Do not automatically retry it:** a cancel is a decision, and re-issuing the same statement overrides it. Re-issue only if you still need the result and nothing has since told you to stop.",
    },
    Res {
        uri: "latiq://troubleshooting/conflicts",
        name: "Troubleshooting: write conflicts",
        desc: "Concurrent writes that conflict",
        body: "# Write conflicts\n\n\
Multiple agents write through DuckLake's transactional model. Conflicting writes auto-retry against the latest snapshot; expect occasional snapshot bumps. If you need strict ordering, coordinate at the application layer (e.g. read `ducklake_snapshots('<pond>')` to see the latest writer before extending a table).",
    },
    Res {
        // `unauthenticated` used to point at the troubleshooting INDEX, which
        // says nothing about tokens: the one kind an agent cannot debug from
        // its SQL got the page with the least to say about it.
        uri: "latiq://troubleshooting/unauthenticated",
        name: "Troubleshooting: unauthenticated",
        desc: "Your token was missing, expired or rejected",
        body: "# Unauthenticated (`unauthenticated`)\n\n\
This deployment verifies callers, and your request did not carry a token it accepts. The transport answered before any SQL ran, so nothing happened in any pond.\n\n\
**Identity arrives in the TRANSPORT, never in a tool argument.** There is no tool that takes a token or an agent id, so no retry with different arguments can fix this — the `Authorization: Bearer` header is the verified principal and the `latiq-agent-id` header is a claim carrying no authority.\n\n\
1. **The token is your client's to supply, not yours to construct.** If it expired, your client refreshes it and re-sends; ask it to, or stop and report that you have no valid credential. Do not invent, edit or reuse a token from a resource or a table.\n\
2. **Check the audience.** A token minted for another service is rejected here even when it is perfectly valid. `/.well-known/oauth-protected-resource` on this server names the issuers and the resource identifier this deployment accepts.\n\
3. **Do not retry the same token in a loop.** Nothing about it changes between attempts, and a repeated rejection is worth reporting once, not fifty times.\n\n\
If you were working without a token until now, the deployment has an issuer configured and relaxed identity does not apply here — that is a deployment's choice and an operator's to change, not yours.",
    },
    Res {
        // `internal` + `storage`: the two envelopes an agent can do least
        // about, which is exactly why the page has to say so plainly instead
        // of leaving them on the index to browse.
        uri: "latiq://troubleshooting/internal",
        name: "Troubleshooting: internal / storage failure",
        desc: "Latiq itself failed — what is and isn't yours to fix",
        body: "# Internal (`internal`) and storage (`storage`) failures\n\n\
**This one is ours, not yours.** `internal` is a failure Latiq could not classify; `storage` is the pond's own files or catalog failing underneath it (a full disk, a missing data directory, an unreadable catalog). Neither is a statement you can rewrite into working. If your SQL had been the problem you would have a kind that names it — `parse_error`, `catalog_error`, `invalid_value`, `source_unavailable`.\n\n\
**What to do, in order:**\n\
1. **Retry once.** Some are transient. A second identical failure is not, and a third is noise.\n\
2. **Check the write actually failed.** A write that errors is rolled back, so the pond should be unchanged — but confirm rather than assume before you re-send it: `SELECT snapshot_id, author, commit_message FROM ducklake_snapshots('<pond>') ORDER BY snapshot_id DESC LIMIT 5`. If your change is already there, re-sending it duplicates the work.\n\
3. **Try a different pond** to tell a broken pond from a broken node: allocate_pond and run a trivial `SELECT 1`. If that works, the first pond's storage is the problem; if it doesn't, the node is.\n\
4. **Report it and stop.** Quote the `message` verbatim to your operator — it carries the detail they need and you cannot act on. Working around a storage failure by writing somewhere else is how data ends up in a pond nobody is looking after.\n\n\
**Do not** treat this as a permissions or a not-found answer. `internal` never means \"you may not\" and never means \"it isn't there\"; assuming either is how an agent talks itself into recreating data that still exists.",
    },
    Res {
        uri: "latiq://troubleshooting/read-only-violation",
        name: "Troubleshooting: read-only violation",
        desc: "A write was sent to read_query",
        body: "# Read-only violation (`read_only_violation`)\n\n\
read_query only runs SELECT and read-only metadata statements. For INSERT/UPDATE/DELETE/DDL, use **write_query** — your writes there are attributed to your identity.\n\n\
You get this error only for a statement that really is recognisable as a write (including a hidden one: `WITH … INSERT`, `EXPLAIN ANALYZE`, a second statement after a `;`, or `BEGIN`/`COMMIT`/`ROLLBACK`, which Latiq owns). A statement it cannot recognise at all is NOT reported here — it goes to the parser and comes back as `parse_error` — so if you see this kind, re-read your SQL for a write rather than assuming a typo.",
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
    /// `(name, description, required)` — DECLARED, not merely mentioned in the
    /// description. A conforming client fills `prompts/get` arguments from this
    /// list; while it was `None` every client sent `{}` and every prompt
    /// rendered its placeholders, so `discover_existing_pond` produced
    /// "Find an existing pond related to '' (intent: read)" — a confident
    /// instruction to do nothing in particular.
    args: &'static [(&'static str, &'static str, bool)],
}

const PROMPTS: &[PromptDef] = &[
    PromptDef {
        name: "setup_multi_agent_pond",
        desc: "Workflow to set up a pond for several agents to collaborate in",
        args: &[
            (
                "pond_name",
                "Name for the new pond, e.g. 'incident-7'.",
                true,
            ),
            (
                "domain",
                "What the pond is for, e.g. 'incident triage'. Defaults to 'the task'.",
                false,
            ),
        ],
    },
    PromptDef {
        name: "discover_existing_pond",
        desc: "Workflow to find and join an existing pond",
        args: &[
            (
                "search_term",
                "What the pond would be about — matched against pond names, e.g. 'incident'.",
                true,
            ),
            (
                "intent",
                "'read' to consume what is there, 'extend' to add tables. Defaults to 'read'.",
                false,
            ),
        ],
    },
    PromptDef {
        name: "design_collaborative_schema",
        desc: "Workflow to design tables other agents can read and extend",
        args: &[(
            "domain",
            "What the tables describe, e.g. 'security incidents'.",
            true,
        )],
    },
    PromptDef {
        name: "recover_from_conflict",
        desc: "Workflow to re-plan a write after another agent's write landed first",
        args: &[(
            "pond_name",
            "The pond you were writing to. Its NAME, not its id: the DuckLake catalog is \
             attached under the pond name, so that is what ducklake_snapshots() takes.",
            true,
        )],
    },
];

pub fn list_prompts() -> Vec<Prompt> {
    PROMPTS
        .iter()
        .map(|p| {
            let args = p
                .args
                .iter()
                .map(|(name, desc, required)| {
                    PromptArgument::new(*name)
                        .with_description(*desc)
                        .with_required(*required)
                })
                .collect();
            Prompt::new(p.name, Some(p.desc), Some(args))
        })
        .collect()
}

/// Why `get_prompt` can fail. A missing REQUIRED argument is an error, not a
/// placeholder: rendering a plausible instruction around an empty string is the
/// one outcome the caller cannot detect.
pub enum PromptError {
    Unknown,
    MissingArgument {
        prompt: &'static str,
        arg: &'static str,
    },
}

fn arg<'a>(args: &'a Map<String, Value>, key: &str, default: &'a str) -> &'a str {
    args.get(key)
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|v| !v.is_empty())
        .unwrap_or(default)
}

/// The value of a REQUIRED argument, or the error naming the one that is missing.
fn required<'a>(
    def: &'static PromptDef,
    args: &'a Map<String, Value>,
    key: &'static str,
) -> Result<&'a str, PromptError> {
    args.get(key)
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|v| !v.is_empty())
        .ok_or(PromptError::MissingArgument {
            prompt: def.name,
            arg: key,
        })
}

pub fn get_prompt(name: &str, args: &Map<String, Value>) -> Result<GetPromptResult, PromptError> {
    let def = PROMPTS
        .iter()
        .find(|p| p.name == name)
        .ok_or(PromptError::Unknown)?;
    let text = match name {
        "setup_multi_agent_pond" => format!(
            "Set up a pond named '{pond}' for {domain} where several agents will collaborate:\n\
1. allocate_pond with name='{pond}'.\n\
2. Design a self-describing schema: CREATE TABLE, then `COMMENT ON TABLE`/`COMMENT ON COLUMN` for each one — a `--` comment in the DDL is discarded and stores nothing. See latiq://recipes/schema-design.\n\
3. Establish an attribution-lookup habit: agents read `ducklake_snapshots('{pond}')` to see who wrote what — and none of them wrap SQL in BEGIN/COMMIT/ROLLBACK, which ends Latiq's own transaction early and leaves the write with no author.\n\
4. Coordinate writes; conflicts auto-retry (latiq://troubleshooting/conflicts).",
            pond = required(def, args, "pond_name")?,
            domain = arg(args, "domain", "the task")
        ),
        "discover_existing_pond" => format!(
            "Find an existing pond related to '{term}' (intent: {intent}):\n\
1. list_ponds and scan names/owners.\n\
2. describe_pond on candidates to inspect tables.\n\
3. read_query `SHOW TABLES` and `DESCRIBE <table>` for the shape, and `SELECT column_name, comment FROM duckdb_columns() WHERE table_name='<table>'` for what the author meant by each column.\n\
4. For intent=extend: add new tables without colliding with existing ones, and COMMENT ON them as you go.",
            term = required(def, args, "search_term")?,
            intent = arg(args, "intent", "read")
        ),
        "design_collaborative_schema" => format!(
            "Design a schema for {domain} that other agents can read and extend:\n\
- Use clear, ANSI types; name tables/columns for cross-agent legibility.\n\
- Document every table and column with a `COMMENT ON TABLE`/`COMMENT ON COLUMN` statement after the CREATE. Those are stored and readable (`SELECT column_name, comment FROM duckdb_columns()`); a `--` comment inside the CREATE is not stored at all.\n\
- Prefer additive evolution; don't rename columns others may depend on.\n\
See latiq://recipes/schema-design.",
            domain = required(def, args, "domain")?
        ),
        "recover_from_conflict" => format!(
            "Re-plan a write in pond '{pond}' after another agent's write landed first.\n\n\
WHEN THIS APPLIES: there is no `write_conflict` error kind and nothing routes you here — Latiq retries conflicting writes for you against the latest snapshot. Use this when a write SUCCEEDED but not against the state you assumed: your UPDATE matched no rows, an INSERT hit a duplicate key, or a table changed shape under you. If you are holding a structured error instead, follow its `see` link; this is not that.\n\n\
1. Re-read the current state: `SELECT max(snapshot_id) FROM ducklake_snapshots('{pond}')`.\n\
2. Identify who wrote last: `SELECT snapshot_id, author, commit_message FROM ducklake_snapshots('{pond}') ORDER BY snapshot_id DESC LIMIT 5`.\n\
3. Re-read the rows you were changing, re-plan against what is actually there, and retry as plain statements — a hand-rolled BEGIN/COMMIT does not help and costs you the author on the write.\n\
See latiq://troubleshooting/conflicts.",
            pond = required(def, args, "pond_name")?
        ),
        // PROMPTS is the only list of names, and `def` was resolved from it.
        _ => return Err(PromptError::Unknown),
    };
    Ok(GetPromptResult::new(vec![PromptMessage::new_text(
        PromptMessageRole::User,
        text,
    )]))
}

#[cfg(test)]
mod tests {
    use super::*;
    use latiq_common::ErrorKind;

    /// Every kind an agent can receive must be able to explain itself.
    ///
    /// `see` is the deeper-learning half of the envelope, and a `see` that
    /// 404s costs the agent a round trip to learn nothing. This is also the
    /// guard on ADDING a kind: a new one whose `see` was invented but never
    /// written fails here rather than in front of an agent.
    #[test]
    fn error_contract_every_error_kind_sees_a_resource_that_exists() {
        // Anti-vacuity: the list is the whole enum, so it cannot silently
        // shrink to nothing (or to the kinds that happen to pass).
        assert_eq!(
            ALL_KINDS.len(),
            18,
            "a kind was added or removed — add it to ALL_KINDS, with a `see` that resolves"
        );
        for kind in ALL_KINDS {
            let see = kind.default_see();
            assert!(
                read_resource(see).is_some(),
                "{}'s see ({see}) is not a resource this server serves",
                kind.as_str()
            );
        }
    }

    /// The two kinds added for the classification fix point at resources
    /// written ABOUT them — not at the troubleshooting index, which would be a
    /// dangling-by-another-name `see`: it resolves, and it teaches nothing.
    #[test]
    fn error_contract_the_new_kinds_have_their_own_page_not_the_index() {
        for (kind, must_say) in [
            (ErrorKind::CatalogError, ["SHOW TABLES", "already exists"]),
            (
                ErrorKind::SourceUnavailable,
                ["reachable from the NODE", "Retry once"],
            ),
        ] {
            let see = kind.default_see();
            assert_ne!(
                see,
                "latiq://troubleshooting",
                "{} must not point at the index",
                kind.as_str()
            );
            let body = RESOURCES
                .iter()
                .find(|r| r.uri == see)
                .unwrap_or_else(|| panic!("{see} is not served"))
                .body;
            for phrase in must_say {
                assert!(
                    body.contains(phrase),
                    "{see} must actually cover this kind — missing {phrase:?}"
                );
            }
        }
    }

    /// The index is how an agent browsing `latiq://troubleshooting` finds them;
    /// a page nothing links to is a page nothing reads. And a page the index
    /// links but nobody serves costs a round trip to learn nothing, so the
    /// correspondence is asserted in BOTH directions.
    #[test]
    fn error_contract_the_troubleshooting_index_lists_every_troubleshooting_page() {
        let index = RESOURCES
            .iter()
            .find(|r| r.uri == "latiq://troubleshooting")
            .expect("the index is served")
            .body;
        let pages: Vec<&str> = RESOURCES
            .iter()
            .map(|r| r.uri)
            .filter(|u| u.starts_with("latiq://troubleshooting/"))
            .collect();
        assert!(pages.len() >= 9, "found only {pages:?}");
        for page in &pages {
            assert!(index.contains(page), "the index does not link {page}");
        }
        // …and nothing it doesn't serve. The index is prose, so the links are
        // recovered by scanning it for the URI prefix.
        let linked: Vec<String> = index
            .match_indices("latiq://troubleshooting/")
            .map(|(at, _)| {
                index[at..]
                    .split(|c: char| c.is_whitespace() || c == '`' || c == ',')
                    .next()
                    .unwrap_or_default()
                    .to_string()
            })
            .collect();
        assert_eq!(
            linked.len(),
            pages.len(),
            "the index links {linked:?} but {pages:?} are served"
        );
        for link in &linked {
            assert!(
                read_resource(link).is_some(),
                "the index links {link}, which is not served"
            );
        }
    }

    /// A `see` that resolves is not the same as a `see` that helps.
    ///
    /// Four kinds — `unauthenticated`, `query_cancelled`, `storage`, `internal`
    /// — used to point at the troubleshooting INDEX, which covered none of
    /// them: `internal` is what an agent holds when it is most stuck, and the
    /// page it landed on was a menu of other agents' problems. The assertion
    /// that catches that (and stays honest as kinds are added) is that the body
    /// names the kind's own wire name.
    #[test]
    fn error_contract_a_troubleshooting_see_names_its_own_kind() {
        let mut checked = 0;
        for kind in ALL_KINDS {
            let see = kind.default_see();
            if !see.starts_with("latiq://troubleshooting") {
                continue; // dialect / guidance / datasets are covered elsewhere.
            }
            assert_ne!(
                see,
                "latiq://troubleshooting",
                "{} points at the index, which is a menu, not an answer",
                kind.as_str()
            );
            let body = RESOURCES
                .iter()
                .find(|r| r.uri == see)
                .unwrap_or_else(|| panic!("{see} is not served"))
                .body;
            assert!(
                body.contains(kind.as_str()),
                "{see} never mentions `{}` — it resolves but does not cover the kind that lands there",
                kind.as_str()
            );
            checked += 1;
        }
        // Anti-vacuity: a `continue`-driven loop passes perfectly when it
        // iterates over nothing.
        assert!(
            checked >= 8,
            "only {checked} kinds route to a troubleshooting page — the loop is skipping"
        );
    }

    // --- prompts ---------------------------------------------------------

    /// Argument names used to live only in prose ("(args: pond_name, domain)"),
    /// so a conforming client that builds `prompts/get` from the declared
    /// schema sent `{}` and got a placeholder rendering.
    #[test]
    fn mcp_prompts_declare_every_argument_they_actually_read() {
        assert_eq!(PROMPTS.len(), 4, "a prompt was added or removed");
        for p in PROMPTS {
            assert!(!p.args.is_empty(), "{} declares no arguments", p.name);
            assert!(
                p.args.iter().any(|(_, _, required)| *required),
                "{} declares nothing required — then nothing can be missing",
                p.name
            );
            // Every DECLARED argument must reach the rendering, or the schema
            // is advertising a knob that does nothing. Each is given a value
            // unique to it, so its absence can only mean it was ignored.
            let mut args = Map::new();
            for (name, _, _) in p.args {
                args.insert((*name).into(), Value::String(format!("VAL-{name}")));
            }
            let text = rendered(p.name, &args);
            for (name, _, _) in p.args {
                assert!(
                    text.contains(&format!("VAL-{name}")),
                    "{}'s declared argument '{name}' never appears in the rendering:\n{text}",
                    p.name
                );
            }
        }
    }

    /// The observed worst case: `discover_existing_pond` with no arguments
    /// rendered "Find an existing pond related to '' (intent: read):" — a
    /// nonsense instruction shaped exactly like a real one. A refusal the
    /// caller can act on beats a confident placeholder.
    #[test]
    fn mcp_prompts_a_missing_required_argument_is_refused_not_defaulted() {
        for p in PROMPTS {
            for (missing, _, _) in p.args.iter().filter(|(_, _, req)| *req) {
                // Every OTHER argument supplied, so the refusal can only be
                // attributed to the one left out.
                let mut args = Map::new();
                for (name, _, _) in p.args.iter().filter(|(n, _, _)| n != missing) {
                    args.insert((*name).into(), Value::String(format!("VAL-{name}")));
                }
                match get_prompt(p.name, &args) {
                    Err(PromptError::MissingArgument { prompt, arg }) => {
                        assert_eq!(prompt, p.name);
                        assert_eq!(arg, *missing, "{} named the wrong argument", p.name);
                    }
                    Err(PromptError::Unknown) => panic!("{} is not a known prompt", p.name),
                    Ok(_) => panic!(
                        "{} rendered without its required '{missing}' — that is the placeholder bug",
                        p.name
                    ),
                }
                // An empty/whitespace string is a missing argument too: it
                // renders the same nonsense a client sending `{}` would get.
                let mut blank = args.clone();
                blank.insert((*missing).into(), Value::String("   ".into()));
                assert!(
                    matches!(
                        get_prompt(p.name, &blank),
                        Err(PromptError::MissingArgument { arg, .. }) if arg == *missing
                    ),
                    "{}: a blank '{missing}' must be refused like an absent one",
                    p.name
                );
            }
        }
    }

    /// `recover_from_conflict` interpolated its prose but not its SQL, so an
    /// agent given a real pond was handed
    /// `ducklake_snapshots('<pond>')` to copy-paste — SQL that cannot run.
    #[test]
    fn mcp_prompts_no_rendered_sql_carries_an_unfilled_placeholder() {
        for p in PROMPTS {
            let mut args = Map::new();
            for (name, _, _) in p.args {
                args.insert((*name).into(), Value::String(format!("VAL-{name}")));
            }
            let text = rendered(p.name, &args);
            assert!(
                !text.contains("('<pond>')"),
                "{} hands the agent literal `<pond>` inside runnable SQL:\n{text}",
                p.name
            );
        }
        // Anti-vacuity: the prompt that had the bug must still contain the
        // call, now filled in — otherwise this passes by the SQL vanishing.
        let mut args = Map::new();
        args.insert("pond_name".into(), Value::String("shared-1".into()));
        let text = rendered("recover_from_conflict", &args);
        assert!(
            text.contains("ducklake_snapshots('shared-1')"),
            "the recovery SQL should name the caller's pond:\n{text}"
        );
    }

    /// The SOP has no error kind and no `see` route into it, so it has to say
    /// for itself when an agent should reach for it.
    #[test]
    fn mcp_prompts_recover_from_conflict_says_when_it_applies() {
        let mut args = Map::new();
        args.insert("pond_name".into(), Value::String("shared-1".into()));
        let text = rendered("recover_from_conflict", &args);
        for phrase in ["WHEN THIS APPLIES", "no `write_conflict` error kind"] {
            assert!(text.contains(phrase), "missing {phrase:?}:\n{text}");
        }
    }

    /// The closed taxonomy, listed so a new kind must be added here too.
    const ALL_KINDS: &[ErrorKind] = &[
        ErrorKind::PondNotFound,
        ErrorKind::DatasetNotFound,
        ErrorKind::NameConflict,
        ErrorKind::CatalogError,
        ErrorKind::ParseError,
        ErrorKind::InvalidValue,
        ErrorKind::MissingArgument,
        ErrorKind::WriteToReservedSchema,
        ErrorKind::ResultCapExceeded,
        ErrorKind::ReadOnlyViolation,
        ErrorKind::UriNotAllowed,
        ErrorKind::QueryTimeout,
        ErrorKind::QueryCancelled,
        ErrorKind::Unauthenticated,
        ErrorKind::PondUnavailable,
        ErrorKind::SourceUnavailable,
        ErrorKind::Storage,
        ErrorKind::Internal,
    ];

    fn rendered(name: &str, args: &Map<String, Value>) -> String {
        let res = match get_prompt(name, args) {
            Ok(r) => r,
            Err(_) => panic!("{name} did not render with every declared argument supplied"),
        };
        res.messages
            .into_iter()
            .filter_map(|m| match m.content {
                rmcp::model::PromptMessageContent::Text { text, .. } => Some(text),
                _ => None,
            })
            .collect::<Vec<_>>()
            .join("\n")
    }
}
