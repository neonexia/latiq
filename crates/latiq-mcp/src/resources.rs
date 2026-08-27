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
- **Discover:** `SHOW TABLES` lists tables (and `information_schema.columns` for columns); list_ponds + describe_pond find existing work to join.\n\
- **External data:** to bring outside data in, use list_datasets + load_dataset (curated public files), or list_catalogs → describe_catalog → pull_catalog (external databases/lakehouses like iceberg — you pull a subset into the pond, then work there). See latiq://recipes/external-data.\n\
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
**Always read `commit_extra_info` alongside `author`.** `author` alone cannot tell a VERIFIED writer from one merely claiming that name — the evidence (issuer/subject, and whether the identity was verified) lives in `commit_extra_info`.",
    },
    Res {
        uri: "latiq://troubleshooting",
        name: "Troubleshooting index",
        desc: "Problem-keyed recovery guides",
        body: "# Troubleshooting\n\n\
- latiq://troubleshooting/pond-not-found — the pond id/name doesn't resolve.\n\
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
The query exceeded the deployment's timeout. Call explain_query to find the heavy operation, add a WHERE on a selective column, reduce the scan, or pre-aggregate, then retry.",
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
3. Establish an attribution-lookup habit: agents read `ducklake_snapshots('<pond>')` to see who wrote what.\n\
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
3. Re-plan your write against the latest snapshot and retry (writes auto-retry, but re-check assumptions).\n\
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
