//! The Latiq MCP server: exposes the agent tools (pond + query + dataset/catalog)
//! over rmcp Streamable-HTTP.
//! Identity is relaxed (Slice 0+): taken from an optional `agent_id` argument,
//! defaulting to anonymous. (M6 moves this to the `X-Latiq-Agent-Id` header.)
use crate::encode::{err_envelope, ok_explain, ok_query, ok_value};
use crate::resources;
use latiq_agent_core::{AgentError, AgentOps};
use latiq_common::Identity;
use rmcp::handler::server::{router::tool::ToolRouter, wrapper::Parameters};
use rmcp::model::{
    CallToolResult, GetPromptRequestParams, GetPromptResult, ListPromptsResult,
    ListResourcesResult, PaginatedRequestParams, ReadResourceRequestParams, ReadResourceResult,
    ServerCapabilities, ServerInfo,
};
use rmcp::schemars::JsonSchema;
use rmcp::service::RequestContext;
use rmcp::transport::streamable_http_server::{
    session::local::LocalSessionManager, StreamableHttpServerConfig, StreamableHttpService,
};
use rmcp::{tool, tool_handler, tool_router, ErrorData as McpError, RoleServer, ServerHandler};
use serde::Deserialize;
use std::net::SocketAddr;
use std::sync::Arc;

#[derive(Debug, Deserialize, JsonSchema)]
#[schemars(crate = "rmcp::schemars")]
pub struct AllocateArgs {
    #[schemars(description = "Optional pond name; Latiq generates one if omitted")]
    pub name: Option<String>,
    #[schemars(
        description = "Resource tier: x-small | small | medium | large | x-large (default medium). Caps the pond's memory + CPU."
    )]
    pub tier: Option<String>,
    #[schemars(
        description = "Optional DuckDB extensions to load on this pond, e.g. [\"spatial\",\"fts\"]. Signed/official extensions only; must be available on the deployment. See the latiq://guidance resource for the supported set."
    )]
    pub extensions: Option<Vec<String>>,
    #[schemars(description = "Calling agent identity (relaxed; defaults to anonymous)")]
    pub agent_id: Option<String>,
}

#[derive(Debug, Deserialize, JsonSchema)]
#[schemars(crate = "rmcp::schemars")]
pub struct PondRefArgs {
    #[schemars(description = "Pond id or name")]
    pub pond: String,
    pub agent_id: Option<String>,
}

#[derive(Debug, Deserialize, JsonSchema)]
#[schemars(crate = "rmcp::schemars")]
pub struct ListArgs {
    pub agent_id: Option<String>,
}

#[derive(Debug, Deserialize, JsonSchema)]
#[schemars(crate = "rmcp::schemars")]
pub struct DropArgs {
    #[schemars(description = "Pond id or name")]
    pub pond: String,
    pub confirm: Option<bool>,
    pub agent_id: Option<String>,
}

#[derive(Debug, Deserialize, JsonSchema)]
#[schemars(crate = "rmcp::schemars")]
pub struct QueryArgs {
    #[schemars(description = "Pond id or name")]
    pub pond: String,
    #[schemars(description = "SQL statement")]
    pub sql: String,
    pub agent_id: Option<String>,
}

#[derive(Debug, Deserialize, JsonSchema)]
#[schemars(crate = "rmcp::schemars")]
pub struct SearchArgs {
    #[schemars(
        description = "Optional search: a tag as `#finance`, a name glob as `sal*`, or a plain substring. Omit for all."
    )]
    pub query: Option<String>,
    pub agent_id: Option<String>,
}

#[derive(Debug, Deserialize, JsonSchema)]
#[schemars(crate = "rmcp::schemars")]
pub struct LoadDatasetArgs {
    #[schemars(description = "Pond id or name to load into")]
    pub pond: String,
    #[schemars(description = "Dataset name (from list_datasets), e.g. `tpch`")]
    pub dataset: String,
    pub agent_id: Option<String>,
}

#[derive(Debug, Deserialize, JsonSchema)]
#[schemars(crate = "rmcp::schemars")]
pub struct CatalogDescribeArgs {
    #[schemars(description = "Pond id or name (the catalog is attached on it transiently)")]
    pub pond: String,
    #[schemars(description = "Catalog name (from list_catalogs), e.g. `lake`")]
    pub catalog: String,
    #[schemars(
        description = "Runtime config + credentials as key→value, e.g. {\"token\":\"<bearer>\"}. Merged over the catalog's stored locator params (these win). NOT stored."
    )]
    pub set: Option<std::collections::HashMap<String, String>>,
    pub agent_id: Option<String>,
}

#[derive(Debug, Deserialize, JsonSchema)]
#[schemars(crate = "rmcp::schemars")]
pub struct CatalogPullArgs {
    #[schemars(description = "Pond id or name to pull into")]
    pub pond: String,
    #[schemars(description = "Catalog name (from list_catalogs), e.g. `lake`")]
    pub catalog: String,
    #[schemars(
        description = "The SQL to materialize, naming the catalog + a target table, e.g. `CREATE TABLE us_orders AS SELECT id,total FROM lake.sales.orders WHERE region='us'`."
    )]
    pub query: String,
    #[schemars(
        description = "Runtime config + credentials as key→value, e.g. {\"token\":\"<bearer>\"}. NOT stored."
    )]
    pub set: Option<std::collections::HashMap<String, String>>,
    pub agent_id: Option<String>,
}

#[derive(Clone)]
pub struct LatiqServer {
    ops: Arc<AgentOps>,
    tool_router: ToolRouter<Self>,
}

impl LatiqServer {
    pub fn new(ops: Arc<AgentOps>) -> Self {
        Self {
            ops,
            tool_router: Self::tool_router(),
        }
    }
}

#[tool_router]
impl LatiqServer {
    /// Allocate a new pond. Optionally name it; Latiq generates a name if omitted.
    /// Returns the pond_id and pond_name. Use list_ponds to discover existing ponds.
    #[tool(
        description = "Allocate a new pond — a private DuckLake workspace you can write to and query with SQL. \
Optionally pass a `name` (Latiq generates one if omitted). Returns `pond_id` + `pond_name`. \
Use this first when you have a task that needs its own data space; use list_ponds to find or join an existing one. \
Then write_query to create tables and load data, and read_query to query. See latiq://guidance.",
        annotations(
            title = "Allocate pond",
            read_only_hint = false,
            destructive_hint = false,
            idempotent_hint = false
        )
    )]
    async fn allocate_pond(&self, Parameters(a): Parameters<AllocateArgs>) -> CallToolResult {
        let id = Identity::claimed(a.agent_id.as_deref());
        let tier = a.tier.as_deref().unwrap_or("medium");
        // Validate requested extensions against the signed/official allowlist
        // before allocating, so a bad name returns a clear, actionable error.
        let exts = match latiq_common::extensions::validate(&a.extensions.unwrap_or_default()) {
            Ok(e) => e,
            Err(msg) => return err_envelope(AgentError::unsupported_extension(msg).envelope()),
        };
        match self.ops.allocate_pond(&id, a.name, "{}", tier, &exts).await {
            Ok(r) => ok_value(serde_json::to_value(r).unwrap_or_default()),
            Err(e) => err_envelope(e.envelope()),
        }
    }

    /// Describe a pond: its metadata + a summary of its tables. Pass pond id or name.
    #[tool(
        description = "Describe a pond: its metadata (name, owner, created_at) plus a summary of its tables. \
Pass `pond` as the id or name. Call this after list_ponds to decide whether to join a pond, or to recall a pond's schema before querying. \
To discover tables/columns in detail, read_query `SHOW TABLES` or `SELECT * FROM information_schema.columns`.",
        annotations(
            title = "Describe pond",
            read_only_hint = true,
            destructive_hint = false,
            idempotent_hint = true
        )
    )]
    async fn describe_pond(&self, Parameters(a): Parameters<PondRefArgs>) -> CallToolResult {
        let id = Identity::claimed(a.agent_id.as_deref());
        match self.ops.describe_pond(&id, &a.pond).await {
            Ok(r) => ok_value(serde_json::to_value(r).unwrap_or_default()),
            Err(e) => err_envelope(e.envelope()),
        }
    }

    /// List all ponds in the deployment.
    #[tool(
        description = "List all ponds in the deployment (id, name, owner). \
Use this to discover existing work before allocating a new pond — multiple agents often collaborate in one pond. \
Follow with describe_pond on a candidate to inspect its tables.",
        annotations(
            title = "List ponds",
            read_only_hint = true,
            destructive_hint = false,
            idempotent_hint = true
        )
    )]
    async fn list_ponds(&self, Parameters(a): Parameters<ListArgs>) -> CallToolResult {
        let id = Identity::claimed(a.agent_id.as_deref());
        match self.ops.list_ponds(&id).await {
            Ok(ponds) => ok_value(serde_json::json!({ "ponds": ponds })),
            Err(e) => err_envelope(e.envelope()),
        }
    }

    /// Drop a pond and reclaim its storage. Destructive.
    #[tool(
        description = "Drop a pond and reclaim its storage. DESTRUCTIVE and not reversible — all tables and data in the pond are removed (the audit trail is preserved). \
Only drop a pond when its work is finished. Do NOT drop a pond other agents may still be using; check list_ponds first.",
        annotations(
            title = "Drop pond",
            read_only_hint = false,
            destructive_hint = true,
            idempotent_hint = true
        )
    )]
    async fn drop_pond(&self, Parameters(a): Parameters<DropArgs>) -> CallToolResult {
        let id = Identity::claimed(a.agent_id.as_deref());
        match self
            .ops
            .drop_pond(&id, &a.pond, a.confirm.unwrap_or(false))
            .await
        {
            Ok(()) => ok_value(serde_json::json!({ "status": "dropped", "pond": a.pond })),
            Err(e) => err_envelope(e.envelope()),
        }
    }

    /// Run a read-only SQL query (SELECT / read-only metadata) against a pond.
    /// For writes/DDL use write_query. Results are bounded by the inline cap.
    #[tool(
        description = "Run a read-only SQL query (SELECT, or read-only metadata like SHOW/DESCRIBE) against a pond. \
For INSERT/UPDATE/DELETE/DDL use write_query instead — those are rejected here. \
Latiq prefers ANSI SQL; DuckDB extensions are tolerated. Discover tables with `SHOW TABLES` (or `information_schema.tables`/`information_schema.columns`) first. \
Do: add WHERE/LIMIT on selective columns and call explain_query if unsure of cost. Don't: unbounded `SELECT *` on large tables — results are capped (~10k rows); narrow, aggregate, or materialize with CREATE TABLE AS SELECT. \
Returns `{columns, rows, statement, status, _meta}`; read `_meta` to self-correct. See latiq://recipes/large-results.",
        annotations(
            title = "Read query",
            read_only_hint = true,
            destructive_hint = false,
            idempotent_hint = true
        )
    )]
    async fn read_query(&self, Parameters(a): Parameters<QueryArgs>) -> CallToolResult {
        let id = Identity::claimed(a.agent_id.as_deref());
        // Reads ride the Arrow internal hop, collected to the neutral result here.
        match self.ops.read_collected(&id, &a.pond, &a.sql).await {
            Ok(qr) => ok_query("read_query", qr),
            Err(e) => err_envelope(e.envelope()),
        }
    }

    /// Run a write/DDL SQL statement (INSERT/UPDATE/DELETE/CREATE/CTAS) against a
    /// pond. Writes are attributed to your agent identity.
    #[tool(
        description = "Run a write or DDL SQL statement (INSERT/UPDATE/DELETE/CREATE/DROP/ALTER/CREATE TABLE AS SELECT) against a pond. \
Your writes are attributed to your agent identity (queryable via `SELECT author, commit_message FROM ducklake_snapshots('<pond>')`). \
Marked destructive because it CAN delete data; clients may require approval. \
Load external public files directly: `CREATE TABLE t AS SELECT * FROM read_csv('https://…')` or `… FROM 's3://bucket/f.parquet'` (public/anonymous only). \
Do: add column COMMENTs so other agents understand your tables. See latiq://recipes/schema-design and latiq://recipes/data-ingestion-m1.",
        annotations(
            title = "Write query",
            read_only_hint = false,
            destructive_hint = true,
            idempotent_hint = false
        )
    )]
    async fn write_query(&self, Parameters(a): Parameters<QueryArgs>) -> CallToolResult {
        let id = Identity::claimed(a.agent_id.as_deref());
        match self.ops.write_query(&id, &a.pond, &a.sql).await {
            Ok(qr) => ok_query("write_query", qr),
            Err(e) => err_envelope(e.envelope()),
        }
    }

    /// Estimate a query's cost without running it. Call before read/write_query to
    /// reason about scan size; refine, then run.
    #[tool(
        description = "Plan a query WITHOUT running it — returns the DuckLake/DuckDB plan so you can reason about cost before executing. \
Use it before an expensive read_query/write_query: inspect the plan, refine (add a WHERE on a selective column, a LIMIT, or pre-aggregate), then run. \
This makes you thrifty rather than greedy. Read-only and side-effect-free.",
        annotations(
            title = "Explain query",
            read_only_hint = true,
            destructive_hint = false,
            idempotent_hint = true
        )
    )]
    async fn explain_query(&self, Parameters(a): Parameters<QueryArgs>) -> CallToolResult {
        let id = Identity::claimed(a.agent_id.as_deref());
        match self.ops.explain_query(&id, &a.pond, &a.sql).await {
            Ok(er) => ok_explain(er),
            Err(e) => err_envelope(e.envelope()),
        }
    }

    /// Discover curated datasets (simple public files) you can copy into a pond.
    #[tool(
        description = "Browse the catalog of curated DATASETS — simple public files (parquet/CSV) an operator registered. \
Returns each dataset's `name`, `tables`, `tags`, and `description`. \
Use this BEFORE load_dataset to find what's available; pass `query` to filter (`#tag`, a `name*` glob, or a substring). \
Datasets are for ready-made files; for an external database/lakehouse use list_catalogs + pull_catalog instead. See latiq://recipes/external-data.",
        annotations(
            title = "List datasets",
            read_only_hint = true,
            destructive_hint = false,
            idempotent_hint = true
        )
    )]
    async fn list_datasets(&self, Parameters(a): Parameters<SearchArgs>) -> CallToolResult {
        match self
            .ops
            .list_datasets(a.query.as_deref().unwrap_or(""))
            .await
        {
            Ok(datasets) => ok_value(serde_json::json!({ "datasets": datasets })),
            Err(e) => err_envelope(e.envelope()),
        }
    }

    /// Copy a dataset's tables into a pond (one table each). Pick a name from list_datasets.
    #[tool(
        description = "Copy a DATASET's tables into a pond — one real table per file, materialized into the pond's DuckLake. \
Pass `dataset` (a name from list_datasets) and the target `pond`. After this, query the new tables with read_query like any other table. \
This is a WRITE (it creates tables, attributed to you). For an external database/lakehouse, use pull_catalog instead. See latiq://recipes/external-data.",
        annotations(
            title = "Load dataset",
            read_only_hint = false,
            destructive_hint = true,
            idempotent_hint = false
        )
    )]
    async fn load_dataset(&self, Parameters(a): Parameters<LoadDatasetArgs>) -> CallToolResult {
        let id = Identity::claimed(a.agent_id.as_deref());
        match self.ops.load_dataset(&id, &a.pond, &a.dataset).await {
            Ok(r) => ok_value(serde_json::to_value(r).unwrap_or_default()),
            Err(e) => err_envelope(e.envelope()),
        }
    }

    /// Discover registered external catalogs (iceberg/…) you can pull data from.
    #[tool(
        description = "Browse registered external CATALOGS — databases/lakehouses (iceberg today) an operator registered. \
Returns each catalog's `name`, `type`, `tags`, and `description`. \
You don't know a catalog's tables until you look: call describe_catalog next. Then pull_catalog to copy a subset into a pond. \
Pass `query` to filter (`#tag`, glob, substring). Catalogs are for external sources; for ready-made files use list_datasets. See latiq://recipes/external-data.",
        annotations(
            title = "List catalogs",
            read_only_hint = true,
            destructive_hint = false,
            idempotent_hint = true
        )
    )]
    async fn list_catalogs(&self, Parameters(a): Parameters<SearchArgs>) -> CallToolResult {
        match self
            .ops
            .list_catalogs(a.query.as_deref().unwrap_or(""))
            .await
        {
            Ok(catalogs) => ok_value(serde_json::json!({ "catalogs": catalogs })),
            Err(e) => err_envelope(e.envelope()),
        }
    }

    /// List an external catalog's tables (transient attach on a pond). Pass creds via `set`.
    #[tool(
        description = "List an external catalog's tables/columns — Latiq transiently attaches it on `pond`, reads its metadata, and detaches. \
Returns `{catalog, tables:[{schema, table}]}`. Use this to learn what to SELECT before pull_catalog. \
Credentials and config go in `set` (e.g. {\"token\":\"<bearer>\"}); they're used for this call only and never stored. \
If a credential is missing the attach fails with a clear error — read it and retry with the right `set`. See latiq://recipes/external-data.",
        annotations(
            title = "Describe catalog",
            read_only_hint = true,
            destructive_hint = false,
            idempotent_hint = true
        )
    )]
    async fn describe_catalog(
        &self,
        Parameters(a): Parameters<CatalogDescribeArgs>,
    ) -> CallToolResult {
        let id = Identity::claimed(a.agent_id.as_deref());
        let set = a.set.unwrap_or_default().into_iter().collect();
        match self
            .ops
            .catalog_describe(&id, &a.pond, &a.catalog, set)
            .await
        {
            Ok(tables) => {
                let rows: Vec<_> = tables
                    .into_iter()
                    .map(|(schema, table)| serde_json::json!({"schema": schema, "table": table}))
                    .collect();
                ok_value(serde_json::json!({ "catalog": a.catalog, "tables": rows }))
            }
            Err(e) => err_envelope(e.envelope()),
        }
    }

    /// Pull a subset of an external catalog into a pond: transient attach → your query → detach.
    #[tool(
        description = "Pull data from an external catalog INTO a pond in one shot: Latiq attaches the catalog (with your creds), runs your `query`, then detaches. \
External catalogs are never queried live — you pull what you need into the pond, then work there. \
Write `query` as a CREATE TABLE that names the catalog, e.g. `CREATE TABLE us AS SELECT id,total FROM lake.sales.orders WHERE region='us'` — DuckDB downloads only the columns/rows you select. \
Use describe_catalog first to learn the table names. Put credentials in `set` (e.g. {\"token\":\"<bearer>\"}) — used once, never stored. This is a WRITE (creates a table in the pond). See latiq://recipes/external-data.",
        annotations(
            title = "Pull from catalog",
            read_only_hint = false,
            destructive_hint = true,
            idempotent_hint = false
        )
    )]
    async fn pull_catalog(&self, Parameters(a): Parameters<CatalogPullArgs>) -> CallToolResult {
        let id = Identity::claimed(a.agent_id.as_deref());
        let set = a.set.unwrap_or_default().into_iter().collect();
        match self
            .ops
            .catalog_pull(&id, &a.pond, &a.catalog, &a.query, set)
            .await
        {
            Ok(r) => ok_value(serde_json::to_value(r).unwrap_or_default()),
            Err(e) => err_envelope(e.envelope()),
        }
    }
}

#[tool_handler(router = self.tool_router)]
impl ServerHandler for LatiqServer {
    fn get_info(&self) -> ServerInfo {
        ServerInfo::new(
            ServerCapabilities::builder()
                .enable_tools()
                .enable_resources()
                .enable_prompts()
                .build(),
        )
        .with_instructions(
            "Latiq — the agent-native data pond. Allocate a pond (a private DuckLake workspace), \
write/read SQL with native attribution. \
FIRST MOVES: list_ponds to find or join a workspace, or allocate_pond for a new one; then write_query/read_query. \
TO BRING IN EXTERNAL DATA: list_datasets + load_dataset for curated public files; or list_catalogs → describe_catalog → \
pull_catalog for an external database/lakehouse (iceberg) — you pull a subset into the pond, then work there \
(external catalogs are never queried live). \
Read latiq://guidance to start and latiq://recipes/external-data for the data-loading flow; tool errors carry \
suggest/see links to latiq:// resources. Prompts provide SOPs for common multi-agent workflows.",
        )
    }

    async fn list_resources(
        &self,
        _request: Option<PaginatedRequestParams>,
        _ctx: RequestContext<RoleServer>,
    ) -> Result<ListResourcesResult, McpError> {
        Ok(ListResourcesResult::with_all_items(
            resources::list_resources(),
        ))
    }

    async fn read_resource(
        &self,
        request: ReadResourceRequestParams,
        _ctx: RequestContext<RoleServer>,
    ) -> Result<ReadResourceResult, McpError> {
        resources::read_resource(&request.uri).ok_or_else(|| {
            McpError::resource_not_found(format!("unknown resource: {}", request.uri), None)
        })
    }

    async fn list_prompts(
        &self,
        _request: Option<PaginatedRequestParams>,
        _ctx: RequestContext<RoleServer>,
    ) -> Result<ListPromptsResult, McpError> {
        Ok(ListPromptsResult::with_all_items(resources::list_prompts()))
    }

    async fn get_prompt(
        &self,
        request: GetPromptRequestParams,
        _ctx: RequestContext<RoleServer>,
    ) -> Result<GetPromptResult, McpError> {
        let args = request.arguments.unwrap_or_default();
        resources::get_prompt(&request.name, &args).ok_or_else(|| {
            McpError::invalid_params(format!("unknown prompt: {}", request.name), None)
        })
    }
}

/// Serve the MCP Streamable-HTTP surface at `/mcp` on `addr`.
pub async fn serve_mcp(
    addr: SocketAddr,
    ops: Arc<AgentOps>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let listener = tokio::net::TcpListener::bind(addr).await?;
    serve_mcp_with_listener(listener, ops).await
}

/// Serve the MCP surface on an already-bound listener (no port race; used by the
/// integration harness).
pub async fn serve_mcp_with_listener(
    listener: tokio::net::TcpListener,
    ops: Arc<AgentOps>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let service = StreamableHttpService::new(
        move || Ok(LatiqServer::new(ops.clone())),
        LocalSessionManager::default().into(),
        StreamableHttpServerConfig::default(),
    );
    let router = axum::Router::new().nest_service("/mcp", service);
    axum::serve(listener, router).await?;
    Ok(())
}
