//! The Latiq MCP server: exposes the 7 agent tools over rmcp Streamable-HTTP.
//! Identity is relaxed (Slice 0+): taken from an optional `agent_id` argument,
//! defaulting to anonymous. (M6 moves this to the `X-Latiq-Agent-Id` header.)
use crate::encode::{err_envelope, ok_explain, ok_query, ok_value};
use latiq_agent_core::AgentOps;
use latiq_common::Identity;
use rmcp::handler::server::{router::tool::ToolRouter, wrapper::Parameters};
use rmcp::model::{CallToolResult, ServerCapabilities, ServerInfo};
use rmcp::schemars::JsonSchema;
use rmcp::transport::streamable_http_server::{
    session::local::LocalSessionManager, StreamableHttpServerConfig, StreamableHttpService,
};
use rmcp::{tool, tool_handler, tool_router, ServerHandler};
use serde::Deserialize;
use std::net::SocketAddr;
use std::sync::Arc;

#[derive(Debug, Deserialize, JsonSchema)]
#[schemars(crate = "rmcp::schemars")]
pub struct AllocateArgs {
    #[schemars(description = "Optional pond name; Latiq generates one if omitted")]
    pub name: Option<String>,
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
        match self.ops.allocate_pond(&id, a.name, "{}").await {
            Ok(r) => ok_value(serde_json::to_value(r).unwrap_or_default()),
            Err(e) => err_envelope(e.envelope()),
        }
    }

    /// Describe a pond: its metadata + a summary of its tables. Pass pond id or name.
    #[tool(
        description = "Describe a pond: its metadata (name, owner, created_at) plus a summary of its tables. \
Pass `pond` as the id or name. Call this after list_ponds to decide whether to join a pond, or to recall a pond's schema before querying. \
To discover columns/comments in detail, read_query `SELECT * FROM _latiq.tables_summary`.",
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
        match self.ops.drop_pond(&id, &a.pond).await {
            Ok(()) => ok_value(serde_json::json!({ "status": "dropped", "pond": a.pond })),
            Err(e) => err_envelope(e.envelope()),
        }
    }

    /// Run a read-only SQL query (SELECT / read-only metadata) against a pond.
    /// For writes/DDL use write_query. Results are bounded by the inline cap.
    #[tool(
        description = "Run a read-only SQL query (SELECT, or read-only metadata like SHOW/DESCRIBE) against a pond. \
For INSERT/UPDATE/DELETE/DDL use write_query instead — those are rejected here. \
Latiq prefers ANSI SQL; DuckDB extensions are tolerated. Discover tables with `SELECT name, comment FROM _latiq.tables_summary` first. \
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
        match self.ops.read_query(&id, &a.pond, &a.sql).await {
            Ok(qr) => ok_query("read_query", qr),
            Err(e) => err_envelope(e.envelope()),
        }
    }

    /// Run a write/DDL SQL statement (INSERT/UPDATE/DELETE/CREATE/CTAS) against a
    /// pond. Writes are attributed to your agent identity.
    #[tool(
        description = "Run a write or DDL SQL statement (INSERT/UPDATE/DELETE/CREATE/DROP/ALTER/CREATE TABLE AS SELECT) against a pond. \
Your writes are attributed to your agent identity (queryable via `SELECT author FROM _latiq.attribution`). \
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
}

#[tool_handler(router = self.tool_router)]
impl ServerHandler for LatiqServer {
    fn get_info(&self) -> ServerInfo {
        ServerInfo::new(ServerCapabilities::builder().enable_tools().build())
            .with_instructions("Latiq — the agent-native data pond. Allocate a pond, write/read SQL, attach nothing (federation is later). Errors carry suggest/see guidance.")
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
