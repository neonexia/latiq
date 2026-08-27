//! The Latiq MCP server: exposes the agent tools (pond + query + dataset/catalog)
//! over rmcp Streamable-HTTP.
//!
//! Identity arrives in the TRANSPORT, never in a tool argument: the claimed leaf
//! is the `latiq-agent-id` HTTP header and a verified principal is an
//! `Authorization: Bearer <jwt>`. A tool argument would be typed by the model
//! itself, which is tolerable for a claimed value and unacceptable for a
//! verified one — so there is no `agent_id` argument at all.
//!
//! With a verifier configured this surface is an OAuth 2.1 resource server: it
//! publishes RFC 9728 metadata at `/.well-known/oauth-protected-resource`,
//! answers any request whose bearer token is missing OR invalid with a 401 +
//! `WWW-Authenticate` challenge, and verifies every request in one layer in
//! front of the router (so `initialize` and the discovery methods are covered
//! too, not just tool calls). Without one, identity stays relaxed (claimed,
//! default anonymous) — the embedded and dev path.
use crate::encode::{err_envelope, ok_explain, ok_query, ok_value};
use crate::resources;
use latiq_agent_core::{with_bearer, AgentError, AgentOps};
use latiq_auth::metadata::{challenge_header, ProtectedResourceMetadata};
use latiq_auth::Verifier;
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
}

#[derive(Debug, Deserialize, JsonSchema)]
#[schemars(crate = "rmcp::schemars")]
pub struct PondRefArgs {
    #[schemars(description = "Pond id or name")]
    pub pond: String,
}

#[derive(Debug, Deserialize, JsonSchema)]
#[schemars(crate = "rmcp::schemars")]
pub struct DropArgs {
    #[schemars(description = "Pond id or name")]
    pub pond: String,
    pub confirm: Option<bool>,
}

#[derive(Debug, Deserialize, JsonSchema)]
#[schemars(crate = "rmcp::schemars")]
pub struct QueryArgs {
    #[schemars(description = "Pond id or name")]
    pub pond: String,
    #[schemars(description = "SQL statement")]
    pub sql: String,
}

#[derive(Debug, Deserialize, JsonSchema)]
#[schemars(crate = "rmcp::schemars")]
pub struct SearchArgs {
    #[schemars(
        description = "Optional search: a tag as `#finance`, a name glob as `sal*`, or a plain substring. Omit for all."
    )]
    pub query: Option<String>,
}

#[derive(Debug, Deserialize, JsonSchema)]
#[schemars(crate = "rmcp::schemars")]
pub struct LoadDatasetArgs {
    #[schemars(description = "Pond id or name to load into")]
    pub pond: String,
    #[schemars(description = "Dataset name (from list_datasets), e.g. `tpch`")]
    pub dataset: String,
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
}

/// The HTTP header carrying the CLAIMED leaf id. Same name as the gRPC metadata
/// key on the Data surface, so one deployment has one spelling.
const AGENT_ID_HEADER: &str = "latiq-agent-id";

/// RFC 9728's fixed location for the protected-resource metadata document.
pub const PROTECTED_RESOURCE_PATH: &str = "/.well-known/oauth-protected-resource";

#[derive(Clone)]
pub struct LatiqServer {
    ops: Arc<AgentOps>,
    verifier: Option<Arc<Verifier>>,
    tool_router: ToolRouter<Self>,
}

impl LatiqServer {
    pub fn new(ops: Arc<AgentOps>) -> Self {
        Self {
            ops,
            verifier: None,
            tool_router: Self::tool_router(),
        }
    }

    /// Require verified bearer tokens on this surface. `None` keeps the relaxed
    /// (embedded / dev) path.
    pub fn with_verifier(mut self, verifier: Option<Arc<Verifier>>) -> Self {
        self.verifier = verifier;
        self
    }

    /// Identity for one MCP request. The claimed leaf comes from the
    /// `latiq-agent-id` HTTP header (NOT a tool argument -- the model must not
    /// be able to type it).
    ///
    /// With a verifier configured this is a LOOKUP, not a decision: the auth
    /// layer in front of the router has already verified the token and stashed
    /// the resulting `VerifiedCaller` in the request's extensions. Verifying
    /// here instead would leave every non-tool method (`initialize`,
    /// `tools/list`, `resources/read`, …) unchecked, and would answer a forged
    /// or expired token with a JSON-RPC error inside HTTP 200 — which no MCP
    /// client can act on, because client-side re-auth keys off a real 401.
    ///
    /// The token is returned ONLY when a verifier produced it, exactly as on the
    /// Data surface: a node that never opted into auth must not start capturing
    /// whatever `authorization` header a client happens to send and replaying it
    /// to a peer over the internal channel.
    ///
    /// rmcp injects the request's `http::request::Parts` on the POST path (which
    /// is where every tool call lands); the SSE/GET stream carries none, so this
    /// is per-request rather than per-session by construction.
    fn identity(
        &self,
        ctx: &RequestContext<RoleServer>,
    ) -> Result<(Identity, Option<String>), McpError> {
        let parts = ctx.extensions.get::<http::request::Parts>();
        let Some(_verifier) = self.verifier.as_ref() else {
            let claimed = parts
                .and_then(|p| p.headers.get(AGENT_ID_HEADER))
                .and_then(|v| v.to_str().ok());
            return Ok((Identity::claimed(claimed), None));
        };
        // Unreachable through the HTTP surface (the layer 401s first), so this
        // is the fail-closed branch for any path that reaches a handler without
        // passing the layer -- never a fallback to a claimed identity.
        parts
            .and_then(|p| p.extensions.get::<VerifiedCaller>())
            .cloned()
            .map(|c| (c.identity, Some(c.token)))
            .ok_or_else(|| McpError::invalid_request("a bearer token is required", None))
    }
}

/// The outcome of verifying one request's bearer token, handed from the auth
/// layer to the handler through the request's extensions. Carrying the decision
/// (rather than the raw token) is what keeps exactly ONE place validating.
#[derive(Clone)]
struct VerifiedCaller {
    identity: Identity,
    /// The original token, replayed on a node-to-node hop so the owning node
    /// verifies it itself.
    token: String,
}

/// 401 + the RFC 9728 challenge. Deliberately bodiless and fixed: an
/// unauthenticated caller must not be able to probe our issuer list or key
/// endpoints by reading error text. The detail goes to the operator's log.
fn unauthorized(challenge: &str) -> axum::response::Response {
    let mut res = axum::response::Response::new(axum::body::Body::empty());
    *res.status_mut() = http::StatusCode::UNAUTHORIZED;
    if let Ok(v) = http::HeaderValue::from_str(challenge) {
        res.headers_mut().insert(http::header::WWW_AUTHENTICATE, v);
    }
    res
}

/// The auth layer: verifies the bearer token for EVERY request to this surface
/// and stashes the result for the handler.
///
/// It sits in front of the router rather than inside the tool handlers for two
/// reasons. First, coverage: `initialize`, `tools/list`, `resources/read` and
/// friends never build an `Identity`, so a handler-only check would let an
/// unauthenticated caller complete the handshake, enumerate the tool catalogue,
/// read every `latiq://` resource, and allocate an rmcp session (plus its
/// worker task) per request. Second, protocol: a missing OR invalid token has
/// to produce a real 401 with a `WWW-Authenticate` challenge — that is what
/// makes an MCP client re-authenticate instead of wedging on an opaque
/// JSON-RPC error when its token expires mid-session.
async fn verify_bearer(
    verifier: Arc<Verifier>,
    challenge: String,
    mut req: axum::extract::Request,
    next: axum::middleware::Next,
) -> axum::response::Response {
    // The metadata document is exempt, or discovery is impossible: a client with
    // no token could never learn which authorization server to ask.
    if req.uri().path() == PROTECTED_RESOURCE_PATH {
        return next.run(req).await;
    }
    let claimed = req
        .headers()
        .get(AGENT_ID_HEADER)
        .and_then(|v| v.to_str().ok())
        .map(String::from);
    // One parser for every surface (`latiq_auth::bearer`) — a second copy of a
    // security-relevant parser drifts.
    let token = req
        .headers()
        .get(http::header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(latiq_auth::bearer)
        .map(String::from);
    let Some(token) = token else {
        return unauthorized(&challenge);
    };
    match verifier.verify(&token, claimed.as_deref()).await {
        Ok(identity) => {
            req.extensions_mut()
                .insert(VerifiedCaller { identity, token });
            next.run(req).await
        }
        Err(e) => {
            tracing::debug!(error = %e, "bearer token rejected");
            unauthorized(&challenge)
        }
    }
}

/// The public MCP URL to advertise, from the endpoint this node advertises to
/// its peers (which already carries `--advertise-addr`) and the port the MCP
/// surface is bound to.
///
/// The bound address is NOT usable for this: every compose file we ship binds
/// `0.0.0.0`, so a challenge derived from it would point clients at
/// `http://0.0.0.0:51402/…` and declare a `resource` identifier no conforming
/// client can match against the host it dialled.
pub fn advertised_mcp_url(advertised_endpoint: &str, mcp_addr: SocketAddr) -> Option<String> {
    let mut url = url::Url::parse(advertised_endpoint).ok()?;
    url.set_port(Some(mcp_addr.port())).ok()?;
    url.set_path("/mcp");
    Some(url.to_string())
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
    async fn allocate_pond(
        &self,
        Parameters(a): Parameters<AllocateArgs>,
        ctx: RequestContext<RoleServer>,
    ) -> Result<CallToolResult, McpError> {
        let (id, tok) = self.identity(&ctx)?;
        let tier = a.tier.as_deref().unwrap_or("medium");
        // Validate requested extensions against the signed/official allowlist
        // before allocating, so a bad name returns a clear, actionable error.
        let exts = match latiq_common::extensions::validate(&a.extensions.unwrap_or_default()) {
            Ok(e) => e,
            Err(msg) => {
                return Ok(err_envelope(
                    AgentError::unsupported_extension(msg).envelope(),
                ))
            }
        };
        Ok(with_bearer(tok, async {
            match self.ops.allocate_pond(&id, a.name, "{}", tier, &exts).await {
                Ok(r) => ok_value(serde_json::to_value(r).unwrap_or_default()),
                Err(e) => err_envelope(e.envelope()),
            }
        })
        .await)
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
    async fn describe_pond(
        &self,
        Parameters(a): Parameters<PondRefArgs>,
        ctx: RequestContext<RoleServer>,
    ) -> Result<CallToolResult, McpError> {
        let (id, tok) = self.identity(&ctx)?;
        Ok(with_bearer(tok, async {
            match self.ops.describe_pond(&id, &a.pond).await {
                Ok(r) => ok_value(serde_json::to_value(r).unwrap_or_default()),
                Err(e) => err_envelope(e.envelope()),
            }
        })
        .await)
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
    async fn list_ponds(
        &self,
        ctx: RequestContext<RoleServer>,
    ) -> Result<CallToolResult, McpError> {
        let (id, tok) = self.identity(&ctx)?;
        Ok(with_bearer(tok, async {
            match self.ops.list_ponds(&id).await {
                Ok(ponds) => ok_value(serde_json::json!({ "ponds": ponds })),
                Err(e) => err_envelope(e.envelope()),
            }
        })
        .await)
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
    async fn drop_pond(
        &self,
        Parameters(a): Parameters<DropArgs>,
        ctx: RequestContext<RoleServer>,
    ) -> Result<CallToolResult, McpError> {
        let (id, tok) = self.identity(&ctx)?;
        Ok(with_bearer(tok, async {
            match self
                .ops
                .drop_pond(&id, &a.pond, a.confirm.unwrap_or(false))
                .await
            {
                Ok(()) => ok_value(serde_json::json!({ "status": "dropped", "pond": a.pond })),
                Err(e) => err_envelope(e.envelope()),
            }
        })
        .await)
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
    async fn read_query(
        &self,
        Parameters(a): Parameters<QueryArgs>,
        ctx: RequestContext<RoleServer>,
    ) -> Result<CallToolResult, McpError> {
        let (id, tok) = self.identity(&ctx)?;
        // Reads ride the Arrow internal hop, collected to the neutral result here.
        Ok(with_bearer(tok, async {
            match self.ops.read_collected(&id, &a.pond, &a.sql).await {
                Ok(qr) => ok_query("read_query", qr),
                Err(e) => err_envelope(e.envelope()),
            }
        })
        .await)
    }

    /// Run a write/DDL SQL statement (INSERT/UPDATE/DELETE/CREATE/CTAS) against a
    /// pond. Writes are attributed to your agent identity.
    #[tool(
        description = "Run a write or DDL SQL statement (INSERT/UPDATE/DELETE/CREATE/DROP/ALTER/CREATE TABLE AS SELECT) against a pond. \
Your writes are attributed to your agent identity (queryable via `SELECT author, commit_message, commit_extra_info FROM ducklake_snapshots('<pond>')` — `commit_extra_info` carries the verified-vs-claimed evidence). \
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
    async fn write_query(
        &self,
        Parameters(a): Parameters<QueryArgs>,
        ctx: RequestContext<RoleServer>,
    ) -> Result<CallToolResult, McpError> {
        let (id, tok) = self.identity(&ctx)?;
        Ok(with_bearer(tok, async {
            match self.ops.write_query(&id, &a.pond, &a.sql).await {
                Ok(qr) => ok_query("write_query", qr),
                Err(e) => err_envelope(e.envelope()),
            }
        })
        .await)
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
    async fn explain_query(
        &self,
        Parameters(a): Parameters<QueryArgs>,
        ctx: RequestContext<RoleServer>,
    ) -> Result<CallToolResult, McpError> {
        let (id, tok) = self.identity(&ctx)?;
        Ok(with_bearer(tok, async {
            match self.ops.explain_query(&id, &a.pond, &a.sql).await {
                Ok(er) => ok_explain(er),
                Err(e) => err_envelope(e.envelope()),
            }
        })
        .await)
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
    async fn list_datasets(
        &self,
        Parameters(a): Parameters<SearchArgs>,
        ctx: RequestContext<RoleServer>,
    ) -> Result<CallToolResult, McpError> {
        // No identity reaches the op (the dataset catalogue is
        // deployment-wide), but the token is still required — and scoped like
        // every other tool, so this stays symmetric with the Data surface
        // rather than relying on "this one happens never to forward".
        let (_id, tok) = self.identity(&ctx)?;
        Ok(with_bearer(tok, async {
            match self
                .ops
                .list_datasets(a.query.as_deref().unwrap_or(""))
                .await
            {
                Ok(datasets) => ok_value(serde_json::json!({ "datasets": datasets })),
                Err(e) => err_envelope(e.envelope()),
            }
        })
        .await)
    }

    /// Copy a dataset's tables into a pond, under a schema named after the dataset. Pick a name from list_datasets.
    #[tool(
        description = "Copy a DATASET's tables into a pond — materialized into the pond's DuckLake under a SCHEMA named after the dataset. \
Pass `dataset` (a name from list_datasets) and the target `pond`. The response returns `schema` and schema-qualified `tables`; \
query them as `<dataset>.<table>` with read_query (e.g. `SELECT * FROM tpch.lineitem`). \
This is a WRITE (it creates a schema + tables, attributed to you). For an external database/lakehouse, use pull_catalog instead. See latiq://recipes/external-data.",
        annotations(
            title = "Load dataset",
            read_only_hint = false,
            destructive_hint = true,
            idempotent_hint = false
        )
    )]
    async fn load_dataset(
        &self,
        Parameters(a): Parameters<LoadDatasetArgs>,
        ctx: RequestContext<RoleServer>,
    ) -> Result<CallToolResult, McpError> {
        let (id, tok) = self.identity(&ctx)?;
        Ok(with_bearer(tok, async {
            match self.ops.load_dataset(&id, &a.pond, &a.dataset).await {
                Ok(r) => ok_value(serde_json::to_value(r).unwrap_or_default()),
                Err(e) => err_envelope(e.envelope()),
            }
        })
        .await)
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
    async fn list_catalogs(
        &self,
        Parameters(a): Parameters<SearchArgs>,
        ctx: RequestContext<RoleServer>,
    ) -> Result<CallToolResult, McpError> {
        // Required + scoped for the same reasons as list_datasets, though no
        // identity reaches the op.
        let (_id, tok) = self.identity(&ctx)?;
        Ok(with_bearer(tok, async {
            match self
                .ops
                .list_catalogs(a.query.as_deref().unwrap_or(""))
                .await
            {
                Ok(catalogs) => ok_value(serde_json::json!({ "catalogs": catalogs })),
                Err(e) => err_envelope(e.envelope()),
            }
        })
        .await)
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
        ctx: RequestContext<RoleServer>,
    ) -> Result<CallToolResult, McpError> {
        let (id, tok) = self.identity(&ctx)?;
        let set = a.set.unwrap_or_default().into_iter().collect();
        Ok(with_bearer(tok, async {
            match self
                .ops
                .catalog_describe(&id, &a.pond, &a.catalog, set)
                .await
            {
                Ok(tables) => {
                    let rows: Vec<_> = tables
                        .into_iter()
                        .map(
                            |(schema, table)| serde_json::json!({"schema": schema, "table": table}),
                        )
                        .collect();
                    ok_value(serde_json::json!({ "catalog": a.catalog, "tables": rows }))
                }
                Err(e) => err_envelope(e.envelope()),
            }
        })
        .await)
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
    async fn pull_catalog(
        &self,
        Parameters(a): Parameters<CatalogPullArgs>,
        ctx: RequestContext<RoleServer>,
    ) -> Result<CallToolResult, McpError> {
        let (id, tok) = self.identity(&ctx)?;
        let set = a.set.unwrap_or_default().into_iter().collect();
        Ok(with_bearer(tok, async {
            match self
                .ops
                .catalog_pull(&id, &a.pond, &a.catalog, &a.query, set)
                .await
            {
                Ok(r) => ok_value(serde_json::to_value(r).unwrap_or_default()),
                Err(e) => err_envelope(e.envelope()),
            }
        })
        .await)
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

/// Serve the MCP Streamable-HTTP surface at `/mcp` on `addr`. `verifier` is
/// built once at startup and shared — never per request.
///
/// `public_url` is the URL clients actually dial (see `advertised_mcp_url`); the
/// bound address is only the fallback, and is wrong on every deployment that
/// binds `0.0.0.0` or sits behind a TLS-terminating gateway.
pub async fn serve_mcp(
    addr: SocketAddr,
    ops: Arc<AgentOps>,
    verifier: Option<Arc<Verifier>>,
    public_url: Option<String>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let listener = tokio::net::TcpListener::bind(addr).await?;
    serve_mcp_with_listener(listener, ops, verifier, public_url).await
}

/// Serve the MCP surface on an already-bound listener (no port race; used by the
/// integration harness).
pub async fn serve_mcp_with_listener(
    listener: tokio::net::TcpListener,
    ops: Arc<AgentOps>,
    verifier: Option<Arc<Verifier>>,
    public_url: Option<String>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let mcp_verifier = verifier.clone();
    let service = StreamableHttpService::new(
        move || Ok(LatiqServer::new(ops.clone()).with_verifier(mcp_verifier.clone())),
        LocalSessionManager::default().into(),
        StreamableHttpServerConfig::default(),
    );
    let mut router = axum::Router::new().nest_service("/mcp", service);

    if let Some(v) = verifier {
        // The URL a client dials, NOT the socket we bound. `local_addr` is only
        // the fallback for a deployment that advertises nothing (and is right
        // just when the two coincide, as they do on loopback in tests).
        let resource = match public_url {
            Some(u) => u,
            None => format!("http://{}/mcp", listener.local_addr()?),
        };
        // The document sits at the well-known path on the same origin. Derived
        // from `resource` so the two can never disagree; a gateway that rewrites
        // paths (rather than only the host) would need this configured.
        let metadata_url = format!(
            "{}{PROTECTED_RESOURCE_PATH}",
            resource.strip_suffix("/mcp").unwrap_or(&resource)
        );
        // ALL configured issuers, from the verifier's NORMALIZED config, so the
        // document advertises exactly what is enforced.
        let issuers: Vec<String> = v
            .config()
            .issuers
            .iter()
            .map(|i| i.issuer.clone())
            .collect();
        let doc = serde_json::to_value(ProtectedResourceMetadata::new(&resource, &issuers))
            .unwrap_or_default();
        let challenge = challenge_header(&metadata_url);

        router = router
            .route(
                PROTECTED_RESOURCE_PATH,
                axum::routing::get(move || {
                    let doc = doc.clone();
                    async move { axum::Json(doc) }
                }),
            )
            // Every request to this surface passes through one verification.
            // Applied AFTER the well-known route is registered, so it covers
            // that route too — `verify_bearer` exempts it by path rather than by
            // layer ordering.
            .layer(axum::middleware::from_fn(
                move |req: axum::extract::Request, next: axum::middleware::Next| {
                    verify_bearer(v.clone(), challenge.clone(), req, next)
                },
            ));
    }

    axum::serve(listener, router).await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::advertised_mcp_url;

    #[test]
    fn advertised_url_takes_the_host_from_the_advertise_endpoint() {
        // The bug this exists for: the node binds 0.0.0.0 but advertises a name.
        let bound = "0.0.0.0:51402".parse().unwrap();
        assert_eq!(
            advertised_mcp_url("http://pond-node-1:51401", bound).as_deref(),
            Some("http://pond-node-1:51402/mcp")
        );
    }

    #[test]
    fn advertised_url_keeps_the_scheme_and_handles_ipv6() {
        let bound = "[::]:51402".parse().unwrap();
        assert_eq!(
            advertised_mcp_url("https://gateway.example:443", bound).as_deref(),
            Some("https://gateway.example:51402/mcp")
        );
        assert_eq!(
            advertised_mcp_url("http://[::1]:51401", bound).as_deref(),
            Some("http://[::1]:51402/mcp")
        );
    }

    #[test]
    fn advertised_url_is_none_for_an_unparseable_endpoint() {
        // Callers fall back to the bound address rather than advertising junk.
        let bound = "0.0.0.0:51402".parse().unwrap();
        assert_eq!(advertised_mcp_url("pond-node-1:51401", bound), None);
    }
}
