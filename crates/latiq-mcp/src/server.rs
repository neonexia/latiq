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
use latiq_agent_core::{with_bearer, AgentError, AgentOps, QueryControls};
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
    #[schemars(
        description = "Record OpenLineage provenance for every query on this pond, readable with get_lineage (default false). Chosen here and FIXED for the pond's lifetime — no RPC turns it on later, so a pond allocated without it can never explain its own history; the only recovery is a new pond. It costs disk and a little per-query time, so ask for it when you need to answer 'where did this data come from?'. See latiq://recipes/lineage."
    )]
    pub lineage: Option<bool>,
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
    #[schemars(
        description = "How long this statement may run, in milliseconds. Omit for the node's default. Asking for MORE than the node allows is not an error — it is clamped to the node's maximum and the query runs at that ceiling, so always read `_meta.timeout_ms` for what was actually applied. On expiry you get a `query_timeout` error naming both numbers. Ignored by explain_query, which does not execute."
    )]
    pub timeout_ms: Option<u64>,
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

#[derive(Debug, Deserialize, JsonSchema)]
#[schemars(crate = "rmcp::schemars")]
pub struct LineageArgs {
    #[schemars(description = "Pond id or name")]
    pub pond: String,
    #[schemars(
        description = "How many events to return, newest first (default 50, max 500). Two events are recorded per operation. Must be at least 1 — `0` is rejected, not read as 'no limit'."
    )]
    pub limit: Option<u32>,
    #[schemars(
        description = "Only events at or after this RFC-3339 instant, INCLUSIVE, e.g. `2026-08-14T10:00:00Z`. For catching up: pass the newest `eventTime` you already have to see what happened since (that one event comes back again, because the bound includes its own instant)."
    )]
    pub since: Option<String>,
    #[schemars(
        description = "Only events strictly BEFORE this RFC-3339 instant, EXCLUSIVE. This is the backward-paging cursor: when a page comes back `truncated`, call again with `before` set to the OLDEST `eventTime` in it to get the next older page. Pages are cut on a timestamp boundary, so this never repeats or skips an event — except for a FULL page whose events ALL share one `eventTime`, which is returned uncut, so a cursor from it skips the rest of that millisecond; raise `limit` if that happens."
    )]
    pub before: Option<String>,
}

/// This request's execution controls: the caller's `timeout_ms` and — the part
/// that makes an agent's cancel real — **its MCP request cancellation**.
///
/// `RequestContext::ct` is rmcp's per-request token, and rmcp cancels it when a
/// matching `notifications/cancelled` arrives (it keys a per-request token pool
/// by request id in its serve loop). So request-id matching is already done for
/// us: nothing here needs to track ids, and there is deliberately no registry of
/// our own to drift out of sync with rmcp's.
///
/// It is a `tokio_util::sync::CancellationToken` — the SAME type as the core's
/// `AbortToken` — so this is a hand-off, not an adapter, and no protocol type
/// crosses into `latiq-agent-core` (invariant 5).
///
/// rmcp does NOT abort the handler future on cancel; it only fires the token. So
/// the handler still runs to completion and still answers — but the query
/// underneath is interrupted, and the answer is a `query_cancelled` envelope
/// rather than rows the client no longer wants. A dropped connection is NOT a
/// cancel source (see this crate's CLAUDE.md); the deadline is the backstop for
/// an abandoned query.
fn query_controls(a: &QueryArgs, ctx: &RequestContext<RoleServer>) -> QueryControls {
    QueryControls::timeout(a.timeout_ms).with_cancel(ctx.ct.clone())
}

/// The HTTP header carrying the CLAIMED leaf id. Same name as the gRPC metadata
/// key on the Data surface, so one deployment has one spelling.
const AGENT_ID_HEADER: &str = "latiq-agent-id";

/// RFC 9728's fixed location for the protected-resource metadata document.
pub const PROTECTED_RESOURCE_PATH: &str = "/.well-known/oauth-protected-resource";

/// The agent-facing MCP handler: the tool router plus the `AgentOps` behind it.
/// Agents only — there is nothing administrative on this surface, and the CLI
/// and SDK never reach it (invariants 1 and 8).
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

/// The public MCP URL this node publishes as its RFC 9728 `resource` identifier
/// and points at in its 401 challenge.
///
/// Resolution order: an explicitly configured `public_mcp_url` (`--public-mcp-url`)
/// wins, then the URL derived from `--advertise-addr`, then the bound address.
/// Only the first is right behind a gateway: `--advertise-addr` is the node's
/// INTERNAL address, used so peer nodes can forward pond requests, and agents
/// never dial it. A conforming client compares the `resource` it discovers
/// against the URL it dialled and refuses on any origin difference, so publishing
/// the node's own address behind a gateway fails the client before it ever asks
/// for a token.
///
/// A configured value is validated rather than trusted: a relative or hostless
/// URL here would break discovery for every client with an error that points
/// nowhere near the deployment's config, so callers should fail startup on `Err`.
pub fn resolve_public_mcp_url(
    public_mcp_url: Option<&str>,
    advertised_endpoint: &str,
    mcp_addr: SocketAddr,
) -> Result<String, String> {
    if let Some(configured) = public_mcp_url {
        let url = url::Url::parse(configured).map_err(|e| {
            format!(
                "--public-mcp-url (or $LATIQ_PUBLIC_MCP_URL) is not a valid absolute URL: \
                 {configured:?} ({e}). Pass the full URL agents dial, e.g. \
                 https://latiq.example.com/mcp."
            )
        })?;
        if !url.has_host() {
            return Err(format!(
                "--public-mcp-url (or $LATIQ_PUBLIC_MCP_URL) has no host: {configured:?}. Pass the \
                 full URL agents dial, e.g. https://latiq.example.com/mcp."
            ));
        }
        return Ok(configured.to_string());
    }
    Ok(advertised_mcp_url(advertised_endpoint, mcp_addr)
        .unwrap_or_else(|| format!("http://{mcp_addr}/mcp")))
}

/// The `Host` authorities rmcp's Streamable-HTTP transport will accept.
///
/// rmcp defends against DNS rebinding by rejecting any request whose `Host` is
/// not loopback (`403 Forbidden: Host header is not allowed`). That default is
/// right for an MCP server an agent runs on its own laptop and WRONG for every
/// deployment we ship: agents reach Latiq through the gateway, so the `Host`
/// they send is the gateway's (`gateway:51510`, `latiq.example.com`) and every
/// JSON-RPC POST is refused — while RFC 9728 discovery and the 401 challenge,
/// served by our own axum routes rather than by rmcp, keep working. The failure
/// therefore *looks* like an auth problem and is not one.
///
/// So keep rmcp's loopback defaults AND add the one name we already know agents
/// dial: the host of the public MCP URL (`--public-mcp-url` /
/// `$LATIQ_PUBLIC_MCP_URL`, see `resolve_public_mcp_url`). The guard stays on —
/// it just learns the deployment's real front door.
///
/// **The host only, never `host:port`.** A port-qualified entry matches only
/// that exact port in rmcp, and proxies routinely rewrite the port out of the
/// `Host` they forward — nginx's `$host` is documented as the name *without*
/// the port, so our own gateway sends `Host: gateway` for a front door on
/// `:51510`. A port would also buy nothing: an attacker who can reach this
/// socket at all has already matched the port, and the whole defense is against
/// an unrecognized *name*.
pub fn mcp_allowed_hosts(public_url: Option<&str>) -> Vec<String> {
    let mut hosts = vec![
        "localhost".to_string(),
        "127.0.0.1".to_string(),
        "::1".to_string(),
    ];
    if let Some(host) = public_url.and_then(public_url_host) {
        if !hosts.contains(&host) {
            hosts.push(host);
        }
    }
    hosts
}

/// The bare host of a public MCP URL. `None` when the URL is not absolute or
/// carries no host — `resolve_public_mcp_url` already rejects those at startup,
/// so reaching here means the loopback defaults are all we can honestly allow.
fn public_url_host(public_url: &str) -> Option<String> {
    let url = url::Url::parse(public_url).ok()?;
    // `host_str` brackets an IPv6 literal (`[::1]`); rmcp strips the brackets
    // on both sides of the comparison, so either form matches.
    url.host_str().map(str::to_string)
}

/// The RFC 9728 document URL for a resource identifier — the well-known path on
/// the resource's own origin. Derived so the document a challenge points at and
/// the document we serve can never disagree; a gateway that rewrites paths
/// (rather than only the host) would need this configured.
pub fn protected_resource_metadata_url(resource: &str) -> String {
    format!(
        "{}{PROTECTED_RESOURCE_PATH}",
        resource.strip_suffix("/mcp").unwrap_or(resource)
    )
}

#[tool_router]
impl LatiqServer {
    /// Allocate a new pond. Optionally name it; Latiq generates a name if omitted.
    /// Returns the pond_id and pond_name. Use list_ponds to discover existing ponds.
    #[tool(
        description = "Allocate a new pond — a private DuckLake workspace you can write to and query with SQL. \
Optionally pass a `name` (Latiq generates one if omitted). Returns `pond_id` + `pond_name`. \
Use this first when you have a task that needs its own data space; use list_ponds to find or join an existing one. \
Then write_query to create tables and load data, and read_query to query. \
DECIDE `lineage` NOW: provenance recording is set here and can never be turned on later — if this pond's work may have to explain where its data came from, pass `lineage: true`, because the only recovery is starting over in a new pond. \
The pond's storage is created on its node BEFORE this returns, so success means a pond you can write to immediately — and on a clustered deployment that costs one extra hop and can fail if that node is down. \
Such a failure says the pond was NOT created and the assignment was rolled back: the name is free, so retry. \
See latiq://guidance.",
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
            match self
                .ops
                .allocate_pond(&id, a.name, "{}", tier, &exts, a.lineage.unwrap_or(false))
                .await
            {
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
The response's `lineage` flag says whether this pond records provenance — check it before get_lineage, and before you rely on a pond to be explainable later (it cannot be switched on). \
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
        description = "Drop a pond and reclaim its storage. DESTRUCTIVE and not reversible — all tables and data in the pond are removed, and its lineage trail goes with them (the deployment's access log is preserved). Read what you still need from get_lineage BEFORE dropping. \
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
For INSERT/UPDATE/DELETE/DDL use write_query instead — those are rejected here, as is transaction control (BEGIN/COMMIT/ROLLBACK): Latiq manages the transaction. \
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
        let controls = query_controls(&a, &ctx);
        // Reads ride the Arrow internal hop, collected to the neutral result here.
        Ok(with_bearer(tok, async {
            match self
                .ops
                .read_collected_with(&id, &a.pond, &a.sql, controls)
                .await
            {
                Ok(qr) => ok_query("read_query", qr),
                Err(e) => err_envelope(e.envelope()),
            }
        })
        .await)
    }

    /// Run a write/DDL SQL statement (INSERT/UPDATE/DELETE/CREATE/CTAS) against a
    /// pond. Writes are attributed to your agent identity, which Latiq records
    /// inside the transaction it owns — caller SQL must not do its own
    /// BEGIN/COMMIT/ROLLBACK.
    #[tool(
        description = "Run a write or DDL SQL statement (INSERT/UPDATE/DELETE/CREATE/DROP/ALTER/CREATE TABLE AS SELECT) against a pond. \
Your writes are attributed to your agent identity (queryable via `SELECT author, commit_message, commit_extra_info FROM ducklake_snapshots('<pond>')` — `commit_extra_info` carries the verified-vs-claimed evidence). \
Latiq runs your statement inside its OWN transaction and records the author just before committing, so send plain statements — several are fine, but do NOT include BEGIN/COMMIT/ROLLBACK/START TRANSACTION. Your own COMMIT ends Latiq's transaction before the author is written, and the change lands in the pond's history with NO author. \
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
        let controls = query_controls(&a, &ctx);
        Ok(with_bearer(tok, async {
            match self
                .ops
                .write_query_with(&id, &a.pond, &a.sql, controls)
                .await
            {
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

    /// Read the pond's OpenLineage trail — canonical events, newest first.
    #[tool(
        description = "Read a pond's PROVENANCE — the OpenLineage events Latiq recorded for every query on it, NEWEST FIRST. \
Use it to answer 'where did this table come from?', 'who wrote it, and was that identity verified?', 'what did that run read?'. \
Only ponds allocated with `lineage: true` record anything; asking a pond that does not returns an error saying so — that is deliberately \
distinct from an empty list, so you can tell 'we were not recording' from 'nothing happened'. \
Each operation contributes a START and a terminal (COMPLETE/FAIL/ABORT) event sharing one `run.runId`; the identity, SQL shape, datasets read/written and the DuckLake snapshot ride the facets. \
Bounded on purpose — `limit` defaults to 50 (max 500) and a page also stops at ~256 KB — so a busy pond cannot flood your context. \
PAGING: `truncated` true means OLDER events remain — page backwards with `before`, catch up with `since` (both documented on the arguments). \
Read `malformed_lines` / `unreadable_files`: non-zero means this page is missing events that were recorded. \
Events are returned verbatim: valid OpenLineage 2-0-2, replayable into any OpenLineage consumer unchanged. \
To FILTER or AGGREGATE the whole trail instead of paging it, read_query over the returned `lineage_dir`: \
`SELECT * FROM read_json_auto('<lineage_dir>/*.jsonl')` — facets differ per event, so the inferred schema can shift between queries; SELECT the fields you need. \
A record, not proof: these are files in the pond, reachable by anything that can write SQL there. See latiq://recipes/lineage.",
        annotations(
            title = "Get lineage",
            read_only_hint = true,
            destructive_hint = false,
            idempotent_hint = true
        )
    )]
    async fn get_lineage(
        &self,
        Parameters(a): Parameters<LineageArgs>,
        ctx: RequestContext<RoleServer>,
    ) -> Result<CallToolResult, McpError> {
        let (id, tok) = self.identity(&ctx)?;
        // The core clamps the upper end and refuses 0; the DEFAULT lives here,
        // because "how much of an agent's context may one answer cost" is a
        // question about this surface's audience.
        let limit = a.limit.unwrap_or(DEFAULT_LINEAGE_LIMIT) as usize;
        Ok(with_bearer(tok, async {
            match self
                .ops
                .get_lineage(&id, &a.pond, limit, a.since.as_deref(), a.before.as_deref())
                .await
            {
                Ok(page) => ok_value(serde_json::to_value(page).unwrap_or_default()),
                Err(e) => err_envelope(e.envelope()),
            }
        })
        .await)
    }
}

/// Events one `get_lineage` returns when the agent does not choose. Modest: an
/// agent asking for provenance is spending its context window on the answer.
const DEFAULT_LINEAGE_LIMIT: u32 = 50;

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
write/read SQL with native attribution. Latiq owns the transaction around every write — send plain statements, never BEGIN/COMMIT/ROLLBACK. \
FIRST MOVES: list_ponds to find or join a workspace, or allocate_pond for a new one; then write_query/read_query. \
TO BRING IN EXTERNAL DATA: list_datasets + load_dataset for curated public files; or list_catalogs → describe_catalog → \
pull_catalog for an external database/lakehouse (iceberg) — you pull a subset into the pond, then work there \
(external catalogs are never queried live). \
WHO YOU ARE: your identity arrives in the transport (bearer token + the `latiq-agent-id` header), never as a tool argument — no tool takes one, so don't look for it. \
PROVENANCE: pass `lineage: true` at allocate_pond if this pond's work must be explainable later; it cannot be enabled afterwards. \
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
    // rmcp's DNS-rebinding guard defaults to loopback-only, which rejects every
    // POST that arrives through the gateway. Teach it the URL agents dial.
    // (`StreamableHttpServerConfig` is `#[non_exhaustive]` — builder, not literal.)
    let config = StreamableHttpServerConfig::default()
        .with_allowed_hosts(mcp_allowed_hosts(public_url.as_deref()));
    let service = StreamableHttpService::new(
        move || Ok(LatiqServer::new(ops.clone()).with_verifier(mcp_verifier.clone())),
        LocalSessionManager::default().into(),
        config,
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
        // The document sits at the well-known path on the same origin, and the
        // Data/Stream challenge derives its URL from the SAME resolved resource.
        let metadata_url = protected_resource_metadata_url(&resource);
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
    use super::{
        advertised_mcp_url, mcp_allowed_hosts, protected_resource_metadata_url,
        resolve_public_mcp_url,
    };

    #[test]
    fn allowed_hosts_keep_loopback_and_add_the_public_host() {
        // The gateway case, and the whole reason this exists: agents dial
        // `gateway`, and rmcp's loopback-only default 403s it.
        //
        // The port is deliberately NOT carried over: rmcp matches a
        // port-qualified entry only against that exact port, and nginx's `$host`
        // forwards the name without one — so `gateway:51510` here would still
        // reject the very request this fix exists for.
        assert_eq!(
            mcp_allowed_hosts(Some("http://gateway:51510/mcp")),
            vec!["localhost", "127.0.0.1", "::1", "gateway"]
        );
        assert_eq!(
            mcp_allowed_hosts(Some("https://latiq.example.com/mcp")),
            vec!["localhost", "127.0.0.1", "::1", "latiq.example.com"]
        );
    }

    #[test]
    fn allowed_hosts_are_loopback_only_without_a_usable_public_url() {
        // Nothing to widen the guard with => it must stay closed, not open.
        let loopback = vec!["localhost", "127.0.0.1", "::1"];
        assert_eq!(mcp_allowed_hosts(None), loopback);
        assert_eq!(mcp_allowed_hosts(Some("not a url")), loopback);
        assert_eq!(mcp_allowed_hosts(Some("/mcp")), loopback);
        // Already covered by the defaults — don't list it twice.
        assert_eq!(mcp_allowed_hosts(Some("http://localhost/mcp")), loopback);
    }

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

    #[test]
    fn public_url_resolution_prefers_the_configured_value() {
        // The gateway case: the node advertises its own internal name for
        // forwarding, but agents dial the gateway, so the configured URL wins
        // whole — scheme, host, port and path included.
        let bound = "0.0.0.0:51402".parse().unwrap();
        assert_eq!(
            resolve_public_mcp_url(
                Some("https://latiq.example.com/mcp"),
                "http://pond-node-1:51401",
                bound,
            ),
            Ok("https://latiq.example.com/mcp".to_string())
        );
    }

    #[test]
    fn public_url_resolution_falls_back_to_advertise_then_bound() {
        let bound = "0.0.0.0:51402".parse().unwrap();
        // Nothing configured: derive from --advertise-addr, as before.
        assert_eq!(
            resolve_public_mcp_url(None, "http://pond-node-1:51401", bound),
            Ok("http://pond-node-1:51402/mcp".to_string())
        );
        // Nothing configured AND an unusable advertised endpoint: the bound
        // address, which is what this has always done.
        assert_eq!(
            resolve_public_mcp_url(None, "pond-node-1:51401", bound),
            Ok("http://0.0.0.0:51402/mcp".to_string())
        );
    }

    #[test]
    fn public_url_resolution_rejects_a_malformed_value() {
        let bound = "0.0.0.0:51402".parse().unwrap();
        // Not absolute: every client's discovery would fail with an error that
        // points nowhere near this setting, so we fail at startup instead.
        assert!(resolve_public_mcp_url(Some("gateway:51510/mcp"), "http://n:1", bound).is_err());
        assert!(resolve_public_mcp_url(Some("/mcp"), "http://n:1", bound).is_err());
        // Absolute but hostless — parses, yet names no origin to compare against.
        assert!(resolve_public_mcp_url(Some("file:///mcp"), "http://n:1", bound).is_err());
    }

    #[test]
    fn metadata_url_sits_on_the_resource_origin() {
        assert_eq!(
            protected_resource_metadata_url("https://latiq.example.com/mcp"),
            "https://latiq.example.com/.well-known/oauth-protected-resource"
        );
        // A resource that is not path-suffixed with /mcp keeps its own path base.
        assert_eq!(
            protected_resource_metadata_url("https://latiq.example.com"),
            "https://latiq.example.com/.well-known/oauth-protected-resource"
        );
    }
}
