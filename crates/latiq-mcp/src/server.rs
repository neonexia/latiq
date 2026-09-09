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
use crate::encode::{err_envelope, ok, ok_explain, ok_query};
use crate::resources;
use crate::response::{
    CatalogTableRef, DescribeCatalogResponse, DropPondResponse, ListCatalogsResponse,
    ListDatasetsResponse, ListPondsResponse, QueryResponse,
};
use crate::schema::output_schema;
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

/// Every argument struct on this surface is `deny_unknown_fields`.
///
/// A misspelled argument used to be dropped in silence: `timout_ms` had no
/// effect, no error, and no way for the model to learn it had mistyped — the
/// query simply ran under a policy it thought it had changed. Serde's rejection
/// names the offending key AND lists the accepted ones, which is exactly the
/// correction an agent needs, and it costs nothing for a conforming client: MCP
/// `arguments` is the object the model produced, and no client of ours adds
/// fields to it (protocol-level extras like `_meta` live beside `arguments` in
/// `params`, never inside it).
#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
#[schemars(crate = "rmcp::schemars")]
pub struct AllocateArgs {
    #[schemars(
        description = "Optional pond name; Latiq generates a uuid if omitted. It becomes the pond's SQL catalog name: 1-64 characters of letters, digits, `_` or `-`, anything else rejected rather than mangled. An empty string is NOT 'generate one for me' — omit the field for that."
    )]
    pub name: Option<String>,
    #[schemars(
        description = "Resource tier (default medium). Caps the pond's memory + CPU. An unrecognised tier is REJECTED, not quietly run at the default.",
        extend("enum" = ["x-small", "small", "medium", "large", "x-large"])
    )]
    pub tier: Option<String>,
    #[schemars(
        description = "Optional DuckDB extensions to load on this pond, e.g. [\"spatial\",\"fts\"]. Signed/official extensions only; must be available on the deployment. See the latiq://guidance resource for the supported set."
    )]
    pub extensions: Option<Vec<String>>,
    #[schemars(
        description = "Record OpenLineage provenance for every query on this pond, readable with get_lineage (default false). FIXED at allocation: nothing turns it on later, so a pond without it can never explain its own history and the only recovery is a new pond. See latiq://recipes/lineage."
    )]
    pub lineage: Option<bool>,
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
#[schemars(crate = "rmcp::schemars")]
pub struct PondRefArgs {
    #[schemars(description = "Pond id or name")]
    pub pond: String,
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
#[schemars(crate = "rmcp::schemars")]
pub struct DropArgs {
    #[schemars(description = "Pond id or name")]
    pub pond: String,
    /// The most consequential argument on the only irreversible tool, and it had
    /// NO description at all — an agent could only learn what it was for by
    /// being refused once.
    #[schemars(
        description = "Must be `true` for the drop to happen; omitted or `false` deletes NOTHING and is refused — that refusal is the safety net, so don't set `true` speculatively. There is no undo: tables, history and lineage go, and re-allocating the same name gives you an empty pond, not this one."
    )]
    pub confirm: Option<bool>,
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
#[schemars(crate = "rmcp::schemars")]
pub struct QueryArgs {
    #[schemars(description = "Pond id or name")]
    pub pond: String,
    #[schemars(description = "SQL statement")]
    pub sql: String,
    #[schemars(
        description = "How long this statement may run, in milliseconds. Omit for the node's default; `0` is rejected, never read as 'no timeout' or 'use the default'. More than the node allows is not an error — it is clamped to the node's maximum, so read `_meta.timeout_ms` for what was actually applied. See latiq://troubleshooting/timeouts."
    )]
    pub timeout_ms: Option<u64>,
}

/// `explain_query`'s arguments — deliberately NOT [`QueryArgs`].
///
/// It carried a `timeout_ms` whose own description said it was ignored: a dead
/// argument advertised to the model, which costs a decision on every call and
/// can only ever be wasted. Explain executes nothing, so there is no deadline to
/// set; if planning itself ever needs bounding, that is a node-side concern, not
/// a knob the agent should be offered.
#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
#[schemars(crate = "rmcp::schemars")]
pub struct ExplainArgs {
    #[schemars(description = "Pond id or name")]
    pub pond: String,
    #[schemars(description = "SQL statement to plan (it is NOT executed)")]
    pub sql: String,
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
#[schemars(crate = "rmcp::schemars")]
pub struct SearchArgs {
    #[schemars(
        description = "Optional search: a tag as `#finance`, a name glob as `sal*`, or a plain substring. Omit for all."
    )]
    pub query: Option<String>,
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
#[schemars(crate = "rmcp::schemars")]
pub struct LoadDatasetArgs {
    #[schemars(description = "Pond id or name to load into")]
    pub pond: String,
    #[schemars(description = "Dataset name (from list_datasets), e.g. `tpch`")]
    pub dataset: String,
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
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
#[serde(deny_unknown_fields)]
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
#[serde(deny_unknown_fields)]
#[schemars(crate = "rmcp::schemars")]
pub struct LineageArgs {
    #[schemars(description = "Pond id or name")]
    pub pond: String,
    #[schemars(
        description = "How many events to return, newest first (default 50). Must be at least 1 — `0` is rejected, never read as 'no limit'. Above the maximum of 500 it is CLAMPED rather than refused, and `limit_applied` reports the value actually used."
    )]
    pub limit: Option<u32>,
    #[schemars(
        description = "Only events at or after this RFC-3339 instant, INCLUSIVE, e.g. `2026-08-14T10:00:00Z` — the catch-up bound: pass the newest `eventTime` you already have. See latiq://recipes/lineage."
    )]
    pub since: Option<String>,
    #[schemars(
        description = "Only events strictly BEFORE this RFC-3339 instant, EXCLUSIVE — the backward-paging cursor: when a page comes back `truncated`, call again with `before` set to the OLDEST `eventTime` in it. See latiq://recipes/lineage for the one case that skips events."
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

/// **The zero rule for this surface.** An explicit `0` from a JSON caller is a
/// value the caller chose, so it is refused; it is never re-read as "unset".
///
/// The two adjacent tools disagreed about exactly this: `get_lineage`'s
/// `limit: 0` was rejected with a message that says so in as many words, while
/// `read_query`'s `timeout_ms: 0` silently ran at the node default and reported
/// 30000 back as though it had been asked for. The gRPC surface still reads a
/// zero as unset and must — proto3 cannot express an absent number — but that is
/// a limitation of that wire, normalised at that boundary, and it has no
/// business reaching a surface where the caller could say what it meant.
fn reject_zero(field: &str, value: Option<u64>) -> Option<CallToolResult> {
    (value == Some(0)).then(|| {
        err_envelope(
            AgentError::rendered_with(
                latiq_common::ErrorKind::InvalidValue,
                "`{field}` must be at least 1.",
                latiq_common::facts! { "field" => field },
                "Omit `{field}` to use the node's default; `0` is not 'unlimited'.",
                "latiq://guidance",
            )
            .envelope(),
        )
    })
}

/// The HTTP header carrying the CLAIMED leaf id. Same name as the gRPC metadata
/// key on the Data surface, so one deployment has one spelling.
const AGENT_ID_HEADER: &str = "latiq-agent-id";

/// The W3C Trace Context header. Standard-spelled, and the SAME name the gRPC
/// surfaces read as metadata — one deployment, one spelling, so an operator
/// greps one thing and an external collector already understands it.
const TRACEPARENT_HEADER: &str = "traceparent";

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

    /// Run one tool handler inside this request's **trace scope**, its span, and
    /// its bearer scope — the MCP twin of the Data surface's `traced`.
    ///
    /// This surface entered no trace scope at all until #101, which is exactly
    /// backwards for the one audience the product is built for: every agent call
    /// logged `trace_id="-"` on the access trail, emitted `traceId: null` in its
    /// lineage, and a forwarded agent query could not be joined to the node that
    /// actually ran it. All three read from the same ambient scope, so all three
    /// are fixed by entering it once, here, rather than by threading an id
    /// through thirteen handlers.
    ///
    /// The bearer scope was already per-handler and is folded in so a handler
    /// cannot acquire one without the other.
    async fn traced<T>(
        &self,
        name: &'static str,
        ctx: &RequestContext<RoleServer>,
        bearer: Option<String>,
        fut: impl std::future::Future<Output = T>,
    ) -> T {
        use tracing::Instrument;
        let trace = trace_of(ctx);
        let tid = trace.trace_id().to_string();
        let inner = latiq_agent_core::with_trace(
            trace,
            fut.instrument(tracing::info_span!("mcp", name, trace_id = %tid)),
        );
        with_bearer(bearer, inner).await
    }
}

/// This request's W3C trace context, from the `traceparent` HTTP header, or a
/// fresh trace when it carried none we could honour.
///
/// Attribution-grade, never authority-grade — the same standing as
/// `latiq-agent-id` (invariant 9): recorded and propagated, never read for an
/// access decision. rmcp injects the request's `http::request::Parts` on the
/// POST path, which is where every tool call lands.
fn trace_of(ctx: &RequestContext<RoleServer>) -> latiq_agent_core::TraceContext {
    latiq_agent_core::TraceContext::inbound(
        ctx.extensions
            .get::<http::request::Parts>()
            .and_then(|p| p.headers.get(TRACEPARENT_HEADER))
            .and_then(|v| v.to_str().ok()),
    )
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
        output_schema = output_schema::<latiq_agent_core::AllocateResult>(),
        description = "Allocate a pond — a private DuckLake SQL workspace. Optional `name`; returns `pond_id` + `pond_name`. \
Decide `lineage` NOW: it is fixed at allocation and can never be turned on later. See latiq://guidance.",
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
        // Argument validation runs INSIDE the trace scope, like the call it
        // guards: every answer this surface gives carries a `_meta.traceparent`
        // (see `encode::trace_meta`), and a refusal that answered before the
        // scope was entered would be the one result an agent could not correlate
        // — on the argument mistakes it is most likely to have to ask about.
        Ok(self
            .traced("allocate_pond", &ctx, tok, async {
                let tier = a.tier.as_deref().unwrap_or("medium");
                // The registry refuses an unknown tier or an illegal name too —
                // it is the choke point every create path shares, and it is what
                // makes the guarantee durable. Checked here as well because THIS
                // surface can see what the caller actually typed: an empty
                // `name` is a deliberate empty string to a JSON caller and
                // "unset" by the time it has crossed proto3, and only one of
                // those two should become a generated uuid.
                if let Some(name) = a.name.as_deref() {
                    if let Err(msg) = latiq_common::pond_name::validate(name) {
                        return err_envelope(
                            AgentError::new(
                                latiq_common::ErrorKind::InvalidValue,
                                msg,
                                "Retry with a name of letters, digits, `_` or `-`, or omit `name` \
                                 and use the one Latiq generates.",
                                "latiq://guidance",
                            )
                            .envelope(),
                        );
                    }
                }
                // Validate requested extensions against the signed/official
                // allowlist before allocating, so a bad name returns a clear,
                // actionable error.
                let exts = match latiq_common::extensions::validate(
                    a.extensions.as_deref().unwrap_or(&[]),
                ) {
                    Ok(e) => e,
                    Err(msg) => {
                        return err_envelope(AgentError::unsupported_extension(msg).envelope())
                    }
                };
                match self
                    .ops
                    .allocate_pond(
                        &id,
                        a.name.clone(),
                        "{}",
                        tier,
                        &exts,
                        a.lineage.unwrap_or(false),
                    )
                    .await
                {
                    Ok(r) => ok(&r),
                    Err(e) => err_envelope(e.envelope()),
                }
            })
            .await)
    }

    /// Describe a pond: its metadata + a summary of its tables. Pass pond id or name.
    #[tool(
        output_schema = output_schema::<latiq_agent_core::DescribeResult>(),
        description = "A pond's metadata plus every table with its columns, row estimate and comment — the whole pond in one call. \
Pass `pond` (id or name). See latiq://guidance.",
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
        Ok(self
            .traced("describe_pond", &ctx, tok, async {
                match self.ops.describe_pond(&id, &a.pond).await {
                    Ok(r) => ok(&r),
                    Err(e) => err_envelope(e.envelope()),
                }
            })
            .await)
    }

    /// List all ponds in the deployment.
    #[tool(
        output_schema = output_schema::<ListPondsResponse>(),
        description = "List the deployment's ponds (id, name, owner). Ponds are shared — look here before allocating, then describe_pond a candidate.",
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
        Ok(self
            .traced("list_ponds", &ctx, tok, async {
                match self.ops.list_ponds(&id).await {
                    Ok(ponds) => ok(&ListPondsResponse { ponds }),
                    Err(e) => err_envelope(e.envelope()),
                }
            })
            .await)
    }

    /// Drop a pond and reclaim its storage. Destructive.
    #[tool(
        output_schema = output_schema::<DropPondResponse>(),
        description = "Delete a pond and everything in it — tables, history and lineage — irreversibly. Requires `confirm: true`. \
Others may be working in it — check list_ponds first.",
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
        Ok(self
            .traced("drop_pond", &ctx, tok, async {
                match self
                    .ops
                    .drop_pond(&id, &a.pond, a.confirm.unwrap_or(false))
                    .await
                {
                    Ok(()) => ok(&DropPondResponse {
                        status: "dropped".into(),
                        pond: a.pond.clone(),
                    }),
                    Err(e) => err_envelope(e.envelope()),
                }
            })
            .await)
    }

    /// Run a read-only SQL query (SELECT / read-only metadata) against a pond.
    /// For writes/DDL use write_query. Results are bounded by the inline cap.
    #[tool(
        output_schema = output_schema::<QueryResponse>(),
        description = "Run a read-only SQL statement (SELECT, SHOW, DESCRIBE) on a pond. \
Writes, DDL and transaction control belong in write_query. \
Results are capped (~10k rows): latiq://recipes/large-results.",
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
        Ok(self
            .traced("read_query", &ctx, tok, async {
                // Inside the scope: a refusal is an answer, and every answer
                // here carries `_meta.traceparent` (`encode::trace_meta`).
                if let Some(refusal) = reject_zero("timeout_ms", a.timeout_ms) {
                    return refusal;
                }
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
        output_schema = output_schema::<QueryResponse>(),
        description = "Run a write or DDL SQL statement on a pond; attributed to your agent identity. \
Send plain statements — several are fine, but NEVER BEGIN/COMMIT/ROLLBACK/START TRANSACTION: your own COMMIT ends Latiq's transaction before the author is recorded, and the change lands in history with NO author. \
Documenting tables: latiq://recipes/schema-design. External files: latiq://recipes/data-ingestion-m1.",
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
        Ok(self
            .traced("write_query", &ctx, tok, async {
                // Inside the scope, for the same reason as `read_query`.
                if let Some(refusal) = reject_zero("timeout_ms", a.timeout_ms) {
                    return refusal;
                }
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
        output_schema = output_schema::<latiq_engine::ExplainResult>(),
        description = "Plan a statement WITHOUT running it: `estimated_rows`, `scan_operations`, `warnings`/`suggestions`, `raw_plan`. \
Planner ESTIMATES, not measurements, and rows never time. See latiq://recipes/large-results.",
        annotations(
            title = "Explain query",
            read_only_hint = true,
            destructive_hint = false,
            idempotent_hint = true
        )
    )]
    async fn explain_query(
        &self,
        Parameters(a): Parameters<ExplainArgs>,
        ctx: RequestContext<RoleServer>,
    ) -> Result<CallToolResult, McpError> {
        let (id, tok) = self.identity(&ctx)?;
        Ok(self
            .traced("explain_query", &ctx, tok, async {
                match self.ops.explain_query(&id, &a.pond, &a.sql).await {
                    Ok(er) => ok_explain(er),
                    Err(e) => err_envelope(e.envelope()),
                }
            })
            .await)
    }

    /// Discover curated datasets (simple public files) you can copy into a pond.
    #[tool(
        output_schema = output_schema::<ListDatasetsResponse>(),
        description = "Browse curated DATASETS — public files an operator registered — then load_dataset one into a pond. \
See latiq://recipes/external-data.",
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
        Ok(self
            .traced("list_datasets", &ctx, tok, async {
                match self
                    .ops
                    .list_datasets(a.query.as_deref().unwrap_or(""))
                    .await
                {
                    Ok(datasets) => ok(&ListDatasetsResponse { datasets }),
                    Err(e) => err_envelope(e.envelope()),
                }
            })
            .await)
    }

    /// Copy a dataset's tables into a pond, under a schema named after the dataset. Pick a name from list_datasets.
    #[tool(
        output_schema = output_schema::<latiq_agent_core::LoadDatasetResult>(),
        description = "Copy a dataset's tables into a pond, under a SCHEMA named after it — query them as `<dataset>.<table>`. \
A write. See latiq://recipes/external-data.",
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
        Ok(self
            .traced("load_dataset", &ctx, tok, async {
                match self.ops.load_dataset(&id, &a.pond, &a.dataset).await {
                    Ok(r) => ok(&r),
                    Err(e) => err_envelope(e.envelope()),
                }
            })
            .await)
    }

    /// Discover registered external catalogs (iceberg/…) you can pull data from.
    #[tool(
        output_schema = output_schema::<ListCatalogsResponse>(),
        description = "Browse registered external CATALOGS (iceberg today), then describe_catalog its tables and pull_catalog a subset into a pond. \
See latiq://recipes/external-data.",
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
        Ok(self
            .traced("list_catalogs", &ctx, tok, async {
                match self
                    .ops
                    .list_catalogs(a.query.as_deref().unwrap_or(""))
                    .await
                {
                    Ok(catalogs) => ok(&ListCatalogsResponse { catalogs }),
                    Err(e) => err_envelope(e.envelope()),
                }
            })
            .await)
    }

    /// List an external catalog's tables (transient attach on a pond). Pass creds via `set`.
    #[tool(
        output_schema = output_schema::<DescribeCatalogResponse>(),
        description = "List an external catalog's tables — attached on `pond` transiently, then detached. \
Credentials in `set`, used once. See latiq://recipes/external-data.",
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
        Ok(self
            .traced("describe_catalog", &ctx, tok, async {
                match self
                    .ops
                    .catalog_describe(&id, &a.pond, &a.catalog, set)
                    .await
                {
                    Ok(tables) => ok(&DescribeCatalogResponse {
                        catalog: a.catalog.clone(),
                        tables: tables
                            .into_iter()
                            .map(|(schema, table)| CatalogTableRef { schema, table })
                            .collect(),
                    }),
                    Err(e) => err_envelope(e.envelope()),
                }
            })
            .await)
    }

    /// Pull a subset of an external catalog into a pond: transient attach → your query → detach.
    #[tool(
        output_schema = output_schema::<latiq_agent_core::PullResult>(),
        description = "Copy a subset of an external catalog INTO a pond: attach → your `query` (a CREATE TABLE naming the catalog) → detach. \
Credentials in `set`, used once. A write. See latiq://recipes/external-data.",
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
        Ok(self
            .traced("pull_catalog", &ctx, tok, async {
                match self
                    .ops
                    .catalog_pull(&id, &a.pond, &a.catalog, &a.query, set)
                    .await
                {
                    Ok(r) => ok(&r),
                    Err(e) => err_envelope(e.envelope()),
                }
            })
            .await)
    }

    /// Read the pond's OpenLineage trail — canonical events, newest first.
    #[tool(
        output_schema = output_schema::<latiq_agent_core::LineagePage>(),
        description = "Read a pond's OpenLineage provenance, newest first. Only a pond allocated with `lineage: true` records any — \
one without it errors rather than returning an empty page. `truncated`/`malformed_lines` mean the page is incomplete. \
See latiq://recipes/lineage.",
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
        Ok(self
            .traced("get_lineage", &ctx, tok, async {
                match self
                    .ops
                    .get_lineage(&id, &a.pond, limit, a.since.as_deref(), a.before.as_deref())
                    .await
                {
                    Ok(page) => ok(&page),
                    Err(e) => err_envelope(e.envelope()),
                }
            })
            .await)
    }
}

/// Events one `get_lineage` returns when the agent does not choose. Modest: an
/// agent asking for provenance is spending its context window on the answer.
const DEFAULT_LINEAGE_LIMIT: u32 = 50;

/// The server `instructions`, sent once at `initialize`.
///
/// **This is the surface's discovery channel, and it is load-bearing.** We used
/// to say tool descriptions did that job; they cannot. Nexus (the agent-readiness
/// harness) drove real models against this server inside a 47-tool belt and every
/// one of them spent its first turn fetching our schemas: the client advertised
/// all thirteen tools by NAME and deferred their descriptions, so a description
/// was read only after the agent had already decided to look at us. What reaches
/// the model unconditionally is the tool NAME and this block — `initialize`
/// carries it before any tool is chosen and no client defers it.
///
/// So it must stay short enough to be read, and it must never point at a tool
/// nobody serves. A renamed tool that left a name here dangling would send an
/// agent's first move to `unknown tool` — the same failure the repo already
/// shipped once with a resource that taught a removed tool — which is why
/// `instructions_name_only_tools_this_server_advertises` checks every tool-shaped
/// name in this text against the router's own list.
const INSTRUCTIONS: &str = "Latiq — the agent-native data pond. Allocate a pond (a private DuckLake workspace), \
write/read SQL with native attribution. Latiq owns the transaction around every write — send plain statements, never BEGIN/COMMIT/ROLLBACK. \
FIRST MOVES: list_ponds to find or join a workspace, or allocate_pond for a new one; then write_query/read_query. \
TO BRING IN EXTERNAL DATA: list_datasets + load_dataset for curated public files; or list_catalogs → describe_catalog → \
pull_catalog for an external database/lakehouse (iceberg) — you pull a subset into the pond, then work there \
(external catalogs are never queried live). \
WHO YOU ARE: your identity arrives in the transport (bearer token + the `latiq-agent-id` header), never as a tool argument — no tool takes one, so don't look for it. \
PROVENANCE: pass `lineage: true` at allocate_pond if this pond's work must be explainable later; it cannot be enabled afterwards. \
Read latiq://guidance to start and latiq://recipes/external-data for the data-loading flow. \
ERRORS ARE STRUCTURED: alongside `message`/`suggest`/`see`, every failure carries `retryable` (`as_is` / `after_change` / `never` — `never` means THIS call, and `suggest` names the different call that works), \
`audience` (`operator` means report it and stop) and `facts` (the numbers as values, so don't parse the sentence). Branch on those, not on the prose — latiq://guidance has the contract. \
Every tool result also carries `_meta.traceparent` IN ITS BODY — the W3C trace id of that call, the one to quote when asking an operator about it (a failure puts the same id on the envelope's own `traceparent`/`trace_id`). \
Prompts provide SOPs for common multi-agent workflows.";

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
        .with_instructions(INSTRUCTIONS)
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
        // A missing REQUIRED argument is refused, never defaulted: the prompt
        // used to render its placeholder ("Find an existing pond related to
        // ''"), which reads exactly like a real instruction. `prompts/list`
        // declares which arguments are required, so this is answerable.
        resources::get_prompt(&request.name, &args).map_err(|e| match e {
            resources::PromptError::Unknown => {
                McpError::invalid_params(format!("unknown prompt: {}", request.name), None)
            }
            resources::PromptError::MissingArgument { prompt, arg } => McpError::invalid_params(
                format!(
                    "prompt {prompt} requires the argument '{arg}' — it is declared as required \
                     in prompts/list; pass it a non-empty value"
                ),
                None,
            ),
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
        resolve_public_mcp_url, LatiqServer, INSTRUCTIONS,
    };

    /// The tool names this server really advertises, from the router itself —
    /// never a list written down here, which is the thing that rots.
    fn advertised_tools() -> Vec<String> {
        LatiqServer::tool_router()
            .list_all()
            .into_iter()
            .map(|t| t.name.to_string())
            .collect()
    }

    /// Snake_case words in the instructions that are NOT tool names, and are not
    /// meant to be: the error-envelope vocabulary the same text documents. Kept
    /// explicit so a genuine tool name can never hide in it.
    // Envelope field VALUES and field NAMES, which are snake_case like a tool
    // name and are not one.
    const NON_TOOL_WORDS: &[&str] = &["as_is", "after_change", "trace_id"];

    /// **Discovery runs on the tool NAME and on this block, not on tool
    /// descriptions** — a client may defer descriptions until after the agent
    /// has already decided to look (Nexus measured exactly that in a 47-tool
    /// belt), but `initialize` carries `instructions` to every session.
    ///
    /// So the first moves it names must be tools that exist. The repo has
    /// already shipped a resource teaching a tool that had been removed; this is
    /// the same class of rot one layer earlier, where it would break an agent's
    /// very first call.
    #[test]
    fn instructions_name_only_tools_this_server_advertises() {
        let tools = advertised_tools();
        assert!(
            tools.len() >= 13,
            "the router advertises {} tools — if this fell, the test below is \
             checking the instructions against almost nothing",
            tools.len()
        );

        // 1. The first moves are named, and each one is served.
        for first_move in ["list_ponds", "allocate_pond", "read_query", "write_query"] {
            assert!(
                INSTRUCTIONS.contains(first_move),
                "the instructions must name `{first_move}` — it is a first move, \
                 and the tool name plus this block is all an agent sees before \
                 it decides whether Latiq is relevant"
            );
            assert!(
                tools.iter().any(|t| t == first_move),
                "the instructions send an agent's FIRST call to `{first_move}`, \
                 which this server does not advertise: {tools:?}"
            );
        }

        // 2. Nothing tool-shaped in the text points at a tool nobody serves.
        let mut named: Vec<String> = Vec::new();
        for word in INSTRUCTIONS.split(|c: char| !(c.is_ascii_lowercase() || c == '_')) {
            // A tool name never starts or ends with `_`, which is what tells a
            // response field (`_meta`) apart from a call.
            if !word.contains('_')
                || word.starts_with('_')
                || word.ends_with('_')
                || NON_TOOL_WORDS.contains(&word)
            {
                continue;
            }
            assert!(
                tools.iter().any(|t| t == word),
                "the instructions mention `{word}`, which is neither a tool this \
                 server advertises ({tools:?}) nor listed in NON_TOOL_WORDS — a \
                 renamed tool leaves an agent's first move pointing at nothing"
            );
            if !named.contains(&word.to_string()) {
                named.push(word.to_string());
            }
        }
        assert!(
            named.len() >= 8,
            "only {} tool names were found in the instructions ({named:?}) — the \
             scan must be finding them, or this guard checks nothing",
            named.len()
        );
    }

    /// **A resource must not teach a tool this build does not serve.**
    ///
    /// The repo has already shipped one that did, and the cost lands on the
    /// agent: it reads the page a `see` routed it to, calls what the page names,
    /// and gets `unknown tool`. The guard above holds the server `instructions`
    /// to this; the resources are the other half, and they carry far more tool
    /// names — the more so now that the descriptions are lean and the teaching
    /// lives there.
    ///
    /// The scan is restricted to words starting with a prefix of a tool this
    /// server really advertises (`read_`, `list_`, `pull_`, …), computed from the
    /// router rather than written down. That keeps it from drowning in the
    /// snake_case that is NOT a tool call — `commit_extra_info`,
    /// `estimated_rows_scanned`, `information_schema` — while still catching the
    /// case it exists for: a name that stopped being a tool.
    #[test]
    fn mcp_resources_name_only_tools_this_server_advertises() {
        let tools = advertised_tools();
        assert!(tools.len() >= 13, "the router advertises {tools:?}");
        // `read_query` -> `read_`. The prefixes come from the tools themselves,
        // so a renamed tool renames the scan with it.
        let prefixes: Vec<String> = tools
            .iter()
            .filter_map(|t| t.split_once('_').map(|(head, _)| format!("{head}_")))
            .collect();
        // The only tool-prefixed words in these bodies that are NOT calls:
        // DuckDB table functions, and the error kinds (`read_only_violation`).
        // Kinds come from the enum so the list cannot fall behind it.
        let sql_functions = ["read_csv", "read_json_auto", "read_parquet"];
        let mut found: Vec<String> = Vec::new();
        for (uri, body) in crate::resources::all_bodies() {
            let words = body.split(|c: char| !(c.is_ascii_lowercase() || c == '_'));
            for word in words {
                if word.is_empty()
                    || !prefixes.iter().any(|p| word.starts_with(p.as_str()))
                    || sql_functions.contains(&word)
                    || latiq_common::ErrorKind::ALL
                        .iter()
                        .any(|k| k.as_str() == word)
                {
                    continue;
                }
                assert!(
                    tools.iter().any(|t| t == word),
                    "{uri} names `{word}`, which is not a tool this server advertises \
                     ({tools:?}) — an agent following this page calls a tool nobody serves"
                );
                if !found.contains(&word.to_string()) {
                    found.push(word.to_string());
                }
            }
        }
        // Anti-vacuity: a prefix scan matching nothing would pass perfectly.
        assert!(
            found.len() >= 8,
            "the scan found only {found:?} tool names across every resource — it is not \
             finding them, so it is guarding nothing"
        );
    }

    /// The tool descriptions this server advertises, `(name, description)`.
    fn tool_descriptions() -> Vec<(String, String)> {
        LatiqServer::tool_router()
            .list_all()
            .into_iter()
            .map(|t| {
                let d = t
                    .description
                    .clone()
                    .unwrap_or_else(|| panic!("{} has no description", t.name));
                (t.name.to_string(), d.to_string())
            })
            .collect()
    }

    /// **The context budget, pinned — this is the experiment.**
    ///
    /// A client in a large tool belt DEFERS tool descriptions and evicts them
    /// under context pressure, then re-fetches them. Nexus measured an agent
    /// doing real work against this server spend 15 `ToolSearch` round trips
    /// re-fetching our schemas for 22 `read_query` calls, because the
    /// descriptions were written as mini-tutorials (9,390 chars, ~2.3k tokens
    /// across 13 tools) and were expensive to hold.
    ///
    /// So the teaching lives in `latiq://` resources — fetched once, by an agent
    /// that decided it needed them — and a description carries only the
    /// contract: what the tool does, its arguments, any rule whose omission
    /// SILENTLY costs something (write_query's transaction rule), and the
    /// resource with the rest. The budget is the whole point of the change, so
    /// it is asserted rather than hoped for.
    #[test]
    fn mcp_tool_descriptions_stay_within_the_context_budget() {
        const TOTAL: usize = 2_500;
        const PER_TOOL: usize = 400;
        let tools = tool_descriptions();
        assert!(
            tools.len() >= 13,
            "the router advertises {} tools — the budget below would be measuring \
             almost nothing",
            tools.len()
        );
        let mut total = 0;
        for (name, d) in &tools {
            // Anti-vacuity in the other direction: a description trimmed to
            // nothing satisfies a byte budget and tells the model nothing.
            assert!(
                d.len() >= 60,
                "{name}'s description ({} chars) is too short to be a contract: {d:?}",
                d.len()
            );
            assert!(
                d.len() <= PER_TOOL,
                "{name}'s description is {} chars (max {PER_TOOL}) — move the teaching \
                 into a latiq:// resource and point at it",
                d.len()
            );
            total += d.len();
        }
        assert!(
            total <= TOTAL,
            "the 13 tool descriptions total {total} chars (~{} tokens), over the {TOTAL} \
             budget: every client that defers and re-fetches them pays this repeatedly",
            total / 4
        );
    }

    /// **A resolving link is not a working one.**
    ///
    /// Each description now ends in a pointer instead of the paragraph it used
    /// to carry, so the pointer has to land on a body that actually says the
    /// thing. Two assertions: every `latiq://` URI in a description RESOLVES,
    /// and each relocated rule is FOUND in the resource its tool now sends the
    /// agent to. Without the second half this passes for a link to a page about
    /// something else — which is exactly how content gets lost in a move.
    #[test]
    fn mcp_tool_descriptions_point_at_resources_that_carry_what_they_promise() {
        // (tool, the resource it points at, a phrase that must be IN that body).
        // Each row is a sentence that used to live in the tool description.
        let promises: &[(&str, &str, &str)] = &[
            // "provenance is fixed at allocation" — the rest of the story.
            (
                "allocate_pond",
                "latiq://guidance",
                "CANNOT be turned on later",
            ),
            // the SHOW TABLES / DESCRIBE / duckdb_columns teaching.
            (
                "describe_pond",
                "latiq://guidance",
                "describe_pond is one call",
            ),
            // the ~10k cap and what to do about it.
            (
                "read_query",
                "latiq://recipes/large-results",
                "Aggregate server-side",
            ),
            // COMMENT ON, and the `--` form that stores nothing (issue #95).
            ("write_query", "latiq://recipes/schema-design", "COMMENT ON"),
            (
                "write_query",
                "latiq://recipes/schema-design",
                "stores NOTHING",
            ),
            // read_csv/s3 loading, and the #79 allow-list caveat.
            (
                "write_query",
                "latiq://recipes/data-ingestion-m1",
                "read_csv",
            ),
            ("write_query", "latiq://recipes/data-ingestion-m1", "#79"),
            // the field-by-field explain walkthrough.
            ("explain_query", "latiq://recipes/large-results", "raw_plan"),
            (
                "explain_query",
                "latiq://recipes/large-results",
                "no time or byte estimate",
            ),
            // dataset vs catalog, the credential rule, the pull shape.
            ("list_datasets", "latiq://recipes/external-data", "Datasets"),
            (
                "load_dataset",
                "latiq://recipes/external-data",
                "<dataset>.<table>",
            ),
            ("list_catalogs", "latiq://recipes/external-data", "Catalogs"),
            (
                "describe_catalog",
                "latiq://recipes/external-data",
                "never stored",
            ),
            (
                "pull_catalog",
                "latiq://recipes/external-data",
                "never queried live",
            ),
            // paging, the page bounds, the facets, and read_json_auto.
            ("get_lineage", "latiq://recipes/lineage", "limit_applied"),
            ("get_lineage", "latiq://recipes/lineage", "256 KB"),
            ("get_lineage", "latiq://recipes/lineage", "read_json_auto"),
            (
                "get_lineage",
                "latiq://recipes/lineage",
                "OpenLineage 2-0-2",
            ),
        ];

        let tools = tool_descriptions();
        let description_of = |name: &str| -> String {
            tools
                .iter()
                .find(|(n, _)| n == name)
                .unwrap_or_else(|| panic!("{name} is not a tool this server advertises"))
                .1
                .clone()
        };

        for (tool, uri, needle) in promises {
            let d = description_of(tool);
            assert!(
                d.contains(uri),
                "{tool}'s description no longer points at {uri}, which is where its \
                 {needle:?} was moved: {d:?}"
            );
            let body = crate::resources::all_bodies()
                .find(|(u, _)| u == uri)
                .unwrap_or_else(|| panic!("{uri} is not served"))
                .1;
            assert!(
                body.contains(needle),
                "{tool} sends the agent to {uri} for {needle:?}, and that body does not \
                 contain it — the sentence was dropped, not relocated"
            );
        }

        // …and nothing points anywhere else. Every URI mentioned by a
        // description OR by an argument's own description (they ride in the same
        // `tools/list` payload and moved teaching out the same way) must resolve.
        let mut linked = 0;
        for tool in LatiqServer::tool_router().list_all() {
            let name = tool.name.to_string();
            let schema = serde_json::to_string(&tool.input_schema).expect("a JSON schema");
            for text in [tool.description.unwrap_or_default().to_string(), schema] {
                for (at, _) in text.match_indices("latiq://") {
                    let uri: String = text[at..]
                        .chars()
                        .take_while(|c| !c.is_whitespace() && !"`,\"\\".contains(*c))
                        .collect();
                    let uri = uri.trim_end_matches('.');
                    assert!(
                        crate::resources::read_resource(uri).is_some(),
                        "{name} points at {uri}, which this server does not serve"
                    );
                    linked += 1;
                }
            }
        }
        // Anti-vacuity for both loops.
        assert!(
            linked >= 12,
            "only {linked} latiq:// links across all descriptions — with the teaching \
             moved out, a description without a pointer strands the agent"
        );
        assert!(promises.len() >= 15, "the relocation table has shrunk");
    }

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
