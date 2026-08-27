//! The Data/Query gRPC surface — a second inbound adapter onto `AgentOps`, for
//! the CLI and SDK (NOT agents). With an issuer configured this surface is an
//! OAuth 2.1 resource server: an `authorization: Bearer <jwt>` is required and
//! verified, and `latiq-agent-id` is only the claimed leaf. Without one identity
//! stays relaxed (claimed, default anonymous). Errors map to a tonic `Status`
//! whose code derives from the ErrorKind and whose `details` carry the
//! JSON-encoded `ErrorEnvelope` (so the client can render kind/suggest/see).
use crate::wire::query_value;
use latiq_agent_core::{AgentError, AgentOps};
use latiq_auth::Verifier;
use latiq_common::{ErrorKind, Identity};
use latiq_proto::v1::data_server::Data;
use latiq_proto::v1::*;
use std::sync::Arc;
use tonic::{Code, Request, Response, Status};

pub struct DataService {
    ops: Arc<AgentOps>,
    verifier: Option<Arc<Verifier>>,
    /// The `WWW-Authenticate` value handed back on a rejection, built once.
    challenge: Option<String>,
}

impl DataService {
    pub fn new(ops: Arc<AgentOps>) -> Self {
        Self {
            ops,
            verifier: None,
            challenge: None,
        }
    }

    /// Require verified bearer tokens on this surface. `None` keeps the relaxed
    /// (embedded / dev) path.
    pub fn with_verifier(mut self, verifier: Option<Arc<Verifier>>) -> Self {
        self.verifier = verifier;
        self
    }

    /// The RFC 9728 protected-resource metadata URL to advertise on a rejection.
    /// See `challenge_of` for what a node passes here.
    pub fn with_metadata_url(mut self, metadata_url: Option<&str>) -> Self {
        self.challenge = challenge_of(metadata_url);
        self
    }

    async fn identity<T>(&self, req: &Request<T>) -> Result<(Identity, Option<String>), Status> {
        identity_of(self.verifier.as_ref(), self.challenge.as_deref(), req).await
    }
}

/// Build the challenge string for a metadata URL, if one was configured. Kept
/// here so the Data and Stream surfaces cannot disagree about its shape.
pub(crate) fn challenge_of(metadata_url: Option<&str>) -> Option<String> {
    metadata_url.map(latiq_auth::metadata::challenge_header)
}

/// `Unauthenticated`, carrying the RFC 9728 challenge when we have one.
///
/// gRPC has no 401 and no header of its own for this, but a tonic `Status`
/// carries trailing metadata — so the same `www-authenticate` value the MCP
/// surface returns on a 401 rides along here. Without it a Data/Stream client
/// that is turned away knows only THAT it needs a token, never where to get one.
fn unauthenticated(message: &'static str, challenge: Option<&str>) -> Status {
    let mut status = Status::unauthenticated(message);
    if let Some(value) = challenge.and_then(|c| c.parse().ok()) {
        status.metadata_mut().insert("www-authenticate", value);
    }
    status
}

/// The raw bearer token from the `authorization` metadata, if one is present.
/// The parsing itself lives in `latiq_auth::bearer` — one copy, shared with the
/// Admin surface, so this security-relevant parser cannot drift.
pub(crate) fn bearer_of<T>(req: &Request<T>) -> Option<String> {
    let raw = req
        .metadata()
        .get("authorization")
        .and_then(|v| v.to_str().ok())?;
    latiq_auth::bearer(raw).map(String::from)
}

/// Identity from gRPC metadata, plus the token to replay on a node-to-node hop.
///
/// With a verifier configured, an `authorization: Bearer <jwt>` header is
/// REQUIRED and verified; `latiq-agent-id` then supplies only the claimed leaf.
/// Without one, identity stays relaxed (claimed, default anonymous) — the
/// embedded and dev path.
///
/// The token is returned ONLY when a verifier is configured. A node that never
/// opted into auth must not start capturing whatever `authorization` header a
/// client happens to send — one meant for an upstream gateway, say — and
/// replaying it to a peer over the internal channel. Returning it here rather
/// than re-reading the metadata at each call site is what makes that structural
/// rather than a rule to remember (and saves a second parse per request).
///
/// Every rejection is a fixed string: an unauthenticated caller must not be able
/// to probe our issuer list or key endpoints by reading error text. The detail
/// goes to the operator's log instead.
pub(crate) async fn identity_of<T>(
    verifier: Option<&Arc<Verifier>>,
    challenge: Option<&str>,
    req: &Request<T>,
) -> Result<(Identity, Option<String>), Status> {
    let claimed = req
        .metadata()
        .get("latiq-agent-id")
        .and_then(|v| v.to_str().ok());
    let Some(verifier) = verifier else {
        return Ok((Identity::claimed(claimed), None));
    };
    let Some(token) = bearer_of(req) else {
        return Err(unauthenticated("a bearer token is required", challenge));
    };
    let identity = verifier.verify(&token, claimed).await.map_err(|e| {
        tracing::debug!(error = %e, "bearer token rejected");
        unauthenticated("the bearer token was rejected", challenge)
    })?;
    Ok((identity, Some(token)))
}

pub(crate) fn to_status(e: AgentError) -> Status {
    let env = e.into_envelope();
    let code = match env.kind {
        ErrorKind::PondNotFound => Code::NotFound,
        ErrorKind::NameConflict => Code::AlreadyExists,
        ErrorKind::QueryTimeout => Code::DeadlineExceeded,
        ErrorKind::QueryCancelled => Code::Cancelled,
        ErrorKind::Storage | ErrorKind::Internal => Code::Internal,
        // ParseError / InvalidValue / MissingArgument / ReadOnlyViolation /
        // WriteToReservedSchema / ResultCapExceeded / UriNotAllowed
        _ => Code::InvalidArgument,
    };
    let details = serde_json::to_vec(&env).unwrap_or_default();
    Status::with_details(code, env.message.clone(), details.into())
}

fn json_resp(value: serde_json::Value) -> Response<JsonResponse> {
    Response::new(JsonResponse {
        json: value.to_string(),
    })
}

/// The request's trace id (`latiq-trace-id` metadata), or a fresh one. Propagated
/// node-to-node by the forwarder so a request's spans correlate across nodes.
pub(crate) fn trace_id_of<T>(req: &Request<T>) -> String {
    req.metadata()
        .get("latiq-trace-id")
        .and_then(|v| v.to_str().ok())
        .map(String::from)
        .unwrap_or_else(latiq_agent_core::new_trace_id)
}

/// Run a handler body under the request's trace id + a span, so AgentOps logs
/// carry the id and the forwarder can read it for the node-to-node hop. The
/// caller's bearer token rides the same scope, for the same reason
/// (`latiq_agent_core::bearer` — shared with the MCP adapter, which forwards
/// through the same `AgentOps`).
pub(crate) async fn traced<T>(
    name: &'static str,
    tid: String,
    bearer: Option<String>,
    fut: impl std::future::Future<Output = T>,
) -> T {
    use tracing::Instrument;
    let inner = latiq_agent_core::with_trace_id(
        tid.clone(),
        fut.instrument(tracing::info_span!("rpc", name, trace_id = %tid)),
    );
    latiq_agent_core::with_bearer(bearer, inner).await
}

#[tonic::async_trait]
impl Data for DataService {
    async fn allocate_pond(
        &self,
        req: Request<AllocatePondRequest>,
    ) -> Result<Response<AllocatePondResponse>, Status> {
        // The one handler with no `traced` scope, so neither the trace id nor
        // the bearer token is ambient here. That is safe only because allocation
        // NEVER forwards: `ops.allocate_pond` picks a node through the control
        // plane and returns, without ever calling the forwarder. If it ever does
        // forward, this handler must be wrapped like the rest — otherwise it
        // would reach a peer with no token at all.
        let (id, _tok) = self.identity(&req).await?;
        let r = req.into_inner();
        let name = if r.name.is_empty() {
            None
        } else {
            Some(r.name)
        };
        let policy = if r.policy_json.is_empty() {
            "{}".to_string()
        } else {
            r.policy_json
        };
        let tier = if r.tier.is_empty() {
            "medium".to_string()
        } else {
            r.tier
        };
        let res = self
            .ops
            .allocate_pond(&id, name, &policy, &tier, &[])
            .await
            .map_err(to_status)?;
        Ok(Response::new(AllocatePondResponse {
            pond_id: res.pond_id,
            pond_name: res.pond_name,
        }))
    }

    async fn drop_pond(
        &self,
        req: Request<DropPondRequest>,
    ) -> Result<Response<DropPondResponse>, Status> {
        let (id, tok) = self.identity(&req).await?;
        let tid = trace_id_of(&req);
        let r = req.into_inner();
        let ops = self.ops.clone();
        traced("drop_pond", tid, tok, async move {
            ops.drop_pond(&id, &r.pond, r.confirm)
                .await
                .map_err(to_status)?;
            Ok(Response::new(DropPondResponse {}))
        })
        .await
    }

    async fn describe_pond(
        &self,
        req: Request<DescribePondRequest>,
    ) -> Result<Response<JsonResponse>, Status> {
        let (id, tok) = self.identity(&req).await?;
        let tid = trace_id_of(&req);
        let r = req.into_inner();
        let ops = self.ops.clone();
        traced("describe_pond", tid, tok, async move {
            let d = ops.describe_pond(&id, &r.pond).await.map_err(to_status)?;
            Ok(json_resp(serde_json::to_value(d).unwrap_or_default()))
        })
        .await
    }

    async fn read_query(
        &self,
        req: Request<QueryRequest>,
    ) -> Result<Response<JsonResponse>, Status> {
        let (id, tok) = self.identity(&req).await?;
        let tid = trace_id_of(&req);
        let r = req.into_inner();
        let ops = self.ops.clone();
        // Reads ride the Arrow internal hop, collected to JSON here at the edge.
        traced("read_query", tid, tok, async move {
            let qr = ops
                .read_collected(&id, &r.pond, &r.sql)
                .await
                .map_err(to_status)?;
            Ok(json_resp(query_value(qr)))
        })
        .await
    }

    async fn write_query(
        &self,
        req: Request<QueryRequest>,
    ) -> Result<Response<JsonResponse>, Status> {
        let (id, tok) = self.identity(&req).await?;
        let tid = trace_id_of(&req);
        let r = req.into_inner();
        let ops = self.ops.clone();
        traced("write_query", tid, tok, async move {
            let qr = ops
                .write_query(&id, &r.pond, &r.sql)
                .await
                .map_err(to_status)?;
            Ok(json_resp(query_value(qr)))
        })
        .await
    }

    async fn explain_query(
        &self,
        req: Request<QueryRequest>,
    ) -> Result<Response<JsonResponse>, Status> {
        let (id, tok) = self.identity(&req).await?;
        let tid = trace_id_of(&req);
        let r = req.into_inner();
        let ops = self.ops.clone();
        traced("explain_query", tid, tok, async move {
            let er = ops
                .explain_query(&id, &r.pond, &r.sql)
                .await
                .map_err(to_status)?;
            Ok(json_resp(serde_json::to_value(er).unwrap_or_default()))
        })
        .await
    }

    async fn load_dataset(
        &self,
        req: Request<LoadDatasetRequest>,
    ) -> Result<Response<JsonResponse>, Status> {
        let (id, tok) = self.identity(&req).await?;
        let tid = trace_id_of(&req);
        let r = req.into_inner();
        let ops = self.ops.clone();
        traced("load_dataset", tid, tok, async move {
            let res = ops
                .load_dataset(&id, &r.pond, &r.dataset)
                .await
                .map_err(to_status)?;
            Ok(json_resp(serde_json::to_value(res).unwrap_or_default()))
        })
        .await
    }

    async fn catalog_pull(
        &self,
        req: Request<CatalogPullRequest>,
    ) -> Result<Response<JsonResponse>, Status> {
        let (id, tok) = self.identity(&req).await?;
        let tid = trace_id_of(&req);
        let r = req.into_inner();
        let ops = self.ops.clone();
        traced("catalog_pull", tid, tok, async move {
            let res = ops
                .catalog_pull(
                    &id,
                    &r.pond,
                    &r.catalog,
                    &r.query,
                    r.params.into_iter().collect(),
                )
                .await
                .map_err(to_status)?;
            Ok(json_resp(serde_json::to_value(res).unwrap_or_default()))
        })
        .await
    }

    async fn catalog_describe(
        &self,
        req: Request<CatalogDescribeRequest>,
    ) -> Result<Response<JsonResponse>, Status> {
        let (id, tok) = self.identity(&req).await?;
        let tid = trace_id_of(&req);
        let r = req.into_inner();
        let ops = self.ops.clone();
        traced("catalog_describe", tid, tok, async move {
            let tables = ops
                .catalog_describe(&id, &r.pond, &r.catalog, r.params.into_iter().collect())
                .await
                .map_err(to_status)?;
            let rows: Vec<_> = tables
                .into_iter()
                .map(|(schema, table)| serde_json::json!({"schema": schema, "table": table}))
                .collect();
            Ok(json_resp(
                serde_json::json!({"catalog": r.catalog, "tables": rows}),
            ))
        })
        .await
    }
}
