//! `GrpcForwarder` — the transport behind agent-core's `Forwarder` trait. When a
//! node receives a request for a pond owned by a *different* node, AgentOps calls
//! this to run the op on the owner via its Data gRPC and relay the result. The
//! peer's `JsonResponse` is re-hydrated into the same neutral result types a
//! local op returns (so MCP/CLI can't tell a forwarded result from a local one).
//!
//! This keeps the transport out of agent-core (invariant 5): the core knows only
//! the owner's endpoint string; the gRPC client lives here in the adapter layer.
use crate::wire::query_result_from_json;
use latiq_agent_core::{AgentError, DescribeResult, Forwarder};
use latiq_common::{ErrorEnvelope, Identity};
use latiq_engine::{ExplainResult, QueryResult};
use latiq_proto::v1::data_client::DataClient;
use latiq_proto::v1::*;
use std::collections::HashMap;
use tokio::sync::Mutex;
use tonic::metadata::MetadataValue;
use tonic::transport::Channel;
use tonic::{Code, Request, Status};

#[derive(Default)]
pub struct GrpcForwarder {
    /// One channel per peer endpoint. tonic channels multiplex concurrent RPCs,
    /// so caching + cloning is the cheap, correct reuse pattern.
    clients: Mutex<HashMap<String, DataClient<Channel>>>,
}

impl GrpcForwarder {
    pub fn new() -> Self {
        Self::default()
    }

    async fn client(&self, endpoint: &str) -> Result<DataClient<Channel>, AgentError> {
        let mut map = self.clients.lock().await;
        if let Some(c) = map.get(endpoint) {
            return Ok(c.clone());
        }
        let c = DataClient::connect(endpoint.to_string())
            .await
            .map_err(|e| AgentError::internal(format!("forward connect {endpoint}: {e}")))?;
        map.insert(endpoint.to_string(), c.clone());
        Ok(c)
    }
}

/// Tag the forwarded request with the caller's identity so the owning node
/// attributes the op to the original agent, not to this node.
fn with_identity<T>(msg: T, id: &Identity) -> Request<T> {
    let mut req = Request::new(msg);
    if let Ok(v) = MetadataValue::try_from(id.agent_id.as_str()) {
        req.metadata_mut().insert("latiq-agent-id", v);
    }
    req
}

/// Rebuild a structured `AgentError` from the peer's `Status`. The Data service
/// puts the JSON `ErrorEnvelope` in `details`, so prefer it (kind/suggest/see
/// survive the hop); fall back to code-based mapping.
fn status_to_error(s: Status) -> AgentError {
    if !s.details().is_empty() {
        if let Ok(env) = serde_json::from_slice::<ErrorEnvelope>(s.details()) {
            return AgentError::new(env.kind, env.message, env.suggest, env.see);
        }
    }
    match s.code() {
        Code::NotFound => AgentError::pond_not_found(s.message()),
        Code::AlreadyExists => AgentError::name_conflict(s.message()),
        _ => AgentError::internal(s.message().to_string()),
    }
}

fn parse_json(json: &str) -> Result<serde_json::Value, AgentError> {
    serde_json::from_str(json)
        .map_err(|e| AgentError::internal(format!("forward decode json: {e}")))
}

#[async_trait::async_trait]
impl Forwarder for GrpcForwarder {
    async fn read(
        &self,
        endpoint: &str,
        identity: &Identity,
        pond: &str,
        sql: &str,
    ) -> Result<QueryResult, AgentError> {
        let mut c = self.client(endpoint).await?;
        let req = with_identity(
            QueryRequest {
                pond: pond.to_string(),
                sql: sql.to_string(),
            },
            identity,
        );
        let resp = c
            .read_query(req)
            .await
            .map_err(status_to_error)?
            .into_inner();
        query_result_from_json(&parse_json(&resp.json)?)
    }

    async fn write(
        &self,
        endpoint: &str,
        identity: &Identity,
        pond: &str,
        sql: &str,
    ) -> Result<QueryResult, AgentError> {
        let mut c = self.client(endpoint).await?;
        let req = with_identity(
            QueryRequest {
                pond: pond.to_string(),
                sql: sql.to_string(),
            },
            identity,
        );
        let resp = c
            .write_query(req)
            .await
            .map_err(status_to_error)?
            .into_inner();
        query_result_from_json(&parse_json(&resp.json)?)
    }

    async fn explain(
        &self,
        endpoint: &str,
        identity: &Identity,
        pond: &str,
        sql: &str,
    ) -> Result<ExplainResult, AgentError> {
        let mut c = self.client(endpoint).await?;
        let req = with_identity(
            QueryRequest {
                pond: pond.to_string(),
                sql: sql.to_string(),
            },
            identity,
        );
        let resp = c
            .explain_query(req)
            .await
            .map_err(status_to_error)?
            .into_inner();
        // ExplainResult is encoded by the Data service as plain serde JSON, so the
        // serde inverse re-hydrates it exactly.
        serde_json::from_value(parse_json(&resp.json)?)
            .map_err(|e| AgentError::internal(format!("forward decode explain: {e}")))
    }

    async fn describe(
        &self,
        endpoint: &str,
        identity: &Identity,
        pond: &str,
    ) -> Result<DescribeResult, AgentError> {
        let mut c = self.client(endpoint).await?;
        let req = with_identity(
            DescribePondRequest {
                pond: pond.to_string(),
            },
            identity,
        );
        let resp = c
            .describe_pond(req)
            .await
            .map_err(status_to_error)?
            .into_inner();
        serde_json::from_value(parse_json(&resp.json)?)
            .map_err(|e| AgentError::internal(format!("forward decode describe: {e}")))
    }

    async fn drop_pond(
        &self,
        endpoint: &str,
        identity: &Identity,
        pond: &str,
        confirm: bool,
    ) -> Result<(), AgentError> {
        let mut c = self.client(endpoint).await?;
        let req = with_identity(
            DropPondRequest {
                pond: pond.to_string(),
                confirm,
            },
            identity,
        );
        c.drop_pond(req).await.map_err(status_to_error)?;
        Ok(())
    }
}
