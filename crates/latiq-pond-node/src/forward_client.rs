//! `GrpcForwarder` — the transport behind agent-core's `Forwarder` trait. When a
//! node receives a request for a pond owned by a *different* node, AgentOps calls
//! this to run the op on the owner via its Data gRPC and relay the result. The
//! peer's `JsonResponse` is re-hydrated into the same neutral result types a
//! local op returns (so MCP/CLI can't tell a forwarded result from a local one).
//!
//! This keeps the transport out of agent-core (invariant 5): the core knows only
//! the owner's endpoint string; the gRPC client lives here in the adapter layer.
use crate::wire::query_result_from_json;
use arrow::buffer::Buffer;
use arrow::datatypes::SchemaRef;
use arrow::ipc::reader::StreamDecoder;
use arrow::record_batch::RecordBatch;
use latiq_agent_core::{AgentError, ArrowReadStream, DescribeResult, Forwarder, PullResult};
use latiq_common::{ErrorEnvelope, Identity};
use latiq_engine::{ExplainResult, QueryResult};
use latiq_proto::v1::data_client::DataClient;
use latiq_proto::v1::stream_client::StreamClient;
use latiq_proto::v1::*;
use std::collections::HashMap;
use tokio::sync::{mpsc, oneshot, Mutex};
use tokio_stream::wrappers::ReceiverStream;
use tonic::metadata::MetadataValue;
use tonic::transport::Channel;
use tonic::{Code, Request, Status, Streaming};

#[derive(Default)]
pub struct GrpcForwarder {
    /// One channel per peer endpoint. tonic channels multiplex concurrent RPCs,
    /// so caching + cloning is the cheap, correct reuse pattern.
    clients: Mutex<HashMap<String, DataClient<Channel>>>,
    /// Same, for the streaming-Arrow read RPC (shares the peer's Data port).
    stream_clients: Mutex<HashMap<String, StreamClient<Channel>>>,
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

    async fn stream_client(&self, endpoint: &str) -> Result<StreamClient<Channel>, AgentError> {
        let mut map = self.stream_clients.lock().await;
        if let Some(c) = map.get(endpoint) {
            return Ok(c.clone());
        }
        let c = StreamClient::connect(endpoint.to_string())
            .await
            .map_err(|e| AgentError::internal(format!("forward connect {endpoint}: {e}")))?;
        map.insert(endpoint.to_string(), c.clone());
        Ok(c)
    }
}

/// Tag the forwarded request with the caller's identity (so the owner attributes
/// the op to the original agent) and the ambient trace id (so the request's spans
/// correlate across the node hop).
///
/// When the caller presented a bearer token, the ORIGINAL token is replayed so
/// the owner verifies it itself and reaches the same verified identity. Without
/// that, only `latiq-agent-id` would cross the hop and a verified caller would
/// arrive downgraded to claimed — fail-safe for authority, but it silently drops
/// subject/issuer and makes the owner's attribution wrong. We deliberately do
/// NOT send an internal header asserting "already verified": a claim the owner
/// trusts without checking is trust laundering.
fn with_identity<T>(msg: T, id: &Identity) -> Request<T> {
    let mut req = Request::new(msg);
    if let Ok(v) = MetadataValue::try_from(id.agent_id.as_str()) {
        req.metadata_mut().insert("latiq-agent-id", v);
    }
    if let Some(token) = latiq_agent_core::current_bearer() {
        if let Ok(v) = MetadataValue::try_from(format!("Bearer {token}").as_str()) {
            req.metadata_mut().insert("authorization", v);
        }
    }
    if let Some(tid) = latiq_agent_core::current_trace_id() {
        if let Ok(v) = MetadataValue::try_from(tid.as_str()) {
            req.metadata_mut().insert("latiq-trace-id", v);
        }
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

/// Deliver the schema on the oneshot the first time the decoder knows it.
fn deliver_schema(
    schema_tx: &mut Option<oneshot::Sender<Result<SchemaRef, AgentError>>>,
    decoder: &StreamDecoder,
) {
    if schema_tx.is_some() {
        if let Some(sc) = decoder.schema() {
            let _ = schema_tx.take().unwrap().send(Ok(sc));
        }
    }
}

/// Decode a peer's `ArrowChunk` IPC stream back into an `ArrowReadStream`: a
/// background task pushes chunk bytes through a `StreamDecoder`, delivering the
/// schema (once known) on a oneshot and batches on a bounded channel. Per-batch,
/// nothing is fully buffered.
fn decode_arrow_stream(mut streaming: Streaming<ArrowChunk>) -> ArrowReadStreamParts {
    let (schema_tx, schema_rx) = oneshot::channel::<Result<SchemaRef, AgentError>>();
    let (batch_tx, batch_rx) = mpsc::channel::<Result<RecordBatch, AgentError>>(4);

    tokio::spawn(async move {
        let mut decoder = StreamDecoder::new();
        let mut schema_tx = Some(schema_tx);
        loop {
            match streaming.message().await {
                Ok(Some(chunk)) => {
                    let mut buf = Buffer::from_vec(chunk.ipc);
                    loop {
                        match decoder.decode(&mut buf) {
                            Ok(Some(batch)) => {
                                deliver_schema(&mut schema_tx, &decoder);
                                if batch_tx.send(Ok(batch)).await.is_err() {
                                    return;
                                }
                            }
                            Ok(None) => break,
                            Err(e) => {
                                let ae = AgentError::internal(format!("forward arrow decode: {e}"));
                                match schema_tx.take() {
                                    Some(s) => {
                                        let _ = s.send(Err(ae));
                                    }
                                    None => {
                                        let _ = batch_tx.send(Err(ae)).await;
                                    }
                                }
                                return;
                            }
                        }
                        if buf.is_empty() {
                            break;
                        }
                    }
                    // Schema-only chunk / empty result: surface the schema now.
                    deliver_schema(&mut schema_tx, &decoder);
                }
                Ok(None) => {
                    // Stream ended; ensure the schema (or an error) was delivered.
                    if let Some(s) = schema_tx.take() {
                        let _ = match decoder.schema() {
                            Some(sc) => s.send(Ok(sc)),
                            None => s.send(Err(AgentError::internal("forward arrow: no schema"))),
                        };
                    }
                    return;
                }
                Err(status) => {
                    let ae = status_to_error(status);
                    match schema_tx.take() {
                        Some(s) => {
                            let _ = s.send(Err(ae));
                        }
                        None => {
                            let _ = batch_tx.send(Err(ae)).await;
                        }
                    }
                    return;
                }
            }
        }
    });

    ArrowReadStreamParts {
        schema_rx,
        batch_rx,
    }
}

struct ArrowReadStreamParts {
    schema_rx: oneshot::Receiver<Result<SchemaRef, AgentError>>,
    batch_rx: mpsc::Receiver<Result<RecordBatch, AgentError>>,
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

    async fn read_arrow(
        &self,
        endpoint: &str,
        identity: &Identity,
        pond: &str,
        sql: &str,
    ) -> Result<ArrowReadStream, AgentError> {
        let mut c = self.stream_client(endpoint).await?;
        let req = with_identity(
            QueryRequest {
                pond: pond.to_string(),
                sql: sql.to_string(),
            },
            identity,
        );
        let streaming = c
            .read_arrow(req)
            .await
            .map_err(status_to_error)?
            .into_inner();
        let parts = decode_arrow_stream(streaming);
        // The schema (and any pre-stream error) resolves before we hand back the
        // stream, mirroring the local path.
        let schema = parts
            .schema_rx
            .await
            .map_err(|_| AgentError::internal("forward arrow: stream closed before schema"))??;
        Ok(ArrowReadStream {
            schema,
            batches: Box::pin(ReceiverStream::new(parts.batch_rx)),
        })
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

    async fn catalog_pull(
        &self,
        endpoint: &str,
        identity: &Identity,
        pond: &str,
        catalog: &str,
        query: &str,
        params: std::collections::BTreeMap<String, String>,
    ) -> Result<PullResult, AgentError> {
        let mut c = self.client(endpoint).await?;
        let req = with_identity(
            CatalogPullRequest {
                pond: pond.to_string(),
                catalog: catalog.to_string(),
                query: query.to_string(),
                params: params.into_iter().collect(),
            },
            identity,
        );
        let resp = c
            .catalog_pull(req)
            .await
            .map_err(status_to_error)?
            .into_inner();
        serde_json::from_value(parse_json(&resp.json)?)
            .map_err(|e| AgentError::internal(format!("forward decode catalog_pull: {e}")))
    }

    async fn catalog_describe(
        &self,
        endpoint: &str,
        identity: &Identity,
        pond: &str,
        catalog: &str,
        params: std::collections::BTreeMap<String, String>,
    ) -> Result<Vec<(String, String)>, AgentError> {
        let mut c = self.client(endpoint).await?;
        let req = with_identity(
            CatalogDescribeRequest {
                pond: pond.to_string(),
                catalog: catalog.to_string(),
                params: params.into_iter().collect(),
            },
            identity,
        );
        let resp = c
            .catalog_describe(req)
            .await
            .map_err(status_to_error)?
            .into_inner();
        // The Data service encodes describe as {catalog, tables:[{schema,table}]}.
        // Re-hydrate the (schema, table) pairs the core returns.
        let v = parse_json(&resp.json)?;
        let tables = v
            .get("tables")
            .and_then(|t| t.as_array())
            .ok_or_else(|| AgentError::internal("forward decode catalog_describe: no tables"))?;
        Ok(tables
            .iter()
            .filter_map(|t| {
                let schema = t.get("schema")?.as_str()?.to_string();
                let table = t.get("table")?.as_str()?.to_string();
                Some((schema, table))
            })
            .collect())
    }
}
