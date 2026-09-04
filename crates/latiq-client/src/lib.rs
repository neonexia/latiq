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

//! latiq-client — an MCP client wrapper over the Latiq agent surface. For
//! integration tests and agent simulation ONLY (invariant 1: the CLI and SDK are
//! not agents and speak gRPC, never MCP). A dev-dependency, never shipped.
use anyhow::Result;
use http::{HeaderName, HeaderValue};
use rmcp::model::{
    CallToolRequest, CallToolRequestParams, CallToolResult, CancelledNotification,
    CancelledNotificationParam, ClientNotification, ClientRequest, ServerResult,
};
use rmcp::service::{PeerRequestOptions, RunningService};
use rmcp::transport::streamable_http_client::StreamableHttpClientTransportConfig;
use rmcp::transport::StreamableHttpClientTransport;
use rmcp::{RoleClient, ServiceExt};
use serde_json::{Map, Value};
use std::collections::HashMap;

/// The decoded outcome of a tool call: the structured result + whether it was an
/// error (in which case `value` is the `ErrorEnvelope`).
#[derive(Debug, Clone)]
pub struct CallOutcome {
    pub value: Value,
    pub is_error: bool,
}

/// A connected MCP session, standing in for an agent. Tests drive the agent
/// surface through this so they exercise the same path a real model does —
/// nothing shipped may depend on it (invariant 1).
pub struct LatiqClient {
    service: RunningService<RoleClient, ()>,
}

impl LatiqClient {
    /// Connect to a Latiq MCP endpoint (e.g. `http://127.0.0.1:8080/mcp`) and run
    /// the MCP handshake. `agent_id` is the CLAIMED leaf id and rides the
    /// `latiq-agent-id` HTTP header on every request — never a tool argument
    /// (an argument is typed by the model; a header is not reachable by it).
    pub async fn connect(endpoint: &str, agent_id: Option<String>) -> Result<Self> {
        Self::connect_with_token(endpoint, agent_id, None).await
    }

    /// Connect presenting a bearer token — the VERIFIED principal on an
    /// auth-enabled deployment. The token rides `Authorization: Bearer` on every
    /// request, including the handshake and the SSE stream.
    pub async fn connect_with_token(
        endpoint: &str,
        agent_id: Option<String>,
        token: Option<String>,
    ) -> Result<Self> {
        Self::connect_traced(endpoint, agent_id, token, None).await
    }

    /// Connect presenting a W3C `traceparent`, so every call on this connection
    /// joins a trace the caller already started.
    ///
    /// Agent-simulation only, like the rest of this crate — but a real one: an
    /// orchestrator that drives several agents through Latiq is exactly the
    /// caller that has a trace of its own, and it is what makes the id an agent
    /// reads back (in `_meta.trace_id`, or on a failed call's envelope) joinable
    /// to something outside Latiq.
    pub async fn connect_traced(
        endpoint: &str,
        agent_id: Option<String>,
        token: Option<String>,
        traceparent: Option<String>,
    ) -> Result<Self> {
        let mut config = StreamableHttpClientTransportConfig::with_uri(endpoint);
        let mut headers = HashMap::new();
        if let Some(a) = agent_id {
            headers.insert(
                HeaderName::from_static("latiq-agent-id"),
                HeaderValue::from_str(&a)?,
            );
        }
        if let Some(tp) = traceparent {
            headers.insert(
                HeaderName::from_static("traceparent"),
                HeaderValue::from_str(&tp)?,
            );
        }
        if !headers.is_empty() {
            config = config.custom_headers(headers);
        }
        if let Some(t) = token {
            config = config.auth_header(t);
        }
        let transport = StreamableHttpClientTransport::from_config(config);
        let service = ().serve(transport).await?;
        Ok(Self { service })
    }

    async fn call(&self, name: &'static str, args: Map<String, Value>) -> Result<CallOutcome> {
        // CallToolRequestParams is #[non_exhaustive]: build via Default + fields.
        let mut params = CallToolRequestParams::default();
        params.name = name.into();
        params.arguments = Some(args);
        let res: CallToolResult = self.service.call_tool(params).await?;
        Ok(CallOutcome {
            value: res.structured_content.unwrap_or(Value::Null),
            is_error: res.is_error.unwrap_or(false),
        })
    }

    pub async fn allocate_pond(&self, name: Option<&str>) -> Result<CallOutcome> {
        let mut a = Map::new();
        if let Some(n) = name {
            a.insert("name".into(), Value::String(n.into()));
        }
        self.call("allocate_pond", a).await
    }

    pub async fn list_ponds(&self) -> Result<CallOutcome> {
        self.call("list_ponds", Map::new()).await
    }

    pub async fn describe_pond(&self, pond: &str) -> Result<CallOutcome> {
        self.call("describe_pond", pond_arg(pond)).await
    }

    pub async fn drop_pond(&self, pond: &str) -> Result<CallOutcome> {
        // Agent-sim: confirm the destructive op by default (the gate is enforced
        // server-side; tests that need the un-confirmed path call drop_pond_raw).
        let mut a = pond_arg(pond);
        a.insert("confirm".into(), Value::Bool(true));
        self.call("drop_pond", a).await
    }

    /// Drive drop_pond with an explicit `confirm` value (to exercise the gate).
    pub async fn drop_pond_raw(&self, pond: &str, confirm: bool) -> Result<CallOutcome> {
        let mut a = pond_arg(pond);
        a.insert("confirm".into(), Value::Bool(confirm));
        self.call("drop_pond", a).await
    }

    pub async fn query(&self, pond: &str, sql: &str) -> Result<CallOutcome> {
        self.call("read_query", query_args(pond, sql)).await
    }

    pub async fn write(&self, pond: &str, sql: &str) -> Result<CallOutcome> {
        self.call("write_query", query_args(pond, sql)).await
    }

    pub async fn explain(&self, pond: &str, sql: &str) -> Result<CallOutcome> {
        self.call("explain_query", query_args(pond, sql)).await
    }

    /// Call a tool and then, after `after`, cancel THAT call the way a real MCP
    /// client does: a `notifications/cancelled` carrying the request's id.
    ///
    /// This is the only honest way to exercise the cancel path. Dropping the
    /// connection is NOT a cancel source (rmcp never surfaces a disconnect to
    /// the handler — see `latiq-mcp`'s CLAUDE.md), so a test that killed the
    /// client would prove nothing about cancellation.
    ///
    /// **Normally returns `Err(ServiceError::Cancelled)`, and that is correct.**
    /// rmcp resolves the caller's request the instant it sends the notification
    /// and discards whatever the server eventually answers — which is what the
    /// MCP spec asks of a client, since it has said it no longer wants the
    /// result. So the server's `query_cancelled` envelope is NOT observable from
    /// here, and a test that expects to read it is testing nothing; observe the
    /// cancel's EFFECT on the server instead (see `crates/latiq/tests/mcp.rs`),
    /// and pin the envelope's kind below the transport
    /// (`latiq-agent-core/tests/agent_ops.rs`).
    ///
    /// `Ok` therefore means only one thing: the server answered before the
    /// cancel went out — the call was too fast to cancel.
    pub async fn call_tool_then_cancel(
        &self,
        name: &'static str,
        args: Map<String, Value>,
        after: std::time::Duration,
    ) -> Result<CallOutcome> {
        let mut params = CallToolRequestParams::default();
        params.name = name.into();
        params.arguments = Some(args);
        let handle = self
            .service
            .send_cancellable_request(
                ClientRequest::CallToolRequest(CallToolRequest::new(params)),
                PeerRequestOptions::no_options(),
            )
            .await?;
        // Taken BEFORE awaiting: `RequestHandle::cancel` consumes the handle and
        // with it the response channel, and the response is the whole point.
        let peer = handle.peer.clone();
        let request_id = handle.id.clone();
        tokio::spawn(async move {
            tokio::time::sleep(after).await;
            let _ = peer
                .send_notification(ClientNotification::CancelledNotification(
                    CancelledNotification::new(CancelledNotificationParam {
                        request_id,
                        reason: Some("agent no longer needs this result".into()),
                    }),
                ))
                .await;
        });
        let res: CallToolResult = match handle.await_response().await? {
            ServerResult::CallToolResult(r) => r,
            other => anyhow::bail!("unexpected response to a tool call: {other:?}"),
        };
        Ok(CallOutcome {
            value: res.structured_content.unwrap_or(Value::Null),
            is_error: res.is_error.unwrap_or(false),
        })
    }

    /// Call any tool by name with arbitrary args (agent-sim escape hatch for the
    /// dataset/catalog tools that don't have typed wrappers here).
    pub async fn call_tool(
        &self,
        name: &'static str,
        args: Map<String, Value>,
    ) -> Result<CallOutcome> {
        self.call(name, args).await
    }

    // --- agent-discovery surface (tools/resources/prompts) ---------------

    /// All tools, with their MCP annotations (for inspecting read_only/destructive hints).
    pub async fn list_tools(&self) -> Result<Vec<rmcp::model::Tool>> {
        Ok(self.service.list_all_tools().await?)
    }

    /// Resource URIs advertised by the server.
    pub async fn list_resource_uris(&self) -> Result<Vec<String>> {
        Ok(self
            .service
            .list_all_resources()
            .await?
            .into_iter()
            .map(|r| r.uri.clone())
            .collect())
    }

    /// Read a resource's text body.
    pub async fn read_resource_text(&self, uri: &str) -> Result<String> {
        let res = self
            .service
            .read_resource(rmcp::model::ReadResourceRequestParams::new(uri))
            .await?;
        let text = res
            .contents
            .into_iter()
            .find_map(|c| match c {
                rmcp::model::ResourceContents::TextResourceContents { text, .. } => Some(text),
                _ => None,
            })
            .unwrap_or_default();
        Ok(text)
    }

    /// Prompt names advertised by the server.
    pub async fn list_prompt_names(&self) -> Result<Vec<String>> {
        Ok(self
            .service
            .list_all_prompts()
            .await?
            .into_iter()
            .map(|p| p.name)
            .collect())
    }

    /// Prompts as advertised, including their DECLARED arguments — what a
    /// conforming client reads to build a `prompts/get` call.
    pub async fn list_prompts(&self) -> Result<Vec<rmcp::model::Prompt>> {
        Ok(self.service.list_all_prompts().await?)
    }

    /// Render a prompt (returns the concatenated message text).
    pub async fn get_prompt_text(&self, name: &str, args: Map<String, Value>) -> Result<String> {
        let mut params = rmcp::model::GetPromptRequestParams::default();
        params.name = name.to_string();
        params.arguments = Some(args);
        let res = self.service.get_prompt(params).await?;
        let text = res
            .messages
            .into_iter()
            .filter_map(|m| match m.content {
                rmcp::model::PromptMessageContent::Text { text, .. } => Some(text),
                _ => None,
            })
            .collect::<Vec<_>>()
            .join("\n");
        Ok(text)
    }

    /// Cleanly close the client session.
    pub async fn close(self) -> Result<()> {
        let _ = self.service.cancel().await;
        Ok(())
    }
}

fn pond_arg(pond: &str) -> Map<String, Value> {
    let mut a = Map::new();
    a.insert("pond".into(), Value::String(pond.into()));
    a
}

fn query_args(pond: &str, sql: &str) -> Map<String, Value> {
    let mut a = pond_arg(pond);
    a.insert("sql".into(), Value::String(sql.into()));
    a
}
