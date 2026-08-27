//! latiq-client — an MCP client wrapper over the Latiq agent surface. Used by
//! the `latiq` CLI and integration tests to drive the server like an agent.
use anyhow::Result;
use http::{HeaderName, HeaderValue};
use rmcp::model::{CallToolRequestParams, CallToolResult};
use rmcp::service::RunningService;
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
        let mut config = StreamableHttpClientTransportConfig::with_uri(endpoint);
        if let Some(a) = agent_id {
            config = config.custom_headers(HashMap::from([(
                HeaderName::from_static("latiq-agent-id"),
                HeaderValue::from_str(&a)?,
            )]));
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
