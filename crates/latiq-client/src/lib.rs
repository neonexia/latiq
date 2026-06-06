//! latiq-client — an MCP client wrapper over the Latiq agent surface. Used by
//! the `latiq` CLI and integration tests to drive the server like an agent.
use anyhow::Result;
use rmcp::model::{CallToolRequestParams, CallToolResult};
use rmcp::service::RunningService;
use rmcp::transport::StreamableHttpClientTransport;
use rmcp::{RoleClient, ServiceExt};
use serde_json::{Map, Value};

/// The decoded outcome of a tool call: the structured result + whether it was an
/// error (in which case `value` is the `ErrorEnvelope`).
#[derive(Debug, Clone)]
pub struct CallOutcome {
    pub value: Value,
    pub is_error: bool,
}

pub struct LatiqClient {
    service: RunningService<RoleClient, ()>,
    agent_id: Option<String>,
}

impl LatiqClient {
    /// Connect to a Latiq MCP endpoint (e.g. `http://127.0.0.1:8080/mcp`) and run
    /// the MCP handshake. `agent_id` is sent as a tool argument on every call.
    pub async fn connect(endpoint: &str, agent_id: Option<String>) -> Result<Self> {
        let transport = StreamableHttpClientTransport::from_uri(endpoint);
        let service = ().serve(transport).await?;
        Ok(Self { service, agent_id })
    }

    async fn call(&self, name: &'static str, mut args: Map<String, Value>) -> Result<CallOutcome> {
        if let Some(a) = &self.agent_id {
            args.insert("agent_id".into(), Value::String(a.clone()));
        }
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
        self.call("drop_pond", pond_arg(pond)).await
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
