//! Probe C + D: rmcp Streamable-HTTP server with:
//!   - `echo` tool: single JSON CallToolResult
//!   - `slow_echo` tool: emits progress notifications then final result over SSE
//!
//! Run: cargo run --bin probe_c
//!
//! Test Probe C (JSON echo):
//!   # Initialize (get session id from mcp-session-id header)
//!   curl -si -X POST http://localhost:8888/mcp \
//!     -H 'Content-Type: application/json' \
//!     -H 'Accept: application/json, text/event-stream' \
//!     -d '{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-03-26","capabilities":{},"clientInfo":{"name":"test","version":"1.0"}}}'
//!
//!   # Call echo tool (use session id from above)
//!   curl -s -X POST http://localhost:8888/mcp \
//!     -H 'Content-Type: application/json' \
//!     -H 'Accept: application/json, text/event-stream' \
//!     -H 'mcp-session-id: <SESSION_ID>' \
//!     -d '{"jsonrpc":"2.0","id":2,"method":"tools/call","params":{"name":"echo","arguments":{"message":"hello"}}}'
//!
//! Test Probe D (SSE progress):
//!   curl -sN -X POST http://localhost:8888/mcp \
//!     -H 'Content-Type: application/json' \
//!     -H 'Accept: application/json, text/event-stream' \
//!     -H 'mcp-session-id: <SESSION_ID>' \
//!     -d '{"jsonrpc":"2.0","id":3,"method":"tools/call","params":{"name":"slow_echo","arguments":{"message":"slow"},"_meta":{"progressToken":"tok1"}}}'

use rmcp::{
    Peer, RoleServer, ServerHandler,
    handler::server::{router::tool::ToolRouter, wrapper::Parameters},
    model::{Meta, ProgressNotificationParam, ServerCapabilities, ServerInfo},
    schemars, tool, tool_handler, tool_router,
    transport::streamable_http_server::{
        StreamableHttpServerConfig, StreamableHttpService, session::local::LocalSessionManager,
    },
};
use schemars::JsonSchema;
use serde::Deserialize;
use tokio_util::sync::CancellationToken;

#[derive(Debug, Deserialize, JsonSchema)]
struct EchoRequest {
    #[schemars(description = "The message to echo back")]
    message: String,
}

#[derive(Debug, Clone)]
struct EchoServer {
    tool_router: ToolRouter<Self>,
}

impl EchoServer {
    fn new() -> Self {
        Self {
            tool_router: Self::tool_router(),
        }
    }
}

#[tool_router]
impl EchoServer {
    /// Echo a message back immediately (single JSON response)
    #[tool(description = "Echo a message back (single response)")]
    fn echo(&self, Parameters(EchoRequest { message }): Parameters<EchoRequest>) -> String {
        format!("echo: {message}")
    }

    /// Echo a message after emitting progress notifications (SSE stream)
    #[tool(description = "Echo a message with progress notifications (SSE stream)")]
    async fn slow_echo(
        &self,
        meta: Meta,
        client: Peer<RoleServer>,
        Parameters(EchoRequest { message }): Parameters<EchoRequest>,
    ) -> Result<String, rmcp::ErrorData> {
        let progress_token = meta.get_progress_token();

        for step in 0..3u32 {
            // Send progress notification if client sent a progress token
            if let Some(ref token) = progress_token {
                let _ = client
                    .notify_progress(ProgressNotificationParam {
                        progress_token: token.clone(),
                        progress: step as f64,
                        total: Some(3.0),
                        message: Some(format!("step {step}/3")),
                    })
                    .await;
            }
            tokio::time::sleep(tokio::time::Duration::from_millis(300)).await;
        }

        Ok(format!("slow_echo: {message}"))
    }
}

#[tool_handler(router = self.tool_router)]
impl ServerHandler for EchoServer {
    fn get_info(&self) -> ServerInfo {
        ServerInfo::new(ServerCapabilities::builder().enable_tools().build())
            .with_instructions("Echo server for spike probes C+D")
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let ct = CancellationToken::new();

    let service: StreamableHttpService<EchoServer, LocalSessionManager> =
        StreamableHttpService::new(
            || Ok(EchoServer::new()),
            Default::default(),
            StreamableHttpServerConfig::default()
                .with_cancellation_token(ct.child_token())
                .with_stateful_mode(true),
        );

    let router = axum::Router::new().nest_service("/mcp", service);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:8888").await?;
    let addr = listener.local_addr()?;
    println!("Echo server listening on http://{addr}/mcp");
    println!("Endpoint: POST /mcp");
    println!("Required headers: Content-Type: application/json, Accept: application/json, text/event-stream");
    println!("Session: mcp-session-id header (returned from initialize response)");
    println!("Press Ctrl+C to stop");

    // Handle ctrl-c
    let ct_clone = ct.clone();
    tokio::spawn(async move {
        tokio::signal::ctrl_c().await.ok();
        ct_clone.cancel();
    });

    axum::serve(listener, router)
        .with_graceful_shutdown(async move { ct.cancelled_owned().await })
        .await?;

    println!("Server stopped.");
    Ok(())
}
