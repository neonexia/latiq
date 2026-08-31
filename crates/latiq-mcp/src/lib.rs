//! latiq-mcp — MCP-over-HTTP surface adapter (rmcp) onto latiq-agent-core.
pub mod encode;
pub mod resources;
pub mod server;

pub use server::{
    advertised_mcp_url, protected_resource_metadata_url, resolve_public_mcp_url, serve_mcp,
    serve_mcp_with_listener, LatiqServer,
};
