//! latiq-mcp — MCP-over-HTTP surface adapter (rmcp) onto latiq-agent-core.
pub mod encode;
pub mod resources;
pub mod server;

pub use server::{serve_mcp, serve_mcp_with_listener, LatiqServer};
