//! latiq-mcp — MCP-over-HTTP surface adapter (rmcp) onto latiq-agent-core.
pub mod encode;
pub mod server;

pub use server::{serve_mcp, LatiqServer};
