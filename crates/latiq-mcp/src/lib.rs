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

//! latiq-mcp — MCP-over-HTTP surface adapter (rmcp) onto latiq-agent-core.
pub mod encode;
pub mod resources;
pub mod server;

pub use server::{
    advertised_mcp_url, protected_resource_metadata_url, resolve_public_mcp_url, serve_mcp,
    serve_mcp_with_listener, LatiqServer,
};
