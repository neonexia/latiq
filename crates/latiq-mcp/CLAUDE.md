# latiq-mcp — CLAUDE.md

The **MCP-over-HTTP inbound adapter** (rmcp Streamable-HTTP) onto `AgentOps`. **Agent-only.**

## Invariants
- **This surface is for agents (frontier LLMs) only.** The CLI/SDK must never call it — they use the Data/Query gRPC surface. `latiq-client` exists for agent-simulation + the MCP integration tests here.
- **Thin adapter.** Extract identity from the request, call the matching `AgentOps` method, encode the neutral result/error. No business logic — that's `agent-core`.
- **Dual encoding:** results carry BOTH a text content block and `structured_content`; errors set `is_error` with the `ErrorEnvelope`. `CallToolResult`/`CallToolRequestParams` are `#[non_exhaustive]` — build via `success()/error()`/`default()` + field set, not struct literals.
- **schemars derive must target rmcp's re-export:** `#[schemars(crate = "rmcp::schemars")]` on arg structs (else a trait-mismatch with a standalone schemars).

## MCP is the product for agents — complete it (M10)
This surface must make a frontier agent immediately effective:
- **Tool annotations** (`read_only_hint`/`destructive_hint`/`idempotent_hint`) on every tool.
- **Mini-tutorial tool descriptions** (what / when-vs-alternatives / concrete SQL / do-don't / `see`).
- **Resources:** `latiq://guidance`, `latiq://dialect`, `latiq://recipes/*`, `latiq://troubleshooting/*`, `latiq://ponds`, `latiq://ponds/{id}/schema`.
- **Prompts:** the 4 SOPs (setup-multi-agent-pond, discover-existing-pond, design-collaborative-schema, recover-from-conflict).
- Errors are next-action-oriented; `see` links must resolve to a real resource.

## Tests
`tests/mcp_e2e.rs` (server+client over real MCP). Surface-level e2e: `crates/latiq/tests/mcp.rs`. Exercise tools, annotations, resources, prompts, and the structured-error path.
