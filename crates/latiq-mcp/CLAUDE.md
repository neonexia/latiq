# latiq-mcp — CLAUDE.md

The **MCP-over-HTTP inbound adapter** (rmcp Streamable-HTTP) onto `AgentOps`. **Agent-only.**

## Invariants
- **This surface is for agents (frontier LLMs) only.** The CLI/SDK must never call it — they use the Data/Query gRPC surface. `latiq-client` exists for agent-simulation + the MCP integration tests here.
- **Thin adapter.** Extract identity from the request, call the matching `AgentOps` method, encode the neutral result/error. No business logic — that's `agent-core`.
- **Identity comes from the TRANSPORT, never a tool argument.** The claimed leaf is the `latiq-agent-id` HTTP header; a verified principal is `Authorization: Bearer`. No `agent_id` field on any tool schema — the model must not be able to type an identity.
- **With a verifier, auth is a LAYER in front of the router**, not a check inside handlers: every JSON-RPC method (`initialize`, `tools/list`, `resources/read`, …) is covered, and a missing/invalid token gets a real `401` + `WWW-Authenticate`, not a JSON-RPC error inside a 200. The handler only *reads* the layer's decision; the no-decision branch fails closed, never falls back to a claimed identity. Only `/.well-known/oauth-protected-resource` (RFC 9728, served here — the one HTTP surface we have) is exempt.
- **Publish the URL agents dial** (`resolve_public_mcp_url`): `--public-mcp-url` behind a gateway, NOT `--advertise-addr` (the internal peer-forwarding address) and never the bound socket. A conforming client rejects a `resource` whose origin differs from the URL it dialled.
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
