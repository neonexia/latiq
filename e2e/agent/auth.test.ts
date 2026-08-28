// Auth-mode assertions for the MCP agent surface. These pin what the ordinary
// harness suite structurally cannot: that an UNauthenticated client is refused,
// that the RFC 9728 metadata document is reachable with no credential at all,
// and that the 401 challenge is well-formed and leaks nothing internal.
//
// The whole file self-skips when LATIQ_AUTH_ISSUER is unset, so the REMOTE and
// EMBEDDED runs are untouched. In auth mode it runs inside the compose network
// (`auth-tests-agent`), where LATIQ_MCP points at the gateway and the issuer is
// the in-network Keycloak — one issuer URL for servers and clients alike.
import assert from "node:assert/strict";
import { test } from "node:test";

import { Client } from "@modelcontextprotocol/sdk/client/index.js";
import { StreamableHTTPClientTransport } from "@modelcontextprotocol/sdk/client/streamableHttp.js";
import { ClientCredentialsProvider } from "@modelcontextprotocol/sdk/client/auth-extensions.js";

const ISSUER = process.env.LATIQ_AUTH_ISSUER;
const URL_ = process.env.LATIQ_MCP ?? "http://127.0.0.1:51402/mcp";
const SKIP = !ISSUER ? { skip: "LATIQ_AUTH_ISSUER unset — cluster has no auth" } : {};

// The RFC 9728 document lives at the ORIGIN, not under the /mcp path.
const PRM_URL = new global.URL("/.well-known/oauth-protected-resource", URL_).toString();

// No `expectedIssuer`: ClientCredentialsProviderOptions has no such field in SDK
// 1.29 — passing one would read as issuer pinning that isn't happening.
const provider = () =>
  new ClientCredentialsProvider({
    clientId: "latiq-agent",
    clientSecret: process.env.LATIQ_CLIENT_SECRET ?? "latiq-agent-secret",
    scope: "openid",
  });

// Two negative tests used to live here and no longer do:
//
//   - "an unauthenticated client cannot even initialize" — superseded by
//     `crates/latiq/tests/mcp.rs::auth_mcp_non_tool_methods_require_a_verified_token`,
//     which probes `initialize`, `tools/list`, `resources/read` AND
//     `prompts/list`, and pins the positive counterpart so the refusal is about
//     the credential and not about the method being blocked outright. It was
//     also WEAK: a bare `assert.rejects` with no error predicate passes when the
//     gateway is down, `LATIQ_MCP` is wrong, nginx misroutes, or the node
//     crashed — it could not tell "refused for want of a token" from
//     "unreachable", which is exactly this tier's failure mode. The no-token
//     path is still covered below, by a bare POST that asserts a 401 and a
//     well-formed challenge.
//
//   - "a garbage bearer token is rejected" — superseded by
//     `crates/latiq/tests/mcp.rs::auth_mcp_rejects_an_invalid_token_with_a_401_challenge`,
//     which runs this exact `alg:none` case plus a foreign signature, an expired
//     token, a wrong audience and a non-JWT. A forged token never reaches the
//     IdP, so a real Keycloak proves nothing a fake one does not.

test("auth: the protected-resource metadata is discoverable with no credential", SKIP, async () => {
  const res = await fetch(PRM_URL);
  assert.equal(res.status, 200, `${PRM_URL} must be exempt from the auth middleware`);
  const doc: any = await res.json();
  assert.ok(doc.resource, "RFC 9728 document names the protected resource");
  assert.ok(Array.isArray(doc.authorization_servers), "authorization_servers is a list");
  assert.equal(doc.authorization_servers[0], ISSUER, "the trusted issuer is advertised");
});

test("auth: a 401 carries a WWW-Authenticate challenge that leaks nothing", SKIP, async () => {
  // A bare POST with no Authorization header: the challenge must point the client
  // at the metadata document and nowhere else.
  const res = await fetch(URL_, {
    method: "POST",
    headers: { "content-type": "application/json", accept: "application/json, text/event-stream" },
    body: JSON.stringify({ jsonrpc: "2.0", id: 1, method: "tools/list", params: {} }),
  });
  assert.equal(res.status, 401, "an unauthenticated JSON-RPC call is 401");

  const challenge = res.headers.get("www-authenticate") ?? "";
  assert.ok(challenge.toLowerCase().startsWith("bearer"), `Bearer challenge expected, got: ${challenge}`);
  assert.ok(challenge.includes("resource_metadata="), `challenge points at the metadata doc: ${challenge}`);
  // It must not leak where the IdP's keys actually live.
  assert.ok(!/jwks/i.test(challenge), `challenge must not leak a JWKS URI: ${challenge}`);

  // KEEP. The *code* property — that the node advertises whatever public URL it
  // was configured with, and that the challenge is built from it — is already
  // proven in-process by `crates/latiq-mcp/tests/mcp_auth.rs` and
  // `crates/latiq-pond-node/tests/public_mcp_url.rs`. What is uniquely e2e here
  // is a CONFIG assertion those cannot make: that the compose file actually sets
  // `LATIQ_PUBLIC_MCP_URL` on each node, and that nginx does not rewrite or drop
  // it on the way back out. Either mistake ships a cluster whose challenge points
  // real clients at an unreachable internal address, with the whole Rust suite
  // still green.
  //
  // The metadata URL must sit on the SAME origin the client dialled. That is the
  // property that matters, and it is what a conforming client enforces: publish a
  // node's internal address while the client came in through a gateway and it
  // refuses the document outright. Asserting the origin rather than blocklisting
  // hostnames keeps this honest wherever it runs -- `gateway:51510` under compose,
  // `127.0.0.1:51406` under ./dev.sh, both legitimately public.
  const dialled = new global.URL(URL_).origin;
  const advertised = challenge.match(/resource_metadata="([^"]+)"/)?.[1] ?? "";
  assert.equal(new global.URL(advertised).origin, dialled,
    `challenge must advertise the origin the client dialled: ${challenge}`);
});

test("auth: a client_credentials token gets a working session", SKIP, async () => {
  // The one thing only this tier can prove: the OFFICIAL MCP SDK's full
  // client-side OAuth handshake against a real authorization server —
  // 401 → RFC 9728 protected-resource discovery → AS metadata discovery →
  // `client_credentials` grant → retry with the bearer token. Nothing in
  // `cargo test` drives a real client through that sequence.
  //
  // Deliberately NOT asserted here: that `agent_id` is absent from the tool
  // schemas. That is a schema property with no dependency on a real IdP and is
  // pinned exactly by `crates/latiq/tests/mcp.rs::auth_mcp_tool_schemas_do_not_expose_agent_id`.
  const client = new Client({ name: "latiq-agent-harness-auth", version: "0.0.0" });
  await client.connect(
    new StreamableHTTPClientTransport(new global.URL(URL_), { authProvider: provider() }),
  );
  const tools = await client.listTools();
  assert.ok(tools.tools.length > 0, "an authenticated client sees the tool surface");
  await client.close();
});
