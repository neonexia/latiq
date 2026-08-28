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

test("auth: an unauthenticated client cannot even initialize", SKIP, async () => {
  // Every JSON-RPC method needs a verified token now — `initialize` included —
  // so a transport with no authProvider must fail to connect at all.
  const client = new Client({ name: "latiq-agent-harness-anon", version: "0.0.0" });
  await assert.rejects(
    () => client.connect(new StreamableHTTPClientTransport(new global.URL(URL_))),
    "an anonymous MCP client must be rejected",
  );
  await client.close().catch(() => {});
});

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
  // It must not leak where the node or the IdP's keys actually live.
  assert.ok(!/jwks/i.test(challenge), `challenge must not leak a JWKS URI: ${challenge}`);
  assert.ok(!/pond-node|127\.0\.0\.1|localhost|keycloak/i.test(challenge),
    `challenge must not leak an internal hostname: ${challenge}`);
});

test("auth: a garbage bearer token is rejected (presence is not enough)", SKIP, async () => {
  // The middleware VERIFIES the token — a syntactically plausible but unsigned
  // JWT must be refused exactly like no token at all.
  const junk = "eyJhbGciOiJub25lIn0.eyJzdWIiOiJhdHRhY2tlciIsImF1ZCI6ImxhdGlxIn0.";
  const res = await fetch(URL_, {
    method: "POST",
    headers: {
      "content-type": "application/json",
      accept: "application/json, text/event-stream",
      authorization: `Bearer ${junk}`,
    },
    body: JSON.stringify({ jsonrpc: "2.0", id: 1, method: "tools/list", params: {} }),
  });
  assert.equal(res.status, 401, "an unverifiable token is rejected");
  assert.ok((res.headers.get("www-authenticate") ?? "").includes("resource_metadata="),
    "the challenge is present on an invalid token too");
});

test("auth: a client_credentials token gets a working session", SKIP, async () => {
  // The positive path in one place. (The whole harness suite also runs through
  // this transport when LATIQ_AUTH_ISSUER is set — this just pins it directly.)
  const client = new Client({ name: "latiq-agent-harness-auth", version: "0.0.0" });
  await client.connect(
    new StreamableHTTPClientTransport(new global.URL(URL_), { authProvider: provider() }),
  );
  const tools = await client.listTools();
  assert.ok(tools.tools.length > 0, "an authenticated client sees the tool surface");
  // Identity is no longer a tool argument — it rides the latiq-agent-id header.
  for (const t of tools.tools) {
    assert.ok(!("agent_id" in ((t.inputSchema as any)?.properties ?? {})),
      `agent_id must not appear in the ${t.name} schema`);
  }
  await client.close();
});
