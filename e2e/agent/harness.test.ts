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

// Deterministic MCP agent harness for Latiq. Drives the agent surface the way a
// real agent would — via the Vercel AI SDK's MCP client (the same client an
// `ai`-built agent uses to discover + call tools) — but with a SCRIPTED sequence
// instead of a live LLM, so it's deterministic and needs no API key. Resources +
// prompts (which the AI SDK client doesn't surface) are exercised via the
// underlying MCP SDK client against the same /mcp endpoint.
//
// MCP_URL points at the cluster gateway (:51510/mcp) in CI, or a node (:51402/mcp)
// locally. Run: `npm test` (after `npm install`), with a Latiq MCP endpoint up.
import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { after, before, test } from "node:test";

import { experimental_createMCPClient } from "ai";
import { Client } from "@modelcontextprotocol/sdk/client/index.js";
import { StreamableHTTPClientTransport } from "@modelcontextprotocol/sdk/client/streamableHttp.js";
import { ClientCredentialsProvider } from "@modelcontextprotocol/sdk/client/auth-extensions.js";

const URL_ = process.env.LATIQ_MCP ?? "http://127.0.0.1:51402/mcp";

/** One provider for both clients. undefined when the cluster has no auth. */
function authProvider() {
  const issuer = process.env.LATIQ_AUTH_ISSUER;
  if (!issuer) return undefined;
  // No `expectedIssuer` on purpose: ClientCredentialsProviderOptions has no such
  // field (SDK 1.29), so passing one would read as issuer pinning that isn't
  // actually happening. The issuer is discovered from the RFC 9728 document.
  return new ClientCredentialsProvider({
    clientId: "latiq-agent",
    clientSecret: process.env.LATIQ_CLIENT_SECRET ?? "latiq-agent-secret",
    scope: "openid",
  });
}

/**
 * The transport BOTH clients use: the AI SDK's MCP client is handed a transport
 * instance we construct, so the official SDK is the OAuth engine in both cases.
 * With LATIQ_AUTH_ISSUER set the whole suite runs authenticated; without it the
 * behaviour is byte-for-byte what it was before.
 */
function transport() {
  const provider = authProvider();
  // NEVER also set requestInit.headers.Authorization -- _commonHeaders spreads
  // requestInit.headers AFTER the provider's, silently overriding the token.
  return new StreamableHTTPClientTransport(
    new global.URL(URL_),
    provider ? { authProvider: provider } : undefined,
  );
}

const EXPECTED_TOOLS = [
  "allocate_pond", "describe_pond", "list_ponds", "drop_pond",
  "read_query", "write_query", "explain_query",
  "list_datasets", "load_dataset", "list_catalogs", "describe_catalog", "pull_catalog",
];

let ai: Awaited<ReturnType<typeof experimental_createMCPClient>>;
let tools: Record<string, any>;
let raw: Client;

const opts = () => ({ toolCallId: randomUUID(), messages: [] as any[] });
// Every AI-SDK MCP tool result is a CallToolResult: { content, isError, structuredContent }.
const ok = (r: any) => { assert.equal(r.isError, false, `tool errored: ${JSON.stringify(r)}`); return r.structuredContent; };
const failed = (r: any) => { assert.equal(r.isError, true, `expected an error result, got: ${JSON.stringify(r)}`); return r.structuredContent; };
const name = (p: string) => `${p}-${randomUUID().slice(0, 8)}`;

before(async () => {
  ai = await experimental_createMCPClient({ transport: transport() });
  tools = await ai.tools();
  raw = new Client({ name: "latiq-agent-harness", version: "0.0.0" });
  await raw.connect(transport());
});

after(async () => {
  await ai?.close();
  await raw?.close();
});

test("the full agent tool surface is advertised", () => {
  const got = Object.keys(tools).sort();
  for (const t of EXPECTED_TOOLS) assert.ok(got.includes(t), `missing MCP tool: ${t}`);
});

test("pond lifecycle + read/write/explain/describe through MCP tools", async () => {
  const pond = name("agent");
  const alloc = ok(await tools.allocate_pond.execute({ name: pond, tier: "medium" }, opts()));
  assert.equal(alloc.pond_name, pond);

  ok(await tools.write_query.execute({ pond, sql: "CREATE TABLE t(id INTEGER, label VARCHAR)" }, opts()));
  ok(await tools.write_query.execute({ pond, sql: "INSERT INTO t VALUES (1,'a'),(2,'b'),(3,'c')" }, opts()));

  const read = ok(await tools.read_query.execute({ pond, sql: "SELECT count(*) AS n FROM t" }, opts()));
  assert.equal(read.rows[0][0], 3, "read returned the rows the write produced");

  const explain = ok(await tools.explain_query.execute({ pond, sql: "SELECT * FROM t WHERE id > 1" }, opts()));
  assert.ok(JSON.stringify(explain).length > 0, "explain returned a plan");

  const desc = ok(await tools.describe_pond.execute({ pond }, opts()));
  assert.ok(JSON.stringify(desc).includes("t"), "describe surfaces the table");

  const list = ok(await tools.list_ponds.execute({}, opts()));
  assert.ok(JSON.stringify(list).includes(pond), "our pond is listed");

  ok(await tools.drop_pond.execute({ pond, confirm: true }, opts()));
  failed(await tools.read_query.execute({ pond, sql: "SELECT 1" }, opts()));
});

test("read tool rejects a mutation (the read-only guard)", async () => {
  const pond = name("guard");
  ok(await tools.allocate_pond.execute({ name: pond }, opts()));
  // read_query must refuse to mutate, even though the SQL is well-formed.
  failed(await tools.read_query.execute({ pond, sql: "CREATE TABLE x(i INT)" }, opts()));
  ok(await tools.drop_pond.execute({ pond, confirm: true }, opts()));
});

test("structured error contract (kind + suggest + see)", async () => {
  const pond = name("err");
  ok(await tools.allocate_pond.execute({ name: pond }, opts()));
  const env = failed(await tools.read_query.execute({ pond, sql: "SELECT * FROM does_not_exist" }, opts()));
  assert.ok(env.kind, `error envelope carries a kind: ${JSON.stringify(env)}`);
  assert.ok(env.message, "error envelope carries a message");
  assert.ok(env.suggest, "error envelope carries a next-action suggestion");
  assert.ok(String(env.see ?? "").startsWith("latiq://"), "error 'see' points at a resource");
  ok(await tools.drop_pond.execute({ pond, confirm: true }, opts()));
});

test("dataset catalog surface: list + load + query a curated dataset", async () => {
  const datasets = ok(await tools.list_datasets.execute({}, opts()));
  const names = JSON.stringify(datasets);
  assert.ok(names.includes("tpch"), "the curated dataset catalog lists tpch");

  // load_dataset pulls a curated public file into a pond, then we query it. This
  // touches the network (the dataset's source URL) — the same flow a user runs.
  const pond = name("ds");
  ok(await tools.allocate_pond.execute({ name: pond }, opts()));
  ok(await tools.load_dataset.execute({ pond, dataset: "tpch" }, opts()));
  // tpch loads into its own schema (schema-per-dataset) — so it's `tpch.nation`.
  const r = ok(await tools.read_query.execute({ pond, sql: "SELECT count(*) AS n FROM tpch.nation" }, opts()));
  assert.equal(r.rows[0][0], 25, "tpch.nation has 25 rows");
  ok(await tools.drop_pond.execute({ pond, confirm: true }, opts()));
});

test("catalog surface is reachable (list_catalogs)", async () => {
  // describe/pull_catalog need a registered external catalog (the dedicated
  // iceberg e2e covers that); here we prove the agent can ENUMERATE them — and
  // asserting only `isError === false` could not fail on any regression in what
  // enumeration returns, so assert the payload the model actually reads.
  const r: any = ok(await tools.list_catalogs.execute({}, opts()));
  assert.ok(Array.isArray(r?.catalogs),
    `list_catalogs returns {catalogs: [...]}: ${JSON.stringify(r)}`);
  // A fresh cluster registers none (that is an operator action via the CLI). If
  // this cluster is meant to have some, update this test rather than loosening it.
  assert.deepEqual(r.catalogs, [],
    `no external catalogs are registered on a fresh cluster: ${JSON.stringify(r.catalogs)}`);
  // Whatever IS listed must be usable by a model: named and typed.
  for (const c of r.catalogs as any[]) {
    assert.ok(c?.name, `a catalog entry is named: ${JSON.stringify(c)}`);
    assert.ok(c?.type, `catalog ${c?.name} must advertise its type: ${JSON.stringify(c)}`);
  }

  // The negative half: an unknown catalog is a structured tool error, not a
  // silent empty success the model would read as "nothing there".
  const pond = name("cat");
  ok(await tools.allocate_pond.execute({ name: pond }, opts()));
  const err = failed(await tools.describe_catalog.execute(
    { pond, catalog: "does-not-exist" }, opts()));
  assert.match(JSON.stringify(err ?? ""), /does-not-exist/,
    `the error names the unknown catalog: ${JSON.stringify(err)}`);
  ok(await tools.drop_pond.execute({ pond, confirm: true }, opts()));
});

test("resources: guidance + dialect are readable", async () => {
  const list = await raw.listResources();
  const uris = list.resources.map((r) => r.uri);
  assert.ok(uris.includes("latiq://guidance"), "guidance resource is advertised");

  const guidance = await raw.readResource({ uri: "latiq://guidance" });
  const text = guidance.contents.map((c: any) => c.text ?? "").join("");
  assert.ok(text.length > 100, "guidance has substantive content");
  assert.ok(/pond|schema|query/i.test(text), "guidance reads like agent guidance");

  const dialect = await raw.readResource({ uri: "latiq://dialect" });
  assert.ok(dialect.contents.length > 0, "dialect resource is readable");
});

test("prompts: the SOPs are advertised and renderable", async () => {
  const list = await raw.listPrompts();
  const names = list.prompts.map((p) => p.name);
  for (const sop of [
    "setup_multi_agent_pond", "discover_existing_pond",
    "design_collaborative_schema", "recover_from_conflict",
  ]) {
    assert.ok(names.includes(sop), `SOP prompt missing: ${sop}`);
  }
  const got = await raw.getPrompt({ name: "setup_multi_agent_pond", arguments: { pond_name: "demo" } });
  const text = got.messages.map((m: any) => m.content?.text ?? "").join("");
  assert.ok(text.includes("allocate_pond"), "the SOP renders concrete next steps");
});
