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

//! Full-stack feature tests for the agent MCP surface: the tools (annotated),
//! resources (latiq://…), and prompts (SOPs), driven by the agent-sim client
//! over the real MCP transport. Names prefixed by feature.
mod common;

use common::start_stack;
use latiq_client::LatiqClient;
use serde_json::{Map, Value};

#[tokio::test]
async fn external_data_tools_discover_and_load_via_mcp() {
    let s = start_stack().await;
    let c = LatiqClient::connect(&s.mcp_endpoint, Some("agent-x".into()))
        .await
        .unwrap();

    // The dataset/catalog tools are advertised, with correct annotations.
    let tools = c.list_tools().await.unwrap();
    for name in [
        "list_datasets",
        "load_dataset",
        "list_catalogs",
        "describe_catalog",
        "pull_catalog",
    ] {
        assert!(tools.iter().any(|t| t.name == name), "missing tool {name}");
    }
    let pull = tools.iter().find(|t| t.name == "pull_catalog").unwrap();
    assert_eq!(
        pull.annotations.as_ref().and_then(|a| a.destructive_hint),
        Some(true),
        "pull_catalog writes into the pond → destructive"
    );

    // The agent-facing recipe is discoverable.
    let uris = c.list_resource_uris().await.unwrap();
    assert!(uris.iter().any(|u| u == "latiq://recipes/external-data"));

    // list_datasets surfaces the seeded samples; load_dataset copies one in.
    let ds = c.call_tool("list_datasets", Map::new()).await.unwrap();
    assert!(!ds.is_error, "{:?}", ds.value);
    let names: Vec<&str> = ds.value["datasets"]
        .as_array()
        .unwrap()
        .iter()
        .filter_map(|d| d["name"].as_str())
        .collect();
    assert!(names.contains(&"holdings"), "seeded datasets: {names:?}");

    c.allocate_pond(Some("work")).await.unwrap();
    let mut args = Map::new();
    args.insert("pond".into(), "work".into());
    args.insert("dataset".into(), "holdings".into());
    let loaded = c.call_tool("load_dataset", args).await.unwrap();
    assert!(!loaded.is_error, "{:?}", loaded.value);
    // Datasets load into a schema named after the dataset (holdings.holdings).
    let r = c
        .query("work", "SELECT count(*) AS n FROM holdings.holdings")
        .await
        .unwrap();
    assert!(r.value["rows"][0][0].as_i64().unwrap() >= 1);
    c.close().await.unwrap();
}

#[tokio::test]
async fn mcp_tools_full_agent_loop() {
    let s = start_stack().await;
    let c = LatiqClient::connect(&s.mcp_endpoint, Some("agent-x".into()))
        .await
        .unwrap();

    let a = c.allocate_pond(Some("demo")).await.unwrap();
    assert!(!a.is_error, "{:?}", a.value);
    assert_eq!(a.value["pond_name"], "demo");

    c.write("demo", "CREATE TABLE t(id INTEGER)").await.unwrap();
    c.write("demo", "INSERT INTO t VALUES (1),(2)")
        .await
        .unwrap();
    let r = c
        .query("demo", "SELECT count(*) AS n FROM t")
        .await
        .unwrap();
    assert_eq!(r.value["rows"][0][0], 2);

    let d = c.drop_pond("demo").await.unwrap();
    assert!(!d.is_error);
    c.close().await.unwrap();
}

#[tokio::test]
async fn explain_reaches_the_agent_with_real_estimates() {
    // The whole stack for explain: engine parse -> AgentOps -> ok_explain's
    // structured content. Six of this response's seven fields used to be
    // hard-coded empty, so what this proves is that an AGENT — not a unit test
    // holding an ExplainResult — actually receives numbers.
    let s = start_stack().await;
    let c = LatiqClient::connect(&s.mcp_endpoint, Some("agent-explain".into()))
        .await
        .unwrap();
    c.allocate_pond(Some("plans")).await.unwrap();
    c.write("plans", "CREATE TABLE big(id INTEGER)")
        .await
        .unwrap();
    // Over the engine's full-scan threshold, so the derived advice fires too.
    c.write("plans", "INSERT INTO big SELECT i FROM range(200000) s(i)")
        .await
        .unwrap();

    let e = c.explain("plans", "SELECT * FROM big").await.unwrap();
    assert!(!e.is_error, "{:?}", e.value);
    assert_eq!(
        e.value["estimated_rows"], 200_000,
        "the agent must see the planner's row estimate, not a stub 0: {:?}",
        e.value
    );
    let scan = &e.value["scan_operations"][0];
    assert_eq!(scan["table"], "big");
    assert_eq!(scan["scan_type"], "full_scan");
    assert_eq!(scan["source"], "pond");
    assert_eq!(scan["estimated_rows_scanned"], 200_000);
    let advice = format!("{} {}", e.value["warnings"], e.value["suggestions"]);
    assert!(
        advice.contains("big") && advice.contains("WHERE"),
        "the warning and suggestion must name the table and a concrete fix: {advice}"
    );
    // The fields we deleted must be GONE, not present-and-zero: a `0` next to
    // real numbers reads as "this query costs no time and no bytes".
    assert!(
        e.value.get("estimated_bytes").is_none() && e.value.get("estimated_duration_ms").is_none(),
        "explain must not report estimates DuckDB does not produce: {:?}",
        e.value
    );
    assert!(
        e.value["raw_plan"].as_str().unwrap_or("").contains("big"),
        "raw_plan stays the escape hatch: {:?}",
        e.value["raw_plan"]
    );

    // Anti-vacuity for the advice: the same table WITH a predicate goes quiet,
    // so `warnings` is not simply always populated.
    let f = c
        .explain("plans", "SELECT * FROM big WHERE id = 7")
        .await
        .unwrap();
    assert_eq!(f.value["scan_operations"][0]["scan_type"], "filtered_scan");
    assert_eq!(
        f.value["warnings"],
        serde_json::json!([]),
        "a filtered scan earns no warning: {:?}",
        f.value
    );
    c.close().await.unwrap();
}

#[tokio::test]
async fn mcp_annotations_mark_destructive_and_readonly_tools() {
    let s = start_stack().await;
    let c = LatiqClient::connect(&s.mcp_endpoint, None).await.unwrap();
    let tools = c.list_tools().await.unwrap();
    let find = |name: &str| tools.iter().find(|t| t.name == name).cloned().unwrap();

    let read = find("read_query");
    let write = find("write_query");
    let drop = find("drop_pond");

    assert_eq!(
        read.annotations.as_ref().and_then(|a| a.read_only_hint),
        Some(true),
        "read_query should be read-only"
    );
    assert_eq!(
        write.annotations.as_ref().and_then(|a| a.destructive_hint),
        Some(true),
        "write_query should be destructive"
    );
    assert_eq!(
        drop.annotations.as_ref().and_then(|a| a.destructive_hint),
        Some(true),
        "drop_pond should be destructive"
    );
    c.close().await.unwrap();
}

#[tokio::test]
async fn mcp_resources_guidance_is_served_and_see_links_resolve() {
    let s = start_stack().await;
    let c = LatiqClient::connect(&s.mcp_endpoint, None).await.unwrap();

    let uris = c.list_resource_uris().await.unwrap();
    assert!(
        uris.contains(&"latiq://guidance".to_string()),
        "got {uris:?}"
    );
    // The `see` target of a pond_not_found error must resolve.
    assert!(uris.contains(&"latiq://troubleshooting/pond-not-found".to_string()));

    let body = c.read_resource_text("latiq://guidance").await.unwrap();
    assert!(body.contains("pond"), "guidance body should be non-trivial");

    let see = c
        .read_resource_text("latiq://troubleshooting/pond-not-found")
        .await
        .unwrap();
    assert!(see.contains("list_ponds"));
    c.close().await.unwrap();
}

#[tokio::test]
async fn mcp_prompts_sops_are_available_and_parameterized() {
    let s = start_stack().await;
    let c = LatiqClient::connect(&s.mcp_endpoint, None).await.unwrap();

    let names = c.list_prompt_names().await.unwrap();
    for expected in [
        "setup_multi_agent_pond",
        "discover_existing_pond",
        "design_collaborative_schema",
        "recover_from_conflict",
    ] {
        assert!(
            names.contains(&expected.to_string()),
            "missing prompt {expected}; got {names:?}"
        );
    }

    let mut args = Map::new();
    args.insert("pond_name".into(), Value::String("incident-7".into()));
    let text = c
        .get_prompt_text("setup_multi_agent_pond", args)
        .await
        .unwrap();
    assert!(
        text.contains("incident-7"),
        "prompt should weave in pond_name; got: {text}"
    );
    c.close().await.unwrap();
}

/// The recipe's SQL is EXECUTED here, not proofread.
///
/// Regression pin. `latiq://recipes/schema-design` taught
/// `CREATE TABLE events (id INTEGER, -- event primary key …)` and asserted
/// "comments are visible via SHOW TABLES / information_schema.columns". DuckDB
/// discards a lexical `--`: every comment came back NULL. The claim was
/// repeated in `latiq://guidance`, `write_query`'s description and two prompts,
/// and survived for months because nobody ran it. So this test reads the recipe
/// off the live MCP surface, runs its own SQL block, and asserts the comments
/// are actually readable afterwards — if the recipe reverts to a form that
/// stores nothing, this fails.
#[tokio::test]
async fn mcp_resources_schema_design_recipe_sql_actually_stores_comments() {
    let s = start_stack().await;
    let c = LatiqClient::connect(&s.mcp_endpoint, Some("schema-author".into()))
        .await
        .unwrap();
    c.allocate_pond(Some("recipe")).await.unwrap();

    let body = c
        .read_resource_text("latiq://recipes/schema-design")
        .await
        .unwrap();
    let blocks = sql_blocks(&body);
    assert_eq!(
        blocks.len(),
        2,
        "the recipe should carry one authoring block and one read-back block; got {blocks:?}"
    );

    // 1. The pattern the recipe tells the agent to write, run verbatim.
    let w = c.write("recipe", &blocks[0]).await.unwrap();
    assert!(!w.is_error, "the recipe's own SQL must run: {:?}", w.value);

    // 2. The read-back the recipe promises, also run verbatim. Statement one
    //    is the column comments; statement two the table's.
    let mut reads = blocks[1]
        .split(';')
        .map(str::trim)
        .filter(|s| !s.is_empty());
    let cols = c.query("recipe", reads.next().unwrap()).await.unwrap();
    assert!(!cols.is_error, "{:?}", cols.value);
    let comments: Vec<(String, Value)> = cols.value["rows"]
        .as_array()
        .unwrap()
        .iter()
        .map(|r| (r[0].as_str().unwrap().to_string(), r[1].clone()))
        .collect();
    assert_eq!(
        comments.len(),
        3,
        "the recipe creates three columns; got {comments:?}"
    );
    for (name, comment) in &comments {
        assert!(
            comment.as_str().is_some_and(|t| !t.is_empty()),
            "column '{name}' has no stored comment ({comment:?}) — the recipe's pattern does not \
             do what the recipe says it does"
        );
    }
    // The exact text, so a comment attached to the wrong column is caught.
    assert_eq!(
        comments
            .iter()
            .find(|(n, _)| n == "id")
            .map(|(_, c)| c.clone()),
        Some(Value::String("event primary key".into()))
    );
    let table = c.query("recipe", reads.next().unwrap()).await.unwrap();
    assert_eq!(
        table.value["rows"][0][0], "One row per observed event.",
        "the table COMMENT should be readable too: {:?}",
        table.value
    );
    assert!(reads.next().is_none(), "unexpected extra read statement");

    // 3. The prose also claims information_schema carries the same text.
    let is_cols = c
        .query(
            "recipe",
            "SELECT column_comment FROM information_schema.columns \
             WHERE table_name='events' AND column_name='id'",
        )
        .await
        .unwrap();
    assert_eq!(
        is_cols.value["rows"][0][0], "event primary key",
        "the recipe says information_schema.columns.column_comment carries it: {:?}",
        is_cols.value
    );

    // 4. And the negative the recipe warns about — the form it used to teach.
    //    Without this, step 2 would pass just as well for a recipe that had
    //    never been fixed but happened to be read back some other way.
    c.write(
        "recipe",
        "CREATE TABLE lexical (\n  id INTEGER, -- event primary key\n  ts TIMESTAMP -- when\n)",
    )
    .await
    .unwrap();
    let dropped = c
        .query(
            "recipe",
            "SELECT column_name, comment FROM duckdb_columns() WHERE table_name='lexical'",
        )
        .await
        .unwrap();
    for row in dropped.value["rows"].as_array().unwrap() {
        assert_eq!(
            row[1],
            Value::Null,
            "a `--` comment must NOT be stored — if DuckDB starts keeping it, the recipe's \
             warning is now wrong and has to be rewritten: {:?}",
            dropped.value
        );
    }
    c.close().await.unwrap();
}

/// The ```sql fences of a served resource body, in order.
fn sql_blocks(body: &str) -> Vec<String> {
    body.split("```sql")
        .skip(1)
        .filter_map(|rest| rest.split("```").next())
        .map(|s| s.trim().to_string())
        .collect()
}

/// A client builds `prompts/get` from the DECLARED argument list. While that
/// list was `None` for all four prompts, every conforming client sent `{}` and
/// every prompt rendered its placeholders — `discover_existing_pond` produced
/// "Find an existing pond related to '' (intent: read)", an instruction shaped
/// like a real one. The unit tests pin the rendering; this pins that the
/// declarations survive the wire, and that the refusal reaches the client.
#[tokio::test]
async fn mcp_prompts_declare_their_arguments_over_the_wire() {
    let s = start_stack().await;
    let c = LatiqClient::connect(&s.mcp_endpoint, None).await.unwrap();

    let prompts = c.list_prompts().await.unwrap();
    assert_eq!(prompts.len(), 4, "got {prompts:?}");
    for p in &prompts {
        let declared = p
            .arguments
            .as_ref()
            .unwrap_or_else(|| panic!("{} declares no arguments", p.name));
        assert!(
            declared.iter().any(|a| a.required == Some(true)),
            "{} declares no REQUIRED argument, so a client cannot know what to ask for",
            p.name
        );
        assert!(
            declared.iter().all(|a| a.description.is_some()),
            "{}'s arguments must say what they are: {declared:?}",
            p.name
        );
    }
    // The one the audit observed, by name and by its required argument.
    let discover = prompts
        .iter()
        .find(|p| p.name == "discover_existing_pond")
        .unwrap();
    let names: Vec<&str> = discover
        .arguments
        .as_ref()
        .unwrap()
        .iter()
        .map(|a| a.name.as_str())
        .collect();
    assert_eq!(names, ["search_term", "intent"], "declared: {names:?}");

    // What a client that ignored the declaration used to get: a rendering.
    // It must now be an error the client can report instead.
    let err = c
        .get_prompt_text("discover_existing_pond", Map::new())
        .await
        .expect_err("a prompt with no arguments must not render placeholders");
    let msg = err.to_string();
    assert!(
        msg.contains("search_term"),
        "the refusal must name the missing argument; got: {msg}"
    );
    c.close().await.unwrap();
}

/// The uncapped tier is operator-only — an uncapped pond can starve every other
/// pond on its node, so an *agent* must not be able to allocate itself one. The
/// rule lives in the registry; this asserts the MCP surface actually carries it,
/// with the structured kind an agent branches on rather than a bare tool error.
#[tokio::test]
async fn policy_tier_none_is_refused_over_mcp() {
    let s = start_stack().await;
    let c = LatiqClient::connect(&s.mcp_endpoint, Some("agent-greedy".into()))
        .await
        .unwrap();
    // Every spelling the parser accepts, or the guard is bypassable by alias.
    for tier in ["none", "uncapped", " NONE "] {
        let mut args = Map::new();
        args.insert("name".into(), format!("greedy-{}", tier.trim()).into());
        args.insert("tier".into(), tier.into());
        let out = c.call_tool("allocate_pond", args).await.unwrap();
        assert!(
            out.is_error,
            "tier `{tier}` must not be self-assignable by an agent: {:?}",
            out.value
        );
        assert_eq!(
            out.value["kind"], "invalid_value",
            "tier `{tier}`: an operator-only tier is an invalid VALUE — the \
             agent must be able to tell it apart from an unknown tier, which is \
             the one outcome this test exists to distinguish it from"
        );
        assert!(
            out.value["message"]
                .as_str()
                .is_some_and(|m| m.contains("set-tier")),
            "tier `{tier}`: the message must name the operator escape hatch: {:?}",
            out.value
        );
    }
    // The refusal is about the tier, not about allocate_pond.
    let mut ok = Map::new();
    ok.insert("name".into(), "polite".into());
    ok.insert("tier".into(), "small".into());
    let out = c.call_tool("allocate_pond", ok).await.unwrap();
    assert!(
        !out.is_error,
        "a normal tier must still allocate: {:?}",
        out.value
    );
    c.close().await.unwrap();
}

#[tokio::test]
async fn mcp_error_contract_is_structured() {
    let s = start_stack().await;
    let c = LatiqClient::connect(&s.mcp_endpoint, None).await.unwrap();
    let out = c.query("ghost", "SELECT 1").await.unwrap();
    assert!(out.is_error);
    assert_eq!(out.value["kind"], "pond_not_found");
    assert!(out.value["see"].as_str().unwrap().starts_with("latiq://"));
    c.close().await.unwrap();
}

// ---------------------------------------------------------------------------
// auth_* — the MCP surface as an OAuth 2.1 resource server. Identity arrives in
// the TRANSPORT (headers), never as a tool argument the model could type.
// Verification only: nothing here gates on WHO the agent is.
// ---------------------------------------------------------------------------

/// The `http://host:port` an mcp endpoint (`…/mcp`) is served from.
fn base_of(mcp_endpoint: &str) -> String {
    mcp_endpoint.trim_end_matches("/mcp").to_string()
}

#[tokio::test]
async fn auth_mcp_tool_schemas_do_not_expose_agent_id() {
    // The whole point of the breaking change: a verified principal must arrive
    // out of band. If `agent_id` is still in a tool's input schema, the model can
    // type its own identity — so assert on the SCHEMAS, not on behaviour.
    let s = start_stack().await;
    let c = LatiqClient::connect(&s.mcp_endpoint, None).await.unwrap();
    for t in c.list_tools().await.unwrap() {
        let schema = serde_json::to_string(&t.input_schema).unwrap();
        assert!(
            !schema.contains("agent_id"),
            "tool {} still advertises agent_id: {schema}",
            t.name
        );
    }
    c.close().await.unwrap();
}

#[tokio::test]
async fn auth_mcp_serves_protected_resource_metadata() {
    let idp = latiq_auth::test_support::TestIdp::start().await;
    let s = common::start_stack_with_auth(idp.auth_config()).await;
    let url = format!(
        "{}/.well-known/oauth-protected-resource",
        base_of(&s.mcp_endpoint)
    );
    // Reachable WITHOUT a token: discovery is impossible otherwise.
    let res = reqwest::get(&url).await.unwrap();
    assert!(res.status().is_success(), "got {}", res.status());
    let doc: Value = res.json().await.unwrap();
    assert_eq!(doc["authorization_servers"][0], Value::String(idp.issuer));
    assert_eq!(doc["resource"], Value::String(s.mcp_endpoint.clone()));
}

#[tokio::test]
async fn auth_mcp_unauthenticated_request_gets_a_401_challenge() {
    let idp = latiq_auth::test_support::TestIdp::start().await;
    let s = common::start_stack_with_auth(idp.auth_config()).await;
    let res = reqwest::Client::new()
        .post(&s.mcp_endpoint)
        .header("content-type", "application/json")
        .body("{}")
        .send()
        .await
        .unwrap();
    assert_eq!(res.status().as_u16(), 401);
    let challenge = res
        .headers()
        .get("www-authenticate")
        .and_then(|v| v.to_str().ok())
        .unwrap_or_default()
        .to_string();
    assert!(
        challenge.contains("resource_metadata="),
        "the 401 must point at the metadata document: {challenge:?}"
    );
    assert!(
        !challenge.to_lowercase().contains("jwks") && !challenge.contains(&idp.issuer),
        "the challenge must not leak issuers or the JWKS uri: {challenge:?}"
    );
}

/// One JSON-RPC request over raw HTTP, so a test can drive the methods an MCP
/// client would never let it send unauthenticated (and see the HTTP status the
/// client library reacts to).
async fn post_rpc(endpoint: &str, token: Option<&str>, method: &str) -> reqwest::Response {
    let body = serde_json::json!({
        "jsonrpc": "2.0",
        "id": 1,
        "method": method,
        "params": {
            "protocolVersion": "2025-06-18",
            "capabilities": {},
            "clientInfo": {"name": "probe", "version": "0"},
            "uri": "latiq://guidance",
        },
    });
    let mut req = reqwest::Client::new()
        .post(endpoint)
        .header("content-type", "application/json")
        .header("accept", "application/json, text/event-stream")
        .body(body.to_string());
    if let Some(t) = token {
        req = req.header("authorization", format!("Bearer {t}"));
    }
    req.send().await.unwrap()
}

/// The `WWW-Authenticate` value, or "" — a challenge is what makes an MCP client
/// re-authenticate instead of wedging.
fn challenge_of(res: &reqwest::Response) -> String {
    res.headers()
        .get("www-authenticate")
        .and_then(|v| v.to_str().ok())
        .unwrap_or_default()
        .to_string()
}

#[tokio::test]
async fn auth_mcp_rejects_an_invalid_token_with_a_401_challenge() {
    // THE security boundary on this surface. A forged, expired or wrong-audience
    // token must be refused with the same 401 + challenge a missing one gets —
    // not a JSON-RPC error inside HTTP 200, which no client can act on, and
    // above all not a silent downgrade to a claimed identity.
    let idp = latiq_auth::test_support::TestIdp::start().await;
    let s = common::start_stack_with_auth(idp.auth_config()).await;
    let cases = [
        ("not a jwt at all", "nonsense".to_string()),
        (
            "signed by a key the issuer does not publish",
            idp.mint_with_foreign_key("svc-mcp", "latiq", &idp.issuer),
        ),
        ("expired", idp.mint("svc-mcp", "latiq", &idp.issuer, -60)),
        (
            "minted for another audience",
            idp.mint("svc-mcp", "not-latiq", &idp.issuer, 300),
        ),
        (
            "alg:none",
            idp.mint_alg_none("svc-mcp", "latiq", &idp.issuer),
        ),
    ];
    for (why, token) in cases {
        let res = post_rpc(&s.mcp_endpoint, Some(&token), "initialize").await;
        assert_eq!(res.status().as_u16(), 401, "{why} should be refused");
        let challenge = challenge_of(&res);
        assert!(
            challenge.contains("resource_metadata="),
            "{why}: a 401 must carry the challenge: {challenge:?}"
        );
        assert!(
            !challenge.contains(&idp.issuer) && !challenge.to_lowercase().contains("jwks"),
            "{why}: the challenge must not leak issuers or the JWKS uri: {challenge:?}"
        );
    }
}

#[tokio::test]
async fn auth_mcp_non_tool_methods_require_a_verified_token() {
    // `initialize`, `tools/list` and `resources/read` never build an Identity,
    // so a check that lived only in the tool handlers would let an
    // unauthenticated caller finish the handshake, enumerate the tool
    // catalogue, read every latiq:// resource — and allocate a session (plus
    // its worker task) on every attempt.
    let idp = latiq_auth::test_support::TestIdp::start().await;
    let s = common::start_stack_with_auth(idp.auth_config()).await;
    for method in ["initialize", "tools/list", "resources/read", "prompts/list"] {
        // A non-empty string after `Bearer ` is not a credential.
        let res = post_rpc(&s.mcp_endpoint, Some("x"), method).await;
        assert_eq!(res.status().as_u16(), 401, "{method} accepted a junk token");
        assert!(challenge_of(&res).contains("resource_metadata="));
    }
    // ...and the same method succeeds with a real token, so the assertion above
    // is about the credential and not about the method being blocked outright.
    let token = idp.mint("svc-mcp", "latiq", &idp.issuer, 300);
    let res = post_rpc(&s.mcp_endpoint, Some(&token), "initialize").await;
    assert!(res.status().is_success(), "got {}", res.status());
}

#[tokio::test]
async fn auth_mcp_nested_well_known_path_is_not_exempt() {
    // Only the exact well-known path is exempt. `/mcp/.well-known/…` is the MCP
    // service's own route table, so correctness here rests on the layer seeing
    // the pre-`StripPrefix` path — pinned so a routing change cannot quietly
    // open a hole.
    let idp = latiq_auth::test_support::TestIdp::start().await;
    let s = common::start_stack_with_auth(idp.auth_config()).await;
    let nested = format!("{}/.well-known/oauth-protected-resource", s.mcp_endpoint);
    let res = post_rpc(&nested, None, "initialize").await;
    assert_eq!(res.status().as_u16(), 401);
    assert!(challenge_of(&res).contains("resource_metadata="));
}

#[tokio::test]
async fn auth_mcp_claimed_agent_id_header_becomes_the_author() {
    // The relaxed path every existing deployment runs on. A typo in the header
    // name would silently downgrade every MCP caller to `anonymous` with the
    // rest of the suite still green, so assert on the recorded author.
    let s = start_stack().await;
    let c = LatiqClient::connect(&s.mcp_endpoint, Some("agent-claimed".into()))
        .await
        .unwrap();
    c.allocate_pond(Some("claimed")).await.unwrap();
    c.write("claimed", "CREATE TABLE t(i INTEGER)")
        .await
        .unwrap();
    let r = c
        .query(
            "claimed",
            "SELECT DISTINCT author FROM ducklake_snapshots('claimed')",
        )
        .await
        .unwrap();
    let authors: Vec<&str> = r.value["rows"]
        .as_array()
        .unwrap()
        .iter()
        .filter_map(|row| row[0].as_str())
        .collect();
    assert!(
        authors.contains(&"agent-claimed"),
        "the latiq-agent-id header should be the claimed author, got {authors:?}"
    );
    c.close().await.unwrap();
}

#[tokio::test]
async fn auth_mcp_unauthenticated_node_does_not_replay_a_client_token() {
    // Mirrors forwarding_does_not_leak_a_client_authorization_header_without_auth
    // for the MCP surface. A node with NO verifier must not capture whatever
    // `authorization` header a client happens to send — one meant for an
    // upstream gateway, say — and replay it to a peer. The owner here REQUIRES a
    // token, so if the greeter had forwarded the (perfectly valid) header this
    // write would succeed. It must not.
    let idp = latiq_auth::test_support::TestIdp::start().await;
    // The control plane is authenticated because the cluster is: it materialises
    // the pond on the owner when `allocate_pond` below runs, and only an
    // authenticated control plane replays the caller's token on that hop.
    let (control, _admin) = common::start_control_plane_with_auth(Some(idp.auth_config())).await;
    let owner = common::add_node("owner", &control, Some(idp.auth_config())).await;
    let token = idp.mint("svc-dave", "latiq", &idp.issuer, 300);

    let mut oc = latiq_proto::v1::data_client::DataClient::connect(owner.data_endpoint.clone())
        .await
        .unwrap();
    let mut alloc = tonic::Request::new(latiq_proto::v1::AllocatePondRequest {
        name: "leakmcp".into(),
        policy_json: String::new(),
        tier: String::new(),
        lineage: false,
    });
    alloc
        .metadata_mut()
        .insert("authorization", format!("Bearer {token}").parse().unwrap());
    oc.allocate_pond(alloc).await.unwrap();

    // The greeter requires nothing, so it must also capture nothing.
    let greeter = common::add_node("greeter", &control, None).await;
    let c = LatiqClient::connect_with_token(
        &greeter.mcp_endpoint,
        Some("agent-x".into()),
        Some(token.clone()),
    )
    .await
    .unwrap();
    let out = c
        .write("leakmcp", "CREATE TABLE t(i INTEGER)")
        .await
        .unwrap();
    assert!(out.is_error, "the hop must fail closed: {:?}", out.value);
    assert!(
        format!("{}", out.value).contains("a bearer token is required"),
        "the client's header must not cross the hop from an unauthenticated node: {:?}",
        out.value
    );
    c.close().await.unwrap();
}

#[tokio::test]
async fn auth_mcp_accepts_a_valid_token_and_marks_identity_verified() {
    let idp = latiq_auth::test_support::TestIdp::start().await;
    let s = common::start_stack_with_auth(idp.auth_config()).await;
    let token = idp.mint("svc-mcp", "latiq", &idp.issuer, 300);
    // The claimed leaf rides its own header; the token is the verified principal.
    let c = LatiqClient::connect_with_token(&s.mcp_endpoint, Some("agent-x".into()), Some(token))
        .await
        .unwrap();

    let a = c.allocate_pond(Some("authed")).await.unwrap();
    assert!(!a.is_error, "{:?}", a.value);
    let w = c
        .write("authed", "CREATE TABLE t(i INTEGER)")
        .await
        .unwrap();
    assert!(!w.is_error, "{:?}", w.value);

    // Exactly the recipe latiq://recipes/attribution-lookup ships, so the
    // snippet's column names are proven copy-pasteable here too.
    let r = c
        .query(
            "authed",
            "SELECT author, commit_extra_info FROM ducklake_snapshots('authed')",
        )
        .await
        .unwrap();
    assert!(!r.is_error, "{:?}", r.value);
    let rows = r.value["rows"].as_array().unwrap();
    let authors: Vec<&str> = rows.iter().filter_map(|row| row[0].as_str()).collect();
    assert!(
        rows.iter().any(|row| row[1]
            .as_str()
            .is_some_and(|e| e.contains("\"verified\":true"))),
        "commit_extra_info should carry the verified evidence: {rows:?}"
    );
    // The proof it was VERIFIED and not merely accepted: the commit author is
    // the token's SUBJECT, and the claimed leaf is absent.
    assert!(
        authors.contains(&"svc-mcp"),
        "author should be the verified subject, got {authors:?}"
    );
    assert!(
        !authors.contains(&"agent-x"),
        "the claimed leaf must not be the author for a verified caller: {authors:?}"
    );
    c.close().await.unwrap();
}

#[tokio::test]
async fn mcp_cross_node_write_forwards_the_bearer_on_an_auth_enabled_cluster() {
    // MCP shares one `AgentOps` + `GrpcForwarder` with the Data surface, so an
    // MCP call against a node that does NOT own the pond has to replay the
    // caller's token for the owner to re-verify. This test used to pin the
    // opposite (the hop failing closed with "a bearer token is required")
    // because MCP had no verifier and scoped no token; it now pins the fix.
    let idp = latiq_auth::test_support::TestIdp::start().await;
    // Authenticated control plane: it is what materialises the pond on the owner
    // (and the only thing that replays the caller's token on that hop).
    let (control, _admin) = common::start_control_plane_with_auth(Some(idp.auth_config())).await;

    // Owner first, alone, so it certainly owns `mcpfwd` (placement is random).
    let owner = common::add_node("owner", &control, Some(idp.auth_config())).await;
    let token = idp.mint("svc-dave", "latiq", &idp.issuer, 300);
    let mut oc = latiq_proto::v1::data_client::DataClient::connect(owner.data_endpoint.clone())
        .await
        .unwrap();
    let mut alloc = tonic::Request::new(latiq_proto::v1::AllocatePondRequest {
        name: "mcpfwd".into(),
        policy_json: String::new(),
        tier: String::new(),
        lineage: false,
    });
    alloc
        .metadata_mut()
        .insert("authorization", format!("Bearer {token}").parse().unwrap());
    oc.allocate_pond(alloc).await.unwrap();

    // Now the peer, driven over MCP — every op on `mcpfwd` must forward.
    let greeter = common::add_node("greeter", &control, Some(idp.auth_config())).await;
    let c = LatiqClient::connect_with_token(
        &greeter.mcp_endpoint,
        Some("agent-x".into()),
        Some(token.clone()),
    )
    .await
    .unwrap();
    let out = c
        .write("mcpfwd", "CREATE TABLE t(i INTEGER)")
        .await
        .unwrap();
    assert!(
        !out.is_error,
        "the caller's token must ride the hop so the owner can re-verify it: {:?}",
        out.value
    );
    // And the owner attributed the forwarded write to the VERIFIED subject.
    let r = c
        .query(
            "mcpfwd",
            "SELECT DISTINCT author FROM ducklake_snapshots('mcpfwd')",
        )
        .await
        .unwrap();
    let authors: Vec<&str> = r.value["rows"]
        .as_array()
        .unwrap()
        .iter()
        .filter_map(|row| row[0].as_str())
        .collect();
    assert!(
        authors.contains(&"svc-dave"),
        "the owner should attribute the forwarded write to the token subject: {authors:?}"
    );
    c.close().await.unwrap();
}

// ---------------------------------------------------------------------------
/// Lineage on the agent surface: an agent may want provenance for its own work,
/// so `allocate_pond` takes the flag, `describe_pond` reports it, and
/// `get_lineage` reads the trail back. Off unless asked for. A submodule, not a new binary (tests/CLAUDE.md rule 5).
// ---------------------------------------------------------------------------
mod lineage {
    use crate::common::{start_stack, start_stack_with_auth};
    use latiq_client::LatiqClient;
    use serde_json::{json, Map, Value};

    /// The vendored core spec, reached across rather than copied: two copies of
    /// a spec drift and only one of them would be wrong.
    const CORE_URI: &str = "https://openlineage.io/spec/2-0-2/OpenLineage.json";
    const CORE: &str = include_str!("../../latiq-lineage/spec/OpenLineage-2-0-2.json");

    /// A validator for the `RunEvent` envelope, formats included (`runId`'s
    /// `uuid` and `eventTime`'s `date-time` are format-only constraints).
    fn run_event_validator() -> jsonschema::Validator {
        let core: Value = serde_json::from_str(CORE).expect("vendored core schema parses");
        let registry = jsonschema::Registry::new()
            .add(CORE_URI, jsonschema::Resource::from_contents(core))
            .expect("core schema URI is valid")
            .prepare()
            .expect("registry builds");
        jsonschema::options()
            .should_validate_formats(true)
            .with_registry(&registry)
            .build(&json!({ "$ref": format!("{CORE_URI}#/$defs/RunEvent") }))
            .expect("schema compiles")
    }

    async fn get_lineage(c: &LatiqClient, pond: &str, limit: Option<u32>) -> Value {
        get_lineage_page(c, pond, limit, None).await
    }

    /// One page, with the backward cursor an agent pages with.
    async fn get_lineage_page(
        c: &LatiqClient,
        pond: &str,
        limit: Option<u32>,
        before: Option<&str>,
    ) -> Value {
        let mut args = Map::new();
        args.insert("pond".into(), pond.into());
        if let Some(l) = limit {
            args.insert("limit".into(), l.into());
        }
        if let Some(b) = before {
            args.insert("before".into(), b.into());
        }
        let out = c.call_tool("get_lineage", args).await.unwrap();
        assert!(!out.is_error, "get_lineage {pond}: {:?}", out.value);
        out.value
    }

    /// A run's identity: the pair `(runId, eventType)` is unique per event, so
    /// a paging walk can prove it saw each one exactly once.
    fn event_key(event: &Value) -> String {
        format!(
            "{}/{}",
            event["run"]["runId"].as_str().unwrap_or("?"),
            event["eventType"].as_str().unwrap_or("?")
        )
    }

    fn event_time(event: &Value) -> &str {
        event["eventTime"]
            .as_str()
            .expect("every event carries eventTime")
    }

    /// The redacted SQL the run's job carries — how a test tells one query's
    /// events from another's.
    fn sql_of(event: &Value) -> &str {
        event["job"]["facets"]["sql"]["query"]
            .as_str()
            .unwrap_or_else(|| panic!("every event carries the SQL facet: {event:#}"))
    }

    async fn allocate(c: &LatiqClient, name: &str, lineage: Option<bool>) {
        let mut args = Map::new();
        args.insert("name".into(), name.into());
        if let Some(l) = lineage {
            args.insert("lineage".into(), Value::Bool(l));
        }
        let a = c.call_tool("allocate_pond", args).await.unwrap();
        assert!(!a.is_error, "allocate {name}: {:?}", a.value);
        assert_eq!(a.value["pond_name"], name);
    }

    async fn described_lineage(c: &LatiqClient, pond: &str) -> Value {
        let d = c.describe_pond(pond).await.unwrap();
        assert!(!d.is_error, "describe {pond}: {:?}", d.value);
        d.value["pond"]["lineage"].clone()
    }

    #[tokio::test]
    async fn lineage_agent_can_request_it_at_allocate_and_see_it_in_describe() {
        let s = start_stack().await;
        let c = LatiqClient::connect(&s.mcp_endpoint, Some("agent-x".into()))
            .await
            .unwrap();

        // Asked for → on, and visible to the agent that asked.
        allocate(&c, "provenance", Some(true)).await;
        assert_eq!(
            described_lineage(&c, "provenance").await,
            json!(true),
            "an agent that asks for lineage must be told it has it"
        );

        // Not asked for → off. The default deployment pays nothing, and an agent
        // must not be told it has provenance when it does not.
        allocate(&c, "plain", None).await;
        assert_eq!(
            described_lineage(&c, "plain").await,
            json!(false),
            "omitting the flag must leave lineage off"
        );

        // Explicitly declined reads the same as omitted.
        allocate(&c, "declined", Some(false)).await;
        assert_eq!(described_lineage(&c, "declined").await, json!(false));

        c.close().await.unwrap();
    }

    #[tokio::test]
    async fn lineage_tool_returns_canonical_openlineage_events() {
        // Canonical means a consumer we have never seen could replay these
        // bytes into Marquez unchanged — so they are held to the real spec, as
        // they come off the tool, not as `latiq-lineage` builds them. The
        // verified subject is checked on the same events because provenance
        // that cannot say WHO ran the query, on the authority of a token rather
        // than a claim, is not provenance.
        let idp = latiq_auth::test_support::TestIdp::start().await;
        let s = start_stack_with_auth(idp.auth_config()).await;
        let token = idp.mint("svc-provenance", "latiq", &idp.issuer, 300);
        let c = LatiqClient::connect_with_token(
            &s.mcp_endpoint,
            Some("agent-claimed".into()),
            Some(token),
        )
        .await
        .unwrap();

        allocate(&c, "canonical", Some(true)).await;
        c.write("canonical", "CREATE TABLE orders(id INTEGER)")
            .await
            .unwrap();
        // No sleep, no second call: the events for the write above are still in
        // the writer's buffer (a batch is 64 events), so a tool that did not
        // flush before reading would return nothing here.
        let page = get_lineage(&c, "canonical", None).await;

        let events = page["events"].as_array().expect("events is a list");
        assert!(
            events.len() >= 2,
            "one write records a START and a terminal event, got {}",
            events.len()
        );
        let validator = run_event_validator();
        for event in events {
            let errors: Vec<String> = validator
                .iter_errors(event)
                .map(|e| e.to_string())
                .collect();
            assert!(
                errors.is_empty(),
                "an event off the tool is not valid OpenLineage: {errors:?}\n{event:#}"
            );
        }
        assert_eq!(
            page["malformed_lines"], 0,
            "nothing on disk should have been unreadable"
        );

        // The identity facet, on the events of the write we just made: verified
        // on the token's authority, and the subject is the token's — not the
        // claimed leaf, which rides alongside as attribution only.
        let write = events
            .iter()
            .find(|e| sql_of(e).contains("orders"))
            .expect("the write's events are in the page");
        let identity = &write["run"]["facets"]["latiq_identity"];
        assert_eq!(
            identity["verified"],
            json!(true),
            "a token-verified caller must be recorded as verified: {identity:#}"
        );
        assert_eq!(
            identity["subject"],
            json!("svc-provenance"),
            "the subject must be the token's, not the claimed leaf: {identity:#}"
        );

        // The directory is handed back so an agent that wants SQL over the
        // whole trail can read_json_auto it — the documented escape hatch from
        // this paged read.
        assert!(
            page["lineage_dir"]
                .as_str()
                .is_some_and(|d| d.ends_with("lineage")),
            "the page must name the pond's lineage directory: {:?}",
            page["lineage_dir"]
        );
        c.close().await.unwrap();
    }

    #[tokio::test]
    async fn lineage_tool_returns_newest_first_and_honours_limit() {
        let s = start_stack().await;
        let c = LatiqClient::connect(&s.mcp_endpoint, Some("agent-x".into()))
            .await
            .unwrap();

        // Exactly one lineage tool: every MCP tool is permanent surface and
        // costs model context on every tools/list.
        let lineage_tools: Vec<String> = c
            .list_tools()
            .await
            .unwrap()
            .into_iter()
            .filter(|t| t.name.contains("lineage"))
            .map(|t| t.name.to_string())
            .collect();
        assert_eq!(
            lineage_tools,
            vec!["get_lineage".to_string()],
            "one lineage tool, and it is the one the recipe names"
        );

        allocate(&c, "ordered", Some(true)).await;
        for table in ["alpha", "beta", "gamma"] {
            c.write("ordered", &format!("CREATE TABLE {table}(id INTEGER)"))
                .await
                .unwrap();
        }

        // The whole trail: three operations, two events each.
        let all = get_lineage(&c, "ordered", Some(100)).await;
        let events = all["events"].as_array().unwrap();
        assert!(
            events.len() >= 6,
            "three writes record three event pairs, got {}",
            events.len()
        );
        assert_eq!(
            all["truncated"],
            json!(false),
            "a page that held everything must not claim there is more"
        );
        // Newest FIRST: the last table written leads, the first trails. Asserted
        // on positions rather than on a timestamp, because two events written in
        // the same millisecond have the same eventTime and would make an
        // ordering assertion on time pass vacuously.
        let position = |table: &str| {
            events
                .iter()
                .position(|e| sql_of(e).contains(table))
                .unwrap_or_else(|| panic!("no event mentions {table}: {events:#?}"))
        };
        assert!(
            position("gamma") < position("beta") && position("beta") < position("alpha"),
            "events must come back newest first, got {:?}",
            events.iter().map(sql_of).collect::<Vec<_>>()
        );

        // The limit binds, and takes from the NEW end. The count is a range,
        // not an equality: a page is cut back to a timestamp boundary rather
        // than ending mid-`eventTime` (that is what makes `before` an exact
        // cursor), so a page of 2 may legitimately come back holding 1.
        let page = get_lineage(&c, "ordered", Some(2)).await;
        let limited = page["events"].as_array().unwrap();
        assert!(
            !limited.is_empty() && limited.len() <= 2,
            "limit=2 must return one or two events, got {}",
            limited.len()
        );
        assert!(limited.len() < events.len(), "the limit must actually bind");
        for event in limited {
            assert!(
                sql_of(event).contains("gamma"),
                "a limited page must hold the NEWEST events, got {}",
                sql_of(event)
            );
        }
        assert_eq!(
            page["truncated"],
            json!(true),
            "a page that dropped four events must say so, or an agent reads it as the whole trail"
        );
        c.close().await.unwrap();
    }

    #[tokio::test]
    async fn lineage_tool_pages_backwards_through_the_whole_history() {
        // Regression pin (d119792): the tool told agents to page backwards with
        // `since`, an INCLUSIVE LOWER bound — which returns the same newest page
        // for ever. Not redundant with the reader's unit tests: this walks the
        // surface an agent actually drives, where the wrong bound lived.
        // THE paging contract, walked the way the tool description tells an
        // agent to walk it: `before` = the oldest eventTime received, exclusive,
        // until `truncated` is false. It must terminate, cover every event, and
        // repeat none — the three ways paging silently breaks.
        let s = start_stack().await;
        let c = LatiqClient::connect(&s.mcp_endpoint, Some("agent-x".into()))
            .await
            .unwrap();

        allocate(&c, "paged", Some(true)).await;
        for i in 0..6 {
            c.write("paged", &format!("CREATE TABLE t{i}(id INTEGER)"))
                .await
                .unwrap();
        }
        // The whole trail in one call is the oracle the walk is compared to.
        let whole = get_lineage(&c, "paged", Some(500)).await;
        let expected: Vec<String> = whole["events"]
            .as_array()
            .unwrap()
            .iter()
            .map(event_key)
            .collect();
        assert!(
            expected.len() >= 12,
            "six writes record six event pairs, got {}",
            expected.len()
        );
        assert_eq!(whole["truncated"], json!(false), "the oracle is complete");

        let mut walked: Vec<String> = Vec::new();
        let mut cursor: Option<String> = None;
        // A bound generous enough for one event per page, tight enough that a
        // non-terminating walk fails instead of hanging.
        let max_pages = expected.len() + 2;
        let mut pages = 0usize;
        loop {
            let page = get_lineage_page(&c, "paged", Some(3), cursor.as_deref()).await;
            let events = page["events"].as_array().unwrap();
            assert!(
                !events.is_empty(),
                "a truncated page promised more events, then returned none"
            );
            // Newest-first WITHIN a page, and every page older than the last.
            if let Some(prev) = cursor.as_deref() {
                assert!(
                    event_time(&events[0]) < prev,
                    "a page must start strictly older than the previous cursor"
                );
            }
            cursor = Some(event_time(events.last().unwrap()).to_string());
            walked.extend(events.iter().map(event_key));
            pages += 1;
            assert!(pages <= max_pages, "the walk did not terminate: {walked:?}");
            if page["truncated"] == json!(false) {
                break;
            }
        }
        assert!(pages >= 2, "a 12-event history at 3 per page must page");

        let mut sorted_walk = walked.clone();
        sorted_walk.sort();
        let mut sorted_expected = expected.clone();
        sorted_expected.sort();
        assert_eq!(
            sorted_walk, sorted_expected,
            "paging must visit every event exactly once — no skips, no repeats"
        );
        c.close().await.unwrap();
    }

    #[tokio::test]
    async fn lineage_tool_on_a_non_owner_node_forwards_to_the_pond_owner() {
        // The events are FILES on the node that ran the query, and behind the
        // gateway we ship an agent lands on a non-owner roughly (1 - 1/n) of
        // the time. Reading locally there would answer an honest question with
        // an empty page, so the peer forwards — like every other pond-scoped op.
        let (control, _admin) = crate::common::start_control_plane_only().await;
        // Owner first and alone, so it certainly owns the pond (placement is
        // random once there are two nodes).
        let owner = crate::common::add_node("owner", &control, None).await;
        let oc = LatiqClient::connect(&owner.mcp_endpoint, Some("agent-x".into()))
            .await
            .unwrap();
        allocate(&oc, "elsewhere", Some(true)).await;
        oc.write("elsewhere", "CREATE TABLE t(id INTEGER)")
            .await
            .unwrap();
        let from_owner = get_lineage(&oc, "elsewhere", None).await;
        let owned: Vec<String> = from_owner["events"]
            .as_array()
            .unwrap()
            .iter()
            .map(event_key)
            .collect();
        assert!(
            !owned.is_empty(),
            "the owning node holds the events, or the peer below proves nothing"
        );

        let peer = crate::common::add_node("peer", &control, None).await;
        let pc = LatiqClient::connect(&peer.mcp_endpoint, Some("agent-x".into()))
            .await
            .unwrap();
        let from_peer = get_lineage(&pc, "elsewhere", None).await;
        let forwarded: Vec<String> = from_peer["events"]
            .as_array()
            .unwrap()
            .iter()
            .map(event_key)
            .collect();
        assert_eq!(
            forwarded, owned,
            "a peer must return the owner's events, not its own empty directory"
        );
        // The proof it really crossed the hop rather than being read locally:
        // the directory named is the OWNER's storage, which the peer does not
        // have — its own pond directory does not exist.
        assert_eq!(
            from_peer["lineage_dir"], from_owner["lineage_dir"],
            "the page must be the owner's, directory included"
        );
        oc.close().await.unwrap();
        pc.close().await.unwrap();
    }

    #[tokio::test]
    async fn lineage_tool_on_a_pond_without_lineage_says_so_clearly() {
        // The distinction this whole feature rests on: an empty list would tell
        // an agent the data appeared from nowhere. "We were not recording" has
        // to be a structured, actionable error instead.
        let s = start_stack().await;
        let c = LatiqClient::connect(&s.mcp_endpoint, Some("agent-x".into()))
            .await
            .unwrap();

        allocate(&c, "unrecorded", None).await;
        c.write("unrecorded", "CREATE TABLE t(id INTEGER)")
            .await
            .unwrap();

        let mut args = Map::new();
        args.insert("pond".into(), "unrecorded".into());
        let out = c.call_tool("get_lineage", args).await.unwrap();
        assert!(
            out.is_error,
            "a pond that never recorded must not answer with an empty page: {:?}",
            out.value
        );
        assert!(
            out.value.get("events").is_none(),
            "and must not answer with events at all: {:?}",
            out.value
        );
        assert_eq!(out.value["kind"], "invalid_value");
        let message = out.value["message"].as_str().unwrap();
        assert!(
            message.contains("unrecorded") && message.contains("does not record lineage"),
            "the message must name the pond and the reason: {message}"
        );
        let suggest = out.value["suggest"].as_str().unwrap();
        assert!(
            suggest.contains("lineage=true"),
            "the suggest must carry the one action that fixes it: {suggest}"
        );

        // The `see` link resolves to a real resource — a dangling one is worse
        // than none, because the agent spends a call finding that out.
        let see = out.value["see"].as_str().unwrap().to_string();
        assert_eq!(see, "latiq://recipes/lineage");
        let body = c.read_resource_text(&see).await.unwrap();
        assert!(
            body.contains("get_lineage") && body.contains("read_json_auto"),
            "the recipe must teach the tool AND the directory escape hatch: {body}"
        );
        c.close().await.unwrap();
    }
}

#[tokio::test]
async fn error_contract_allocate_on_an_unreachable_node_reads_as_not_created() {
    // The agent-facing half of eager allocation. The Data-gRPC test
    // (`tests/forwarding.rs::eager_allocation`) proves the mechanism; what this
    // adds is the rendering an agent actually meets — and, above all, that the
    // `see` it is sent to is a resource that EXISTS and covers this case. A
    // dangling `see` costs the agent a round trip to discover nothing.
    let (control, _admin) = common::start_control_plane_only().await;
    let _ghost = common::register_ghost_node(&control, "gone").await;
    let greeter = common::add_greeter_node("greeter", &control).await;
    let c = LatiqClient::connect(&greeter.mcp_endpoint, Some("agent-x".into()))
        .await
        .unwrap();

    let mut args = Map::new();
    args.insert("name".into(), "unreachable".into());
    let out = c.call_tool("allocate_pond", args).await.unwrap();
    assert!(
        out.is_error,
        "a pond nobody could create must not be reported as allocated: {:?}",
        out.value
    );
    assert!(
        out.value.get("pond_id").is_none(),
        "and must not hand back an id for a pond that does not exist: {:?}",
        out.value
    );
    assert_eq!(out.value["kind"], "pond_unavailable");
    let message = out.value["message"].as_str().unwrap();
    assert!(
        message.contains("was NOT created") && message.contains("rolled back"),
        "the message must say plainly that there is no pond and nothing was left \
         behind — 'storage error' reads as 'maybe it half-worked': {message}"
    );
    let suggest = out.value["suggest"].as_str().unwrap();
    assert!(
        suggest.contains("Retry allocate_pond"),
        "and give the agent the one move it can make itself: {suggest}"
    );

    let see = out.value["see"].as_str().unwrap().to_string();
    assert_eq!(see, "latiq://troubleshooting/pond-unavailable");
    let body = c.read_resource_text(&see).await.unwrap();
    assert!(
        body.contains("The same error from allocate_pond"),
        "the resource must cover the allocation case, not only the stranded-pond \
         one it was written for: {body}"
    );
    c.close().await.unwrap();
}

/// Timeouts and real cancellation on the agent surface. Both stop a query by
/// firing the same DuckDB interrupt, so the thing worth proving is that the
/// agent can still tell them apart — a timeout is "ask for more time or ask for
/// less data", a cancel is "you asked for this".
mod timeouts {
    use super::*;
    use common::start_stack_with_timeouts;
    use latiq_common::QueryTimeouts;
    use std::time::Duration;

    /// Cheap to submit, effectively unbounded to run: a generated range, so the
    /// test needs no data loaded and no table size decides its flakiness.
    const SLOW_SQL: &str = "SELECT count(*) FROM range(0, 100000000000) t(i) WHERE i % 999983 = 0";

    fn query_args(pond: &str, sql: &str) -> Map<String, Value> {
        let mut a = Map::new();
        a.insert("pond".into(), Value::String(pond.into()));
        a.insert("sql".into(), Value::String(sql.into()));
        a
    }

    #[tokio::test]
    async fn cancellation_a_timeout_is_reported_as_query_timeout_to_the_agent() {
        let s = start_stack_with_timeouts(QueryTimeouts::new(30_000, 30_000).unwrap()).await;
        let c = LatiqClient::connect(&s.mcp_endpoint, Some("agent-t".into()))
            .await
            .unwrap();
        c.allocate_pond(Some("kinds")).await.unwrap();

        let mut args = query_args("kinds", SLOW_SQL);
        args.insert("timeout_ms".into(), Value::from(400u64));
        let started = std::time::Instant::now();
        let r = c.call_tool("read_query", args).await.unwrap();
        assert!(r.is_error, "the deadline must fail the call: {}", r.value);
        assert_eq!(
            r.value["kind"], "query_timeout",
            "our deadline fired — not a generic engine error, and not query_cancelled \
             (nobody cancelled this): {}",
            r.value
        );
        assert!(
            r.value["message"]
                .as_str()
                .unwrap_or_default()
                .contains("400 ms"),
            "the agent must be told what it actually got: {}",
            r.value
        );
        assert!(
            started.elapsed() < std::time::Duration::from_secs(20),
            "the deadline must CUT the query, not be reported after it finished on \
             its own (took {:?})",
            started.elapsed()
        );

        // A wedged pooled connection is the real damage; the next reader proves
        // there is none.
        let ok = c.query("kinds", "SELECT 41 + 1 AS v").await.unwrap();
        assert!(!ok.is_error, "the pond must still answer: {}", ok.value);
        assert_eq!(ok.value["rows"][0][0], 42);
        c.close().await.unwrap();
    }

    #[tokio::test]
    async fn cancellation_notifications_cancelled_actually_stops_the_running_query() {
        // Observed through the pond's WRITER, which is exclusive (one DuckDB
        // instance per pond, one writer mutex). If the cancel does not reach the
        // engine, the abandoned write holds that mutex for its full 30 s
        // deadline and the second write below waits behind it.
        //
        // It has to be observed this way: a conforming MCP client resolves its
        // own request the instant it sends `notifications/cancelled` and drops
        // whatever the server answers, so the `query_cancelled` envelope is not
        // visible from here. Its KIND is pinned in
        // `latiq-agent-core/tests/agent_ops.rs::timeouts`; what this proves is
        // the half that only the real transport can — that rmcp's per-request
        // token reaches the running statement at all.
        let s = start_stack_with_timeouts(QueryTimeouts::new(30_000, 30_000).unwrap()).await;
        let c = LatiqClient::connect(&s.mcp_endpoint, Some("agent-k".into()))
            .await
            .unwrap();
        c.allocate_pond(Some("cancelme")).await.unwrap();

        let err = c
            .call_tool_then_cancel(
                "write_query",
                query_args(
                    "cancelme",
                    "CREATE TABLE t AS SELECT i FROM range(0, 100000000000) t(i) \
                     WHERE i % 999983 = 0",
                ),
                Duration::from_millis(400),
            )
            .await
            .expect_err("the client abandons its own request once it cancels");
        assert!(
            err.to_string().contains("cancelled"),
            "the notification must have gone out (this is the client's own \
             bookkeeping, not the server's answer): {err}"
        );

        let started = std::time::Instant::now();
        let ok = c
            .write("cancelme", "CREATE TABLE fine AS SELECT 1 AS a")
            .await
            .unwrap();
        let waited = started.elapsed();
        assert!(!ok.is_error, "the pond's writer must survive: {}", ok.value);
        assert!(
            waited < Duration::from_secs(10),
            "the cancelled write must have RELEASED the pond's writer; waiting {waited:?} \
             means the notification never reached the query and it ran on to its \
             30 s deadline"
        );
        c.close().await.unwrap();
    }

    #[tokio::test]
    async fn cancellation_the_clamp_is_visible_in_meta_on_the_agent_surface() {
        let s = start_stack_with_timeouts(QueryTimeouts::new(1_000, 2_000).unwrap()).await;
        let c = LatiqClient::connect(&s.mcp_endpoint, Some("agent-c".into()))
            .await
            .unwrap();
        c.allocate_pond(Some("clamp")).await.unwrap();

        let mut args = query_args("clamp", "SELECT 1 AS v");
        args.insert("timeout_ms".into(), Value::from(1_800_000u64));
        let r = c.call_tool("read_query", args).await.unwrap();
        assert!(!r.is_error, "an over-max ask runs, clamped: {}", r.value);
        assert_eq!(
            r.value["_meta"]["timeout_ms"], 2_000,
            "the agent asked for 30 minutes and must be able to SEE it got 2 s, \
             or it cannot understand why its next query dies early"
        );

        // A write reports it too, and an ask inside the ceiling is untouched.
        let mut args = query_args("clamp", "CREATE TABLE t AS SELECT 1 AS a");
        args.insert("timeout_ms".into(), Value::from(1_500u64));
        let w = c.call_tool("write_query", args).await.unwrap();
        assert!(!w.is_error, "{}", w.value);
        assert_eq!(w.value["_meta"]["timeout_ms"], 1_500);
        c.close().await.unwrap();
    }

    #[tokio::test]
    async fn cancellation_the_timeout_tool_argument_is_advertised_on_both_query_tools() {
        // An argument the model cannot discover is an argument it will not use.
        let s = start_stack().await;
        let c = LatiqClient::connect(&s.mcp_endpoint, Some("agent-s".into()))
            .await
            .unwrap();
        let tools = c.list_tools().await.unwrap();
        let mut checked = 0;
        for name in ["read_query", "write_query"] {
            let t = tools
                .iter()
                .find(|t| t.name == name)
                .unwrap_or_else(|| panic!("missing tool {name}"));
            let props = t.input_schema.get("properties").expect("a properties map");
            let d = props["timeout_ms"]["description"]
                .as_str()
                .unwrap_or_else(|| panic!("{name}.timeout_ms needs a description"));
            assert!(
                d.contains("clamped") && d.contains("_meta.timeout_ms"),
                "the description must tell the model that an over-max ask is clamped \
                 and where to read what was applied: {d}"
            );
            checked += 1;
        }
        assert_eq!(checked, 2, "both query tools must carry the argument");

        // …and explain_query must NOT. It shared `QueryArgs` and so advertised a
        // `timeout_ms` its own description admitted was ignored — a dead
        // argument the model must spend a decision on and can never benefit
        // from. explain executes nothing, so there is no deadline to set.
        let explain = tools
            .iter()
            .find(|t| t.name == "explain_query")
            .expect("missing tool explain_query");
        let props = explain
            .input_schema
            .get("properties")
            .and_then(|p| p.as_object())
            .expect("a properties map");
        assert!(
            !props.contains_key("timeout_ms"),
            "explain_query must not advertise an argument it ignores: {:?}",
            props.keys().collect::<Vec<_>>()
        );
        // Anti-vacuity: the arguments it DOES take are still advertised, so this
        // is not passing because the schema went empty.
        assert!(
            props.contains_key("pond") && props.contains_key("sql"),
            "explain_query still needs pond + sql: {:?}",
            props.keys().collect::<Vec<_>>()
        );
        c.close().await.unwrap();
    }
}

/// Error classification as an AGENT meets it. Every assertion here is the same
/// question: from this envelope alone, can the agent decide its next call
/// without a human?
///
/// The kinds these pin were all `internal` + "Retry; if it persists, report to
/// your operator", or `parse_error` + "Check the SQL syntax", or
/// `read_only_violation` — three answers that between them sent an agent to
/// retry a statement that can never succeed, to read a grammar for a name that
/// does not exist, and to call write_query with a typo.
mod classification {
    use super::*;

    /// One pond with one table, plus a client. Every case below is a single
    /// tool call against it, so they share the fixture rather than a stack
    /// each.
    async fn pond_with_a_table() -> (common::TestStack, LatiqClient) {
        let s = start_stack().await;
        let c = LatiqClient::connect(&s.mcp_endpoint, Some("agent-e".into()))
            .await
            .unwrap();
        let a = c.allocate_pond(Some("errs")).await.unwrap();
        assert!(!a.is_error, "allocate: {:?}", a.value);
        let out = call(
            &c,
            "write_query",
            "CREATE TABLE t(id INTEGER, name VARCHAR)",
        )
        .await;
        assert!(!out.is_error, "fixture: {:?}", out.value);
        (s, c)
    }

    async fn call(c: &LatiqClient, tool: &'static str, sql: &str) -> latiq_client::CallOutcome {
        let mut a = Map::new();
        a.insert("pond".into(), Value::String("errs".into()));
        a.insert("sql".into(), Value::String(sql.into()));
        c.call_tool(tool, a).await.unwrap()
    }

    fn field(out: &latiq_client::CallOutcome, key: &str) -> String {
        out.value[key]
            .as_str()
            .unwrap_or_else(|| panic!("no {key} in {:?}", out.value))
            .to_string()
    }

    /// D1 + D7: a name that doesn't resolve — the commonest failure in ordinary
    /// agent work — and its mirror image, a name that already does.
    ///
    /// `INSERT INTO nope` was `parse_error` ("Check the SQL syntax against the
    /// supported dialect") because DuckDB binds INSERT at prepare time;
    /// `CREATE TABLE t` on an existing `t` was `internal` ("Retry…report to
    /// your operator") because DuckDB defers that check to execution. Same
    /// mistake, same fix, two different answers decided by binder phasing.
    #[tokio::test]
    async fn error_contract_a_name_that_does_not_resolve_is_a_catalog_error() {
        let (_s, c) = pond_with_a_table().await;
        for (sql, tool) in [
            ("INSERT INTO nope VALUES (1)", "write_query"),
            ("SELECT * FROM nope", "read_query"),
            ("SELECT nosuchcol FROM t", "read_query"),
            ("CREATE TABLE t(id INTEGER)", "write_query"),
            (
                "CREATE TABLE information_schema.x(i INTEGER)",
                "write_query",
            ),
        ] {
            let out = call(&c, tool, sql).await;
            assert!(out.is_error, "{sql} must fail");
            assert_eq!(out.value["kind"], "catalog_error", "{sql}: {:?}", out.value);
            let suggest = field(&out, "suggest");
            // The next call, named. Not a dialect reference, and not "retry".
            assert!(
                suggest.contains("describe_pond") && suggest.contains("SHOW TABLES"),
                "{sql}: the suggest must name the call that answers this: {suggest}"
            );
            assert!(
                !suggest.contains("report to your operator"),
                "{sql}: no operator can fix a name in the caller's SQL: {suggest}"
            );
            let message = field(&out, "message");
            assert!(
                !message.starts_with("SQL parse error") && !message.starts_with("engine error"),
                "{sql}: the message must not mislabel what happened: {message}"
            );
        }
        // The `see` is a page about THIS kind, and it exists.
        let out = call(&c, "write_query", "INSERT INTO nope VALUES (1)").await;
        let see = field(&out, "see");
        assert_eq!(see, "latiq://troubleshooting/catalog-error");
        let body = c.read_resource_text(&see).await.unwrap();
        assert!(
            body.contains("SHOW TABLES") && body.contains("already exists"),
            "the page must cover both halves of the kind: {body}"
        );
        c.close().await.unwrap();
    }

    /// D4: a typo in read_query was reported as a write.
    ///
    /// `SELEKT * FROM t` does not start with a read keyword, so the read guard
    /// called it a write: "read_query received a statement that is not
    /// read-only… Use write_query for INSERT/UPDATE/DELETE/DDL". The agent
    /// obeys, calls write_query, and only then learns it made a typo — two
    /// calls and a false belief to fix one character.
    #[tokio::test]
    async fn error_contract_a_typo_in_read_query_is_a_parse_error_not_a_write() {
        let (_s, c) = pond_with_a_table().await;
        for sql in ["SELEKT * FROM t", "@@@@", "", "   ", "SELECT * FRM t"] {
            let out = call(&c, "read_query", sql).await;
            assert!(out.is_error, "{sql:?} must fail");
            assert_eq!(
                out.value["kind"], "parse_error",
                "{sql:?} is a typo, not a write: {:?}",
                out.value
            );
        }
        // The control: read_query must still refuse a REAL write, or the fix
        // above would have been a hole in the read guard rather than a
        // correction to it.
        for sql in [
            "INSERT INTO t VALUES (1, 'a')",
            "DROP TABLE t",
            "WITH x AS (SELECT 1) INSERT INTO t SELECT 1, 'a'",
            "SELECT 1;DROP TABLE t",
            "SET memory_limit='1GB'",
        ] {
            let out = call(&c, "read_query", sql).await;
            assert_eq!(
                out.value["kind"], "read_only_violation",
                "{sql} is a write and must still be refused as one: {:?}",
                out.value
            );
        }
        c.close().await.unwrap();
    }

    /// D7: a value DuckDB could not convert, and a source it could not reach,
    /// were both `parse_error` — "Check the SQL syntax against the supported
    /// dialect", for SQL whose syntax was fine.
    #[tokio::test]
    async fn error_contract_a_rejected_value_and_an_unreachable_source_are_told_apart() {
        let (_s, c) = pond_with_a_table().await;

        let bad_value = call(&c, "write_query", "INSERT INTO t VALUES ('notanint','x')").await;
        assert_eq!(
            bad_value.value["kind"], "invalid_value",
            "a value the engine could not convert is not a syntax error: {:?}",
            bad_value.value
        );
        let suggest = field(&bad_value, "suggest");
        assert!(
            suggest.contains("CAST") && suggest.contains("DESCRIBE"),
            "the suggest must name how to fix a value: {suggest}"
        );

        // Port 9 (discard) refuses immediately — an unreachable source with no
        // network wait and nothing to be flaky about.
        let unreachable = call(
            &c,
            "read_query",
            "SELECT * FROM read_csv('http://127.0.0.1:9/none.csv')",
        )
        .await;
        assert_eq!(
            unreachable.value["kind"], "source_unavailable",
            "an address the caller supplied is not our failure and not a syntax \
             error: {:?}",
            unreachable.value
        );
        let suggest = field(&unreachable, "suggest");
        assert!(
            !suggest.contains("report to your operator"),
            "an operator cannot fix a URL in the caller's SQL: {suggest}"
        );
        let see = field(&unreachable, "see");
        assert_eq!(see, "latiq://troubleshooting/source-unavailable");
        let body = c.read_resource_text(&see).await.unwrap();
        assert!(
            body.contains("reachable from the NODE"),
            "the page must be about this kind: {body}"
        );

        // And a genuine syntax error is still a parse error — the kinds above
        // are distinctions, not a wholesale relabelling.
        let syntax = call(&c, "write_query", "INSERT INTO t VALUES (").await;
        assert_eq!(syntax.value["kind"], "parse_error", "{:?}", syntax.value);
        c.close().await.unwrap();
    }
}

/// **Invariant 13 on the agent surface: a short or degraded answer must never
/// look like a complete one.** Every test here pins a case where Latiq used to
/// succeed, or report a number, and the number was wrong or the degradation was
/// invisible — which is worse than an error, because an agent acts confidently
/// on it. All were observed live against a running stack.
mod honest_answers {
    use super::*;

    fn args(pairs: &[(&str, Value)]) -> Map<String, Value> {
        pairs
            .iter()
            .map(|(k, v)| ((*k).to_string(), v.clone()))
            .collect()
    }

    /// D6. `allocate_pond {"tier":"gigantic"}` used to succeed: the pond ran at
    /// medium (`PondTier::parse(t).unwrap_or_default()`) while `describe_pond`
    /// reported `gigantic` for the rest of its life — a DURABLE lie, and the
    /// agent path was permissive while the operator path (`pond set-tier`) was
    /// strict, which is backwards.
    #[tokio::test]
    async fn policy_tier_an_unknown_tier_is_refused_rather_than_run_at_the_default() {
        let s = start_stack().await;
        let c = LatiqClient::connect(&s.mcp_endpoint, Some("agent-t".into()))
            .await
            .unwrap();
        for bad in ["gigantic", "larg", "Medium!"] {
            let out = c
                .call_tool(
                    "allocate_pond",
                    args(&[("name", "tierbad".into()), ("tier", bad.into())]),
                )
                .await
                .unwrap();
            assert!(out.is_error, "tier `{bad}` must not create a pond");
            assert_eq!(
                out.value["kind"], "invalid_value",
                "tier `{bad}` is a bad VALUE, not an internal failure: {:?}",
                out.value
            );
            let msg = out.value["message"].as_str().unwrap_or_default();
            assert!(msg.contains(bad), "must name the offender: {msg}");
            for t in latiq_common::tier::CREATABLE {
                assert!(msg.contains(t), "must offer '{t}': {msg}");
            }
            // The pond genuinely does not exist — the lie was durable, so the
            // absence has to be too.
            let d = c
                .call_tool("describe_pond", args(&[("pond", "tierbad".into())]))
                .await
                .unwrap();
            assert_eq!(
                d.value["kind"], "pond_not_found",
                "a refused tier must leave no pond behind: {:?}",
                d.value
            );
        }
        // Anti-vacuity: a real tier still allocates AND is reported back.
        let out = c
            .call_tool(
                "allocate_pond",
                args(&[("name", "tierok".into()), ("tier", "large".into())]),
            )
            .await
            .unwrap();
        assert!(!out.is_error, "{:?}", out.value);
        let d = c
            .call_tool("describe_pond", args(&[("pond", "tierok".into())]))
            .await
            .unwrap();
        assert_eq!(d.value["pond"]["tier"], "large");
        c.close().await.unwrap();
    }

    /// D6, the discovery half: a model that cannot see the options guesses. The
    /// schema now enumerates them, so the tier is chosen before the call rather
    /// than corrected after a failure.
    #[tokio::test]
    async fn policy_tier_the_schema_enumerates_the_tiers_a_model_may_choose() {
        let s = start_stack().await;
        let c = LatiqClient::connect(&s.mcp_endpoint, None).await.unwrap();
        let tools = c.list_tools().await.unwrap();
        let alloc = tools
            .iter()
            .find(|t| t.name == "allocate_pond")
            .expect("missing tool allocate_pond");
        let tier = &alloc.input_schema["properties"]["tier"];
        let listed: Vec<&str> = tier["enum"]
            .as_array()
            .unwrap_or_else(|| panic!("tier needs an `enum`; got {tier}"))
            .iter()
            .filter_map(|v| v.as_str())
            .collect();
        assert_eq!(
            listed,
            latiq_common::tier::CREATABLE,
            "the schema must offer exactly the tiers the server accepts at \
             creation — `none` is an operator grant and must not be advertised"
        );
        c.close().await.unwrap();
    }

    /// D11. A pond name becomes the pond's SQL catalog identifier. `""` used to
    /// succeed and silently become the pond's uuid; `a b/c` used to succeed as
    /// itself, slash and all.
    #[tokio::test]
    async fn pond_lifecycle_an_illegal_pond_name_is_refused_not_repaired() {
        let s = start_stack().await;
        let c = LatiqClient::connect(&s.mcp_endpoint, Some("agent-n".into()))
            .await
            .unwrap();
        for bad in ["", "a b/c", "sales.2026"] {
            let out = c
                .call_tool("allocate_pond", args(&[("name", bad.into())]))
                .await
                .unwrap();
            assert!(out.is_error, "name '{bad}' must be refused, not repaired");
            assert_eq!(out.value["kind"], "invalid_value", "{:?}", out.value);
            let msg = out.value["message"].as_str().unwrap_or_default();
            assert!(
                msg.contains(latiq_common::pond_name::RULE),
                "must say what a name may be: {msg}"
            );
        }
        // Empty must point at the way to get a generated name, since that is
        // what it used to do by accident.
        let empty = c
            .call_tool("allocate_pond", args(&[("name", "".into())]))
            .await
            .unwrap();
        assert!(
            empty.value["message"]
                .as_str()
                .is_some_and(|m| m.contains("omit `name`")),
            "{:?}",
            empty.value
        );
        // Anti-vacuity: omitting the name still works and still names the pond.
        let gen = c.call_tool("allocate_pond", Map::new()).await.unwrap();
        assert!(!gen.is_error, "{:?}", gen.value);
        assert!(
            gen.value["pond_name"]
                .as_str()
                .is_some_and(|n| !n.is_empty()),
            "an omitted name must still produce one: {:?}",
            gen.value
        );
        c.close().await.unwrap();
    }

    /// D11, the `0` divergence. `get_lineage {"limit":0}` was rejected with an
    /// excellent message while `read_query {"timeout_ms":0}` silently ran at
    /// 30000 and reported 30000 back — two adjacent tools, opposite policies for
    /// the same literal. On a JSON surface an explicit `0` is a value the caller
    /// chose, so both refuse it now.
    #[tokio::test]
    async fn cancellation_a_zero_timeout_is_refused_like_a_zero_lineage_limit() {
        let s = start_stack().await;
        let c = LatiqClient::connect(&s.mcp_endpoint, Some("agent-z".into()))
            .await
            .unwrap();
        c.call_tool("allocate_pond", args(&[("name", "zeros".into())]))
            .await
            .unwrap();
        for tool in ["read_query", "write_query"] {
            let out = c
                .call_tool(
                    tool,
                    args(&[
                        ("pond", "zeros".into()),
                        ("sql", "SELECT 1".into()),
                        ("timeout_ms", Value::from(0)),
                    ]),
                )
                .await
                .unwrap();
            assert!(
                out.is_error,
                "{tool}: `0` must not be read as 'use the default': {:?}",
                out.value
            );
            assert_eq!(out.value["kind"], "invalid_value", "{:?}", out.value);
            assert!(
                out.value["message"]
                    .as_str()
                    .is_some_and(|m| m.contains("timeout_ms") && m.contains("at least 1")),
                "the refusal must name the field and the rule: {:?}",
                out.value
            );
        }
        // Anti-vacuity: omitting it still runs, and a real value is honoured —
        // this is a rule about `0`, not about `timeout_ms`.
        let ok = c
            .call_tool(
                "read_query",
                args(&[("pond", "zeros".into()), ("sql", "SELECT 1".into())]),
            )
            .await
            .unwrap();
        assert!(!ok.is_error, "{:?}", ok.value);
        assert!(
            ok.value["_meta"]["timeout_ms"].as_u64().unwrap_or(0) > 0,
            "{:?}",
            ok.value
        );
        c.close().await.unwrap();
    }

    /// D11. `get_lineage {"limit":99999}` came back as a 500-event page with
    /// nothing saying it had been clamped — indistinguishable from a pond that
    /// has exactly 500 events. `read_query`'s timeout clamp has always been
    /// reported via `_meta.timeout_ms`; this is the same discipline.
    #[tokio::test]
    async fn lineage_an_over_max_limit_is_clamped_and_the_page_says_so() {
        let s = start_stack().await;
        let c = LatiqClient::connect(&s.mcp_endpoint, Some("agent-l".into()))
            .await
            .unwrap();
        c.call_tool(
            "allocate_pond",
            args(&[("name", "traced".into()), ("lineage", Value::Bool(true))]),
        )
        .await
        .unwrap();
        c.call_tool(
            "write_query",
            args(&[
                ("pond", "traced".into()),
                ("sql", "CREATE TABLE t(i INTEGER)".into()),
            ]),
        )
        .await
        .unwrap();

        let page = |limit: u64| {
            let c = &c;
            async move {
                let out = c
                    .call_tool(
                        "get_lineage",
                        args(&[("pond", "traced".into()), ("limit", Value::from(limit))]),
                    )
                    .await
                    .unwrap();
                assert!(!out.is_error, "{:?}", out.value);
                out.value
            }
        };
        assert_eq!(
            page(99_999).await["limit_applied"],
            Value::from(latiq_lineage::MAX_LIMIT),
            "a clamped ask must report the value that was actually applied"
        );
        // Anti-vacuity: it reports the CALLER's number when nothing was
        // clamped, so it is not a constant.
        assert_eq!(page(7).await["limit_applied"], Value::from(7));
        c.close().await.unwrap();
    }

    /// D11. A misspelled argument was dropped in silence, so a typo had no
    /// effect and no warning — an agent could believe it had set a timeout it
    /// had not set.
    #[tokio::test]
    async fn error_contract_an_unknown_argument_is_reported_not_ignored() {
        let s = start_stack().await;
        let c = LatiqClient::connect(&s.mcp_endpoint, Some("agent-u".into()))
            .await
            .unwrap();
        c.call_tool("allocate_pond", args(&[("name", "typos".into())]))
            .await
            .unwrap();
        let err = c
            .call_tool(
                "read_query",
                args(&[
                    ("pond", "typos".into()),
                    ("sql", "SELECT 1".into()),
                    ("timout_ms", Value::from(5_000)),
                ]),
            )
            .await
            .expect_err("a misspelled argument must not be silently dropped");
        let msg = err.to_string();
        assert!(
            msg.contains("timout_ms"),
            "the refusal must name the key that was not understood: {msg}"
        );
        assert!(
            msg.contains("timeout_ms"),
            "and list the ones that are, which is the correction: {msg}"
        );
        c.close().await.unwrap();
    }

    /// D16. `describe_pond` reported `"columns": []` for every table in every
    /// pond — a hard-coded empty vec. An agent reads that as "this table has no
    /// columns", which is worse than the field being absent, and the tool's own
    /// description promises "a summary of its tables".
    #[tokio::test]
    async fn pond_lifecycle_describe_reports_each_table_s_columns() {
        let s = start_stack().await;
        let c = LatiqClient::connect(&s.mcp_endpoint, Some("agent-d".into()))
            .await
            .unwrap();
        c.call_tool("allocate_pond", args(&[("name", "described".into())]))
            .await
            .unwrap();
        c.call_tool(
            "write_query",
            args(&[
                ("pond", "described".into()),
                (
                    "sql",
                    "CREATE TABLE orders(id INTEGER, total DOUBLE)".into(),
                ),
            ]),
        )
        .await
        .unwrap();
        let out = c
            .call_tool("describe_pond", args(&[("pond", "described".into())]))
            .await
            .unwrap();
        assert!(!out.is_error, "{:?}", out.value);
        let orders = out.value["schema"]["tables"]
            .as_array()
            .expect("tables")
            .iter()
            .find(|t| t["name"] == "orders")
            .unwrap_or_else(|| panic!("orders must be listed: {:?}", out.value));
        let cols = orders["columns"].as_array().expect("columns");
        let names: Vec<&str> = cols.iter().filter_map(|c| c[0].as_str()).collect();
        assert_eq!(
            names,
            vec!["id", "total"],
            "describe must name the table's columns, in declaration order: {orders}"
        );
        assert!(
            cols[0][1].as_str().is_some_and(|t| t.contains("INTEGER")),
            "and carry each column's type: {orders}"
        );
        c.close().await.unwrap();
    }

    /// `drop_pond`'s `confirm` is the single most consequential argument on the
    /// only irreversibly destructive tool, and it had NO description at all — an
    /// agent could only learn what it was for by being refused once.
    #[tokio::test]
    async fn pond_lifecycle_the_confirm_argument_is_documented_before_it_is_needed() {
        let s = start_stack().await;
        let c = LatiqClient::connect(&s.mcp_endpoint, None).await.unwrap();
        let tools = c.list_tools().await.unwrap();
        let drop = tools
            .iter()
            .find(|t| t.name == "drop_pond")
            .expect("missing tool drop_pond");
        let d = drop.input_schema["properties"]["confirm"]["description"]
            .as_str()
            .unwrap_or_else(|| panic!("confirm needs a description: {:?}", drop.input_schema));
        assert!(
            d.contains("true"),
            "it must say what value performs the drop: {d}"
        );
        assert!(
            d.contains("no undo") || d.contains("irreversible"),
            "and that there is no undo — that is the decision it gates: {d}"
        );
        c.close().await.unwrap();
    }
}

// ---------------------------------------------------------------------------
/// **The declared output contract.** Every tool publishes an `outputSchema`, and
/// the point of publishing one is that a client may rely on it — so this drives
/// each tool to a REAL success response over the real transport and validates
/// that response against the schema the server itself advertised in
/// `tools/list`. rmcp deliberately does not validate responses against
/// `outputSchema` ("since rust is a strong type language…", `model.rs`), so if
/// we do not, nobody does and the declaration is a document rather than a
/// contract.
///
/// A submodule, not a new binary (tests/CLAUDE.md rule 5).
// ---------------------------------------------------------------------------
mod output_schema {
    use crate::common::start_stack;
    use latiq_client::LatiqClient;
    use latiq_proto::v1::admin_client::AdminClient;
    use latiq_proto::v1::{CatalogAddRequest, CatalogMsg};
    use serde_json::{Map, Value};
    use std::collections::HashMap;

    /// Every tool this surface advertises. Pinned as a list so a NEW tool fails
    /// this test rather than slipping in undeclared and unvalidated: the count
    /// assertion below is the anti-vacuity guard (tests/CLAUDE.md rule 3).
    const TOOLS: &[&str] = &[
        "allocate_pond",
        "describe_pond",
        "list_ponds",
        "drop_pond",
        "read_query",
        "write_query",
        "explain_query",
        "list_datasets",
        "load_dataset",
        "list_catalogs",
        "describe_catalog",
        "pull_catalog",
        "get_lineage",
    ];

    /// A local DuckLake catalog with one table — file metadata + local data, no
    /// network and no docker, so `describe_catalog`/`pull_catalog` reach a real
    /// SUCCESS response in this suite rather than only an error one. Same seed
    /// the Data-surface catalog test uses (`admin.rs::catalogs`).
    fn seed_ducklake(dir: &std::path::Path) -> (String, String) {
        let meta = dir.join("meta.duckdb");
        let data = dir.join("data");
        std::fs::create_dir_all(&data).unwrap();
        let conn = duckdb::Connection::open_in_memory().unwrap();
        conn.execute_batch(&format!(
            "INSTALL ducklake; LOAD ducklake;
             ATTACH 'ducklake:{}' AS ext (DATA_PATH '{}');
             CREATE TABLE ext.widgets AS
               SELECT * FROM (VALUES (1,'gear',9.99),(2,'bolt',0.99)) t(id,name,price);",
            meta.display(),
            data.display(),
        ))
        .unwrap();
        (meta.display().to_string(), data.display().to_string())
    }

    fn args(pairs: &[(&str, Value)]) -> Map<String, Value> {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.clone()))
            .collect()
    }

    #[tokio::test]
    async fn output_schema_every_tool_declares_one_and_its_real_response_validates() {
        let tmp = tempfile::tempdir().unwrap();
        let (metadata_path, data_path) = seed_ducklake(tmp.path());
        let s = start_stack().await;
        AdminClient::connect(s.admin_endpoint.clone())
            .await
            .unwrap()
            .catalog_add(CatalogAddRequest {
                catalog: Some(CatalogMsg {
                    name: "ext".into(),
                    r#type: "ducklake".into(),
                    params: HashMap::from([
                        ("metadata_path".into(), metadata_path),
                        ("data_path".into(), data_path),
                    ]),
                    description: "local ducklake".into(),
                    tags: vec!["test".into()],
                    created_by: String::new(),
                    created_at: String::new(),
                }),
            })
            .await
            .unwrap();

        let c = LatiqClient::connect(&s.mcp_endpoint, Some("agent-x".into()))
            .await
            .unwrap();

        // What the server ADVERTISES — the schemas a client would compile.
        let tools = c.list_tools().await.unwrap();
        let mut declared: HashMap<String, Value> = HashMap::new();
        for t in &tools {
            let schema = t.output_schema.as_ref().unwrap_or_else(|| {
                panic!(
                    "tool `{}` declares no outputSchema; every result of ours is \
                     structured, so an undeclared one is a promise we are not making",
                    t.name
                )
            });
            declared.insert(t.name.to_string(), Value::Object((**schema).clone()));
        }
        let mut names: Vec<&str> = TOOLS.to_vec();
        names.sort();
        let mut advertised: Vec<&str> = tools.iter().map(|t| t.name.as_ref()).collect();
        advertised.sort();
        assert_eq!(
            advertised, names,
            "the advertised tool set must match the list this test drives — a new \
             tool has to gain a declared+validated response, not slip past"
        );

        // Drive every tool to a real SUCCESS response, in dependency order.
        let mut observed: Vec<(&str, Value)> = Vec::new();
        let mut record = |name: &'static str, out: latiq_client::CallOutcome| {
            assert!(!out.is_error, "{name} must succeed here: {:#}", out.value);
            observed.push((name, out.value));
        };

        record(
            "allocate_pond",
            c.call_tool(
                "allocate_pond",
                args(&[("name", "sch".into()), ("lineage", true.into())]),
            )
            .await
            .unwrap(),
        );
        record(
            "write_query",
            c.write("sch", "CREATE TABLE t AS SELECT 1 AS id, 'a' AS nm")
                .await
                .unwrap(),
        );
        record(
            "read_query",
            c.query("sch", "SELECT * FROM t").await.unwrap(),
        );
        record(
            "explain_query",
            c.explain("sch", "SELECT * FROM t").await.unwrap(),
        );
        record("describe_pond", c.describe_pond("sch").await.unwrap());
        record("list_ponds", c.list_ponds().await.unwrap());
        record(
            "list_datasets",
            c.call_tool("list_datasets", Map::new()).await.unwrap(),
        );
        record(
            "load_dataset",
            c.call_tool(
                "load_dataset",
                args(&[("pond", "sch".into()), ("dataset", "holdings".into())]),
            )
            .await
            .unwrap(),
        );
        record(
            "list_catalogs",
            c.call_tool("list_catalogs", Map::new()).await.unwrap(),
        );
        record(
            "describe_catalog",
            c.call_tool(
                "describe_catalog",
                args(&[("pond", "sch".into()), ("catalog", "ext".into())]),
            )
            .await
            .unwrap(),
        );
        record(
            "pull_catalog",
            c.call_tool(
                "pull_catalog",
                args(&[
                    ("pond", "sch".into()),
                    ("catalog", "ext".into()),
                    (
                        "query",
                        "CREATE TABLE cheap AS SELECT id,name FROM ext.widgets WHERE price < 10"
                            .into(),
                    ),
                ]),
            )
            .await
            .unwrap(),
        );
        record(
            "get_lineage",
            c.call_tool("get_lineage", args(&[("pond", "sch".into())]))
                .await
                .unwrap(),
        );
        // Destructive, so last.
        record("drop_pond", c.drop_pond("sch").await.unwrap());

        // The whole point: the real response satisfies the declared schema.
        for (name, value) in &observed {
            let schema = declared
                .get(*name)
                .unwrap_or_else(|| panic!("no declared schema for {name}"));
            let validator = jsonschema::validator_for(schema)
                .unwrap_or_else(|e| panic!("{name}'s declared outputSchema must compile: {e}"));
            if let Err(e) = validator.validate(value) {
                panic!(
                    "{name}'s real response does not satisfy its DECLARED outputSchema \
                     at `{}`: {e}\nresponse: {value:#}\nschema: {schema:#}",
                    e.instance_path()
                );
            }
        }
        assert_eq!(
            observed.len(),
            TOOLS.len(),
            "every tool must contribute a real response — a tool validated against \
             nothing is a tool nobody checked"
        );
        c.close().await.unwrap();
    }

    /// **Errors are deliberately OUTSIDE the declared schema.** A failed tool
    /// call answers with `isError: true` and the `ErrorEnvelope` in
    /// `structuredContent` — not the success shape — and both reference MCP
    /// clients skip output-schema validation entirely on an error result (the
    /// TypeScript SDK guards both branches with `&& !result.isError`; the Python
    /// SDK with `if ... and not result.is_error`). So the schemas describe the
    /// success shape only, and the envelope stays one shape across all 13 tools
    /// instead of thirteen `anyOf`s that would have to be edited in lockstep.
    ///
    /// This test pins that decision from both ends: the envelope really is what
    /// comes back, and it really would NOT satisfy the success schema — which is
    /// exactly why the schema must not be read as covering it.
    #[tokio::test]
    async fn output_schema_errors_answer_with_the_envelope_outside_the_success_shape() {
        let s = start_stack().await;
        let c = LatiqClient::connect(&s.mcp_endpoint, Some("agent-x".into()))
            .await
            .unwrap();
        let tools = c.list_tools().await.unwrap();
        let schema = Value::Object(
            (**tools
                .iter()
                .find(|t| t.name == "read_query")
                .unwrap()
                .output_schema
                .as_ref()
                .expect("read_query declares an outputSchema"))
            .clone(),
        );

        let out = c.query("no-such-pond", "SELECT 1").await.unwrap();
        assert!(
            out.is_error,
            "an unknown pond is a tool error: {:#}",
            out.value
        );
        assert_eq!(
            out.value["kind"], "pond_not_found",
            "the envelope, keyed by the field agents route on: {:#}",
            out.value
        );

        let validator = jsonschema::validator_for(&schema).unwrap();
        assert!(
            validator.validate(&out.value).is_err(),
            "the envelope is NOT the success shape — if it validated, the schema \
             would be too loose to promise anything about a successful read"
        );
        c.close().await.unwrap();
    }
}
