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
    let (control, _admin) = common::start_control_plane_only().await;
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
    let (control, _admin) = common::start_control_plane_only().await;

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
/// so `allocate_pond` takes the flag and `describe_pond` reports it — an agent
/// can tell whether `lineage.events` will exist before querying it. Off unless
/// asked for. A submodule, not a new binary (tests/CLAUDE.md rule 5).
// ---------------------------------------------------------------------------
mod lineage {
    use crate::common::start_stack;
    use latiq_client::LatiqClient;
    use serde_json::{json, Map, Value};

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
        // must not be told `lineage.events` exists when it does not.
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
}
