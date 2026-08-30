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
