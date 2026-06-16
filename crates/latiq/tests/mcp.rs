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
