//! End-to-end: a real Latiq MCP server (over rmcp Streamable-HTTP) driven by the
//! real latiq-client. Exercises the full agent loop across the network boundary.
use latiq_agent_core::{AgentConfig, AgentOps, RegistryControlPlane};
use latiq_client::LatiqClient;
use latiq_control_plane::Registry;
use latiq_engine_duckdb::DuckEngine;
use latiq_mcp::serve_mcp;
use latiq_storage::TempFs;
use std::sync::Arc;

fn build_ops() -> Arc<AgentOps> {
    let registry = Registry::open(None).unwrap();
    registry
        .register_node(
            "node-a",
            "http://127.0.0.1:0/mcp",
            "http://127.0.0.1:0",
            100,
        )
        .unwrap();
    let control = Arc::new(RegistryControlPlane::new(registry));
    let storage = Arc::new(TempFs::new());
    let engine = Arc::new(DuckEngine::new());
    Arc::new(AgentOps::new(
        control,
        storage,
        engine,
        AgentConfig::default(),
    ))
}

#[tokio::test]
async fn server_client_agent_loop() {
    let ops = build_ops();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    drop(listener);
    let addr = format!("127.0.0.1:{port}").parse().unwrap();

    tokio::spawn(async move {
        serve_mcp(addr, ops, None).await.unwrap();
    });
    tokio::time::sleep(std::time::Duration::from_millis(400)).await;

    let endpoint = format!("http://127.0.0.1:{port}/mcp");
    let client = LatiqClient::connect(&endpoint, Some("agent-cli".into()))
        .await
        .unwrap();

    // Allocate.
    let alloc = client.allocate_pond(Some("demo")).await.unwrap();
    assert!(!alloc.is_error, "allocate failed: {:?}", alloc.value);
    assert_eq!(alloc.value["pond_name"], "demo");

    // Write (DDL + insert).
    let w1 = client
        .write("demo", "CREATE TABLE events(id INTEGER, sev VARCHAR)")
        .await
        .unwrap();
    assert!(!w1.is_error, "create failed: {:?}", w1.value);
    let w2 = client
        .write(
            "demo",
            "INSERT INTO events VALUES (1,'high'),(2,'critical')",
        )
        .await
        .unwrap();
    assert!(!w2.is_error);

    // Read.
    let r = client
        .query("demo", "SELECT id, sev FROM events ORDER BY id")
        .await
        .unwrap();
    assert!(!r.is_error, "read failed: {:?}", r.value);
    assert_eq!(r.value["rows"].as_array().unwrap().len(), 2);
    assert_eq!(r.value["rows"][1][1], "critical");

    // Attribution visible.
    let attr = client
        .query(
            "demo",
            "SELECT DISTINCT author FROM ducklake_snapshots('demo')",
        )
        .await
        .unwrap();
    let authors: Vec<_> = attr.value["rows"]
        .as_array()
        .unwrap()
        .iter()
        .filter_map(|row| row[0].as_str())
        .collect();
    assert!(authors.contains(&"agent-cli"), "got {authors:?}");

    // Describe + list.
    let desc = client.describe_pond("demo").await.unwrap();
    assert_eq!(desc.value["pond"]["name"], "demo");
    let list = client.list_ponds().await.unwrap();
    assert_eq!(list.value["ponds"].as_array().unwrap().len(), 1);

    // Structured error path: unknown pond.
    let err = client.query("ghost", "SELECT 1").await.unwrap();
    assert!(err.is_error);
    assert_eq!(err.value["kind"], "pond_not_found");

    // Drop.
    let drop = client.drop_pond("demo").await.unwrap();
    assert!(!drop.is_error);

    client.close().await.unwrap();
}
