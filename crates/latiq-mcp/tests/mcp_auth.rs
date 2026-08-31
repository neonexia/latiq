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

//! The MCP surface at the crate boundary: the full agent loop over real rmcp
//! Streamable-HTTP, and the OAuth 2.1 resource-server metadata in front of it.
//!
//! The metadata tests live here rather than in the full-stack suite because the
//! property under test is precisely that the ADVERTISED url wins over the BOUND
//! one — and the full-stack harness binds loopback, where the two coincide and
//! any confusion between them passes.
//!
//! One binary, not two: each integration binary statically links a bundled
//! DuckDB (~130-160 MB), and both halves want the same `build_ops()` fixture.
use latiq_agent_core::{AgentConfig, AgentOps, RegistryControlPlane};
use latiq_client::LatiqClient;
use latiq_control_plane::Registry;
use latiq_engine_duckdb::DuckEngine;
use latiq_mcp::{serve_mcp, serve_mcp_with_listener};
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
    Arc::new(AgentOps::new(
        Arc::new(RegistryControlPlane::new(registry)),
        Arc::new(TempFs::new()),
        Arc::new(DuckEngine::new()),
        AgentConfig::default(),
    ))
}

/// Serve MCP on loopback with auth on, advertising `public_url`. Returns the
/// base url to dial (loopback), which deliberately differs from what is
/// advertised.
async fn serve(auth: latiq_auth::AuthConfig, public_url: Option<&str>) -> String {
    let verifier = Arc::new(latiq_auth::Verifier::new(auth).expect("build verifier"));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = format!("http://{}", listener.local_addr().unwrap());
    let public = public_url.map(String::from);
    tokio::spawn(async move {
        serve_mcp_with_listener(listener, build_ops(), Some(verifier), public)
            .await
            .unwrap();
    });
    // Wait for the listener's accept loop rather than sleeping blind.
    for _ in 0..100 {
        if reqwest::get(format!("{base}/.well-known/oauth-protected-resource"))
            .await
            .is_ok()
        {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
    base
}

#[tokio::test]
async fn auth_metadata_advertises_the_public_url_not_the_bound_socket() {
    // A node binds 0.0.0.0 (or sits behind a gateway) and advertises a routable
    // name. Both the `resource` identifier and the challenge's metadata url must
    // be the advertised one: a conforming client compares `resource` against the
    // host it dialled, and a challenge pointing at the bind address is
    // undiscoverable.
    let idp = latiq_auth::test_support::TestIdp::start().await;
    let base = serve(idp.auth_config(), Some("https://gateway.example/mcp")).await;

    let doc: serde_json::Value =
        reqwest::get(format!("{base}/.well-known/oauth-protected-resource"))
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
    assert_eq!(doc["resource"], "https://gateway.example/mcp");
    assert_eq!(doc["authorization_servers"][0], idp.issuer);

    let res = reqwest::Client::new()
        .post(format!("{base}/mcp"))
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
        .unwrap_or_default();
    assert!(
        challenge.contains("https://gateway.example/.well-known/oauth-protected-resource"),
        "the challenge must point at the ADVERTISED document: {challenge:?}"
    );
    assert!(
        !challenge.contains("127.0.0.1"),
        "the bound socket must not leak into the challenge: {challenge:?}"
    );
}

#[tokio::test]
async fn auth_metadata_publishes_the_configured_public_url_over_the_advertised_one() {
    // `--public-mcp-url`: behind a gateway, the node's own advertised address is
    // NOT what agents dial, and a conforming client refuses a `resource` whose
    // origin differs from the URL it dialled — before it ever asks for a token.
    // So the configured value must win over both the advertised and bound ones,
    // in the document AND in the challenge.
    let idp = latiq_auth::test_support::TestIdp::start().await;
    let public = latiq_mcp::resolve_public_mcp_url(
        Some("https://latiq.example.com/mcp"),
        "http://pond-node-1:51401",
        "0.0.0.0:51402".parse().unwrap(),
    )
    .expect("a well-formed public url resolves");
    let base = serve(idp.auth_config(), Some(&public)).await;
    let bound = base.trim_start_matches("http://").to_string();

    let doc: serde_json::Value =
        reqwest::get(format!("{base}/.well-known/oauth-protected-resource"))
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
    assert_eq!(doc["resource"], "https://latiq.example.com/mcp");
    let doc_text = doc.to_string();
    assert!(
        !doc_text.contains(&bound) && !doc_text.contains("pond-node-1"),
        "neither the bound socket nor the internal address may leak: {doc_text}"
    );

    let res = reqwest::Client::new()
        .post(format!("{base}/mcp"))
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
        .unwrap_or_default();
    assert_eq!(
        challenge,
        r#"Bearer resource_metadata="https://latiq.example.com/.well-known/oauth-protected-resource""#
    );
}

#[tokio::test]
async fn auth_metadata_falls_back_to_the_bound_address_when_nothing_is_advertised() {
    let idp = latiq_auth::test_support::TestIdp::start().await;
    let base = serve(idp.auth_config(), None).await;
    let doc: serde_json::Value =
        reqwest::get(format!("{base}/.well-known/oauth-protected-resource"))
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
    assert_eq!(doc["resource"], format!("{base}/mcp"));
}

// ---------------------------------------------------------------------------
// The full agent loop: a real Latiq MCP server driven by the real latiq-client,
// across the network boundary.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn server_client_agent_loop() {
    let ops = build_ops();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    drop(listener);
    let addr = format!("127.0.0.1:{port}").parse().unwrap();

    tokio::spawn(async move {
        serve_mcp(addr, ops, None, None).await.unwrap();
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
