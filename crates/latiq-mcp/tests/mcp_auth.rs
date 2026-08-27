//! The MCP surface's OAuth 2.1 resource-server metadata, at the crate boundary.
//!
//! Lives here rather than in the full-stack suite because the property under
//! test is precisely that the ADVERTISED url wins over the BOUND one — and the
//! full-stack harness binds loopback, where the two coincide and any confusion
//! between them passes.
use latiq_agent_core::{AgentConfig, AgentOps, RegistryControlPlane};
use latiq_control_plane::Registry;
use latiq_engine_duckdb::DuckEngine;
use latiq_mcp::serve_mcp_with_listener;
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
