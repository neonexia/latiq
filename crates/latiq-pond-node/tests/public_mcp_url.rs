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

//! The node's auth-discovery strings, across BOTH surfaces it serves.
//!
//! The property under test is that the MCP surface and the Data/Stream gRPC
//! surface publish ONE resolved public URL: the RFC 9728 document the gRPC
//! challenge points at is the document MCP serves, and its `resource` is the
//! URL clients dial (the gateway's, not this node's). Two challenges naming
//! different documents would send a client somewhere that does not exist.
use latiq_agent_core::{AgentConfig, AgentOps, RegistryControlPlane};
use latiq_control_plane::Registry;
use latiq_engine_duckdb::DuckEngine;
use latiq_mcp::{protected_resource_metadata_url, resolve_public_mcp_url, serve_mcp_with_listener};
use latiq_pond_node::serve_data;
use latiq_proto::v1::data_client::DataClient;
use latiq_proto::v1::DescribePondRequest;
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

#[tokio::test]
async fn auth_both_surfaces_publish_the_configured_public_mcp_url() {
    let idp = latiq_auth::test_support::TestIdp::start().await;
    let verifier = Arc::new(latiq_auth::Verifier::new(idp.auth_config()).expect("build verifier"));

    // What `run_pond_node` does: resolve ONCE, then hand the same value to the
    // MCP surface and derive the Data/Stream challenge from it. The node
    // advertises its own internal name for peer forwarding; agents dial the
    // gateway, so the configured URL is what gets published.
    let mcp_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mcp_bound = mcp_listener.local_addr().unwrap();
    let public = resolve_public_mcp_url(
        Some("https://latiq.example.com/mcp"),
        "http://pond-node-1:51401",
        mcp_bound,
    )
    .expect("a well-formed public url resolves");
    let data_metadata_url = protected_resource_metadata_url(&public);

    let base = format!("http://{mcp_bound}");
    {
        let (v, p) = (verifier.clone(), public.clone());
        tokio::spawn(async move {
            serve_mcp_with_listener(mcp_listener, build_ops(), Some(v), Some(p))
                .await
                .unwrap();
        });
    }
    let data_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let data_addr = data_listener.local_addr().unwrap();
    drop(data_listener); // serve_data binds itself; we only wanted a free port.
    {
        let (v, m) = (verifier.clone(), data_metadata_url.clone());
        tokio::spawn(async move {
            serve_data(data_addr, build_ops(), Some(v), Some(m))
                .await
                .unwrap();
        });
    }

    // Wait for the MCP accept loop rather than sleeping blind.
    for _ in 0..100 {
        if reqwest::get(format!("{base}/.well-known/oauth-protected-resource"))
            .await
            .is_ok()
        {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }

    // 1. The document MCP serves names the public URL as the resource.
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
        !doc_text.contains(&mcp_bound.to_string()),
        "the bound socket must not leak into the document: {doc_text}"
    );

    // 2. The MCP challenge points at that document.
    let res = reqwest::Client::new()
        .post(format!("{base}/mcp"))
        .header("content-type", "application/json")
        .body("{}")
        .send()
        .await
        .unwrap();
    assert_eq!(res.status().as_u16(), 401);
    let mcp_challenge = res
        .headers()
        .get("www-authenticate")
        .and_then(|v| v.to_str().ok())
        .unwrap_or_default()
        .to_string();

    // 3. The Data/Stream challenge is the SAME string — one document, two
    //    surfaces. A client turned away on gRPC and one turned away on MCP are
    //    sent to the same place.
    let mut client = None;
    for _ in 0..100 {
        if let Ok(c) = DataClient::connect(format!("http://{data_addr}")).await {
            client = Some(c);
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
    let err = client
        .expect("data surface accepts connections")
        .describe_pond(DescribePondRequest {
            pond: "nope".into(),
        })
        .await
        .expect_err("no token: the call must be rejected");
    let grpc_challenge = err
        .metadata()
        .get("www-authenticate")
        .expect("a rejection must advertise where to get a token")
        .to_str()
        .unwrap()
        .to_string();

    assert_eq!(grpc_challenge, mcp_challenge);
    assert!(
        grpc_challenge.contains("https://latiq.example.com/.well-known/oauth-protected-resource"),
        "got {grpc_challenge}"
    );
    for challenge in [&mcp_challenge, &grpc_challenge] {
        assert!(
            !challenge.contains("127.0.0.1") && !challenge.contains("pond-node-1"),
            "neither the bound address nor the internal one may leak: {challenge}"
        );
    }
}
