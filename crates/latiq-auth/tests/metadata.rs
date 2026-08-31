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

//! The discovery half of the OAuth handshake: the RFC 9728 document and the
//! `WWW-Authenticate` challenge that points a client at it. Serving them over
//! HTTP belongs to the MCP adapter; this crate only produces the values.
use latiq_auth::metadata::{challenge_header, ProtectedResourceMetadata};

#[test]
fn auth_metadata_document_advertises_every_authorization_server() {
    let doc = ProtectedResourceMetadata::new(
        "http://node-1:51402/mcp",
        &["https://idp.example/realms/latiq".to_string()],
    );
    let json = serde_json::to_value(&doc).expect("serialize");
    assert_eq!(json["resource"], "http://node-1:51402/mcp");
    assert_eq!(
        json["authorization_servers"][0],
        "https://idp.example/realms/latiq"
    );
    assert_eq!(json["bearer_methods_supported"][0], "header");
}

#[test]
fn auth_metadata_preserves_issuer_order_and_exact_strings() {
    // The document must publish the SAME strings the verifier enforces, in the
    // configured order -- a client picking the first entry should get the
    // operator's first-listed IdP. Multi-issuer is the whole reason this field
    // is an array: a workforce IdP for operators and a workload IdP for agents
    // must both be discoverable, so both entries are asserted by exact value.
    let doc = ProtectedResourceMetadata::new(
        "http://node-1:51402/mcp",
        &[
            "https://workforce.example".to_string(),
            "https://workload.example".to_string(),
        ],
    );
    let json = serde_json::to_value(&doc).expect("serialize");
    assert_eq!(
        json["authorization_servers"][0],
        "https://workforce.example"
    );
    assert_eq!(json["authorization_servers"][1], "https://workload.example");
}

#[test]
fn auth_metadata_serializes_with_the_spec_field_names() {
    // RFC 9728 names these fields exactly; a client parses by name, so a rename
    // (or a stray serde rename attribute) silently breaks discovery.
    let doc = ProtectedResourceMetadata::new("http://node-1:51402/mcp", &[]);
    let json = serde_json::to_value(&doc).expect("serialize");
    let obj = json.as_object().expect("object");
    let mut keys: Vec<&str> = obj.keys().map(String::as_str).collect();
    keys.sort_unstable();
    assert_eq!(
        keys,
        vec![
            "authorization_servers",
            "bearer_methods_supported",
            "resource"
        ]
    );
    // Even with no issuers configured the field is present and an empty ARRAY,
    // not null: a client must not have to distinguish "absent" from "none".
    assert_eq!(
        json["authorization_servers"]
            .as_array()
            .expect("array")
            .len(),
        0
    );
}

/// The one test that proves `challenge_header` actually RUNS the value through
/// `encode_quoted` — without it the encoder could be unwired and every unit test
/// in `src/metadata.rs` would still pass. What the encoder does to hostile input
/// (quotes, CR/LF and other controls, non-ASCII) and to ordinary URLs is pinned
/// by those unit tests: `quoted_string_metacharacters_are_encoded`,
/// `controls_and_space_are_encoded`, `non_ascii_is_encoded_per_utf8_byte`,
/// `ordinary_urls_pass_through_unchanged`.
#[test]
fn auth_challenge_points_the_client_at_the_metadata_document() {
    let h = challenge_header("http://node-1:51402/.well-known/oauth-protected-resource");
    assert!(h.starts_with("Bearer "));
    assert!(h.contains(
        r#"resource_metadata="http://node-1:51402/.well-known/oauth-protected-resource""#
    ));
    // The encoder is wired in: a value that must change, changes.
    let hostile = challenge_header("https://idp.example/x\"\r\n");
    assert_eq!(hostile.matches('"').count(), 2, "header value: {hostile}");
    assert!(
        hostile.contains("%22") && hostile.contains("%0D%0A"),
        "header value: {hostile}"
    );
}
