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
fn auth_metadata_carries_all_issuers_not_just_the_first() {
    // Multi-issuer is the whole reason this field is an array: a workforce IdP
    // for operators and a workload IdP for agents must both be discoverable.
    let doc = ProtectedResourceMetadata::new(
        "http://node-1:51402/mcp",
        &[
            "https://workforce.example".to_string(),
            "https://workload.example".to_string(),
        ],
    );
    let json = serde_json::to_value(&doc).expect("serialize");
    assert_eq!(
        json["authorization_servers"]
            .as_array()
            .expect("array")
            .len(),
        2
    );
}

#[test]
fn auth_metadata_preserves_issuer_order_and_exact_strings() {
    // The document must publish the SAME strings the verifier enforces, in the
    // configured order -- a client picking the first entry should get the
    // operator's first-listed IdP.
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

#[test]
fn auth_challenge_points_the_client_at_the_metadata_document() {
    let h = challenge_header("http://node-1:51402/.well-known/oauth-protected-resource");
    assert!(h.starts_with("Bearer "));
    assert!(h.contains(
        r#"resource_metadata="http://node-1:51402/.well-known/oauth-protected-resource""#
    ));
}

#[test]
fn auth_challenge_escapes_a_quote_that_would_close_the_parameter_early() {
    // A bare `"` in the URL would terminate the quoted-string and let whatever
    // follows be read as further auth-params. Percent-encoding keeps the value
    // one parameter (and a URL is allowed to carry `%22` for a literal quote).
    let h = challenge_header(r#"https://idp.example/x"?evil="1"#);
    assert_eq!(h.matches('"').count(), 2, "header value: {h}");
    assert!(h.contains("%22"), "header value: {h}");
}

#[test]
fn auth_challenge_strips_control_characters_that_would_split_the_response() {
    // CR/LF in a header value is response splitting: everything after the CRLF
    // is read by the client as a new header (or a new response body).
    let h = challenge_header("https://idp.example/x\r\nX-Injected: yes");
    assert!(!h.contains('\r'), "header value: {h}");
    assert!(!h.contains('\n'), "header value: {h}");
    assert!(!h.contains("X-Injected: yes"), "header value: {h}");
    // NUL and other C0 controls are rejected by HTTP header codecs outright,
    // which would turn a misconfiguration into a 500 instead of a 401.
    let h = challenge_header("https://idp.example/x\0\u{7f}y");
    assert!(!h.contains('\0'), "header value: {h}");
    assert!(h.chars().all(|c| !c.is_control()), "header value: {h}");
}

#[test]
fn auth_challenge_leaves_ordinary_url_punctuation_alone() {
    // The encoding must not mangle a legitimate URL: a client compares the
    // advertised URL against what it fetches.
    let h =
        challenge_header("https://idp.example:8443/.well-known/oauth-protected-resource?a=b&c=d");
    assert!(
        h.contains(
            r#"resource_metadata="https://idp.example:8443/.well-known/oauth-protected-resource?a=b&c=d""#
        ),
        "header value: {h}"
    );
}

#[test]
fn auth_challenge_encodes_non_ascii_rather_than_emitting_raw_bytes() {
    // Header values are opaque bytes, but a non-ASCII one is read differently by
    // different clients. Anything outside printable ASCII gets percent-encoded.
    let h = challenge_header("https://idp.example/café");
    assert!(h.is_ascii(), "header value: {h}");
}
