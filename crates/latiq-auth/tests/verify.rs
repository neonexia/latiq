//! Token verification. Every token here is wrong in exactly ONE way, so a
//! rejection can only be attributed to the check under test.
use latiq_auth::test_support::{now_secs, TestIdp};
use latiq_auth::{AuthConfig, AuthError, IssuerConfig, Verifier};
use serde_json::json;

const AUD: &str = "latiq";

/// One issuer, with the JWKS URI given explicitly -- the fixture's issuer is a
/// bare `http://host:port` with no discovery document behind it.
fn config(idp: &TestIdp) -> AuthConfig {
    AuthConfig {
        audience: AUD.to_string(),
        issuers: vec![IssuerConfig {
            issuer: idp.issuer.clone(),
            jwks_uri: Some(idp.jwks_uri.clone()),
        }],
    }
}

fn verifier(idp: &TestIdp) -> Verifier {
    Verifier::new(config(idp)).expect("valid config")
}

/// Assert WHY a token was rejected, not merely that it was. Without this an
/// unreachable fixture -- or any error that fires earlier than the check under
/// test -- turns a rejection test green while the check it names is broken.
#[track_caller]
fn assert_rejected_because(err: &AuthError, needle: &str) {
    let rendered = err.to_string();
    assert!(
        rendered.contains(needle),
        "expected the rejection to mention {needle:?}, got: {rendered}"
    );
}

#[tokio::test]
async fn auth_valid_token_yields_a_verified_identity() {
    let idp = TestIdp::start().await;
    let token = idp.mint("svc-orchestrator", AUD, &idp.issuer, 300);

    let identity = verifier(&idp)
        .verify(&token, Some("agent-7"))
        .await
        .expect("a well-formed token from a configured issuer verifies");

    assert!(identity.verified);
    assert_eq!(identity.subject, "svc-orchestrator");
    assert_eq!(identity.issuer, idp.issuer);
    // The leaf stays claimed even for a verified caller.
    assert_eq!(identity.agent_id, "agent-7");
}

#[tokio::test]
async fn auth_rejects_wrong_audience() {
    // The single most important check: a token minted for another service must
    // not be replayable at us.
    let idp = TestIdp::start().await;
    let token = idp.mint("svc-orchestrator", "some-other-service", &idp.issuer, 300);

    let err = verifier(&idp)
        .verify(&token, None)
        .await
        .expect_err("a token audienced at another service must be rejected");
    assert_rejected_because(&err, "InvalidAudience");
}

#[tokio::test]
async fn auth_rejects_an_unlisted_issuer() {
    let idp = TestIdp::start().await;
    let token = idp.mint("svc-orchestrator", AUD, "https://evil.example", 300);

    let err = verifier(&idp)
        .verify(&token, None)
        .await
        .expect_err("an issuer that is not configured must be rejected");
    assert_rejected_because(&err, "issuer is not configured");
}

#[tokio::test]
async fn auth_rejects_expired_token() {
    let idp = TestIdp::start().await;
    let token = idp.mint("svc-orchestrator", AUD, &idp.issuer, -300);

    let err = verifier(&idp)
        .verify(&token, None)
        .await
        .expect_err("an expired token must be rejected");
    assert_rejected_because(&err, "ExpiredSignature");
}

#[tokio::test]
async fn auth_rejects_foreign_signature() {
    let idp = TestIdp::start().await;
    // Right issuer, right audience, right kid -- signed by a key the IdP does
    // not publish.
    let token = idp.mint_with_foreign_key("svc-orchestrator", AUD, &idp.issuer);

    let err = verifier(&idp)
        .verify(&token, None)
        .await
        .expect_err("a signature from an unpublished key must be rejected");
    assert_rejected_because(&err, "InvalidSignature");
}

#[tokio::test]
async fn auth_rejects_token_without_kid() {
    let idp = TestIdp::start().await;
    let token = idp.mint_with_kid("svc-orchestrator", AUD, &idp.issuer, 300, None);

    verifier(&idp)
        .verify(&token, None)
        .await
        .expect_err("without a kid we cannot pin a key, so the token is unusable");
}

#[tokio::test]
async fn auth_rejects_garbage() {
    let idp = TestIdp::start().await;
    let verifier = verifier(&idp);

    verifier
        .verify("not.a.token", None)
        .await
        .expect_err("a non-JWT string must be rejected");
    verifier
        .verify("", None)
        .await
        .expect_err("an empty token must be rejected");
}

#[tokio::test]
async fn auth_leaf_agent_id_defaults_to_subject() {
    let idp = TestIdp::start().await;
    let token = idp.mint("svc-orchestrator", AUD, &idp.issuer, 300);

    let identity = verifier(&idp).verify(&token, None).await.expect("verifies");

    assert_eq!(identity.agent_id, "svc-orchestrator");
}

#[tokio::test]
async fn auth_rejects_an_empty_subject() {
    // `Identity::verified` guards this only with a debug_assert, which is
    // nothing in release, and an empty subject produces an empty DuckLake
    // commit author.
    let idp = TestIdp::start().await;
    let verifier = verifier(&idp);

    for sub in ["", "   "] {
        let token = idp.mint(sub, AUD, &idp.issuer, 300);
        let err = verifier
            .verify(&token, None)
            .await
            .expect_err("a blank subject must be rejected");
        // WHY, not merely that: a bare `expect_err` here would stay green if the
        // token were rejected for its audience, its signature, or an unreachable
        // fixture -- with the emptiness check itself removed.
        assert_rejected_because(&err, "'sub' is empty");
    }
}

// ---- multi-issuer

#[tokio::test]
async fn auth_accepts_tokens_from_either_configured_issuer() {
    let a = TestIdp::start().await;
    let b = TestIdp::start_alt().await;
    let verifier = Verifier::new(AuthConfig {
        audience: AUD.to_string(),
        issuers: vec![
            IssuerConfig {
                issuer: a.issuer.clone(),
                jwks_uri: Some(a.jwks_uri.clone()),
            },
            IssuerConfig {
                issuer: b.issuer.clone(),
                jwks_uri: Some(b.jwks_uri.clone()),
            },
        ],
    })
    .expect("valid config");

    let from_a = verifier
        .verify(&a.mint("operator-jane", AUD, &a.issuer, 300), None)
        .await
        .expect("workforce token verifies");
    assert_eq!(from_a.issuer, a.issuer);
    assert_eq!(from_a.subject, "operator-jane");

    let from_b = verifier
        .verify(&b.mint("svc-agent", AUD, &b.issuer, 300), None)
        .await
        .expect("workload token verifies");
    assert_eq!(from_b.issuer, b.issuer);
    assert_eq!(from_b.subject, "svc-agent");
}

#[tokio::test]
async fn auth_a_token_cannot_borrow_another_issuers_identity() {
    // This is the test that proves reading the UNVERIFIED `iss` for key
    // selection is safe. Both fixtures publish the same `kid` under DIFFERENT
    // keys, so the lookup succeeds and the rejection can only come from the
    // signature check against issuer a's real key.
    let a = TestIdp::start().await;
    let b = TestIdp::start_alt().await;
    let verifier = Verifier::new(AuthConfig {
        audience: AUD.to_string(),
        issuers: vec![
            IssuerConfig {
                issuer: a.issuer.clone(),
                jwks_uri: Some(a.jwks_uri.clone()),
            },
            IssuerConfig {
                issuer: b.issuer.clone(),
                jwks_uri: Some(b.jwks_uri.clone()),
            },
        ],
    })
    .expect("valid config");

    // Sanity: the kid IS resolvable under issuer a, so a rejection below is not
    // merely an unknown-key miss.
    verifier
        .verify(&a.mint("operator-jane", AUD, &a.issuer, 300), None)
        .await
        .expect("issuer a's own token verifies with this kid");

    // Signed by b, claiming a.
    let borrowed = b.mint("operator-jane", AUD, &a.issuer, 300);
    let err = verifier
        .verify(&borrowed, None)
        .await
        .expect_err("a token signed by b must not pass as issued by a");
    // Specifically the SIGNATURE, not an unknown-kid miss and not an
    // unconfigured-issuer miss: those would pass a looser assertion while
    // leaving the actual cross-issuer confusion untested.
    match &err {
        latiq_auth::AuthError::Invalid(msg) => {
            assert!(
                msg.contains("InvalidSignature"),
                "expected a signature failure against issuer a's key, got {msg}"
            );
        }
        other => panic!("expected a signature rejection, got {other:?}"),
    }
}

// ---- config validation

#[test]
fn auth_rejects_a_plaintext_jwks_uri() {
    // A plaintext JWKS URI is a total auth bypass: anyone on-path substitutes
    // keys and mints arbitrary identities.
    Verifier::new(AuthConfig {
        audience: AUD.to_string(),
        issuers: vec![IssuerConfig {
            issuer: "https://idp.example/realms/latiq".to_string(),
            jwks_uri: Some("http://idp.example/realms/latiq/jwks".to_string()),
        }],
    })
    .expect_err("plaintext jwks over the network must be refused");

    // Also when derived from a plaintext issuer.
    Verifier::new(AuthConfig {
        audience: AUD.to_string(),
        issuers: vec![IssuerConfig {
            issuer: "http://idp.example/realms/latiq".to_string(),
            jwks_uri: None,
        }],
    })
    .expect_err("a derived plaintext jwks uri must be refused too");
}

#[test]
fn auth_allows_plaintext_jwks_on_loopback() {
    // Tests and `./dev.sh --auth` legitimately use http on loopback.
    for uri in [
        "http://127.0.0.1:8080/jwks",
        "http://[::1]:8080/jwks",
        "http://localhost:8080/jwks",
    ] {
        Verifier::new(AuthConfig {
            audience: AUD.to_string(),
            issuers: vec![IssuerConfig {
                issuer: "http://localhost:8080/realms/latiq".to_string(),
                jwks_uri: Some(uri.to_string()),
            }],
        })
        .unwrap_or_else(|e| panic!("loopback plaintext must be allowed for {uri}: {e}"));
    }
}

#[test]
fn auth_rejects_a_degenerate_config() {
    let issuer = || IssuerConfig {
        issuer: "https://idp.example".to_string(),
        jwks_uri: None,
    };

    Verifier::new(AuthConfig {
        audience: String::new(),
        issuers: vec![issuer()],
    })
    .expect_err("an empty audience means nothing pins the token to us");

    Verifier::new(AuthConfig {
        audience: AUD.to_string(),
        issuers: vec![],
    })
    .expect_err("auth with no issuers can never succeed");

    Verifier::new(AuthConfig {
        audience: AUD.to_string(),
        issuers: vec![issuer(), issuer()],
    })
    .expect_err("a duplicate issuer entry is a misconfiguration");
}

#[test]
fn auth_config_is_readable_back() {
    let v = Verifier::new(AuthConfig {
        audience: AUD.to_string(),
        issuers: vec![IssuerConfig {
            issuer: "https://idp.example".to_string(),
            jwks_uri: None,
        }],
    })
    .expect("valid config");
    assert_eq!(v.config().audience, AUD);
    assert_eq!(v.config().issuers.len(), 1);
}

// ---- the algorithm allowlist
//
// The single most important check in the verifier: everything else assumes the
// signature was checked with an algorithm we chose, not one the caller did.

#[tokio::test]
async fn auth_rejects_a_symmetric_algorithm() {
    let idp = TestIdp::start().await;
    let token = idp.mint_hs256("svc-orchestrator", AUD, &idp.issuer, b"whatever-secret");

    let err = verifier(&idp)
        .verify(&token, None)
        .await
        .expect_err("HS256 must never be accepted by a resource server");
    assert_rejected_because(&err, "HS256");
}

#[tokio::test]
async fn auth_rejects_hs256_signed_with_the_rsa_public_key() {
    // The classic algorithm-confusion attack: hand the verifier's own PUBLIC key
    // back as an HMAC secret. It only works if the verifier takes `alg` from the
    // token header, so a rejection here is the allowlist doing its job -- and
    // the "secret" the attacker used is published in the JWKS for anyone to read.
    let idp = TestIdp::start().await;
    let token = idp.mint_hs256("svc-orchestrator", AUD, &idp.issuer, &idp.public_modulus());

    let err = verifier(&idp)
        .verify(&token, None)
        .await
        .expect_err("a public key used as an HMAC secret must be rejected");
    assert_rejected_because(&err, "HS256");
}

#[tokio::test]
async fn auth_rejects_alg_none() {
    // An unsigned token, which is a valid JWT and a total bypass if honoured.
    let idp = TestIdp::start().await;
    let token = idp.mint_alg_none("svc-orchestrator", AUD, &idp.issuer);

    let err = verifier(&idp)
        .verify(&token, None)
        .await
        .expect_err("an unsigned token must be rejected");
    // Rejected before the allowlist even runs: `jsonwebtoken` cannot represent
    // `none`, so the header does not deserialize. Belt and braces, but the
    // assertion pins WHICH belt.
    assert!(
        matches!(err, AuthError::Malformed(_)),
        "expected a malformed-header rejection, got {err:?}"
    );
}

// ---- claims

#[tokio::test]
async fn auth_accepts_an_array_audience() {
    // Keycloak's default shape. If this ever regresses, every real token fails.
    let idp = TestIdp::start().await;
    let now = now_secs();
    let token = idp.mint_claims(json!({
        "sub": "svc-orchestrator",
        "aud": ["account", AUD, "some-other-service"],
        "iss": idp.issuer,
        "iat": now,
        "exp": now + 300,
    }));

    let identity = verifier(&idp)
        .verify(&token, None)
        .await
        .expect("an aud array containing our audience must verify");
    assert_eq!(identity.subject, "svc-orchestrator");
}

#[tokio::test]
async fn auth_rejects_an_array_audience_without_us() {
    let idp = TestIdp::start().await;
    let now = now_secs();
    let token = idp.mint_claims(json!({
        "sub": "svc-orchestrator",
        "aud": ["account", "some-other-service"],
        "iss": idp.issuer,
        "iat": now,
        "exp": now + 300,
    }));

    let err = verifier(&idp)
        .verify(&token, None)
        .await
        .expect_err("an aud array that does not name us must be rejected");
    assert_rejected_because(&err, "InvalidAudience");
}

#[tokio::test]
async fn auth_rejects_a_token_with_no_subject_claim() {
    // Distinct from a blank `sub`: this one is absent entirely, and is caught by
    // the required-claims set rather than by our own emptiness check.
    let idp = TestIdp::start().await;
    let now = now_secs();
    let token = idp.mint_claims(json!({
        "aud": AUD,
        "iss": idp.issuer,
        "iat": now,
        "exp": now + 300,
    }));

    let err = verifier(&idp)
        .verify(&token, None)
        .await
        .expect_err("a token with no sub has no identity to attribute");
    assert_rejected_because(&err, "Missing required claim: sub");
}

#[tokio::test]
async fn auth_rejects_a_not_yet_valid_token() {
    // RFC 7519 4.1.5: a token MUST NOT be accepted before its `nbf`. Off by
    // default in jsonwebtoken, so without an explicit opt-in this token -- issued
    // for an hour from now -- would be usable an hour early.
    let idp = TestIdp::start().await;
    let now = now_secs();
    let token = idp.mint_claims(json!({
        "sub": "svc-orchestrator",
        "aud": AUD,
        "iss": idp.issuer,
        "iat": now,
        "nbf": now + 3600,
        "exp": now + 7200,
    }));

    let err = verifier(&idp)
        .verify(&token, None)
        .await
        .expect_err("a token that is not yet valid must be rejected");
    assert_rejected_because(&err, "ImmatureSignature");
}

#[tokio::test]
async fn auth_still_accepts_a_token_with_a_past_nbf() {
    // Validating `nbf` must not break the ordinary case: `nbf` stays optional,
    // and a past one is simply satisfied.
    let idp = TestIdp::start().await;
    let now = now_secs();
    let token = idp.mint_claims(json!({
        "sub": "svc-orchestrator",
        "aud": AUD,
        "iss": idp.issuer,
        "iat": now,
        "nbf": now - 60,
        "exp": now + 300,
    }));

    verifier(&idp)
        .verify(&token, None)
        .await
        .expect("a token whose nbf has passed must verify");
}

#[tokio::test]
async fn auth_rejects_an_oversize_token() {
    // `verify()` is protocol-neutral, so it must not rely on the calling
    // transport's header cap to bound this unauthenticated path.
    let idp = TestIdp::start().await;
    let huge = "a".repeat(64 * 1024);

    let err = verifier(&idp)
        .verify(&huge, None)
        .await
        .expect_err("an oversize token must be refused before it is parsed");
    assert_rejected_because(&err, "over the");
}

// ---- config normalization

#[test]
fn auth_config_is_stored_normalized() {
    // A padded audience previously passed validation and then rejected every
    // token with an undiagnosable InvalidAudience. What is stored is what is
    // enforced -- and what the metadata document will later publish.
    let v = Verifier::new(AuthConfig {
        audience: "  latiq  ".to_string(),
        issuers: vec![IssuerConfig {
            issuer: "  https://idp.example  ".to_string(),
            jwks_uri: None,
        }],
    })
    .expect("padding is normalized, not rejected");

    assert_eq!(v.config().audience, "latiq");
    assert_eq!(v.config().issuers[0].issuer, "https://idp.example");
    // The derived URI is resolved into the stored config rather than left None.
    assert_eq!(
        v.config().issuers[0].jwks_uri.as_deref(),
        Some("https://idp.example/protocol/openid-connect/certs")
    );
}

#[tokio::test]
async fn auth_a_padded_audience_still_verifies_tokens() {
    let idp = TestIdp::start().await;
    let mut cfg = config(&idp);
    cfg.audience = format!("  {AUD}  ");
    let verifier = Verifier::new(cfg).expect("valid config");

    verifier
        .verify(&idp.mint("svc-orchestrator", AUD, &idp.issuer, 300), None)
        .await
        .expect("the padding must not leak into the audience comparison");
}

#[test]
fn auth_rejects_a_jwks_uri_that_smuggles_a_loopback_host() {
    // A backslash is a path separator to the WHATWG parser, so the host is
    // evil.com and
    // the guard must refuse the plaintext fetch.
    Verifier::new(AuthConfig {
        audience: AUD.to_string(),
        issuers: vec![IssuerConfig {
            issuer: "https://idp.example".to_string(),
            jwks_uri: Some("http://evil.com\\@127.0.0.1/jwks".to_string()),
        }],
    })
    .expect_err("a backslash must not smuggle a loopback host past the tls guard");
}

// ---- what the JWKS itself says about a key

/// The fixture's JWKS document with one member of its single key overridden.
fn doctored_jwks(field: &str, value: serde_json::Value) -> String {
    let mut doc: serde_json::Value = serde_json::from_str(
        &latiq_auth::test_support::jwks_document(latiq_auth::test_support::KID),
    )
    .expect("fixture jwks parses");
    doc["keys"][0][field] = value;
    doc.to_string()
}

#[tokio::test]
async fn auth_rejects_a_token_signed_by_an_encryption_key() {
    // An IdP routinely publishes encryption keys beside signing keys. Importing
    // one would verify tokens against a key its issuer never meant to sign with.
    let idp = TestIdp::start().await;
    idp.set_jwks_body(doctored_jwks("use", json!("enc"))).await;
    let token = idp.mint("svc-orchestrator", AUD, &idp.issuer, 300);

    let err = verifier(&idp)
        .verify(&token, None)
        .await
        .expect_err("a key marked use=enc must not be usable for signatures");
    assert!(
        matches!(err, AuthError::UnknownKid(_)),
        "the enc key should never have been imported, got {err:?}"
    );
}

#[tokio::test]
async fn auth_rejects_a_token_weaker_than_the_key_declares() {
    // The key is published as RS512; the token is RS256. Pinning only the
    // token's alg would quietly let the caller downgrade the issuer's policy.
    let idp = TestIdp::start().await;
    idp.set_jwks_body(doctored_jwks("alg", json!("RS512")))
        .await;
    let token = idp.mint("svc-orchestrator", AUD, &idp.issuer, 300);

    let err = verifier(&idp)
        .verify(&token, None)
        .await
        .expect_err("the algorithm the key was published for must be enforced");
    assert_rejected_because(&err, "does not match the RS512 declared");
}

#[tokio::test]
async fn auth_accepts_a_key_that_declares_no_algorithm() {
    // `alg` is optional in a JWK; without it the key is unconstrained and only
    // our own allowlist applies.
    let idp = TestIdp::start().await;
    idp.set_jwks_body(doctored_jwks("alg", serde_json::Value::Null))
        .await;
    let token = idp.mint("svc-orchestrator", AUD, &idp.issuer, 300);

    verifier(&idp)
        .verify(&token, None)
        .await
        .expect("a key with no declared alg must still verify");
}
