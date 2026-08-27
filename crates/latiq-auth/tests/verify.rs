//! Token verification. Every token here is wrong in exactly ONE way, so a
//! rejection can only be attributed to the check under test.
use latiq_auth::test_support::TestIdp;
use latiq_auth::{AuthConfig, IssuerConfig, Verifier};

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

    verifier(&idp)
        .verify(&token, None)
        .await
        .expect_err("a token audienced at another service must be rejected");
}

#[tokio::test]
async fn auth_rejects_an_unlisted_issuer() {
    let idp = TestIdp::start().await;
    let token = idp.mint("svc-orchestrator", AUD, "https://evil.example", 300);

    verifier(&idp)
        .verify(&token, None)
        .await
        .expect_err("an issuer that is not configured must be rejected");
}

#[tokio::test]
async fn auth_rejects_expired_token() {
    let idp = TestIdp::start().await;
    let token = idp.mint("svc-orchestrator", AUD, &idp.issuer, -300);

    verifier(&idp)
        .verify(&token, None)
        .await
        .expect_err("an expired token must be rejected");
}

#[tokio::test]
async fn auth_rejects_foreign_signature() {
    let idp = TestIdp::start().await;
    // Right issuer, right audience, right kid -- signed by a key the IdP does
    // not publish.
    let token = idp.mint_with_foreign_key("svc-orchestrator", AUD, &idp.issuer);

    verifier(&idp)
        .verify(&token, None)
        .await
        .expect_err("a signature from an unpublished key must be rejected");
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
        verifier
            .verify(&token, None)
            .await
            .expect_err("a blank subject must be rejected");
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
