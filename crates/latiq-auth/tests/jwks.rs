use latiq_auth::jwks::JwksCache;
use latiq_auth::test_support::{jwks_document, TestIdp, KID};
use std::sync::Arc;
use std::time::Duration;

/// Most tests want rotation behaviour without waiting out the production
/// interval floor. Tests that are ABOUT the floor use `JwksCache::new`.
fn eager_cache(idp: &TestIdp) -> JwksCache {
    JwksCache::with_min_refresh_interval(idp.jwks_uri.clone(), Duration::ZERO)
}

#[tokio::test]
async fn auth_jwks_fetches_and_caches_by_kid() {
    let idp = TestIdp::start().await;
    let cache = JwksCache::new(idp.jwks_uri.clone());

    assert!(cache.key_for(KID).await.is_ok());
    // Second call must not re-fetch.
    assert!(cache.key_for(KID).await.is_ok());
    assert_eq!(cache.fetch_count(), 1);
}

#[tokio::test]
async fn auth_jwks_unknown_kids_do_not_amplify_to_the_idp() {
    let idp = TestIdp::start().await;
    let cache = JwksCache::new(idp.jwks_uri.clone());

    assert!(cache.key_for(KID).await.is_ok());
    // A flood of tokens bearing bogus kids is the cheapest unauthenticated
    // attack there is -- kid selects the key, so it is reached before any
    // signature check. It must cost the customer's IdP essentially nothing.
    for i in 0..50 {
        assert!(cache.key_for(&format!("bogus-kid-{i}")).await.is_err());
    }
    assert!(
        cache.fetch_count() <= 2,
        "50 bogus kids drove {} fetches",
        cache.fetch_count()
    );
}

#[tokio::test]
async fn auth_jwks_concurrent_misses_collapse_into_one_fetch() {
    let idp = TestIdp::start().await;
    let cache = Arc::new(eager_cache(&idp));

    // Even with no interval floor, the single-flight guard must keep a
    // simultaneous burst down to one fetch.
    let gate = Arc::new(tokio::sync::Barrier::new(16));
    let mut handles = Vec::new();
    for _ in 0..16 {
        let cache = cache.clone();
        let gate = gate.clone();
        handles.push(tokio::spawn(async move {
            gate.wait().await;
            cache.key_for(KID).await.is_ok()
        }));
    }
    for handle in handles {
        assert!(handle.await.expect("task panicked"));
    }
    assert_eq!(cache.fetch_count(), 1);
}

#[tokio::test]
async fn auth_jwks_concurrent_misses_on_an_unpublished_kid_collapse_too() {
    let idp = TestIdp::start().await;
    let cache = Arc::new(eager_cache(&idp));

    // The hostile shape of the previous test: the kid is never published, so a
    // guard that only re-checks the map for a hit would let all 16 stampede
    // the IdP. Bounded rather than exact: with the floor set to zero, a task
    // that starts after the first fetch has already finished is a new,
    // legitimate refresh, not a stampede. Production's 60s floor collapses
    // those too -- that is what the amplification test pins.
    let gate = Arc::new(tokio::sync::Barrier::new(16));
    let mut handles = Vec::new();
    for _ in 0..16 {
        let cache = cache.clone();
        let gate = gate.clone();
        handles.push(tokio::spawn(async move {
            gate.wait().await;
            cache.key_for("never-published").await.is_err()
        }));
    }
    for handle in handles {
        assert!(handle.await.expect("task panicked"));
    }
    assert!(
        cache.fetch_count() <= 2,
        "16 concurrent misses drove {} fetches",
        cache.fetch_count()
    );
}

#[tokio::test]
async fn auth_jwks_picks_up_a_rotated_key() {
    let idp = TestIdp::start().await;
    let cache = eager_cache(&idp);

    assert!(cache.key_for(KID).await.is_ok());
    idp.rotate("test-key-2").await;

    // The whole point of refetching on a miss: a genuine rotation resolves.
    assert!(cache.key_for("test-key-2").await.is_ok());
    assert_eq!(cache.fetch_count(), 2);
    // ...and the retired kid is gone, because the map is replaced wholesale.
    assert!(cache.key_for(KID).await.is_err());
}

#[tokio::test]
async fn auth_jwks_min_interval_defers_a_rotation_rather_than_refetching() {
    let idp = TestIdp::start().await;
    let cache = JwksCache::new(idp.jwks_uri.clone());

    assert!(cache.key_for(KID).await.is_ok());
    idp.rotate("test-key-2").await;
    // Inside the floor the new key is simply not visible yet. That is the
    // trade: bounded IdP traffic, rotation picked up within the interval.
    assert!(cache.key_for("test-key-2").await.is_err());
    assert_eq!(cache.fetch_count(), 1);
}

#[tokio::test]
async fn auth_jwks_edge_one_unusable_key_does_not_poison_the_set() {
    let idp = TestIdp::start().await;
    // A key with no kid (unaddressable) and a key from_jwk rejects (its
    // modulus is not base64), either side of the good one.
    let good: serde_json::Value =
        serde_json::from_str(&jwks_document(KID)).expect("good jwks parses");
    let body = serde_json::json!({
        "keys": [
            { "kty": "RSA", "use": "sig", "alg": "RS256", "n": "AQAB", "e": "AQAB" },
            good["keys"][0],
            { "kty": "RSA", "use": "sig", "alg": "RS256", "kid": "broken",
              "n": "!!! not base64 !!!", "e": "AQAB" },
        ]
    })
    .to_string();
    idp.set_jwks_body(body).await;

    let cache = eager_cache(&idp);
    assert!(cache.key_for(KID).await.is_ok(), "good key must survive");
    assert!(cache.key_for("broken").await.is_err());
}

#[tokio::test]
async fn auth_jwks_edge_non_success_status_is_an_error() {
    let idp = TestIdp::start().await;
    idp.set_status(503).await;

    let cache = eager_cache(&idp);
    assert!(cache.key_for(KID).await.is_err());
}

#[tokio::test]
async fn auth_jwks_edge_malformed_body_is_an_error() {
    let idp = TestIdp::start().await;
    idp.set_jwks_body("<html>definitely not a jwks</html>".to_string())
        .await;

    let cache = eager_cache(&idp);
    assert!(cache.key_for(KID).await.is_err());
}

#[tokio::test]
async fn auth_jwks_edge_oversize_document_is_rejected() {
    let idp = TestIdp::start().await;
    idp.set_jwks_body(format!(
        "{{\"keys\":[],\"pad\":\"{}\"}}",
        "p".repeat(300 * 1024)
    ))
    .await;

    let cache = eager_cache(&idp);
    let err = cache
        .key_for(KID)
        .await
        .err()
        .expect("oversize must be an error");
    assert!(
        err.to_string().contains("too large"),
        "unexpected error: {err}"
    );
}

#[tokio::test]
async fn auth_jwks_unreachable_endpoint_is_an_error_not_a_panic() {
    let cache = JwksCache::new("http://127.0.0.1:1/jwks".to_string());
    assert!(cache.key_for(KID).await.is_err());
}

#[tokio::test]
async fn auth_jwks_errors_do_not_leak_the_endpoint() {
    // These messages reach an unauthenticated caller once this is wired to a
    // surface; the URI routinely names an internal host.
    let cache = JwksCache::new("http://127.0.0.1:1/internal-idp/jwks".to_string());
    let err = cache.key_for(KID).await.err().expect("must fail");
    let message = err.to_string();
    assert!(!message.contains("127.0.0.1"), "leaked: {message}");
    assert!(!message.contains("internal-idp"), "leaked: {message}");
}

#[tokio::test]
async fn auth_jwks_edge_unknown_kid_message_is_bounded_and_sanitized() {
    let idp = TestIdp::start().await;
    let cache = eager_cache(&idp);

    // kid is attacker-controlled and unbounded: a log-injection and log-volume
    // vector if echoed raw.
    let hostile = format!("{}{}", "A".repeat(4096), "\ninjected log line");
    let err = cache.key_for(&hostile).await.err().expect("must fail");
    let message = err.to_string();
    assert!(!message.contains('\n'), "unescaped newline: {message}");
    assert!(message.len() < 200, "unbounded message: {}", message.len());
}
