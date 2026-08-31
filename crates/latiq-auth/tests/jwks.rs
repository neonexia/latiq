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
    assert_eq!(
        cache.fetch_count(),
        1,
        "50 bogus kids must not drive more than the one warm-up fetch"
    );
}

#[tokio::test]
async fn auth_jwks_cold_start_burst_all_succeed_on_one_fetch() {
    let idp = TestIdp::start().await;
    // Production settings, cold cache: the ordinary shape of a node starting
    // up or of a rotation window, NOT an attack. Every one of these requests
    // carries a valid, currently-published kid.
    let cache = Arc::new(JwksCache::new(idp.jwks_uri.clone()));

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
    let ok = futures_count_ok(handles).await;

    // Requests arriving while the first fetch is in flight must queue on it and
    // ride its result. Stamping the floor on ENTRY instead of completion made
    // them skip the guard and get rejected: 2 of 16 succeeded.
    assert_eq!(ok, 16, "cold start rejected {} of 16 valid tokens", 16 - ok);
    assert_eq!(cache.fetch_count(), 1);
}

async fn futures_count_ok(handles: Vec<tokio::task::JoinHandle<bool>>) -> usize {
    let mut ok = 0;
    for handle in handles {
        if handle.await.expect("task panicked") {
            ok += 1;
        }
    }
    ok
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
    // the IdP.
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
    assert_eq!(
        cache.fetch_count(),
        1,
        "16 concurrent misses must collapse into one fetch"
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
async fn auth_jwks_picks_up_a_rotated_key_once_the_interval_elapses() {
    let idp = TestIdp::start().await;
    let cache =
        JwksCache::with_min_refresh_interval(idp.jwks_uri.clone(), Duration::from_millis(300));

    assert!(cache.key_for(KID).await.is_ok());
    idp.rotate("test-key-2").await;
    // Suppressed inside the floor...
    assert!(cache.key_for("test-key-2").await.is_err());
    assert_eq!(cache.fetch_count(), 1);

    tokio::time::sleep(Duration::from_millis(400)).await;

    // ...and picked up once it elapses. This is the half of the bargain the
    // floor's doc comment promises; the deferral test only proves the other.
    assert!(cache.key_for("test-key-2").await.is_ok());
    assert_eq!(cache.fetch_count(), 2);
}

#[tokio::test]
async fn auth_jwks_recovers_from_a_transient_failure_without_a_long_lockout() {
    let idp = TestIdp::start().await;
    idp.set_status(503).await;
    let cache =
        JwksCache::new(idp.jwks_uri.clone()).with_failure_backoff_base(Duration::from_millis(200));

    assert!(cache.key_for(KID).await.is_err());
    idp.set_status(200).await;

    // A blip must not cost a full success-interval lockout: the post-failure
    // floor is separate and far shorter.
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert!(cache.key_for(KID).await.is_ok());
    assert_eq!(cache.fetch_count(), 2);
}

#[tokio::test]
async fn auth_jwks_edge_suppressed_after_failure_blames_the_idp_not_the_token() {
    let idp = TestIdp::start().await;
    idp.set_status(503).await;
    let cache = JwksCache::new(idp.jwks_uri.clone());

    let first = cache.key_for(KID).await.err().expect("must fail");
    assert!(first.to_string().contains("identity provider"), "{first}");

    // Inside the post-failure floor we cannot ask, so we do not KNOW the key is
    // absent. Reporting "unknown signing key" here blames the caller's token
    // for the IdP's outage and sends the operator hunting in the wrong place.
    for _ in 0..5 {
        let err = cache.key_for(KID).await.err().expect("must fail");
        let message = err.to_string();
        assert!(
            message.contains("identity provider"),
            "suppressed refresh blamed the token: {message}"
        );
        assert!(!message.contains("signing key"), "{message}");
    }
    assert_eq!(cache.fetch_count(), 1, "suppression must still hold");
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
async fn auth_jwks_edge_a_key_management_alg_is_not_imported_as_a_signing_key() {
    // The `use: "enc"` skip only fires when `use` is present, and IdPs routinely
    // omit it. A key declaring a KEY-MANAGEMENT alg (RSA-OAEP) is an encryption
    // key just as plainly, and importing it would leave it UNCONSTRAINED --
    // usable to verify a signature its issuer never meant it to make. Defence in
    // depth (an attacker still needs the private key), but skipping is strictly
    // safer than importing with no algorithm pinned.
    let idp = TestIdp::start().await;
    let good: serde_json::Value =
        serde_json::from_str(&jwks_document(KID)).expect("good jwks parses");
    // Same real RSA material as the good key, so nothing but the declared `alg`
    // distinguishes them: `from_jwk` accepts it, which is exactly the problem.
    let mut enc = good["keys"][0].clone();
    enc["kid"] = serde_json::json!("enc-key");
    enc["alg"] = serde_json::json!("RSA-OAEP");
    // No `use` at all -- the case the `use: "enc"` skip cannot catch.
    enc.as_object_mut().expect("jwk object").remove("use");
    idp.set_jwks_body(serde_json::json!({ "keys": [enc, good["keys"][0]] }).to_string())
        .await;

    let cache = eager_cache(&idp);
    assert!(
        cache.key_for(KID).await.is_ok(),
        "one skipped key must not poison the set"
    );
    assert!(
        cache.key_for("enc-key").await.is_err(),
        "a key-management alg must not be importable as a signing key"
    );
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
async fn auth_jwks_edge_oversize_is_rejected_without_a_content_length() {
    let idp = TestIdp::start().await;
    idp.set_jwks_body(format!(
        "{{\"keys\":[],\"pad\":\"{}\"}}",
        "p".repeat(300 * 1024)
    ))
    .await;

    // The cheap pre-check on the advertised length cannot fire here, so this is
    // the only test that reaches the cap enforced while reading -- the branch
    // that exists precisely because a bodiless-length response is otherwise
    // unbounded.
    assert_eq!(
        latiq_auth::test_support::advertised_content_length(&idp.jwks_stream_uri).await,
        None,
        "the streaming route must not advertise a length, or this proves nothing"
    );

    let cache = JwksCache::with_min_refresh_interval(idp.jwks_stream_uri.clone(), Duration::ZERO);
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
