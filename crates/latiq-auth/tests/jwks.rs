use latiq_auth::jwks::JwksCache;
use latiq_auth::test_support::{TestIdp, KID};

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
async fn auth_jwks_refetches_once_on_unknown_kid() {
    let idp = TestIdp::start().await;
    let cache = JwksCache::new(idp.jwks_uri.clone());

    assert!(cache.key_for(KID).await.is_ok());
    // An unknown kid triggers exactly one refresh, then fails -- it must not
    // hammer the IdP on every request with a bogus kid.
    assert!(cache.key_for("rotated-key").await.is_err());
    assert_eq!(cache.fetch_count(), 2);
}

#[tokio::test]
async fn auth_jwks_unreachable_endpoint_is_an_error_not_a_panic() {
    let cache = JwksCache::new("http://127.0.0.1:1/jwks".to_string());
    assert!(cache.key_for(KID).await.is_err());
}
