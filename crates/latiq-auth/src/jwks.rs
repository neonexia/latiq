//! JWKS fetch + cache. Verification is offline after the first fetch: no IdP
//! round-trip on the request path, because this sits in front of every query.
//!
//! `kid` selects the key, so the cache lookup is the FIRST thing an
//! unauthenticated caller reaches -- before any signature is checked. A naive
//! refetch-on-miss therefore hands an attacker one outbound request to the
//! customer's IdP per attacker request. Refetching is bounded three ways: a
//! minimum interval between fetches, a single-flight guard so concurrent misses
//! collapse into one fetch, and hard timeouts + a body cap on the fetch itself.
use crate::AuthError;
use jsonwebtoken::jwk::{JwkSet, PublicKeyUse};
use jsonwebtoken::{Algorithm, DecodingKey};
use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Mutex as StdMutex;
use std::time::{Duration, Instant};
use tokio::sync::{Mutex, RwLock};

/// How long after a SUCCESSFUL fetch we refuse to fetch again. Caps IdP traffic
/// at ~1 req/min regardless of attacker volume, while still picking up a genuine
/// key rotation within a minute.
pub const DEFAULT_MIN_REFRESH_INTERVAL: Duration = Duration::from_secs(60);

/// The floor after a FAILED fetch, doubling per consecutive failure up to the
/// success interval. Retrying a down IdP once per request is the same
/// amplification aimed at a sick endpoint; but a transient blip must not lock
/// auth out for a full minute, so failures start far shorter than successes.
pub const DEFAULT_FAILURE_BACKOFF_BASE: Duration = Duration::from_secs(1);

/// Real JWKS documents are single-digit KiB. This is the guard against a
/// `jwks_uri` misconfigured to point at a log file or an object-store listing.
const MAX_JWKS_BYTES: usize = 256 * 1024;

/// The one message an unauthenticated caller may see for any fetch failure.
/// Details (the URI, the transport error, the IdP's status) go to the log --
/// they routinely name internal hosts and addresses.
const IDP_UNAVAILABLE: &str = "identity provider JWKS endpoint unavailable";

/// A signing key as the issuer published it. The declared `alg` rides along
/// because it is the ISSUER's statement about what the key may be used for; the
/// verifier enforces it so a caller cannot pick a weaker algorithm than the one
/// the key was published for.
#[derive(Clone)]
pub struct SigningKey {
    pub key: DecodingKey,
    pub alg: Option<Algorithm>,
}

/// The conventional JWKS location for an issuer. Keycloak and most OIDC
/// providers serve `<issuer>/protocol/openid-connect/certs`. Deployments whose
/// issuer identifier is not a reachable address configure `jwks_uri` explicitly
/// instead.
pub fn discover_uri(issuer: &str) -> String {
    format!(
        "{}/protocol/openid-connect/certs",
        issuer.trim_end_matches('/')
    )
}

pub struct JwksCache {
    uri: String,
    keys: RwLock<HashMap<String, SigningKey>>,
    http: reqwest::Client,
    fetches: AtomicUsize,
    /// Bumped when a refresh FINISHES. Snapshotting it before queueing on the
    /// guard is what lets a waiter tell "a fetch completed while I waited" from
    /// "the fetch I saw start is the one I am about to duplicate" -- `fetches`
    /// is bumped on entry, so it cannot distinguish those.
    refreshes_completed: AtomicUsize,
    /// Single-flight guard: concurrent misses queue here so exactly one of them
    /// fetches. Also closes the lost-update window -- `refresh` replaces the map
    /// wholesale, so two interleaved refreshes could otherwise discard a freshly
    /// rotated key.
    refreshing: Mutex<()>,
    /// Guarded by a std mutex: no await is taken inside it.
    state: StdMutex<RefreshState>,
    min_refresh_interval: Duration,
    failure_backoff_base: Duration,
}

/// What the last refresh did, which decides both how long we wait before the
/// next one and which error a suppressed refresh owes the caller.
#[derive(Default)]
struct RefreshState {
    /// When the last refresh FINISHED. `None` until one has. Stamping on
    /// completion rather than on entry is load-bearing: stamped on entry, every
    /// request concurrent with the very first fetch sees the floor as already
    /// running, skips the single-flight guard it should have queued on, and is
    /// rejected -- a cold start would reject nearly every valid token.
    completed_at: Option<Instant>,
    /// 0 means the last refresh succeeded.
    consecutive_failures: u32,
}

impl RefreshState {
    fn last_failed(&self) -> bool {
        self.consecutive_failures > 0
    }
}

impl JwksCache {
    pub fn new(uri: String) -> Self {
        Self::with_min_refresh_interval(uri, DEFAULT_MIN_REFRESH_INTERVAL)
    }

    /// Same, with the refetch floor overridden. Tests use this to exercise
    /// rotation without sleeping out the default interval.
    pub fn with_min_refresh_interval(uri: String, min_refresh_interval: Duration) -> Self {
        let http = reqwest::Client::builder()
            .connect_timeout(Duration::from_secs(3))
            .timeout(Duration::from_secs(5))
            // Default is 10. A JWKS endpoint has no business redirecting more
            // than once or twice.
            .redirect(reqwest::redirect::Policy::limited(2))
            .build()
            // Loud on purpose: a client we cannot build means we would silently
            // degrade to no timeouts, which is the thing we are guarding against.
            .expect("build the JWKS HTTP client");
        Self {
            uri,
            keys: RwLock::new(HashMap::new()),
            http,
            fetches: AtomicUsize::new(0),
            refreshes_completed: AtomicUsize::new(0),
            refreshing: Mutex::new(()),
            state: StdMutex::new(RefreshState::default()),
            min_refresh_interval,
            failure_backoff_base: DEFAULT_FAILURE_BACKOFF_BASE,
        }
    }

    /// Override the post-failure floor. Tests use this to exercise recovery
    /// from a transient IdP blip without sleeping out the real backoff.
    pub fn with_failure_backoff_base(mut self, failure_backoff_base: Duration) -> Self {
        self.failure_backoff_base = failure_backoff_base;
        self
    }

    /// Number of times the JWKS document has been fetched. Test observability.
    pub fn fetch_count(&self) -> usize {
        self.fetches.load(Ordering::SeqCst)
    }

    /// Look up a signing key by `kid`.
    ///
    /// A hit is served from memory. A miss triggers at most one fetch, and only
    /// if no fetch has *completed* within the current floor -- so a genuine key
    /// rotation is picked up within that interval, while a flood of tokens
    /// bearing unknown `kid`s costs the IdP nothing beyond that one fetch.
    /// Requests arriving during an in-flight fetch queue on it and ride its
    /// result rather than being rejected.
    pub async fn key_for(&self, kid: &str) -> Result<SigningKey, AuthError> {
        if let Some(key) = self.keys.read().await.get(kid) {
            return Ok(key.clone());
        }
        self.refresh_once().await?;
        if let Some(key) = self.keys.read().await.get(kid) {
            return Ok(key.clone());
        }
        // Still missing. Which error we owe the caller depends on whether we
        // actually know the key is absent, or merely could not ask: reporting
        // an IdP outage as a bad token sends the operator hunting in entirely
        // the wrong place.
        if self.last_refresh_failed() {
            tracing::debug!(
                uri = %self.uri,
                "JWKS refresh suppressed after a recent failure; reporting the IdP as unavailable"
            );
            return Err(AuthError::Jwks(IDP_UNAVAILABLE.to_string()));
        }
        Err(AuthError::UnknownKid(sanitize_kid(kid)))
    }

    /// Refresh on behalf of a miss, subject to the interval floor and the
    /// single-flight guard. Returning `Ok` does NOT mean the key is now known.
    async fn refresh_once(&self) -> Result<(), AuthError> {
        // Fast path: a fetch COMPLETED recently, so there is nothing to wait
        // for and nothing to gain from asking again. An in-flight fetch does
        // not trip this -- those callers fall through to the guard below.
        if self.too_soon() {
            return Ok(());
        }
        // Snapshot BEFORE queueing on the guard. If a refresh completed while
        // we waited, our miss rode along with it and must not fetch again --
        // true whether or not the key turned up, so the bogus-kid flood
        // collapses too, and true even with the interval floor set to zero.
        let generation = self.refreshes_completed.load(Ordering::SeqCst);
        let _flight = self.refreshing.lock().await;
        if self.refreshes_completed.load(Ordering::SeqCst) != generation || self.too_soon() {
            return Ok(());
        }
        self.refresh().await
    }

    fn too_soon(&self) -> bool {
        let state = self.state();
        match state.completed_at {
            None => false,
            Some(at) => at.elapsed() < self.floor(&state),
        }
    }

    /// How long to wait after the last refresh before attempting another.
    fn floor(&self, state: &RefreshState) -> Duration {
        if !state.last_failed() {
            return self.min_refresh_interval;
        }
        // Exponential: base, 2x, 4x ... capped at the success interval, so a
        // sustained outage settles at ~1 req/min while a single blip clears in
        // about a second.
        let doublings = state.consecutive_failures.saturating_sub(1).min(16);
        self.failure_backoff_base
            .saturating_mul(1u32 << doublings)
            .min(self.min_refresh_interval)
    }

    fn last_refresh_failed(&self) -> bool {
        self.state().last_failed()
    }

    fn state(&self) -> std::sync::MutexGuard<'_, RefreshState> {
        self.state
            .lock()
            .expect("jwks refresh state mutex poisoned")
    }

    /// Wraps the refresh so completion is recorded on EVERY exit, success or
    /// failure. A failed refresh still satisfies the waiters behind the guard --
    /// retrying it 16 times over is the stampede we are avoiding.
    async fn refresh(&self) -> Result<(), AuthError> {
        let result = self.refresh_inner().await;
        {
            let mut state = self.state();
            // On COMPLETION, so the floor measures dead time rather than
            // swallowing the fetch's own duration, and so nothing concurrent
            // with this fetch ever sees the floor as already running.
            state.completed_at = Some(Instant::now());
            state.consecutive_failures = if result.is_ok() {
                0
            } else {
                state.consecutive_failures.saturating_add(1)
            };
        }
        self.refreshes_completed.fetch_add(1, Ordering::SeqCst);
        result
    }

    async fn refresh_inner(&self) -> Result<(), AuthError> {
        self.fetches.fetch_add(1, Ordering::SeqCst);
        let set = self.fetch().await?;

        // One unusable key must not poison the whole set: an IdP may publish an
        // encryption key, or an algorithm we do not support, beside the good one.
        let mut fresh = HashMap::new();
        for jwk in &set.keys {
            let Some(kid) = jwk.common.key_id.clone() else {
                tracing::warn!(uri = %self.uri, "skipping JWKS key with no kid");
                continue;
            };
            // An IdP routinely publishes encryption keys beside signing keys.
            // Importing one would let a token be verified against a key its
            // issuer never intended to sign anything with.
            if matches!(jwk.common.public_key_use, Some(PublicKeyUse::Encryption)) {
                tracing::debug!(uri = %self.uri, kid = %sanitize_kid(&kid), "skipping JWKS key marked use=enc");
                continue;
            }
            match DecodingKey::from_jwk(jwk) {
                Ok(key) => {
                    fresh.insert(
                        kid,
                        SigningKey {
                            key,
                            alg: jwk.common.key_algorithm.and_then(as_signing_alg),
                        },
                    );
                }
                Err(e) => tracing::warn!(
                    uri = %self.uri,
                    kid = %sanitize_kid(&kid),
                    error = %e,
                    "skipping unusable JWKS key"
                ),
            }
        }

        *self.keys.write().await = fresh;
        Ok(())
    }

    async fn fetch(&self) -> Result<JwkSet, AuthError> {
        let mut response = self.http.get(&self.uri).send().await.map_err(|e| {
            tracing::warn!(uri = %self.uri, error = %e, "JWKS fetch failed");
            AuthError::Jwks(IDP_UNAVAILABLE.to_string())
        })?;

        if !response.status().is_success() {
            tracing::warn!(uri = %self.uri, status = %response.status(), "JWKS endpoint returned an error status");
            return Err(AuthError::Jwks(IDP_UNAVAILABLE.to_string()));
        }

        // Advertised length first (cheap rejection), then enforce the cap while
        // reading -- a body with no content-length is otherwise unbounded.
        if response
            .content_length()
            .is_some_and(|len| len > MAX_JWKS_BYTES as u64)
        {
            tracing::warn!(uri = %self.uri, content_length = ?response.content_length(), "JWKS document too large");
            return Err(AuthError::Jwks("jwks document too large".to_string()));
        }
        let mut body = Vec::new();
        while let Some(chunk) = response.chunk().await.map_err(|e| {
            tracing::warn!(uri = %self.uri, error = %e, "JWKS body read failed");
            AuthError::Jwks(IDP_UNAVAILABLE.to_string())
        })? {
            if body.len() + chunk.len() > MAX_JWKS_BYTES {
                tracing::warn!(uri = %self.uri, "JWKS document too large");
                return Err(AuthError::Jwks("jwks document too large".to_string()));
            }
            body.extend_from_slice(&chunk);
        }

        serde_json::from_slice(&body).map_err(|e| {
            tracing::warn!(uri = %self.uri, error = %e, "JWKS document did not parse");
            AuthError::Jwks(IDP_UNAVAILABLE.to_string())
        })
    }
}

/// Map a JWKS `alg` to the signature algorithm it names. `None` for anything
/// that is not a signature algorithm (key-management algs appear here on
/// encryption keys), which leaves the key unconstrained rather than
/// mis-constrained.
fn as_signing_alg(alg: jsonwebtoken::jwk::KeyAlgorithm) -> Option<Algorithm> {
    use jsonwebtoken::jwk::KeyAlgorithm as K;
    Some(match alg {
        K::HS256 => Algorithm::HS256,
        K::HS384 => Algorithm::HS384,
        K::HS512 => Algorithm::HS512,
        K::ES256 => Algorithm::ES256,
        K::ES384 => Algorithm::ES384,
        K::RS256 => Algorithm::RS256,
        K::RS384 => Algorithm::RS384,
        K::RS512 => Algorithm::RS512,
        K::PS256 => Algorithm::PS256,
        K::PS384 => Algorithm::PS384,
        K::PS512 => Algorithm::PS512,
        K::EdDSA => Algorithm::EdDSA,
        _ => return None,
    })
}

/// `kid` is an unvalidated, unbounded JWT header field under attacker control.
/// Anything we put in a message or a log line gets bounded and stripped of
/// control characters first -- otherwise it is a log-injection and log-volume
/// vector.
fn sanitize_kid(kid: &str) -> String {
    kid.chars()
        .filter(|c| !c.is_control())
        .take(64)
        .collect::<String>()
}

#[cfg(test)]
mod tests {
    use super::{discover_uri, sanitize_kid};

    #[test]
    fn discover_uri_appends_the_conventional_path() {
        assert_eq!(
            discover_uri("https://idp.example/realms/latiq"),
            "https://idp.example/realms/latiq/protocol/openid-connect/certs"
        );
    }

    #[test]
    fn discover_uri_does_not_double_the_separator() {
        // Issuer identifiers are copy-pasted from consoles that sometimes
        // include the trailing slash; a `//` would 404 on some gateways.
        assert_eq!(
            discover_uri("https://idp.example/realms/latiq/"),
            "https://idp.example/realms/latiq/protocol/openid-connect/certs"
        );
    }

    #[test]
    fn sanitize_kid_strips_control_characters() {
        assert_eq!(sanitize_kid("ab\nc\r\0d"), "abcd");
    }

    #[test]
    fn sanitize_kid_bounds_length_in_chars_not_bytes() {
        // Multi-byte on purpose: with ASCII the two counts coincide and the
        // assertion would not distinguish char truncation from byte truncation
        // (which would also risk splitting a code point).
        let bounded = sanitize_kid(&"é".repeat(4096));
        assert_eq!(bounded.chars().count(), 64);
        assert_eq!(bounded.len(), 128);
    }
}
