//! JWKS fetch + cache. Verification is offline after the first fetch: no IdP
//! round-trip on the request path, because this sits in front of every query.
use crate::AuthError;
use jsonwebtoken::jwk::JwkSet;
use jsonwebtoken::DecodingKey;
use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use tokio::sync::RwLock;

pub struct JwksCache {
    uri: String,
    keys: RwLock<HashMap<String, DecodingKey>>,
    http: reqwest::Client,
    fetches: AtomicUsize,
}

impl JwksCache {
    pub fn new(uri: String) -> Self {
        Self {
            uri,
            keys: RwLock::new(HashMap::new()),
            http: reqwest::Client::new(),
            fetches: AtomicUsize::new(0),
        }
    }

    /// Number of times the JWKS document has been fetched. Test observability.
    pub fn fetch_count(&self) -> usize {
        self.fetches.load(Ordering::SeqCst)
    }

    /// Look up a signing key by `kid`, refetching ONCE if it is unknown (key
    /// rotation). A second miss is an error -- never a refetch loop.
    pub async fn key_for(&self, kid: &str) -> Result<DecodingKey, AuthError> {
        if let Some(key) = self.keys.read().await.get(kid) {
            return Ok(key.clone());
        }
        self.refresh().await?;
        self.keys
            .read()
            .await
            .get(kid)
            .cloned()
            .ok_or_else(|| AuthError::UnknownKid(kid.to_string()))
    }

    async fn refresh(&self) -> Result<(), AuthError> {
        self.fetches.fetch_add(1, Ordering::SeqCst);
        let response = self
            .http
            .get(&self.uri)
            .send()
            .await
            .map_err(|e| AuthError::Jwks(format!("fetch {}: {e}", self.uri)))?
            .error_for_status()
            .map_err(|e| AuthError::Jwks(format!("fetch {}: {e}", self.uri)))?;
        let set: JwkSet = response
            .json()
            .await
            .map_err(|e| AuthError::Jwks(format!("parse {}: {e}", self.uri)))?;

        // One unusable key must not poison the whole set: an IdP may publish an
        // encryption key or an algorithm we do not support alongside the good one.
        let mut fresh = HashMap::new();
        for jwk in &set.keys {
            let Some(kid) = jwk.common.key_id.clone() else {
                tracing::warn!(jwks_uri = %self.uri, "skipping JWKS key with no kid");
                continue;
            };
            match DecodingKey::from_jwk(jwk) {
                Ok(key) => {
                    fresh.insert(kid, key);
                }
                Err(e) => {
                    tracing::warn!(jwks_uri = %self.uri, %kid, error = %e, "skipping unusable JWKS key")
                }
            }
        }

        *self.keys.write().await = fresh;
        Ok(())
    }
}
