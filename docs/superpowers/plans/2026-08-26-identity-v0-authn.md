# Identity v0 — Authentication Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make Latiq an OAuth 2.1 resource server so an enterprise IdP (Okta / Auth0 / Entra / Keycloak) can authenticate agents, the SDK, and operators across all three surfaces — with zero behaviour change when auth is not configured.

**Architecture:** A new `latiq-auth` crate owns JWKS fetching and JWT verification and knows nothing about transports. Each inbound adapter (MCP HTTP, Data/Stream gRPC, Admin gRPC) extracts a bearer token from its own carrier and calls the same verifier, satisfying invariant 5. `Identity` gains verified `subject`/`issuer` fields alongside the always-claimed `agent_id`. Authorization is **out of scope** — nothing gates on the result yet; it flows to attribution and the `latiq::access` trail.

**Tech Stack:** Rust, `jsonwebtoken` 9 (the only new dependency), `axum` 0.8 and `reqwest` (already in the tree), `tonic` interceptors, Keycloak 26 in Docker for the nightly e2e.

**Design source:** [`docs/identity.md`](../../identity.md). Read it before starting.

---

## Deviation from the design note (decide before Task 1)

`docs/identity.md` names the type `Principal`. **This plan keeps it named `Identity`** and adds fields instead. Rationale: the rename touches 12 source files and ~30 test call sites for zero behaviour change, and `Identity` is an honest name for `{subject, issuer, verified, agent_id}`. CLAUDE.md invariant 12 says make it boring. If the reviewer prefers `Principal`, do the rename as a standalone mechanical commit **before** Task 1 and update `docs/identity.md` is unnecessary — it already says `Principal`, so instead update the doc's code block to say `Identity`.

---

## File structure

**New crate — `crates/latiq-auth/`** (server-only; must NOT be pulled into the client-only CLI build, which is why it is not in `latiq-common`):

| File | Responsibility |
|---|---|
| `src/lib.rs` | `AuthConfig`, `Verifier`, `AuthError`; re-exports |
| `src/jwks.rs` | JWKS fetch + in-memory cache + refresh on unknown `kid` |
| `src/verify.rs` | Claim validation: signature, `iss`, `aud`, `exp`, algorithm allowlist |
| `src/metadata.rs` | The RFC 9728 protected-resource metadata document + the `WWW-Authenticate` challenge string |
| `tests/support/mod.rs` | Test IdP fixture: RSA keypair, token minting, a local JWKS server |
| `tests/verify.rs` | Fast-tier verification tests (no Docker) |

**Modified:**

| File | Change |
|---|---|
| `crates/latiq-common/src/identity.rs` | Add `subject`, `issuer`; add `Identity::verified(...)` |
| `crates/latiq-mcp/src/server.rs` | Remove `agent_id` from 9 arg structs; read the `Authorization` header from `RequestContext::extensions`; serve the metadata document; 401 challenge |
| `crates/latiq-pond-node/src/data_service.rs` | `identity_of` verifies a bearer token when configured |
| `crates/latiq-pond-node/src/stream_service.rs` | Inherits `identity_of` |
| `crates/latiq-control-plane/src/admin_service.rs` | Extract identity at all (it reads none today) and audit it |
| `crates/latiq-pond-node/src/lib.rs` | `PondNodeConfig` carries the auth settings |
| `crates/latiq-control-plane/src/lib.rs` | `serve_control_plane` takes auth settings |
| `crates/latiq/src/main.rs` | `--auth-issuer` / `--auth-audience` server flags; `--token` / `LATIQ_TOKEN` client side |
| `crates/latiq-sdk/src/lib.rs` | `token=` connect parameter → gRPC metadata |
| `deploy/cluster/docker-compose.yml` | Keycloak behind an `auth` profile (internal compose only) |
| `.github/workflows/nightly.yml` | Auth e2e job |

---

## Task 1: `Identity` carries verified fields

**Files:**
- Modify: `crates/latiq-common/src/identity.rs:1-40`

- [ ] **Step 1: Write the failing tests**

Append to `crates/latiq-common/src/identity.rs` inside `mod tests`:

```rust
    #[test]
    fn claimed_has_no_verified_fields() {
        let id = Identity::claimed(Some("agent-7"));
        assert_eq!(id.agent_id, "agent-7");
        assert!(!id.verified);
        assert_eq!(id.subject, "");
        assert_eq!(id.issuer, "");
    }

    #[test]
    fn verified_carries_subject_and_issuer() {
        let id = Identity::verified("svc-orchestrator", "https://idp.example/realms/latiq", Some("agent-7"));
        assert!(id.verified);
        assert_eq!(id.subject, "svc-orchestrator");
        assert_eq!(id.issuer, "https://idp.example/realms/latiq");
        assert_eq!(id.agent_id, "agent-7");
    }

    #[test]
    fn verified_agent_id_falls_back_to_subject() {
        // The leaf is optional; without it the subject is the best attribution
        // we have -- "anonymous" would be a lie for a verified caller.
        let id = Identity::verified("svc-orchestrator", "https://idp.example", None);
        assert_eq!(id.agent_id, "svc-orchestrator");
    }
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cargo test -p latiq-common --lib`
Expected: FAIL — `no function or associated item named 'verified' found`, and `no field 'subject'`.

- [ ] **Step 3: Implement**

Replace the struct and impl in `crates/latiq-common/src/identity.rs` (keep the existing two tests untouched):

```rust
//! Caller identity. Each field knows whether it was verified: `subject` and
//! `issuer` come from a validated IdP token; `agent_id` is ALWAYS claimed and
//! must never carry authority. See docs/identity.md.
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Identity {
    /// The claimed leaf agent instance. Never verified. Attribution only.
    pub agent_id: String,
    /// The IdP's `sub`. Empty unless `verified`.
    pub subject: String,
    /// The `iss` of the token that produced `subject`. Empty unless `verified`.
    /// Carried separately so subjects from different issuers cannot collide.
    pub issuer: String,
    pub verified: bool,
}

impl Identity {
    /// Build a claimed (unverified) identity, defaulting to "anonymous" when absent/empty.
    pub fn claimed(header: Option<&str>) -> Self {
        let agent_id = match header.map(str::trim) {
            Some(s) if !s.is_empty() => s.to_string(),
            _ => "anonymous".to_string(),
        };
        Self {
            agent_id,
            subject: String::new(),
            issuer: String::new(),
            verified: false,
        }
    }

    /// Build a verified identity from validated token claims. The leaf agent id
    /// stays claimed; absent, it falls back to the subject.
    pub fn verified(subject: &str, issuer: &str, claimed_agent: Option<&str>) -> Self {
        let agent_id = match claimed_agent.map(str::trim) {
            Some(s) if !s.is_empty() => s.to_string(),
            _ => subject.to_string(),
        };
        Self {
            agent_id,
            subject: subject.to_string(),
            issuer: issuer.to_string(),
            verified: true,
        }
    }
}
```

- [ ] **Step 4: Run the tests**

Run: `cargo test -p latiq-common --lib`
Expected: PASS, 5 tests.

- [ ] **Step 5: Fix the fallout and run the workspace**

Struct literal construction of `Identity` outside `latiq-common` will now fail to compile. Run `cargo build --workspace` and fix each site by calling `Identity::claimed(...)` instead of a literal.

Run: `cargo test --workspace`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add crates/latiq-common/src/identity.rs
git commit -m "feat(identity): Identity carries verified subject and issuer alongside the claimed leaf"
```

---

## Task 2: The `latiq-auth` crate — JWKS cache

**Files:**
- Create: `crates/latiq-auth/Cargo.toml`
- Create: `crates/latiq-auth/src/lib.rs`
- Create: `crates/latiq-auth/src/jwks.rs`
- Create: `crates/latiq-auth/tests/support/mod.rs`
- Create: `crates/latiq-auth/tests/jwks.rs`
- Modify: root `Cargo.toml` (workspace members + `[workspace.dependencies]`)

- [ ] **Step 1: Add the crate and dependencies**

Root `Cargo.toml` — add `"crates/latiq-auth"` to `members`, and to `[workspace.dependencies]`:

```toml
jsonwebtoken = "9.3"
reqwest = { version = "0.13", default-features = false, features = ["json", "rustls-tls"] }
http = "1"
```

Pin `reqwest` to `0.13` to match the copy rmcp already pulls in; a second major would duplicate the hyper/rustls tree.

`crates/latiq-auth/Cargo.toml`:

```toml
[package]
name = "latiq-auth"
version.workspace = true
edition.workspace = true

[dependencies]
jsonwebtoken.workspace = true
reqwest.workspace = true
serde.workspace = true
serde_json.workspace = true
thiserror.workspace = true
tokio.workspace = true
tracing.workspace = true
latiq-common = { path = "../latiq-common" }

[dev-dependencies]
axum.workspace = true
rsa = "0.9"
rand = "0.8"
```

If `thiserror` / `serde_json` are not yet in `[workspace.dependencies]`, add them (`thiserror = "2"`, `serde_json = "1"`).

- [ ] **Step 2: Write the test fixture (a fake IdP)**

`crates/latiq-auth/tests/support/mod.rs` — this is the fast tier's whole reason for existing: it mints tokens we control, so every negative path is deterministic and needs no container.

```rust
//! A minimal in-process IdP for tests: one RSA keypair, a JWKS endpoint, and a
//! token minter. Lets us produce a token that is wrong in exactly one way.
use jsonwebtoken::{Algorithm, EncodingKey, Header};
use rsa::pkcs1::EncodeRsaPrivateKey;
use rsa::traits::PublicKeyParts;
use rsa::RsaPrivateKey;
use serde_json::json;
use std::net::SocketAddr;

pub const KID: &str = "test-key-1";

pub struct TestIdp {
    key: RsaPrivateKey,
    encoding: EncodingKey,
    pub issuer: String,
    pub jwks_uri: String,
}

impl TestIdp {
    /// Start the JWKS server on an ephemeral port and return the fixture.
    pub async fn start() -> Self {
        let mut rng = rand::thread_rng();
        let key = RsaPrivateKey::new(&mut rng, 2048).expect("generate key");
        let pem = key.to_pkcs1_pem(rsa::pkcs1::LineEnding::LF).expect("pem");
        let encoding = EncodingKey::from_rsa_pem(pem.as_bytes()).expect("encoding key");

        let n = base64_url(&key.n().to_bytes_be());
        let e = base64_url(&key.e().to_bytes_be());
        let jwks = json!({"keys": [{
            "kty": "RSA", "use": "sig", "alg": "RS256", "kid": KID, "n": n, "e": e
        }]});

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let addr: SocketAddr = listener.local_addr().expect("addr");
        let app = axum::Router::new().route(
            "/jwks",
            axum::routing::get(move || {
                let jwks = jwks.clone();
                async move { axum::Json(jwks) }
            }),
        );
        tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });

        Self {
            key,
            encoding,
            issuer: format!("http://{addr}"),
            jwks_uri: format!("http://{addr}/jwks"),
        }
    }

    /// Mint a token. `exp_offset_secs` is relative to now, so a negative value
    /// produces an already-expired token.
    pub fn mint(&self, sub: &str, aud: &str, iss: &str, exp_offset_secs: i64) -> String {
        self.mint_with_kid(sub, aud, iss, exp_offset_secs, Some(KID.to_string()))
    }

    pub fn mint_with_kid(
        &self,
        sub: &str,
        aud: &str,
        iss: &str,
        exp_offset_secs: i64,
        kid: Option<String>,
    ) -> String {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock")
            .as_secs() as i64;
        let claims = json!({
            "sub": sub, "aud": aud, "iss": iss,
            "iat": now, "exp": now + exp_offset_secs,
        });
        let mut header = Header::new(Algorithm::RS256);
        header.kid = kid;
        jsonwebtoken::encode(&header, &claims, &self.encoding).expect("encode")
    }

    /// A token signed by a DIFFERENT key, to prove signature checking works.
    pub fn mint_with_foreign_key(&self, sub: &str, aud: &str, iss: &str) -> String {
        let mut rng = rand::thread_rng();
        let other = RsaPrivateKey::new(&mut rng, 2048).expect("generate key");
        let pem = other.to_pkcs1_pem(rsa::pkcs1::LineEnding::LF).expect("pem");
        let enc = EncodingKey::from_rsa_pem(pem.as_bytes()).expect("encoding key");
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock")
            .as_secs() as i64;
        let claims = json!({"sub": sub, "aud": aud, "iss": iss, "iat": now, "exp": now + 300});
        let mut header = Header::new(Algorithm::RS256);
        header.kid = Some(KID.to_string());
        jsonwebtoken::encode(&header, &claims, &enc).expect("encode")
    }
}

fn base64_url(bytes: &[u8]) -> String {
    use base64::Engine;
    base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(bytes)
}
```

Add `base64 = "0.22"` to `[dev-dependencies]`. Note `self.key` is retained only so the struct owns the key material; if the compiler warns it is unused, prefix it `_key`.

- [ ] **Step 3: Write the failing JWKS test**

`crates/latiq-auth/tests/jwks.rs`:

```rust
mod support;
use latiq_auth::jwks::JwksCache;

#[tokio::test]
async fn auth_jwks_fetches_and_caches_by_kid() {
    let idp = support::TestIdp::start().await;
    let cache = JwksCache::new(idp.jwks_uri.clone());

    assert!(cache.key_for(support::KID).await.is_ok());
    // Second call must not re-fetch.
    assert!(cache.key_for(support::KID).await.is_ok());
    assert_eq!(cache.fetch_count(), 1);
}

#[tokio::test]
async fn auth_jwks_refetches_once_on_unknown_kid() {
    let idp = support::TestIdp::start().await;
    let cache = JwksCache::new(idp.jwks_uri.clone());

    assert!(cache.key_for(support::KID).await.is_ok());
    // An unknown kid triggers exactly one refresh, then fails -- it must not
    // hammer the IdP on every request with a bogus kid.
    assert!(cache.key_for("rotated-key").await.is_err());
    assert_eq!(cache.fetch_count(), 2);
}

#[tokio::test]
async fn auth_jwks_unreachable_endpoint_is_an_error_not_a_panic() {
    let cache = JwksCache::new("http://127.0.0.1:1/jwks".to_string());
    assert!(cache.key_for(support::KID).await.is_err());
}
```

- [ ] **Step 4: Run to verify failure**

Run: `cargo test -p latiq-auth --test jwks`
Expected: FAIL to compile — `latiq_auth::jwks` does not exist.

- [ ] **Step 5: Implement the JWKS cache**

`crates/latiq-auth/src/jwks.rs`:

```rust
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
        if let Some(k) = self.keys.read().await.get(kid) {
            return Ok(k.clone());
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
        let set: JwkSet = self
            .http
            .get(&self.uri)
            .send()
            .await
            .map_err(|e| AuthError::Jwks(format!("fetch {}: {e}", self.uri)))?
            .json()
            .await
            .map_err(|e| AuthError::Jwks(format!("parse {}: {e}", self.uri)))?;

        let mut map = HashMap::new();
        for jwk in &set.keys {
            let Some(kid) = jwk.common.key_id.clone() else {
                continue;
            };
            match DecodingKey::from_jwk(jwk) {
                Ok(k) => {
                    map.insert(kid, k);
                }
                Err(e) => tracing::warn!(kid = %kid, error = %e, "skipping unusable JWK"),
            }
        }
        *self.keys.write().await = map;
        Ok(())
    }
}
```

`crates/latiq-auth/src/lib.rs` (initial):

```rust
//! Token verification for every Latiq surface. PROTOCOL-NEUTRAL: this crate
//! takes a token string and returns an Identity. Adapters extract the token
//! from their own carrier. See docs/identity.md.
pub mod jwks;

#[derive(Debug, thiserror::Error)]
pub enum AuthError {
    #[error("no bearer token presented")]
    Missing,
    #[error("malformed token: {0}")]
    Malformed(String),
    #[error("token signing key '{0}' is not known to the issuer's JWKS")]
    UnknownKid(String),
    #[error("token rejected: {0}")]
    Invalid(String),
    #[error("jwks: {0}")]
    Jwks(String),
}
```

- [ ] **Step 6: Run the tests**

Run: `cargo test -p latiq-auth --test jwks`
Expected: PASS, 3 tests.

- [ ] **Step 7: Commit**

```bash
git add crates/latiq-auth Cargo.toml
git commit -m "feat(auth): latiq-auth crate with a caching JWKS key store"
```

---

## Task 3: Token verification

**Files:**
- Create: `crates/latiq-auth/src/verify.rs`
- Create: `crates/latiq-auth/tests/verify.rs`
- Modify: `crates/latiq-auth/src/lib.rs`

- [ ] **Step 1: Write the failing tests**

`crates/latiq-auth/tests/verify.rs` — these are the tests a container cannot write better than we can:

```rust
mod support;
use latiq_auth::{AuthConfig, Verifier};

const AUD: &str = "latiq";

async fn verifier(idp: &support::TestIdp) -> Verifier {
    Verifier::new(AuthConfig {
        issuer: idp.issuer.clone(),
        audience: AUD.to_string(),
        jwks_uri: idp.jwks_uri.clone(),
    })
}

#[tokio::test]
async fn auth_valid_token_yields_a_verified_identity() {
    let idp = support::TestIdp::start().await;
    let v = verifier(&idp).await;
    let token = idp.mint("svc-orch", AUD, &idp.issuer, 300);

    let id = v.verify(&token, Some("agent-7")).await.expect("verify");
    assert!(id.verified);
    assert_eq!(id.subject, "svc-orch");
    assert_eq!(id.issuer, idp.issuer);
    assert_eq!(id.agent_id, "agent-7");
}

#[tokio::test]
async fn auth_rejects_wrong_audience() {
    // The single most important check: a token minted for another service
    // must not be replayable at us.
    let idp = support::TestIdp::start().await;
    let v = verifier(&idp).await;
    let token = idp.mint("svc-orch", "some-other-service", &idp.issuer, 300);
    assert!(v.verify(&token, None).await.is_err());
}

#[tokio::test]
async fn auth_rejects_wrong_issuer() {
    let idp = support::TestIdp::start().await;
    let v = verifier(&idp).await;
    let token = idp.mint("svc-orch", AUD, "https://evil.example", 300);
    assert!(v.verify(&token, None).await.is_err());
}

#[tokio::test]
async fn auth_rejects_expired_token() {
    let idp = support::TestIdp::start().await;
    let v = verifier(&idp).await;
    let token = idp.mint("svc-orch", AUD, &idp.issuer, -60);
    assert!(v.verify(&token, None).await.is_err());
}

#[tokio::test]
async fn auth_rejects_foreign_signature() {
    let idp = support::TestIdp::start().await;
    let v = verifier(&idp).await;
    let token = idp.mint_with_foreign_key("svc-orch", AUD, &idp.issuer);
    assert!(v.verify(&token, None).await.is_err());
}

#[tokio::test]
async fn auth_rejects_token_without_kid() {
    let idp = support::TestIdp::start().await;
    let v = verifier(&idp).await;
    let token = idp.mint_with_kid("svc-orch", AUD, &idp.issuer, 300, None);
    assert!(v.verify(&token, None).await.is_err());
}

#[tokio::test]
async fn auth_rejects_garbage() {
    let idp = support::TestIdp::start().await;
    let v = verifier(&idp).await;
    assert!(v.verify("not.a.token", None).await.is_err());
    assert!(v.verify("", None).await.is_err());
}

#[tokio::test]
async fn auth_leaf_agent_id_defaults_to_subject() {
    let idp = support::TestIdp::start().await;
    let v = verifier(&idp).await;
    let token = idp.mint("svc-orch", AUD, &idp.issuer, 300);
    let id = v.verify(&token, None).await.expect("verify");
    assert_eq!(id.agent_id, "svc-orch");
}
```

- [ ] **Step 2: Run to verify failure**

Run: `cargo test -p latiq-auth --test verify`
Expected: FAIL to compile — `Verifier` and `AuthConfig` do not exist.

- [ ] **Step 3: Implement**

`crates/latiq-auth/src/verify.rs`:

```rust
//! JWT claim validation. Algorithms are an ALLOWLIST, never taken from the
//! token header -- accepting the header's `alg` is the classic algorithm
//! confusion bug.
use crate::jwks::JwksCache;
use crate::AuthError;
use jsonwebtoken::{decode, decode_header, Algorithm, Validation};
use latiq_common::Identity;
use serde::Deserialize;

/// Signature algorithms we accept. Asymmetric only: a symmetric alg would mean
/// the verifier holds a signing secret, which a resource server must not.
const ALLOWED_ALGS: &[Algorithm] = &[Algorithm::RS256, Algorithm::RS384, Algorithm::RS512, Algorithm::ES256];

#[derive(Debug, Clone)]
pub struct AuthConfig {
    pub issuer: String,
    pub audience: String,
    pub jwks_uri: String,
}

#[derive(Debug, Deserialize)]
struct Claims {
    sub: String,
}

pub struct Verifier {
    cfg: AuthConfig,
    jwks: JwksCache,
}

impl Verifier {
    pub fn new(cfg: AuthConfig) -> Self {
        let jwks = JwksCache::new(cfg.jwks_uri.clone());
        Self { cfg, jwks }
    }

    pub fn config(&self) -> &AuthConfig {
        &self.cfg
    }

    /// Verify a bearer token and build a verified Identity. `claimed_agent` is
    /// the caller-asserted leaf id -- never verified, attribution only.
    pub async fn verify(&self, token: &str, claimed_agent: Option<&str>) -> Result<Identity, AuthError> {
        if token.trim().is_empty() {
            return Err(AuthError::Missing);
        }
        let header = decode_header(token).map_err(|e| AuthError::Malformed(e.to_string()))?;
        if !ALLOWED_ALGS.contains(&header.alg) {
            return Err(AuthError::Invalid(format!("algorithm {:?} not allowed", header.alg)));
        }
        let kid = header.kid.ok_or_else(|| {
            AuthError::Malformed("token header has no 'kid'; cannot select a signing key".into())
        })?;
        let key = self.jwks.key_for(&kid).await?;

        let mut validation = Validation::new(header.alg);
        validation.set_audience(&[self.cfg.audience.as_str()]);
        validation.set_issuer(&[self.cfg.issuer.as_str()]);
        validation.set_required_spec_claims(&["exp", "aud", "iss", "sub"]);

        let data = decode::<Claims>(token, &key, &validation)
            .map_err(|e| AuthError::Invalid(e.to_string()))?;

        Ok(Identity::verified(&data.claims.sub, &self.cfg.issuer, claimed_agent))
    }
}
```

Add to `crates/latiq-auth/src/lib.rs`:

```rust
pub mod verify;
pub use verify::{AuthConfig, Verifier};
```

- [ ] **Step 4: Run the tests**

Run: `cargo test -p latiq-auth`
Expected: PASS, 11 tests total.

- [ ] **Step 5: Commit**

```bash
git add crates/latiq-auth
git commit -m "feat(auth): verify bearer tokens against issuer, audience, expiry, and an algorithm allowlist"
```

---

## Task 4: Protected-resource metadata and the 401 challenge

**Files:**
- Create: `crates/latiq-auth/src/metadata.rs`
- Create: `crates/latiq-auth/tests/metadata.rs`
- Modify: `crates/latiq-auth/src/lib.rs`

- [ ] **Step 1: Write the failing test**

`crates/latiq-auth/tests/metadata.rs`:

```rust
use latiq_auth::metadata::{challenge_header, ProtectedResourceMetadata};

#[test]
fn auth_metadata_document_advertises_the_authorization_server() {
    let doc = ProtectedResourceMetadata::new(
        "http://node-1:51402/mcp",
        "https://idp.example/realms/latiq",
    );
    let json = serde_json::to_value(&doc).expect("serialize");
    assert_eq!(json["resource"], "http://node-1:51402/mcp");
    assert_eq!(json["authorization_servers"][0], "https://idp.example/realms/latiq");
    assert_eq!(json["bearer_methods_supported"][0], "header");
}

#[test]
fn auth_challenge_points_the_client_at_the_metadata_document() {
    let h = challenge_header("http://node-1:51402/.well-known/oauth-protected-resource");
    assert!(h.starts_with("Bearer "));
    assert!(h.contains(r#"resource_metadata="http://node-1:51402/.well-known/oauth-protected-resource""#));
}
```

- [ ] **Step 2: Run to verify failure**

Run: `cargo test -p latiq-auth --test metadata`
Expected: FAIL to compile — module `metadata` not found.

- [ ] **Step 3: Implement**

`crates/latiq-auth/src/metadata.rs`:

```rust
//! RFC 9728 protected-resource metadata -- how an MCP client discovers which
//! authorization server to go to. We are never in the token exchange; this
//! document is the entire handshake we participate in.
use serde::Serialize;

#[derive(Debug, Clone, Serialize)]
pub struct ProtectedResourceMetadata {
    pub resource: String,
    pub authorization_servers: Vec<String>,
    pub bearer_methods_supported: Vec<String>,
}

impl ProtectedResourceMetadata {
    pub fn new(resource: &str, authorization_server: &str) -> Self {
        Self {
            resource: resource.to_string(),
            authorization_servers: vec![authorization_server.to_string()],
            bearer_methods_supported: vec!["header".to_string()],
        }
    }
}

/// The `WWW-Authenticate` value returned with a 401 so the client can find the
/// metadata document without knowing anything about us in advance.
pub fn challenge_header(metadata_url: &str) -> String {
    format!(r#"Bearer realm="latiq", resource_metadata="{metadata_url}""#)
}
```

Add to `crates/latiq-auth/src/lib.rs`: `pub mod metadata;`

- [ ] **Step 4: Run the tests**

Run: `cargo test -p latiq-auth`
Expected: PASS, 13 tests.

- [ ] **Step 5: Commit**

```bash
git add crates/latiq-auth
git commit -m "feat(auth): RFC 9728 protected-resource metadata and the 401 bearer challenge"
```

---

## Task 5: Wire the Data + Stream gRPC surface

Do gRPC before MCP: it is the smaller change and it proves the verifier end-to-end before the breaking MCP schema change.

**Files:**
- Modify: `crates/latiq-pond-node/src/data_service.rs:24-31`
- Modify: `crates/latiq-pond-node/src/lib.rs:29-43` (`PondNodeConfig`)
- Modify: `crates/latiq-pond-node/Cargo.toml`
- Test: `crates/latiq/tests/query_grpc.rs`

- [ ] **Step 1: Write the failing tests**

Append to `crates/latiq/tests/query_grpc.rs`. Reuse the file's existing `req`, `alloc`, `q`, `client` helpers.

```rust
#[tokio::test]
async fn auth_absent_config_keeps_relaxed_identity() {
    // The embedded/dev path: no issuer configured means behave exactly as before.
    let stack = common::start_stack().await;
    let mut c = client(&stack.data_endpoint).await;
    let pond = alloc(&mut c, "auth-off").await;
    let res = c.read_query(req(q(&pond, "SELECT 1 AS n"), "agent-7")).await;
    assert!(res.is_ok(), "unauthenticated mode must still work");
}
```

The authenticated cases need a stack started with auth configured. Add to `crates/latiq/tests/common/mod.rs` a variant `start_stack_with_auth(cfg: latiq_auth::AuthConfig)` that threads the config into `PondNodeConfig`, then:

```rust
#[tokio::test]
async fn auth_rejects_missing_token_when_configured() {
    let idp = common::TestIdp::start().await;
    let stack = common::start_stack_with_auth(idp.auth_config()).await;
    let mut c = client(&stack.data_endpoint).await;
    let err = alloc_result(&mut c, "no-token").await.expect_err("must be unauthenticated");
    assert_eq!(err.code(), tonic::Code::Unauthenticated);
}

#[tokio::test]
async fn auth_accepts_a_valid_token_and_marks_identity_verified() {
    let idp = common::TestIdp::start().await;
    let stack = common::start_stack_with_auth(idp.auth_config()).await;
    let mut c = client(&stack.data_endpoint).await;
    let token = idp.mint("svc-orch", "latiq", &idp.issuer, 300);

    let mut r = tonic::Request::new(/* AllocatePondRequest as `alloc` builds it */);
    r.metadata_mut().insert("authorization", format!("Bearer {token}").parse().unwrap());
    assert!(c.allocate_pond(r).await.is_ok());
}
```

Move the `TestIdp` fixture from `crates/latiq-auth/tests/support/mod.rs` into the `latiq-auth` crate itself behind a `test-support` feature so both crates can use it — duplicating it would guarantee drift. Add to `crates/latiq-auth/Cargo.toml`:

```toml
[features]
test-support = ["dep:axum", "dep:rsa", "dep:rand", "dep:base64"]
```
and move `TestIdp` to `src/test_support.rs` gated by `#[cfg(feature = "test-support")]`, adding `pub fn auth_config(&self) -> AuthConfig`.

- [ ] **Step 2: Run to verify failure**

Run: `cargo test -p latiq --test query_grpc auth_`
Expected: FAIL to compile — `start_stack_with_auth` does not exist.

- [ ] **Step 3: Implement**

`crates/latiq-pond-node/src/data_service.rs` — replace `identity_of` (L24-31):

```rust
/// Identity from gRPC metadata. With a verifier configured, an `authorization:
/// Bearer <jwt>` header is REQUIRED and verified; `latiq-agent-id` then supplies
/// only the claimed leaf. Without one, identity stays relaxed (claimed, default
/// anonymous) -- the embedded and dev path.
pub(crate) async fn identity_of<T>(
    verifier: Option<&Arc<Verifier>>,
    req: &Request<T>,
) -> Result<Identity, Status> {
    let claimed = req.metadata().get("latiq-agent-id").and_then(|v| v.to_str().ok());
    let Some(v) = verifier else {
        return Ok(Identity::claimed(claimed));
    };
    let token = req
        .metadata()
        .get("authorization")
        .and_then(|h| h.to_str().ok())
        .and_then(|h| h.strip_prefix("Bearer ").or_else(|| h.strip_prefix("bearer ")))
        .ok_or_else(|| Status::unauthenticated("a bearer token is required"))?;
    v.verify(token, claimed)
        .await
        .map_err(|e| Status::unauthenticated(e.to_string()))
}
```

`identity_of` becomes `async` and fallible, so all 9 call sites in `data_service.rs` (L86, 118, 135, 150, 169, 187, 205, 223, 247) and the one in `stream_service.rs` (L41) change from `let id = identity_of(&req);` to `let id = identity_of(self.verifier.as_ref(), &req).await?;`. Add a `verifier: Option<Arc<Verifier>>` field to both service structs, populated from `PondNodeConfig`.

`crates/latiq-pond-node/src/lib.rs` — add to `PondNodeConfig`:

```rust
    /// When set, every surface on this node requires a verified bearer token.
    pub auth: Option<latiq_auth::AuthConfig>,
```

- [ ] **Step 4: Run the tests**

Run: `cargo test -p latiq --test query_grpc`
Expected: PASS, including the three new `auth_*` tests and all pre-existing ones.

- [ ] **Step 5: Commit**

```bash
git add crates/latiq-pond-node crates/latiq-auth crates/latiq/tests
git commit -m "feat(auth): Data and Stream gRPC verify bearer tokens when an issuer is configured"
```

---

## Task 6: Wire the Admin gRPC surface

Admin reads **no** identity today — the CLI sends `latiq-agent-id` and the server discards it. This task closes that.

**Files:**
- Modify: `crates/latiq-control-plane/src/admin_service.rs:8-16`
- Modify: `crates/latiq-control-plane/src/lib.rs:67`
- Test: `crates/latiq/tests/admin.rs`

- [ ] **Step 1: Write the failing test**

Append to `crates/latiq/tests/admin.rs`:

```rust
#[tokio::test]
async fn auth_admin_rejects_missing_token_when_configured() {
    let idp = latiq_auth::test_support::TestIdp::start().await;
    let stack = common::start_control_plane_with_auth(idp.auth_config()).await;
    let mut c = admin_client(&stack.admin_endpoint).await;
    let err = c.pond_list(tonic::Request::new(PondListRequest {})).await.expect_err("must reject");
    assert_eq!(err.code(), tonic::Code::Unauthenticated);
}

#[tokio::test]
async fn auth_admin_accepts_a_valid_token() {
    let idp = latiq_auth::test_support::TestIdp::start().await;
    let stack = common::start_control_plane_with_auth(idp.auth_config()).await;
    let mut c = admin_client(&stack.admin_endpoint).await;
    let token = idp.mint("ops-alice", "latiq", &idp.issuer, 300);
    let mut r = tonic::Request::new(PondListRequest {});
    r.metadata_mut().insert("authorization", format!("Bearer {token}").parse().unwrap());
    assert!(c.pond_list(r).await.is_ok());
}
```

- [ ] **Step 2: Run to verify failure**

Run: `cargo test -p latiq --test admin auth_`
Expected: FAIL to compile — `start_control_plane_with_auth` does not exist.

- [ ] **Step 3: Implement**

Give `AdminService` a `verifier: Option<Arc<Verifier>>` and add the same extraction helper used on the node. Put it in `crates/latiq-auth/src/lib.rs` so it is written once:

```rust
/// Pull a bearer token out of an HTTP-style `Authorization` value.
pub fn bearer(value: &str) -> Option<&str> {
    value
        .strip_prefix("Bearer ")
        .or_else(|| value.strip_prefix("bearer "))
        .map(str::trim)
        .filter(|t| !t.is_empty())
}
```

Use it in both `data_service.rs` and `admin_service.rs`. `serve_control_plane(addr, registry)` gains a third parameter `auth: Option<AuthConfig>`; update its three call sites (`crates/latiq/src/main.rs:749` and the two test mounts at `crates/latiq-control-plane/src/lib.rs:86` / `:98`).

Each admin handler calls the helper and passes the identity to a new `audit` call mirroring `ops.rs:699-717`, so operator actions land on the same `latiq::access` target as agent actions.

- [ ] **Step 4: Run the tests**

Run: `cargo test -p latiq --test admin && cargo test -p latiq-control-plane`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add crates/latiq-control-plane crates/latiq-auth crates/latiq/tests/admin.rs
git commit -m "feat(auth): Admin gRPC verifies identity and audits operator actions"
```

---

## Task 7: Move identity out of the MCP tool arguments (BREAKING)

The one urgent item: today the model types its own `agent_id` into a tool parameter. rmcp 1.7 injects `http::request::Parts` into `RequestContext::extensions`, so the handler can read the real header.

**Files:**
- Modify: `crates/latiq-mcp/src/server.rs` (9 arg structs, 10 handlers, `serve_mcp_with_listener`)
- Modify: `crates/latiq-mcp/Cargo.toml` (add `http = "1"`, `latiq-auth`)
- Test: `crates/latiq/tests/mcp.rs`

- [ ] **Step 1: Write the failing tests**

Append to `crates/latiq/tests/mcp.rs`:

```rust
#[tokio::test]
async fn auth_mcp_tool_schemas_do_not_expose_agent_id() {
    // The model must not be able to type its own identity.
    let stack = common::start_stack().await;
    let client = common::mcp_client(&stack.mcp_endpoint).await;
    let tools = client.list_tools(Default::default()).await.expect("list tools");
    for t in tools.tools {
        let schema = serde_json::to_string(&t.input_schema).expect("schema");
        assert!(!schema.contains("agent_id"), "tool {} still exposes agent_id", t.name);
    }
}

#[tokio::test]
async fn auth_mcp_serves_protected_resource_metadata() {
    let idp = latiq_auth::test_support::TestIdp::start().await;
    let stack = common::start_stack_with_auth(idp.auth_config()).await;
    let url = format!("{}/.well-known/oauth-protected-resource", stack.mcp_base_url);
    let doc: serde_json::Value = reqwest::get(&url).await.expect("get").json().await.expect("json");
    assert_eq!(doc["authorization_servers"][0], idp.issuer);
}

#[tokio::test]
async fn auth_mcp_unauthenticated_request_gets_a_401_challenge() {
    let idp = latiq_auth::test_support::TestIdp::start().await;
    let stack = common::start_stack_with_auth(idp.auth_config()).await;
    let res = reqwest::Client::new()
        .post(format!("{}/mcp", stack.mcp_base_url))
        .header("content-type", "application/json")
        .body(r#"{"jsonrpc":"2.0","id":1,"method":"tools/list"}"#)
        .send()
        .await
        .expect("post");
    assert_eq!(res.status(), reqwest::StatusCode::UNAUTHORIZED);
    let challenge = res.headers().get("www-authenticate").expect("challenge").to_str().unwrap();
    assert!(challenge.contains("resource_metadata="));
}
```

- [ ] **Step 2: Run to verify failure**

Run: `cargo test -p latiq --test mcp auth_`
Expected: FAIL — `agent_id` is present in every tool schema; the metadata route 404s.

- [ ] **Step 3: Remove the `agent_id` fields**

Delete the `pub agent_id: Option<String>,` field from all 9 structs in `crates/latiq-mcp/src/server.rs`: `AllocateArgs` (L40), `PondRefArgs` (L48), `ListArgs` (L54), `DropArgs` (L63), `QueryArgs` (L73), `SearchArgs` (L83), `LoadDatasetArgs` (L93), `CatalogDescribeArgs` (L107), `CatalogPullArgs` (L125).

- [ ] **Step 4: Read identity from the transport instead**

Add `ctx: RequestContext<RoleServer>` to each of the 10 tool handlers and replace `let id = Identity::claimed(a.agent_id.as_deref());` with `let id = self.identity(&ctx).await?;`, where:

```rust
    /// Identity for one MCP request. The claimed leaf comes from the
    /// `latiq-agent-id` HTTP header (NOT a tool argument -- the model must not
    /// be able to type it). With a verifier configured, a bearer token is
    /// required and verified.
    async fn identity(&self, ctx: &RequestContext<RoleServer>) -> Result<Identity, ErrorData> {
        let headers = ctx
            .extensions
            .get::<http::request::Parts>()
            .map(|p| &p.headers);
        let claimed = headers
            .and_then(|h| h.get("latiq-agent-id"))
            .and_then(|v| v.to_str().ok());
        let Some(v) = &self.verifier else {
            return Ok(Identity::claimed(claimed));
        };
        let token = headers
            .and_then(|h| h.get(http::header::AUTHORIZATION))
            .and_then(|v| v.to_str().ok())
            .and_then(latiq_auth::bearer)
            .ok_or_else(|| ErrorData::invalid_request("a bearer token is required", None))?;
        v.verify(token, claimed)
            .await
            .map_err(|e| ErrorData::invalid_request(e.to_string(), None))
    }
```

Note the limitation the survey turned up: rmcp injects `Parts` on the POST request path only, not the SSE/GET stream path. Per-request header identity is therefore correct for tool calls, which is all we need; do **not** try to cache identity per session in this task.

- [ ] **Step 5: Serve metadata and challenge unauthenticated requests**

In `serve_mcp_with_listener` (`crates/latiq-mcp/src/server.rs:506-521`), when auth is configured, add the well-known route and a middleware that 401s a request with no bearer token before it reaches the session manager:

```rust
    let mut router = axum::Router::new().nest_service("/mcp", service);
    if let Some(v) = &verifier {
        let doc = ProtectedResourceMetadata::new(&resource_url, &v.config().issuer);
        let challenge = challenge_header(&metadata_url);
        router = router
            .route(
                "/.well-known/oauth-protected-resource",
                axum::routing::get(move || {
                    let doc = doc.clone();
                    async move { axum::Json(doc) }
                }),
            )
            .layer(axum::middleware::from_fn(move |req, next| {
                let challenge = challenge.clone();
                async move { require_bearer(challenge, req, next).await }
            }));
    }
```

`require_bearer` returns `401` with the `WWW-Authenticate` header when the `Authorization` header is absent, and calls `next.run(req)` otherwise. It checks only *presence*; the handler does the cryptographic verification, so there is exactly one place that validates a token. Exempt the well-known path from the layer.

- [ ] **Step 6: Run the tests**

Run: `cargo test -p latiq-mcp && cargo test -p latiq --test mcp`
Expected: PASS. The existing MCP tests must pass unchanged — they never set `agent_id`.

- [ ] **Step 7: Update the agent-facing guidance**

`crates/latiq-mcp/src/resources.rs` and any prompt SOP text that tells an agent to pass `agent_id` must be updated, or agents will be instructed to send a field that no longer exists. Grep: `grep -rn "agent_id" crates/latiq-mcp/src/resources.rs docs/`.

- [ ] **Step 8: Commit**

```bash
git add crates/latiq-mcp crates/latiq/tests/mcp.rs
git commit -m "feat(auth)!: MCP identity moves from tool arguments to the transport

BREAKING: the agent_id tool argument is removed from all 9 tool schemas. The
claimed leaf id now travels as the latiq-agent-id HTTP header, and a verified
principal as an Authorization: Bearer token -- neither reachable by the model."
```

---

## Task 8: CLI and SDK send tokens

**Files:**
- Modify: `crates/latiq/src/main.rs` (server flags + client token)
- Modify: `crates/latiq-sdk/src/lib.rs`
- Test: `crates/latiq/tests/query_grpc.rs`

- [ ] **Step 1: Add the server flags**

`ServeArgs` (`crates/latiq/src/main.rs:178-194`) and `NodeAddArgs` (L208-232) each gain:

```rust
    /// OIDC issuer URL. Setting this turns on token verification for every
    /// surface on this process. Unset = relaxed claimed identity (dev/embedded).
    #[arg(long, env = "LATIQ_AUTH_ISSUER")]
    auth_issuer: Option<String>,

    /// The audience this deployment expects in a token (`aud`). Required with
    /// --auth-issuer: without it, a token minted for any other service that
    /// trusts the same IdP would be accepted here.
    #[arg(long, env = "LATIQ_AUTH_AUDIENCE")]
    auth_audience: Option<String>,

    /// JWKS URL. Defaults to <issuer>/protocol/openid-connect/certs when the
    /// issuer looks like Keycloak, otherwise <issuer>/.well-known/jwks.json.
    #[arg(long, env = "LATIQ_AUTH_JWKS_URI")]
    auth_jwks_uri: Option<String>,
```

Build the `AuthConfig` in `run_serve` (L735) and `run_node_add` (L758). **Fail fast**: if `auth_issuer` is set and `auth_audience` is not, exit with a clear error rather than defaulting — a wrong audience is a silent security hole.

- [ ] **Step 2: Add the client token**

The CLI's gRPC request builders (`crates/latiq/src/main.rs:832-838`, where `latiq-agent-id` is set) also attach `authorization: Bearer <token>` when `LATIQ_TOKEN` is set or `--token` is passed. Add `--token` alongside the existing `--agent-id` on `QueryArgs` (L304-317).

`crates/latiq-sdk/src/lib.rs` — `connect()` accepts `token=`, stored next to the cached channels and attached to each request's metadata. Do **not** put it in the `Channel`; tonic metadata is per-request.

- [ ] **Step 3: Test**

```rust
#[tokio::test]
async fn auth_cli_token_env_var_is_sent_as_a_bearer_header() {
    let idp = latiq_auth::test_support::TestIdp::start().await;
    let stack = common::start_stack_with_auth(idp.auth_config()).await;
    let token = idp.mint("svc-cli", "latiq", &idp.issuer, 300);
    // Drive the CLI's own request builder rather than hand-rolling metadata.
    let out = common::run_cli(&["pond", "list"], &[("LATIQ_TOKEN", &token), ("LATIQ_SERVER", &stack.control_endpoint)]);
    assert!(out.status.success(), "stderr: {}", String::from_utf8_lossy(&out.stderr));
}
```

Run: `cargo test -p latiq --test query_grpc auth_`
Expected: PASS.

- [ ] **Step 4: Commit**

```bash
git add crates/latiq crates/latiq-sdk
git commit -m "feat(auth): --auth-issuer/--auth-audience server flags and LATIQ_TOKEN on the client"
```

---

## Task 9: Keycloak in the internal compose

**Files:**
- Create: `deploy/cluster/keycloak-realm.json`
- Modify: `deploy/cluster/docker-compose.yml`
- Modify: `deploy/CLAUDE.md`

- [ ] **Step 1: Write the realm import**

`deploy/cluster/keycloak-realm.json` — a realm `latiq` with a confidential client `latiq-agent` (service accounts enabled = `client_credentials`), and **an audience mapper**. The mapper is essential and easy to miss: Keycloak does not put a custom `aud` in access tokens by default, so without it every token fails our audience check.

```json
{
  "realm": "latiq",
  "enabled": true,
  "clients": [
    {
      "clientId": "latiq-agent",
      "enabled": true,
      "publicClient": false,
      "secret": "latiq-agent-secret",
      "serviceAccountsEnabled": true,
      "standardFlowEnabled": false,
      "directAccessGrantsEnabled": true,
      "protocolMappers": [
        {
          "name": "latiq-audience",
          "protocol": "openid-connect",
          "protocolMapper": "oidc-audience-mapper",
          "config": {
            "included.custom.audience": "latiq",
            "access.token.claim": "true"
          }
        }
      ]
    }
  ]
}
```

- [ ] **Step 2: Add the service behind a profile**

`deploy/cluster/docker-compose.yml` — profiles belong on the internal compose only; `deploy/latiq-compose.yml` (the user-facing one) must not change.

```yaml
  keycloak:
    image: quay.io/keycloak/keycloak:26.0
    profiles: ["auth"]
    command: ["start-dev", "--import-realm"]
    environment:
      KC_BOOTSTRAP_ADMIN_USERNAME: admin
      KC_BOOTSTRAP_ADMIN_PASSWORD: admin
      KC_HTTP_PORT: "8080"
      # Pins the `iss` claim. Everything -- pond nodes inside the network and
      # test clients outside it -- must use this exact hostname, so `keycloak`
      # has to resolve on the host too (see Task 12 Step 1).
      KC_HOSTNAME_URL: http://keycloak:8080
    volumes:
      - ./keycloak-realm.json:/opt/keycloak/data/import/realm.json:ro
    ports:
      - "8080:8080"
```

No container healthcheck: recent Keycloak images ship without `curl`, and its health endpoint lives on a separate management port. CI polls the discovery URL from the runner instead (Task 10).

- [ ] **Step 3: Verify locally**

Because `KC_HOSTNAME_URL` pins the issuer to `http://keycloak:8080`, the name must resolve on your machine too. Once, on the dev box:

```bash
echo "127.0.0.1 keycloak" | sudo tee -a /etc/hosts
```

Then:

```bash
docker compose -f deploy/cluster/docker-compose.yml --profile auth up -d keycloak
until curl -sf http://keycloak:8080/realms/latiq/.well-known/openid-configuration >/dev/null; do sleep 2; done
curl -s -d grant_type=client_credentials -d client_id=latiq-agent -d client_secret=latiq-agent-secret \
  http://keycloak:8080/realms/latiq/protocol/openid-connect/token | python3 -c 'import sys,json,base64;
t=json.load(sys.stdin)["access_token"].split(".")[1]; t+="="*(-len(t)%4)
print(json.loads(base64.urlsafe_b64decode(t))["aud"])'
```
Expected: prints an audience list containing `latiq`. If it does not, the audience mapper is wrong — fix it here, not in Rust.

- [ ] **Step 4: Commit**

```bash
git add deploy/cluster/keycloak-realm.json deploy/cluster/docker-compose.yml deploy/CLAUDE.md
git commit -m "test(auth): Keycloak realm behind an auth profile on the internal compose"
```

---

## Task 10: Python SDK auth e2e

**Files:**
- Create: `e2e/sdk/test_auth.py`
- Modify: `e2e/CLAUDE.md`

- [ ] **Step 1: Write the test**

`e2e/sdk/test_auth.py` — skips itself when `LATIQ_AUTH_ISSUER` is unset, so the existing EMBEDDED and unauthenticated REMOTE runs are unaffected.

```python
"""Auth e2e against a REAL IdP (Keycloak). Proves the parts a hand-minted token
cannot: real discovery documents, real claim sets, a real client_credentials
grant. Skipped unless the cluster was brought up with auth."""
import os
import urllib.parse
import urllib.request
import json
import pytest
import latiq

ISSUER = os.environ.get("LATIQ_AUTH_ISSUER")
pytestmark = pytest.mark.skipif(not ISSUER, reason="cluster not running with auth")


def token(client_id="latiq-agent", secret="latiq-agent-secret"):
    body = urllib.parse.urlencode({
        "grant_type": "client_credentials",
        "client_id": client_id,
        "client_secret": secret,
    }).encode()
    url = f"{ISSUER}/protocol/openid-connect/token"
    with urllib.request.urlopen(urllib.request.Request(url, data=body)) as r:
        return json.load(r)["access_token"]


def test_auth_valid_token_can_allocate_and_query():
    c = latiq.connect(os.environ["LATIQ_GATEWAY"], token=token())
    pond = c.allocate_pond("auth-e2e")
    try:
        c.write_query(pond, "CREATE TABLE t AS SELECT 42 AS n")
        assert c.read_query(pond, "SELECT n FROM t").to_pylist() == [{"n": 42}]
    finally:
        c.drop_pond(pond, confirm=True)


def test_auth_missing_token_is_rejected():
    c = latiq.connect(os.environ["LATIQ_GATEWAY"])
    with pytest.raises(Exception) as e:
        c.allocate_pond("auth-e2e-no-token")
    assert "unauthenticated" in str(e.value).lower() or "bearer" in str(e.value).lower()


def test_auth_garbage_token_is_rejected():
    c = latiq.connect(os.environ["LATIQ_GATEWAY"], token="not.a.real.token")
    with pytest.raises(Exception):
        c.allocate_pond("auth-e2e-bad-token")
```

- [ ] **Step 2: Run against a local authenticated cluster**

```bash
docker compose -f deploy/cluster/docker-compose.yml --profile auth up -d
LATIQ_AUTH_ISSUER=http://keycloak:8080/realms/latiq \
LATIQ_GATEWAY=127.0.0.1:51500 pytest e2e/sdk/test_auth.py -v
```
Expected: 3 passed.

- [ ] **Step 3: Commit**

```bash
git add e2e/sdk/test_auth.py e2e/CLAUDE.md
git commit -m "test(auth): SDK e2e against a real Keycloak client_credentials grant"
```

---

## Task 11: MCP agent-harness auth e2e

**Background you need before touching this file.** `e2e/agent/` uses two clients: `ai@4.3.19`'s `experimental_createMCPClient` drives the tools, and `@modelcontextprotocol/sdk@1.29.0`'s `Client` drives resources and prompts (the AI SDK doesn't surface those). The Vercel AI SDK implements **no** OAuth of its own — its `ai@4.x` transport config is `{ type: 'sse', url, headers? }`, static headers only.

**That does not block us**, because of how the harness is already written: it hands `experimental_createMCPClient` a **transport instance it constructs itself**, and that transport is the official SDK's `StreamableHTTPClientTransport` — which does accept an `authProvider`. So one provider configures both clients, and the OAuth engine is the official SDK in both cases.

`ClientCredentialsProvider` (from `@modelcontextprotocol/sdk/client/auth-extensions.js`, added in **1.24.0**) runs non-interactively: with no `redirectUrl`, `auth()` skips the browser redirect and performs a `client_credentials` grant. `package.json` declares `^1.12.0` and resolves to 1.29.0, so no install changes — but tighten the range to `^1.24.0` to make the requirement explicit rather than accidental.

**Files:**
- Modify: `e2e/agent/package.json` (bump the `@modelcontextprotocol/sdk` range)
- Modify: `e2e/agent/harness.test.ts:36-41` (transport construction)
- Create: `e2e/agent/auth.test.ts`

- [ ] **Step 1: Tighten the SDK range**

`e2e/agent/package.json`: change `"@modelcontextprotocol/sdk": "^1.12.0"` to `"^1.24.0"`. Run `npm install --prefix e2e/agent` and commit the lockfile change.

- [ ] **Step 2: Build the transports through one optional provider**

In `e2e/agent/harness.test.ts`, replace the two bare transport constructions (L36-41) with a shared helper so both clients authenticate identically:

```ts
import { ClientCredentialsProvider } from "@modelcontextprotocol/sdk/client/auth-extensions.js";

/** One provider for both clients. Returns undefined when the cluster has no
 *  auth configured, so the existing unauthenticated runs are unchanged. */
function authProvider() {
  const issuer = process.env.LATIQ_AUTH_ISSUER;
  if (!issuer) return undefined;
  return new ClientCredentialsProvider({
    clientId: "latiq-agent",
    clientSecret: process.env.LATIQ_CLIENT_SECRET ?? "latiq-agent-secret",
    scope: "openid",
    expectedIssuer: issuer,
  });
}

function transport() {
  const provider = authProvider();
  // NOTE: never also set requestInit.headers.Authorization -- _commonHeaders()
  // spreads requestInit.headers AFTER the provider's header, so a hand-set
  // Authorization silently overrides the provider's token.
  return new StreamableHTTPClientTransport(
    new global.URL(URL_),
    provider ? { authProvider: provider } : undefined,
  );
}
```

Then `ai = await experimental_createMCPClient({ transport: transport() })` and `await raw.connect(transport())`.

- [ ] **Step 3: Write the auth-specific test**

`e2e/agent/auth.test.ts` — asserts the two things the unauthenticated harness cannot:

```ts
import { describe, it, expect } from "vitest";
import { Client } from "@modelcontextprotocol/sdk/client/index.js";
import { StreamableHTTPClientTransport } from "@modelcontextprotocol/sdk/client/streamableHttp.js";

const URL_ = process.env.LATIQ_MCP!;
const ISSUER = process.env.LATIQ_AUTH_ISSUER;

describe.skipIf(!ISSUER)("auth (MCP)", () => {
  it("auth_mcp_rejects_an_unauthenticated_client", async () => {
    const raw = new Client({ name: "latiq-auth-e2e", version: "0.0.0" });
    await expect(
      raw.connect(new StreamableHTTPClientTransport(new global.URL(URL_))),
    ).rejects.toThrow();
  });

  it("auth_mcp_discovers_the_authorization_server_from_our_metadata", async () => {
    // Proves the RFC 9728 document we serve is the one a real client reads.
    const base = new global.URL(URL_);
    const res = await fetch(
      `${base.protocol}//${base.host}/.well-known/oauth-protected-resource`,
    );
    expect(res.status).toBe(200);
    const doc = await res.json();
    expect(doc.authorization_servers[0]).toBe(ISSUER);
  });
});
```

The full authenticated tool loop is already covered: with `LATIQ_AUTH_ISSUER` set, every existing test in `harness.test.ts` runs through the authenticated transport. That is the point of Step 2 — we get authenticated coverage of all nine tools without duplicating a single assertion.

- [ ] **Step 4: Run it locally**

```bash
docker compose -f deploy/cluster/docker-compose.yml --profile auth up -d
LATIQ_AUTH_ISSUER=http://keycloak:8080/realms/latiq \
LATIQ_MCP=http://127.0.0.1:51510/mcp npm --prefix e2e/agent test
```
Expected: the existing harness tests pass through the authenticated transport, plus 2 new auth tests. Requires the `keycloak` hosts entry from Task 12 Step 1.

- [ ] **Step 5: Commit**

```bash
git add e2e/agent
git commit -m "test(auth): agent harness authenticates via the MCP SDK client_credentials provider"
```

---

## Task 12: Nightly wiring

**Files:**
- Modify: `.github/workflows/nightly.yml`

- [ ] **Step 1: Add an authenticated cluster job**

A separate job rather than a flag on `e2e-suite`: the unauthenticated path is the default deployment and must keep being tested exactly as it is today.

```yaml
  auth-e2e:
    name: e2e — authenticated cluster (Keycloak)
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      # ONE hostname for Keycloak everywhere. The `iss` claim inside a token is
      # whatever Keycloak was configured with, and both the pond nodes (inside
      # the compose network) and the test clients (on the runner) compare
      # against it. Two hostnames = a guaranteed issuer mismatch for one of
      # them, so make `keycloak` resolve on the runner too.
      - name: Make `keycloak` resolve on the runner
        run: echo "127.0.0.1 keycloak" | sudo tee -a /etc/hosts
      - name: Start Keycloak
        run: docker compose -f deploy/cluster/docker-compose.yml --profile auth up -d keycloak
      - name: Wait for the realm to import
        run: |
          for i in $(seq 1 60); do
            curl -sf http://keycloak:8080/realms/latiq/.well-known/openid-configuration >/dev/null && exit 0
            sleep 2
          done
          echo "::error::Keycloak realm did not come up"
          docker compose -f deploy/cluster/docker-compose.yml logs keycloak
          exit 1
      - name: Start the cluster with auth on
        env:
          LATIQ_AUTH_ISSUER: http://keycloak:8080/realms/latiq
          LATIQ_AUTH_AUDIENCE: latiq
        run: docker compose -f deploy/cluster/docker-compose.yml --profile auth up -d
      - name: SDK auth e2e
        env:
          LATIQ_AUTH_ISSUER: http://keycloak:8080/realms/latiq
          LATIQ_GATEWAY: 127.0.0.1:51500
        run: pytest e2e/sdk/test_auth.py -v
      - name: Agent (MCP) auth e2e
        env:
          LATIQ_AUTH_ISSUER: http://keycloak:8080/realms/latiq
          LATIQ_MCP: http://127.0.0.1:51510/mcp
        run: npm --prefix e2e/agent test
```

The compose service must pin the same hostname so the tokens it mints carry that issuer — add `KC_HOSTNAME_URL: http://keycloak:8080` to the `keycloak` service environment in Task 9 Step 2 and map `8080:8080` (already done).

- [ ] **Step 2: Add it to the publish gate**

Add `auth-e2e` to the `needs:` list of the publish job (`.github/workflows/nightly.yml:222`).

- [ ] **Step 3: Commit**

```bash
git add .github/workflows/nightly.yml
git commit -m "ci(auth): nightly e2e against an authenticated cluster"
```

---

## Task 13: Documentation

**Files:**
- Modify: `docs/identity.md`, `docs/dev.md`, `docs/roadmap.md`, `CLAUDE.md`, `crates/latiq-agent-core/CLAUDE.md`

- [ ] **Step 1: Update the docs**

- `docs/identity.md`: change the `Principal` code block to `Identity` (per the deviation note at the top of this plan), and mark the implemented parts **today**.
- `docs/dev.md`: how to run a local authenticated cluster (the Task 9 Step 3 commands).
- `docs/roadmap.md`: flip the "Identity v0 — authentication" row to ✅ Shipped.
- `CLAUDE.md`: add `latiq-auth` to the crate list; note that identity is verified when configured.
- `crates/latiq-agent-core/CLAUDE.md`: its invariant list says "Identity is relaxed (`Identity::claimed`, default anonymous)" — update to describe both modes.

- [ ] **Step 2: Full gate**

```bash
cargo fmt --all && cargo clippy --workspace --all-targets -- -D warnings && cargo test --workspace
```
Expected: clean, all tests pass.

- [ ] **Step 3: Commit**

```bash
git add docs CLAUDE.md crates/latiq-agent-core/CLAUDE.md
git commit -m "docs(auth): identity v0 is shipped; document authenticated deployment"
```

---

## Self-review notes

**Spec coverage vs `docs/identity.md`:** the flow (Tasks 4, 7), one-verifier-three-carriers (5, 6, 7), the `Identity` shape (1), unauthenticated mode preserved (1, 5, 7 — asserted, not assumed), the MCP carrier change (7), audience checking called out as critical (3, 8, 9). Authorization is correctly absent.

**Known gaps, deliberately deferred:** multiple issuers (one `AuthConfig`, not a list); token expiry mid-query (validated at admission only); dynamic client registration (Keycloak's realm import pre-registers the client, so RFC 7591 is untested); gateway-level verification (nodes verify, the gateway passes through).
