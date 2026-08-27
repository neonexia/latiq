# Identity v0 — Authentication Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make Latiq an OAuth 2.1 resource server so an enterprise IdP (Okta / Auth0 / Entra / Keycloak) can authenticate agents, the SDK, and operators across all three surfaces — with zero behaviour change when auth is not configured.

**Architecture:** A new `latiq-auth` crate owns JWKS fetching and JWT verification and knows nothing about transports. Each inbound adapter (MCP HTTP, Data/Stream gRPC, Admin gRPC) extracts a bearer token from its own carrier and calls the same verifier, satisfying invariant 5. `Identity` gains verified `subject`/`issuer` fields alongside the always-claimed `agent_id`. Authorization is **out of scope** — nothing gates on the result yet; it flows to attribution and the `latiq::access` trail.

**Tech Stack:** Rust, `jsonwebtoken` 9 (the only new dependency), `axum` 0.8 and `reqwest` (already in the tree), `tonic` interceptors, Keycloak 26 in Docker for the nightly e2e.

**Design source:** [`docs/identity.md`](../../identity.md). Read it before starting.

---

## Decisions taken before implementation

- **The type stays `Identity`.** An earlier draft of `docs/identity.md` called it
  `Principal`; the rename was dropped because it touches 12 source files and ~30
  test call sites for zero behaviour change. `docs/identity.md` has been updated to
  match, so the doc and this plan agree.
- **Execution is subagent-driven** (`superpowers:subagent-driven-development`): one
  fresh subagent per task, then a spec-compliance review and a code-quality review
  before the next task starts.
- **Branch:** `feat/identity-v0-authn`.

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
| `crates/latiq/src/main.rs` | `--auth-issuer` (repeatable) / `--auth-audience` server flags; `--token` / `LATIQ_TOKEN` client side |
| `dev.sh` | `--auth` flag: throwaway Keycloak in Docker, stack runs verified (debugging only) |
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

## Task 1b: Attribution and the access trail must not stamp a claimed value as verified

**Why this exists.** Found in review of Task 1, and it is a design hole in this plan, not a coding slip. Both consumers of `Identity` today emit the **claimed** `agent_id` right next to the `verified` flag, and neither emits `subject`:

- `crates/latiq-engine-duckdb/src/exec.rs:299-302` — `set_commit_message('{agent_id}', 'write_query', ...)`
- `crates/latiq-agent-core/src/ops.rs:709-711` — `agent = %identity.agent_id, verified = identity.verified`

Once tokens are verified, a caller holding a valid token for `sub=svc-lowpriv` can claim `agent_id: "svc-admin"`, and the DuckLake commit history — the artifact an operator actually reads to answer "who wrote this" — records `svc-admin` alongside `verified: true`. That is precisely the "claimed value made load-bearing" failure the design rule forbids. The `agent_id := subject` fallback compounds it: a reader cannot distinguish a fallback from an attacker-supplied leaf that happens to equal a subject.

It is currently unreachable (nothing constructs a verified identity yet), which is exactly why it must land **before** any surface can produce one — i.e. before Task 5.

**Files:**
- Modify: `crates/latiq-agent-core/src/ops.rs:699-717` (`audit`)
- Modify: `crates/latiq-engine-duckdb/src/exec.rs:296-305` (`set_commit_message`)
- Test: `crates/latiq-engine-duckdb/tests/engine_e2e.rs`, `crates/latiq-agent-core/tests/agent_ops.rs`

- [ ] **Step 1: Write the failing tests**

In `crates/latiq-engine-duckdb/tests/engine_e2e.rs`, assert that a verified write records the subject and issuer, not just the claimed leaf:

```rust
#[test]
fn attribution_records_the_verified_subject_not_only_the_claimed_leaf() {
    // A caller with a valid token for `svc-lowpriv` claims a flattering leaf id.
    // History must make the verified subject visible so the claim cannot pass
    // itself off as authenticated identity.
    let id = Identity::verified("svc-lowpriv", "https://idp.example/realms/latiq", Some("svc-admin"));
    // ... allocate a pond, run a write with `id`, then read pond.snapshots() ...
    let msg = latest_commit_message(&pond);
    assert!(msg.contains("svc-lowpriv"), "commit message must carry the verified subject: {msg}");
    assert!(msg.contains("https://idp.example/realms/latiq"), "must carry the issuer: {msg}");
}
```

Follow the existing attribution test in that file for pond setup and for how the commit message is read back; do not invent a new helper if one exists.

In `crates/latiq-agent-core/tests/agent_ops.rs`, assert the access trail carries the same fields. If the existing tests do not capture `tracing` output, use `tracing_subscriber`'s test capture (add it as a dev-dependency) rather than restructuring `audit()` to return a value — the emitter must stay a fire-and-forget trace.

- [ ] **Step 2: Run to verify failure**

Run: `cargo test -p latiq-engine-duckdb attribution_ && cargo test -p latiq-agent-core --test agent_ops`
Expected: FAIL — the commit message and the log line carry only `agent_id`.

- [ ] **Step 3: Implement**

`crates/latiq-agent-core/src/ops.rs`, in `audit()` — emit the verified pair as their own fields, so `verified` is unambiguously about `subject` and never about `agent`:

```rust
        info!(
            target: "latiq::access",
            agent = %identity.agent_id,          // CLAIMED. never authority.
            subject = %identity.subject,         // verified, or "" when not
            issuer = %identity.issuer,
            verified = identity.verified,        // scopes subject/issuer, NOT agent
            op = operation,
            pond = pond_id.unwrap_or("-"),
            duration_ms,
            summary = request_summary.as_deref().unwrap_or(""),
            "access",
        );
```

`crates/latiq-engine-duckdb/src/exec.rs` — the DuckLake commit author becomes the **verified subject when there is one**, falling back to the claimed leaf only for unauthenticated deployments, with the claimed leaf preserved in the structured extra info:

```rust
    // The author is the strongest identity we have: the verified subject when
    // the caller authenticated, the claimed leaf otherwise. The claimed leaf is
    // always recorded separately so history distinguishes the two.
    let author = if identity.verified { &identity.subject } else { &identity.agent_id };
```

Escape `author`, `issuer`, and `agent_id` for SQL exactly as the existing code escapes `agent` (`.replace('\'', "''")`), and put `agent_id`/`issuer`/`verified` into the `extra_info` JSON. **Never emit a bare `verified` next to a claimed field.**

- [ ] **Step 4: Run the tests**

Run: `cargo test -p latiq-engine-duckdb && cargo test -p latiq-agent-core`
Expected: PASS. The pre-existing `attribution_*` tests must still pass — unauthenticated behaviour is unchanged, since `verified` is false and `author` falls back to `agent_id`.

- [ ] **Step 5: Commit**

```bash
git add crates/latiq-agent-core crates/latiq-engine-duckdb
git commit -m "fix(identity): attribution and the access trail record the verified subject

A claimed agent_id must never appear next to a bare verified flag: a caller
holding a valid token for one subject could otherwise claim any leaf id and
have history record it as authenticated. The DuckLake commit author is now the
verified subject when present, with the claimed leaf kept separately."
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

## Task 3: Token verification (multi-issuer)

**Why N issuers from the start.** A realistic enterprise has two: a *workforce* IdP for operators using the CLI, and a *workload* IdP for machine agents. Supporting one and adding the second later means changing `AuthConfig`'s shape, the CLI flag arity, and the metadata document — all public surface. Supporting N now costs about fifteen extra lines of verification logic and one extra field, and the metadata document's `authorization_servers` is *already* an array in the spec. So: a list from day one, with one entry as the ordinary case.

**How key selection stays safe.** To pick the right JWKS we need the token's `iss`, which we can only read *before* verifying. That is safe as long as the unverified `iss` is used **only to select a key**, and the final validation still pins issuer and audience: a token claiming an issuer it wasn't signed by gets checked against that issuer's real keys and fails the signature. What we must never do is trust an unverified claim for anything else. Selecting per-issuer also avoids `kid` collisions between two IdPs that happen to use the same key id.

**Files:**
- Create: `crates/latiq-auth/src/verify.rs`
- Create: `crates/latiq-auth/tests/verify.rs`
- Modify: `crates/latiq-auth/src/lib.rs`
- Modify: `crates/latiq-auth/src/jwks.rs` (one cache per issuer)

- [ ] **Step 1: Write the failing tests**

`crates/latiq-auth/tests/verify.rs` — these are the tests a container cannot write better than we can:

```rust
mod support;
use latiq_auth::{AuthConfig, IssuerConfig, Verifier};

const AUD: &str = "latiq";

fn config(idp: &support::TestIdp) -> AuthConfig {
    AuthConfig {
        audience: AUD.to_string(),
        issuers: vec![IssuerConfig {
            issuer: idp.issuer.clone(),
            jwks_uri: Some(idp.jwks_uri.clone()),
        }],
    }
}

#[tokio::test]
async fn auth_valid_token_yields_a_verified_identity() {
    let idp = support::TestIdp::start().await;
    let v = Verifier::new(config(&idp));
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
    let v = Verifier::new(config(&idp));
    let token = idp.mint("svc-orch", "some-other-service", &idp.issuer, 300);
    assert!(v.verify(&token, None).await.is_err());
}

#[tokio::test]
async fn auth_rejects_an_unlisted_issuer() {
    let idp = support::TestIdp::start().await;
    let v = Verifier::new(config(&idp));
    let token = idp.mint("svc-orch", AUD, "https://evil.example", 300);
    assert!(v.verify(&token, None).await.is_err());
}

#[tokio::test]
async fn auth_rejects_expired_token() {
    let idp = support::TestIdp::start().await;
    let v = Verifier::new(config(&idp));
    let token = idp.mint("svc-orch", AUD, &idp.issuer, -60);
    assert!(v.verify(&token, None).await.is_err());
}

#[tokio::test]
async fn auth_rejects_foreign_signature() {
    let idp = support::TestIdp::start().await;
    let v = Verifier::new(config(&idp));
    let token = idp.mint_with_foreign_key("svc-orch", AUD, &idp.issuer);
    assert!(v.verify(&token, None).await.is_err());
}

#[tokio::test]
async fn auth_rejects_token_without_kid() {
    let idp = support::TestIdp::start().await;
    let v = Verifier::new(config(&idp));
    let token = idp.mint_with_kid("svc-orch", AUD, &idp.issuer, 300, None);
    assert!(v.verify(&token, None).await.is_err());
}

#[tokio::test]
async fn auth_rejects_garbage() {
    let idp = support::TestIdp::start().await;
    let v = Verifier::new(config(&idp));
    assert!(v.verify("not.a.token", None).await.is_err());
    assert!(v.verify("", None).await.is_err());
}

#[tokio::test]
async fn auth_leaf_agent_id_defaults_to_subject() {
    let idp = support::TestIdp::start().await;
    let v = Verifier::new(config(&idp));
    let token = idp.mint("svc-orch", AUD, &idp.issuer, 300);
    let id = v.verify(&token, None).await.expect("verify");
    assert_eq!(id.agent_id, "svc-orch");
}

// ---- multi-issuer

#[tokio::test]
async fn auth_accepts_tokens_from_either_configured_issuer() {
    // The workforce-IdP + workload-IdP case.
    let a = support::TestIdp::start().await;
    let b = support::TestIdp::start().await;
    let v = Verifier::new(AuthConfig {
        audience: AUD.to_string(),
        issuers: vec![
            IssuerConfig { issuer: a.issuer.clone(), jwks_uri: Some(a.jwks_uri.clone()) },
            IssuerConfig { issuer: b.issuer.clone(), jwks_uri: Some(b.jwks_uri.clone()) },
        ],
    });

    let ida = v.verify(&a.mint("ops-alice", AUD, &a.issuer, 300), None).await.expect("idp a");
    assert_eq!(ida.issuer, a.issuer);
    let idb = v.verify(&b.mint("svc-agent", AUD, &b.issuer, 300), None).await.expect("idp b");
    assert_eq!(idb.issuer, b.issuer);
}

#[tokio::test]
async fn auth_a_token_cannot_borrow_another_issuers_identity() {
    // Signed by IdP b, but CLAIMS to come from IdP a. Selecting the key by the
    // unverified `iss` must then check it against a's real keys -- and fail.
    let a = support::TestIdp::start().await;
    let b = support::TestIdp::start().await;
    let v = Verifier::new(AuthConfig {
        audience: AUD.to_string(),
        issuers: vec![
            IssuerConfig { issuer: a.issuer.clone(), jwks_uri: Some(a.jwks_uri.clone()) },
            IssuerConfig { issuer: b.issuer.clone(), jwks_uri: Some(b.jwks_uri.clone()) },
        ],
    });

    let forged = b.mint("svc-agent", AUD, &a.issuer, 300);  // b's key, a's iss
    assert!(v.verify(&forged, None).await.is_err());
}
```

- [ ] **Step 2: Run to verify failure**

Run: `cargo test -p latiq-auth --test verify`
Expected: FAIL to compile — `Verifier`, `AuthConfig`, and `IssuerConfig` do not exist.

- [ ] **Step 3: Implement**

`crates/latiq-auth/src/verify.rs`:

```rust
//! JWT claim validation. Algorithms are an ALLOWLIST, never taken from the
//! token header -- accepting the header's `alg` is the classic algorithm
//! confusion bug. Issuers are an allowlist too: an `iss` we do not know is
//! rejected before any key lookup happens.
use crate::jwks::JwksCache;
use crate::AuthError;
use jsonwebtoken::{decode, decode_header, Algorithm, Validation};
use latiq_common::Identity;
use serde::Deserialize;
use std::collections::HashMap;

/// Signature algorithms we accept. Asymmetric only: a symmetric alg would mean
/// the verifier holds a signing secret, which a resource server must not.
const ALLOWED_ALGS: &[Algorithm] =
    &[Algorithm::RS256, Algorithm::RS384, Algorithm::RS512, Algorithm::ES256];

#[derive(Debug, Clone)]
pub struct IssuerConfig {
    /// Compared as a STRING against the token's `iss`. Never dialed.
    pub issuer: String,
    /// The URL actually fetched for signing keys. `None` = derive it by OIDC
    /// discovery from `issuer`. An explicit value covers split-horizon
    /// deployments where the issuer identifier is not a reachable address.
    pub jwks_uri: Option<String>,
}

#[derive(Debug, Clone)]
pub struct AuthConfig {
    /// The `aud` this deployment expects. One value across all issuers: the
    /// audience names US, not who vouched for the caller.
    pub audience: String,
    /// Trusted issuers. One is the ordinary case; two covers the common
    /// workforce-IdP + workload-IdP split.
    pub issuers: Vec<IssuerConfig>,
}

#[derive(Debug, Deserialize)]
struct Claims {
    sub: String,
}

/// Only ever used to SELECT a key. Every field is unverified at this point.
#[derive(Debug, Deserialize)]
struct Untrusted {
    iss: String,
}

pub struct Verifier {
    cfg: AuthConfig,
    /// One cache per issuer -- two IdPs may legitimately use the same `kid`.
    jwks: HashMap<String, JwksCache>,
}

impl Verifier {
    pub fn new(cfg: AuthConfig) -> Self {
        let jwks = cfg
            .issuers
            .iter()
            .map(|i| {
                let uri = i
                    .jwks_uri
                    .clone()
                    .unwrap_or_else(|| JwksCache::discover_uri(&i.issuer));
                (i.issuer.clone(), JwksCache::new(uri))
            })
            .collect();
        Self { cfg, jwks }
    }

    pub fn config(&self) -> &AuthConfig {
        &self.cfg
    }

    /// Verify a bearer token and build a verified Identity. `claimed_agent` is
    /// the caller-asserted leaf id -- never verified, attribution only.
    pub async fn verify(
        &self,
        token: &str,
        claimed_agent: Option<&str>,
    ) -> Result<Identity, AuthError> {
        if token.trim().is_empty() {
            return Err(AuthError::Missing);
        }
        let header = decode_header(token).map_err(|e| AuthError::Malformed(e.to_string()))?;
        if !ALLOWED_ALGS.contains(&header.alg) {
            return Err(AuthError::Invalid(format!(
                "algorithm {:?} not allowed",
                header.alg
            )));
        }
        let kid = header.kid.ok_or_else(|| {
            AuthError::Malformed("token header has no 'kid'; cannot select a signing key".into())
        })?;

        // Read `iss` WITHOUT verifying, purely to choose which issuer's keys to
        // check against. Safe because the real validation below pins iss and
        // aud: a token claiming an issuer it wasn't signed by fails signature.
        let iss = untrusted_issuer(token)?;
        let cache = self
            .jwks
            .get(&iss)
            .ok_or_else(|| AuthError::Invalid(format!("issuer '{iss}' is not trusted here")))?;
        let key = cache.key_for(&kid).await?;

        let mut validation = Validation::new(header.alg);
        validation.set_audience(&[self.cfg.audience.as_str()]);
        validation.set_issuer(&[iss.as_str()]);
        validation.set_required_spec_claims(&["exp", "aud", "iss", "sub"]);

        let data =
            decode::<Claims>(token, &key, &validation).map_err(|e| AuthError::Invalid(e.to_string()))?;

        // A blank `sub` would produce an identity whose author field is empty,
        // and `Identity::verified` only guards this with a debug_assert -- which
        // is nothing in release. Reject it here, where it is a real runtime
        // condition: a token with no usable subject is not a usable identity.
        if data.claims.sub.trim().is_empty() {
            return Err(AuthError::Invalid("token has an empty 'sub' claim".into()));
        }
        Ok(Identity::verified(&data.claims.sub, &iss, claimed_agent))
    }
}

/// Decode the payload without verifying, to read `iss` for key selection only.
fn untrusted_issuer(token: &str) -> Result<String, AuthError> {
    use base64::Engine;
    let payload = token
        .split('.')
        .nth(1)
        .ok_or_else(|| AuthError::Malformed("token is not a JWT".into()))?;
    let bytes = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .decode(payload)
        .map_err(|e| AuthError::Malformed(format!("payload is not base64url: {e}")))?;
    let u: Untrusted = serde_json::from_slice(&bytes)
        .map_err(|e| AuthError::Malformed(format!("payload has no usable 'iss': {e}")))?;
    Ok(u.iss)
}
```

Add `base64 = "0.22"` to `latiq-auth`'s **regular** dependencies (it was gated behind `test-support` in Task 2 for the fixture; it is now needed at runtime).

**Also validate the config, which is where a misconfiguration becomes an auth bypass.** Add a constructor that rejects bad input rather than trusting it:

- **A non-`https` `jwks_uri` is a total auth bypass.** Signing keys fetched over plaintext can be replaced by anyone on-path, letting them mint arbitrary identities. Reject any `jwks_uri` (explicit or derived) whose scheme is not `https`, **unless** the host is a loopback address — tests and `./dev.sh --auth` legitimately use `http://127.0.0.1` and `http://localhost`. Raised in review of Task 2 and deferred to here on purpose: this is config validation, not cache behaviour.
- Reject an empty `audience`, an empty `issuers` list where auth is meant to be on, and a duplicate issuer entry.

Test each rejection. `auth_rejects_a_plaintext_jwks_uri` and `auth_allows_plaintext_jwks_on_loopback` are the two that matter.

Add to `crates/latiq-auth/src/jwks.rs`:

```rust
impl JwksCache {
    /// The conventional JWKS location for an issuer, used when no explicit
    /// `jwks_uri` is configured. Keycloak and most OIDC providers serve
    /// `<issuer>/protocol/openid-connect/certs`; the generic OIDC discovery
    /// document lives at `<issuer>/.well-known/openid-configuration`.
    pub fn discover_uri(issuer: &str) -> String {
        let base = issuer.trim_end_matches('/');
        format!("{base}/protocol/openid-connect/certs")
    }
}
```

Add to `crates/latiq-auth/src/lib.rs`:

```rust
pub mod verify;
pub use verify::{AuthConfig, IssuerConfig, Verifier};
```

- [ ] **Step 4: Run the tests**

Run: `cargo test -p latiq-auth`
Expected: PASS, 13 tests total (3 JWKS + 10 verify).

- [ ] **Step 5: Commit**

```bash
git add crates/latiq-auth
git commit -m "feat(auth): verify bearer tokens against an issuer allowlist, audience, expiry, and allowed algorithms"
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
fn auth_metadata_document_advertises_every_authorization_server() {
    let doc = ProtectedResourceMetadata::new(
        "http://node-1:51402/mcp",
        &["https://idp.example/realms/latiq".to_string()],
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
    /// `authorization_servers` mirrors the configured issuer allowlist -- the
    /// field is an array in RFC 9728 precisely because a resource may trust
    /// more than one.
    pub fn new(resource: &str, authorization_servers: &[String]) -> Self {
        Self {
            resource: resource.to_string(),
            authorization_servers: authorization_servers.to_vec(),
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

**Node-to-node forwarding also carries identity**, and today it does not carry enough. `crates/latiq-pond-node/src/forward_client.rs:69` (`with_identity`) re-injects only the `latiq-agent-id` header on the forwarded hop, so a verified identity arrives at the owning node **downgraded to claimed** — fail-safe for authority, but it silently loses `subject`/`issuer` and would make the owning node's attribution wrong. Forward the original `Authorization` header instead, so the owning node verifies the same token itself. Do NOT invent a trusted internal header that asserts `verified: true` across the hop — that would be exactly the trust laundering Task 1b exists to prevent. Add a test that a forwarded write records the verified subject on the owning node.

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
        let servers: Vec<String> =
            v.config().issuers.iter().map(|i| i.issuer.clone()).collect();
        let doc = ProtectedResourceMetadata::new(&resource_url, &servers);
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

Two separate problems in the same files.

**(a) The removed argument.** `crates/latiq-mcp/src/resources.rs` and any prompt SOP text that tells an agent to pass `agent_id` must be updated, or agents will be instructed to send a field that no longer exists. Grep: `grep -rn "agent_id" crates/latiq-mcp/src/resources.rs docs/`.

**(b) The history recipes read the wrong columns.** Found in review of Task 1b. `crates/latiq-mcp/src/resources.rs` (around L25, L41, L96) and `docs/dev.md` (~L183) all tell agents and operators to read history with:

```sql
SELECT author, commit_message FROM ducklake_snapshots(...)
```

After Task 1b the verified-vs-claimed evidence lives in the **`commit_extra_info`** column, which no recipe mentions — so a reader following the shipped guidance sees `author=svc-admin` for both a genuinely verified `svc-admin` and an unauthenticated caller merely claiming it. The data distinguishes them; the documented way of reading it does not. Update every recipe to select `commit_extra_info` as well.

Note the exact column name is **`commit_extra_info`**, not `extra_info` — state it correctly so the snippets are copy-pasteable.

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
    /// Trusted OIDC issuer URL. Repeatable: pass it more than once, or set
    /// LATIQ_AUTH_ISSUER to a comma-separated list, to trust several IdPs (the
    /// usual case being a workforce IdP for operators plus a workload IdP for
    /// agents). Any issuer here turns on verification for every surface on this
    /// process. None = relaxed claimed identity (dev / embedded).
    #[arg(long, env = "LATIQ_AUTH_ISSUER", value_delimiter = ',')]
    auth_issuer: Vec<String>,

    /// The audience this deployment expects in a token (`aud`). Required
    /// whenever an issuer is set: without it, a token minted for any other
    /// service that trusts the same IdP would be accepted here. One value for
    /// all issuers -- the audience names US, not who vouched for the caller.
    #[arg(long, env = "LATIQ_AUTH_AUDIENCE")]
    auth_audience: Option<String>,

    /// Explicit JWKS URL, overriding the default derived from the issuer. Only
    /// valid with exactly ONE --auth-issuer, since it cannot be matched to a
    /// particular issuer otherwise. Needed for split-horizon deployments where
    /// the issuer identifier is not a reachable address.
    #[arg(long, env = "LATIQ_AUTH_JWKS_URI")]
    auth_jwks_uri: Option<String>,
```

Build the `AuthConfig` in `run_serve` (L735) and `run_node_add` (L758). **Fail fast** on both of these, with a clear message rather than a default:

- an issuer is set but `auth_audience` is not — a wrong or absent audience is a silent security hole;
- `auth_jwks_uri` is set alongside more than one issuer — ambiguous, and guessing which issuer it belongs to would be worse than refusing.

- [ ] **Step 1a: Treat a blank issuer as absent**

This one line is what lets the cluster run from a **single** compose file. Compose interpolation (`${LATIQ_AUTH_ISSUER:-}`) always sets the variable, just empty, and clap's `env =` reports an empty string as `Some("")` — which would switch auth on with a meaningless issuer. Normalize at the boundary:

```rust
/// A blank value means "not set". Compose always passes the variable through
/// (possibly empty), so an empty string must mean auth off, not a broken issuer.
fn non_blank(v: Option<String>) -> Option<String> {
    v.map(|s| s.trim().to_string()).filter(|s| !s.is_empty())
}
```

Apply it to `auth_audience` and `auth_jwks_uri`, and filter `auth_issuer` with the same rule — `LATIQ_AUTH_ISSUER=` must yield an **empty** issuer list (auth off), not a one-element list containing `""`.

Test it, because this is exactly the sort of thing that silently regresses:

```rust
#[test]
fn auth_blank_env_values_mean_auth_is_off() {
    assert_eq!(non_blank(Some(String::new())), None);
    assert_eq!(non_blank(Some("   ".into())), None);
    assert_eq!(non_blank(None), None);
    assert_eq!(non_blank(Some(" https://idp ".into())), Some("https://idp".to_string()));
}
```

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
## Task 9: Keycloak and the test runners — one compose file, env-driven

**Auth is nightly-only and container-only.** Nothing in daily development runs in auth mode — `./dev.sh`, `cargo test`, and a plain `docker compose up` all stay exactly as they are. And because the test clients are containers on the same network as Keycloak and the pond nodes, **everything resolves `keycloak:8080` through Docker DNS**: one issuer, no host-versus-container address split to reconcile.

**One file, no override, no second invocation form.** Two Compose mechanisms do the work together:

- **`profiles:`** gates whether a *service* starts, and the profile list can be set from the environment via `COMPOSE_PROFILES` — no `--profile` flag needed.
- **Interpolation** (`${VAR:-default}`) injects host environment variables into an *existing* service's definition. This is what turns auth on for the pond nodes; profiles cannot do it, which is the only reason an override file ever looked necessary.

`deploy/cluster/docker-compose.yml` stays the single source of truth, and `deploy/cluster/auth.env` carries the settings so the command line stays short.

**Files:**
- Create: `deploy/cluster/keycloak-realm.json`
- Create: `deploy/cluster/auth.env`
- Modify: `deploy/cluster/docker-compose.yml`
- Modify: `deploy/CLAUDE.md`

- [ ] **Step 1: Write the realm import**

`deploy/cluster/keycloak-realm.json` — realm `latiq` with a confidential client `latiq-agent` (service accounts on = `client_credentials`), and **an audience mapper**. The mapper is essential and easy to miss: Keycloak does not put a custom `aud` in access tokens by default, so without it every token fails our audience check.

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

- [ ] **Step 2: Add the auth env to the pond nodes**

In `deploy/cluster/docker-compose.yml`, add two lines to the `environment:` block of `pond-node-1` (L34-35), `pond-node-2` (L50-51), and `pond-node-3` (L69-70). Interpolation with an empty default means an ordinary `docker compose up` passes a blank issuer, which the binary treats as "no auth" (Task 8 Step 1a):

```yaml
    environment:
      LATIQ_SERVER: http://control-plane:51400
      # Blank unless auth mode. Set by auth.env; a blank issuer = auth off.
      LATIQ_AUTH_ISSUER: ${LATIQ_AUTH_ISSUER:-}
      LATIQ_AUTH_AUDIENCE: ${LATIQ_AUTH_AUDIENCE:-latiq}
```

- [ ] **Step 3: Add Keycloak and the two test runners behind the `auth` profile**

Append to the same file, alongside the existing `test` / `scale` / `tools` profile services:

```yaml
  # ---- Auth mode (`auth` profile). Nightly + container-only: nothing here runs
  # in normal development. Every party -- pond nodes and test clients alike --
  # is on this network and reaches Keycloak as `keycloak:8080`, so there is one
  # issuer and no host/container address split.
  keycloak:
    image: quay.io/keycloak/keycloak:26.0
    hostname: keycloak
    profiles: ["auth"]
    command: ["start-dev", "--import-realm"]
    environment:
      KC_BOOTSTRAP_ADMIN_USERNAME: admin
      KC_BOOTSTRAP_ADMIN_PASSWORD: admin
      KC_HTTP_PORT: "8080"
      # Pins the `iss` claim. Every party uses this exact URL.
      KC_HOSTNAME_URL: http://keycloak:8080
    volumes:
      - ./keycloak-realm.json:/opt/keycloak/data/import/realm.json:ro
    # No published ports: nothing outside the network needs to reach it. Add
    # `ports: ["8080:8080"]` temporarily if you want the admin console.

  # One-shot test runners, same in-network pattern as the `cli` service.
  #   docker compose --env-file auth.env run --rm auth-tests-sdk
  auth-tests-sdk:
    image: python:3.11-slim
    profiles: ["auth"]
    restart: "no"
    working_dir: /repo
    volumes:
      - ../..:/repo
    environment:
      LATIQ_AUTH_ISSUER: ${LATIQ_AUTH_ISSUER:-}
      LATIQ_GATEWAY: gateway:51500
      LATIQ_SERVER: http://control-plane:51400
    command:
      - sh
      - -c
      - |
        set -e
        pip install -q /repo/dist/*.whl -r /repo/e2e/sdk/requirements.txt
        pytest /repo/e2e/sdk/test_auth.py -v
    depends_on: [gateway, keycloak]

  auth-tests-agent:
    image: node:22-slim
    profiles: ["auth"]
    restart: "no"
    working_dir: /repo/e2e/agent
    volumes:
      - ../..:/repo
    environment:
      LATIQ_AUTH_ISSUER: ${LATIQ_AUTH_ISSUER:-}
      LATIQ_MCP: http://gateway:51510/mcp
    command:
      - sh
      - -c
      - |
        set -e
        npm ci
        npm test
    depends_on: [gateway, keycloak]
```

The SDK runner installs the wheel from `/repo/dist`, so **the wheel must be built before this runs** — CI already builds it for the other e2e jobs.

- [ ] **Step 4: Write the env file**

`deploy/cluster/auth.env` — Compose reads `COMPOSE_PROFILES` from an env file just like any other setting, so this one file both selects the profile and supplies the settings:

```bash
# Auth mode for the cluster compose. Usage:
#   docker compose --env-file auth.env up -d
# Without it, `docker compose up -d` is the ordinary unauthenticated cluster.
COMPOSE_PROFILES=auth
LATIQ_AUTH_ISSUER=http://keycloak:8080/realms/latiq
LATIQ_AUTH_AUDIENCE=latiq
```

- [ ] **Step 5: Verify the realm and the audience mapper**

The audience mapper is the single most likely thing to be wrong, and it fails as an opaque "token rejected". Check it directly, from inside the network:

```bash
cd deploy/cluster
docker compose --env-file auth.env up -d keycloak
docker compose --env-file auth.env run --rm --entrypoint sh auth-tests-sdk -c '
  pip install -q requests
  python - <<PY
import base64, json, requests
t = requests.post("http://keycloak:8080/realms/latiq/protocol/openid-connect/token",
    data={"grant_type":"client_credentials","client_id":"latiq-agent",
          "client_secret":"latiq-agent-secret"}).json()["access_token"]
p = t.split(".")[1]; p += "=" * (-len(p) % 4)
c = json.loads(base64.urlsafe_b64decode(p))
print("iss:", c["iss"]); print("aud:", c["aud"])
PY'
```
Expected: `iss: http://keycloak:8080/realms/latiq` and an `aud` containing `latiq`. If `aud` is missing, the mapper is wrong — fix it here, not in Rust.

- [ ] **Step 6: Confirm the default path is unchanged**

```bash
cd deploy/cluster
docker compose up -d
docker compose ps --services   # must NOT list keycloak / auth-tests-*
docker compose logs pond-node-1 | grep -i auth   # must show nothing
```
Expected: the ordinary 2-node unauthenticated cluster, exactly as before.

- [ ] **Step 7: Commit**

```bash
git add deploy/cluster/keycloak-realm.json deploy/cluster/auth.env deploy/cluster/docker-compose.yml deploy/CLAUDE.md
git commit -m "test(auth): Keycloak and in-network test runners behind the auth profile"
```

---

## Task 9a: `./dev.sh --auth` for debugging

`./dev.sh` must stay **unauthenticated by default** — it is the inner development loop and nothing about it changes. But when a nightly auth failure needs reproducing, spinning up the whole compose cluster to poke at one node is heavy. An `--auth` flag gives the same native dev stack with verification on.

**This case is simpler than the cluster**, because `dev.sh` runs the binaries natively on the host: Keycloak is published on `localhost:8080` and *everything* — nodes and clients alike — uses that one address. No Docker DNS, no split, no override.

**Files:**
- Modify: `dev.sh` (flag parsing at L41-51, usage at L16-39, and the node/control-plane launch)

- [ ] **Step 1: Add the flag**

In the `while` loop at `dev.sh:41-51`, alongside `--nodes` / `--root` / `--down`:

```bash
    --auth)      AUTH=1;        shift ;;
```

Initialize `AUTH=0` next to the other defaults (L9-14), and add to `usage()`:

```
  --auth                Start Keycloak in Docker and run the stack with token
                        verification on. Debugging only -- auth is otherwise
                        exercised only by the nightly. Requires Docker.
```

- [ ] **Step 2: Start Keycloak and export the settings**

After argument validation (around L53) and before the control plane starts:

```bash
KC_PORT=8080
KC_NAME=latiq-dev-keycloak
if [[ "$AUTH" == "1" ]]; then
  command -v docker >/dev/null || {
    echo "--auth needs Docker (it runs Keycloak in a container)" >&2; exit 2; }

  if ! docker ps --format '{{.Names}}' | grep -qx "$KC_NAME"; then
    echo "Starting Keycloak on :$KC_PORT ..."
    docker run -d --rm --name "$KC_NAME" -p "$KC_PORT:8080" \
      -e KC_BOOTSTRAP_ADMIN_USERNAME=admin \
      -e KC_BOOTSTRAP_ADMIN_PASSWORD=admin \
      -e KC_HOSTNAME_URL="http://localhost:$KC_PORT" \
      -v "$PWD/deploy/cluster/keycloak-realm.json:/opt/keycloak/data/import/realm.json:ro" \
      quay.io/keycloak/keycloak:26.0 start-dev --import-realm >/dev/null
  fi

  echo -n "Waiting for the latiq realm "
  for _ in $(seq 1 60); do
    curl -sf "http://localhost:$KC_PORT/realms/latiq/.well-known/openid-configuration" \
      >/dev/null && break
    echo -n "."; sleep 2
  done
  echo

  # Native processes on the host, so ONE address works for everyone.
  export LATIQ_AUTH_ISSUER="http://localhost:$KC_PORT/realms/latiq"
  export LATIQ_AUTH_AUDIENCE=latiq
fi
```

`dev.sh` launches the control plane and nodes as children, so exporting these is enough — the existing `serve` / `node add` invocations need no new arguments.

Note `KC_HOSTNAME_URL` here is `localhost:8080`, **not** the `keycloak:8080` the compose file uses. Both are correct for their context: compose clients are containers, `dev.sh` clients are host processes.

- [ ] **Step 3: Tear it down with the stack**

`--auth` must not leave a container running. In the existing EXIT trap and the `--down` path (see the PID-file cleanup documented at `dev.sh:83-88`):

```bash
docker rm -f "$KC_NAME" >/dev/null 2>&1 || true
```

Unconditional and error-suppressed, so `--down` cleans up a Keycloak left behind by a hard-killed run — the same reasoning the PID file already follows.

- [ ] **Step 4: Print how to get a token**

The stack banner is useless in auth mode without one. Add, when `AUTH=1`:

```bash
  cat <<BANNER
Auth is ON. Get a token with:
  export LATIQ_TOKEN=\$(curl -s -d grant_type=client_credentials \\
    -d client_id=latiq-agent -d client_secret=latiq-agent-secret \\
    http://localhost:$KC_PORT/realms/latiq/protocol/openid-connect/token \\
    | python3 -c 'import sys,json;print(json.load(sys.stdin)["access_token"])')
Then the CLI and SDK pick it up automatically.
BANNER
```

- [ ] **Step 5: Verify both modes**

```bash
./dev.sh --nodes 2                 # unauthenticated -- must be unchanged
# in another shell:
latiq pond list                    # succeeds with no token

./dev.sh --nodes 2 --auth          # authenticated
latiq pond list                    # must FAIL: unauthenticated
export LATIQ_TOKEN=$(...)          # per the banner
latiq pond list                    # must SUCCEED
```

Then `./dev.sh --down` and confirm `docker ps` shows no `latiq-dev-keycloak`.

- [ ] **Step 6: Commit**

```bash
git add dev.sh
git commit -m "feat(dev): ./dev.sh --auth runs the local stack against a throwaway Keycloak"
```

---

## Task 10: Python SDK auth e2e

**Files:**
- Create: `e2e/sdk/test_auth.py`
- Modify: `e2e/CLAUDE.md`

- [ ] **Step 1: Write the test**

`e2e/sdk/test_auth.py` — skips itself when `LATIQ_AUTH_ISSUER` is unset, so the existing EMBEDDED and unauthenticated REMOTE runs are untouched. It only ever runs inside `auth-tests-sdk`.

```python
"""Auth e2e against a REAL IdP (Keycloak). Proves what a hand-minted token
cannot: real discovery documents, real claim sets, a real client_credentials
grant. Runs ONLY inside the auth-tests-sdk container, on the compose network."""
import json
import os
import urllib.parse
import urllib.request

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
    msg = str(e.value).lower()
    assert "unauthenticated" in msg or "bearer" in msg


def test_auth_garbage_token_is_rejected():
    c = latiq.connect(os.environ["LATIQ_GATEWAY"], token="not.a.real.token")
    with pytest.raises(Exception):
        c.allocate_pond("auth-e2e-bad-token")
```

- [ ] **Step 2: Run it**

```bash
cd deploy/cluster
docker compose --env-file auth.env up -d
docker compose --env-file auth.env run --rm auth-tests-sdk
```
Expected: 3 passed. (`docker compose run` starts `depends_on` services automatically.)

- [ ] **Step 3: Commit**

```bash
git add e2e/sdk/test_auth.py e2e/CLAUDE.md
git commit -m "test(auth): SDK e2e against a real Keycloak client_credentials grant"
```

---

## Task 11: MCP agent-harness auth e2e

**Background you need before touching this file.** `e2e/agent/` uses two clients: `ai@4.3.19`'s `experimental_createMCPClient` drives the tools, and `@modelcontextprotocol/sdk@1.29.0`'s `Client` drives resources and prompts (the AI SDK doesn't surface those). The Vercel AI SDK implements **no** OAuth of its own — its `ai@4.x` transport config is `{ type: 'sse', url, headers? }`, static headers only.

**That does not block us**, because of how the harness is already written: it hands `experimental_createMCPClient` a **transport instance it constructs itself**, and that transport is the official SDK's `StreamableHTTPClientTransport` — which does accept an `authProvider`. So one provider configures both clients, and the OAuth engine is the official SDK either way.

`ClientCredentialsProvider` (from `@modelcontextprotocol/sdk/client/auth-extensions.js`, added in **1.24.0**) runs non-interactively: with no `redirectUrl`, `auth()` skips the browser redirect and performs a `client_credentials` grant. `package.json` declares `^1.12.0` and resolves to 1.29.0, so no install change is strictly needed — but tighten the range to `^1.24.0` to make the requirement explicit rather than accidental.

**Files:**
- Modify: `e2e/agent/package.json`
- Modify: `e2e/agent/harness.test.ts:36-41`
- Create: `e2e/agent/auth.test.ts`

- [ ] **Step 1: Tighten the SDK range**

`e2e/agent/package.json`: change `"@modelcontextprotocol/sdk": "^1.12.0"` to `"^1.24.0"`. Run `npm install --prefix e2e/agent` and commit the lockfile change.

- [ ] **Step 2: Build both transports through one optional provider**

In `e2e/agent/harness.test.ts`, replace the two bare transport constructions (L36-41):

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

`e2e/agent/auth.test.ts`:

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

The full authenticated tool loop needs no new assertions: with `LATIQ_AUTH_ISSUER` set, every existing test in `harness.test.ts` already runs through the authenticated transport. That is the point of Step 2 — authenticated coverage of all nine tools without duplicating anything.

- [ ] **Step 4: Run it**

```bash
cd deploy/cluster
docker compose --env-file auth.env run --rm auth-tests-agent
```
Expected: the existing harness tests pass through the authenticated transport, plus 2 new auth tests.

- [ ] **Step 5: Commit**

```bash
git add e2e/agent
git commit -m "test(auth): agent harness authenticates via the MCP SDK client_credentials provider"
```

---

## Task 12: Nightly wiring

**Files:**
- Modify: `.github/workflows/nightly.yml`

- [ ] **Step 1: Add the authenticated-cluster job**

A separate job, not a flag on `e2e-suite`: the unauthenticated path is the default deployment and must keep being tested exactly as it is today. Every client here is a container on the compose network, so the workflow needs no issuer/gateway env of its own — it all lives in `auth.env`.

```yaml
  auth-e2e:
    name: e2e — authenticated cluster (Keycloak, in-network)
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
        with:
          python-version: "3.11"
      - name: Install protoc
        run: sudo apt-get update && sudo apt-get install -y protobuf-compiler
      - uses: dtolnay/rust-toolchain@stable
      - uses: Swatinem/rust-cache@v2
      # The SDK test runner installs this wheel from /repo/dist.
      - name: Build the wheel
        run: |
          python -m pip install -U pip maturin
          maturin build --release -m sdk/python/Cargo.toml -o dist
      - name: Build the latiq image
        run: docker build -f deploy/cluster/Dockerfile -t ghcr.io/neonexia/latiq:dev .
      - name: Bring up the authenticated cluster
        working-directory: deploy/cluster
        run: docker compose --env-file auth.env up -d
      - name: SDK auth e2e (in-network container)
        working-directory: deploy/cluster
        run: docker compose --env-file auth.env run --rm auth-tests-sdk
      - name: Agent (MCP) auth e2e (in-network container)
        working-directory: deploy/cluster
        run: docker compose --env-file auth.env run --rm auth-tests-agent
      - name: Dump logs on failure
        if: failure()
        working-directory: deploy/cluster
        run: docker compose --env-file auth.env logs --no-color
```

Match the image build/tag step to whatever the existing `cluster-scale-out` job does — do not invent a second way to build the image.

- [ ] **Step 2: Add it to the publish gate**

Add `auth-e2e` to the `needs:` list of the publish job (`.github/workflows/nightly.yml:222`).

- [ ] **Step 3: Commit**

```bash
git add .github/workflows/nightly.yml
git commit -m "ci(auth): nightly e2e against an authenticated cluster, clients in-network"
```

---

## Task 13: Documentation

**Files:**
- Modify: `docs/identity.md`, `docs/dev.md`, `docs/roadmap.md`, `CLAUDE.md`, `crates/latiq-agent-core/CLAUDE.md`, `e2e/CLAUDE.md`, `dev.sh` usage text

- [ ] **Step 1: Update the docs**

- `docs/identity.md`: mark the implemented parts **today**, and update the attribution description — it still says the id "rides DuckLake's native `set_commit_message`" as a single claimed string. After Task 1b the author is the *verified subject* when present, with the claimed leaf and issuer in `commit_extra_info`.
- `docs/dev.md`: document `./dev.sh --auth` (Task 9a) and state plainly that **auth mode is otherwise nightly-and-container-only** — `./dev.sh`, `cargo test`, and a plain `docker compose up` all stay unauthenticated. Include the `docker compose --env-file auth.env up -d` invocation for the rare case someone needs to reproduce a nightly auth failure locally.
- `e2e/CLAUDE.md`: add a third mode alongside REMOTE and EMBEDDED — **AUTH**, selected by `--env-file auth.env`, with every client in-network.
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

**Known gaps, deliberately deferred:** token expiry mid-query (validated at admission only); dynamic client registration (Keycloak's realm import pre-registers the client, so RFC 7591 is untested); gateway-level verification (nodes verify, the gateway passes through).

**Multiple issuers: supported from the start.** `AuthConfig.issuers` is a list, `--auth-issuer` is repeatable, and the metadata document advertises all of them. The cost was ~15 lines; retrofitting would have been a breaking change to the config shape, the flag arity, and the published metadata. Key selection reads the *unverified* `iss` only to pick which issuer's JWKS to check against, and the final validation still pins issuer and audience -- `auth_a_token_cannot_borrow_another_issuers_identity` proves a token signed by IdP B cannot claim IdP A.

**`./dev.sh` stays unauthenticated by default** (Task 9a). `--auth` is a debugging affordance, not a mode anyone works in; it needs no address split because the binaries run natively on the host, so everything uses `localhost:8080`.

**Testing posture, decided:** auth runs **only** in the nightly, **only** in containers, with every client on the compose network. Consequences accepted: (a) there is no host-side auth test, so a bug that only manifests for a client outside the network would be missed — acceptable because the gateway is the boundary either way and it is address-agnostic; (b) `docker compose run` reinstalls the wheel and `npm ci` on each invocation, costing roughly a minute — acceptable for a nightly, and it keeps us from maintaining a bespoke test image.

**Not needed any more:** an `/etc/hosts` entry, a published Keycloak port, an issuer/JWKS address split, and a second compose file. All were artifacts of running the test client on the host. `AuthConfig` still keeps `issuer` and `jwks_uri` as separate fields — that is right for real split-horizon IdP deployments — but nothing in this plan depends on them differing.

**One compose file, two knobs:** `COMPOSE_PROFILES` decides which *services* start, interpolation decides what the *existing* services see. Both come from `deploy/cluster/auth.env`, so `docker compose up -d` is the unauthenticated cluster and `docker compose --env-file auth.env up -d` is the authenticated one. Task 9 Step 6 asserts the default path is unchanged rather than assuming it.
