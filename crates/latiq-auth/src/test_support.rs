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

//! A minimal in-process IdP for tests: one keypair (RSA or Ed25519), a JWKS
//! endpoint, and a token minter. Lets us produce a token that is wrong in
//! exactly one way.
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use base64::Engine as _;
use jsonwebtoken::{Algorithm, EncodingKey, Header};
use rsa::pkcs1::EncodeRsaPrivateKey;
use rsa::traits::PublicKeyParts;
use rsa::RsaPrivateKey;
use serde_json::json;
use std::net::SocketAddr;
use std::sync::{Arc, OnceLock};
use tokio::sync::RwLock;

pub const KID: &str = "test-key-1";

/// The public half of a keypair, in the shape its JWK publishes it.
enum PublicJwk {
    Rsa {
        n: String,
        e: String,
    },
    /// An OKP key: the raw 32-byte Ed25519 public key, base64url.
    Okp {
        x: String,
    },
}

/// A generated keypair: the signing key, the algorithm it signs with, and its
/// JWKS public components.
struct Keypair {
    encoding: EncodingKey,
    alg: Algorithm,
    public: PublicJwk,
}

/// 2048-bit RSA keygen costs 1-5 seconds and is wildly variable. Tests need a
/// CONSISTENT key, not a unique one, so every `TestIdp` shares these. Each is
/// generated lazily, so a test run only pays for the ones it touches. (Ed25519
/// keygen is cheap, but it shares the pattern so the fixtures stay uniform.)
static SIGNING_KEY: OnceLock<Keypair> = OnceLock::new();
static ALT_KEY: OnceLock<Keypair> = OnceLock::new();
static FOREIGN_KEY: OnceLock<Keypair> = OnceLock::new();
static ED25519_KEY: OnceLock<Keypair> = OnceLock::new();
static ED25519_FOREIGN_KEY: OnceLock<Keypair> = OnceLock::new();

fn signing_key() -> &'static Keypair {
    SIGNING_KEY.get_or_init(generate)
}

/// The key of the SECOND fixture IdP. Published, but by a different issuer --
/// which is what makes cross-issuer confusion testable.
fn alt_key() -> &'static Keypair {
    ALT_KEY.get_or_init(generate)
}

/// A key NO fixture publishes, for proving signature checking works.
fn foreign_key() -> &'static Keypair {
    FOREIGN_KEY.get_or_init(generate)
}

/// The key of the Ed25519 fixture IdP, for the EdDSA end of the allowlist.
fn ed25519_key() -> &'static Keypair {
    ED25519_KEY.get_or_init(generate_ed25519)
}

/// The EdDSA counterpart of `foreign_key`: an Ed25519 key no fixture publishes,
/// so an EdDSA signature can be proven to actually be checked.
fn ed25519_foreign_key() -> &'static Keypair {
    ED25519_FOREIGN_KEY.get_or_init(generate_ed25519)
}

fn generate() -> Keypair {
    let mut rng = rand::thread_rng();
    let key = RsaPrivateKey::new(&mut rng, 2048).expect("generate rsa key");
    let pem = key
        .to_pkcs1_pem(rsa::pkcs1::LineEnding::LF)
        .expect("pkcs1 pem");
    let encoding = EncodingKey::from_rsa_pem(pem.as_bytes()).expect("encoding key");
    let public = key.to_public_key();
    Keypair {
        encoding,
        alg: Algorithm::RS256,
        public: PublicJwk::Rsa {
            n: URL_SAFE_NO_PAD.encode(public.n().to_bytes_be()),
            e: URL_SAFE_NO_PAD.encode(public.e().to_bytes_be()),
        },
    }
}

/// `ring` rather than a fresh dependency: it is already in the tree underneath
/// `jsonwebtoken`, and it is the same implementation that will verify the
/// signature -- so the fixture cannot drift from the verifier.
fn generate_ed25519() -> Keypair {
    let pkcs8 = ring::signature::Ed25519KeyPair::generate_pkcs8(&ring::rand::SystemRandom::new())
        .expect("generate ed25519 key");
    let pair = ring::signature::Ed25519KeyPair::from_pkcs8(pkcs8.as_ref())
        .expect("parse the pkcs8 we just generated");
    Keypair {
        // PKCS#8, which is what `jsonwebtoken`'s EdDSA signer expects.
        encoding: EncodingKey::from_ed_der(pkcs8.as_ref()),
        alg: Algorithm::EdDSA,
        public: PublicJwk::Okp {
            x: URL_SAFE_NO_PAD.encode(<_ as AsRef<[u8]>>::as_ref(
                ring::signature::KeyPair::public_key(&pair),
            )),
        },
    }
}

/// The JWKS document as served, plus the status to serve it with. Mutable so a
/// test can rotate keys, serve a broken document, or fail the endpoint.
struct IdpState {
    body: String,
    status: u16,
}

pub struct TestIdp {
    state: Arc<RwLock<IdpState>>,
    /// The key this IdP signs with AND publishes.
    key: &'static Keypair,
    pub issuer: String,
    pub jwks_uri: String,
    /// Serves the same document as `jwks_uri`, but chunked and WITHOUT a
    /// Content-Length. The size cap has two branches -- the cheap check on the
    /// advertised length, and the one that counts bytes as they arrive -- and
    /// only this URI reaches the second.
    pub jwks_stream_uri: String,
}

/// GET `uri` and report the Content-Length it advertises. Lets a test prove it
/// is exercising the streaming branch of the size cap rather than the
/// pre-check.
pub async fn advertised_content_length(uri: &str) -> Option<u64> {
    reqwest::get(uri)
        .await
        .expect("probe request")
        .content_length()
}

/// A one-key JWKS document publishing the shared signing key under `kid`.
pub fn jwks_document(kid: &str) -> String {
    jwks_document_for(signing_key(), kid)
}

fn jwks_document_for(key: &Keypair, kid: &str) -> String {
    let jwk = match &key.public {
        PublicJwk::Rsa { n, e } => json!({
            "kty": "RSA",
            "use": "sig",
            "alg": "RS256",
            "kid": kid,
            "n": n,
            "e": e,
        }),
        PublicJwk::Okp { x } => json!({
            "kty": "OKP",
            "use": "sig",
            "alg": "EdDSA",
            "crv": "Ed25519",
            "kid": kid,
            "x": x,
        }),
    };
    json!({ "keys": [jwk] }).to_string()
}

/// Unix seconds. Public so a test can build `nbf`/`exp` relative to now.
pub fn now_secs() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system clock before unix epoch")
        .as_secs() as i64
}

impl TestIdp {
    /// Start the JWKS server on an ephemeral port and return the fixture.
    pub async fn start() -> Self {
        Self::start_with_key(signing_key()).await
    }

    /// A SECOND IdP, publishing a different key under the SAME `kid`. That
    /// collision is deliberate: it is how a test proves the verifier picks keys
    /// per issuer rather than from one shared pool.
    pub async fn start_alt() -> Self {
        Self::start_with_key(alt_key()).await
    }

    /// An IdP that signs with Ed25519 and publishes an OKP JWK. Same fixture in
    /// every other respect, so an EdDSA test differs from its RS256 twin in
    /// exactly the algorithm.
    pub async fn start_ed25519() -> Self {
        Self::start_with_key(ed25519_key()).await
    }

    async fn start_with_key(key: &'static Keypair) -> Self {
        let state = Arc::new(RwLock::new(IdpState {
            body: jwks_document_for(key, KID),
            status: 200,
        }));

        let handler_state = state.clone();
        let stream_state = state.clone();
        let app = axum::Router::new()
            .route(
                "/jwks",
                axum::routing::get(move || {
                    let state = handler_state.clone();
                    async move {
                        let state = state.read().await;
                        let status = axum::http::StatusCode::from_u16(state.status)
                            .unwrap_or(axum::http::StatusCode::INTERNAL_SERVER_ERROR);
                        (
                            status,
                            [(axum::http::header::CONTENT_TYPE, "application/json")],
                            state.body.clone(),
                        )
                    }
                }),
            )
            .route(
                "/jwks-stream",
                axum::routing::get(move || {
                    let state = stream_state.clone();
                    async move {
                        let body = state.read().await.body.clone();
                        // Handed over as a stream of unknown length, so hyper
                        // uses chunked encoding and emits no Content-Length.
                        let chunks: Vec<Result<Vec<u8>, std::io::Error>> = body
                            .into_bytes()
                            .chunks(16 * 1024)
                            .map(|chunk| Ok(chunk.to_vec()))
                            .collect();
                        axum::body::Body::from_stream(tokio_stream::iter(chunks))
                    }
                }),
            );

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind ephemeral port");
        let addr: SocketAddr = listener.local_addr().expect("local addr");
        tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });

        Self {
            state,
            key,
            issuer: format!("http://{addr}"),
            jwks_uri: format!("http://{addr}/jwks"),
            jwks_stream_uri: format!("http://{addr}/jwks-stream"),
        }
    }

    /// Publish the signing key under a new `kid` instead of the old one, as an
    /// IdP does when it rotates.
    pub async fn rotate(&self, kid: &str) {
        self.set_jwks_body(jwks_document_for(self.key, kid)).await;
    }

    /// An `AuthConfig` naming this fixture as the only issuer, with the JWKS
    /// URI given explicitly -- the fixture's issuer is a bare `http://host:port`
    /// with no discovery document behind it.
    pub fn auth_config(&self) -> crate::AuthConfig {
        crate::AuthConfig {
            audience: "latiq".to_string(),
            issuers: vec![crate::IssuerConfig {
                issuer: self.issuer.clone(),
                jwks_uri: Some(self.jwks_uri.clone()),
            }],
            // The fixture is on loopback, so the plaintext guard exempts it
            // already; tests must never need the escape.
            allow_insecure_jwks: false,
        }
    }

    /// Serve an arbitrary body, for documents we could not otherwise produce
    /// (unusable keys, non-JSON, oversize).
    pub async fn set_jwks_body(&self, body: String) {
        self.state.write().await.body = body;
    }

    /// Serve this HTTP status instead of 200.
    pub async fn set_status(&self, status: u16) {
        self.state.write().await.status = status;
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
        encode(self.key, sub, aud, iss, exp_offset_secs, kid)
    }

    /// Sign an ARBITRARY claims object with this IdP's key, under the usual
    /// `kid`. The escape hatch for tokens the typed minters cannot express: an
    /// array-valued `aud`, a future `nbf`, a missing `sub`.
    pub fn mint_claims(&self, claims: serde_json::Value) -> String {
        let mut header = Header::new(self.key.alg);
        header.kid = Some(KID.to_string());
        jsonwebtoken::encode(&header, &claims, &self.key.encoding).expect("mint token")
    }

    /// An HS256 token signed with `secret`. Symmetric, so it must never verify
    /// against a resource server -- which holds no signing secrets at all.
    pub fn mint_hs256(&self, sub: &str, aud: &str, iss: &str, secret: &[u8]) -> String {
        let mut header = Header::new(Algorithm::HS256);
        header.kid = Some(KID.to_string());
        let now = now_secs();
        jsonwebtoken::encode(
            &header,
            &json!({ "sub": sub, "aud": aud, "iss": iss, "iat": now, "exp": now + 300 }),
            &EncodingKey::from_secret(secret),
        )
        .expect("mint hs256 token")
    }

    /// This IdP's RSA modulus, raw. Fed back as an HMAC secret it is the
    /// classic algorithm-confusion attack: the "secret" is public.
    pub fn public_modulus(&self) -> Vec<u8> {
        let PublicJwk::Rsa { n, .. } = &self.key.public else {
            panic!("public_modulus is only meaningful for an RSA fixture");
        };
        URL_SAFE_NO_PAD.decode(n).expect("modulus is base64url")
    }

    /// An `alg: "none"` token: header and claims, empty signature. Assembled by
    /// hand because `jsonwebtoken` has no way to express it.
    pub fn mint_alg_none(&self, sub: &str, aud: &str, iss: &str) -> String {
        let now = now_secs();
        let header =
            URL_SAFE_NO_PAD.encode(json!({ "alg": "none", "typ": "JWT", "kid": KID }).to_string());
        let claims = URL_SAFE_NO_PAD.encode(
            json!({ "sub": sub, "aud": aud, "iss": iss, "iat": now, "exp": now + 300 }).to_string(),
        );
        format!("{header}.{claims}.")
    }

    /// A token signed by a key NO fixture publishes, to prove signature
    /// checking works.
    pub fn mint_with_foreign_key(&self, sub: &str, aud: &str, iss: &str) -> String {
        encode(foreign_key(), sub, aud, iss, 300, Some(KID.to_string()))
    }

    /// The same, for EdDSA: a token signed by an Ed25519 key no fixture
    /// publishes. Proves the EdDSA branch checks signatures rather than
    /// accepting anything whose `alg` is on the allowlist.
    pub fn mint_with_foreign_ed25519_key(&self, sub: &str, aud: &str, iss: &str) -> String {
        encode(
            ed25519_foreign_key(),
            sub,
            aud,
            iss,
            300,
            Some(KID.to_string()),
        )
    }
}

fn encode(
    key: &Keypair,
    sub: &str,
    aud: &str,
    iss: &str,
    exp_offset_secs: i64,
    kid: Option<String>,
) -> String {
    let mut header = Header::new(key.alg);
    header.kid = kid;
    let now = now_secs();
    let claims = json!({
        "sub": sub,
        "aud": aud,
        "iss": iss,
        "iat": now,
        "exp": now + exp_offset_secs,
    });
    jsonwebtoken::encode(&header, &claims, &key.encoding).expect("mint token")
}
