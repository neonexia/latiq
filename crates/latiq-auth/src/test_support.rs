//! A minimal in-process IdP for tests: one RSA keypair, a JWKS endpoint, and a
//! token minter. Lets us produce a token that is wrong in exactly one way.
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

/// A generated keypair: the signing key plus its JWKS public components.
struct Keypair {
    encoding: EncodingKey,
    n: String,
    e: String,
}

/// 2048-bit RSA keygen costs 1-5 seconds and is wildly variable. Tests need a
/// CONSISTENT key, not a unique one, so every `TestIdp` shares these two.
static SIGNING_KEY: OnceLock<Keypair> = OnceLock::new();
static FOREIGN_KEY: OnceLock<Keypair> = OnceLock::new();

fn signing_key() -> &'static Keypair {
    SIGNING_KEY.get_or_init(generate)
}

/// A key the IdP never publishes, for proving signature checking works.
fn foreign_key() -> &'static Keypair {
    FOREIGN_KEY.get_or_init(generate)
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
        n: URL_SAFE_NO_PAD.encode(public.n().to_bytes_be()),
        e: URL_SAFE_NO_PAD.encode(public.e().to_bytes_be()),
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
    let key = signing_key();
    json!({
        "keys": [{
            "kty": "RSA",
            "use": "sig",
            "alg": "RS256",
            "kid": kid,
            "n": key.n,
            "e": key.e,
        }]
    })
    .to_string()
}

fn now_secs() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system clock before unix epoch")
        .as_secs() as i64
}

impl TestIdp {
    /// Start the JWKS server on an ephemeral port and return the fixture.
    pub async fn start() -> Self {
        let state = Arc::new(RwLock::new(IdpState {
            body: jwks_document(KID),
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
            issuer: format!("http://{addr}"),
            jwks_uri: format!("http://{addr}/jwks"),
            jwks_stream_uri: format!("http://{addr}/jwks-stream"),
        }
    }

    /// Publish the signing key under a new `kid` instead of the old one, as an
    /// IdP does when it rotates.
    pub async fn rotate(&self, kid: &str) {
        self.set_jwks_body(jwks_document(kid)).await;
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
        encode(&signing_key().encoding, sub, aud, iss, exp_offset_secs, kid)
    }

    /// A token signed by a DIFFERENT key, to prove signature checking works.
    pub fn mint_with_foreign_key(&self, sub: &str, aud: &str, iss: &str) -> String {
        encode(
            &foreign_key().encoding,
            sub,
            aud,
            iss,
            300,
            Some(KID.to_string()),
        )
    }
}

fn encode(
    key: &EncodingKey,
    sub: &str,
    aud: &str,
    iss: &str,
    exp_offset_secs: i64,
    kid: Option<String>,
) -> String {
    let mut header = Header::new(Algorithm::RS256);
    header.kid = kid;
    let now = now_secs();
    let claims = json!({
        "sub": sub,
        "aud": aud,
        "iss": iss,
        "iat": now,
        "exp": now + exp_offset_secs,
    });
    jsonwebtoken::encode(&header, &claims, key).expect("mint token")
}
