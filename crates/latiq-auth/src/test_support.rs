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

pub const KID: &str = "test-key-1";

pub struct TestIdp {
    encoding: EncodingKey,
    pub issuer: String,
    pub jwks_uri: String,
}

/// Generate a keypair and return (encoding key, JWKS `n`, JWKS `e`).
fn keypair() -> (EncodingKey, String, String) {
    let mut rng = rand::thread_rng();
    let key = RsaPrivateKey::new(&mut rng, 2048).expect("generate rsa key");
    let pem = key
        .to_pkcs1_pem(rsa::pkcs1::LineEnding::LF)
        .expect("pkcs1 pem");
    let encoding = EncodingKey::from_rsa_pem(pem.as_bytes()).expect("encoding key");
    let public = key.to_public_key();
    let n = URL_SAFE_NO_PAD.encode(public.n().to_bytes_be());
    let e = URL_SAFE_NO_PAD.encode(public.e().to_bytes_be());
    (encoding, n, e)
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
        let (encoding, n, e) = keypair();
        let jwks = json!({
            "keys": [{
                "kty": "RSA",
                "use": "sig",
                "alg": "RS256",
                "kid": KID,
                "n": n,
                "e": e,
            }]
        })
        .to_string();

        let app = axum::Router::new().route(
            "/jwks",
            axum::routing::get(move || {
                let jwks = jwks.clone();
                async move {
                    (
                        [(axum::http::header::CONTENT_TYPE, "application/json")],
                        jwks,
                    )
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
        jsonwebtoken::encode(&header, &claims, &self.encoding).expect("mint token")
    }

    /// A token signed by a DIFFERENT key, to prove signature checking works.
    pub fn mint_with_foreign_key(&self, sub: &str, aud: &str, iss: &str) -> String {
        let (foreign, _, _) = keypair();
        let mut header = Header::new(Algorithm::RS256);
        header.kid = Some(KID.to_string());
        let now = now_secs();
        let claims = json!({
            "sub": sub,
            "aud": aud,
            "iss": iss,
            "iat": now,
            "exp": now + 300,
        });
        jsonwebtoken::encode(&header, &claims, &foreign).expect("mint token")
    }
}
