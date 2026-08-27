//! JWT claim validation. Algorithms are an ALLOWLIST, never taken from the
//! token header -- accepting the header's `alg` is the classic algorithm
//! confusion bug. Issuers are an allowlist too: an `iss` we do not know is
//! rejected before any key lookup happens.
use crate::jwks::{discover_uri, JwksCache};
use crate::AuthError;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use base64::Engine as _;
use jsonwebtoken::{Algorithm, Validation};
use latiq_common::Identity;
use serde::Deserialize;

/// Asymmetric only: a symmetric alg would mean the verifier holds a signing
/// secret, which a resource server must not.
const ALLOWED_ALGS: &[Algorithm] = &[
    Algorithm::RS256,
    Algorithm::RS384,
    Algorithm::RS512,
    Algorithm::ES256,
];

/// The claims we need after validation. `aud`/`exp`/`iss` are checked by
/// `jsonwebtoken` itself against the pinned `Validation`, so they need no field
/// here.
#[derive(Deserialize)]
struct Claims {
    sub: String,
}

/// The one claim we read BEFORE verifying anything, purely to pick which
/// issuer's keys to check the signature against.
#[derive(Deserialize)]
struct UnverifiedIssuer {
    iss: Option<String>,
}

#[derive(Debug, Clone)]
pub struct IssuerConfig {
    /// Compared as a STRING against the token's `iss`. Never dialed.
    pub issuer: String,
    /// The URL actually fetched for signing keys. `None` = derive from `issuer`.
    /// An explicit value covers split-horizon deployments where the issuer
    /// identifier is not a reachable address.
    pub jwks_uri: Option<String>,
}

#[derive(Debug, Clone)]
pub struct AuthConfig {
    /// The `aud` this deployment expects. One value across all issuers: the
    /// audience names US, not who vouched for the caller.
    pub audience: String,
    pub issuers: Vec<IssuerConfig>,
}

pub struct Verifier {
    cfg: AuthConfig,
    /// One cache PER ISSUER -- two IdPs may legitimately publish the same `kid`
    /// under different keys, and a shared map would let either one's key satisfy
    /// the other's tokens.
    caches: Vec<(String, JwksCache)>,
}

/// Hand-written because a `JwksCache` holds live keys and an HTTP client, and
/// neither belongs in a log line.
impl std::fmt::Debug for Verifier {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Verifier")
            .field("audience", &self.cfg.audience)
            .field(
                "issuers",
                &self.caches.iter().map(|(i, _)| i).collect::<Vec<_>>(),
            )
            .finish()
    }
}

impl Verifier {
    /// Build a verifier, validating the config. This is where a
    /// misconfiguration becomes an auth bypass, so every check here is a
    /// hard failure rather than a warning.
    pub fn new(cfg: AuthConfig) -> Result<Self, AuthError> {
        if cfg.audience.trim().is_empty() {
            return Err(AuthError::Invalid(
                "auth audience must not be empty: without it any token minted for any service \
                 would be accepted"
                    .to_string(),
            ));
        }
        if cfg.issuers.is_empty() {
            return Err(AuthError::Invalid(
                "auth requires at least one configured issuer".to_string(),
            ));
        }

        let mut caches = Vec::with_capacity(cfg.issuers.len());
        for issuer in &cfg.issuers {
            let name = issuer.issuer.trim().to_string();
            if name.is_empty() {
                return Err(AuthError::Invalid(
                    "issuer identifier must not be empty".to_string(),
                ));
            }
            if caches.iter().any(|(known, _): &(String, _)| known == &name) {
                return Err(AuthError::Invalid(format!(
                    "issuer '{name}' is configured more than once"
                )));
            }
            let uri = match issuer.jwks_uri.as_deref().map(str::trim) {
                Some(u) if !u.is_empty() => u.to_string(),
                _ => discover_uri(&name),
            };
            require_tls_or_loopback(&uri)?;
            caches.push((name, JwksCache::new(uri)));
        }

        Ok(Self { cfg, caches })
    }

    pub fn config(&self) -> &AuthConfig {
        &self.cfg
    }

    /// Verify a bearer token and produce a verified `Identity`. `claimed_agent`
    /// is the caller-asserted leaf, which stays unverified by construction.
    pub async fn verify(
        &self,
        token: &str,
        claimed_agent: Option<&str>,
    ) -> Result<Identity, AuthError> {
        let token = token.trim();
        if token.is_empty() {
            return Err(AuthError::Missing);
        }

        let header = jsonwebtoken::decode_header(token)
            .map_err(|e| AuthError::Malformed(format!("unreadable token header: {e}")))?;

        // The header's `alg` selects nothing; it is checked AGAINST our
        // allowlist, and the same allowlist is pinned into `Validation` below so
        // the decode cannot be talked into anything else.
        if !ALLOWED_ALGS.contains(&header.alg) {
            return Err(AuthError::Invalid(format!(
                "token algorithm {:?} is not accepted",
                header.alg
            )));
        }
        let Some(kid) = header.kid else {
            return Err(AuthError::Malformed(
                "token header has no 'kid', so no signing key can be pinned".to_string(),
            ));
        };

        // UNVERIFIED. Used for exactly one thing: choosing whose keys to check
        // the signature against. The decode below still pins issuer and
        // audience, so a token claiming an issuer it was not signed by is
        // checked against that issuer's real keys and fails.
        let claimed_issuer = unverified_issuer(token)?;
        let Some((issuer, cache)) = self
            .caches
            .iter()
            .find(|(known, _)| known == &claimed_issuer)
            .map(|(known, cache)| (known.as_str(), cache))
        else {
            // Deliberately does not echo the claimed issuer: it is
            // attacker-controlled and unbounded.
            return Err(AuthError::Invalid(
                "token issuer is not configured for this deployment".to_string(),
            ));
        };

        let key = cache.key_for(&kid).await?;

        // `header.alg` is safe to pin here ONLY because it was checked against
        // ALLOWED_ALGS above and the key came from the issuer's JWKS -- so an
        // `alg` swap can at most pick another asymmetric algorithm this key
        // cannot satisfy. (The list cannot simply be ALLOWED_ALGS: jsonwebtoken
        // requires every entry to belong to the decoding key's family.)
        let mut validation = Validation::new(header.alg);
        validation.set_audience(&[self.cfg.audience.as_str()]);
        validation.set_issuer(&[issuer]);
        validation.set_required_spec_claims(&["exp", "aud", "iss", "sub"]);

        let data = jsonwebtoken::decode::<Claims>(token, &key, &validation)
            .map_err(|e| AuthError::Invalid(e.to_string()))?;

        // `sub` being *present* is not the same as it being usable: an empty
        // subject would become an empty DuckLake commit author.
        let subject = data.claims.sub.trim();
        if subject.is_empty() {
            return Err(AuthError::Invalid(
                "token 'sub' is empty, so the caller has no identity to attribute".to_string(),
            ));
        }

        Ok(Identity::verified(subject, issuer, claimed_agent))
    }
}

/// Read the `iss` out of the payload segment WITHOUT verifying the signature.
fn unverified_issuer(token: &str) -> Result<String, AuthError> {
    let mut parts = token.split('.');
    let (Some(_header), Some(payload), Some(_sig), None) =
        (parts.next(), parts.next(), parts.next(), parts.next())
    else {
        return Err(AuthError::Malformed(
            "token is not a three-segment JWS".to_string(),
        ));
    };
    let bytes = URL_SAFE_NO_PAD
        .decode(payload)
        .map_err(|e| AuthError::Malformed(format!("token payload is not base64url: {e}")))?;
    let claims: UnverifiedIssuer = serde_json::from_slice(&bytes)
        .map_err(|e| AuthError::Malformed(format!("token payload is not JSON claims: {e}")))?;
    claims.iss.ok_or_else(|| {
        AuthError::Malformed("token has no 'iss' claim, so no key set can be chosen".to_string())
    })
}

/// A plaintext JWKS URI is a total auth bypass -- anyone on-path substitutes
/// keys and mints arbitrary identities. Loopback is exempt because tests and
/// `./dev.sh --auth` legitimately run a fake IdP there, and there is no network
/// to be on-path of.
fn require_tls_or_loopback(uri: &str) -> Result<(), AuthError> {
    let Some((scheme, rest)) = uri.split_once("://") else {
        return Err(AuthError::Invalid(format!(
            "jwks uri '{uri}' is not an absolute http(s) URL"
        )));
    };
    match scheme.to_ascii_lowercase().as_str() {
        "https" => Ok(()),
        "http" if is_loopback(rest) => Ok(()),
        "http" => Err(AuthError::Invalid(format!(
            "jwks uri '{uri}' is plaintext http: signing keys must be fetched over https \
             (loopback excepted)"
        ))),
        other => Err(AuthError::Invalid(format!(
            "jwks uri scheme '{other}' is not supported; use https"
        ))),
    }
}

/// `rest` is everything after `://`. Extracts the host and asks whether it is a
/// loopback name or address.
fn is_loopback(rest: &str) -> bool {
    let authority = rest
        .split(['/', '?', '#'])
        .next()
        .unwrap_or_default()
        // Drop any userinfo: `http://evil.example@127.0.0.1/` and
        // `http://127.0.0.1@evil.example/` differ only after the last '@'.
        .rsplit('@')
        .next()
        .unwrap_or_default();

    let host = if let Some(literal) = authority.strip_prefix('[') {
        // IPv6 literal: `[::1]:8080`.
        match literal.split_once(']') {
            // Only a port may follow the bracket; anything else means this was
            // never an IPv6 authority.
            Some((inner, after)) if after.is_empty() || after.starts_with(':') => inner,
            _ => return false,
        }
    } else {
        authority.split(':').next().unwrap_or_default()
    };

    matches!(
        host.to_ascii_lowercase().as_str(),
        "localhost" | "127.0.0.1" | "::1"
    )
}

#[cfg(test)]
mod tests {
    use super::{is_loopback, require_tls_or_loopback};

    #[test]
    fn https_is_always_allowed() {
        assert!(require_tls_or_loopback("https://idp.example/jwks").is_ok());
        assert!(require_tls_or_loopback("HTTPS://idp.example/jwks").is_ok());
    }

    #[test]
    fn plaintext_off_loopback_is_refused() {
        assert!(require_tls_or_loopback("http://idp.example/jwks").is_err());
        assert!(require_tls_or_loopback("ftp://idp.example/jwks").is_err());
        assert!(require_tls_or_loopback("idp.example/jwks").is_err());
    }

    #[test]
    fn userinfo_cannot_fake_a_loopback_host() {
        // The host is what comes AFTER the last '@'.
        assert!(!is_loopback("127.0.0.1@evil.example/jwks"));
        assert!(is_loopback("evil.example@127.0.0.1/jwks"));
    }

    #[test]
    fn loopback_forms() {
        assert!(is_loopback("127.0.0.1:8080/jwks"));
        assert!(is_loopback("[::1]:8080/jwks"));
        assert!(is_loopback("LocalHost/jwks"));
        assert!(!is_loopback("127.0.0.1.evil.example/jwks"));
        assert!(!is_loopback("[::1].evil.example/jwks"));
    }
}
