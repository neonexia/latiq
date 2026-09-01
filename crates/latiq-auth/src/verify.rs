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
use url::{Host, Url};

/// The line is drawn at ASYMMETRIC signatures: with a symmetric alg the
/// verifier would hold a signing secret, which a resource server must not --
/// and it is exactly what the algorithm-confusion attack reaches for, feeding a
/// public key back as an HMAC secret. Everything an enterprise IdP actually
/// issues is here (RSA PKCS#1 v1.5, RSA-PSS, ECDSA, Ed25519); nothing symmetric
/// is, and `none` cannot be expressed by this type at all.
const ALLOWED_ALGS: &[Algorithm] = &[
    Algorithm::RS256,
    Algorithm::RS384,
    Algorithm::RS512,
    Algorithm::PS256,
    Algorithm::PS384,
    Algorithm::PS512,
    Algorithm::ES256,
    Algorithm::ES384,
    // Asymmetric like the rest, and the one the JWKS translation in `jwks.rs`
    // already maps -- leaving it out meant refusing an algorithm we could
    // otherwise import a key for, with no explanation to the operator.
    Algorithm::EdDSA,
];

/// Tokens are bounded BEFORE any parsing. `verify()` is protocol-neutral by
/// design, so it must not inherit whatever header cap the calling transport
/// happens to impose: an unauthenticated caller could otherwise spend our CPU
/// and allocator on a multi-megabyte "token" that was always going to be
/// rejected. Real IdP access tokens are 1-4 KB.
const MAX_TOKEN_BYTES: usize = 8 * 1024;

/// Accepted clock skew between us and the IdP, applied to `exp`/`nbf`. An
/// explicit decision rather than jsonwebtoken's inherited 60s default: 30s
/// covers ordinary NTP drift while halving the window in which a revoked or
/// expired token still works.
const CLOCK_SKEW_LEEWAY_SECS: u64 = 30;

/// The claims we need after validation. `aud`/`exp`/`iss` are checked by
/// `jsonwebtoken` itself against the pinned `Validation`, so they need no field
/// here.
#[derive(Deserialize)]
struct Claims {
    /// Optional at the SERDE layer only. `sub` is in the required-spec-claims
    /// set, so an absent one is rejected by `jsonwebtoken` with a clear
    /// `MissingRequiredClaim` -- a non-optional field here would instead fail
    /// deserialization first and surface as an opaque JSON error.
    sub: Option<String>,
}

/// The one claim we read BEFORE verifying anything, purely to pick which
/// issuer's keys to check the signature against.
#[derive(Deserialize)]
struct UnverifiedIssuer {
    iss: Option<String>,
}

/// One trusted issuer. The `issuer` string and the URL we fetch keys from are
/// separate fields because they are separate things: one is an identifier that
/// is matched, the other an address that is dialed.
#[derive(Debug, Clone)]
pub struct IssuerConfig {
    /// Compared as a STRING against the token's `iss`. Never dialed.
    pub issuer: String,
    /// The URL actually fetched for signing keys. `None` = derive from `issuer`.
    /// An explicit value covers split-horizon deployments where the issuer
    /// identifier is not a reachable address.
    pub jwks_uri: Option<String>,
}

/// A deployment's verification policy. Configuring one turns every surface into
/// an OAuth 2.1 resource server; omitting it leaves identity relaxed (claimed,
/// default anonymous) — which is the default and what the SDK and tests run.
#[derive(Debug, Clone)]
pub struct AuthConfig {
    /// The `aud` this deployment expects. One value across all issuers: the
    /// audience names US, not who vouched for the caller.
    pub audience: String,
    pub issuers: Vec<IssuerConfig>,
    /// Permit a plaintext-http JWKS URI to a NON-loopback host. Off in every
    /// production path and deliberately unpleasant to name: fetching signing
    /// keys in the clear hands anyone on-path the ability to mint identities.
    /// It exists for one case — an IdP container on a private Docker network
    /// (`deploy/cluster/`'s auth profile: `http://keycloak:8080/...`), which is
    /// neither loopback nor able to present a certificate. Setting it warns on
    /// every startup. See `docs/identity.md`.
    pub allow_insecure_jwks: bool,
}

/// Turns a token string into a verified [`Identity`], or refuses. Share one per
/// deployment: it holds the per-issuer key caches, and a second instance would
/// re-fetch every JWKS and double the traffic aimed at the customer's IdP.
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
        let audience = cfg.audience.trim().to_string();
        if audience.is_empty() {
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

        // The stored config is the NORMALIZED one. Anything that reads it back
        // -- the later protected-resource metadata document above all -- must
        // publish the same strings the verifier actually enforces.
        let mut issuers = Vec::with_capacity(cfg.issuers.len());
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
            let uri = checked_jwks_uri(&uri, cfg.allow_insecure_jwks)?;
            issuers.push(IssuerConfig {
                issuer: name.clone(),
                jwks_uri: Some(uri.clone()),
            });
            caches.push((name, JwksCache::new(uri)));
        }

        Ok(Self {
            cfg: AuthConfig {
                audience,
                issuers,
                allow_insecure_jwks: cfg.allow_insecure_jwks,
            },
            caches,
        })
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
        if token.len() > MAX_TOKEN_BYTES {
            return Err(AuthError::Malformed(format!(
                "token is {} bytes, over the {MAX_TOKEN_BYTES}-byte limit",
                token.len()
            )));
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

        // A JWK may declare the ONE algorithm its key is for. Honour it: pinning
        // only the token's `alg` would let a key published as RS512 verify an
        // RS256 token, which is the issuer's policy being quietly downgraded by
        // the caller.
        if let Some(declared) = key.alg {
            if declared != header.alg {
                return Err(AuthError::Invalid(format!(
                    "token algorithm {:?} does not match the {declared:?} declared by signing key",
                    header.alg
                )));
            }
        }

        // `header.alg` is safe to pin here ONLY because it was checked against
        // ALLOWED_ALGS above and the key came from the issuer's JWKS -- so an
        // `alg` swap can at most pick another asymmetric algorithm this key
        // cannot satisfy. (The list cannot simply be ALLOWED_ALGS: jsonwebtoken
        // requires every entry to belong to the decoding key's family.)
        let mut validation = Validation::new(header.alg);
        validation.leeway = CLOCK_SKEW_LEEWAY_SECS;
        // Off by default in jsonwebtoken. `nbf` stays OPTIONAL (it is not in the
        // required set), but when a token carries one, RFC 7519 4.1.5 says the
        // token MUST NOT be accepted before it.
        validation.validate_nbf = true;
        validation.set_audience(&[self.cfg.audience.as_str()]);
        validation.set_issuer(&[issuer]);
        validation.set_required_spec_claims(&["exp", "aud", "iss", "sub"]);

        let data = jsonwebtoken::decode::<Claims>(token, &key.key, &validation)
            .map_err(|e| AuthError::Invalid(e.to_string()))?;

        // `sub` being *present* is not the same as it being usable: an empty
        // subject would become an empty DuckLake commit author.
        let subject = data.claims.sub.as_deref().unwrap_or_default().trim();
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

/// Validate a JWKS URI and return the NORMALIZED form that will actually be
/// fetched. A plaintext URI is a total auth bypass -- anyone on-path substitutes
/// keys and mints arbitrary identities -- so the guard must run on the same host
/// the HTTP client will dial. That is why this parses with `url` rather than
/// splitting the authority by hand: hand-rolled splitting disagrees with the
/// WHATWG parser, and `http://evil.com\@127.0.0.1/jwks` is exactly that
/// disagreement (the backslash is a path separator, so the host is `evil.com`,
/// not the loopback address it reads as).
///
/// Loopback is exempt from the https requirement because tests and
/// `./dev.sh --auth` legitimately run a fake IdP there, and there is no network
/// to be on-path of.
///
/// `allow_insecure` is [`AuthConfig::allow_insecure_jwks`] — the deliberate,
/// off-by-default escape for a containerised IdP on a private network, where
/// the host is neither loopback nor able to present a certificate. It relaxes
/// THIS ARM ONLY, and warns every time it is used. Never set it in production.
fn checked_jwks_uri(uri: &str, allow_insecure: bool) -> Result<String, AuthError> {
    // Caught on the RAW string, before parsing: for a special scheme the WHATWG
    // parser silently promotes the first path segment to the authority, so BOTH
    // `https:///jwks` (empty authority) and `https:/jwks` (no `//` at all)
    // become a confidently-https fetch of a host named `jwks`. Checking only for
    // `://` misses the second, which then fails at fetch time on the request
    // path instead of loudly at startup.
    //
    // A URI with no `:` at all is left to `Url::parse` below, which reports it
    // as the relative URL it is rather than as a host problem.
    if let Some((_, rest)) = uri.split_once(':') {
        let empty_host = match rest.strip_prefix("//") {
            Some(authority) => authority.starts_with('/'),
            None => true,
        };
        if empty_host {
            return Err(AuthError::Invalid(format!(
                "jwks uri '{uri}' has no host; expected scheme://host/path"
            )));
        }
    }

    let parsed = Url::parse(uri)
        .map_err(|e| AuthError::Invalid(format!("jwks uri '{uri}' is not a valid URL: {e}")))?;

    match parsed.scheme() {
        "https" => {}
        "http" if is_loopback(&parsed) => {}
        // Loud, unconditional, and every startup: an operator who left this on
        // by accident must see it in the logs, not discover it after a forged
        // token. `warn!` rather than `debug!` for exactly that reason.
        "http" if allow_insecure => tracing::warn!(
            jwks_uri = %uri,
            "INSECURE: fetching JWKS signing keys over plaintext http to a non-loopback host \
             because --auth-allow-insecure-jwks (LATIQ_AUTH_ALLOW_INSECURE_JWKS) is set. Anyone \
             who can intercept this fetch can substitute their own signing keys and mint any \
             identity, defeating authentication entirely. This exists for local/test deployments \
             where the IdP is a container on a private network. NEVER set it in production."
        ),
        "http" => {
            return Err(AuthError::Invalid(format!(
                "jwks uri '{uri}' is plaintext http to a non-loopback host: signing keys must be \
                 fetched over https"
            )))
        }
        other => {
            return Err(AuthError::Invalid(format!(
                "jwks uri scheme '{other}' is not supported; use https"
            )))
        }
    }

    Ok(parsed.to_string())
}

/// Whether the host the client will DIAL is loopback. Decided on the parsed
/// host, so `127.1`, `2130706433` and `[::ffff:127.0.0.1]` -- all of which
/// resolve to 127.0.0.1 -- are recognised, while `127.0.0.1.evil.example` and
/// `evil.com\@127.0.0.1` are not.
fn is_loopback(url: &Url) -> bool {
    match url.host() {
        Some(Host::Domain(name)) => name.eq_ignore_ascii_case("localhost"),
        Some(Host::Ipv4(ip)) => ip.is_loopback(),
        Some(Host::Ipv6(ip)) => {
            // `is_loopback` on an Ipv6Addr is false for the IPv4-MAPPED
            // loopback, which still dials 127.0.0.1.
            ip.is_loopback() || ip.to_ipv4_mapped().is_some_and(|v4| v4.is_loopback())
        }
        None => false,
    }
}

#[cfg(test)]
mod tests {
    use super::checked_jwks_uri;

    fn allowed(uri: &str) -> bool {
        checked_jwks_uri(uri, false).is_ok()
    }

    /// The same call the `--auth-allow-insecure-jwks` escape makes.
    fn allowed_with_escape(uri: &str) -> bool {
        checked_jwks_uri(uri, true).is_ok()
    }

    #[test]
    fn https_is_always_allowed() {
        assert!(allowed("https://idp.example/jwks"));
        assert!(allowed("HTTPS://idp.example/jwks"));
    }

    #[test]
    fn plaintext_off_loopback_is_refused() {
        assert!(!allowed("http://idp.example/jwks"));
        assert!(!allowed("ftp://idp.example/jwks"));
        assert!(!allowed("idp.example/jwks"));
    }

    #[test]
    fn a_backslash_cannot_smuggle_a_loopback_host() {
        // The WHATWG parser treats a backslash as a path separator for special
        // schemes, so the host here is `evil.com` and the "127.0.0.1" is path. A
        // hand-rolled authority split reads it the other way and waves the URI
        // through -- an on-path attacker then substitutes signing keys.
        assert!(!allowed("http://evil.com\\@127.0.0.1/jwks"));
        assert!(!allowed("http://evil.com\\127.0.0.1/jwks"));
    }

    #[test]
    fn userinfo_cannot_fake_a_loopback_host() {
        assert!(!allowed("http://127.0.0.1@evil.example/jwks"));
        assert!(allowed("http://evil.example@127.0.0.1/jwks"));
    }

    #[test]
    fn an_empty_host_is_refused() {
        // Would otherwise parse to the host `jwks`.
        assert!(!allowed("https:///jwks"));
        assert!(!allowed("http:///jwks"));
    }

    #[test]
    fn a_missing_authority_marker_is_refused() {
        // Single slash: the WHATWG parser fills the authority in from the path
        // just as it does for `///`, so `https:/jwks` is an https fetch of a
        // host named `jwks`. A misconfiguration must fail at startup, not on
        // the first request.
        assert!(!allowed("https:/jwks"));
        assert!(!allowed("https:jwks"));
        // The http single-slash forms were already refused by the scheme arm
        // (they resolve to real non-loopback hosts); pin that they still are,
        // and for the right reason.
        assert!(!allowed("http:/jwks"));
    }

    #[test]
    fn loopback_forms() {
        assert!(allowed("http://127.0.0.1:8080/jwks"));
        assert!(allowed("http://[::1]:8080/jwks"));
        assert!(allowed("http://LocalHost/jwks"));
        // All three of these dial 127.0.0.1.
        assert!(allowed("http://127.1/jwks"));
        assert!(allowed("http://2130706433/jwks"));
        assert!(allowed("http://[::ffff:127.0.0.1]/jwks"));
    }

    /// The half that matters: if anyone ever flips the escape's default, this
    /// fails. `deploy/cluster/`'s auth profile opts in so Keycloak can be reached
    /// at `http://keycloak:8080` on a private compose network; nothing else does,
    /// and nothing may make that the default. Asserted on the message, not
    /// `is_err()`, so it cannot pass because of the empty-host or scheme arm.
    #[test]
    fn plaintext_to_a_container_host_is_refused_unless_explicitly_allowed() {
        let uri = "http://keycloak:8080/realms/latiq/protocol/openid-connect/certs";
        let e = checked_jwks_uri(uri, false).expect_err("plaintext http off loopback must refuse");
        assert!(
            e.to_string()
                .contains("plaintext http to a non-loopback host"),
            "refused for the wrong reason: {e}"
        );
        assert_eq!(
            checked_jwks_uri(uri, true).expect("the explicit escape must allow it"),
            uri,
            "the escape must return the URI that will actually be fetched"
        );
    }

    /// The escape is scoped to the http/non-loopback arm alone. A malformed or
    /// unsupported URI is still a misconfiguration, not something to wave
    /// through, and the backslash bypass this guard was written for stays shut.
    #[test]
    fn the_escape_relaxes_only_the_plaintext_arm() {
        assert!(!allowed_with_escape("ftp://idp.example/jwks"));
        assert!(!allowed_with_escape("https:///jwks"));
        assert!(!allowed_with_escape("http:/jwks"));
        assert!(!allowed_with_escape("idp.example/jwks"));
        // Still https-only in spirit: this one is now permitted, but only
        // because it IS plaintext-off-loopback -- the host is `evil.com`, which
        // an operator who set the flag has accepted responsibility for.
        assert!(allowed_with_escape("http://evil.com\\@127.0.0.1/jwks"));
    }

    #[test]
    fn loopback_lookalikes_are_refused() {
        assert!(!allowed("http://127.0.0.1.evil.example/jwks"));
        assert!(!allowed("http://localhost.evil.example/jwks"));
        assert!(!allowed("http://notlocalhost/jwks"));
    }
}
