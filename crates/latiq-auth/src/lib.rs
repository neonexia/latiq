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

//! Token verification for every Latiq surface. PROTOCOL-NEUTRAL: this crate
//! takes a token string and returns an Identity. Adapters extract the token
//! from their own carrier. See docs/identity.md.
pub mod jwks;
pub mod metadata;
pub mod verify;

pub use verify::{AuthConfig, IssuerConfig, Verifier};

#[cfg(feature = "test-support")]
pub mod test_support;

/// The raw token from an `Authorization` header value, if it carries a bearer
/// credential. Both `Bearer ` and the lowercase `bearer ` are accepted — RFC
/// 6750's scheme name is case-insensitive and real clients send both.
///
/// Lives here, not in an adapter: every surface parses the same header, and two
/// copies of a security-relevant parser drift. Adapters pull the header value
/// out of their own carrier (gRPC metadata, HTTP headers) and hand it here.
pub fn bearer(header_value: &str) -> Option<&str> {
    let (scheme, token) = header_value.split_once(' ')?;
    if !scheme.eq_ignore_ascii_case("bearer") {
        return None;
    }
    let token = token.trim();
    (!token.is_empty()).then_some(token)
}

#[cfg(test)]
mod bearer_tests {
    use super::bearer;

    #[test]
    fn accepts_either_case_and_trims() {
        assert_eq!(bearer("Bearer abc"), Some("abc"));
        assert_eq!(bearer("bearer abc"), Some("abc"));
        assert_eq!(bearer("BEARER  abc "), Some("abc"));
    }

    #[test]
    fn rejects_other_schemes_and_empty_credentials() {
        assert_eq!(bearer("Basic abc"), None);
        assert_eq!(bearer("Bearer"), None);
        assert_eq!(bearer("Bearer "), None);
        assert_eq!(bearer(""), None);
        // No scheme at all is not a bearer credential, even though a bare token
        // looks like one.
        assert_eq!(bearer("abc"), None);
    }
}

/// Why a token was not accepted. Every variant is one `Unauthenticated` to the
/// caller — the distinction is for the operator's log, since telling an
/// unauthenticated caller *which* check failed is a probing oracle.
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
