//! Token verification for every Latiq surface. PROTOCOL-NEUTRAL: this crate
//! takes a token string and returns an Identity. Adapters extract the token
//! from their own carrier. See docs/identity.md.
pub mod jwks;
pub mod metadata;
pub mod verify;

pub use verify::{AuthConfig, IssuerConfig, Verifier};

#[cfg(feature = "test-support")]
pub mod test_support;

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
