//! latiq-common — shared kernel (ids, identity, errors, results, config).
pub mod id;
pub use id::PondId;
pub mod identity;
pub use identity::Identity;
pub mod error;
pub use error::{ErrorEnvelope, ErrorKind, Location};
