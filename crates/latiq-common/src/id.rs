//! Strongly-typed identifiers.
use serde::{Deserialize, Serialize};
use std::fmt;
use uuid::Uuid;

/// Opaque pond identifier (UUIDv4).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct PondId(Uuid);

impl PondId {
    #[allow(clippy::new_without_default)]
    pub fn new() -> Self {
        Self(Uuid::new_v4())
    }
    pub fn parse(s: &str) -> Result<Self, uuid::Error> {
        Ok(Self(Uuid::parse_str(s)?))
    }
}

impl fmt::Display for PondId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrips_through_string() {
        let id = PondId::new();
        let parsed = PondId::parse(&id.to_string()).unwrap();
        assert_eq!(id, parsed);
    }

    #[test]
    fn rejects_garbage() {
        assert!(PondId::parse("not-a-uuid").is_err());
    }
}
