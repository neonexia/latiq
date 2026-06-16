//! Agent-facing structured error envelope (adopted from trelisdb).
//! Philosophy: 80% of errors recoverable from `suggest` alone; 20% fetch `see`.
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct Location {
    pub line: u32,
    pub column: u32,
    pub byte: u32,
}

/// Closed taxonomy of error kinds (serialized as snake_case strings).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ErrorKind {
    PondNotFound,
    DatasetNotFound,
    NameConflict,
    ParseError,
    InvalidValue,
    MissingArgument,
    WriteToReservedSchema,
    ResultCapExceeded,
    ReadOnlyViolation,
    UriNotAllowed,
    QueryTimeout,
    QueryCancelled,
    Storage,
    Internal,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ErrorEnvelope {
    pub kind: ErrorKind,
    /// One sentence on what went wrong. No "suggest" text here.
    pub message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub location: Option<Location>,
    /// Copy-paste-ready corrected example — the immediate retry path.
    pub suggest: String,
    /// `latiq://` resource URI + anchor — the deeper learning path.
    pub see: String,
}

impl ErrorEnvelope {
    pub fn new(
        kind: ErrorKind,
        message: impl Into<String>,
        suggest: impl Into<String>,
        see: impl Into<String>,
    ) -> Self {
        Self {
            kind,
            message: message.into(),
            location: None,
            suggest: suggest.into(),
            see: see.into(),
        }
    }

    pub fn with_location(mut self, loc: Location) -> Self {
        self.location = Some(loc);
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn serializes_kind_as_snake_case() {
        let e = ErrorEnvelope::new(
            ErrorKind::PondNotFound,
            "Pond 'incident-001' does not exist.",
            "Call list_ponds to see available ponds.",
            "latiq://troubleshooting/pond-not-found",
        );
        let v = serde_json::to_value(&e).unwrap();
        assert_eq!(v["kind"], "pond_not_found");
        assert!(v.get("location").is_none(), "location omitted when None");
    }

    #[test]
    fn includes_location_when_set() {
        let e = ErrorEnvelope::new(
            ErrorKind::ParseError,
            "bad SQL",
            "fix it",
            "latiq://dialect",
        )
        .with_location(Location {
            line: 1,
            column: 8,
            byte: 7,
        });
        let v = serde_json::to_value(&e).unwrap();
        assert_eq!(v["location"]["line"], 1);
    }
}
