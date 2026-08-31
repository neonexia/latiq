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
    /// The caller's credential was absent, expired, or refused. The one failure
    /// a client can actually ACT on (re-mint the token and retry), so it must be
    /// distinguishable from a crash all the way to the edge — including across a
    /// node-to-node forward, where the owner re-verifies on its own authority.
    Unauthenticated,
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

impl ErrorKind {
    /// The snake_case wire name (matches the serde `rename_all` serialization) —
    /// use this for metric labels / logs so they agree with the envelope on the
    /// wire (`format!("{:?}", kind)` would give PascalCase and not match).
    pub fn as_str(self) -> &'static str {
        match self {
            ErrorKind::PondNotFound => "pond_not_found",
            ErrorKind::DatasetNotFound => "dataset_not_found",
            ErrorKind::NameConflict => "name_conflict",
            ErrorKind::ParseError => "parse_error",
            ErrorKind::InvalidValue => "invalid_value",
            ErrorKind::MissingArgument => "missing_argument",
            ErrorKind::WriteToReservedSchema => "write_to_reserved_schema",
            ErrorKind::ResultCapExceeded => "result_cap_exceeded",
            ErrorKind::ReadOnlyViolation => "read_only_violation",
            ErrorKind::UriNotAllowed => "uri_not_allowed",
            ErrorKind::QueryTimeout => "query_timeout",
            ErrorKind::QueryCancelled => "query_cancelled",
            ErrorKind::Unauthenticated => "unauthenticated",
            ErrorKind::Storage => "storage",
            ErrorKind::Internal => "internal",
        }
    }

    /// The canonical, copy-paste-ready next step for this kind — the single
    /// source of `suggest` text, shared by every surface (agent-core, the
    /// control-plane gRPC, the CLI) so the same kind reads identically wherever
    /// it surfaces. A specific call site may still override via `ErrorEnvelope::new`.
    pub fn default_suggest(self) -> &'static str {
        match self {
            ErrorKind::PondNotFound => {
                "Call list_ponds to see available ponds, or allocate_pond to create one."
            }
            ErrorKind::DatasetNotFound => "Call list_datasets to see what's available.",
            ErrorKind::NameConflict => {
                "Choose a different name, or omit name to let Latiq generate one."
            }
            ErrorKind::ParseError => "Check the SQL syntax against the supported dialect.",
            ErrorKind::InvalidValue => "Fix the value and retry.",
            ErrorKind::MissingArgument => "Provide the required argument and retry.",
            ErrorKind::WriteToReservedSchema => {
                "Write to your own tables/schema, not a reserved one."
            }
            ErrorKind::ResultCapExceeded => {
                "Narrow with WHERE/LIMIT, aggregate server-side (GROUP BY/count/sum), or materialize with CREATE TABLE AS SELECT."
            }
            ErrorKind::ReadOnlyViolation => {
                "Use write_query for INSERT/UPDATE/DELETE/DDL; read_query is for SELECT."
            }
            ErrorKind::UriNotAllowed => "Use an allowed source URI (a public http(s)/s3 path).",
            ErrorKind::QueryTimeout => {
                "Narrow the query (WHERE/LIMIT) or aggregate server-side, then retry."
            }
            ErrorKind::QueryCancelled => "Re-issue the query if you still need the result.",
            ErrorKind::Unauthenticated => {
                "Obtain a fresh access token for this deployment and retry with it."
            }
            ErrorKind::Storage | ErrorKind::Internal => {
                "Retry; if it persists, report to your operator."
            }
        }
    }

    /// The canonical `latiq://` resource for this kind — the deeper-learning link.
    pub fn default_see(self) -> &'static str {
        match self {
            ErrorKind::PondNotFound => "latiq://troubleshooting/pond-not-found",
            ErrorKind::DatasetNotFound => "latiq://datasets",
            ErrorKind::ResultCapExceeded => "latiq://troubleshooting/large-results",
            ErrorKind::ReadOnlyViolation
            | ErrorKind::ParseError
            | ErrorKind::WriteToReservedSchema => "latiq://dialect",
            ErrorKind::QueryTimeout => "latiq://troubleshooting/timeouts",
            ErrorKind::NameConflict
            | ErrorKind::InvalidValue
            | ErrorKind::MissingArgument
            | ErrorKind::UriNotAllowed => "latiq://guidance",
            // No dedicated resource: `latiq://troubleshooting` is a real served
            // resource, and inventing a URI here would send a caller after one
            // that does not exist.
            ErrorKind::Unauthenticated
            | ErrorKind::QueryCancelled
            | ErrorKind::Storage
            | ErrorKind::Internal => "latiq://troubleshooting",
        }
    }
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

    /// Build an envelope for `kind` with its canonical `suggest`/`see` defaults —
    /// the one-liner every surface uses so guidance is consistent. Pass only the
    /// specific `message`; use `new` when a call site needs bespoke suggest text.
    pub fn for_kind(kind: ErrorKind, message: impl Into<String>) -> Self {
        Self::new(kind, message, kind.default_suggest(), kind.default_see())
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
    fn for_kind_fills_canonical_suggest_and_see() {
        let e = ErrorEnvelope::for_kind(ErrorKind::PondNotFound, "Pond 'x' does not exist.");
        assert_eq!(e.message, "Pond 'x' does not exist.");
        assert_eq!(e.suggest, ErrorKind::PondNotFound.default_suggest());
        assert_eq!(e.see, "latiq://troubleshooting/pond-not-found");
    }

    #[test]
    fn as_str_matches_serde_snake_case() {
        // The metric-label name must equal the wire serialization, or dashboards
        // keyed on the envelope's kind won't match the metric.
        for kind in [
            ErrorKind::PondNotFound,
            ErrorKind::DatasetNotFound,
            ErrorKind::NameConflict,
            ErrorKind::ParseError,
            ErrorKind::InvalidValue,
            ErrorKind::MissingArgument,
            ErrorKind::WriteToReservedSchema,
            ErrorKind::ResultCapExceeded,
            ErrorKind::ReadOnlyViolation,
            ErrorKind::UriNotAllowed,
            ErrorKind::QueryTimeout,
            ErrorKind::QueryCancelled,
            ErrorKind::Unauthenticated,
            ErrorKind::Storage,
            ErrorKind::Internal,
        ] {
            let serde_name = serde_json::to_value(kind).unwrap();
            assert_eq!(serde_name.as_str().unwrap(), kind.as_str(), "{kind:?}");
        }
    }

    #[test]
    fn every_kind_has_non_empty_guidance() {
        for kind in [
            ErrorKind::PondNotFound,
            ErrorKind::DatasetNotFound,
            ErrorKind::NameConflict,
            ErrorKind::ParseError,
            ErrorKind::InvalidValue,
            ErrorKind::MissingArgument,
            ErrorKind::WriteToReservedSchema,
            ErrorKind::ResultCapExceeded,
            ErrorKind::ReadOnlyViolation,
            ErrorKind::UriNotAllowed,
            ErrorKind::QueryTimeout,
            ErrorKind::QueryCancelled,
            ErrorKind::Unauthenticated,
            ErrorKind::Storage,
            ErrorKind::Internal,
        ] {
            assert!(!kind.default_suggest().is_empty(), "{kind:?} suggest");
            assert!(
                kind.default_see().starts_with("latiq://"),
                "{kind:?} see must be a latiq:// resource"
            );
        }
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
