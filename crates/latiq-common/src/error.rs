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

/// Where in the caller's SQL the error was found, so a client can point at it
/// rather than restating the whole statement.
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
    /// The statement is valid SQL and a name in it does not resolve against the
    /// pond — or already exists there. Deliberately NOT `ParseError`: nothing is
    /// wrong with the syntax, so "check the SQL against the dialect" sends the
    /// agent to read a grammar when what it needs is `SHOW TABLES`. And
    /// deliberately not `Internal`: `CREATE TABLE t` on an existing `t` used to
    /// arrive as "Retry; if it persists, report to your operator", which is
    /// advice to repeat a statement that cannot ever succeed and then to wake a
    /// human about it. Also not `NameConflict`, which is about POND names and
    /// tells the caller to pick a different pond name.
    CatalogError,
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
    /// The pond EXISTS in the registry but no registered node is serving it —
    /// its owning node is not in the registry, so nothing can reach its files.
    /// Deliberately NOT `PondNotFound`: the record is there and the name still
    /// resolves, so "allocate a new one" is the wrong advice, and a node that
    /// answered such a request locally would silently create an empty pond of
    /// its own and hand back plausible, empty results. Deliberately not
    /// `Storage`/`Internal` either: nothing failed and nothing crashed, and
    /// neither "retry" nor "report a bug" is the action that fixes it — an
    /// operator has to bring the node back or forget the pond.
    PondUnavailable,
    /// A data source named IN THE STATEMENT could not be read or written — a
    /// URL, an object-store path, a file. The failure is outside the pond and
    /// usually outside this deployment, so neither `Storage` nor `Internal`
    /// fits: nothing of ours broke, and "report to your operator" is the wrong
    /// move for a mistyped URL. The action is the caller's: fix the address or
    /// the credentials, or accept that the source is down.
    SourceUnavailable,
    Storage,
    Internal,
}

/// The one error shape every surface returns. Four fields, each with a distinct
/// job: `kind` to branch on, `message` to read, `suggest` to retry from, `see`
/// to learn from. Prefer [`ErrorEnvelope::for_kind`] so the guidance stays the
/// same wherever a kind surfaces.
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
            ErrorKind::CatalogError => "catalog_error",
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
            ErrorKind::PondUnavailable => "pond_unavailable",
            ErrorKind::SourceUnavailable => "source_unavailable",
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
            // The one an agent hits most in ordinary work (`INSERT INTO nope`),
            // so it names the exact call that answers it — first, and by name.
            ErrorKind::CatalogError => {
                "Look up what the pond actually has before retrying: describe_pond, or read_query \
                 \"SHOW TABLES\" / \"DESCRIBE <table>\" / \"SELECT * FROM \
                 information_schema.columns\". Then use a name that exists — or create it with \
                 write_query first. If the name already exists, pick another or use CREATE OR \
                 REPLACE / CREATE TABLE IF NOT EXISTS."
            }
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
            // Addressed to an OPERATOR, because no agent action fixes it: the
            // pond's node has to come back, or the record has to go.
            ErrorKind::PondUnavailable => {
                "Ask an operator to bring the pond's node back (latiq node list shows which are \
                 registered); if it is gone for good, `latiq pond forget <pond> --confirm` drops \
                 the registry record — the data on that node is NOT deleted."
            }
            // Addressed to the CALLER: the address in the statement is the
            // caller's, and an operator cannot fix a typo in it.
            ErrorKind::SourceUnavailable => {
                "Check the path or URL in the statement — spelling, host, bucket, and whether it \
                 needs credentials this deployment does not have. It must be reachable from the \
                 node, whose network is not yours. One retry is worth it if the \
                 failure could be a transient network fault; a second identical failure means the \
                 source, not the query. To work offline of it, load the data into the pond first \
                 (load_dataset / pull_catalog)."
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
            ErrorKind::CatalogError => "latiq://troubleshooting/catalog-error",
            ErrorKind::SourceUnavailable => "latiq://troubleshooting/source-unavailable",
            ErrorKind::PondUnavailable => "latiq://troubleshooting/pond-unavailable",
            ErrorKind::NameConflict
            | ErrorKind::InvalidValue
            | ErrorKind::MissingArgument
            | ErrorKind::UriNotAllowed => "latiq://guidance",
            // These four used to share `latiq://troubleshooting`, the INDEX.
            // It resolved, so the guard was green — and it covered none of
            // them, so an agent holding the two worst envelopes landed on a
            // menu of other agents' problems. Each now points at a page that
            // names its kind (pinned by
            // `latiq-mcp::resources::error_contract_a_troubleshooting_see_names_its_own_kind`).
            ErrorKind::Unauthenticated => "latiq://troubleshooting/unauthenticated",
            // The timeouts page already contrasts the two, because "the node's
            // deadline" vs "somebody asked me to stop" is the whole question an
            // agent has when a query is killed.
            ErrorKind::QueryCancelled => "latiq://troubleshooting/timeouts",
            ErrorKind::Storage | ErrorKind::Internal => "latiq://troubleshooting/internal",
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
            ErrorKind::CatalogError,
            ErrorKind::InvalidValue,
            ErrorKind::MissingArgument,
            ErrorKind::WriteToReservedSchema,
            ErrorKind::ResultCapExceeded,
            ErrorKind::ReadOnlyViolation,
            ErrorKind::UriNotAllowed,
            ErrorKind::QueryTimeout,
            ErrorKind::QueryCancelled,
            ErrorKind::Unauthenticated,
            ErrorKind::PondUnavailable,
            ErrorKind::SourceUnavailable,
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
            ErrorKind::CatalogError,
            ErrorKind::InvalidValue,
            ErrorKind::MissingArgument,
            ErrorKind::WriteToReservedSchema,
            ErrorKind::ResultCapExceeded,
            ErrorKind::ReadOnlyViolation,
            ErrorKind::UriNotAllowed,
            ErrorKind::QueryTimeout,
            ErrorKind::QueryCancelled,
            ErrorKind::Unauthenticated,
            ErrorKind::PondUnavailable,
            ErrorKind::SourceUnavailable,
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
