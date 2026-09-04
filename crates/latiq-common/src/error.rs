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
//!
//! An error here is not an exception to be logged — it is an **actionable**: a
//! structured instruction an agent can act on without waking a human. `kind` says
//! what went wrong, `suggest` names the next call, `see` teaches, and three
//! machine-readable fields decide the agent's control flow without parsing prose:
//! [`ErrorEnvelope::audience`] (who can fix this), [`ErrorEnvelope::retryable`]
//! (may I send this again, and must I change it first), and
//! [`ErrorEnvelope::facts`] (the numbers and names, as values rather than
//! sentences). The shape is pinned by the vendored JSON Schema in
//! `crates/latiq-common/spec/` — see that directory's README for why we validate
//! it ourselves.
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

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

/// Who can actually fix this failure.
///
/// An agent must be able to tell "you can fix this" from "only your operator
/// can" without reading prose: the first is a retry loop it should enter, the
/// second is one it must not. Derived per kind by [`ErrorKind::audience`].
///
/// **Two-valued on purpose.** A third `human` value was specified and dropped:
/// every failure we raise is fixable either by the caller or by whoever runs the
/// deployment, so `human` would have been a variant no envelope could carry —
/// the enum version of the dead `ErrorKind` this repo has shipped before. Adding
/// a variant later is backward-compatible for producers; removing a shipped one
/// is not. `error_contract_audience_partitions_the_kinds` pins the split, so a
/// new kind forces the decision rather than defaulting it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Audience {
    /// The caller. Fixable from the tool/RPC it just made — a corrected
    /// statement, a different name, a larger timeout, a fresh token.
    Agent,
    /// Whoever runs the deployment. No sequence of calls by the caller resolves
    /// it: a node has to come back, a disk has to work, a bug has to be fixed.
    Operator,
}

impl Audience {
    /// Every value, once — the same reason [`ErrorKind::ALL`] exists. The
    /// agent-facing documentation of this field (`latiq://guidance`) is pinned
    /// against this list by `latiq-mcp`, so a variant added here without being
    /// explained to agents fails the build rather than shipping as a value
    /// nothing on the surface defines.
    pub const ALL: [Audience; 2] = [Audience::Agent, Audience::Operator];

    /// The snake_case wire name (matches the serde `rename_all`
    /// serialization) — use it wherever the value is compared as a string, so
    /// prose and wire cannot disagree.
    pub fn as_str(self) -> &'static str {
        match self {
            Audience::Agent => "agent",
            Audience::Operator => "operator",
        }
    }
}

/// Whether sending this request again can ever work — the field that stops an
/// agent looping on a statement that can never succeed.
///
/// The distinction between `Never` and `AfterChange` is about **this call**, not
/// about the task: `read_query` handed a write is `Never`, because no edit to the
/// arguments of `read_query` makes a write legal there (the move is `write_query`,
/// a different call, which is what `suggest` says). A syntax error is
/// `AfterChange`, because the same call with a corrected statement is the fix.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Retryable {
    /// Do not send this call again. Either nothing about it can be changed into
    /// something that works, or the fix is not the caller's to make.
    Never,
    /// Send the same call again, unchanged. The failure was about timing,
    /// capacity or the world, not about the request.
    AsIs,
    /// Send this call again only after changing it — the arguments are what
    /// failed. `suggest` says what to change.
    AfterChange,
}

impl Retryable {
    /// Every value, once. See [`Audience::ALL`] — same guard, same reason: the
    /// field is a control channel only while the agent surface explains it, and
    /// `latiq-mcp` asserts `latiq://guidance` names every value in this list.
    pub const ALL: [Retryable; 3] = [Retryable::Never, Retryable::AsIs, Retryable::AfterChange];

    /// The snake_case wire name (matches the serde `rename_all` serialization).
    pub fn as_str(self) -> &'static str {
        match self {
            Retryable::Never => "never",
            Retryable::AsIs => "as_is",
            Retryable::AfterChange => "after_change",
        }
    }
}

/// One value behind a message: a number we MEASURED, or a name we were given.
///
/// Deliberately a closed scalar set. Facts exist so a client can branch on a
/// value instead of parsing it back out of a sentence, and so the sentence
/// cannot disagree with the value (see [`ErrorEnvelope::rendered`]); a nested
/// structure would be a second response shape smuggled into an error.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum Fact {
    /// A count, a cap, a millisecond budget — a number that was measured or
    /// applied, never one estimated to make prose read well (invariant 13).
    Number(u64),
    /// A name: a pond, a table, a column, an argument, a URI.
    Text(String),
    /// A yes/no the caller has to branch on. Rare, and only where the prose
    /// already states it: `compensated` on a failed allocation is the difference
    /// between "retry with the same name" and "an operator has work to do", and
    /// an agent should not have to find that in a sentence.
    Flag(bool),
}

impl std::fmt::Display for Fact {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Fact::Number(n) => write!(f, "{n}"),
            Fact::Text(s) => f.write_str(s),
            Fact::Flag(b) => write!(f, "{b}"),
        }
    }
}

impl From<u64> for Fact {
    fn from(v: u64) -> Self {
        Fact::Number(v)
    }
}

impl From<usize> for Fact {
    fn from(v: usize) -> Self {
        Fact::Number(v as u64)
    }
}

impl From<bool> for Fact {
    fn from(v: bool) -> Self {
        Fact::Flag(v)
    }
}

impl From<&str> for Fact {
    fn from(v: &str) -> Self {
        Fact::Text(v.to_string())
    }
}

impl From<String> for Fact {
    fn from(v: String) -> Self {
        Fact::Text(v)
    }
}

/// The named values behind one failure. A `BTreeMap` so the JSON object is
/// ordered deterministically — two identical failures must serialize identically.
pub type Facts = BTreeMap<String, Fact>;

/// Build a [`Facts`] map: `facts! { "pond" => pond_ref, "rows" => n }`.
#[macro_export]
macro_rules! facts {
    ($($k:expr => $v:expr),* $(,)?) => {{
        let mut m = $crate::error::Facts::new();
        $( m.insert($k.to_string(), $crate::error::Fact::from($v)); )*
        m
    }};
}

/// Substitute `{name}` placeholders in `template` with the fact of that name.
///
/// Applied to `suggest` as well as `message`, because a suggest sometimes has to
/// quote the same numbers the message does — a timeout error names the node's
/// ceiling in both — and two independent `format!`s is exactly the drift this
/// module exists to prevent.
///
/// A placeholder with no matching fact is left **verbatim**, which is ugly on
/// purpose: the alternative — dropping it, or filling it with an empty string —
/// produces a sentence that reads as complete and is missing the number the
/// caller needed. `error_contract_every_rendered_message_resolves_its_facts`
/// drives every fact-carrying constructor and fails on a leftover brace, so a
/// typo'd placeholder cannot reach a caller.
fn render_facts(template: &str, facts: &Facts) -> String {
    let mut out = String::with_capacity(template.len());
    let mut rest = template;
    while let Some(open) = rest.find('{') {
        let Some(close) = rest[open..].find('}').map(|i| open + i) else {
            break;
        };
        let key = &rest[open + 1..close];
        match facts.get(key) {
            Some(fact) => {
                out.push_str(&rest[..open]);
                out.push_str(&fact.to_string());
            }
            None => out.push_str(&rest[..=close]),
        }
        rest = &rest[close + 1..];
    }
    out.push_str(rest);
    out
}

/// The one error shape every surface returns.
///
/// Four fields are for reading — `kind` to branch on, `message` to read,
/// `suggest` to retry from, `see` to learn from — and the rest make it an
/// actionable rather than a report: `audience`, `retryable`, `facts`, and
/// `trace_id`/`traceparent` (the same trace, with and without our span). Prefer
/// [`ErrorEnvelope::for_kind`] (canonical guidance) or [`ErrorEnvelope::rendered`]
/// (guidance plus facts) so the wording stays the same wherever a kind surfaces.
///
/// There is deliberately **no `next` field**: `suggest` already names the next
/// call, and a second one would invite two spellings of one concept.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ErrorEnvelope {
    pub kind: ErrorKind,
    /// One sentence on what went wrong. No "suggest" text here.
    ///
    /// Where the sentence quotes a number or a name, it is **rendered from**
    /// [`Self::facts`] (see [`Self::rendered`]) rather than formatted
    /// independently — prose and numbers cannot drift if the prose is generated
    /// from the numbers.
    pub message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub location: Option<Location>,
    /// Copy-paste-ready corrected example — the immediate retry path.
    pub suggest: String,
    /// `latiq://` resource URI + anchor — the deeper learning path.
    pub see: String,
    /// Who can fix this. Defaults from `kind`; see [`Audience`].
    pub audience: Audience,
    /// Whether to send this call again, and whether it must change first.
    /// Defaults from `kind`; see [`Retryable`].
    pub retryable: Retryable,
    /// The named values behind `message`, so a client branches on a number
    /// instead of parsing it out of a sentence. Omitted when there are none —
    /// an engine error whose message is the engine's own words has no facts of
    /// ours, and inventing some would be inventing values.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub facts: Facts,
    /// The trace id of the request that failed, so an agent can cite the id of
    /// its own failed request. Stamped by each inbound adapter from the ambient
    /// trace scope; `None` outside one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub trace_id: Option<String>,
    /// The full W3C `traceparent` of the span that PRODUCED this error — the
    /// same trace as [`Self::trace_id`], plus the span id that field cannot
    /// carry.
    ///
    /// Added because `trace_id` alone gives a collector a flat set: every record
    /// under one trace, with no parent/child edge between the greeter and the
    /// owner of a forwarded query. A caller that wants only the join key still
    /// reads `trace_id`; a caller building a span tree gets a parent to attach
    /// to. **Additive on purpose** — nothing that reads `trace_id` changes.
    ///
    /// `None` where there is no span of ours to name: outside a trace scope, and
    /// on the control plane's Admin/Control surfaces, which deliberately keep no
    /// trace scope and mint no span (see `latiq-control-plane`'s `trace_meta`).
    /// An invented span id, logged nowhere and propagated nowhere, would look
    /// like a hierarchy and join nothing.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub traceparent: Option<String>,
}

impl ErrorKind {
    /// Every kind, once. The taxonomy is closed, and several guards have to
    /// enumerate it — the `see`-resolves check in `latiq-mcp`, the schema
    /// validation below, the audience/retryable partitions. Each of those used
    /// to carry its own hand-written copy of the list, which is how a kind gets
    /// added and covered by none of them.
    pub const ALL: [ErrorKind; 18] = [
        ErrorKind::PondNotFound,
        ErrorKind::DatasetNotFound,
        ErrorKind::NameConflict,
        ErrorKind::CatalogError,
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
        ErrorKind::PondUnavailable,
        ErrorKind::SourceUnavailable,
        ErrorKind::Storage,
        ErrorKind::Internal,
    ];

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
                "Narrow with WHERE/LIMIT, aggregate server-side (GROUP BY/count/sum), or \
                 materialize with CREATE TABLE AS SELECT. Call explain_query on the statement \
                 first: its `estimated_rows` is how many rows this would return, so you can see \
                 whether narrowing is enough before spending another attempt on it."
            }
            ErrorKind::ReadOnlyViolation => {
                "Use write_query for INSERT/UPDATE/DELETE/DDL; read_query is for SELECT."
            }
            ErrorKind::UriNotAllowed => "Use an allowed source URI (a public http(s)/s3 path).",
            ErrorKind::QueryTimeout => {
                "Retry with a larger timeout_ms (up to the node's maximum), or narrow the query \
                 (WHERE/LIMIT) or aggregate server-side. explain_query shows which table is \
                 scanned and how big it is estimated to be — it predicts no duration, but the \
                 largest full_scan in `scan_operations` is what to fix."
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

    /// Who can fix a failure of this kind. The split follows `default_suggest`
    /// exactly: a kind whose advice names a call the CALLER can make is
    /// `Agent`; a kind whose advice is addressed to whoever runs the deployment
    /// is `Operator`. If the two ever disagree, one of them is lying to an agent.
    pub fn audience(self) -> Audience {
        match self {
            // The caller's, every one: a different name, a corrected statement,
            // a narrower query, a fresh token, an allowed URI.
            ErrorKind::PondNotFound
            | ErrorKind::DatasetNotFound
            | ErrorKind::NameConflict
            | ErrorKind::CatalogError
            | ErrorKind::ParseError
            | ErrorKind::InvalidValue
            | ErrorKind::MissingArgument
            | ErrorKind::WriteToReservedSchema
            | ErrorKind::ResultCapExceeded
            | ErrorKind::ReadOnlyViolation
            | ErrorKind::UriNotAllowed
            | ErrorKind::QueryTimeout
            | ErrorKind::QueryCancelled
            | ErrorKind::Unauthenticated
            // The address in the statement is the caller's, and an operator
            // cannot fix a typo in it — the same reasoning that keeps this kind
            // out of `internal`.
            | ErrorKind::SourceUnavailable => Audience::Agent,
            // Nothing the caller can call resolves these: a node has to come
            // back, a disk has to work, a bug has to be fixed.
            ErrorKind::PondUnavailable | ErrorKind::Storage | ErrorKind::Internal => {
                Audience::Operator
            }
        }
    }

    /// Whether a caller may send the same call again — the field that stops an
    /// agent looping on a statement that can never succeed (#94, made
    /// structural). See [`Retryable`] for why `read_only_violation` is `Never`
    /// rather than `AfterChange`.
    pub fn retryable(self) -> Retryable {
        match self {
            // Nothing about the request failed: the deadline, the caller's
            // cancel, or a source that may be transiently unreachable. Its own
            // `suggest` says one retry is worth it, a second identical failure
            // is not.
            ErrorKind::QueryTimeout | ErrorKind::QueryCancelled | ErrorKind::SourceUnavailable => {
                Retryable::AsIs
            }
            // Ours to fix, and the caller cannot: retrying is the only thing it
            // CAN do, which is exactly what this kind's advice already says.
            ErrorKind::Storage | ErrorKind::Internal => Retryable::AsIs,
            // The arguments are what failed; `suggest` says what to change.
            ErrorKind::PondNotFound
            | ErrorKind::DatasetNotFound
            | ErrorKind::NameConflict
            | ErrorKind::CatalogError
            | ErrorKind::ParseError
            | ErrorKind::InvalidValue
            | ErrorKind::MissingArgument
            | ErrorKind::WriteToReservedSchema
            | ErrorKind::ResultCapExceeded
            | ErrorKind::UriNotAllowed
            // A new token is a change to the request, not the same request
            // again: replaying the rejected one loops forever.
            | ErrorKind::Unauthenticated => Retryable::AfterChange,
            // No edit to a `read_query` call makes a write legal on it. The move
            // is `write_query` — a different call, which is what `suggest` says.
            ErrorKind::ReadOnlyViolation => Retryable::Never,
            // An operator has to bring the node back or forget the pond; a
            // client retry loop only adds load to a cluster already unwell.
            ErrorKind::PondUnavailable => Retryable::Never,
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
            audience: kind.audience(),
            retryable: kind.retryable(),
            facts: Facts::new(),
            trace_id: None,
            traceparent: None,
        }
    }

    /// Build an envelope for `kind` with its canonical `suggest`/`see` defaults —
    /// the one-liner every surface uses so guidance is consistent. Pass only the
    /// specific `message`; use `new` when a call site needs bespoke suggest text.
    pub fn for_kind(kind: ErrorKind, message: impl Into<String>) -> Self {
        Self::new(kind, message, kind.default_suggest(), kind.default_see())
    }

    /// Build an envelope whose `message` is RENDERED from its `facts`.
    ///
    /// `template` is prose with `{name}` placeholders; each is replaced by the
    /// fact of that name. This is the mechanism behind invariant 13's "a number
    /// we report must be one we measured": a call site cannot state a number in
    /// the sentence without putting it in `facts`, and cannot change one without
    /// changing both, because there is only one copy.
    ///
    /// Use it wherever the sentence quotes a number or a name. Where the message
    /// is text we did not compose — an engine's own error string — use
    /// [`Self::for_kind`] and carry no facts, rather than inventing some.
    pub fn rendered(kind: ErrorKind, template: &str, facts: Facts) -> Self {
        Self::rendered_with(
            kind,
            template,
            facts,
            kind.default_suggest(),
            kind.default_see(),
        )
    }

    /// As [`Self::rendered`], with bespoke `suggest`/`see` for a call site whose
    /// next move is more specific than the kind's default. **`suggest` is a
    /// template too** — it is rendered from the same facts, so a number quoted
    /// in both sentences is one value, not two.
    pub fn rendered_with(
        kind: ErrorKind,
        template: &str,
        facts: Facts,
        suggest: &str,
        see: impl Into<String>,
    ) -> Self {
        Self {
            kind,
            message: render_facts(template, &facts),
            location: None,
            suggest: render_facts(suggest, &facts),
            see: see.into(),
            audience: kind.audience(),
            retryable: kind.retryable(),
            facts,
            trace_id: None,
            traceparent: None,
        }
    }

    pub fn with_location(mut self, loc: Location) -> Self {
        self.location = Some(loc);
        self
    }

    /// Stamp the request's trace id, so the caller can cite the id of its own
    /// failed request. Called by the inbound adapters from the ambient trace
    /// scope — never by a construction site deep in the core, which would have
    /// to remember to.
    pub fn with_trace_id(mut self, trace_id: Option<String>) -> Self {
        self.trace_id = trace_id;
        self
    }

    /// Stamp the `traceparent` of the span producing this envelope — **keeping
    /// one that is already there**.
    ///
    /// That is the difference from [`Self::with_trace_id`], and it is not a
    /// style choice. The trace id is the same on both sides of a node hop, so
    /// re-stamping it is a no-op; the span id is not. An envelope decoded from
    /// the pond's owner names the OWNER's span, and the owner is the node that
    /// produced the failure — the same rule `QueryMeta` follows for
    /// `served_by`/`trace_id`, where a forwarding node never overwrites what it
    /// relayed with its own. One rule for both records: the span named is the
    /// span that did the work this record describes.
    pub fn with_traceparent(mut self, traceparent: Option<String>) -> Self {
        if self.traceparent.is_none() {
            self.traceparent = traceparent;
        }
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::{json, Value};

    /// The vendored schema, read at COMPILE time: an `include_str!` cannot pass
    /// because a file was missing at runtime.
    const SCHEMA: &str = include_str!("../spec/ErrorEnvelope-1-0-1.json");

    fn validator() -> jsonschema::Validator {
        let schema: Value = serde_json::from_str(SCHEMA).expect("the vendored schema is JSON");
        jsonschema::validator_for(&schema).expect("the vendored schema is a valid JSON Schema")
    }

    /// A representative envelope for `kind`, exercising every optional field
    /// (facts, location, trace_id) on at least one of them.
    fn sample(kind: ErrorKind) -> ErrorEnvelope {
        let env = ErrorEnvelope::rendered(
            kind,
            "Pond '{pond}' failed after {duration_ms} ms.",
            facts! { "pond" => "incident-001", "duration_ms" => 42u64 },
        )
        .with_trace_id(Some("4bf92f3577b34da6a3ce929d0e0e4736".into()))
        .with_traceparent(Some(
            "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01".into(),
        ));
        if kind == ErrorKind::ParseError {
            env.with_location(Location {
                line: 1,
                column: 8,
                byte: 7,
            })
        } else {
            env
        }
    }

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
        assert!(v.get("facts").is_none(), "empty facts omitted");
        assert!(v.get("trace_id").is_none(), "an unset trace id is omitted");
    }

    #[test]
    fn for_kind_fills_canonical_suggest_and_see() {
        let e = ErrorEnvelope::for_kind(ErrorKind::PondNotFound, "Pond 'x' does not exist.");
        assert_eq!(e.message, "Pond 'x' does not exist.");
        assert_eq!(e.suggest, ErrorKind::PondNotFound.default_suggest());
        assert_eq!(e.see, "latiq://troubleshooting/pond-not-found");
        assert_eq!(e.audience, Audience::Agent);
        assert_eq!(e.retryable, Retryable::AfterChange);
    }

    #[test]
    fn as_str_matches_serde_snake_case() {
        // The metric-label name must equal the wire serialization, or dashboards
        // keyed on the envelope's kind won't match the metric.
        for kind in ErrorKind::ALL {
            let serde_name = serde_json::to_value(kind).unwrap();
            assert_eq!(serde_name.as_str().unwrap(), kind.as_str(), "{kind:?}");
        }
    }

    #[test]
    fn every_kind_has_non_empty_guidance() {
        for kind in ErrorKind::ALL {
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

    /// The whole point of `facts`: the sentence is GENERATED from the values, so
    /// prose and numbers cannot drift.
    #[test]
    fn error_contract_a_rendered_message_is_generated_from_its_facts() {
        let e = ErrorEnvelope::rendered(
            ErrorKind::ResultCapExceeded,
            "Result has {rows} rows, over the inline cap of {cap}.",
            facts! { "rows" => 10_001usize, "cap" => 10_000usize },
        );
        assert_eq!(
            e.message,
            "Result has 10001 rows, over the inline cap of 10000."
        );
        assert_eq!(e.facts["rows"], Fact::Number(10_001));
        // And the values are on the wire as VALUES, not only inside the prose:
        // a client sizing its next LIMIT must not have to parse the sentence.
        let v = serde_json::to_value(&e).unwrap();
        assert_eq!(v["facts"]["rows"], json!(10_001));
        assert_eq!(v["facts"]["cap"], json!(10_000));
    }

    /// `suggest` is rendered from the same facts as `message`, because a number
    /// quoted in both (a timeout's ceiling) must be one value, not two.
    #[test]
    fn error_contract_a_rendered_suggest_shares_the_message_facts() {
        let e = ErrorEnvelope::rendered_with(
            ErrorKind::QueryTimeout,
            "Query stopped after {timeout_ms} ms.",
            facts! { "timeout_ms" => 500u64, "max_timeout_ms" => 60_000u64 },
            "Retry with a larger timeout_ms (up to {max_timeout_ms}).",
            "latiq://troubleshooting/timeouts",
        );
        assert_eq!(e.message, "Query stopped after 500 ms.");
        assert_eq!(e.suggest, "Retry with a larger timeout_ms (up to 60000).");
    }

    /// Regression guard on the renderer itself: a placeholder with no fact is
    /// left VERBATIM. Dropping it, or filling it with an empty string, produces
    /// a sentence that reads as complete and is missing the number the caller
    /// needed — and the leftover brace is what lets
    /// `error_contract_every_rendered_message_resolves_its_facts` (in
    /// `latiq-agent-core`) catch a typo'd placeholder at all.
    #[test]
    fn error_contract_an_unresolved_placeholder_is_left_visible() {
        let e = ErrorEnvelope::rendered(
            ErrorKind::InvalidValue,
            "Pond '{pond}' rejected {whoops}.",
            facts! { "pond" => "p1" },
        );
        assert_eq!(e.message, "Pond 'p1' rejected {whoops}.");
    }

    /// Facts are only what we were given or measured. A message that is text we
    /// did not compose — an engine's own error string — carries none, rather
    /// than a plausible-looking invention.
    #[test]
    fn error_contract_a_passthrough_message_carries_no_facts() {
        let e = ErrorEnvelope::for_kind(
            ErrorKind::ParseError,
            "Parser Error: syntax error at or near \"SELEKT\"",
        );
        assert!(e.facts.is_empty());
        assert!(serde_json::to_value(&e).unwrap().get("facts").is_none());
    }

    /// The four kinds #100 names as load-bearing, plus their audience — the
    /// pairs an agent's control flow actually branches on.
    #[test]
    fn error_contract_retryable_says_whether_to_send_the_call_again() {
        // A typo is fixed in the statement and re-sent: the one case where
        // looping is right, and only after a change.
        assert_eq!(ErrorKind::ParseError.retryable(), Retryable::AfterChange);
        // The deadline is about timing, not about the request.
        assert_eq!(ErrorKind::QueryTimeout.retryable(), Retryable::AsIs);
        // Ours to fix; retrying is the only thing the caller CAN do, which is
        // what this kind's advice already says.
        assert_eq!(ErrorKind::Internal.retryable(), Retryable::AsIs);
        assert_eq!(ErrorKind::Internal.audience(), Audience::Operator);
        // No edit to a `read_query` call makes a write legal on it. This is the
        // field that has to stop the loop: `after_change` here would send an
        // agent round rewriting SQL for a tool that will never accept it.
        assert_eq!(
            ErrorKind::ReadOnlyViolation.retryable(),
            Retryable::Never,
            "read_query handed a write can never succeed on THIS call"
        );
        assert_eq!(ErrorKind::ReadOnlyViolation.audience(), Audience::Agent);
    }

    /// The wire names the agent surface documents must be the wire names we
    /// serialize. `latiq://guidance` teaches these values as literal strings
    /// (`retryable: after_change`), and `latiq-mcp` checks the text against
    /// `as_str()` — so if `as_str()` and serde ever disagreed, the guard would
    /// pass while teaching agents a value they will never receive.
    #[test]
    fn error_contract_audience_and_retryable_wire_names_match_serde() {
        for a in Audience::ALL {
            assert_eq!(serde_json::to_value(a).unwrap(), json!(a.as_str()));
        }
        for r in Retryable::ALL {
            assert_eq!(serde_json::to_value(r).unwrap(), json!(r.as_str()));
        }
        assert_eq!(Audience::ALL.len(), 2);
        assert_eq!(Retryable::ALL.len(), 3);
    }

    /// The claim `latiq://guidance` makes about `never`, run rather than
    /// proofread: **`never` is about THIS call, and the goal may still be
    /// reachable — `suggest` names the different call that reaches it.**
    ///
    /// An agent that reads "never" and finds no alternative in the advice
    /// abandons the sub-goal, which is the failure mode the Nexus audit flagged
    /// (its finding 2) and the wording is chosen to avoid. Asserted across every
    /// `Never` kind rather than the one we happen to be thinking of, so a new
    /// one cannot be added with advice that is a dead end.
    #[test]
    fn error_contract_never_still_names_a_move() {
        // Not exhaustive, and it does not need to be: the property is "the
        // advice names a call", so any one of these appearing proves it.
        const CALLS: [&str; 6] = [
            "write_query",
            "read_query",
            "describe_pond",
            "list_ponds",
            "allocate_pond",
            "explain_query",
        ];
        let mut agent = 0;
        let mut operator = 0;
        for kind in ErrorKind::ALL
            .iter()
            .copied()
            .filter(|k| k.retryable() == Retryable::Never)
        {
            let suggest = kind.default_suggest();
            match kind.audience() {
                Audience::Agent => {
                    agent += 1;
                    assert!(
                        CALLS.iter().any(|c| suggest.contains(c)),
                        "{kind:?} is `never` and the caller's to fix, so its advice must name \
                         the call that DOES work — otherwise `never` reads as 'give up': {suggest}"
                    );
                }
                Audience::Operator => {
                    operator += 1;
                    // The other half of `never`: there is no different call,
                    // and the advice must say who to tell instead of leaving
                    // the agent to invent one.
                    assert!(suggest.contains("operator"), "{kind:?}: {suggest}");
                }
            }
        }
        // Anti-vacuity: both branches ran, on the kinds we mean.
        assert_eq!(agent, 1, "ReadOnlyViolation");
        assert_eq!(operator, 1, "PondUnavailable");
    }

    /// `traceparent` names the span that PRODUCED the error, so an envelope
    /// relayed from a pond's owner keeps the owner's span across the hop — the
    /// same rule `QueryMeta` follows for `served_by`. The trace id, being equal
    /// on both sides, is re-stamped freely; the span id is not.
    #[test]
    fn error_contract_a_relayed_traceparent_is_not_overwritten_by_the_forwarder() {
        const OWNER: &str = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01";
        const GREETER: &str = "00-4bf92f3577b34da6a3ce929d0e0e4736-b7a9021ba760f000-01";
        let relayed = ErrorEnvelope::for_kind(ErrorKind::CatalogError, "no such table")
            .with_traceparent(Some(OWNER.into()))
            .with_traceparent(Some(GREETER.into()));
        assert_eq!(relayed.traceparent.as_deref(), Some(OWNER));
        // …and an envelope raised locally still gets stamped.
        let local = ErrorEnvelope::for_kind(ErrorKind::CatalogError, "no such table")
            .with_traceparent(Some(GREETER.into()));
        assert_eq!(local.traceparent.as_deref(), Some(GREETER));
        // Absent by default: a span we do not have is not invented.
        assert!(ErrorEnvelope::for_kind(ErrorKind::Internal, "boom")
            .traceparent
            .is_none());
    }

    /// `audience` must agree with `suggest`: a kind whose advice tells the
    /// caller to fetch an operator is not the caller's to fix, and a kind whose
    /// advice names a call the caller can make is. If the two disagree, one of
    /// them is lying to an agent.
    #[test]
    fn error_contract_audience_partitions_the_kinds() {
        let mut operator = 0;
        let mut agent = 0;
        for kind in ErrorKind::ALL {
            let suggest = kind.default_suggest();
            match kind.audience() {
                Audience::Operator => {
                    operator += 1;
                    assert!(
                        suggest.contains("operator"),
                        "{kind:?} is the operator's to fix, so its advice must say so: {suggest}"
                    );
                    // And it must never invite an unbounded retry loop from a
                    // caller who cannot resolve it.
                    assert_ne!(kind.retryable(), Retryable::AfterChange, "{kind:?}");
                }
                Audience::Agent => {
                    agent += 1;
                    // The inverse is NOT "never mentions an operator": several
                    // agent-fixable kinds legitimately end with "if it keeps
                    // failing, tell your operator". What must hold is that the
                    // advice opens with something the caller can do itself.
                    assert!(
                        !suggest.starts_with("Retry; if it persists"),
                        "{kind:?} is the caller's to fix, so its advice must not be \
                         the operator hand-off: {suggest}"
                    );
                }
            }
        }
        // Anti-vacuity: both sides are populated, and every kind was classified.
        assert_eq!(operator, 3, "PondUnavailable, Storage, Internal");
        assert_eq!(agent, 15);
        assert_eq!(agent + operator, ErrorKind::ALL.len());
    }

    /// The guarantee rmcp does not give us: every envelope we can construct
    /// matches the vendored schema. Drives ALL 18 kinds, because the schema
    /// enumerates the `kind` values — a kind added to the enum without being
    /// listed there must fail here rather than ship unannounced.
    #[test]
    fn error_contract_every_kind_validates_against_the_vendored_schema() {
        let v = validator();
        for kind in ErrorKind::ALL {
            let value = serde_json::to_value(sample(kind)).unwrap();
            let errors: Vec<String> = v.iter_errors(&value).map(|e| e.to_string()).collect();
            assert!(errors.is_empty(), "{kind:?}: {errors:?}\n{value:#}");
        }
        assert_eq!(ErrorKind::ALL.len(), 18, "every kind was driven");
    }

    /// **Anti-vacuity for the test above.** A schema that accepts anything would
    /// leave it green while guaranteeing nothing — the same failure mode as a CI
    /// check that passes because it executed nothing. So break a real envelope in
    /// each way the schema exists to catch, and require a rejection every time.
    #[test]
    fn error_contract_the_vendored_schema_rejects_a_malformed_envelope() {
        let v = validator();
        let good = serde_json::to_value(sample(ErrorKind::PondNotFound)).unwrap();
        assert!(
            v.is_valid(&good),
            "the baseline must be valid, or the mutations below prove nothing"
        );

        let mutate = |f: &dyn Fn(&mut Value)| {
            let mut bad = good.clone();
            f(&mut bad);
            bad
        };
        let cases: Vec<(&str, Value)> = vec![
            (
                "a kind outside the closed taxonomy",
                mutate(&|b| b["kind"] = json!("something_new")),
            ),
            (
                "an audience value we do not define",
                mutate(&|b| b["audience"] = json!("human")),
            ),
            (
                "a retryable value we do not define",
                mutate(&|b| b["retryable"] = json!("maybe")),
            ),
            (
                "an empty message — an actionable with nothing to read",
                mutate(&|b| b["message"] = json!("")),
            ),
            (
                "an empty suggest — an actionable with no next call",
                mutate(&|b| b["suggest"] = json!("")),
            ),
            (
                "a see that is not a latiq:// resource",
                mutate(&|b| b["see"] = json!("https://example.com/help")),
            ),
            (
                "a nested fact — a second response shape smuggled into an error",
                mutate(&|b| b["facts"]["rows"] = json!({"exact": 10})),
            ),
            (
                "a trace id that is not a W3C trace-id",
                mutate(&|b| b["trace_id"] = json!("not-a-trace")),
            ),
            (
                "a traceparent missing its span — the half a bare trace id already gave us",
                mutate(&|b| b["traceparent"] = json!("00-4bf92f3577b34da6a3ce929d0e0e4736-01")),
            ),
            (
                "an extra field — a surface inventing its own contract",
                mutate(&|b| b["next"] = json!("call describe_pond")),
            ),
            ("a missing required field", {
                let mut b = good.clone();
                b.as_object_mut().unwrap().remove("retryable");
                b
            }),
        ];
        for (why, bad) in &cases {
            assert!(!v.is_valid(bad), "the schema must reject {why}: {bad}");
        }
        assert_eq!(cases.len(), 11, "every mutation was probed");
    }
}
