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

//! Agent-facing errors carrying a structured `ErrorEnvelope`.
use latiq_common::{facts, ErrorEnvelope, ErrorKind, Facts};
use latiq_engine::EngineError;

/// The core's one error type: a newtype over [`ErrorEnvelope`], so an error that
/// crosses a node hop or a surface boundary keeps the kind and guidance it was
/// created with rather than being re-derived at each layer.
///
/// **Boxed**, so `AgentError` is one pointer wide. Nearly every method in this
/// crate returns `Result<_, AgentError>`, and the envelope grew past clippy's
/// `result_large_err` threshold when it gained `facts` and `trace_id` — which is
/// the lint working: an error carried by value in every `Result` in the system
/// makes the SUCCESS path pay for the failure path's size. An error is cold, so
/// the indirection costs nothing that matters.
#[derive(Debug, Clone)]
pub struct AgentError(Box<ErrorEnvelope>);

impl AgentError {
    pub fn new(
        kind: ErrorKind,
        message: impl Into<String>,
        suggest: impl Into<String>,
        see: impl Into<String>,
    ) -> Self {
        AgentError(Box::new(ErrorEnvelope::new(kind, message, suggest, see)))
    }

    /// Wrap an already-built envelope (e.g. one decoded from a gRPC `Status`'s
    /// details, or produced by `ControlPlaneError::envelope()`), so every surface
    /// carries the same guidance rather than re-deriving it.
    pub fn from_envelope(env: ErrorEnvelope) -> Self {
        AgentError(Box::new(env))
    }

    pub fn envelope(&self) -> &ErrorEnvelope {
        &self.0
    }

    pub fn into_envelope(self) -> ErrorEnvelope {
        *self.0
    }

    /// Build from `kind`'s canonical suggest/see defaults (the single source in
    /// `latiq-common`); pass only the specific message.
    ///
    /// For a message that quotes a number or a name, use [`Self::rendered`]
    /// instead: it renders the sentence FROM the facts, so the two cannot drift.
    pub fn of_kind(kind: ErrorKind, message: impl Into<String>) -> Self {
        AgentError(Box::new(ErrorEnvelope::for_kind(kind, message)))
    }

    /// Build from `kind`'s canonical guidance with a message rendered from
    /// `facts` — see [`ErrorEnvelope::rendered`].
    pub fn rendered(kind: ErrorKind, template: &str, facts: Facts) -> Self {
        AgentError(Box::new(ErrorEnvelope::rendered(kind, template, facts)))
    }

    /// As [`Self::rendered`] with bespoke `suggest`/`see`, both rendered from
    /// the same facts — see [`ErrorEnvelope::rendered_with`].
    pub fn rendered_with(
        kind: ErrorKind,
        template: &str,
        facts: Facts,
        suggest: &str,
        see: impl Into<String>,
    ) -> Self {
        AgentError(Box::new(ErrorEnvelope::rendered_with(
            kind, template, facts, suggest, see,
        )))
    }

    pub fn pond_not_found(pond_ref: &str) -> Self {
        Self::rendered(
            ErrorKind::PondNotFound,
            "Pond '{pond}' does not exist.",
            facts! { "pond" => pond_ref },
        )
    }

    /// The pond resolves, but the registry names no node that is serving it —
    /// see [`ErrorKind::PondUnavailable`]. Raised INSTEAD of falling through to
    /// a local execution: the node that received the request does not hold this
    /// pond's files, and serving it here would create an empty pond of the same
    /// name and answer with plausible, empty results.
    pub fn pond_unavailable(pond_ref: &str) -> Self {
        Self::rendered(
            ErrorKind::PondUnavailable,
            "Pond '{pond}' exists, but the node that owns it is not registered with this \
             deployment — no node is currently serving it, and this node does not hold its data.",
            facts! { "pond" => pond_ref },
        )
    }

    pub fn name_conflict(name: &str) -> Self {
        Self::rendered(
            ErrorKind::NameConflict,
            "A pond named '{name}' already exists.",
            facts! { "name" => name },
        )
    }

    /// The result was fully materialized before the cap was checked, so `rows`
    /// is the **true total** and the caller can size its next attempt from it.
    ///
    /// Use this ONLY where that is true. The streaming collector cannot say it —
    /// see [`AgentError::result_cap_exceeded_unknown`], and the sentences are
    /// deliberately different so the two can never be mistaken for each other.
    pub fn result_cap_exceeded(rows: usize, cap: usize) -> Self {
        Self::rendered(
            ErrorKind::ResultCapExceeded,
            "Result has {rows} rows, over the inline cap of {cap}.",
            // `rows` is an exact total, which is the whole difference between
            // this constructor and the one below — a client sizing its next
            // LIMIT can read it as a number here and cannot there.
            facts! { "rows" => rows, "cap" => cap },
        )
    }

    /// The cap was crossed **mid-stream**, so the total is not known and must
    /// not be implied.
    ///
    /// The streaming collector stops on the first Arrow batch that carries it
    /// past the cap, and it used to report the number of rows it had collected
    /// when it stopped — a batch boundary. Every result from 10 240 rows to a
    /// billion reported "10240", which is not merely imprecise: it is a
    /// plausible number, two per cent over the cap, and an agent that trusts it
    /// narrows by two per cent and fails again with the same number. Counting
    /// the rest honestly would mean draining the whole result the caller just
    /// asked us not to hand back, so the answer is to stop claiming precision.
    pub fn result_cap_exceeded_unknown(cap: usize) -> Self {
        Self::rendered(
            ErrorKind::ResultCapExceeded,
            "Result has more than {cap} rows, over the inline cap of {cap}. Collection stopped at \
             the cap, so the exact row count is not known — use `SELECT count(*)` or \
             explain_query's `estimated_rows` to size it.",
            // Deliberately NO `rows` fact. The count is not known, and a fact is
            // a value we measured; supplying the batch boundary here is the very
            // bug the two-constructor split exists to prevent.
            facts! { "cap" => cap },
        )
    }

    pub fn dataset_not_found(reference: &str) -> Self {
        Self::rendered(
            ErrorKind::DatasetNotFound,
            "Dataset '{dataset}' is not in the catalog.",
            facts! { "dataset" => reference },
        )
    }

    pub fn unsupported_extension(message: impl Into<String>) -> Self {
        // Bespoke suggest (extension-specific), so not the InvalidValue default.
        Self::new(
            ErrorKind::InvalidValue,
            message,
            "Request only signed/official extensions baked into this deployment; see latiq://guidance for the supported set.",
            "latiq://guidance",
        )
    }

    /// The node cut the query on its deadline. Names BOTH numbers — the timeout
    /// that was actually in effect (which may be a clamped version of what the
    /// caller asked for) and the node's ceiling — because those two are what
    /// decide the agent's next move, and it can obtain neither any other way.
    ///
    /// The `suggest` covers the three levers, and drops the one that is not
    /// available: at the ceiling there is no larger `timeout_ms` to retry with,
    /// and telling an agent to ask for one would send it round a loop it cannot
    /// win. That case is the tier's problem, not the timeout's.
    pub fn query_timeout(effective_ms: u64, max_ms: u64) -> Self {
        let at_ceiling = effective_ms >= max_ms;
        let facts = facts! { "timeout_ms" => effective_ms, "max_timeout_ms" => max_ms };
        let suggest = if at_ceiling {
            "This ran at the node's maximum, so a larger timeout_ms is not available. Narrow the \
             query — add a WHERE on a selective column, a LIMIT, or fewer columns — or aggregate \
             server-side (GROUP BY/count/sum) instead of scanning. If the work is genuinely this \
             large, it is too big for this pond's tier: ask an operator to re-tier the pond."
        } else {
            // Both numbers in the suggest come from the same facts as the
            // message, so the ceiling an agent is told to retry under can never
            // differ from the one the message quotes.
            "Retry with a larger timeout_ms (this node allows up to {max_timeout_ms}), or narrow \
             the query — add a WHERE on a selective column, a LIMIT, or fewer columns — or \
             aggregate server-side (GROUP BY/count/sum). If it still times out at \
             {max_timeout_ms} ms, the query is too large for this pond's tier: ask an operator to \
             re-tier it."
        };
        Self::rendered_with(
            ErrorKind::QueryTimeout,
            "Query stopped after {timeout_ms} ms — the timeout in effect for this request. This \
             node allows up to {max_timeout_ms} ms.",
            facts,
            suggest,
            ErrorKind::QueryTimeout.default_see(),
        )
    }

    pub fn internal(message: impl Into<String>) -> Self {
        Self::of_kind(ErrorKind::Internal, message)
    }
}

impl std::fmt::Display for AgentError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0.message)
    }
}

impl std::error::Error for AgentError {}

/// The engine's classification → the agent-facing kind.
///
/// One rule governs every arm: the kind and its `suggest` must name a next move
/// the CALLER can make, and must not name one it cannot. `internal` + "Retry; if
/// it persists, report to your operator" is the answer for a failure of ours —
/// it was being given for a duplicate table name, a mistyped column and an
/// unreachable URL, none of which retrying or an operator can fix.
impl From<EngineError> for AgentError {
    fn from(e: EngineError) -> Self {
        match e {
            EngineError::ReadOnlyViolation => AgentError::of_kind(
                ErrorKind::ReadOnlyViolation,
                "read_query received a statement that is not read-only.",
            ),
            EngineError::Cancelled => {
                AgentError::of_kind(ErrorKind::QueryCancelled, "The query was cancelled.")
            }
            EngineError::Timeout => {
                AgentError::of_kind(ErrorKind::QueryTimeout, "The query exceeded the timeout.")
            }
            // The message is passed through as the engine gave it. It used to be
            // prefixed with "SQL parse error:", which was a false statement for
            // every catalog, conversion and I/O failure that ended up in this
            // arm — and is redundant for the one that belongs here, since the
            // engine's own message already begins "Parser Error:".
            EngineError::Parse(m) => AgentError::of_kind(ErrorKind::ParseError, m),
            EngineError::Catalog(m) => AgentError::of_kind(ErrorKind::CatalogError, m),
            // Kind: the value is invalid. Suggest: bespoke, because the two
            // ways a value can be rejected have different fixes — a wrong TYPE
            // is fixed in the statement, a constraint violation is about the
            // rows already in the table.
            EngineError::Conversion(m) => AgentError::new(
                ErrorKind::InvalidValue,
                m,
                "A value does not match the type it is being used as. Check the column types \
                 (read_query \"DESCRIBE <table>\" or describe_pond), then supply a value of that \
                 type or CAST it explicitly — e.g. CAST('7' AS INTEGER). Quoted text is never \
                 coerced into a numeric column just because it looks numeric.",
                "latiq://dialect",
            ),
            EngineError::Constraint(m) => AgentError::new(
                ErrorKind::InvalidValue,
                m,
                "The value is the right type but breaks a rule on the table (primary key, unique, \
                 not null, or check). Read the conflicting rows first — read_query \"SELECT * FROM \
                 <table> WHERE <key> = <value>\" — then either correct the value, UPDATE the \
                 existing row instead of inserting, or use INSERT OR REPLACE / ON CONFLICT.",
                "latiq://dialect",
            ),
            EngineError::SourceIo(m) => AgentError::of_kind(ErrorKind::SourceUnavailable, m),
            EngineError::Engine(m) => AgentError::internal(format!("engine error: {m}")),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The mapping table, at the cheapest layer that can prove it.
    ///
    /// The full-stack tests prove an agent really receives these kinds; this
    /// proves the mapping itself is total and that no caller-fixable engine
    /// failure lands on `internal` — the one bucket whose advice ("retry, then
    /// wake a human") is wrong for everything the caller could fix.
    #[test]
    fn error_contract_every_engine_error_maps_to_a_kind_the_caller_can_act_on() {
        let cases = [
            (
                EngineError::Parse("Parser Error: x".into()),
                ErrorKind::ParseError,
            ),
            (
                EngineError::Catalog("Catalog Error: x".into()),
                ErrorKind::CatalogError,
            ),
            (
                EngineError::Conversion("Conversion Error: x".into()),
                ErrorKind::InvalidValue,
            ),
            (
                EngineError::Constraint("Constraint Error: x".into()),
                ErrorKind::InvalidValue,
            ),
            (
                EngineError::SourceIo("IO Error: x".into()),
                ErrorKind::SourceUnavailable,
            ),
            (EngineError::ReadOnlyViolation, ErrorKind::ReadOnlyViolation),
            (EngineError::Cancelled, ErrorKind::QueryCancelled),
            (EngineError::Timeout, ErrorKind::QueryTimeout),
        ];
        // Anti-vacuity: the list is every variant except `Engine`, which is the
        // deliberate `internal` one. A new variant added without a mapping
        // decision fails here.
        assert_eq!(cases.len(), 8, "an EngineError variant is unaccounted for");
        for (engine_err, want) in cases {
            let label = format!("{engine_err:?}");
            let env = AgentError::from(engine_err).into_envelope();
            assert_eq!(env.kind, want, "{label}");
            assert!(
                !env.suggest.contains("report to your operator"),
                "{label}: this is the caller's to fix, so the advice must not be \
                 to wake an operator: {}",
                env.suggest
            );
            assert!(env.see.starts_with("latiq://"), "{label}: {}", env.see);
        }
        // And the catch-all still is one: a failure we have NOT classified must
        // keep saying so rather than borrowing someone else's advice.
        let internal = AgentError::from(EngineError::Engine("connection reset".into()));
        assert_eq!(internal.envelope().kind, ErrorKind::Internal);
    }

    /// The message an agent reads must not assert something untrue about the
    /// failure. Every one of these used to be prefixed "SQL parse error:".
    #[test]
    fn error_contract_a_message_is_not_relabelled_on_its_way_out() {
        for e in [
            EngineError::Parse("Parser Error: syntax error at or near \"SELEKT\"".into()),
            EngineError::Catalog("Catalog Error: Table with name nope does not exist!".into()),
            EngineError::SourceIo("IO Error: Could not connect to server".into()),
        ] {
            let expected = match &e {
                EngineError::Parse(m) | EngineError::Catalog(m) | EngineError::SourceIo(m) => {
                    m.clone()
                }
                _ => unreachable!(),
            };
            assert_eq!(
                AgentError::from(e).envelope().message,
                expected,
                "the engine's own words, unprefixed"
            );
        }
    }

    /// Regression pin. The two cap messages used to be ONE sentence with two
    /// meanings: `SELECT * FROM range(20000)` and `range(1000000)` both reported
    /// "Result has 10240 rows" — the Arrow batch boundary where the streaming
    /// collector gave up — in the same words the materialized path uses for a
    /// true total. So the number that decides an agent's next `LIMIT` was wrong,
    /// and wrong in the direction that looks like a near miss.
    #[test]
    fn error_contract_the_cap_message_only_states_a_row_count_it_actually_knows() {
        let exact = AgentError::result_cap_exceeded(10_001, 10_000)
            .envelope()
            .message
            .clone();
        assert!(
            exact.contains("has 10001 rows"),
            "a materialized result knows its size and should say it: {exact}"
        );

        let unknown = AgentError::result_cap_exceeded_unknown(10_000)
            .envelope()
            .message
            .clone();
        assert!(
            unknown.contains("more than 10000"),
            "a stream cut at the cap can only bound the count: {unknown}"
        );
        assert!(
            !unknown.contains("10240"),
            "the batch boundary must not appear at all: {unknown}"
        );
        assert!(
            unknown.contains("not known"),
            "and it must SAY the count is unknown, or `more than` reads as a \
             turn of phrase: {unknown}"
        );
        assert_ne!(
            exact, unknown,
            "two different claims must not read as the same sentence"
        );
        // And the difference is machine-readable, not only prose: the exact
        // path publishes `rows` as a value; the streaming path publishes NO
        // `rows` fact at all, because it does not know one. A client that
        // branches on the fact can never be handed a batch boundary.
        assert_eq!(
            AgentError::result_cap_exceeded(10_001, 10_000)
                .envelope()
                .facts
                .get("rows"),
            Some(&latiq_common::Fact::Number(10_001))
        );
        assert!(
            !AgentError::result_cap_exceeded_unknown(10_000)
                .envelope()
                .facts
                .contains_key("rows"),
            "a count we did not measure must not appear as a fact"
        );
        // Both are the same kind, so `see`/`suggest` routing is unchanged.
        for e in [
            AgentError::result_cap_exceeded(1, 0),
            AgentError::result_cap_exceeded_unknown(0),
        ] {
            assert_eq!(e.envelope().kind, ErrorKind::ResultCapExceeded);
        }
    }

    /// Every fact-carrying constructor, driven for real, asserting the two
    /// things that make `facts` worth having.
    ///
    /// 1. **No `{placeholder}` survives into what a caller reads.** The renderer
    ///    deliberately leaves an unresolved placeholder verbatim (see
    ///    `latiq-common`'s `error_contract_an_unresolved_placeholder_is_left_visible`),
    ///    so a typo'd name in a template is visible here rather than shipping a
    ///    sentence with a hole in it. This is the whole reason the renderer does
    ///    not silently drop them.
    /// 2. **Every fact is actually USED**, in the message or the suggest. A fact
    ///    nobody renders is a value the prose is not built from, which is the
    ///    drift this mechanism exists to prevent — in the other direction.
    #[test]
    fn error_contract_every_rendered_message_resolves_its_facts() {
        let cases = [
            AgentError::pond_not_found("incident-001"),
            AgentError::pond_unavailable("incident-001"),
            AgentError::name_conflict("taken"),
            AgentError::dataset_not_found("tpch"),
            AgentError::result_cap_exceeded(10_001, 10_000),
            AgentError::result_cap_exceeded_unknown(10_000),
            // Both timeout shapes: below the ceiling (the suggest quotes the
            // ceiling twice) and AT it (the suggest quotes nothing).
            AgentError::query_timeout(500, 60_000),
            AgentError::query_timeout(60_000, 60_000),
        ];
        // Anti-vacuity: an empty list would make every assertion below vacuous.
        assert_eq!(cases.len(), 8, "every fact-carrying constructor is driven");
        for e in cases {
            let env = e.envelope();
            let label = format!("{:?}", env.kind);
            assert!(
                !env.facts.is_empty(),
                "{label}: a constructor that quotes a value must publish it"
            );
            for text in [&env.message, &env.suggest] {
                assert!(
                    !text.contains('{'),
                    "{label}: an unresolved placeholder reached the caller: {text}"
                );
            }
            for (name, fact) in &env.facts {
                let rendered = fact.to_string();
                assert!(
                    env.message.contains(&rendered) || env.suggest.contains(&rendered),
                    "{label}: fact `{name}` = {rendered} is published but never rendered — \
                     the prose is not built from it"
                );
            }
        }
    }

    /// Regression pin (#100). `AgentError::from(EngineError::…)` must NOT
    /// fabricate facts: the message is the engine's own words, and the only
    /// honest set of named values behind it is the empty one. A `sql` or
    /// `table` fact here would be us guessing at what DuckDB meant.
    #[test]
    fn error_contract_an_engine_message_publishes_no_facts_of_ours() {
        let env = AgentError::from(EngineError::Catalog(
            "Catalog Error: Table with name nope does not exist!".into(),
        ))
        .into_envelope();
        assert_eq!(env.kind, ErrorKind::CatalogError);
        assert!(env.facts.is_empty(), "{:?}", env.facts);
    }
}
