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

//! DuckDB error **class** → `EngineError`.
//!
//! Every DuckDB error message begins with the name of its exception class:
//! `Parser Error: …`, `Catalog Error: …`, `Conversion Error: …`. That prefix is
//! what says *what went wrong*, and it is the only thing we classify on.
//!
//! What we deliberately do NOT classify on is which duckdb-rs call returned the
//! error. That was the old scheme — `prepare()` failed ⇒ `Parse`, anything else
//! ⇒ `Engine` — and DuckDB decides for itself how much work happens at prepare
//! time: it binds `INSERT INTO nope` there (so a missing table came back as a
//! *parse error*) but defers `CREATE TABLE t` duplicate detection to execution
//! (so an existing table came back as *internal*, with "retry" as the advice).
//! The kind an agent received was an accident of binder phasing.
//!
//! Unrecognised classes stay `EngineError::Engine`. That is the honest answer
//! for a class we have never seen — but it is the WRONG answer for a class we
//! know is caller-fixable and simply had not listed. `Not implemented Error:`
//! (`CREATE TABLE … PRIMARY KEY`, which is ordinary SQL an agent writes on its
//! first try) fell through to `internal` + "Retry; if it persists, report to
//! your operator": a loop that can never succeed, then an escalation to someone
//! with nothing to fix. Nexus finding 8. DuckDB's classes are a finite set, so
//! the ones a caller's statement can raise are enumerated below rather than
//! defaulted; see [`class_of`] for the ones deliberately left unmapped.
use latiq_engine::EngineError;

/// The class prefixes we key on. Pinned against the real engine by
/// `tests/engine_e2e.rs::error_contract_duckdb_error_classes_are_unchanged` —
/// a DuckDB upgrade that renames one silently drops everything in that class
/// back to `internal` + "retry", so it must fail loudly instead.
pub const PARSER: &str = "Parser Error";
pub const SYNTAX: &str = "Syntax Error";
pub const CATALOG: &str = "Catalog Error";
pub const BINDER: &str = "Binder Error";
pub const CONVERSION: &str = "Conversion Error";
pub const CONSTRAINT: &str = "Constraint Error";
pub const IO: &str = "IO Error";
pub const HTTP: &str = "HTTP Error";
pub const NOT_IMPLEMENTED: &str = "Not implemented Error";
pub const INVALID_INPUT: &str = "Invalid Input Error";
pub const OUT_OF_RANGE: &str = "Out of Range Error";
pub const TRANSACTION: &str = "TransactionContext Error";

/// Classify a duckdb-rs error by the exception class in its message.
///
/// The class is matched at the START of the message (after trimming), because a
/// class name can also appear *inside* an unrelated message — a `Parser Error`
/// raised on the text of a query that happens to mention "IO Error" must not be
/// reclassified by its own payload.
pub fn classify(err: &duckdb::Error) -> EngineError {
    classify_message(&err.to_string())
}

/// The same, from a message we already own (a nested error, or one duckdb-rs has
/// already stringified).
pub fn classify_message(msg: &str) -> EngineError {
    let m = msg.trim();
    // The class leads a LINE, not necessarily the message: duckdb-rs can put
    // its own wrapper text first, and DuckDB's own messages continue onto
    // further lines. Take the first line that leads with a class we know.
    let line = m
        .lines()
        .find(|l| class_of(l.trim()).is_some())
        .map(str::trim)
        .unwrap_or(m);
    // The message we carry is always the WHOLE thing — DuckDB's errors often
    // continue onto further lines with the offending SQL and a caret, and that
    // is the most useful part for the caller. Only the classification looks at
    // one line.
    let owned = || m.to_string();
    match class_of(line) {
        // `Syntax Error` is DuckDB's other name for the same thing — the
        // statement (or a value inside a `SET`) is not well formed. One class
        // prefix, one action: fix the statement.
        Some(PARSER) | Some(SYNTAX) => EngineError::Parse(owned()),
        // Catalog and Binder are one action: the statement is valid SQL, but a
        // name in it doesn't match the pond. "Table with name nope does not
        // exist", "Referenced column x not found", "No function matches the
        // given name and argument types", "Cannot create entry in system
        // catalog" — all answered by looking at what the pond has.
        Some(CATALOG) | Some(BINDER) => EngineError::Catalog(owned()),
        Some(CONVERSION) => EngineError::Conversion(owned()),
        Some(CONSTRAINT) => EngineError::Constraint(owned()),
        // The source is not ours: a URL, a bucket, a file. `HTTP Error` is
        // httpfs's own class for the same situation.
        Some(IO) | Some(HTTP) => EngineError::SourceIo(owned()),
        // The statement is fine; the engine does not implement what it asks
        // for. Everything DuckLake rejects this way is a clause the caller
        // wrote and can delete — measured against the real engine, not assumed:
        // PRIMARY KEY/UNIQUE, CHECK, indexes, sequences and generated columns
        // all arrive here (`engine_e2e.rs`'s class pin, and
        // `latiq-agent-core`'s
        // `error_contract_no_statement_a_caller_can_edit_is_addressed_to_an_operator`,
        // which drives each of them through to the envelope).
        Some(NOT_IMPLEMENTED) => EngineError::Unsupported {
            feature: unsupported_feature(line),
            message: owned(),
        },
        // Both are "a value or argument in the statement is not acceptable":
        // an overflowing sum, a string that does not match a format specifier,
        // a file that is not the format it was read as. One action — fix the
        // value — so one variant, the same merge Catalog/Binder already make.
        Some(INVALID_INPUT) | Some(OUT_OF_RANGE) => EngineError::InvalidInput(owned()),
        // The caller sent transaction control inside the bracket the write path
        // owns. Its own variant because the advice has to warn that part of the
        // statement may already have committed.
        Some(TRANSACTION) => EngineError::TransactionControl(owned()),
        _ => EngineError::Engine(m.to_string()),
    }
}

/// The class prefix this message leads with, if it is one we key on.
///
/// **What is deliberately NOT here, and why** — the point being that the
/// remainder is a decision rather than a default:
/// - `Internal` / `FATAL` — ours. `internal` + "report to your operator" is
///   exactly right, and is the only thing it is right for.
/// - `Out of Memory` — the deployment's ceiling, not a property of the
///   statement: it is raised by the pond's memory cap, and a retry under less
///   load can succeed where an identical retry after a `Parser Error` never
///   can. Stays `internal`/`as_is`, which is what that advice means.
/// - `INTERRUPT` — normalized to `Cancelled` by `run_with_abort`, which only
///   inspects `Engine`, so keying on it here would break cancellation.
/// - `Dependency`, `Permission`, `Serialization` — we could not raise any of
///   them through a pond. The objects that create dependencies (indexes,
///   sequences, CHECK constraints) are themselves `Not implemented` in
///   DuckLake, every statement runs as the one local process that owns the
///   catalog, and writes are serialized by the pond's writer mutex before they
///   reach DuckDB. A mapping for a class nothing can raise is a mapping nothing
///   tests; add one the day a statement produces it.
fn class_of(msg: &str) -> Option<&'static str> {
    [
        PARSER,
        SYNTAX,
        CATALOG,
        BINDER,
        CONVERSION,
        CONSTRAINT,
        IO,
        HTTP,
        NOT_IMPLEMENTED,
        INVALID_INPUT,
        OUT_OF_RANGE,
        TRANSACTION,
    ]
    .into_iter()
    .find(|c| is_class_prefix(msg, c))
}

/// The feature DuckDB named as unsupported, in ITS words, or `None`.
///
/// The value goes into the envelope's `facts` so a client can branch on
/// `feature == "indexes"` instead of matching a sentence (invariant 13). It is
/// therefore only ever lifted out of the engine's own message — never composed
/// by us — and the two shapes DuckLake uses are the two matched here:
/// `"PRIMARY KEY/UNIQUE constraints are not supported in DuckLake"` and
/// `"DuckLake does not support indexes"`. Anything else yields no fact at all,
/// because a fact we guessed at is worse than one we did not publish.
fn unsupported_feature(line: &str) -> Option<String> {
    let detail = line
        .split_once(':')
        .map(|(_, rest)| rest.trim())
        .unwrap_or(line);
    let trim = |s: &str| {
        let s = s.trim().trim_end_matches(['.', '!']).trim();
        (!s.is_empty()).then(|| s.to_string())
    };
    if let Some((_, feature)) = detail.split_once(" does not support ") {
        return trim(feature);
    }
    for marker in [" are not supported", " is not supported"] {
        if let Some((feature, _)) = detail.split_once(marker) {
            return trim(feature);
        }
    }
    None
}

/// `"<Class>:"` at the head of the message. The colon is required so a message
/// that merely begins with the words is not mistaken for the class.
fn is_class_prefix(msg: &str, class: &str) -> bool {
    msg.len() > class.len()
        && msg.is_char_boundary(class.len())
        && msg[..class.len()].eq_ignore_ascii_case(class)
        && msg[class.len()..].starts_with(':')
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The variant name, so the table below reads as data rather than as a
    /// column of closures.
    fn variant(e: &EngineError) -> &'static str {
        match e {
            EngineError::Parse(_) => "Parse",
            EngineError::Catalog(_) => "Catalog",
            EngineError::Conversion(_) => "Conversion",
            EngineError::Constraint(_) => "Constraint",
            EngineError::SourceIo(_) => "SourceIo",
            EngineError::Unsupported { .. } => "Unsupported",
            EngineError::InvalidInput(_) => "InvalidInput",
            EngineError::TransactionControl(_) => "TransactionControl",
            EngineError::Engine(_) => "Engine",
            // Never produced by `class_of`: a cache miss is raised at the LOAD
            // site (`instance::extension_not_cached`), which knows which
            // extension it asked for — DuckDB's message class does not.
            EngineError::CapabilityUnavailable { .. } => "CapabilityUnavailable",
            EngineError::ReadOnlyViolation => "ReadOnlyViolation",
            EngineError::Cancelled => "Cancelled",
            EngineError::Timeout => "Timeout",
        }
    }

    #[test]
    fn error_contract_each_class_maps_to_its_own_variant() {
        // The messages are real DuckDB ones (the pin test in
        // `tests/engine_e2e.rs` is what keeps them real); this asserts only our
        // mapping, with no engine in the loop.
        let cases = [
            ("Parser Error: syntax error at or near \"SELEKT\"", "Parse"),
            (
                "Catalog Error: Table with name nope does not exist!",
                "Catalog",
            ),
            (
                "Binder Error: Cannot create entry in system catalog",
                "Catalog",
            ),
            (
                "Conversion Error: Could not convert string 'notanint' to INT32",
                "Conversion",
            ),
            (
                "Constraint Error: NOT NULL constraint failed: t.id",
                "Constraint",
            ),
            ("IO Error: Could not connect to server", "SourceIo"),
            (
                "HTTP Error: HTTP GET error on 'https://x' (404)",
                "SourceIo",
            ),
            (
                "Not implemented Error: PRIMARY KEY/UNIQUE constraints are not supported in \
                 DuckLake",
                "Unsupported",
            ),
            (
                "Invalid Input Error: No magic bytes found at end of file 'x.parquet'",
                "InvalidInput",
            ),
            (
                "Out of Range Error: Overflow in addition of INT64 (9223372036854775807 + 1)!",
                "InvalidInput",
            ),
            (
                "TransactionContext Error: cannot start a transaction within a transaction",
                "TransactionControl",
            ),
            ("Syntax Error: Must have at least 1 thread!", "Parse"),
            // Not a class we have decided a CALLER action for: the deployment's
            // memory ceiling is not something a different statement is
            // guaranteed to dodge, and a retry under less load can work. It
            // stays internal rather than borrowing someone else's advice.
            ("Out of Memory Error: failed to allocate", "Engine"),
            // The interrupt is normalized to `Cancelled` by `run_with_abort`,
            // which only inspects `Engine` — so it must land there.
            ("INTERRUPT Error: Interrupted!", "Engine"),
        ];
        for (msg, want) in cases {
            assert_eq!(variant(&classify_message(msg)), want, "{msg}");
        }
    }

    /// The `feature` fact exists so a client branches on a value rather than on
    /// a sentence — so it must be the engine's own noun, and must be absent
    /// when the engine did not give us one. The four DuckLake messages below
    /// are real (see the e2e pin); the last two are the honest-`None` cases.
    #[test]
    fn error_contract_the_unsupported_feature_fact_is_lifted_never_invented() {
        let feature = |msg: &str| match classify_message(msg) {
            EngineError::Unsupported { feature, .. } => feature,
            other => panic!("{msg} did not classify as unsupported: {other:?}"),
        };
        assert_eq!(
            feature(
                "Not implemented Error: PRIMARY KEY/UNIQUE constraints are not supported in \
                 DuckLake"
            )
            .as_deref(),
            Some("PRIMARY KEY/UNIQUE constraints")
        );
        assert_eq!(
            feature("Not implemented Error: CHECK constraints are not supported in DuckLake")
                .as_deref(),
            Some("CHECK constraints")
        );
        assert_eq!(
            feature("Not implemented Error: DuckLake does not support indexes").as_deref(),
            Some("indexes")
        );
        assert_eq!(
            feature("Not implemented Error: DuckLake does not support generated columns")
                .as_deref(),
            Some("generated columns")
        );
        // A message in neither shape publishes NO fact: a value we made up
        // would be a value a client could branch on and be wrong about.
        assert_eq!(feature("Not implemented Error: unsupported type"), None);
        // …and the classification still happens, so the agent keeps the
        // agent-facing kind even where the fact is missing.
        assert!(matches!(
            classify_message("Not implemented Error: unsupported type"),
            EngineError::Unsupported { feature: None, .. }
        ));
    }

    #[test]
    fn a_class_named_inside_a_message_does_not_reclassify_it() {
        // The payload of a parse error is the caller's own SQL, which can say
        // anything at all — including the name of another class.
        let e = classify_message("Parser Error: syntax error at or near \"IO Error: nope\"");
        assert!(
            matches!(e, EngineError::Parse(_)),
            "the LEADING class decides, not a mention in the payload: {e:?}"
        );
    }

    #[test]
    fn a_message_with_no_class_is_not_guessed_at() {
        let e = classify_message("connection closed");
        let EngineError::Engine(m) = &e else {
            panic!("an unclassifiable message must stay internal, got {e:?}");
        };
        assert_eq!(m, "connection closed", "and must keep its text verbatim");
    }
}
