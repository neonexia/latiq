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
//! Unrecognised classes stay `EngineError::Engine`. That is the honest answer:
//! we have not decided what a caller should do about them, and inventing an
//! action is worse than admitting we have none.
use latiq_engine::EngineError;

/// The class prefixes we key on. Pinned against the real engine by
/// `tests/engine_e2e.rs::error_contract_duckdb_error_classes_are_unchanged` —
/// a DuckDB upgrade that renames one silently drops everything in that class
/// back to `internal` + "retry", so it must fail loudly instead.
pub const PARSER: &str = "Parser Error";
pub const CATALOG: &str = "Catalog Error";
pub const BINDER: &str = "Binder Error";
pub const CONVERSION: &str = "Conversion Error";
pub const CONSTRAINT: &str = "Constraint Error";
pub const IO: &str = "IO Error";
pub const HTTP: &str = "HTTP Error";

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
        Some(PARSER) => EngineError::Parse(owned()),
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
        _ => EngineError::Engine(m.to_string()),
    }
}

/// The class prefix this message leads with, if it is one we key on.
fn class_of(msg: &str) -> Option<&'static str> {
    [PARSER, CATALOG, BINDER, CONVERSION, CONSTRAINT, IO, HTTP]
        .into_iter()
        .find(|c| is_class_prefix(msg, c))
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
            EngineError::Engine(_) => "Engine",
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
            // Not a class we have decided an action for: it stays internal
            // rather than borrowing someone else's advice.
            ("Out of Memory Error: failed to allocate", "Engine"),
            // The interrupt is normalized to `Cancelled` by `run_with_abort`,
            // which only inspects `Engine` — so it must land there.
            ("INTERRUPT Error: Interrupted!", "Engine"),
        ];
        for (msg, want) in cases {
            assert_eq!(variant(&classify_message(msg)), want, "{msg}");
        }
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
