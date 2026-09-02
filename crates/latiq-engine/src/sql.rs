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

//! Engine-neutral SQL classification. The heuristic lives here (not in the DuckDB
//! adapter) so every layer that needs to tell reads from writes — the engine, the
//! Arrow read path, the CLI's read/write routing — shares one definition.

/// What this text looks like to the read/write routing — and, crucially, whether
/// it looks like *anything at all*.
///
/// The third variant is the whole point. `is_read_only` used to answer a yes/no
/// question, so "recognisably a write" and "not recognisable as SQL" collapsed
/// into the same `false`, and the read path reported a typo (`SELEKT * FROM t`,
/// `@@@@`, an empty string) as a `read_only_violation` — telling an agent to
/// call `write_query` with SQL that will never parse anywhere. Two calls and a
/// false belief to fix one character.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SqlShape {
    /// Starts with a read keyword and contains no data-modifying one.
    Read,
    /// Recognisably a statement that is not a read: it starts with a
    /// write/side-effecting keyword, or it starts as a read and hides one
    /// (`WITH … INSERT`, `SELECT 1;DROP TABLE t`, `EXPLAIN ANALYZE …`).
    Write,
    /// Not recognisable as any statement we know. NOT a write: hand it to the
    /// parser and let it say what is actually wrong with it.
    Unrecognized,
}

/// Heuristic: is this SQL a read-only statement (safe for the read path)?
///
/// Kept as the yes/no form for callers that only route (the CLI, the SDK), where
/// "not a read" is the whole decision. Anything that must *explain* the refusal
/// wants [`classify`], because only it can tell a write from a typo.
pub fn is_read_only(sql: &str) -> bool {
    matches!(classify(sql), SqlShape::Read)
}

/// Classify a statement for the read path.
///
/// SQL engines expose no portable statement-type introspection, so this is a
/// careful heuristic: a read must start with a read keyword AND not contain
/// any data-modifying keyword (which catches `WITH … INSERT`, `EXPLAIN ANALYZE …`,
/// side-effecting `PRAGMA`/`CALL`, etc.). It errs toward rejecting ambiguous
/// statements — a wrongly-rejected read is recoverable; a write slipping through
/// the read path is not.
///
/// What it does NOT do any more is call *unknown* text a write. The leading
/// keyword must be one we actually recognise as non-read (see
/// [`NON_READ_LEADING_KEYWORDS`]) for that; everything else is `Unrecognized`
/// and goes to the parser. The safety argument is unchanged, because the read
/// paths run inside `BEGIN TRANSACTION READ ONLY` — a statement that turns out
/// to modify anything is refused by the engine itself, not by this scan.
pub fn classify(sql: &str) -> SqlShape {
    let s = sql.trim_start().to_lowercase();
    let starts_read = s.starts_with("select")
        || s.starts_with("with")
        || s.starts_with("describe")
        || s.starts_with("show")
        || s.starts_with("explain")
        || s.starts_with("pragma")
        // DuckDB read-first shorthands: `FROM t`, `TABLE t`, `VALUES (...)` are all
        // SELECTs. No write statement starts with these (DELETE/UPDATE start with
        // their own keyword), so treating them as reads is safe.
        || s.starts_with("from")
        || s.starts_with("table")
        || s.starts_with("values");
    if !starts_read {
        return if leads_with_a_non_read_keyword(&s) {
            SqlShape::Write
        } else {
            // A typo, punctuation, prose, an empty string — or a perfectly good
            // read shorthand nobody listed above. None of those is a write, and
            // saying so would send the caller to write_query for nothing.
            SqlShape::Unrecognized
        };
    }
    // Word-boundary scan for data-modifying / side-effecting keywords.
    //
    // `;` is a separator like any other whitespace here. It was missing, and a
    // keyword sitting directly against one therefore never matched: `SELECT
    // 1;INSERT INTO t VALUES (1)` passed as a read and the read path EXECUTED
    // the insert (verified). Only the spaced form `SELECT 1; INSERT …` was
    // caught, so the guard was one character away from being bypassed.
    let normalized = format!(" {} ", s.replace(['\n', '\t', '(', ')', ',', ';'], " "));
    const WRITE_KEYWORDS: &[&str] = &[
        " insert ",
        " update ",
        " delete ",
        " create ",
        " drop ",
        " alter ",
        " truncate ",
        " attach ",
        " detach ",
        " copy ",
        " call ",
        " install ",
        " load ",
        " replace ",
        " analyze ",
        " vacuum ",
        " checkpoint ",
        // Transaction control. The read path runs the statement inside a
        // read-only transaction it opened itself, and a `COMMIT` or `ROLLBACK`
        // in user SQL ends THAT transaction: the pinned snapshot is released
        // mid-statement, and a following `BEGIN` opens a fresh one that our own
        // COMMIT then closes without complaint — so the version recorded for
        // the read describes a state it never saw. Refusing it here makes that
        // a structured error instead of silent corruption.
        " begin ",
        " commit ",
        " rollback ",
        " abort ",
        " start transaction ",
        " end transaction ",
        // NOT listed: bare `end` and bare `start`. DuckDB accepts `END` as a
        // synonym for COMMIT (verified), but `CASE … END` makes that word
        // ordinary in perfectly good reads, and `start` is a common column
        // name; blocking either would reject far more real reads than it
        // protects. The residual is contained rather than silent: reopening a
        // transaction needs `BEGIN`/`START TRANSACTION`, both refused above, so
        // a stray `END` can only make our own COMMIT fail loudly.
    ];
    if WRITE_KEYWORDS.iter().any(|w| normalized.contains(w)) {
        SqlShape::Write
    } else {
        SqlShape::Read
    }
}

/// Statement keywords that can START a statement which is not a read.
///
/// Deliberately a keyword list and not "anything that isn't a read": the
/// difference between the two is exactly what D4 was. It is a superset of
/// `WRITE_KEYWORDS` because a few of these are only ever *leading* words —
/// `SET`/`RESET`/`USE`/`COMMENT`/`EXPORT`/`IMPORT` mutate session or catalog
/// state and must keep being refused by the read path, but `set` appears inside
/// ordinary reads (`SELECT … ` on a column named `set`, `GROUPING SETS`), so
/// they cannot go in the contained-keyword scan above.
const NON_READ_LEADING_KEYWORDS: &[&str] = &[
    "insert",
    "update",
    "delete",
    "create",
    "drop",
    "alter",
    "truncate",
    "attach",
    "detach",
    "copy",
    "call",
    "install",
    "load",
    "replace",
    "analyze",
    "vacuum",
    "checkpoint",
    "begin",
    "commit",
    "rollback",
    "abort",
    "start",
    "end",
    "set",
    "reset",
    "use",
    "comment",
    "export",
    "import",
    "merge",
    "grant",
    "revoke",
];

/// Does the statement's FIRST word name something that is not a read? Compared
/// as a whole word, so a table called `sets` or a typo'd `selectt` is not
/// mistaken for one.
fn leads_with_a_non_read_keyword(lowered: &str) -> bool {
    let first = lowered
        .split(|c: char| c.is_whitespace() || c == '(' || c == ';')
        .find(|w| !w.is_empty())
        .unwrap_or("");
    NON_READ_LEADING_KEYWORDS.contains(&first)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reads_and_shorthands_are_reads() {
        for s in [
            "SELECT 1",
            "  with x as (select 1) select * from x",
            "FROM t",
            "TABLE t",
            "VALUES (1),(2)",
            "SHOW TABLES",
            "DESCRIBE t",
            // `CASE … END` is why bare `END` is not in the keyword list: it is
            // ordinary in analytical reads, and blocking it would reject them.
            "SELECT CASE WHEN i > 0 THEN 'p' ELSE 'n' END AS sign FROM t",
            "SELECT 1;",
        ] {
            assert!(is_read_only(s), "should be read: {s}");
        }
    }

    #[test]
    fn a_keyword_against_a_semicolon_is_still_a_keyword() {
        // Regression: `;` was not a separator in the normalizer, so a keyword
        // sitting directly against one never matched. `SELECT 1;INSERT …`
        // passed the read guard and the read path EXECUTED the insert; only the
        // spaced form was caught.
        assert!(
            !is_read_only("SELECT 1;INSERT INTO t VALUES (1)"),
            "an unspaced write after a semicolon must not pass as a read"
        );
        assert!(!is_read_only("SELECT 1; INSERT INTO t VALUES (1)"));
        assert!(!is_read_only("SELECT 1;DROP TABLE t"));
        assert!(!is_read_only("FROM t;DELETE FROM t"));
    }

    #[test]
    fn transaction_control_is_not_a_read() {
        // The read path runs inside a read-only transaction it opened; user SQL
        // that ends or reopens one silently detaches the read from the snapshot
        // its provenance claims. Both spacings, since the semicolon fix above is
        // what makes the unspaced form reachable at all.
        for s in [
            "SELECT * FROM a; COMMIT; BEGIN TRANSACTION",
            "SELECT * FROM a;COMMIT;BEGIN TRANSACTION",
            "SELECT 1; ROLLBACK",
            "SELECT 1;ABORT",
            "SELECT 1; START TRANSACTION",
            "SELECT 1; END TRANSACTION",
            "SELECT 1; BEGIN",
        ] {
            assert!(!is_read_only(s), "transaction control must not pass: {s}");
        }
    }

    #[test]
    fn error_contract_unrecognisable_text_is_not_called_a_write() {
        // D4: `is_read_only` answered `false` for a typo as readily as for an
        // INSERT, and the read path turned that `false` into
        // `read_only_violation` — "you sent a write; use write_query". An agent
        // that obeys spends a second call to be told what was wrong the first
        // time. None of these is a write, and none of them may be reported as
        // one; the parser gets to say what they are.
        for s in [
            "SELEKT * FROM t",
            "@@@@",
            "",
            "   ",
            "\n\t",
            "how many rows are in t?",
            "-- just a comment",
        ] {
            assert_eq!(
                classify(s),
                SqlShape::Unrecognized,
                "must reach the parser, not be reported as a write: {s:?}"
            );
        }
    }

    #[test]
    fn error_contract_leading_write_keywords_are_still_writes() {
        // The other half of the same change: loosening "not a read" must not
        // loosen "is a write". Every one of these mutates data, catalog or
        // session state, and the read path must keep refusing them by name —
        // including the ones that can only ever LEAD a statement (`SET`,
        // `USE`, …), which the contained-keyword scan cannot catch.
        for s in [
            "INSERT INTO t VALUES (1)",
            "insert into t values (1)",
            "CREATE TABLE t(i INT)",
            "DROP TABLE t",
            "SET memory_limit='1GB'",
            "RESET memory_limit",
            "USE other_db",
            "COMMENT ON TABLE t IS 'x'",
            "EXPORT DATABASE 'dir'",
            "IMPORT DATABASE 'dir'",
            "ATTACH 'x.db'",
            "COMMIT",
            "BEGIN",
        ] {
            assert_eq!(
                classify(s),
                SqlShape::Write,
                "must still be refused as a write: {s}"
            );
        }
    }

    #[test]
    fn a_word_that_merely_starts_with_a_keyword_is_not_that_keyword() {
        // The leading-keyword check is whole-word. `sets`/`ending` are ordinary
        // identifiers, and matching them by prefix would put real statements
        // back in the "you sent a write" bucket D4 is about.
        assert_eq!(classify("setting up"), SqlShape::Unrecognized);
        assert_eq!(classify("uses of t"), SqlShape::Unrecognized);
    }

    #[test]
    fn writes_and_hidden_writes_are_not_reads() {
        for s in [
            "INSERT INTO t VALUES (1)",
            "CREATE TABLE t(i INT)",
            "WITH x AS (SELECT 1) INSERT INTO t SELECT * FROM x",
            "EXPLAIN ANALYZE SELECT 1",
            "DELETE FROM t",
            "COPY t TO 'f.csv'",
        ] {
            assert!(!is_read_only(s), "should NOT be read: {s}");
        }
    }
}
