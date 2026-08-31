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

/// Heuristic: is this SQL a read-only statement (safe for the read path)?
///
/// SQL engines expose no portable statement-type introspection, so this is a
/// careful heuristic: the statement must start with a read keyword AND not contain
/// any data-modifying keyword (which catches `WITH … INSERT`, `EXPLAIN ANALYZE …`,
/// side-effecting `PRAGMA`/`CALL`, etc.). It errs toward rejecting ambiguous
/// statements — a wrongly-rejected read is recoverable; a write slipping through
/// the read path is not.
pub fn is_read_only(sql: &str) -> bool {
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
        return false;
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
    !WRITE_KEYWORDS.iter().any(|w| normalized.contains(w))
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
