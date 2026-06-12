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
    let normalized = format!(" {} ", s.replace(['\n', '\t', '(', ')', ','], " "));
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
        ] {
            assert!(is_read_only(s), "should be read: {s}");
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
