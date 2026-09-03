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

//! Turn DuckDB's `EXPLAIN (FORMAT JSON)` plan into the estimates an agent can
//! act on.
//!
//! **The plan JSON is a serialisation internal with no stability guarantee.**
//! The lineage work learned that the hard way with `json_serialize_plan`, whose
//! key names differ per table function. So everything here is defensive: a key
//! we do not recognise degrades to *fewer* estimates, never to an error, and
//! `engine_e2e.rs::explain_plan_key_names_still_match_this_duckdb_version` pins
//! the five keys we read so a DuckDB upgrade fails there with a name.
//!
//! **This is not a lineage source.** The plan emits bare table names for
//! DuckLake scans (`t`, not `pond.main.t`) and conflates catalogs, which is
//! precisely why lineage extraction uses the bound plan instead. We want
//! cardinality and filters, not dataset identity — nobody should reuse this for
//! provenance.

use latiq_engine::ScanOp;

/// Keys we read out of the plan. Named once so the drift pin and the parser
/// cannot disagree about what is being pinned.
pub mod keys {
    /// The operator name, e.g. `DUCKLAKE_SCAN`.
    pub const NAME: &str = "name";
    /// The child operators.
    pub const CHILDREN: &str = "children";
    /// The per-operator detail bag holding everything below.
    pub const EXTRA_INFO: &str = "extra_info";
    /// The optimiser's row estimate for this operator, as a *string*.
    pub const CARDINALITY: &str = "Estimated Cardinality";
    /// The table a scan reads. Present on table scans only.
    pub const TABLE: &str = "Table";
    /// The predicates pushed into a scan. Absent when there are none.
    pub const FILTERS: &str = "Filters";
}

/// The operator name DuckLake gives a scan of the pond's own storage. Every
/// other scan reads something attached from outside the pond.
const DUCKLAKE_SCAN: &str = "DUCKLAKE_SCAN";

/// A scan reading at least this many rows with no predicate is worth telling the
/// agent about. Below it, a full scan is cheaper than the round trip spent
/// avoiding it — DuckDB scans 100k rows of Parquet in milliseconds — and a
/// warning on every small table would train the agent to ignore warnings.
pub const FULL_SCAN_WARN_ROWS: u64 = 100_000;

pub const FULL_SCAN: &str = "full_scan";
pub const FILTERED_SCAN: &str = "filtered_scan";

/// What we could recover from one plan. Everything is best-effort: a field we
/// could not read stays at its empty value.
#[derive(Debug, Default, PartialEq)]
pub struct Estimates {
    pub estimated_rows: u64,
    pub scan_operations: Vec<ScanOp>,
    pub warnings: Vec<String>,
    pub suggestions: Vec<String>,
}

/// Why a plan produced no estimates at all. Returned rather than logged in here
/// so the caller can log it once, with the SQL, at the right level.
#[derive(Debug, PartialEq)]
pub enum PlanUnreadable {
    /// The `EXPLAIN` column did not hold JSON.
    NotJson(String),
    /// Valid JSON, but not the `[{...}]` / `{...}` operator tree we parse.
    NotAPlanTree,
    /// An operator tree whose root has no `name` — i.e. the node shape moved.
    NoOperatorName,
}

impl std::fmt::Display for PlanUnreadable {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NotJson(e) => write!(f, "EXPLAIN (FORMAT JSON) did not return JSON: {e}"),
            Self::NotAPlanTree => write!(f, "the plan JSON is not an operator tree"),
            Self::NoOperatorName => write!(f, "the root operator has no `{}`", keys::NAME),
        }
    }
}

/// Parse one `EXPLAIN (FORMAT JSON)` payload.
///
/// `Err` means "we understood nothing" — the caller still returns the plan text
/// with empty estimates, because an explain that errors is worse than one that
/// under-reports. Everything softer than that (a missing cardinality, a scan
/// with no `Table`) degrades inside `Ok`.
pub fn parse(plan_json: &str) -> Result<Estimates, PlanUnreadable> {
    let value: serde_json::Value =
        serde_json::from_str(plan_json).map_err(|e| PlanUnreadable::NotJson(e.to_string()))?;
    // DuckDB wraps the root operator in a one-element array; tolerate a bare
    // object too, since that costs one line and survives a wrapper change.
    let root = match &value {
        serde_json::Value::Array(a) => a.first().ok_or(PlanUnreadable::NotAPlanTree)?,
        serde_json::Value::Object(_) => &value,
        _ => return Err(PlanUnreadable::NotAPlanTree),
    };
    if node_name(root).is_none() {
        return Err(PlanUnreadable::NoOperatorName);
    }

    let mut nodes = Vec::new();
    collect(root, &mut nodes);

    // The ROOT's estimate is the query's result size — that is the number an
    // agent compares against the inline cap. Some root operators (ORDER_BY)
    // carry none, so fall back to the nearest descendant that does rather than
    // reporting a confident zero.
    let estimated_rows = nodes.iter().find_map(|n| cardinality(n)).unwrap_or(0);

    let scan_operations: Vec<ScanOp> = nodes.iter().filter_map(|n| scan_op(n)).collect();

    let mut warnings = Vec::new();
    let mut suggestions = Vec::new();
    for s in &scan_operations {
        if s.scan_type == FULL_SCAN && s.estimated_rows_scanned >= FULL_SCAN_WARN_ROWS {
            warnings.push(format!(
                "full scan of `{}`: an estimated {} rows are read with no filter",
                s.table, s.estimated_rows_scanned
            ));
            suggestions.push(format!(
                "add a WHERE on a selective column of `{}` (or a LIMIT, or aggregate with GROUP BY) \
                 — this scan reads ~{} rows unfiltered",
                s.table, s.estimated_rows_scanned
            ));
        }
    }

    Ok(Estimates {
        estimated_rows,
        scan_operations,
        warnings,
        suggestions,
    })
}

/// Pre-order flatten, so `nodes[0]` is the root and the cardinality fallback
/// walks outward-in rather than picking an arbitrary leaf.
fn collect<'a>(node: &'a serde_json::Value, out: &mut Vec<&'a serde_json::Value>) {
    out.push(node);
    if let Some(children) = node.get(keys::CHILDREN).and_then(|c| c.as_array()) {
        for c in children {
            collect(c, out);
        }
    }
}

fn node_name(node: &serde_json::Value) -> Option<&str> {
    node.get(keys::NAME)?.as_str()
}

fn extra<'a>(node: &'a serde_json::Value, key: &str) -> Option<&'a serde_json::Value> {
    node.get(keys::EXTRA_INFO)?.get(key)
}

/// DuckDB writes cardinalities as strings (`"20000"`); accept a number too, so a
/// change of encoding is not a silent zero.
fn cardinality(node: &serde_json::Value) -> Option<u64> {
    let v = extra(node, keys::CARDINALITY)?;
    v.as_u64().or_else(|| v.as_str()?.trim().parse().ok())
}

/// A scan is any operator naming a `Table`. File readers (`READ_PARQUET`) have
/// no table in this plan — the JSON carries no path for them — so they show up
/// in `raw_plan` only, and we do not invent an identity for them.
fn scan_op(node: &serde_json::Value) -> Option<ScanOp> {
    let table = extra(node, keys::TABLE)?.as_str()?;
    // SEQ_SCAN quotes reserved catalog names (`"temp".main.tmp`); the quotes are
    // the serialiser's, not part of the name an agent would type.
    let table = table.replace('"', "");
    let filter = filter_text(node);
    Some(ScanOp {
        table,
        scan_type: if filter.is_some() {
            FILTERED_SCAN.to_string()
        } else {
            FULL_SCAN.to_string()
        },
        estimated_rows_scanned: cardinality(node).unwrap_or(0),
        source: if node_name(node) == Some(DUCKLAKE_SCAN) {
            "pond".to_string()
        } else {
            "attached".to_string()
        },
    })
}

/// The predicates pushed into a scan, if any *the agent wrote*.
///
/// `Filters` may be a string or an array of them. A value prefixed `optional:`
/// is a **dynamic filter** the optimiser synthesised from a join or a TOP-N — it
/// prunes at runtime but says nothing about the agent's SQL, so counting it as a
/// filter would suppress the very advice this exists to give.
fn filter_text(node: &serde_json::Value) -> Option<String> {
    let v = extra(node, keys::FILTERS)?;
    let joined = match v {
        serde_json::Value::String(s) => s.clone(),
        serde_json::Value::Array(a) => a
            .iter()
            .filter_map(|x| x.as_str())
            .filter(|s| !is_dynamic(s))
            .collect::<Vec<_>>()
            .join(" AND "),
        _ => return None,
    };
    let joined = joined.trim();
    if joined.is_empty() || is_dynamic(joined) {
        return None;
    }
    Some(joined.to_string())
}

fn is_dynamic(filter: &str) -> bool {
    filter.trim_start().starts_with("optional:")
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The shape DuckDB 1.5.3 emits, kept minimal: one scan under one root.
    fn plan(scan_extra: &str) -> String {
        format!(
            r#"[{{"name":"HASH_GROUP_BY","children":[
                 {{"name":"DUCKLAKE_SCAN","children":[],"extra_info":{{{scan_extra}}}}}
               ],"extra_info":{{"Estimated Cardinality":"7"}}}}]"#
        )
    }

    #[test]
    fn explain_parse_reads_the_root_cardinality_and_the_scan() {
        let e = parse(&plan(
            r#""Table":"events","Filters":"id>100","Estimated Cardinality":"4000""#,
        ))
        .expect("a well-formed plan parses");
        assert_eq!(
            e.estimated_rows, 7,
            "estimated_rows is the ROOT's cardinality (the result size), not a scan's"
        );
        assert_eq!(e.scan_operations.len(), 1);
        let s = &e.scan_operations[0];
        assert_eq!(s.table, "events");
        assert_eq!(s.estimated_rows_scanned, 4000);
        assert_eq!(s.scan_type, FILTERED_SCAN, "the scan carries `Filters`");
        assert_eq!(
            s.source, "pond",
            "a DUCKLAKE_SCAN reads the pond's own data"
        );
        assert!(
            e.warnings.is_empty() && e.suggestions.is_empty(),
            "a filtered scan is not worth a warning: {e:?}"
        );
    }

    #[test]
    fn explain_parse_falls_back_to_a_descendant_when_the_root_has_no_estimate() {
        // Real shape: an ORDER_BY root carries no `Estimated Cardinality`.
        // Reporting 0 there would read as "this query returns nothing".
        let e = parse(
            r#"[{"name":"ORDER_BY","children":[
                 {"name":"SEQ_SCAN","children":[],"extra_info":{"Estimated Cardinality":"900"}}
               ],"extra_info":{"Order By":"t.id ASC"}}]"#,
        )
        .unwrap();
        assert_eq!(e.estimated_rows, 900);
    }

    #[test]
    fn explain_parse_warns_about_a_big_unfiltered_scan_and_names_the_table() {
        let e = parse(&plan(
            r#""Table":"events","Estimated Cardinality":"250000""#,
        ))
        .unwrap();
        assert_eq!(e.scan_operations[0].scan_type, FULL_SCAN);
        let w = e.warnings.join("\n");
        assert!(
            w.contains("full scan") && w.contains("`events`") && w.contains("250000"),
            "the warning must name the table and the size, not just exist: {w:?}"
        );
        let s = e.suggestions.join("\n");
        assert!(
            s.contains("WHERE") && s.contains("`events`"),
            "the suggestion must be actionable on THIS table: {s:?}"
        );
    }

    #[test]
    fn explain_parse_stays_quiet_about_a_small_unfiltered_scan() {
        // The threshold is the whole point of the rule: warning on every scan
        // of every lookup table would train the agent to ignore warnings.
        let e = parse(&plan(&format!(
            r#""Table":"lookup","Estimated Cardinality":"{}""#,
            FULL_SCAN_WARN_ROWS - 1
        )))
        .unwrap();
        assert_eq!(e.scan_operations[0].scan_type, FULL_SCAN);
        assert!(
            e.warnings.is_empty(),
            "below {FULL_SCAN_WARN_ROWS} rows a full scan is cheaper than avoiding it: {:?}",
            e.warnings
        );
    }

    #[test]
    fn explain_parse_does_not_count_a_dynamic_filter_as_the_agents_predicate() {
        // A TOP-N or join pushes `optional: Dynamic Filter (id)` into a scan.
        // Treating it as a WHERE would suppress the advice on exactly the
        // queries (big table, no predicate) that most need it.
        let e = parse(&plan(
            r#""Table":"events","Filters":"optional: Dynamic Filter (id)","Estimated Cardinality":"250000""#,
        ))
        .unwrap();
        assert_eq!(
            e.scan_operations[0].scan_type, FULL_SCAN,
            "a synthesised filter is not a predicate the agent wrote"
        );
        assert!(
            !e.warnings.is_empty(),
            "so the full-scan advice still fires"
        );
    }

    #[test]
    fn explain_parse_accepts_an_array_of_filters() {
        let e = parse(&plan(
            r#""Table":"events","Filters":["id>100","g='a'"],"Estimated Cardinality":"10""#,
        ))
        .unwrap();
        assert_eq!(e.scan_operations[0].scan_type, FILTERED_SCAN);
    }

    #[test]
    fn explain_parse_marks_a_non_ducklake_scan_as_attached() {
        let e = parse(
            r#"[{"name":"SEQ_SCAN","children":[],
                 "extra_info":{"Table":"\"temp\".main.tmp","Estimated Cardinality":"1"}}]"#,
        )
        .unwrap();
        assert_eq!(e.scan_operations[0].source, "attached");
        assert_eq!(
            e.scan_operations[0].table, "temp.main.tmp",
            "the serialiser's quoting is not part of the name"
        );
    }

    #[test]
    fn explain_parse_reports_operators_with_no_table_as_no_scan() {
        // A DUMMY_SCAN (`SELECT 1`) and a COLUMN_DATA_SCAN (a materialised
        // count) are scans by name only; naming them as tables would be a lie.
        let e = parse(r#"[{"name":"DUMMY_SCAN","children":[],"extra_info":{}}]"#).unwrap();
        assert!(e.scan_operations.is_empty());
        assert_eq!(e.estimated_rows, 0);
    }

    #[test]
    fn explain_parse_degrades_rather_than_failing_on_a_shape_it_does_not_know() {
        // Each input is unreadable in exactly ONE way, so the reason returned
        // can only be attributed to the check under test.
        assert!(
            matches!(parse("not json at all"), Err(PlanUnreadable::NotJson(_))),
            "a non-JSON payload"
        );
        assert_eq!(
            parse("[]").unwrap_err(),
            PlanUnreadable::NotAPlanTree,
            "an empty wrapper holds no operator"
        );
        assert_eq!(
            parse("\"a string\"").unwrap_err(),
            PlanUnreadable::NotAPlanTree,
            "valid JSON that is not a tree"
        );
        assert_eq!(
            parse(r#"[{"operator":"SEQ_SCAN","children":[]}]"#).unwrap_err(),
            PlanUnreadable::NoOperatorName,
            "the node shape moved: `name` is gone"
        );
        // A renamed `extra_info` / `Estimated Cardinality` is softer — it is
        // still a tree, so we report what we can and lose only the numbers.
        let e = parse(r#"[{"name":"SEQ_SCAN","children":[],"info":{"Rows":"5"}}]"#).unwrap();
        assert_eq!(e, Estimates::default(), "no numbers, but no error either");
    }

    #[test]
    fn explain_parse_accepts_a_numeric_cardinality() {
        // DuckDB writes these as strings today. If that ever becomes a number,
        // the parse must not silently start reporting zero.
        let e = parse(
            r#"[{"name":"SEQ_SCAN","children":[],"extra_info":{"Estimated Cardinality":42}}]"#,
        )
        .unwrap();
        assert_eq!(e.estimated_rows, 42);
    }
}
