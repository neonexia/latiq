//! Neutral, protocol-agnostic query result types produced by any QueryEngine.
use latiq_common::QueryMeta;
use serde::{Deserialize, Serialize};

/// A query result. Rows are positional cells aligned to `columns`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct QueryResult {
    pub columns: Vec<String>,
    pub rows: Vec<Vec<serde_json::Value>>,
    pub meta: QueryMeta,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ScanOp {
    pub table: String,
    /// "full_scan" | "filtered_scan" | "indexed"
    pub scan_type: String,
    pub estimated_rows_scanned: u64,
    /// "pond" | "attached"
    pub source: String,
}

/// Result of explain_query — estimates + guidance + raw plan.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ExplainResult {
    pub estimated_rows: u64,
    pub estimated_bytes: u64,
    pub estimated_duration_ms: u64,
    #[serde(default)]
    pub scan_operations: Vec<ScanOp>,
    #[serde(default)]
    pub warnings: Vec<String>,
    #[serde(default)]
    pub suggestions: Vec<String>,
    pub raw_plan: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TableInfo {
    pub name: String,
    pub columns: Vec<(String, String)>, // (name, type)
    pub row_count_estimate: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub comment: Option<String>,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct SchemaSummary {
    pub tables: Vec<TableInfo>,
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn query_result_serializes() {
        let r = QueryResult {
            columns: vec!["id".into()],
            rows: vec![vec![serde_json::json!(1)]],
            meta: QueryMeta {
                rows: 1,
                ..Default::default()
            },
        };
        let v = serde_json::to_value(&r).unwrap();
        assert_eq!(v["columns"][0], "id");
        assert_eq!(v["rows"][0][0], 1);
    }
}
