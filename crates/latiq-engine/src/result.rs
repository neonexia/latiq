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

/// One table access in an explained plan — the part of a plan an agent can act
/// on (a `full_scan` is a hint to add a filter).
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

/// One user table, as `describe_pond` reports it. `row_count_estimate` is what
/// the engine's catalog says, not a `count(*)` — describe must stay cheap.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TableInfo {
    pub name: String,
    pub columns: Vec<(String, String)>, // (name, type)
    pub row_count_estimate: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub comment: Option<String>,
}

/// A pond's user tables — the orientation an agent reads before writing SQL.
/// Latiq's own objects never appear here (invariant 6: nothing of ours lives in
/// the pond catalog).
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
