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
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// A query result. Rows are positional cells aligned to `columns`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct QueryResult {
    pub columns: Vec<String>,
    pub rows: Vec<Vec<serde_json::Value>>,
    pub meta: QueryMeta,
}

/// One table access in an explained plan — the part of a plan an agent can act
/// on (a `full_scan` is a hint to add a filter). File readers (`read_parquet`)
/// are absent on purpose: the plan JSON carries no path for them, so they are
/// visible in `raw_plan` only rather than under an invented name.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct ScanOp {
    pub table: String,
    /// "full_scan" | "filtered_scan" | "indexed"
    pub scan_type: String,
    pub estimated_rows_scanned: u64,
    /// "pond" | "attached"
    pub source: String,
}

/// Result of explain_query — estimates + guidance + raw plan.
///
/// Every number here is the **optimiser's estimate**, not a measurement: the
/// statement was planned and discarded, never run. Estimates are routinely wrong
/// on multi-way joins. Treat them as an order of magnitude.
///
/// There is deliberately no `estimated_bytes` and no `estimated_duration_ms`:
/// the plan carries no byte estimate and predicts no time, and a field that is
/// always `0` reads as "this query is free", which is worse than its absence.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct ExplainResult {
    /// The root operator's estimated row count — the size of the RESULT, the
    /// number to compare against the inline result cap.
    pub estimated_rows: u64,
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
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
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
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct SchemaSummary {
    pub tables: Vec<TableInfo>,
}

/// An external catalog currently ATTACHED to a pond — what
/// `list_attached_catalogs` reports and what the engine holds while it is
/// mounted.
///
/// **There is no credential on this type, in any form.** It is returned by a
/// surface, so a field for one could only ever be a leak; the credential lives
/// in the engine's `CREATE SECRET` and in nothing else. `options` is the locator
/// the caller supplied, which is safe by construction: the `--option` /
/// `--secret` split in `latiq_common::catalog` refuses a credential key on the
/// option side.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct AttachedCatalog {
    /// The alias, which is also the SQL namespace: `FROM lake.sales.orders`.
    pub name: String,
    /// The catalog type (`iceberg`, `ducklake`).
    #[serde(rename = "type")]
    pub type_: String,
    /// How the SOURCE identifies itself — the REST endpoint + warehouse, or
    /// `ducklake:<metadata>`. The same value lineage files its datasets under,
    /// so an agent can tell two attachments of the same type apart by what they
    /// actually point at rather than by the local alias.
    pub namespace: String,
    /// The locator options this attachment was made with, verbatim.
    pub options: std::collections::BTreeMap<String, String>,
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
