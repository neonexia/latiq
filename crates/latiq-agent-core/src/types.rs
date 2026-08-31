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

//! Neutral, protocol-agnostic result/info types produced by AgentOps.
use latiq_engine::SchemaSummary;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PondInfo {
    pub pond_id: String,
    pub name: String,
    pub owner: String,
    pub created_at: String,
    pub policy_json: String,
    /// Internal endpoint of the node that owns this pond (`None` if the owning
    /// node is gone). A node uses this to decide local-vs-forward.
    #[serde(default)]
    pub node_endpoint: Option<String>,
    /// Resource tier name (small/medium/large/x-large); the engine maps it to
    /// the pond instance's memory/thread caps. Empty → medium.
    #[serde(default)]
    pub tier: String,
    /// Optional DuckDB extensions the pond loads on open (LOADed from the image).
    #[serde(default)]
    pub extensions: Vec<String>,
    /// Agent-discovery text: what this pond is for (empty = none).
    #[serde(default)]
    pub description: String,
    /// Whether this pond records OpenLineage events into its `lineage`
    /// directory. Opt-in at creation and fixed for the pond's lifetime; off by
    /// default, so a deployment that does not want it pays nothing. Carried
    /// from the registry to the node the same way `tier` and `extensions` are,
    /// and describe reports it so a caller can tell whether `get_lineage` will
    /// have anything to read before it asks.
    #[serde(default)]
    pub lineage: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AllocateResult {
    pub pond_id: String,
    pub pond_name: String,
}

/// One file table in a dataset.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DatasetTableInfo {
    pub table_name: String,
    pub source_uri: String,
    pub format: String,
}

/// A dataset: simple file tables in the built-in `latiq` catalog.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DatasetInfo {
    pub name: String,
    pub description: String,
    pub tags: Vec<String>,
    pub tables: Vec<DatasetTableInfo>,
    pub created_by: String,
    pub created_at: String,
}

/// An external catalog (iceberg/…): a type + locator params (no credentials).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CatalogInfo {
    pub name: String,
    pub r#type: String,
    pub params: std::collections::BTreeMap<String, String>,
    pub description: String,
    pub tags: Vec<String>,
    pub created_by: String,
    pub created_at: String,
}

/// Result of loading a dataset into a pond.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LoadDatasetResult {
    pub dataset: String,
    /// The schema the dataset's tables were created under (named after the
    /// dataset). Query them as `<schema>.<table>`.
    pub schema: String,
    /// Schema-qualified table names (e.g. `tpch.lineitem`).
    pub tables: Vec<String>,
}

/// Result of a transient pull from an external catalog (the query ran in-pond).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PullResult {
    pub catalog: String,
    pub query: String,
}

/// A page of a pond's OpenLineage trail, newest first — what `get_lineage`
/// returns. The events are the recorded JSON **verbatim** (see
/// `latiq_lineage::reader`), so a consumer can replay them into an OpenLineage
/// backend unchanged.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LineagePage {
    pub pond_id: String,
    pub pond_name: String,
    /// The directory these events live in, on the node that ran the queries.
    /// Handed back so a caller that wants SQL over the whole trail — filtering
    /// or aggregating, which this paged read deliberately does not do — can
    /// `read_json_auto('<dir>/*.jsonl')` instead of pulling every event through
    /// its own context.
    pub lineage_dir: String,
    pub events: Vec<serde_json::Value>,
    /// Older events matched than this page could carry. Read the next page by
    /// passing the oldest `eventTime` in `events` as the next call's `before`
    /// (exclusive) — the page is cut on a timestamp boundary, so that walks the
    /// history backwards without skipping or repeating an event.
    pub truncated: bool,
    /// Lines skipped because they were not valid JSON. Reported rather than
    /// swallowed: a short answer must not look like a complete one.
    pub malformed_lines: usize,
    /// Whole batch files (up to 64 events each) that could not be read. Same
    /// reason as `malformed_lines`, one level up.
    #[serde(default)]
    pub unreadable_files: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DescribeResult {
    pub pond: PondInfo,
    pub schema: SchemaSummary,
}
