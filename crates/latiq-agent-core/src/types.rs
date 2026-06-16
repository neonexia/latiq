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

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DescribeResult {
    pub pond: PondInfo,
    pub schema: SchemaSummary,
}
