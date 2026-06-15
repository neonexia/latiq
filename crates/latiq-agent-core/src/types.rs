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

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DescribeResult {
    pub pond: PondInfo,
    pub schema: SchemaSummary,
}

/// What AgentOps hands the ControlPlane to record (already shape-summarized).
#[derive(Debug, Clone)]
pub struct AuditRecord {
    pub agent_identity: String,
    pub verified: bool,
    pub operation: String,
    pub pond_id: Option<String>,
    pub request_summary: Option<String>,
    pub duration_ms: u64,
}
