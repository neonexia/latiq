//! Where a pond's bytes live — the descriptor handed to the query engine.
use latiq_common::ResourceLimits;
use serde::{Deserialize, Serialize};

/// Resolved physical location of a pond. The engine consumes this to ATTACH.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PondLocation {
    /// DuckLake catalog connection string, e.g. "ducklake:duckdb:/var/lib/latiq/ponds/<id>/catalog.duckdb".
    pub catalog_uri: String,
    /// DuckLake DATA_PATH for parquet files, e.g. "/var/lib/latiq/ponds/<id>/data".
    pub data_path: String,
    /// The name the catalog is attached as — the pond's name, so callers query
    /// `<pond>.snapshots()` / `<pond>.main.<table>`. Storage defaults this to
    /// `pond`; the orchestrator (AgentOps) overrides it with the registry name.
    pub catalog_name: String,
    /// Per-pond resource caps (from its tier), applied to the DuckDB instance on
    /// open. `None` → engine defaults. Storage leaves this `None`; AgentOps sets
    /// it from the pond's tier.
    #[serde(default)]
    pub limits: Option<ResourceLimits>,
    /// Optional DuckDB extensions to LOAD on open (from the pond's registry
    /// record). Storage leaves this empty; AgentOps sets it from the pond info.
    /// LOADed from the deployment image — never installed in the pond path.
    #[serde(default)]
    pub extensions: Vec<String>,
    /// Whether this pond records lineage — the per-pond opt-in from its registry
    /// record, off by default. Storage leaves this false; AgentOps sets it from
    /// the pond info, the same path `tier` and `extensions` take. It rides on
    /// the location so the storage layer knows to materialize `lineage_dir`,
    /// and so later stages can decide whether to emit events for this pond.
    #[serde(default)]
    pub lineage: bool,
    /// Where this pond's OpenLineage `.jsonl` event files live,
    /// `<root>/<pond-id>/lineage` (no trailing separator). Computed
    /// unconditionally — only its
    /// *existence on disk* reflects the `lineage` opt-in, and because it sits
    /// inside the pond directory, `drop_pond` reaps it with everything else.
    #[serde(default)]
    pub lineage_dir: String,
}
