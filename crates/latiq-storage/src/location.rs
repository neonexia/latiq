//! Where a pond's bytes live — the descriptor handed to the query engine.
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
}
