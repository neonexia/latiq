//! Control-plane error type.
#[derive(Debug, thiserror::Error)]
pub enum ControlPlaneError {
    #[error("pond name already exists: {0}")]
    NameConflict(String),
    #[error("pond not found: {0}")]
    PondNotFound(String),
    #[error("node not found: {0}")]
    NodeNotFound(String),
    #[error("storage error: {0}")]
    Storage(String),
}

impl From<duckdb::Error> for ControlPlaneError {
    fn from(e: duckdb::Error) -> Self {
        ControlPlaneError::Storage(e.to_string())
    }
}
