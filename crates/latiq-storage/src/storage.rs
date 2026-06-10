//! Pluggable physical storage for ponds (LocalFs now; S3/MinIO later).
use crate::location::PondLocation;
use latiq_common::PondId;

#[derive(Debug, thiserror::Error)]
pub enum StorageError {
    #[error("pond already exists: {0}")]
    AlreadyExists(PondId),
    #[error("pond not found: {0}")]
    NotFound(PondId),
    #[error("io error: {0}")]
    Io(String),
}

/// Storage backend. Implementations own the physical layout and produce the
/// per-pond `PondLocation` the engine attaches. They do NOT touch DuckLake.
pub trait PondStorage: Send + Sync {
    /// Provision storage for a new pond. Errors if it already exists.
    fn create_pond(&self, pond_id: PondId) -> Result<PondLocation, StorageError>;
    /// Resolve an existing pond's location.
    fn pond_location(&self, pond_id: PondId) -> Result<PondLocation, StorageError>;
    /// Resolve a pond's location, provisioning storage if it doesn't exist yet.
    /// Lazy materialization: a pond assigned by the registry gets its physical
    /// storage on first use, so allocation can be a pure control-plane op.
    fn ensure_pond(&self, pond_id: PondId) -> Result<PondLocation, StorageError>;
    /// Remove a pond's storage entirely.
    fn drop_pond(&self, pond_id: PondId) -> Result<(), StorageError>;
    /// Whether the pond's storage exists.
    fn pond_exists(&self, pond_id: PondId) -> bool;
}
