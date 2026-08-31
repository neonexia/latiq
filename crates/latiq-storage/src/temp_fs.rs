//! Ephemeral pond storage backed by a self-cleaning temp dir. For tests.
use crate::local_fs::LocalFs;
use crate::location::PondLocation;
use crate::storage::{PondStorage, StorageError};
use latiq_common::PondId;
use tempfile::TempDir;

pub struct TempFs {
    _dir: TempDir, // dropped → removed
    inner: LocalFs,
}

impl TempFs {
    pub fn new() -> Self {
        let dir = TempDir::new().expect("create temp dir");
        let inner = LocalFs::new(dir.path());
        Self { _dir: dir, inner }
    }
}

impl Default for TempFs {
    fn default() -> Self {
        Self::new()
    }
}

impl PondStorage for TempFs {
    fn create_pond(&self, id: PondId, lineage: bool) -> Result<PondLocation, StorageError> {
        self.inner.create_pond(id, lineage)
    }
    fn pond_location(&self, id: PondId) -> Result<PondLocation, StorageError> {
        self.inner.pond_location(id)
    }
    fn ensure_pond(&self, id: PondId, lineage: bool) -> Result<PondLocation, StorageError> {
        self.inner.ensure_pond(id, lineage)
    }
    fn drop_pond(&self, id: PondId) -> Result<(), StorageError> {
        self.inner.drop_pond(id)
    }
    fn pond_exists(&self, id: PondId) -> bool {
        self.inner.pond_exists(id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn proves_the_seam_with_a_second_backend() {
        let fs = TempFs::new();
        let id = PondId::new();
        let loc = fs.create_pond(id, false).unwrap();
        assert!(loc.catalog_uri.starts_with("ducklake:duckdb:"));
        assert!(fs.pond_exists(id));
    }
}
