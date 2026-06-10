//! Local-filesystem pond storage: <root>/<pond-id>/{catalog.duckdb, data/}.
use crate::location::PondLocation;
use crate::storage::{PondStorage, StorageError};
use latiq_common::PondId;
use std::path::{Path, PathBuf};

pub struct LocalFs {
    root: PathBuf,
}

impl LocalFs {
    pub fn new(root: impl AsRef<Path>) -> Self {
        Self {
            root: root.as_ref().to_path_buf(),
        }
    }
    fn pond_dir(&self, pond_id: PondId) -> PathBuf {
        self.root.join(pond_id.to_string())
    }
    fn location_for(&self, pond_id: PondId) -> PondLocation {
        let dir = self.pond_dir(pond_id);
        PondLocation {
            catalog_uri: format!("ducklake:duckdb:{}", dir.join("catalog.duckdb").display()),
            data_path: dir.join("data").display().to_string(),
            // Default alias; AgentOps overrides with the pond's registry name.
            catalog_name: "pond".to_string(),
        }
    }
}

impl PondStorage for LocalFs {
    fn create_pond(&self, pond_id: PondId) -> Result<PondLocation, StorageError> {
        let dir = self.pond_dir(pond_id);
        if dir.exists() {
            return Err(StorageError::AlreadyExists(pond_id));
        }
        std::fs::create_dir_all(dir.join("data")).map_err(|e| StorageError::Io(e.to_string()))?;
        Ok(self.location_for(pond_id))
    }
    fn pond_location(&self, pond_id: PondId) -> Result<PondLocation, StorageError> {
        if self.pond_exists(pond_id) {
            Ok(self.location_for(pond_id))
        } else {
            Err(StorageError::NotFound(pond_id))
        }
    }
    fn ensure_pond(&self, pond_id: PondId) -> Result<PondLocation, StorageError> {
        if self.pond_exists(pond_id) {
            Ok(self.location_for(pond_id))
        } else {
            self.create_pond(pond_id)
        }
    }
    fn drop_pond(&self, pond_id: PondId) -> Result<(), StorageError> {
        let dir = self.pond_dir(pond_id);
        if !dir.exists() {
            return Err(StorageError::NotFound(pond_id));
        }
        std::fs::remove_dir_all(dir).map_err(|e| StorageError::Io(e.to_string()))
    }
    fn pond_exists(&self, pond_id: PondId) -> bool {
        self.pond_dir(pond_id).exists()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn create_resolve_drop_lifecycle() {
        let tmp = std::env::temp_dir().join(format!("latiq-localfs-test-{}", PondId::new()));
        let fs = LocalFs::new(&tmp);
        let id = PondId::new();
        assert!(!fs.pond_exists(id));
        let loc = fs.create_pond(id).unwrap();
        assert!(loc.catalog_uri.starts_with("ducklake:duckdb:"));
        assert!(loc.catalog_uri.contains(&id.to_string()));
        assert!(fs.pond_exists(id));
        assert_eq!(fs.pond_location(id).unwrap(), loc);
        assert!(matches!(
            fs.create_pond(id),
            Err(StorageError::AlreadyExists(_))
        ));
        fs.drop_pond(id).unwrap();
        assert!(!fs.pond_exists(id));
        assert!(matches!(
            fs.pond_location(id),
            Err(StorageError::NotFound(_))
        ));
        let _ = std::fs::remove_dir_all(&tmp);
    }
}
