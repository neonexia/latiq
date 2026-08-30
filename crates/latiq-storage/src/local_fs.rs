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
            // AgentOps sets caps from the pond's tier; default to engine defaults.
            limits: None,
            // AgentOps sets these from the pond's registry record.
            extensions: Vec::new(),
            lineage: false,
            lineage_dir: dir.join("lineage").display().to_string(),
        }
    }
    /// Materialize `<root>/<pond-id>/lineage/`. Idempotent — `create_dir_all`
    /// is a no-op when the directory is already there, which is what the
    /// `ensure_pond` path needs for a pond whose storage already exists.
    fn create_lineage_dir(&self, pond_id: PondId) -> Result<(), StorageError> {
        std::fs::create_dir_all(self.pond_dir(pond_id).join("lineage"))
            .map_err(|e| StorageError::Io(e.to_string()))
    }
}

impl PondStorage for LocalFs {
    fn create_pond(&self, pond_id: PondId, lineage: bool) -> Result<PondLocation, StorageError> {
        let dir = self.pond_dir(pond_id);
        if dir.exists() {
            return Err(StorageError::AlreadyExists(pond_id));
        }
        std::fs::create_dir_all(dir.join("data")).map_err(|e| StorageError::Io(e.to_string()))?;
        if lineage {
            self.create_lineage_dir(pond_id)?;
        }
        Ok(self.location_for(pond_id))
    }
    fn pond_location(&self, pond_id: PondId) -> Result<PondLocation, StorageError> {
        if self.pond_exists(pond_id) {
            Ok(self.location_for(pond_id))
        } else {
            Err(StorageError::NotFound(pond_id))
        }
    }
    fn ensure_pond(&self, pond_id: PondId, lineage: bool) -> Result<PondLocation, StorageError> {
        if self.pond_exists(pond_id) {
            // A pond provisioned before lineage was enabled still needs the
            // directory, so create it here too — not only on first create.
            if lineage {
                self.create_lineage_dir(pond_id)?;
            }
            Ok(self.location_for(pond_id))
        } else {
            self.create_pond(pond_id, lineage)
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
        let loc = fs.create_pond(id, false).unwrap();
        assert!(loc.catalog_uri.starts_with("ducklake:duckdb:"));
        assert!(loc.catalog_uri.contains(&id.to_string()));
        assert!(fs.pond_exists(id));
        assert_eq!(fs.pond_location(id).unwrap(), loc);
        assert!(matches!(
            fs.create_pond(id, false),
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

    #[test]
    fn lineage_dir_lives_under_the_pond_directory() {
        // Inside the pond dir, so drop_pond's remove_dir_all reaps it and
        // lineage needs no reaper of its own.
        let tmp = std::env::temp_dir().join(format!("latiq-localfs-test-{}", PondId::new()));
        let fs = LocalFs::new(&tmp);
        let id = PondId::new();
        let loc = fs.create_pond(id, true).unwrap();

        // The exact path, not merely "somewhere": <root>/<pond-id>/lineage.
        let pond_dir = tmp.join(id.to_string());
        assert_eq!(
            PathBuf::from(&loc.lineage_dir),
            pond_dir.join("lineage"),
            "lineage dir must be <root>/<pond-id>/lineage"
        );
        // And it is genuinely nested under the dir drop_pond removes, which is
        // the property that makes it self-reaping.
        assert!(PathBuf::from(&loc.lineage_dir).starts_with(&pond_dir));
        assert!(PathBuf::from(&loc.lineage_dir).is_dir());

        let _ = std::fs::remove_dir_all(&tmp);
    }

    #[test]
    fn lineage_dir_is_created_only_when_lineage_is_enabled() {
        // A pond without lineage must have no lineage directory at all --
        // the opt-in has to be visible on disk, not just in the registry.
        let tmp = std::env::temp_dir().join(format!("latiq-localfs-test-{}", PondId::new()));
        let fs = LocalFs::new(&tmp);

        let off = PondId::new();
        let off_loc = fs.create_pond(off, false).unwrap();
        assert!(
            !PathBuf::from(&off_loc.lineage_dir).exists(),
            "lineage-off pond must not have a lineage directory on disk"
        );
        // The pond itself was still provisioned — the absence above is the
        // opt-out, not a failed create.
        assert!(PathBuf::from(&off_loc.data_path).is_dir());

        let on = PondId::new();
        let on_loc = fs.create_pond(on, true).unwrap();
        assert!(
            PathBuf::from(&on_loc.lineage_dir).is_dir(),
            "lineage-on pond must have a lineage directory on disk"
        );

        // ensure_pond is the lazy-materialisation path: it must create the
        // directory even for a pond dir that already exists.
        let ensured = fs.ensure_pond(off, true).unwrap();
        assert!(
            PathBuf::from(&ensured.lineage_dir).is_dir(),
            "ensure_pond must materialise the lineage dir for an existing pond"
        );
        // Idempotent: a second ensure with the dir already there is not an error.
        fs.ensure_pond(off, true).unwrap();

        let _ = std::fs::remove_dir_all(&tmp);
    }

    #[test]
    fn lineage_is_reaped_with_the_pond() {
        let tmp = std::env::temp_dir().join(format!("latiq-localfs-test-{}", PondId::new()));
        let fs = LocalFs::new(&tmp);
        let id = PondId::new();
        let loc = fs.create_pond(id, true).unwrap();

        let event = PathBuf::from(&loc.lineage_dir).join("events.jsonl");
        std::fs::write(&event, "{\"eventType\":\"COMPLETE\"}\n").unwrap();
        assert!(event.is_file(), "precondition: the event file was written");

        fs.drop_pond(id).unwrap();

        assert!(!event.exists(), "lineage events must go with the pond");
        assert!(!PathBuf::from(&loc.lineage_dir).exists());

        let _ = std::fs::remove_dir_all(&tmp);
    }
}
