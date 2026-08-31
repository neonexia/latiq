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

//! Local-filesystem pond storage: <root>/<pond-id>/{catalog.duckdb, data/},
//! plus `lineage/` for ponds that opted into lineage — that one directory is
//! conditional, so the opt-in is visible on disk and not only in the registry.
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
    /// The single definition of the lineage path, so the string handed out in
    /// `PondLocation` and the directory actually created cannot diverge.
    fn lineage_dir(&self, pond_id: PondId) -> PathBuf {
        self.pond_dir(pond_id).join("lineage")
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
            lineage_dir: self.lineage_dir(pond_id).display().to_string(),
        }
    }
    /// Materialize `<root>/<pond-id>/lineage`. Idempotent, and cheap in the
    /// steady state: `ensure_pond` runs on every query, so an already-created
    /// directory costs one `stat` and — more importantly — cannot fail. Without
    /// the fast path a transient mkdir error would turn an ordinary read, which
    /// touches nothing under `lineage/`, into a storage error.
    fn create_lineage_dir(&self, pond_id: PondId) -> Result<(), StorageError> {
        let dir = self.lineage_dir(pond_id);
        if dir.exists() {
            return Ok(());
        }
        std::fs::create_dir_all(dir).map_err(|e| StorageError::Io(e.to_string()))
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
    fn ensure_pond_on_a_materialised_lineage_pond_leaves_it_intact() {
        // Regression pin (7f0e518): `create_lineage_dir` ran an unconditional
        // `create_dir_all` on every query, so a transient mkdir error turned an
        // ordinary read — which touches nothing under `lineage/` — into a
        // storage error. The existence fast path is what this holds in place.
        // ensure_pond runs on every query, so re-ensuring must be a no-op:
        // it must succeed and must not disturb events already written there.
        let tmp = std::env::temp_dir().join(format!("latiq-localfs-test-{}", PondId::new()));
        let fs = LocalFs::new(&tmp);
        let id = PondId::new();
        let loc = fs.create_pond(id, true).unwrap();

        let event = PathBuf::from(&loc.lineage_dir).join("events.jsonl");
        std::fs::write(&event, "{\"eventType\":\"START\"}\n").unwrap();

        for _ in 0..3 {
            let again = fs.ensure_pond(id, true).unwrap();
            assert_eq!(again.lineage_dir, loc.lineage_dir);
        }

        // Non-vacuous: the exact bytes are still there, so the directory was
        // neither recreated nor cleared by the re-ensures above.
        assert_eq!(
            std::fs::read_to_string(&event).unwrap(),
            "{\"eventType\":\"START\"}\n"
        );
        assert!(PathBuf::from(&loc.lineage_dir).is_dir());

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
