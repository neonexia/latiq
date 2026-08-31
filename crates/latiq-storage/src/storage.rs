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

//! Pluggable physical storage for ponds (LocalFs now; S3/MinIO later).
use crate::location::PondLocation;
use latiq_common::PondId;

/// Why a pond's storage could not be provisioned or resolved. Backend-neutral:
/// anything a specific backend knows (a path, an S3 status) is flattened into
/// `Io`'s message.
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
    /// `lineage` is the pond's registry opt-in: when true the backend also
    /// provisions the pond's lineage directory.
    fn create_pond(&self, pond_id: PondId, lineage: bool) -> Result<PondLocation, StorageError>;
    /// Resolve an existing pond's location.
    fn pond_location(&self, pond_id: PondId) -> Result<PondLocation, StorageError>;
    /// Resolve a pond's location, provisioning storage if it doesn't exist yet.
    /// Lazy materialization: a pond assigned by the registry gets its physical
    /// storage on first use, so allocation can be a pure control-plane op.
    /// With `lineage` true the lineage directory is provisioned too, even when
    /// the pond directory already exists.
    fn ensure_pond(&self, pond_id: PondId, lineage: bool) -> Result<PondLocation, StorageError>;
    /// Remove a pond's storage entirely.
    fn drop_pond(&self, pond_id: PondId) -> Result<(), StorageError>;
    /// Whether the pond's storage exists.
    fn pond_exists(&self, pond_id: PondId) -> bool;
}
