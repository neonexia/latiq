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

//! The control-plane operations AgentOps depends on. Abstracted as a trait so
//! it can be backed in-process by the Registry (`RegistryControlPlane`) or over
//! the Control gRPC (`GrpcControlPlane`, in `latiq-pond-node`).
use crate::error::AgentError;
use crate::types::{CatalogInfo, DatasetInfo, PondInfo};

#[async_trait::async_trait]
pub trait ControlPlane: Send + Sync {
    async fn create_pond(
        &self,
        name: Option<String>,
        owner: &str,
        policy_json: &str,
        tier: &str,
        extensions: &[String],
        // Opt-in lineage recording. Fixed for the pond's lifetime — there is
        // deliberately no setter anywhere on this trait.
        lineage: bool,
    ) -> Result<PondInfo, AgentError>;

    /// Resolve a pond ref (id or name) to its pond_id, erroring if absent.
    async fn resolve_pond(&self, pond_ref: &str) -> Result<String, AgentError>;

    async fn list_ponds(&self) -> Result<Vec<PondInfo>, AgentError>;

    async fn pond_info(&self, pond_ref: &str) -> Result<PondInfo, AgentError>;

    async fn drop_pond(&self, pond_id: &str) -> Result<(), AgentError>;

    /// Read the dataset/catalog registry (for loading/pulling + agent discovery).
    async fn list_datasets(&self, query: &str) -> Result<Vec<DatasetInfo>, AgentError>;
    async fn get_dataset(&self, name: &str) -> Result<DatasetInfo, AgentError>;
    async fn list_catalogs(&self, query: &str) -> Result<Vec<CatalogInfo>, AgentError>;
    async fn get_catalog(&self, name: &str) -> Result<CatalogInfo, AgentError>;
}
