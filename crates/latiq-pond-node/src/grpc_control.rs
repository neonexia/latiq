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

//! `GrpcControlPlane` — implements agent-core's `ControlPlane` over a tonic
//! Control client, so a pond-node (separate process) reaches the control-plane
//! registry remotely. `ControlClient<Channel>` is cheaply cloneable, so each
//! call clones it (tonic RPC methods take `&mut self`).
use latiq_agent_core::{AgentError, ControlPlane};
use latiq_agent_core::{CatalogInfo, DatasetInfo, DatasetTableInfo, PondInfo};
use latiq_proto::v1::control_client::ControlClient;
use latiq_proto::v1::*;
use tonic::transport::Channel;
use tonic::Code;

/// The deployed `ControlPlane`: registry calls over the wire. Cheap to clone —
/// the channel underneath multiplexes, so cloning does not open a connection.
#[derive(Clone)]
pub struct GrpcControlPlane {
    client: ControlClient<Channel>,
}

impl GrpcControlPlane {
    pub async fn connect(control_endpoint: String) -> anyhow::Result<Self> {
        let client = ControlClient::connect(control_endpoint).await?;
        Ok(Self { client })
    }

    pub fn client(&self) -> ControlClient<Channel> {
        self.client.clone()
    }
}

fn status_err(s: tonic::Status) -> AgentError {
    // Prefer the structured ErrorEnvelope the control plane attaches to the
    // Status details (control_service/admin_service to_status) — it carries the
    // correct kind + guidance (e.g. a missing catalog is dataset_not_found, not
    // pond_not_found). Fall back to a code-based mapping only when absent.
    if let Ok(env) = serde_json::from_slice::<latiq_common::ErrorEnvelope>(s.details()) {
        return AgentError::from_envelope(env);
    }
    match s.code() {
        Code::NotFound => AgentError::pond_not_found(s.message()),
        Code::AlreadyExists => AgentError::name_conflict(s.message()),
        // FailedPrecondition = no pond node available to host the request (an
        // availability error, not a missing pond) — keep it distinct from NotFound.
        Code::FailedPrecondition => {
            AgentError::internal(format!("no pond node available: {}", s.message()))
        }
        _ => AgentError::internal(s.message().to_string()),
    }
}

fn to_info(m: PondInfoMsg) -> PondInfo {
    PondInfo {
        pond_id: m.pond_id,
        name: m.name,
        owner: m.owner,
        created_at: m.created_at,
        policy_json: m.policy_json,
        // Empty wire string means "the control plane could not name the owning
        // node" → None, which `placement` refuses rather than guessing at.
        node_id: Some(m.node_id).filter(|s| !s.is_empty()),
        // Empty wire string means "no live owning node" → None.
        node_endpoint: Some(m.node_endpoint).filter(|s| !s.is_empty()),
        tier: m.tier,
        extensions: m.extensions,
        description: m.description,
        lineage: m.lineage,
    }
}

fn dataset_to_info(m: DatasetMsg) -> DatasetInfo {
    DatasetInfo {
        name: m.name,
        description: m.description,
        tags: m.tags,
        tables: m
            .tables
            .into_iter()
            .map(|t| DatasetTableInfo {
                table_name: t.table_name,
                source_uri: t.source_uri,
                format: t.format,
            })
            .collect(),
        created_by: m.created_by,
        created_at: m.created_at,
    }
}

fn catalog_to_info(m: CatalogMsg) -> CatalogInfo {
    CatalogInfo {
        name: m.name,
        r#type: m.r#type,
        params: m.params.into_iter().collect(),
        description: m.description,
        tags: m.tags,
        created_by: m.created_by,
        created_at: m.created_at,
    }
}

#[async_trait::async_trait]
impl ControlPlane for GrpcControlPlane {
    async fn create_pond(
        &self,
        name: Option<String>,
        owner: &str,
        policy_json: &str,
        tier: &str,
        extensions: &[String],
        lineage: bool,
    ) -> Result<PondInfo, AgentError> {
        let mut c = self.client.clone();
        let created = c
            .create_pond_assignment(CreatePondAssignmentRequest {
                name: name.unwrap_or_default(),
                owner_identity: owner.to_string(),
                policy_json: policy_json.to_string(),
                tier: tier.to_string(),
                extensions: extensions.to_vec(),
                // The ControlPlane trait's create_pond doesn't carry a description
                // (MCP/agent allocate defaults empty); the CLI/SDK set it via the
                // Control gRPC create_pond_assignment directly.
                description: String::new(),
                lineage,
            })
            .await
            .map_err(status_err)?
            .into_inner();
        let info = c
            .get_pond_info(GetPondInfoRequest {
                pond_ref: created.pond_id.clone(),
            })
            .await
            .map_err(status_err)?
            .into_inner()
            .pond
            .ok_or_else(|| AgentError::internal("control plane returned no pond info"))?;
        Ok(to_info(info))
    }

    async fn resolve_pond(&self, pond_ref: &str) -> Result<String, AgentError> {
        let mut c = self.client.clone();
        let loc = c
            .get_pond_location(GetPondLocationRequest {
                pond_ref: pond_ref.to_string(),
            })
            .await
            .map_err(status_err)?
            .into_inner();
        Ok(loc.pond_id)
    }

    async fn list_ponds(&self) -> Result<Vec<PondInfo>, AgentError> {
        let mut c = self.client.clone();
        let resp = c
            .list_ponds(ListPondsRequest {})
            .await
            .map_err(status_err)?
            .into_inner();
        Ok(resp.ponds.into_iter().map(to_info).collect())
    }

    async fn pond_info(&self, pond_ref: &str) -> Result<PondInfo, AgentError> {
        let mut c = self.client.clone();
        let resp = c
            .get_pond_info(GetPondInfoRequest {
                pond_ref: pond_ref.to_string(),
            })
            .await
            .map_err(status_err)?
            .into_inner();
        resp.pond
            .map(to_info)
            .ok_or_else(|| AgentError::pond_not_found(pond_ref))
    }

    async fn drop_pond(&self, pond_id: &str) -> Result<(), AgentError> {
        let mut c = self.client.clone();
        c.drop_pond_assignment(DropPondAssignmentRequest {
            pond_id: pond_id.to_string(),
        })
        .await
        .map_err(status_err)?;
        Ok(())
    }

    async fn list_datasets(&self, query: &str) -> Result<Vec<DatasetInfo>, AgentError> {
        let mut c = self.client.clone();
        let resp = c
            .list_datasets(ListDatasetsRequest {
                query: query.to_string(),
            })
            .await
            .map_err(status_err)?
            .into_inner();
        Ok(resp.datasets.into_iter().map(dataset_to_info).collect())
    }

    async fn get_dataset(&self, name: &str) -> Result<DatasetInfo, AgentError> {
        let mut c = self.client.clone();
        let resp = c
            .get_dataset(GetDatasetRequest {
                name: name.to_string(),
            })
            .await
            .map_err(status_err)?
            .into_inner();
        resp.dataset
            .map(dataset_to_info)
            .ok_or_else(|| AgentError::dataset_not_found(name))
    }

    async fn list_catalogs(&self, query: &str) -> Result<Vec<CatalogInfo>, AgentError> {
        let mut c = self.client.clone();
        let resp = c
            .list_catalogs(ListCatalogsRequest {
                query: query.to_string(),
            })
            .await
            .map_err(status_err)?
            .into_inner();
        Ok(resp.catalogs.into_iter().map(catalog_to_info).collect())
    }

    async fn get_catalog(&self, name: &str) -> Result<CatalogInfo, AgentError> {
        let mut c = self.client.clone();
        let resp = c
            .get_catalog(GetCatalogRequest {
                name: name.to_string(),
            })
            .await
            .map_err(status_err)?
            .into_inner();
        resp.catalog
            .map(catalog_to_info)
            .ok_or_else(|| AgentError::internal(format!("catalog '{name}' not found")))
    }
}
