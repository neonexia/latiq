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

//! Control gRPC service (pond-nodes call this).
use crate::error::{to_status, ControlPlaneError};
use crate::registry::Registry;
use latiq_proto::v1::control_server::Control;
use latiq_proto::v1::*;
use tonic::{Request, Response, Status};

pub struct ControlService {
    pub registry: Registry,
}

impl ControlService {
    pub fn new(registry: Registry) -> Self {
        Self { registry }
    }
}

#[tonic::async_trait]
impl Control for ControlService {
    async fn register_node(
        &self,
        req: Request<RegisterNodeRequest>,
    ) -> Result<Response<RegisterNodeResponse>, Status> {
        let r = req.into_inner();
        self.registry
            .register_node(
                &r.node_id,
                &r.mcp_endpoint,
                &r.internal_endpoint,
                r.capacity,
            )
            .map_err(to_status)?;
        Ok(Response::new(RegisterNodeResponse {}))
    }

    async fn heartbeat(
        &self,
        req: Request<HeartbeatRequest>,
    ) -> Result<Response<HeartbeatResponse>, Status> {
        let r = req.into_inner();
        self.registry
            .heartbeat(&r.node_id, r.pond_count)
            .map_err(to_status)?;
        Ok(Response::new(HeartbeatResponse {}))
    }

    async fn create_pond_assignment(
        &self,
        req: Request<CreatePondAssignmentRequest>,
    ) -> Result<Response<CreatePondAssignmentResponse>, Status> {
        let r = req.into_inner();
        let name = if r.name.is_empty() {
            None
        } else {
            Some(r.name)
        };
        let tier = if r.tier.is_empty() {
            "medium"
        } else {
            r.tier.as_str()
        };
        let pond = self
            .registry
            .create_pond(
                name,
                &r.owner_identity,
                &r.policy_json,
                tier,
                &r.extensions,
                &r.description,
                r.lineage,
            )
            .map_err(to_status)?;
        let (_pid, endpoint) = self
            .registry
            .get_pond_location(&pond.pond_id)
            .map_err(to_status)?;
        Ok(Response::new(CreatePondAssignmentResponse {
            pond_id: pond.pond_id,
            assigned_node_endpoint: endpoint,
        }))
    }

    async fn get_pond_location(
        &self,
        req: Request<GetPondLocationRequest>,
    ) -> Result<Response<GetPondLocationResponse>, Status> {
        let (pond_id, node_endpoint) = self
            .registry
            .get_pond_location(&req.into_inner().pond_ref)
            .map_err(to_status)?;
        Ok(Response::new(GetPondLocationResponse {
            pond_id,
            node_endpoint,
        }))
    }

    async fn drop_pond_assignment(
        &self,
        req: Request<DropPondAssignmentRequest>,
    ) -> Result<Response<DropPondAssignmentResponse>, Status> {
        self.registry
            .drop_pond(&req.into_inner().pond_id)
            .map_err(to_status)?;
        Ok(Response::new(DropPondAssignmentResponse {}))
    }

    async fn list_ponds(
        &self,
        _req: Request<ListPondsRequest>,
    ) -> Result<Response<ListPondsResponse>, Status> {
        let rows = self.registry.list_ponds().map_err(to_status)?;
        let mut ponds = Vec::with_capacity(rows.len());
        for row in rows {
            // N+1 list-then-detail read: skip a pond dropped between the list and
            // its pond_info lookup instead of failing the whole call (review #9).
            let (row, created_at, policy_json, endpoint) =
                match self.registry.pond_info(&row.pond_id) {
                    Ok(info) => info,
                    Err(ControlPlaneError::PondNotFound(_)) => continue,
                    Err(e) => return Err(to_status(e)),
                };
            ponds.push(PondInfoMsg {
                pond_id: row.pond_id,
                name: row.name,
                owner: row.owner_identity,
                created_at,
                policy_json,
                node_endpoint: endpoint.unwrap_or_default(),
                tier: row.tier,
                extensions: row.extensions,
                description: row.description,
                lineage: row.lineage,
            });
        }
        Ok(Response::new(ListPondsResponse { ponds }))
    }

    async fn get_pond_info(
        &self,
        req: Request<GetPondInfoRequest>,
    ) -> Result<Response<GetPondInfoResponse>, Status> {
        let (row, created_at, policy_json, endpoint) = self
            .registry
            .pond_info(&req.into_inner().pond_ref)
            .map_err(to_status)?;
        Ok(Response::new(GetPondInfoResponse {
            pond: Some(PondInfoMsg {
                pond_id: row.pond_id,
                name: row.name,
                owner: row.owner_identity,
                created_at,
                policy_json,
                node_endpoint: endpoint.unwrap_or_default(),
                tier: row.tier,
                extensions: row.extensions,
                description: row.description,
                lineage: row.lineage,
            }),
        }))
    }

    async fn get_dataset(
        &self,
        req: Request<GetDatasetRequest>,
    ) -> Result<Response<GetDatasetResponse>, Status> {
        let d = self
            .registry
            .get_dataset(&req.into_inner().name)
            .map_err(to_status)?;
        Ok(Response::new(GetDatasetResponse {
            dataset: Some(crate::dataset_convert::dataset_to_msg(d)),
        }))
    }

    async fn list_datasets(
        &self,
        req: Request<ListDatasetsRequest>,
    ) -> Result<Response<ListDatasetsResponse>, Status> {
        let datasets = self
            .registry
            .list_datasets(&req.into_inner().query)
            .map_err(to_status)?
            .into_iter()
            .map(crate::dataset_convert::dataset_to_msg)
            .collect();
        Ok(Response::new(ListDatasetsResponse { datasets }))
    }

    async fn get_catalog(
        &self,
        req: Request<GetCatalogRequest>,
    ) -> Result<Response<GetCatalogResponse>, Status> {
        let c = self
            .registry
            .get_catalog(&req.into_inner().name)
            .map_err(to_status)?;
        Ok(Response::new(GetCatalogResponse {
            catalog: Some(crate::dataset_convert::catalog_to_msg(c)),
        }))
    }

    async fn list_catalogs(
        &self,
        req: Request<ListCatalogsRequest>,
    ) -> Result<Response<ListCatalogsResponse>, Status> {
        let catalogs = self
            .registry
            .list_catalogs(&req.into_inner().query)
            .map_err(to_status)?
            .into_iter()
            .map(crate::dataset_convert::catalog_to_msg)
            .collect();
        Ok(Response::new(ListCatalogsResponse { catalogs }))
    }
}
