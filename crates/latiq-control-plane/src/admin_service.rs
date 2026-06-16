//! Admin gRPC service (the latiq CLI calls this).
use crate::error::ControlPlaneError;
use crate::registry::Registry;
use latiq_proto::v1::admin_server::Admin;
use latiq_proto::v1::*;
use tonic::{Request, Response, Status};

pub struct AdminService {
    pub registry: Registry,
}

impl AdminService {
    pub fn new(registry: Registry) -> Self {
        Self { registry }
    }
}

fn to_status(e: ControlPlaneError) -> Status {
    match e {
        ControlPlaneError::PondNotFound(m)
        | ControlPlaneError::NodeNotFound(m)
        | ControlPlaneError::DatasetNotFound(m)
        | ControlPlaneError::CatalogNotFound(m) => Status::not_found(m),
        ControlPlaneError::NameConflict(m) => Status::already_exists(m),
        ControlPlaneError::Invalid(m) => Status::invalid_argument(m),
        ControlPlaneError::Storage(m) => Status::internal(m),
    }
}

#[tonic::async_trait]
impl Admin for AdminService {
    async fn list_nodes(
        &self,
        _req: Request<ListNodesRequest>,
    ) -> Result<Response<ListNodesResponse>, Status> {
        let nodes = self
            .registry
            .list_nodes()
            .map_err(to_status)?
            .into_iter()
            .map(node_info)
            .collect();
        Ok(Response::new(ListNodesResponse { nodes }))
    }

    async fn describe_node(
        &self,
        req: Request<DescribeNodeRequest>,
    ) -> Result<Response<DescribeNodeResponse>, Status> {
        let n = self
            .registry
            .describe_node(&req.into_inner().node_id)
            .map_err(to_status)?;
        Ok(Response::new(DescribeNodeResponse {
            node: Some(node_info(n)),
        }))
    }

    async fn policy_get(
        &self,
        _req: Request<PolicyGetRequest>,
    ) -> Result<Response<PolicyGetResponse>, Status> {
        let policy = self.registry.policy_get().map_err(to_status)?;
        Ok(Response::new(PolicyGetResponse {
            policy_json: policy.to_string(),
        }))
    }

    async fn policy_set(
        &self,
        req: Request<PolicySetRequest>,
    ) -> Result<Response<PolicySetResponse>, Status> {
        let r = req.into_inner();
        self.registry
            .policy_set(&r.key, &r.value)
            .map_err(to_status)?;
        Ok(Response::new(PolicySetResponse {}))
    }

    async fn pond_list(
        &self,
        _req: Request<PondListRequest>,
    ) -> Result<Response<PondListResponse>, Status> {
        let rows = self.registry.list_ponds().map_err(to_status)?;
        let mut ponds = Vec::with_capacity(rows.len());
        for row in rows {
            // N+1 list-then-detail read: skip a pond dropped between the list and
            // its pond_info lookup instead of failing the whole call (review #9).
            let (row, created_at, _policy, _endpoint) = match self.registry.pond_info(&row.pond_id)
            {
                Ok(info) => info,
                Err(ControlPlaneError::PondNotFound(_)) => continue,
                Err(e) => return Err(to_status(e)),
            };
            ponds.push(PondSummary {
                pond_id: row.pond_id,
                name: row.name,
                owner: row.owner_identity,
                created_at,
                node_id: row.node_id,
                tier: row.tier,
            });
        }
        Ok(Response::new(PondListResponse { ponds }))
    }

    async fn dataset_add(
        &self,
        req: Request<DatasetAddRequest>,
    ) -> Result<Response<DatasetAddResponse>, Status> {
        let d = req
            .into_inner()
            .dataset
            .ok_or_else(|| Status::invalid_argument("dataset is required"))?;
        let row = crate::registry::DatasetRow {
            name: d.name,
            description: d.description,
            tags: d.tags,
            tables: d
                .tables
                .into_iter()
                .map(crate::dataset_convert::dataset_table_from_msg)
                .collect(),
            created_by: if d.created_by.is_empty() {
                "anonymous".into()
            } else {
                d.created_by
            },
            created_at: String::new(),
        };
        let name = self.registry.add_dataset(&row).map_err(to_status)?;
        Ok(Response::new(DatasetAddResponse { name }))
    }

    async fn dataset_remove(
        &self,
        req: Request<DatasetRemoveRequest>,
    ) -> Result<Response<DatasetRemoveResponse>, Status> {
        self.registry
            .remove_dataset(&req.into_inner().name)
            .map_err(to_status)?;
        Ok(Response::new(DatasetRemoveResponse {}))
    }

    async fn dataset_list(
        &self,
        req: Request<DatasetListRequest>,
    ) -> Result<Response<DatasetListResponse>, Status> {
        let datasets = self
            .registry
            .list_datasets(&req.into_inner().query)
            .map_err(to_status)?
            .into_iter()
            .map(crate::dataset_convert::dataset_to_msg)
            .collect();
        Ok(Response::new(DatasetListResponse { datasets }))
    }

    async fn catalog_add(
        &self,
        req: Request<CatalogAddRequest>,
    ) -> Result<Response<CatalogAddResponse>, Status> {
        let c = req
            .into_inner()
            .catalog
            .ok_or_else(|| Status::invalid_argument("catalog is required"))?;
        if !latiq_common::catalog::is_known_type(&c.r#type) {
            return Err(Status::invalid_argument(format!(
                "unknown catalog type '{}' (supported: iceberg)",
                c.r#type
            )));
        }
        // Allowlist the params at registration — credentials never persist.
        let incoming: std::collections::BTreeMap<String, String> = c.params.into_iter().collect();
        let (kept, dropped) = latiq_common::catalog::filter_params(&c.r#type, &incoming);
        let row = crate::registry::CatalogRow {
            name: c.name,
            r#type: c.r#type,
            params: kept,
            description: c.description,
            tags: c.tags,
            created_by: if c.created_by.is_empty() {
                "anonymous".into()
            } else {
                c.created_by
            },
            created_at: String::new(),
        };
        let name = self.registry.add_catalog(&row).map_err(to_status)?;
        Ok(Response::new(CatalogAddResponse {
            name,
            dropped_params: dropped,
        }))
    }

    async fn catalog_remove(
        &self,
        req: Request<CatalogRemoveRequest>,
    ) -> Result<Response<CatalogRemoveResponse>, Status> {
        self.registry
            .remove_catalog(&req.into_inner().name)
            .map_err(to_status)?;
        Ok(Response::new(CatalogRemoveResponse {}))
    }

    async fn catalog_list(
        &self,
        req: Request<CatalogListRequest>,
    ) -> Result<Response<CatalogListResponse>, Status> {
        let catalogs = self
            .registry
            .list_catalogs(&req.into_inner().query)
            .map_err(to_status)?
            .into_iter()
            .map(crate::dataset_convert::catalog_to_msg)
            .collect();
        Ok(Response::new(CatalogListResponse { catalogs }))
    }
}

fn node_info(n: crate::registry::NodeRow) -> NodeInfo {
    NodeInfo {
        node_id: n.node_id,
        mcp_endpoint: n.mcp_endpoint,
        state: n.state,
        pond_count: n.pond_count,
        last_heartbeat: n.last_heartbeat,
        heartbeat_age_seconds: n.heartbeat_age_seconds.max(0) as u64,
    }
}
