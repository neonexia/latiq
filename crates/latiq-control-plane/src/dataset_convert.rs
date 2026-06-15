//! Map registry rows ↔ proto messages for datasets and catalogs, shared by the
//! Admin and Control gRPC services (keeps `registry` free of transport types).
use crate::registry::{CatalogRow, DatasetRow, DatasetTableRow};
use latiq_proto::v1::{CatalogMsg, DatasetMsg, DatasetTableMsg};

pub fn dataset_to_msg(d: DatasetRow) -> DatasetMsg {
    DatasetMsg {
        name: d.name,
        description: d.description,
        tags: d.tags,
        tables: d
            .tables
            .into_iter()
            .map(|t| DatasetTableMsg {
                table_name: t.table_name,
                source_uri: t.source_uri,
                format: t.format,
            })
            .collect(),
        created_by: d.created_by,
        created_at: d.created_at,
    }
}

pub fn dataset_table_from_msg(t: DatasetTableMsg) -> DatasetTableRow {
    DatasetTableRow {
        table_name: t.table_name,
        source_uri: t.source_uri,
        format: if t.format.trim().is_empty() {
            "auto".to_string()
        } else {
            t.format
        },
    }
}

pub fn catalog_to_msg(c: CatalogRow) -> CatalogMsg {
    CatalogMsg {
        name: c.name,
        r#type: c.r#type,
        params: c.params.into_iter().collect(),
        description: c.description,
        tags: c.tags,
        created_by: c.created_by,
        created_at: c.created_at,
    }
}
