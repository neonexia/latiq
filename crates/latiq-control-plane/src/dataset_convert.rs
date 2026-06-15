//! Map registry dataset rows ↔ proto `DatasetMsg`, shared by the Admin and
//! Control gRPC services (keeps `registry` free of transport types).
use crate::registry::{DatasetRow, DatasetTableRow};
use latiq_proto::v1::{DatasetMsg, DatasetTableMsg};

pub fn to_msg(d: DatasetRow) -> DatasetMsg {
    DatasetMsg {
        r#ref: d.reference,
        namespace: d.namespace,
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

pub fn table_from_msg(t: DatasetTableMsg) -> DatasetTableRow {
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
