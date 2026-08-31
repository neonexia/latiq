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
