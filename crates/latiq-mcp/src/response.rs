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

//! The response shapes this surface WRAPS around a neutral `AgentOps` result.
//!
//! Most tools hand back a core type verbatim (`AllocateResult`, `DescribeResult`,
//! `ExplainResult`, `LineagePage`, …) and need nothing here. The few that used to
//! be assembled with `serde_json::json!` get a struct instead — same bytes on the
//! wire, but now a TYPE, which is what lets `outputSchema` be derived rather than
//! restated. A `json!` literal cannot be turned into a schema, so declaring one
//! would have meant hand-writing the very copy that drifts.
//!
//! Nothing here changes a shape. If one of these looks wrong, that is a separate
//! change.
use latiq_agent_core::{CatalogInfo, DatasetInfo, PondInfo};
use latiq_common::QueryMeta;
use rmcp::schemars::JsonSchema;
use serde::Serialize;
use serde_json::Value;

/// `read_query` / `write_query`: the spec §8 result shape.
#[derive(Debug, Serialize, JsonSchema)]
#[schemars(crate = "rmcp::schemars")]
pub struct QueryResponse {
    pub columns: Vec<String>,
    /// Positional cells aligned to `columns`. Deliberately untyped: a cell is
    /// whatever the pond's SQL produced, and a schema that claimed otherwise
    /// would be claiming to know the caller's tables.
    pub rows: Vec<Vec<Value>>,
    /// Which tool produced this — `"read_query"` or `"write_query"`.
    pub statement: String,
    pub status: String,
    #[serde(rename = "_meta")]
    pub meta: QueryMeta,
}

/// `list_ponds`.
#[derive(Debug, Serialize, JsonSchema)]
#[schemars(crate = "rmcp::schemars")]
pub struct ListPondsResponse {
    pub ponds: Vec<PondInfo>,
}

/// `drop_pond` — the one tool whose success carries no data of its own.
#[derive(Debug, Serialize, JsonSchema)]
#[schemars(crate = "rmcp::schemars")]
pub struct DropPondResponse {
    pub status: String,
    /// Echoed back as the caller passed it (an id or a name).
    pub pond: String,
}

/// `list_datasets`.
#[derive(Debug, Serialize, JsonSchema)]
#[schemars(crate = "rmcp::schemars")]
pub struct ListDatasetsResponse {
    pub datasets: Vec<DatasetInfo>,
}

/// `list_catalogs`.
#[derive(Debug, Serialize, JsonSchema)]
#[schemars(crate = "rmcp::schemars")]
pub struct ListCatalogsResponse {
    pub catalogs: Vec<CatalogInfo>,
}

/// One table an external catalog holds.
#[derive(Debug, Serialize, JsonSchema)]
#[schemars(crate = "rmcp::schemars")]
pub struct CatalogTableRef {
    pub schema: String,
    pub table: String,
}

/// `describe_catalog`.
#[derive(Debug, Serialize, JsonSchema)]
#[schemars(crate = "rmcp::schemars")]
pub struct DescribeCatalogResponse {
    pub catalog: String,
    pub tables: Vec<CatalogTableRef>,
}
