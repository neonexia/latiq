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

//! latiq-agent-core — protocol-neutral agent operations + in-flight/abort registry.
pub mod access;
pub mod arrow;
pub mod bearer;
pub mod control;
pub mod deadline;
pub mod error;
pub mod forward;
pub mod inflight;
/// The lineage emitter. Private: unlike `access`, which every surface calls, it
/// is reached only from the public ops methods — one emit per operation, on the
/// node that ran it (see the module doc).
pub(crate) mod lineage;
pub mod ops;
pub mod registry_control;
pub mod trace;
pub mod types;

pub use access::record as record_access;
pub use arrow::{ArrowReadStream, BatchStream};
pub use bearer::{current_bearer, with_bearer};
pub use control::ControlPlane;
pub use deadline::QueryControls;
pub use error::AgentError;
pub use forward::Forwarder;
pub use inflight::InFlightRegistry;
pub use ops::{AgentConfig, AgentOps};
pub use registry_control::RegistryControlPlane;
pub use trace::{current_trace_id, new_trace_id, with_trace_id};
pub use types::{
    AllocateResult, CatalogInfo, DatasetInfo, DatasetTableInfo, DescribeResult, LineagePage,
    LoadDatasetResult, PondInfo, PullResult,
};
