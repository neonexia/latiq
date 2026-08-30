//! latiq-agent-core — protocol-neutral agent operations + in-flight/abort registry.
pub mod access;
pub mod arrow;
pub mod bearer;
pub mod control;
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
pub use error::AgentError;
pub use forward::Forwarder;
pub use inflight::InFlightRegistry;
pub use ops::{AgentConfig, AgentOps};
pub use registry_control::RegistryControlPlane;
pub use trace::{current_trace_id, new_trace_id, with_trace_id};
pub use types::{
    AllocateResult, CatalogInfo, DatasetInfo, DatasetTableInfo, DescribeResult, LoadDatasetResult,
    PondInfo, PullResult,
};
