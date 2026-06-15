//! latiq-agent-core — protocol-neutral agent operations + in-flight/abort registry.
pub mod arrow;
pub mod control;
pub mod error;
pub mod forward;
pub mod inflight;
pub mod ops;
pub mod registry_control;
pub mod trace;
pub mod types;

pub use arrow::{ArrowReadStream, BatchStream};
pub use control::ControlPlane;
pub use error::AgentError;
pub use forward::Forwarder;
pub use inflight::InFlightRegistry;
pub use ops::{AgentConfig, AgentOps};
pub use registry_control::RegistryControlPlane;
pub use trace::{current_trace_id, new_trace_id, with_trace_id};
pub use types::{
    AllocateResult, AuditRecord, DatasetInfo, DatasetTableInfo, DescribeResult, LoadDatasetResult,
    PondInfo,
};
