//! latiq-agent-core — protocol-neutral agent operations + in-flight/abort registry.
pub mod control;
pub mod error;
pub mod inflight;
pub mod ops;
pub mod registry_control;
pub mod types;

pub use control::ControlPlane;
pub use error::AgentError;
pub use inflight::InFlightRegistry;
pub use ops::{AgentConfig, AgentOps};
pub use registry_control::RegistryControlPlane;
pub use types::{AllocateResult, AuditRecord, DescribeResult, PondInfo};
