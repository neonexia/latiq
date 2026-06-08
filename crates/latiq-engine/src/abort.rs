//! Cancellation primitive shared across engines. The in-flight *registry*
//! (op-id → token) lives in latiq-agent-core; here we only define the token
//! and the contract that execute() must honor it and release resources promptly.
pub use tokio_util::sync::CancellationToken as AbortToken;
