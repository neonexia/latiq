//! Request trace correlation. Each inbound adapter sets a `trace_id` as a
//! task-local for the duration of handling one request (from the incoming
//! `latiq-trace-id` metadata, or freshly generated). AgentOps logs inherit it
//! via the span the adapter enters, and the forwarder reads it to stamp the
//! node-to-node call — so one request's spans correlate across nodes by trace id.
//!
//! Protocol-neutral (just a `String` task-local, invariant 5): the gRPC-metadata
//! plumbing lives in the adapters, never here.
use latiq_common::PondId;
use std::future::Future;

tokio::task_local! {
    static TRACE_ID: String;
}

/// A fresh trace id (a UUID).
pub fn new_trace_id() -> String {
    PondId::new().to_string()
}

/// Run `fut` with `id` as the ambient trace id for the whole request — including
/// any forwarded calls it makes (they run in the same task).
pub async fn with_trace_id<F: Future>(id: String, fut: F) -> F::Output {
    TRACE_ID.scope(id, fut).await
}

/// The ambient trace id, if one is set (the forwarder reads this to propagate).
pub fn current_trace_id() -> Option<String> {
    TRACE_ID.try_with(|id| id.clone()).ok()
}
