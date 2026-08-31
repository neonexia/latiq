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
