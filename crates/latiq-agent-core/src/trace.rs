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

//! Request trace correlation: the ambient [`TraceContext`] for one request.
//!
//! Each inbound adapter builds a context from the request's `traceparent`
//! header (or mints a fresh one) and holds it as a task-local for the duration
//! of handling that request. `AgentOps` logs inherit the id via the span the
//! adapter enters, the access trail and the lineage emitter read it from here,
//! and every outbound hop — the node-to-node forwarder, the node -> control-plane
//! client — stamps [`TraceContext::traceparent`] back onto the wire. So one
//! request's spans, its access-trail records, its lineage events and its error
//! envelope all carry ONE id, across every process it touches.
//!
//! The context type itself is `latiq_common::TraceContext` (the control plane
//! needs the same parser and cannot depend on this crate). What lives here is
//! only the scope, which is why this file has no protocol types either
//! (invariant 5): the header plumbing lives in the adapters.
use std::future::Future;

pub use latiq_common::TraceContext;

tokio::task_local! {
    static TRACE: TraceContext;
}

/// Run `fut` with `ctx` as the ambient trace for the whole request — including
/// any forwarded calls it makes (they run in the same task).
pub async fn with_trace<F: Future>(ctx: TraceContext, fut: F) -> F::Output {
    TRACE.scope(ctx, fut).await
}

/// The ambient trace context, if one is set.
pub fn current_trace() -> Option<TraceContext> {
    TRACE.try_with(|c| c.clone()).ok()
}

/// The ambient trace id — what the access trail, the lineage events and the
/// error envelope report.
pub fn current_trace_id() -> Option<String> {
    TRACE.try_with(|c| c.trace_id().to_string()).ok()
}

/// The `traceparent` value for an outbound hop, if we are in a trace scope.
/// Every outbound client reads this; none of them formats the header itself.
pub fn current_traceparent() -> Option<String> {
    TRACE.try_with(|c| c.traceparent()).ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn trace_the_ambient_scope_is_what_outbound_hops_read() {
        assert_eq!(current_trace_id(), None, "outside a scope there is no id");
        assert_eq!(current_traceparent(), None);

        let ctx = TraceContext::new();
        let expected = ctx.trace_id().to_string();
        let outbound = ctx.traceparent();
        let (id, header) =
            with_trace(ctx, async { (current_trace_id(), current_traceparent()) }).await;

        assert_eq!(id.as_deref(), Some(expected.as_str()));
        // The header a hop sends is this scope's, verbatim — one span id for the
        // whole request, so the peer's work is a child of ours and not of a span
        // that changes on every outbound call.
        assert_eq!(header.as_deref(), Some(outbound.as_str()));
        assert_eq!(
            TraceContext::parse(&outbound).unwrap().trace_id(),
            expected,
            "the id survives the hop, which is the entire point"
        );
    }
}
