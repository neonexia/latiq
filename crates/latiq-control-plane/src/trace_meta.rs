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

//! Reading W3C `traceparent` off a gRPC request, for the control plane's two
//! surfaces.
//!
//! The parser is `latiq_common::TraceContext` — shared with the pond node, not
//! copied, because it decides what we are willing to believe from a caller and a
//! second copy of that is how the two ends of one hop start disagreeing.
//!
//! Unlike the pond node, the control plane keeps **no ambient scope**: it has no
//! deep call stack to thread an id through — a handler is a registry read plus,
//! for exactly one RPC, a call to a node. Passing the value explicitly is fewer
//! moving parts than a second task-local, and it makes the one place that
//! propagates it visible in the signature.
use latiq_common::TraceContext;
use tonic::Request;

/// The `traceparent` header value, if the caller sent one.
fn header<T>(req: &Request<T>) -> Option<&str> {
    req.metadata()
        .get("traceparent")
        .and_then(|v| v.to_str().ok())
}

/// The caller's trace id, or `""` when it sent no usable `traceparent`.
///
/// Deliberately does NOT mint one. This feeds the `latiq::access` record and
/// nothing else on this surface, and an id we invented, logged once and never
/// propagated is not a correlation — it just looks like one. `""` renders as the
/// trail's `-`, which is what "there is nothing to join this to" already means
/// everywhere else in that stream.
pub(crate) fn trace_id_of<T>(req: &Request<T>) -> String {
    header(req)
        .and_then(TraceContext::parse)
        .map(|c| c.trace_id().to_string())
        .unwrap_or_default()
}

/// The caller's trace context, or a fresh one — for the RPC that makes an
/// outbound call. Here minting IS right: the id goes onto the node hop, so it
/// joins the control plane's record to the node's.
pub(crate) fn trace_of<T>(req: &Request<T>) -> TraceContext {
    TraceContext::inbound(header(req))
}
