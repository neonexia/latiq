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

//! Encode AgentOps results into MCP `CallToolResult`s with BOTH a text content
//! block (legacy clients) and `structured_content` (modern clients), per spec §8.
//!
//! **Every success goes through a TYPE** (`ok`), never a `serde_json::json!`
//! literal — that is what lets each tool declare an `outputSchema` derived from
//! the shape it really sends (see `schema.rs`). An error still carries the
//! `ErrorEnvelope`, which is deliberately OUTSIDE the declared schema: a failed
//! call sets `is_error`, and a client that skips output-schema validation on an
//! error result never sees the mismatch — so one envelope shape serves all
//! thirteen tools instead of thirteen `anyOf`s, each of which would also have
//! loosened its success schema.
//!
//! **What that rests on, stated exactly.** The skip is verified in the two
//! REFERENCE SDKs and nowhere else: `modelcontextprotocol/typescript-sdk`, whose
//! client guards both validation branches with `&& !result.isError`, and
//! `modelcontextprotocol/python-sdk`, whose client session guards its own with
//! `if … and not result.is_error`. It is **not** in the MCP specification, so it
//! is a fact
//! about two implementations, not a guarantee about clients in general, and no
//! test here can hold it: the behaviour lives in someone else's code, and it is
//! load-bearing for every error this surface returns — a client that DID
//! validate would turn each one into a protocol violation instead of an
//! actionable. It also widened when `40cb97a` gave every tool an `outputSchema`;
//! before that there was no schema to validate against and the question could not
//! arise. Nexus (the agent-readiness harness) is settling it empirically against
//! Claude Code — a real client that is neither reference SDK — in its L0
//! workflow; until that lands, treat the scope above as the whole of the
//! evidence and do not restate it as "clients skip validation".
use crate::response::QueryResponse;
use latiq_common::ErrorEnvelope;
use latiq_engine::{ExplainResult, QueryResult};
use rmcp::model::{CallToolResult, Content};
use serde::Serialize;
use serde_json::Value;

/// The `_meta` key carrying this call's W3C trace context.
///
/// Unprefixed on purpose: MCP reserves the `modelcontextprotocol`/`mcp` prefixes
/// and leaves bare names to the implementation, and `traceparent` is the one
/// spelling this deployment already uses on every surface and every hop
/// (invariant 9) — a `latiq.dev/`-prefixed synonym would be a second name for
/// one thing that an agent then has to be taught.
const TRACEPARENT_META: &str = "traceparent";

fn dual(value: Value, is_error: bool) -> CallToolResult {
    let text = serde_json::to_string(&value).unwrap_or_else(|_| "<unserializable>".into());
    let content = vec![Content::text(text)];
    // CallToolResult is #[non_exhaustive]: use the builders, then set structured.
    let mut r = if is_error {
        CallToolResult::error(content)
    } else {
        CallToolResult::success(content)
    };
    r.structured_content = Some(value);
    r.meta = trace_meta();
    r
}

/// The protocol-level `_meta` every tool result carries: this request's
/// `traceparent`, read from the ambient trace scope `traced` entered.
///
/// **Set HERE, once, for the same reason `err_envelope` stamps the trace id
/// here** — every tool result on this surface funnels through `dual`, so a tool
/// added tomorrow correlates without its author remembering anything. Until
/// #113 only the three query tools returned a trace id on SUCCESS (they carry a
/// `QueryMeta`, which gained `traceparent` in #110), while every FAILURE carried
/// one — exactly backwards, since a successful `allocate_pond` is the call you
/// later need to join against the lineage and access records it produced. The
/// alternative — a `traceparent` field on nine neutral result structs — is nine
/// chances to forget, and would have put an MCP concern in `latiq-agent-core`
/// (invariant 5).
///
/// **One source, so the two places cannot disagree by accident.** The query
/// tools also report a `traceparent` inside their body (`QueryMeta`), and both
/// values come from `latiq_agent_core::current_traceparent()`. Note what "agree"
/// means: the TRACE ID is identical by construction (one ambient scope per
/// request). The SPAN id is identical only when the pond is local — a forwarded
/// query relays the OWNER's `QueryMeta.traceparent`, because the span the caller
/// wants nested is the one that ran the statement (`latiq-agent-core`'s
/// "`QueryMeta::traceparent` follows `served_by`"). So the protocol `_meta` is
/// always the span of the node the agent dialled, and the body's is always the
/// span that did the work; forcing them equal would destroy the distinction
/// #110 exists for.
///
/// Absent rather than empty when there is no scope (a handful of argument
/// validations answer before `traced` is entered): invariant 13(a) — we do not
/// report a correlation id we did not have.
fn trace_meta() -> Option<rmcp::model::Meta> {
    let tp = latiq_agent_core::current_traceparent()?;
    let mut meta = rmcp::model::Meta::new();
    meta.0
        .insert(TRACEPARENT_META.to_string(), Value::String(tp));
    Some(meta)
}

/// Success result from a value of the tool's DECLARED response type.
pub fn ok<T: Serialize>(value: &T) -> CallToolResult {
    dual(serde_json::to_value(value).unwrap_or_default(), false)
}

/// Error result carrying the structured `ErrorEnvelope`.
///
/// The request's trace id is stamped HERE, at the edge, from the ambient scope
/// — the same place and for the same reason as the Data surface's `to_status`.
/// Every tool error funnels through this one function, so an agent can always
/// cite the id of its own failed request; asking ~40 construction sites deep in
/// the core to remember would guarantee that the one that forgot is the one an
/// agent is holding when it needs to ask about it.
pub fn err_envelope(env: &ErrorEnvelope) -> CallToolResult {
    let stamped = env
        .clone()
        .with_trace_id(latiq_agent_core::current_trace_id())
        // Additive to the id, and it KEEPS one already there: an envelope
        // decoded from the pond's owner names the owner's span, which is the
        // span that produced the failure.
        .with_traceparent(latiq_agent_core::current_traceparent());
    let value = serde_json::to_value(&stamped).unwrap_or(Value::Null);
    dual(value, true)
}

/// Encode a query result in the spec §8 shape:
/// `{ columns, rows, statement, status, _meta }`.
pub fn ok_query(statement: &str, qr: QueryResult) -> CallToolResult {
    ok(&QueryResponse {
        columns: qr.columns,
        rows: qr.rows,
        statement: statement.to_string(),
        status: "ok".to_string(),
        meta: qr.meta,
    })
}

/// Encode an explain result.
pub fn ok_explain(er: ExplainResult) -> CallToolResult {
    ok(&er)
}
