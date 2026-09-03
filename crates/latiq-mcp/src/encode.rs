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
//! call sets `is_error`, and both reference MCP clients skip output-schema
//! validation entirely on an error result, so one envelope shape serves all
//! thirteen tools instead of thirteen `anyOf`s.
use crate::response::QueryResponse;
use latiq_common::ErrorEnvelope;
use latiq_engine::{ExplainResult, QueryResult};
use rmcp::model::{CallToolResult, Content};
use serde::Serialize;
use serde_json::Value;

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
    r
}

/// Success result from a value of the tool's DECLARED response type.
pub fn ok<T: Serialize>(value: &T) -> CallToolResult {
    dual(serde_json::to_value(value).unwrap_or_default(), false)
}

/// Error result carrying the structured `ErrorEnvelope`.
pub fn err_envelope(env: &ErrorEnvelope) -> CallToolResult {
    let value = serde_json::to_value(env).unwrap_or(Value::Null);
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
