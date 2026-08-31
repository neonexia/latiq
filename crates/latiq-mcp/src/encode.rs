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
use latiq_common::ErrorEnvelope;
use latiq_engine::{ExplainResult, QueryResult};
use rmcp::model::{CallToolResult, Content};
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

/// Success result from any serializable value.
pub fn ok_value(value: Value) -> CallToolResult {
    dual(value, false)
}

/// Error result carrying the structured `ErrorEnvelope`.
pub fn err_envelope(env: &ErrorEnvelope) -> CallToolResult {
    let value = serde_json::to_value(env).unwrap_or(Value::Null);
    dual(value, true)
}

/// Encode a query result in the spec §8 shape:
/// `{ rows, columns, statement, status, _meta }`.
pub fn ok_query(statement: &str, qr: QueryResult) -> CallToolResult {
    let value = serde_json::json!({
        "columns": qr.columns,
        "rows": qr.rows,
        "statement": statement,
        "status": "ok",
        "_meta": qr.meta,
    });
    dual(value, false)
}

/// Encode an explain result.
pub fn ok_explain(er: ExplainResult) -> CallToolResult {
    let value = serde_json::to_value(er).unwrap_or(Value::Null);
    dual(value, false)
}
