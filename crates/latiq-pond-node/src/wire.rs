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

//! The canonical JSON shape for a query result on the Data gRPC wire, with its
//! encoder and decoder kept side by side so they can't drift. `query_value`
//! encodes a `QueryResult` for a `JsonResponse`; `query_result_from_json` is its
//! inverse, used when a node forwards a read/write to the owning node and must
//! re-hydrate the peer's response into the same neutral `QueryResult` a local op
//! would have returned. Keep the field names in the two functions in lockstep.
use latiq_agent_core::AgentError;
use latiq_engine::QueryResult;

pub fn query_value(qr: QueryResult) -> serde_json::Value {
    // The `query` CLI command routes everything through write_query, so label by
    // what actually happened: a write creates a snapshot, a read does not.
    let statement = if qr.meta.snapshot_id.is_some() {
        "write_query"
    } else {
        "read_query"
    };
    serde_json::json!({
        "columns": qr.columns,
        "rows": qr.rows,
        "statement": statement,
        "status": "ok",
        "_meta": qr.meta,
    })
}

/// Inverse of `query_value`. `statement`/`status` are presentation-only and are
/// re-derived locally, so we only need `columns`, `rows`, and `_meta`.
pub fn query_result_from_json(v: &serde_json::Value) -> Result<QueryResult, AgentError> {
    let take = |name: &str| v.get(name).cloned().unwrap_or(serde_json::Value::Null);
    let bad = |name: &str, e: serde_json::Error| {
        AgentError::internal(format!("forward decode {name}: {e}"))
    };
    Ok(QueryResult {
        columns: serde_json::from_value(take("columns")).map_err(|e| bad("columns", e))?,
        rows: serde_json::from_value(take("rows")).map_err(|e| bad("rows", e))?,
        meta: serde_json::from_value(take("_meta")).map_err(|e| bad("_meta", e))?,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use latiq_common::QueryMeta;

    #[test]
    fn query_value_round_trips_through_decoder() {
        let qr = QueryResult {
            columns: vec!["id".into(), "sev".into()],
            rows: vec![vec![serde_json::json!(1), serde_json::json!("high")]],
            meta: QueryMeta {
                rows: 1,
                ..Default::default()
            },
        };
        let decoded = query_result_from_json(&query_value(qr.clone())).unwrap();
        assert_eq!(decoded, qr);
    }

    #[test]
    fn a_read_that_records_the_version_it_observed_is_still_labelled_a_read() {
        // Pins the inference above, which is load-bearing and was unguarded.
        // A read now records the DuckLake snapshot it actually observed — but on
        // the input dataset, NOT in `meta.snapshot_id`. Moving it into
        // `snapshot_id` would relabel every read on the Data gRPC wire as
        // `write_query`, which is why the version lives where it does.
        let mut input = latiq_common::DatasetRef::table("pond", "main", "t");
        input.version = Some(42);
        let mut meta = QueryMeta {
            rows: 1,
            ..Default::default()
        };
        meta.set_datasets(vec![input], vec![]);
        assert_eq!(meta.snapshot_id, None, "a read creates no snapshot");
        let read = query_value(QueryResult {
            columns: vec!["i".into()],
            rows: vec![vec![serde_json::json!(1)]],
            meta,
        });
        assert_eq!(read["statement"], "read_query");
        assert_eq!(
            read["_meta"]["inputs"][0]["version"],
            serde_json::json!(42),
            "and the observed version still reaches the wire, on the input"
        );

        // The other half of the same equality: a real write, which does carry a
        // snapshot id, must still be labelled a write.
        let write = query_value(QueryResult {
            columns: vec![],
            rows: vec![],
            meta: QueryMeta {
                snapshot_id: Some(42),
                ..Default::default()
            },
        });
        assert_eq!(write["statement"], "write_query");
    }
}
