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

//! `outputSchema` generation for the agent tools.
//!
//! **Generated from the types we actually serialize**, never hand-written. A
//! hand-written JSON Schema restating a struct is a second copy of that struct
//! that nothing keeps in step: the moment a field is added, renamed or made
//! optional, the declaration is a lie and no compiler notices. So every response
//! shape on this surface derives `JsonSchema`, and this module turns the derive
//! into the object that rides `tools/list`.
//!
//! **rmcp does not validate responses against `outputSchema`** (see its
//! `model.rs`: "since rust is a strong type language, we don't need to do json
//! schema validation here"). That is why declaring one cannot break us at
//! runtime — and why `crates/latiq/tests/mcp.rs`'s `output_schema` module
//! validates a REAL response from a running stack against the DECLARED schema.
//! Without that test the declaration is a document, not a contract.
use rmcp::model::JsonObject;
use rmcp::schemars::generate::SchemaSettings;
use rmcp::schemars::JsonSchema;
use serde_json::Value;
use std::any::{Any, TypeId};
use std::collections::HashMap;
use std::sync::{Arc, OnceLock, RwLock};

/// The declared output schema for `T`, cached per type.
///
/// Cached because rmcp builds a fresh `LatiqServer` (and therefore a fresh tool
/// router) per session, so an uncached generator would re-derive all thirteen
/// schemas on every agent connection.
pub fn output_schema<T: JsonSchema + Any>() -> Arc<JsonObject> {
    static CACHE: OnceLock<RwLock<HashMap<TypeId, Arc<JsonObject>>>> = OnceLock::new();
    let cache = CACHE.get_or_init(Default::default);
    if let Some(hit) = cache
        .read()
        .expect("output schema cache poisoned")
        .get(&TypeId::of::<T>())
    {
        return hit.clone();
    }
    let schema = Arc::new(build::<T>());
    cache
        .write()
        .expect("output schema cache poisoned")
        .insert(TypeId::of::<T>(), schema.clone());
    schema
}

fn build<T: JsonSchema>() -> JsonObject {
    // draft 2020-12 is the dialect MCP settled on, and the one rmcp already
    // generates every `inputSchema` under — one dialect per tool entry.
    let root = SchemaSettings::draft2020_12()
        .into_generator()
        .into_root_schema_for::<T>();
    let mut object = match serde_json::to_value(root) {
        Ok(Value::Object(o)) => o,
        other => panic!(
            "a JsonSchema root must serialize to an object, got {other:?} for `{}`",
            std::any::type_name::<T>()
        ),
    };
    strip_descriptions(&mut object);
    // MCP requires the root of an `outputSchema` to be `type: "object"` (rmcp
    // enforces the same on the paths that generate one for you). Every shape we
    // declare is a struct, so this is a guard against a future one that is not —
    // a bare array would have to be wrapped, not published as-is.
    assert_eq!(
        object.get("type").and_then(Value::as_str),
        Some("object"),
        "an MCP outputSchema must have root type `object`; `{}` does not — wrap it",
        std::any::type_name::<T>()
    );
    object
}

/// Drop every `description` keyword from a generated schema.
///
/// The schemas ride in EVERY `tools/list`, so they are paid for out of the
/// model's context on every session. schemars turns Rust doc comments into
/// `description`s, and ours are written for the next engineer — they cite issue
/// numbers, past bugs and internal invariants. Reproducing them thirteen times
/// in an agent's context buys the agent nothing the tool's own description does
/// not already say. What an output schema is *for* is structure: field names,
/// types, which fields are optional. Keep that; drop the prose.
///
/// Keyword-aware rather than a blind key removal: `description` is also a real
/// field name on our dataset/catalog shapes, and it lives under `properties`,
/// where it is a schema NAME and must survive.
fn strip_descriptions(schema: &mut JsonObject) {
    /// Keywords whose value is a `name -> schema` map.
    const SCHEMA_MAPS: &[&str] = &[
        "properties",
        "patternProperties",
        "dependentSchemas",
        "$defs",
        "definitions",
    ];
    /// Keywords whose value is a single schema.
    const SUBSCHEMAS: &[&str] = &[
        "items",
        "additionalItems",
        "additionalProperties",
        "propertyNames",
        "contains",
        "not",
        "if",
        "then",
        "else",
    ];
    /// Keywords whose value is an array of schemas.
    const SCHEMA_LISTS: &[&str] = &["allOf", "anyOf", "oneOf", "prefixItems"];

    schema.remove("description");
    for (key, value) in schema.iter_mut() {
        if SCHEMA_MAPS.contains(&key.as_str()) {
            if let Some(map) = value.as_object_mut() {
                for sub in map.values_mut() {
                    if let Some(sub) = sub.as_object_mut() {
                        strip_descriptions(sub);
                    }
                }
            }
        } else if SUBSCHEMAS.contains(&key.as_str()) {
            if let Some(sub) = value.as_object_mut() {
                strip_descriptions(sub);
            }
        } else if SCHEMA_LISTS.contains(&key.as_str()) {
            if let Some(list) = value.as_array_mut() {
                for sub in list.iter_mut() {
                    if let Some(sub) = sub.as_object_mut() {
                        strip_descriptions(sub);
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::response::{ListDatasetsResponse, QueryResponse};

    /// The `required` list of the schema reached by walking `properties` from
    /// the root, following the `$ref` schemars emits for every named struct.
    fn required_at(schema: &JsonObject, path: &[&str]) -> Vec<String> {
        let root = Value::Object(schema.clone());
        let mut node = root.clone();
        for step in path {
            node = node["properties"][step].clone();
            if let Some(r) = node.get("$ref").and_then(Value::as_str) {
                let name = r.rsplit('/').next().expect("a $ref names a definition");
                node = root["$defs"][name].clone();
            }
        }
        node["required"]
            .as_array()
            .map(|a| {
                a.iter()
                    .filter_map(|v| v.as_str().map(str::to_string))
                    .collect()
            })
            .unwrap_or_default()
    }

    /// **The most likely way to ship a wrong schema.** Several fields on the
    /// `_meta` envelope are `skip_serializing_if`, so a perfectly ordinary
    /// response omits them. If the generated schema listed them as `required`, a
    /// client that does what the spec tells it to do — validate — would reject
    /// our own valid responses.
    #[test]
    fn optional_meta_fields_are_never_required() {
        let schema = output_schema::<QueryResponse>();
        let req = required_at(&schema, &["_meta"]);
        for field in [
            "served_by",
            "timeout_ms",
            "inputs",
            "outputs",
            "hint",
            "warnings",
            "snapshot_id",
            "tables_touched",
        ] {
            assert!(
                !req.contains(&field.to_string()),
                "`_meta.{field}` is omitted from ordinary responses, so requiring \
                 it would make a conforming client reject them; required = {req:?}"
            );
        }
        // Anti-vacuity: the walk really reached the envelope's schema, and the
        // fields that ARE always serialized are still required — so this cannot
        // pass against an empty or missing sub-schema.
        assert!(
            req.contains(&"rows".to_string()) && req.contains(&"duration_ms".to_string()),
            "the always-serialized fields must stay required: {req:?}"
        );
    }

    /// The same rule one level up, on the shapes with optional fields of their
    /// own: `TableInfo.comment` (most tables have none) and `LineagePage`'s
    /// `#[serde(default)]` counters.
    #[test]
    fn optional_fields_on_the_other_declared_shapes_are_not_required() {
        // `describe_pond`: most tables carry no COMMENT, so `comment` is absent
        // from an entirely ordinary response.
        let describe =
            Value::Object((*output_schema::<latiq_agent_core::DescribeResult>()).clone());
        let table_req: Vec<String> = describe["$defs"]["TableInfo"]["required"]
            .as_array()
            .expect("TableInfo is a defined struct")
            .iter()
            .filter_map(|v| v.as_str().map(str::to_string))
            .collect();
        assert!(
            !table_req.contains(&"comment".to_string()),
            "an undocumented table omits `comment`: {table_req:?}"
        );
        assert!(
            table_req.contains(&"name".to_string()),
            "…while the always-present fields stay required: {table_req:?}"
        );

        // `get_lineage`: the clamp report and the unreadable-file counter are
        // `#[serde(default)]` and were added after the shape shipped, so a page
        // from an older node need not carry them.
        let page = output_schema::<latiq_agent_core::LineagePage>();
        let page_req = required_at(&page, &[]);
        for field in ["limit_applied", "unreadable_files"] {
            assert!(
                !page_req.contains(&field.to_string()),
                "`{field}` defaults rather than being required: {page_req:?}"
            );
        }
        assert!(
            page_req.contains(&"events".to_string()),
            "…while the page's substance stays required: {page_req:?}"
        );
    }

    /// The schemas are paid for out of the agent's context on every session, so
    /// the Rust doc comments (written for engineers, citing issue numbers and
    /// past bugs) must not travel with them.
    #[test]
    fn descriptions_are_stripped_but_a_field_named_description_survives() {
        let schema = output_schema::<ListDatasetsResponse>();
        let text = serde_json::to_string(&*schema).expect("a schema serializes");
        assert!(
            !text.contains("\"description\":\""),
            "no `description` keyword may survive: {text}"
        );
        // …while the dataset's own `description` FIELD is still declared. A
        // blind key removal would have deleted it, and the schema would then
        // have been wrong about a field we always send.
        let props = Value::Object((*schema).clone())["$defs"]["DatasetInfo"]["properties"].clone();
        assert!(
            props.get("description").is_some(),
            "`DatasetInfo.description` is a field name, not prose: {props:#}"
        );
    }
}
