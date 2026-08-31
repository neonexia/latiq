//! The `_meta` envelope carried on every query response ("every response carries signal").
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WarningKind {
    Performance,
    Portability,
    SchemaHygiene,
    ResultHygiene,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Warning {
    pub kind: WarningKind,
    pub message: String,
}

/// One dataset a statement read or wrote, as the engine's bound plan named it.
///
/// Structured rather than a bare string because provenance needs all three
/// parts: a name that says which *catalog* a table came from (two ponds can
/// both hold `main.orders`), the namespace an external source must keep, and
/// the version it was read at.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct DatasetRef {
    /// `None` for a table in one of the node's own catalogs — the pond owns it,
    /// and the lineage emitter supplies the pond's namespace, which is the only
    /// place that knows the pond id.
    ///
    /// `Some` only for an **external** source, which keeps its **standard**
    /// scheme (`s3://{bucket}`, `file`) unmodified: that identifier is what
    /// another tool's lineage joins on, and rewriting it would make our events
    /// unjoinable.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub namespace: Option<String>,
    /// `{catalog}.{schema}.{table}` for a table; the object key or path for a
    /// file.
    pub name: String,
    /// The DuckLake snapshot this dataset was **read at**. `None` for anything
    /// the plan gives no snapshot for — a temp table, a Parquet file, a catalog
    /// that is not DuckLake, and every **output**: the version a write produces
    /// is only known once it commits, so the emitter supplies that one from the
    /// statement's own `snapshot_id` rather than finding it here.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub version: Option<i64>,
    /// The dataset's columns, for the OpenLineage `SchemaDatasetFacet`. Empty
    /// unless the engine could read them **cheaply and truthfully**: a table in
    /// the pond's own catalog, on a pond that opted into lineage. An external
    /// dataset (`s3://…`, a Parquet file, another catalog) is deliberately left
    /// empty — we do not have its columns without paying for them, and a
    /// guessed schema is worse than an absent one.
    ///
    /// **Not on the wire** (`serde(skip)`): this exists for the lineage
    /// emitter, which runs in the same process as the engine — a forwarded op
    /// records on its owner, so these never need to cross a hop. Serializing
    /// them would put a column list for every table on every `_meta` envelope,
    /// widening a client-visible contract for a consumer that is not the
    /// client.
    #[serde(skip)]
    pub fields: Vec<DatasetField>,
}

/// One column of a dataset: what OpenLineage's `SchemaDatasetFacet` calls a
/// field. `type_name` is the **engine's own** type name (`DECIMAL(10,2)`,
/// `VARCHAR`), passed through rather than normalised — normalising would tell a
/// consumer something the engine did not say. There is no description: we have
/// none, and inventing one is the column-level version of inventing an input.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct DatasetField {
    pub name: String,
    pub type_name: String,
}

impl DatasetRef {
    /// A table in one of the node's own catalogs.
    pub fn table(catalog: &str, schema: &str, table: &str) -> Self {
        Self {
            namespace: None,
            name: format!("{catalog}.{schema}.{table}"),
            version: None,
            fields: Vec::new(),
        }
    }

    /// An external file or object, split into the standard `namespace`/`name`
    /// pair OpenLineage expects: `s3://bucket` + `key`, or `file` + the path.
    /// Any other scheme keeps `{scheme}://{authority}` as its namespace, so a
    /// source we have never seen still lands somewhere a consumer can join on.
    pub fn external(uri: &str) -> Self {
        let (namespace, name) = match uri.split_once("://") {
            Some(("file", rest)) => ("file".to_string(), rest.to_string()),
            Some((scheme, rest)) => match rest.split_once('/') {
                Some((authority, path)) => (format!("{scheme}://{authority}"), path.to_string()),
                // No path at all (`s3://bucket`) — keep the whole URI as the
                // name. An empty name would be a dataset a consumer cannot
                // display, group or join on.
                None => (format!("{scheme}://{rest}"), uri.to_string()),
            },
            // A bare local path.
            None => ("file".to_string(), uri.to_string()),
        };
        Self {
            namespace: Some(namespace),
            name,
            version: None,
            fields: Vec::new(),
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct QueryMeta {
    pub rows: u64,
    pub rows_affected: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub snapshot_id: Option<i64>,
    pub duration_ms: u64,
    pub bytes_scanned: u64,
    /// Every dataset the statement touched, by name — the flat, human-facing
    /// summary the `_meta` envelope has always advertised. `inputs`/`outputs`
    /// carry the same datasets with the detail provenance needs; this is
    /// derived from them, never populated independently.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub tables_touched: Vec<String>,
    /// What the statement READ, from the engine's bound plan.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub inputs: Vec<DatasetRef>,
    /// What the statement WROTE (or created/dropped), from the same plan.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub outputs: Vec<DatasetRef>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub warnings: Vec<Warning>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub hint: Option<String>,
}

impl QueryMeta {
    /// Record what the engine's plan said this statement read and wrote, and
    /// derive the flat `tables_touched` summary from them — in one place, so
    /// the two views can never disagree. Outputs come first (what a statement
    /// produced is what a reader looks for), and a dataset that is both read
    /// and written (an `UPDATE`) appears once.
    pub fn set_datasets(&mut self, inputs: Vec<DatasetRef>, outputs: Vec<DatasetRef>) {
        self.tables_touched = outputs
            .iter()
            .chain(inputs.iter())
            .map(|d| d.name.clone())
            .fold(Vec::new(), |mut acc, name| {
                if !acc.contains(&name) {
                    acc.push(name);
                }
                acc
            });
        self.inputs = inputs;
        self.outputs = outputs;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn external_datasets_keep_their_standard_scheme() {
        // These identifiers are what another tool's lineage joins on: an
        // s3 object is `s3://{bucket}` + key, a local file is `file` + path.
        // Rewriting either would make our events unjoinable, silently.
        let s3 = DatasetRef::external("s3://warehouse/raw/events.parquet");
        assert_eq!(s3.namespace.as_deref(), Some("s3://warehouse"));
        assert_eq!(s3.name, "raw/events.parquet");

        let local = DatasetRef::external("/data/events.parquet");
        assert_eq!(local.namespace.as_deref(), Some("file"));
        assert_eq!(local.name, "/data/events.parquet");

        let file_uri = DatasetRef::external("file:///data/events.parquet");
        assert_eq!(file_uri.namespace.as_deref(), Some("file"));
        assert_eq!(file_uri.name, "/data/events.parquet");

        // A URI with no path at all (a whole-bucket export target). A dataset
        // with an empty name is one a consumer cannot display or join on.
        let bucket = DatasetRef::external("s3://warehouse");
        assert_eq!(bucket.namespace.as_deref(), Some("s3://warehouse"));
        assert_eq!(bucket.name, "s3://warehouse", "a name must never be empty");
    }

    #[test]
    fn tables_touched_summarizes_the_structured_datasets_without_duplicates() {
        // An UPDATE reads and writes the same table; the flat summary must name
        // it once, and must not lose the input that is not also an output.
        let mut m = QueryMeta::default();
        m.set_datasets(
            vec![
                DatasetRef::table("pond", "main", "orders"),
                DatasetRef::table("pond", "main", "prices"),
            ],
            vec![DatasetRef::table("pond", "main", "orders")],
        );
        assert_eq!(
            m.tables_touched,
            vec!["pond.main.orders", "pond.main.prices"]
        );
        assert_eq!(m.inputs.len(), 2, "the structured lists keep both sides");
        assert_eq!(m.outputs[0].name, "pond.main.orders");
    }

    #[test]
    fn omits_empty_optional_fields() {
        let m = QueryMeta {
            rows: 10,
            duration_ms: 5,
            ..Default::default()
        };
        let v = serde_json::to_value(&m).unwrap();
        assert_eq!(v["rows"], 10);
        assert!(v.get("snapshot_id").is_none());
        assert!(v.get("warnings").is_none(), "empty warnings omitted");
    }

    #[test]
    fn serializes_warning_kind_snake_case() {
        let w = Warning {
            kind: WarningKind::Performance,
            message: "full scan".into(),
        };
        assert_eq!(serde_json::to_value(&w).unwrap()["kind"], "performance");
    }
}
