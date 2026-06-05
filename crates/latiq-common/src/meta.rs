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

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct QueryMeta {
    pub rows: u64,
    pub rows_affected: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub snapshot_id: Option<i64>,
    pub duration_ms: u64,
    pub bytes_scanned: u64,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub tables_touched: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub warnings: Vec<Warning>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub hint: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;

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
