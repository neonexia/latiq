//! End-to-end pond lifecycle through the public seams only (PondStorage +
//! QueryEngine). Proves storage + engine compose: create → init → attributed
//! write → read → attribution + schema via `_latiq` → drop.
use latiq_common::{Identity, PondId};
use latiq_engine::{AbortToken, QueryEngine};
use latiq_engine_duckdb::DuckEngine;
use latiq_storage::{PondStorage, TempFs};

#[test]
fn pond_lifecycle_end_to_end() {
    let fs = TempFs::new();
    let eng = DuckEngine::new();
    let id = PondId::new();
    let loc = fs.create_pond(id).unwrap();
    eng.init_pond(&loc).unwrap();

    let agent = Identity::claimed(Some("agent-e2e"));
    eng.write_query(
        &loc,
        "CREATE TABLE events(id INTEGER, sev VARCHAR)",
        &agent,
        AbortToken::new(),
    )
    .unwrap();
    let w = eng
        .write_query(
            &loc,
            "INSERT INTO events VALUES (1,'high'),(2,'critical')",
            &agent,
            AbortToken::new(),
        )
        .unwrap();
    assert!(
        w.meta.snapshot_id.is_some(),
        "write should record a snapshot id"
    );

    let r = eng
        .read_query(
            &loc,
            "SELECT id, sev FROM events ORDER BY id",
            AbortToken::new(),
        )
        .unwrap();
    assert_eq!(r.rows.len(), 2);
    assert_eq!(r.rows[1][1], serde_json::json!("critical"));

    let attr = eng
        .read_query(
            &loc,
            "SELECT DISTINCT author FROM _latiq.attribution",
            AbortToken::new(),
        )
        .unwrap();
    let authors: Vec<_> = attr.rows.iter().filter_map(|row| row[0].as_str()).collect();
    assert!(
        authors.contains(&"agent-e2e"),
        "attribution must name the writer; got {authors:?}"
    );

    let schema = eng.describe_schema(&loc).unwrap();
    assert!(
        schema.tables.iter().any(|t| t.name == "events"),
        "describe_schema should list 'events'; got {:?}",
        schema.tables
    );

    fs.drop_pond(id).unwrap();
    assert!(!fs.pond_exists(id));
}
