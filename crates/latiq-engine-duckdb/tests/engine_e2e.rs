//! End-to-end pond lifecycle through the public seams only (PondStorage +
//! QueryEngine). Proves storage + engine compose: create → init → attributed
//! write → read → attribution (native DuckLake pond.snapshots()) + schema → drop.
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
            "SELECT DISTINCT author FROM pond.snapshots()",
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

#[test]
fn attribution_records_the_verified_subject_not_only_the_claimed_leaf() {
    // A caller with a valid token for `svc-lowpriv` claims a flattering leaf id.
    // History must make the verified subject visible so the claim cannot pass
    // itself off as authenticated identity.
    let fs = TempFs::new();
    let eng = DuckEngine::new();
    let pond = PondId::new();
    let loc = fs.create_pond(pond).unwrap();
    eng.init_pond(&loc).unwrap();

    let id = Identity::verified(
        "svc-lowpriv",
        "https://idp.example/realms/latiq",
        Some("svc-admin"),
    );
    eng.write_query(
        &loc,
        "CREATE TABLE events(id INTEGER)",
        &id,
        AbortToken::new(),
    )
    .unwrap();

    let attr = eng
        .read_query(
            &loc,
            "SELECT * FROM pond.snapshots() ORDER BY snapshot_id DESC LIMIT 1",
            AbortToken::new(),
        )
        .unwrap();
    let msg = serde_json::to_string(&attr.rows[0]).unwrap();
    assert!(
        msg.contains("svc-lowpriv"),
        "must carry the verified subject: {msg}"
    );
    assert!(
        msg.contains("https://idp.example/realms/latiq"),
        "must carry the issuer: {msg}"
    );
    // The claimed leaf is still recorded -- but as a claim, never as the author.
    assert!(
        msg.contains("svc-admin"),
        "must keep the claimed leaf: {msg}"
    );
    let author_ix = attr
        .columns
        .iter()
        .position(|c| c == "author")
        .expect("snapshots() must expose an author column");
    assert_eq!(
        attr.rows[0][author_ix],
        serde_json::json!("svc-lowpriv"),
        "the author must be the verified subject, not the claimed leaf"
    );

    fs.drop_pond(pond).unwrap();
}
