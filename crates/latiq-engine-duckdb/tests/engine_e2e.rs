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
    let loc = fs.create_pond(id, false).unwrap();
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
    let loc = fs.create_pond(pond, false).unwrap();
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
    // The claimed leaf is still recorded -- but as a claim, in its own field, so
    // a reader can never mistake it for the authenticated identity. Parsed, not
    // substring-matched: a substring hit would also pass if the value leaked
    // into an unrelated column.
    let extra = latest_extra_info(&eng, &loc);
    assert_eq!(extra["agent_id"], serde_json::json!("svc-admin"));
    assert_eq!(extra["verified"], serde_json::json!(true));
    assert_eq!(
        extra["issuer"],
        serde_json::json!("https://idp.example/realms/latiq")
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

/// The latest snapshot's `author` and parsed `commit_extra_info`, read the
/// native DuckLake way (`pond.snapshots()`) — no Latiq objects in the catalog.
fn latest_attribution(
    eng: &DuckEngine,
    loc: &latiq_storage::PondLocation,
) -> (String, serde_json::Value) {
    let r = eng
        .read_query(
            loc,
            "SELECT author, commit_extra_info FROM pond.snapshots() \
             ORDER BY snapshot_id DESC LIMIT 1",
            AbortToken::new(),
        )
        .unwrap();
    let row = &r.rows[0];
    let author = row[0].as_str().expect("author is text").to_string();
    let extra = serde_json::from_str(row[1].as_str().expect("commit_extra_info is text"))
        .expect("commit_extra_info must be the JSON we wrote");
    (author, extra)
}

fn latest_extra_info(eng: &DuckEngine, loc: &latiq_storage::PondLocation) -> serde_json::Value {
    latest_attribution(eng, loc).1
}

#[test]
fn attribution_escapes_hostile_identity_values() {
    // Identity values reach a `CALL … set_commit_message('…')` by interpolation:
    // `agent_id` is caller-supplied and `subject`/`issuer` come from a token, so
    // neither is trustworthy text. This pins the escaping — a refactor that
    // escapes before serializing, drops one `.replace`, or hand-builds the JSON
    // must fail here.
    const INJECT: &str = "x'); DROP TABLE t; --";
    const DOUBLED: &str = "a''b";
    const BACKSLASH: &str = r"back\slash\'quote";

    let fs = TempFs::new();
    let eng = DuckEngine::new();
    let pond = PondId::new();
    let loc = fs.create_pond(pond, false).unwrap();
    eng.init_pond(&loc).unwrap();

    let write = |sql: &str, id: &Identity| {
        eng.write_query(&loc, sql, id, AbortToken::new()).unwrap();
    };
    let benign = Identity::claimed(Some("agent-setup"));
    write("CREATE TABLE t(id INTEGER)", &benign);
    write("INSERT INTO t VALUES (1),(2)", &benign);

    // 1. Hostile value as the CLAIMED leaf (unverified caller → it is the author).
    write("INSERT INTO t VALUES (3)", &Identity::claimed(Some(INJECT)));
    let (author, extra) = latest_attribution(&eng, &loc);
    assert_eq!(author, INJECT, "claimed leaf must round-trip verbatim");
    assert_eq!(extra["agent_id"], serde_json::json!(INJECT));
    assert_eq!(extra["verified"], serde_json::json!(false));

    // 2. The same string as a VERIFIED subject (→ it is the author), with a
    //    doubled quote in the claimed leaf and a backslash in the issuer.
    write(
        "INSERT INTO t VALUES (4)",
        &Identity::verified(INJECT, BACKSLASH, Some(DOUBLED)),
    );
    let (author, extra) = latest_attribution(&eng, &loc);
    assert_eq!(author, INJECT, "verified subject must round-trip verbatim");
    assert_eq!(extra["agent_id"], serde_json::json!(DOUBLED));
    assert_eq!(extra["issuer"], serde_json::json!(BACKSLASH));
    assert_eq!(extra["verified"], serde_json::json!(true));

    // 3. A doubled quote as the author itself — naive escaping mangles this.
    write(
        "INSERT INTO t VALUES (5)",
        &Identity::claimed(Some(DOUBLED)),
    );
    let (author, _) = latest_attribution(&eng, &loc);
    assert_eq!(author, DOUBLED, "a doubled quote must not be re-escaped");

    // The injected DROP never executed: the table is intact with every row.
    let rows = eng
        .read_query(&loc, "SELECT count(*) AS c FROM t", AbortToken::new())
        .unwrap();
    assert_eq!(
        rows.rows[0][0],
        serde_json::json!(5),
        "the injected DROP TABLE must not have executed"
    );

    fs.drop_pond(pond).unwrap();
}

#[test]
fn attribution_unverified_caller_has_empty_verified_fields() {
    // The unverified shape is a compatible superset of the old one: the claimed
    // leaf is still the author, and `verified` is explicitly false so no reader
    // can take the claim for an authenticated identity.
    let fs = TempFs::new();
    let eng = DuckEngine::new();
    let pond = PondId::new();
    let loc = fs.create_pond(pond, false).unwrap();
    eng.init_pond(&loc).unwrap();

    let id = Identity::claimed(Some("agent-plain"));
    eng.write_query(&loc, "CREATE TABLE t(id INTEGER)", &id, AbortToken::new())
        .unwrap();

    let (author, extra) = latest_attribution(&eng, &loc);
    assert_eq!(author, "agent-plain");
    assert_eq!(extra["agent_id"], serde_json::json!("agent-plain"));
    assert_eq!(extra["verified"], serde_json::json!(false));
    assert_eq!(extra["issuer"], serde_json::json!(""));

    fs.drop_pond(pond).unwrap();
}
