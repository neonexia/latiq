//! End-to-end test of the embedded ("local") mode: start an in-process
//! control-plane + pond-node backed by a temp dir, then drive the full pond
//! lifecycle + a query round-trip over the real gRPC surfaces. Remote mode is the
//! same client against an external control plane (exercised by the CLI/full-stack
//! tests), so verifying embedded here covers the shared wire path.
use arrow::array::{Array, Int64Array};
use latiq_sdk::Latiq;

#[test]
fn embedded_handle_lifecycle_and_arrow_query() {
    let dir = tempfile::tempdir().unwrap();
    let db = Latiq::connect("local", Some(dir.path().to_path_buf())).unwrap();
    assert!(db.server().starts_with("http://127.0.0.1:"));

    // create_pond returns a handle carrying metadata (incl. description).
    let work = db
        .create_pond(Some("work"), "medium", "round-trip test")
        .unwrap();
    assert_eq!(work.name(), "work");
    assert!(!work.id().is_empty());
    assert_eq!(work.description(), "round-trip test");
    // list_ponds is a map keyed by name.
    assert!(db.list_ponds().unwrap().contains_key("work"));

    // One `query` verb on the handle. Reads stream → Arrow batches (uncapped);
    // writes are attributed server-side and return no rows.
    work.query("CREATE TABLE t(id INTEGER, note VARCHAR)")
        .unwrap();
    work.query("INSERT INTO t VALUES (1,'a'),(2,'b')").unwrap();
    let batches = work.query("SELECT count(*) AS n FROM t").unwrap();
    assert_eq!(batches.len(), 1, "one batch for a scalar");
    let col = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("count is int64");
    assert_eq!(col.value(0), 2, "round-tripped row count");

    // get_pond re-fetches metadata as a handle.
    assert_eq!(
        db.get_pond("work").unwrap().description(),
        "round-trip test"
    );

    // Drop requires confirm; after it, the pond no longer resolves.
    assert!(db.drop_pond("work", false).is_err(), "drop needs confirm");
    db.drop_pond("work", true).unwrap();
    assert!(
        db.query("work", "SELECT 1").is_err(),
        "pond must be gone after drop"
    );
}
