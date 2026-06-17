//! End-to-end test of the embedded ("local") mode: start an in-process
//! control-plane + pond-node backed by a temp dir, then drive the full pond
//! lifecycle + a query round-trip over the real gRPC surfaces. Remote mode is the
//! same client against an external control plane (exercised by the CLI/full-stack
//! tests), so verifying embedded here covers the shared wire path.
use latiq_sdk::Latiq;

#[test]
fn embedded_pond_lifecycle_and_query_round_trip() {
    let dir = tempfile::tempdir().unwrap();
    let db = Latiq::connect("local", Some(dir.path().to_path_buf())).unwrap();
    assert!(db.server().starts_with("http://127.0.0.1:"));

    // Create → it shows up in the (control-plane) list.
    let p = db.create_pond(Some("work"), "medium").unwrap();
    assert_eq!(p.name, "work");
    assert!(!p.pond_id.is_empty());
    assert!(db.list_ponds().unwrap().iter().any(|x| x.name == "work"));

    // Write then read (node-direct; materializes the pond's DuckLake on first use).
    db.write("work", "CREATE TABLE t(id INTEGER, note VARCHAR)")
        .unwrap();
    db.write("work", "INSERT INTO t VALUES (1,'a'),(2,'b')")
        .unwrap();
    let r = db.read("work", "SELECT count(*) AS n FROM t").unwrap();
    assert_eq!(r["rows"][0][0], 2, "round-tripped rows: {r}");

    // Describe surfaces the pond + its schema.
    let d = db.describe_pond("work").unwrap();
    assert_eq!(d["pond"]["name"], "work");

    // A read is rejected as a write guard check (the engine classifies).
    assert!(
        db.read("work", "INSERT INTO t VALUES (3,'c')").is_err(),
        "read_query must reject a write"
    );

    // Drop requires confirm; after it, the pond no longer resolves.
    assert!(db.drop_pond("work", false).is_err(), "drop needs confirm");
    db.drop_pond("work", true).unwrap();
    assert!(
        db.read("work", "SELECT 1").is_err(),
        "pond must be gone after drop"
    );
}
