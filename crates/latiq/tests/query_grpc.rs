//! Full-stack feature tests for the Data/Query gRPC surface (CLI/SDK path),
//! driven through a real control-plane + pond-node (GrpcControlPlane). Test
//! names are prefixed by feature so `cargo test <feature>` targets them.
mod common;

use common::start_stack;
use latiq_common::ErrorEnvelope;
use latiq_proto::v1::data_client::DataClient;
use latiq_proto::v1::*;
use tonic::{Code, Request};

fn req<T>(msg: T, agent: &str) -> Request<T> {
    let mut r = Request::new(msg);
    r.metadata_mut()
        .insert("latiq-agent-id", agent.parse().unwrap());
    r
}

fn alloc(name: &str) -> AllocatePondRequest {
    AllocatePondRequest {
        name: name.into(),
        policy_json: String::new(),
        tier: String::new(),
    }
}
fn q(pond: &str, sql: &str) -> QueryRequest {
    QueryRequest {
        pond: pond.into(),
        sql: sql.into(),
    }
}
/// Decode the ErrorEnvelope from a tonic Status' details.
fn envelope(status: &tonic::Status) -> ErrorEnvelope {
    serde_json::from_slice(status.details()).expect("status details carry an ErrorEnvelope")
}

async fn client(ep: &str) -> DataClient<tonic::transport::Channel> {
    DataClient::connect(ep.to_string()).await.unwrap()
}

#[tokio::test]
async fn pond_lifecycle_tier_recorded_and_described() {
    let s = start_stack().await;
    let mut c = client(&s.data_endpoint).await;
    c.allocate_pond(req(
        AllocatePondRequest {
            name: "big".into(),
            policy_json: String::new(),
            tier: "large".into(),
        },
        "a",
    ))
    .await
    .unwrap();
    let d = c
        .describe_pond(req(DescribePondRequest { pond: "big".into() }, "a"))
        .await
        .unwrap()
        .into_inner();
    let v: serde_json::Value = serde_json::from_str(&d.json).unwrap();
    assert_eq!(v["pond"]["tier"], "large", "tier recorded + described");
}

#[tokio::test]
async fn result_encoding_arrow_edge_renders_date_and_nested() {
    // The Data read path now collects from the Arrow hop and renders to JSON at
    // the edge. Confirm common types still produce sane JSON (a date string, a
    // nested array) rather than garbage.
    let s = start_stack().await;
    let mut c = client(&s.data_endpoint).await;
    c.allocate_pond(req(alloc("enc"), "a")).await.unwrap();
    let resp = c
        .read_query(req(
            q("enc", "SELECT DATE '2021-03-04' AS d, [10,20,30] AS arr"),
            "a",
        ))
        .await
        .unwrap()
        .into_inner();
    let v: serde_json::Value = serde_json::from_str(&resp.json).unwrap();
    let row0 = &v["rows"][0];
    assert_eq!(v["columns"], serde_json::json!(["d", "arr"]));
    assert!(
        row0[0].as_str().is_some_and(|s| s.contains("2021")),
        "date should render as a readable string, got {}",
        row0[0]
    );
    assert_eq!(row0[1], serde_json::json!([10, 20, 30]), "nested array");
}

#[tokio::test]
async fn result_encoding_renders_timestamptz_named_zone() {
    // Regression: a TIMESTAMP WITH TIME ZONE comes back as Arrow Timestamp(_,
    // Some("UTC")) — a *named* zone (DuckLake's snapshots() does exactly this).
    // The arrow->json path needs arrow's `chrono-tz` feature, or it errors with
    // "only offset based timezones supported without chrono-tz feature".
    let s = start_stack().await;
    let mut c = client(&s.data_endpoint).await;
    c.allocate_pond(req(alloc("tz"), "a")).await.unwrap();
    let resp = c
        .read_query(req(
            q("tz", "SELECT TIMESTAMPTZ '2024-01-02 03:04:05+00' AS ts"),
            "a",
        ))
        .await
        .unwrap()
        .into_inner();
    let v: serde_json::Value = serde_json::from_str(&resp.json).unwrap();
    // The point is that the named-zone timestamp *renders* (no chrono-tz error).
    // The exact value is session-timezone-dependent (icu shifts TIMESTAMPTZ to the
    // local zone — e.g. -08:00 yields 2024-01-01T19:04:05), so assert only that it
    // came back as a non-empty timestamp-ish string, not a specific wall-clock date.
    assert!(
        v["rows"][0][0]
            .as_str()
            .is_some_and(|s| s.contains("2024") && s.contains(':')),
        "timestamptz should render as a readable string, got {}",
        v["rows"][0][0]
    );
}

#[tokio::test]
async fn pond_lifecycle_happy() {
    let s = start_stack().await;
    let mut c = client(&s.data_endpoint).await;

    let a = c
        .allocate_pond(req(alloc("demo"), "alice"))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(a.pond_name, "demo");
    assert!(!a.pond_id.is_empty());

    let d = c
        .describe_pond(req(
            DescribePondRequest {
                pond: "demo".into(),
            },
            "alice",
        ))
        .await
        .unwrap()
        .into_inner();
    let v: serde_json::Value = serde_json::from_str(&d.json).unwrap();
    assert_eq!(v["pond"]["name"], "demo");

    c.drop_pond(req(
        DropPondRequest {
            pond: "demo".into(),
            confirm: true,
        },
        "alice",
    ))
    .await
    .unwrap();

    // describe after drop → NotFound
    let err = c
        .describe_pond(req(
            DescribePondRequest {
                pond: "demo".into(),
            },
            "alice",
        ))
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::NotFound);
}

#[tokio::test]
async fn pond_lifecycle_drop_requires_confirm() {
    let s = start_stack().await;
    let mut c = client(&s.data_endpoint).await;
    c.allocate_pond(req(alloc("keepme"), "alice"))
        .await
        .unwrap();

    // confirm=false → the destructive drop is refused with a structured error.
    let err = c
        .drop_pond(req(
            DropPondRequest {
                pond: "keepme".into(),
                confirm: false,
            },
            "alice",
        ))
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::InvalidArgument);
    assert_eq!(
        envelope(&err).kind,
        latiq_common::ErrorKind::MissingArgument
    );

    // The pond survives the refused drop.
    c.describe_pond(req(
        DescribePondRequest {
            pond: "keepme".into(),
        },
        "alice",
    ))
    .await
    .expect("pond must still exist after an un-confirmed drop");
}

#[tokio::test]
async fn pond_lifecycle_duplicate_name_conflicts() {
    let s = start_stack().await;
    let mut c = client(&s.data_endpoint).await;
    c.allocate_pond(req(alloc("dup"), "alice")).await.unwrap();
    let err = c.allocate_pond(req(alloc("dup"), "bob")).await.unwrap_err();
    assert_eq!(err.code(), Code::AlreadyExists);
    assert_eq!(envelope(&err).kind, latiq_common::ErrorKind::NameConflict);
}

#[tokio::test]
async fn sql_read_write_happy() {
    let s = start_stack().await;
    let mut c = client(&s.data_endpoint).await;
    c.allocate_pond(req(alloc("p"), "alice")).await.unwrap();

    c.write_query(req(
        q("p", "CREATE TABLE t(id INTEGER, note VARCHAR)"),
        "alice",
    ))
    .await
    .unwrap();
    c.write_query(req(q("p", "INSERT INTO t VALUES (1,'a'),(2,'b')"), "alice"))
        .await
        .unwrap();

    let r = c
        .read_query(req(q("p", "SELECT id, note FROM t ORDER BY id"), "alice"))
        .await
        .unwrap()
        .into_inner();
    let v: serde_json::Value = serde_json::from_str(&r.json).unwrap();
    assert_eq!(v["columns"], serde_json::json!(["id", "note"]));
    assert_eq!(v["rows"].as_array().unwrap().len(), 2);
    assert_eq!(v["rows"][1][1], "b");
    assert_eq!(v["statement"], "read_query");
}

#[tokio::test]
async fn sql_read_write_read_rejects_writes() {
    let s = start_stack().await;
    let mut c = client(&s.data_endpoint).await;
    c.allocate_pond(req(alloc("p"), "alice")).await.unwrap();
    let err = c
        .read_query(req(q("p", "INSERT INTO t VALUES (1)"), "alice"))
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::InvalidArgument);
    assert_eq!(
        envelope(&err).kind,
        latiq_common::ErrorKind::ReadOnlyViolation
    );
}

#[tokio::test]
async fn attribution_records_the_writer_identity() {
    let s = start_stack().await;
    let mut c = client(&s.data_endpoint).await;
    c.allocate_pond(req(alloc("p"), "alice")).await.unwrap();
    c.write_query(req(q("p", "CREATE TABLE t(id INTEGER)"), "bob"))
        .await
        .unwrap();
    c.write_query(req(q("p", "INSERT INTO t VALUES (1)"), "bob"))
        .await
        .unwrap();

    let r = c
        .read_query(req(
            q("p", "SELECT DISTINCT author FROM ducklake_snapshots('p')"),
            "viewer",
        ))
        .await
        .unwrap()
        .into_inner();
    let v: serde_json::Value = serde_json::from_str(&r.json).unwrap();
    let authors: Vec<_> = v["rows"]
        .as_array()
        .unwrap()
        .iter()
        .filter_map(|row| row[0].as_str())
        .collect();
    assert!(authors.contains(&"bob"), "got {authors:?}");
}

#[tokio::test]
async fn result_encoding_carries_meta() {
    let s = start_stack().await;
    let mut c = client(&s.data_endpoint).await;
    c.allocate_pond(req(alloc("p"), "alice")).await.unwrap();
    let r = c
        .read_query(req(q("p", "SELECT 1 AS x"), "alice"))
        .await
        .unwrap()
        .into_inner();
    let v: serde_json::Value = serde_json::from_str(&r.json).unwrap();
    assert_eq!(v["status"], "ok");
    assert_eq!(v["_meta"]["rows"], 1);
    assert!(v["_meta"].get("duration_ms").is_some());
}

#[tokio::test]
async fn error_contract_pond_not_found_carries_envelope() {
    let s = start_stack().await;
    let mut c = client(&s.data_endpoint).await;
    let err = c
        .read_query(req(q("ghost", "SELECT 1"), "alice"))
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::NotFound);
    let e = envelope(&err);
    assert_eq!(e.kind, latiq_common::ErrorKind::PondNotFound);
    assert!(!e.suggest.is_empty());
    assert!(e.see.starts_with("latiq://"));
}

#[tokio::test]
async fn identity_defaults_to_anonymous_when_metadata_absent() {
    let s = start_stack().await;
    let mut c = client(&s.data_endpoint).await;
    // No latiq-agent-id metadata.
    c.allocate_pond(Request::new(alloc("anon"))).await.unwrap();
    c.write_query(Request::new(q("anon", "CREATE TABLE t(id INTEGER)")))
        .await
        .unwrap();
    c.write_query(Request::new(q("anon", "INSERT INTO t VALUES (1)")))
        .await
        .unwrap();
    let r = c
        .read_query(Request::new(q(
            "anon",
            "SELECT DISTINCT author FROM ducklake_snapshots('anon')",
        )))
        .await
        .unwrap()
        .into_inner();
    let v: serde_json::Value = serde_json::from_str(&r.json).unwrap();
    let authors: Vec<_> = v["rows"]
        .as_array()
        .unwrap()
        .iter()
        .filter_map(|row| row[0].as_str())
        .collect();
    assert!(authors.contains(&"anonymous"), "got {authors:?}");
}
