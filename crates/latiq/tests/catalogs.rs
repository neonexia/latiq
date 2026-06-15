//! Full-stack e2e for the dataset + external-catalog surface, driven over the
//! real Admin + Data gRPC against an in-process control-plane + pond-node.
//!
//! The catalog path is exercised against a **real local DuckLake catalog** (file
//! metadata + local data — no network, no docker), so it runs in the normal CI
//! suite. The iceberg/MinIO variant lives behind docker (see deploy/iceberg-minio
//! + tests/catalogs_iceberg.rs, `#[ignore]`d).
mod common;

use common::start_stack;
use latiq_proto::v1::admin_client::AdminClient;
use latiq_proto::v1::data_client::DataClient;
use latiq_proto::v1::*;
use std::collections::HashMap;
use tonic::Request;

fn req<T>(msg: T, agent: &str) -> Request<T> {
    let mut r = Request::new(msg);
    r.metadata_mut()
        .insert("latiq-agent-id", agent.parse().unwrap());
    r
}

fn json(resp: JsonResponse) -> serde_json::Value {
    serde_json::from_str(&resp.json).unwrap()
}

/// Create a local DuckLake catalog with one table, returning (metadata_path,
/// data_path). Uses a throwaway in-memory DuckDB — same engine the pond uses.
fn seed_ducklake(dir: &std::path::Path) -> (String, String) {
    let meta = dir.join("meta.duckdb");
    let data = dir.join("data");
    std::fs::create_dir_all(&data).unwrap();
    let conn = duckdb::Connection::open_in_memory().unwrap();
    conn.execute_batch(&format!(
        "INSTALL ducklake; LOAD ducklake;
         ATTACH 'ducklake:{}' AS ext (DATA_PATH '{}');
         CREATE TABLE ext.widgets AS
           SELECT * FROM (VALUES (1,'gear',9.99),(2,'bolt',0.99),(3,'pulley',12.40))
                         t(id,name,price);",
        meta.display(),
        data.display(),
    ))
    .unwrap();
    (meta.display().to_string(), data.display().to_string())
}

#[tokio::test]
async fn dataset_load_copies_seeded_sample_into_pond() {
    let s = start_stack().await;
    let mut data = DataClient::connect(s.data_endpoint.clone()).await.unwrap();
    data.allocate_pond(req(
        AllocatePondRequest {
            name: "work".into(),
            policy_json: String::new(),
            tier: String::new(),
        },
        "agent-x",
    ))
    .await
    .unwrap();

    // `holdings` is a seeded sample dataset (one public CSV).
    let loaded = json(
        data.load_dataset(req(
            LoadDatasetRequest {
                pond: "work".into(),
                dataset: "holdings".into(),
            },
            "agent-x",
        ))
        .await
        .unwrap()
        .into_inner(),
    );
    assert_eq!(loaded["dataset"], "holdings");

    let r = json(
        data.read_query(req(
            QueryRequest {
                pond: "work".into(),
                sql: "SELECT count(*) AS n FROM holdings".into(),
            },
            "agent-x",
        ))
        .await
        .unwrap()
        .into_inner(),
    );
    assert!(
        r["rows"][0][0].as_i64().unwrap() >= 1,
        "holdings loaded: {r}"
    );
}

#[tokio::test]
async fn catalog_pull_from_local_ducklake_lands_in_pond() {
    let tmp = tempfile::tempdir().unwrap();
    let (metadata_path, data_path) = seed_ducklake(tmp.path());

    let s = start_stack().await;
    let mut admin = AdminClient::connect(s.admin_endpoint.clone())
        .await
        .unwrap();
    let mut data = DataClient::connect(s.data_endpoint.clone()).await.unwrap();

    // Register the external catalog (operator).
    let added = admin
        .catalog_add(CatalogAddRequest {
            catalog: Some(CatalogMsg {
                name: "ext".into(),
                r#type: "ducklake".into(),
                params: HashMap::from([
                    ("metadata_path".into(), metadata_path),
                    ("data_path".into(), data_path),
                ]),
                description: "local ducklake".into(),
                tags: vec!["test".into()],
                created_by: String::new(),
                created_at: String::new(),
            }),
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(added.name, "ext");

    data.allocate_pond(req(
        AllocatePondRequest {
            name: "shop".into(),
            policy_json: String::new(),
            tier: String::new(),
        },
        "agent-x",
    ))
    .await
    .unwrap();

    // Describe: discover the catalog's tables (transient attach → detach).
    let described = json(
        data.catalog_describe(req(
            CatalogDescribeRequest {
                pond: "shop".into(),
                catalog: "ext".into(),
                params: HashMap::new(),
            },
            "agent-x",
        ))
        .await
        .unwrap()
        .into_inner(),
    );
    let tables: Vec<&str> = described["tables"]
        .as_array()
        .unwrap()
        .iter()
        .filter_map(|t| t["table"].as_str())
        .collect();
    assert!(
        tables.contains(&"widgets"),
        "describe found tables: {tables:?}"
    );

    // Pull a subset into the pond, then detach.
    data.catalog_pull(req(
        CatalogPullRequest {
            pond: "shop".into(),
            catalog: "ext".into(),
            query: "CREATE TABLE cheap AS SELECT id,name FROM ext.widgets WHERE price < 10".into(),
            params: HashMap::new(),
        },
        "agent-x",
    ))
    .await
    .unwrap();

    // The data is now a pond table; the external catalog is detached.
    let r = json(
        data.read_query(req(
            QueryRequest {
                pond: "shop".into(),
                sql: "SELECT count(*) AS n FROM cheap".into(),
            },
            "agent-x",
        ))
        .await
        .unwrap()
        .into_inner(),
    );
    assert_eq!(r["rows"][0][0].as_i64().unwrap(), 2, "pulled rows: {r}");

    // After detach, the external catalog is no longer queryable from the pond.
    let after = data
        .read_query(req(
            QueryRequest {
                pond: "shop".into(),
                sql: "SELECT count(*) FROM ext.widgets".into(),
            },
            "agent-x",
        ))
        .await;
    assert!(
        after.is_err(),
        "external catalog must be detached after pull"
    );
}

#[tokio::test]
async fn catalog_add_drops_credentials_and_rejects_unknown_type() {
    let s = start_stack().await;
    let mut admin = AdminClient::connect(s.admin_endpoint.clone())
        .await
        .unwrap();

    // A credential-shaped param is dropped at add (never persisted).
    let added = admin
        .catalog_add(CatalogAddRequest {
            catalog: Some(CatalogMsg {
                name: "lake".into(),
                r#type: "iceberg".into(),
                params: HashMap::from([
                    ("endpoint".into(), "https://polaris/api".into()),
                    ("token".into(), "SECRET".into()),
                ]),
                description: String::new(),
                tags: vec![],
                created_by: String::new(),
                created_at: String::new(),
            }),
        })
        .await
        .unwrap()
        .into_inner();
    assert!(added.dropped_params.contains(&"token".to_string()));

    // Unknown type is rejected at add.
    let err = admin
        .catalog_add(CatalogAddRequest {
            catalog: Some(CatalogMsg {
                name: "no".into(),
                r#type: "snowflake".into(),
                params: HashMap::new(),
                description: String::new(),
                tags: vec![],
                created_by: String::new(),
                created_at: String::new(),
            }),
        })
        .await;
    assert!(err.is_err(), "unknown catalog type must be rejected");
}
