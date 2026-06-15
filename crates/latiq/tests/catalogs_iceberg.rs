//! Iceberg + MinIO end-to-end for the catalog pull path. `#[ignore]`d because it
//! needs a live Iceberg REST catalog + S3 (MinIO) — bring them up with
//! `deploy/iceberg-minio/up.sh`, then run with `--ignored`. Config comes from env
//! (set by the harness / CI):
//!
//!   LATIQ_ICEBERG_ENDPOINT  LATIQ_ICEBERG_WAREHOUSE  LATIQ_ICEBERG_TOKEN
//!   LATIQ_S3_ENDPOINT  LATIQ_S3_ACCESS_KEY  LATIQ_S3_SECRET_KEY
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
fn env(k: &str) -> String {
    std::env::var(k).unwrap_or_else(|_| panic!("set {k} (see deploy/iceberg-minio/up.sh)"))
}
fn json(resp: JsonResponse) -> serde_json::Value {
    serde_json::from_str(&resp.json).unwrap()
}

#[tokio::test]
#[ignore = "needs a live Iceberg REST + MinIO; see deploy/iceberg-minio/up.sh"]
async fn iceberg_pull_seeded_widgets_into_pond() {
    // Storage creds + the REST bearer ride in at pull/describe — never persisted.
    let runtime = HashMap::from([
        ("token".to_string(), env("LATIQ_ICEBERG_TOKEN")),
        ("s3_endpoint".to_string(), env("LATIQ_S3_ENDPOINT")),
        ("s3_access_key".to_string(), env("LATIQ_S3_ACCESS_KEY")),
        ("s3_secret_key".to_string(), env("LATIQ_S3_SECRET_KEY")),
        ("s3_region".to_string(), "us-east-1".to_string()),
    ]);

    let s = start_stack().await;
    let mut admin = AdminClient::connect(s.admin_endpoint.clone())
        .await
        .unwrap();
    let mut data = DataClient::connect(s.data_endpoint.clone()).await.unwrap();

    admin
        .catalog_add(CatalogAddRequest {
            catalog: Some(CatalogMsg {
                name: "lake".into(),
                r#type: "iceberg".into(),
                params: HashMap::from([
                    ("endpoint".into(), env("LATIQ_ICEBERG_ENDPOINT")),
                    ("warehouse".into(), env("LATIQ_ICEBERG_WAREHOUSE")),
                    ("s3_endpoint".into(), env("LATIQ_S3_ENDPOINT")),
                ]),
                description: "local iceberg".into(),
                tags: vec!["test".into()],
                created_by: String::new(),
                created_at: String::new(),
            }),
        })
        .await
        .unwrap();

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

    let described = json(
        data.catalog_describe(req(
            CatalogDescribeRequest {
                pond: "shop".into(),
                catalog: "lake".into(),
                params: runtime.clone(),
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
    assert!(tables.contains(&"widgets"), "tables: {tables:?}");

    data.catalog_pull(req(
        CatalogPullRequest {
            pond: "shop".into(),
            catalog: "lake".into(),
            query: "CREATE TABLE cheap AS SELECT id,name FROM lake.demo.widgets WHERE price < 10"
                .into(),
            params: runtime,
        },
        "agent-x",
    ))
    .await
    .unwrap();

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
    assert_eq!(r["rows"][0][0].as_i64().unwrap(), 2);
}
