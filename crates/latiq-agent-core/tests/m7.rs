//! M7 success-criterion tests: query-by-URI ingestion (local file, hermetic) and
//! concurrent multi-agent writes with correct per-identity attribution.
use latiq_agent_core::{AgentConfig, AgentOps, RegistryControlPlane};
use latiq_common::Identity;
use latiq_control_plane::Registry;
use latiq_engine_duckdb::DuckEngine;
use latiq_storage::TempFs;
use std::sync::Arc;

fn ops() -> AgentOps {
    let registry = Registry::open(None).unwrap();
    registry
        .register_node(
            "node-a",
            "http://127.0.0.1:8080/mcp",
            "http://127.0.0.1:9092",
            100,
        )
        .unwrap();
    let control = Arc::new(RegistryControlPlane::new(registry));
    let storage = Arc::new(TempFs::new());
    let engine = Arc::new(DuckEngine::new());
    AgentOps::new(control, storage, engine, AgentConfig::default())
}

#[tokio::test]
async fn query_by_uri_local_csv_ingestion() {
    let ops = ops();
    let id = Identity::claimed(Some("ingestor"));
    ops.allocate_pond(&id, Some("ing".into()), "{}")
        .await
        .unwrap();

    let dir = tempfile::tempdir().unwrap();
    let csv = dir.path().join("data.csv");
    std::fs::write(&csv, "id,name\n1,alice\n2,bob\n3,carol\n").unwrap();

    let sql = format!(
        "CREATE TABLE people AS SELECT * FROM read_csv('{}', header=true)",
        csv.display()
    );
    ops.write_query(&id, "ing", &sql).await.unwrap();

    let r = ops
        .read_query(&id, "ing", "SELECT count(*) AS n FROM people")
        .await
        .unwrap();
    assert_eq!(r.rows[0][0], serde_json::json!(3));
}

#[tokio::test]
async fn concurrent_multi_agent_writes_are_consistent_and_attributed() {
    let ops = ops();
    let setup = Identity::claimed(Some("setup"));
    ops.allocate_pond(&setup, Some("shared".into()), "{}")
        .await
        .unwrap();
    ops.write_query(
        &setup,
        "shared",
        "CREATE TABLE log(agent VARCHAR, n INTEGER)",
    )
    .await
    .unwrap();

    let n_agents = 4;
    let per = 3;
    let mut handles = vec![];
    for a in 0..n_agents {
        let ops2 = ops.clone();
        handles.push(tokio::spawn(async move {
            let id = Identity::claimed(Some(&format!("agent-{a}")));
            for i in 0..per {
                let sql = format!("INSERT INTO log VALUES ('agent-{a}', {i})");
                // DuckLake uses optimistic concurrency; retry on conflict.
                let mut attempts = 0;
                loop {
                    match ops2.write_query(&id, "shared", &sql).await {
                        Ok(_) => break,
                        Err(e) => {
                            attempts += 1;
                            assert!(
                                attempts < 50,
                                "write kept conflicting: {}",
                                e.envelope().message
                            );
                            tokio::time::sleep(std::time::Duration::from_millis(15)).await;
                        }
                    }
                }
            }
        }));
    }
    for h in handles {
        h.await.unwrap();
    }

    let total = ops
        .read_query(&setup, "shared", "SELECT count(*) AS n FROM log")
        .await
        .unwrap();
    assert_eq!(
        total.rows[0][0],
        serde_json::json!(n_agents * per),
        "all concurrent writes must be durable"
    );

    let authors = ops
        .read_query(
            &setup,
            "shared",
            "SELECT count(DISTINCT author) AS a FROM _latiq.attribution WHERE author LIKE 'agent-%'",
        )
        .await
        .unwrap();
    assert_eq!(
        authors.rows[0][0],
        serde_json::json!(n_agents),
        "each agent's writes must be attributed to it"
    );
}
