//! Full-stack feature tests for the operator Admin gRPC surface (control plane):
//! node list, pond list (metadata read), policy, audit. Names prefixed by feature.
mod common;

use common::start_stack;
use latiq_proto::v1::admin_client::AdminClient;
use latiq_proto::v1::data_client::DataClient;
use latiq_proto::v1::*;
use tonic::Request;

fn id_req<T>(msg: T, agent: &str) -> Request<T> {
    let mut r = Request::new(msg);
    r.metadata_mut()
        .insert("latiq-agent-id", agent.parse().unwrap());
    r
}

#[tokio::test]
async fn audit_tail_records_operations_with_identity() {
    let s = start_stack().await;
    let mut data = DataClient::connect(s.data_endpoint.clone()).await.unwrap();
    data.allocate_pond(id_req(
        AllocatePondRequest {
            name: "p".into(),
            policy_json: String::new(),
        },
        "alice",
    ))
    .await
    .unwrap();
    data.write_query(id_req(
        QueryRequest {
            pond: "p".into(),
            sql: "CREATE TABLE t(id INTEGER)".into(),
        },
        "alice",
    ))
    .await
    .unwrap();

    let mut admin = AdminClient::connect(s.admin_endpoint.clone())
        .await
        .unwrap();
    let entries = admin
        .audit_tail(AuditTailRequest { limit: 10 })
        .await
        .unwrap()
        .into_inner()
        .entries;
    assert!(entries
        .iter()
        .any(|e| e.operation == "write_query" && e.agent_identity == "alice"));
    assert!(entries.iter().any(|e| e.operation == "allocate_pond"));
}

#[tokio::test]
async fn pond_list_reads_from_control_plane() {
    let s = start_stack().await;
    let mut data = DataClient::connect(s.data_endpoint.clone()).await.unwrap();
    data.allocate_pond(id_req(
        AllocatePondRequest {
            name: "alpha".into(),
            policy_json: String::new(),
        },
        "alice",
    ))
    .await
    .unwrap();

    let mut admin = AdminClient::connect(s.admin_endpoint.clone())
        .await
        .unwrap();
    let ponds = admin
        .pond_list(PondListRequest {})
        .await
        .unwrap()
        .into_inner()
        .ponds;
    let p = ponds
        .iter()
        .find(|p| p.name == "alpha")
        .expect("alpha listed");
    assert_eq!(p.owner, "alice");
    assert!(!p.created_at.is_empty());
}

#[tokio::test]
async fn node_list_shows_the_registered_node() {
    let s = start_stack().await;
    let mut admin = AdminClient::connect(s.admin_endpoint.clone())
        .await
        .unwrap();
    let nodes = admin
        .list_nodes(ListNodesRequest {})
        .await
        .unwrap()
        .into_inner()
        .nodes;
    assert_eq!(nodes.len(), 1);
    assert_eq!(nodes[0].node_id, "node-test");
    assert_eq!(nodes[0].state, "active");
}

#[tokio::test]
async fn policy_show_and_set_round_trip() {
    let s = start_stack().await;
    let mut admin = AdminClient::connect(s.admin_endpoint.clone())
        .await
        .unwrap();
    admin
        .policy_set(PolicySetRequest {
            key: "query_timeout_seconds".into(),
            value: "45".into(),
        })
        .await
        .unwrap();
    let p = admin
        .policy_get(PolicyGetRequest {})
        .await
        .unwrap()
        .into_inner();
    let v: serde_json::Value = serde_json::from_str(&p.policy_json).unwrap();
    assert_eq!(v["query_timeout_seconds"], "45");
}
