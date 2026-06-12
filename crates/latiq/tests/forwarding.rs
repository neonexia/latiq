//! Full-stack node-to-node forwarding: with two pond nodes behind one control
//! plane, a request sent to a node that doesn't own the pond is forwarded to the
//! owner and the result relayed back, indistinguishable from a local one. We
//! never rely on which node the registry picks — we resolve the owner, then
//! deliberately drive the *other* node.
mod common;

use common::{start_stack_n, MultiStack};
use latiq_proto::v1::control_client::ControlClient;
use latiq_proto::v1::data_client::DataClient;
use latiq_proto::v1::*;
use tonic::{Code, Request};

fn req<T>(msg: T, agent: &str) -> Request<T> {
    let mut r = Request::new(msg);
    r.metadata_mut()
        .insert("latiq-agent-id", agent.parse().unwrap());
    r
}
fn q(pond: &str, sql: &str) -> QueryRequest {
    QueryRequest {
        pond: pond.into(),
        sql: sql.into(),
    }
}
async fn client(ep: &str) -> DataClient<tonic::transport::Channel> {
    DataClient::connect(ep.to_string()).await.unwrap()
}

/// Allocate `name` via node 0, then resolve which node the control plane placed
/// it on. Returns the owner's internal endpoint.
async fn allocate_and_locate(stack: &MultiStack, name: &str) -> String {
    let mut c0 = client(&stack.nodes[0].data_endpoint).await;
    c0.allocate_pond(req(
        AllocatePondRequest {
            name: name.into(),
            policy_json: String::new(),
            tier: String::new(),
        },
        "alice",
    ))
    .await
    .unwrap();
    let mut ctl = ControlClient::connect(stack.control_endpoint.clone())
        .await
        .unwrap();
    ctl.get_pond_location(GetPondLocationRequest {
        pond_ref: name.into(),
    })
    .await
    .unwrap()
    .into_inner()
    .node_endpoint
}

fn rows(json: &str) -> serde_json::Value {
    serde_json::from_str::<serde_json::Value>(json).unwrap()["rows"].clone()
}

#[tokio::test]
async fn forwarding_read_happy() {
    let stack = start_stack_n(2).await;
    let owner = allocate_and_locate(&stack, "fwd").await;
    // Drive the NON-owner: every op below must be forwarded to `owner`.
    let mut n = client(&stack.other_than(&owner).data_endpoint).await;

    n.write_query(req(q("fwd", "CREATE TABLE t(i INTEGER)"), "alice"))
        .await
        .unwrap();
    n.write_query(req(q("fwd", "INSERT INTO t VALUES (1),(2),(3)"), "alice"))
        .await
        .unwrap();
    let r = n
        .read_query(req(q("fwd", "SELECT count(*) AS n FROM t"), "alice"))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(rows(&r.json)[0][0], 3);
}

#[tokio::test]
async fn forwarding_write_then_read_consistent() {
    let stack = start_stack_n(2).await;
    let owner = allocate_and_locate(&stack, "fwd2").await;
    let mut non_owner = client(&stack.other_than(&owner).data_endpoint).await;
    let mut owner_c = client(&owner).await;

    // Write through the non-owner (forwarded)...
    non_owner
        .write_query(req(q("fwd2", "CREATE TABLE t AS SELECT 42 AS v"), "alice"))
        .await
        .unwrap();
    // ...and read it back directly from the owner (local). Same data both ways.
    let direct = owner_c
        .read_query(req(q("fwd2", "SELECT v FROM t"), "alice"))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(rows(&direct.json)[0][0], 42);
    let forwarded = non_owner
        .read_query(req(q("fwd2", "SELECT v FROM t"), "alice"))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(rows(&forwarded.json)[0][0], 42);
}

#[tokio::test]
async fn forwarding_attribution_preserved_across_hop() {
    // A forwarded write must be attributed to the original agent (identity rides
    // the hop), visible in the owner's native DuckLake snapshots.
    let stack = start_stack_n(2).await;
    let owner = allocate_and_locate(&stack, "fwd3").await;
    let mut n = client(&stack.other_than(&owner).data_endpoint).await;

    n.write_query(req(q("fwd3", "CREATE TABLE t(i INTEGER)"), "carol"))
        .await
        .unwrap();
    let authors = n
        .read_query(req(
            q(
                "fwd3",
                "SELECT DISTINCT author FROM ducklake_snapshots('fwd3')",
            ),
            "carol",
        ))
        .await
        .unwrap()
        .into_inner();
    let r = rows(&authors.json);
    let found = r.as_array().unwrap().iter().any(|row| row[0] == "carol");
    assert!(found, "forwarded write should be attributed to carol: {r}");
}

#[tokio::test]
async fn forwarding_describe_and_drop() {
    let stack = start_stack_n(2).await;
    let owner = allocate_and_locate(&stack, "fwd4").await;
    let mut n = client(&stack.other_than(&owner).data_endpoint).await;

    n.write_query(req(q("fwd4", "CREATE TABLE t(i INTEGER)"), "alice"))
        .await
        .unwrap();
    // describe forwarded → sees the table created on the owner.
    let d = n
        .describe_pond(req(
            DescribePondRequest {
                pond: "fwd4".into(),
            },
            "alice",
        ))
        .await
        .unwrap()
        .into_inner();
    let v: serde_json::Value = serde_json::from_str(&d.json).unwrap();
    assert_eq!(v["pond"]["name"], "fwd4");

    // drop forwarded → the pond is gone from the registry.
    n.drop_pond(req(
        DropPondRequest {
            pond: "fwd4".into(),
            confirm: true,
        },
        "alice",
    ))
    .await
    .unwrap();
    let err = n
        .read_query(req(q("fwd4", "SELECT 1"), "alice"))
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::NotFound);
}

#[tokio::test]
async fn forwarding_pond_not_found_propagates() {
    // A non-existent pond has no owner to forward to: the greeter resolves it
    // against the control plane and returns NotFound directly.
    let stack = start_stack_n(2).await;
    let mut n = client(&stack.nodes[1].data_endpoint).await;
    let err = n
        .read_query(req(q("ghost", "SELECT 1"), "alice"))
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::NotFound);
}

#[tokio::test]
async fn forwarding_engine_error_propagates_across_hop() {
    // An error raised by the engine on the OWNER must survive the forward hop:
    // status_to_error rebuilds an AgentError from the peer Status, then the
    // greeter re-encodes it for the caller (envelope carried in details).
    let stack = start_stack_n(2).await;
    let owner = allocate_and_locate(&stack, "fwd6").await;
    let mut n = client(&stack.other_than(&owner).data_endpoint).await;
    let err = n
        .read_query(req(q("fwd6", "SELECT * FROM does_not_exist"), "alice"))
        .await
        .unwrap_err();
    assert!(
        !err.message().is_empty(),
        "owner's error message should cross the hop"
    );
    assert!(
        !err.details().is_empty(),
        "structured envelope should cross the hop"
    );
}
