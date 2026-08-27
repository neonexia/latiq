//! Full-stack feature tests for the operator Admin gRPC surface (control plane):
//! node list, pond list (metadata read), policy. Names prefixed by feature.
mod common;

use common::start_stack;
use latiq_proto::v1::admin_client::AdminClient;
use latiq_proto::v1::control_client::ControlClient;
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
async fn pond_list_reads_from_control_plane() {
    let s = start_stack().await;
    let mut data = DataClient::connect(s.data_endpoint.clone()).await.unwrap();
    data.allocate_pond(id_req(
        AllocatePondRequest {
            name: "alpha".into(),
            policy_json: String::new(),
            tier: String::new(),
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
async fn pond_lifecycle_description_shown_in_list() {
    // The CLI/SDK create path (Control gRPC create_pond_assignment) carries a
    // description; it surfaces on the admin pond_list metadata read.
    let s = start_stack().await;
    let mut control = ControlClient::connect(s.control_endpoint.clone())
        .await
        .unwrap();
    control
        .create_pond_assignment(CreatePondAssignmentRequest {
            name: "described".into(),
            owner_identity: "alice".into(),
            policy_json: "{}".into(),
            tier: "medium".into(),
            extensions: vec![],
            description: "nightly etl scratch".into(),
        })
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
        .find(|p| p.name == "described")
        .expect("described listed");
    assert_eq!(p.description, "nightly etl scratch");
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

// ---------------------------------------------------------------------------
// auth_admin_* — the operator Admin gRPC surface as an OAuth 2.1 resource
// server. Verification only: every authenticated operator can still do
// everything; we record WHO, we do not decide WHAT.
// ---------------------------------------------------------------------------

/// A request carrying both the claimed leaf and an `authorization` bearer token.
fn bearer_req<T>(msg: T, agent: &str, token: &str) -> Request<T> {
    let mut r = id_req(msg, agent);
    r.metadata_mut()
        .insert("authorization", format!("Bearer {token}").parse().unwrap());
    r
}

#[tokio::test]
async fn auth_admin_absent_config_keeps_relaxed_identity() {
    // Unchanged behaviour when no issuer is configured -- every existing
    // deployment and every existing test depends on this.
    let (_control, admin_endpoint) = common::start_control_plane_with_auth(None).await;
    let mut admin = AdminClient::connect(admin_endpoint).await.unwrap();
    admin
        .policy_set(id_req(
            PolicySetRequest {
                key: "query_timeout_seconds".into(),
                value: "45".into(),
            },
            "opsbot",
        ))
        .await
        .unwrap();
    // ...and with no `latiq-agent-id` at all, the fully anonymous path.
    admin.policy_get(PolicyGetRequest {}).await.unwrap();
    admin.list_nodes(ListNodesRequest {}).await.unwrap();
}

#[tokio::test]
async fn auth_admin_rejects_missing_token_when_configured() {
    let idp = latiq_auth::test_support::TestIdp::start().await;
    let (_control, admin_endpoint) =
        common::start_control_plane_with_auth(Some(idp.auth_config())).await;
    let mut admin = AdminClient::connect(admin_endpoint).await.unwrap();
    let err = admin.list_nodes(ListNodesRequest {}).await.unwrap_err();
    assert_eq!(err.code(), tonic::Code::Unauthenticated);
    let msg = err.message().to_lowercase();
    assert!(
        !msg.contains(&idp.issuer.to_lowercase()) && !msg.contains("jwks"),
        "the challenge must not leak issuers or the JWKS uri: {msg}"
    );
    // A mutating handler is guarded the same way -- the reads are not the only
    // thing on this surface, and the mutations are the ones that matter.
    let err = admin
        .policy_set(PolicySetRequest {
            key: "query_timeout_seconds".into(),
            value: "45".into(),
        })
        .await
        .unwrap_err();
    assert_eq!(err.code(), tonic::Code::Unauthenticated);
    let err = admin
        .dataset_remove(DatasetRemoveRequest {
            name: "nope".into(),
        })
        .await
        .unwrap_err();
    assert_eq!(err.code(), tonic::Code::Unauthenticated);
    let err = admin
        .catalog_remove(CatalogRemoveRequest {
            name: "nope".into(),
        })
        .await
        .unwrap_err();
    assert_eq!(err.code(), tonic::Code::Unauthenticated);
}

#[tokio::test]
async fn auth_admin_rejects_an_invalid_token_when_configured() {
    let idp = latiq_auth::test_support::TestIdp::start().await;
    let (_control, admin_endpoint) =
        common::start_control_plane_with_auth(Some(idp.auth_config())).await;
    let mut admin = AdminClient::connect(admin_endpoint).await.unwrap();
    let err = admin
        .list_nodes(bearer_req(ListNodesRequest {}, "opsbot", "not-a-jwt"))
        .await
        .unwrap_err();
    assert_eq!(err.code(), tonic::Code::Unauthenticated);
    // An expired token from the real issuer is rejected the same way.
    let expired = idp.mint("svc-ops", "latiq", &idp.issuer, -60);
    let err = admin
        .list_nodes(bearer_req(ListNodesRequest {}, "opsbot", &expired))
        .await
        .unwrap_err();
    assert_eq!(err.code(), tonic::Code::Unauthenticated);
}

#[tokio::test]
async fn auth_admin_accepts_a_valid_token() {
    let idp = latiq_auth::test_support::TestIdp::start().await;
    let (_control, admin_endpoint) =
        common::start_control_plane_with_auth(Some(idp.auth_config())).await;
    let mut admin = AdminClient::connect(admin_endpoint).await.unwrap();
    let token = idp.mint("svc-ops", "latiq", &idp.issuer, 300);

    admin
        .list_nodes(bearer_req(ListNodesRequest {}, "opsbot", &token))
        .await
        .unwrap();
    admin
        .policy_set(bearer_req(
            PolicySetRequest {
                key: "query_timeout_seconds".into(),
                value: "45".into(),
            },
            "opsbot",
            &token,
        ))
        .await
        .unwrap();
    let p = admin
        .policy_get(bearer_req(PolicyGetRequest {}, "opsbot", &token))
        .await
        .unwrap()
        .into_inner();
    let v: serde_json::Value = serde_json::from_str(&p.policy_json).unwrap();
    assert_eq!(v["query_timeout_seconds"], "45");
    admin
        .pond_list(bearer_req(PondListRequest {}, "opsbot", &token))
        .await
        .unwrap();
    admin
        .dataset_list(bearer_req(
            DatasetListRequest {
                query: String::new(),
            },
            "opsbot",
            &token,
        ))
        .await
        .unwrap();
    admin
        .catalog_list(bearer_req(
            CatalogListRequest {
                query: String::new(),
            },
            "opsbot",
            &token,
        ))
        .await
        .unwrap();
}
