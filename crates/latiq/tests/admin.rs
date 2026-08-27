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

/// An operator whose CLI is turned away needs the same discovery hint an agent
/// gets from the MCP 401: which authorization server this deployment trusts.
#[tokio::test]
async fn auth_admin_rejection_carries_the_discovery_challenge() {
    let idp = latiq_auth::test_support::TestIdp::start().await;
    let (_control, admin_endpoint) =
        common::start_control_plane_with_auth(Some(idp.auth_config())).await;
    let mut admin = AdminClient::connect(admin_endpoint).await.unwrap();

    let err = admin.list_nodes(ListNodesRequest {}).await.unwrap_err();
    let challenge = err
        .metadata()
        .get("www-authenticate")
        .expect("a rejection must advertise where to get a token")
        .to_str()
        .unwrap()
        .to_string();
    assert!(
        challenge.starts_with(r#"Bearer resource_metadata=""#),
        "got {challenge}"
    );
    assert!(
        challenge.contains("/.well-known/oauth-protected-resource"),
        "got {challenge}"
    );

    // A token that fails verification gets the same challenge.
    let err = admin
        .list_nodes(bearer_req(ListNodesRequest {}, "opsbot", "not-a-jwt"))
        .await
        .unwrap_err();
    assert_eq!(
        err.metadata()
            .get("www-authenticate")
            .map(|v| v.to_str().unwrap()),
        Some(challenge.as_str())
    );
}

/// With no verifier there is nothing to discover, and an ordinary error must not
/// start carrying an auth challenge.
#[tokio::test]
async fn auth_admin_absent_config_sends_no_challenge() {
    let (_control, admin_endpoint) = common::start_control_plane_with_auth(None).await;
    let mut admin = AdminClient::connect(admin_endpoint).await.unwrap();
    let err = admin
        .describe_node(DescribeNodeRequest {
            node_id: "nope".into(),
        })
        .await
        .unwrap_err();
    assert!(err.metadata().get("www-authenticate").is_none());
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

/// Every RPC on the Admin service. A handler that forgot `identity_of` is an
/// unattributed operator action, so the guard below enumerates the surface
/// rather than sampling it: bump this and add a `probe!` line when the proto
/// gains an RPC — the length assertion fails until you do.
const ADMIN_RPC_COUNT: usize = 12;

#[tokio::test]
async fn auth_admin_every_rpc_rejects_a_missing_token() {
    let idp = latiq_auth::test_support::TestIdp::start().await;
    let (_control, admin_endpoint) =
        common::start_control_plane_with_auth(Some(idp.auth_config())).await;
    let mut admin = AdminClient::connect(admin_endpoint).await.unwrap();

    // Default requests: identity is checked before any argument validation, so
    // an empty message still has to be rejected as unauthenticated.
    macro_rules! probe {
        ($method:ident, $req:ty) => {{
            let err = admin
                .$method(<$req>::default())
                .await
                .expect_err(concat!(stringify!($method), " must require a token"));
            assert_eq!(
                err.code(),
                tonic::Code::Unauthenticated,
                concat!(stringify!($method), " must reject a tokenless call")
            );
            stringify!($method)
        }};
    }

    let probed = vec![
        probe!(list_nodes, ListNodesRequest),
        probe!(describe_node, DescribeNodeRequest),
        probe!(policy_get, PolicyGetRequest),
        probe!(policy_set, PolicySetRequest),
        probe!(pond_list, PondListRequest),
        probe!(pond_set_tier, PondSetTierRequest),
        probe!(dataset_add, DatasetAddRequest),
        probe!(dataset_remove, DatasetRemoveRequest),
        probe!(dataset_list, DatasetListRequest),
        probe!(catalog_add, CatalogAddRequest),
        probe!(catalog_remove, CatalogRemoveRequest),
        probe!(catalog_list, CatalogListRequest),
    ];
    assert_eq!(
        probed.len(),
        ADMIN_RPC_COUNT,
        "every Admin RPC must be probed here, not a sample: {probed:?}"
    );
}

#[tokio::test]
async fn auth_admin_records_the_verified_subject_as_the_creator() {
    // `created_by` arrives as a client claim. With a verified subject in hand it
    // must not be trusted -- otherwise the durable registry row can be lied to
    // even though the access trail knows better.
    let idp = latiq_auth::test_support::TestIdp::start().await;
    let (_control, admin_endpoint) =
        common::start_control_plane_with_auth(Some(idp.auth_config())).await;
    let mut admin = AdminClient::connect(admin_endpoint).await.unwrap();
    let token = idp.mint("svc-ops", "latiq", &idp.issuer, 300);

    admin
        .dataset_add(bearer_req(
            DatasetAddRequest {
                dataset: Some(DatasetMsg {
                    name: "attributed".into(),
                    created_by: "someone-else".into(),
                    tables: vec![DatasetTableMsg {
                        table_name: "t".into(),
                        source_uri: "https://example.invalid/t.parquet".into(),
                        format: "parquet".into(),
                    }],
                    ..Default::default()
                }),
            },
            "opsbot",
            &token,
        ))
        .await
        .unwrap();
    admin
        .catalog_add(bearer_req(
            CatalogAddRequest {
                catalog: Some(CatalogMsg {
                    name: "attributed".into(),
                    r#type: "iceberg".into(),
                    created_by: "someone-else".into(),
                    ..Default::default()
                }),
            },
            "opsbot",
            &token,
        ))
        .await
        .unwrap();

    let d = admin
        .dataset_list(bearer_req(DatasetListRequest::default(), "opsbot", &token))
        .await
        .unwrap()
        .into_inner()
        .datasets;
    let added = d.iter().find(|x| x.name == "attributed").expect("listed");
    assert_eq!(
        added.created_by, "svc-ops",
        "the verified subject wins over the request's claim: {d:?}"
    );
    let c = admin
        .catalog_list(bearer_req(CatalogListRequest::default(), "opsbot", &token))
        .await
        .unwrap()
        .into_inner()
        .catalogs;
    let added = c.iter().find(|x| x.name == "attributed").expect("listed");
    assert_eq!(
        added.created_by, "svc-ops",
        "the verified subject wins over the request's claim: {c:?}"
    );
}

#[tokio::test]
async fn auth_admin_unverified_creator_falls_back_to_the_claim() {
    // With no issuer configured there is nothing to prefer, so the relaxed path
    // keeps honouring the request's own `created_by`.
    let (_control, admin_endpoint) = common::start_control_plane_with_auth(None).await;
    let mut admin = AdminClient::connect(admin_endpoint).await.unwrap();
    admin
        .dataset_add(DatasetAddRequest {
            dataset: Some(DatasetMsg {
                name: "relaxed".into(),
                created_by: "dana".into(),
                tables: vec![DatasetTableMsg {
                    table_name: "t".into(),
                    source_uri: "https://example.invalid/t.parquet".into(),
                    format: "parquet".into(),
                }],
                ..Default::default()
            }),
        })
        .await
        .unwrap();
    let d = admin
        .dataset_list(DatasetListRequest::default())
        .await
        .unwrap()
        .into_inner()
        .datasets;
    let added = d.iter().find(|x| x.name == "relaxed").expect("listed");
    assert_eq!(added.created_by, "dana", "{d:?}");
}
