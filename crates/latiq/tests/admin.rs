// Copyright 2026 Neonexia
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! The operator-facing surfaces, in ONE test binary.
//!
//! At the top level: full-stack feature tests for the operator Admin gRPC
//! surface (control plane) — node list, pond list (metadata read), policy.
//! Names prefixed by feature.
//!
//! In submodules, the operator-adjacent surfaces that used to be a binary each:
//! `catalogs` and `catalogs_iceberg` (datasets + external catalogs, registered
//! over Admin), `cli_auth` (the CLI as an OAuth client) and `sdk_auth` (the SDK
//! against an authenticated stack). Each integration binary statically links a
//! bundled DuckDB (~130-160 MB), so a new file is expensive and a new module is
//! free — see `crates/latiq/tests/CLAUDE.md` rule 5.
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
            lineage: false,
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

/// `latiq pond set-tier` (Admin `PondSetTier`) had no functional test — only
/// registry-level state, which proves the string was stored and nothing about
/// the caps ever reaching the pond. This drives the real RPC against a running
/// node and reads the settings back out of DuckDB through a normal read query,
/// so the whole seam is covered: registry write -> the node re-resolving the
/// tier -> re-opening the instance with the new caps.
///
/// It also covers the operator escape hatch: `none` is refused at allocate time
/// (see the surface tests) but IS grantable here, and the pond must then run
/// genuinely uncapped.
#[tokio::test]
async fn policy_set_tier_applies_the_new_caps_including_the_uncapped_grant() {
    let s = start_stack().await;
    let mut data = DataClient::connect(s.data_endpoint.clone()).await.unwrap();
    data.allocate_pond(id_req(
        AllocatePondRequest {
            name: "retiered".into(),
            policy_json: String::new(),
            tier: "medium".into(),
            lineage: false,
        },
        "alice",
    ))
    .await
    .unwrap();
    let mut admin = AdminClient::connect(s.admin_endpoint.clone())
        .await
        .unwrap();

    // Read a DuckDB setting through the ordinary read path, so what is asserted
    // is what a query on this pond actually runs under.
    let setting = |data: &mut DataClient<tonic::transport::Channel>, name: &'static str| {
        let mut data = data.clone();
        async move {
            let r = data
                .read_query(id_req(
                    QueryRequest {
                        pond: "retiered".into(),
                        sql: format!("SELECT current_setting('{name}')::VARCHAR AS v"),
                        timeout_ms: 0,
                    },
                    "alice",
                ))
                .await
                .unwrap()
                .into_inner();
            let v: serde_json::Value = serde_json::from_str(&r.json).unwrap();
            v["rows"][0][0].as_str().unwrap().to_string()
        }
    };

    let medium = latiq_common::PondTier::Medium.limits().unwrap();
    assert_eq!(
        setting(&mut data, "threads").await,
        medium.cores.to_string(),
        "the pond must start under its allocated tier's caps"
    );

    // Down-tier: the caps must actually change on the running node, not just in
    // the registry row.
    let x_small = latiq_common::PondTier::XSmall.limits().unwrap();
    let resp = admin
        .pond_set_tier(PondSetTierRequest {
            pond: "retiered".into(),
            tier: "x-small".into(),
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(resp.tier, "x-small");
    assert_eq!(
        setting(&mut data, "threads").await,
        x_small.cores.to_string(),
        "set-tier must reach the engine, not only the registry"
    );
    let capped_memory = setting(&mut data, "memory_limit").await;

    // The operator grant. `none` is refused at allocate time on every caller
    // surface; here it must be accepted, and the pond must then run under
    // DuckDB's own defaults rather than any tier's caps.
    admin
        .pond_set_tier(PondSetTierRequest {
            pond: "retiered".into(),
            tier: "none".into(),
        })
        .await
        .expect("an operator MUST be able to grant the uncapped tier");
    // The reference is DuckDB's own default in this process, read from a bare
    // connection that never went through a pond — pinning numbers would make
    // this depend on the host's cores and RAM.
    let bare = duckdb::Connection::open_in_memory().unwrap();
    let default = |name: &str| -> String {
        bare.query_row(
            &format!("SELECT current_setting('{name}')::VARCHAR"),
            [],
            |r| r.get(0),
        )
        .unwrap()
    };
    assert_eq!(
        setting(&mut data, "threads").await,
        default("threads"),
        "an uncapped pond must run under DuckDB's own thread default"
    );
    let uncapped_memory = setting(&mut data, "memory_limit").await;
    assert_eq!(
        uncapped_memory,
        default("memory_limit"),
        "an uncapped pond must run under DuckDB's own memory default"
    );
    // Anti-vacuity: the two assertions above would also pass if the caps had
    // never been applied at any point, so prove the capped state was observably
    // different from the uncapped one on this host.
    assert_ne!(
        capped_memory, uncapped_memory,
        "x-small's cap must be distinguishable from uncapped, or this test \
         cannot tell a granted `none` from a set-tier that did nothing"
    );

    // …and the registry agrees with what the node is running.
    let ponds = admin
        .pond_list(PondListRequest {})
        .await
        .unwrap()
        .into_inner()
        .ponds;
    let p = ponds.iter().find(|p| p.name == "retiered").unwrap();
    assert_eq!(p.tier, "none");
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
            lineage: false,
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
async fn pond_lifecycle_forget_is_refused_for_a_pond_a_live_node_serves() {
    // `pond forget` deletes a registry record and ORPHANS the data, so on a
    // running stack — where the owning node is up and `pond drop` works — it
    // has to refuse. This is the whole-surface half of the guard: the registry
    // unit tests prove the decision, this proves an operator reaching the real
    // Admin RPC gets it, with a code and guidance they can act on.
    let s = start_stack().await;
    let mut data = DataClient::connect(s.data_endpoint.clone()).await.unwrap();
    data.allocate_pond(id_req(
        AllocatePondRequest {
            name: "live-one".into(),
            policy_json: String::new(),
            tier: String::new(),
            lineage: false,
        },
        "alice",
    ))
    .await
    .unwrap();
    let mut admin = AdminClient::connect(s.admin_endpoint.clone())
        .await
        .unwrap();

    // Without confirm: refused BEFORE anything is looked at, like drop's gate.
    let unconfirmed = admin
        .pond_forget(PondForgetRequest {
            pond: "live-one".into(),
            confirm: false,
        })
        .await
        .expect_err("a destructive operator action must require confirm");
    assert_eq!(unconfirmed.code(), tonic::Code::InvalidArgument);
    assert!(
        unconfirmed.message().contains("confirm=true"),
        "the refusal must say how to proceed: {}",
        unconfirmed.message()
    );

    // With confirm: still refused, now on the guard rail — and the message must
    // send the operator to the verb that actually works here.
    let err = admin
        .pond_forget(PondForgetRequest {
            pond: "live-one".into(),
            confirm: true,
        })
        .await
        .expect_err("a pond a live node still serves must not be forgotten");
    assert_eq!(
        err.code(),
        tonic::Code::FailedPrecondition,
        "not InvalidArgument: the request is fine, the cluster state is what refuses it"
    );
    let env: latiq_common::ErrorEnvelope = serde_json::from_slice(err.details()).expect("envelope");
    assert!(
        env.suggest.contains("pond drop"),
        "must point at the verb that works: {}",
        env.suggest
    );

    // And the pond is untouched — a refusal that had already deleted the row
    // would be the worst possible outcome of this command.
    let ponds = admin
        .pond_list(PondListRequest {})
        .await
        .unwrap()
        .into_inner()
        .ponds;
    assert!(
        ponds.iter().any(|p| p.name == "live-one"),
        "the refused pond must still be registered"
    );
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

/// The Admin mirror of `query_grpc.rs::auth_rejects_every_bad_credential_on_both_surfaces`:
/// the rejection matrix on ONE control plane rather than three startups for
/// three tests. An operator whose CLI is turned away needs the same discovery
/// hint an agent gets from the MCP 401 — which authorization server this
/// deployment trusts — and needs it however the credential failed.
#[tokio::test]
async fn auth_admin_rejects_every_bad_credential_with_a_discovery_challenge() {
    let idp = latiq_auth::test_support::TestIdp::start().await;
    let (_control, admin_endpoint) =
        common::start_control_plane_with_auth(Some(idp.auth_config())).await;
    let mut admin = AdminClient::connect(admin_endpoint).await.unwrap();
    let expired = idp.mint("svc-ops", "latiq", &idp.issuer, -60);

    let credentials: [(&str, Option<&str>); 3] = [
        ("no token at all", None),
        ("a token that is not a JWT", Some("not-a-jwt")),
        (
            "an expired token from the real issuer",
            Some(expired.as_str()),
        ),
    ];

    let mut challenges: Vec<String> = Vec::new();
    for (why, token) in credentials {
        // A read and a MUTATION per row: the reads are not the only thing on
        // this surface, and the mutations are the ones that matter.
        let list = match token {
            Some(t) => bearer_req(ListNodesRequest {}, "opsbot", t),
            None => Request::new(ListNodesRequest {}),
        };
        let set = match token {
            Some(t) => bearer_req(
                PolicySetRequest {
                    key: "query_timeout_seconds".into(),
                    value: "45".into(),
                },
                "opsbot",
                t,
            ),
            None => Request::new(PolicySetRequest {
                key: "query_timeout_seconds".into(),
                value: "45".into(),
            }),
        };

        for (rpc, err) in [
            ("list_nodes", admin.list_nodes(list).await.unwrap_err()),
            ("policy_set", admin.policy_set(set).await.unwrap_err()),
        ] {
            assert_eq!(err.code(), tonic::Code::Unauthenticated, "{rpc}: {why}");

            // The rejection must not tell an unauthenticated caller which
            // issuers we trust or where their keys live.
            let msg = err.message().to_lowercase();
            assert!(
                !msg.contains(&idp.issuer.to_lowercase()) && !msg.contains("jwks"),
                "{rpc}: {why} — the rejection leaks issuers or the JWKS uri: {msg}"
            );

            let challenge = err
                .metadata()
                .get("www-authenticate")
                .unwrap_or_else(|| {
                    panic!("{rpc}: {why} — a rejection must advertise where to get a token")
                })
                .to_str()
                .unwrap()
                .to_string();
            assert!(
                challenge.starts_with(r#"Bearer resource_metadata=""#)
                    && challenge.contains("/.well-known/oauth-protected-resource"),
                "{rpc}: {why} — got {challenge}"
            );
            challenges.push(challenge);
        }
    }
    // The same challenge every time: an operator's recovery does not depend on
    // WHICH RPC they hit or HOW their credential failed.
    assert!(
        challenges.windows(2).all(|w| w[0] == w[1]),
        "the challenge must not vary by rpc or failure mode: {challenges:?}"
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
const ADMIN_RPC_COUNT: usize = 13;

#[tokio::test]
async fn auth_admin_every_rpc_rejects_a_missing_token() {
    let idp = latiq_auth::test_support::TestIdp::start().await;
    let (_control, admin_endpoint) =
        common::start_control_plane_with_auth(Some(idp.auth_config())).await;
    let mut admin = AdminClient::connect(admin_endpoint).await.unwrap();

    let issuer = idp.issuer.to_lowercase();

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
            // No RPC may tell an unauthenticated caller which issuers we trust
            // or where their keys live. Asserted HERE, across the whole surface,
            // rather than on one sampled RPC as it used to be: a leak added to a
            // single handler is exactly what a sample misses.
            let msg = err.message().to_lowercase();
            assert!(
                !msg.contains(&issuer) && !msg.contains("jwks"),
                concat!(
                    stringify!($method),
                    " leaks issuers or the JWKS uri in its rejection: {}"
                ),
                msg
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
        probe!(pond_forget, PondForgetRequest),
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

/// Dataset + external-catalog e2e over the real Admin + Data gRPC.
///
/// The catalog path runs against a **real local DuckLake catalog** (file
/// metadata + local data — no network, no docker), so it runs in the normal CI
/// suite. The iceberg/MinIO variant is `mod catalogs_iceberg` below.
mod catalogs {
    use crate::common::start_stack;
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

    /// A request carrying the claimed agent id AND a caller-minted trace, so a
    /// trace id recorded in the pond can be compared against a value this test
    /// chose — not one it read back out of the same record it is checking.
    fn traced_req<T>(msg: T, agent: &str, trace_id: &str) -> Request<T> {
        let mut r = req(msg, agent);
        r.metadata_mut().insert(
            "traceparent",
            format!("00-{trace_id}-00f067aa0ba902b7-01")
                .parse()
                .unwrap(),
        );
        r
    }

    fn json(resp: JsonResponse) -> serde_json::Value {
        serde_json::from_str(&resp.json).unwrap()
    }

    /// The `ErrorEnvelope` a Data-gRPC failure carries in its `details`.
    ///
    /// Asserted on rather than on `Status::message`, because the envelope is
    /// what an agent actually branches on — `kind`, `audience`, `retryable` and
    /// `facts` — and a test that reads only the prose cannot tell a correct
    /// kind from a plausible sentence.
    fn envelope(status: &tonic::Status) -> serde_json::Value {
        assert!(
            !status.details().is_empty(),
            "no ErrorEnvelope rode on this status, so there is nothing for an \
             agent to act on: {status:?}"
        );
        serde_json::from_slice(status.details()).expect("the details are an ErrorEnvelope")
    }

    /// Create a local DuckLake catalog with a `widgets` table, returning the
    /// ATTACH OPTIONS for it (`metadata_path` + `data_path`) — the shape
    /// `catalog attach` takes, so no caller has to rebuild the map. A throwaway
    /// in-memory DuckDB seeds it: the same engine the pond uses, no network.
    fn seed_ducklake(dir: &std::path::Path) -> HashMap<String, String> {
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
        HashMap::from([
            ("metadata_path".to_string(), meta.display().to_string()),
            ("data_path".to_string(), data.display().to_string()),
        ])
    }

    /// A SECOND, independent DuckLake catalog — `customers`, joinable to
    /// `widgets` on `id`. Two separate sources is the whole point of the
    /// headline test: one catalog would prove nothing the old transient pull
    /// could not already do.
    fn seed_customers(dir: &std::path::Path) -> HashMap<String, String> {
        let meta = dir.join("meta.duckdb");
        let data = dir.join("data");
        std::fs::create_dir_all(&data).unwrap();
        let conn = duckdb::Connection::open_in_memory().unwrap();
        conn.execute_batch(&format!(
            "INSTALL ducklake; LOAD ducklake;
             ATTACH 'ducklake:{}' AS ext (DATA_PATH '{}');
             CREATE TABLE ext.customers AS
               SELECT * FROM (VALUES (1,'gold'),(2,'silver')) t(id,segment);",
            meta.display(),
            data.display(),
        ))
        .unwrap();
        HashMap::from([
            ("metadata_path".to_string(), meta.display().to_string()),
            ("data_path".to_string(), data.display().to_string()),
        ])
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
                lineage: false,
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
        // Datasets load into a schema named after the dataset; tables are reported
        // schema-qualified (holdings.holdings).
        assert_eq!(loaded["schema"], "holdings");
        assert_eq!(loaded["tables"][0], "holdings.holdings");

        let r = json(
            data.read_query(req(
                QueryRequest {
                    pond: "work".into(),
                    sql: "SELECT count(*) AS n FROM holdings.holdings".into(),
                    timeout_ms: 0,
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

    /// **The headline, over the real Data gRPC surface.** Two external catalogs
    /// attached to ONE pond, a single `write_query` joining across both into a
    /// pond table, then detach.
    ///
    /// The old transient `catalog_pull` could not express this at all — it
    /// attached and detached inside one call, so two sources were never mounted
    /// at the same time. Everything the extract needs is ordinary SQL; nothing
    /// in this test calls a catalog-aware query verb, because there is not one.
    #[tokio::test]
    async fn catalog_attach_two_catalogs_join_in_one_write_and_detach() {
        let orders_dir = tempfile::tempdir().unwrap();
        let crm_dir = tempfile::tempdir().unwrap();
        let orders = seed_ducklake(orders_dir.path());
        let customers = seed_customers(crm_dir.path());

        let s = start_stack().await;
        let mut data = DataClient::connect(s.data_endpoint.clone()).await.unwrap();
        data.allocate_pond(req(
            AllocatePondRequest {
                name: "shop".into(),
                policy_json: String::new(),
                tier: String::new(),
                lineage: false,
            },
            "agent-x",
        ))
        .await
        .unwrap();

        // Attach both. The whole locator arrives with the call — no registry
        // lookup, and no credential at all for a local DuckLake source, which
        // the response says in as many words rather than leaving to inference.
        let first = json(
            data.catalog_attach(req(
                CatalogAttachRequest {
                    pond: "shop".into(),
                    name: "lake".into(),
                    r#type: "ducklake".into(),
                    options: orders.clone(),
                    secrets: HashMap::new(),
                    secret_ref: String::new(),
                },
                "agent-x",
            ))
            .await
            .unwrap()
            .into_inner(),
        );
        assert_eq!(first["catalog"]["name"], "lake");
        assert_eq!(
            first["credential_mode"], "none",
            "a local ducklake authenticates to nothing, and the response must SAY \
             no credential was applied rather than let the caller assume one was: {first}"
        );
        data.catalog_attach(req(
            CatalogAttachRequest {
                pond: "shop".into(),
                name: "crm".into(),
                r#type: "ducklake".into(),
                options: customers,
                secrets: HashMap::new(),
                secret_ref: String::new(),
            },
            "agent-x",
        ))
        .await
        .expect("a SECOND catalog attaches while the first is still mounted");

        // Both are listed, with their locators — and nothing else. The
        // credential containment test below is what proves "nothing else".
        let attached = json(
            data.catalog_list_attached(req(
                CatalogListAttachedRequest {
                    pond: "shop".into(),
                },
                "agent-x",
            ))
            .await
            .unwrap()
            .into_inner(),
        );
        let names: Vec<&str> = attached["catalogs"]
            .as_array()
            .unwrap()
            .iter()
            .filter_map(|c| c["name"].as_str())
            .collect();
        assert_eq!(names, vec!["lake", "crm"], "attached: {attached}");

        // THE extract: one ordinary write, joining two external catalogs.
        const TRACE: &str = "4bf92f3577b34da6a3ce929d0e0e4736";
        data.write_query(traced_req(
            QueryRequest {
                pond: "shop".into(),
                sql: "CREATE TABLE enriched AS \
                      SELECT o.id, o.name AS item, c.segment \
                      FROM lake.main.widgets o JOIN crm.main.customers c ON c.id = o.id"
                    .into(),
                timeout_ms: 0,
            },
            "puller-ann",
            TRACE,
        ))
        .await
        .expect("a write joining two attached catalogs must succeed");

        let r = json(
            data.read_query(req(
                QueryRequest {
                    pond: "shop".into(),
                    sql: "SELECT count(*) AS n FROM enriched".into(),
                    timeout_ms: 0,
                },
                "agent-x",
            ))
            .await
            .unwrap()
            .into_inner(),
        );
        assert_eq!(
            r["rows"][0][0].as_i64().unwrap(),
            2,
            "the join must produce the two matching rows: {r}"
        );

        // **Attribution.** The extract is a write, so the pond's own history
        // must name who did it, what the operation was, and under which trace —
        // through the one bracket every pond-writing path shares. Values, never
        // "not null": the string `unknown` would satisfy a null check and tell
        // nobody anything. The trace id is the one this test minted and sent in
        // the `traceparent` header, so it is compared against a value produced
        // independently of the record being checked.
        let attr = json(
            data.read_query(req(
                QueryRequest {
                    pond: "shop".into(),
                    sql: "SELECT author, commit_message, commit_extra_info \
                          FROM ducklake_snapshots('shop') ORDER BY snapshot_id DESC LIMIT 1"
                        .into(),
                    timeout_ms: 0,
                },
                "viewer",
            ))
            .await
            .unwrap()
            .into_inner(),
        );
        let row = &attr["rows"][0];
        assert_eq!(
            (row[0].as_str(), row[1].as_str()),
            (Some("puller-ann"), Some("write_query")),
            "an extract across two lakehouses is attributed like any other write: {attr}"
        );
        let extra: serde_json::Value =
            serde_json::from_str(row[2].as_str().expect("commit_extra_info")).unwrap();
        assert_eq!(
            extra["trace_id"],
            serde_json::json!(TRACE),
            "the snapshot must carry the trace the caller sent, or it cannot be \
             joined to that request's lineage: {extra}"
        );
        assert_eq!(extra["agent_id"], serde_json::json!("puller-ann"));
        assert_eq!(extra["verified"], serde_json::json!(false));

        // Detach both, and the pond table survives.
        for name in ["lake", "crm"] {
            data.catalog_detach(req(
                CatalogDetachRequest {
                    pond: "shop".into(),
                    name: name.into(),
                },
                "agent-x",
            ))
            .await
            .unwrap_or_else(|e| panic!("detach {name}: {e}"));
        }
        let after = json(
            data.read_query(req(
                QueryRequest {
                    pond: "shop".into(),
                    sql: "SELECT count(*) AS n FROM enriched".into(),
                    timeout_ms: 0,
                },
                "agent-x",
            ))
            .await
            .expect("detaching must not disturb the pond")
            .into_inner(),
        );
        assert_eq!(after["rows"][0][0].as_i64().unwrap(), 2);
    }

    /// A statement naming a catalog nobody attached must hand an agent an
    /// envelope it can ACT on: the kind, the alias as a value, and a `suggest`
    /// naming `attach_catalog` — not `catalog_error`'s canonical "run SHOW
    /// TABLES", which lists nothing called `lake`.
    ///
    /// Driven by a REAL detach, not by a fabricated error: the condition this
    /// error exists for is an attachment that is genuinely gone (a node restart
    /// produces the same thing), and a kind nobody can reach is not a feature.
    #[tokio::test]
    async fn error_contract_a_statement_naming_a_detached_catalog_says_to_attach_it() {
        let tmp = tempfile::tempdir().unwrap();
        let orders = seed_ducklake(tmp.path());
        let s = start_stack().await;
        let mut data = DataClient::connect(s.data_endpoint.clone()).await.unwrap();
        data.allocate_pond(req(
            AllocatePondRequest {
                name: "shop".into(),
                policy_json: String::new(),
                tier: String::new(),
                lineage: false,
            },
            "agent-x",
        ))
        .await
        .unwrap();
        data.catalog_attach(req(
            CatalogAttachRequest {
                pond: "shop".into(),
                name: "lake".into(),
                r#type: "ducklake".into(),
                options: orders,
                secrets: HashMap::new(),
                secret_ref: String::new(),
            },
            "agent-x",
        ))
        .await
        .unwrap();
        // It resolves while attached — so the failure below can only be the
        // detach, and not a table name that was never right.
        data.read_query(req(
            QueryRequest {
                pond: "shop".into(),
                sql: "SELECT count(*) FROM lake.main.widgets".into(),
                timeout_ms: 0,
            },
            "agent-x",
        ))
        .await
        .expect("the attached catalog resolves");

        data.catalog_detach(req(
            CatalogDetachRequest {
                pond: "shop".into(),
                name: "lake".into(),
            },
            "agent-x",
        ))
        .await
        .unwrap();

        let err = data
            .read_query(req(
                QueryRequest {
                    pond: "shop".into(),
                    sql: "SELECT count(*) FROM lake.main.widgets".into(),
                    timeout_ms: 0,
                },
                "agent-x",
            ))
            .await
            .expect_err("a detached catalog must stop resolving");
        let env = envelope(&err);
        assert_eq!(env["kind"], "catalog_error", "envelope: {env}");
        assert_eq!(env["audience"], "agent", "the caller can fix this: {env}");
        assert_eq!(
            env["retryable"], "after_change",
            "re-sending the identical statement cannot work until the catalog is \
             back, so `as_is` would be the retry loop `retryable` prevents: {env}"
        );
        assert_eq!(
            env["facts"]["catalog"], "lake",
            "the alias must ride as a VALUE, not only inside the sentence: {env}"
        );
        assert!(
            env["suggest"]
                .as_str()
                .is_some_and(|s| s.contains("attach_catalog")),
            "the suggest must name the call that fixes it: {env}"
        );
        assert!(
            env["suggest"]
                .as_str()
                .is_some_and(|s| s.contains("list_attached_catalogs")),
            "…and the call that answers 'what IS attached': {env}"
        );

        // Re-attaching is the fix the suggest names, and it works.
        let re = seed_ducklake(tempfile::tempdir().unwrap().keep().as_path());
        data.catalog_attach(req(
            CatalogAttachRequest {
                pond: "shop".into(),
                name: "lake".into(),
                r#type: "ducklake".into(),
                options: re,
                secrets: HashMap::new(),
                secret_ref: String::new(),
            },
            "agent-x",
        ))
        .await
        .expect("the advice must be advice that works");
        data.read_query(req(
            QueryRequest {
                pond: "shop".into(),
                sql: "SELECT count(*) FROM lake.main.widgets".into(),
                timeout_ms: 0,
            },
            "agent-x",
        ))
        .await
        .expect("and the original statement then runs unchanged");
    }

    /// Attaching twice over one alias is a `name_conflict` whose advice is the
    /// two calls that resolve it — NOT `name_conflict`'s canonical "omit the
    /// name and let Latiq generate one", which is about pond names and is
    /// impossible here: the alias is the SQL namespace the caller has to type.
    #[tokio::test]
    async fn error_contract_attaching_over_a_live_alias_names_detach_catalog() {
        let tmp = tempfile::tempdir().unwrap();
        let orders = seed_ducklake(tmp.path());
        let s = start_stack().await;
        let mut data = DataClient::connect(s.data_endpoint.clone()).await.unwrap();
        data.allocate_pond(req(
            AllocatePondRequest {
                name: "shop".into(),
                policy_json: String::new(),
                tier: String::new(),
                lineage: false,
            },
            "agent-x",
        ))
        .await
        .unwrap();
        let attach = |options: HashMap<String, String>| CatalogAttachRequest {
            pond: "shop".into(),
            name: "lake".into(),
            r#type: "ducklake".into(),
            options,
            secrets: HashMap::new(),
            secret_ref: String::new(),
        };
        data.catalog_attach(req(attach(orders.clone()), "agent-x"))
            .await
            .unwrap();
        let err = data
            .catalog_attach(req(attach(orders), "agent-x"))
            .await
            .expect_err("a second attach of a live alias must be refused");
        let env = envelope(&err);
        assert_eq!(env["kind"], "name_conflict", "envelope: {env}");
        assert_eq!(env["facts"]["catalog"], "lake");
        let suggest = env["suggest"].as_str().unwrap_or_default();
        assert!(
            suggest.contains("detach_catalog"),
            "the suggest must name the call that frees the alias: {env}"
        );
        assert!(
            !suggest.contains("omit"),
            "there is no generated catalog alias — the caller has to type it in \
             every statement — so the pond-name advice must not leak in: {env}"
        );
    }

    /// **Credential containment, over the whole stack.**
    ///
    /// A wrong credential fails the attach for a real reason, and the value must
    /// then appear in NO log line, NO error envelope, and be returned by NO
    /// surface. Every one of those is a separate escape route: `tracing` renders
    /// `Debug`, the envelope serializes, and `list_attached_catalogs`
    /// round-trips the attachment.
    ///
    /// (The engine-seam half — that the DuckDB statement text and the engine's
    /// own error do not carry it — is in `latiq-engine-duckdb`'s
    /// `catalog_attach_a_failed_attach_leaks_neither_the_credential_nor_the_secret`.)
    #[tokio::test]
    async fn catalog_attach_a_rejected_credential_appears_on_no_surface() {
        const CREDENTIAL: &str = "totally-secret-value-9f3a";
        let s = start_stack().await;
        let mut data = DataClient::connect(s.data_endpoint.clone()).await.unwrap();
        data.allocate_pond(req(
            AllocatePondRequest {
                name: "shop".into(),
                policy_json: String::new(),
                tier: String::new(),
                lineage: true,
            },
            "agent-x",
        ))
        .await
        .unwrap();

        // A real failure with a real credential in the plan: the metadata path
        // is under a directory that does not exist, so the ATTACH fails AFTER
        // the `CREATE SECRET` has run.
        let err = data
            .catalog_attach(req(
                CatalogAttachRequest {
                    pond: "shop".into(),
                    name: "lake".into(),
                    r#type: "ducklake".into(),
                    options: HashMap::from([
                        (
                            "metadata_path".into(),
                            "/nonexistent_dir_xyz/meta.duckdb".into(),
                        ),
                        ("data_path".into(), "/nonexistent_dir_xyz/data".into()),
                    ]),
                    secrets: HashMap::from([
                        ("s3_access_key".into(), "AKIAEXAMPLE".into()),
                        ("s3_secret_key".into(), CREDENTIAL.into()),
                    ]),
                    secret_ref: String::new(),
                },
                "agent-x",
            ))
            .await
            .expect_err("an attach under a non-existent directory must fail");

        // 1. Not in the envelope — message, suggest, facts, anywhere.
        let env = envelope(&err);
        assert_eq!(
            env["kind"], "source_unavailable",
            "the address is the caller's, so this is not our failure: {env}"
        );
        let rendered = env.to_string();
        assert!(
            !rendered.contains(CREDENTIAL),
            "the credential reached the error envelope: {rendered}"
        );
        // 2. …nor in the raw Status, which is what a non-envelope-aware client
        // prints.
        assert!(
            !format!("{err:?}").contains(CREDENTIAL),
            "the credential reached the gRPC status: {err:?}"
        );

        // 3. Not returned by any surface. Nothing attached, so there is nothing
        // to list — which is itself the assertion that the failed attach did not
        // half-register.
        let attached = json(
            data.catalog_list_attached(req(
                CatalogListAttachedRequest {
                    pond: "shop".into(),
                },
                "agent-x",
            ))
            .await
            .unwrap()
            .into_inner(),
        );
        assert_eq!(
            attached["catalogs"].as_array().map(|a| a.len()),
            Some(0),
            "a failed attach must leave nothing mounted: {attached}"
        );

        // 4. Now a SUCCESSFUL attach carrying the same credential, so the
        // containment is proved on the path where the value is actually held
        // for the life of the attachment — the list must still not carry it.
        let tmp = tempfile::tempdir().unwrap();
        data.catalog_attach(req(
            CatalogAttachRequest {
                pond: "shop".into(),
                name: "lake".into(),
                r#type: "ducklake".into(),
                options: seed_ducklake(tmp.path()),
                secrets: HashMap::from([
                    ("s3_access_key".into(), "AKIAEXAMPLE".into()),
                    ("s3_secret_key".into(), CREDENTIAL.into()),
                ]),
                secret_ref: String::new(),
            },
            "agent-x",
        ))
        .await
        .expect("a local ducklake attaches even with storage credentials supplied");
        let attached = json(
            data.catalog_list_attached(req(
                CatalogListAttachedRequest {
                    pond: "shop".into(),
                },
                "agent-x",
            ))
            .await
            .unwrap()
            .into_inner(),
        );
        assert_eq!(attached["catalogs"][0]["name"], "lake");
        assert_eq!(
            attached["credential_mode"],
            serde_json::Value::Null,
            "the list must not even have a credential-shaped field: {attached}"
        );
        assert!(
            !attached.to_string().contains(CREDENTIAL),
            "the credential is readable through list_attached_catalogs: {attached}"
        );

        // 5. …and the lineage the pond recorded for this work carries none of
        // it either. (This pond opted in, so there is a trail to check; a pond
        // without one would make this assertion vacuous.)
        let page = json(
            data.get_lineage(req(
                GetLineageRequest {
                    pond: "shop".into(),
                    limit: 50,
                    since: String::new(),
                    before: String::new(),
                },
                "agent-x",
            ))
            .await
            .unwrap()
            .into_inner(),
        );
        assert!(
            !page.to_string().contains(CREDENTIAL),
            "the credential reached the pond's lineage trail: {page}"
        );
    }

    /// The `--option` / `--secret` split is the security boundary of this
    /// surface, and it is enforced BEFORE anything reaches the engine: an option
    /// is echoed back by `list_attached_catalogs`, so a credential that arrived
    /// there would be readable through a surface.
    #[tokio::test]
    async fn error_contract_a_credential_passed_as_an_option_is_refused_naming_secrets() {
        let s = start_stack().await;
        let mut data = DataClient::connect(s.data_endpoint.clone()).await.unwrap();
        data.allocate_pond(req(
            AllocatePondRequest {
                name: "shop".into(),
                policy_json: String::new(),
                tier: String::new(),
                lineage: false,
            },
            "agent-x",
        ))
        .await
        .unwrap();

        let err = data
            .catalog_attach(req(
                CatalogAttachRequest {
                    pond: "shop".into(),
                    name: "lake".into(),
                    r#type: "iceberg".into(),
                    options: HashMap::from([
                        ("endpoint".into(), "https://polaris/api".into()),
                        ("token".into(), "SECRET".into()),
                    ]),
                    secrets: HashMap::new(),
                    secret_ref: String::new(),
                },
                "agent-x",
            ))
            .await
            .expect_err("a credential passed as an option must be refused");
        let env = envelope(&err);
        assert_eq!(env["kind"], "invalid_value", "envelope: {env}");
        let message = env["message"].as_str().unwrap_or_default();
        assert!(
            message.contains("token") && message.contains("secret"),
            "the refusal must name the key AND the field it belongs on: {env}"
        );

        // Its sibling: an option this type does not know is refused, not
        // dropped. A dropped typo is a locator the caller believes it set.
        let err = data
            .catalog_attach(req(
                CatalogAttachRequest {
                    pond: "shop".into(),
                    name: "lake".into(),
                    r#type: "iceberg".into(),
                    options: HashMap::from([("endpont".into(), "https://polaris/api".into())]),
                    secrets: HashMap::new(),
                    secret_ref: String::new(),
                },
                "agent-x",
            ))
            .await
            .expect_err("a typo'd option must be refused, never dropped");
        let env = envelope(&err);
        assert_eq!(env["kind"], "invalid_value");
        let message = env["message"].as_str().unwrap_or_default();
        assert!(
            message.contains("endpont") && message.contains("endpoint"),
            "the refusal must name the typo and the legal set: {env}"
        );
    }

    /// The three credential modes are mutually exclusive, and a `secret_ref`
    /// scheme this deployment cannot serve is `capability_unavailable` /
    /// `after_provisioning` — the kind that tells an agent its call was RIGHT
    /// and to escalate rather than loop.
    #[tokio::test]
    async fn error_contract_a_secret_ref_this_node_cannot_serve_escalates() {
        let s = start_stack().await;
        let mut data = DataClient::connect(s.data_endpoint.clone()).await.unwrap();
        data.allocate_pond(req(
            AllocatePondRequest {
                name: "shop".into(),
                policy_json: String::new(),
                tier: String::new(),
                lineage: false,
            },
            "agent-x",
        ))
        .await
        .unwrap();
        let attach = |secrets: HashMap<String, String>, secret_ref: &str| CatalogAttachRequest {
            pond: "shop".into(),
            name: "lake".into(),
            r#type: "iceberg".into(),
            options: HashMap::from([("endpoint".into(), "https://polaris/api".into())]),
            secrets,
            secret_ref: secret_ref.into(),
        };

        // Two modes at once: refused, naming the legal shapes.
        let err = data
            .catalog_attach(req(
                attach(HashMap::from([("token".into(), "t".into())]), "env://lake"),
                "agent-x",
            ))
            .await
            .expect_err("two credential modes at once must be refused");
        let env = envelope(&err);
        assert_eq!(env["kind"], "invalid_value", "envelope: {env}");
        for shape in ["secrets", "secret_ref", "passthrough"] {
            assert!(
                env["message"].as_str().is_some_and(|m| m.contains(shape)),
                "the refusal must name every legal shape, and '{shape}' is missing: {env}"
            );
        }

        // A scheme with no backend: the call was correct, the deployment is
        // missing a piece, and an agent must escalate rather than retry.
        let err = data
            .catalog_attach(req(attach(HashMap::new(), "vault://team/lake"), "agent-x"))
            .await
            .expect_err("no vault backend is configured on this node");
        let env = envelope(&err);
        assert_eq!(env["kind"], "capability_unavailable", "envelope: {env}");
        assert_eq!(env["audience"], "operator");
        assert_eq!(env["retryable"], "after_provisioning");
        assert_eq!(
            env["facts"]["capability"], "vault://",
            "the missing capability must be a value a client can branch on: {env}"
        );
    }

    /// **`passthrough`: the caller's OWN bearer becomes the catalog credential.**
    ///
    /// The mode that stores nothing anywhere, and the one Iceberg REST, Unity
    /// Catalog and Snowflake External OAuth actually want — so it is the one worth
    /// proving end to end rather than at a seam.
    ///
    /// The bearer here is a REAL one: minted by a real IdP, verified by the node on
    /// the way in, and never written down by this test in any other form. What is
    /// asserted is the byte sequence a real HTTP server on the other side of DuckDB
    /// received — so nothing in between (the adapter's mapping of "no secrets
    /// supplied", the resolver's choice of key, the attacher's `CREATE SECRET`, and
    /// DuckDB's own REST client) can be right in isolation and wrong together.
    ///
    /// The stand-in catalog answers 404, so the attach fails — deliberately. A
    /// server that answered correctly would need to be an Iceberg REST catalog; the
    /// question here is what was SENT, and that is settled before the status code.
    /// The failure is also asserted, because a request that never happened would
    /// leave `seen` empty and every other check vacuous.
    #[tokio::test(flavor = "multi_thread")]
    async fn catalog_attach_passthrough_sends_the_callers_own_bearer_to_the_catalog() {
        use std::io::{Read, Write};
        use std::sync::{Arc, Mutex};

        // A stand-in REST catalog that records what it was asked, on a real socket.
        let seen: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let catalog_addr = listener.local_addr().unwrap();
        let recorder = seen.clone();
        std::thread::spawn(move || {
            for stream in listener.incoming().take(4) {
                let Ok(mut stream) = stream else { break };
                let mut buf = [0u8; 8192];
                let n = stream.read(&mut buf).unwrap_or(0);
                recorder
                    .lock()
                    .unwrap()
                    .push(String::from_utf8_lossy(&buf[..n]).to_string());
                let _ = stream.write_all(b"HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\n\r\n");
            }
        });

        let idp = latiq_auth::test_support::TestIdp::start().await;
        let token = idp.mint("svc-extractor", "latiq", &idp.issuer, 300);
        let s = crate::common::start_stack_with_auth(idp.auth_config()).await;
        let mut data = DataClient::connect(s.data_endpoint.clone()).await.unwrap();

        /// Every call on an authenticated stack carries the bearer — including the
        /// allocate, which is what makes this the caller's real, verified token
        /// rather than a header this test happened to set on one request.
        fn authed<T>(msg: T, token: &str) -> Request<T> {
            let mut r = Request::new(msg);
            r.metadata_mut()
                .insert("latiq-agent-id", "svc-extractor".parse().unwrap());
            r.metadata_mut()
                .insert("authorization", format!("Bearer {token}").parse().unwrap());
            r
        }

        data.allocate_pond(authed(
            AllocatePondRequest {
                name: "shop".into(),
                policy_json: String::new(),
                tier: String::new(),
                lineage: false,
            },
            &token,
        ))
        .await
        .unwrap();

        // NEITHER `secrets` NOR `secret_ref`: that is the caller selecting
        // passthrough, not an omission.
        let err = data
            .catalog_attach(authed(
                CatalogAttachRequest {
                    pond: "shop".into(),
                    name: "lake".into(),
                    r#type: "iceberg".into(),
                    options: HashMap::from([
                        ("endpoint".into(), format!("http://{catalog_addr}")),
                        ("warehouse".into(), "demo".into()),
                    ]),
                    secrets: HashMap::new(),
                    secret_ref: String::new(),
                },
                &token,
            ))
            .await
            .expect_err("the stand-in catalog answers 404, so the attach fails");
        let env = envelope(&err);
        assert_eq!(
            env["kind"], "source_unavailable",
            "a catalog that answers wrongly is the caller's address to fix: {env}"
        );

        // THE assertion: the request DuckDB made to the external catalog carried the
        // caller's own token as its credential.
        let requests = seen.lock().unwrap().clone();
        let config_request = requests
            .iter()
            .find(|r| r.contains("/v1/config"))
            .unwrap_or_else(|| {
                panic!("the catalog was never contacted, so nothing below is proved: {requests:?}")
            });
        assert!(
            config_request.contains(&format!("Authorization: Bearer {token}")),
            "the caller's own bearer must be what authenticates to the catalog — this \
             is the whole of `passthrough`, and it stores nothing anywhere. The \
             catalog received:\n{config_request}"
        );
        // …and nothing else did. A node that substituted a token of its own (a
        // service account, a cached one) would still send SOMETHING here.
        assert_eq!(
            config_request.matches("Authorization:").count(),
            1,
            "exactly one credential may be presented: {config_request}"
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

        // Unknown type is rejected at add -- and rejected FOR THAT REASON. A bare
        // `is_err()` is equally satisfied by a complaint about the empty `params`
        // (no `endpoint`), which would leave the type allowlist untested.
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
            .await
            .expect_err("unknown catalog type must be rejected");
        let msg = err.message().to_lowercase();
        assert!(
            msg.contains("catalog type") && msg.contains("snowflake"),
            "the rejection must name the unsupported type: {msg}"
        );
    }
}

/// Iceberg + MinIO end-to-end for the catalog attach path. `#[ignore]`d because
/// it needs a live Iceberg REST catalog + S3 (MinIO) — bring them up with
/// `deploy/iceberg-minio/up.sh`, then run with `--ignored`. Config comes from env
/// (set by the harness / CI):
///
///   LATIQ_ICEBERG_ENDPOINT  LATIQ_ICEBERG_WAREHOUSE  LATIQ_ICEBERG_TOKEN
///   LATIQ_S3_ENDPOINT  LATIQ_S3_ACCESS_KEY  LATIQ_S3_SECRET_KEY
mod catalogs_iceberg {
    use crate::common::start_stack;
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

    /// A local DuckLake catalog to JOIN the iceberg one against — the cheapest
    /// possible SECOND source, seeded with rows that match the fixture's
    /// `demo.widgets` on `id`.
    fn seed_local(dir: &std::path::Path) -> HashMap<String, String> {
        let meta = dir.join("meta.duckdb");
        let data = dir.join("data");
        std::fs::create_dir_all(&data).unwrap();
        let conn = duckdb::Connection::open_in_memory().unwrap();
        conn.execute_batch(&format!(
            "INSTALL ducklake; LOAD ducklake;
             ATTACH 'ducklake:{}' AS ext (DATA_PATH '{}');
             CREATE TABLE ext.tiers AS
               SELECT * FROM (VALUES (1,'gold'),(2,'silver'),(3,'bronze')) t(id,tier);",
            meta.display(),
            data.display(),
        ))
        .unwrap();
        HashMap::from([
            ("metadata_path".to_string(), meta.display().to_string()),
            ("data_path".to_string(), data.display().to_string()),
        ])
    }

    /// **The headline use case against a REAL lakehouse.**
    ///
    /// An Iceberg REST catalog behind MinIO and a local DuckLake catalog are
    /// attached to one pond at the same time, and a single ordinary
    /// `write_query` joins across both into a pond table. Then both are
    /// detached and the pond table is still there.
    ///
    /// The in-suite `mod catalogs` proves the same shape over two local
    /// DuckLake sources, which is cheap and runs everywhere; this one proves it
    /// against a real REST catalog with real S3 credentials — the attach path
    /// that has an `iceberg` secret, an S3 secret and a network in it.
    #[tokio::test]
    #[ignore = "needs a live Iceberg REST + MinIO; see deploy/iceberg-minio/up.sh"]
    async fn iceberg_attach_and_join_a_second_catalog_into_a_pond() {
        // Locators (echoed back by list_attached_catalogs) and CREDENTIALS
        // (never echoed by anything) are separate fields, not one bag.
        let options = HashMap::from([
            ("endpoint".to_string(), env("LATIQ_ICEBERG_ENDPOINT")),
            ("warehouse".to_string(), env("LATIQ_ICEBERG_WAREHOUSE")),
            ("s3_endpoint".to_string(), env("LATIQ_S3_ENDPOINT")),
            ("s3_region".to_string(), "us-east-1".to_string()),
        ]);
        let secrets = HashMap::from([
            ("token".to_string(), env("LATIQ_ICEBERG_TOKEN")),
            ("s3_access_key".to_string(), env("LATIQ_S3_ACCESS_KEY")),
            ("s3_secret_key".to_string(), env("LATIQ_S3_SECRET_KEY")),
        ]);

        let s = start_stack().await;
        let mut data = DataClient::connect(s.data_endpoint.clone()).await.unwrap();
        data.allocate_pond(req(
            AllocatePondRequest {
                name: "shop".into(),
                policy_json: String::new(),
                tier: String::new(),
                lineage: false,
            },
            "agent-x",
        ))
        .await
        .unwrap();

        let attached = json(
            data.catalog_attach(req(
                CatalogAttachRequest {
                    pond: "shop".into(),
                    name: "lake".into(),
                    r#type: "iceberg".into(),
                    options,
                    secrets: secrets.clone(),
                    secret_ref: String::new(),
                },
                "agent-x",
            ))
            .await
            .expect("the iceberg catalog must attach")
            .into_inner(),
        );
        assert_eq!(
            attached["credential_mode"], "explicit",
            "the explicit token was supplied, and the response must say it was \
             the one applied — `none` here would mean an unauthenticated attach \
             that happened to work: {attached}"
        );
        assert!(
            !attached.to_string().contains(&secrets["s3_secret_key"]),
            "the attach response must not echo a credential: {attached}"
        );

        // The second source, mounted at the same time. This is what the
        // transient pull could never do.
        let tmp = tempfile::tempdir().unwrap();
        data.catalog_attach(req(
            CatalogAttachRequest {
                pond: "shop".into(),
                name: "local".into(),
                r#type: "ducklake".into(),
                options: seed_local(tmp.path()),
                secrets: HashMap::new(),
                secret_ref: String::new(),
            },
            "agent-x",
        ))
        .await
        .expect("a local ducklake attaches alongside the iceberg one");

        // Orientation is ordinary SQL now — no describe_catalog tool.
        let tables = json(
            data.read_query(req(
                QueryRequest {
                    pond: "shop".into(),
                    sql: "SELECT table_name FROM information_schema.tables \
                          WHERE table_catalog = 'lake'"
                        .into(),
                    timeout_ms: 0,
                },
                "agent-x",
            ))
            .await
            .unwrap()
            .into_inner(),
        );
        let names: Vec<&str> = tables["rows"]
            .as_array()
            .unwrap()
            .iter()
            .filter_map(|r| r[0].as_str())
            .collect();
        assert!(names.contains(&"widgets"), "iceberg tables: {names:?}");

        // ONE write, joining the lakehouse to the local catalog.
        data.write_query(req(
            QueryRequest {
                pond: "shop".into(),
                sql: "CREATE TABLE cheap AS \
                      SELECT w.id, w.name, t.tier \
                      FROM lake.demo.widgets w JOIN local.main.tiers t ON t.id = w.id \
                      WHERE w.price < 10"
                    .into(),
                timeout_ms: 0,
            },
            "agent-x",
        ))
        .await
        .expect("a write joining iceberg and ducklake must succeed");

        let r = json(
            data.read_query(req(
                QueryRequest {
                    pond: "shop".into(),
                    sql: "SELECT count(*) AS n FROM cheap".into(),
                    timeout_ms: 0,
                },
                "agent-x",
            ))
            .await
            .unwrap()
            .into_inner(),
        );
        assert_eq!(r["rows"][0][0].as_i64().unwrap(), 2, "joined rows: {r}");

        // The extract is a write, so the pond's history must say who did it and
        // how — against a REAL external catalog, not only the local fixture the
        // in-suite test uses. The values, not non-null: `"unknown"` would pass a
        // null check and tell nobody anything.
        let attr = json(
            data.read_query(req(
                QueryRequest {
                    pond: "shop".into(),
                    sql: "SELECT author, commit_message FROM ducklake_snapshots('shop') \
                          ORDER BY snapshot_id DESC LIMIT 1"
                        .into(),
                    timeout_ms: 0,
                },
                "agent-x",
            ))
            .await
            .unwrap()
            .into_inner(),
        );
        assert_eq!(
            (attr["rows"][0][0].as_str(), attr["rows"][0][1].as_str()),
            (Some("agent-x"), Some("write_query")),
            "an iceberg extract is attributed like any other write: {attr}"
        );

        // Detach both; the pond table survives and the aliases stop resolving.
        for name in ["lake", "local"] {
            data.catalog_detach(req(
                CatalogDetachRequest {
                    pond: "shop".into(),
                    name: name.into(),
                },
                "agent-x",
            ))
            .await
            .unwrap_or_else(|e| panic!("detach {name}: {e}"));
        }
        let after = json(
            data.read_query(req(
                QueryRequest {
                    pond: "shop".into(),
                    sql: "SELECT count(*) AS n FROM cheap".into(),
                    timeout_ms: 0,
                },
                "agent-x",
            ))
            .await
            .expect("the extracted pond table survives the detach")
            .into_inner(),
        );
        assert_eq!(after["rows"][0][0].as_i64().unwrap(), 2);
    }

    /// **An iceberg `endpoint` that is not a REST catalog is the caller's to
    /// fix, and must never come back as `internal` + "retry".**
    ///
    /// Regression pin, and the only place `Invalid Configuration Error` is
    /// driven anywhere in the suite. A wrong catalog URL is the single most
    /// likely mistake in an `attach_catalog` call, and DuckDB's iceberg
    /// extension reports it as `Invalid Configuration Error: Request to
    /// 'http://…/v1/config?warehouse=…' returned a non-200 status code` — a
    /// class `errclass` did not key on, so it fell into `EngineError::Engine` →
    /// `internal` → `audience: operator`, `retryable: as_is`, "Retry; if it
    /// persists, report to your operator". An agent re-sending a typo for ever
    /// and then waking an operator who has nothing to fix: Nexus finding 8's
    /// shape, at a third site.
    ///
    /// It has to be driven against a real HTTP server, and that is why it lives
    /// here rather than in `engine_e2e.rs`'s offline class table: nothing
    /// offline raises this class (measured — a bad `SET` gives `Parser Error`,
    /// `Catalog Error` or `Invalid Input Error`). A fabricated `EngineError`
    /// would assert our mapping and stay green while the production path
    /// produced something else, which is exactly how this one survived.
    ///
    /// Note what is deliberately NOT tested here: a WRONG WAREHOUSE. Measured
    /// against this fixture, `warehouse: "no-such-warehouse"` attaches happily
    /// and its queries return the real `demo.widgets` rows — the REST catalog
    /// serves one warehouse and ignores the parameter — so a test asserting a
    /// failure there would be asserting a condition that does not exist.
    #[tokio::test]
    #[ignore = "needs a live Iceberg REST + MinIO; see deploy/iceberg-minio/up.sh"]
    async fn error_contract_an_iceberg_endpoint_that_is_not_a_catalog_is_not_our_failure() {
        const WRONG: &str = "wrong-key-8c21";
        let s = start_stack().await;
        let mut data = DataClient::connect(s.data_endpoint.clone()).await.unwrap();
        data.allocate_pond(req(
            AllocatePondRequest {
                name: "shop".into(),
                policy_json: String::new(),
                tier: String::new(),
                lineage: false,
            },
            "agent-x",
        ))
        .await
        .unwrap();

        let err = data
            .catalog_attach(req(
                CatalogAttachRequest {
                    pond: "shop".into(),
                    name: "lake".into(),
                    r#type: "iceberg".into(),
                    options: HashMap::from([
                        // The STORAGE endpoint, not the catalog endpoint — a
                        // real HTTP server that answers, and answers the REST
                        // `/v1/config` request the iceberg extension makes at
                        // ATTACH with a non-200. Chosen over a dead port on
                        // purpose: a refused connection is an `IO Error`
                        // (already mapped), and the class this pins is the one
                        // an HTTP server reaching back with the WRONG answer
                        // produces. It is also a mistake people really make.
                        ("endpoint".to_string(), env("LATIQ_S3_ENDPOINT")),
                        ("warehouse".to_string(), env("LATIQ_ICEBERG_WAREHOUSE")),
                        ("s3_endpoint".to_string(), env("LATIQ_S3_ENDPOINT")),
                    ]),
                    secrets: HashMap::from([
                        ("token".to_string(), env("LATIQ_ICEBERG_TOKEN")),
                        ("s3_access_key".to_string(), "AKIAEXAMPLE".to_string()),
                        ("s3_secret_key".to_string(), WRONG.to_string()),
                    ]),
                    secret_ref: String::new(),
                },
                "agent-x",
            ))
            .await
            .expect_err("an endpoint that is not a REST catalog must fail the attach");

        assert!(
            !err.details().is_empty(),
            "no ErrorEnvelope rode on this status: {err:?}"
        );
        let envelope: serde_json::Value = serde_json::from_slice(err.details()).unwrap();
        assert_eq!(
            envelope["kind"], "source_unavailable",
            "the endpoint is the CALLER's, so an attach that cannot reach a REST \
             catalog there is `source_unavailable` + 'check the path or URL' — \
             NOT `internal` + 'retry, then report to your operator': {envelope}"
        );
        assert_eq!(
            envelope["audience"], "agent",
            "there is nothing for an operator to do about a wrong URL: {envelope}"
        );
        // The SAME wrong endpoint refused at the TCP level is an `IO Error` and
        // already mapped here; this pins that answering-wrongly lands in the
        // same place, so the advice does not depend on how the far side failed.
        assert!(
            envelope["suggest"]
                .as_str()
                .is_some_and(|s| s.contains("path or URL")),
            "the advice must be about the address the caller supplied: {envelope}"
        );
        assert!(
            envelope["message"]
                .as_str()
                .is_some_and(|m| m.contains("/v1/config")),
            "DuckDB's own sentence names the request it made and what came back, \
             and that is the most useful part for whoever fixes it: {envelope}"
        );
        assert!(
            !envelope.to_string().contains(WRONG) && !format!("{err:?}").contains(WRONG),
            "the credential reached the caller: {envelope} / {err:?}"
        );

        // Nothing half-attached.
        let attached = json(
            data.catalog_list_attached(req(
                CatalogListAttachedRequest {
                    pond: "shop".into(),
                },
                "agent-x",
            ))
            .await
            .unwrap()
            .into_inner(),
        );
        assert_eq!(attached["catalogs"].as_array().map(|a| a.len()), Some(0));
    }
}

/// The `latiq` CLI as an OAuth client, driven as a real subprocess.
///
/// Admin gRPC is the OPERATOR surface, so the operator's CLI has to be able to
/// reach an authenticated control plane. Every command here talks to Control or
/// Admin — the surfaces `latiq serve --auth-issuer` protects — and none of them
/// is a data op, which is exactly the gap this module exists to hold shut.
mod cli_auth {
    use crate::common::start_control_plane_one_port;
    use std::process::Command;

    /// Run the CLI against `server`, optionally with a token in the environment.
    /// Returns (success, stderr) — the CLI renders gRPC errors to stderr and exits 1.
    fn cli(server: &str, token: Option<&str>, args: &[&str]) -> (bool, String) {
        let mut cmd = Command::new(env!("CARGO_BIN_EXE_latiq"));
        cmd.env("LATIQ_SERVER", server)
            // Never inherited from the developer's shell: it would mask exactly the
            // failure these tests look for.
            .env_remove("LATIQ_TOKEN")
            .env_remove("LATIQ_QUERY_GATEWAY")
            .args(args);
        if let Some(t) = token {
            cmd.env("LATIQ_TOKEN", t);
        }
        let out = cmd.output().expect("run the latiq binary");
        (
            out.status.success(),
            String::from_utf8_lossy(&out.stderr).to_string(),
        )
    }

    /// The CLI's rendering of an `Unauthenticated` status (no ErrorEnvelope rides on
    /// those, so it is the raw message).
    fn is_unauthenticated(stderr: &str) -> bool {
        stderr.contains("bearer token")
    }

    async fn blocking<T: Send + 'static>(f: impl FnOnce() -> T + Send + 'static) -> T {
        tokio::task::spawn_blocking(f).await.unwrap()
    }

    /// The headline case: an operator listing ponds against an authenticated control
    /// plane. This is a pure Admin call — no pond node is involved at all.
    #[tokio::test(flavor = "multi_thread")]
    async fn auth_cli_admin_command_needs_a_token() {
        let idp = latiq_auth::test_support::TestIdp::start().await;
        let server = start_control_plane_one_port(Some(idp.auth_config())).await;
        let token = idp.mint("svc-ops", "latiq", &idp.issuer, 300);

        let s = server.clone();
        let (ok, stderr) = blocking(move || cli(&s, None, &["pond", "list"])).await;
        assert!(!ok, "`pond list` must be refused without a token");
        assert!(is_unauthenticated(&stderr), "got: {stderr}");

        // $LATIQ_TOKEN alone is enough — no flag, no code change.
        let (s, t) = (server.clone(), token.clone());
        let (ok, stderr) = blocking(move || cli(&s, Some(&t), &["pond", "list"])).await;
        assert!(ok, "`pond list` must succeed with LATIQ_TOKEN: {stderr}");

        // …and so is the explicit global flag, on a subcommand that never declared
        // one of its own.
        let (s, t) = (server, token);
        let (ok, stderr) = blocking(move || cli(&s, None, &["pond", "list", "--token", &t])).await;
        assert!(ok, "`pond list --token` must succeed: {stderr}");
    }

    /// The guard against a future command that builds its own client and bypasses
    /// the shared helper. In the spirit of the 12-RPC enumeration in `admin.rs`:
    /// enumerate the CLI commands that talk to a server and assert none of them is
    /// turned away when a valid token is present. A command may still FAIL here (a
    /// missing pond, an empty registry) — what it may never do is fail for want of a
    /// credential it was handed.
    #[tokio::test(flavor = "multi_thread")]
    async fn auth_cli_every_server_command_sends_the_token() {
        let idp = latiq_auth::test_support::TestIdp::start().await;
        let server = start_control_plane_one_port(Some(idp.auth_config())).await;
        let token = idp.mint("svc-ops", "latiq", &idp.issuer, 300);

        // Every CLI command that reaches the ADMIN surface — the one `--auth-issuer`
        // protects. Data ops (`query`, `pond drop|describe`, `dataset load`,
        // `catalog attach|detach|list --pond`) need a pond node and are covered by
        // the Data-surface tests; `pond create` is the one command on the internal
        // Control surface, which carries no verifier by design, so its credential is
        // covered structurally by the constructor guard below instead.
        let commands: Vec<Vec<&str>> = vec![
            vec!["pond", "list"],
            vec!["pond", "set-tier", "cliauth", "--tier", "small"],
            vec!["node", "list"],
            vec!["node", "describe", "node-nope"],
            vec!["dataset", "list"],
            vec!["dataset", "add", "sales", "--table", "t=/tmp/t.parquet"],
            vec!["dataset", "remove", "sales"],
            vec!["catalog", "list"],
            vec!["catalog", "add", "lake", "--type", "iceberg"],
            vec!["catalog", "remove", "lake"],
            vec!["stats"],
        ];

        for args in commands {
            let (s, t, a) = (server.clone(), token.clone(), args.clone());
            let (_ok, stderr) = blocking(move || cli(&s, Some(&t), &a)).await;
            assert!(
                !is_unauthenticated(&stderr),
                "`latiq {}` did not send the bearer token — it is building a client \
                 outside the shared helper: {stderr}",
                args.join(" ")
            );

            // The same command with no token must be refused, which is what proves
            // the assertion above is testing the token and not merely a command that
            // never reaches the server.
            let (s, a) = (server.clone(), args.clone());
            let (_ok, stderr) = blocking(move || cli(&s, None, &a)).await;
            assert!(
                is_unauthenticated(&stderr),
                "`latiq {}` was NOT refused without a token: {stderr}",
                args.join(" ")
            );
        }
    }
}

/// The SDK against an auth-enabled stack: the client half of identity v0.
///
/// The Rust SDK is what the Python SDK wraps, so proving the token reaches the
/// Data/Stream surface from here covers both.
///
/// CHANGED with centralised pond creation: this used to assert that
/// `create_pond` succeeds WITHOUT a token, because "pond CREATION is a pure
/// control-plane op". It no longer is — the control plane materialises the pond
/// on the owning node before reporting it created, and that node requires a
/// token like every other caller. The invariant the test is really about is
/// unchanged and now covers one more call: **no token, nothing works**.
mod sdk_auth {
    use crate::common::{start_stack_one_port_with_auth, start_stack_with_auth};
    use arrow::array::Array;
    use latiq_sdk::Latiq;

    /// The SDK is blocking (it owns its own runtime), so every call runs on a
    /// blocking thread rather than on this test's runtime.
    async fn blocking<T: Send + 'static>(f: impl FnOnce() -> T + Send + 'static) -> T {
        tokio::task::spawn_blocking(f).await.unwrap()
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn auth_sdk_token_is_required_and_sufficient() {
        let idp = latiq_auth::test_support::TestIdp::start().await;
        let s = start_stack_with_auth(idp.auth_config()).await;
        let token = idp.mint("svc-sdk", "latiq", &idp.issuer, 300);
        let (control, gateway) = (s.control_endpoint.clone(), s.data_endpoint.clone());

        // ── no token: even CREATE is refused now ────────────────────────
        // Creation reaches the pond node (the control plane materialises the
        // pond there before reporting it created), so the token is needed from
        // the very first call, not only from the first query.
        let (c, g) = (control.clone(), gateway.clone());
        let err = blocking(move || {
            let db = Latiq::connect_with(&c, None, Some(&g)).unwrap();
            match db.create_pond(Some("sdkauth"), "medium", "", false) {
                Ok(_) => panic!("creating a pond with no token must be refused"),
                Err(e) => e.to_string(),
            }
        })
        .await;
        assert!(
            err.to_lowercase().contains("token"),
            "creating a pond with no token must be refused at the node the control \
             plane materialises it on: {err}"
        );
        // …and it was refused by NOT CREATING it: the name is still free, which
        // is the compensation working end to end through the SDK.
        let (c, g, t) = (control.clone(), gateway.clone(), token.clone());
        blocking(move || {
            let db = Latiq::connect_with_token(&c, None, Some(&g), Some(&t)).unwrap();
            db.create_pond(Some("sdkauth"), "medium", "", false)
                .map(|p| p.id().to_string())
                .expect("the rolled-back attempt must leave the name free")
        })
        .await;

        // ── explicit token ──────────────────────────────────────────────
        let (c, g, t) = (control.clone(), gateway.clone(), token.clone());
        let batches = blocking(move || {
            let db = Latiq::connect_with_token(&c, None, Some(&g), Some(&t)).unwrap();
            db.query("sdkauth", "SELECT 1 AS n").unwrap()
        })
        .await;
        assert_eq!(batches.iter().map(|b| b.num_rows()).sum::<usize>(), 1);

        // ── LATIQ_TOKEN ─────────────────────────────────────────────────
        // Same call, no code change: the env var is how a notebook or a job gets a
        // token in without threading it through every `connect`.
        let (c, g, t) = (control.clone(), gateway.clone(), token.clone());
        let rows = blocking(move || {
            // Set and cleared on this thread's process — done inside ONE test so no
            // other test can observe the window.
            std::env::set_var("LATIQ_TOKEN", &t);
            let db = Latiq::connect_with(&c, None, Some(&g)).unwrap();
            let out = db.query("sdkauth", "SELECT 1 AS n");
            std::env::remove_var("LATIQ_TOKEN");
            out.unwrap().iter().map(|b| b.num_rows()).sum::<usize>()
        })
        .await;
        assert_eq!(rows, 1);

        // ── writes are attributed to the token's subject ────────────────
        let (c, g, t) = (control, gateway, token);
        let authors = blocking(move || {
            let db = Latiq::connect_with_token(&c, None, Some(&g), Some(&t)).unwrap();
            db.query("sdkauth", "CREATE TABLE t(i INTEGER)").unwrap();
            let b = db
                .query(
                    "sdkauth",
                    "SELECT DISTINCT author FROM ducklake_snapshots('sdkauth')",
                )
                .unwrap();
            b.iter()
                .flat_map(|batch| {
                    let col = batch
                        .column(0)
                        .as_any()
                        .downcast_ref::<arrow::array::StringArray>()
                        .expect("author is a string column");
                    (0..col.len())
                        .map(|i| col.value(i).to_string())
                        .collect::<Vec<_>>()
                })
                .collect::<Vec<_>>()
        })
        .await;
        assert!(
            authors.iter().any(|a| a == "svc-sdk"),
            "the DuckLake author must be the token's subject, got {authors:?}"
        );
    }

    /// `list_ponds` is the SDK's ONE Admin call, and Admin is the surface a client
    /// is most likely to leave un-tokened: everything else it does rides Data or
    /// Control. Against a fully authenticated stack a tokened client must be able to
    /// read pond metadata, and an un-tokened one must be refused — the second half
    /// is what proves the first is not passing because nothing is enforced.
    #[tokio::test(flavor = "multi_thread")]
    async fn auth_sdk_admin_metadata_read_carries_the_token() {
        let idp = latiq_auth::test_support::TestIdp::start().await;
        let s = start_stack_one_port_with_auth(idp.auth_config()).await;
        let token = idp.mint("svc-sdk", "latiq", &idp.issuer, 300);
        let (server, gateway) = (s.control_endpoint.clone(), s.data_endpoint.clone());

        let (c, g, t) = (server.clone(), gateway.clone(), token);
        let listed = blocking(move || {
            let db = Latiq::connect_with_token(&c, None, Some(&g), Some(&t)).unwrap();
            db.create_pond(Some("sdkadmin"), "medium", "", false)
                .unwrap();
            db.list_ponds().unwrap()
        })
        .await;
        assert!(
            listed.contains_key("sdkadmin"),
            "a tokened operator read must see the pond, got {:?}",
            listed.keys().collect::<Vec<_>>()
        );

        let (c, g) = (server, gateway);
        let err = blocking(move || {
            let db = Latiq::connect_with(&c, None, Some(&g)).unwrap();
            db.list_ponds().unwrap_err().to_string()
        })
        .await;
        assert!(
            err.to_lowercase().contains("token"),
            "an un-tokened Admin read must be refused: {err}"
        );
    }
}
