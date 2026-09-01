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
const ADMIN_RPC_COUNT: usize = 12;

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
                lineage: false,
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
                query: "CREATE TABLE cheap AS SELECT id,name FROM ext.widgets WHERE price < 10"
                    .into(),
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
        // After detach, the external catalog is no longer queryable from the pond.
        // Asserted on the REASON: `is_err()` alone is satisfied by a dropped pond, a
        // dead node, or a syntax error -- i.e. by everything except the detach.
        let err = data
            .read_query(req(
                QueryRequest {
                    pond: "shop".into(),
                    sql: "SELECT count(*) FROM ext.widgets".into(),
                },
                "agent-x",
            ))
            .await
            .expect_err("external catalog must be detached after pull");
        let msg = err.message().to_lowercase();
        assert!(
            msg.contains("ext") && msg.contains("does not exist"),
            "the failure must be `ext` being gone, not some other error: {msg}"
        );
        // ...and the pond itself is fine, which is what rules out "the whole pond
        // went away" as the reason above.
        data.read_query(req(
            QueryRequest {
                pond: "shop".into(),
                sql: "SELECT count(*) FROM cheap".into(),
            },
            "agent-x",
        ))
        .await
        .expect("detaching the catalog must not disturb the pond");
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

/// Iceberg + MinIO end-to-end for the catalog pull path. `#[ignore]`d because it
/// needs a live Iceberg REST catalog + S3 (MinIO) — bring them up with
/// `deploy/iceberg-minio/up.sh`, then run with `--ignored`. Config comes from env
/// (set by the harness / CI):
///
///   LATIQ_ICEBERG_ENDPOINT  LATIQ_ICEBERG_WAREHOUSE  LATIQ_ICEBERG_TOKEN
///   LATIQ_S3_ENDPOINT  LATIQ_S3_ACCESS_KEY  LATIQ_S3_SECRET_KEY
mod catalogs_iceberg {
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
                lineage: false,
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
                query:
                    "CREATE TABLE cheap AS SELECT id,name FROM lake.demo.widgets WHERE price < 10"
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
        // `catalog describe|pull`) need a pond node and are covered by the
        // Data-surface tests; `pond create` is the one command on the internal
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
/// Data/Stream surface from here covers both. The stack's pond node requires a
/// verified bearer token; its control plane does not (pond CREATION is a pure
/// control-plane op), which is exactly the asymmetry that makes "the query is
/// the thing that needs the token" visible.
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

        // ── no token ────────────────────────────────────────────────────
        // The pond is allocated through the (unauthenticated) control plane, so the
        // first thing that touches the node is the query — and it is refused.
        let (c, g) = (control.clone(), gateway.clone());
        let err = blocking(move || {
            let db = Latiq::connect_with(&c, None, Some(&g)).unwrap();
            db.create_pond(Some("sdkauth"), "medium", "", false)
                .unwrap();
            db.query("sdkauth", "SELECT 1 AS n")
                .unwrap_err()
                .to_string()
        })
        .await;
        assert!(
            err.to_lowercase().contains("token"),
            "an SDK call with no token must be refused: {err}"
        );

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
