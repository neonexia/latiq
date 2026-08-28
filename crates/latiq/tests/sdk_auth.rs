//! The SDK against an auth-enabled stack: the client half of identity v0.
//!
//! The Rust SDK is what the Python SDK wraps, so proving the token reaches the
//! Data/Stream surface from here covers both. The stack's pond node requires a
//! verified bearer token; its control plane does not (pond CREATION is a pure
//! control-plane op), which is exactly the asymmetry that makes "the query is
//! the thing that needs the token" visible.
mod common;

use arrow::array::Array;
use common::{start_stack_one_port_with_auth, start_stack_with_auth};
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
        db.create_pond(Some("sdkauth"), "medium", "").unwrap();
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
        db.create_pond(Some("sdkadmin"), "medium", "").unwrap();
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
