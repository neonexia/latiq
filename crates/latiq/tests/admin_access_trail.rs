//! Operator actions on the Admin gRPC must land in the SAME `latiq::access`
//! stream agents' actions land in, carrying the VERIFIED subject. Succeeding is
//! not enough: an unattributed `policy_set` is exactly the gap this closes.
//!
//! Its own test binary on purpose: capturing `tracing` output needs a subscriber
//! installed as the *process* default, because callsite interest is cached
//! process-wide the first time a callsite is hit. Sibling tests running in
//! parallel with no subscriber would cache it as "never" and this test would see
//! an empty log. Same reasoning (and shape) as
//! `crates/latiq-agent-core/tests/access_trail.rs`.
mod common;

use latiq_proto::v1::admin_client::AdminClient;
use latiq_proto::v1::*;
use std::sync::{Arc, Mutex};
use tonic::Request;

/// A `tracing` writer that collects everything into a shared buffer.
#[derive(Clone, Default)]
struct CapturedLog(Arc<Mutex<Vec<u8>>>);

impl std::io::Write for CapturedLog {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.0.lock().unwrap().extend_from_slice(buf);
        Ok(buf.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for CapturedLog {
    type Writer = Self;
    fn make_writer(&'a self) -> Self::Writer {
        self.clone()
    }
}

fn bearer_req<T>(msg: T, agent: &str, token: &str) -> Request<T> {
    let mut r = Request::new(msg);
    r.metadata_mut()
        .insert("latiq-agent-id", agent.parse().unwrap());
    r.metadata_mut()
        .insert("authorization", format!("Bearer {token}").parse().unwrap());
    r
}

#[tokio::test]
async fn auth_admin_actions_are_attributed_in_the_access_trail() {
    let captured = CapturedLog::default();
    tracing::subscriber::set_global_default(
        tracing_subscriber::fmt()
            .with_writer(captured.clone())
            .with_max_level(tracing::Level::INFO)
            .with_ansi(false)
            .finish(),
    )
    .expect("this binary runs one test, so nothing else installs a subscriber");

    let idp = latiq_auth::test_support::TestIdp::start().await;
    let (_control, admin_endpoint) =
        common::start_control_plane_with_auth(Some(idp.auth_config())).await;
    let mut admin = AdminClient::connect(admin_endpoint.clone()).await.unwrap();
    let token = idp.mint("svc-ops", "latiq", &idp.issuer, 300);

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
    admin
        .catalog_add(bearer_req(
            CatalogAddRequest {
                catalog: Some(CatalogMsg {
                    name: "audited".into(),
                    r#type: "iceberg".into(),
                    params: Default::default(),
                    description: String::new(),
                    tags: vec![],
                    created_by: String::new(),
                    created_at: String::new(),
                }),
            },
            "opsbot",
            &token,
        ))
        .await
        .unwrap();

    // A FAILING action, to prove the trail distinguishes it from a real one.
    admin
        .dataset_remove(bearer_req(
            DatasetRemoveRequest {
                name: "never-existed".into(),
            },
            "opsbot",
            &token,
        ))
        .await
        .unwrap_err();

    // ...and two rejected callers, who leave a record precisely BECAUSE they
    // were rejected: a surface whose job is "record who tried" must not go
    // silent on the attempts worth reading.
    let mut anon = AdminClient::connect(admin_endpoint.clone()).await.unwrap();
    anon.policy_get(PolicyGetRequest {}).await.unwrap_err();
    anon.policy_get(bearer_req(PolicyGetRequest {}, "intruder", "not-a-jwt"))
        .await
        .unwrap_err();

    let log = String::from_utf8(captured.0.lock().unwrap().clone()).unwrap();
    let access = |needle: &str| -> String {
        log.lines()
            .filter(|l| l.contains("latiq::access"))
            .find(|l| l.contains(needle))
            .unwrap_or_else(|| panic!("no access record matching {needle}: {log}"))
            .to_string()
    };

    for op in ["policy_set", "catalog_add"] {
        let line = log
            .lines()
            .filter(|l| l.contains("latiq::access"))
            // Quoted because `op` is a string field -- exactly how `ops.rs`
            // renders it for agent actions.
            .find(|l| l.contains(&format!("op={op:?}")))
            .unwrap_or_else(|| panic!("operator action {op} must be on the access trail: {log}"));
        assert!(
            line.contains("outcome=\"ok\""),
            "a successful action must be marked as such: {line}"
        );
        assert!(
            line.contains("agent=opsbot"),
            "the claimed leaf must still be recorded: {line}"
        );
        assert!(
            line.contains("subject=svc-ops"),
            "the access trail must carry the verified subject: {line}"
        );
        assert!(
            line.contains(&format!("issuer={}", idp.issuer)),
            "the access trail must carry the issuer: {line}"
        );
        assert!(
            line.contains("verified=true"),
            "the access trail must mark the pair as verified: {line}"
        );
        // Same field set as `AgentOps::audit`, so one grep finds operator and
        // agent actions alike.
        assert!(
            line.contains("pond=\"-\"")
                && line.contains("duration_ms=")
                && line.contains("summary="),
            "operator records must carry the same fields as agent records: {line}"
        );
    }

    // A failed removal must NOT read like a successful one. Without `outcome`
    // the two records are byte-identical and the trail is confidently wrong.
    let failed = access("op=\"dataset_remove\"");
    assert!(
        failed.contains("outcome=\"error\""),
        "a failed action must be marked failed: {failed}"
    );
    assert!(
        failed.contains("dataset=never-existed"),
        "the attempted target is still recorded: {failed}"
    );

    let no_token = access("rejected: no token");
    assert!(
        no_token.contains("op=\"policy_get\"") && no_token.contains("outcome=\"error\""),
        "a tokenless attempt must name the RPC it targeted: {no_token}"
    );
    assert!(
        no_token.contains("verified=false") && no_token.contains("subject= "),
        "a rejected caller has no verified identity: {no_token}"
    );
    let bad_token = access("rejected: invalid token");
    assert!(
        bad_token.contains("op=\"policy_get\"") && bad_token.contains("outcome=\"error\""),
        "a forged-token attempt must be recorded: {bad_token}"
    );
    assert!(
        bad_token.contains("agent=intruder") && bad_token.contains("verified=false"),
        "the claim is all a rejected caller has, and it is not authority: {bad_token}"
    );
}
