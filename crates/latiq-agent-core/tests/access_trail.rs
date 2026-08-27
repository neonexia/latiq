//! The `latiq::access` trace is the operator's record of who did what, so it
//! must distinguish the verified subject from the claimed leaf agent id.
//!
//! Its own test binary on purpose: capturing `tracing` output needs a subscriber
//! installed as the *process* default, because callsite interest is cached
//! process-wide the first time a callsite is hit. Sibling tests running in
//! parallel with no subscriber would cache it as "never" and this test would see
//! an empty log.
use latiq_agent_core::{AgentConfig, AgentOps, RegistryControlPlane};
use latiq_common::Identity;
use latiq_control_plane::Registry;
use latiq_engine_duckdb::DuckEngine;
use latiq_storage::TempFs;
use std::sync::{Arc, Mutex};

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

#[tokio::test]
async fn access_trail_records_the_verified_subject_and_issuer() {
    // A caller holding a valid token for `svc-lowpriv` claims a flattering leaf
    // id. `verified` must scope the subject/issuer it belongs to, never the
    // claimed agent — otherwise the trail reads as an authenticated `svc-admin`.
    let captured = CapturedLog::default();
    tracing::subscriber::set_global_default(
        tracing_subscriber::fmt()
            .with_writer(captured.clone())
            .with_max_level(tracing::Level::INFO)
            .with_ansi(false)
            .finish(),
    )
    .expect("this binary runs one test, so nothing else installs a subscriber");

    let registry = Registry::open(None).unwrap();
    registry
        .register_node(
            "node-a",
            "http://127.0.0.1:8080/mcp",
            "http://127.0.0.1:9092",
            100,
        )
        .unwrap();
    let ops = AgentOps::new(
        Arc::new(RegistryControlPlane::new(registry)),
        Arc::new(TempFs::new()),
        Arc::new(DuckEngine::new()),
        AgentConfig::default(),
    );

    let id = Identity::verified(
        "svc-lowpriv",
        "https://idp.example/realms/latiq",
        Some("svc-admin"),
    );
    ops.allocate_pond(&id, Some("audited".into()), "{}", "medium", &[])
        .await
        .unwrap();

    let log = String::from_utf8(captured.0.lock().unwrap().clone()).unwrap();
    assert!(
        log.contains("subject=svc-lowpriv"),
        "access trail must carry the verified subject: {log}"
    );
    assert!(
        log.contains("issuer=https://idp.example/realms/latiq"),
        "access trail must carry the issuer: {log}"
    );
    // The claimed leaf is still recorded — as a claim, never as authority.
    assert!(
        log.contains("agent=svc-admin"),
        "the claimed leaf is still recorded: {log}"
    );
}
