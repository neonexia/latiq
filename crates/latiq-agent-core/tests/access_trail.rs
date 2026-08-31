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
    ops.allocate_pond(&id, Some("audited".into()), "{}", "medium", &[], false)
        .await
        .unwrap();

    // And an unverified caller through the same path, to pin both shapes.
    ops.allocate_pond(
        &Identity::claimed(Some("agent-plain")),
        Some("unaudited".into()),
        "{}",
        "medium",
        &[],
        false,
    )
    .await
    .unwrap();

    let log = String::from_utf8(captured.0.lock().unwrap().clone()).unwrap();
    let verified_line = log
        .lines()
        .find(|l| l.contains("agent=svc-admin"))
        .unwrap_or_else(|| panic!("the claimed leaf must still be recorded: {log}"));
    assert!(
        verified_line.contains("subject=svc-lowpriv"),
        "access trail must carry the verified subject: {verified_line}"
    );
    assert!(
        verified_line.contains("issuer=https://idp.example/realms/latiq"),
        "access trail must carry the issuer: {verified_line}"
    );
    // `verified` is the field that makes subject/issuer readable as authority;
    // without it on the line the pairing means nothing.
    assert!(
        verified_line.contains("verified=true"),
        "access trail must mark the pair as verified: {verified_line}"
    );

    // The unverified shape: the claim stands alone, with nothing that could be
    // read as an authenticated identity.
    let claimed_line = log
        .lines()
        .find(|l| l.contains("agent=agent-plain"))
        .unwrap_or_else(|| panic!("the unverified caller must be recorded: {log}"));
    assert!(
        claimed_line.contains("subject=") && claimed_line.contains("issuer="),
        "the verified fields must still be present, just empty: {claimed_line}"
    );
    assert!(
        claimed_line.contains("subject= "),
        "an unverified caller has no subject: {claimed_line}"
    );
    assert!(
        claimed_line.contains("verified=false"),
        "an unverified caller must be marked unverified: {claimed_line}"
    );

    // `outcome` is part of the shared field set: the Admin surface writes it on
    // the same target, and a stream where only one producer says whether the
    // action LANDED cannot be filtered honestly.
    for line in [&verified_line, &claimed_line] {
        assert!(
            line.contains("outcome=\"ok\""),
            "an agent record must say whether the action landed: {line}"
        );
    }

    // A FAILED op must leave a record too — the whole point of the change. The
    // name is taken, so this allocation cannot succeed.
    ops.allocate_pond(&id, Some("audited".into()), "{}", "medium", &[], false)
        .await
        .expect_err("the name is already taken");
    let log = String::from_utf8(captured.0.lock().unwrap().clone()).unwrap();
    let failed = log
        .lines()
        .filter(|l| l.contains("latiq::access"))
        .find(|l| l.contains("outcome=\"error\""))
        .unwrap_or_else(|| panic!("a failed op must be on the access trail: {log}"));
    assert!(
        failed.contains("op=\"allocate_pond\"") && failed.contains("subject=svc-lowpriv"),
        "a failed op is still attributed: {failed}"
    );
    assert!(
        failed.contains("pond=\"-\""),
        "an allocation that failed has no pond to name: {failed}"
    );
}
