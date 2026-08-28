//! The `latiq` CLI as an OAuth client, driven as a real subprocess.
//!
//! Admin gRPC is the OPERATOR surface, so the operator's CLI has to be able to
//! reach an authenticated control plane. Every command here talks to Control or
//! Admin — the surfaces `latiq serve --auth-issuer` protects — and none of them
//! is a data op, which is exactly the gap this file exists to hold shut.
mod common;

use common::start_control_plane_one_port;
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

/// A structural guard the table above cannot give: the gRPC client constructors
/// appear exactly once each, inside the helpers that attach the token. A command
/// that dialed its own `AdminClient` would send no credential no matter what the
/// table says, and would trip this.
///
/// Pinned to EXACTLY one, not "at most one": `<= 1` is satisfied by zero, so the
/// guard passed vacuously the moment a helper was renamed or a client stopped
/// being built — silently guarding nothing. Same counter-assertion the SDK's
/// `client_construction.rs` uses.
#[test]
fn auth_cli_clients_are_only_built_in_the_shared_helpers() {
    let src = include_str!("../src/main.rs");

    // The three clients the CLI actually dials, each from exactly one helper.
    for ctor in ["AdminClient::", "ControlClient::", "DataClient::"] {
        let uses = src.matches(ctor).count();
        assert_eq!(
            uses, 1,
            "{ctor} is constructed {uses} times; it must be built exactly once, in \
             the shared helper that attaches the bearer token"
        );
        let authed = src.matches(&format!("{ctor}with_interceptor")).count();
        assert_eq!(
            authed, 1,
            "the one {ctor} construction is not `with_interceptor`, so it would send \
             neither `latiq-agent-id` nor the bearer token"
        );
    }

    // `StreamClient` has no CLI command today (the SDK owns the streaming path),
    // so it is asserted differently: not "exactly one", which would fire on an
    // honest new command, but "every construction, if any, is authenticated".
    assert_eq!(
        src.matches("StreamClient::").count(),
        src.matches("StreamClient::with_interceptor").count(),
        "a StreamClient is being built outside the shared builder, so it would \
         carry no bearer token"
    );
}
