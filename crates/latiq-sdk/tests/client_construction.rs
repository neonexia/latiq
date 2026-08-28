//! A structural guard on how the SDK builds gRPC clients.
//!
//! The identity headers (`latiq-agent-id` and, where a deployment configures an
//! issuer, `authorization: Bearer …`) are attached by the `BearerAuth`
//! interceptor installed in the four client helpers. That makes CONSTRUCTION the
//! only place a request can lose them: an interceptor cannot be forgotten at a
//! call site, but a client built straight from a raw `Channel` has no
//! interceptor at all and would send neither header.
//!
//! This is the shape of bug that shipped once already — `list_ponds` issued the
//! SDK's one Admin call without the wrapper that carried the token, so a
//! fully-tokened client was refused by the control plane. The wrapper is gone;
//! this test is what keeps its replacement from being routed around.
//!
//! A source-level assertion rather than a behavioural one because the thing
//! being guarded is a code shape, and the behavioural half already exists:
//! `crates/latiq/tests/sdk_auth.rs` drives Data, Stream and Admin against an
//! authenticated stack.

/// Every way tonic exposes to build a client from a channel. `with_interceptor`
/// is the only one that installs `BearerAuth`; the others hand back a bare
/// client that would send no identity at all.
const UNAUTHENTICATED_CONSTRUCTORS: &[&str] = &["::new(", "::connect(", "::with_origin("];

const CLIENTS: &[&str] = &["ControlClient", "AdminClient", "DataClient", "StreamClient"];

#[test]
fn every_grpc_client_is_built_with_the_auth_interceptor() {
    let src = include_str!("../src/lib.rs");
    for client in CLIENTS {
        for ctor in UNAUTHENTICATED_CONSTRUCTORS {
            let pattern = format!("{client}{ctor}");
            assert!(
                !src.contains(&pattern),
                "`{pattern}` builds a client with no interceptor, so it would send \
                 neither `latiq-agent-id` nor the bearer token. Every gRPC client \
                 must come from the helper that installs `BearerAuth` — see the \
                 `list_ponds` regression in crates/latiq/tests/sdk_auth.rs."
            );
        }
        // …and each client really is built, the authenticated way, at least
        // once: a guard that passes because nothing matches is no guard.
        //
        // Deliberately NOT pinned to exactly one. The invariant is "every
        // construction is authenticated", not "there is only one construction" —
        // `AdminClient` legitimately has two (the `admin()` helper and the
        // embedded readiness probe). Pinning the count would make a future
        // honest builder look like a violation, and the pressure would be to
        // relax this test rather than to route the new builder correctly.
        let authed = src.matches(&format!("{client}::with_interceptor")).count();
        assert!(
            authed >= 1,
            "no authenticated `{client}` builder found — the assertions above \
             would then be passing vacuously"
        );
    }
}

/// The interceptor is only worth anything if it attaches both headers. Asserted
/// on the source for the same reason as above: it is one function, and the wire
/// behaviour it produces is covered end-to-end elsewhere.
#[test]
fn the_interceptor_attaches_both_identity_headers() {
    let src = include_str!("../src/lib.rs");
    let body = src
        .split_once("impl tonic::service::Interceptor for BearerAuth")
        .expect("the SDK's interceptor is named BearerAuth")
        .1;
    assert!(
        body.contains("latiq-agent-id"),
        "the claimed leaf is missing"
    );
    assert!(
        body.contains("authorization"),
        "the bearer token is missing — this is exactly the list_ponds bug"
    );
}
