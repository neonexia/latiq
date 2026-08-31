//! The caller's raw bearer token, ambient for the duration of one request.
//!
//! It exists for exactly one reason: the node-to-node hop. Re-injecting only the
//! claimed leaf id would hand the owning node a CLAIMED identity, silently
//! losing subject/issuer and corrupting its attribution — so the forwarder
//! replays the original token and the owner verifies it itself. Deliberately NOT
//! an internal "already verified" header: a header the owner trusts without
//! checking is exactly the trust laundering this design forbids.
//!
//! Protocol-neutral (just a `String` task-local, invariant 5): every inbound
//! adapter — gRPC metadata, MCP over HTTP headers — extracts the token from its
//! own carrier and scopes it here, and the forwarder reads it back. It lives in
//! the core rather than in one adapter because MCP and the Data surface share
//! one `AgentOps` and one forwarder; a token scoped in only one of them is a
//! surface that silently fails (or forwards unauthenticated) on a cluster.
use std::future::Future;

tokio::task_local! {
    static BEARER: String;
}

/// Run `fut` with `token` as the ambient bearer credential — including any
/// forwarded calls it makes (they run in the same task). `None` leaves the scope
/// unset, which is what an unauthenticated deployment must do: a node that never
/// opted into auth must not capture whatever `authorization` header a client
/// happens to send (one meant for an upstream gateway, say) and replay it to a
/// peer.
pub async fn with_bearer<F: Future>(token: Option<String>, fut: F) -> F::Output {
    match token {
        Some(t) => BEARER.scope(t, fut).await,
        None => fut.await,
    }
}

/// The ambient bearer token, if this task is handling an authenticated request.
pub fn current_bearer() -> Option<String> {
    BEARER.try_with(|t| t.clone()).ok()
}
