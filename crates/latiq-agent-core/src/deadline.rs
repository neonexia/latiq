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

//! Per-request execution controls: the query's deadline and the caller's own
//! cancel, both expressed on the ONE `AbortToken` the engine already honors.
//!
//! DuckDB has no timeout of its own — every setting it exposes for the purpose
//! (`memory_limit`, `pivot_limit`) is about size, not time — so the deadline has
//! to be ours: a watcher that fires the interrupt the engine already installs.
//! Which means the interrupt an expiry produces is byte-identical to the one a
//! user-requested cancel produces (`INTERRUPT Error: Interrupted!`), and the two
//! can only be told apart by remembering **who pulled the trigger**. That is
//! what [`Deadline::expired`] is for, and why classification lives here rather
//! than in the engine, which cannot know.
//!
//! Protocol-neutral, like the rest of the core: `AbortToken` is
//! `tokio_util`'s cancellation token and nothing here knows about MCP or gRPC.
use crate::error::AgentError;
use latiq_common::QueryTimeouts;
use latiq_engine::{AbortToken, EngineError};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

/// What a caller may say about how one statement is allowed to run. Both fields
/// are optional and both default to "nothing special", so the existing call
/// paths keep their behaviour exactly.
#[derive(Clone, Default)]
pub struct QueryControls {
    /// How long the caller wants the statement to be allowed to run. Clamped to
    /// the node's maximum (never refused) and reported back in
    /// `QueryMeta::timeout_ms`.
    pub timeout_ms: Option<u64>,
    /// The caller's own cancellation, if its transport has one — MCP's
    /// `notifications/cancelled` arrives as exactly this. Cancelling it
    /// interrupts the running statement and the caller gets `QueryCancelled`,
    /// which is deliberately a DIFFERENT kind from the deadline's.
    pub cancel: Option<AbortToken>,
}

impl QueryControls {
    /// The caller asked for nothing: node default timeout, no external cancel.
    pub fn none() -> Self {
        Self::default()
    }

    /// The caller's requested timeout (`None` = use the node's default).
    pub fn timeout(timeout_ms: Option<u64>) -> Self {
        Self {
            timeout_ms,
            cancel: None,
        }
    }

    /// Attach the caller's cancellation source.
    pub fn with_cancel(mut self, cancel: AbortToken) -> Self {
        self.cancel = Some(cancel);
        self
    }
}

/// A statement's armed deadline. Holds the watcher task that fires the abort
/// token on expiry (or when the caller cancels), and remembers which of the two
/// happened so the error can say so.
///
/// **Drop stops the watcher AND fires the abort token**, so the guard's lifetime
/// is the statement's lifetime in both directions: keep it alive for exactly as
/// long as the statement runs (for a streamed read that is the life of the
/// stream, not of the call that started it).
///
/// Firing on drop is what covers the request nobody is waiting for any more. A
/// gRPC client that hangs up drops the handler future mid-await, but the engine
/// call is already detached on the blocking pool — so a guard that merely
/// stopped its watcher would leave that query running with NO deadline at all,
/// which is the one outcome the node's maximum exists to prevent. On the normal
/// path the statement has already finished and the token is inert, so the extra
/// cancel costs nothing.
pub(crate) struct Deadline {
    effective_ms: u64,
    max_ms: u64,
    expired: Arc<AtomicBool>,
    token: AbortToken,
    watcher: tokio::task::JoinHandle<()>,
}

impl Deadline {
    /// Arm `token` with this request's deadline and the caller's cancel.
    ///
    /// One watcher for both, rather than two tasks: they race for the same
    /// token, and a single `select!` is what makes the winner unambiguous — the
    /// flag is only ever set by the timer arm, so a cancel that lands first can
    /// never be reported as a timeout.
    pub(crate) fn arm(
        token: &AbortToken,
        timeouts: QueryTimeouts,
        controls: &QueryControls,
    ) -> Self {
        let effective_ms = timeouts.effective(controls.timeout_ms);
        let expired = Arc::new(AtomicBool::new(false));
        let token = token.clone();
        let watched = token.clone();
        let flag = expired.clone();
        let external = controls.cancel.clone();
        let watcher = tokio::spawn(async move {
            let token = watched;
            let caller_cancelled = async move {
                match external {
                    Some(ct) => ct.cancelled().await,
                    // A caller with no cancellation source must never resolve
                    // this arm — `pending()`, not an immediate return.
                    None => std::future::pending::<()>().await,
                }
            };
            tokio::select! {
                _ = tokio::time::sleep(Duration::from_millis(effective_ms)) => {
                    // Ordered: the flag is visible before the interrupt can
                    // possibly be observed as an error, so a classification
                    // racing the abort cannot read a stale `false`.
                    flag.store(true, Ordering::SeqCst);
                    token.cancel();
                }
                _ = caller_cancelled => token.cancel(),
            }
        });
        Self {
            effective_ms,
            max_ms: timeouts.max_ms,
            expired,
            token,
            watcher,
        }
    }

    /// The timeout actually applied — what `QueryMeta::timeout_ms` reports.
    pub(crate) fn effective_ms(&self) -> u64 {
        self.effective_ms
    }

    /// Did WE cut this query on its deadline (as opposed to someone cancelling
    /// it)?
    pub(crate) fn expired(&self) -> bool {
        self.expired.load(Ordering::SeqCst)
    }

    /// Turn an engine error into the agent-facing one, splitting the engine's
    /// single `Cancelled` into the two things it can mean. Every other engine
    /// error keeps its usual mapping.
    pub(crate) fn classify(&self, e: EngineError) -> AgentError {
        match e {
            EngineError::Cancelled if self.expired() => {
                AgentError::query_timeout(self.effective_ms, self.max_ms)
            }
            other => AgentError::from(other),
        }
    }
}

impl Drop for Deadline {
    fn drop(&mut self) {
        self.watcher.abort();
        // Not `expired`: nothing about a dropped guard says a deadline was
        // reached, and labelling it a timeout would name a number that never
        // applied. It is a cancel — the caller stopped caring.
        self.token.cancel();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use latiq_common::ErrorKind;

    fn timeouts(default_ms: u64, max_ms: u64) -> QueryTimeouts {
        QueryTimeouts::new(default_ms, max_ms).unwrap()
    }

    #[tokio::test]
    async fn cancellation_deadline_fires_the_abort_token_and_says_it_expired() {
        let token = AbortToken::new();
        let d = Deadline::arm(&token, timeouts(20, 1_000), &QueryControls::none());
        token.cancelled().await;
        assert!(
            d.expired(),
            "the timer arm must record that IT cancelled, or the error cannot say so"
        );
        assert_eq!(d.effective_ms(), 20);
    }

    #[tokio::test]
    async fn cancellation_a_caller_cancel_fires_the_token_without_claiming_a_timeout() {
        let token = AbortToken::new();
        let caller = AbortToken::new();
        // A deadline far away, so only the caller's cancel can win.
        let d = Deadline::arm(
            &token,
            timeouts(60_000, 60_000),
            &QueryControls::none().with_cancel(caller.clone()),
        );
        caller.cancel();
        token.cancelled().await;
        assert!(
            !d.expired(),
            "a caller cancel must NOT be reported as a timeout — they are different kinds"
        );
        assert_eq!(
            d.classify(EngineError::Cancelled).envelope().kind,
            ErrorKind::QueryCancelled
        );
    }

    #[tokio::test]
    async fn cancellation_classify_splits_the_engines_one_cancelled_into_two_kinds() {
        // The engine cannot tell these apart: an expiry and a cancel produce the
        // SAME `INTERRUPT Error: Interrupted!`. Only the deadline knows.
        let token = AbortToken::new();
        let expired = Deadline::arm(&token, timeouts(10, 1_000), &QueryControls::none());
        token.cancelled().await;
        let env = expired.classify(EngineError::Cancelled).into_envelope();
        assert_eq!(env.kind, ErrorKind::QueryTimeout);
        assert!(
            env.message.contains("10 ms") && env.message.contains("1000 ms"),
            "the message must name the applied timeout AND the ceiling: {}",
            env.message
        );

        let never = AbortToken::new();
        let live = Deadline::arm(&never, timeouts(60_000, 60_000), &QueryControls::none());
        assert_eq!(
            live.classify(EngineError::Cancelled).envelope().kind,
            ErrorKind::QueryCancelled,
            "no deadline fired, so this was somebody's cancel"
        );
    }

    #[tokio::test]
    async fn cancellation_classify_leaves_every_other_engine_error_alone() {
        // A deadline that HAS expired must not relabel an unrelated failure as a
        // timeout — otherwise a syntax error on a slow node reads as one.
        let token = AbortToken::new();
        let d = Deadline::arm(&token, timeouts(10, 1_000), &QueryControls::none());
        token.cancelled().await;
        assert!(d.expired());
        assert_eq!(
            d.classify(EngineError::ReadOnlyViolation).envelope().kind,
            ErrorKind::ReadOnlyViolation
        );
        assert_eq!(
            d.classify(EngineError::Parse("bad".into())).envelope().kind,
            ErrorKind::ParseError
        );
    }

    #[tokio::test]
    async fn cancellation_dropping_the_guard_stops_the_query_it_was_guarding() {
        // The abandoned-request case: a client hangs up, the handler future is
        // dropped, but the engine call is already detached on the blocking pool.
        // A guard that only stopped its watcher would leave that query running
        // with no deadline at all.
        let token = AbortToken::new();
        {
            let _d = Deadline::arm(&token, timeouts(60_000, 60_000), &QueryControls::none());
            assert!(!token.is_cancelled(), "nothing has happened yet");
        }
        assert!(
            token.is_cancelled(),
            "dropping the guard must fire the abort token, or an abandoned query \
             outlives every bound the node has"
        );
    }

    #[tokio::test]
    async fn cancellation_the_effective_timeout_is_the_clamped_one() {
        let token = AbortToken::new();
        let d = Deadline::arm(
            &token,
            timeouts(30_000, 300_000),
            &QueryControls::timeout(Some(1_800_000)),
        );
        assert_eq!(
            d.effective_ms(),
            300_000,
            "an over-max ask is clamped, and this is the number _meta must report"
        );
        // And the error quotes the clamped value, not what was asked for.
        let msg = AgentError::query_timeout(d.effective_ms(), 300_000)
            .into_envelope()
            .message;
        assert!(msg.contains("300000 ms"), "{msg}");
        assert!(
            !msg.contains("1800000"),
            "never quote the unclamped ask: {msg}"
        );
    }
}
