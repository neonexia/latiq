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

//! The optional second sink: an OpenLineage HTTP backend.
//!
//! **This module is the one deliberate exception to the crate's protocol
//! neutrality**, and it is shaped so the exception cannot spread:
//!
//! - [`EventSink`] — a trait over `&str`, always compiled, with no transport in
//!   it. That is all [`crate::writer`] and `latiq-agent-core` (itself
//!   protocol-neutral, invariant 5) ever see.
//! - [`HttpSink`] — the only HTTP thing here, behind the **`http-sink` Cargo
//!   feature**, which nothing but `latiq-pond-node` turns on. With the feature
//!   off, `reqwest` and `tokio` are not even dependencies of this crate, so
//!   "the neutral crate does not link a transport" is enforced by Cargo rather
//!   than by a reviewer noticing.
//!
//! Three properties everything below exists to hold:
//!
//! 1. **A sink can never fail, slow or block a query.** [`EventSink::submit`]
//!    returns `()`, is called with a query in flight, and for the HTTP sink is
//!    a `push_back` under a mutex that is never held across an `await`. Every
//!    POST — and therefore every dead endpoint, hung endpoint, TLS failure, DNS
//!    failure and 500 — happens on a background task the query never awaits. A
//!    full queue **drops**; it never grows and it never blocks.
//! 2. **The posted bytes are the stored bytes.** The writer serializes an event
//!    exactly once and hands that same `String` to the file buffer and to the
//!    sink, so what a backend receives is byte-identical to what the pond's
//!    files hold and to what `get_lineage` returns. If the wire form and the
//!    stored form could drift, "OpenLineage compliant" would mean nothing.
//! 3. **Delivery is best effort, and says so.** A failed POST is dropped, never
//!    retried, and a full queue drops the **oldest** events. The pond's own
//!    files keep every event regardless; the sink is a mirror, not a queue with
//!    a delivery guarantee. [`EventSink::drain`] is what stops a node shutdown
//!    from throwing away the mirror's backlog, and it too is bounded.

use std::future::Future;
use std::pin::Pin;
use std::time::Duration;

/// A consumer of one already-serialized OpenLineage event.
///
/// `&str` and not `&RunEvent` on purpose: the bytes are what compliance is
/// about, and re-serializing per sink is the one way the wire form and the
/// stored form could ever disagree.
///
/// **Implementations must not block and must not fail.** They are called from
/// the query hot path, under the writer's own discipline: swallow, warn, drop.
pub trait EventSink: Send + Sync {
    /// Hand over one serialized event (no trailing newline).
    fn submit(&self, event: &str);

    /// Deliver whatever is still queued, giving up after `budget`.
    ///
    /// Called **once, on shutdown**, after the pond files have been flushed —
    /// never on the query path, which is why this one may await. Without it a
    /// SIGTERM would discard the whole backlog, which is precisely the window
    /// the sink exists to carry off the node: a backend that was down for
    /// thirty seconds before the signal has thirty seconds of events queued.
    ///
    /// Bounded on purpose, and best-effort by contract: a sink that cannot
    /// finish inside `budget` must give up and return rather than hold the
    /// process open. Losing some events is strictly better than a node that
    /// will not die.
    ///
    /// The default does nothing, which is correct for a sink that queues
    /// nothing.
    fn drain(&self, budget: Duration) -> Pin<Box<dyn Future<Output = ()> + Send + '_>> {
        let _ = budget;
        Box::pin(std::future::ready(()))
    }
}

#[cfg(feature = "http-sink")]
mod http {
    use std::collections::VecDeque;
    use std::future::Future;
    use std::pin::Pin;
    use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
    use std::sync::{Arc, Mutex};
    use std::time::{Duration, Instant};

    use tokio::sync::Notify;

    use super::EventSink;

    /// Events that may be waiting to be posted, per node. Bounded because the
    /// alternative is a node whose memory grows for as long as its lineage
    /// backend is down — the same argument as the writer's buffer cap. At ~1 KB
    /// an event this is roughly 1 MB.
    const QUEUE_CAPACITY: usize = 1024;

    /// Ceiling on one POST. A backend that accepts a connection and then never
    /// answers is the worst case for a sink — without this the poster task
    /// would stop forever on one event and the queue would fill behind it,
    /// silently. With it, a hung backend costs this much per event and then
    /// recovers.
    ///
    /// **It composes with the caller's shutdown budget, and the two are not
    /// independent.** [`super::EventSink::drain`] is bounded by whatever budget
    /// the node hands it, and it cannot outrun a POST that is already in
    /// flight — so a budget shorter than this makes the drain unreachable in
    /// exactly the case it exists for (a backend that hangs), and one hung
    /// request would discard the whole backlog. Public for that reason:
    /// `latiq-pond-node` asserts its `SHUTDOWN_BUDGET` against it at compile
    /// time, so lowering the budget below this fails the build rather than
    /// quietly disabling the drain.
    pub const POST_TIMEOUT: Duration = Duration::from_secs(10);

    /// The queue, its counters, and the two wakeups. Shared by the sink handle
    /// (which pushes) and the poster task (which pops), so the poster can be
    /// spawned once and outlive any particular caller.
    #[derive(Default)]
    struct Shared {
        /// A `std::sync::Mutex`, never held across an `await` — `submit` is
        /// called with a query in flight and must not be able to park on a
        /// runtime-aware lock.
        queue: Mutex<VecDeque<String>>,
        /// Producer → poster: there is something to post (or it is time to
        /// stop). `notify_one` stores a permit when nobody is waiting, so a
        /// wakeup racing the poster's empty-queue check is never lost.
        work: Notify,
        /// Poster → drainer: one event has been dealt with. Same permit
        /// semantics, for the same reason.
        progress: Notify,
        /// Every event ever accepted. `settled` chases this.
        enqueued: AtomicU64,
        /// Events POSTed (successfully or not) — the poster is done with them.
        posted: AtomicU64,
        /// Events evicted from a full queue, never POSTed.
        dropped: AtomicU64,
        /// True while the queue is at capacity, so the eviction warning fires
        /// once per episode rather than once per event on an already-failing
        /// node — the pattern `LineageWriter` established for its own buffer.
        overflowing: AtomicBool,
        /// A poisoned queue mutex is permanent, so this warns exactly once.
        poison_warned: AtomicBool,
        /// Set when the last `HttpSink` handle drops, so the poster task ends
        /// instead of parking on `work` forever.
        closing: AtomicBool,
    }

    impl Shared {
        /// Push one event, evicting the **oldest** if that puts the queue over
        /// capacity.
        ///
        /// **Drop-oldest, matching `LineageWriter::enforce_capacity`**, and for
        /// its reason: the queue only fills when the backend is failing, and
        /// the events nearest the failure are the ones an investigation wants.
        /// Dropping the newest — which is what a bounded channel's `try_send`
        /// does, and what this sink used to do — would discard exactly the
        /// window the sink exists to preserve.
        fn push(&self, event: &str) {
            let Ok(mut queue) = self.queue.lock() else {
                // A panic elsewhere poisoned the lock. Nothing to recover and
                // nothing to fail: drop the event, say so once.
                if !self.poison_warned.swap(true, Ordering::Relaxed) {
                    tracing::warn!("lineage sink queue is poisoned; dropping events from now on");
                }
                return;
            };
            queue.push_back(event.to_string());
            self.enqueued.fetch_add(1, Ordering::Relaxed);
            if queue.len() > QUEUE_CAPACITY {
                let excess = queue.len() - QUEUE_CAPACITY;
                queue.drain(..excess);
                self.dropped.fetch_add(excess as u64, Ordering::Relaxed);
                if !self.overflowing.swap(true, Ordering::Relaxed) {
                    tracing::warn!(
                        capacity = QUEUE_CAPACITY,
                        "lineage sink queue is full; dropping the OLDEST events until it drains \
                         (the pond's own files still have every one of them)"
                    );
                }
            } else if queue.len() < QUEUE_CAPACITY
                && self.overflowing.swap(false, Ordering::Relaxed)
            {
                tracing::info!("lineage sink queue drained; posting every event again");
            }
        }

        fn pop(&self) -> Option<String> {
            self.queue.lock().ok()?.pop_front()
        }

        /// Events that will never be posted plus events already posted — i.e.
        /// everything the poster is finished with. `drain` waits for this to
        /// reach the `enqueued` count it snapshotted.
        fn settled(&self) -> u64 {
            self.posted.load(Ordering::Relaxed) + self.dropped.load(Ordering::Relaxed)
        }
    }

    /// POSTs each event to an OpenLineage-compatible receiver (Marquez, or
    /// anything else that speaks the standard).
    ///
    /// **No credentials.** The configuration is a URL and nothing else; a
    /// backend that needs auth is a later, explicit decision rather than a
    /// scheme invented here.
    pub struct HttpSink {
        shared: Arc<Shared>,
    }

    impl HttpSink {
        /// Validate the URL, build the client, and start the poster.
        ///
        /// Fallible **on purpose, and only here**: a malformed backend URL is a
        /// configuration error the operator can fix, and it must stop the node
        /// at startup rather than turn into a warning per query forever. Once
        /// built, nothing this sink does can fail upwards.
        ///
        /// Requires a Tokio runtime (it spawns).
        pub fn new(url: &str) -> Result<Self, String> {
            let url = validate_url(url)?;
            if is_plaintext_remote(&url) {
                // Not an error: a Marquez on a private network over http is a
                // legitimate deployment. But every body carries pond and table
                // names, redacted SQL, and the caller's subject and issuer, and
                // since no credentials are supported there is nothing else that
                // would make an operator notice the traffic is in the clear.
                tracing::warn!(
                    backend = %url,
                    "the lineage backend URL is plaintext http to a non-loopback host: pond and \
                     table names, redacted SQL and caller identity will leave this node \
                     unencrypted. Use https unless the network is already trusted."
                );
            }
            let client = reqwest::Client::builder()
                .timeout(POST_TIMEOUT)
                .build()
                .map_err(|e| format!("could not build the lineage backend HTTP client: {e}"))?;
            let shared = Arc::new(Shared::default());
            tokio::spawn(post_loop(client, url, shared.clone()));
            Ok(Self { shared })
        }

        /// Events accepted onto the queue since startup.
        ///
        /// This and [`HttpSink::dropped`] are what `latiq-pond-node` publishes
        /// as the `latiq_lineage_sink_*` gauges — the POSTs happen on a task
        /// nobody awaits, so without them an operator has no way to answer "is
        /// anything actually leaving this node?".
        pub fn submitted(&self) -> u64 {
            self.shared.enqueued.load(Ordering::Relaxed)
        }

        /// Events evicted from a full queue, never posted. The honest companion
        /// to [`HttpSink::submitted`]: a backend that cannot keep up shows up
        /// here and nowhere else.
        pub fn dropped(&self) -> u64 {
            self.shared.dropped.load(Ordering::Relaxed)
        }

        /// Events handed to the backend, whether it accepted them or not — a
        /// POST that returned 500 is posted and gone, because nothing is
        /// retried.
        pub fn posted(&self) -> u64 {
            self.shared.posted.load(Ordering::Relaxed)
        }
    }

    impl EventSink for HttpSink {
        fn submit(&self, event: &str) {
            self.shared.push(event);
            self.shared.work.notify_one();
        }

        fn drain(&self, budget: Duration) -> Pin<Box<dyn Future<Output = ()> + Send + '_>> {
            Box::pin(async move {
                // Snapshotted, not re-read: draining must not chase events
                // that arrive after the shutdown began, or a node still taking
                // traffic could never finish.
                let target = self.shared.enqueued.load(Ordering::Relaxed);
                let deadline = Instant::now() + budget;
                loop {
                    let settled = self.shared.settled();
                    if settled >= target {
                        return;
                    }
                    let left = deadline.saturating_duration_since(Instant::now());
                    if left.is_zero() {
                        // Named, not silent: this is the one place events are
                        // lost on a clean shutdown, and an operator who sees a
                        // gap deserves to find out why here.
                        tracing::warn!(
                            unposted = target - settled,
                            budget_ms = budget.as_millis() as u64,
                            "lineage sink did not finish posting before the shutdown budget ran \
                             out; the remaining events are lost from the backend (the pond's own \
                             files still have them)"
                        );
                        return;
                    }
                    let _ = tokio::time::timeout(left, self.shared.progress.notified()).await;
                }
            })
        }
    }

    impl Drop for HttpSink {
        /// Let the poster task end. Without this it would park on `work`
        /// forever holding its `Arc<Shared>`, which leaks a task and a queue
        /// per sink — invisible on a node (there is one, for the process's
        /// life) and not invisible in a test binary that builds several.
        fn drop(&mut self) {
            self.shared.closing.store(true, Ordering::Relaxed);
            self.shared.work.notify_one();
        }
    }

    /// A URL we can actually POST to, or a message naming what is wrong with
    /// the one configured. `http`/`https` only: any other scheme parses fine
    /// and then fails on every single event.
    fn validate_url(url: &str) -> Result<reqwest::Url, String> {
        let parsed = reqwest::Url::parse(url).map_err(|e| {
            format!(
                "--lineage-backend-url is not a URL ('{url}'): {e}. It is the FULL endpoint to \
                 POST events to, e.g. http://marquez:5000/api/v1/lineage."
            )
        })?;
        match parsed.scheme() {
            "http" | "https" => Ok(parsed),
            other => Err(format!(
                "--lineage-backend-url must be http or https, got '{other}' ('{url}'). It is the \
                 FULL endpoint to POST events to, e.g. http://marquez:5000/api/v1/lineage."
            )),
        }
    }

    /// Whether events would cross a network in the clear. Loopback is exempt:
    /// `dev.sh`, the SDK's embedded stack and every test dial `127.0.0.1`, and
    /// warning there would train an operator to ignore the warning that matters.
    fn is_plaintext_remote(url: &reqwest::Url) -> bool {
        if url.scheme() != "http" {
            return false;
        }
        let Some(host) = url.host_str() else {
            return false; // no host to reach at all; `new` never gets this far
        };
        // `host_str` keeps IPv6 brackets.
        let host = host.trim_start_matches('[').trim_end_matches(']');
        if host.eq_ignore_ascii_case("localhost") || host.eq_ignore_ascii_case("localhost.") {
            return false;
        }
        match host.parse::<std::net::IpAddr>() {
            Ok(ip) => !ip.is_loopback(),
            // A name we cannot resolve here. Assume it is remote: the warning
            // is advisory, and a false positive costs one log line where a
            // false negative costs silence about cleartext identity data.
            Err(_) => true,
        }
    }

    /// POST events one at a time until the sink is dropped.
    ///
    /// Modelled on the node's heartbeat loop: it tolerates a dead endpoint,
    /// keeps going, and needs no supervision to recover — `reqwest` pools and
    /// re-dials, so a backend that comes back is used again on the next event.
    ///
    /// One event per request because that is what the OpenLineage HTTP API
    /// takes, and because it is what makes the body byte-identical to the
    /// stored line.
    ///
    /// **A failed POST is dropped, not retried.** Retrying would mean holding
    /// the event while more arrive behind it, which is how a bounded queue
    /// turns into a queue that is always full. The local files are the
    /// durability answer for a pond that still exists; the backend is the
    /// durability answer for one that does not.
    async fn post_loop(client: reqwest::Client, url: reqwest::Url, shared: Arc<Shared>) {
        // Rate-limits the failure log to the transitions, like the writer's
        // `failing` flag: a node whose backend is down posts on every query and
        // would otherwise drown the log it shares with the access trail.
        let mut failing = false;
        loop {
            let Some(body) = shared.pop() else {
                // Checked only when the queue is empty, so a drop never
                // discards a backlog `drain` might still be waiting on.
                if shared.closing.load(Ordering::Relaxed) {
                    return;
                }
                shared.work.notified().await;
                continue;
            };
            let result = client
                .post(url.clone())
                .header(reqwest::header::CONTENT_TYPE, "application/json")
                .body(body)
                .send()
                .await;
            let failure = match result {
                Ok(response) if response.status().is_success() => None,
                Ok(response) => Some(format!("backend answered {}", response.status())),
                Err(error) => Some(error.to_string()),
            };
            // Counted BEFORE the wakeup, so a drainer that wakes sees it.
            shared.posted.fetch_add(1, Ordering::Relaxed);
            shared.progress.notify_one();
            match failure {
                None => {
                    if std::mem::replace(&mut failing, false) {
                        tracing::info!(backend = %url, "lineage sink recovered");
                    }
                }
                Some(reason) => {
                    if !std::mem::replace(&mut failing, true) {
                        tracing::warn!(
                            backend = %url,
                            reason,
                            "lineage sink is failing; posted events are being dropped (never \
                             retried) until it recovers. The pond's own files are unaffected."
                        );
                    }
                }
            }
        }
    }

    #[cfg(test)]
    mod tests {
        use super::*;

        #[test]
        fn only_an_http_url_is_accepted() {
            // Checked once at startup so a typo stops the node, instead of
            // becoming a warning on every query for the life of the process.
            // The positive case is what stops this passing vacuously.
            assert_eq!(
                validate_url("http://marquez:5000/api/v1/lineage")
                    .expect("a plain http endpoint is valid")
                    .as_str(),
                "http://marquez:5000/api/v1/lineage"
            );
            let scheme = validate_url("file:///tmp/events").expect_err("file:// cannot be POSTed");
            assert!(
                scheme.contains("must be http or https"),
                "the error must name the scheme rule, got {scheme}"
            );
            let unparsed =
                validate_url("/api/v1/lineage").expect_err("a bare path is not an absolute URL");
            assert!(
                unparsed.contains("FULL endpoint"),
                "the error must say what a correct value looks like, got {unparsed}"
            );
        }

        #[test]
        fn plaintext_is_flagged_only_when_it_leaves_the_host() {
            // Every event body carries pond and table names, redacted SQL and
            // the caller's subject -- and no credential is ever sent, so this
            // warning is the only thing that would make an operator notice the
            // traffic is in the clear. Loopback must NOT warn: dev.sh, the
            // embedded SDK and every test dial 127.0.0.1, and a warning there
            // would train the operator to ignore the one that matters.
            for (url, expected) in [
                ("http://marquez:5000/api/v1/lineage", true),
                ("http://10.0.0.7:5000/api/v1/lineage", true),
                ("https://marquez.example/api/v1/lineage", false),
                ("http://127.0.0.1:5000/api/v1/lineage", false),
                ("http://localhost:5000/api/v1/lineage", false),
                ("http://[::1]:5000/api/v1/lineage", false),
            ] {
                let parsed = validate_url(url).expect("fixture urls are valid");
                assert_eq!(
                    is_plaintext_remote(&parsed),
                    expected,
                    "wrong plaintext verdict for {url}"
                );
            }
        }

        #[test]
        fn a_full_queue_drops_the_oldest_event() {
            // Pins the divergence this replaced: a bounded channel's `try_send`
            // drops the NEWEST, which on a failing backend discards exactly the
            // events nearest the failure -- the opposite of what
            // `LineageWriter::enforce_capacity` reasons its way to, and the
            // window an investigation actually wants. Asserting WHICH events
            // survive is the whole point; a length check would pass either way.
            let shared = Shared::default();
            for i in 0..(QUEUE_CAPACITY + 3) {
                shared.push(&format!("{{\"i\":{i}}}"));
            }
            let queue = shared.queue.lock().expect("queue lock");
            assert_eq!(queue.len(), QUEUE_CAPACITY, "the queue must stay bounded");
            assert_eq!(
                queue.front().map(String::as_str),
                Some("{\"i\":3}"),
                "the three OLDEST events must be the ones evicted"
            );
            assert_eq!(
                queue.back().map(String::as_str),
                Some(format!("{{\"i\":{}}}", QUEUE_CAPACITY + 2)).as_deref(),
                "the newest event must always survive"
            );
            drop(queue);
            assert_eq!(shared.dropped.load(Ordering::Relaxed), 3);
            assert_eq!(
                shared.enqueued.load(Ordering::Relaxed),
                QUEUE_CAPACITY as u64 + 3
            );
        }
    }
}

#[cfg(feature = "http-sink")]
pub use http::{HttpSink, POST_TIMEOUT};
