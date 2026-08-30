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
//! Two properties everything below exists to hold:
//!
//! 1. **A sink can never fail, slow or block a query.** [`EventSink::submit`]
//!    returns `()`, is called with a query in flight, and for the HTTP sink is
//!    one `try_send` onto a bounded queue. Every POST — and therefore every
//!    dead endpoint, hung endpoint, TLS failure, DNS failure and 500 — happens
//!    on a background task the query never awaits. A full queue **drops**, with
//!    a warning; it never grows and it never blocks.
//! 2. **The posted bytes are the stored bytes.** The writer serializes an event
//!    exactly once and hands that same `String` to the file buffer and to the
//!    sink, so what a backend receives is byte-identical to what the pond's
//!    files hold and to what `get_lineage` returns. If the wire form and the
//!    stored form could drift, "OpenLineage compliant" would mean nothing.

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
}

#[cfg(feature = "http-sink")]
mod http {
    use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
    use std::time::Duration;

    use tokio::sync::mpsc;

    use super::EventSink;

    /// Events that may be waiting to be posted, per node. Bounded because the
    /// alternative is a node whose memory grows for as long as its lineage
    /// backend is down — the same argument as the writer's buffer cap, and the
    /// reason a full queue drops rather than waits. At ~1 KB an event this is
    /// roughly 1 MB.
    const QUEUE_CAPACITY: usize = 1024;

    /// Ceiling on one POST. A backend that accepts a connection and then never
    /// answers is the worst case for a sink — without this the poster task
    /// would stop forever on one event and the queue would fill behind it,
    /// silently. With it, a hung backend costs this much per event and then
    /// recovers.
    const POST_TIMEOUT: Duration = Duration::from_secs(10);

    /// POSTs each event to an OpenLineage-compatible receiver (Marquez, or
    /// anything else that speaks the standard).
    ///
    /// **No credentials.** The configuration is a URL and nothing else; a
    /// backend that needs auth is a later, explicit decision rather than a
    /// scheme invented here.
    pub struct HttpSink {
        tx: mpsc::Sender<String>,
        /// True while the queue is full, so the drop warning fires once per
        /// episode rather than once per event on an already-failing node —
        /// the pattern `LineageWriter` established for its own buffer.
        queue_full: AtomicBool,
        /// A closed queue means the poster task is gone, which is permanent.
        closed_warned: AtomicBool,
        submitted: AtomicU64,
        dropped: AtomicU64,
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
            let client = reqwest::Client::builder()
                .timeout(POST_TIMEOUT)
                .build()
                .map_err(|e| format!("could not build the lineage backend HTTP client: {e}"))?;
            let (tx, rx) = mpsc::channel(QUEUE_CAPACITY);
            tokio::spawn(post_loop(client, url, rx));
            Ok(Self {
                tx,
                queue_full: AtomicBool::new(false),
                closed_warned: AtomicBool::new(false),
                submitted: AtomicU64::new(0),
                dropped: AtomicU64::new(0),
            })
        }

        /// Events accepted onto the queue since startup. An operator asking
        /// "is anything actually leaving this node?" has nothing else to look
        /// at — the POSTs happen on a task nobody awaits.
        pub fn submitted(&self) -> u64 {
            self.submitted.load(Ordering::Relaxed)
        }

        /// Events dropped because the queue was full (the backend could not
        /// keep up, or is down). The honest companion to `submitted`.
        pub fn dropped(&self) -> u64 {
            self.dropped.load(Ordering::Relaxed)
        }
    }

    impl EventSink for HttpSink {
        fn submit(&self, event: &str) {
            match self.tx.try_send(event.to_string()) {
                Ok(()) => {
                    self.submitted.fetch_add(1, Ordering::Relaxed);
                    if self.queue_full.swap(false, Ordering::Relaxed) {
                        tracing::info!("lineage sink queue drained; posting events again");
                    }
                }
                Err(mpsc::error::TrySendError::Full(_)) => {
                    self.dropped.fetch_add(1, Ordering::Relaxed);
                    if !self.queue_full.swap(true, Ordering::Relaxed) {
                        tracing::warn!(
                            capacity = QUEUE_CAPACITY,
                            "lineage sink queue is full; dropping events until it drains \
                             (the pond's own files still have them)"
                        );
                    }
                }
                Err(mpsc::error::TrySendError::Closed(_)) => {
                    self.dropped.fetch_add(1, Ordering::Relaxed);
                    if !self.closed_warned.swap(true, Ordering::Relaxed) {
                        tracing::warn!(
                            "lineage sink poster has stopped; no further events will be posted"
                        );
                    }
                }
            }
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

    /// POST events one at a time, forever.
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
    async fn post_loop(client: reqwest::Client, url: reqwest::Url, mut rx: mpsc::Receiver<String>) {
        // Rate-limits the failure log to the transitions, like the writer's
        // `failing` flag: a node whose backend is down posts on every query and
        // would otherwise drown the log it shares with the access trail.
        let mut failing = false;
        while let Some(body) = rx.recv().await {
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
                            "lineage sink is failing; dropping posted events until it recovers \
                             (the pond's own files are unaffected)"
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
    }
}

#[cfg(feature = "http-sink")]
pub use http::HttpSink;
