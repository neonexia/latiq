//! Reading the trail back out of a pond's `lineage` directory — the other half
//! of [`crate::writer`], and the only reader of the file convention that module
//! defines.
//!
//! It lives beside the writer because everything it relies on is the writer's
//! contract, not a caller's: names are `{unix_millis:013}-{uuid}.jsonl` so
//! **sorting the names sorts by time**, an in-progress batch is `.tmp-<uuid>`
//! and must never be read, and a visible `.jsonl` is a whole batch (the rename
//! is atomic) of one JSON event per line. Newest-first therefore costs a
//! `read_dir` and a sort — no `stat`, and no parse of a file we do not return.
//!
//! Three properties the caller depends on:
//!
//! - **Verbatim events.** A line is parsed to [`serde_json::Value`] and handed
//!   on as-is. It is deliberately NOT round-tripped through `RunEvent`: an
//!   event written by a different build of Latiq may carry a facet or field
//!   this one does not know, and a consumer replaying our output into an
//!   OpenLineage backend must get everything that was recorded.
//! - **One bad line denies nothing.** A truncated or malformed line is counted
//!   and skipped, never propagated: a single torn record must not cost an agent
//!   all of its provenance.
//! - **The response is bounded** — by event count, by bytes, and by files
//!   scanned. An agent's context is the scarce resource here, and an event
//!   count alone does not bound bytes (a plan-heavy event is far larger than a
//!   `SELECT 1`).

use std::fs;
use std::path::Path;

use chrono::{DateTime, Utc};
use serde_json::Value;

/// Events returned when the caller does not choose. Modest on purpose: two
/// events per operation, and an agent asking "where did this come from?" reads
/// the answer into a finite context window.
pub const DEFAULT_LIMIT: usize = 50;

/// The most events one call may return, however large a `limit` is asked for.
pub const MAX_LIMIT: usize = 500;

/// The byte ceiling on one page of events, applied to the raw JSONL bytes.
/// Events vary in size by more than an order of magnitude, so the count cap
/// alone does not bound the response; whichever cap binds first wins.
pub const MAX_BYTES: usize = 256 * 1024;

/// Files examined before giving up and reporting the page as truncated. Bounds
/// the work a `since` far in the past — which filters, but cannot let us stop
/// early — can ask of a long-lived pond.
const MAX_FILES: usize = 500;

/// One page of events, newest first.
#[derive(Debug, Default)]
pub struct EventPage {
    /// The events, newest first, verbatim.
    pub events: Vec<Value>,
    /// More events matched than fit in this page (count, bytes, or files
    /// scanned). Not "there are older events" — a caller that read everything
    /// gets `false`.
    pub truncated: bool,
    /// Lines that were not valid JSON and were skipped. Surfaced rather than
    /// swallowed: silently returning 9 of 10 events is indistinguishable from a
    /// pond that only ever recorded 9.
    pub malformed_lines: usize,
}

/// Why a read could not be attempted at all. Distinct from a bad *line*, which
/// is skipped rather than raised.
#[derive(Debug)]
pub enum ReadError {
    /// `since` was not an RFC-3339 timestamp. The caller's mistake, and worth
    /// saying so: silently ignoring it would return events it asked to exclude.
    BadSince(String),
    /// The directory could not be listed.
    Io(std::io::Error),
}

impl std::fmt::Display for ReadError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ReadError::BadSince(s) => write!(f, "`since` is not an RFC-3339 timestamp: {s}"),
            ReadError::Io(e) => write!(f, "{e}"),
        }
    }
}

impl std::error::Error for ReadError {}

/// The newest `limit` events in `dir`, newest first, optionally only those at
/// or after `since` (RFC 3339).
///
/// `limit` is clamped to `1..=MAX_LIMIT`, and the page additionally stops at
/// [`MAX_BYTES`] — see [`EventPage::truncated`].
pub fn read_newest(dir: &Path, limit: usize, since: Option<&str>) -> Result<EventPage, ReadError> {
    let since = match since {
        Some(s) => Some(parse_since(s)?),
        None => None,
    };
    let limit = limit.clamp(1, MAX_LIMIT);

    // Only finished batches. The `.jsonl` suffix already excludes `.tmp-<uuid>`
    // (it has none); the explicit test is here so the rule survives a future
    // change to either name.
    let mut names: Vec<String> = fs::read_dir(dir)
        .map_err(ReadError::Io)?
        .filter_map(Result::ok)
        .map(|e| e.file_name().to_string_lossy().into_owned())
        .filter(|n| n.ends_with(".jsonl") && !n.starts_with(".tmp-"))
        .collect();
    names.sort();

    let mut page = EventPage::default();
    let mut bytes = 0usize;
    // One event past the limit is read on purpose: it is the only way to tell
    // "exactly a page" from "a page and more behind it" without a second pass.
    // It is dropped below.
    let target = limit + 1;

    'files: for (scanned, name) in names.iter().rev().enumerate() {
        if scanned >= MAX_FILES {
            page.truncated = true;
            break;
        }
        let path = dir.join(name);
        let body = match fs::read_to_string(&path) {
            Ok(b) => b,
            Err(error) => {
                // A file we cannot read is not a reason to deny the rest.
                tracing::warn!(%error, path = %path.display(), "skipping an unreadable lineage file");
                continue;
            }
        };
        // Within a file the lines are oldest-first, so newest-first means
        // reading it backwards.
        for line in body.lines().rev() {
            let line = line.trim();
            if line.is_empty() {
                continue;
            }
            let event: Value = match serde_json::from_str(line) {
                Ok(v) => v,
                Err(error) => {
                    page.malformed_lines += 1;
                    tracing::warn!(%error, path = %path.display(), "skipping a malformed lineage line");
                    continue;
                }
            };
            if let Some(since) = since {
                if is_older(&event, since) {
                    continue;
                }
            }
            // The ceiling never returns an empty page: one oversized event is
            // still the answer to "what happened here?".
            if !page.events.is_empty() && bytes + line.len() > MAX_BYTES {
                page.truncated = true;
                break 'files;
            }
            bytes += line.len();
            page.events.push(event);
            if page.events.len() >= target {
                break 'files;
            }
        }
    }

    if page.events.len() > limit {
        page.truncated = true;
        page.events.truncate(limit);
    }
    Ok(page)
}

fn parse_since(s: &str) -> Result<DateTime<Utc>, ReadError> {
    DateTime::parse_from_rfc3339(s)
        .map(|dt| dt.with_timezone(&Utc))
        .map_err(|_| ReadError::BadSince(s.to_string()))
}

/// Whether this event predates `since`. An event whose `eventTime` is missing
/// or unparseable is KEPT: dropping provenance over a timestamp we failed to
/// read would hide exactly the events most worth looking at.
fn is_older(event: &Value, since: DateTime<Utc>) -> bool {
    match event.get("eventTime").and_then(Value::as_str) {
        Some(t) => match DateTime::parse_from_rfc3339(t) {
            Ok(dt) => dt.with_timezone(&Utc) < since,
            Err(_) => false,
        },
        None => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    /// `n` batches of one line each, written oldest-first with distinct
    /// timestamps in the NAME (the property the reader orders by).
    fn write_batches(dir: &Path, times: &[u128]) {
        for (i, millis) in times.iter().enumerate() {
            let body = format!("{{\"eventTime\":\"2026-08-14T10:00:0{i}.000Z\",\"i\":{i}}}\n");
            fs::write(dir.join(format!("{millis:013}-aaa{i}.jsonl")), body).expect("write");
        }
    }

    fn index_of(event: &Value) -> i64 {
        event["i"].as_i64().expect("fixture events carry `i`")
    }

    #[test]
    fn reader_returns_newest_first_and_ignores_in_progress_batches() {
        // The ordering claim rests entirely on the file NAME, and the `.tmp-`
        // file is deliberately unparseable: if the reader ever picked it up,
        // the malformed count would give it away rather than it passing as a
        // skipped line.
        let dir = tempfile::tempdir().expect("tempdir");
        write_batches(dir.path(), &[1_700_000_000_001, 1_700_000_000_002]);
        fs::write(dir.path().join(".tmp-abc"), "{\"i\":9,,,").expect("write");

        let page = read_newest(dir.path(), 10, None).expect("readable");
        let order: Vec<i64> = page.events.iter().map(index_of).collect();
        assert_eq!(order, vec![1, 0], "newest batch first");
        assert_eq!(
            page.malformed_lines, 0,
            "the in-progress file must not be read at all"
        );
        assert!(!page.truncated, "everything fit");
    }

    #[test]
    fn reader_skips_a_malformed_line_and_returns_the_rest() {
        // A single torn record must not deny an agent all of its provenance.
        let dir = tempfile::tempdir().expect("tempdir");
        fs::write(
            dir.path().join("1700000000001-aaa.jsonl"),
            "{\"i\":0}\n{\"i\": tru\n{\"i\":2}\n",
        )
        .expect("write");

        let page = read_newest(dir.path(), 10, None).expect("readable");
        let order: Vec<i64> = page.events.iter().map(index_of).collect();
        assert_eq!(order, vec![2, 0], "both good lines survive, newest first");
        assert_eq!(page.malformed_lines, 1, "and the loss is reported");
    }

    #[test]
    fn reader_caps_the_page_by_count_and_says_it_did() {
        let dir = tempfile::tempdir().expect("tempdir");
        write_batches(
            dir.path(),
            &[
                1_700_000_000_001,
                1_700_000_000_002,
                1_700_000_000_003,
                1_700_000_000_004,
            ],
        );

        let page = read_newest(dir.path(), 2, None).expect("readable");
        let order: Vec<i64> = page.events.iter().map(index_of).collect();
        assert_eq!(order, vec![3, 2], "the newest two, and only two");
        assert!(page.truncated, "two of four is truncated");

        // The same read that consumes everything is NOT truncated — otherwise
        // the flag would be a constant and prove nothing above.
        let all = read_newest(dir.path(), 4, None).expect("readable");
        assert_eq!(all.events.len(), 4);
        assert!(!all.truncated);
    }

    #[test]
    fn reader_caps_the_page_by_bytes_even_when_the_count_would_fit() {
        // Why the byte ceiling exists: 20 events is a modest count and 20 MB is
        // not a response an agent can hold.
        let dir = tempfile::tempdir().expect("tempdir");
        let fat = "x".repeat(MAX_BYTES / 4);
        let body: String = (0..20)
            .map(|i| format!("{{\"i\":{i},\"pad\":\"{fat}\"}}\n"))
            .collect();
        fs::write(dir.path().join("1700000000001-aaa.jsonl"), body).expect("write");

        let page = read_newest(dir.path(), 20, None).expect("readable");
        assert!(
            page.events.len() < 20 && !page.events.is_empty(),
            "the byte ceiling must bind before the count does, got {} events",
            page.events.len()
        );
        assert!(page.truncated, "and the caller is told the page was cut");
    }

    #[test]
    fn reader_since_excludes_older_events_and_keeps_the_boundary() {
        let dir = tempfile::tempdir().expect("tempdir");
        // Event `i` is stamped 10:00:0i.
        write_batches(
            dir.path(),
            &[1_700_000_000_001, 1_700_000_000_002, 1_700_000_000_003],
        );

        let page = read_newest(dir.path(), 10, Some("2026-08-14T10:00:01.000Z")).expect("readable");
        let order: Vec<i64> = page.events.iter().map(index_of).collect();
        assert_eq!(order, vec![2, 1], "`since` is inclusive of its own instant");

        // An offset timezone is the same instant, so it must select the same
        // events — a string comparison on eventTime would fail this.
        let offset =
            read_newest(dir.path(), 10, Some("2026-08-14T11:00:01.000+01:00")).expect("readable");
        assert_eq!(
            offset.events.iter().map(index_of).collect::<Vec<_>>(),
            order
        );
    }

    #[test]
    fn reader_rejects_a_since_it_cannot_parse() {
        // Ignoring it would return events the caller asked to exclude and let
        // it conclude they happened after `since`.
        let dir = tempfile::tempdir().expect("tempdir");
        let err = read_newest(dir.path(), 10, Some("yesterday")).expect_err("must refuse");
        assert!(
            matches!(err, ReadError::BadSince(ref s) if s == "yesterday"),
            "got {err:?}"
        );
    }

    #[test]
    fn reader_hands_back_fields_it_does_not_know() {
        // Verbatim, not round-tripped through RunEvent: an event written by
        // another build carries facets this one has never heard of, and a
        // consumer replaying into an OpenLineage backend must still get them.
        let dir = tempfile::tempdir().expect("tempdir");
        fs::write(
            dir.path().join("1700000000001-aaa.jsonl"),
            "{\"i\":0,\"run\":{\"facets\":{\"fromTheFuture\":{\"x\":1}}}}\n",
        )
        .expect("write");

        let page = read_newest(dir.path(), 10, None).expect("readable");
        assert_eq!(page.events[0]["run"]["facets"]["fromTheFuture"]["x"], 1);
    }
}
