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

//! Reading the trail back out of a pond's `lineage` directory — the other half
//! of [`crate::writer`], and the only reader of the file convention that module
//! defines.
//!
//! It lives beside the writer because everything it relies on is the writer's
//! contract, not a caller's: names are `{unix_millis:013}-{uuid}.jsonl` so
//! **sorting the names sorts by time**, an in-progress batch is `.tmp-<uuid>`
//! and must never be read, and a visible `.jsonl` is a whole batch (the rename
//! is atomic) of one JSON event per line. Newest-first therefore costs a
//! `read_dir` and a bounded sort — no `stat`, and no parse of a file we do not
//! return.
//!
//! Four properties the caller depends on:
//!
//! - **Verbatim events.** A line is parsed to [`serde_json::Value`] and handed
//!   on as-is. It is deliberately NOT round-tripped through `RunEvent`: an
//!   event written by a different build of Latiq may carry a facet or field
//!   this one does not know, and a consumer replaying our output into an
//!   OpenLineage backend must get everything that was recorded.
//! - **One bad line denies nothing.** A truncated or malformed line is counted
//!   and skipped, never propagated: a single torn record must not cost an agent
//!   all of its provenance. A file that cannot be read at all is counted too
//!   ([`EventPage::unreadable_files`]) — an answer missing a whole 64-event
//!   batch must not look like a complete one.
//! - **Bounded memory, not just a bounded response.** Files are streamed line
//!   by line into a ring holding only the events that could still make the
//!   page, so a single huge batch (the writer's buffer cap is 10 000 events)
//!   is never materialised. The directory listing is bounded the same way.
//! - **A page is never cut in the middle of one `eventTime`**, which is what
//!   makes `before` an exact cursor — see [`read_newest`].

use std::cmp::Reverse;
use std::collections::{BinaryHeap, VecDeque};
use std::fs;
use std::io::{BufRead, BufReader};
use std::path::Path;

use chrono::{DateTime, Utc};
use serde_json::Value;

/// The most events one call may return, however large a `limit` is asked for.
/// A caller's `limit` is clamped to this rather than refused: the cap exists to
/// protect the caller's own context, and silently returning less is the
/// harmless direction (the page says `truncated`).
pub const MAX_LIMIT: usize = 500;

/// The byte ceiling on one page of events, applied to the raw JSONL bytes.
/// Events vary in size by more than an order of magnitude, so the count cap
/// alone does not bound the response; whichever cap binds first wins.
pub const MAX_BYTES: usize = 256 * 1024;

/// Newest files examined in one call. Bounds the work a filter that matches
/// nothing recent can ask of a long-lived pond: `before` and `since` are
/// applied per event, and a filter that excludes everything in the newest files
/// would otherwise walk the pond's whole history looking for a page it will
/// never fill. Older files beyond this are reported as `truncated` rather than
/// silently ignored.
///
/// It bounds the *listing* too, not only the reads: names are kept in a heap of
/// this size, so a pond with a million batches costs a bounded allocation.
const MAX_FILES: usize = 500;

/// What to select. A struct rather than four positional arguments so a call
/// site cannot transpose the two timestamps — they mean opposite things.
#[derive(Debug, Default, Clone, Copy)]
pub struct PageRequest<'a> {
    /// Events to return. Clamped to `1..=MAX_LIMIT`.
    pub limit: usize,
    /// **Inclusive** lower bound (RFC 3339): keep events at or after it. "What
    /// happened since I last looked."
    pub since: Option<&'a str>,
    /// **Exclusive** upper bound (RFC 3339): keep events strictly before it.
    /// The backward-paging cursor — see [`read_newest`].
    pub before: Option<&'a str>,
}

/// One page of events, newest first.
#[derive(Debug, Default)]
pub struct EventPage {
    /// The events, newest first, verbatim.
    pub events: Vec<Value>,
    /// Events matched that this page does not contain — because a cap bound
    /// (count, bytes) or because older files were left unexamined. A caller
    /// that read everything gets `false`.
    pub truncated: bool,
    /// Lines that were not valid JSON and were skipped. Surfaced rather than
    /// swallowed: silently returning 9 of 10 events is indistinguishable from a
    /// pond that only ever recorded 9.
    pub malformed_lines: usize,
    /// Whole batch files that could not be read (bad UTF-8, an IO error). Same
    /// reasoning as `malformed_lines`, one level up — a missing batch is 64
    /// missing events. A file that *vanished* between listing and opening is
    /// NOT counted: a concurrent `drop_pond` is expected, not a fault.
    pub unreadable_files: usize,
}

/// Why a read could not be attempted at all. Distinct from a bad *line* or a
/// bad *file*, which are counted rather than raised.
#[derive(Debug)]
pub enum ReadError {
    /// A bound was not an RFC-3339 timestamp. The caller's mistake, and worth
    /// saying so: silently ignoring it would return events it asked to exclude.
    BadTimestamp { field: &'static str, value: String },
    /// The directory could not be listed.
    Io(std::io::Error),
}

impl std::fmt::Display for ReadError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ReadError::BadTimestamp { field, value } => {
                write!(f, "`{field}` is not an RFC-3339 timestamp: {value}")
            }
            ReadError::Io(e) => write!(f, "{e}"),
        }
    }
}

impl std::error::Error for ReadError {}

/// The newest events in `dir` matching `req`, newest first.
///
/// **The paging contract.** `before` is exclusive and `since` is inclusive, so
/// an agent walks a pond's history backwards by passing the oldest `eventTime`
/// it received as the next call's `before`. That is exact — no event is
/// returned twice and none is skipped — because a page is never cut in the
/// middle of one `eventTime`: when a cap binds, every event sharing the
/// timestamp of the first excluded event is dropped from the page too, so the
/// cursor always lands on a clean boundary.
///
/// The one exception is degenerate and documented rather than fixed: if EVERY
/// event in a full page shares a single `eventTime` (more than `limit` events
/// recorded in the same millisecond), the page is returned uncut — a cursor
/// taken from it could skip the rest of that millisecond. Ask for a larger
/// `limit` if a pond can really do that.
pub fn read_newest(dir: &Path, req: PageRequest<'_>) -> Result<EventPage, ReadError> {
    let since = parse_bound("since", req.since)?;
    let before = parse_bound("before", req.before)?;
    let limit = req.limit.clamp(1, MAX_LIMIT);

    let mut page = EventPage::default();
    let (names, skipped_files) = newest_names(dir)?;
    // Older batches exist that this call never opened: that is a truncation,
    // and a caller told otherwise would read a partial answer as the whole
    // history.
    page.truncated = skipped_files > 0;

    // The event that did NOT fit. Its timestamp is the boundary the page is cut
    // on, so the caller's next `before` cannot skip anything sharing it.
    let mut excluded_time: Option<Option<String>> = None;
    let mut bytes = 0usize;

    'files: for name in &names {
        let path = dir.join(name);
        // Only as many events as could still make the page, so one huge batch
        // is never held in memory.
        let tail = match read_tail(
            &path,
            limit - page.events.len() + 1,
            since,
            before,
            &mut page,
        ) {
            Some(tail) => tail,
            None => continue, // counted in `page` already
        };
        // The ring holds this file's newest candidates in file order; the page
        // is newest-first.
        for (event, len) in tail.ring.into_iter().rev() {
            if page.events.len() >= limit || (!page.events.is_empty() && bytes + len > MAX_BYTES) {
                excluded_time = Some(event_time(&event).map(str::to_string));
                page.truncated = true;
                break 'files;
            }
            bytes += len;
            page.events.push(event);
        }
        // The ring itself dropped matching events (it retains only what could
        // still make the page), and we consumed everything it kept — so the
        // newest thing it dropped is the first event this page excludes. Losing
        // that would leave the page silently short AND give the caller a cursor
        // that skips those events.
        if let Some(dropped) = tail.newest_dropped {
            excluded_time = Some(dropped);
            page.truncated = true;
            break 'files;
        }
    }

    if let Some(boundary) = excluded_time {
        cut_on_a_timestamp_boundary(&mut page.events, boundary.as_deref());
    }
    Ok(page)
}

/// Drop the trailing events that share `boundary` — the timestamp of the first
/// event that did not fit — so a cursor taken from this page cannot skip them.
/// If that would empty the page (every event in it shares one timestamp), the
/// page is left alone: an empty page is a worse answer than an uncut one.
fn cut_on_a_timestamp_boundary(events: &mut Vec<Value>, boundary: Option<&str>) {
    let Some(boundary) = boundary else {
        return; // no timestamp to compare against; nothing safe to cut
    };
    let keep = events
        .iter()
        .position(|e| event_time(e) == Some(boundary))
        .unwrap_or(events.len());
    if keep > 0 {
        events.truncate(keep);
    }
}

/// The newest `MAX_FILES` batch names, newest first, plus how many older ones
/// were left out. A bounded heap rather than "collect everything and sort": the
/// listing itself must not grow with the pond's history.
fn newest_names(dir: &Path) -> Result<(Vec<String>, usize), ReadError> {
    // A min-heap of the greatest names seen: the smallest (oldest) falls out.
    let mut newest: BinaryHeap<Reverse<String>> = BinaryHeap::with_capacity(MAX_FILES + 1);
    let mut skipped = 0usize;
    for entry in fs::read_dir(dir).map_err(ReadError::Io)? {
        let Ok(entry) = entry else { continue };
        let name = entry.file_name().to_string_lossy().into_owned();
        // Only finished batches. The `.jsonl` suffix already excludes
        // `.tmp-<uuid>` (it has none); the explicit test is here so the rule
        // survives a future change to either name.
        if !name.ends_with(".jsonl") || name.starts_with(".tmp-") {
            continue;
        }
        newest.push(Reverse(name));
        if newest.len() > MAX_FILES {
            newest.pop();
            skipped += 1;
        }
    }
    // Ascending by `Reverse` == descending by name == newest first.
    let names = newest.into_sorted_vec().into_iter().map(|r| r.0).collect();
    Ok((names, skipped))
}

/// What one batch file contributed: its newest matching events, and — if the
/// ring had to drop some to stay bounded — the `eventTime` of the newest event
/// it dropped, which is the first event any page built from this ring excludes.
struct Tail {
    ring: VecDeque<(Value, usize)>,
    newest_dropped: Option<Option<String>>,
}

/// The last `need` matching events of one batch file, in file order, streamed.
///
/// `None` means the file contributed nothing and the reason is already recorded
/// on `page` (or was a benign disappearance). Lines are parsed as they are read
/// because the timestamp filters decide what is worth retaining, and retention
/// is what bounds the memory.
fn read_tail(
    path: &Path,
    need: usize,
    since: Option<DateTime<Utc>>,
    before: Option<DateTime<Utc>>,
    page: &mut EventPage,
) -> Option<Tail> {
    let file = match fs::File::open(path) {
        Ok(f) => f,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            // Expected: a concurrent `drop_pond` reaped the pond's files
            // between the listing and this open. Not a fault, so not counted.
            return None;
        }
        Err(error) => {
            page.unreadable_files += 1;
            tracing::warn!(%error, path = %path.display(), "lineage batch could not be opened");
            return None;
        }
    };

    let mut ring: VecDeque<(Value, usize)> = VecDeque::with_capacity(need.min(MAX_LIMIT) + 1);
    let mut ring_bytes = 0usize;
    // The ring pops the OLDEST it holds, so successive pops are progressively
    // newer: the last one is the newest event this file will not contribute.
    let mut newest_dropped: Option<Option<String>> = None;
    for line in BufReader::new(file).lines() {
        let line = match line {
            Ok(l) => l,
            Err(error) => {
                // Invalid UTF-8 or an IO fault mid-file: the rest of the file
                // is not reliably readable, so stop and account for it.
                page.unreadable_files += 1;
                tracing::warn!(%error, path = %path.display(), "lineage batch could not be read to the end");
                break;
            }
        };
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
        if !selected(&event, since, before) {
            continue;
        }
        ring_bytes += line.len();
        ring.push_back((event, line.len()));
        // Retain only what could still make the page: `need` events, and no
        // more raw bytes than a whole page may carry (plus the one event that
        // will prove the page was cut).
        while ring.len() > need || (ring.len() > 1 && ring_bytes > MAX_BYTES) {
            if let Some((dropped, len)) = ring.pop_front() {
                ring_bytes -= len;
                newest_dropped = Some(event_time(&dropped).map(str::to_string));
            }
        }
    }
    Some(Tail {
        ring,
        newest_dropped,
    })
}

fn parse_bound(field: &'static str, raw: Option<&str>) -> Result<Option<DateTime<Utc>>, ReadError> {
    raw.map(|s| {
        DateTime::parse_from_rfc3339(s)
            .map(|dt| dt.with_timezone(&Utc))
            .map_err(|_| ReadError::BadTimestamp {
                field,
                value: s.to_string(),
            })
    })
    .transpose()
}

fn event_time(event: &Value) -> Option<&str> {
    event.get("eventTime").and_then(Value::as_str)
}

/// Whether this event is in `[since, before)`. An event whose `eventTime` is
/// missing or unparseable is KEPT: dropping provenance over a timestamp we
/// failed to read would hide exactly the events most worth looking at.
fn selected(event: &Value, since: Option<DateTime<Utc>>, before: Option<DateTime<Utc>>) -> bool {
    if since.is_none() && before.is_none() {
        return true;
    }
    let Some(t) = event_time(event).and_then(|t| DateTime::parse_from_rfc3339(t).ok()) else {
        return true;
    };
    let t = t.with_timezone(&Utc);
    since.is_none_or(|s| t >= s) && before.is_none_or(|b| t < b)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    fn page(limit: usize) -> PageRequest<'static> {
        PageRequest {
            limit,
            since: None,
            before: None,
        }
    }

    /// One batch per timestamp, one line each, with a distinct `eventTime` —
    /// event `i` is stamped `10:00:0i`.
    fn write_batches(dir: &Path, times: &[u128]) {
        for (i, millis) in times.iter().enumerate() {
            let body = format!("{{\"eventTime\":\"2026-08-14T10:00:0{i}.000Z\",\"i\":{i}}}\n");
            fs::write(dir.join(format!("{millis:013}-aaa{i}.jsonl")), body).expect("write");
        }
    }

    fn index_of(event: &Value) -> i64 {
        event["i"].as_i64().expect("fixture events carry `i`")
    }

    fn indices(page: &EventPage) -> Vec<i64> {
        page.events.iter().map(index_of).collect()
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

        let p = read_newest(dir.path(), page(10)).expect("readable");
        assert_eq!(indices(&p), vec![1, 0], "newest batch first");
        assert_eq!(
            p.malformed_lines, 0,
            "the in-progress file must not be read at all"
        );
        assert!(!p.truncated, "everything fit");
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

        let p = read_newest(dir.path(), page(10)).expect("readable");
        assert_eq!(
            indices(&p),
            vec![2, 0],
            "both good lines survive, newest first"
        );
        assert_eq!(p.malformed_lines, 1, "and the loss is reported");
        assert_eq!(p.unreadable_files, 0, "the file itself read fine");
    }

    #[test]
    fn reader_reports_a_file_it_could_not_read() {
        // The finding this pins: a whole batch vanishing silently made a page
        // missing 64 events indistinguishable from a complete one.
        let dir = tempfile::tempdir().expect("tempdir");
        write_batches(dir.path(), &[1_700_000_000_001]);
        // Invalid UTF-8 mid-file: `BufRead::lines` fails, `read_dir` does not.
        fs::write(
            dir.path().join("1700000000002-bad.jsonl"),
            [b'{', 0xff, 0xfe, b'}', b'\n'],
        )
        .expect("write");

        let p = read_newest(dir.path(), page(10)).expect("readable");
        assert_eq!(
            p.unreadable_files, 1,
            "the caller must be able to tell this page is short"
        );
        assert_eq!(
            indices(&p),
            vec![0],
            "and the readable batch still comes back"
        );
    }

    #[test]
    fn reader_ignores_a_batch_that_vanished_under_it() {
        // A concurrent drop_pond is expected, not a fault: the pond's files go
        // away between the listing and the open, and that must not be reported
        // as corruption.
        let dir = tempfile::tempdir().expect("tempdir");
        write_batches(dir.path(), &[1_700_000_000_001]);
        let gone = dir.path().join("1700000000002-gone.jsonl");
        fs::write(&gone, "{\"i\":9}\n").expect("write");
        let (names, _) = newest_names(dir.path()).expect("listed");
        assert_eq!(names.len(), 2, "both files were listed");
        fs::remove_file(&gone).expect("remove");

        let p = read_newest(dir.path(), page(10)).expect("readable");
        assert_eq!(p.unreadable_files, 0, "a reaped file is not a corrupt one");
        assert_eq!(indices(&p), vec![0]);
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

        let p = read_newest(dir.path(), page(2)).expect("readable");
        assert_eq!(indices(&p), vec![3, 2], "the newest two, and only two");
        assert!(p.truncated, "two of four is truncated");

        // The same read that consumes everything is NOT truncated — otherwise
        // the flag would be a constant and prove nothing above.
        let all = read_newest(dir.path(), page(4)).expect("readable");
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
            .map(|i| {
                format!("{{\"eventTime\":\"2026-08-14T10:00:{i:02}.000Z\",\"i\":{i},\"pad\":\"{fat}\"}}\n")
            })
            .collect();
        fs::write(dir.path().join("1700000000001-aaa.jsonl"), body).expect("write");

        let p = read_newest(dir.path(), page(20)).expect("readable");
        assert!(
            p.events.len() < 20 && !p.events.is_empty(),
            "the byte ceiling must bind before the count does, got {} events",
            p.events.len()
        );
        assert!(p.truncated, "and the caller is told the page was cut");
    }

    #[test]
    fn reader_since_is_an_inclusive_lower_bound_and_before_an_exclusive_upper_one() {
        let dir = tempfile::tempdir().expect("tempdir");
        write_batches(
            dir.path(),
            &[1_700_000_000_001, 1_700_000_000_002, 1_700_000_000_003],
        );

        let from = read_newest(
            dir.path(),
            PageRequest {
                limit: 10,
                since: Some("2026-08-14T10:00:01.000Z"),
                before: None,
            },
        )
        .expect("readable");
        assert_eq!(indices(&from), vec![2, 1], "`since` keeps its own instant");

        let upto = read_newest(
            dir.path(),
            PageRequest {
                limit: 10,
                since: None,
                before: Some("2026-08-14T10:00:01.000Z"),
            },
        )
        .expect("readable");
        assert_eq!(
            indices(&upto),
            vec![0],
            "`before` excludes its own instant — that is what makes it a cursor"
        );

        // An offset timezone is the same instant, so it must select the same
        // events; a string comparison on eventTime would fail this.
        let offset = read_newest(
            dir.path(),
            PageRequest {
                limit: 10,
                since: Some("2026-08-14T11:00:01.000+01:00"),
                before: None,
            },
        )
        .expect("readable");
        assert_eq!(indices(&offset), indices(&from));
    }

    #[test]
    fn reader_cuts_a_page_on_a_timestamp_boundary_so_before_cannot_skip() {
        // Regression pin (d119792): pages ended wherever the cap bound, so a
        // page cut mid-`eventTime` handed back a cursor that skipped every
        // event sharing that timestamp.
        // Paging exactness: events 1 and 2 share 10:00:01. A page of 2 would
        // otherwise end between them, and the next call's `before` (exclusive)
        // would skip the older one. So the page cuts back to the boundary.
        let dir = tempfile::tempdir().expect("tempdir");
        let body = "{\"eventTime\":\"2026-08-14T10:00:00.000Z\",\"i\":0}\n\
                    {\"eventTime\":\"2026-08-14T10:00:01.000Z\",\"i\":1}\n\
                    {\"eventTime\":\"2026-08-14T10:00:01.000Z\",\"i\":2}\n\
                    {\"eventTime\":\"2026-08-14T10:00:02.000Z\",\"i\":3}\n";
        fs::write(dir.path().join("1700000000001-aaa.jsonl"), body).expect("write");

        let first = read_newest(dir.path(), page(2)).expect("readable");
        assert_eq!(
            indices(&first),
            vec![3],
            "the page gives up its second slot rather than end mid-timestamp"
        );
        assert!(first.truncated);

        // Walk it: the cursor is the oldest eventTime received, exclusive.
        let mut seen: Vec<i64> = Vec::new();
        let mut cursor: Option<String> = None;
        for _ in 0..10 {
            let p = read_newest(
                dir.path(),
                PageRequest {
                    limit: 2,
                    since: None,
                    before: cursor.as_deref(),
                },
            )
            .expect("readable");
            if p.events.is_empty() {
                break;
            }
            cursor = Some(
                event_time(p.events.last().expect("non-empty"))
                    .expect("fixtures carry eventTime")
                    .to_string(),
            );
            seen.extend(indices(&p));
        }
        seen.sort_unstable();
        assert_eq!(
            seen,
            vec![0, 1, 2, 3],
            "paging must cover every event exactly once"
        );
    }

    #[test]
    fn reader_returns_an_uncut_page_when_every_event_shares_one_timestamp() {
        // The documented degenerate case: cutting to the boundary would empty
        // the page, and an empty page is a worse answer than an uncut one.
        let dir = tempfile::tempdir().expect("tempdir");
        let body = "{\"eventTime\":\"2026-08-14T10:00:01.000Z\",\"i\":1}\n\
                    {\"eventTime\":\"2026-08-14T10:00:01.000Z\",\"i\":2}\n\
                    {\"eventTime\":\"2026-08-14T10:00:01.000Z\",\"i\":3}\n";
        fs::write(dir.path().join("1700000000001-aaa.jsonl"), body).expect("write");

        let p = read_newest(dir.path(), page(2)).expect("readable");
        assert_eq!(p.events.len(), 2, "the page is still returned");
        assert!(p.truncated);
    }

    #[test]
    fn reader_rejects_a_bound_it_cannot_parse() {
        // Ignoring one would return events the caller asked to exclude and let
        // it conclude they happened inside the window.
        let dir = tempfile::tempdir().expect("tempdir");
        for (field, req) in [
            (
                "since",
                PageRequest {
                    limit: 10,
                    since: Some("yesterday"),
                    before: None,
                },
            ),
            (
                "before",
                PageRequest {
                    limit: 10,
                    since: None,
                    before: Some("yesterday"),
                },
            ),
        ] {
            let err = read_newest(dir.path(), req).expect_err("must refuse");
            assert!(
                matches!(err, ReadError::BadTimestamp { field: f, ref value } if f == field && value == "yesterday"),
                "{field}: got {err:?}"
            );
        }
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

        let p = read_newest(dir.path(), page(10)).expect("readable");
        assert_eq!(p.events[0]["run"]["facets"]["fromTheFuture"]["x"], 1);
    }

    #[test]
    fn reader_holds_only_what_could_make_the_page() {
        // Regression pin (d119792): the reader collected whole batch files, so
        // a 10 000-event batch (the writer's buffer cap) was materialised in
        // full to answer `limit=1`.
        // The memory bound: a batch far larger than the page must not be
        // materialised. `read_tail` is the thing under test, because the page
        // it feeds looks identical either way.
        let dir = tempfile::tempdir().expect("tempdir");
        let body: String = (0..5_000)
            .map(|i| format!("{{\"eventTime\":\"2026-08-14T10:00:00.000Z\",\"i\":{i}}}\n"))
            .collect();
        let path = dir.path().join("1700000000001-aaa.jsonl");
        fs::write(&path, body).expect("write");

        let mut p = EventPage::default();
        let tail = read_tail(&path, 3, None, None, &mut p).expect("readable");
        assert_eq!(
            tail.ring.len(),
            3,
            "a 5000-event batch must not be held to answer a 2-event page"
        );
        assert!(
            tail.newest_dropped.is_some(),
            "and what it dropped is remembered, or the page would look complete"
        );
        // And it kept the NEWEST three, which is what the page needs.
        let kept: Vec<i64> = tail.ring.iter().map(|(e, _)| index_of(e)).collect();
        assert_eq!(kept, vec![4_997, 4_998, 4_999]);
    }
}
