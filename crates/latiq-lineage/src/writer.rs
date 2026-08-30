//! The file sink: batched JSONL in the pond's own `lineage` directory.
//!
//! Three properties, in the order they matter:
//!
//! 1. **A reader never sees a torn record.** Every batch is written to
//!    `.tmp-<uuid>` and then **renamed** into place. Rename is the whole
//!    durability story — the MCP tool in a later slice globs this directory
//!    while queries are running, and a partially written file would break it.
//! 2. **Emission cannot fail a query.** `record()` serializes and buffers; it
//!    returns `()` and every failure below it is a `warn!`. Nothing here
//!    produces an error a caller could accidentally propagate into a result.
//! 3. **Nothing escapes the pond.** The directory must be absolute; see
//!    [`LineageWriter::with_limits`].

use std::collections::VecDeque;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Mutex;
use std::time::{SystemTime, UNIX_EPOCH};

use uuid::Uuid;

use crate::event::RunEvent;

/// Events buffered before a batch is written. Small enough that a quiet pond's
/// events land promptly (a shutdown flush covers the rest), large enough that a
/// busy one is not one file per query — file count dominates read cost.
const DEFAULT_BATCH_SIZE: usize = 64;

/// Hard ceiling on buffered events. Reached only when flushing keeps failing;
/// without it a broken disk would grow the buffer until the node died, and
/// lineage must never be able to OOM a node.
const DEFAULT_CAPACITY: usize = 10_000;

#[derive(Default)]
struct Buffer {
    events: VecDeque<String>,
    /// True while the buffer is full, so the drop warning fires once per
    /// episode instead of once per event on an already-failing node.
    overflowing: bool,
}

pub struct LineageWriter {
    /// `None` disables the writer entirely — a rejected directory (see
    /// [`LineageWriter::with_limits`]) must not fall back to *somewhere else*.
    dir: Option<PathBuf>,
    batch_size: usize,
    capacity: usize,
    buffer: Mutex<Buffer>,
}

impl LineageWriter {
    /// A writer for a pond's `lineage_dir`, with the default batch and cap.
    pub fn new(dir: &str) -> Self {
        Self::with_limits(dir, DEFAULT_BATCH_SIZE, DEFAULT_CAPACITY)
    }

    /// As [`LineageWriter::new`], with the batch size and buffer cap chosen
    /// explicitly. Mostly for tests, which cannot afford to record 64 events to
    /// observe one file.
    ///
    /// **A non-absolute directory is refused** and yields a disabled writer.
    /// `PondLocation::lineage_dir` is `#[serde(default)]`, so a location sent by
    /// a node that predates the field arrives as `""`; joining that with a file
    /// name would scatter events across the process CWD — outside any pond, and
    /// outside the `remove_dir_all` that reaps one. Disabling is right rather
    /// than returning an error because the caller is on the query hot path and
    /// has nothing useful to do with the failure: losing lineage is the correct
    /// outcome, failing the query is not.
    pub fn with_limits(dir: &str, batch_size: usize, capacity: usize) -> Self {
        let path = Path::new(dir);
        let dir = if dir.is_empty() || !path.is_absolute() {
            tracing::warn!(
                lineage_dir = dir,
                "lineage directory is not absolute; lineage is disabled for this pond"
            );
            None
        } else {
            Some(path.to_path_buf())
        };
        Self {
            dir,
            batch_size: batch_size.max(1),
            capacity: capacity.max(1),
            buffer: Mutex::new(Buffer::default()),
        }
    }

    /// Whether this writer will ever write. False for a rejected directory.
    pub fn is_enabled(&self) -> bool {
        self.dir.is_some()
    }

    /// Buffer one event. Called with a query in flight: the common case is a
    /// serialize plus a push under a short-lived lock, with no IO at all — only
    /// every `batch_size`th call writes, and that write happens after the lock
    /// is released.
    pub fn record(&self, event: &RunEvent) {
        self.record_all(std::slice::from_ref(event));
    }

    /// Buffer several events together — how a caller emits a run's `START` and
    /// its terminal event, which belong in the same batch so a reader never
    /// finds one without the other in the directory.
    pub fn record_all(&self, events: &[RunEvent]) {
        if self.dir.is_none() || events.is_empty() {
            return;
        }
        let lines: Vec<String> = events
            .iter()
            .filter_map(|event| match serde_json::to_string(event) {
                Ok(line) => Some(line),
                Err(error) => {
                    // Cannot happen for `RunEvent` (no maps with non-string
                    // keys, no non-finite floats); dropped rather than
                    // propagated because a query must not fail over lineage.
                    tracing::warn!(%error, "dropping a lineage event that would not serialize");
                    None
                }
            })
            .collect();

        let batch = {
            let Ok(mut buffer) = self.buffer.lock() else {
                // A panic in another thread poisoned the lock. There is nothing
                // to recover and nothing to fail; drop the events.
                tracing::warn!("lineage buffer is poisoned; dropping events");
                return;
            };
            buffer.events.extend(lines);
            // Drop **oldest** on overflow: the buffer only fills when writes are
            // failing, and the events nearest the failure are the ones an
            // investigation wants. The cost is a run whose START was dropped
            // while its terminal event survives, which consumers tolerate.
            if buffer.events.len() > self.capacity {
                let dropped = buffer.events.len() - self.capacity;
                buffer.events.drain(..dropped);
                if !buffer.overflowing {
                    buffer.overflowing = true;
                    tracing::warn!(
                        capacity = self.capacity,
                        "lineage buffer is full; dropping the oldest events"
                    );
                }
            }
            (buffer.events.len() >= self.batch_size).then(|| buffer.events.drain(..).collect())
        };
        // Outside the lock: an IO stall must not block the next query's record().
        if let Some(batch) = batch {
            self.write_batch(batch);
        }
    }

    /// Write whatever is buffered, regardless of batch size.
    pub fn flush(&self) {
        let batch: Vec<String> = {
            let Ok(mut buffer) = self.buffer.lock() else {
                tracing::warn!("lineage buffer is poisoned; dropping events");
                return;
            };
            buffer.overflowing = false;
            buffer.events.drain(..).collect()
        };
        if !batch.is_empty() {
            self.write_batch(batch);
        }
    }

    fn write_batch(&self, batch: Vec<String>) {
        let Some(dir) = self.dir.as_deref() else {
            return;
        };
        let mut body = batch.join("\n");
        body.push('\n');

        let id = Uuid::new_v4();
        let temp = dir.join(format!(".tmp-{id}"));
        if let Err(error) = fs::write(&temp, body.as_bytes()) {
            tracing::warn!(%error, path = %temp.display(), "dropping a lineage batch: write failed");
            return;
        }
        // `{unix_millis:013}-{uuid}.jsonl`: zero-padded so **lexicographic order
        // is chronological**, which is what lets a reader take the newest files
        // by sorting names instead of stat-ing or parsing every file in the
        // directory. 13 digits covers every timestamp until the year 2286.
        let millis = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_millis())
            .unwrap_or(0);
        let final_path = dir.join(format!("{millis:013}-{id}.jsonl"));
        if let Err(error) = fs::rename(&temp, &final_path) {
            tracing::warn!(%error, path = %final_path.display(), "dropping a lineage batch: rename failed");
            // Best effort: a temp file left behind would be picked up by nothing
            // (readers only glob `*.jsonl`) but would still occupy the pond.
            let _ = fs::remove_file(&temp);
        }
    }
}

impl Drop for LineageWriter {
    /// Events recorded before a shutdown land. That window — the last few
    /// queries before a node went down — is exactly the one an incident
    /// investigation asks about.
    fn drop(&mut self) {
        self.flush();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_relative_directory_is_refused_but_an_absolute_one_is_not() {
        // Guards the `lineage_dir: ""` case a pre-field PondLocation produces.
        // The absolute half is what stops this passing vacuously.
        assert!(!LineageWriter::new("").is_enabled());
        assert!(!LineageWriter::new("ponds/p1/lineage").is_enabled());
        assert!(LineageWriter::new("/var/lib/latiq/ponds/p1/lineage").is_enabled());
    }
}
