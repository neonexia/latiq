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

//! The file sink: batched JSONL in the pond's own `lineage` directory.
//!
//! What a batch write guarantees, precisely — the loose version of this claim
//! ("rename is the whole durability story") was wrong and is worth spelling out:
//!
//! - **Atomic visibility.** A batch is written to `.tmp-<uuid>`, `fsync`ed, and
//!   only then **renamed** to its `.jsonl` name. A reader globbing `*.jsonl`
//!   therefore sees either nothing or the whole batch — never a torn record.
//!   The Task 6 MCP tool globs this directory while queries are running, and a
//!   half-written file would break it. The `.tmp-` prefix keeps the in-progress
//!   file out of a `*.jsonl` glob *and* out of a plain directory listing on
//!   unix, so a reader needs no special-casing beyond the extension.
//! - **Durability of the contents, once visible.** The `fsync` is what buys
//!   this: without it a crash can leave a renamed `.jsonl` containing zero or
//!   partial bytes, which is exactly the torn record the rename was supposed to
//!   prevent.
//! - **NOT durability of the directory entry.** We deliberately do not fsync
//!   the directory, so a crash immediately after the rename can lose the whole
//!   batch. That trade is right for lineage: losing a batch is acceptable, a
//!   torn record is not, and an fsync per batch on the directory would put a
//!   second synchronous metadata flush on a path a query is waiting behind.
//!
//! Two more properties, in the order they matter:
//!
//! - **Emission cannot fail a query.** `record()` serializes and buffers; it
//!   returns `()` and every failure below it is a `warn!`. Nothing here
//!   produces an error a caller could accidentally propagate into a result.
//! - **Nothing escapes the pond.** The directory must be absolute; see
//!   [`LineageWriter::with_limits`].

use std::collections::VecDeque;
use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Mutex, MutexGuard};
use std::time::{SystemTime, UNIX_EPOCH};

use std::sync::Arc;
use uuid::Uuid;

use crate::event::RunEvent;
use crate::sink::EventSink;

/// Events buffered before a batch is written. Small enough that a quiet pond's
/// events land promptly (a shutdown flush covers the rest), large enough that a
/// busy one is not one file per query — file count dominates read cost.
const DEFAULT_BATCH_SIZE: usize = 64;

/// Hard ceiling on buffered events. A failed batch is put **back** in the
/// buffer to be retried, so a directory that stays unwritable would otherwise
/// grow it without bound; this is what stops lineage OOM-ing a node. At ~1 KB
/// an event that is roughly 10 MB of retained events per pond.
const DEFAULT_CAPACITY: usize = 10_000;

#[derive(Default)]
struct Buffer {
    events: VecDeque<String>,
    /// True while the buffer is at capacity, so the drop warning fires once per
    /// overflow episode rather than once per event on an already-failing node.
    /// Cleared wherever the buffer falls back below capacity — including the
    /// batch-size drain, or a pond that recovered and overflowed again a day
    /// later would log nothing the second time.
    overflowing: bool,
}

pub struct LineageWriter {
    /// `None` disables the writer entirely — a rejected directory (see
    /// [`LineageWriter::with_limits`]) must not fall back to *somewhere else*.
    dir: Option<PathBuf>,
    batch_size: usize,
    capacity: usize,
    buffer: Mutex<Buffer>,
    /// True while batch writes are failing. Same once-per-episode discipline as
    /// `Buffer::overflowing`, but it lives outside the mutex because the write
    /// deliberately happens with the lock released. Retrying means a
    /// permanently unwritable directory would otherwise warn on every flush,
    /// forever.
    failing: AtomicBool,
    /// A poisoned mutex is permanent, so this warns exactly once per writer.
    poison_warned: AtomicBool,
    /// The optional second sink (an OpenLineage HTTP backend). It is handed the
    /// **same serialized string** the file buffer takes, which is the whole
    /// reason it lives here rather than beside the writer: a second
    /// `serde_json::to_string` somewhere else is exactly how the posted bytes
    /// and the stored bytes would come to differ. `None` — no backend
    /// configured — is the default and costs one `Option` check per event.
    sink: Option<Arc<dyn EventSink>>,
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
            failing: AtomicBool::new(false),
            poison_warned: AtomicBool::new(false),
            sink: None,
        }
    }

    /// Also hand every event to `sink` — the optional OpenLineage HTTP backend.
    ///
    /// Additive: the pond's files are written exactly as before, and a sink
    /// that is down, slow or dead changes nothing about them. See
    /// [`crate::sink`] for why it cannot reach the query.
    pub fn with_sink(mut self, sink: Arc<dyn EventSink>) -> Self {
        self.sink = Some(sink);
        self
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
    ///
    /// Writes the batch INLINE when one comes due. An async caller must use
    /// [`LineageWriter::buffer_all`] instead and hand the resulting
    /// [`LineageWriter::flush`] to a blocking pool — the write fsyncs, and a
    /// Tokio worker is the wrong thread to do that on.
    pub fn record_all(&self, events: &[RunEvent]) {
        if self.buffer_all(events) {
            self.flush();
        }
    }

    /// Buffer events **without touching the filesystem**, returning whether a
    /// batch has come due.
    ///
    /// This is the split that keeps lineage off the query's critical path: the
    /// common call serializes and pushes under a short-lived lock and is over,
    /// and the caller that sees `true` can move the (fsyncing) write elsewhere
    /// rather than paying for it inline. Nothing here can block on IO, so
    /// "recorded" is observable to a later `flush()` the moment this returns.
    pub fn buffer_all(&self, events: &[RunEvent]) -> bool {
        if events.is_empty() || (self.dir.is_none() && self.sink.is_none()) {
            return false;
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

        // The sink sees the SAME strings the files get, before anything else
        // can touch them — and it sees them even when the file half is
        // disabled (a rejected directory), because a configured backend is the
        // last place those events could still land.
        if let Some(sink) = self.sink.as_deref() {
            for line in &lines {
                sink.submit(line);
            }
        }
        if self.dir.is_none() {
            return false;
        }

        let Some(mut buffer) = self.lock() else {
            return false;
        };
        buffer.events.extend(lines);
        self.enforce_capacity(&mut buffer);
        buffer.events.len() >= self.batch_size
    }

    /// Write whatever is buffered, regardless of batch size.
    pub fn flush(&self) {
        let drained = {
            let Some(mut buffer) = self.lock() else {
                return;
            };
            (!buffer.events.is_empty()).then(|| self.take_batch(&mut buffer))
        };
        if let Some((batch, millis)) = drained {
            self.write_or_requeue(batch, millis);
        }
    }

    fn lock(&self) -> Option<MutexGuard<'_, Buffer>> {
        match self.buffer.lock() {
            Ok(guard) => Some(guard),
            Err(_) => {
                // A panic in another thread poisoned the lock. There is nothing
                // to recover and nothing to fail; drop the events. Poisoning is
                // permanent, so this warns once and never "recovers".
                if !self.poison_warned.swap(true, Ordering::Relaxed) {
                    tracing::warn!("lineage buffer is poisoned; dropping events from now on");
                }
                None
            }
        }
    }

    /// Take everything buffered **and stamp the batch's timestamp**, both while
    /// the lock is held. Sampling the clock later — after the file is written —
    /// would make the file names race: a thread that drained an older batch and
    /// then stalled in `write` would get the *larger* prefix, inverting the
    /// order a reader relies on.
    fn take_batch(&self, buffer: &mut Buffer) -> (Vec<String>, u128) {
        let batch = buffer.events.drain(..).collect();
        // Empty is below capacity, so the overflow episode is over.
        buffer.overflowing = false;
        let millis = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_millis())
            .unwrap_or(0);
        (batch, millis)
    }

    /// Trim to capacity, dropping the **oldest**: the buffer only fills when
    /// writes are failing, and the events nearest the failure are the ones an
    /// investigation wants. The cost is a run whose `START` was dropped while
    /// its terminal event survives, which consumers tolerate.
    fn enforce_capacity(&self, buffer: &mut Buffer) {
        if buffer.events.len() > self.capacity {
            let excess = buffer.events.len() - self.capacity;
            buffer.events.drain(..excess);
            if !buffer.overflowing {
                buffer.overflowing = true;
                tracing::warn!(
                    capacity = self.capacity,
                    "lineage buffer is full; dropping the oldest events"
                );
            }
        } else if buffer.events.len() < self.capacity {
            buffer.overflowing = false;
        }
    }

    /// Write a drained batch, or put it **back at the front of the buffer** so
    /// the next flush retries it. Retrying is what makes the capacity bound
    /// meaningful — and what makes a transient failure (a brief ENOSPC, an EIO)
    /// cost latency rather than events. The price is one write attempt per
    /// batch while the directory stays unwritable; a failing `write` syscall is
    /// cheap, and the warning is rate-limited to the transitions.
    fn write_or_requeue(&self, batch: Vec<String>, millis: u128) {
        match self.write_batch(batch, millis) {
            Ok(()) => {
                if self.failing.swap(false, Ordering::Relaxed) {
                    tracing::info!("lineage writing recovered");
                }
            }
            Err(batch) => {
                let Some(mut buffer) = self.lock() else {
                    return;
                };
                for line in batch.into_iter().rev() {
                    buffer.events.push_front(line);
                }
                self.enforce_capacity(&mut buffer);
            }
        }
    }

    /// Returns the batch back on failure, so the caller can retry it.
    fn write_batch(&self, batch: Vec<String>, millis: u128) -> Result<(), Vec<String>> {
        let Some(dir) = self.dir.as_deref() else {
            return Ok(());
        };
        let mut body = batch.join("\n");
        body.push('\n');

        let id = Uuid::new_v4();
        let temp = dir.join(format!(".tmp-{id}"));
        if let Err(error) = write_and_sync(&temp, body.as_bytes()) {
            self.warn_failure(&error, &temp, "write");
            // Every failure path removes the temp file: `write` can fail after
            // creating and partially filling it (ENOSPC, EIO), and a leftover
            // would sit in the pond until the pond is dropped. Nothing *reads*
            // it — readers glob `*.jsonl` — so this is hygiene, not correctness.
            let _ = fs::remove_file(&temp);
            return Err(batch);
        }
        // `{unix_millis:013}-{uuid}.jsonl`, zero-padded, so sorting names sorts
        // by time — which is how the reader takes the newest files without
        // stat-ing or parsing every file in the directory. The guarantee is
        // chronological **to millisecond granularity**: batches drained within
        // the same millisecond are ordered arbitrarily by their random UUID,
        // which is fine because they are equally new. 13 digits covers every
        // timestamp until the year 2286.
        let final_path = dir.join(format!("{millis:013}-{id}.jsonl"));
        if let Err(error) = fs::rename(&temp, &final_path) {
            self.warn_failure(&error, &final_path, "rename");
            let _ = fs::remove_file(&temp);
            return Err(batch);
        }
        Ok(())
    }

    /// Warn on the transition into a failing state only. A node whose lineage
    /// directory is permanently unwritable retries every batch; without this it
    /// would emit a warning per query, forever, and drown the log it shares
    /// with the access trail.
    fn warn_failure(&self, error: &std::io::Error, path: &Path, step: &'static str) {
        if !self.failing.swap(true, Ordering::Relaxed) {
            tracing::warn!(
                %error,
                path = %path.display(),
                step,
                "lineage batch could not be written; buffering and retrying"
            );
        }
    }
}

/// `fsync` before the rename: the rename makes the batch *visible* atomically,
/// this makes its bytes *durable*. Without it a crash can leave a fully
/// renamed `.jsonl` holding nothing.
fn write_and_sync(path: &Path, bytes: &[u8]) -> std::io::Result<()> {
    let mut file = fs::File::create(path)?;
    file.write_all(bytes)?;
    file.sync_all()
}

impl Drop for LineageWriter {
    /// Events recorded before a shutdown land. That window — the last few
    /// queries before a node went down — is exactly the one an incident
    /// investigation asks about.
    ///
    /// **This drop performs synchronous, fsync-ing IO** (nothing at all when
    /// the writer is disabled). Once the writer lives behind an `Arc` shared by
    /// request handlers, whichever task holds the last reference pays for the
    /// final batch, and on a Tokio worker that blocks the thread. Whoever wires
    /// this in owns where that last drop happens — keep the writer owned by the
    /// pond's own lifecycle, not by a request future.
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

    #[test]
    fn the_file_name_uses_the_timestamp_sampled_at_drain_time() {
        // Pins the fix for a real ordering race: the timestamp is chosen when
        // the batch is drained (under the lock) and carried into the write, so
        // a slow write cannot give an older batch a newer name. A `write_batch`
        // that sampled its own clock would ignore this argument and fail here.
        let dir = tempfile::tempdir().expect("tempdir");
        let writer = LineageWriter::new(dir.path().to_str().expect("utf-8 tempdir"));
        writer
            .write_batch(vec!["{}".to_string()], 1_700_000_000_123)
            .expect("write succeeds");

        let name = fs::read_dir(dir.path())
            .expect("readable")
            .next()
            .expect("one file")
            .expect("entry")
            .file_name()
            .to_string_lossy()
            .into_owned();
        assert!(
            name.starts_with("1700000000123-"),
            "the name must carry the drained-at timestamp, got {name}"
        );
    }

    #[test]
    fn the_overflow_episode_ends_when_the_buffer_drains() {
        // The flag guards a once-per-episode warning. Clearing it only in
        // flush() meant a pond that overflowed, recovered, and overflowed again
        // a day later logged nothing the second time.
        let dir = tempfile::tempdir().expect("tempdir");
        let writer = LineageWriter::with_limits(dir.path().to_str().expect("utf-8 tempdir"), 3, 3);
        {
            let mut buffer = writer.buffer.lock().expect("lock");
            buffer
                .events
                .extend((0..4).map(|i| format!("{{\"i\":{i}}}")));
            writer.enforce_capacity(&mut buffer);
            assert!(
                buffer.overflowing,
                "four events over a cap of three overflows"
            );
            let _ = writer.take_batch(&mut buffer);
            assert!(
                !buffer.overflowing,
                "draining the batch ends the episode, so the next one warns again"
            );
        }
    }
}
