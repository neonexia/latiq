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

//! The interrupt watcher: how an `AbortToken` becomes a stopped DuckDB
//! statement.
//!
//! DuckDB's interrupt is **edge-triggered against a running statement**, not a
//! sticky property of the connection: the flag is cleared as each statement
//! begins, so an `interrupt()` delivered at a moment when nothing is executing
//! is discarded. That is the whole design constraint here, and it is why the
//! watcher fires **repeatedly for as long as the token is cancelled** rather
//! than once.
//!
//! A single shot was a real hole, not a theoretical one. Nightly run
//! 33789446152 failed with a write that reported `duration_ms: 2694228` under
//! `timeout_ms: 60000` — and *committed*. The abort had been spent in one of the
//! gaps around the statement (before the operation reached the engine at all,
//! or between our `BEGIN` and the caller's SQL), and once the old watcher had
//! fired it exited, leaving the statement that started next running with no
//! bound of any kind. A deadline that can be consumed before the work starts is
//! not a deadline.
//!
//! The other half of the contract is [`AbortWatcher::disarm`], which **joins**
//! the thread rather than merely flagging it. Re-firing is only safe while the
//! statement being guarded is the caller's: a `ROLLBACK` that is itself
//! interrupted leaves the pond's writer connection inside an open transaction,
//! and that connection is kept rather than discarded, so every later write to
//! the pond fails. Joining is what lets `DuckEngine::write_query` issue its
//! recovery rollback afterwards and know it cannot be interrupted.
use crate::instance::PondInstance;
use latiq_engine::AbortToken;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread::JoinHandle;
use std::time::Duration;

/// How often the watcher re-checks the token — and, once cancelled, re-fires the
/// interrupt. Bounds how long a statement can start *after* its abort and still
/// run before it is stopped.
const POLL: Duration = Duration::from_millis(10);

/// A live interrupt watcher bound to one connection and one token, stopped by
/// [`disarm`](Self::disarm) or by drop.
pub(crate) struct AbortWatcher {
    done: Arc<AtomicBool>,
    watcher: Option<JoinHandle<()>>,
}

impl AbortWatcher {
    /// Start watching `abort` on behalf of `inst`.
    ///
    /// The interrupt handle is taken here, on the caller's thread, because
    /// `Connection` is not `Send` while `Arc<InterruptHandle>` is — that
    /// asymmetry is the reason this can be a thread at all.
    pub(crate) fn arm(inst: &PondInstance, abort: &AbortToken) -> Self {
        let handle = inst.conn.interrupt_handle();
        let abort = abort.clone();
        let done = Arc::new(AtomicBool::new(false));
        let done_w = done.clone();
        let watcher = std::thread::spawn(move || loop {
            // `done` first: a disarm racing a cancel must win, or the interrupt
            // lands on the framing statement that follows.
            if done_w.load(Ordering::Acquire) {
                break;
            }
            if abort.is_cancelled() {
                // Deliberately no `break`. The statement we mean to stop may not
                // have started yet, and DuckDB drops an interrupt that arrives
                // between statements — so keep firing until whoever owns the
                // connection says the work is over.
                handle.interrupt();
            }
            // Parked rather than slept so `disarm` can wake it at once: this sits
            // on the write path's hot line, and a plain sleep would charge every
            // small write up to a full POLL on the way out.
            std::thread::park_timeout(POLL);
        });
        Self {
            done,
            watcher: Some(watcher),
        }
    }

    /// Stop firing, and wait until the watcher thread has observed that.
    ///
    /// Joining rather than merely setting the flag is the point: after this
    /// returns, no further `interrupt()` can be issued, so a statement the
    /// caller runs next — the write path's recovery `ROLLBACK` — is safe.
    ///
    /// Idempotent, so the explicit call and the drop can both happen.
    pub(crate) fn disarm(&mut self) {
        self.done.store(true, Ordering::Release);
        if let Some(w) = self.watcher.take() {
            w.thread().unpark();
            let _ = w.join();
        }
    }
}

impl Drop for AbortWatcher {
    fn drop(&mut self) {
        self.disarm();
    }
}
