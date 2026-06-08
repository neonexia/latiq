//! In-flight query registry: maps an op-id to its AbortToken so cancellation
//! sources (MCP cancel, drop_pond, timeout) can interrupt a running query.
use latiq_common::PondId;
use latiq_engine::AbortToken;
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};

struct Entry {
    token: AbortToken,
    pond_id: Option<String>,
}

#[derive(Clone, Default)]
pub struct InFlightRegistry {
    map: Arc<Mutex<HashMap<String, Entry>>>,
    /// Ponds currently being dropped. A query that registers for one of these
    /// gets a pre-cancelled token, closing the drop_pond cancel TOCTOU (a query
    /// that passed resolve_pond before the drop but registers after the cancel).
    dropping: Arc<Mutex<HashSet<String>>>,
}

impl InFlightRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    /// Register a new in-flight op, returning its id and a fresh AbortToken. If
    /// the pond is being dropped, the token comes back already cancelled so the
    /// op aborts immediately rather than running against files being deleted.
    pub fn register(&self, pond_id: Option<String>) -> (String, AbortToken) {
        let op_id = PondId::new().to_string();
        let token = AbortToken::new();
        // Check the tombstone before inserting. Lock order is always dropping→map
        // and the two are never held simultaneously, so this can't deadlock with
        // begin_drop (which locks dropping, then map via cancel_for_pond).
        if let Some(pid) = &pond_id {
            if self.dropping.lock().unwrap().contains(pid) {
                token.cancel();
            }
        }
        self.map.lock().unwrap().insert(
            op_id.clone(),
            Entry {
                token: token.clone(),
                pond_id,
            },
        );
        (op_id, token)
    }

    /// Mark an op complete (remove it).
    pub fn complete(&self, op_id: &str) {
        self.map.lock().unwrap().remove(op_id);
    }

    /// Cancel a specific op by id.
    pub fn cancel(&self, op_id: &str) {
        if let Some(e) = self.map.lock().unwrap().get(op_id) {
            e.token.cancel();
        }
    }

    /// Cancel all in-flight ops on a pond (drop_pond is authoritative).
    pub fn cancel_for_pond(&self, pond_id: &str) {
        let map = self.map.lock().unwrap();
        for e in map.values() {
            if e.pond_id.as_deref() == Some(pond_id) {
                e.token.cancel();
            }
        }
    }

    /// Begin dropping a pond: tombstone it (so newly-registered ops get a
    /// pre-cancelled token) and cancel everything already in flight on it. Pair
    /// with `end_drop` once the drop finishes (or aborts).
    pub fn begin_drop(&self, pond_id: &str) {
        self.dropping.lock().unwrap().insert(pond_id.to_string());
        self.cancel_for_pond(pond_id);
    }

    /// Clear a pond's drop tombstone. Called after the drop completes, or to
    /// unblock the pond if the drop failed and it still exists.
    pub fn end_drop(&self, pond_id: &str) {
        self.dropping.lock().unwrap().remove(pond_id);
    }

    pub fn len(&self) -> usize {
        self.map.lock().unwrap().len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn register_cancel_complete() {
        let reg = InFlightRegistry::new();
        let (op, token) = reg.register(Some("pond-1".into()));
        assert_eq!(reg.len(), 1);
        assert!(!token.is_cancelled());
        reg.cancel(&op);
        assert!(token.is_cancelled());
        reg.complete(&op);
        assert!(reg.is_empty());
    }

    #[test]
    fn cancel_for_pond_targets_only_that_pond() {
        let reg = InFlightRegistry::new();
        let (_o1, t1) = reg.register(Some("pond-a".into()));
        let (_o2, t2) = reg.register(Some("pond-b".into()));
        reg.cancel_for_pond("pond-a");
        assert!(t1.is_cancelled());
        assert!(!t2.is_cancelled());
    }

    #[test]
    fn begin_drop_cancels_existing_ops_on_the_pond() {
        let reg = InFlightRegistry::new();
        let (_op, token) = reg.register(Some("pond-x".into()));
        assert!(!token.is_cancelled());
        reg.begin_drop("pond-x");
        assert!(token.is_cancelled());
    }

    #[test]
    fn register_during_drop_is_pre_cancelled() {
        let reg = InFlightRegistry::new();
        reg.begin_drop("pond-x");
        // A query that slips past resolve_pond and registers mid-drop is aborted
        // immediately (the TOCTOU window).
        let (_op, token) = reg.register(Some("pond-x".into()));
        assert!(token.is_cancelled());
        // A different pond is unaffected.
        let (_o2, t2) = reg.register(Some("pond-y".into()));
        assert!(!t2.is_cancelled());
        // Once the drop finishes (or aborts), the tombstone clears.
        reg.end_drop("pond-x");
        let (_o3, t3) = reg.register(Some("pond-x".into()));
        assert!(!t3.is_cancelled());
    }
}
