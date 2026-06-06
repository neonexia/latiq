//! In-flight query registry: maps an op-id to its AbortToken so cancellation
//! sources (MCP cancel, drop_pond, timeout) can interrupt a running query.
use latiq_common::PondId;
use latiq_engine::AbortToken;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

struct Entry {
    token: AbortToken,
    pond_id: Option<String>,
}

#[derive(Clone, Default)]
pub struct InFlightRegistry {
    map: Arc<Mutex<HashMap<String, Entry>>>,
}

impl InFlightRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    /// Register a new in-flight op, returning its id and a fresh AbortToken.
    pub fn register(&self, pond_id: Option<String>) -> (String, AbortToken) {
        let op_id = PondId::new().to_string();
        let token = AbortToken::new();
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
}
