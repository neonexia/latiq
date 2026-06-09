//! DuckDB-backed control-plane registry. The control-plane process is the sole
//! writer, so a single connection behind a Mutex is correct (single-writer).
use crate::error::ControlPlaneError;
use crate::migrations::run_migrations;
use duckdb::Connection;
use latiq_common::PondId;
use serde::{Deserialize, Serialize};
use std::path::Path;
use std::sync::{Arc, Mutex};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeRow {
    pub node_id: String,
    pub mcp_endpoint: String,
    pub internal_endpoint: String,
    pub capacity: u32,
    pub pond_count: u32,
    pub state: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PondRow {
    pub pond_id: String,
    pub name: String,
    pub owner_identity: String,
    pub node_id: String,
}

#[derive(Debug, Clone)]
pub struct AuditInsert {
    pub agent_identity: String,
    pub identity_verified: bool,
    pub operation: String,
    pub pond_id: Option<String>,
    pub request_summary: Option<String>,
    pub result_summary: Option<String>,
    pub duration_ms: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuditRow {
    pub ts: String,
    pub agent_identity: String,
    pub verified: bool,
    pub operation: String,
    pub pond_id: Option<String>,
    pub duration_ms: u64,
}

#[derive(Clone)]
pub struct Registry {
    conn: Arc<Mutex<Connection>>,
}

impl Registry {
    pub fn open(path: Option<&Path>) -> Result<Self, ControlPlaneError> {
        let conn = match path {
            Some(p) => Connection::open(p)?,
            None => Connection::open_in_memory()?,
        };
        run_migrations(&conn)?;
        Ok(Self {
            conn: Arc::new(Mutex::new(conn)),
        })
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, Connection> {
        // Recover the guard if a prior holder panicked: a poisoned registry mutex
        // must not brick the whole control plane. The DuckDB connection is still
        // usable; the next statement either succeeds or returns a normal error.
        self.conn.lock().unwrap_or_else(|e| e.into_inner())
    }

    pub fn register_node(
        &self,
        node_id: &str,
        mcp: &str,
        internal: &str,
        capacity: u32,
    ) -> Result<(), ControlPlaneError> {
        let c = self.lock();
        c.execute(
            "INSERT INTO nodes(node_id, mcp_endpoint, internal_endpoint, capacity)
             VALUES (?,?,?,?)
             ON CONFLICT (node_id) DO UPDATE SET
               mcp_endpoint=excluded.mcp_endpoint,
               internal_endpoint=excluded.internal_endpoint,
               capacity=excluded.capacity,
               last_heartbeat=now()",
            duckdb::params![node_id, mcp, internal, capacity],
        )?;
        Ok(())
    }

    pub fn heartbeat(&self, node_id: &str, pond_count: u32) -> Result<(), ControlPlaneError> {
        let c = self.lock();
        let n = c.execute(
            "UPDATE nodes SET pond_count=?, last_heartbeat=now() WHERE node_id=?",
            duckdb::params![pond_count, node_id],
        )?;
        if n == 0 {
            return Err(ControlPlaneError::NodeNotFound(node_id.to_string()));
        }
        Ok(())
    }

    pub fn list_nodes(&self) -> Result<Vec<NodeRow>, ControlPlaneError> {
        let c = self.lock();
        let mut stmt = c.prepare(
            "SELECT node_id, mcp_endpoint, internal_endpoint, capacity, pond_count, state
             FROM nodes ORDER BY node_id",
        )?;
        let rows = stmt.query_map([], |r| {
            Ok(NodeRow {
                node_id: r.get(0)?,
                mcp_endpoint: r.get(1)?,
                internal_endpoint: r.get(2)?,
                capacity: r.get(3)?,
                pond_count: r.get(4)?,
                state: r.get(5)?,
            })
        })?;
        Ok(rows.collect::<Result<_, _>>()?)
    }

    pub fn describe_node(&self, node_id: &str) -> Result<NodeRow, ControlPlaneError> {
        self.list_nodes()?
            .into_iter()
            .find(|n| n.node_id == node_id)
            .ok_or_else(|| ControlPlaneError::NodeNotFound(node_id.to_string()))
    }

    pub fn create_pond(
        &self,
        name: Option<String>,
        owner_identity: &str,
        policy_json: &str,
    ) -> Result<PondRow, ControlPlaneError> {
        let pond_id = PondId::new().to_string();
        let name = name.unwrap_or_else(|| pond_id.clone());
        let c = self.lock();
        // Pick a random active node to balance ponds across nodes. Uniform random
        // is the only strategy for now; a pluggable strategy can come later.
        let node_id: String = c
            .query_row(
                "SELECT node_id FROM nodes WHERE state='active' ORDER BY random() LIMIT 1",
                [],
                |r| r.get(0),
            )
            .map_err(|_| ControlPlaneError::NodeNotFound("no active node registered".into()))?;
        let exists: i64 = c.query_row(
            "SELECT count(*) FROM ponds WHERE name=?",
            duckdb::params![name],
            |r| r.get(0),
        )?;
        if exists > 0 {
            return Err(ControlPlaneError::NameConflict(name));
        }
        c.execute(
            "INSERT INTO ponds(pond_id, name, owner_identity, node_id, policy_json) VALUES (?,?,?,?,?)",
            duckdb::params![pond_id, name, owner_identity, node_id, policy_json],
        )?;
        Ok(PondRow {
            pond_id,
            name,
            owner_identity: owner_identity.to_string(),
            node_id,
        })
    }

    pub fn get_pond_location(&self, pond_ref: &str) -> Result<(String, String), ControlPlaneError> {
        let c = self.lock();
        c.query_row(
            "SELECT p.pond_id, n.internal_endpoint FROM ponds p JOIN nodes n ON n.node_id=p.node_id
             WHERE p.pond_id=? OR p.name=? LIMIT 1",
            duckdb::params![pond_ref, pond_ref],
            |r| Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?)),
        )
        .map_err(|_| ControlPlaneError::PondNotFound(pond_ref.to_string()))
    }

    pub fn list_ponds(&self) -> Result<Vec<PondRow>, ControlPlaneError> {
        let c = self.lock();
        let mut stmt = c.prepare(
            "SELECT pond_id, name, owner_identity, node_id FROM ponds ORDER BY created_at",
        )?;
        let rows = stmt.query_map([], |r| {
            Ok(PondRow {
                pond_id: r.get(0)?,
                name: r.get(1)?,
                owner_identity: r.get(2)?,
                node_id: r.get(3)?,
            })
        })?;
        Ok(rows.collect::<Result<_, _>>()?)
    }

    /// Returns (PondRow, created_at, policy_json) resolved by id or name.
    pub fn pond_info(
        &self,
        pond_ref: &str,
    ) -> Result<(PondRow, String, String), ControlPlaneError> {
        let c = self.lock();
        c.query_row(
            "SELECT pond_id, name, owner_identity, node_id, created_at::VARCHAR, policy_json
             FROM ponds WHERE pond_id=? OR name=? LIMIT 1",
            duckdb::params![pond_ref, pond_ref],
            |r| {
                Ok((
                    PondRow {
                        pond_id: r.get(0)?,
                        name: r.get(1)?,
                        owner_identity: r.get(2)?,
                        node_id: r.get(3)?,
                    },
                    r.get::<_, String>(4)?,
                    r.get::<_, String>(5)?,
                ))
            },
        )
        .map_err(|_| ControlPlaneError::PondNotFound(pond_ref.to_string()))
    }

    pub fn drop_pond(&self, pond_id: &str) -> Result<(), ControlPlaneError> {
        let c = self.lock();
        let n = c.execute(
            "DELETE FROM ponds WHERE pond_id=? OR name=?",
            duckdb::params![pond_id, pond_id],
        )?;
        if n == 0 {
            return Err(ControlPlaneError::PondNotFound(pond_id.to_string()));
        }
        Ok(())
    }

    pub fn policy_get(&self) -> Result<serde_json::Value, ControlPlaneError> {
        let c = self.lock();
        let mut stmt = c.prepare("SELECT key, value FROM policy ORDER BY key")?;
        let rows = stmt.query_map([], |r| Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?)))?;
        let mut map = serde_json::Map::new();
        for kv in rows {
            let (k, v) = kv?;
            map.insert(k, serde_json::Value::String(v));
        }
        Ok(serde_json::Value::Object(map))
    }

    pub fn policy_set(&self, key: &str, value: &str) -> Result<(), ControlPlaneError> {
        let c = self.lock();
        c.execute(
            "INSERT INTO policy(key,value) VALUES (?,?) ON CONFLICT (key) DO UPDATE SET value=excluded.value",
            duckdb::params![key, value],
        )?;
        Ok(())
    }

    pub fn record_audit(&self, e: AuditInsert) -> Result<(), ControlPlaneError> {
        let audit_id = PondId::new().to_string();
        let c = self.lock();
        c.execute(
            "INSERT INTO audit_log(audit_id, agent_identity, identity_verified, operation, pond_id, request_summary, result_summary, duration_ms)
             VALUES (?,?,?,?,?,?,?,?)",
            duckdb::params![
                audit_id,
                e.agent_identity,
                e.identity_verified,
                e.operation,
                e.pond_id,
                e.request_summary,
                e.result_summary,
                e.duration_ms
            ],
        )?;
        Ok(())
    }

    pub fn audit_tail(&self, limit: u32) -> Result<Vec<AuditRow>, ControlPlaneError> {
        let c = self.lock();
        let mut stmt = c.prepare(
            "SELECT ts::VARCHAR, agent_identity, identity_verified, operation, pond_id, duration_ms
             FROM audit_log ORDER BY ts DESC LIMIT ?",
        )?;
        let rows = stmt.query_map(duckdb::params![limit], audit_row)?;
        Ok(rows.collect::<Result<_, _>>()?)
    }

    pub fn audit_search(
        &self,
        identity: &str,
        since: &str,
    ) -> Result<Vec<AuditRow>, ControlPlaneError> {
        let c = self.lock();
        let mut stmt = c.prepare(
            "SELECT ts::VARCHAR, agent_identity, identity_verified, operation, pond_id, duration_ms
             FROM audit_log WHERE agent_identity=? AND ts >= CAST(? AS TIMESTAMP) ORDER BY ts DESC",
        )?;
        let rows = stmt.query_map(duckdb::params![identity, since], audit_row)?;
        Ok(rows.collect::<Result<_, _>>()?)
    }
}

fn audit_row(r: &duckdb::Row<'_>) -> duckdb::Result<AuditRow> {
    Ok(AuditRow {
        ts: r.get(0)?,
        agent_identity: r.get(1)?,
        verified: r.get(2)?,
        operation: r.get(3)?,
        pond_id: r.get(4)?,
        duration_ms: r.get(5)?,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn reg() -> Registry {
        Registry::open(None).unwrap()
    }

    #[test]
    fn list_ponds_and_pond_info() {
        let r = reg();
        r.register_node("node-a", "http://n:8080/mcp", "http://n:9092", 100)
            .unwrap();
        r.create_pond(Some("p-one".into()), "agent-x", "{\"k\":1}")
            .unwrap();
        r.create_pond(Some("p-two".into()), "agent-y", "{}")
            .unwrap();
        let ponds = r.list_ponds().unwrap();
        assert_eq!(ponds.len(), 2);
        let (row, created_at, policy) = r.pond_info("p-one").unwrap();
        assert_eq!(row.name, "p-one");
        assert_eq!(row.owner_identity, "agent-x");
        assert!(!created_at.is_empty());
        assert_eq!(policy, "{\"k\":1}");
        assert!(matches!(
            r.pond_info("nope"),
            Err(ControlPlaneError::PondNotFound(_))
        ));
    }

    #[test]
    fn node_and_pond_lifecycle() {
        let r = reg();
        r.register_node("node-a", "http://n:8080/mcp", "http://n:9092", 100)
            .unwrap();
        r.heartbeat("node-a", 3).unwrap();
        assert_eq!(r.list_nodes().unwrap().len(), 1);
        let p = r
            .create_pond(Some("incident-1".into()), "agent-x", "{}")
            .unwrap();
        assert_eq!(p.name, "incident-1");
        let (pid, endpoint) = r.get_pond_location("incident-1").unwrap();
        assert_eq!(pid, p.pond_id);
        assert_eq!(endpoint, "http://n:9092");
        assert!(matches!(
            r.create_pond(Some("incident-1".into()), "y", "{}"),
            Err(ControlPlaneError::NameConflict(_))
        ));
        r.drop_pond(&p.pond_id).unwrap();
        assert!(matches!(
            r.get_pond_location("incident-1"),
            Err(ControlPlaneError::PondNotFound(_))
        ));
    }

    #[test]
    fn create_pond_without_node_errors() {
        let r = reg();
        assert!(matches!(
            r.create_pond(None, "x", "{}"),
            Err(ControlPlaneError::NodeNotFound(_))
        ));
    }

    #[test]
    fn policy_and_audit() {
        let r = reg();
        r.policy_set("query_timeout_seconds", "60").unwrap();
        assert_eq!(r.policy_get().unwrap()["query_timeout_seconds"], "60");
        r.record_audit(AuditInsert {
            agent_identity: "agent-x".into(),
            identity_verified: false,
            operation: "read_query".into(),
            pond_id: Some("p1".into()),
            request_summary: Some("SELECT ?".into()),
            result_summary: None,
            duration_ms: 12,
        })
        .unwrap();
        let tail = r.audit_tail(10).unwrap();
        assert_eq!(tail.len(), 1);
        assert_eq!(tail[0].operation, "read_query");
        assert_eq!(r.audit_search("agent-x", "1970-01-01").unwrap().len(), 1);
    }

    #[test]
    fn recovers_from_poisoned_mutex() {
        use std::panic::{catch_unwind, AssertUnwindSafe};
        let r = reg();
        let rc = r.clone();
        // Poison the connection mutex: panic while holding its guard.
        let res = catch_unwind(AssertUnwindSafe(|| {
            let _g = rc.conn.lock().unwrap();
            panic!("boom while holding the registry conn lock");
        }));
        assert!(res.is_err());

        // The control plane must still serve reads/writes after the poison.
        r.register_node("node-a", "http://n:8080/mcp", "http://n:9092", 10)
            .unwrap();
        assert_eq!(r.list_nodes().unwrap().len(), 1);
        assert!(r.list_ponds().unwrap().is_empty());
    }
}
