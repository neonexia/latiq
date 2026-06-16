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
    /// Live count of ponds assigned to this node (computed from the ponds table,
    /// the source of truth — not the heartbeat's vestigial field).
    pub pond_count: u32,
    /// `active` | `down` (the reaper flips active→down on a stale heartbeat).
    pub state: String,
    /// Last heartbeat timestamp (string) and its age in seconds (now − beat).
    pub last_heartbeat: String,
    pub heartbeat_age_seconds: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PondRow {
    pub pond_id: String,
    pub name: String,
    pub owner_identity: String,
    pub node_id: String,
    /// Resource tier name (small/medium/large/x-large); defaults to medium.
    pub tier: String,
    /// Optional DuckDB extensions the pond loads on open (empty = none).
    pub extensions: Vec<String>,
}

/// One external table in a dataset (the table created in the pond + its source).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DatasetTableRow {
    pub table_name: String,
    pub source_uri: String,
    pub format: String,
}

/// A dataset: one or more simple file tables living in the built-in `latiq`
/// catalog. `load`ed into a pond (copied).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DatasetRow {
    pub name: String,
    pub description: String,
    pub tags: Vec<String>,
    pub tables: Vec<DatasetTableRow>,
    pub created_by: String,
    pub created_at: String,
}

/// An external catalog (iceberg/…): a `type` + opaque locator `params` (the
/// per-type attacher allowlists them — never credentials). Pulled from
/// transiently; its tables are discovered.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CatalogRow {
    pub name: String,
    pub r#type: String,
    /// Locator params (key→value). Credentials are never stored here.
    pub params: std::collections::BTreeMap<String, String>,
    pub description: String,
    pub tags: Vec<String>,
    pub created_by: String,
    pub created_at: String,
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
               last_heartbeat=now(),
               state='active'",
            duckdb::params![node_id, mcp, internal, capacity],
        )?;
        // Flush the WAL into registry.duckdb so a lost/deleted .wal can't drop it
        // (serve is often killed without a clean close, which is the only other
        // time DuckDB would checkpoint).
        let _ = c.execute_batch("CHECKPOINT");
        Ok(())
    }

    /// Record a heartbeat: refresh `last_heartbeat` and revive the node to
    /// `active` (so a node the reaper downed comes back as soon as it beats).
    /// The `pond_count` field is vestigial — the real count is computed on read.
    pub fn heartbeat(&self, node_id: &str, _pond_count: u32) -> Result<(), ControlPlaneError> {
        let c = self.lock();
        let n = c.execute(
            "UPDATE nodes SET last_heartbeat=now(), state='active' WHERE node_id=?",
            duckdb::params![node_id],
        )?;
        if n == 0 {
            return Err(ControlPlaneError::NodeNotFound(node_id.to_string()));
        }
        Ok(())
    }

    /// Mark `active` nodes whose last heartbeat is older than `ttl_secs` as
    /// `down`. Returns the number newly downed. A subsequent heartbeat/register
    /// revives them. Placement (`create_pond`) only picks `active` nodes.
    pub fn reap_stale_nodes(&self, ttl_secs: u32) -> Result<usize, ControlPlaneError> {
        let c = self.lock();
        let n = c.execute(
            "UPDATE nodes SET state='down'
             WHERE state='active' AND last_heartbeat < now()::TIMESTAMP - to_seconds(?)",
            duckdb::params![ttl_secs],
        )?;
        if n > 0 {
            let _ = c.execute_batch("CHECKPOINT");
            metrics::counter!("latiq_nodes_reaped_total").increment(n as u64);
        }
        Ok(n)
    }

    pub fn list_nodes(&self) -> Result<Vec<NodeRow>, ControlPlaneError> {
        let c = self.lock();
        // pond_count is computed live from the ponds table (source of truth for
        // placement), not the heartbeat's stored value. age is in seconds.
        let mut stmt = c.prepare(
            "SELECT n.node_id, n.mcp_endpoint, n.internal_endpoint, n.capacity,
                    (SELECT count(*) FROM ponds p WHERE p.node_id = n.node_id),
                    n.state, n.last_heartbeat::VARCHAR,
                    date_diff('second', n.last_heartbeat, now()::TIMESTAMP)
             FROM nodes n ORDER BY n.node_id",
        )?;
        let rows = stmt.query_map([], |r| {
            Ok(NodeRow {
                node_id: r.get(0)?,
                mcp_endpoint: r.get(1)?,
                internal_endpoint: r.get(2)?,
                capacity: r.get(3)?,
                pond_count: r.get::<_, i64>(4)? as u32,
                state: r.get(5)?,
                last_heartbeat: r.get(6)?,
                heartbeat_age_seconds: r.get(7)?,
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
        tier: &str,
        extensions: &[String],
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
        let ext_csv = extensions.join(",");
        c.execute(
            "INSERT INTO ponds(pond_id, name, owner_identity, node_id, policy_json, tier, extensions) VALUES (?,?,?,?,?,?,?)",
            duckdb::params![pond_id, name, owner_identity, node_id, policy_json, tier, ext_csv],
        )?;
        // Persist immediately so the pond survives a lost .wal (see register_node).
        let _ = c.execute_batch("CHECKPOINT");
        metrics::counter!("latiq_pond_allocations_total").increment(1);
        Ok(PondRow {
            pond_id,
            name,
            owner_identity: owner_identity.to_string(),
            node_id,
            tier: tier.to_string(),
            extensions: extensions.to_vec(),
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
            "SELECT pond_id, name, owner_identity, node_id, coalesce(tier, 'medium'),
                    coalesce(extensions, '') FROM ponds ORDER BY created_at",
        )?;
        let rows = stmt.query_map([], |r| {
            Ok(PondRow {
                pond_id: r.get(0)?,
                name: r.get(1)?,
                owner_identity: r.get(2)?,
                node_id: r.get(3)?,
                tier: r.get(4)?,
                extensions: latiq_common::extensions::parse_csv(&r.get::<_, String>(5)?),
            })
        })?;
        Ok(rows.collect::<Result<_, _>>()?)
    }

    /// Returns (PondRow, created_at, policy_json, owning node's internal_endpoint)
    /// resolved by id or name. The endpoint is `None` if the owning node has gone
    /// (LEFT JOIN) — the pond row still exists, it just has no live host.
    pub fn pond_info(
        &self,
        pond_ref: &str,
    ) -> Result<(PondRow, String, String, Option<String>), ControlPlaneError> {
        let c = self.lock();
        c.query_row(
            "SELECT p.pond_id, p.name, p.owner_identity, p.node_id,
                    p.created_at::VARCHAR, p.policy_json, n.internal_endpoint,
                    coalesce(p.tier, 'medium'), coalesce(p.extensions, '')
             FROM ponds p LEFT JOIN nodes n ON n.node_id = p.node_id
             WHERE p.pond_id=? OR p.name=? LIMIT 1",
            duckdb::params![pond_ref, pond_ref],
            |r| {
                Ok((
                    PondRow {
                        pond_id: r.get(0)?,
                        name: r.get(1)?,
                        owner_identity: r.get(2)?,
                        node_id: r.get(3)?,
                        tier: r.get::<_, String>(7)?,
                        extensions: latiq_common::extensions::parse_csv(&r.get::<_, String>(8)?),
                    },
                    r.get::<_, String>(4)?,
                    r.get::<_, String>(5)?,
                    r.get::<_, Option<String>>(6)?,
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
        let _ = c.execute_batch("CHECKPOINT");
        Ok(())
    }

    // ---- datasets (file tables in the built-in `latiq` catalog) -----------

    /// Add (or replace) a dataset. Tables/tags are replaced wholesale so re-adding
    /// is idempotent.
    pub fn add_dataset(&self, d: &DatasetRow) -> Result<String, ControlPlaneError> {
        if d.name.is_empty() {
            return Err(ControlPlaneError::Invalid(
                "dataset name is required".into(),
            ));
        }
        if !is_ident(&d.name) {
            return Err(ControlPlaneError::Invalid(
                "dataset name must be a bare identifier (letters/digits/underscore)".into(),
            ));
        }
        if d.tables.is_empty() {
            return Err(ControlPlaneError::Invalid(
                "a dataset needs at least one table".into(),
            ));
        }
        let c = self.lock();
        c.execute(
            "DELETE FROM dataset_tables WHERE dataset=?",
            duckdb::params![d.name],
        )?;
        c.execute(
            "DELETE FROM dataset_tags WHERE dataset=?",
            duckdb::params![d.name],
        )?;
        c.execute("DELETE FROM datasets WHERE name=?", duckdb::params![d.name])?;
        c.execute(
            "INSERT INTO datasets(name, description, created_by) VALUES (?,?,?)",
            duckdb::params![d.name, d.description, d.created_by],
        )?;
        for t in &d.tables {
            c.execute(
                "INSERT INTO dataset_tables(dataset, table_name, source_uri, format) VALUES (?,?,?,?)",
                duckdb::params![d.name, t.table_name, t.source_uri, t.format],
            )?;
        }
        for tag in &d.tags {
            c.execute(
                "INSERT OR IGNORE INTO dataset_tags(dataset, tag) VALUES (?,?)",
                duckdb::params![d.name, tag],
            )?;
        }
        let _ = c.execute_batch("CHECKPOINT");
        Ok(d.name.clone())
    }

    pub fn remove_dataset(&self, name: &str) -> Result<(), ControlPlaneError> {
        let c = self.lock();
        let n = c.execute("DELETE FROM datasets WHERE name=?", duckdb::params![name])?;
        if n == 0 {
            return Err(ControlPlaneError::DatasetNotFound(name.to_string()));
        }
        c.execute(
            "DELETE FROM dataset_tables WHERE dataset=?",
            duckdb::params![name],
        )?;
        c.execute(
            "DELETE FROM dataset_tags WHERE dataset=?",
            duckdb::params![name],
        )?;
        let _ = c.execute_batch("CHECKPOINT");
        Ok(())
    }

    pub fn get_dataset(&self, name: &str) -> Result<DatasetRow, ControlPlaneError> {
        let c = self.lock();
        let (description, created_by, created_at) = c
            .query_row(
                "SELECT description, created_by, created_at::VARCHAR FROM datasets WHERE name=?",
                duckdb::params![name],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
            )
            .map_err(|_| ControlPlaneError::DatasetNotFound(name.to_string()))?;
        let mut ts = c.prepare(
            "SELECT table_name, source_uri, format FROM dataset_tables
             WHERE dataset=? ORDER BY table_name",
        )?;
        let tables = ts
            .query_map(duckdb::params![name], |r| {
                Ok(DatasetTableRow {
                    table_name: r.get(0)?,
                    source_uri: r.get(1)?,
                    format: r.get(2)?,
                })
            })?
            .collect::<Result<_, _>>()?;
        let tags = tags_for(&c, "dataset_tags", "dataset", name)?;
        Ok(DatasetRow {
            name: name.to_string(),
            description,
            tags,
            tables,
            created_by,
            created_at,
        })
    }

    /// Search datasets. `query`: empty = all; `#tag` = by tag; `prefix*` = name
    /// glob; otherwise a substring over name/description/tag.
    pub fn list_datasets(&self, query: &str) -> Result<Vec<DatasetRow>, ControlPlaneError> {
        let names = self.search("datasets", "dataset_tags", "dataset", query)?;
        names.into_iter().map(|n| self.get_dataset(&n)).collect()
    }

    // ---- external catalogs ------------------------------------------------

    /// Add (or replace) an external catalog. `params` should already be filtered
    /// to the type's allowlist (no credentials) by the caller.
    pub fn add_catalog(&self, ca: &CatalogRow) -> Result<String, ControlPlaneError> {
        if ca.name.is_empty() || ca.r#type.is_empty() {
            return Err(ControlPlaneError::Invalid(
                "catalog name and type are required".into(),
            ));
        }
        if !is_ident(&ca.name) {
            return Err(ControlPlaneError::Invalid(
                "catalog name must be a bare identifier (letters/digits/underscore)".into(),
            ));
        }
        let params_json = serde_json::to_string(&ca.params)
            .map_err(|e| ControlPlaneError::Invalid(e.to_string()))?;
        let c = self.lock();
        c.execute(
            "DELETE FROM catalog_tags WHERE catalog=?",
            duckdb::params![ca.name],
        )?;
        c.execute(
            "DELETE FROM catalogs WHERE name=?",
            duckdb::params![ca.name],
        )?;
        c.execute(
            "INSERT INTO catalogs(name, type, params_json, description, created_by) VALUES (?,?,?,?,?)",
            duckdb::params![ca.name, ca.r#type, params_json, ca.description, ca.created_by],
        )?;
        for tag in &ca.tags {
            c.execute(
                "INSERT OR IGNORE INTO catalog_tags(catalog, tag) VALUES (?,?)",
                duckdb::params![ca.name, tag],
            )?;
        }
        let _ = c.execute_batch("CHECKPOINT");
        Ok(ca.name.clone())
    }

    pub fn remove_catalog(&self, name: &str) -> Result<(), ControlPlaneError> {
        let c = self.lock();
        let n = c.execute("DELETE FROM catalogs WHERE name=?", duckdb::params![name])?;
        if n == 0 {
            return Err(ControlPlaneError::CatalogNotFound(name.to_string()));
        }
        c.execute(
            "DELETE FROM catalog_tags WHERE catalog=?",
            duckdb::params![name],
        )?;
        let _ = c.execute_batch("CHECKPOINT");
        Ok(())
    }

    pub fn get_catalog(&self, name: &str) -> Result<CatalogRow, ControlPlaneError> {
        let c = self.lock();
        let (r#type, params_json, description, created_by, created_at) = c
            .query_row(
                "SELECT type, params_json, description, created_by, created_at::VARCHAR
                 FROM catalogs WHERE name=?",
                duckdb::params![name],
                |r| {
                    Ok((
                        r.get(0)?,
                        r.get::<_, String>(1)?,
                        r.get(2)?,
                        r.get(3)?,
                        r.get(4)?,
                    ))
                },
            )
            .map_err(|_| ControlPlaneError::CatalogNotFound(name.to_string()))?;
        let params = serde_json::from_str(&params_json).unwrap_or_default();
        let tags = tags_for(&c, "catalog_tags", "catalog", name)?;
        Ok(CatalogRow {
            name: name.to_string(),
            r#type,
            params,
            description,
            tags,
            created_by,
            created_at,
        })
    }

    pub fn list_catalogs(&self, query: &str) -> Result<Vec<CatalogRow>, ControlPlaneError> {
        let names = self.search("catalogs", "catalog_tags", "catalog", query)?;
        names.into_iter().map(|n| self.get_catalog(&n)).collect()
    }

    /// Shared name search over a `<table>(name)` + `<tag_table>(<owner_col>, tag)`
    /// pair: empty = all; `#tag` = by tag; `prefix*` = name glob; else substring
    /// over name/description/tag.
    fn search(
        &self,
        table: &str,
        tag_table: &str,
        owner_col: &str,
        query: &str,
    ) -> Result<Vec<String>, ControlPlaneError> {
        let c = self.lock();
        let q = query.trim();
        let names: Vec<String> = if q.is_empty() {
            let mut s = c.prepare(&format!("SELECT name FROM {table} ORDER BY name"))?;
            s.query_map([], |r| r.get(0))?.collect::<Result<_, _>>()?
        } else if let Some(tag) = q.strip_prefix('#') {
            let mut s = c.prepare(&format!(
                "SELECT {owner_col} FROM {tag_table} WHERE tag=? ORDER BY {owner_col}"
            ))?;
            s.query_map(duckdb::params![tag], |r| r.get(0))?
                .collect::<Result<_, _>>()?
        } else if q.contains('*') {
            let like = q.replace('*', "%");
            let mut s = c.prepare(&format!(
                "SELECT name FROM {table} WHERE name LIKE ? ORDER BY name"
            ))?;
            s.query_map(duckdb::params![like], |r| r.get(0))?
                .collect::<Result<_, _>>()?
        } else {
            let like = format!("%{q}%");
            let mut s = c.prepare(&format!(
                "SELECT name FROM {table}
                 WHERE name ILIKE ?1 OR description ILIKE ?1
                    OR name IN (SELECT {owner_col} FROM {tag_table} WHERE tag ILIKE ?1)
                 ORDER BY name"
            ))?;
            s.query_map(duckdb::params![like], |r| r.get(0))?
                .collect::<Result<_, _>>()?
        };
        Ok(names)
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
}

/// A bare SQL identifier — letters/digits/underscore, starting with a letter or
/// underscore. Catalog/dataset names flow into SQL (schema/catalog aliases) that
/// agents write by hand, so we reject anything that would need quoting.
fn is_ident(s: &str) -> bool {
    let mut chars = s.chars();
    matches!(chars.next(), Some(c) if c.is_ascii_alphabetic() || c == '_')
        && chars.all(|c| c.is_ascii_alphanumeric() || c == '_')
}

/// Read tags from a `<tag_table>(<owner_col>, tag)` table for one owner.
fn tags_for(
    c: &Connection,
    tag_table: &str,
    owner_col: &str,
    owner: &str,
) -> Result<Vec<String>, ControlPlaneError> {
    let mut s = c.prepare(&format!(
        "SELECT tag FROM {tag_table} WHERE {owner_col}=? ORDER BY tag"
    ))?;
    Ok(s.query_map(duckdb::params![owner], |r| r.get(0))?
        .collect::<Result<_, _>>()?)
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
        r.create_pond(Some("p-one".into()), "agent-x", "{\"k\":1}", "medium", &[])
            .unwrap();
        r.create_pond(Some("p-two".into()), "agent-y", "{}", "medium", &[])
            .unwrap();
        let ponds = r.list_ponds().unwrap();
        assert_eq!(ponds.len(), 2);
        let (row, created_at, policy, endpoint) = r.pond_info("p-one").unwrap();
        assert_eq!(row.name, "p-one");
        assert_eq!(row.owner_identity, "agent-x");
        assert!(!created_at.is_empty());
        assert_eq!(policy, "{\"k\":1}");
        // pond_info joins the owning node so a greeter can resolve where to forward.
        assert_eq!(endpoint.as_deref(), Some("http://n:9092"));
        assert!(matches!(
            r.pond_info("nope"),
            Err(ControlPlaneError::PondNotFound(_))
        ));
    }

    fn dataset(name: &str, desc: &str, tags: &[&str], uri: &str) -> DatasetRow {
        DatasetRow {
            name: name.into(),
            description: desc.into(),
            tags: tags.iter().map(|s| s.to_string()).collect(),
            tables: vec![DatasetTableRow {
                table_name: name.into(),
                source_uri: uri.into(),
                format: "auto".into(),
            }],
            created_by: "op".into(),
            created_at: String::new(),
        }
    }

    #[test]
    fn datasets_seeded_add_search_remove() {
        let r = reg();
        // The v4 migration seeds the samples in the latiq catalog.
        assert!(r
            .list_datasets("")
            .unwrap()
            .iter()
            .any(|d| d.name == "tpch"));
        let tpch = r.get_dataset("tpch").unwrap();
        assert_eq!(tpch.tables.len(), 8);
        assert!(tpch.tags.contains(&"tpch".to_string()));

        // Add, search by tag/glob/substring, idempotent re-add, remove.
        assert_eq!(
            r.add_dataset(&dataset(
                "sales",
                "Acme sales",
                &["finance"],
                "https://x/s.parquet"
            ))
            .unwrap(),
            "sales"
        );
        assert_eq!(r.list_datasets("#finance").unwrap().len(), 1);
        assert_eq!(r.list_datasets("sal*").unwrap()[0].name, "sales");
        assert!(r
            .list_datasets("acme")
            .unwrap()
            .iter()
            .any(|d| d.name == "sales"));
        r.add_dataset(&dataset("sales", "v2", &[], "https://x/s2.parquet"))
            .unwrap();
        assert_eq!(r.get_dataset("sales").unwrap().description, "v2");
        r.remove_dataset("sales").unwrap();
        assert!(matches!(
            r.get_dataset("sales"),
            Err(ControlPlaneError::DatasetNotFound(_))
        ));
        // Names must be bare identifiers (they flow into SQL).
        assert!(r.add_dataset(&dataset("bad.name", "", &[], "u")).is_err());
    }

    #[test]
    fn catalog_add_get_remove() {
        let r = reg();
        let ca = CatalogRow {
            name: "lake".into(),
            r#type: "iceberg".into(),
            params: std::collections::BTreeMap::from([
                ("endpoint".to_string(), "https://polaris/api".to_string()),
                ("warehouse".to_string(), "prod".to_string()),
            ]),
            description: "Acme lake".into(),
            tags: vec!["prod".into()],
            created_by: "op".into(),
            created_at: String::new(),
        };
        assert_eq!(r.add_catalog(&ca).unwrap(), "lake");
        let got = r.get_catalog("lake").unwrap();
        assert_eq!(got.r#type, "iceberg");
        assert_eq!(got.params["warehouse"], "prod");
        assert_eq!(r.list_catalogs("#prod").unwrap().len(), 1);
        r.remove_catalog("lake").unwrap();
        assert!(matches!(
            r.get_catalog("lake"),
            Err(ControlPlaneError::CatalogNotFound(_))
        ));
    }

    #[test]
    fn create_pond_round_trips_extensions() {
        let r = reg();
        r.register_node("node-a", "http://n/mcp", "http://n:9092", 100)
            .unwrap();
        let row = r
            .create_pond(
                Some("geo".into()),
                "x",
                "{}",
                "large",
                &["spatial".to_string()],
            )
            .unwrap();
        assert_eq!(row.extensions, vec!["spatial".to_string()]);
        // pond_info and list_ponds both carry the extension set back.
        let (info, ..) = r.pond_info("geo").unwrap();
        assert_eq!(info.extensions, vec!["spatial".to_string()]);
        let listed = r.list_ponds().unwrap();
        assert_eq!(listed[0].extensions, vec!["spatial".to_string()]);
        // A pond created with no extensions reads back as empty.
        r.create_pond(Some("plain".into()), "x", "{}", "medium", &[])
            .unwrap();
        let (plain, ..) = r.pond_info("plain").unwrap();
        assert!(plain.extensions.is_empty());
    }

    #[test]
    fn node_and_pond_lifecycle() {
        let r = reg();
        r.register_node("node-a", "http://n:8080/mcp", "http://n:9092", 100)
            .unwrap();
        r.heartbeat("node-a", 3).unwrap();
        assert_eq!(r.list_nodes().unwrap().len(), 1);
        let p = r
            .create_pond(Some("incident-1".into()), "agent-x", "{}", "medium", &[])
            .unwrap();
        assert_eq!(p.name, "incident-1");
        let (pid, endpoint) = r.get_pond_location("incident-1").unwrap();
        assert_eq!(pid, p.pond_id);
        assert_eq!(endpoint, "http://n:9092");
        assert!(matches!(
            r.create_pond(Some("incident-1".into()), "y", "{}", "medium", &[]),
            Err(ControlPlaneError::NameConflict(_))
        ));
        r.drop_pond(&p.pond_id).unwrap();
        assert!(matches!(
            r.get_pond_location("incident-1"),
            Err(ControlPlaneError::PondNotFound(_))
        ));
    }

    #[test]
    fn reaper_downs_stale_node_and_heartbeat_revives() {
        let r = reg();
        r.register_node("node-a", "http://n/mcp", "http://n:9092", 10)
            .unwrap();
        // Fresh registration → not reaped.
        assert_eq!(r.reap_stale_nodes(60).unwrap(), 0);
        assert_eq!(r.describe_node("node-a").unwrap().state, "active");

        // Age past a 1s TTL, then reap → downed.
        std::thread::sleep(std::time::Duration::from_millis(1100));
        assert_eq!(r.reap_stale_nodes(1).unwrap(), 1);
        let n = r.describe_node("node-a").unwrap();
        assert_eq!(n.state, "down");
        assert!(n.heartbeat_age_seconds >= 1, "age tracked: {n:?}");

        // A heartbeat revives it; reaping again is a no-op (beat is fresh).
        r.heartbeat("node-a", 0).unwrap();
        assert_eq!(r.describe_node("node-a").unwrap().state, "active");
        assert_eq!(r.reap_stale_nodes(1).unwrap(), 0);
    }

    #[test]
    fn placement_skips_down_nodes_and_pond_count_is_live() {
        let r = reg();
        r.register_node("node-a", "http://a/mcp", "http://a:9092", 10)
            .unwrap();
        r.register_node("node-b", "http://b/mcp", "http://b:9092", 10)
            .unwrap();
        // Down both, then revive only node-b.
        std::thread::sleep(std::time::Duration::from_millis(1100));
        assert_eq!(r.reap_stale_nodes(1).unwrap(), 2);
        r.heartbeat("node-b", 0).unwrap();

        // create_pond must pick the only active node (node-b), repeatedly.
        for i in 0..5 {
            let p = r
                .create_pond(Some(format!("p{i}")), "x", "{}", "medium", &[])
                .unwrap();
            assert_eq!(p.node_id, "node-b", "placement skipped down node-a");
        }
        // pond_count is computed live from assignments, not the heartbeat.
        let b = r.describe_node("node-b").unwrap();
        assert_eq!(b.pond_count, 5);
        assert_eq!(r.describe_node("node-a").unwrap().pond_count, 0);
    }

    #[test]
    fn create_pond_without_node_errors() {
        let r = reg();
        assert!(matches!(
            r.create_pond(None, "x", "{}", "medium", &[]),
            Err(ControlPlaneError::NodeNotFound(_))
        ));
    }

    #[test]
    fn create_pond_checkpoints_so_main_file_survives_wal_loss() {
        // create_pond/register_node CHECKPOINT, so the data is in registry.duckdb
        // (not only the .wal). Copying ONLY the main file (no .wal) and reopening
        // must still show the pond — i.e. a deleted .wal can't empty the registry,
        // even though serve is usually killed without a clean (checkpointing) close.
        let dir = std::env::temp_dir().join(format!("latiq-ckpt-{}", PondId::new()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("registry.duckdb");
        let r = Registry::open(Some(&path)).unwrap();
        r.register_node("node-a", "http://n/mcp", "http://n:9092", 10)
            .unwrap();
        r.create_pond(Some("durable".into()), "agent-x", "{}", "medium", &[])
            .unwrap();

        let copy = dir.join("copy.duckdb");
        std::fs::copy(&path, &copy).unwrap();
        let reopened = Registry::open(Some(&copy)).unwrap();
        assert_eq!(
            reopened.list_ponds().unwrap().len(),
            1,
            "pond must persist in the main file (WAL was checkpointed)"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn policy_get_set() {
        let r = reg();
        r.policy_set("query_timeout_seconds", "60").unwrap();
        assert_eq!(r.policy_get().unwrap()["query_timeout_seconds"], "60");
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
