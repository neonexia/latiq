//! Forwarding decision in AgentOps: a request for a pond owned by another node
//! is delegated to the `Forwarder`; a pond owned by *this* node (or with no live
//! owner) runs locally. Uses a fake ControlPlane (to pin the owner endpoint) and
//! a recording Forwarder (to observe delegation) — no real cluster needed.
use latiq_agent_core::{
    AgentConfig, AgentError, AgentOps, ArrowReadStream, AuditRecord, ControlPlane, DescribeResult,
    Forwarder, PondInfo,
};
use latiq_common::{Identity, QueryMeta};
use latiq_engine::{ExplainResult, QueryResult, SchemaSummary};
use latiq_engine_duckdb::DuckEngine;
use latiq_storage::TempFs;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

const PID: &str = "00000000-0000-0000-0000-000000000001";

/// Every pond resolves to a fixed owner endpoint (or none).
struct FixedOwner {
    endpoint: Option<String>,
}

#[async_trait::async_trait]
impl ControlPlane for FixedOwner {
    async fn create_pond(
        &self,
        _: Option<String>,
        _: &str,
        _: &str,
        _: &str,
        _: &[String],
    ) -> Result<PondInfo, AgentError> {
        unreachable!("not exercised")
    }
    async fn resolve_pond(&self, pond_ref: &str) -> Result<String, AgentError> {
        Ok(pond_ref.to_string())
    }
    async fn list_ponds(&self) -> Result<Vec<PondInfo>, AgentError> {
        Ok(vec![])
    }
    async fn pond_info(&self, pond_ref: &str) -> Result<PondInfo, AgentError> {
        Ok(PondInfo {
            pond_id: PID.to_string(),
            name: pond_ref.to_string(),
            owner: "owner".to_string(),
            created_at: String::new(),
            policy_json: "{}".to_string(),
            node_endpoint: self.endpoint.clone(),
            tier: "medium".to_string(),
            extensions: vec![],
        })
    }
    async fn drop_pond(&self, _: &str) -> Result<(), AgentError> {
        Ok(())
    }
    async fn list_datasets(
        &self,
        _: &str,
    ) -> Result<Vec<latiq_agent_core::DatasetInfo>, AgentError> {
        Ok(vec![])
    }
    async fn get_dataset(&self, r: &str) -> Result<latiq_agent_core::DatasetInfo, AgentError> {
        Err(AgentError::dataset_not_found(r))
    }
    async fn list_catalogs(
        &self,
        _: &str,
    ) -> Result<Vec<latiq_agent_core::CatalogInfo>, AgentError> {
        Ok(vec![])
    }
    async fn get_catalog(&self, r: &str) -> Result<latiq_agent_core::CatalogInfo, AgentError> {
        Err(AgentError::internal(format!("no catalog {r}")))
    }
    async fn record_audit(&self, _: AuditRecord) {}
}

#[derive(Default)]
struct RecordingForwarder {
    reads: AtomicUsize,
    writes: AtomicUsize,
    pulls: AtomicUsize,
    describes: AtomicUsize,
    last_endpoint: Mutex<String>,
    last_pond: Mutex<String>,
    last_sql: Mutex<String>,
}

impl RecordingForwarder {
    fn note(&self, endpoint: &str, pond: &str, sql: &str) {
        *self.last_endpoint.lock().unwrap() = endpoint.to_string();
        *self.last_pond.lock().unwrap() = pond.to_string();
        *self.last_sql.lock().unwrap() = sql.to_string();
    }
}

fn sentinel(tag: &str) -> QueryResult {
    QueryResult {
        columns: vec![tag.to_string()],
        rows: vec![],
        meta: QueryMeta::default(),
    }
}

#[async_trait::async_trait]
impl Forwarder for RecordingForwarder {
    async fn read(
        &self,
        e: &str,
        _: &Identity,
        p: &str,
        s: &str,
    ) -> Result<QueryResult, AgentError> {
        self.reads.fetch_add(1, Ordering::SeqCst);
        self.note(e, p, s);
        Ok(sentinel("forwarded_read"))
    }
    async fn write(
        &self,
        e: &str,
        _: &Identity,
        p: &str,
        s: &str,
    ) -> Result<QueryResult, AgentError> {
        self.writes.fetch_add(1, Ordering::SeqCst);
        self.note(e, p, s);
        Ok(sentinel("forwarded_write"))
    }
    async fn read_arrow(
        &self,
        e: &str,
        _: &Identity,
        p: &str,
        s: &str,
    ) -> Result<ArrowReadStream, AgentError> {
        self.reads.fetch_add(1, Ordering::SeqCst);
        self.note(e, p, s);
        Ok(ArrowReadStream {
            schema: std::sync::Arc::new(arrow::datatypes::Schema::empty()),
            batches: Box::pin(tokio_stream::iter(Vec::new())),
        })
    }
    async fn explain(
        &self,
        e: &str,
        _: &Identity,
        p: &str,
        s: &str,
    ) -> Result<ExplainResult, AgentError> {
        self.note(e, p, s);
        Ok(ExplainResult {
            estimated_rows: 0,
            estimated_bytes: 0,
            estimated_duration_ms: 0,
            scan_operations: vec![],
            warnings: vec![],
            suggestions: vec![],
            raw_plan: "forwarded".to_string(),
        })
    }
    async fn describe(&self, e: &str, _: &Identity, p: &str) -> Result<DescribeResult, AgentError> {
        self.note(e, p, "");
        Ok(DescribeResult {
            pond: PondInfo {
                pond_id: PID.to_string(),
                name: p.to_string(),
                owner: "owner".to_string(),
                created_at: String::new(),
                policy_json: "{}".to_string(),
                node_endpoint: Some(e.to_string()),
                tier: "medium".to_string(),
                extensions: vec![],
            },
            schema: SchemaSummary::default(),
        })
    }
    async fn drop_pond(&self, e: &str, _: &Identity, p: &str, _: bool) -> Result<(), AgentError> {
        self.note(e, p, "");
        Ok(())
    }
    async fn catalog_pull(
        &self,
        e: &str,
        _: &Identity,
        p: &str,
        catalog: &str,
        q: &str,
        _params: std::collections::BTreeMap<String, String>,
    ) -> Result<latiq_agent_core::PullResult, AgentError> {
        self.pulls.fetch_add(1, Ordering::SeqCst);
        self.note(e, p, q);
        Ok(latiq_agent_core::PullResult {
            catalog: catalog.to_string(),
            query: q.to_string(),
        })
    }
    async fn catalog_describe(
        &self,
        e: &str,
        _: &Identity,
        p: &str,
        _catalog: &str,
        _params: std::collections::BTreeMap<String, String>,
    ) -> Result<Vec<(String, String)>, AgentError> {
        self.describes.fetch_add(1, Ordering::SeqCst);
        self.note(e, p, "");
        Ok(vec![("main".to_string(), "forwarded_table".to_string())])
    }
}

fn ops_with(owner: Option<&str>, self_ep: &str, fwd: Arc<RecordingForwarder>) -> AgentOps {
    let control = Arc::new(FixedOwner {
        endpoint: owner.map(|s| s.to_string()),
    });
    AgentOps::new(
        control,
        Arc::new(TempFs::new()),
        Arc::new(DuckEngine::new()),
        AgentConfig::default(),
    )
    .with_forwarding(self_ep.to_string(), fwd)
}

#[tokio::test]
async fn forwarding_read_delegates_to_owner() {
    let fwd = Arc::new(RecordingForwarder::default());
    let ops = ops_with(
        Some("http://owner:9092"),
        "http://greeter:9092",
        fwd.clone(),
    );
    let r = ops
        .read_query(&Identity::claimed(Some("a")), "pond-x", "SELECT 1")
        .await
        .unwrap();
    assert_eq!(fwd.reads.load(Ordering::SeqCst), 1);
    assert_eq!(*fwd.last_endpoint.lock().unwrap(), "http://owner:9092");
    assert_eq!(*fwd.last_pond.lock().unwrap(), "pond-x");
    assert_eq!(*fwd.last_sql.lock().unwrap(), "SELECT 1");
    // The sentinel proves the result came from the forwarder, not a local run.
    assert_eq!(r.columns, vec!["forwarded_read".to_string()]);
}

#[tokio::test]
async fn forwarding_write_delegates_to_owner() {
    let fwd = Arc::new(RecordingForwarder::default());
    let ops = ops_with(
        Some("http://owner:9092"),
        "http://greeter:9092",
        fwd.clone(),
    );
    let r = ops
        .write_query(
            &Identity::claimed(Some("a")),
            "pond-x",
            "CREATE TABLE t(i INT)",
        )
        .await
        .unwrap();
    assert_eq!(fwd.writes.load(Ordering::SeqCst), 1);
    assert_eq!(r.columns, vec!["forwarded_write".to_string()]);
}

#[tokio::test]
async fn forwarding_skipped_when_self_owns() {
    // self_endpoint == owner → no forward; the read runs locally (DuckDB).
    let fwd = Arc::new(RecordingForwarder::default());
    let ops = ops_with(Some("http://me:9092"), "http://me:9092", fwd.clone());
    let r = ops
        .read_query(&Identity::claimed(Some("a")), "pond-x", "SELECT 1 AS one")
        .await
        .unwrap();
    assert_eq!(
        fwd.reads.load(Ordering::SeqCst),
        0,
        "must not forward to self"
    );
    assert_eq!(r.rows[0][0], serde_json::json!(1));
}

#[tokio::test]
async fn forwarding_catalog_pull_delegates_to_owner() {
    let fwd = Arc::new(RecordingForwarder::default());
    let ops = ops_with(
        Some("http://owner:9092"),
        "http://greeter:9092",
        fwd.clone(),
    );
    let r = ops
        .catalog_pull(
            &Identity::claimed(Some("a")),
            "pond-x",
            "lake",
            "CREATE TABLE t AS SELECT 1",
            std::collections::BTreeMap::new(),
        )
        .await
        .unwrap();
    assert_eq!(fwd.pulls.load(Ordering::SeqCst), 1);
    assert_eq!(*fwd.last_endpoint.lock().unwrap(), "http://owner:9092");
    assert_eq!(*fwd.last_pond.lock().unwrap(), "pond-x");
    assert_eq!(r.catalog, "lake");
}

#[tokio::test]
async fn forwarding_catalog_describe_delegates_to_owner() {
    let fwd = Arc::new(RecordingForwarder::default());
    let ops = ops_with(
        Some("http://owner:9092"),
        "http://greeter:9092",
        fwd.clone(),
    );
    let tables = ops
        .catalog_describe(
            &Identity::claimed(Some("a")),
            "pond-x",
            "lake",
            std::collections::BTreeMap::new(),
        )
        .await
        .unwrap();
    assert_eq!(fwd.describes.load(Ordering::SeqCst), 1);
    assert_eq!(*fwd.last_endpoint.lock().unwrap(), "http://owner:9092");
    assert_eq!(
        tables,
        vec![("main".to_string(), "forwarded_table".to_string())]
    );
}

#[tokio::test]
async fn forwarding_skipped_when_no_live_owner() {
    // node_endpoint == None (owning node gone) → run locally, surface real errors
    // rather than forwarding to nowhere.
    let fwd = Arc::new(RecordingForwarder::default());
    let ops = ops_with(None, "http://me:9092", fwd.clone());
    let r = ops
        .read_query(&Identity::claimed(Some("a")), "pond-x", "SELECT 1 AS one")
        .await
        .unwrap();
    assert_eq!(fwd.reads.load(Ordering::SeqCst), 0);
    assert_eq!(r.rows[0][0], serde_json::json!(1));
}
