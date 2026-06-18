# Slice B — SDK handle redesign + Arrow streaming reads + front-door routing

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Reshape the SDK so the **pond is the object and SQL is the verb**: a
handle-centric API, `query()` returns a `pyarrow.Table` over the existing
`ReadArrow` stream, and the data path goes through a **front door + greeter
forwarding** (never node-direct, which is dead behind a k8s LB).

**Architecture:** Rust core (`latiq-sdk`) gains front-door routing + an Arrow read
that drains the `Stream::ReadArrow` IPC chunks; PyO3 (`sdk/python`) exposes a
handle (`Pond`) returning `pyarrow.Table`. Reads stream (uncapped); writes stay on
unary `WriteQuery`. MCP is untouched.

**Tech Stack:** Rust, tonic gRPC (`Stream`/`Data` clients), `arrow` (IPC decode),
PyO3 0.22 (abi3), `pyarrow` (runtime, Python side).

**Spec:** `docs/superpowers/specs/2026-06-18-sdk-handle-arrow-design.md`
(Routing + Slice 3). **Depends on Slice A merged** (the `description` field on
`PondInfoMsg`/`PondSummary`/neutral `PondInfo`).

**Branch:** `sdk-handle-arrow` off `main` AFTER Slice A is merged (so the proto has
`description`). Direct push to `main` is blocked — branch → PR → merge.

**Pre-req sanity check (run first):**
```bash
grep -n "description" crates/latiq-proto/proto/latiq/v1/control.proto   # must show field 9 on PondInfoMsg
```
If absent, Slice A isn't merged — stop and rebase onto it.

---

### Task 1: Front-door routing (kill node-direct `data_for`)

Replace the unconditional `get_pond_location`→node-direct dial with a single
**data front door**: embedded → the in-process node; remote → `query_gateway`
(or `server`). The greeter forwards by pond for all data ops.

**Files:**
- Modify: `crates/latiq-sdk/src/lib.rs` — `Latiq` struct (`:33-39`), `connect` (`:53-81`), `data_for` (`:216-228`), `LocalCluster` (`:241-276`)
- Test: `crates/latiq-sdk/tests/embedded.rs` (existing — must still pass)

- [ ] **Step 1: Store a data endpoint on `Latiq` + `LocalCluster`.** In
`LocalCluster` (`:241-243`) add the node's data endpoint (we already compute
`data_port` at `:249` and `internal_endpoint` at `:266`):

```rust
struct LocalCluster {
    control_endpoint: String,
    data_endpoint: String,
}
```
In `LocalCluster::start` (`:275`), return it:
```rust
        Ok(Self {
            control_endpoint,
            data_endpoint: format!("http://127.0.0.1:{data_port}"),
        })
```
Add a `data_endpoint` field to `Latiq` (`:33-39`):
```rust
pub struct Latiq {
    rt: Arc<tokio::runtime::Runtime>,
    control_endpoint: String,
    /// The Data+Stream front door: the in-process node (embedded) or the query
    /// gateway / control front door (remote). Data ops dial THIS and rely on the
    /// greeter to forward by pond — never node-direct (unroutable behind a LB).
    data_endpoint: String,
    identity: String,
    _local: Option<LocalCluster>,
}
```

- [ ] **Step 2: Set `data_endpoint` + add `query_gateway` to `connect`.** Rewrite
`connect` (`:53-81`) signature and both branches:

```rust
    pub fn connect(server: &str, root: Option<PathBuf>) -> Result<Self> {
        Self::connect_with(server, root, None)
    }

    /// `query_gateway`: the Data/Stream front door when it differs from `server`
    /// (e.g. nginx exposes Control/Admin and Data/Stream on separate addresses).
    /// `None` → reuse `server` (unified front door). Ignored for `"local"`.
    pub fn connect_with(
        server: &str,
        root: Option<PathBuf>,
        query_gateway: Option<&str>,
    ) -> Result<Self> {
        let rt = Arc::new(
            tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .worker_threads(2)
                .build()?,
        );
        if server == "local" {
            let root = root.unwrap_or_else(default_local_root);
            let local = rt.block_on(LocalCluster::start(&rt, &root))?;
            let control_endpoint = local.control_endpoint.clone();
            let data_endpoint = local.data_endpoint.clone();
            Ok(Self {
                rt,
                control_endpoint,
                data_endpoint,
                identity: "sdk".into(),
                _local: Some(local),
            })
        } else {
            let control_endpoint = normalize_endpoint(server);
            let data_endpoint = query_gateway
                .map(normalize_endpoint)
                .unwrap_or_else(|| control_endpoint.clone());
            rt.block_on(wait_connectable(&control_endpoint))
                .with_context(|| format!("connecting to control plane at {server}"))?;
            Ok(Self {
                rt,
                control_endpoint,
                data_endpoint,
                identity: "sdk".into(),
                _local: None,
            })
        }
    }
```

- [ ] **Step 3: Make `data_for` dial the front door (no `get_pond_location`).**
Replace `data_for` (`:214-228`) entirely:

```rust
    /// A Data gRPC client on the front door. The greeter forwards by pond — we do
    /// NOT resolve the owner node directly (its address is unroutable behind a LB).
    async fn data(&self) -> RtClient<DataClient<Channel>> {
        DataClient::connect(self.data_endpoint.clone())
            .await
            .map_err(|e| anyhow!("data plane unreachable at {}: {e}", self.data_endpoint))
    }
```
Then update the three call sites that used `self.data_for(pond)` —
`describe_pond` (`:145`), `drop_pond` (`:159`), `query` (`:175`) — to
`self.data().await?`. (They pass `pond` in the request body already, so
forwarding routes them.)

- [ ] **Step 4: Run the embedded test — must still pass** (single node = its own
front door, so behaviour is unchanged locally).

Run: `cargo test -p latiq-sdk --test embedded`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add crates/latiq-sdk/src/lib.rs
git commit -m "fix(sdk): route data ops through the front door, not node-direct (k8s-safe)"
```

---

### Task 2: Arrow streaming reads → `Vec<RecordBatch>`

Reads drain the existing `Stream::ReadArrow` IPC and return Arrow batches
(uncapped); writes stay unary. The `query` return type changes from
`serde_json::Value` to `Vec<RecordBatch>`.

**Files:**
- Modify: `crates/latiq-sdk/Cargo.toml` (add `arrow`)
- Modify: `crates/latiq-sdk/src/lib.rs` (imports, `query`, new helpers)
- Test: `crates/latiq-sdk/tests/embedded.rs`

- [ ] **Step 1: Add the `arrow` dep** — `crates/latiq-sdk/Cargo.toml` under
`[dependencies]` (workspace pins `arrow = { version = "58", features = ["ipc",
"json", "chrono-tz"] }`):

```toml
arrow = { workspace = true }
```
> If `arrow` isn't a `[workspace.dependencies]` entry, use the explicit form
> `arrow = { version = "58", features = ["ipc"] }`. Verify with
> `grep -n 'arrow' Cargo.toml`.

- [ ] **Step 2: Write the failing test** — replace the query assertions in
`crates/latiq-sdk/tests/embedded.rs` (`:22-27`) to read from Arrow batches:

```rust
    db.query("work", "CREATE TABLE t(id INTEGER, note VARCHAR)").unwrap();
    db.query("work", "INSERT INTO t VALUES (1,'a'),(2,'b')").unwrap();
    let batches = db.query("work", "SELECT count(*) AS n FROM t").unwrap();
    assert_eq!(batches.len(), 1, "one batch for a scalar");
    let col = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<arrow::array::Int64Array>()
        .expect("count is int64");
    assert_eq!(col.value(0), 2, "round-tripped row count");
```
Add `use arrow::array::Array;` at the top of the test file if the trait isn't in
scope for `.value`.

- [ ] **Step 3: Run it — expect compile failure** (`query` returns `Value`, not
`Vec<RecordBatch>`).

Run: `cargo test -p latiq-sdk --test embedded`
Expected: FAIL — "no method `column` on `serde_json::Value`" / type mismatch.

- [ ] **Step 4: Add imports + the Arrow read helper** to `lib.rs`. Add near the
top:

```rust
use arrow::ipc::reader::StreamDecoder;
use arrow::buffer::Buffer;
use arrow::record_batch::RecordBatch;
use latiq_proto::v1::stream_client::StreamClient;
```
Add a `stream` client helper next to `data` (Task 1):
```rust
    async fn stream(&self) -> RtClient<StreamClient<Channel>> {
        StreamClient::connect(self.data_endpoint.clone())
            .await
            .map_err(|e| anyhow!("stream plane unreachable at {}: {e}", self.data_endpoint))
    }
```
Add the drain helper (mirrors `crates/latiq/tests/arrow_stream.rs:61-83`):
```rust
    /// Drive `Stream::ReadArrow` and collect all IPC chunks into RecordBatches.
    /// Uncapped — the node streams; nothing is buffered server-side.
    async fn read_arrow(&self, pond: &str, sql: &str) -> Result<Vec<RecordBatch>> {
        let mut sc = self.stream().await?;
        let mut streaming = sc
            .read_arrow(self.with_id(QueryRequest {
                pond: pond.to_string(),
                sql: sql.to_string(),
            }))
            .await
            .map_err(|s| anyhow!("read: {}", s.message()))?
            .into_inner();
        let mut decoder = StreamDecoder::new();
        let mut batches = Vec::new();
        while let Some(chunk) = streaming.message().await? {
            let mut buf = Buffer::from_vec(chunk.ipc);
            while !buf.is_empty() {
                match decoder.decode(&mut buf).map_err(|e| anyhow!("arrow ipc: {e}"))? {
                    Some(batch) => batches.push(batch),
                    None => break,
                }
            }
        }
        Ok(batches)
    }
```

- [ ] **Step 5: Rewrite `query` to route read→stream, write→unary** (`:173-188`):

```rust
    /// Run SQL against `pond`. Reads stream over `ReadArrow` and return Arrow
    /// batches (uncapped); writes go unary (attributed/snapshotted server-side)
    /// and return no rows. The client classifies by statement — callers don't
    /// pick read vs write.
    pub fn query(&self, pond: &str, sql: &str) -> Result<Vec<RecordBatch>> {
        self.rt.block_on(async {
            if latiq_engine::is_read_only(sql) {
                self.read_arrow(pond, sql).await
            } else {
                let mut d = self.data().await?;
                d.write_query(self.with_id(QueryRequest {
                    pond: pond.to_string(),
                    sql: sql.to_string(),
                }))
                .await
                .map_err(|s| anyhow!("write: {}", s.message()))?;
                Ok(Vec::new())
            }
        })
    }
```

- [ ] **Step 6: Run the embedded test — expect PASS**

Run: `cargo test -p latiq-sdk --test embedded`
Expected: PASS. (`parse_json` may now be unused — if clippy flags it, delete it
in the gate task.)

- [ ] **Step 7: Commit**

```bash
git add crates/latiq-sdk/Cargo.toml crates/latiq-sdk/src/lib.rs crates/latiq-sdk/tests/embedded.rs
git commit -m "feat(sdk): reads stream over ReadArrow → Arrow batches (uncapped); writes unary"
```

---

### Task 3: Handle-centric Rust API (`PondInfo` + `Pond<'a>`)

`create_pond`/`get_pond` return info that wraps into a `Pond` handle;
`list_ponds` returns a map keyed by name (with `description`); `drop_pond` stays
on `Latiq`.

**Files:**
- Modify: `crates/latiq-sdk/src/lib.rs` — `Pond` struct (`:41-48`), `create_pond` (`:95-122`), `list_ponds` (`:125-140`); add `get_pond`, `Pond` handle, `BTreeMap`
- Test: `crates/latiq-sdk/tests/embedded.rs`

- [ ] **Step 1: Rename the data struct → `PondInfo`, add `description`.** Replace
`Pond` (`:41-48`):

```rust
/// One pond's metadata (from create/get/list).
#[derive(Debug, Clone)]
pub struct PondInfo {
    pub pond_id: String,
    pub name: String,
    pub node_id: String,
    pub tier: String,
    pub description: String,
}

/// A handle to a pond: metadata + SQL. `db.get_pond("x").query("SELECT …")`.
pub struct Pond<'a> {
    latiq: &'a Latiq,
    pub info: PondInfo,
}

impl Pond<'_> {
    pub fn query(&self, sql: &str) -> Result<Vec<arrow::record_batch::RecordBatch>> {
        self.latiq.query(&self.info.name, sql)
    }
    pub fn describe(&self) -> Result<serde_json::Value> {
        self.latiq.describe_pond(&self.info.name)
    }
    pub fn name(&self) -> &str { &self.info.name }
    pub fn id(&self) -> &str { &self.info.pond_id }
    pub fn tier(&self) -> &str { &self.info.tier }
    pub fn node(&self) -> &str { &self.info.node_id }
    pub fn description(&self) -> &str { &self.info.description }
}
```
Add `use std::collections::BTreeMap;` to the imports.

- [ ] **Step 2: `create_pond` returns `Pond<'_>`; carry `description`.** Change its
signature + body (`:95-122`). It now takes `description`, calls
`create_pond_assignment` (with the Slice-A field), reads back info, and wraps:

```rust
    /// Allocate a pond and return a handle. `description` is agent-discovery text.
    pub fn create_pond(
        &self,
        name: Option<&str>,
        tier: &str,
        description: &str,
    ) -> Result<Pond<'_>> {
        let info = self.rt.block_on(async {
            let mut c = self.control().await?;
            let r = c
                .create_pond_assignment(CreatePondAssignmentRequest {
                    name: name.unwrap_or_default().to_string(),
                    owner_identity: self.identity.clone(),
                    policy_json: "{}".into(),
                    tier: tier.to_string(),
                    extensions: vec![],
                    description: description.to_string(),
                })
                .await?
                .into_inner();
            Self::info_from_msg(
                c.get_pond_info(GetPondInfoRequest { pond_ref: r.pond_id.clone() })
                    .await?
                    .into_inner()
                    .pond,
                r.pond_id,
            )
        })?;
        Ok(Pond { latiq: self, info })
    }
```
Add a small mapper (handles the `Option<PondInfoMsg>` + empty node):
```rust
    fn info_from_msg(
        msg: Option<latiq_proto::v1::PondInfoMsg>,
        fallback_id: String,
    ) -> Result<PondInfo> {
        let m = msg.ok_or_else(|| anyhow!("pond vanished after create"))?;
        Ok(PondInfo {
            pond_id: if m.pond_id.is_empty() { fallback_id } else { m.pond_id },
            name: m.name,
            node_id: String::new(), // PondInfoMsg carries node_endpoint, not node_id
            tier: m.tier,
            description: m.description,
        })
    }
```

- [ ] **Step 3: Add `get_pond`** (after `create_pond`):

```rust
    /// Fetch a pond's metadata and return a handle (one round-trip).
    pub fn get_pond(&self, pond: &str) -> Result<Pond<'_>> {
        let info = self.rt.block_on(async {
            let mut c = self.control().await?;
            let resp = c
                .get_pond_info(GetPondInfoRequest { pond_ref: pond.to_string() })
                .await
                .map_err(|s| anyhow!("pond '{pond}': {}", s.message()))?
                .into_inner();
            Self::info_from_msg(resp.pond, pond.to_string())
        })?;
        Ok(Pond { latiq: self, info })
    }
```

- [ ] **Step 4: `list_ponds` → `BTreeMap<String, PondInfo>`** (`:125-140`):

```rust
    /// List ponds keyed by name (admin metadata read; works if nodes are down).
    pub fn list_ponds(&self) -> Result<BTreeMap<String, PondInfo>> {
        self.rt.block_on(async {
            let mut a = self.admin().await?;
            let resp = a.pond_list(PondListRequest {}).await?.into_inner();
            Ok(resp
                .ponds
                .into_iter()
                .map(|p| {
                    (
                        p.name.clone(),
                        PondInfo {
                            pond_id: p.pond_id,
                            name: p.name,
                            node_id: p.node_id,
                            tier: p.tier,
                            description: p.description,
                        },
                    )
                })
                .collect())
        })
    }
```
(`drop_pond` and `describe_pond` keep their current signatures — both already on
`Latiq`, both now route via the front door from Task 1.)

- [ ] **Step 5: Update the embedded test to the handle API.** Rewrite
`crates/latiq-sdk/tests/embedded.rs` body to use handles:

```rust
    let work = db.create_pond(Some("work"), "medium", "round-trip test").unwrap();
    assert_eq!(work.name(), "work");
    assert!(!work.id().is_empty());
    assert_eq!(work.description(), "round-trip test");
    assert!(db.list_ponds().unwrap().contains_key("work"));

    work.query("CREATE TABLE t(id INTEGER, note VARCHAR)").unwrap();
    work.query("INSERT INTO t VALUES (1,'a'),(2,'b')").unwrap();
    let batches = work.query("SELECT count(*) AS n FROM t").unwrap();
    let col = batches[0].column(0).as_any()
        .downcast_ref::<arrow::array::Int64Array>().unwrap();
    assert_eq!(col.value(0), 2);

    assert_eq!(db.get_pond("work").unwrap().description(), "round-trip test");
    assert!(db.drop_pond("work", false).is_err(), "drop needs confirm");
    db.drop_pond("work", true).unwrap();
    assert!(db.query("work", "SELECT 1").is_err(), "gone after drop");
```

- [ ] **Step 6: Run it — expect PASS**

Run: `cargo test -p latiq-sdk --test embedded`
Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add crates/latiq-sdk/src/lib.rs crates/latiq-sdk/tests/embedded.rs
git commit -m "feat(sdk): handle-centric API — Pond handle, get_pond, list_ponds map, description"
```

---

### Task 4: PyO3 layer — handle + `pyarrow.Table` + named args

**Files:**
- Modify: `sdk/python/pyproject.toml` (declare runtime `pyarrow`)
- Modify: `sdk/python/src/lib.rs` (rewrite to handle-centric + Arrow)
- Modify: `sdk/python/tests/test_sdk.py`, `sdk/python/README.md`

- [ ] **Step 1: Declare the pyarrow runtime dep** — `sdk/python/pyproject.toml`,
add under `[project]` (find the `dependencies` array; create it if missing):

```toml
dependencies = ["pyarrow>=14"]
```

- [ ] **Step 2: Rewrite `sdk/python/src/lib.rs`.** Key changes: `query` returns a
`pyarrow.Table` (reads decode IPC bytes via Python's `pyarrow.ipc`; writes return
an empty Table); `create_pond`/`get_pond` return `PyPond` with metadata
attributes; `list_ponds` → `dict`; `drop_pond` on `PyDatabase`; named-arg
signatures. Add a Rust method on `Latiq` first to expose **IPC bytes** to Python
(so we don't need the `arrow`/`pyo3` C-data feature coupling):

  In `crates/latiq-sdk/src/lib.rs`, add a public method that returns the raw IPC
  stream bytes for reads (concatenating the chunks reconstitutes one IPC stream,
  since the server uses a single `StreamWriter`):

```rust
    /// Reads only: the full Arrow IPC stream bytes (schema + batches), for FFI
    /// consumers (Python `pyarrow.ipc.open_stream`). Uncapped.
    pub fn query_ipc(&self, pond: &str, sql: &str) -> Result<Vec<u8>> {
        self.rt.block_on(async {
            if !latiq_engine::is_read_only(sql) {
                let mut d = self.data().await?;
                d.write_query(self.with_id(QueryRequest {
                    pond: pond.to_string(),
                    sql: sql.to_string(),
                }))
                .await
                .map_err(|s| anyhow!("write: {}", s.message()))?;
                return Ok(Vec::new());
            }
            let mut sc = self.stream().await?;
            let mut streaming = sc
                .read_arrow(self.with_id(QueryRequest {
                    pond: pond.to_string(),
                    sql: sql.to_string(),
                }))
                .await
                .map_err(|s| anyhow!("read: {}", s.message()))?
                .into_inner();
            let mut out = Vec::new();
            while let Some(chunk) = streaming.message().await? {
                out.extend_from_slice(&chunk.ipc);
            }
            Ok(out)
        })
    }
```
  Commit this with Task 4 (it's the Python boundary). Then the PyO3 module:

```rust
//! PyO3 bindings exposing `latiq-sdk` as the Python module `latiq`. Thin wrappers;
//! all logic lives in `latiq-sdk`. Blocking gRPC releases the GIL via allow_threads.
use latiq_sdk::{Latiq, PondInfo};
use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyDict};
use std::path::PathBuf;
use std::sync::Arc;

fn err(e: impl std::fmt::Display) -> PyErr {
    PyRuntimeError::new_err(e.to_string())
}

/// Build a pyarrow.Table from Arrow IPC stream bytes (empty bytes → empty table).
fn ipc_to_table(py: Python<'_>, ipc: Vec<u8>) -> PyResult<PyObject> {
    let pa = py.import_bound("pyarrow")?;
    if ipc.is_empty() {
        // writes / empty reads: a 0-row, 0-col table is the honest result
        return Ok(pa.call_method0("table_from_pydict_empty").map(|t| t.into())
            .unwrap_or_else(|_| {
                pa.getattr("Table").unwrap()
                    .call_method1("from_arrays", (pyo3::types::PyList::empty_bound(py), pyo3::types::PyList::empty_bound(py)))
                    .unwrap().into()
            }));
    }
    let reader = pa
        .getattr("ipc")?
        .call_method1("open_stream", (PyBytes::new_bound(py, &ipc),))?;
    Ok(reader.call_method0("read_all")?.into())
}

fn info_to_pypond(inner: &Arc<Latiq>, info: PondInfo) -> PyPond {
    PyPond { inner: inner.clone(), info }
}

#[pyclass(name = "Database", module = "latiq")]
struct PyDatabase {
    inner: Arc<Latiq>,
}

#[pymethods]
impl PyDatabase {
    #[getter]
    fn server(&self) -> String {
        self.inner.server().to_string()
    }

    /// Allocate a pond → handle. `db.create_pond(name="work", tier="medium", description="…")`.
    #[pyo3(signature = (name=None, tier="medium", description=""))]
    fn create_pond(&self, py: Python<'_>, name: Option<&str>, tier: &str, description: &str) -> PyResult<PyPond> {
        let inner = self.inner.clone();
        let info = py
            .allow_threads(|| inner.create_pond(name, tier, description).map(|p| p.info))
            .map_err(err)?;
        Ok(info_to_pypond(&self.inner, info))
    }

    /// Existing pond → handle. `db.get_pond(pond="work")`.
    #[pyo3(signature = (pond))]
    fn get_pond(&self, py: Python<'_>, pond: &str) -> PyResult<PyPond> {
        let inner = self.inner.clone();
        let info = py
            .allow_threads(|| inner.get_pond(pond).map(|p| p.info))
            .map_err(err)?;
        Ok(info_to_pypond(&self.inner, info))
    }

    /// Ponds keyed by name: `{name: {pond_id, tier, node_id, description}}`.
    fn list_ponds<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyDict>> {
        let inner = self.inner.clone();
        let map = py.allow_threads(|| inner.list_ponds()).map_err(err)?;
        let d = PyDict::new_bound(py);
        for (name, info) in map {
            let v = PyDict::new_bound(py);
            v.set_item("pond_id", &info.pond_id)?;
            v.set_item("tier", &info.tier)?;
            v.set_item("node_id", &info.node_id)?;
            v.set_item("description", &info.description)?;
            d.set_item(name, v)?;
        }
        Ok(d)
    }

    /// Drop a pond (`confirm` must be true).
    #[pyo3(signature = (pond, confirm=true))]
    fn drop_pond(&self, py: Python<'_>, pond: &str, confirm: bool) -> PyResult<()> {
        let inner = self.inner.clone();
        py.allow_threads(|| inner.drop_pond(pond, confirm)).map_err(err)
    }

    fn __repr__(&self) -> String {
        format!("Database(server='{}')", self.inner.server())
    }
}

/// A handle to one pond: metadata attributes + `query`.
#[pyclass(name = "Pond", module = "latiq")]
struct PyPond {
    inner: Arc<Latiq>,
    info: PondInfo,
}

#[pymethods]
impl PyPond {
    #[getter] fn name(&self) -> String { self.info.name.clone() }
    #[getter] fn id(&self) -> String { self.info.pond_id.clone() }
    #[getter] fn tier(&self) -> String { self.info.tier.clone() }
    #[getter] fn node(&self) -> String { self.info.node_id.clone() }
    #[getter] fn description(&self) -> String { self.info.description.clone() }

    /// Run SQL. Reads → `pyarrow.Table` (streamed, uncapped); writes execute and
    /// return an empty table. `pond.query(sql="SELECT …")`.
    #[pyo3(signature = (sql))]
    fn query(&self, py: Python<'_>, sql: &str) -> PyResult<PyObject> {
        let (inner, pond) = (self.inner.clone(), self.info.name.clone());
        let ipc = py.allow_threads(|| inner.query_ipc(&pond, sql)).map_err(err)?;
        ipc_to_table(py, ipc)
    }

    fn describe(&self, py: Python<'_>) -> PyResult<PyObject> {
        let (inner, pond) = (self.inner.clone(), self.info.name.clone());
        let v = py.allow_threads(|| inner.describe_pond(&pond)).map_err(err)?;
        pythonize::pythonize(py, &v).map(|b| b.into()).map_err(err)
    }

    #[pyo3(signature = (confirm=true))]
    fn drop(&self, py: Python<'_>, confirm: bool) -> PyResult<()> {
        let (inner, pond) = (self.inner.clone(), self.info.name.clone());
        py.allow_threads(|| inner.drop_pond(&pond, confirm)).map_err(err)
    }

    fn __repr__(&self) -> String {
        format!("Pond(name='{}')", self.info.name)
    }
}

/// Connect. `latiq.connect(server="local", root=None, query_gateway=None)`.
#[pyfunction]
#[pyo3(signature = (server="local", root=None, query_gateway=None))]
fn connect(server: &str, root: Option<PathBuf>, query_gateway: Option<&str>) -> PyResult<PyDatabase> {
    let inner = Latiq::connect_with(server, root, query_gateway).map_err(err)?;
    Ok(PyDatabase { inner: Arc::new(inner) })
}

#[pymodule]
fn latiq(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(connect, m)?)?;
    m.add_class::<PyDatabase>()?;
    m.add_class::<PyPond>()?;
    Ok(())
}
```
> **Empty-table detail:** the `ipc_to_table` empty branch must produce a real
> `pyarrow.Table`. The fallback above is defensive; the clean form is
> `pa.getattr("table")?.call1((PyDict::new_bound(py),))?` (i.e. `pyarrow.table({})`).
> Use that one-liner and delete the `unwrap_or_else` scaffold — verify with the
> write test in Step 4 (it just needs `db.query(write)` to not raise).

- [ ] **Step 3: Build the wheel**

```bash
cd sdk/python
uv venv && uv pip install maturin pytest pyarrow
uv run maturin develop
```
Expected: builds `latiq` into `.venv` (first DuckDB build slow).

- [ ] **Step 4: Rewrite the pytest e2e** — `sdk/python/tests/test_sdk.py`:

```python
"""E2E of the Latiq Python SDK against a real in-process cluster."""
import tempfile
import pyarrow as pa
import latiq


def test_embedded_handle_lifecycle_and_arrow_query():
    with tempfile.TemporaryDirectory() as root:
        db = latiq.connect(server="local", root=root)
        assert db.server.startswith("http://127.0.0.1:")

        work = db.create_pond(name="work", tier="medium",
                              description="raw clickstream 2024")
        assert work.name == "work" and work.id
        assert work.description == "raw clickstream 2024"

        # list_ponds → dict keyed by name, carrying description
        ponds = db.list_ponds()
        assert "work" in ponds
        assert ponds["work"]["description"] == "raw clickstream 2024"

        # one query verb; reads → pyarrow.Table, writes execute
        work.query(sql="CREATE TABLE t(id INTEGER, note VARCHAR)")
        work.query(sql="INSERT INTO t VALUES (1,'a'),(2,'b'),(3,'c')")
        tbl = work.query(sql="SELECT count(*) AS n FROM t")
        assert isinstance(tbl, pa.Table)
        assert tbl.column("n")[0].as_py() == 3

        assert db.get_pond(pond="work").description == "raw clickstream 2024"

        # drop requires confirm; gone afterwards
        try:
            db.drop_pond(pond="work", confirm=False)
            assert False, "drop must require confirm"
        except RuntimeError:
            pass
        db.drop_pond(pond="work", confirm=True)
        try:
            work.query(sql="SELECT 1")
            assert False, "pond gone after drop"
        except RuntimeError:
            pass


def test_arrow_types_and_handle_repr():
    with tempfile.TemporaryDirectory() as root:
        db = latiq.connect(server="local", root=root)
        shop = db.create_pond(name="shop")
        tbl = shop.query(sql="SELECT 1 AS id, 'gear' AS name")
        assert tbl.schema.field("id").type == pa.int32()
        assert tbl.column("name")[0].as_py() == "gear"
        assert "shop" in repr(shop)
```

- [ ] **Step 5: Run pytest** (the `--no-sync` / direct-venv form — plain `uv run
pytest` reinstalls a cached wheel and clobbers `maturin develop`):

```bash
cd sdk/python
.venv/bin/python -m pytest -v
```
Expected: 2 passed.

- [ ] **Step 6: Update the README surface** — `sdk/python/README.md`: replace the
example + the stale `Surface` line (`read`/`write` → handle + `query`):

```markdown
db = latiq.connect(server="local")                 # default root ~/.latiq/local
# db = latiq.connect(server="grpc://lb:51400")     # remote: front door + greeter forwarding
# db = latiq.connect(server="grpc://cp:51400", query_gateway="grpc://data-lb:51500")

work = db.create_pond(name="work", tier="medium", description="raw events 2024")
work.query(sql="CREATE TABLE t(id INT)")
work.query(sql="INSERT INTO t VALUES (1),(2)")
tbl = work.query(sql="SELECT count(*) FROM t")     # → pyarrow.Table
db.list_ponds()                                    # → {"work": {pond_id, tier, node_id, description}}
print(work.name, work.tier, work.description)      # metadata attributes
db.drop_pond(pond="work", confirm=True)
```
And the surface line:
```markdown
`connect` · `Database.{server, create_pond, get_pond, list_ponds, drop_pond}` ·
`Pond.{name, id, tier, node, description, query, describe, drop}`. Reads return
`pyarrow.Table` over the streaming `ReadArrow` RPC; the data path uses the
front door + greeter forwarding (k8s-safe). Dataset/catalog/stats deferred.
```

- [ ] **Step 7: Commit**

```bash
git add crates/latiq-sdk/src/lib.rs sdk/python/pyproject.toml sdk/python/src/lib.rs sdk/python/tests/test_sdk.py sdk/python/README.md
git commit -m "feat(py-sdk): handle API, pyarrow.Table reads, list_ponds dict, query_gateway"
```

---

### Task 5: Full gate + PR

- [ ] **Step 1: Workspace gate** (the standalone `sdk/python` workspace is excluded
from `--workspace`, so build it separately):

```bash
cargo fmt --all
cargo clippy --workspace --all-targets -- -D warnings    # delete now-dead parse_json if flagged
cargo test --workspace
(cd sdk/python && cargo fmt && cargo clippy -- -D warnings)
```
Expected: all green.

- [ ] **Step 2: Re-run the Python e2e** to confirm the committed wheel still passes:

```bash
cd sdk/python && uv run maturin develop && .venv/bin/python -m pytest -v
```
Expected: 2 passed.

- [ ] **Step 3: Open the PR**

```bash
gh pr create --title "Slice B: SDK handle API + Arrow streaming reads + front-door routing" \
  --body "Handle-centric SDK (\`db.create_pond(...).query(...)\`), reads return a \`pyarrow.Table\` over the existing \`ReadArrow\` stream (uncapped), writes stay unary. Data path now goes through the front door + greeter forwarding instead of node-direct \`get_pond_location\` (fixes the k8s-LB routing bug). \`list_ponds()\` → dict keyed by name with \`description\`; \`get_pond\`; \`drop_pond\` on db; named-arg signatures; optional \`query_gateway\`.

Depends on Slice A (pond description). MCP unchanged (stays JSON + cap — see spec).

🤖 Generated with [Claude Code](https://claude.com/claude-code)"
```

---

## Self-review notes
- **Spec coverage:** front-door routing (✓ T1), Arrow streaming reads→Table (✓ T2/T4),
  handle API + `get_pond` + `list_ponds` dict + `drop_pond` on db + named args (✓ T3/T4),
  `description` attribute (✓ T3/T4), README de-stale (✓ T4). MCP untouched (✓ by omission).
- **Type consistency:** Rust `query` → `Vec<RecordBatch>`; `query_ipc` → `Vec<u8>`
  (Python boundary); `create_pond`/`get_pond` → `Pond<'_>`; `list_ponds` →
  `BTreeMap<String, PondInfo>`; `PondInfo` has `description`. Python `query` →
  `pyarrow.Table`; `list_ponds` → `dict`.
- **Routing invariant:** all data ops (`query`, `describe_pond`, `drop_pond`,
  `query_ipc`) dial `self.data_endpoint` (front door); the greeter forwards. No
  `get_pond_location` in the data path.
- **Risk flagged:** the `ipc_to_table` empty-write branch must yield a real
  `pyarrow.Table` — Step 2's note pins the clean `pyarrow.table({})` form; the
  write test in Step 4 guards it.
- **Known limitation (documented, out of scope):** writes return an empty Table
  (no RETURNING rows, no snapshot surfaced to the SDK) — fine for this slice.
