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

//! PyO3 bindings exposing `latiq-sdk` as the Python module `latiq`. Thin wrappers
//! only — all logic lives in `latiq-sdk` (no business logic in Python). The
//! blocking gRPC calls release the GIL via `allow_threads`.
//!
//! ```python
//! import latiq
//! db = latiq.connect(server="local")                 # in-process cluster (~/.latiq/local)
//! # db = latiq.connect(server="grpc://lb:51400")     # remote: front door + greeter forwarding
//! work = db.create_pond(name="work", tier="medium", description="raw events")
//! work.query(sql="CREATE TABLE t(id INT)")
//! work.query(sql="INSERT INTO t VALUES (1),(2)")
//! print(work.query(sql="SELECT count(*) FROM t"))    # → pyarrow.Table
//! ```
// pyo3 0.22's #[pymethods]/#[pyfunction] macros emit a PyErr→PyErr `.into()` in
// the generated wrappers, which newer clippy flags as useless_conversion. It's
// macro-generated, not our code; allow it crate-wide.
#![allow(clippy::useless_conversion)]
use latiq_sdk::{Latiq, PondInfo};
use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyDict};
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

fn err(e: impl std::fmt::Display) -> PyErr {
    PyRuntimeError::new_err(e.to_string())
}

/// Convert SDK JSON (datasets/catalogs/explain/describe results) to a Python object.
fn json_to_py(py: Python<'_>, v: &serde_json::Value) -> PyResult<PyObject> {
    pythonize::pythonize(py, v).map(|b| b.into()).map_err(err)
}

/// Build a streaming `pyarrow.RecordBatchReader` from Arrow IPC bytes (yields
/// batches lazily; empty bytes → an empty reader).
fn ipc_to_reader(py: Python<'_>, ipc: Vec<u8>) -> PyResult<PyObject> {
    let pa = py.import_bound("pyarrow")?;
    if ipc.is_empty() {
        let schema = pa.call_method1("schema", (pyo3::types::PyList::empty_bound(py),))?;
        let empty = pyo3::types::PyList::empty_bound(py);
        return Ok(pa
            .getattr("RecordBatchReader")?
            .call_method1("from_batches", (schema, empty))?
            .into());
    }
    Ok(pa
        .getattr("ipc")?
        .call_method1("open_stream", (PyBytes::new_bound(py, &ipc),))?
        .into())
}

/// Build a `pyarrow.Table` from Arrow IPC stream bytes. Empty bytes (writes /
/// empty reads) → an empty table (`pyarrow.table({})`).
fn ipc_to_table(py: Python<'_>, ipc: Vec<u8>) -> PyResult<PyObject> {
    let pa = py.import_bound("pyarrow")?;
    if ipc.is_empty() {
        let empty = PyDict::new_bound(py);
        return Ok(pa.call_method1("table", (empty,))?.into());
    }
    let reader = pa
        .getattr("ipc")?
        .call_method1("open_stream", (PyBytes::new_bound(py, &ipc),))?;
    Ok(reader.call_method0("read_all")?.into())
}

fn info_to_pypond(inner: &Arc<Latiq>, info: PondInfo) -> PyPond {
    PyPond {
        inner: inner.clone(),
        info,
    }
}

/// Connection handle. `latiq.connect(server="local"|"grpc://…")`.
#[pyclass(name = "Database", module = "latiq")]
struct PyDatabase {
    inner: Arc<Latiq>,
}

#[pymethods]
impl PyDatabase {
    /// The control-plane endpoint this client is bound to.
    #[getter]
    fn server(&self) -> String {
        self.inner.server().to_string()
    }

    /// Allocate a pond → handle. `db.create_pond(name="work", tier="medium", description="…")`.
    ///
    /// `lineage=True` opts the pond into OpenLineage provenance for every query.
    /// Off by default, as on every other surface, and **fixed for the pond's
    /// lifetime** — turning it on later would leave a hole at the start of the
    /// record, so there is no call to change it.
    #[pyo3(signature = (name=None, tier="medium", description="", lineage=false))]
    fn create_pond(
        &self,
        py: Python<'_>,
        name: Option<&str>,
        tier: &str,
        description: &str,
        lineage: bool,
    ) -> PyResult<PyPond> {
        let inner = self.inner.clone();
        let info = py
            .allow_threads(|| {
                inner
                    .create_pond(name, tier, description, lineage)
                    .map(|p| p.info)
            })
            .map_err(err)?;
        Ok(info_to_pypond(&self.inner, info))
    }

    /// Existing pond → handle (metadata pre-fetched). `db.get_pond(pond="work")`.
    #[pyo3(signature = (pond))]
    fn get_pond(&self, py: Python<'_>, pond: &str) -> PyResult<PyPond> {
        let inner = self.inner.clone();
        let info = py
            .allow_threads(|| inner.get_pond(pond).map(|p| p.info))
            .map_err(err)?;
        Ok(info_to_pypond(&self.inner, info))
    }

    /// Ponds keyed by name: `{name: {pond_id, tier, node_id, description, lineage}}`.
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
            v.set_item("lineage", info.lineage)?;
            d.set_item(name, v)?;
        }
        Ok(d)
    }

    /// Curated datasets keyed by name. `query`: `""`/None = all, `"#tag"`,
    /// `"prefix*"`, or a substring.
    #[pyo3(signature = (query=None))]
    fn list_datasets(&self, py: Python<'_>, query: Option<&str>) -> PyResult<PyObject> {
        let inner = self.inner.clone();
        let v = py
            .allow_threads(|| inner.list_datasets(query.unwrap_or_default()))
            .map_err(err)?;
        json_to_py(py, &v)
    }

    /// External catalogs keyed by name (same `query` filter as `list_datasets`).
    #[pyo3(signature = (query=None))]
    fn list_catalogs(&self, py: Python<'_>, query: Option<&str>) -> PyResult<PyObject> {
        let inner = self.inner.clone();
        let v = py
            .allow_threads(|| inner.list_catalogs(query.unwrap_or_default()))
            .map_err(err)?;
        json_to_py(py, &v)
    }

    /// Drop a pond and all its data (`confirm` must be true).
    #[pyo3(signature = (pond, confirm=true))]
    fn drop_pond(&self, py: Python<'_>, pond: &str, confirm: bool) -> PyResult<()> {
        let inner = self.inner.clone();
        py.allow_threads(|| inner.drop_pond(pond, confirm))
            .map_err(err)
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
    #[getter]
    fn name(&self) -> String {
        self.info.name.clone()
    }
    #[getter]
    fn id(&self) -> String {
        self.info.pond_id.clone()
    }
    #[getter]
    fn tier(&self) -> String {
        self.info.tier.clone()
    }
    #[getter]
    fn description(&self) -> String {
        self.info.description.clone()
    }
    /// Whether this pond records OpenLineage events (into its own `lineage/`
    /// directory on its node). Fixed at creation; agents read the events with
    /// the `get_lineage` MCP tool.
    #[getter]
    fn lineage(&self) -> bool {
        self.info.lineage
    }

    /// Run SQL. Reads → `pyarrow.Table` (streamed, uncapped); writes execute and
    /// return an empty table. `pond.query(sql="SELECT …")`.
    /// Run SQL. Reads → `pyarrow.Table` (streamed, uncapped); writes execute and
    /// return an empty table. `stream=True` returns a `pyarrow.RecordBatchReader`
    /// to iterate batches instead of materializing the whole table.
    #[pyo3(signature = (sql, stream=false))]
    fn query(&self, py: Python<'_>, sql: &str, stream: bool) -> PyResult<PyObject> {
        let (inner, pond) = (self.inner.clone(), self.info.name.clone());
        let ipc = py
            .allow_threads(|| inner.query_ipc(&pond, sql))
            .map_err(err)?;
        if stream {
            ipc_to_reader(py, ipc)
        } else {
            ipc_to_table(py, ipc)
        }
    }

    /// Explain a query plan (no execution).
    #[pyo3(signature = (sql))]
    fn explain(&self, py: Python<'_>, sql: &str) -> PyResult<PyObject> {
        let (inner, pond) = (self.inner.clone(), self.info.name.clone());
        let v = py
            .allow_threads(|| inner.explain(&pond, sql))
            .map_err(err)?;
        json_to_py(py, &v)
    }

    /// This pond's DuckLake snapshot history (who wrote what) as a `pyarrow.Table`.
    fn snapshots(&self, py: Python<'_>) -> PyResult<PyObject> {
        let (inner, pond) = (self.inner.clone(), self.info.name.clone());
        let ipc = py
            .allow_threads(|| inner.snapshots_ipc(&pond))
            .map_err(err)?;
        ipc_to_table(py, ipc)
    }

    /// Load a curated dataset (by name, from `db.list_datasets()`) into this pond.
    #[pyo3(signature = (dataset))]
    fn load_dataset(&self, py: Python<'_>, dataset: &str) -> PyResult<PyObject> {
        let (inner, pond) = (self.inner.clone(), self.info.name.clone());
        let v = py
            .allow_threads(|| inner.load_dataset(&pond, dataset))
            .map_err(err)?;
        json_to_py(py, &v)
    }

    /// Describe an external catalog's tables (attached transiently on this pond).
    /// `set`: runtime config + credentials (e.g. `{"token": "…"}`); never stored.
    #[pyo3(signature = (catalog, set=None))]
    fn describe_catalog(
        &self,
        py: Python<'_>,
        catalog: &str,
        set: Option<HashMap<String, String>>,
    ) -> PyResult<PyObject> {
        let (inner, pond) = (self.inner.clone(), self.info.name.clone());
        let v = py
            .allow_threads(|| inner.describe_catalog(&pond, catalog, set.unwrap_or_default()))
            .map_err(err)?;
        json_to_py(py, &v)
    }

    /// Pull a subset of an external catalog into a pond table. `query` is the
    /// materialization SQL. `set`: runtime config + credentials; never stored.
    #[pyo3(signature = (catalog, query, set=None))]
    fn pull_catalog(
        &self,
        py: Python<'_>,
        catalog: &str,
        query: &str,
        set: Option<HashMap<String, String>>,
    ) -> PyResult<PyObject> {
        let (inner, pond) = (self.inner.clone(), self.info.name.clone());
        let v = py
            .allow_threads(|| inner.pull_catalog(&pond, catalog, query, set.unwrap_or_default()))
            .map_err(err)?;
        json_to_py(py, &v)
    }

    fn describe(&self, py: Python<'_>) -> PyResult<PyObject> {
        let (inner, pond) = (self.inner.clone(), self.info.name.clone());
        let v = py
            .allow_threads(|| inner.describe_pond(&pond))
            .map_err(err)?;
        pythonize::pythonize(py, &v).map(|b| b.into()).map_err(err)
    }

    #[pyo3(signature = (confirm=true))]
    fn drop(&self, py: Python<'_>, confirm: bool) -> PyResult<()> {
        let (inner, pond) = (self.inner.clone(), self.info.name.clone());
        py.allow_threads(|| inner.drop_pond(&pond, confirm))
            .map_err(err)
    }

    fn __repr__(&self) -> String {
        format!("Pond(name='{}')", self.info.name)
    }
}

/// Connect to Latiq. `server="local"` starts an in-process cluster backed by
/// `root` (default `~/.latiq/local`); any other value is a remote control-plane
/// endpoint. `query_gateway` overrides the Data/Stream front door when it differs
/// from `server` (else `server` is used; the greeter forwards by pond). `token`
/// is the OAuth bearer token presented on every request, defaulting to
/// `$LATIQ_TOKEN`; it is only needed where the deployment configures an issuer.
#[pyfunction]
#[pyo3(signature = (server="local", root=None, query_gateway=None, token=None))]
fn connect(
    server: &str,
    root: Option<PathBuf>,
    query_gateway: Option<&str>,
    token: Option<&str>,
) -> PyResult<PyDatabase> {
    let inner = Latiq::connect_with_token(server, root, query_gateway, token).map_err(err)?;
    Ok(PyDatabase {
        inner: Arc::new(inner),
    })
}

#[pymodule]
fn latiq(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(connect, m)?)?;
    m.add_class::<PyDatabase>()?;
    m.add_class::<PyPond>()?;
    Ok(())
}
