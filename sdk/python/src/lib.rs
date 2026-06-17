//! PyO3 bindings exposing `latiq-sdk` as the Python module `latiq`. Thin wrappers
//! only — all logic lives in `latiq-sdk` / the Rust core (no business logic in
//! Python). The blocking gRPC calls release the GIL via `allow_threads`.
//!
//! ```python
//! import latiq
//! db = latiq.connect("local")                 # in-process cluster (~/.latiq/local)
//! # db = latiq.connect("grpc://host:51400")   # or a remote control plane
//! db.create_pond("work")
//! db.query("work", "CREATE TABLE t(id INT)")
//! db.query("work", "INSERT INTO t VALUES (1),(2)")
//! print(db.query("work", "SELECT count(*) FROM t"))   # {"columns": [...], "rows": [[2]]}
//! ```
use latiq_sdk::Latiq;
use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use pyo3::types::PyDict;
use std::path::PathBuf;
use std::sync::Arc;

fn err(e: impl std::fmt::Display) -> PyErr {
    PyRuntimeError::new_err(e.to_string())
}

fn json_to_py(py: Python<'_>, v: &serde_json::Value) -> PyResult<PyObject> {
    pythonize::pythonize(py, v)
        .map(|b| b.into())
        .map_err(err)
}

fn pond_to_dict<'py>(py: Python<'py>, p: &latiq_sdk::Pond) -> PyResult<Bound<'py, PyDict>> {
    let d = PyDict::new_bound(py);
    d.set_item("pond_id", &p.pond_id)?;
    d.set_item("name", &p.name)?;
    d.set_item("node_id", &p.node_id)?;
    d.set_item("tier", &p.tier)?;
    Ok(d)
}

/// Connection handle. `latiq.connect("local"|"grpc://…")`.
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

    /// Allocate a pond. Returns `{pond_id, name, node_id, tier}`.
    #[pyo3(signature = (name=None, tier="medium"))]
    fn create_pond<'py>(
        &self,
        py: Python<'py>,
        name: Option<&str>,
        tier: &str,
    ) -> PyResult<Bound<'py, PyDict>> {
        let inner = self.inner.clone();
        let p = py
            .allow_threads(|| inner.create_pond(name, tier))
            .map_err(err)?;
        pond_to_dict(py, &p)
    }

    /// List ponds (control-plane metadata read).
    fn list_ponds<'py>(&self, py: Python<'py>) -> PyResult<Vec<Bound<'py, PyDict>>> {
        let inner = self.inner.clone();
        let ponds = py.allow_threads(|| inner.list_ponds()).map_err(err)?;
        ponds.iter().map(|p| pond_to_dict(py, p)).collect()
    }

    /// Describe a pond's schema.
    fn describe_pond(&self, py: Python<'_>, pond: &str) -> PyResult<PyObject> {
        let inner = self.inner.clone();
        let v = py
            .allow_threads(|| inner.describe_pond(pond))
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

    /// Run SQL against `pond`. One verb — reads return rows (and are rejected if
    /// they mutate); writes (INSERT/UPDATE/DELETE/DDL) are attributed to this
    /// client. Returns `{columns, rows, …}`.
    fn query(&self, py: Python<'_>, pond: &str, sql: &str) -> PyResult<PyObject> {
        let inner = self.inner.clone();
        let v = py.allow_threads(|| inner.query(pond, sql)).map_err(err)?;
        json_to_py(py, &v)
    }

    /// A lazy handle to one pond — `db.pond("work").query("SELECT …")`.
    fn pond(&self, name: &str) -> PyPond {
        PyPond {
            inner: self.inner.clone(),
            name: name.to_string(),
        }
    }

    fn __repr__(&self) -> String {
        format!("Database(server='{}')", self.inner.server())
    }
}

/// Lazy per-pond handle (no round-trip to create; ops resolve the owning node).
#[pyclass(name = "Pond", module = "latiq")]
struct PyPond {
    inner: Arc<Latiq>,
    name: String,
}

#[pymethods]
impl PyPond {
    #[getter]
    fn name(&self) -> String {
        self.name.clone()
    }

    fn query(&self, py: Python<'_>, sql: &str) -> PyResult<PyObject> {
        let (inner, pond) = (self.inner.clone(), self.name.clone());
        let v = py.allow_threads(|| inner.query(&pond, sql)).map_err(err)?;
        json_to_py(py, &v)
    }

    fn describe(&self, py: Python<'_>) -> PyResult<PyObject> {
        let (inner, pond) = (self.inner.clone(), self.name.clone());
        let v = py
            .allow_threads(|| inner.describe_pond(&pond))
            .map_err(err)?;
        json_to_py(py, &v)
    }

    #[pyo3(signature = (confirm=true))]
    fn drop(&self, py: Python<'_>, confirm: bool) -> PyResult<()> {
        let (inner, pond) = (self.inner.clone(), self.name.clone());
        py.allow_threads(|| inner.drop_pond(&pond, confirm))
            .map_err(err)
    }

    fn __repr__(&self) -> String {
        format!("Pond(name='{}')", self.name)
    }
}

/// Connect to Latiq. `server="local"` starts an in-process cluster backed by
/// `root` (default `~/.latiq/local`); any other value is a remote control-plane
/// endpoint (e.g. `grpc://host:51400`).
#[pyfunction]
#[pyo3(signature = (server="local", root=None))]
fn connect(server: &str, root: Option<PathBuf>) -> PyResult<PyDatabase> {
    let inner = Latiq::connect(server, root).map_err(err)?;
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
