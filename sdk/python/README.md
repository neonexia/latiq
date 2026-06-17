# latiq — Python SDK

PyO3 bindings over `latiq-sdk`. **Two ways to get a cluster, one wire shape**
(calls always ride gRPC; the SDK is a CLI/SDK client, never MCP):

```python
import latiq

# Embedded: starts a control-plane + pond-node in-process, backed by a local dir.
db = latiq.connect("local")                 # default root: ~/.latiq/local
db = latiq.connect("local", root="/tmp/x")  # or a specific path

# Remote: a running control plane (LATIQ_SERVER semantics).
db = latiq.connect("grpc://host:51400")

db.create_pond("work", tier="medium")
db.query("work", "CREATE TABLE t(id INT)")
db.query("work", "INSERT INTO t VALUES (1),(2)")
db.query("work", "SELECT count(*) FROM t")   # → {"columns": [...], "rows": [[2]], ...}  (one verb; reads vs writes routed for you)
db.describe_pond("work")

# Lazy per-pond handle.
pond = db.pond("work")
pond.query("INSERT INTO t VALUES (3)")
pond.query("SELECT * FROM t")
pond.drop()                                  # confirm=True by default on .drop()
```

## Build & test

Uses [maturin](https://www.maturin.rs/). With [uv](https://docs.astral.sh/uv/):

```bash
cd sdk/python
uv venv && uv pip install maturin pytest
uv run maturin develop            # builds the extension into the venv (first build compiles DuckDB — slow)
uv run --no-sync pytest -v        # --no-sync: keep maturin's fresh build (plain `uv run` would reinstall a cached wheel)
```

The extension links the whole server stack (so embedded mode can spawn a cluster
in-process), so the wheel is large; that's the cost of zero-dependency local mode.

## Surface (this slice)

`connect` · `Database.{server, create_pond, list_ponds, describe_pond, drop_pond,
read, write, pond}` · `Pond.{name, read, write, describe, drop}`. Dataset/catalog/
stats are deferred.
