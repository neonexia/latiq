# latiq — Python SDK

PyO3 bindings over `latiq-sdk`. **Two ways to get a cluster, one wire shape**
(calls always ride gRPC; the SDK is a CLI/SDK client, never MCP). The pond is the
object, SQL is the verb, reads come back as `pyarrow.Table`:

```python
import latiq

# Embedded: starts a control-plane + pond-node in-process, backed by a local dir.
db = latiq.connect(server="local")                 # default root: ~/.latiq/local
db = latiq.connect(server="local", root="/tmp/x")  # or a specific path

# Remote: hit the front door (LB/nginx); the greeter forwards by pond.
db = latiq.connect(server="grpc://lb:51400")
# Control/Admin and Data/Stream on separate addresses? override the data gateway:
db = latiq.connect(server="grpc://cp:51400", query_gateway="grpc://data-lb:51500")

work = db.create_pond(name="work", tier="medium", description="raw events 2024")
work.query(sql="CREATE TABLE t(id INT)")
work.query(sql="INSERT INTO t VALUES (1),(2)")
tbl = work.query(sql="SELECT count(*) FROM t")     # → pyarrow.Table (reads stream, uncapped)

db.list_ponds()                                    # → {"work": {pond_id, tier, node_id, description}}
print(work.name, work.tier, work.description)      # metadata as attributes
work.describe()                                    # structured table/column schema

existing = db.get_pond(pond="work")                # re-fetch a handle by name
db.drop_pond(pond="work", confirm=True)
```

## Build & test

Uses [maturin](https://www.maturin.rs/). With [uv](https://docs.astral.sh/uv/):

```bash
cd sdk/python
uv venv && uv pip install maturin pytest pyarrow
uv run maturin develop            # builds the extension into the venv (first build compiles DuckDB — slow)
.venv/bin/python -m pytest -v     # NOT `uv run pytest` — that reinstalls a cached wheel and clobbers maturin develop
```

The extension links the whole server stack (so embedded mode can spawn a cluster
in-process), so the wheel is large; that's the cost of zero-dependency local mode.

## Surface (this slice)

`connect(server, root, query_gateway)` · `Database.{server, create_pond, get_pond,
list_ponds, drop_pond}` · `Pond.{name, id, tier, node, description, query, describe,
drop}`. Reads return `pyarrow.Table` over the streaming `ReadArrow` RPC; the data
path uses the front door + greeter forwarding (k8s-safe). Dataset/catalog/stats
are deferred.
