"""End-to-end SDK suite against a live (multi-node, gatewayed) Latiq cluster — the
data/SDK audience, driven exactly as a user's program would: allocate ponds, write
and read SQL through the front door, pull results as Arrow, and hand them to pandas
for analysis. In REMOTE mode this exercises the nginx gateway + greeter forwarding
+ multi-node placement; in EMBEDDED mode the same logic runs single-node.
"""
import uuid

import pandas as pd
import pyarrow as pa
import pytest


def _name(prefix: str) -> str:
    # Unique per run so reruns against a persistent cluster don't name-collide.
    return f"{prefix}-{uuid.uuid4().hex[:8]}"


def test_pond_lifecycle_and_handle_metadata(db):
    p = db.create_pond(name=_name("life"), tier="medium", description="lifecycle e2e")
    assert p.id and p.tier == "medium" and p.description == "lifecycle e2e"
    # list_ponds is a dict keyed by name and carries the description.
    listed = db.list_ponds()
    assert p.name in listed
    assert listed[p.name]["description"] == "lifecycle e2e"
    # get_pond re-fetches the same metadata as a handle.
    assert db.get_pond(pond=p.name).description == "lifecycle e2e"
    # describe() surfaces the structured pond/schema.
    assert db.get_pond(pond=p.name).describe()["pond"]["name"] == p.name
    # drop requires confirm.
    with pytest.raises(RuntimeError):
        db.drop_pond(pond=p.name, confirm=False)
    db.drop_pond(pond=p.name, confirm=True)
    with pytest.raises(RuntimeError):
        db.get_pond(pond=p.name).query(sql="SELECT 1")


def test_read_returns_arrow_table_with_faithful_types(db):
    p = db.create_pond(name=_name("types"))
    p.query(sql="CREATE TABLE t(id INTEGER, name VARCHAR, amt DOUBLE, ok BOOLEAN)")
    p.query(sql="INSERT INTO t VALUES (1,'a',1.5,true),(2,'b',2.5,false)")
    tbl = p.query(sql="SELECT id, name, amt, ok FROM t ORDER BY id")
    assert isinstance(tbl, pa.Table)
    assert tbl.num_rows == 2
    assert tbl.schema.field("id").type == pa.int32()
    assert tbl.schema.field("name").type == pa.string()
    assert tbl.schema.field("amt").type == pa.float64()
    assert tbl.column("name").to_pylist() == ["a", "b"]
    db.drop_pond(pond=p.name, confirm=True)


def test_arrow_to_pandas_analysis_matches_sql(db):
    """The headline interop proof: pull a query as Arrow → pandas → run a groupby
    analysis → assert it equals the same aggregate computed in SQL."""
    p = db.create_pond(name=_name("analytics"), description="arrow↔pandas proof")
    p.query(sql="CREATE TABLE sales(region VARCHAR, product VARCHAR, amount INTEGER)")
    p.query(
        sql="""INSERT INTO sales VALUES
        ('east','widget',10),('east','widget',20),('east','gadget',15),
        ('west','widget',30),('west','gadget',5),('west','gadget',25)"""
    )

    # Arrow → pandas, analyze in pandas.
    df = p.query(sql="SELECT region, amount FROM sales").to_pandas()
    assert isinstance(df, pd.DataFrame)
    pandas_by_region = df.groupby("region")["amount"].sum().sort_index()

    # Ground truth: the same aggregate computed by the engine in SQL.
    sql_df = p.query(
        sql="SELECT region, sum(amount) AS s FROM sales GROUP BY region ORDER BY region"
    ).to_pandas()

    assert pandas_by_region.index.tolist() == sql_df["region"].tolist()
    assert pandas_by_region.tolist() == sql_df["s"].tolist()
    # And the concrete numbers, so a silently-empty/garbled frame can't pass.
    assert pandas_by_region.to_dict() == {"east": 45, "west": 60}

    # A two-key pandas pivot the engine didn't pre-compute — proves the full typed
    # frame (not just one column) survived Arrow into pandas.
    full = p.query(sql="SELECT region, product, amount FROM sales").to_pandas()
    pivot = full.pivot_table(
        index="region", columns="product", values="amount", aggfunc="sum"
    )
    assert int(pivot.loc["east", "widget"]) == 30
    assert int(pivot.loc["west", "gadget"]) == 30
    db.drop_pond(pond=p.name, confirm=True)


def test_large_read_streams_uncapped_past_the_json_cap(db):
    """Reads ride ReadArrow (streamed, uncapped) — a result well past the 10k JSON
    inline cap comes back whole, through the gateway + greeter forwarding."""
    p = db.create_pond(name=_name("big"))
    n = 25_000  # > the 10k inline cap the JSON/MCP edge enforces
    p.query(sql=f"CREATE TABLE t AS SELECT range AS i FROM range({n})")
    tbl = p.query(sql="SELECT i FROM t ORDER BY i")
    assert tbl.num_rows == n, "all rows streamed back uncapped"
    assert tbl.column("i")[0].as_py() == 0
    assert tbl.column("i")[n - 1].as_py() == n - 1
    # Sum via pandas as an independent check that values (not just count) are intact.
    assert int(tbl.to_pandas()["i"].sum()) == n * (n - 1) // 2
    db.drop_pond(pond=p.name, confirm=True)


def test_explain_returns_a_plan(db):
    p = db.create_pond(name=_name("explain"))
    p.query(sql="CREATE TABLE t(id INT)")
    plan = p.explain(sql="SELECT * FROM t WHERE id > 1")
    assert plan, "explain returned a plan"
    assert len(str(plan)) > 0
    db.drop_pond(pond=p.name, confirm=True)


def test_writes_are_visible_via_snapshots(db):
    """A write commits a DuckLake snapshot; `snapshots()` surfaces the history
    (who wrote what) — the SDK's window onto write attribution."""
    p = db.create_pond(name=_name("snaps"))
    p.query(sql="CREATE TABLE t(id INT)")
    p.query(sql="INSERT INTO t VALUES (1),(2)")
    snaps = p.snapshots()
    assert isinstance(snaps, pa.Table)
    assert snaps.num_rows >= 1, "the write(s) produced snapshot(s)"
    assert "snapshot_id" in snaps.schema.names
    db.drop_pond(pond=p.name, confirm=True)


def test_streaming_read_yields_batches(db):
    """`query(stream=True)` returns a RecordBatchReader to iterate batches rather
    than materializing the whole table."""
    p = db.create_pond(name=_name("stream"))
    n = 30_000
    p.query(sql=f"CREATE TABLE t AS SELECT range AS i FROM range({n})")
    reader = p.query(sql="SELECT i FROM t", stream=True)
    assert isinstance(reader, pa.RecordBatchReader)
    rows = sum(batch.num_rows for batch in reader)
    assert rows == n, "all rows arrived across the streamed batches"
    db.drop_pond(pond=p.name, confirm=True)


def test_datasets_list_load_and_query(db):
    """List the curated dataset catalog, load one into a pond, and query it.
    `load_dataset` pulls from the dataset's source URL (a real network flow)."""
    datasets = db.list_datasets()
    assert "tpch" in datasets, "curated catalog lists tpch"
    assert datasets["tpch"]["tables"], "tpch advertises its tables"

    p = db.create_pond(name=_name("ds"))
    p.load_dataset(dataset="tpch")
    # Datasets load into their own schema (schema-per-dataset).
    n = p.query(sql="SELECT count(*) AS n FROM tpch.nation")
    assert n.column("n")[0].as_py() == 25
    db.drop_pond(pond=p.name, confirm=True)


def test_catalogs_surface_reachable(db):
    """A fresh cluster has no external catalogs registered (that's an operator
    action via the CLI), so list_catalogs is empty and describing an unknown one
    errors — full pull/describe is covered by the iceberg e2e."""
    cats = db.list_catalogs()
    assert isinstance(cats, dict)
    p = db.create_pond(name=_name("cat"))
    with pytest.raises(RuntimeError):
        p.describe_catalog(catalog="does-not-exist")
    db.drop_pond(pond=p.name, confirm=True)


def test_error_contract_surfaces_failures(db):
    p = db.create_pond(name=_name("errs"))
    # A read against a missing table must raise, not return empty.
    with pytest.raises(RuntimeError):
        p.query(sql="SELECT * FROM does_not_exist")
    db.drop_pond(pond=p.name, confirm=True)


def test_multi_node_placement_and_forwarding(db, is_remote):
    """Allocate enough ponds that random placement spreads them across nodes, then
    write+read EACH through the gateway — proving the greeter forwards to whichever
    node owns the pond (incl. nodes not in the gateway's own upstream pool)."""
    if not is_remote:
        pytest.skip("single-node embedded cluster — no cross-node forwarding to prove")

    names = [_name("shard") for _ in range(12)]
    for nm in names:
        db.create_pond(name=nm)

    listed = db.list_ponds()
    nodes = {listed[nm]["node_id"] for nm in names if nm in listed}
    assert len(nodes) >= 2, f"ponds should spread across nodes, got {nodes}"

    # Every pond is reachable + correct through the single gateway address.
    for nm in names:
        h = db.get_pond(pond=nm)
        h.query(sql="CREATE TABLE t AS SELECT 7 AS v")
        got = h.query(sql="SELECT v FROM t")
        assert got.column("v")[0].as_py() == 7, f"forwarded query failed for {nm}"

    for nm in names:
        db.drop_pond(pond=nm, confirm=True)
