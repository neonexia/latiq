"""End-to-end tests of the Latiq Python SDK against a real in-process cluster
(`connect(server="local")` spawns control-plane + pond-node). Everything is driven
through the Python API — the same surface a user gets."""
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

        # get_pond re-fetches metadata as a handle
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
