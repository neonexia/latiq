"""End-to-end tests of the Latiq Python SDK against a real in-process cluster
(`connect("local")` spawns control-plane + pond-node). Everything is driven
through the Python API — the same surface a user gets."""
import tempfile

import latiq


def test_embedded_pond_lifecycle_and_query():
    with tempfile.TemporaryDirectory() as root:
        db = latiq.connect("local", root=root)
        assert db.server.startswith("http://127.0.0.1:")

        # Create → shows up in the control-plane list.
        p = db.create_pond("work", tier="medium")
        assert p["name"] == "work"
        assert p["pond_id"]
        assert any(x["name"] == "work" for x in db.list_ponds())

        # Write then read (node-direct; materializes the pond on first use).
        db.write("work", "CREATE TABLE t(id INTEGER, note VARCHAR)")
        db.write("work", "INSERT INTO t VALUES (1,'a'),(2,'b'),(3,'c')")
        r = db.read("work", "SELECT count(*) AS n FROM t")
        assert r["rows"][0][0] == 3

        # Describe surfaces the pond + schema.
        d = db.describe_pond("work")
        assert d["pond"]["name"] == "work"

        # The read guard rejects a write.
        try:
            db.read("work", "INSERT INTO t VALUES (4,'d')")
            assert False, "read_query must reject a write"
        except RuntimeError:
            pass

        # Drop requires confirm; afterwards the pond is gone.
        try:
            db.drop_pond("work", confirm=False)
            assert False, "drop must require confirm"
        except RuntimeError:
            pass
        db.drop_pond("work", confirm=True)
        try:
            db.read("work", "SELECT 1")
            assert False, "pond must be gone after drop"
        except RuntimeError:
            pass


def test_pond_handle_ergonomics():
    with tempfile.TemporaryDirectory() as root:
        db = latiq.connect("local", root=root)
        db.create_pond("shop")
        pond = db.pond("shop")                       # lazy handle, no round-trip
        assert pond.name == "shop"
        pond.write("CREATE TABLE items AS SELECT 1 AS id, 'gear' AS name")
        rows = pond.read("SELECT name FROM items")["rows"]
        assert rows[0][0] == "gear"
        assert pond.describe()["pond"]["name"] == "shop"
        pond.drop()
