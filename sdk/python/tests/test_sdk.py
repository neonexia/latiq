# Copyright 2026 Neonexia
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

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

        # a multi-row read returns every typed value across the streamed batches
        rows = work.query(sql="SELECT id, note FROM t ORDER BY id")
        assert rows.num_rows == 3
        assert rows.column("id").to_pylist() == [1, 2, 3]
        assert rows.column("note").to_pylist() == ["a", "b", "c"]

        # describe() returns the structured pond/schema
        assert work.describe()["pond"]["name"] == "work"

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


def test_lineage_is_an_opt_in_at_creation_and_readable_afterwards():
    """The allocation flag, on the surface most users touch. `lineage` is fixed
    for the pond's lifetime, so a caller can only get it right here — and can
    only tell whether `get_lineage` will have anything to say by reading it
    back. Reading it back is asserted on BOTH values: a property hard-coded to
    True would pass a one-pond test."""
    with tempfile.TemporaryDirectory() as root:
        db = latiq.connect(server="local", root=root)

        traced = db.create_pond(name="traced", lineage=True)
        quiet = db.create_pond(name="quiet")  # off by default, as everywhere else

        assert traced.lineage is True
        assert quiet.lineage is False, "lineage defaults to off"

        ponds = db.list_ponds()
        assert ponds["traced"]["lineage"] is True
        assert ponds["quiet"]["lineage"] is False

        # A handle fetched fresh from the server carries it too, so a caller who
        # did not allocate the pond can still tell.
        assert db.get_pond(pond="traced").lineage is True
        assert db.get_pond(pond="quiet").lineage is False


def test_arrow_types_and_handle_repr():
    with tempfile.TemporaryDirectory() as root:
        db = latiq.connect(server="local", root=root)
        shop = db.create_pond(name="shop")
        tbl = shop.query(sql="SELECT 1 AS id, 'gear' AS name")
        assert tbl.schema.field("id").type == pa.int32()
        assert tbl.column("name")[0].as_py() == "gear"
        assert "shop" in repr(shop)
