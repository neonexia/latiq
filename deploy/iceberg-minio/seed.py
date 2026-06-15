"""Seed a `demo.widgets` Iceberg table into the local REST catalog (run inside
the compose network by the `seed` service — see docker-compose.yml)."""
import pyarrow as pa
from pyiceberg.catalog import load_catalog

cat = load_catalog(
    "rest",
    **{
        "uri": "http://iceberg-rest:8181",
        "warehouse": "s3://warehouse/",
        "s3.endpoint": "http://minio:9000",
        "s3.access-key-id": "admin",
        "s3.secret-access-key": "password",
        "s3.path-style-access": "true",
    },
)

cat.create_namespace_if_not_exists("demo")
schema = pa.schema(
    [("id", pa.int64()), ("name", pa.string()), ("price", pa.float64())]
)
table = cat.create_table_if_not_exists("demo.widgets", schema=schema)
table.append(
    pa.table(
        {"id": [1, 2, 3], "name": ["gear", "bolt", "pulley"], "price": [9.99, 0.99, 12.40]},
        schema=schema,
    )
)
print("seeded demo.widgets:", table.scan().to_arrow().num_rows, "rows")
