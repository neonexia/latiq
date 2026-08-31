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
