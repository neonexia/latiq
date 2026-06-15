# Local Iceberg + MinIO harness

Stands up a real **Iceberg REST catalog** backed by **MinIO** (S3) so Latiq's
iceberg attacher can be exercised end-to-end. Requires Docker.

```bash
chmod +x up.sh
./up.sh                     # start MinIO + Iceberg REST, seed demo.widgets (3 rows)
# … run the e2e (below) …
./up.sh down               # tear down + wipe volumes
```

Endpoints once up:

| | URL | creds |
|---|---|---|
| Iceberg REST | `http://localhost:8181` | bearer (the fixture accepts any) |
| MinIO S3 | `http://localhost:9000` (console `:9001`) | `admin` / `password` |

## Run the e2e

The iceberg e2e (`crates/latiq/tests/catalogs_iceberg.rs`) is `#[ignore]`d so it
never runs in the normal suite. With the harness up:

```bash
LATIQ_ICEBERG_ENDPOINT=http://localhost:8181/v1/catalog \
LATIQ_ICEBERG_WAREHOUSE=demo LATIQ_ICEBERG_TOKEN=dummy \
LATIQ_S3_ENDPOINT=http://localhost:9000 \
LATIQ_S3_ACCESS_KEY=admin LATIQ_S3_SECRET_KEY=password \
cargo test -p latiq --test catalogs_iceberg -- --ignored --nocapture
```

It registers the catalog, `describe`s it (finds `demo.widgets`), `pull`s a subset
into a pond, and verifies the rows landed — the same flow an agent runs over MCP.

In CI this is the opt-in `iceberg` job (commit message contains `[iceberg-ci]`,
or trigger the workflow manually). The deterministic **DuckLake** catalog e2e
(`tests/catalogs.rs`) covers the attacher path on every push without Docker.

> Note: this harness was authored without a running Docker daemon available, so
> the compose/seed may need minor version pinning for your environment (the
> `apache/iceberg-rest-fixture` + `minio` images and `pyiceberg` are the moving
> parts). The Rust side (`catalogs_iceberg.rs`) is the stable contract.
