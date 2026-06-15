#!/usr/bin/env bash
# Bring up MinIO + an Iceberg REST catalog and seed a `demo.widgets` table, so the
# iceberg attacher can be exercised end-to-end. Requires Docker.
#
#   ./up.sh          # start + seed
#   ./up.sh down     # tear down (and wipe volumes)
#
# Then run the (otherwise-ignored) e2e:
#   LATIQ_ICEBERG_ENDPOINT=http://localhost:8181/v1/catalog \
#   LATIQ_ICEBERG_WAREHOUSE=demo LATIQ_ICEBERG_TOKEN=dummy \
#   LATIQ_S3_ENDPOINT=http://localhost:9000 \
#   LATIQ_S3_ACCESS_KEY=admin LATIQ_S3_SECRET_KEY=password \
#   cargo test -p latiq --test catalogs_iceberg -- --ignored --nocapture
set -euo pipefail
cd "$(dirname "$0")"

if [[ "${1:-}" == "down" ]]; then
  docker compose down -v
  exit 0
fi

echo "starting MinIO + Iceberg REST…"
docker compose up -d minio mc iceberg-rest

echo "waiting for the Iceberg REST catalog…"
for _ in $(seq 1 60); do
  if curl -fsS http://localhost:8181/v1/config >/dev/null 2>&1; then break; fi
  sleep 2
done
curl -fsS http://localhost:8181/v1/config >/dev/null || { echo "iceberg-rest not ready" >&2; exit 1; }

echo "seeding demo.widgets…"
docker compose run --rm seed

echo
echo "ready:"
echo "  Iceberg REST : http://localhost:8181"
echo "  MinIO S3     : http://localhost:9000  (console :9001, admin/password)"
echo "  table        : demo.widgets (3 rows)"
