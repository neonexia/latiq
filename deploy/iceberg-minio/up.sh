#!/usr/bin/env bash
# Bring up MinIO + an Iceberg REST catalog and seed a `demo.widgets` table, so the
# iceberg attacher can be exercised end-to-end. Requires Docker.
#
#   ./up.sh          # start + seed
#   ./up.sh down     # tear down (and wipe volumes)
#
# Then run the (otherwise-ignored) e2e:
#   LATIQ_ICEBERG_ENDPOINT=http://localhost:8181 \
#   LATIQ_ICEBERG_WAREHOUSE=demo LATIQ_ICEBERG_TOKEN=dummy \
#   LATIQ_S3_ENDPOINT=http://localhost:9000 \
#   LATIQ_S3_ACCESS_KEY=admin LATIQ_S3_SECRET_KEY=password \
#   cargo test -p latiq --test catalogs_iceberg -- --ignored --nocapture
set -euo pipefail
cd "$(dirname "$0")"

# Pick a container runtime: prefer Docker if its daemon is up, else fall back to
# Podman (`podman compose`). Override with LATIQ_COMPOSE="docker compose" etc.
pick_compose() {
  if [[ -n "${LATIQ_COMPOSE:-}" ]]; then echo "$LATIQ_COMPOSE"; return; fi
  if docker info >/dev/null 2>&1; then echo "docker compose"; return; fi
  if command -v podman >/dev/null 2>&1 && podman info >/dev/null 2>&1; then
    echo "podman compose"; return
  fi
  echo "no running Docker or Podman found (start one, or set LATIQ_COMPOSE)" >&2
  exit 1
}
COMPOSE="$(pick_compose)"
echo "using: $COMPOSE"

if [[ "${1:-}" == "down" ]]; then
  $COMPOSE down -v
  exit 0
fi

echo "starting MinIO + Iceberg REST…"
$COMPOSE up -d minio mc iceberg-rest

ICEBERG_PORT="${LATIQ_ICEBERG_PORT:-8181}"
echo "waiting for the Iceberg REST catalog…"
for _ in $(seq 1 60); do
  if curl -fsS "http://localhost:${ICEBERG_PORT}/v1/config" >/dev/null 2>&1; then break; fi
  sleep 2
done
curl -fsS "http://localhost:${ICEBERG_PORT}/v1/config" >/dev/null || { echo "iceberg-rest not ready" >&2; exit 1; }

echo "seeding demo.widgets…"
$COMPOSE run --rm seed

echo
echo "ready:"
echo "  Iceberg REST : http://localhost:${ICEBERG_PORT}"
echo "  MinIO S3     : http://localhost:${LATIQ_S3_PORT:-9000}  (console :${LATIQ_S3_CONSOLE_PORT:-9001}, admin/password)"
echo "  table        : demo.widgets (3 rows)"
