#!/usr/bin/env bash
# Run Latiq locally with one command — no repo clone needed. Pulls the published
# image from GHCR and starts a control plane + 2 pond nodes, then drops a `./latiq`
# CLI wrapper you use to talk to the cluster.
#
#   curl -fsSL https://raw.githubusercontent.com/neonexia/latiq/main/deploy/latiq-up.sh | sh
#   # or download + run:  ./latiq-up.sh        (./latiq-up.sh down  to stop)
#
# Works with Docker or Podman (auto-detected; override with LATIQ_COMPOSE).
# Pin a version with LATIQ_IMAGE=ghcr.io/neonexia/latiq:v0.1.0
set -euo pipefail

IMAGE="${LATIQ_IMAGE:-ghcr.io/neonexia/latiq:latest}"
WORKDIR="${LATIQ_HOME:-$HOME/.latiq-local}"
mkdir -p "$WORKDIR"

pick_compose() {
  if [[ -n "${LATIQ_COMPOSE:-}" ]]; then echo "$LATIQ_COMPOSE"; return; fi
  if command -v docker >/dev/null 2>&1 && docker info >/dev/null 2>&1; then echo "docker compose"; return; fi
  if command -v podman >/dev/null 2>&1 && podman info >/dev/null 2>&1; then echo "podman compose"; return; fi
  echo "need a running Docker or Podman (set LATIQ_COMPOSE to override)" >&2; exit 1
}
COMPOSE="$(pick_compose) -f $WORKDIR/docker-compose.yml"

# Self-contained compose (control plane + 2 pond nodes). Each node advertises its
# compose hostname so forwarding/CLI routing works across containers.
cat > "$WORKDIR/docker-compose.yml" <<YAML
x-latiq: &latiq
  image: ${IMAGE}
  restart: unless-stopped
services:
  control-plane:
    <<: *latiq
    hostname: control-plane
    command: ["serve","--bind","0.0.0.0","--port","51400","--root","/var/lib/latiq"]
    ports: ["51400:51400"]
    healthcheck: { test: ["CMD","curl","-fsS","http://localhost:52400/metrics"], interval: 3s, timeout: 5s, retries: 30 }
  pond-node-1:
    <<: *latiq
    hostname: pond-node-1
    command: ["node","add","--node-id","node-1","--bind","0.0.0.0","--port","51401","--advertise-addr","pond-node-1:51401","--root","/var/lib/latiq"]
    environment: { LATIQ_SERVER: "http://control-plane:51400" }
    depends_on: { control-plane: { condition: service_healthy } }
  pond-node-2:
    <<: *latiq
    hostname: pond-node-2
    command: ["node","add","--node-id","node-2","--bind","0.0.0.0","--port","51401","--advertise-addr","pond-node-2:51401","--root","/var/lib/latiq"]
    environment: { LATIQ_SERVER: "http://control-plane:51400" }
    depends_on: { control-plane: { condition: service_healthy } }
  cli:
    <<: *latiq
    restart: "no"
    profiles: ["tools"]
    entrypoint: ["latiq"]
    environment: { LATIQ_SERVER: "http://control-plane:51400" }
    depends_on: [control-plane, pond-node-1, pond-node-2]
YAML

if [[ "${1:-}" == "down" ]]; then $COMPOSE --profile tools down -v; exit 0; fi

echo "pulling $IMAGE …";  $COMPOSE pull
echo "starting cluster …"; $COMPOSE up -d control-plane pond-node-1 pond-node-2

# `latiq` CLI wrapper — runs the CLI inside the cluster network so it can resolve
# the pond nodes. Use it like the native binary: `./latiq pond create --name demo`.
printf '#!/usr/bin/env bash\nexec %s run --rm --no-deps -T cli "$@"\n' "$COMPOSE" > "$WORKDIR/latiq"
chmod +x "$WORKDIR/latiq"

cat <<EOF

Latiq is up.
  control plane : localhost:51400   (set LATIQ_SERVER=http://localhost:51400 for a native CLI)
  CLI wrapper   : $WORKDIR/latiq

Try it:
  $WORKDIR/latiq pond create --name demo
  $WORKDIR/latiq query --pond demo "CREATE TABLE t AS SELECT 42 AS n"
  $WORKDIR/latiq query --pond demo "SELECT * FROM t"
  $WORKDIR/latiq stats

Native CLI (optional): the same single binary serves the CLI. Until prebuilt
binaries are published, the wrapper above runs it from the image. To install
natively, build from source (cargo build --release -p latiq) and point it at the
cluster with: export LATIQ_SERVER=http://localhost:51400

Stop everything:  ./latiq-up.sh down
EOF
