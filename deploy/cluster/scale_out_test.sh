#!/usr/bin/env bash
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

# Dynamic scale-out e2e against the real cluster (deploy/cluster/docker-compose.yml).
# Proves: start with 2 pond nodes -> add a 3rd at runtime -> new ponds land on it
# -> queries routed to the new node return correct results.
#
# Assumes the 2-node cluster is already up:
#   LATIQ_IMAGE=ghcr.io/neonexia/latiq:dev docker compose -f docker-compose.yml up -d \
#       control-plane pond-node-1 pond-node-2 prometheus
# Then: ./scale_out_test.sh
#
# Runtime auto-detects docker vs podman (override with LATIQ_COMPOSE). Needs jq.
set -euo pipefail
cd "$(dirname "$0")"

pick_compose() {
  if [[ -n "${LATIQ_COMPOSE:-}" ]]; then echo "$LATIQ_COMPOSE"; return; fi
  if docker info >/dev/null 2>&1; then echo "docker compose"; return; fi
  if command -v podman >/dev/null 2>&1 && podman info >/dev/null 2>&1; then
    echo "podman compose"; return
  fi
  echo "no running Docker or Podman (set LATIQ_COMPOSE)" >&2; exit 1
}
COMPOSE="$(pick_compose) -f docker-compose.yml"
command -v jq >/dev/null || { echo "jq is required" >&2; exit 1; }

# Run the CLI inside the compose network (so it resolves pond-node-N + control-plane).
cli() { $COMPOSE run --rm --no-deps -T cli "$@"; }

active_nodes() { cli stats --format json 2>/dev/null | jq '[.node_detail[] | select(.state=="active")] | length'; }
node_state()   { cli stats --format json 2>/dev/null | jq -r --arg n "$1" '.node_detail[] | select(.node_id==$n) | .state'; }
ponds_on()     { cli pond list --format json 2>/dev/null | jq -r --arg n "$1" '[.[] | select(.node_id==$n)] | length'; }
first_pond_on(){ cli pond list --format json 2>/dev/null | jq -r --arg n "$1" '[.[] | select(.node_id==$n)][0].name'; }

echo "==> baseline: expect 2 active nodes"
for _ in $(seq 1 60); do [[ "$(active_nodes)" == "2" ]] && break; sleep 2; done
[[ "$(active_nodes)" == "2" ]] || { echo "expected 2 active nodes, got $(active_nodes)" >&2; exit 1; }

echo "==> baseline: create a pond, write + read"
cli pond create --name base >/dev/null
cli query --pond base "CREATE TABLE t AS SELECT 42 AS n" >/dev/null
GOT="$(cli query --pond base --format json "SELECT n FROM t" 2>/dev/null | jq -r '.rows[0][0]')"
[[ "$GOT" == "42" ]] || { echo "baseline query wrong: $GOT" >&2; exit 1; }
echo "    baseline OK"

echo "==> scale up: start pond-node-3 at runtime"
$COMPOSE --profile scale up -d pond-node-3
for _ in $(seq 1 60); do [[ "$(node_state node-3)" == "active" ]] && break; sleep 2; done
[[ "$(node_state node-3)" == "active" ]] || { echo "node-3 never became active" >&2; exit 1; }
echo "    node-3 registered + active"

echo "==> allocate ponds; expect some to land on node-3 (random placement)"
for i in $(seq 1 12); do cli pond create --name "shop-$i" >/dev/null; done
N3="$(ponds_on node-3)"
[[ "$N3" -ge 1 ]] || { echo "no ponds landed on node-3 after 12 allocations" >&2; exit 1; }
echo "    node-3 owns $N3 pond(s)"

echo "==> run a query on a node-3-owned pond (CLI resolves the owner node-direct)"
P="$(first_pond_on node-3)"
cli query --pond "$P" "CREATE TABLE hits AS SELECT 7 AS v" >/dev/null
V="$(cli query --pond "$P" --format json "SELECT v FROM hits" 2>/dev/null | jq -r '.rows[0][0]')"
[[ "$V" == "7" ]] || { echo "query on node-3 pond '$P' wrong: $V" >&2; exit 1; }
echo "    query on node-3 pond '$P' returned 7"

echo "SCALE-OUT E2E PASSED: pond-node-3 added at runtime, owns ponds, serves queries."
