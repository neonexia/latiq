#!/usr/bin/env bash
# Start a Latiq dev stack: control-plane + one pond-node, print endpoints.
set -euo pipefail
cd "$(dirname "$0")"

DB=${LATIQ_DB:-./latiq-cp.duckdb}
DATA=${LATIQ_DATA:-./latiq-data}

echo "Building latiq..."
cargo build -p latiq
BIN=target/debug/latiq

echo "Starting control-plane (Control :9090, Admin :9091)..."
"$BIN" control-plane --db "$DB" &
CP_PID=$!
sleep 2

echo "Starting pond-node (MCP :8080)..."
"$BIN" pond-node --data-dir "$DATA" &
PN_PID=$!
sleep 2

cat <<EOF

Latiq dev stack is up:
  MCP (agents only):    http://127.0.0.1:8080/mcp
  Data gRPC (CLI/SDK):  127.0.0.1:8081
  Control gRPC:         127.0.0.1:9090
  Admin gRPC (ops):     127.0.0.1:9091

Try (agent client CLI):
  $BIN pond create --name demo
  $BIN write --pond demo "CREATE TABLE t(id INTEGER, note VARCHAR)"
  $BIN write --pond demo "INSERT INTO t VALUES (1,'hello')"
  $BIN query --pond demo "SELECT * FROM t"
  $BIN pond list

Operator CLI:
  $BIN node list
  $BIN policy show
  $BIN audit tail

Press Ctrl+C to stop.
EOF

trap 'kill $CP_PID $PN_PID 2>/dev/null || true' INT TERM
wait
