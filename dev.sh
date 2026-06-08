#!/usr/bin/env bash
# Start a Latiq dev stack: control-plane + one pond-node, print endpoints.
# Ports are overridable via flags so you can run alongside other services.
# Everything binds to 127.0.0.1 (prod deployments like k8s bind loopback anyway).
# See ./dev.sh --help.
set -euo pipefail
cd "$(dirname "$0")"

HOST=127.0.0.1
CONTROL_PORT=9090
ADMIN_PORT=9091
MCP_PORT=8080
DATA_PORT=8081
DB=./latiq-cp.duckdb
DATA=./latiq-data

usage() {
  cat <<EOF
Usage: ./dev.sh [options]

  --control-port <port>  Control gRPC port    (default $CONTROL_PORT)
  --admin-port   <port>  Admin gRPC port      (default $ADMIN_PORT)
  --mcp-port     <port>  MCP-over-HTTP port   (default $MCP_PORT)
  --data-port    <port>  Data/Query gRPC port (default $DATA_PORT)
  --db           <path>  Registry DuckDB file (default $DB)
  --data-dir     <path>  Pond storage root    (default $DATA)
  -h, --help             Show this help

Everything binds to $HOST.

Example (run alongside another stack):
  ./dev.sh --control-port 19090 --admin-port 19091 --mcp-port 18080 --data-port 18081
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --control-port) CONTROL_PORT=$2; shift 2 ;;
    --admin-port)   ADMIN_PORT=$2;   shift 2 ;;
    --mcp-port)     MCP_PORT=$2;     shift 2 ;;
    --data-port)    DATA_PORT=$2;    shift 2 ;;
    --db)           DB=$2;           shift 2 ;;
    --data-dir)     DATA=$2;         shift 2 ;;
    -h|--help)      usage; exit 0 ;;
    *) echo "Unknown option: $1" >&2; usage >&2; exit 2 ;;
  esac
done

CONTROL_ADDR=$HOST:$CONTROL_PORT
ADMIN_ADDR=$HOST:$ADMIN_PORT
MCP_ADDR=$HOST:$MCP_PORT
DATA_ADDR=$HOST:$DATA_PORT

# Fail early (with the culprit) if a port is already taken — a stale stack or
# another service squatting on it is the usual cause of confusing startup errors.
check_port() {
  local port=$1 name=$2
  if lsof -nP -iTCP:"$port" -sTCP:LISTEN >/dev/null 2>&1; then
    echo "ERROR: $name port $port is already in use by:" >&2
    lsof -nP -iTCP:"$port" -sTCP:LISTEN >&2
    echo "Free it, or pick another port, e.g. ./dev.sh --control-port 19090" >&2
    exit 1
  fi
}
check_port "$CONTROL_PORT" "Control gRPC"
check_port "$ADMIN_PORT" "Admin gRPC"
check_port "$MCP_PORT" "MCP"
check_port "$DATA_PORT" "Data gRPC"

echo "Building latiq..."
cargo build -p latiq
BIN=target/debug/latiq

CP_PID=""
PN_PID=""
cleanup() { kill "$CP_PID" "$PN_PID" 2>/dev/null || true; }
trap cleanup INT TERM EXIT

# A backgrounded server that's already gone died on startup — surface it.
alive() {
  if ! kill -0 "$1" 2>/dev/null; then
    echo "ERROR: $2 exited during startup (see its error above)." >&2
    exit 1
  fi
}

echo "Starting control-plane (Control $CONTROL_ADDR, Admin $ADMIN_ADDR)..."
"$BIN" control-plane --control-addr "$CONTROL_ADDR" --admin-addr "$ADMIN_ADDR" --db "$DB" &
CP_PID=$!
sleep 2
alive "$CP_PID" "control-plane"

echo "Starting pond-node (MCP $MCP_ADDR, Data $DATA_ADDR)..."
"$BIN" pond-node --node-id node-1 --mcp-addr "$MCP_ADDR" --data-addr "$DATA_ADDR" \
  --control "http://$CONTROL_ADDR" --data-dir "$DATA" &
PN_PID=$!
sleep 2
alive "$PN_PID" "pond-node"

cat <<EOF

Latiq dev stack is up:
  MCP (agents only):    http://$MCP_ADDR/mcp
  Data gRPC (CLI/SDK):  $DATA_ADDR
  Control gRPC:         $CONTROL_ADDR
  Admin gRPC (ops):     $ADMIN_ADDR

Try (data CLI — pond node):
  $BIN pond create --name demo
  $BIN write --pond demo "CREATE TABLE t(id INTEGER, note VARCHAR)"
  $BIN write --pond demo "INSERT INTO t VALUES (1,'hello')"
  $BIN query --pond demo "SELECT * FROM t"
  $BIN pond list

Operator CLI (control plane):
  $BIN node list
  $BIN policy show
  $BIN audit tail

Press Ctrl+C to stop.
EOF

wait
