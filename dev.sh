#!/usr/bin/env bash
# Start a Latiq dev stack: control-plane + one pond-node, print endpoints.
# All addresses/paths are overridable via flags so you can run alongside other
# services. See ./dev.sh --help.
set -euo pipefail
cd "$(dirname "$0")"

CONTROL_ADDR=127.0.0.1:9090
ADMIN_ADDR=127.0.0.1:9091
MCP_ADDR=127.0.0.1:8080
DATA_ADDR=127.0.0.1:8081
DB=./latiq-cp.duckdb
DATA=./latiq-data

usage() {
  cat <<EOF
Usage: ./dev.sh [options]

  --control-addr <host:port>  Control gRPC bind   (default $CONTROL_ADDR)
  --admin-addr   <host:port>  Admin gRPC bind     (default $ADMIN_ADDR)
  --mcp-addr     <host:port>  MCP-over-HTTP bind  (default $MCP_ADDR)
  --data-addr    <host:port>  Data/Query gRPC bind(default $DATA_ADDR)
  --db           <path>       Registry DuckDB file(default $DB)
  --data-dir     <path>       Pond storage root   (default $DATA)
  -h, --help                  Show this help

Example (run alongside another stack):
  ./dev.sh --control-addr 127.0.0.1:19090 --admin-addr 127.0.0.1:19091 \\
           --mcp-addr 127.0.0.1:18080 --data-addr 127.0.0.1:18081
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --control-addr) CONTROL_ADDR=$2; shift 2 ;;
    --admin-addr)   ADMIN_ADDR=$2;   shift 2 ;;
    --mcp-addr)     MCP_ADDR=$2;     shift 2 ;;
    --data-addr)    DATA_ADDR=$2;    shift 2 ;;
    --db)           DB=$2;           shift 2 ;;
    --data-dir)     DATA=$2;         shift 2 ;;
    -h|--help)      usage; exit 0 ;;
    *) echo "Unknown option: $1" >&2; usage >&2; exit 2 ;;
  esac
done

# Fail early (with the culprit) if a port is already taken — a stale stack or
# another service squatting on it is the usual cause of confusing startup errors.
check_port() {
  local addr=$1 name=$2 port=${1##*:}
  if lsof -nP -iTCP:"$port" -sTCP:LISTEN >/dev/null 2>&1; then
    echo "ERROR: $name port $addr is already in use by:" >&2
    lsof -nP -iTCP:"$port" -sTCP:LISTEN >&2
    echo "Free it, or pick another port, e.g. ./dev.sh --control-addr 127.0.0.1:19090" >&2
    exit 1
  fi
}
check_port "$CONTROL_ADDR" "Control gRPC"
check_port "$ADMIN_ADDR" "Admin gRPC"
check_port "$MCP_ADDR" "MCP"
check_port "$DATA_ADDR" "Data gRPC"

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
