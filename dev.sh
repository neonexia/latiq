#!/usr/bin/env bash
# Start a Latiq dev stack: control plane (`serve`) + one pond node (`node add`).
# Binds 127.0.0.1; ports/root overridable via flags. See ./dev.sh --help.
set -euo pipefail
cd "$(dirname "$0")"

HOST=127.0.0.1
CP_PORT=51400
DATA_PORT=51401
ROOT="${HOME}/.latiq"

usage() {
  cat <<EOF
Usage: ./dev.sh [options]

  --cp-port   <port>  Control plane (Control + Admin gRPC)  (default $CP_PORT)
  --data-port <port>  Pond node Data gRPC; MCP on port + 1  (default $DATA_PORT)
  --root      <path>  Data root (registry + pond storage)   (default $ROOT)
  -h, --help          Show this help

Example (run alongside another stack, or with throwaway state):
  ./dev.sh --cp-port 41400 --data-port 41401 --root /tmp/latiq-dev
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --cp-port)   CP_PORT=$2;   shift 2 ;;
    --data-port) DATA_PORT=$2; shift 2 ;;
    --root)      ROOT=$2;      shift 2 ;;
    -h|--help)   usage; exit 0 ;;
    *) echo "Unknown option: $1" >&2; usage >&2; exit 2 ;;
  esac
done

MCP_PORT=$((DATA_PORT + 1))

# Fail early (with the culprit) if a port is already taken — a stale stack or
# another service squatting on it is the usual cause of confusing startup errors.
check_port() {
  local port=$1 name=$2
  if lsof -nP -iTCP:"$port" -sTCP:LISTEN >/dev/null 2>&1; then
    echo "ERROR: $name port $port is already in use by:" >&2
    lsof -nP -iTCP:"$port" -sTCP:LISTEN >&2
    echo "Free it, or pick another port, e.g. ./dev.sh --cp-port 41400" >&2
    exit 1
  fi
}
check_port "$CP_PORT" "Control plane"
check_port "$DATA_PORT" "Data gRPC"
check_port "$MCP_PORT" "MCP"

echo "Building latiq..."
cargo build -p latiq
BIN=target/debug/latiq

CP_PID=""
PN_PID=""
cleanup() { kill "$CP_PID" "$PN_PID" 2>/dev/null || true; }
trap cleanup INT TERM EXIT

# Wait until a backgrounded server accepts connections on all its ports, failing
# fast if it dies first — avoids the race where the node registers before the
# control plane is listening.
wait_ready() {
  local pid=$1 name=$2
  shift 2
  local ports=("$@")
  local i p all_up
  for ((i = 0; i < 150; i++)); do
    kill -0 "$pid" 2>/dev/null || {
      echo "ERROR: $name exited during startup (see its error above)." >&2
      exit 1
    }
    all_up=1
    for p in "${ports[@]}"; do
      (exec 3<>"/dev/tcp/$HOST/$p") 2>/dev/null && exec 3>&- 3<&- || all_up=0
    done
    [[ $all_up -eq 1 ]] && return 0
    sleep 0.2
  done
  echo "ERROR: $name did not start listening (ports: ${ports[*]}) in time." >&2
  exit 1
}

echo "Starting control plane (Control + Admin on $CP_PORT, root $ROOT)..."
"$BIN" serve --port "$CP_PORT" --root "$ROOT" &
CP_PID=$!
wait_ready "$CP_PID" "control plane" "$CP_PORT"

echo "Starting pond node (Data $DATA_PORT, MCP $MCP_PORT)..."
LATIQ_CONTROL="http://$HOST:$CP_PORT" "$BIN" node add --port "$DATA_PORT" --root "$ROOT" &
PN_PID=$!
wait_ready "$PN_PID" "pond node" "$DATA_PORT" "$MCP_PORT"

cat <<EOF

Latiq dev stack is up:
  Control plane (CLI):  $HOST:$CP_PORT   (Control + Admin gRPC)
  Pond node Data gRPC:  $HOST:$DATA_PORT
  MCP (agents only):    http://$HOST:$MCP_PORT/mcp
  Root:                 $ROOT

Try (the CLI talks only to the control plane; it routes to the node for you):
  $BIN pond create --name demo
  $BIN query --pond demo "CREATE TABLE t(id INTEGER, note VARCHAR)"
  $BIN query --pond demo "INSERT INTO t VALUES (1,'hello')"
  $BIN query --pond demo "SELECT * FROM t"
  $BIN pond list
  $BIN node list

The CLI reads \$LATIQ_CONTROL (default http://127.0.0.1:51400). If you changed
--cp-port, export it in your CLI shell:
  export LATIQ_CONTROL=http://$HOST:$CP_PORT
Press Ctrl+C to stop.
EOF

wait
