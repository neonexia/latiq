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
LOG_DIR="$ROOT/logs"

# Colors — only when stdout is a terminal (so piping/redirecting stays clean).
if [ -t 1 ]; then
  NAVY=$'\033[1;38;2;0;0;128m'   # navy blue, bold
  LBL=$'\033[36m'                # labels: cyan
  VAL=$'\033[1;37m'              # values: bright white
  DIM=$'\033[2m'
  ERRC=$'\033[1;31m'
  RST=$'\033[0m'
else
  NAVY='' LBL='' VAL='' DIM='' ERRC='' RST=''
fi

# Fail early (with the culprit) if a port is already taken.
check_port() {
  local port=$1 name=$2
  if lsof -nP -iTCP:"$port" -sTCP:LISTEN >/dev/null 2>&1; then
    printf '%sERROR%s: %s port %s is already in use by:\n' "$ERRC" "$RST" "$name" "$port" >&2
    lsof -nP -iTCP:"$port" -sTCP:LISTEN >&2
    printf 'Free it, or pick another port, e.g. ./dev.sh --cp-port 41400\n' >&2
    exit 1
  fi
}
check_port "$CP_PORT" "Control plane"
check_port "$DATA_PORT" "Data gRPC"
check_port "$MCP_PORT" "MCP"

printf '%sbuilding latiq…%s\n' "$DIM" "$RST"
cargo build -q -p latiq
BIN=target/debug/latiq
VERSION=$("$BIN" --version 2>/dev/null | awk '{print $2}')
mkdir -p "$LOG_DIR"
CP_LOG="$LOG_DIR/control-plane.log"
PN_LOG="$LOG_DIR/node-1.log"

CP_PID=""
PN_PID=""
cleanup() { kill "$CP_PID" "$PN_PID" 2>/dev/null || true; }
trap cleanup INT TERM EXIT

# Wait until a backgrounded server accepts connections on all its ports, failing
# fast (and showing its log) if it dies first.
wait_ready() {
  local pid=$1 name=$2 log=$3
  shift 3
  local ports=("$@")
  local i p all_up
  for ((i = 0; i < 150; i++)); do
    kill -0 "$pid" 2>/dev/null || {
      printf '%sERROR%s: %s exited during startup — last lines of %s:\n' "$ERRC" "$RST" "$name" "$log" >&2
      tail -n 15 "$log" >&2
      exit 1
    }
    all_up=1
    for p in "${ports[@]}"; do
      (exec 3<>"/dev/tcp/$HOST/$p") 2>/dev/null && exec 3>&- 3<&- || all_up=0
    done
    [[ $all_up -eq 1 ]] && return 0
    sleep 0.2
  done
  printf '%sERROR%s: %s did not start listening in time (see %s).\n' "$ERRC" "$RST" "$name" "$log" >&2
  exit 1
}

"$BIN" serve --port "$CP_PORT" --root "$ROOT" >"$CP_LOG" 2>&1 &
CP_PID=$!
wait_ready "$CP_PID" "control plane" "$CP_LOG" "$CP_PORT"

LATIQ_CONTROL="http://$HOST:$CP_PORT" "$BIN" node add --port "$DATA_PORT" --root "$ROOT" >"$PN_LOG" 2>&1 &
PN_PID=$!
wait_ready "$PN_PID" "pond node" "$PN_LOG" "$DATA_PORT" "$MCP_PORT"

# --- banner -------------------------------------------------------------
row() { printf '   %s%-12s%s %s%s%s\n' "$LBL" "$1" "$RST" "$VAL" "$2" "$RST"; }
echo
printf '%s ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━%s\n' "$NAVY" "$RST"
printf '%s  latiq%s %sagent-native data pond%s %s· v%s%s\n' "$NAVY" "$RST" "$DIM" "$RST" "$DIM" "${VERSION:-?}" "$RST"
printf '%s ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━%s\n' "$NAVY" "$RST"
echo
row "control"   "$HOST:$CP_PORT   (Control + Admin gRPC)"
row "data gRPC" "$HOST:$DATA_PORT"
row "mcp"       "http://$HOST:$MCP_PORT/mcp"
row "node"      "node-1"
row "registry"  "$ROOT/registry.duckdb"
row "ponds"     "$ROOT/ponds"
row "logs"      "$LOG_DIR/{control-plane,node-1}.log"
echo
printf '   %sCtrl+C to stop.%s\n' "$DIM" "$RST"

wait
