#!/usr/bin/env bash
# Start a Latiq dev stack: control plane (`serve`) + one or more pond nodes
# (`node add`). With --nodes >1, an nginx front door fans the agent (MCP) and
# data (gRPC) surfaces across the nodes, and node-to-node forwarding routes each
# request to the pond's owning node. Binds 127.0.0.1. See ./dev.sh --help.
set -euo pipefail
cd "$(dirname "$0")"

HOST=127.0.0.1
SERVER_PORT=51400
DATA_PORT=51401
NODES=1
ROOT="${HOME}/.latiq"

usage() {
  cat <<EOF
Usage: ./dev.sh [options]

  --server-port <port>  Control plane (Control + Admin gRPC)  (default $SERVER_PORT)
  --data-port   <port>  First pond node's Data gRPC; MCP = +1  (default $DATA_PORT)
  --nodes       <n>     Number of pond nodes to start          (default $NODES)
  --root        <path>  Data root (registry + pond storage)    (default $ROOT)
  -h, --help            Show this help

Node i binds data port (data-port + 2*i) and MCP (data + 1). With --nodes > 1 an
nginx front door is started (requires nginx) and \$LATIQ_QUERY_GATEWAY is printed.

Examples:
  ./dev.sh                                  # single node, no front door
  ./dev.sh --nodes 3                        # 3 nodes behind nginx
  ./dev.sh --nodes 2 --root /tmp/latiq-dev  # throwaway state
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --server-port) SERVER_PORT=$2; shift 2 ;;
    --data-port) DATA_PORT=$2; shift 2 ;;
    --nodes)     NODES=$2;     shift 2 ;;
    --root)      ROOT=$2;      shift 2 ;;
    -h|--help)   usage; exit 0 ;;
    *) echo "Unknown option: $1" >&2; usage >&2; exit 2 ;;
  esac
done

[[ "$NODES" =~ ^[0-9]+$ && "$NODES" -ge 1 ]] || { echo "--nodes must be a positive integer" >&2; exit 2; }

mkdir -p "$ROOT"
ROOT=$(cd "$ROOT" && pwd)        # resolve to an absolute path for the banner
LOG_DIR="$ROOT/logs"
MULTI=0; [[ "$NODES" -gt 1 ]] && MULTI=1

# Per-node ports, and (multi-node) the nginx front-door ports just past them.
NODE_DATA=(); NODE_MCP=(); NODE_METRICS=()
for ((i = 0; i < NODES; i++)); do
  NODE_DATA+=($((DATA_PORT + 2 * i)))
  NODE_MCP+=($((DATA_PORT + 2 * i + 1)))
  NODE_METRICS+=($((DATA_PORT + 2 * i + 1000)))   # binary default: data port + 1000
done
GW_DATA=$((DATA_PORT + 2 * NODES))
GW_MCP=$((GW_DATA + 1))
CP_METRICS=$((SERVER_PORT + 1000))                     # control-plane /metrics

# Colors — only when stdout is a terminal (so piping/redirecting stays clean).
if [ -t 1 ]; then
  HDR=$'\033[1m'      # banner: bold white
  LBL=$'\033[2m'      # labels: dim
  VAL=$'\033[0m'      # values: normal white
  DIM=$'\033[2m'
  ERRC=$'\033[1;31m'  # errors: red (kept visible)
  RST=$'\033[0m'
else
  HDR='' LBL='' VAL='' DIM='' ERRC='' RST=''
fi

# Fail early (with the culprit) if a port is already taken. `$3` is the
# remedy hint: the flag that actually moves THIS port (control-plane ports
# shift with --server-port; every node/gateway port derives from the
# --data-port base), so the suggestion is always actionable.
check_port() {
  local port=$1 name=$2 hint=$3
  if lsof -nP -iTCP:"$port" -sTCP:LISTEN >/dev/null 2>&1; then
    printf '%sERROR%s: %s port %s is already in use by:\n' "$ERRC" "$RST" "$name" "$port" >&2
    lsof -nP -iTCP:"$port" -sTCP:LISTEN >&2
    printf 'Free it, or pick another port, e.g. ./dev.sh %s\n' "$hint" >&2
    exit 1
  fi
}
check_port "$SERVER_PORT" "Control plane" "--server-port 41400"
for ((i = 0; i < NODES; i++)); do
  check_port "${NODE_DATA[$i]}" "node-$i Data gRPC" "--data-port 41401"
  check_port "${NODE_MCP[$i]}" "node-$i MCP" "--data-port 41401"
done
if [[ $MULTI -eq 1 ]]; then
  command -v nginx >/dev/null 2>&1 || {
    printf '%sERROR%s: --nodes > 1 needs nginx for the front door. Install it: brew install nginx\n' "$ERRC" "$RST" >&2
    exit 1
  }
  check_port "$GW_DATA" "gateway Data gRPC" "--data-port 41401"
  check_port "$GW_MCP" "gateway MCP" "--data-port 41401"
fi

printf '%sbuilding latiq…%s\n' "$DIM" "$RST"
cargo build -q -p latiq
BIN=target/debug/latiq
VERSION=$("$BIN" --version 2>/dev/null | awk '{print $2}')
mkdir -p "$LOG_DIR"
CP_LOG="$LOG_DIR/control-plane.log"

PIDS=()
NGINX_PID=""
cleanup() {
  [[ -n "$NGINX_PID" ]] && kill "$NGINX_PID" 2>/dev/null || true
  for p in "${PIDS[@]}"; do kill "$p" 2>/dev/null || true; done
}
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

# --- control plane ------------------------------------------------------
"$BIN" serve --port "$SERVER_PORT" --root "$ROOT" >"$CP_LOG" 2>&1 &
CP_PID=$!; PIDS+=("$CP_PID")
wait_ready "$CP_PID" "control plane" "$CP_LOG" "$SERVER_PORT"

# --- pond nodes ---------------------------------------------------------
for ((i = 0; i < NODES; i++)); do
  log="$LOG_DIR/node-$i.log"
  LATIQ_SERVER="http://$HOST:$SERVER_PORT" "$BIN" node add \
    --node-id "node-$i" --port "${NODE_DATA[$i]}" --root "$ROOT" >"$log" 2>&1 &
  pid=$!; PIDS+=("$pid")
  wait_ready "$pid" "node-$i" "$log" "${NODE_DATA[$i]}" "${NODE_MCP[$i]}"
done

# --- nginx front door (multi-node only) ---------------------------------
if [[ $MULTI -eq 1 ]]; then
  NGINX_CONF="$ROOT/nginx.conf"
  TMP="$ROOT/nginx-tmp"
  mkdir -p "$TMP"
  mcp_servers=""; data_servers=""
  for ((i = 0; i < NODES; i++)); do
    mcp_servers+="        server $HOST:${NODE_MCP[$i]};"$'\n'
    data_servers+="        server $HOST:${NODE_DATA[$i]};"$'\n'
  done
  cat >"$NGINX_CONF" <<EOF
worker_processes 1;
error_log $LOG_DIR/nginx-error.log warn;
pid $ROOT/nginx.pid;
events { worker_connections 256; }
http {
    access_log $LOG_DIR/nginx-access.log;
    client_body_temp_path $TMP/body;
    proxy_temp_path $TMP/proxy;
    fastcgi_temp_path $TMP/fastcgi;
    uwsgi_temp_path $TMP/uwsgi;
    scgi_temp_path $TMP/scgi;

    # Agents (MCP) — sticky so a streamable-HTTP session stays on its greeter.
    upstream latiq_mcp {
        ip_hash;
$mcp_servers    }
    # CLI/SDK (Data gRPC) — spread; node-to-node forwarding handles ownership.
    upstream latiq_data {
$data_servers    }

    server {
        listen $GW_MCP;
        location /mcp {
            proxy_pass http://latiq_mcp;
            proxy_http_version 1.1;
            proxy_set_header Connection "";
            proxy_set_header Host \$host;
            proxy_buffering off;
        }
    }
    server {
        listen $GW_DATA http2;
        location / { grpc_pass grpc://latiq_data; }
    }
}
EOF
  nginx -c "$NGINX_CONF" -p "$ROOT" -g 'daemon off;' >"$LOG_DIR/nginx.log" 2>&1 &
  NGINX_PID=$!
  wait_ready "$NGINX_PID" "nginx front door" "$LOG_DIR/nginx.log" "$GW_DATA" "$GW_MCP"
fi

# --- prometheus scrape config -------------------------------------------
# Each process serves /metrics on its port + 1000. Write a ready-to-use config
# (60s scrape = per-minute) so the operator can `prometheus --config.file=...`.
PROM_CFG="$ROOT/prometheus.yml"
{
  printf 'global:\n  scrape_interval: 60s\nscrape_configs:\n  - job_name: latiq\n    static_configs:\n      - targets:\n'
  printf '          - "%s:%s"\n' "$HOST" "$CP_METRICS"
  for ((i = 0; i < NODES; i++)); do printf '          - "%s:%s"\n' "$HOST" "${NODE_METRICS[$i]}"; done
} >"$PROM_CFG"

# --- banner -------------------------------------------------------------
row() { printf '   %s%-12s%s %s%s%s\n' "$LBL" "$1" "$RST" "$VAL" "$2" "$RST"; }
echo
printf '%s ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━%s\n' "$HDR" "$RST"
printf '%s  latiq%s %sagent-native data pond%s %s· v%s%s\n' "$HDR" "$RST" "$DIM" "$RST" "$DIM" "${VERSION:-?}" "$RST"
printf '%s ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━%s\n' "$HDR" "$RST"
echo
row "control"   "$HOST:$SERVER_PORT   (Control + Admin gRPC)"
if [[ $MULTI -eq 1 ]]; then
  row "gateway"   "data gRPC $HOST:$GW_DATA · mcp http://$HOST:$GW_MCP/mcp"
  for ((i = 0; i < NODES; i++)); do
    row "node-$i" "data $HOST:${NODE_DATA[$i]} · mcp $HOST:${NODE_MCP[$i]}"
  done
else
  row "data gRPC" "$HOST:${NODE_DATA[0]}"
  row "mcp"       "http://$HOST:${NODE_MCP[0]}/mcp"
  row "node"      "node-0"
fi
row "registry"  "$ROOT/registry.duckdb"
row "ponds"     "$ROOT/ponds"
row "logs"      "$LOG_DIR/"
if [[ $MULTI -eq 1 ]]; then
  metrics_list="cp $HOST:$CP_METRICS"
  for ((i = 0; i < NODES; i++)); do metrics_list+=" · n$i $HOST:${NODE_METRICS[$i]}"; done
  row "metrics" "$metrics_list"
else
  row "metrics" "cp http://$HOST:$CP_METRICS/metrics · node http://$HOST:${NODE_METRICS[0]}/metrics"
fi
row "prometheus" "$PROM_CFG  (prometheus --config.file=$PROM_CFG)"
echo
if [[ $MULTI -eq 1 ]]; then
  printf '   %sDrive the CLI through the front door:%s\n' "$DIM" "$RST"
  printf '   %sexport LATIQ_QUERY_GATEWAY=http://%s:%s%s\n' "$VAL" "$HOST" "$GW_DATA" "$RST"
  echo
fi
printf '   %sCtrl+C to stop.%s\n' "$DIM" "$RST"

wait
