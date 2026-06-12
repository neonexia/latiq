# Node liveness + `latiq stats` — design

**Date:** 2026-06-12
**Issue:** #13 (node liveness). Metrics/Prometheus is a separate later pass.
**Goal:** Give the existing node→control-plane heartbeat *consequences* (a reaper
marks stale nodes `down`, placement skips them) and *visibility* (surface
`last_heartbeat`/state; a `latiq stats` system snapshot).

## What exists (so we don't re-build it)

Nodes already push `heartbeat` to the control plane every 10s (`run_pond_node`),
and `register_node` runs on startup. The `nodes` table has `state` (default
`active`) + `last_heartbeat`. **Gaps:** nothing ever sets `state` away from
`active` (no reaper), `heartbeat` hardcodes `pond_count: 0`, and `NodeInfo`
doesn't expose `last_heartbeat`.

## Design

### 1. Reaper (control plane)
- `Registry::reap_stale_nodes(ttl_secs) -> usize` (count newly downed):
  `UPDATE nodes SET state='down' WHERE state='active' AND last_heartbeat < now() - INTERVAL (ttl_secs) SECOND`.
- A background task in `serve_control_plane`: every **10s**, `reap_stale_nodes(30)`
  (TTL = 3 missed heartbeats). Log (`warn!`) when it downs nodes.
- **Revive is automatic:** `heartbeat()` and `register_node()` also set
  `state='active'` (today they refresh `last_heartbeat` but not `state`, so a
  recovered node would stay `down`). A node never knows it was reaped.
- **Placement already filters `state='active'`** (`create_pond`), so downed nodes
  stop receiving ponds with no change there.

### 2. Real pond_count (computed, not stored)
The node doesn't own assignment data, so don't make it report a count. The
registry computes it from the source of truth: `list_nodes` returns
`(SELECT count(*) FROM ponds WHERE node_id = nodes.node_id)`. The heartbeat's
`pond_count` field becomes vestigial (kept on the wire, ignored).

### 3. Surface health
- `NodeRow` gains `last_heartbeat: String` + `heartbeat_age_seconds: i64`
  (`date_diff('second', last_heartbeat, now())` — computed in SQL so the client
  doesn't parse timestamps).
- proto `NodeInfo` gains `last_heartbeat` + `heartbeat_age_seconds`.
- proto `PondSummary` gains `tier` (for the stats by-tier view).
- `admin_service` maps them; `latiq node list` / `node describe` show state +
  beat age.

### 4. `latiq stats` — system snapshot (CLI)
A read against the Admin surface (`list_nodes` + `pond_list`) rendered as a clean
snapshot (TTY-gated color; active=green, down=red):
```
 ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  latiq · system snapshot
 ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

  nodes   3 total · 2 active · 1 down
  ponds   12  ·  medium 8 · large 3 · small 1

  NODE     STATE   PONDS  LAST BEAT  ENDPOINT
  -------  ------  -----  ---------  ----------------
  node-0   active      5  3s ago     127.0.0.1:51401
  node-2   down        3  62s ago    127.0.0.1:51405
```
`--format json` prints the raw snapshot for scripts. (CPU/memory per node is the
*metrics* pass — not here; this snapshot is registry state only.)

## Non-goals
- Prometheus/metrics, CPU/memory collection (next pass).
- Flapping hysteresis / quorum / pluggable TTL — fixed 30s TTL, plain sweep.
- Pond failover/reassignment off a downed node (separate; depends on storage model).
- No migration: `state` + `last_heartbeat` columns already exist.

## Testing
- registry: `reap_stale_nodes` downs a stale node (register, age the heartbeat,
  reap with a short TTL); `heartbeat`/`register_node` revive a downed node;
  `create_pond` skips `down` nodes; `list_nodes` pond_count reflects real
  assignments + carries `heartbeat_age_seconds`.
- admin/e2e: `node list` shows state + beat age.
- `latiq stats` renders (smoke via the full-stack harness or a unit on the
  summary computation).

Gate: `fmt` + `clippy --workspace --all-targets -D warnings` + `cargo test --workspace`.
