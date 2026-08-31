#!/usr/bin/env python3
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

"""Mid-size performance check against a live Latiq cluster, driven through the
Python SDK exactly as a user's analytics program would. Records write/read/pandas
throughput + aggregate-query latency, asserts generous floors (to catch gross
regressions, not microbenchmark noise), and prints the numbers for trend-watching.

REMOTE (CI): LATIQ_CONTROL + LATIQ_GATEWAY → the dockerized gateway/cluster.
EMBEDDED (local): unset → in-process single node. LATIQ_PERF_ROWS overrides the size.
"""
import os
import statistics
import sys
import time
import uuid

import latiq

REMOTE = bool(os.environ.get("LATIQ_GATEWAY"))
N = int(os.environ.get("LATIQ_PERF_ROWS", "500000"))
ITERS = int(os.environ.get("LATIQ_PERF_ITERS", "20"))

# Generous absolute floors — a healthy cluster clears these by a wide margin; they
# exist to fail loudly on a gross regression (e.g. streaming collapsed to per-row).
FLOOR_READ_ROWS_PER_S = 50_000
CEIL_AGG_P95_MS = 30_000


def connect():
    if REMOTE:
        return latiq.connect(
            server=os.environ["LATIQ_CONTROL"],
            query_gateway=os.environ["LATIQ_GATEWAY"],
        )
    return latiq.connect(server="local")


def main() -> int:
    db = connect()
    mode = "REMOTE (gateway + cluster)" if REMOTE else "EMBEDDED (in-process)"
    print(f"== Latiq perf — {mode}, N={N:,} rows, {ITERS} agg iters ==")

    pond = db.create_pond(name=f"perf-{uuid.uuid4().hex[:8]}", description="perf run")

    # --- Write throughput: materialize N rows (write path + DuckLake commit) -----
    t0 = time.perf_counter()
    pond.query(
        sql=f"CREATE TABLE t AS "
        f"SELECT range AS i, range % 100 AS k, (range * 2654435761) % 1000000 AS v "
        f"FROM range({N})"
    )
    write_s = time.perf_counter() - t0
    write_rps = N / write_s

    # --- Read throughput: stream the whole table back as Arrow (uncapped) --------
    t0 = time.perf_counter()
    tbl = pond.query(sql="SELECT i, k, v FROM t")
    read_s = time.perf_counter() - t0
    assert tbl.num_rows == N, f"read {tbl.num_rows} of {N} rows"
    read_rps = N / read_s

    # --- Arrow → pandas sink throughput (the analytics handoff) ------------------
    t0 = time.perf_counter()
    df = tbl.to_pandas()
    pandas_s = time.perf_counter() - t0
    assert len(df) == N
    # An independent correctness check on the streamed values.
    assert int(df["i"].sum()) == N * (N - 1) // 2
    pandas_rps = N / pandas_s

    # --- Aggregate-query latency -------------------------------------------------
    lat_ms = []
    for _ in range(ITERS):
        t0 = time.perf_counter()
        pond.query(sql="SELECT k, count(*) AS c, avg(v) AS a FROM t GROUP BY k")
        lat_ms.append((time.perf_counter() - t0) * 1000)
    p50 = statistics.median(lat_ms)
    p95 = sorted(lat_ms)[max(0, int(len(lat_ms) * 0.95) - 1)]

    # --- Cross-node fan (remote only): run a query on ponds spread over nodes -----
    cross = ""
    if REMOTE:
        ponds = [db.create_pond(name=f"perf-x-{uuid.uuid4().hex[:6]}") for _ in range(6)]
        for p in ponds:
            p.query(sql="CREATE TABLE q AS SELECT range AS i FROM range(10000)")
        t0 = time.perf_counter()
        for p in ponds:
            assert p.query(sql="SELECT count(*) AS n FROM q").column("n")[0].as_py() == 10000
        fan_s = time.perf_counter() - t0
        nodes = {db.list_ponds()[p.name]["node_id"] for p in ponds}
        cross = f"  cross-node fan : {len(ponds)} ponds over {len(nodes)} node(s), {fan_s:.3f}s\n"
        for p in ponds:
            db.drop_pond(pond=p.name, confirm=True)

    print(
        f"  write (CTAS)   : {write_rps:>12,.0f} rows/s  ({write_s:.3f}s)\n"
        f"  read  (Arrow)  : {read_rps:>12,.0f} rows/s  ({read_s:.3f}s)\n"
        f"  arrow→pandas   : {pandas_rps:>12,.0f} rows/s  ({pandas_s:.3f}s)\n"
        f"  agg latency    : p50 {p50:.1f} ms   p95 {p95:.1f} ms\n"
        f"{cross}"
    )

    db.drop_pond(pond=pond.name, confirm=True)

    # --- Floors (gross-regression guards) ---------------------------------------
    failures = []
    if read_rps < FLOOR_READ_ROWS_PER_S:
        failures.append(f"read throughput {read_rps:,.0f} < floor {FLOOR_READ_ROWS_PER_S:,} rows/s")
    if p95 > CEIL_AGG_P95_MS:
        failures.append(f"agg p95 {p95:.0f}ms > ceiling {CEIL_AGG_P95_MS}ms")
    if failures:
        print("PERF REGRESSION:\n  " + "\n  ".join(failures))
        return 1
    print("perf floors OK")
    return 0


if __name__ == "__main__":
    sys.exit(main())
