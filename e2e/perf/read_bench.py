#!/usr/bin/env python3
"""Read-focused concurrency + reliability benchmark for Latiq.

Answers the questions the product spec makes load-bearing:
  * "300 agents can share one pond"  -> how does read throughput/latency behave
    as N concurrent readers hit ONE shared pond?
  * "scale out, don't distribute"    -> how does the same load behave spread
    across N ponds (the shape Latiq is designed for)?
  * resource isolation               -> does a greedy neighbour wreck a small pond?
  * soak / leaks                     -> do RSS, fds, or latency drift over time?

Two agent-graph shapes, kept separate on purpose:
  A) ONE writer (an orchestrator seeds state) + MANY readers   <- the common case
  B) MANY writers + MANY readers (agents also create state)

Runs against embedded mode by default (in-process: the engine's CPU/RSS ARE this
process's, so sampling is exact and there is no container VM capping cores).

  python read_bench.py                      # full suite -> results JSON
  python read_bench.py --quick              # small sweep, for iterating
  python read_bench.py --only shared_pond
"""
from __future__ import annotations

import argparse
import json
import os
import platform
import statistics
import subprocess
import sys
import threading
import time
from dataclasses import asdict, dataclass, field

import psutil

import latiq

# The read under test: a real analytical scan+aggregate, sized so one query is
# tens of ms — long enough that queueing shows up, short enough to get many
# samples. NOT a point lookup; agents run analytics.
READ_SQL = "SELECT k, count(*) AS n, avg(v) AS av FROM t GROUP BY k ORDER BY n DESC LIMIT 10"
# Scenario B's writer statement: appends agent state to its own table.
WRITE_SQL = "INSERT INTO w VALUES ({i}, 'agent-state-{i}')"


# ---------------------------------------------------------------- machine ----
def machine_profile() -> dict:
    """Capture the box, so numbers mean something to whoever reads the report."""

    def sysctl(key: str) -> str | None:
        try:
            return subprocess.check_output(["sysctl", "-n", key], text=True).strip()
        except Exception:
            return None

    mem = psutil.virtual_memory().total
    prof = {
        "cpu": sysctl("machdep.cpu.brand_string") or platform.processor() or "unknown",
        "model": sysctl("hw.model"),
        "logical_cores": psutil.cpu_count(logical=True),
        "physical_cores": psutil.cpu_count(logical=False),
        # Apple Silicon splits cores by performance level: 0 = P-cores, 1 = E-cores.
        "p_cores": _int(sysctl("hw.perflevel0.logicalcpu")),
        "e_cores": _int(sysctl("hw.perflevel1.logicalcpu")),
        "memory_gb": round(mem / 1024**3),
        "platform": f"{platform.system()} {platform.release()}",
        "python": platform.python_version(),
    }
    return prof


def _int(v):
    try:
        return int(v)
    except (TypeError, ValueError):
        return None


# ---------------------------------------------------------------- sampling ---
class Sampler(threading.Thread):
    """Samples this process while a scenario runs: CPU%, RSS, fds, threads.

    In embedded mode the Latiq engine lives in this process, so these are the
    engine's numbers. cpu_percent is process-wide and can exceed 100% (100% =
    one core fully busy), which is exactly how we see whether the box is used.
    """

    def __init__(self, interval: float = 0.25):
        super().__init__(daemon=True)
        self.interval = interval
        self.proc = psutil.Process()
        self.cpu: list[float] = []
        self.rss: list[int] = []
        self.fds: list[int] = []
        self.threads: list[int] = []
        self._stop = threading.Event()
        self.proc.cpu_percent(None)  # prime

    def run(self):
        while not self._stop.wait(self.interval):
            try:
                self.cpu.append(self.proc.cpu_percent(None))
                self.rss.append(self.proc.memory_info().rss)
                self.fds.append(self.proc.num_fds())
                self.threads.append(self.proc.num_threads())
            except Exception:
                pass

    def stop(self) -> dict:
        self._stop.set()
        self.join(timeout=2)
        busy = [c for c in self.cpu if c > 0]
        return {
            "cpu_pct_mean": round(statistics.mean(busy), 1) if busy else 0.0,
            "cpu_pct_peak": round(max(self.cpu), 1) if self.cpu else 0.0,
            "cores_busy_mean": round(statistics.mean(busy) / 100, 2) if busy else 0.0,
            "rss_mb_start": round(self.rss[0] / 1024**2, 1) if self.rss else 0,
            "rss_mb_end": round(self.rss[-1] / 1024**2, 1) if self.rss else 0,
            "rss_mb_peak": round(max(self.rss) / 1024**2, 1) if self.rss else 0,
            "fds_start": self.fds[0] if self.fds else 0,
            "fds_end": self.fds[-1] if self.fds else 0,
            "threads_peak": max(self.threads) if self.threads else 0,
        }


# ----------------------------------------------------------------- results ---
@dataclass
class Point:
    """One measured data point: a concurrency level within a scenario."""

    concurrency: int
    seconds: float
    ops: int
    ops_per_s: float
    p50_ms: float
    p95_ms: float
    p99_ms: float
    max_ms: float
    errors: int
    error_kinds: dict = field(default_factory=dict)
    resources: dict = field(default_factory=dict)
    # Scenario B only
    writer_ops: int = 0
    writer_p95_ms: float = 0.0


def summarize(lat: list[float], secs: float, errs: list[str], res: dict, c: int) -> Point:
    lat_sorted = sorted(lat)

    def pct(p):
        if not lat_sorted:
            return 0.0
        return round(lat_sorted[min(len(lat_sorted) - 1, int(len(lat_sorted) * p))], 2)

    kinds: dict[str, int] = {}
    for e in errs:
        kinds[e] = kinds.get(e, 0) + 1
    return Point(
        concurrency=c,
        seconds=round(secs, 2),
        ops=len(lat),
        ops_per_s=round(len(lat) / secs, 1) if secs else 0.0,
        p50_ms=pct(0.50),
        p95_ms=pct(0.95),
        p99_ms=pct(0.99),
        max_ms=round(max(lat_sorted), 2) if lat_sorted else 0.0,
        errors=len(errs),
        error_kinds=kinds,
        resources=res,
    )


# ------------------------------------------------------------------ driver ---
def drive_reads(pond_for_worker, concurrency: int, duration: float) -> tuple[list, list]:
    """Run `concurrency` reader threads for `duration`s. Returns (latencies, errors).

    The SDK releases the GIL around every gRPC call, so Python threads issue
    genuinely concurrent reads — the client is not the bottleneck.
    """
    lat: list[float] = []
    errs: list[str] = []
    lock = threading.Lock()
    stop_at = time.perf_counter() + duration
    barrier = threading.Barrier(concurrency)

    def worker(idx: int):
        mine: list[float] = []
        mine_err: list[str] = []
        try:
            pond = pond_for_worker(idx)
            # All readers start together; timeout so one sick worker can't wedge
            # the whole run.
            barrier.wait(timeout=60)
        except Exception as e:
            with lock:
                errs.append(f"setup:{type(e).__name__}")
            return
        while time.perf_counter() < stop_at:
            t0 = time.perf_counter()
            try:
                pond.query(sql=READ_SQL)
                mine.append((time.perf_counter() - t0) * 1000)
            except Exception as e:  # record, don't abort: error rate is a metric
                msg = str(e).split("\n")[0][:120]
                mine_err.append(f"{type(e).__name__}: {msg}")
        with lock:
            lat.extend(mine)
            errs.extend(mine_err)

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(concurrency)]
    t0 = time.perf_counter()
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    return lat, errs, time.perf_counter() - t0


def measure(pond_for_worker, concurrency: int, duration: float) -> Point:
    s = Sampler()
    s.start()
    lat, errs, secs = drive_reads(pond_for_worker, concurrency, duration)
    res = s.stop()
    return summarize(lat, secs, errs, res, concurrency)


# --------------------------------------------------------------- scenarios ---
def seed(db, name: str, rows: int, tier: str = "medium"):
    """Orchestrator-style seed: one writer materializes the shared working set."""
    pond = db.create_pond(name=name, tier=tier, description="read-bench")
    pond.query(
        sql=f"CREATE TABLE t AS SELECT (i % 50) AS k, i AS v "
        f"FROM range(0, {rows}) tbl(i)"
    )
    pond.query(sql="CREATE TABLE w(i BIGINT, s VARCHAR)")
    return pond


def scenario_shared_pond(db, levels, duration, rows) -> dict:
    """A) ONE writer seeded it; MANY readers share ONE pond. The common case."""
    pond = seed(db, "bench_shared", rows)
    pond.query(sql=READ_SQL)  # warm
    points = []
    for c in levels:
        print(f"  shared-pond   readers={c:<3}", end="", flush=True)
        p = measure(lambda _i: pond, c, duration)
        points.append(p)
        print(f" -> {p.ops_per_s:>8.1f} q/s  p95 {p.p95_ms:>7.2f}ms  "
              f"cores~{p.resources['cores_busy_mean']:>5.2f}  err {p.errors}")
    return {"points": [asdict(p) for p in points]}


def scenario_pond_per_reader(db, levels, duration, rows) -> dict:
    """The scale-out shape: each reader gets its OWN pond (own engine instance)."""
    maxc = max(levels)
    ponds = []
    print(f"  (allocating {maxc} ponds…)", flush=True)
    for i in range(maxc):
        ponds.append(seed(db, f"bench_pp_{i}", rows))
    for p in ponds[: min(4, len(ponds))]:
        p.query(sql=READ_SQL)  # warm a few
    points = []
    for c in levels:
        print(f"  pond-per-rdr  readers={c:<3}", end="", flush=True)
        p = measure(lambda i: ponds[i], c, duration)
        points.append(p)
        print(f" -> {p.ops_per_s:>8.1f} q/s  p95 {p.p95_ms:>7.2f}ms  "
              f"cores~{p.resources['cores_busy_mean']:>5.2f}  err {p.errors}")
    return {"points": [asdict(p) for p in points]}


def scenario_mixed(db, levels, duration, rows) -> dict:
    """B) MANY readers + a writer load on the SAME pond (agents also write state)."""
    pond = seed(db, "bench_mixed", rows)
    pond.query(sql=READ_SQL)
    points = []
    for c in levels:
        print(f"  mixed r+w     readers={c:<3}", end="", flush=True)
        wr_lat: list[float] = []
        wr_err: list[str] = []
        stop = threading.Event()

        def writer():
            i = 0
            while not stop.is_set():
                t0 = time.perf_counter()
                try:
                    pond.query(sql=WRITE_SQL.format(i=i))
                    wr_lat.append((time.perf_counter() - t0) * 1000)
                except Exception as e:
                    wr_err.append(type(e).__name__)
                i += 1

        wt = threading.Thread(target=writer, daemon=True)
        wt.start()
        p = measure(lambda _i: pond, c, duration)
        stop.set()
        wt.join(timeout=10)
        p.writer_ops = len(wr_lat)
        p.writer_p95_ms = round(sorted(wr_lat)[int(len(wr_lat) * 0.95)], 2) if wr_lat else 0.0
        p.errors += len(wr_err)
        points.append(p)
        print(f" -> read {p.ops_per_s:>7.1f} q/s  p95 {p.p95_ms:>7.2f}ms | "
              f"write {p.writer_ops:>5} ops p95 {p.writer_p95_ms:>7.2f}ms  err {p.errors}")
    return {"points": [asdict(p) for p in points]}


def scenario_isolation(db, duration, rows) -> dict:
    """Does a greedy neighbour on a big tier wreck a small pond's read latency?"""
    victim = seed(db, "bench_victim", rows, tier="small")
    victim.query(sql=READ_SQL)
    print("  isolation     baseline (quiet)   ", end="", flush=True)
    quiet = measure(lambda _i: victim, 2, duration)
    print(f"-> p95 {quiet.p95_ms:>7.2f}ms  {quiet.ops_per_s:>7.1f} q/s")

    noisy = seed(db, "bench_noisy", rows * 4, tier="large")
    stop = threading.Event()

    def hog():
        while not stop.is_set():
            try:
                noisy.query(sql="SELECT k, count(*), sum(v), avg(v) FROM t GROUP BY k")
            except Exception:
                pass

    hogs = [threading.Thread(target=hog, daemon=True) for _ in range(8)]
    for h in hogs:
        h.start()
    print("  isolation     under noisy neighbr ", end="", flush=True)
    stressed = measure(lambda _i: victim, 2, duration)
    stop.set()
    for h in hogs:
        h.join(timeout=5)
    print(f"-> p95 {stressed.p95_ms:>7.2f}ms  {stressed.ops_per_s:>7.1f} q/s")
    return {
        "quiet": asdict(quiet),
        "stressed": asdict(stressed),
        "p95_degradation_x": round(stressed.p95_ms / quiet.p95_ms, 2) if quiet.p95_ms else 0,
    }


def scenario_soak(db, duration, rows, concurrency) -> dict:
    """Sustained reads: watch RSS / fds / latency drift. Leak + stability check."""
    pond = seed(db, "bench_soak", rows)
    pond.query(sql=READ_SQL)
    print(f"  soak          {concurrency} readers for {duration:.0f}s", flush=True)
    s = Sampler(interval=0.5)
    s.start()
    buckets = []
    slice_s = max(5.0, duration / 12)
    t_end = time.perf_counter() + duration
    while time.perf_counter() < t_end:
        lat, errs, secs = drive_reads(lambda _i: pond, concurrency, slice_s)
        b = summarize(lat, secs, errs, {}, concurrency)
        buckets.append(asdict(b))
        print(f"    t+{len(buckets) * slice_s:>5.0f}s  {b.ops_per_s:>7.1f} q/s  "
              f"p95 {b.p95_ms:>7.2f}ms  err {b.errors}", flush=True)
    res = s.stop()
    first, last = buckets[0], buckets[-1]
    return {
        "buckets": buckets,
        "resources": res,
        "rss_growth_mb": round(res["rss_mb_end"] - res["rss_mb_start"], 1),
        "fd_growth": res["fds_end"] - res["fds_start"],
        "p95_drift_x": round(last["p95_ms"] / first["p95_ms"], 2) if first["p95_ms"] else 0,
    }


# -------------------------------------------------------------------- main ---
def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--rows", type=int, default=2_000_000, help="rows seeded per pond")
    ap.add_argument("--duration", type=float, default=6.0, help="seconds per data point")
    ap.add_argument("--soak", type=float, default=90.0, help="soak seconds")
    ap.add_argument("--quick", action="store_true", help="small sweep, short runs")
    ap.add_argument("--only", default=None, help="run one scenario by name")
    ap.add_argument("--out", default="e2e/perf/read_bench_results.json")
    a = ap.parse_args()

    levels = [1, 2, 4, 8, 16, 32]
    if a.quick:
        levels, a.duration, a.soak, a.rows = [1, 4, 16], 3.0, 20.0, 500_000

    mp = machine_profile()
    print("=" * 78)
    print(f"Latiq read benchmark — {mp['cpu']} ({mp['p_cores']}P+{mp['e_cores']}E "
          f"= {mp['logical_cores']} cores), {mp['memory_gb']} GB")
    print(f"  mode=EMBEDDED  rows/pond={a.rows:,}  {a.duration}s per point  "
          f"levels={levels}")
    print("=" * 78)

    db = latiq.connect("local")
    results = {
        "machine": mp,
        "config": {"rows": a.rows, "duration_s": a.duration, "levels": levels,
                   "soak_s": a.soak, "mode": "embedded", "read_sql": READ_SQL},
        "started": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "scenarios": {},
    }
    want = (lambda n: a.only is None or a.only == n)

    if want("shared_pond"):
        print("\n[A] ONE writer seeded · MANY readers · ONE shared pond")
        results["scenarios"]["shared_pond"] = scenario_shared_pond(db, levels, a.duration, a.rows)
    if want("pond_per_reader"):
        print("\n[A'] scale-out contrast · each reader its OWN pond")
        results["scenarios"]["pond_per_reader"] = scenario_pond_per_reader(db, levels, a.duration, a.rows)
    if want("mixed"):
        print("\n[B] MANY readers + a concurrent writer · same pond")
        results["scenarios"]["mixed"] = scenario_mixed(db, levels, a.duration, a.rows)
    if want("isolation"):
        print("\n[C] resource isolation · small pond next to a greedy neighbour")
        results["scenarios"]["isolation"] = scenario_isolation(db, a.duration, a.rows)
    if want("soak"):
        print("\n[D] soak · sustained reads, leak + drift watch")
        results["scenarios"]["soak"] = scenario_soak(db, a.soak, a.rows, 8)

    results["finished"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    out = os.path.abspath(a.out)
    os.makedirs(os.path.dirname(out), exist_ok=True)
    with open(out, "w") as f:
        json.dump(results, f, indent=2)
    print(f"\nresults -> {out}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
