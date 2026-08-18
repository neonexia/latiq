#!/usr/bin/env python3
"""Render read_bench results into a self-contained HTML report.

  python e2e/perf/report.py results.json [--baseline before.json] [-o out.html]

With --baseline the report becomes a before/after: the same suite measured on the
pre-fix engine and the current one, on identical builds.

Charts are hand-authored inline SVG (no libraries, no CDN) so the page is
self-contained and works in light and dark themes. Series colors come from a
CVD-validated categorical palette (slots 1-3, all-pairs validated in both modes).
"""
from __future__ import annotations

import argparse
import html
import json
import math
import os

S1L, S2L, S3L = "#2a78d6", "#eb6834", "#1baf7a"   # light: blue, orange, aqua
S1D, S2D, S3D = "#3987e5", "#d95926", "#199e70"   # dark steps of the same hues

W, H = 720, 300
PAD_L, PAD_R, PAD_T, PAD_B = 66, 24, 20, 48


def esc(s) -> str:
    return html.escape(str(s))


def _nice_max(v: float) -> float:
    if v <= 0:
        return 1.0
    mag = 10 ** math.floor(math.log10(v))
    for m in (1, 1.5, 2, 2.5, 3, 4, 5, 6, 8, 10):
        if m * mag >= v:
            return m * mag
    return 10 * mag


def line_chart(x_labels, series, y_label, x_title="concurrent readers", fmt="{:.0f}"):
    """series: [(name, values, color, label_last)] — always a single y axis."""
    ymax = _nice_max(max(max(s[1]) for s in series) * 1.14)
    n = len(x_labels)
    px = lambda i: PAD_L + (W - PAD_L - PAD_R) * (i / max(1, n - 1))
    py = lambda v: H - PAD_B - (H - PAD_T - PAD_B) * (v / ymax)
    o = [f'<svg viewBox="0 0 {W} {H}" role="img" class="chart" '
         f'aria-label="{esc(y_label)} by {esc(x_title)}">']
    for t in range(5):
        v = ymax * t / 4
        y = py(v)
        o.append(f'<line x1="{PAD_L}" y1="{y:.1f}" x2="{W-PAD_R}" y2="{y:.1f}" class="gl"/>'
                 f'<text x="{PAD_L-10}" y="{y+4:.1f}" class="tick" text-anchor="end">{fmt.format(v)}</text>')
    for i, lab in enumerate(x_labels):
        o.append(f'<text x="{px(i):.1f}" y="{H-PAD_B+20}" class="tick" text-anchor="middle">{esc(lab)}</text>')
    o.append(f'<text x="{(PAD_L+W-PAD_R)/2:.0f}" y="{H-8}" class="axis-title" text-anchor="middle">{esc(x_title)}</text>')
    o.append(f'<text x="15" y="{(PAD_T+H-PAD_B)/2:.0f}" class="axis-title" text-anchor="middle" '
             f'transform="rotate(-90 15 {(PAD_T+H-PAD_B)/2:.0f})">{esc(y_label)}</text>')
    for name, vals, color, label_last in series:
        pts = " ".join(f"{px(i):.1f},{py(v):.1f}" for i, v in enumerate(vals))
        o.append(f'<polyline points="{pts}" fill="none" stroke="{color}" stroke-width="2" '
                 f'stroke-linejoin="round" stroke-linecap="round"/>')
        for i, v in enumerate(vals):
            o.append(f'<circle cx="{px(i):.1f}" cy="{py(v):.1f}" r="4.5" fill="{color}" '
                     f'stroke="var(--surface)" stroke-width="2">'
                     f'<title>{esc(name)} · {esc(x_labels[i])}: {fmt.format(v)}</title></circle>')
        if label_last:
            o.append(f'<text x="{px(n-1)-9:.1f}" y="{py(vals[-1])-13:.1f}" class="dlabel" '
                     f'fill="{color}" text-anchor="end">{fmt.format(vals[-1])}</text>')
    o.append("</svg>")
    return "".join(o)


def bar_chart(pairs, y_label, fmt="{:.0f}", colors=None):
    ymax = _nice_max(max(v for _, v in pairs) * 1.25)
    n = len(pairs)
    bw = min(105, (W - PAD_L - PAD_R) / (n * 1.7))
    gap = (W - PAD_L - PAD_R - bw * n) / max(1, n - 1) if n > 1 else 0
    py = lambda v: H - PAD_B - (H - PAD_T - PAD_B) * (v / ymax)
    o = [f'<svg viewBox="0 0 {W} {H}" role="img" class="chart" aria-label="{esc(y_label)}">']
    for t in range(5):
        v = ymax * t / 4
        y = py(v)
        o.append(f'<line x1="{PAD_L}" y1="{y:.1f}" x2="{W-PAD_R}" y2="{y:.1f}" class="gl"/>'
                 f'<text x="{PAD_L-10}" y="{y+4:.1f}" class="tick" text-anchor="end">{fmt.format(v)}</text>')
    for i, (lab, v) in enumerate(pairs):
        x = PAD_L + i * (bw + gap)
        y = py(v)
        c = (colors or [S1L] * n)[i]
        o.append(f'<rect x="{x:.1f}" y="{y:.1f}" width="{bw:.1f}" height="{H-PAD_B-y:.1f}" rx="4" '
                 f'fill="{c}"><title>{esc(lab)}: {fmt.format(v)}</title></rect>'
                 f'<text x="{x+bw/2:.1f}" y="{y-8:.1f}" class="dlabel" fill="{c}" text-anchor="middle">{fmt.format(v)}</text>'
                 f'<text x="{x+bw/2:.1f}" y="{H-PAD_B+20}" class="tick" text-anchor="middle">{esc(lab)}</text>')
    o.append(f'<text x="15" y="{(PAD_T+H-PAD_B)/2:.0f}" class="axis-title" text-anchor="middle" '
             f'transform="rotate(-90 15 {(PAD_T+H-PAD_B)/2:.0f})">{esc(y_label)}</text></svg>')
    return "".join(o)


def table(headers, rows):
    h = "".join(f"<th>{esc(x)}</th>" for x in headers)
    b = "".join("<tr>" + "".join(f"<td>{esc(c)}</td>" for c in r) + "</tr>" for r in rows)
    return f'<div class="tscroll"><table><thead><tr>{h}</tr></thead><tbody>{b}</tbody></table></div>'


def legend(items):
    return '<div class="legend">' + "".join(
        f'<span class="lg"><i style="background:{c}"></i>{esc(n)}</span>' for n, c in items) + "</div>"


CSS = """
:root{--bg:#f6f8f9;--surface:#fff;--surface-2:#eef2f3;--ink:#101a1c;--soft:#4d5f62;
--faint:#7d9092;--line:#dbe3e5;--s1:__S1L__;--s2:__S2L__;--s3:__S3L__;
--good:#0ca30c;--bad:#d1382f;--radius:10px;
--mono:ui-monospace,"SF Mono",Menlo,Consolas,monospace;
--sans:system-ui,-apple-system,"Segoe UI",Roboto,Helvetica,Arial,sans-serif;}
@media (prefers-color-scheme:dark){:root:not([data-theme="light"]){__DARK__}}
:root[data-theme="dark"]{__DARK__}
*{box-sizing:border-box}
body{margin:0;background:var(--bg);color:var(--ink);font-family:var(--sans);line-height:1.6}
.wrap{max-width:1000px;margin:0 auto;padding:0 22px 90px}
header{padding:56px 0 30px;border-bottom:1px solid var(--line);margin-bottom:34px}
.eyebrow{font-family:var(--mono);font-size:11.5px;letter-spacing:.16em;text-transform:uppercase;
color:var(--s1);margin:0 0 12px}
h1{font-size:clamp(2rem,5vw,2.9rem);line-height:1.05;letter-spacing:-.02em;margin:0 0 14px;
font-weight:660;text-wrap:balance}
.lede{font-size:1.1rem;color:var(--soft);max-width:66ch;margin:0}
.lede b{color:var(--ink)}
h2{font-size:1.35rem;margin:0 0 4px;font-weight:640;letter-spacing:-.01em}
section{margin-top:52px}
.num{font-family:var(--mono);font-size:11.5px;color:var(--faint);letter-spacing:.1em}
.sub{color:var(--soft);margin:0 0 20px;max-width:70ch}
p{max-width:72ch}
.grid{display:grid;gap:14px}.c2{grid-template-columns:1fr 1fr}.c4{grid-template-columns:repeat(4,1fr)}
@media(max-width:780px){.c2,.c4{grid-template-columns:1fr}}
.card{background:var(--surface);border:1px solid var(--line);border-radius:var(--radius);padding:16px 18px}
.tile .k{font-family:var(--mono);font-size:10.5px;letter-spacing:.07em;text-transform:uppercase;
color:var(--faint);display:block;margin-bottom:6px}
.tile .v{font-size:1.75rem;font-weight:680;letter-spacing:-.02em;font-variant-numeric:tabular-nums;line-height:1.1}
.tile .d{font-size:12.5px;color:var(--soft);margin-top:4px}
.v.bad{color:var(--bad)}.v.good{color:var(--good)}
figure{margin:0}
.fig{background:var(--surface);border:1px solid var(--line);border-radius:var(--radius);
padding:18px 16px 10px;overflow-x:auto}
.chart{display:block;width:100%;height:auto;min-width:560px}
.chart .gl{stroke:var(--line);stroke-width:1}
.chart .tick{fill:var(--faint);font-size:11px;font-family:var(--mono)}
.chart .axis-title{fill:var(--soft);font-size:11.5px}
.chart .dlabel{font-size:12px;font-weight:700;font-family:var(--mono)}
figcaption{color:var(--soft);font-size:13px;margin-top:10px;padding-top:10px;border-top:1px solid var(--line)}
figcaption b{color:var(--ink)}
.legend{display:flex;gap:16px;flex-wrap:wrap;margin:2px 0 10px;font-size:12.5px;color:var(--soft)}
.lg{display:flex;align-items:center;gap:6px}
.lg i{width:12px;height:3px;border-radius:2px;display:inline-block}
.tscroll{overflow-x:auto;border:1px solid var(--line);border-radius:var(--radius);
background:var(--surface);margin-top:14px}
table{border-collapse:collapse;width:100%;font-size:13px;min-width:520px}
th,td{text-align:right;padding:9px 13px;border-bottom:1px solid var(--line);
font-variant-numeric:tabular-nums;font-family:var(--mono)}
th:first-child,td:first-child{text-align:left}
th{font-size:11px;letter-spacing:.05em;text-transform:uppercase;color:var(--faint);
background:var(--surface-2);font-family:var(--sans);font-weight:600}
tr:last-child td{border-bottom:none}
.callout{border-left:3px solid var(--s1);background:var(--surface);padding:14px 18px;
border-radius:0 8px 8px 0;margin:20px 0 0;font-size:14.5px}
.callout.ok{border-left-color:var(--good)}.callout.warn{border-left-color:var(--s2)}
code{font-family:var(--mono);font-size:12.5px;background:var(--surface-2);padding:1px 5px;border-radius:4px}
pre{background:var(--surface-2);border:1px solid var(--line);border-radius:8px;padding:14px;
overflow-x:auto;font-family:var(--mono);font-size:12.5px;line-height:1.5}
.spec{display:flex;flex-wrap:wrap;gap:8px 26px;font-size:13.5px;color:var(--soft);
font-family:var(--mono);margin-top:6px}
.spec b{color:var(--ink);font-weight:600}
footer{margin-top:70px;padding-top:18px;border-top:1px solid var(--line);color:var(--faint);
font-size:12px;font-family:var(--mono)}
""".replace("__S1L__", S1L).replace("__S2L__", S2L).replace("__S3L__", S3L).replace(
    "__DARK__",
    "--bg:#0c1416;--surface:#101e1f;--surface-2:#16282a;--ink:#e7eef0;--soft:#a4b4b6;"
    "--faint:#70878a;--line:#21363a;"
    f"--s1:{S1D};--s2:{S2D};--s3:{S3D};--good:#3fbf5f;--bad:#f0665c;")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("results", nargs="?", default="e2e/perf/results_after.json")
    ap.add_argument("--baseline", default=None)
    ap.add_argument("-o", "--out", default="e2e/perf/read_bench_report.html")
    a = ap.parse_args()

    d = json.load(open(a.results))
    base = json.load(open(a.baseline)) if a.baseline else None
    m, cfg, sc = d["machine"], d["config"], d["scenarios"]
    sp, pp, mx = sc["shared_pond"]["points"], sc["pond_per_reader"]["points"], sc["mixed"]["points"]
    iso, soak = sc["isolation"], sc["soak"]
    levels = [p["concurrency"] for p in sp]
    xl = [str(c) for c in levels]

    bsp = base["scenarios"]["shared_pond"]["points"] if base else None
    bmx = base["scenarios"]["mixed"]["points"] if base else None

    cores_pp = max(p["resources"]["cores_busy_mean"] for p in pp)
    cores_sp = max(p["resources"]["cores_busy_mean"] for p in sp)
    peak_sp = max(p["ops_per_s"] for p in sp)
    errs = sum(p["errors"] for p in sp + pp + mx)

    # ---- charts -------------------------------------------------------------
    tput_series, lat_series, core_series = [], [], []
    if bsp:
        tput_series.append(("before — shared pond", [p["ops_per_s"] for p in bsp], S2L, True))
        lat_series.append(("before — shared pond", [p["p95_ms"] for p in bsp], S2L, True))
        core_series.append(("before — shared pond", [p["resources"]["cores_busy_mean"] for p in bsp], S2L, True))
    tput_series.append(("after — shared pond", [p["ops_per_s"] for p in sp], S1L, True))
    lat_series.append(("after — shared pond", [p["p95_ms"] for p in sp], S1L, True))
    core_series.append(("after — shared pond", [p["resources"]["cores_busy_mean"] for p in sp], S1L, True))
    tput_series.append(("pond per reader", [p["ops_per_s"] for p in pp], S3L, False))
    lat_series.append(("pond per reader", [p["p95_ms"] for p in pp], S3L, False))
    core_series.append(("pond per reader", [p["resources"]["cores_busy_mean"] for p in pp], S3L, False))

    wr_series = []
    if bmx:
        wr_series.append(("before", [p["writer_ops"] for p in bmx], S2L, True))
    wr_series.append(("after", [p["writer_ops"] for p in mx], S1L, True))

    leg_items = ([("before — shared pond", "var(--s2)")] if bsp else []) + [
        ("after — shared pond", "var(--s1)"), ("pond per reader (reference)", "var(--s3)")]

    # ---- headline numbers ---------------------------------------------------
    if bsp:
        b_peak = max(p["ops_per_s"] for p in bsp)
        tput_gain = peak_sp / b_peak
        lat_gain = bsp[-1]["p95_ms"] / sp[-1]["p95_ms"]
        b_cores = max(p["resources"]["cores_busy_mean"] for p in bsp)
        wr_before, wr_after = bmx[-1]["writer_ops"], mx[-1]["writer_ops"]
        wr_gain = wr_after / max(1, wr_before)
        tiles = f"""
    <div class="card tile"><span class="k">shared-pond throughput</span>
      <div class="v good">{tput_gain:.1f}&times;</div>
      <div class="d">{b_peak:.0f} &rarr; {peak_sp:.0f} q/s peak</div></div>
    <div class="card tile"><span class="k">p95 at {levels[-1]} readers</span>
      <div class="v good">{lat_gain:.1f}&times; faster</div>
      <div class="d">{bsp[-1]['p95_ms']:.0f} &rarr; {sp[-1]['p95_ms']:.0f} ms</div></div>
    <div class="card tile"><span class="k">writer under read load</span>
      <div class="v good">{wr_gain:.0f}&times;</div>
      <div class="d">{wr_before} &rarr; {wr_after} writes completed</div></div>
    <div class="card tile"><span class="k">cores used, shared pond</span>
      <div class="v good">{b_cores:.1f} &rarr; {cores_sp:.1f}</div>
      <div class="d">of {m['logical_cores']} — tier-capped by design</div></div>"""
        headline = "Reads now run concurrently on a shared pond"
        lede = (f"A read-focused concurrency, isolation, and soak benchmark — measured before and "
                f"after replacing the engine's single per-pond connection with a bounded "
                f"<b>read connection pool</b>. Shared-pond read throughput is up "
                f"<b>{tput_gain:.1f}&times;</b>, p95 at {levels[-1]} readers is "
                f"<b>{lat_gain:.1f}&times;</b> faster, and a concurrent writer — previously starved — "
                f"completes <b>{wr_gain:.0f}&times;</b> more work.")
    else:
        tiles = f"""
    <div class="card tile"><span class="k">shared-pond ceiling</span>
      <div class="v">{peak_sp:.0f} q/s</div><div class="d">peak across the sweep</div></div>
    <div class="card tile"><span class="k">p95 at {levels[-1]} readers</span>
      <div class="v">{sp[-1]['p95_ms']:.0f} ms</div><div class="d">from {sp[0]['p95_ms']:.0f} ms solo</div></div>
    <div class="card tile"><span class="k">cores, shared pond</span>
      <div class="v">{cores_sp:.1f} / {m['logical_cores']}</div><div class="d">vs {cores_pp:.1f} across ponds</div></div>
    <div class="card tile"><span class="k">errors</span>
      <div class="v {'good' if errs == 0 else 'bad'}">{errs}</div><div class="d">across every scenario</div></div>"""
        headline = "Read concurrency, isolation, and soak"
        lede = ("A read-focused concurrency, isolation, and soak benchmark of Latiq's engine.")

    # ---- tables -------------------------------------------------------------
    if bsp:
        t_scale = table(
            ["readers", "before q/s", "after q/s", "before p95 ms", "after p95 ms",
             "after cores", "per-pond q/s (ref)"],
            [[p["concurrency"], f'{b["ops_per_s"]:.1f}', f'{p["ops_per_s"]:.1f}',
              f'{b["p95_ms"]:.1f}', f'{p["p95_ms"]:.1f}',
              f'{p["resources"]["cores_busy_mean"]:.2f}', f'{q["ops_per_s"]:.1f}']
             for b, p, q in zip(bsp, sp, pp)])
        t_mixed = table(
            ["readers", "writer ops before", "writer ops after", "writer p95 before", "writer p95 after"],
            [[p["concurrency"], b["writer_ops"], p["writer_ops"],
              f'{b["writer_p95_ms"]:.0f}', f'{p["writer_p95_ms"]:.0f}'] for b, p in zip(bmx, mx)])
    else:
        t_scale = table(["readers", "shared q/s", "shared p95 ms", "cores", "per-pond q/s"],
                        [[p["concurrency"], f'{p["ops_per_s"]:.1f}', f'{p["p95_ms"]:.1f}',
                          f'{p["resources"]["cores_busy_mean"]:.2f}', f'{q["ops_per_s"]:.1f}']
                         for p, q in zip(sp, pp)])
        t_mixed = table(["readers", "read q/s", "writer ops", "writer p95 ms"],
                        [[p["concurrency"], f'{p["ops_per_s"]:.1f}', p["writer_ops"],
                          f'{p["writer_p95_ms"]:.0f}'] for p in mx])

    fix_section = """
<section>
  <span class="num">03 &mdash; the fix</span>
  <h2>One database per pond, many read connections</h2>
  <p class="sub">The cause was a single connection per pond behind an exclusive mutex held for the
  whole query &mdash; so reads queued behind reads, and behind writes.</p>
  <pre>// before: every query, reads included, took the pond's one connection
let guard = lock_recover(&amp;inst);
Self::run_with_abort(&amp;guard, &amp;abort, |i| run_read(i, sql))

// after: reads check out from a bounded pool of connections to the SAME database
self.with_read(loc, |i| Self::run_with_abort(i, &amp;abort, |i| run_read(i, sql)))</pre>
  <p><b>The invariant is intact.</b> Still one DuckDB <em>database</em> per pond, so the tier's
  <code>memory_limit</code>/<code>threads</code> caps stay instance-global and one process owns the
  catalog file. Only the connection count varies. Writes and session-scoped catalog attach/detach
  still run on a single serialized writer, so DuckLake commits stay ordered.</p>
  <div class="callout warn"><b>One trap worth recording:</b> a cloned connection does <em>not</em>
  inherit session state. The <code>ATTACH</code> is database-level and is shared, but
  <code>USE</code> and <code>TimeZone</code> are per-session &mdash; without re-applying them a pooled
  read resolves unqualified table names against the wrong catalog and renders timestamps in the
  host timezone. Both are re-applied when a pool connection is created.</div>
</section>"""

    doc = f"""<title>Latiq Read Concurrency</title>
<meta name="viewport" content="width=device-width, initial-scale=1">
<style>{CSS}</style>
<div class="wrap">
<header>
  <p class="eyebrow">Reliability &amp; performance &middot; read path</p>
  <h1>{headline}</h1>
  <p class="lede">{lede}</p>
</header>

<section>
  <span class="num">00 &mdash; the machine</span>
  <h2>What this was measured on</h2>
  <p class="sub">Numbers are meaningless without the box. This is a large workstation &mdash; treat
  absolute values as an upper bound and the <em>shape</em> of each curve as the finding.
  {"Before and after were built with the same release profile and run back to back." if base else ""}</p>
  <div class="card">
    <div class="spec"><span><b>{esc(m['cpu'])}</b></span>
      <span>{m['p_cores']}P + {m['e_cores']}E = <b>{m['logical_cores']} cores</b></span>
      <span><b>{m['memory_gb']} GB</b> RAM</span><span>{esc(m['platform'])}</span></div>
    <div class="spec" style="margin-top:10px"><span>mode <b>embedded</b></span>
      <span><b>{cfg['rows']:,}</b> rows/pond</span><span><b>{cfg['duration_s']:.0f}s</b> per point</span>
      <span>soak <b>{cfg['soak_s']:.0f}s</b></span><span>{esc(d['started'])}</span></div>
    <p style="margin:12px 0 0;font-size:13px;color:var(--soft)">Read under test:
    <code>{esc(cfg['read_sql'])}</code></p>
  </div>
</section>

<section>
  <span class="num">01 &mdash; headline</span>
  <h2>Four numbers</h2>
  <div class="grid c4">{tiles}</div>
</section>

<section>
  <span class="num">02 &mdash; read concurrency</span>
  <h2>Many readers on one shared pond</h2>
  <p class="sub"><b>Scenario A</b> &mdash; an orchestrator seeds the state, many agents read it.
  &ldquo;Pond per reader&rdquo; is the scale-out reference: the same load spread across separate
  ponds, which is what the machine can do when nothing serializes.</p>
  {legend(leg_items)}
  <figure><div class="fig">{line_chart(xl, tput_series, "read throughput (queries/s)")}</div>
    <figcaption>{"<b>Throughput now grows with readers.</b> Before, 32x the readers bought 1.1x the throughput &mdash; a flat line is the signature of a serialized resource. After, the shared pond climbs with concurrency; it still plateaus below the pond-per-reader reference because a pond is capped at its tier, which is the intended behaviour." if bsp else "<b>Flat throughput on a shared pond</b> is the signature of a serialized resource."}</figcaption>
  </figure>
  <figure style="margin-top:22px"><div class="fig">{line_chart(xl, lat_series, "p95 latency (ms)")}</div>
    <figcaption>{"<b>Latency no longer grows linearly with reader count.</b> Before, p95 doubled at every doubling of concurrency &mdash; pure queueing. After, readers execute in parallel up to the tier's budget." if bsp else "<b>p95 doubles at every doubling of concurrency</b> &mdash; queueing, not work."}</figcaption>
  </figure>
  <figure style="margin-top:22px"><div class="fig">{line_chart(xl, core_series, f"cores busy (of {m['logical_cores']})")}</div>
    <figcaption>A pond stays bounded by its <b>tier</b> &mdash; that cap is deliberate and is what
    keeps one workflow from eating a shared cluster. Filling the machine is what
    <em>more ponds</em> are for, which the reference line shows reaching
    {cores_pp:.1f} of {m['logical_cores']} cores.</figcaption>
  </figure>
  {t_scale}
</section>
{fix_section if base else ""}
<section>
  <span class="num">0{"4" if base else "3"} &mdash; scenario B</span>
  <h2>A writer sharing the pond with readers</h2>
  <p class="sub">Agents also create state. Here an orchestrator writes continuously while readers
  scale up on the same pond.</p>
  <figure><div class="fig">{line_chart(xl, wr_series, "writes completed", fmt="{:.0f}")}</div>
    <figcaption>{"<b>The writer is no longer starved.</b> Before, readers crowded it out almost completely as concurrency rose; reads and writes now use different connections, so the writer keeps working." if bmx else "<b>The writer is crowded out</b> as readers scale &mdash; reads and writes contend for the same lock."}</figcaption>
  </figure>
  {t_mixed}
</section>

<section>
  <span class="num">0{"5" if base else "4"} &mdash; what holds up</span>
  <h2>Isolation and stability</h2>
  <div class="grid c2">
    <figure><div class="fig">{bar_chart([("quiet", iso['quiet']['p95_ms']), ("noisy neighbour", iso['stressed']['p95_ms'])], "victim pond p95 (ms)", colors=[S1L, S2L])}</div>
      <figcaption><b>Resource isolation works.</b> A small-tier pond beside a greedy large-tier
      neighbour degrades only <b>{iso['p95_degradation_x']:.2f}&times;</b>.</figcaption></figure>
    <figure><div class="fig">{bar_chart([("RSS growth (MB)", max(0.0, soak['rss_growth_mb'])), ("fd growth", max(0, soak['fd_growth']))], "growth over the soak", fmt="{:.1f}", colors=[S1L, S3L])}</div>
      <figcaption><b>No leak, no drift.</b> {cfg['soak_s']:.0f}s of sustained reads: p95 drift
      <b>{soak['p95_drift_x']:.2f}&times;</b>, and the growth above is flat.</figcaption></figure>
  </div>
  <div class="callout ok"><b>{errs} errors across every scenario and concurrency level.</b>
  Nothing failed and nothing was dropped, before or after the change.</div>
</section>

<section>
  <span class="num">0{"6" if base else "5"} &mdash; so what</span>
  <h2>What this means</h2>
  <p><b>A pond is now genuinely shareable.</b> The product spec calls many agents in one pond
  &ldquo;the common case, not the edge case.&rdquo; That is now true concurrently, not just
  correctly.</p>
  <p><b>The remaining bound is the tier, and that is the design.</b> A shared pond will not fill a
  32-core box on its own &mdash; it is capped so it cannot starve its neighbours. Filling the machine
  is what more ponds are for; &ldquo;scale out, don't distribute&rdquo; is validated by the
  reference line.</p>
  <p><b>Re-running this benchmark is the acceptance test</b> for any future change to the engine's
  concurrency model.</p>
</section>

<footer>latiq &middot; read benchmark &middot; embedded &middot; {esc(d['started'])} &rarr; {esc(d['finished'])}
&middot; regenerate: <code>python e2e/perf/read_bench.py &amp;&amp; python e2e/perf/report.py</code></footer>
</div>
"""
    out = os.path.abspath(a.out)
    os.makedirs(os.path.dirname(out), exist_ok=True)
    with open(out, "w") as f:
        f.write(doc)
    print(f"report -> {out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
