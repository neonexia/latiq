# latiq-engine-duckdb — CLAUDE.md

The DuckDB + DuckLake implementation of `QueryEngine`.

## Invariants
- **One DuckDB instance per pond** — mutex-guarded, reused across queries (cached in `DuckEngine.instances` keyed by `catalog_uri`). NEVER revert to instance-per-query: independent instances racing on one DuckLake catalog file lose concurrent writes. (See the concurrency test.)
- **Pure DuckLake — nothing on top.** Attribution = native `CALL pond.set_commit_message(...)`. `_latiq` = read-only views over `pond.snapshots()` / DuckDB catalog. No shadow store.
- **Write path must isolate user SQL from our framing.** Don't build a single string batch of `BEGIN; <user sql>; CALL set_commit_message; COMMIT` — user comments/`;` can comment out the COMMIT/attribution or inject statements. Execute user SQL as its own statement, then our `set_commit_message`, then COMMIT, and ROLLBACK on error. Our attribution call must be last so it can't be forged.
- **Cancellation = `Connection::interrupt_handle()`** from a watcher thread; on abort, interrupt + (for safety) discard/reset so a stale interrupt can't bleed into the next query on the reused connection. `abort` must release engine resources promptly (the cancellation test is the gate).
- **`drop_pond` must evict the cached instance** (close the connection) — or it leaks fds/connections to deleted catalog files.
- **Read/write/explain guards are ours.** `read_query` must reject writes (incl. `WITH … INSERT`); `_latiq` writes are blocked; `explain_query` must NOT execute (`EXPLAIN ANALYZE` runs the statement — reject it). DuckDB exposes no statement-type introspection, so these are careful heuristics — test the edge cases.
- **Cell conversion is ours:** `cell_to_json` must not silently truncate (e.g. `HugeInt`/i128 → encode as string when outside i64 range, never `as i64`).

## Testing philosophy
**Do not test DuckDB.** It's a production engine. Test *our* integration only: the guards, attribution plumbing, cell conversion, cancellation + resource release, concurrency correctness, and the `_latiq` views. Unit + `tests/engine_e2e.rs`.
