# latiq-engine-duckdb — CLAUDE.md

The DuckDB + DuckLake implementation of `QueryEngine`.

## Invariants
- **One DuckDB instance per pond** — mutex-guarded, reused across queries (cached in `DuckEngine.instances` keyed by `catalog_uri`). It's the unit of **resource isolation** (per-pond memory/CPU caps belong on the instance — DuckDB's `memory_limit`/`threads` are instance-global) and of concurrency ownership (one process owns each catalog file; independent instances racing on one catalog lose writes). NEVER revert to instance-per-query. (See the concurrency test.)
- **Pure DuckLake — nothing on top.** Attribution = native `CALL pond.set_commit_message(...)`. Callers read history via native `pond.snapshots()` and tables/columns via `SHOW TABLES`/`information_schema`. **Create NO Latiq objects in the pond catalog** — no `_latiq` schema, views, or macros — and no shadow store. (`describe_schema` may use `duckdb_tables()` *internally* — that's the DuckDB adapter's own introspection, not an object in the pond.)
- **Write path must isolate user SQL from our framing.** Don't build a single string batch of `BEGIN; <user sql>; CALL set_commit_message; COMMIT` — user comments/`;` can comment out the COMMIT/attribution or inject statements. Execute user SQL as its own statement, then our `set_commit_message`, then COMMIT, and ROLLBACK on error. Our attribution call must be last so it can't be forged.
- **Cancellation = `Connection::interrupt_handle()`** from a watcher thread; on abort, interrupt + (for safety) discard/reset so a stale interrupt can't bleed into the next query on the reused connection. `abort` must release engine resources promptly (the cancellation test is the gate).
- **`drop_pond` must evict the cached instance** (close the connection) — or it leaks fds/connections to deleted catalog files.
- **Read/write/explain guards are ours.** `read_query` must reject writes (incl. `WITH … INSERT`); `explain_query` must NOT execute (`EXPLAIN ANALYZE` runs the statement — reject it). DuckDB exposes no statement-type introspection, so these are careful heuristics — test the edge cases.
- **Cell conversion is ours:** `cell_to_json` must not silently truncate (e.g. `HugeInt`/i128 → encode as string when outside i64 range, never `as i64`).

## Testing philosophy
**Do not test DuckDB.** It's a production engine. Test *our* integration only: the guards, attribution plumbing (native `pond.snapshots()`), cell conversion, cancellation + resource release, and concurrency correctness. Unit + `tests/engine_e2e.rs`.
