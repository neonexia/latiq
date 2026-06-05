# M1 De-risk Spike Findings

**Date:** 2026-06-04
**Branch:** slice0-m1-m2
**Author:** Latiq Dev (spike)

This document records findings from the M1 de-risk SPIKE probes against real resolved crate versions. The goal is to discover working API syntax, identify blockers, and inform the M3+ design decisions.

---

## Resolved Crate Versions

| Crate | Version | Notes |
|---|---|---|
| `duckdb` | `1.10503.1` | `bundled` feature; wraps DuckDB bundled C++ |
| `rmcp` | `1.7.0` | MCP Streamable-HTTP server |
| `tokio` | `1.x` | `full` |
| `axum` | `0.8` | Required for rmcp HTTP server (dev dep pattern from rmcp tests) |
| `anyhow` | `1.x` | |
| `serde_json` | `1.x` | |

### rmcp 1.7.0 Actual Feature Map

The `server` feature alone is insufficient for HTTP transport. Required features:

```toml
rmcp = { version = "1.7.0", features = ["server", "transport-streamable-http-server"] }
```

Feature breakdown:
- `server` = `transport-async-rw` + `schemars` + `pastey` (macros) — bare server trait + tool macros
- `transport-streamable-http-server` = `server-side-http` + `transport-worker` + `session` types
- `server-side-http` pulls in: `uuid`, `rand`, `tokio-stream`, `http`, `http-body`, `http-body-util`, `bytes`, `sse-stream`, `tower`

**Must also add `axum = "0.8"` as a direct dependency** — rmcp's `StreamableHttpService` implements `tower::Service` and plugs into an axum `Router` via `nest_service`. rmcp does NOT bundle axum.

---

## Probe A — DuckLake round-trip (duckdb-rs)

**Status: CONFIRMED**

### Working ATTACH syntax

```sql
ATTACH 'ducklake:duckdb:/path/to/catalog.duckdb' AS pond (DATA_PATH '/path/to/data');
```

- The `ducklake:duckdb:` prefix is correct.
- `DATA_PATH` is the directory where Parquet files are written.
- `AS pond` creates the database alias used in all subsequent table references.

### Network requirement

`INSTALL ducklake;` requires network to the DuckDB extension repository (`extensions.duckdb.org`). The install succeeded in this environment. If network is unavailable, extensions must be pre-staged or `SET custom_extension_repository` used.

### Full working code

```rust
use duckdb::Connection;

let dir = std::env::temp_dir().join("latiq-spike-a");
std::fs::remove_dir_all(&dir).ok(); // clean any prior state
std::fs::create_dir_all(dir.join("data"))?;
let catalog = dir.join("catalog.duckdb");
let data = dir.join("data");

let conn = Connection::open_in_memory()?;
conn.execute_batch("INSTALL ducklake; LOAD ducklake;")?;
conn.execute_batch(&format!(
    "ATTACH 'ducklake:duckdb:{}' AS pond (DATA_PATH '{}');",
    catalog.display(), data.display()
))?;
conn.execute_batch("CREATE TABLE pond.events(id INTEGER, sev VARCHAR);")?;
conn.execute_batch("INSERT INTO pond.events VALUES (1,'high'),(2,'low');")?;
let n: i64 = conn.query_row("SELECT count(*) FROM pond.events", [], |r| r.get(0))?;
assert_eq!(n, 2);
```

### Build time

First build: ~42s on M1 (duckdb was already cached in the workspace; fresh machine would be 10-20 min).

---

## Probe B — Native attribution (set_commit_message)

**Status: CONFIRMED — attribution works natively**

### set_commit_message call syntax

The pond-qualified `CALL pond.set_commit_message(...)` form works. Parameters:
1. `author` (VARCHAR): the identity string to attribute
2. `operation` (VARCHAR): e.g., `'write_query'`
3. `extra_info =>` (named param, VARCHAR): JSON string

```sql
BEGIN;
  INSERT INTO pond.events VALUES (3,'critical');
  CALL pond.set_commit_message('agent-spike', 'write_query', extra_info => '{"verified":false}');
COMMIT;
```

### Snapshot query — both forms work

Both `pond.snapshots()` and `ducklake_snapshots('pond')` work and return identical columns:

```
columns: ["snapshot_id", "author", "commit_message"]
```

**Critical duckdb-rs API note:** `Statement::column_names()` panics if called before `query_map()` or `query_row()` executes the statement (the schema field is None until execution). Always call `column_names()` via `r.as_ref().column_names()` from inside a row closure, or after executing.

### Attribution confirmed

The `author` column is populated with `"agent-spike"` exactly as passed to `set_commit_message`. The earlier snapshots (from CREATE TABLE and INSERT before the attributed write) have `author = None`.

```
snapshot: id=0, author=None, message=None          (CREATE TABLE snapshot)
snapshot: id=1, author=None, message=None          (first INSERT snapshot)
snapshot: id=2, author=None, message=None          (structural snapshot)
snapshot: id=3, author=Some("agent-spike"), message=Some("write_query")
  --> ATTRIBUTION CONFIRMED
```

### Design implication

Native DuckLake attribution via `set_commit_message` is fully functional. The §5 write path design (transaction-wrap + set_commit_message) is confirmed correct. **No design change needed.**

---

## Probe C — rmcp Streamable-HTTP server, JSON tool call

**Status: CONFIRMED**

### Server construction pattern

```rust
use rmcp::{
    ServerHandler,
    handler::server::{router::tool::ToolRouter, wrapper::Parameters},
    model::{ServerCapabilities, ServerInfo},
    schemars, tool, tool_handler, tool_router,
    transport::streamable_http_server::{
        StreamableHttpServerConfig, StreamableHttpService, session::local::LocalSessionManager,
    },
};
use tokio_util::sync::CancellationToken;

#[derive(Debug, Clone)]
struct EchoServer { tool_router: ToolRouter<Self> }

impl EchoServer { fn new() -> Self { Self { tool_router: Self::tool_router() } } }

#[tool_router]
impl EchoServer {
    #[tool(description = "Echo a message back")]
    fn echo(&self, Parameters(EchoRequest { message }): Parameters<EchoRequest>) -> String {
        format!("echo: {message}")
    }
}

#[tool_handler(router = self.tool_router)]
impl ServerHandler for EchoServer {
    fn get_info(&self) -> ServerInfo {
        ServerInfo::new(ServerCapabilities::builder().enable_tools().build())
    }
}

// Wire into axum:
let ct = CancellationToken::new();
let service: StreamableHttpService<EchoServer, LocalSessionManager> =
    StreamableHttpService::new(
        || Ok(EchoServer::new()),
        Default::default(),
        StreamableHttpServerConfig::default()
            .with_cancellation_token(ct.child_token())
            .with_stateful_mode(true),
    );
let router = axum::Router::new().nest_service("/mcp", service);
let listener = tokio::net::TcpListener::bind("127.0.0.1:8888").await?;
axum::serve(listener, router)
    .with_graceful_shutdown(async move { ct.cancelled_owned().await })
    .await?;
```

### Endpoint and headers

- **Endpoint:** `POST /mcp` (the path you pass to `nest_service`)
- **Required request headers:**
  - `Content-Type: application/json`
  - `Accept: application/json, text/event-stream`
  - `mcp-session-id: <uuid>` (from initialize response header, for subsequent calls)
- **Response headers:**
  - `content-type: text/event-stream` (default stateful mode)
  - `mcp-session-id: <uuid>` (returned on initialize)

### Initialize request/response

Request:
```json
{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-03-26","capabilities":{},"clientInfo":{"name":"test","version":"1.0"}}}
```

Response (SSE frame):
```
data: {"jsonrpc":"2.0","id":1,"result":{"protocolVersion":"2025-03-26","capabilities":{"tools":{}},"serverInfo":{"name":"rmcp","version":"1.7.0"},"instructions":"..."}}
```

### Tool call request/response

Request:
```json
{"jsonrpc":"2.0","id":2,"method":"tools/call","params":{"name":"echo","arguments":{"message":"hello world"}}}
```

Response (SSE frame):
```
data: {"jsonrpc":"2.0","id":2,"result":{"content":[{"type":"text","text":"echo: hello world"}],"isError":false}}
```

### CallToolResult structure

Handler return type `String` becomes `content[0].text` automatically. `isError: false` is set by the macro. To return an error: return `Err(rmcp::ErrorData::...)` from an `async fn -> Result<String, rmcp::ErrorData>` handler.

`structuredContent` is NOT populated automatically from a `String` return — for the dual-encode pattern (§8 result encoding) the handler must explicitly set it.

---

## Probe D — SSE response + progress notification

**Status: CONFIRMED (progress works; disconnect NOT auto-detected)**

### Progress notification pattern

```rust
#[tool_router]
impl EchoServer {
    #[tool(description = "Echo with progress")]
    async fn slow_echo(
        &self,
        meta: Meta,                          // inject for progress token
        client: Peer<RoleServer>,            // inject for notify_progress
        Parameters(EchoRequest { message }): Parameters<EchoRequest>,
    ) -> Result<String, rmcp::ErrorData> {
        let progress_token = meta.get_progress_token();
        for step in 0..3u32 {
            if let Some(ref token) = progress_token {
                let _ = client.notify_progress(ProgressNotificationParam {
                    progress_token: token.clone(),
                    progress: step as f64,
                    total: Some(3.0),
                    message: Some(format!("step {step}/3")),
                }).await;
            }
            tokio::time::sleep(Duration::from_millis(300)).await;
        }
        Ok(format!("slow_echo: {message}"))
    }
}
```

Client must send `_meta.progressToken` in the tool call params for notifications to flow:
```json
{"jsonrpc":"2.0","id":3,"method":"tools/call","params":{"name":"slow_echo","arguments":{"message":"hi"},"_meta":{"progressToken":"tok1"}}}
```

### Observed SSE frames

```
data: (empty keepalive)
id: 0/0
retry: 3000

data: {"jsonrpc":"2.0","method":"notifications/progress","params":{"message":"step 0/3","progress":0.0,"progressToken":"tok1","total":3.0}}
id: 1/0

data: {"jsonrpc":"2.0","method":"notifications/progress","params":{"message":"step 1/3","progress":1.0,"progressToken":"tok1","total":3.0}}
id: 2/0

data: {"jsonrpc":"2.0","method":"notifications/progress","params":{"message":"step 2/3","progress":2.0,"progressToken":"tok1","total":3.0}}
id: 3/0

data: {"jsonrpc":"2.0","id":3,"result":{"content":[{"type":"text","text":"slow_echo: probe_d_test"}],"isError":false}}
id: 4/0
```

### Disconnect detection

**Result: NOT automatic.** When curl is killed mid-stream (via `--max-time`), the server continues running and the handler continues executing (sleeping, sending progress). The server does NOT automatically detect the client disconnect and propagate it to the handler.

**Implication for §6 cancellation:** The "client disconnect → abort" path described in §7 ("detected via SSE write failure") is NOT provided out-of-the-box by rmcp 1.7.0. The `notify_progress` call returns `Ok(())` even after client disconnect (buffered/dropped). The abort must be triggered by a separate `notifications/cancelled` POST from the client, or by an external mechanism.

**Design recommendation:** For the disconnect→abort path, Latiq needs either:
1. Wrap `notify_progress` calls to detect `Err` (connection lost) and trigger abort — but rmcp may buffer and not immediately fail.
2. Rely exclusively on `notifications/cancelled` POST from the MCP client (spec-compliant).
3. Add a heartbeat ping loop alongside the query that detects write failure and triggers abort.

This is a **design-affecting finding** (affects §6 + §7).

---

## Probe E — Query cancellation (Connection::interrupt_handle)

**Status: CONFIRMED**

### Real API

`Connection` is NOT `Send` (uses `RefCell` internally). The interrupt API is:

```rust
let conn = Connection::open_in_memory()?;

// Get interrupt handle BEFORE moving conn into thread
// Returns Arc<InterruptHandle> which IS Send + Sync
let interrupt_handle = conn.interrupt_handle();

// Move conn into query thread
let handle = std::thread::spawn(move || {
    conn.execute_batch("SELECT count(*) FROM range(100000000000) t1, range(1000) t2;")
});

std::thread::sleep(Duration::from_millis(200));

// Interrupt from any thread/async context
interrupt_handle.interrupt();

let res = handle.join().unwrap();
assert!(res.is_err()); // Error message: "INTERRUPT Error: Interrupted!"
```

**NOT** `conn.interrupt()` — the starter code was incorrect. The real method is `conn.interrupt_handle()` returning `Arc<InterruptHandle>`, then `interrupt_handle.interrupt()`.

### Observed abort latency

```
elapsed=206.829916ms err=true
error=INTERRUPT Error: Interrupted!
```

The 200ms was the initial sleep before interrupt was called. **Actual abort latency after `interrupt()` call: effectively instant (<10ms)**. Well within the bounded-window contract.

### Design implication

The §6 DuckDB adapter plan (`Connection::interrupt()` from control thread) must use `connection.interrupt_handle()` to get a shareable `Arc<InterruptHandle>`, then call `.interrupt()`. The `Connection` itself cannot be shared between threads — **one connection per query thread, hold the `interrupt_handle` in the in-flight registry**.

The pool "discard after abort" strategy in §6 is correct: after interrupt, drop the connection and re-create lazily.

---

## API Surprises Summary

| Surprise | Impact |
|---|---|
| rmcp `server` feature alone is insufficient — must add `transport-streamable-http-server` | Low: just add the feature |
| Must add `axum = "0.8"` as a direct dep (rmcp does not re-export it) | Low: straightforward |
| `duckdb::Statement::column_names()` panics before query execution | Low: use `r.as_ref().column_names()` inside row closure |
| `Connection` is NOT `Send` — cannot share across threads | Medium: interrupt pattern changes (see Probe E) |
| `interrupt()` is on `Arc<InterruptHandle>` returned by `interrupt_handle()`, not on `Connection` directly | Medium: design §6 must be updated to reflect this |
| rmcp disconnect NOT auto-detected by handler | **High: design §6/§7 affected — disconnect→abort requires explicit mechanism** |
| `structuredContent` not auto-populated from String return | Low: must be set explicitly for dual-encode §8 pattern |

---

## Decisions for M3+

### §5 Storage & Attribution — NO CHANGE NEEDED

- DuckLake `set_commit_message` attribution works exactly as specced.
- Both `pond.snapshots()` and `ducklake_snapshots('pond')` return `snapshot_id`, `author`, `commit_message`.
- The `author` column is populated with the string passed as first arg to `set_commit_message`.
- ATTACH syntax confirmed: `ATTACH 'ducklake:duckdb:<path>' AS pond (DATA_PATH '<dir>');`
- The per-pond on-disk layout and write-path-wraps-in-transaction design are correct.

### §6 Cancellation — MINOR IMPL CHANGE

The abort mechanism is confirmed working but the API differs from the spec description:

- **Change:** `Connection::interrupt()` does not exist. Use `conn.interrupt_handle()` → `Arc<InterruptHandle>` → store in in-flight registry → call `.interrupt()` to abort.
- **Change:** `Connection` is not `Send`. Each query runs on a dedicated thread (not shared). Pool strategy: hold `interrupt_handle` per active query; on abort, call `.interrupt()` + discard the connection thread.
- **No design-level change** — the abort-on-cancel strategy is sound, just the implementation detail of *how* to call interrupt.

### §6/§7 Disconnect→Abort — DESIGN GAP, NEEDS RESOLUTION

rmcp 1.7.0 does NOT propagate client disconnects to the handler. The spec describes "client disconnect (detected via SSE write failure)" as a cancel source. This is NOT automatic.

**Options (pick before M3+ implementation):**

1. **Rely on `notifications/cancelled`** (spec-compliant, client must send it): simplest; works for well-behaved MCP clients. Does not handle ungraceful client crashes.
2. **Heartbeat ping in the query wrapper**: alongside each query, spawn a task that sends periodic keepalive pings; detect `Err` from `notify_progress` or a write probe and trigger abort. More robust but adds complexity.
3. **Axum connection-close detection**: intercept at the tower/axum layer to detect TCP close and signal abort via a `CancellationToken`. Most reliable but couples to transport layer.

**Recommendation:** Ship option 1 (notifications/cancelled only) for Slice 0+, add option 3 as a follow-up. Update §7 to note this limitation.

### §7 Transport — MINOR CLARIFICATION

- The default rmcp stateful-mode response is `text/event-stream` (SSE) for ALL requests.
- JSON response mode (`with_json_response(true)`) only works in stateless mode.
- For the spec's "Single-JSON for explain/lifecycle tools" design: rmcp wraps even fast synchronous responses in SSE frames. This is fine — the client still gets a single result, it's just SSE-framed. No behavioral change for agents.
- `structuredContent` must be set explicitly by handlers — does not auto-populate from `String` return. For the dual-encode pattern (§8), handlers must build both `content[0].text` and `structuredContent` manually.

### Inference for testing strategy

The `Connection` not being `Send` means integration tests cannot easily share a DuckDB connection across test tasks. Each test should own its connection. The interrupt handle pattern makes cancellation testable: allocate a `Connection`, get its `interrupt_handle`, move to a background thread, interrupt from the test, verify error.

---

## Appendix: Exact Working Code Snippets

### Probe A — ATTACH (working)
```rust
conn.execute_batch("INSTALL ducklake; LOAD ducklake;")?;
conn.execute_batch(&format!(
    "ATTACH 'ducklake:duckdb:{}' AS pond (DATA_PATH '{}');",
    catalog_path.display(), data_dir.display()
))?;
```

### Probe B — set_commit_message (working)
```sql
BEGIN;
  INSERT INTO pond.events VALUES (3,'critical');
  CALL pond.set_commit_message('agent-spike', 'write_query', extra_info => '{"verified":false}');
COMMIT;
```
Snapshot query: `SELECT snapshot_id, author, commit_message FROM pond.snapshots()`

### Probe C — rmcp server (working, minimal)
See Probe C section above.

### Probe E — interrupt (working)
```rust
let conn = Connection::open_in_memory()?;
let interrupt_handle = conn.interrupt_handle(); // Arc<InterruptHandle> — get BEFORE moving conn
let thread = std::thread::spawn(move || conn.execute_batch("SELECT ...heavy..."));
std::thread::sleep(Duration::from_millis(200));
interrupt_handle.interrupt(); // Works from any thread
let result = thread.join().unwrap();
assert!(result.is_err()); // "INTERRUPT Error: Interrupted!"
```
