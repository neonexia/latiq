# crates/latiq/tests — CLAUDE.md

Rules for **writing** tests, anywhere in the workspace. The root `CLAUDE.md`'s
*Test taxonomy* says where a test goes and how to run it; this says whether it
should exist and what it must assert. Every rule below is here because an audit
of ~294 Rust + ~30 e2e tests found the same mistake more than once — the reason
is part of the rule, because a rule whose reason is remembered gets followed.

## 1. Prove it at the cheapest layer that can prove it

The ladder — unit → crate integration → full-stack (`crates/latiq/tests/`) →
containerised (`e2e/`) — is a **cost ladder**. Climb only for wiring you cannot
otherwise reach.

- **Legitimate duplication:** the allocate/write/read loop exists at four layers
  and each adds a seam — storage+engine (`latiq-engine-duckdb`), `AgentOps`
  (`latiq-agent-core`), Data gRPC over a real control plane (`tests/query_grpc.rs`),
  MCP over rmcp (`tests/mcp.rs`). Same shape, four different things proven.
- **Waste:** four `latiq-auth/tests/metadata.rs` tests re-proved `encode_quoted`
  through a one-line wrapper, while the unit tests beside it in `src/metadata.rs`
  already asserted the same properties with exact values. An integration test that
  calls a pure function through one hop is a unit test with a slower build.

Ask what *wiring* the higher layer adds. If the answer is "none", write it lower.

## 2. Assert **why**, not **that**

A bare `is_err()` / `expect_err()` passes for reasons that have nothing to do with
the subject of the test. Real ones we shipped:

- `read_arrow(...).is_err()` — would have passed if the pond had vanished.
- `"external catalog must be detached after pull"` (now `admin.rs`'s `catalogs`
  module) — passed for *any* error, including one raised before the catalog was
  ever attached.
- a tier-alias test that would have passed if the alias were rejected as an
  *unknown tier* — the one outcome it exists to distinguish from the uncapped
  grant it is really about.

The pattern to copy is `assert_rejected_because` in
`crates/latiq-auth/tests/verify.rs`, plus that file's discipline: **every token is
wrong in exactly one way**, so a rejection can only be attributed to the check
under test. Positive tests get the same treatment — assert the value, not `is_ok()`.

## 3. A guard that counts must assert a lower bound

The sharpest finding. `auth_cli_clients_are_only_built_in_the_shared_helpers`
asserted `uses <= 1` — **so it passed when the count was zero**. If `main.rs` had
stopped building that client, or a helper were renamed so the literal vanished, it
would have stayed green while guarding nothing. It is now pinned to exactly one
and lives in `crates/latiq/src/main.rs`'s unit tests.

Any grep-based, macro-enumerated, or table-driven guard needs an **anti-vacuity**
assertion. Two that get it right:

- `crates/latiq-sdk/src/lib.rs`'s `client_construction` module — after asserting
  no unauthenticated constructor appears, it asserts `authed >= 1`.
- `tests/admin.rs::auth_admin_every_rpc_rejects_a_missing_token` — probes each RPC
  and then asserts `probed.len() == ADMIN_RPC_COUNT`, so a new RPC that nobody
  added to the list fails the build instead of being silently unprobed.

## 4. Blocklists are not guards; assert the positive property

An agent test asserted the 401 challenge contained no `127.0.0.1` — and broke the
moment `127.0.0.1` became the legitimate public gateway address under `./dev.sh`.
The property that actually matters is that the **advertised origin equals the
origin the client dialled**, which is what a conforming MCP client enforces.
Blocklists rot as the environment changes; equalities do not. If you find yourself
listing bad values, you have not yet found the invariant.

## 5. Every test binary statically links a bundled DuckDB (~130–160 MB)

25 binaries was a material contributor to a disk exhaustion that stopped work;
they are now 15 (~1.8 GB less per build profile). **Adding a test to an existing
binary is free; adding a binary is not.** Default to the existing per-surface file
(`admin.rs`, `query_grpc.rs`, `mcp.rs`, …), as a **submodule** if the group wants
its own fixtures — `admin.rs` carries `catalogs`, `catalogs_iceberg`, `cli_auth`
and `sdk_auth` that way, and `cargo test <feature>` still targets by name because
the module qualifier only prefixes it.

If a test needs no runtime deps at all — an `include_str!` grep, a pure function —
it is a `#[cfg(test)]` unit module, not a binary. See
`latiq-sdk/src/lib.rs::client_construction` and
`latiq/src/main.rs::auth_cli_clients_are_only_built_in_the_shared_helpers`. Both
grep only the **non-test half** of their own file: a guard that searches itself
matches its own string literals, and one written in the test module would mask a
real violation.

The one legitimate reason to split: `tracing` caches callsite `Interest`
process-wide, so a log-capture test cannot share a binary with tests that install
no subscriber. But that argument buys **one subscriber per binary, not one test per
binary** — the two full-stack access-trail binaries are now one `access_trail.rs`
that installs the subscriber behind a `OnceLock` and shares the buffer, with every
lookup narrowed by RPC so neither test can match the other's records.
`latiq-agent-core/tests/access_trail.rs` stays separate for the same reason:
merging it would force the capture subscriber onto seven unrelated tests.

## 6. Label regression pins, and never delete them

A test that exists because a specific bug was found is the highest-value test in
the suite, and to a future reader it will look narrow and redundant — which is
exactly why it needs a comment naming the bug it pins. Do not remove one as
"redundant" without finding that comment first. Models:

- `latiq-auth/tests/jwks.rs::auth_jwks_cold_start_burst_all_succeed_on_one_fetch`
  — its comment names the actual failure ("2 of 16 succeeded").
- the header-leak-across-a-hop pair, deliberately duplicated on two surfaces
  because the leak is per-surface.
- `latiq-engine-duckdb/tests/engine_e2e.rs::attribution_escapes_hostile_identity_values`.

## 7. Mutation-test anything security-relevant

Break the code, confirm the test fails, restore. Several tests on this branch were
caught passing for the wrong reason exactly this way — and one *fix* was caught
being wrong: a single-flight guard that re-checked the map still let all 16
concurrent waiters through for a kid that was never published (now pinned by
`auth_jwks_concurrent_misses_on_an_unpublished_kid_collapse_too`). **If a test
cannot be made to fail, it is not a test.**

## 8. Never weaken a test to make it pass

If a test fails after a change, either the change is wrong or the test encoded the
wrong invariant. Decide which, explicitly, and say which in the commit message.

Weakening is occasionally right, and looks like this: the SDK guard asserting
*exactly one* authenticated client builder was loosened to *at least one* when a
second legitimate builder (the embedded readiness probe) appeared — because the
invariant was always "every construction is authenticated", never "there is only
one construction". Note the direction: the assertion got more precise about the
real property, not merely quieter.

## 9. What the containerised `e2e/` tier is for

Only what an in-process fake cannot prove: real discovery documents, real claim
sets, a real grant, real token shapes (Keycloak emits `aud` as an **array**), the
gatewayed topology, the real client SDKs — and **config assertions**, that the
compose file sets what it should and nginx routes what it must. Anything else put
there is slow, container-dependent, and fails for environmental reasons.

Both e2e suites have earned their place by finding bugs the Rust suite
structurally could not: it binds and dials `127.0.0.1`, so origins match by
accident and a wrong advertised origin is invisible. See `e2e/CLAUDE.md`.

## Before you add a test

1. What regression would this catch that nothing else in the suite catches?
2. Can a cheaper layer prove it? (rule 1)
3. Does it assert *why*, with a value or a reason, not just `is_ok`/`is_err`? (rule 2)
4. Can it pass vacuously — zero matches, zero iterations, an error from the wrong
   place? (rules 2, 3)
5. Does it need a new binary, and if so is it a *subscriber* boundary? (rule 5)
6. Did you break the code and watch it fail? (rule 7)
