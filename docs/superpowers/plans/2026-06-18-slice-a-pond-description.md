# Slice A — Pond `description` end-to-end Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add an optional agent-discovery `description` to a pond, threaded from
create through the registry to every read surface (control `PondInfoMsg`, admin
`PondSummary`, neutral `PondInfo`) and the CLI (`--description`, shown in
list/describe).

**Architecture:** A new nullable `description` column on the `ponds` table
(forward-only migration), carried on `PondRow`, set at `create_pond`, and surfaced
through the existing control/admin handlers + the neutral `PondInfo`. No new RPCs
— only new fields on existing messages.

**Tech Stack:** Rust, tonic/prost (proto codegen), DuckDB registry, clap (CLI).

**Spec:** `docs/superpowers/specs/2026-06-18-sdk-handle-arrow-design.md` (Slice 2).

**Branch:** create `sdk-pond-description` off `main` (NOT off `sdk-python`).
Direct push to `main` is blocked — branch → `gh pr create` → merge.

---

### Task 1: Registry — column, migration, `PondRow`, create/read SQL

**Files:**
- Modify: `crates/latiq-control-plane/src/migrations.rs` (append to `MIGRATIONS`, currently ends at v4)
- Modify: `crates/latiq-control-plane/src/registry.rs:28-37` (`PondRow`), `:189-233` (`create_pond`), `:246-263` (`list_ponds`), `:268-297` (`pond_info`)
- Test: `crates/latiq-control-plane/src/registry.rs` (add `#[cfg(test)]` unit test)

- [ ] **Step 1: Write the failing test** — append to the `#[cfg(test)] mod tests` block in `registry.rs` (find it with `grep -n "mod tests" crates/latiq-control-plane/src/registry.rs`; if none exists, add `#[cfg(test)] mod tests { use super::*; ... }` at end of file). Use the in-memory registry constructor the other registry tests use (check an existing test for `Registry::open(None)` + a `register_node` helper; reuse that exact setup):

```rust
#[test]
fn create_pond_round_trips_description() {
    let reg = Registry::open(None).unwrap();
    reg.register_node("n1", "http://m", "http://i", 10).unwrap();
    let row = reg
        .create_pond(Some("docs".into()), "owner", "{}", "medium", &[], "raw events 2024")
        .unwrap();
    assert_eq!(row.description, "raw events 2024");
    // list + info both carry it
    let listed = reg.list_ponds().unwrap();
    assert_eq!(listed[0].description, "raw events 2024");
    let (info, _, _, _) = reg.pond_info("docs").unwrap();
    assert_eq!(info.description, "raw events 2024");
}
```

> If `register_node`'s signature differs, copy it verbatim from a neighbouring
> registry test — do not guess the arg order.

- [ ] **Step 2: Run it to confirm it fails to compile** — `create_pond` has 5 args, not 6, and `PondRow` has no `description`.

Run: `cargo test -p latiq-control-plane create_pond_round_trips_description`
Expected: FAIL — "this function takes 5 arguments but 6 were supplied" / "no field `description`".

- [ ] **Step 3: Add the migration** — append to `MIGRATIONS` in `migrations.rs` after the v4 entry (it is the last element of the slice; add a trailing element):

```rust
    // v5: optional agent-discovery description (what the pond is for, so other
    // agents can find it). Nullable + default so existing rows read as empty.
    "ALTER TABLE ponds ADD COLUMN description VARCHAR DEFAULT '';",
```

- [ ] **Step 4: Add the field to `PondRow`** — in `registry.rs:28-37`, after `extensions`:

```rust
    /// Optional agent-discovery text: what this pond is for. Empty = none.
    pub description: String,
```

- [ ] **Step 5: Thread `description` through `create_pond`** — change the signature (`registry.rs:189-196`) to add a final param, the INSERT (`:218-221`) to include the column, and the returned `PondRow` (`:225-232`):

```rust
    pub fn create_pond(
        &self,
        name: Option<String>,
        owner_identity: &str,
        policy_json: &str,
        tier: &str,
        extensions: &[String],
        description: &str,
    ) -> Result<PondRow, ControlPlaneError> {
```
```rust
        c.execute(
            "INSERT INTO ponds(pond_id, name, owner_identity, node_id, policy_json, tier, extensions, description) VALUES (?,?,?,?,?,?,?,?)",
            duckdb::params![pond_id, name, owner_identity, node_id, policy_json, tier, ext_csv, description],
        )?;
```
```rust
        Ok(PondRow {
            pond_id,
            name,
            owner_identity: owner_identity.to_string(),
            node_id,
            tier: tier.to_string(),
            extensions: extensions.to_vec(),
            description: description.to_string(),
        })
```

- [ ] **Step 6: Read `description` in `list_ponds` and `pond_info`** — in `list_ponds` (`:248-261`) add the column to the SELECT and the `PondRow`:

```rust
        let mut stmt = c.prepare(
            "SELECT pond_id, name, owner_identity, node_id, coalesce(tier, 'medium'),
                    coalesce(extensions, ''), coalesce(description, '') FROM ponds ORDER BY created_at",
        )?;
        let rows = stmt.query_map([], |r| {
            Ok(PondRow {
                pond_id: r.get(0)?,
                name: r.get(1)?,
                owner_identity: r.get(2)?,
                node_id: r.get(3)?,
                tier: r.get(4)?,
                extensions: latiq_common::extensions::parse_csv(&r.get::<_, String>(5)?),
                description: r.get(6)?,
            })
        })?;
```

In `pond_info` (`:273-295`) add `coalesce(p.description, '')` as the last SELECT
column (index 9) and read it into `PondRow`:

```rust
        c.query_row(
            "SELECT p.pond_id, p.name, p.owner_identity, p.node_id,
                    p.created_at::VARCHAR, p.policy_json, n.internal_endpoint,
                    coalesce(p.tier, 'medium'), coalesce(p.extensions, ''),
                    coalesce(p.description, '')
             FROM ponds p LEFT JOIN nodes n ON n.node_id = p.node_id
             WHERE p.pond_id=? OR p.name=? LIMIT 1",
            duckdb::params![pond_ref, pond_ref],
            |r| {
                Ok((
                    PondRow {
                        pond_id: r.get(0)?,
                        name: r.get(1)?,
                        owner_identity: r.get(2)?,
                        node_id: r.get(3)?,
                        tier: r.get::<_, String>(7)?,
                        extensions: latiq_common::extensions::parse_csv(&r.get::<_, String>(8)?),
                        description: r.get::<_, String>(9)?,
                    },
                    r.get::<_, String>(4)?,
                    r.get::<_, String>(5)?,
                    r.get::<_, Option<String>>(6)?,
                ))
            },
        )
```

- [ ] **Step 7: Run the test — expect PASS**

Run: `cargo test -p latiq-control-plane create_pond_round_trips_description`
Expected: PASS. Then `cargo build -p latiq-control-plane` — expect an error at the
**`control_service.rs` call site** of `create_pond` (5 args). That's Task 2.

- [ ] **Step 8: Commit**

```bash
git add crates/latiq-control-plane/src/migrations.rs crates/latiq-control-plane/src/registry.rs
git commit -m "feat(registry): pond description column + create/read plumbing"
```

---

### Task 2: Proto + control/admin handlers carry `description`

**Files:**
- Modify: `crates/latiq-proto/proto/latiq/v1/control.proto:50` (CreatePondAssignmentRequest), `:30-40` (PondInfoMsg)
- Modify: `crates/latiq-proto/proto/latiq/v1/admin.proto:39` (PondSummary)
- Modify: `crates/latiq-control-plane/src/control_service.rs:62-65` (create), `:115-124` (list), `:138-147` (get_info)
- Modify: `crates/latiq-control-plane/src/admin_service.rs:83-90` (pond_list)
- Test: `crates/latiq-control-plane/tests/grpc_integration.rs`

- [ ] **Step 1: Add the proto fields.** `control.proto` line 50 — add field 6:

```proto
message CreatePondAssignmentRequest { string name = 1; string owner_identity = 2; string policy_json = 3; string tier = 4; repeated string extensions = 5; string description = 6; }
```

`control.proto` `PondInfoMsg` (after `extensions = 8;`, line 39) — add field 9:

```proto
  // Agent-discovery text: what this pond is for (empty = none).
  string description = 9;
```

`admin.proto` line 39 — add field 7 to `PondSummary`:

```proto
message PondSummary { string pond_id = 1; string name = 2; string owner = 3; string created_at = 4; string node_id = 5; string tier = 6; string description = 7; }
```

- [ ] **Step 2: Thread it in `control_service.rs`.** `create_pond_assignment` (`:62-65`) — pass `&r.description` as the new last arg:

```rust
        let pond = self
            .registry
            .create_pond(name, &r.owner_identity, &r.policy_json, tier, &r.extensions, &r.description)
            .map_err(to_status)?;
```

`list_ponds` (`:115-124`) and `get_pond_info` (`:138-147`) — add `description:
row.description,` to each `PondInfoMsg { … }` literal (last field).

- [ ] **Step 3: Thread it in `admin_service.rs`** `pond_list` (`:83-90`) — add to the `PondSummary { … }` literal:

```rust
            ponds.push(PondSummary {
                pond_id: row.pond_id,
                name: row.name,
                owner: row.owner_identity,
                created_at,
                node_id: row.node_id,
                tier: row.tier,
                description: row.description,
            });
```

- [ ] **Step 4: Build the crate** — proto codegen regenerates the structs.

Run: `cargo build -p latiq-control-plane`
Expected: PASS (the Task 1 call-site error is now resolved).

- [ ] **Step 5: Add a gRPC round-trip assertion.** In `grpc_integration.rs`, find the test that creates a pond and calls `pond_list`/`get_pond_info` (grep `pond_list`/`create_pond_assignment`). Add a `description` to the create request and assert it round-trips. Minimal new test if none fits:

```rust
#[tokio::test]
async fn pond_description_round_trips_over_grpc() {
    let h = TestHarness::start().await; // use whatever the file's setup helper is
    let mut ctrl = h.control().await;
    let mut admin = h.admin().await;
    ctrl.create_pond_assignment(CreatePondAssignmentRequest {
        name: "docs".into(), owner_identity: "o".into(), policy_json: "{}".into(),
        tier: "medium".into(), extensions: vec![], description: "raw events".into(),
    }).await.unwrap();
    let ponds = admin.pond_list(PondListRequest {}).await.unwrap().into_inner().ponds;
    assert_eq!(ponds.iter().find(|p| p.name == "docs").unwrap().description, "raw events");
}
```

> Match the file's existing harness/builder names exactly — copy a sibling test's
> setup lines rather than inventing `TestHarness`.

- [ ] **Step 6: Run it — expect PASS**

Run: `cargo test -p latiq-control-plane --test grpc_integration pond_description_round_trips_over_grpc`
Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add crates/latiq-proto/proto crates/latiq-control-plane/src/control_service.rs crates/latiq-control-plane/src/admin_service.rs crates/latiq-control-plane/tests/grpc_integration.rs
git commit -m "feat(proto,control): carry pond description on create/list/info"
```

---

### Task 3: Neutral `PondInfo` + both `ControlPlane` mappings

**Files:**
- Modify: `crates/latiq-agent-core/src/types.rs:6-23` (`PondInfo`)
- Modify: `crates/latiq-pond-node/src/grpc_control.rs:48-60` (`to_info`)
- Modify: `crates/latiq-agent-core/src/registry_control.rs:57-73` (`to_info`)

- [ ] **Step 1: Add the field to neutral `PondInfo`** — `types.rs`, after `extensions` (`:22`):

```rust
    /// Agent-discovery text: what this pond is for (empty = none).
    #[serde(default)]
    pub description: String,
```

- [ ] **Step 2: Map it in the gRPC `ControlPlane`** — `grpc_control.rs` `to_info` (`:48-60`), add to the `PondInfo { … }` literal:

```rust
        description: m.description,
```

- [ ] **Step 3: Map it in the in-process `ControlPlane`** — `registry_control.rs` `to_info` (`:62-73`), add:

```rust
        description: row.description,
```

- [ ] **Step 4: Build the workspace core** — these two are the only `PondInfo {}` constructors; a missing-field error elsewhere means another constructor exists (grep `PondInfo {` and fix it the same way).

Run: `cargo build -p latiq-agent-core -p latiq-pond-node`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add crates/latiq-agent-core/src/types.rs crates/latiq-agent-core/src/registry_control.rs crates/latiq-pond-node/src/grpc_control.rs
git commit -m "feat(agent-core): description on neutral PondInfo + both ControlPlane mappings"
```

---

### Task 4: CLI — `--description` on create, shown in list/describe

**Files:**
- Modify: `crates/latiq/src/main.rs:225-240` (`PondCmd::Create`), `:832-857` (create handler), `:885-893` (list json), `:1154-1190` (`print_pond_list_table`)
- Test: `crates/latiq/tests/admin.rs`

- [ ] **Step 1: Add the flag** — `PondCmd::Create` (`:225-240`), after `extensions`:

```rust
        /// Free-text description of what this pond is for, so other agents can
        /// discover it (shown in `pond list`/`describe`).
        #[arg(short, long)]
        description: Option<String>,
```

- [ ] **Step 2: Destructure + send it** — create handler (`:832-857`): add `description,` to the destructure pattern, and set the request field:

```rust
        PondCmd::Create {
            name,
            tier,
            extensions,
            agent_id,
            description,
        } => {
```
```rust
                .create_pond_assignment(CreatePondAssignmentRequest {
                    name: name.unwrap_or_default(),
                    owner_identity: owner,
                    policy_json: "{}".into(),
                    tier,
                    extensions: exts,
                    description: description.unwrap_or_default(),
                })
```

- [ ] **Step 3: Show it in `pond list --format json`** (`:885-893`) — add to the json map:

```rust
                            serde_json::json!({
                                "pond_id": p.pond_id, "name": p.name, "owner": p.owner,
                                "node_id": p.node_id, "tier": p.tier, "created_at": p.created_at,
                                "description": p.description,
                            })
```

- [ ] **Step 4: Add a DESCRIPTION column to the table** — `print_pond_list_table`
(`:1154-1190`): widen the header + row arrays from 7 to 8 columns. Change the
`header` array to end with `"DESCRIPTION"`, change `Vec<[String; 7]>` →
`Vec<[String; 8]>`, and append `p.description.clone()` as the last cell. The
width-computation loop iterates the arrays generically, so no other change is
needed — but read the rest of the function (`:1190+`) to confirm the column count
isn't hard-coded anywhere; if it is, bump it to 8.

- [ ] **Step 5: Build + verify describe shows it.** `pond describe` prints the
`DescribeResult` JSON, whose `pond` is the neutral `PondInfo` (now with
`description`), so no describe-handler change is needed.

Run: `cargo build -p latiq`
Expected: PASS.

- [ ] **Step 6: Add a CLI e2e assertion.** In `crates/latiq/tests/admin.rs`, find
a `pond_lifecycle_*` test that runs `pond create` then `pond list` through the
harness (grep `pond create` / the harness's CLI-invocation helper). Add
`--description "raw events"` to the create invocation and assert the JSON list
output contains `"description": "raw events"`. Follow the file's existing
command-runner helper exactly.

- [ ] **Step 7: Run it — expect PASS**

Run: `cargo test -p latiq --test admin`
Expected: PASS (all admin e2e, including the new assertion).

- [ ] **Step 8: Commit**

```bash
git add crates/latiq/src/main.rs crates/latiq/tests/admin.rs
git commit -m "feat(cli): pond create --description; show in list/describe"
```

---

### Task 5: Full gate + PR

- [ ] **Step 1: Format + lint + full test**

```bash
cargo fmt --all
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
```
Expected: all green. (First DuckDB build is slow.)

- [ ] **Step 2: Open the PR**

```bash
gh pr create --title "Slice A: pond description end-to-end" \
  --body "Adds an optional agent-discovery \`description\` to ponds: registry column (migration v5) → control \`PondInfoMsg\`/admin \`PondSummary\`/neutral \`PondInfo\` → CLI \`--description\`, shown in list/describe. Unblocks the SDK handle's \`description\` attribute (Slice B).

🤖 Generated with [Claude Code](https://claude.com/claude-code)"
```

---

## Self-review notes
- **Spec coverage:** proto (✓ T2), registry migration+column (✓ T1), control+admin
  (✓ T2), neutral `PondInfo` (✓ T3), CLI create/list/describe (✓ T4). All Slice-2
  bullets covered.
- **Type consistency:** the new field is `description: String` everywhere
  (`PondRow`, `PondInfo`, proto `string description`); CLI flag is
  `Option<String>` → `.unwrap_or_default()`. `create_pond`'s new param is the
  **last** arg in both the def and the one call site (control_service).
- **Migration safety:** append-only v5 (index 4); never edits a shipped migration
  (control-plane invariant).
