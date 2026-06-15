# Latiq — Dataset Catalog

The dataset catalog is a **registry-backed list of external tables** that operators
curate and agents/clients load into ponds. Instead of every caller remembering a
pile of `read_parquet('https://…')` URLs, a dataset gives those sources a **name**,
a **namespace**, **tags**, and a **description** — so they're discoverable and
loadable in one command.

> **Slice 1 (this doc):** public sources only (https/public object stores).
> Credentialed sources (private S3, Iceberg/Databricks REST catalogs) arrive with
> the identity work — see [issue #26](https://github.com/neonexia/latiq/issues/26).

---

## Concepts

A **dataset** is:

| Field | Meaning | Example |
|---|---|---|
| **namespace** | a dotted path that groups datasets | `latiq.sample`, `hf.acme` |
| **name** | the leaf name within the namespace | `tpch`, `sales` |
| **reference** | the full id, `<namespace>.<name>` | `latiq.sample.tpch` |
| **tables** | one or more external tables (`name` + `source_uri` + `format`) | `lineitem` → `…/lineitem.parquet` |
| **tags** | searchable labels | `finance`, `apple_hr` |
| **description** | a human summary | "TPC-H scale 0.01" |

The reference is parsed by splitting on the **last** dot, so the namespace may be
arbitrarily deep: `hf.acme.sales` → namespace `hf.acme`, name `sales`.

**Where it lives:** the catalog is metadata in the **control-plane registry** (it
isn't pond data). Loading a dataset materializes its tables into a pond as normal
tables — so after a load you query them like anything else, and the pond's catalog
holds no reference back to the source.

**Built-in samples:** the public sample datasets ship seeded under the
`latiq.sample` namespace (`startrek`, `holdings`, `tpch`, `taxi`).

---

## Commands

### List / search

```bash
latiq dataset list                 # everything
latiq dataset list '#finance'      # by tag (leading #)
latiq dataset list 'hf.*'          # by namespace / ref glob (* → wildcard)
latiq dataset list sales           # substring over ref / description / tags
```

Output (one row per dataset):

```
NAME      NAMESPACE     TAGS               TABLES  DESCRIPTION
sales     hf.acme       #apple_hr,finance       1  Acme sales export
holdings  latiq.sample  sample                  1  Example stock holdings — CSV, ~300 B
tpch      latiq.sample  sample,tpch             8  TPC-H scale 0.01 — 8 tables, Parquet
```

**Search syntax** (the optional `query` argument):
- `#tag` — datasets carrying that exact tag.
- `prefix*` — glob over the full reference (e.g. `hf.*`, `latiq.sample.*`, `*tpch`).
- anything else — case-insensitive substring over reference, description, and tags.
- omitted — list all.

### Add (operator)

```bash
latiq dataset add hf.acme.sales \
  --table sales=https://example.com/sales.parquet \
  --tag finance --tag apple_hr \
  --description "Acme sales export"
```

- The first positional argument is the full reference `<namespace>.<name>`.
- `--table NAME=URI` is **repeatable** — one per table the dataset exposes:
  ```bash
  latiq dataset add acme.events \
    --table clicks=https://example.com/clicks.parquet \
    --table views=https://example.com/views.parquet \
    --description "Acme web events"
  ```
- `--tag` is repeatable. `--format` (`parquet` | `csv` | `json` | `auto`, default
  `auto`) applies to all tables; `auto` infers the reader from the URI extension.
- Re-adding the same reference **replaces** it (idempotent).

### Remove (operator)

```bash
latiq dataset remove hf.acme.sales
```

### Load into a pond

```bash
latiq pond create --name demo
latiq dataset load latiq.sample.tpch -p demo     # materializes all 8 TPC-H tables
latiq query -p demo "SELECT count(*) FROM orders" # 15000
```

Each table is created with `CREATE OR REPLACE TABLE <name> AS SELECT * FROM
read_*('<uri>')` through the **normal write path**, so it's attributed to
`--agent-id`, snapshotted, and routed/forwarded to the pond's owning node like any
other write. Loading needs network (the data is fetched from the source URIs).

---

## Surfaces

The catalog respects Latiq's surface separation — agents can load datasets but
only operators can curate them.

| Operation | Admin gRPC (operators) | Data gRPC (CLI / SDK) | MCP (agents) |
|---|---|---|---|
| add / remove | ✅ | — | — |
| list / search | ✅ | ✅ (via the node) | ✅ *(next slice)* |
| load into a pond | — | ✅ | ✅ *(next slice)* |

Internally a pond node reads the catalog over **Control gRPC** (`GetDataset` /
`ListDatasets`) and loads via the Data gRPC `LoadDataset`; the control plane is
never in the data path.

> **Agent (MCP) access** — list/load tools plus a `latiq://datasets` resource so
> agents can discover and pull datasets — is the next slice on top of the same
> `AgentOps` methods.

---

## Credentials (deferred)

Slice 1 datasets point at **public** URIs. For private sources, Latiq will be a
**pass-through**: it stores no credentials — the caller's token is attached to a
temporary, source-scoped DuckDB secret only for the duration of the load, then
dropped. See [issue #26](https://github.com/neonexia/latiq/issues/26) (done with
the identity work, [#5](https://github.com/neonexia/latiq/issues/5)).
