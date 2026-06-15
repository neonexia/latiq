# Latiq — Datasets & Catalogs

Latiq has **two first-class, separate concepts** for getting external data into a
pond. The rule of thumb: a **dataset** is a simple file you *copy in*; a
**catalog** is an external database you *pull from once*.

> **Everything lands in the Latiq data lake.** We never query external catalogs
> live — a catalog is a tap you open, pull through, and close. All real work
> happens on the pond (DuckLake).

---

## Datasets — simple files in the `latiq` catalog

A dataset is one or more file tables (parquet/CSV/…), each a public URL. Adding
one registers it; `load` copies its tables into a pond.

```bash
latiq dataset add sales --table sales=https://example.com/sales.parquet \
    --tag finance --description "Acme sales export"
latiq dataset add events \
    --table clicks=https://example.com/clicks.parquet \
    --table views=https://example.com/views.parquet         # multiple tables

latiq dataset list                 # all
latiq dataset list '#finance'      # by tag
latiq dataset list sal*            # name glob / substring
latiq dataset load tpch -p shop    # copy a dataset's tables into pond `shop`
latiq dataset remove sales
```

The built-in samples (`startrek`, `holdings`, `tpch`, `taxi`) are seeded in the
`latiq` catalog.

---

## Catalogs — external sources you pull from

A catalog is an external attachable database (iceberg today; ducklake/duckdb/…
later). An operator registers it with a **type** and **`--set` params** (locator
metadata only). Agents/clients then **pull** from it: a one-shot transient
`attach → run your query → detach` that materializes a subset into the pond.

```bash
# register (operator). --set carries locator params; credentials are NOT stored.
latiq catalog add lake --type iceberg \
    --set endpoint=https://polaris.acme/api/catalog \
    --set warehouse=prod \
    --description "Acme Iceberg" --tag prod

latiq catalog list                       # all / '#tag' / glob / substring
latiq catalog describe lake -p shop --set token="$BEARER"   # list its tables (transient)

# pull a subset into the pond (the query names the catalog + its target table):
latiq catalog pull lake -p shop --set token="$BEARER" \
    --query "CREATE TABLE us_orders AS SELECT id,total FROM lake.sales.orders WHERE region='us'"

latiq catalog remove lake
```

DuckDB's parquet/Iceberg pushdown means the pull downloads only the columns and
row-groups your query touches — not the whole table.

### Credentials (`--set` everywhere, never stored)

The same `--set key=value` is used on `add`, `describe`, and `pull`. The
per-type **attacher** maps the keys to the right DuckDB `CREATE SECRET` / `ATTACH`
clauses. Two rules keep Latiq credential-free:

- **At `add`, credential-shaped keys are dropped** (an allowlist per type keeps
  only locator metadata like `endpoint`/`warehouse`). The CLI tells you:
  `(not stored, pass at pull: token)`.
- **Credentials ride in at `pull`/`describe`** as `--set token=…` (or, later, the
  caller's identity bearer). They build a *temporary* DuckDB secret for that one
  operation and are dropped on detach. Nothing persists.

Pull-time `--set` values are merged over the catalog's stored locator params
(**pull wins**). See [issue #26](https://github.com/neonexia/latiq/issues/26) for
the identity-bearer integration.

### Iceberg params

| `--set` key | When | Meaning |
|---|---|---|
| `endpoint` | add | REST catalog URL |
| `warehouse` | add | warehouse/catalog name to ATTACH |
| `s3_endpoint`, `s3_region` | add | storage backend locator |
| `token` | pull/describe | OAuth bearer for the REST catalog |
| `s3_access_key`, `s3_secret_key` | pull/describe | SigV4 storage creds |

---

## The split

| | **Dataset** | **Catalog** |
|---|---|---|
| What | one or more simple files | an external database/lake |
| Lives | in the built-in `latiq` catalog | first-class, registered |
| Tables | it *is* the tables | **discovered** (`describe`) |
| Into a pond | **load** (copy) | **pull** (transient attach → query → detach) |
| Credentials | n/a (public) | at pull, never stored |

## Surfaces

`dataset add/remove`, `catalog add/remove` are **operator** (Admin gRPC) actions.
`list`, `dataset load`, `catalog describe/pull` are available to **CLI/SDK** (Data
gRPC) and **agents** (MCP — the tool layer is the next slice on top of the same
`AgentOps` methods).
