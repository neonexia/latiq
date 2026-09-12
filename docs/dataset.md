# Latiq — Datasets & Catalogs

Latiq has **two first-class, separate concepts** for getting external data into a
pond. The rule of thumb: a **dataset** is a simple file you *copy in*; a
**catalog** is an external database you *attach, extract from, and detach*.

> **Everything you keep lands in the Latiq data lake.** We never query external
> catalogs live — a catalog is a tap you open, pull through, and close. All real
> work happens on the pond (DuckLake).

---

## Datasets — simple files in the `latiq` catalog

A dataset is one or more file tables (parquet/CSV/…), each a public URL. Adding
one registers it; `load` copies its tables into a pond, under a **schema named
after the dataset** (so a multi-table dataset like `tpch` becomes
`tpch.lineitem`, `tpch.orders`, … and never collides with another dataset's
tables). Query them schema-qualified: `SELECT * FROM tpch.orders`.

```bash
latiq dataset add sales --table sales=https://example.com/sales.parquet \
    --tag finance --description "Acme sales export"
latiq dataset add events \
    --table clicks=https://example.com/clicks.parquet \
    --table views=https://example.com/views.parquet         # multiple tables

latiq dataset list                 # all
latiq dataset list '#finance'      # by tag
latiq dataset list sal*            # name glob / substring
latiq dataset load tpch -p shop    # -> schema `tpch` in pond `shop` (tpch.lineitem, …)
latiq dataset remove sales
```

The built-in samples (`startrek`, `holdings`, `tpch`, `taxi`) are seeded in the
`latiq` catalog.

---

## Catalogs — external sources you attach to a pond

A catalog is an external attachable database (iceberg and ducklake today). You
**attach** it to a pond under a name you choose, and it **stays attached**: from
then on it is ordinary SQL, and the name is the SQL namespace.

```bash
latiq catalog attach --name lake --type iceberg --pond shop \
    --option endpoint=https://polaris.acme/api/catalog \
    --option warehouse=prod \
    --option s3_endpoint=http://localhost:9000 \
    --secret token="$BEARER"

latiq query "SHOW TABLES FROM lake" -p shop            # orientation: just SQL
latiq query "CREATE TABLE us_orders AS SELECT id,total FROM lake.sales.orders WHERE region='us'" -p shop

latiq catalog list --pond shop                          # what is attached right now
latiq catalog detach --name lake --pond shop
```

**The attachment persists, and that is the whole point:** two catalogs attached
at once is what lets one statement **join across them**.

```bash
latiq catalog attach --name lake --type iceberg  --pond shop --option endpoint=… --option warehouse=prod
latiq catalog attach --name crm  --type ducklake --pond shop --option metadata_path=/srv/crm.duckdb --option data_path=/srv/crm
latiq query "CREATE TABLE enriched AS
             SELECT o.id, o.total, c.segment
             FROM lake.sales.orders o JOIN crm.main.customers c ON c.id = o.customer_id" -p shop
```

DuckDB's parquet/Iceberg pushdown means the extract downloads only the columns
and row-groups your query touches — not the whole table.

### The attachment is NOT persisted

It lives in the pond node's DuckDB instance. A node restart loses it, and the
next statement naming the alias comes back as `catalog_error` whose `suggest`
names `attach_catalog` — re-attach with the same arguments and re-send the
statement unchanged. **Tables you already extracted INTO the pond are
unaffected**: they are pond tables and need no catalog.

Making attachments survive a restart (and an operator-registers-once flow that
agents discover and attach from) is [issue #113](https://github.com/neonexia/latiq/issues/113).

### `--option` vs `--secret`

`--option` carries **locator** parameters, one-for-one with DuckDB's own
parenthesised `ATTACH '<target>' AS <alias> (KEY 'value', …)` options — which is
exactly why they are called options. They are **listable and loggable**, and
`latiq catalog list --pond` echoes them back.

`--secret` carries **credentials**. They are `Secret`-typed end to end: masked
`Debug`/`Display`/`Serialize`, exposed at exactly two places (the `CREATE SECRET`
statement, and the internal node-to-node hop that carries them to the node that
builds it), **never logged, never returned by any surface, never on an error
envelope**. They are dropped when the catalog is detached.

A credential key passed via `--option` is **refused** naming `--secret`, and an
option this type does not know is refused naming the legal set — never silently
dropped.

### Credentials — three modes, pick exactly one

| mode | you supply | the credential is |
|---|---|---|
| **explicit** | `--secret key=value` | those values |
| **passthrough** | *nothing* | **your own bearer token** (`LATIQ_TOKEN`) |
| **ref** | `--secret-ref <uri>` | whatever the node's backend for that URI scheme returns |

**Passthrough is the interesting one, and usually the right one.** Iceberg REST,
Unity Catalog and Snowflake External OAuth consume a bearer directly, so the
token you already presented to Latiq *is* the catalog credential — and nothing is
stored anywhere, by anyone.

```bash
# passthrough: no --secret, no --secret-ref.
LATIQ_TOKEN=$BEARER latiq catalog attach --name lake --type iceberg --pond shop \
    --option endpoint=https://polaris.acme/api/catalog --option warehouse=prod
```

`--secret-ref` takes an **opaque** `<scheme>://<location>` URI that the NODE
dereferences — for credentials the client must not hold. `env://<name>` ships
built in: it reads every `LATIQ_CATALOG_SECRET_<NAME>_<KEY>` variable exported on
the pond node, so `env://lake` with `LATIQ_CATALOG_SECRET_LAKE_TOKEN=…` resolves
to `{token: …}`. A scheme this deployment has no backend for is
`capability_unavailable` / `after_provisioning`: the call was right and an
operator has to provision it. Adding Vault or AWS Secrets Manager is one
implementation of `SecretRefResolver`, not a change to any surface.

Supplying two modes is refused naming all three. The attach response reports the
mode that was **applied** — including `none`, which means no credential was used
at all (correct for a local DuckLake; a warning sign for an Iceberg REST catalog,
and the tell that a passthrough had no bearer to pass through).

**Latiq stores no credentials.** Nothing reaches the control-plane registry, and
nothing is written to disk; an explicit or resolved credential lives only in the
pond node's DuckDB secret for as long as the catalog is attached.

### Params by type

| key | `--option` / `--secret` | Meaning |
|---|---|---|
| `endpoint` | option (iceberg) | REST catalog URL |
| `warehouse` | option (iceberg) | warehouse/catalog name to ATTACH |
| `metadata_path` | option (ducklake) | the catalog database |
| `data_path` | option (ducklake) | where its data lives |
| `s3_endpoint`, `s3_region` | option | storage backend locator |
| `token` | secret (iceberg) | OAuth bearer for the REST catalog |
| `s3_access_key`, `s3_secret_key` | secret | SigV4 storage creds |

### Registering a catalog (discovery only)

An operator can register a catalog's locator so agents do not have to be told it.
This is **discovery, not credentials and not state**: `latiq catalog list`
returns each one's type and `params`, which is exactly what you pass to
`catalog attach` as `--option`.

```bash
latiq catalog add lake --type iceberg \
    --option endpoint=https://polaris.acme/api/catalog \
    --option warehouse=prod \
    --description "Acme Iceberg — orders, customers. Prod, read-only." --tag prod

latiq catalog list                       # all / '#tag' / glob / substring
latiq catalog remove lake
```

Credential-shaped keys passed to `catalog add` are dropped and never persisted;
the CLI says so: `(not stored, pass at attach with --secret: token)`.

---

## The split

| | **Dataset** | **Catalog** |
|---|---|---|
| What | one or more simple files | an external database/lake |
| Lives | in the built-in `latiq` catalog | attached to a pond, under a name you choose |
| Tables | it *is* the tables | **discovered with SQL** (`SHOW TABLES FROM <name>`) |
| Into a pond | **load** (copy) | **attach → ordinary write_query → detach** |
| Two at once | n/a | yes — and joinable in one statement |
| Credentials | n/a (public) | at attach, never stored |

## Surfaces

`dataset add/remove`, `catalog add/remove` are **operator** (Admin gRPC) actions.
`list`, `dataset load` and `catalog attach/detach/list --pond` are available to
**CLI/SDK** (Data gRPC) and **agents** (MCP: `attach_catalog`, `detach_catalog`,
`list_attached_catalogs`, over the same `AgentOps` methods).
