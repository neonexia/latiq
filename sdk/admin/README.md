# latiq-admin

The lean operator CLI for [Latiq](https://github.com/neonexia/latiq) — drives a
cluster someone else is running, over gRPC.

```bash
pipx install latiq-admin          # or: pip install latiq-admin
export LATIQ_SERVER=http://your-control-plane:51400
latiq stats                       # nodes, ponds, tiers
latiq pond list
```

This wheel contains a **native executable**, not Python. pip is only the delivery
mechanism — the same one `pipx` uses for any other packaged CLI.

## What's in it, and what isn't

It is the `latiq` CLI built **client-only**: no control plane, no pond node, and
no bundled DuckDB, which is most of the full build's size. Everything that talks
to a cluster is here — `pond`, `query`, `dataset`, `catalog`, `node list`,
`stats`.

The server roles are not. `latiq serve` and `latiq node add` still *parse*, and
fail with a message pointing at the install that can run them:

```
$ latiq serve
Error: `latiq serve` needs the full build; this is latiq-admin, the lean
operator CLI (client commands only — no control plane, no pond node, no query
engine).
```

## Which package do I want?

| I want to… | Install |
|---|---|
| drive a cluster from my laptop / CI | **`latiq-admin`** (this one) |
| run a cluster, or use the Python SDK | **`latiq`** |
| run a cluster in containers | the [images](https://github.com/neonexia/latiq/blob/main/deploy/README.md) |

`latiq` and `latiq-admin` are two builds of one CLI and install the same `latiq`
command, so put one or the other in a given environment — not both. `pipx` gives
this its own isolated environment, which is why it's the recommended form.

## Configuration

- `LATIQ_SERVER` — the control plane's address (Admin gRPC, default port 51400).
- `LATIQ_TOKEN` — an OAuth bearer token, where the deployment configures an
  issuer. Unset is correct for a deployment that doesn't.

Apache-2.0. Docs, issues and the full picture:
[github.com/neonexia/latiq](https://github.com/neonexia/latiq).
