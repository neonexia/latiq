# The ErrorEnvelope schema

`ErrorEnvelope-1-0-1.json` (the current version — see *Versioning*) is the
machine-auditable shape of the one error every Latiq surface returns. It is here for the same reason `latiq-lineage/spec/` holds
the OpenLineage schemas: so `cargo test -p latiq-common` can prove, offline, that
what we actually construct matches what we say we return. `jsonschema` is a
**dev-dependency** and nothing in `src/` reads this file at runtime.

## Why we validate it ourselves

**rmcp does not validate tool responses against a declared schema** — its
`model.rs` says so in as many words, and `latiq-mcp`'s CLAUDE.md records the
consequence: a declared schema nobody checks is a document, not a contract. The
`ErrorEnvelope` is doubly exposed, because it is deliberately *outside* each
tool's `outputSchema` (a failed call sets `is_error`, and the two reference MCP
SDKs — the only clients we have checked — skip output-schema validation entirely
on an error result; see `latiq-mcp/src/encode.rs` for the exact scope of that
claim, which is not something the MCP spec guarantees). So the only
thing standing between an agent and a malformed actionable is
`error.rs`'s `error_contract_every_kind_validates_against_the_vendored_schema`,
which builds an envelope of **every** `ErrorKind` and validates each one here.

The same file is what an observability tool audits against later — the
agent-simulator work reads actionables out of Langfuse and needs a shape to
compare them to.

## Versioning

The version is this envelope's, not the release's, and it lives in the filename
and the `$id`. Bump it — a new file beside the old one, not an edit in place —
when the shape changes in a way a consumer would notice: a new required field, a
removed one, a narrowed type. Adding a value to the `kind` enum is a shape change
too; the enum is listed in full on purpose, so a kind added to `ErrorKind`
without being listed here fails the test rather than shipping unannounced.

**An added OPTIONAL field is a bump as well**, which is why there are two files
here. `additionalProperties: false` is the whole point of this schema, so a
validator holding `1-0-0` rejects an envelope carrying a field only `1-0-1`
knows: "optional to the producer" is not "invisible to the consumer".

### `ErrorEnvelope-1-0-1.json` — current

Adds optional `traceparent`. `trace_id` is unchanged and stays: a consumer that
only wants the join key reads the same field it always did, while one building a
span tree gets the span id `trace_id` cannot carry. It is absent — rather than
faked — wherever we have no span of our own to name, which today is the control
plane's Admin/Control surfaces (see `latiq-control-plane`'s `trace_meta`: it
deliberately mints nothing it would not also propagate).

### `ErrorEnvelope-1-0-0.json` — superseded

Kept as the record of what `0.1.x` shipped before `traceparent`. Nothing in the
test suite validates against it any more.

As with the Latiq lineage facets, the `$id` is an **identifier, not a fetchable
document** — the repo is private and no `error-envelope-1-0-0` ref has been cut.
Do not write anywhere that it resolves.

## What the schema is strict about, and why

- `audience` is two-valued (`agent`/`operator`). A third `human` was specified
  and dropped: no kind can reach it, and a variant no envelope can carry is the
  enum version of the dead `ErrorKind` this repo has shipped before.
- `additionalProperties: false` — an unknown field is a surface inventing its own
  contract, which is how two spellings of one concept get shipped.
- `message` and `suggest` are `minLength: 1` — an actionable with nothing to read
  or no next call is not an actionable.
- `see` must be a `latiq://` URI; `latiq-mcp`'s
  `error_contract_every_error_kind_sees_a_resource_that_exists` separately proves
  the URI resolves to a resource that is actually served.
- `facts` values are scalars (`string`, non-negative `integer`, or `boolean`). Facts exist so
  a client can branch on a value rather than parse it out of a sentence; a nested
  structure would be a second response shape smuggled into an error.
- `trace_id` is 32 lowercase hex digits — the W3C `trace-id`, the same one the
  access trail and the lineage events carry, so the three join. `traceparent` is
  the full four-field W3C header and must contain that same trace id; the pattern
  pins the shape so a half-built header (a missing span, an uppercase digit) is
  caught here rather than by whatever collector we hand it to.
