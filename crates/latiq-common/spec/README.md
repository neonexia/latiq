# The ErrorEnvelope schema

`ErrorEnvelope-1-0-3.json` (the current version — see *Versioning*) is the
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

**An added OPTIONAL field is a bump as well**, which is why there is more than
one file here. `additionalProperties: false` is the whole point of this schema, so a
validator holding `1-0-0` rejects an envelope carrying a field only `1-0-1`
knows: "optional to the producer" is not "invisible to the consumer".

### `ErrorEnvelope-1-0-3.json` — current

Two enums widen, in the same change and for one reason. `retryable` gains
**`after_provisioning`** and `kind` gains **`capability_unavailable`**.

`retryable` had three values and two of them were being asked to describe three
situations: "your call was wrong, fix it" (`after_change`), "the world was busy,
send it again" (`as_is`) and — with nothing honest to say — "your call was fine,
this deployment has not provisioned something it needs". The third was arriving
as `internal` + `as_is`, i.e. advice to repeat a call that cannot succeed until
somebody installs an extension, and then to wake a human about it. So
`after_provisioning` says exactly that: the call is correct, escalate rather than
retry, re-send only once told the capability exists. It is always paired with
`audience: operator`, which already names who acts — a human in the loop **or an
orchestrating agent with higher privilege**; the sub-agent's job is to surface it
and stop. Nothing asynchronous is implied: there is no callback and no approval
channel, because every Latiq surface is request/response.

`capability_unavailable` is the kind that carries it, and today its one
construction site is a DuckDB extension missing from the node's cache
(`latiq-engine-duckdb`'s `instance::extension_not_cached`), with the extension
name in `facts.capability` so a client branches on the value. Other kinds that
might reasonably move to this value — an unconfigured auth issuer, tier caps, an
unattached catalog — are deliberately **not** re-mapped: `read_only_violation` in
particular stays `never` while Nexus is measuring how agents read that value.

### `ErrorEnvelope-1-0-2.json` — superseded

Two `kind` values change, which is a shape change in both directions: adds
`unsupported_feature` and removes `uri_not_allowed`. The new kind is what a
statement asking for a feature the engine does not implement now returns —
`CREATE TABLE t(id INTEGER PRIMARY KEY)` used to arrive as `internal` +
"report to your operator" (Nexus finding 8). The removed one had **zero
construction sites**: it was declared, documented to agents, and unreachable,
because the URI allowlist it described is not built (issue #79). A kind nobody
can reach is not a feature, so it went rather than being left as a promise the
surface does not keep.

### `ErrorEnvelope-1-0-1.json` — superseded

Adds optional `traceparent`. `trace_id` is unchanged and stays: a consumer that
only wants the join key reads the same field it always did, while one building a
span tree gets the span id `trace_id` cannot carry. It is absent — rather than
faked — wherever we have no span of our own to name, which today is the control
plane's Admin/Control surfaces (see `latiq-control-plane`'s `trace_meta`: it
deliberately mints nothing it would not also propagate).

### `ErrorEnvelope-1-0-0.json` — superseded

Kept as the record of what `0.1.x` shipped before `traceparent`. Nothing in the
test suite validates against `1-0-0`, `1-0-1` or `1-0-2` any more.

As with the Latiq lineage facets, the `$id` is an **identifier, not a fetchable
document** — the repo is private and no `error-envelope-1-0-0` ref has been cut.
Do not write anywhere that it resolves.

## What the schema is strict about, and why

- `audience` is two-valued (`agent`/`operator`). A third `human` was specified
  and dropped: no kind can reach it, and a variant no envelope can carry is the
  enum version of the dead `ErrorKind` this repo has shipped before. The same
  test decides `retryable`'s fourth value: `after_provisioning` is listed here
  because `capability_unavailable` really raises it, from a real cache miss.
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
