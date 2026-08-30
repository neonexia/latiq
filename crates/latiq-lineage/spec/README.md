# Vendored OpenLineage schemas

These are here so `cargo test -p latiq-lineage` can prove every event we emit is a
real OpenLineage `RunEvent` **without network access**. They are inputs to the
test suite only — nothing in `src/` reads them at runtime, and `jsonschema` is a
dev-dependency.

## Upstream, pinned

Fetched from the OpenLineage repo at git tag **`1.24.2`**, which is the tag whose
`spec/OpenLineage.json` declares `"$id": "https://openlineage.io/spec/2-0-2/OpenLineage.json"`
— i.e. **core spec version 2-0-2**, the version this crate targets. Re-fetch and
diff with:

```
T=1.24.2
B=https://raw.githubusercontent.com/OpenLineage/OpenLineage/$T/spec
curl -sSf $B/OpenLineage.json                        # -> OpenLineage-2-0-2.json
curl -sSf $B/facets/ParentRunFacet.json              # -> facets/ParentRunFacet-1-0-1.json
curl -sSf $B/facets/SQLJobFacet.json                 # -> facets/SQLJobFacet-1-0-1.json
curl -sSf $B/facets/DatasetVersionDatasetFacet.json  # -> facets/DatasetVersionDatasetFacet-1-0-1.json
curl -sSf $B/facets/ErrorMessageRunFacet.json        # -> facets/ErrorMessageRunFacet-1-0-1.json
curl -sSf $B/facets/JobTypeJobFacet.json             # -> facets/JobTypeJobFacet-2-0-3.json
curl -sSf $B/facets/ProcessingEngineRunFacet.json    # -> facets/ProcessingEngineRunFacet-1-1-1.json
```

The version in each filename is the facet's own `$id` version at that tag — the
facets version independently of the core spec, so the tag alone does not identify
them. Only the facets this crate actually emits are vendored; adding a facet to
`src/event.rs` means vendoring its schema here too, or the compliance test cannot
see it.

The facet schemas `$ref` the core schema by its absolute `https://openlineage.io`
URL. The test registers the vendored `OpenLineage-2-0-2.json` under that URI
rather than dereferencing it, so validation stays offline.

## Our custom facets — `facets/1-0-0/Latiq*.json`

`latiq_identity`, `latiq_pond`, `latiq_query` and `latiq_parent_claim` are Latiq
facets (OpenLineage requires a prefix; ours is `latiq`). They exist as files so
that each facet's mandatory `_schemaURL` points at a real, immutable document:
the `$id` of each is its own path in this repo at the release tag, which is what
`src/event.rs` stamps into the events. That URL must never change for a given
facet version — a consumer treats `_schemaURL` as an opaque identity for the
facet's shape (Marquez and DataHub do not dereference it), so changing it renames
the facet as far as every downstream store is concerned. A breaking change to a
facet's fields gets a new `1-0-1/` directory, not an edit in place.
