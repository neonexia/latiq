# Latiq — Roadmap

*Feature status, releases, and horizons. Kept out of [`product.md`](product.md) on
purpose — positioning is stable, this evolves. **Timeframes are approximate**:
"Now / Next / Later" are horizons, not committed dates. Items are tracked as
GitHub issues; this table is the human-readable summary.*

**Status:** ✅ Shipped · 🚧 In progress · 📋 Planned · 💤 Deferred
**Timeframe:** Now = in M1 today · Next ≈ the M2 slice · Later = beyond M2

| Feature | Timeframe | Release | Status |
|---|---|---|---|
| Pond primitive — allocate / query / collaborate / drop (pure DuckLake, DuckDB engine, one instance per pond) | Now | M1 | ✅ Shipped |
| Multi-node scale-out — control-plane registry, node registration + liveness reaping, nginx gateway front door, owner-node forwarding | Now | M1 | ✅ Shipped |
| Datasets (curated files, copied in) + catalogs (external, transiently pulled; Iceberg first; no stored creds) | Now | M1 | ✅ Shipped |
| Per-pond resource tiers → engine memory / thread caps | Now | M1 | ✅ Shipped |
| Explain / cost estimation before running a query | Now | M1 | ✅ Shipped |
| Multi-agent collaboration — native DuckLake attribution + conflict retry | Now | M1 | ✅ Shipped |
| MCP surface — tools, `latiq://` guidance resources, prompt SOPs | Now | M1 | ✅ Shipped |
| Python SDK — embedded + cluster modes, Arrow results, uncapped streaming reads | Now | M1 | ✅ Shipped |
| Admin CLI (client-only build) + compose deployment (Docker / Podman) | Now | M1 | ✅ Shipped |
| Observability — Prometheus `/metrics`, structured JSON logs, `trace_id` propagation, `latiq::access` trail | Now | M1 | ✅ Shipped |
| Distribution — GHCR images (`latiq` + `latiq-gateway`), PyPI wheel, native CLI binaries; nightly test-gated + change-gated publish | Now | M1 | ✅ Shipped |
| **Identity v0 — authentication** — MCP-spec OAuth 2.1 resource server (enterprise IdP: Okta / Auth0 / Entra), multi-issuer local JWKS + audience verification, RFC 9728 discovery, one verifier across MCP + Data + Admin, identity out of the MCP tool args and into the transport, token replayed on the node hop (see [`identity.md`](identity.md), #5) | Now | M1 | ✅ Shipped |
| **Lineage** — OpenLineage `2-0-2` events per query, opt-in per pond, written as JSONL in the pond's own directory and read back with the `get_lineage` MCP tool; inputs/outputs from the bound plan, versioned by native DuckLake snapshots, identity (verified subject + claimed agent) on every event; optional OpenLineage HTTP backend (see [`lineage.md`](lineage.md)) | Now | M1 | ✅ Shipped |
| **Authorization** — pond ownership + grants bound to the verified subject, catalog grants, filtered discovery, group/role claims (deliberately split out of identity v0; #72) | Next | M2 | 📋 Planned |
| Pond lifecycle enforcement — reap expired ponds; ownership + release authority move to the run | Next | M2 | 📋 Planned |
| Placement policy — which node a new pond lands on (binpacking on a busy cluster) | Next | M2 | 📋 Planned |
| Rate limiting per principal | Next | M2 | 📋 Planned |
| Multi-arch container images (arm64 + amd64) — #66 | Next | M2 | 🚧 In progress |
| OTLP trace export to a collector (trace ids live in logs today) | Later | — | 📋 Planned |
| Kubernetes deployment as a first-class artifact | Later | — | 📋 Planned |
| Arrow Flight SQL streaming for large result sets (M1 Data gRPC is unary + inline-capped) | Later | — | 💤 Deferred |
| Streaming ingestion | Later | — | 💤 Deferred |
| DataFusion engine option (alternative to DuckDB) | Later | — | 💤 Deferred |
| Open-source readiness — public repo, crates.io — #55 | Later | — | 📋 Planned |
