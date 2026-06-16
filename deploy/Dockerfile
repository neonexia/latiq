# Single-binary Latiq image (all roles: control-plane via `serve`, pond-node via
# `node add`, and the CLI). DuckDB is compiled from source on first build (slow),
# so we cache the cargo registry + target dir via BuildKit cache mounts.
#
#   docker build -f deploy/Dockerfile -t ghcr.io/neonexia/latiq:dev .
#
# The runtime stage bakes the DuckDB extensions (ducklake/httpfs/iceberg/…) into
# the image's cache via `latiq warm-extensions`, so nodes start without network.

# ---- builder ---------------------------------------------------------------
FROM rust:1-bookworm AS builder
RUN apt-get update && apt-get install -y --no-install-recommends \
        protobuf-compiler ca-certificates \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /src
COPY . .
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/src/target \
    cargo build --release -p latiq && \
    cp /src/target/release/latiq /usr/local/bin/latiq

# ---- runtime ---------------------------------------------------------------
FROM debian:bookworm-slim AS runtime
RUN apt-get update && apt-get install -y --no-install-recommends \
        ca-certificates curl \
    && rm -rf /var/lib/apt/lists/*
COPY --from=builder /usr/local/bin/latiq /usr/local/bin/latiq

# Bake the DuckDB extensions into the image cache (INSTALL hits the network here,
# once, at build time — nodes then start offline). HOME drives the cache path
# (~/.duckdb/extensions/<version>/<platform>/).
ENV HOME=/root
RUN latiq warm-extensions

# Data root for the registry (control plane) / pond storage (pond node). There is
# no LATIQ_ROOT env — the root is set with the `--root` flag (see compose).
RUN mkdir -p /var/lib/latiq
WORKDIR /var/lib/latiq

# No default CMD — compose/k8s pick the role:
#   control plane : latiq serve --bind 0.0.0.0 --root /var/lib/latiq
#   pond node     : latiq node add --bind 0.0.0.0 --advertise-addr <host>:<port> ...
ENTRYPOINT ["latiq"]
