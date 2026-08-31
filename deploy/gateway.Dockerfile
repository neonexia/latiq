# The Latiq query gateway image: nginx with the front-door config baked in, so the
# user-facing compose (deploy/docker-compose.yml) is pure images + ports — no
# inline `configs:` and no file mounts. That makes it
# runtime-agnostic: identical under `docker compose` and `podman compose`.
#
# The baked config IS deploy/cluster/nginx.conf (the same file the internal test
# compose mounts), so there is one gateway-config source of truth.
#
# Build (from the repo root):  docker build -f deploy/gateway.Dockerfile -t ghcr.io/neonexia/latiq-gateway:dev deploy/cluster
FROM nginx:1.27
COPY nginx.conf /etc/nginx/nginx.conf
