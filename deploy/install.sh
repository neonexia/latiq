#!/bin/sh
# Install the Latiq admin CLI (a small, client-only `latiq` — no server/DuckDB).
#   curl -fsSL https://raw.githubusercontent.com/neonexia/latiq/main/deploy/install.sh | sh
# Then:  export LATIQ_SERVER=http://your-control-plane:51400 && latiq stats
#
# Override the install dir with LATIQ_BIN_DIR (default ~/.local/bin), and the
# release source with LATIQ_RELEASE_REPO / LATIQ_RELEASE_TAG.
set -eu

# Where the prebuilt CLI binaries are published. `cli-latest` is a ROLLING release
# in this repo, refreshed by the nightly's `publish-cli` job and by every tagged
# release (.github/workflows/nightly.yml + release.yml) — change those and this
# together, or the installer and the publisher disagree.
# Pin a specific version instead with  LATIQ_RELEASE_TAG=v0.2.0.
repo="${LATIQ_RELEASE_REPO:-neonexia/latiq}"
tag="${LATIQ_RELEASE_TAG:-cli-latest}"

os=$(uname -s)
arch=$(uname -m)
case "$os" in
  Darwin) os=darwin ;;
  Linux)  os=linux ;;
  *) echo "latiq: unsupported OS '$os' (macOS/Linux only)"; exit 1 ;;
esac
case "$arch" in
  arm64|aarch64) arch=arm64 ;;
  x86_64|amd64)  arch=x86_64 ;;
  *) echo "latiq: unsupported arch '$arch'"; exit 1 ;;
esac

asset="latiq-${os}-${arch}"
url="https://github.com/${repo}/releases/download/${tag}/${asset}"
dir="${LATIQ_BIN_DIR:-$HOME/.local/bin}"

echo "latiq: downloading ${asset}"
mkdir -p "$dir"
curl -fSL "$url" -o "$dir/latiq"
chmod +x "$dir/latiq"
echo "latiq: installed to $dir/latiq"

case ":$PATH:" in
  *":$dir:"*) : ;;
  *) echo "latiq: add $dir to your PATH, e.g.  export PATH=\"$dir:\$PATH\"" ;;
esac
echo "latiq: try  export LATIQ_SERVER=http://your-control-plane:51400 && latiq stats"
