#!/usr/bin/env bash
# Install BOTH wheels for one platform and actually drive them (#108).
#
# Called by `wheels.yml`'s `verify-linux-arm64` and `verify-macos-arm64` jobs
# with:
#   $V     the version the wheels were built as (`0.2.0`, `0.1.1.dev2026…`)
#   $PLAT  a substring of the platform tag to select — `manylinux_2_28_aarch64`
#          or `macosx`
# and `dist/latiq/` + `dist/latiq-admin/` already populated.
#
# Every assertion is on an ARTIFACT or a STRING, never on an exit status alone:
# the wheel for THIS platform must exist (and be the only match), the console
# script must be at the path pip generates, `latiq --version` must report the
# version we built, and `latiq serve` must serve a real client round-trip. A
# `--help` check would pass against a build that could not serve, and a `pip
# install` that silently resolved some OTHER platform's wheel would pass a check
# that only looked at exit codes.
#
# The two wheels install a `latiq` script EACH — they are two builds of one CLI —
# so they get one virtualenv each. Installing both into one env would leave
# whichever landed second on PATH and quietly test it twice.
set -euo pipefail

: "${V:?the wheel version must be passed as \$V}"
: "${PLAT:?the platform tag substring must be passed as \$PLAT}"

# The BINARY reports the plain workspace version (clap/CARGO_PKG_VERSION), while
# a nightly wheel is `<base>.dev<stamp>`. Derive rather than take a second input.
BASE="${V%%.dev*}"

pick() {   # <dir> — the one wheel in <dir> whose platform tag matches $PLAT
  local dir="$1" f
  local -a matches=()
  shopt -s nullglob
  for f in "$dir"/*.whl; do
    case "$f" in *"$PLAT"*) matches+=("$f") ;; esac
  done
  shopt -u nullglob
  # Exactly one: zero means this platform was never built (the #108 state), and
  # more than one means the selection is ambiguous and the check below would be
  # asserting against an arbitrary file.
  if [ "${#matches[@]}" -ne 1 ]; then
    echo "::error::expected exactly 1 ${PLAT} wheel in ${dir}, found ${#matches[@]}" >&2
    ls -1 "$dir" >&2 || true
    exit 1
  fi
  printf '%s' "${matches[0]}"
}

sdk_wheel=$(pick dist/latiq)
admin_wheel=$(pick dist/latiq-admin)
echo "SDK wheel:   ${sdk_wheel}"
echo "admin wheel: ${admin_wheel}"

# ---- 1. the `latiq` (SDK + full CLI) wheel -----------------------------------
echo "::group::pip install ${sdk_wheel}"
python -m venv /tmp/venv-sdk
# shellcheck disable=SC1091
. /tmp/venv-sdk/bin/activate
python -m pip install -U pip >/dev/null
# No `--platform`, no `--only-binary`, no index: the file itself, resolved by
# pip's own compatibility rules. pip REFUSES a wheel whose platform tag this
# machine does not support, so a mis-tagged wheel fails right here.
python -m pip install "$sdk_wheel"
echo "::endgroup::"

bin=$(command -v latiq) || { echo "::error::the latiq wheel installed no \`latiq\` script"; exit 1; }
echo "installed: $bin"
got=$(latiq --version)
[ "$got" = "latiq ${BASE}" ] || { echo "::error::\`latiq --version\` says '${got}', expected 'latiq ${BASE}'"; exit 1; }

# The Python API: importing the extension module is what actually loads the
# native code for this arch into this interpreter.
python -c "import latiq, sys; print('import latiq OK from', latiq.__file__)"

# And the server the wheel carries, proved by a CLIENT round-trip rather than an
# open socket: `pond list` drives Admin gRPC against the control plane this wheel
# just started.
latiq serve --bind 127.0.0.1 --port 51466 --root "${RUNNER_TEMP:-/tmp}/cp" &
serve_pid=$!
for _ in $(seq 1 60); do
  (exec 3<>/dev/tcp/127.0.0.1/51466) 2>/dev/null && break
  sleep 1
done
if ! out=$(LATIQ_SERVER=http://127.0.0.1:51466 latiq pond list); then
  echo "::error::\`latiq pond list\` could not reach the control plane the wheel started"
  kill "$serve_pid" 2>/dev/null || true
  exit 1
fi
echo "pond list: ${out}"
kill "$serve_pid" 2>/dev/null || true

# The wheel's own suite — the Python API a user calls (`connect(\"local\")`,
# allocate/write/read, the lineage flag). Run from the repo root against the
# INSTALLED wheel, not a source tree. pytest exits 5 when it collects nothing,
# so a moved suite fails here instead of passing silently.
python -m pip install pytest >/dev/null
pytest sdk/python/tests -v
deactivate

# ---- 2. the `latiq-admin` (lean operator CLI) wheel ---------------------------
echo "::group::pip install ${admin_wheel}"
python -m venv /tmp/venv-admin
# shellcheck disable=SC1091
. /tmp/venv-admin/bin/activate
python -m pip install -U pip >/dev/null
python -m pip install "$admin_wheel"
echo "::endgroup::"

bin=$(command -v latiq) || { echo "::error::no \`latiq\` on PATH after installing latiq-admin"; exit 1; }
echo "installed: $bin"
# A native executable, not a Python shim: `bindings = "bin"` ships the compiled
# binary itself, and this channel promises exactly that.
file "$bin"
got=$(latiq --version)
[ "$got" = "latiq ${BASE}" ] || { echo "::error::\`latiq --version\` says '${got}', expected 'latiq ${BASE}'"; exit 1; }

# It is the LEAN build: `latiq serve` must FAIL with OUR packaging refusal. In
# the full build it would bind a port and never return, so the inverted status is
# the assertion — and the message must be ours, not clap's or a bind error.
if out=$(latiq serve --bind 127.0.0.1 --port 51467 2>&1); then
  echo "::error::\`latiq serve\` succeeded in latiq-admin — this wheel is not the client-only build"
  exit 1
fi
echo "$out"
echo "$out" | grep -q 'needs the full build' \
  || { echo "::error::not the packaging refusal we ship"; exit 1; }
echo "$out" | grep -q 'pip install latiq' \
  || { echo "::error::the refusal must name the install that CAN serve"; exit 1; }
deactivate

echo "OK: both ${PLAT} wheels install and run on $(uname -m)"
