#!/usr/bin/env bash
# The ONE list of platforms every published wheel set must cover, and the two
# ways of checking it (#108).
#
# WHY THIS IS A SCRIPT and not two copies of a `run:` block: it is called from
# three places — `wheels.yml` (over the freshly built `dist/`), `nightly.yml` and
# `release.yml` (over what PyPI actually reports, after an upload). A check that
# lives in three copies drifts, and the copy that drifts is the one that stops
# catching anything.
#
#   assert-wheel-platforms.sh dist <dir> <latiq|latiq-admin> <version>
#   assert-wheel-platforms.sh pypi <latiq|latiq-admin> <version>
#
# `dist` asserts over the files on disk; `pypi` asserts over the filenames the
# PyPI JSON API reports for that exact version — i.e. what `pip install` will
# have to resolve from. Both fail if ANY expected platform is missing, and both
# fail if they examined ZERO files: this repo has shipped CI steps that were
# green while doing nothing, and "the loop found nothing to check" is that shape.
set -euo pipefail

# `latiq` 0.1.0 shipped exactly one file — manylinux x86_64 — so `pip install
# latiq` had nothing to resolve on an Apple Silicon Mac and, with no sdist, no
# source fallback either. These three are the platforms developers actually run.
# ADD TO THIS LIST and the build matrix in `wheels.yml` together; the assertion
# is worth nothing if it only knows about the platforms we already build.
EXPECTED_TAGS=(
  'manylinux_2_28_x86_64'
  'manylinux_2_28_aarch64'
  'macosx_[0-9_]+_arm64'
)

die() { echo "::error::$*" >&2; exit 1; }

# PyPI's file naming: the distribution name is normalised, `-` → `_`.
dist_prefix() {
  printf '%s' "${1//-/_}"
}

# --- the two sources of filenames -------------------------------------------

list_dist() {   # <dir>
  local dir="$1"
  [ -d "$dir" ] || die "no such directory: ${dir}"
  # Basenames only (the PyPI branch has nothing else), and no `find -printf`:
  # that is GNU-only and this script also runs on macOS runners.
  (cd "$dir" && ls -1) | grep '\.whl$' | sort || true
}

list_pypi() {   # <project> <version>
  local project="$1" version="$2" url body
  url="https://pypi.org/pypi/${project}/${version}/json"
  # Freshly uploaded files can take a moment to appear in the JSON API; a single
  # 404 here would otherwise read as "we published nothing".
  for i in $(seq 1 12); do
    if body=$(curl -fsSL "$url" 2>/dev/null); then
      printf '%s' "$body" | jq -r '.urls[].filename'
      return 0
    fi
    echo "waiting for PyPI to serve ${project} ${version} (attempt ${i})" >&2
    sleep 10
  done
  die "PyPI never served ${project} ${version} (${url})"
}

# --- the assertion ------------------------------------------------------------

assert_files() {   # <project> <version> <<< filenames on stdin
  local project="$1" version="$2"
  local prefix files n fail=0
  prefix="$(dist_prefix "$project")-${version}-"
  files=$(cat)

  n=$(printf '%s\n' "$files" | grep -c '\.whl$' || true)
  [ "$n" -gt 0 ] || die "examined 0 wheels for ${project} ${version} — nothing was asserted"

  echo "wheels for ${project} ${version} (${n}):"
  printf '%s\n' "$files" | sed 's/^/  /'

  # Every file must be THIS project at THIS version. Catches a missed version
  # stamp and a dist/ that quietly picked up someone else's build.
  while IFS= read -r f; do
    [ -n "$f" ] || continue
    case "$f" in
      "${prefix}"*) ;;
      *) echo "::error::${f} is not ${prefix}*"; fail=1 ;;
    esac
  done <<< "$files"

  for tag in "${EXPECTED_TAGS[@]}"; do
    if printf '%s\n' "$files" | grep -Eq "^${prefix}.*-${tag}\.whl$"; then
      echo "OK   ${project}: ${tag}"
    else
      echo "::error::${project} ${version} has no wheel for ${tag}"
      fail=1
    fi
  done

  [ "$fail" = "0" ] || die "${project} ${version} does not cover every required platform"
  echo "OK: ${project} ${version} covers all ${#EXPECTED_TAGS[@]} required platforms"
}

mode="${1:-}"
case "$mode" in
  dist)
    [ "$#" -eq 4 ] || die "usage: $0 dist <dir> <project> <version>"
    list_dist "$2" | assert_files "$3" "$4"
    ;;
  pypi)
    [ "$#" -eq 3 ] || die "usage: $0 pypi <project> <version>"
    list_pypi "$2" "$3" | assert_files "$2" "$3"
    ;;
  *)
    die "usage: $0 dist <dir> <project> <version> | $0 pypi <project> <version>"
    ;;
esac
