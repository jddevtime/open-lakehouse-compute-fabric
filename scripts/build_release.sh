#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
#
# Build everything a release consists of, and verify it:
#
#   dist/olcf-<v>-py3-none-any.whl   dist/olcf-<v>.tar.gz   dist/olcf-<v>.cdx.json (CycloneDX SBOM)           (the server)
#   dist/olcf_fabric-<v>-py3-none-any.whl   dist/olcf_fabric-<v>.tar.gz   dist/olcf_fabric-<v>.cdx.json         (the Fabric SDK, sdk/fabric)
#   dist/THIRD_PARTY_NOTICES.md   dist/SHA256SUMS
#
# Steps: release identity preflight (warning, or an error with --require-identity) -> version consistency (server, module, Fabric SDK) -> uv build of both
# distributions (wheel + sdist each) -> scripts/verify_dist.py gate -> install each wheel (the server with extras, wheels only) into its own throw-away
# virtualenv and run it -> SBOMs -> third-party notices (scripts/licenses.py) -> SHA256SUMS (and re-verify it).
# Signing (sigstore/cosign) and publishing happen in .github/workflows/release.yml; this script only produces and checks the artifacts.
#
# Usage: bash scripts/build_release.sh [--out DIR] [--extras LIST] [--no-sbom] [--no-install-test] [--update-notices] [--require-identity]
#   --out DIR           write the artifacts to DIR, which must not exist or must be empty (a non-empty directory is an error, never deleted)
#   --extras LIST       extras installed for the SBOM / notices environment (default: all)
#   --update-notices    also write the generated notices to ./THIRD_PARTY_NOTICES.md
#   --require-identity  fail when scripts/check_release_config.py finds unresolved owner/repository placeholders (the release workflow passes it);
#                       without it the findings are printed as a warning so that local development builds keep working
# Needs: uv (https://docs.astral.sh/uv/), network access for the isolated build and the throw-away environments.  No compiler is needed.

set -euo pipefail

ROOT=$(cd "$(dirname "$0")/.." && pwd)
OUT="$ROOT/dist"
EXTRAS="all"
SBOM=1
INSTALL_TEST=1
UPDATE_NOTICES=0
REQUIRE_IDENTITY=0

while [ "$#" -gt 0 ]; do
  case "$1" in
    --out)              [ "$#" -ge 2 ] || { echo "--out needs a directory" >&2; exit 2; }; OUT=$(mkdir -p "$2" && cd "$2" && pwd); shift 2;;
    --extras)           EXTRAS=$2; shift 2;;
    --no-sbom)          SBOM=0; shift;;
    --no-install-test)  INSTALL_TEST=0; shift;;
    --update-notices)   UPDATE_NOTICES=1; shift;;
    --require-identity) REQUIRE_IDENTITY=1; shift;;
    -h|--help)          sed -n '2,/^set -euo/p' "$0" | sed '$d;s/^# \{0,1\}//' >&2; exit 0;;
    *)                  echo "unknown option: $1" >&2; exit 2;;
  esac
done

say() { printf '==> %s\n' "$*" >&2; }
die() { printf 'error: %s\n' "$*" >&2; exit 1; }

command -v uv >/dev/null 2>&1 || die "uv is required (https://docs.astral.sh/uv/getting-started/installation/)"

# --- 0. the output directory is never cleaned: a release directory that already holds files is somebody's earlier build or something else entirely
if [ -n "$(ls -A "$OUT" 2>/dev/null)" ]; then
  die "output directory $OUT is not empty; choose a new or empty directory (nothing was changed or deleted)"
fi

# --- 0b. release identity (owner/repository placeholders in the public files): a warning for a development build, an error for a release
if ! python3 "$ROOT/scripts/check_release_config.py" >&2; then
  if [ "$REQUIRE_IDENTITY" = 1 ]; then
    die "release identity metadata is incomplete (see above); a release build needs it resolved (--require-identity)"
  fi
  say "warning: release identity metadata is incomplete (see above); fine for a development build, a release needs it (--require-identity)"
fi

# --- reproducible timestamps: SOURCE_DATE_EPOCH from the environment, else the last commit (git is only run where it is a real tool, never a macOS stub)
if [ -z "${SOURCE_DATE_EPOCH:-}" ] && [ -d "$ROOT/.git" ] && command -v git >/dev/null 2>&1; then
  if [ "$(uname -s)" != Darwin ] || /usr/bin/xcode-select -p >/dev/null 2>&1; then
    SOURCE_DATE_EPOCH=$(git -C "$ROOT" log -1 --format=%ct 2>/dev/null || true)
    [ -z "$SOURCE_DATE_EPOCH" ] || export SOURCE_DATE_EPOCH
  fi
fi

# --- 1. versions
VERSION=$(sed -n 's/^version *= *"\([^"]*\)".*/\1/p' "$ROOT/pyproject.toml" | head -n 1)
MODVER=$(sed -n 's/^__version__ *= *"\([^"]*\)".*/\1/p' "$ROOT/src/olcf/version.py" | head -n 1)
SDKVER=$(sed -n 's/^__version__ *= *"\([^"]*\)".*/\1/p' "$ROOT/sdk/fabric/olcf_fabric/__init__.py" | head -n 1)
[ -n "$VERSION" ] || die "could not read the version from pyproject.toml"
[ "$VERSION" = "$MODVER" ] || die "pyproject.toml says $VERSION but src/olcf/version.py says $MODVER - make them equal before releasing"
[ "$VERSION" = "$SDKVER" ] || die "the server is $VERSION but sdk/fabric/olcf_fabric/__init__.py says '$SDKVER' - the Fabric SDK is versioned with the server"
if [ -n "${GITHUB_REF_NAME:-}" ] && [ "${GITHUB_REF_TYPE:-}" = tag ]; then
  [ "${GITHUB_REF_NAME#v}" = "$VERSION" ] || die "tag $GITHUB_REF_NAME does not match version $VERSION"
fi
[ -f "$ROOT/README.md" ] || die "README.md is missing (pyproject.toml names it as the package readme; the build cannot succeed without it)"
for f in LICENSE NOTICE; do
  [ -f "$ROOT/sdk/fabric/$f" ] || die "sdk/fabric/$f is missing (the Fabric SDK distribution must ship the Apache license and notice)"
done
cmp -s "$ROOT/LICENSE" "$ROOT/sdk/fabric/LICENSE" || die "sdk/fabric/LICENSE differs from ./LICENSE (both are the Apache-2.0 text)"
say "building olcf and olcf-fabric $VERSION into $OUT"

# --- 2. build
(cd "$ROOT" && uv build --out-dir "$OUT" >&2)
WHEEL="$OUT/olcf-$VERSION-py3-none-any.whl"
SDIST="$OUT/olcf-$VERSION.tar.gz"
[ -f "$WHEEL" ] && [ -f "$SDIST" ] || die "uv build did not produce $WHEEL and $SDIST"
(cd "$ROOT" && uv build --out-dir "$OUT" sdk/fabric >&2)
SDK_WHEEL="$OUT/olcf_fabric-$VERSION-py3-none-any.whl"
SDK_SDIST="$OUT/olcf_fabric-$VERSION.tar.gz"
[ -f "$SDK_WHEEL" ] && [ -f "$SDK_SDIST" ] || die "uv build did not produce $SDK_WHEEL and $SDK_SDIST"
rm -f "$OUT/.gitignore"

# --- 3. gate
say "verifying the artifacts"
python3 "$ROOT/scripts/verify_dist.py" "$OUT" --require-sdk || die "artifact verification failed"

# --- 4. install test + SBOM + notices from one throw-away environment
TMP=$(mktemp -d "${TMPDIR:-/tmp}/olcf-release.XXXXXX")
trap 'rm -rf "$TMP"' EXIT
if [ "$INSTALL_TEST" = 1 ] || [ "$SBOM" = 1 ]; then
  say "installing the wheel (wheels only, extras: $EXTRAS) into a throw-away environment"
  uv venv -q --python 3.13 --python-preference only-managed "$TMP/venv" >&2
  PY="$TMP/venv/bin/python"
  uv pip install -q --python "$PY" --only-binary :all: "olcf[$EXTRAS] @ file://$WHEEL" >&2
  GOT=$("$PY" -m olcf.cli.main --json version | sed -n 's/.*"olcf": *"\([^"]*\)".*/\1/p' | head -n 1)
  [ "$GOT" = "$VERSION" ] || die "installed wheel reports version '$GOT', expected '$VERSION'"
  OLCF_DATA_DIR="$TMP/data" OLCF_CONFIG_DIR="$TMP/cfg" "$PY" -m olcf.cli.main -q init --no-cli-context --port 18999 >/dev/null
  say "install test passed (olcf version $GOT, olcf init)"

  # the Fabric SDK in its own environment: it must install and import without the server (it is attached to a Fabric Environment on its own)
  say "installing the Fabric SDK wheel into its own throw-away environment"
  uv venv -q --python 3.13 --python-preference only-managed "$TMP/sdkvenv" >&2
  uv pip install -q --python "$TMP/sdkvenv/bin/python" --only-binary :all: "olcf-fabric @ file://$SDK_WHEEL" >&2
  GOT=$("$TMP/sdkvenv/bin/python" -I -c 'import sys, olcf_fabric; assert "olcf" not in sys.modules, "the SDK imported the server package"; print(olcf_fabric.__version__)')
  [ "$GOT" = "$VERSION" ] || die "installed Fabric SDK reports version '$GOT', expected '$VERSION'"
  say "SDK install test passed (olcf_fabric $GOT imports without the server)"
fi

if [ "$SBOM" = 1 ]; then
  say "writing the CycloneDX SBOM"
  uvx --from cyclonedx-bom cyclonedx-py environment --pyproject "$ROOT/pyproject.toml" --mc-type application --sv 1.6 --of JSON \
      --output-reproducible -o "$OUT/olcf-$VERSION.cdx.json" "$TMP/venv/bin/python" >&2
  [ -s "$OUT/olcf-$VERSION.cdx.json" ] || die "SBOM was not written"
  uvx --from cyclonedx-bom cyclonedx-py environment --pyproject "$ROOT/sdk/fabric/pyproject.toml" --mc-type library --sv 1.6 --of JSON \
      --output-reproducible -o "$OUT/olcf_fabric-$VERSION.cdx.json" "$TMP/sdkvenv/bin/python" >&2
  [ -s "$OUT/olcf_fabric-$VERSION.cdx.json" ] || die "SDK SBOM was not written"
  say "writing THIRD_PARTY_NOTICES.md"
  "$TMP/venv/bin/python" "$ROOT/scripts/licenses.py" --python "$TMP/venv/bin/python" --out "$OUT/THIRD_PARTY_NOTICES.md" --check >&2
  [ "$UPDATE_NOTICES" = 0 ] || cp "$OUT/THIRD_PARTY_NOTICES.md" "$ROOT/THIRD_PARTY_NOTICES.md"
fi

# --- 5. checksums
say "writing SHA256SUMS"
cd "$OUT"
FILES=$(ls -1 | grep -v '^SHA256SUMS$' | LC_ALL=C sort)
if command -v sha256sum >/dev/null 2>&1; then
  # shellcheck disable=SC2086
  sha256sum $FILES > SHA256SUMS
  sha256sum -c SHA256SUMS >&2
else
  # shellcheck disable=SC2086
  shasum -a 256 $FILES > SHA256SUMS
  shasum -a 256 -c SHA256SUMS >&2
fi
say "release artifacts:"
ls -l "$OUT" >&2
