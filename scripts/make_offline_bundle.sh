#!/bin/sh
# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
#
# Build an offline bundle for `scripts/install.sh --offline-bundle DIR` (air-gapped machines).  Run it on any machine WITH network access that has
# curl, tar and either `uv` on PATH or --uv-bin; the target platform may differ from the machine it runs on.
#
#   sh scripts/make_offline_bundle.sh --wheel dist/olcf-0.1.0-py3-none-any.whl --platform macos-arm64 --out olcf-bundle-macos-arm64
#   tar -czf olcf-bundle-macos-arm64.tar.gz olcf-bundle-macos-arm64        # carry it over, unpack it, then:
#   sh olcf-bundle-macos-arm64/install.sh --offline-bundle olcf-bundle-macos-arm64
#
# Bundle layout (what install.sh expects):
#   uv                      the static uv binary for the target (SHA-256 verified when downloaded)
#   wheels/                 olcf-<v>-py3-none-any.whl and every dependency as a wheel (nothing is ever compiled on the target)
#   python/<tag>/<file>     python-build-standalone archive(s), laid out as a mirror for UV_PYTHON_INSTALL_MIRROR
#   runtime/*.olcfrt        optional: a prebuilt Spark runtime bundle (olcf runtime export); installs without network access
#                           (its manifest signature is checked on the TARGET against that machine's runtime trust anchor: a bundle made from a bundled, development-signed
#                           manifest imports on a `olcf init` machine (environment = "development"); for production targets build it from an operator-signed manifest and
#                           put the public key in OLCF_RUNTIME_TRUST_DIR there - the installer runs `olcf init` before the runtime step)
#   install.sh, uninstall.sh   copies of the installer and uninstaller
#   MANIFEST.sha256         checksums of every file above (verify with: cd DIR && shasum -a 256 -c MANIFEST.sha256)
set -eu

UV_VERSION="${OLCF_UV_VERSION:-0.11.32}"
PYTHON_VERSION="${OLCF_PYTHON_VERSION:-3.13}"
PLATFORM=""
OUT=""
WHEEL=""
EXTRAS="${OLCF_EXTRAS:-}"
UV_BIN=""
RUNTIME_BUNDLE=""

die() { printf 'error: %s\n' "$*" >&2; exit 1; }
say() { printf '%s\n' "$*" >&2; }

while [ "$#" -gt 0 ]; do
  case "$1" in
    --wheel) WHEEL=$2; shift 2;;
    --out) OUT=$2; shift 2;;
    --platform) PLATFORM=$2; shift 2;;
    --extras) EXTRAS=$2; shift 2;;
    --python) PYTHON_VERSION=$2; shift 2;;
    --uv-bin) UV_BIN=$2; shift 2;;
    --runtime-bundle) RUNTIME_BUNDLE=$2; shift 2;;
    -h|--help) sed -n '5,22p' "$0" | sed 's/^# \{0,1\}//'; exit 0;;
    *) die "unknown option: $1 (try --help)";;
  esac
done
[ -n "$WHEEL" ] && [ -f "$WHEEL" ] || die "--wheel FILE is required (build one with scripts/build_release.sh)"
[ -n "$OUT" ] || die "--out DIR is required"
[ ! -e "$OUT" ] || [ -z "$(ls -A "$OUT" 2>/dev/null)" ] || die "$OUT exists and is not empty"
case "$(basename -- "$WHEEL")" in olcf-*-*.whl) ;; *) die "'$WHEEL' is not an olcf wheel";; esac

if [ -z "$PLATFORM" ]; then
  case "$(uname -s)-$(uname -m)" in
    Linux-x86_64) PLATFORM=linux-x86_64;; Linux-aarch64|Linux-arm64) PLATFORM=linux-aarch64;;
    Darwin-arm64) PLATFORM=macos-arm64;; Darwin-x86_64) PLATFORM=macos-x86_64;;
    *) die "cannot detect the platform; pass --platform linux-x86_64|linux-aarch64|macos-arm64|macos-x86_64";;
  esac
fi
case "$PLATFORM" in
  linux-x86_64)  TRIPLE=x86_64-unknown-linux-gnu;  UVPLAT=x86_64-manylinux_2_28;  PIPPLAT="--platform manylinux_2_28_x86_64 --platform manylinux_2_17_x86_64 --platform manylinux2014_x86_64 --platform linux_x86_64";;
  linux-aarch64) TRIPLE=aarch64-unknown-linux-gnu; UVPLAT=aarch64-manylinux_2_28; PIPPLAT="--platform manylinux_2_28_aarch64 --platform manylinux_2_17_aarch64 --platform manylinux2014_aarch64 --platform linux_aarch64";;
  macos-arm64)   TRIPLE=aarch64-apple-darwin;      UVPLAT=aarch64-apple-darwin;   PIPPLAT="--platform macosx_14_0_arm64 --platform macosx_13_0_arm64 --platform macosx_12_0_arm64 --platform macosx_11_0_arm64";;
  macos-x86_64)  TRIPLE=x86_64-apple-darwin;       UVPLAT=x86_64-apple-darwin;    PIPPLAT="--platform macosx_14_0_x86_64 --platform macosx_13_0_x86_64 --platform macosx_12_0_x86_64 --platform macosx_11_0_x86_64 --platform macosx_10_13_x86_64";;
  *) die "unsupported platform '$PLATFORM'";;
esac
PYTAG="cp$(printf '%s' "$PYTHON_VERSION" | tr -d .)"

TMP=$(mktemp -d "${TMPDIR:-/tmp}/olcf-bundle.XXXXXX")
trap 'rm -rf "$TMP"' EXIT INT TERM
sha256_of() { if command -v sha256sum >/dev/null 2>&1; then sha256sum "$1" | cut -d' ' -f1; else shasum -a 256 "$1" | cut -d' ' -f1; fi; }

mkdir -p "$OUT/wheels" "$OUT/python" "$OUT/runtime"
HERE=$(CDPATH='' cd -- "$(dirname -- "$0")" && pwd)

# ---- uv for the target
if [ -n "$UV_BIN" ]; then UV_TARGET_BIN=$UV_BIN; else
  name="uv-$TRIPLE.tar.gz"; url="https://github.com/astral-sh/uv/releases/download/$UV_VERSION/$name"
  say "==> downloading uv $UV_VERSION for $PLATFORM"
  curl -fsSL -o "$TMP/$name" "$url" && curl -fsSL -o "$TMP/$name.sha256" "$url.sha256" || die "download failed: $url"
  want=$(cut -d' ' -f1 "$TMP/$name.sha256" | tr 'A-F' 'a-f'); got=$(sha256_of "$TMP/$name")
  [ -n "$want" ] && [ "$want" = "$got" ] || die "SHA-256 mismatch for $name"
  mkdir -p "$TMP/uvx" && tar -xzf "$TMP/$name" -C "$TMP/uvx" --strip-components=1
  UV_TARGET_BIN="$TMP/uvx/uv"
fi
[ -f "$UV_TARGET_BIN" ] || die "no uv binary for the bundle"
cp "$UV_TARGET_BIN" "$OUT/uv"; chmod 755 "$OUT/uv"

# ---- a uv that runs HERE (for resolving and downloading)
if [ -n "$UV_BIN" ] && "$UV_BIN" --version >/dev/null 2>&1; then UVH=$UV_BIN
elif command -v uv >/dev/null 2>&1; then UVH=$(command -v uv)
elif "$UV_TARGET_BIN" --version >/dev/null 2>&1; then UVH=$UV_TARGET_BIN
else die "need a uv that runs on this machine (install uv or pass --uv-bin)"; fi

# ---- wheels: resolve for the target, then download each pinned wheel for the target's tags
say "==> resolving dependencies for $PLATFORM / Python $PYTHON_VERSION (wheels only)"
cp "$WHEEL" "$OUT/wheels/"
WHEEL_ABS=$(CDPATH='' cd -- "$(dirname -- "$WHEEL")" && pwd)/$(basename -- "$WHEEL")
SPEC="olcf${EXTRAS:+[$EXTRAS]} @ file://$WHEEL_ABS"
printf '%s\n' "$SPEC" > "$TMP/in.txt"
"$UVH" pip compile "$TMP/in.txt" --python-version "$PYTHON_VERSION" --python-platform "$UVPLAT" --only-binary :all: --no-header --no-annotate \
  --no-emit-package olcf -o "$TMP/pins.txt" >&2 || die "dependency resolution failed (a dependency without a wheel for $PLATFORM?)"
"$UVH" venv --python "$PYTHON_VERSION" "$TMP/venv" >&2 || die "could not create a helper environment (Python $PYTHON_VERSION)"
"$UVH" pip install --python "$TMP/venv/bin/python" pip >&2
say "==> downloading $(grep -c '==' "$TMP/pins.txt") wheels"
# shellcheck disable=SC2086
"$TMP/venv/bin/python" -m pip download --disable-pip-version-check --no-deps --only-binary :all: -r "$TMP/pins.txt" \
  --python-version "$PYTHON_VERSION" --implementation cp --abi "$PYTAG" --abi abi3 --abi none $PIPPLAT -d "$OUT/wheels" >&2 \
  || die "downloading wheels failed"

# ---- Python (python-build-standalone) as a mirror tree for UV_PYTHON_INSTALL_MIRROR
say "==> fetching Python $PYTHON_VERSION for $PLATFORM"
"$UVH" python list "$PYTHON_VERSION" --only-downloads --all-platforms --output-format json > "$TMP/pythons.json" 2>/dev/null || die "could not list Python downloads"
URL=$("$TMP/venv/bin/python" -I - "$TMP/pythons.json" "$TRIPLE" "$PYTHON_VERSION" <<'PY'
import json, sys
rows = json.load(open(sys.argv[1], encoding="utf-8"))
triple = sys.argv[2]
want = {"x86_64-unknown-linux-gnu": ("linux", "x86_64", "gnu"), "aarch64-unknown-linux-gnu": ("linux", "aarch64", "gnu"),
        "aarch64-apple-darwin": ("macos", "aarch64", None), "x86_64-apple-darwin": ("macos", "x86_64", None)}[triple]
best = None
for r in rows:
    if not r.get("url") or r.get("variant", "default") != "default":
        continue
    os_, arch, libc = (r.get("os") or ""), (r.get("arch") or ""), (r.get("libc") or "")
    if os_ == want[0] and arch == want[1] and (want[2] is None or libc == want[2]):
        ver = tuple(r["version_parts"].values()) if isinstance(r.get("version_parts"), dict) else (0,)
        if best is None or ver > best[0]:
            best = (ver, r["url"])
print(best[1] if best else "")
PY
)
[ -n "$URL" ] || die "no python-build-standalone download found for $PLATFORM"
REL=${URL#*/releases/download/}
[ "$REL" != "$URL" ] || die "unexpected Python download URL: $URL"
# uv's own file URL handling decodes %2B, so the mirror tree stores the plain "+" in the file name
REL_DEC=$(printf '%s' "$REL" | sed 's/%2B/+/g')
mkdir -p "$OUT/python/$(dirname -- "$REL_DEC")"
GH_URL="https://github.com/astral-sh/python-build-standalone/releases/download/$REL"
curl -fsSL -o "$OUT/python/$REL_DEC" "$URL" 2>/dev/null || curl -fsSL -o "$OUT/python/$REL_DEC" "$GH_URL" || die "download failed: $URL and $GH_URL"
tar -tzf "$OUT/python/$REL_DEC" >/dev/null 2>&1 || die "the downloaded Python archive is not a valid tar.gz"

if [ -n "$RUNTIME_BUNDLE" ]; then
  [ -f "$RUNTIME_BUNDLE" ] || die "--runtime-bundle: $RUNTIME_BUNDLE not found"
  cp "$RUNTIME_BUNDLE" "$OUT/runtime/"
else
  rmdir "$OUT/runtime"
fi
cp "$HERE/install.sh" "$HERE/uninstall.sh" "$OUT/"

( cd "$OUT" && find . -type f ! -name MANIFEST.sha256 | LC_ALL=C sort | while IFS= read -r f; do
    printf '%s  %s\n' "$(sha256_of "$f")" "$f"
  done > MANIFEST.sha256 )
say "==> bundle ready: $OUT ($(du -sh "$OUT" | cut -f1))  [$PLATFORM, Python $PYTHON_VERSION, $(ls "$OUT/wheels" | wc -l | tr -d ' ') wheels]"
