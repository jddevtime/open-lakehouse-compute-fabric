#!/bin/sh
# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
#
# OLCF installer for macOS and Linux (POSIX sh; also fine under dash/bash/zsh in sh mode).
#
#   curl -fsSL https://raw.githubusercontent.com/jddevtime/open-lakehouse-compute-fabric/main/scripts/install.sh | sh
#   sh scripts/install.sh --wheel dist/olcf-0.1.0-py3-none-any.whl            (from a local build)
#   sh scripts/install.sh --help
#
# Designed for a Mac WITHOUT Xcode Command Line Tools: it needs only sh, uname, curl (or wget), tar and a SHA-256 tool - all of which ship
# with macOS.  It never runs git, python3, java, clang or make, and never executes /usr/bin/python3|java|git (on such a Mac those are
# installer dialogs).  Instead it bootstraps a private toolchain under the prefix:
#
#   uv (static binary from github.com/astral-sh/uv, SHA-256 verified)
#     -> uv python install   (python-build-standalone; UV_PYTHON_PREFERENCE=only-managed)
#       -> a virtual environment per OLCF version (wheel-only installs: nothing is ever compiled)
#         -> `olcf runtime install --with-jdk`  (Spark runtime + checksum-verified Eclipse Temurin JDK 21)
#
# Layout (identical to what `olcf upgrade` expects; see src/olcf/cli/upgrade.py):
#   <prefix>/install.json  <prefix>/bin/{uv,olcf}  <prefix>/python/  <prefix>/cache/  <prefix>/versions/<v>/venv/  <prefix>/current -> versions/<v>
#
# Re-running is safe: the same version is reused, a different version is installed next to the old one, the database is backed up and
# migrated, and `current` is switched only after the new version passed a smoke test.  Roll back with `olcf upgrade --rollback`.

set -eu

# --------------------------------------------------------------------------------------------- defaults
UV_VERSION_DEFAULT="0.11.32"
PYTHON_DEFAULT="3.13"

VERSION="${OLCF_VERSION:-}"
PREFIX="${OLCF_PREFIX:-}"
DATA_DIR="${OLCF_DATA_DIR:-}"
WHEEL="${OLCF_WHEEL:-}"
SOURCE="${OLCF_SOURCE:-}"
REF="${OLCF_REF:-main}"
# Until OLCF is published on a package index the default is the GitHub source archive of this repository (pure Python: nothing is compiled).
REPO_ARCHIVE="https://github.com/jddevtime/open-lakehouse-compute-fabric/archive"
FROM_SOURCE=0
INDEX_URL="${OLCF_INDEX_URL:-}"
FIND_LINKS="${OLCF_FIND_LINKS:-}"
EXTRAS="${OLCF_EXTRAS:-}"
PYTHON_VERSION="${OLCF_PYTHON_VERSION:-$PYTHON_DEFAULT}"
UV_VERSION="${OLCF_UV_VERSION:-$UV_VERSION_DEFAULT}"
UV_BIN="${OLCF_UV_BIN:-}"
UV_BASE_URL="${OLCF_UV_BASE_URL:-https://github.com/astral-sh/uv/releases/download}"
INSTALL_RUNTIME=1
RUNTIME_ID=""
WITH_JDK=1
DO_INIT=1
WITH_SERVICE=0
MODIFY_PATH=1
OFFLINE_BUNDLE=""
REINSTALL=0
DRY_RUN=0
KEEP_VERSIONS=2
SHOW_DOCKER=0
PORT=""

# --------------------------------------------------------------------------------------------- output helpers
if [ -t 2 ] && [ -z "${NO_COLOR:-}" ]; then
  C_B=$(printf '\033[1m'); C_Y=$(printf '\033[33m'); C_R=$(printf '\033[31m'); C_G=$(printf '\033[32m'); C_0=$(printf '\033[0m')
else
  C_B=""; C_Y=""; C_R=""; C_G=""; C_0=""
fi
say()  { printf '%s\n' "$*" >&2; }
step() { printf '%s==>%s %s\n' "$C_B" "$C_0" "$*" >&2; }
ok()   { printf '%s ok%s %s\n' "$C_G" "$C_0" "$*" >&2; }
warn() { printf '%swarning:%s %s\n' "$C_Y" "$C_0" "$*" >&2; }
die()  { printf '%serror:%s %s\n' "$C_R" "$C_0" "$*" >&2; exit "${2:-1}"; }

usage() {
  cat >&2 <<'EOF'
OLCF installer (macOS, Linux)

Usage: install.sh [options]

What to install
  (default)              the newest source of this repository's main branch (GitHub source archive; pure Python, nothing is compiled)
  --ref REF              a branch, tag or commit of the repository instead of main, e.g. --ref v0.1.0
  --source FILE|URL      an OLCF source archive (.tar.gz) instead of the GitHub one
  --version V            OLCF version from a package index (needs --index-url or a published package)
  --wheel FILE|URL       install this olcf-*.whl instead
  --extras LIST          optional extras, e.g. postgres,s3,azure,k8s,keyring,iceberg or all
  --index-url URL        private PyPI mirror (wheels only)
  --find-links DIR|URL   extra location with wheels
  --offline-bundle DIR   air-gapped install from a bundle directory (see deploy/README.md); no network access at all

Where
  --prefix DIR           program files (default: $HOME/.local/share/olcf)
  --data-dir DIR         data directory used by `olcf init` (default: $HOME/.olcf)
  --python X.Y           Python minor version uv should provide (default: 3.13)

Steps
  --no-runtime           do not install the Spark runtime (SQL on DuckDB, lakehouses and the UI work without it)
  --runtime ID           runtime manifest to install (default: newest bundled one)
  --no-jdk               never download a JDK (use an existing Java 17/21 only)
  --no-init              do not create the data directory / first administrator
  --port N               port written to config.toml by `olcf init`
  --with-service         also start OLCF at login / boot (launchd on macOS, systemd --user on Linux)
  --no-modify-path       do not add <prefix>/bin to your shell profile
  --reinstall            rebuild the virtual environment of this version even if it exists
  --keep N               versions to keep after an upgrade (default 2)

Other
  --uv-bin FILE          use this uv binary instead of downloading one
  --uv-version V         uv version to download (default 0.11.32)
  --docker               print how to run OLCF with Docker instead and exit (no installation)
  --dry-run              show what would happen
  -h, --help             this text

Environment: OLCF_VERSION OLCF_PREFIX OLCF_DATA_DIR OLCF_WHEEL OLCF_INDEX_URL OLCF_FIND_LINKS OLCF_EXTRAS OLCF_PYTHON_VERSION OLCF_UV_BIN OLCF_UV_VERSION
             OLCF_CACHE_DIR (uv's package cache; default <prefix>/cache. Point several installs at one directory to download the ~600 MB of wheels once)
             OLCF_UV_BASE_URL (mirror of the astral-sh/uv release downloads), HTTPS_PROXY / SSL_CERT_FILE (honoured by curl, uv and pip).
EOF
}

docker_hint() {
  cat >&2 <<'EOF'
Docker route (no installation on the host at all):

  docker run -d --name olcf -p 127.0.0.1:8470:8470 -v olcf-data:/data ghcr.io/jddevtime/olcf:latest
  docker logs olcf            # shows the first administrator password once
  open http://127.0.0.1:8470  # or xdg-open / your browser

or with the compose file from this repository:

  cp deploy/.env.example deploy/.env && docker compose -f deploy/docker-compose.yml --profile allinone up -d

The image reference is a placeholder until the project is published; build it yourself with
`docker build -f deploy/Dockerfile -t olcf:local .`.  See deploy/README.md.
EOF
}

# --------------------------------------------------------------------------------------------- arguments
need_arg() { [ "$#" -ge 2 ] && [ -n "$2" ] || die "option $1 needs a value"; }
while [ "$#" -gt 0 ]; do
  case "$1" in
    --version)         need_arg "$@"; VERSION=$2; shift 2;;
    --version=*)       VERSION=${1#*=}; shift;;
    --prefix)          need_arg "$@"; PREFIX=$2; shift 2;;
    --prefix=*)        PREFIX=${1#*=}; shift;;
    --data-dir)        need_arg "$@"; DATA_DIR=$2; shift 2;;
    --data-dir=*)      DATA_DIR=${1#*=}; shift;;
    --wheel)           need_arg "$@"; WHEEL=$2; shift 2;;
    --wheel=*)         WHEEL=${1#*=}; shift;;
    --source)          need_arg "$@"; SOURCE=$2; shift 2;;
    --source=*)        SOURCE=${1#*=}; shift;;
    --ref)             need_arg "$@"; REF=$2; shift 2;;
    --ref=*)           REF=${1#*=}; shift;;
    --extras)          need_arg "$@"; EXTRAS=$2; shift 2;;
    --extras=*)        EXTRAS=${1#*=}; shift;;
    --index-url)       need_arg "$@"; INDEX_URL=$2; shift 2;;
    --index-url=*)     INDEX_URL=${1#*=}; shift;;
    --find-links)      need_arg "$@"; FIND_LINKS=$2; shift 2;;
    --find-links=*)    FIND_LINKS=${1#*=}; shift;;
    --offline-bundle)  need_arg "$@"; OFFLINE_BUNDLE=$2; shift 2;;
    --offline-bundle=*) OFFLINE_BUNDLE=${1#*=}; shift;;
    --python)          need_arg "$@"; PYTHON_VERSION=$2; shift 2;;
    --python=*)        PYTHON_VERSION=${1#*=}; shift;;
    --uv-bin)          need_arg "$@"; UV_BIN=$2; shift 2;;
    --uv-bin=*)        UV_BIN=${1#*=}; shift;;
    --uv-version)      need_arg "$@"; UV_VERSION=$2; shift 2;;
    --uv-version=*)    UV_VERSION=${1#*=}; shift;;
    --runtime)         need_arg "$@"; RUNTIME_ID=$2; shift 2;;
    --runtime=*)       RUNTIME_ID=${1#*=}; shift;;
    --port)            need_arg "$@"; PORT=$2; shift 2;;
    --port=*)          PORT=${1#*=}; shift;;
    --keep)            need_arg "$@"; KEEP_VERSIONS=$2; shift 2;;
    --keep=*)          KEEP_VERSIONS=${1#*=}; shift;;
    --no-runtime)      INSTALL_RUNTIME=0; shift;;
    --no-jdk)          WITH_JDK=0; shift;;
    --with-jdk)        WITH_JDK=1; shift;;
    --no-init)         DO_INIT=0; shift;;
    --with-service)    WITH_SERVICE=1; shift;;
    --no-modify-path)  MODIFY_PATH=0; shift;;
    --reinstall)       REINSTALL=1; shift;;
    --dry-run)         DRY_RUN=1; shift;;
    --docker)          SHOW_DOCKER=1; shift;;
    -h|--help)         usage; exit 0;;
    *)                 usage; die "unknown option: $1" 2;;
  esac
done

if [ "$SHOW_DOCKER" = 1 ]; then docker_hint; exit 0; fi

# --------------------------------------------------------------------------------------------- platform
OS_NAME=$(uname -s 2>/dev/null || echo unknown)
MACHINE=$(uname -m 2>/dev/null || echo unknown)
[ -n "${HOME:-}" ] || die "HOME is not set"
[ "$(id -u 2>/dev/null || echo 1)" != 0 ] || warn "running as root: OLCF is meant to be installed per user (use --prefix/--data-dir to choose locations)"

case "$OS_NAME" in
  Darwin) UV_OS="apple-darwin";;
  Linux)  UV_OS="unknown-linux-musl";;      # static musl build: runs on any glibc/musl distribution
  *)      die "unsupported operating system '$OS_NAME'. On Windows use scripts/install.ps1; anywhere else use the Docker image (--docker)." ;;
esac
case "$MACHINE" in
  arm64|aarch64) UV_ARCH="aarch64";;
  x86_64|amd64)  UV_ARCH="x86_64";;
  *)             die "unsupported CPU architecture '$MACHINE' (supported: arm64/aarch64, x86_64)";;
esac
if [ "$OS_NAME" = Darwin ] && [ "$UV_ARCH" = x86_64 ]; then
  # an x86_64 shell on Apple silicon (Rosetta): use native arm64 builds
  if [ "$(sysctl -n sysctl.proc_translated 2>/dev/null || echo 0)" = 1 ]; then UV_ARCH="aarch64"; fi
fi
UV_TRIPLE="$UV_ARCH-$UV_OS"

[ -n "$PREFIX" ] || PREFIX="${XDG_DATA_HOME:-$HOME/.local/share}/olcf"
[ -n "$DATA_DIR" ] || DATA_DIR="$HOME/.olcf"
case "$PREFIX" in /*) ;; *) PREFIX="$(pwd)/$PREFIX";; esac
case "$DATA_DIR" in /*) ;; *) DATA_DIR="$(pwd)/$DATA_DIR";; esac
case "$KEEP_VERSIONS" in ''|*[!0-9]*) die "--keep needs a number";; esac
case "$PYTHON_VERSION" in [0-9]*.[0-9]*) ;; *) die "--python must look like 3.13";; esac
case "$PREFIX" in *[\"\`\$\\]*) die "the prefix may not contain quotes, backslashes, backticks or dollar signs";; esac

if [ -n "$WHEEL" ] && [ -n "$VERSION" ]; then die "use either --wheel or --version, not both"; fi
if [ -n "$SOURCE" ] && { [ -n "$WHEEL" ] || [ -n "$VERSION" ]; }; then die "--source cannot be combined with --wheel or --version"; fi
if [ -n "$OFFLINE_BUNDLE" ]; then
  [ -d "$OFFLINE_BUNDLE" ] || die "--offline-bundle: '$OFFLINE_BUNDLE' is not a directory"
  OFFLINE_BUNDLE=$(CDPATH='' cd -- "$OFFLINE_BUNDLE" && pwd)
fi

SUDO_NOTE=""
[ "$DRY_RUN" = 0 ] || SUDO_NOTE=" (dry run: nothing is changed)"

step "OLCF installer: $OS_NAME/$MACHINE -> prefix $PREFIX, data $DATA_DIR$SUDO_NOTE"
if [ "$OS_NAME" = Darwin ]; then
  # `xcode-select -p` is a real program that never opens a dialog; it is only used to reassure, never required.
  if [ -x /usr/bin/xcode-select ] && /usr/bin/xcode-select -p >/dev/null 2>&1; then
    say "    developer tools are present but not needed"
  else
    say "    no Xcode Command Line Tools - none are needed (OLCF brings its own Python and JDK)"
  fi
fi

# --------------------------------------------------------------------------------------------- tools (nothing here is ever a macOS stub)
HAVE_CURL=0; HAVE_WGET=0
command -v curl >/dev/null 2>&1 && HAVE_CURL=1
command -v wget >/dev/null 2>&1 && HAVE_WGET=1

download() {  # download URL DEST
  [ -z "$OFFLINE_BUNDLE" ] || die "internal error: network access requested in offline mode ($1)"
  case "$1" in https://*) ;; *) die "refusing to download from a non-HTTPS URL: $1";; esac
  if [ "$HAVE_CURL" = 1 ]; then
    curl --proto '=https' --tlsv1.2 -fsSL --retry 3 --retry-delay 2 --connect-timeout 20 -o "$2" "$1" || return 1
  elif [ "$HAVE_WGET" = 1 ]; then
    wget -q --https-only --tries=3 -O "$2" "$1" || return 1
  else
    die "neither curl nor wget is available"
  fi
}

sha256_of() {  # sha256_of FILE -> hex digest
  if command -v sha256sum >/dev/null 2>&1; then sha256sum "$1" | cut -d' ' -f1
  elif command -v shasum >/dev/null 2>&1; then shasum -a 256 "$1" | cut -d' ' -f1
  elif command -v openssl >/dev/null 2>&1; then openssl dgst -sha256 "$1" | sed 's/^.*= *//'
  else die "no SHA-256 tool found (sha256sum, shasum or openssl)"; fi
}

run() {  # run a command, or only print it in dry-run mode
  if [ "$DRY_RUN" = 1 ]; then say "    would run: $*"; return 0; fi
  "$@"
}

TMP=""
cleanup() {
  [ -z "$TMP" ] || rm -rf "$TMP"
  if [ -n "${PARTIAL_VERSION_DIR:-}" ] && [ -d "$PARTIAL_VERSION_DIR" ]; then rm -rf "$PARTIAL_VERSION_DIR"; fi
}
trap cleanup EXIT
trap 'exit 130' INT TERM

if [ "$DRY_RUN" = 0 ]; then
  mkdir -p "$PREFIX/bin" "$PREFIX/versions" "$PREFIX/python" "${OLCF_CACHE_DIR:-$PREFIX/cache}"
  TMP=$(mktemp -d "${TMPDIR:-/tmp}/olcf-install.XXXXXX")
fi

# --------------------------------------------------------------------------------------------- uv
uv_ok() {  # uv_ok FILE -> true when it is a working uv >= 0.5
  [ -x "$1" ] || return 1
  v=$("$1" --version 2>/dev/null | sed -n 's/^uv \([0-9][0-9]*\)\.\([0-9][0-9]*\).*/\1 \2/p') || return 1
  [ -n "$v" ] || return 1
  set -- $v
  [ "$1" -gt 0 ] || [ "$2" -ge 5 ]
}

ensure_uv() {
  if [ -n "$UV_BIN" ]; then
    uv_ok "$UV_BIN" || die "--uv-bin '$UV_BIN' is not a working uv (>= 0.5)"
    UV="$UV_BIN"; return
  fi
  if uv_ok "$PREFIX/bin/uv"; then UV="$PREFIX/bin/uv"; return; fi
  if [ -n "$OFFLINE_BUNDLE" ]; then
    for cand in "$OFFLINE_BUNDLE/uv" "$OFFLINE_BUNDLE/bin/uv" "$OFFLINE_BUNDLE/uv-$UV_TRIPLE/uv"; do
      if uv_ok "$cand"; then
        run cp "$cand" "$PREFIX/bin/uv"; run chmod 755 "$PREFIX/bin/uv"; UV="$PREFIX/bin/uv"; return
      fi
    done
    die "the offline bundle has no uv binary for $UV_TRIPLE (expected $OFFLINE_BUNDLE/uv)"
  fi
  if command -v uv >/dev/null 2>&1 && uv_ok "$(command -v uv)"; then UV=$(command -v uv); say "    using the uv found on PATH: $UV"; return; fi
  name="uv-$UV_TRIPLE.tar.gz"
  url="$UV_BASE_URL/$UV_VERSION/$name"
  step "downloading uv $UV_VERSION ($UV_TRIPLE)"
  if [ "$DRY_RUN" = 1 ]; then say "    would download $url and verify its SHA-256"; UV="$PREFIX/bin/uv"; return; fi
  download "$url" "$TMP/$name" || die "could not download $url (check your network/proxy, or pass --uv-bin FILE)"
  download "$url.sha256" "$TMP/$name.sha256" || die "could not download the checksum file $url.sha256"
  want=$(cut -d' ' -f1 "$TMP/$name.sha256" | tr 'A-F' 'a-f')
  got=$(sha256_of "$TMP/$name")
  [ -n "$want" ] && [ "$want" = "$got" ] || die "SHA-256 mismatch for $name (expected '$want', got '$got'); refusing to run it"
  mkdir -p "$TMP/uv" && tar -xzf "$TMP/$name" -C "$TMP/uv" --strip-components=1 || die "could not unpack $name"
  [ -x "$TMP/uv/uv" ] || die "$name did not contain a uv binary"
  cp "$TMP/uv/uv" "$PREFIX/bin/uv.new" && chmod 755 "$PREFIX/bin/uv.new" && mv -f "$PREFIX/bin/uv.new" "$PREFIX/bin/uv"
  uv_ok "$PREFIX/bin/uv" || die "the downloaded uv does not run on this machine"
  UV="$PREFIX/bin/uv"
  ok "uv $UV_VERSION verified and installed to $PREFIX/bin/uv"
}

# uv must never choose the system python (on a stock Mac /usr/bin/python3 is an installer stub) and must keep all its state under the prefix.
UV_ENV_PYTHON_DIR="$PREFIX/python"
uv_run() {
  UV_PYTHON_INSTALL_DIR="$UV_ENV_PYTHON_DIR" UV_CACHE_DIR="${OLCF_CACHE_DIR:-$PREFIX/cache}" UV_PYTHON_PREFERENCE=only-managed UV_NO_PROGRESS=1 UV_NO_CONFIG=1 "$@"
}

# --------------------------------------------------------------------------------------------- resolve what to install
resolve_target() {
  EXTRA_SPEC=""
  [ -z "$EXTRAS" ] || EXTRA_SPEC="[$EXTRAS]"
  if [ -n "$WHEEL" ]; then
    case "$WHEEL" in
      https://*) base=${WHEEL##*/};;
      /*|./*|../*|*/*) [ -f "$WHEEL" ] || die "wheel not found: $WHEEL"; WHEEL="$(CDPATH='' cd -- "$(dirname -- "$WHEEL")" && pwd)/$(basename -- "$WHEEL")"; base=${WHEEL##*/};;
      *) [ -f "$WHEEL" ] || die "wheel not found: $WHEEL"; WHEEL="$(pwd)/$WHEEL"; base=${WHEEL##*/};;
    esac
    case "$base" in olcf-*-*.whl) ;; *) die "'$base' is not an olcf wheel (expected olcf-<version>-py3-none-any.whl)";; esac
    TARGET_VERSION=$(printf '%s' "$base" | cut -d- -f2)
    case "$WHEEL" in https://*) TARGET_SPEC="olcf$EXTRA_SPEC @ $WHEEL";; *) TARGET_SPEC="olcf$EXTRA_SPEC @ file://$WHEEL";; esac
    return
  fi
  if [ -n "$OFFLINE_BUNDLE" ]; then
    set -- "$OFFLINE_BUNDLE"/wheels/olcf-*.whl
    [ -f "$1" ] || die "the offline bundle has no wheels/olcf-*.whl"
    [ "$#" -eq 1 ] || die "the offline bundle contains several olcf wheels; keep exactly one"
    base=${1##*/}
    TARGET_VERSION=$(printf '%s' "$base" | cut -d- -f2)
    TARGET_SPEC="olcf$EXTRA_SPEC==$TARGET_VERSION"
    return
  fi
  if [ -n "$VERSION" ]; then
    TARGET_VERSION=$VERSION; TARGET_SPEC="olcf$EXTRA_SPEC==$VERSION"; return
  fi
  if [ -z "$INDEX_URL" ]; then
    # default: the source archive of this repository (or --source)
    FROM_SOURCE=1
    if [ -z "$SOURCE" ]; then
      case "$REF" in v[0-9]*|[0-9]*.[0-9]*) SOURCE="$REPO_ARCHIVE/refs/tags/$REF.tar.gz";; *) SOURCE="$REPO_ARCHIVE/refs/heads/$REF.tar.gz";; esac
    fi
    step "reading the OLCF source archive ($SOURCE)"
    if [ "$DRY_RUN" = 1 ]; then TARGET_VERSION="from-source"; TARGET_SPEC="olcf$EXTRA_SPEC @ $SOURCE"; return; fi
    [ -n "$TMP" ] || TMP=$(mktemp -d "${TMPDIR:-/tmp}/olcf-install.XXXXXX")
    case "$SOURCE" in
      https://*) download "$SOURCE" "$TMP/olcf-source.tar.gz" || die "could not download $SOURCE (is the repository public and is '$REF' a real branch or tag?)";;
      *) [ -f "$SOURCE" ] || die "source archive not found: $SOURCE"; cp "$SOURCE" "$TMP/olcf-source.tar.gz";;
    esac
    top=$(tar -tzf "$TMP/olcf-source.tar.gz" 2>/dev/null | head -n 1 | cut -d/ -f1) || true
    [ -n "$top" ] || die "'$SOURCE' is not a .tar.gz source archive"
    TARGET_VERSION=$(tar -xzOf "$TMP/olcf-source.tar.gz" "$top/pyproject.toml" 2>/dev/null | sed -n 's/^version *= *"\([^"]*\)".*/\1/p' | head -n 1) || true
    [ -n "$TARGET_VERSION" ] || die "'$SOURCE' has no pyproject.toml with a version: is it an OLCF source archive?"
    TARGET_SPEC="olcf$EXTRA_SPEC @ file://$TMP/olcf-source.tar.gz"
    return
  fi
  step "looking up the newest olcf release"
  if [ "$DRY_RUN" = 1 ]; then TARGET_VERSION="latest"; TARGET_SPEC="olcf$EXTRA_SPEC"; return; fi
  idx=""; [ -z "$INDEX_URL" ] || idx="--index-url $INDEX_URL"
  # shellcheck disable=SC2086
  latest=$(printf 'olcf%s\n' "$EXTRA_SPEC" | uv_run "$UV" pip compile - --python-version "$PYTHON_VERSION" --only-binary :all: --no-header --no-annotate $idx 2>/dev/null \
           | sed -n 's/^olcf==\([^ ;]*\).*/\1/p' | head -n 1) || true
  [ -n "$latest" ] || die "could not find olcf in the package index. If it is not published yet, omit --index-url to install from the GitHub source archive, or use --wheel dist/olcf-<version>-py3-none-any.whl"
  TARGET_VERSION=$latest; TARGET_SPEC="olcf$EXTRA_SPEC==$latest"
}

# --------------------------------------------------------------------------------------------- install one version
venv_python() { printf '%s/versions/%s/venv/bin/python' "$PREFIX" "$1"; }

olcf_cli() {  # olcf_cli VERSION args...  (always through the version's own interpreter; no PATH lookup)
  v=$1; shift
  OLCF_DATA_DIR="$DATA_DIR" "$(venv_python "$v")" -m olcf.cli.main "$@"
}

build_version() {
  dest="$PREFIX/versions/$TARGET_VERSION"
  if [ -d "$dest/venv" ] && [ "$REINSTALL" = 0 ] && "$(venv_python "$TARGET_VERSION")" -c 'import olcf' >/dev/null 2>&1; then
    ok "olcf $TARGET_VERSION is already installed (use --reinstall to rebuild it)"; return
  fi
  step "installing olcf $TARGET_VERSION"
  if [ "$DRY_RUN" = 1 ]; then
    say "    would: uv python install $PYTHON_VERSION; uv venv $dest/venv; uv pip install --only-binary :all: '$TARGET_SPEC'"; return
  fi
  PARTIAL_VERSION_DIR="$dest"
  rm -rf "$dest"; mkdir -p "$dest"
  if [ -n "$OFFLINE_BUNDLE" ] && [ -d "$OFFLINE_BUNDLE/python" ]; then
    UV_PYTHON_INSTALL_MIRROR="file://$OFFLINE_BUNDLE/python"; export UV_PYTHON_INSTALL_MIRROR
  fi
  say "    Python $PYTHON_VERSION (uv-managed, under $PREFIX/python)"
  uv_run "$UV" python install "$PYTHON_VERSION" ${OFFLINE_BUNDLE:+--offline} >&2 || die "could not install Python $PYTHON_VERSION with uv. Behind a proxy? Set HTTPS_PROXY / SSL_CERT_FILE; offline? use --offline-bundle."
  uv_run "$UV" venv --python "$PYTHON_VERSION" --python-preference only-managed "$dest/venv" >&2 || die "could not create the virtual environment"
  set -- pip install --python "$dest/venv/bin/python" --only-binary :all: --no-config
  [ "$FROM_SOURCE" = 0 ] || set -- "$@" --no-binary olcf     # OLCF itself is pure Python and is built from the archive; every dependency is still a wheel
  [ -z "$INDEX_URL" ] || set -- "$@" --index-url "$INDEX_URL"
  [ -z "$FIND_LINKS" ] || set -- "$@" --find-links "$FIND_LINKS"
  if [ -n "$OFFLINE_BUNDLE" ]; then set -- "$@" --offline --no-index --find-links "$OFFLINE_BUNDLE/wheels"; fi
  if [ "$FROM_SOURCE" = 1 ]; then say "    installing (every dependency is a wheel, nothing is compiled; OLCF itself is pure Python, built from the source archive)"
  else say "    installing wheels (nothing is compiled; --only-binary :all:)"; fi
  uv_run "$UV" "$@" "$TARGET_SPEC" >&2 || die "installing '$TARGET_SPEC' failed. A package without a wheel for this platform cannot be installed without compilers - see the message above."
  got=$(olcf_cli "$TARGET_VERSION" --json version 2>/dev/null | sed -n 's/.*"olcf": *"\([^"]*\)".*/\1/p' | head -n 1) || true
  [ "$got" = "$TARGET_VERSION" ] || die "smoke test failed: the new environment reports version '${got:-?}', expected '$TARGET_VERSION'"
  PARTIAL_VERSION_DIR=""
  ok "olcf $TARGET_VERSION installed in $dest/venv"
}

# --------------------------------------------------------------------------------------------- switch (with backup + migration when upgrading)
current_version() {
  if [ -L "$PREFIX/current" ]; then
    t=$(readlink "$PREFIX/current" 2>/dev/null || true)
    basename -- "$t"
  fi
}

atomic_link() {  # atomic_link TARGET LINK
  "$(venv_python "$TARGET_VERSION")" -I -c 'import os,sys; t=sys.argv[2]+".new"
try: os.unlink(t)
except FileNotFoundError: pass
os.symlink(sys.argv[1], t); os.replace(t, sys.argv[2])' "$1" "$2" 2>/dev/null || ln -sfn "$1" "$2"
}

write_install_json() {  # merged with the previous document; written by the new interpreter (proper JSON escaping)
  "$(venv_python "$TARGET_VERSION")" -I -c '
import json, os, sys, time
prefix, version, previous, pyver, data_dir, method, uvbin = sys.argv[1:8]
path = os.path.join(prefix, "install.json")
try:
    info = json.load(open(path, encoding="utf-8"))
except (OSError, ValueError):
    info = {}
info.update(schema=1, prefix=prefix, version=version, python=pyver, data_dir=data_dir, method=method, uv=uvbin,
            installed_at=time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()))
if previous and previous != version:
    info["previous"] = previous
tmp = path + ".tmp"
with open(tmp, "w", encoding="utf-8") as fh:
    json.dump(info, fh, indent=2, sort_keys=True); fh.write("\n")
os.replace(tmp, path)
' "$PREFIX" "$TARGET_VERSION" "$1" "$PYTHON_VERSION" "$DATA_DIR" "$2" "$UV"
}

switch_version() {
  cur=$(current_version || true)
  if [ "$DRY_RUN" = 1 ]; then say "    would switch $PREFIX/current -> versions/$TARGET_VERSION (from '${cur:-none}')"; return; fi
  if [ -n "$cur" ] && [ "$cur" != "$TARGET_VERSION" ] && [ -x "$(venv_python "$cur")" ]; then
    step "upgrading $cur -> $TARGET_VERSION"
    if [ -f "$DATA_DIR/olcf.pid" ] || [ -f "$DATA_DIR/olcf.db" ]; then
      if [ -f "$DATA_DIR/olcf.pid" ]; then
        say "    stopping the running server"
        olcf_cli "$cur" -q stop --grace 45 >&2 2>&1 || warn "could not stop the running server cleanly; continuing"
      fi
      if [ -f "$DATA_DIR/olcf.db" ]; then
        bk="$DATA_DIR/backups/pre-upgrade-$cur-to-$TARGET_VERSION-$(date +%Y%m%dT%H%M%S).olcfbak"
        say "    backing up the database -> $bk"
        mkdir -p "$DATA_DIR/backups"
        olcf_cli "$cur" -q backup create -o "$bk" >&2 || die "the pre-upgrade backup failed; the old version ($cur) stays active. Nothing was changed."
      fi
      say "    migrating the database with $TARGET_VERSION"
      olcf_cli "$TARGET_VERSION" -q db migrate >&2 || die "the database migration failed; the old version ($cur) stays active${bk:+ (backup: $bk)}"
    fi
  fi
  atomic_link "$PREFIX/versions/$TARGET_VERSION" "$PREFIX/current"
  atomic_link "../current/venv/bin/olcf" "$PREFIX/bin/olcf"
  if [ -x "$PREFIX/versions/$TARGET_VERSION/venv/bin/olcf-worker" ]; then atomic_link "../current/venv/bin/olcf-worker" "$PREFIX/bin/olcf-worker"; fi
  method="wheel"; [ -n "$WHEEL" ] || method="index"; [ "$FROM_SOURCE" = 0 ] || method="source"; [ -z "$OFFLINE_BUNDLE" ] || method="offline-bundle"
  write_install_json "${cur:-}" "$method"
  prune_versions "$cur"
  ok "current -> versions/$TARGET_VERSION"
}

prune_versions() {  # keep the newest $KEEP_VERSIONS, never the current or the previous one
  cur=$1
  [ "$KEEP_VERSIONS" -gt 0 ] || return 0
  count=0
  # shellcheck disable=SC2045  # directory names are version strings created by this installer
  for d in $(ls -t "$PREFIX/versions" 2>/dev/null); do
    count=$((count + 1))
    [ "$count" -gt "$KEEP_VERSIONS" ] || continue
    [ "$d" = "$TARGET_VERSION" ] || [ "$d" = "$cur" ] || { say "    removing old version $d"; rm -rf "${PREFIX:?}/versions/$d"; }
  done
}

# --------------------------------------------------------------------------------------------- PATH
add_to_path() {
  [ "$MODIFY_PATH" = 1 ] || return 0
  [ "$DRY_RUN" = 0 ] || { say "    would add $PREFIX/bin to your shell profile"; return 0; }
  cat > "$PREFIX/env.sh" <<EOF
# generated by the OLCF installer
case ":\${PATH}:" in *":$PREFIX/bin:"*) ;; *) export PATH="$PREFIX/bin:\$PATH";; esac
EOF
  shell_name=$(basename -- "${SHELL:-/bin/sh}")
  rc=""
  case "$shell_name" in
    zsh)  rc="${ZDOTDIR:-$HOME}/.zshrc";;
    bash) if [ "$OS_NAME" = Darwin ]; then rc="$HOME/.bash_profile"; else rc="$HOME/.bashrc"; fi;;
    fish) mkdir -p "$HOME/.config/fish/conf.d"
          printf '# generated by the OLCF installer\nfish_add_path -g "%s/bin"\n' "$PREFIX" > "$HOME/.config/fish/conf.d/olcf.fish"
          ok "added $PREFIX/bin to fish (open a new terminal)"; return 0;;
    *)    rc="$HOME/.profile";;
  esac
  begin="# >>> olcf >>>"; end="# <<< olcf <<<"
  if [ -f "$rc" ] && grep -qF "$begin" "$rc" 2>/dev/null; then
    awk -v b="$begin" -v e="$end" '$0==b{skip=1} !skip{print} $0==e{skip=0}' "$rc" > "$TMP/rc.new" && cat "$TMP/rc.new" > "$rc"
  fi
  { printf '\n%s\n. "%s/env.sh"\n%s\n' "$begin" "$PREFIX" "$end"; } >> "$rc"
  ok "added $PREFIX/bin to $rc (open a new terminal, or run: . \"$PREFIX/env.sh\")"
}

# --------------------------------------------------------------------------------------------- run
ensure_uv
resolve_target
build_version
switch_version
add_to_path

OLCF_EXE="$PREFIX/bin/olcf"
[ "$DRY_RUN" = 1 ] || [ -x "$OLCF_EXE" ] || die "internal error: $OLCF_EXE was not created"

# `olcf init` runs BEFORE the runtime install: it writes config.toml with environment = "development", which is what makes this installation trust the development key that
# signs the bundled Spark runtime manifests.  Without a config.toml the data directory counts as production and `olcf runtime install` refuses them.
if [ "$DO_INIT" = 1 ]; then
  if [ "$DRY_RUN" = 1 ]; then step "initialising $DATA_DIR"; say "    would run: olcf init"; else
    set -- --data-dir "$DATA_DIR" init
    [ -z "$PORT" ] || set -- "$@" --port "$PORT"
    "$OLCF_EXE" "$@" >&2 || die "olcf init failed"
  fi
fi

RUNTIME_NOTE=""
if [ "$INSTALL_RUNTIME" = 1 ]; then
  step "installing the Spark runtime (several minutes the first time: Python wheels, PySpark, JARs and - if no Java 17/21 exists - a verified Temurin JDK)"
  if [ "$DRY_RUN" = 1 ]; then
    say "    would run: olcf runtime install ${RUNTIME_ID:+$RUNTIME_ID }$([ "$WITH_JDK" = 1 ] && echo --with-jdk || echo --no-jdk)"
  else
    set -- --data-dir "$DATA_DIR" runtime install
    [ -z "$RUNTIME_ID" ] || set -- "$@" "$RUNTIME_ID"
    if [ "$WITH_JDK" = 1 ]; then set -- "$@" --with-jdk; else set -- "$@" --no-jdk; fi
    [ -z "$INDEX_URL" ] || set -- "$@" --index-url "$INDEX_URL"
    [ -z "$FIND_LINKS" ] || set -- "$@" --find-links "$FIND_LINKS"
    if [ -n "$OFFLINE_BUNDLE" ]; then
      bundle=$(ls "$OFFLINE_BUNDLE"/runtime/*.olcfrt 2>/dev/null | head -n 1 || true)
      [ -z "$bundle" ] || set -- "$@" --bundle "$bundle"
    fi
    if "$OLCF_EXE" "$@" >&2; then ok "runtime installed"; else
      RUNTIME_NOTE="the Spark runtime could not be installed (see the messages above). OLCF works without it for SQL/lakehouses/UI; retry with: olcf runtime install --with-jdk (if the message says the signing key is not trusted: run olcf init first, or set OLCF_RUNTIME_TRUST_DIR to your key directory)"
      warn "$RUNTIME_NOTE"
    fi
  fi
fi

if [ "$WITH_SERVICE" = 1 ]; then
  step "installing the login service"
  if [ "$DRY_RUN" = 1 ]; then say "    would run: olcf service install"; else
    "$OLCF_EXE" --data-dir "$DATA_DIR" service install >&2 || warn "could not install the service; run 'olcf service install' later to see why"
  fi
fi

say ""
say "${C_G}OLCF $TARGET_VERSION is installed.${C_0}"
say "  start it:       olcf serve            (or: olcf quickstart)"
say "  check it:        olcf doctor"
say "  upgrade later:   olcf upgrade --version X.Y.Z     roll back: olcf upgrade --rollback"
say "  remove it:       sh scripts/uninstall.sh   (a copy is kept at $PREFIX/uninstall.sh when you installed from a checkout)"
[ -z "$RUNTIME_NOTE" ] || say "  ${C_Y}note:${C_0} $RUNTIME_NOTE"
case ":$PATH:" in *":$PREFIX/bin:"*) ;; *) [ "$MODIFY_PATH" = 0 ] || say "  Open a new terminal (or run  . \"$PREFIX/env.sh\" ) so that 'olcf' is on your PATH.";; esac

# keep a copy of the uninstaller next to the program so it works without the repository (only when run from a real file, not `curl | sh`)
if [ "$DRY_RUN" = 0 ] && [ -f "$0" ]; then
  src_dir=$(CDPATH='' cd -- "$(dirname -- "$0")" 2>/dev/null && pwd || true)
  if [ -n "$src_dir" ] && [ -f "$src_dir/uninstall.sh" ] && grep -q 'OLCF uninstaller' "$src_dir/uninstall.sh" 2>/dev/null; then
    cp "$src_dir/uninstall.sh" "$PREFIX/uninstall.sh" 2>/dev/null || true
  fi
fi
exit 0
