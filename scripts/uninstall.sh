#!/bin/sh
# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
#
# OLCF uninstaller for macOS and Linux (POSIX sh).  Like the installer it never runs git, python3 or java from /usr/bin.
#
# By default it removes only the PROGRAM (versions, bundled Python, uv, PATH entry, login service) and keeps all of your data.
# Data categories are removed independently and only when you ask for them:
#
#   --remove-metadata   database (olcf.db), config.toml, job records, artifacts      <data-dir>/{olcf.db*,config.toml,artifacts,jobs}
#   --remove-secrets    master key, signing key, bootstrap password, built-in CA     <data-dir>/{secrets,pki}      (IRREVERSIBLE: backups and encrypted secrets become unreadable)
#   --remove-logs       log files                                                     <data-dir>/logs
#   --remove-storage    lakehouse files and tables stored in the data directory       <data-dir>/storage            (YOUR DATA)
#   --remove-runtimes   Spark runtimes, downloaded JDK, wheelhouse, uv cache          <data-dir>/{runtimes,wheelhouse,workers,tmp}, <prefix>/cache
#   --remove-backups    backup files                                                  <data-dir>/backups
#   --remove-cli-config saved CLI logins (contexts and tokens)                        ~/.config/olcf (or the platform equivalent)
#   --purge             all of the above, including the data directory itself
#
# Anything that deletes data needs --yes (or an interactive "yes"); --dry-run shows what would be removed.

set -eu

PREFIX="${OLCF_PREFIX:-}"
DATA_DIR="${OLCF_DATA_DIR:-}"
KEEP_PROGRAM=0
RM_META=0; RM_SECRETS=0; RM_LOGS=0; RM_STORAGE=0; RM_RUNTIMES=0; RM_BACKUPS=0; RM_CLI=0
YES=0
DRY=0

if [ -t 2 ] && [ -z "${NO_COLOR:-}" ]; then C_B=$(printf '\033[1m'); C_Y=$(printf '\033[33m'); C_0=$(printf '\033[0m'); else C_B=""; C_Y=""; C_0=""; fi
say()  { printf '%s\n' "$*" >&2; }
step() { printf '%s==>%s %s\n' "$C_B" "$C_0" "$*" >&2; }
warn() { printf '%swarning:%s %s\n' "$C_Y" "$C_0" "$*" >&2; }
die()  { printf 'error: %s\n' "$*" >&2; exit "${2:-1}"; }

usage() {
  cat >&2 <<'EOF'
OLCF uninstaller (macOS, Linux)

Usage: uninstall.sh [options]

  --prefix DIR          program directory (default: from $OLCF_PREFIX, else $HOME/.local/share/olcf)
  --data-dir DIR        data directory (default: the one recorded at install time, else $HOME/.olcf)
  --keep-program        do not remove the program, only the data categories selected below

Data (kept unless selected):
  --remove-metadata     database, config.toml, jobs, artifacts
  --remove-secrets      master/signing keys, bootstrap password, built-in CA   (irreversible)
  --remove-logs         logs
  --remove-storage      lakehouse files/tables inside the data directory
  --remove-runtimes     Spark runtimes, JDK, wheelhouse, uv cache
  --remove-backups      backups
  --remove-cli-config   saved CLI contexts and tokens
  --purge               everything above and the empty data directory

  -y, --yes             do not ask before deleting data
  --dry-run             only show what would be removed
  -h, --help
EOF
}

while [ "$#" -gt 0 ]; do
  case "$1" in
    --prefix)           [ "$#" -ge 2 ] || die "--prefix needs a value"; PREFIX=$2; shift 2;;
    --prefix=*)         PREFIX=${1#*=}; shift;;
    --data-dir)         [ "$#" -ge 2 ] || die "--data-dir needs a value"; DATA_DIR=$2; shift 2;;
    --data-dir=*)       DATA_DIR=${1#*=}; shift;;
    --keep-program)     KEEP_PROGRAM=1; shift;;
    --remove-metadata)  RM_META=1; shift;;
    --remove-secrets)   RM_SECRETS=1; shift;;
    --remove-logs)      RM_LOGS=1; shift;;
    --remove-storage)   RM_STORAGE=1; shift;;
    --remove-runtimes)  RM_RUNTIMES=1; shift;;
    --remove-backups)   RM_BACKUPS=1; shift;;
    --remove-cli-config) RM_CLI=1; shift;;
    --purge)            RM_META=1; RM_SECRETS=1; RM_LOGS=1; RM_STORAGE=1; RM_RUNTIMES=1; RM_BACKUPS=1; RM_CLI=1; PURGE=1; shift;;
    -y|--yes)           YES=1; shift;;
    --dry-run)          DRY=1; shift;;
    -h|--help)          usage; exit 0;;
    *)                  usage; die "unknown option: $1" 2;;
  esac
done
PURGE="${PURGE:-0}"

[ -n "${HOME:-}" ] || die "HOME is not set"
OS_NAME=$(uname -s 2>/dev/null || echo unknown)
[ -n "$PREFIX" ] || PREFIX="${XDG_DATA_HOME:-$HOME/.local/share}/olcf"
case "$PREFIX" in /*) ;; *) PREFIX="$(pwd)/$PREFIX";; esac

# the data directory recorded by the installer wins over the default
if [ -z "$DATA_DIR" ] && [ -f "$PREFIX/install.json" ]; then
  DATA_DIR=$(sed -n 's/^ *"data_dir": *"\(.*\)",\{0,1\}$/\1/p' "$PREFIX/install.json" | head -n 1 | sed 's/",$//; s/"$//')
fi
[ -n "$DATA_DIR" ] || DATA_DIR="$HOME/.olcf"
case "$DATA_DIR" in /*) ;; *) DATA_DIR="$(pwd)/$DATA_DIR";; esac

# --------------------------------------------------------------------------------------------- safety: only delete what is recognisably ours
is_prefix() { [ -f "$PREFIX/install.json" ] && [ -d "$PREFIX/versions" ]; }
is_datadir() { [ -f "$DATA_DIR/config.toml" ] || [ -f "$DATA_DIR/olcf.db" ] || [ -d "$DATA_DIR/secrets" ]; }
case "$PREFIX" in /|"$HOME"|"$HOME/"|/usr|/usr/*|/bin|/etc|/var|/opt) die "refusing to treat '$PREFIX' as the OLCF prefix";; esac
case "$DATA_DIR" in /|"$HOME"|"$HOME/"|/usr|/usr/*|/bin|/etc|/var|/opt) die "refusing to treat '$DATA_DIR' as the OLCF data directory";; esac

act() {  # act DESCRIPTION CMD ARGS...
  desc=$1; shift
  if [ "$DRY" = 1 ]; then say "    would remove $desc"; else "$@" && say "    removed: $desc"; fi
}

confirm_data() {
  [ "$RM_META$RM_SECRETS$RM_LOGS$RM_STORAGE$RM_RUNTIMES$RM_BACKUPS$RM_CLI" != "0000000" ] || return 0
  [ "$YES" = 1 ] || [ "$DRY" = 1 ] && return 0
  if [ -t 0 ]; then
    printf 'This permanently deletes the selected data under %s. Type "yes" to continue: ' "$DATA_DIR" >&2
    read -r answer || answer=""
    [ "$answer" = yes ] || die "aborted; nothing was deleted" 1
  else
    die "refusing to delete data in a non-interactive session without --yes"
  fi
}

OLCF=""
if [ -x "$PREFIX/bin/olcf" ]; then OLCF="$PREFIX/bin/olcf"; fi

step "OLCF uninstaller: prefix $PREFIX, data $DATA_DIR$([ "$DRY" = 1 ] && echo ' (dry run)')"
confirm_data

# --------------------------------------------------------------------------------------------- service + server
if [ -n "$OLCF" ] && [ "$KEEP_PROGRAM" = 0 ]; then
  if [ -f "$DATA_DIR/olcf.pid" ]; then
    step "stopping the running server"
    if [ "$DRY" = 1 ]; then say "    would run: olcf stop"; else "$OLCF" --data-dir "$DATA_DIR" -q stop >&2 2>&1 || warn "could not stop the server cleanly"; fi
  fi
  step "removing the login service (if any)"
  if [ "$DRY" = 1 ]; then say "    would run: olcf service uninstall"; else "$OLCF" --data-dir "$DATA_DIR" -q service uninstall >&2 2>&1 || true; fi
fi

# --------------------------------------------------------------------------------------------- program
remove_profile_block() {
  rc=$1
  [ -f "$rc" ] || return 0
  grep -qF '# >>> olcf >>>' "$rc" 2>/dev/null || return 0
  if [ "$DRY" = 1 ]; then say "    would remove the olcf block from $rc"; return 0; fi
  tmp="$rc.olcf-tmp.$$"
  awk -v b='# >>> olcf >>>' -v e='# <<< olcf <<<' '$0==b{skip=1} !skip{print} $0==e{skip=0}' "$rc" > "$tmp" && cat "$tmp" > "$rc"
  rm -f "$tmp"
  say "    removed: PATH entry in $rc"
}

if [ "$KEEP_PROGRAM" = 0 ]; then
  step "removing the program"
  if is_prefix; then
    for rc in "${ZDOTDIR:-$HOME}/.zshrc" "$HOME/.bashrc" "$HOME/.bash_profile" "$HOME/.profile"; do remove_profile_block "$rc"; done
    if [ -f "$HOME/.config/fish/conf.d/olcf.fish" ] && grep -q "$PREFIX" "$HOME/.config/fish/conf.d/olcf.fish" 2>/dev/null; then
      act "fish PATH snippet" rm -f "$HOME/.config/fish/conf.d/olcf.fish"
    fi
    act "$PREFIX" rm -rf "$PREFIX"
  elif [ -e "$PREFIX" ]; then
    warn "$PREFIX does not look like an OLCF installation (no install.json / versions); leaving it alone"
  else
    say "    nothing to remove at $PREFIX"
  fi
fi

# --------------------------------------------------------------------------------------------- data (each category on its own)
if [ "$RM_META$RM_SECRETS$RM_LOGS$RM_STORAGE$RM_RUNTIMES$RM_BACKUPS" != "000000" ]; then
  step "removing selected data from $DATA_DIR"
  if ! is_datadir; then
    warn "$DATA_DIR does not look like an OLCF data directory; skipping data removal"
  else
    [ "$RM_META" = 0 ]     || { for f in olcf.db olcf.db-wal olcf.db-shm olcf.db-journal config.toml olcf.pid; do [ ! -e "$DATA_DIR/$f" ] || act "$DATA_DIR/$f" rm -f "$DATA_DIR/$f"; done
                               for d in artifacts jobs; do [ ! -e "$DATA_DIR/$d" ] || act "$DATA_DIR/$d" rm -rf "$DATA_DIR/$d"; done; }
    [ "$RM_SECRETS" = 0 ]  || { for d in secrets pki; do [ ! -e "$DATA_DIR/$d" ] || act "$DATA_DIR/$d" rm -rf "$DATA_DIR/$d"; done; }
    [ "$RM_LOGS" = 0 ]     || { [ ! -e "$DATA_DIR/logs" ] || act "$DATA_DIR/logs" rm -rf "$DATA_DIR/logs"; }
    [ "$RM_STORAGE" = 0 ]  || { [ ! -e "$DATA_DIR/storage" ] || act "$DATA_DIR/storage" rm -rf "$DATA_DIR/storage"; }
    [ "$RM_RUNTIMES" = 0 ] || { for d in runtimes wheelhouse workers tmp; do [ ! -e "$DATA_DIR/$d" ] || act "$DATA_DIR/$d" rm -rf "$DATA_DIR/$d"; done
                               if [ "$KEEP_PROGRAM" = 1 ] && [ -d "$PREFIX/cache" ]; then act "$PREFIX/cache" rm -rf "$PREFIX/cache"; fi; }
    [ "$RM_BACKUPS" = 0 ]  || { [ ! -e "$DATA_DIR/backups" ] || act "$DATA_DIR/backups" rm -rf "$DATA_DIR/backups"; }
    if [ "$PURGE" = 1 ] && [ "$DRY" = 0 ]; then rmdir "$DATA_DIR" 2>/dev/null && say "    removed: $DATA_DIR" || warn "$DATA_DIR still contains other files; left in place"; fi
  fi
fi

if [ "$RM_CLI" = 1 ]; then
  step "removing saved CLI logins"
  if [ -n "${OLCF_CONFIG_DIR:-}" ]; then cfg=$OLCF_CONFIG_DIR
  elif [ "$OS_NAME" = Darwin ]; then cfg="$HOME/Library/Application Support/olcf"
  else cfg="${XDG_CONFIG_HOME:-$HOME/.config}/olcf"; fi
  if [ -f "$cfg/contexts.json" ] || [ -d "$cfg/stubguard" ]; then act "$cfg" rm -rf "$cfg"; else say "    nothing at $cfg"; fi
fi

say ""
if [ "$DRY" = 1 ]; then say "Dry run: nothing was changed."; fi
kept=""
[ "$RM_META" = 1 ] || kept="$kept metadata"
[ "$RM_SECRETS" = 1 ] || kept="$kept secrets"
[ "$RM_STORAGE" = 1 ] || kept="$kept storage"
[ "$RM_LOGS" = 1 ] || kept="$kept logs"
[ "$RM_BACKUPS" = 1 ] || kept="$kept backups"
if [ -d "$DATA_DIR" ] && [ "$DRY" = 0 ]; then say "Kept in $DATA_DIR:$kept (remove with the --remove-* options, or --purge)."; fi
say "Done."
exit 0
