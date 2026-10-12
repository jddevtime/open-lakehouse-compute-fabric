#!/bin/sh
# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
#
# Container entrypoint of the OLCF control plane image.
#   serve (default)  first start: `olcf init` (config.toml, database, first administrator); then `olcf serve` in the foreground
#   anything else    executed as given (e.g. `olcf backup create`, `sh`)
#
# Secrets never appear on a command line: they arrive as environment variables (OLCF_MASTER_KEY, OLCF_JWT_SIGNING_KEY, OLCF_DB__URL, ...), or as files
# named by <NAME>_FILE (Docker/Kubernetes secrets).  The first administrator's password comes from OLCF_ADMIN_PASSWORD(_FILE); when absent one is generated
# and printed ONCE to the container log.
set -eu
umask 077

DATA="${OLCF_DATA_DIR:-/data}"
export OLCF_DATA_DIR="$DATA"
export OLCF_CONFIG_DIR="${OLCF_CONFIG_DIR:-$DATA/.cli}"
export HOME="${HOME:-$DATA/home}"
mkdir -p "$DATA" "$OLCF_CONFIG_DIR" "$HOME"

# <NAME>_FILE -> <NAME>
for name in OLCF_MASTER_KEY OLCF_JWT_SIGNING_KEY OLCF_DB__URL OLCF_ADMIN_PASSWORD OLCF_VAULT_TOKEN; do
  eval "file=\${${name}_FILE:-}"
  if [ -n "$file" ]; then
    [ -r "$file" ] || { echo "olcf-entrypoint: ${name}_FILE=$file is not readable" >&2; exit 64; }
    eval "export $name=\$(cat \"\$file\")"
  fi
done

# runtime trust anchor baked into the image (build-arg RUNTIME_TRUST_DIR): used unless the operator set OLCF_RUNTIME_TRUST_DIR (or runtime.trust.dir) themselves
IMAGE_TRUST="${OLCF_IMAGE_TRUST_DIR:-/etc/olcf/runtime-trust}"
if [ -z "${OLCF_RUNTIME_TRUST_DIR:-}" ]; then
  for key in "$IMAGE_TRUST"/*.pub; do
    if [ -f "$key" ]; then export OLCF_RUNTIME_TRUST_DIR="$IMAGE_TRUST"; break; fi
  done
fi

SEED="${OLCF_SEED_DIR:-/opt/olcf-seed}"
seed_runtime() {
  # a baked runtime (build-arg BAKE_RUNTIME) is copied into the persistent volume on first start; the embedded worker's state dir is named after the
  # container hostname, which compose pins (hostname: olcf-allinone) so that it survives container re-creation
  host=$(hostname | cut -d. -f1)
  target="$DATA/workers/$host"
  if [ -f "$SEED/.dev-signed" ] && [ "${OLCF_ENVIRONMENT:-production}" = production ] && [ -z "${OLCF_RUNTIME_TRUST_DIR:-}" ]; then
    echo "olcf-entrypoint: WARNING the baked Spark runtime is signed with the OLCF development key, which a production container does not trust: it will not be offered." >&2
    echo "olcf-entrypoint: for a try-out run with OLCF_ENVIRONMENT=development; for production rebuild with RUNTIME_TRUST_DIR and an operator-signed manifest (deploy/README.md)." >&2
  fi
  if [ -d "$SEED/workers" ] && [ ! -d "$target/runtimes" ]; then
    seed=$(find "$SEED/workers" -mindepth 1 -maxdepth 1 -type d | head -n 1)
    if [ -n "$seed" ] && [ -d "$seed/runtimes" ]; then
      mkdir -p "$target" && cp -a "$seed/." "$target/" && echo "olcf-entrypoint: seeded the baked Spark runtime into $target" >&2
    fi
  fi
}

first_start() {
  # own positional parameters: must not disturb the arguments that follow `serve`
  set -- init --host "$1" --port "$2"
  [ -z "${OLCF_PUBLIC_URL:-}" ] || set -- "$@" --public-url "$OLCF_PUBLIC_URL"
  [ "${OLCF_TLS_ENABLED:-false}" != true ] || set -- "$@" --tls
  if [ -n "${OLCF_ADMIN_PASSWORD:-}" ]; then
    printf '%s\n' "$OLCF_ADMIN_PASSWORD" | olcf "$@" --admin-password-stdin
  else
    olcf "$@"
  fi
}

case "${1:-serve}" in
  serve)
    [ "$#" -eq 0 ] || shift
    PORT="${OLCF_PORT:-8470}"
    BIND="${OLCF_BIND_HOST:-0.0.0.0}"
    if [ ! -f "$DATA/config.toml" ]; then
      echo "olcf-entrypoint: first start - initialising $DATA" >&2
      first_start "$BIND" "$PORT"
    fi
    seed_runtime
    [ -z "${OLCF_PUBLIC_URL:-}" ] || set -- --public-url "$OLCF_PUBLIC_URL" "$@"
    # shellcheck disable=SC2086
    exec olcf serve --host "$BIND" --port "$PORT" ${OLCF_SERVE_ARGS:-} "$@"
    ;;
  *)
    exec "$@"
    ;;
esac
