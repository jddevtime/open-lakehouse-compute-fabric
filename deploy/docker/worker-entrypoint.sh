#!/bin/sh
# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
#
# Worker container entrypoint.  `run` (default): enrol on first start - the single-use token comes from OLCF_WORKER_TOKEN, from the file named by
# OLCF_WORKER_TOKEN_FILE, or (compose "cluster" profile) from /enroll/<name>.token written by the `enroll` service - then run the agent in the foreground.
set -eu
umask 077
STATE="${OLCF_WORKER_STATE_DIR:-/var/lib/olcf-worker}"
mkdir -p "$STATE"

# runtime trust anchor baked into the image (build-arg RUNTIME_TRUST_DIR): used unless the operator set OLCF_RUNTIME_TRUST_DIR (or passes --runtime-trust-dir) themselves
IMAGE_TRUST="${OLCF_IMAGE_TRUST_DIR:-/etc/olcf/runtime-trust}"
if [ -z "${OLCF_RUNTIME_TRUST_DIR:-}" ]; then
  for key in "$IMAGE_TRUST"/*.pub; do
    if [ -f "$key" ]; then export OLCF_RUNTIME_TRUST_DIR="$IMAGE_TRUST"; break; fi
  done
fi

case "${1:-run}" in
  run)
    [ "$#" -eq 0 ] || shift
    NAME="${OLCF_WORKER_NAME:-$(hostname | cut -d. -f1)}"
    if [ -n "${OLCF_WORKER_TOKEN_FILE:-}" ] && [ -z "${OLCF_WORKER_TOKEN:-}" ] && [ -r "$OLCF_WORKER_TOKEN_FILE" ]; then
      OLCF_WORKER_TOKEN=$(cat "$OLCF_WORKER_TOKEN_FILE"); export OLCF_WORKER_TOKEN
    fi
    if [ ! -f "$STATE/identity.json" ]; then
      ENROLL_DIR="${OLCF_ENROLL_DIR:-/enroll}"
      if [ -z "${OLCF_WORKER_TOKEN:-}" ] && [ -d "$ENROLL_DIR" ]; then
        echo "olcf-worker: waiting for an enrolment token at $ENROLL_DIR/$NAME.token" >&2
        i=0
        while [ ! -r "$ENROLL_DIR/$NAME.token" ]; do
          i=$((i + 1)); [ "$i" -lt 150 ] || { echo "olcf-worker: no enrolment token appeared" >&2; exit 2; }
          sleep 2
        done
        OLCF_WORKER_TOKEN=$(cat "$ENROLL_DIR/$NAME.token"); export OLCF_WORKER_TOKEN
      fi
      [ -n "${OLCF_WORKER_TOKEN:-}" ] || { echo "olcf-worker: first start needs OLCF_WORKER_TOKEN (create one with: olcf worker enroll-token)" >&2; exit 2; }
      [ -n "${OLCF_WORKER_URL:-}" ] || { echo "olcf-worker: OLCF_WORKER_URL is not set" >&2; exit 2; }
    fi
    set -- olcf-worker run --name "$NAME" --state-dir "$STATE" "$@"
    [ -z "${OLCF_WORKER_POOL:-}" ] || set -- "$@" --pool "$OLCF_WORKER_POOL"
    [ -z "${OLCF_WORKER_LABELS:-}" ] || set -- "$@" --labels "$OLCF_WORKER_LABELS"
    [ -z "${OLCF_WORKER_ADVERTISE_HOST:-}" ] || set -- "$@" --advertise-host "$OLCF_WORKER_ADVERTISE_HOST"
    [ -z "${OLCF_WORKER_CA_PEM:-}" ] || [ ! -r "$OLCF_WORKER_CA_PEM" ] || set -- "$@" --ca-pem "$OLCF_WORKER_CA_PEM"
    [ -z "${OLCF_WORKER_MAX_JOBS:-}" ] || set -- "$@" --max-jobs "$OLCF_WORKER_MAX_JOBS"
    [ -z "${OLCF_WORKER_PIP_INDEX_URL:-}" ] || set -- "$@" --pip-index-url "$OLCF_WORKER_PIP_INDEX_URL"
    [ "${OLCF_WORKER_INSECURE_WS:-false}" != true ] || set -- "$@" --insecure-ws
    exec "$@"
    ;;
  *)
    exec "$@"
    ;;
esac
