#!/bin/sh
# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
#
# One-shot helper of the compose "cluster" profile.  Runs in the control plane's network namespace, waits until the server is healthy, then writes one
# single-use worker enrolment token (and the CA certificate) per worker name into /enroll, which the worker containers mount read-only.
#   usage: olcf-enroll NAME [NAME...]
set -eu
umask 077
OUT="${OLCF_ENROLL_DIR:-/enroll}"
TTL="${OLCF_ENROLL_TTL:-3600}"
mkdir -p "$OUT"

i=0
until olcf --json status >/dev/null 2>&1; do
  i=$((i + 1))
  [ "$i" -lt 120 ] || { echo "olcf-enroll: the control plane did not become ready" >&2; exit 1; }
  sleep 2
done

for name in "$@"; do
  if [ -f "$OUT/$name.done" ]; then echo "olcf-enroll: $name already enrolled" >&2; continue; fi
  tok=$(olcf --json worker enroll-token --name "$name" --ttl "$TTL" | /opt/olcf/bin/python -c 'import json,sys; d=json.load(sys.stdin); print(d.get("token") or d.get("enrollment_token"))')
  [ -n "$tok" ] && [ "$tok" != None ] || { echo "olcf-enroll: no token returned for $name" >&2; exit 1; }
  printf '%s' "$tok" > "$OUT/$name.token.tmp" && mv "$OUT/$name.token.tmp" "$OUT/$name.token"
  echo "olcf-enroll: token for $name written" >&2
done
[ ! -f "${OLCF_DATA_DIR:-/data}/pki/ca.crt" ] || cp "${OLCF_DATA_DIR:-/data}/pki/ca.crt" "$OUT/ca.pem"
chmod 0644 "$OUT/ca.pem" 2>/dev/null || true
