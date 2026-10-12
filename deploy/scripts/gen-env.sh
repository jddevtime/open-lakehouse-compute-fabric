#!/bin/sh
# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
#
# Create deploy/.env from .env.example with fresh random secrets (mode 0600).  Refuses to overwrite an existing .env unless --force.
#   sh deploy/scripts/gen-env.sh [--force] [--out FILE]
# Uses only openssl (present on macOS and Linux) or /dev/urandom + base64; nothing is printed except the file name (secrets stay in the file).
set -eu
umask 077
here=$(CDPATH='' cd -- "$(dirname -- "$0")/.." && pwd)
out="$here/.env"; force=0
while [ "$#" -gt 0 ]; do
  case "$1" in
    --force) force=1; shift;;
    --out) out=$2; shift 2;;
    -h|--help) sed -n '2,8p' "$0" | sed 's/^# \{0,1\}//'; exit 0;;
    *) echo "unknown option: $1" >&2; exit 2;;
  esac
done
[ "$force" = 1 ] || [ ! -e "$out" ] || { echo "$out exists; use --force to replace it" >&2; exit 1; }

rand_b64() { if command -v openssl >/dev/null 2>&1; then openssl rand -base64 "$1" | tr -d '\n'; else head -c "$1" /dev/urandom | base64 | tr -d '\n'; fi; }
rand_hex() { if command -v openssl >/dev/null 2>&1; then openssl rand -hex "$1"; else head -c "$1" /dev/urandom | od -An -tx1 | tr -d ' \n'; fi; }
urlsafe() { tr '+/' '-_'; }

master=$(rand_b64 32)
jwt=$(rand_b64 48 | urlsafe)
admin=$(rand_b64 18 | tr -d '/+=' | cut -c1-20)
pg=$(rand_hex 24)
minio=$(rand_hex 20)

tmp="$out.tmp.$$"
sed -e "s|^OLCF_MASTER_KEY=.*|OLCF_MASTER_KEY=$master|" \
    -e "s|^OLCF_JWT_SIGNING_KEY=.*|OLCF_JWT_SIGNING_KEY=$jwt|" \
    -e "s|^OLCF_ADMIN_PASSWORD=.*|OLCF_ADMIN_PASSWORD=$admin|" \
    -e "s|^POSTGRES_PASSWORD=.*|POSTGRES_PASSWORD=$pg|" \
    -e "s|^MINIO_ROOT_PASSWORD=.*|MINIO_ROOT_PASSWORD=$minio|" \
    "$here/.env.example" > "$tmp"
mv "$tmp" "$out"
chmod 600 "$out"
echo "wrote $out (mode 600). The administrator password is in that file (OLCF_ADMIN_PASSWORD); keep an offline copy of OLCF_MASTER_KEY." >&2
