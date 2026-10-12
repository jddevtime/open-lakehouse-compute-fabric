#!/bin/sh
# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
#
# Used ONLY by the throwaway `runtime-seed` stage of deploy/Dockerfile (never copied into the final image):  install the Spark runtime named by the BAKE_RUNTIME
# build argument into the seed directory that the final image carries.  Two modes, chosen by what the build context provides:
#
#   operator trust anchor   deploy/runtime-trust (or --build-arg RUNTIME_TRUST_DIR=<dir in the build context>) holds at least one *.pub key.  The runtime is installed in
#                           PRODUCTION mode with OLCF_RUNTIME_TRUST_DIR pointing at that directory, so the manifest must be signed by one of those keys.  BAKE_RUNTIME is the
#                           file name of an operator-signed manifest inside that directory (e.g. olcf-rt-2.0.0.json).  The final image trusts the same directory.
#   development             no *.pub key.  BAKE_RUNTIME is the id of a bundled manifest (e.g. olcf-rt-2.0.0); it is signed with the OLCF development key, which is trusted
#                           ONLY inside this build stage (OLCF_ENVIRONMENT=development is exported here and nowhere else).  A marker file records that the seed is
#                           development-signed: a production container refuses it (the entrypoint says so); run it with OLCF_ENVIRONMENT=development for a try-out.
#
#   usage: bake-runtime.sh BAKE_RUNTIME SEED_DIR [TRUST_DIR]       (empty BAKE_RUNTIME: nothing to do)
set -eu
RUNTIME="${1:-}"
SEED="${2:-/opt/olcf-seed}"
TRUST="${3:-/etc/olcf/runtime-trust}"
mkdir -p "$SEED"
[ -n "$RUNTIME" ] || exit 0

have_keys=0
for k in "$TRUST"/*.pub; do
  [ -f "$k" ] && have_keys=1 && break
done

export OLCF_DATA_DIR="$SEED"
export OLCF_CONFIG_DIR="${OLCF_CONFIG_DIR:-/tmp/cli}"
if [ "$have_keys" = 1 ]; then
  export OLCF_RUNTIME_TRUST_DIR="$TRUST"
  case "$RUNTIME" in
    /*) ref="$RUNTIME";;
    *.json) ref="$TRUST/$RUNTIME";;
    *) echo "bake-runtime: BAKE_RUNTIME=$RUNTIME is a bundled runtime id, which is signed with the development key and is not trusted by $TRUST." >&2
       echo "bake-runtime: sign the manifest with your own key and pass its file name (BAKE_RUNTIME=<name>.json, placed next to your *.pub keys), or remove the keys for a development build." >&2
       exit 2;;
  esac
  [ -f "$ref" ] || { echo "bake-runtime: manifest $ref not found (put the operator-signed manifest in the RUNTIME_TRUST_DIR directory)" >&2; exit 2; }
  exec olcf runtime install "$ref" --no-jdk
fi

case "$RUNTIME" in
  */*|*.json) echo "bake-runtime: a custom manifest ($RUNTIME) needs the public key that signed it: put it in RUNTIME_TRUST_DIR (default deploy/runtime-trust) as <name>.pub" >&2; exit 2;;
esac
export OLCF_ENVIRONMENT=development
: > "$SEED/.dev-signed"
echo "bake-runtime: development build - $RUNTIME is signed with the OLCF development key (trusted in this build stage only)" >&2
exec olcf runtime install "$RUNTIME" --no-jdk
