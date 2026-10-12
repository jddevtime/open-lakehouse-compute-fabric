#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
#
# Build the stdlib-only Fabric SDK wheel (olcf-fabric) into ./dist (or $1).
#
#   scripts/build_fabric_sdk.sh [outdir]
#
# Needs only setuptools (pip wheel --no-build-isolation --no-deps), so it works without network access. The version comes from
# sdk/fabric/olcf_fabric/__init__.py and must equal olcf.version.__version__ (checked here and by tests/fabric/test_sdk_packaging.py).
set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
out="${1:-$root/dist}"
python="${PYTHON:-python}"

# Prerequisites, checked up front so that a bare environment (``uv venv`` has no pip, and setuptools is not guaranteed either) fails with the fix instead of a bare
# "No module named pip" half way through. Exit status 2 = prerequisite missing.
if ! "$python" -c 'import sys' >/dev/null 2>&1; then
  echo "cannot run the Python interpreter '$python' (set PYTHON=/path/to/python)" >&2
  exit 2
fi
if ! "$python" -m pip --version >/dev/null 2>&1; then
  echo "pip is not installed for $python, and building the wheel needs it." >&2
  echo "  fix: $python -m ensurepip --upgrade    (or: uv pip install pip --python $python)" >&2
  echo "  or create the environment with 'uv venv --seed'; the olcf[dev] extra includes pip." >&2
  exit 2
fi
if ! "$python" -c 'import setuptools' >/dev/null 2>&1; then
  echo "setuptools is not installed for $python, and the wheel is built with --no-build-isolation (no network)." >&2
  echo "  fix: $python -m pip install setuptools" >&2
  exit 2
fi

sdk_version="$(PYTHONPATH="$root/sdk/fabric" "$python" -c 'import olcf_fabric; print(olcf_fabric.__version__)')"
srv_version="$(PYTHONPATH="$root/src" "$python" -c 'from olcf.version import __version__; print(__version__)')"
if [ "$sdk_version" != "$srv_version" ]; then
  echo "version mismatch: SDK $sdk_version != server $srv_version (the snippet advertises the server version)" >&2
  exit 1
fi

work="$(mktemp -d)"
trap 'rm -rf "$work"' EXIT
cp -R "$root/sdk/fabric/." "$work/"
find "$work" -name '__pycache__' -type d -prune -exec rm -rf {} +
mkdir -p "$out"
(cd "$work" && "$python" -m pip wheel --no-build-isolation --no-deps --wheel-dir "$out" . >/dev/null)
ls -l "$out"/olcf_fabric-"$sdk_version"-py3-none-any.whl
