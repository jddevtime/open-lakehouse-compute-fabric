# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Builds (once) and reuses a real runtime venv with pyspark 4.1.1 from PyPI under ``/tmp/olcf-test-runtime/``.

The runtime is installed by the production :class:`olcf.runtime.manager.RuntimeManager` from a manifest that is generated
with the production lock tool (real PyPI hashes) and signed with a *test* key (exercising the configurable trust store).
``pyspark``'s 450 MB sdist is downloaded once into ``wheelhouse/`` and reused. The first run takes several minutes
(sdist build); later runs only refresh the ``olcf`` slice if the source changed.

Run directly (``python -m tests.node.runtime_fixture``) to pre-build the runtime.
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path

from olcf.runtime import locktool
from olcf.runtime.manager import ManagerConfig, RuntimeManager
from olcf.runtime.signing import (
    TrustedKey,
    TrustStore,
    generate_keypair,
    key_id_for,
    load_private_key,
    load_public_key,
    sign_manifest_dict,
)

CACHE = Path(os.environ.get("OLCF_TEST_RUNTIME_DIR", "/tmp/olcf-test-runtime"))  # noqa: S108
RUNTIME_ID = "olcf-rt-test-4.1.1"
SPEC = {
    "id": RUNTIME_ID, "version": "4.1.1", "family": "olcf-native", "title": "OLCF test runtime (Spark 4.1.1, no Delta jars)",
    "description": "Used by the NODE integration tests: real pyspark 4.1.1 from PyPI, no Maven jars.",
    "spark": {"version": "4.1.1"}, "scala": "2.13", "java": {"major": 21, "supported": [17, 21]}, "python": {"version": "3.13"},
    "os": ["linux", "macos", "windows"], "arch": ["x86_64", "aarch64"],
    "provided_jars": [], "spark_conf_defaults": {"spark.ui.enabled": "true"}, "approved_spark_conf": ["spark.*"],
    "verify": {"smoke": "spark", "imports": ["pyspark", "pandas", "pyarrow", "ipykernel", "jupyter_client", "nbformat"]},
    "build_top_level": ["setuptools>=75", "wheel"],
    "top_level": ["pyspark[sql]==4.1.1", "pandas>=2.2,<3", "ipykernel>=6.29", "jupyter_client>=8.6", "nbformat>=5.10", "packaging", "psutil"],
}


def _keys() -> tuple[Path, Path]:
    CACHE.mkdir(parents=True, exist_ok=True)
    priv, pub = CACHE / "test-signing.key", CACHE / "trust" / "test-signing.pub"
    if not priv.exists():
        k, p = generate_keypair()
        priv.write_bytes(k)
        priv.chmod(0o600)
        pub.parent.mkdir(exist_ok=True)
        pub.write_bytes(p)
    return priv, pub


def test_trust() -> TrustStore:
    _, pub = _keys()
    pk = load_public_key(pub.read_bytes())
    return TrustStore([TrustedKey(key_id_for(pk), pk, "olcf-test", dev=True)])


def manager_config(**over) -> ManagerConfig:
    cfg = ManagerConfig(state_dir=CACHE / "state", trust=test_trust(), pip_find_links=[str(CACHE / "wheelhouse")],
                        smoke_command_prefix=["flock", "/tmp/olcf-spark.lock"], maven_cache=CACHE / "maven-cache")  # noqa: S108
    for k, v in over.items():
        setattr(cfg, k, v)
    return cfg


def ensure_test_runtime(log=print) -> tuple[RuntimeManager, str]:
    """Return ``(manager, runtime_id)`` of a verified test runtime, building it on first use."""
    priv, _ = _keys()
    mf = CACHE / "manifest.json"
    if not mf.exists():
        log("locking test runtime manifest (PyPI hashes)...")
        manifest = locktool.build_manifest(SPEC, progress=lambda s: None)
        mf.write_text(json.dumps(sign_manifest_dict(manifest, load_private_key(priv)), indent=2, sort_keys=True))
    (CACHE / "wheelhouse").mkdir(exist_ok=True)
    mgr = RuntimeManager(manager_config())
    if RUNTIME_ID not in mgr.ready_ids():
        log("installing test runtime (first run: builds pyspark sdist, takes several minutes)...")
        if not any((CACHE / "wheelhouse").glob("pyspark-4.1.1*")):
            subprocess.run([sys.executable, "-m", "pip", "download", "--no-deps", "-d", str(CACHE / "wheelhouse"), "pyspark==4.1.1"], check=True)  # noqa: S603
        mgr.install(mf, progress=lambda s, d: log(f"  [{s}] {d}"))
    mgr.sync_olcf_package(RUNTIME_ID)
    return mgr, RUNTIME_ID


if __name__ == "__main__":
    m, rid = ensure_test_runtime()
    print(json.dumps(m.list(), indent=2))
