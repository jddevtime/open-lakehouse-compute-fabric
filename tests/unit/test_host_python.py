# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
# ruff: noqa: S101, S603
"""The host-Python pseudo-runtime: fingerprint, the summary reported by workers, how its processes are started (clean ``-I`` interpreter, import root) and
that a missing Spark is explained, not reported as a bare ``ModuleNotFoundError``."""

from __future__ import annotations

import subprocess
import sys

from olcf.core.hostruntime import HOST_FAMILY, HOST_RUNTIME_ID
from olcf.runtime import hostpython


def test_describe_is_stable_and_names_the_key_libraries():
    a, b = hostpython.describe(refresh=True), hostpython.describe()
    assert a["fingerprint"] == b["fingerprint"] and a["fingerprint"].startswith("sha256:")
    assert a["python"] == sys.version.split()[0].rstrip("+") or a["python"].startswith(f"{sys.version_info.major}.{sys.version_info.minor}")
    assert {"duckdb", "deltalake", "pyarrow"} <= set(a["packages"]) and a["missing"] == []


def test_summary_is_ready_flagged_no_spark_and_python_only():
    s = hostpython.summary()
    assert s["id"] == HOST_RUNTIME_ID and s["family"] == HOST_FAMILY == "host-python" and s["state"] == "ready" and s["verified"] is True
    assert s["no_spark"] is True and s["spark"] is None and s["engines"] == ["python"] and "no Spark" in s["detail"]


def test_summary_is_failed_with_the_missing_module_named(monkeypatch):
    monkeypatch.setattr(hostpython, "REQUIRED_MODULES", ("definitely_not_installed_module",))
    s = hostpython.summary() if hostpython.describe(refresh=True)["missing"] else {}
    try:
        assert s["state"] == "failed" and "definitely_not_installed_module" in s["detail"] and "pip install" in s["detail"]
    finally:
        monkeypatch.undo()
        hostpython.describe(refresh=True)


def test_module_argv_gives_a_clean_interpreter_that_can_import_olcf(tmp_path):
    h = hostpython.make_handle(tmp_path)
    argv = h.module_argv("olcf.runtime_lib.failures", "--help")  # any module; only the launch line matters here
    assert argv[0] == sys.executable and "-I" in argv
    # the launcher must work in `-I` mode (PYTHONPATH ignored) whether olcf is installed or only importable from a source tree
    code = subprocess.run(h.module_argv("platform"), capture_output=True, text=True, check=False)  # stdlib module as a launch smoke test
    assert code.returncode == 0, code.stderr
    probe = [*h.module_argv("olcf.version")]
    assert subprocess.run(probe, capture_output=True, text=True, check=False).returncode == 0
    env = h.env()
    assert env["OLCF_HOST_PYTHON"] == "1" and env["PATH"].startswith(str(__import__("pathlib").Path(sys.executable).parent))
    assert "JAVA_HOME" not in env and "SPARK_HOME" not in env


def test_handle_refuses_to_pretend_it_has_spark(tmp_path):
    import pytest

    from olcf.core.errors import OlcfError

    h = hostpython.make_handle(tmp_path)
    assert h.has_spark is False and h.family == "host-python"
    with pytest.raises(OlcfError) as ei:
        _ = h.spark_home
    assert "Spark runtime" in (ei.value.remediation or "")


def test_missing_pyspark_is_explained_for_host_jobs():
    from olcf.runtime_lib.spark import no_spark_error

    host = no_spark_error({"runtime": {"id": HOST_RUNTIME_ID}})
    assert "no Spark" in str(host) and "olcf runtime install" in str(host)
    other = no_spark_error({"runtime": {"id": "olcf-rt-2.0.0"}})
    assert "PySpark is not installed" in str(other) and "olcf-rt-2.0.0" in str(other)
