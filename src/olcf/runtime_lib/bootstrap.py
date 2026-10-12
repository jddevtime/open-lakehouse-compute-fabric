# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Bootstrap hook executed *before* any user code (runner, notebook executor and interactive kernels share it).

Responsibilities:

* ``@@OLCF@@`` progress events on stdout (:func:`emit_event`) that the worker agent forwards as ``job_event`` frames;
* the job environment (:func:`apply_job_env`): non-secret ``env``, ``secrets`` and ``storage_conf.env`` are exported
  into ``os.environ`` of the runner so every child (kernel, JVM, python workers) inherits them - the agent itself never
  puts secrets into process environments or logs;
* the lazily created ``spark`` session (:class:`LazySpark`) so that ``%%configure`` can still change Spark settings
  before the first use;
* the **runtime lineage probe** (:func:`install_lineage_probe`): ``@@OLCF@@ dataset_read / dataset_written / dataset_statement`` events for what user code reads and writes;
* the **notebookutils hook**: ``olcf.runtime_lib.notebookutils.install(job_spec, spark)`` is called when that module
  exists (FABRIC package). Only ``ImportError`` of *that module itself* is tolerated; any other error surfaces. When
  absent, a minimal built-in ``notebookutils`` shim provides ``notebook.exit``;
* the **exit contract**: ``notebookutils.notebook.exit(value)`` publishes a display-data message with the reserved MIME
  type ``application/vnd.olcf.exit+json`` and payload ``{"value": <json>}`` inside kernels, or raises
  :class:`NotebookExit` in plain scripts. Other packages must call :func:`exit_notebook` to take part in this contract.
"""

from __future__ import annotations

import builtins
import json
import os
import sys
import threading
import types
from typing import Any

EXIT_MIME = "application/vnd.olcf.exit+json"
EVENT_PREFIX = "@@OLCF@@"
_emit_lock = threading.Lock()


def emit_event(event: str, **data: Any) -> None:
    """Print one ``@@OLCF@@ {json}`` line (single write + flush, so lines never interleave)."""
    payload = {"event": event, **data}
    try:
        line = f"{EVENT_PREFIX} {json.dumps(payload, default=str)}\n"
        with _emit_lock:
            sys.__stdout__.write(line)  # type: ignore[union-attr]
            sys.__stdout__.flush()  # type: ignore[union-attr]
    except (OSError, ValueError, AttributeError):  # stdout closed: progress events are best effort
        pass


class NotebookExit(BaseException):  # noqa: N818 - control-flow signal, deliberately not an Exception
    """Raised by :func:`exit_notebook` outside kernels to stop a script; carries the exit value."""

    def __init__(self, value: Any = None) -> None:
        super().__init__("notebook exit")
        self.value = value


def _to_jsonable(value: Any) -> Any:
    try:
        json.dumps(value)
        return value
    except (TypeError, ValueError):
        return str(value)


def exit_notebook(value: Any = None) -> None:
    """End the current notebook/script run with ``value`` (the job's ``exit_value``)."""
    value = _to_jsonable(value)
    ip = _get_ipython()
    if ip is not None and getattr(ip, "kernel", None) is not None:
        from IPython.display import display  # noqa: PLC0415

        display({EXIT_MIME: {"value": value}}, raw=True)
        raise NotebookExit(value)
    raise NotebookExit(value)


def _get_ipython() -> Any:
    try:
        from IPython import get_ipython  # noqa: PLC0415
    except ImportError:
        return None
    return get_ipython()


# --------------------------------------------------------------------------------------------- environment
def load_job_spec(path: str) -> dict[str, Any]:
    with open(path, encoding="utf-8") as fh:
        return json.load(fh)


def apply_job_env(spec: dict[str, Any]) -> list[str]:
    """Export job ``env`` / ``secrets`` / ``storage_conf.env`` into ``os.environ``; return secret *values* for log scrubbing."""
    secret_values: list[str] = []
    for k, v in (spec.get("env") or {}).items():
        os.environ[str(k)] = str(v)
    for k, v in ((spec.get("storage_conf") or {}).get("env") or {}).items():
        os.environ[str(k)] = str(v)
        if len(str(v)) >= 6:
            secret_values.append(str(v))
    for k, v in (spec.get("secrets") or {}).items():
        os.environ[str(k)] = str(v)
        if len(str(v)) >= 4:
            secret_values.append(str(v))
    apply_layer(spec)
    return secret_values


def apply_layer(spec: dict[str, Any]) -> str | None:
    """Put the node-built dependency layer (``spec.runtime.layer.path``) in front of the runtime's site-packages: ``sys.path`` for this process (driver / kernel) and
    ``PYTHONPATH`` for everything it starts (notebook kernel, Spark python workers on this node, ``spark-submit``).  Idempotent; returns the directory."""
    path = ((spec.get("runtime") or {}).get("layer") or {}).get("path")
    if not path or not os.path.isdir(path):
        return None
    if path in sys.path:
        sys.path.remove(path)
    sys.path.insert(0, path)
    parts = [p for p in os.environ.get("PYTHONPATH", "").split(os.pathsep) if p and p != path]
    os.environ["PYTHONPATH"] = os.pathsep.join([path, *parts])
    return path


# --------------------------------------------------------------------------------------------- lazy spark
class LazySpark:
    """Proxy that creates the real ``SparkSession`` on first attribute access.

    ``%%configure`` (Fabric semantics) must be able to set Spark conf *before* the session exists, and
    pure-python jobs must not pay JVM start-up. ``materialize()`` forces creation; ``reset()`` stops the
    session so the next access builds a new one with the updated configuration.
    """

    def __init__(self, spec: dict[str, Any], overrides: dict[str, Any] | None = None) -> None:
        object.__setattr__(self, "_spec", spec)
        object.__setattr__(self, "_overrides", overrides if overrides is not None else {})
        object.__setattr__(self, "_session", None)
        object.__setattr__(self, "_lock", threading.RLock())

    @property
    def overrides(self) -> dict[str, Any]:
        return object.__getattribute__(self, "_overrides")

    @property
    def materialized(self) -> bool:
        return object.__getattribute__(self, "_session") is not None

    def materialize(self) -> Any:
        lock = object.__getattribute__(self, "_lock")
        with lock:
            session = object.__getattribute__(self, "_session")
            if session is None:
                from olcf.runtime_lib import spark as spark_mod  # noqa: PLC0415

                session = spark_mod.create_session(object.__getattribute__(self, "_spec"), self.overrides)
                object.__setattr__(self, "_session", session)
            return session

    def reset(self) -> None:
        lock = object.__getattribute__(self, "_lock")
        with lock:
            session = object.__getattribute__(self, "_session")
            if session is not None:
                session.stop()
                object.__setattr__(self, "_session", None)

    def stop(self) -> None:
        self.reset()

    def __getattr__(self, name: str) -> Any:
        return getattr(self.materialize(), name)

    def __setattr__(self, name: str, value: Any) -> None:
        setattr(self.materialize(), name, value)

    def __repr__(self) -> str:
        return "<LazySpark (materialised)>" if self.materialized else "<LazySpark (not started)>"


# --------------------------------------------------------------------------------------------- display()
def display(obj: Any = None, *, n: int = 1000, **_: Any) -> None:
    """Minimal ``display()``: Spark DataFrames render as an HTML/text table (first ``n`` rows), others via IPython."""
    if obj is None:
        return
    ip = _get_ipython()
    mod = type(obj).__module__ or ""
    is_spark_df = mod.startswith("pyspark") and hasattr(obj, "toPandas") and hasattr(obj, "limit")
    if is_spark_df:
        pdf = obj.limit(n).toPandas() if _has_pandas() else None
        if pdf is not None and ip is not None:
            from IPython.display import display as ipy_display  # noqa: PLC0415

            ipy_display(pdf)
            return
        rows = obj.limit(n).collect()
        cols = obj.columns
        print("\t".join(cols))
        for r in rows:
            print("\t".join("" if v is None else str(v) for v in r))
        return
    if ip is not None:
        from IPython.display import display as ipy_display  # noqa: PLC0415

        ipy_display(obj)
    else:
        print(obj)


def _has_pandas() -> bool:
    try:
        import pandas  # noqa: F401, PLC0415
    except ImportError:
        return False
    return True


# --------------------------------------------------------------------------------------------- notebookutils
def _fallback_notebookutils() -> types.SimpleNamespace:
    """Smallest useful ``notebookutils``: ``notebook.exit`` only (FABRIC's package provides the real thing)."""
    ns = types.SimpleNamespace()
    ns.notebook = types.SimpleNamespace(exit=exit_notebook)
    ns.help = lambda *a, **k: print("olcf fallback notebookutils: only notebook.exit(value) is available")
    return ns


def install_notebookutils(spec: dict[str, Any], spark: Any) -> Any:
    """Call FABRIC's ``olcf.runtime_lib.notebookutils.install(job_spec, spark)`` if the module exists, else install the shim.

    Returns the object user code should see as ``notebookutils``. Errors other than the *module itself* being missing
    propagate (a broken notebookutils must fail the job loudly, not silently degrade).
    """
    try:
        import olcf.runtime_lib.notebookutils as nbu  # noqa: PLC0415
    except ModuleNotFoundError as exc:
        if exc.name not in ("olcf.runtime_lib.notebookutils", "olcf.runtime_lib"):
            raise
        nbu = None
    if nbu is not None and hasattr(nbu, "install"):
        result = nbu.install(spec, spark)
        utils = result if result is not None else sys.modules.get("notebookutils", nbu)
    else:
        utils = _fallback_notebookutils()
    sys.modules.setdefault("notebookutils", utils)  # type: ignore[arg-type]
    builtins.notebookutils = utils  # type: ignore[attr-defined]
    return utils


def install_lineage_probe(spec: dict[str, Any]) -> None:
    """Start the runtime lineage probe (:mod:`olcf.runtime_lib.lineage_probe`) for this process: it reports which datasets user code reads and writes as
    ``@@OLCF@@ dataset_*`` events. Lazy hooks only (nothing heavy is imported); an error here never fails the job."""
    try:
        from olcf.runtime_lib import lineage_probe  # noqa: PLC0415

        lineage_probe.install(spec)
    except Exception:  # noqa: BLE001 - lineage is observability, never a reason to fail user code
        pass


def build_namespace(spec: dict[str, Any], spark: Any) -> dict[str, Any]:
    """Names predefined for user code: ``spark``, ``sc`` (lazy), ``sqlContext``, ``display``, ``notebookutils``, ``mssparkutils``."""
    install_lineage_probe(spec)
    utils = install_notebookutils(spec, spark)
    ns: dict[str, Any] = {"spark": spark, "display": display, "notebookutils": utils}
    ns["mssparkutils"] = sys.modules.get("mssparkutils", utils)
    ns["sc"] = _LazyAttr(spark, "sparkContext")
    ns["sqlContext"] = _LazyAttr(spark, "_wrapped")
    return ns


class _LazyAttr:
    """Proxy for ``spark.sparkContext`` so ``sc`` exists without starting Spark early."""

    def __init__(self, spark: Any, attr: str) -> None:
        object.__setattr__(self, "_spark", spark)
        object.__setattr__(self, "_attr", attr)

    def _target(self) -> Any:
        spark = object.__getattribute__(self, "_spark")
        attr = object.__getattribute__(self, "_attr")
        if attr == "_wrapped":
            from pyspark.sql import SQLContext  # noqa: PLC0415

            return SQLContext(spark.sparkContext, spark)  # noqa: S608 - not SQL
        return getattr(spark, attr)

    def __getattr__(self, name: str) -> Any:
        return getattr(self._target(), name)

    def __repr__(self) -> str:
        return f"<lazy {object.__getattribute__(self, '_attr')}>"
