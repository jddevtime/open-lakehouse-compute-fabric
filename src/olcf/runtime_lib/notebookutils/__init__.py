# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""``notebookutils`` / ``mssparkutils`` for OLCF runs - the NODE hook N7 implementation (stdlib only; runs inside runtime venvs).

Contract with the runner (``olcf.runtime_lib.bootstrap``):

* :func:`install` ``(job_spec, spark)`` builds the facade, registers it as ``notebookutils`` **and** ``mssparkutils`` in ``sys.modules`` (so
  ``import notebookutils``, ``from notebookutils import mssparkutils`` and ``import mssparkutils`` all work), exposes ``displayHTML`` and returns the facade;
* :func:`resolve_path` ``(path, *, for_, spec)`` is the path hook of ``olcf.runtime_lib.paths.resolve`` (return ``None`` to decline).

What is implemented (everything else raises :class:`~olcf.runtime_lib.notebookutils.errors.NotSupportedError` with remediation - never a silent no-op):

=====================  =====================================================================================
``runtime.context``    rebuilt from ``job_spec["context"]["fabric"]`` (the dict the notebook saw in Fabric)
``fs``                 ls / mkdirs / cp / fastcp / mv / rm / head / put / append / exists (Hadoop FS or local)
``notebook``           exit / run / runMultiple (child jobs through the job-scoped token)
``credentials``        getSecret (granted secrets only), getToken (storage / OneLake audience)
``lakehouse``          get / list (lakehouses attached to the job)
``session``            stop
``env``                getWorkspaceName / getWorkspaceId / getUserName / getUserId / getJobId
=====================  =====================================================================================

Not Microsoft Fabric software: this is an independent, API-compatible subset implemented for the OLCF "Fabric Runtime 2.0 Compatible" profile.
"""

from __future__ import annotations

import builtins
import posixpath
import re
import sys
import types
import urllib.parse
import urllib.request
from typing import Any

from olcf.runtime_lib.notebookutils import context as ctxmod
from olcf.runtime_lib.notebookutils.credentials import CredentialsNS
from olcf.runtime_lib.notebookutils.errors import (
    LakehouseNotAttached,
    LakehouseNotFound,
    NotSupportedError,
    SecretNotGranted,
)
from olcf.runtime_lib.notebookutils.fs import FS, FileInfo
from olcf.runtime_lib.notebookutils.lakehouse import LakehouseNS
from olcf.runtime_lib.notebookutils.naming import normalise_vault, secret_env_name
from olcf.runtime_lib.notebookutils.notebook import NotebookNS

SHIM_VERSION = "1.0.0"
__all__ = ["SHIM_VERSION", "FileInfo", "LakehouseNotAttached", "LakehouseNotFound", "NotSupportedError", "SecretNotGranted", "install", "normalise_vault", "resolve_path",
           "secret_env_name", "uninstall"]

_STATE: dict[str, Any] = {"spec": {}, "spark": None}
_ONELAKE_URL = re.compile(r"^abfss?://(?P<ws>[^@/]+)@onelake\.(?:dfs|blob)\.fabric\.microsoft\.com(?:/(?P<item>[^/]+))?(?:/(?P<area>Files|Tables)(?:/(?P<rest>.*))?)?$", re.I)
_MOUNT = re.compile(r"^/lakehouse/(?P<name>[^/]+)(?:/(?P<area>Files|Tables)(?:/(?P<rest>.*))?)?$")

_FABRIC_ONLY = {
    "variableLibrary": ("Variable libraries are Fabric workspace items.", "Pass the values as declared notebook parameters (olcf.run_remote(parameters={...})) or use an OLCF variable set."),
    "connections": ("Fabric connections are a Fabric service.", "Use an OLCF storage connection (local, s3/minio, adls, onelake) for the data, put other settings in a Spark job definition or variable set, or call the service directly with credentials granted to the job."),
    "warehouse": ("Fabric warehouses are a Fabric service.", "Read the warehouse tables from OneLake (Delta), or export them to a storage connection OLCF can read (OLCF has no JDBC connection type)."),
    "udf": ("Fabric user data functions are a Fabric service.", "Call the function's HTTP endpoint with a granted credential."),
    "data": ("Fabric data utilities are a Fabric service.", "Use Spark to read and write the data."),
    "cognitiveservices": ("Azure AI services are reached through the Fabric session identity.", "Call the service directly with a credential granted to the job."),
    "visualization": ("Fabric visualization helpers are part of the Fabric notebook UI.", "Use matplotlib/plotly or display()."),
    "user": ("Fabric user utilities need the interactive session.", "Pass the values as declared notebook parameters."),
}


class _UnsupportedArea:
    """A whole Fabric-only area (``notebookutils.warehouse`` ...): every access explains why and what to do instead."""

    def __init__(self, name: str) -> None:
        object.__setattr__(self, "_name", name)

    def __getattr__(self, attr: str) -> Any:
        if attr.startswith("_"):
            raise AttributeError(attr)
        name = object.__getattribute__(self, "_name")
        reason, fix = _FABRIC_ONLY[name]
        raise NotSupportedError(f"notebookutils.{name}.{attr}", reason, fix)

    def __call__(self, *args: Any, **kwargs: Any) -> Any:
        name = object.__getattribute__(self, "_name")
        reason, fix = _FABRIC_ONLY[name]
        raise NotSupportedError(f"notebookutils.{name}", reason, fix)

    def __repr__(self) -> str:
        return f"<notebookutils.{object.__getattribute__(self, '_name')}: not supported in OLCF>"


class RuntimeNS:
    """``notebookutils.runtime``."""

    def __init__(self, state: dict[str, Any]) -> None:
        self._state = state

    @property
    def context(self) -> dict[str, Any]:
        return ctxmod.build(self._state["spec"])

    def restartPython(self, *args: Any, **kwargs: Any) -> Any:  # noqa: N802
        raise NotSupportedError("notebookutils.runtime.restartPython", "restarting the Python interpreter would discard the remote run's state",
                                "Remove restartPython(); declare packages in the Environment instead of installing them in the notebook.")

    def help(self, method_name: str = "") -> None:
        print("notebookutils.runtime (OLCF):\n  context -> dict (Fabric keys: currentNotebookName, currentWorkspaceId, defaultLakehouseName, ...)\n"
              "  restartPython: not supported in OLCF")


class SessionNS:
    """``notebookutils.session``."""

    def __init__(self, state: dict[str, Any]) -> None:
        self._state = state

    def stop(self) -> None:
        spark = self._state.get("spark")
        if spark is not None:
            spark.stop()

    def help(self, method_name: str = "") -> None:
        print("notebookutils.session (OLCF):\n  stop() -> stops the job's Spark session")


class EnvNS:
    """``notebookutils.env`` (legacy ``mssparkutils.env``)."""

    def __init__(self, state: dict[str, Any]) -> None:
        self._state = state

    def _c(self) -> dict[str, Any]:
        return ctxmod.build(self._state["spec"])

    def getWorkspaceName(self) -> Any:  # noqa: N802
        return self._c().get("currentWorkspaceName")

    def getWorkspaceId(self) -> Any:  # noqa: N802
        return self._c().get("currentWorkspaceId")

    def getUserName(self) -> Any:  # noqa: N802
        return self._c().get("userName")

    def getUserId(self) -> Any:  # noqa: N802
        return self._c().get("userId")

    def getJobId(self) -> Any:  # noqa: N802
        return self._c().get("currentRunId")

    def __getattr__(self, name: str) -> Any:
        if name in {"getClusterId", "getPoolName", "getLakehouseId"}:
            def _raise(*a: Any, **k: Any) -> Any:
                raise NotSupportedError(f"notebookutils.env.{name}", "Fabric cluster/pool details do not exist on OLCF",
                                        "Use notebookutils.runtime.context for workspace/lakehouse identity and the Spark UI for cluster details.")
            return _raise
        if name.startswith("_"):
            raise AttributeError(name)
        raise NotSupportedError(f"notebookutils.env.{name}")

    def help(self, method_name: str = "") -> None:
        print("notebookutils.env (OLCF):\n  getWorkspaceName / getWorkspaceId / getUserName / getUserId / getJobId")


class _Facade(types.ModuleType):
    """The object user code sees as ``notebookutils`` (and ``mssparkutils``)."""

    def __getattr__(self, name: str) -> Any:
        if name.startswith("_"):
            raise AttributeError(name)
        raise NotSupportedError(f"notebookutils.{name}")

    def help(self, method_name: str = "") -> None:  # noqa: D102 - printed help
        print(
            "notebookutils / mssparkutils (OLCF shim " + SHIM_VERSION + ", Fabric Runtime 2.0 Compatible subset)\n"
            "  fs           ls mkdirs cp fastcp mv rm head put append exists   (mount/unmount/mounts/getMountPath: not supported)\n"
            "  notebook     exit run runMultiple                               (item management: not supported)\n"
            "  credentials  getSecret (granted secrets) getToken('storage')\n"
            "  lakehouse    get list                                           (attached lakehouses only)\n"
            "  runtime      context                                            (restartPython: not supported)\n"
            "  session      stop\n"
            "  env          getWorkspaceName getWorkspaceId getUserName getUserId getJobId\n"
            "  not supported in OLCF: " + ", ".join(sorted(_FABRIC_ONLY)))


def displayHTML(html: str) -> None:  # noqa: N802 - Fabric name
    """Render HTML in the output notebook (falls back to printing the markup outside a notebook)."""
    try:
        from IPython.display import HTML, display  # noqa: PLC0415

        display(HTML(html))
    except ImportError:
        print(html)


def _build(state: dict[str, Any]) -> _Facade:
    f = _Facade("notebookutils", "OLCF notebookutils shim (Fabric Runtime 2.0 Compatible subset)")
    f.__version__ = SHIM_VERSION  # type: ignore[attr-defined]
    f.fs = FS(state)  # type: ignore[attr-defined]
    f.notebook = NotebookNS(state)  # type: ignore[attr-defined]
    f.credentials = CredentialsNS(state)  # type: ignore[attr-defined]
    f.lakehouse = LakehouseNS(state)  # type: ignore[attr-defined]
    f.runtime = RuntimeNS(state)  # type: ignore[attr-defined]
    f.session = SessionNS(state)  # type: ignore[attr-defined]
    f.env = EnvNS(state)  # type: ignore[attr-defined]
    for n in _FABRIC_ONLY:
        setattr(f, n, _UnsupportedArea(n))
    f.mssparkutils = f  # type: ignore[attr-defined]
    f.notebookutils = f  # type: ignore[attr-defined]
    f.NotSupportedError = NotSupportedError  # type: ignore[attr-defined]
    return f


_INSTALLED_NAMES = ("notebookutils", "mssparkutils", "notebookutils.mssparkutils", "notebookutils.fs", "notebookutils.notebook", "notebookutils.credentials",
                    "notebookutils.lakehouse", "notebookutils.runtime", "notebookutils.session", "notebookutils.env")


def install(spec: dict[str, Any], spark: Any) -> _Facade:
    """NODE hook N7: build and register the shim for ``spec``. Idempotent per process (a second call rebinds to the new spec)."""
    _STATE["spec"], _STATE["spark"] = spec or {}, spark
    facade = _build(_STATE)
    for name in _INSTALLED_NAMES:
        sys.modules[name] = facade if name in ("notebookutils", "mssparkutils", "notebookutils.mssparkutils") else getattr(facade, name.split(".", 1)[1])  # type: ignore[assignment]
    builtins.notebookutils = facade  # type: ignore[attr-defined]
    builtins.mssparkutils = facade  # type: ignore[attr-defined]
    builtins.displayHTML = displayHTML  # type: ignore[attr-defined]
    return facade


def uninstall() -> None:
    """Remove what :func:`install` registered (tests and kernels that restart)."""
    for name in _INSTALLED_NAMES:
        sys.modules.pop(name, None)
    for attr in ("notebookutils", "mssparkutils", "displayHTML"):
        if hasattr(builtins, attr):
            delattr(builtins, attr)


# =============================================================================================== path hook
def _join(base: str, rel: str) -> str:
    norm = posixpath.normpath(rel) if rel else ""
    if norm == ".." or norm.startswith("../"):
        raise ValueError(f"path {rel!r} escapes the lakehouse area")
    norm = "" if norm == "." else norm
    return base.rstrip("/") + ("/" + norm if norm else "")


def _to_local(uri: str) -> str:
    return urllib.request.url2pathname(urllib.parse.urlparse(uri).path) if uri.startswith("file:") else uri


def _lakehouses(spec: dict[str, Any]) -> list[dict[str, Any]]:
    return [lh for lh in [spec.get("default_lakehouse"), *(spec.get("lakehouses") or [])] if lh]


def resolve_path(path: str, *, for_: str = "spark", spec: dict[str, Any] | None = None) -> str | None:
    """Path hook of :func:`olcf.runtime_lib.paths.resolve` (CONTRACTS N7). Returns ``None`` to decline (the built-in rules then apply).

    * ``/lakehouse/<default|name>/Files|Tables/...`` - the Fabric FUSE mount spelling. Local lakehouses are rewritten deterministically to the real path
      (``for_="local"``) or ``file://`` URI (``for_="spark"``). For object-store / OneLake lakehouses plain-Python access (``for_="local"``) is **not
      possible** and raises :class:`NotSupportedError` with remediation; Spark access (``for_="spark"``) maps to the lakehouse URI.
    * ``abfss://<ws>@onelake.dfs.fabric.microsoft.com/<lakehouse>/Files|Tables/...`` - when the job has that lakehouse attached under another physical
      location (mirror, local copy), the URL is redirected there; otherwise the URL is used as is (workers read OneLake directly).
    """
    spec = spec if spec is not None else _STATE["spec"]
    if not isinstance(path, str):
        return None
    m = _MOUNT.match(path)
    if m and path.startswith("/lakehouse/"):
        known = _lakehouses(spec)
        name = m.group("name")
        lh = known[0] if name == "default" and known else next((k for k in known if name in (k.get("name"), k.get("id"))), None)
        if lh is None:
            raise LakehouseNotAttached(path, "no default lakehouse is attached to this run" if name == "default" else f"lakehouse '{name}' is not attached to this run",
                                       "Attach a default lakehouse in Fabric (it is captured from the notebook context) or map the Fabric lakehouse to an OLCF lakehouse "
                                       "in the integration settings.")
        area = m.group("area")
        if area is None:
            raise NotSupportedError(path, "the lakehouse root is not a data area", "Use /lakehouse/default/Files/... or /lakehouse/default/Tables/...")
        base = lh["files_uri"] if area == "Files" else lh["tables_uri"]
        local = str(base).startswith("file:") or "://" not in str(base)
        if for_ == "local":
            if not local:
                raise NotSupportedError(path, "the Fabric mount (/lakehouse/default) does not exist for object-store or OneLake lakehouses, so Python file APIs cannot reach the data",
                                        "Use Spark (spark.read...), notebookutils.fs.* or a relative Files/ path with Spark APIs; for pandas read abfss:// with storage "
                                        "options, or copy the file with notebookutils.fs.cp to a local path first.")
            return _to_local(_join(base, m.group("rest") or ""))
        return _join(base, m.group("rest") or "")
    u = _ONELAKE_URL.match(path)
    if u and for_ == "spark":
        item = (u.group("item") or "")
        item_name = item[: -len(".Lakehouse")] if item.lower().endswith(".lakehouse") else item
        for lh in _lakehouses(spec):
            fab = lh.get("fabric") or {}
            if item_name not in (lh.get("name"), fab.get("lakehouse_id"), lh.get("id")):
                continue
            base = lh["files_uri"] if u.group("area") == "Files" else lh["tables_uri"]
            if u.group("area") and "onelake" not in str(base).lower():
                return _join(base, u.group("rest") or "")
            return None
    return None
