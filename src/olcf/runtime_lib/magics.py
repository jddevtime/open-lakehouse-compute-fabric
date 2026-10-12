# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""IPython magics available in OLCF kernels (interactive and batch): ``%%sql``, ``%%pyspark``, ``%%configure``, ``%run``, ``%pip``.

Semantics follow Microsoft Fabric notebooks where they exist and fail closed (CONTRACTS section 0) where OLCF cannot honour
them:

* ``%%sql [-o VAR] [--limit N]`` - run one or more Spark SQL statements; the last result is displayed (and stored in ``VAR``).
* ``%%pyspark`` - run the cell as Python (the default language).
* ``%%configure [-f]`` + JSON - Spark settings applied **before** the session starts (``conf``, ``driverMemory``, ``driverCores``,
  ``executorMemory``, ``executorCores``, ``numExecutors``, ``jars``, ``pyFiles``, ``files``, ``archives``, ``name``,
  ``defaultLakehouse``). If the session already exists the cell fails unless ``-f`` is given (the session is then restarted).
  Platform-managed keys (master, driver networking, auth, ``spark.jars.packages``) and unknown keys are rejected.
* ``%run <notebook> [{"param": value}]`` - run another notebook inline in this session. Notebooks are looked up in the job spec's
  ``notebooks`` map (name -> file) or as a file below the job's ``source/`` directory; anything else is rejected.
* ``%pip ...`` - never installs by default. ``install`` is converted into a **declared-environment check**: requirements already
  satisfied by the runtime's environment are accepted, anything else raises :class:`DependencyBlocked` (category ``dependency``)
  unless the job policy sets ``allow_pip_install`` (then pip installs into a per-job layer directory on ``sys.path``).
"""

from __future__ import annotations

import argparse
import importlib.metadata as md
import json
import os
import shlex
import subprocess
import sys
from pathlib import Path
from typing import Any

from olcf.runtime_lib import paths
from olcf.runtime_lib.bootstrap import LazySpark, emit_event
from olcf.runtime_lib.spark import is_protected

_CONFIGURE_KEYS = {
    "conf", "driverMemory", "driverCores", "executorMemory", "executorCores", "numExecutors", "jars", "pyFiles", "files", "archives", "name",
    "defaultLakehouse", "queue", "proxyUser",
}
_IGNORED_KEYS = {"useStarterPool", "useWorkspacePool", "sessionTimeoutInMinutes"}  # Fabric pool selectors: accepted, no OLCF equivalent


class DependencyBlocked(Exception):  # noqa: N818 - public name used in docs/tests
    """Raised for notebook-time package installs that are not part of the declared environment."""

    olcf_category = "dependency"
    olcf_remediation = ("Add the package to the job's Environment (hash-pinned dependency lock) and re-run, or ask an administrator to set "
                        "policy.allow_pip_install for this job.")


class MagicError(Exception):
    """Invalid magic usage or rejected configuration (category user-code unless ``olcf_category`` says otherwise)."""


# --------------------------------------------------------------------------------------------- SQL helpers
def split_statements(sql: str, *, strict: bool = False) -> list[str]:
    """Split on ``;`` outside quotes/comments (single, double, backtick quotes; ``--``, ``//`` is not SQL; ``/* */``).

    ``strict=True`` (used by the Livy provider, which must not guess) raises ``ValueError("unterminated ...")`` for a quote or block comment that never closes, instead of
    returning the rest of the text as one statement."""
    out: list[str] = []
    buf: list[str] = []
    i, n = 0, len(sql)
    quote: str | None = None
    while i < n:
        ch = sql[i]
        nxt = sql[i + 1] if i + 1 < n else ""
        if quote:
            buf.append(ch)
            if ch == "\\" and quote != "`":
                if nxt:
                    buf.append(nxt)
                    i += 1
            elif ch == quote:
                if nxt == quote:  # doubled quote escape
                    buf.append(nxt)
                    i += 1
                else:
                    quote = None
        elif ch in ("'", '"', "`"):
            quote = ch
            buf.append(ch)
        elif ch == "-" and nxt == "-":
            j = sql.find("\n", i)
            j = n if j == -1 else j
            buf.append(sql[i:j])
            i = j - 1
        elif ch == "/" and nxt == "*":
            j = sql.find("*/", i + 2)
            if j == -1 and strict:
                raise ValueError("unterminated block comment")
            j = n if j == -1 else j + 2
            buf.append(sql[i:j])
            i = j - 1
        elif ch == ";":
            stmt = "".join(buf).strip()
            if _has_code(stmt):
                out.append(stmt)
            buf = []
        else:
            buf.append(ch)
        i += 1
    if quote and strict:
        raise ValueError(f"unterminated quoted string or identifier (opening {quote})")
    tail = "".join(buf).strip()
    if _has_code(tail):
        out.append(tail)
    return out


def _has_code(stmt: str) -> bool:
    """True when ``stmt`` contains something other than comments/whitespace."""
    s = stmt
    while True:
        s = s.strip()
        if s.startswith("--"):
            j = s.find("\n")
            s = "" if j == -1 else s[j + 1:]
        elif s.startswith("/*"):
            j = s.find("*/")
            s = "" if j == -1 else s[j + 2:]
        else:
            return bool(s)


def run_sql(spark: Any, sql: str) -> Any:
    """Execute every statement, return the DataFrame of the last one (``None`` if there was no statement)."""
    result = None
    for stmt in split_statements(sql):
        result = spark.sql(stmt)
    return result


# --------------------------------------------------------------------------------------------- %%configure
def configure_to_conf(cfg: dict[str, Any], approved: list[str] | None = None) -> tuple[dict[str, str], dict[str, Any]]:
    """Translate a Fabric ``%%configure`` document into ``(spark_conf, extras)`` (extras: name, defaultLakehouse)."""
    unknown = set(cfg) - _CONFIGURE_KEYS - _IGNORED_KEYS
    if unknown:
        raise MagicError(f"%%configure: unsupported key(s) {sorted(unknown)}; supported: {sorted(_CONFIGURE_KEYS)}")
    conf: dict[str, str] = {}
    mapping = {"driverMemory": "spark.driver.memory", "driverCores": "spark.driver.cores", "executorMemory": "spark.executor.memory",
               "executorCores": "spark.executor.cores", "numExecutors": "spark.executor.instances", "name": "spark.app.name"}
    for key, sk in mapping.items():
        if key in cfg:
            conf[sk] = str(cfg[key])
    for key, sk in (("jars", "spark.jars"), ("pyFiles", "spark.submit.pyFiles"), ("files", "spark.files"), ("archives", "spark.archives")):
        if key in cfg:
            conf[sk] = ",".join(str(x) for x in cfg[key]) if isinstance(cfg[key], list) else str(cfg[key])
    for k, v in (cfg.get("conf") or {}).items():
        conf[str(k)] = ("true" if v else "false") if isinstance(v, bool) else str(v)
    for k in conf:
        if is_protected(k):
            raise MagicError(f"%%configure: {k} is managed by the platform and cannot be set from a notebook")
    if approved:
        import fnmatch

        bad = [k for k in conf if not any(fnmatch.fnmatch(k, pat) for pat in approved)]
        if bad:
            raise MagicError(f"%%configure: setting(s) {bad} are not on the runtime's approved Spark configuration list")
    extras = {k: cfg[k] for k in ("name", "defaultLakehouse") if k in cfg}
    return conf, extras


# --------------------------------------------------------------------------------------------- %pip
def check_requirements(reqs: list[str]) -> tuple[list[str], list[str]]:
    """Return ``(satisfied, missing)`` for pip requirement strings against the installed distributions."""
    from packaging.requirements import InvalidRequirement, Requirement

    ok: list[str] = []
    missing: list[str] = []
    for text in reqs:
        try:
            req = Requirement(text)
        except InvalidRequirement:
            missing.append(text)
            continue
        if req.marker is not None and not req.marker.evaluate():
            ok.append(text)
            continue
        try:
            installed = md.version(req.name)
        except md.PackageNotFoundError:
            missing.append(text)
            continue
        if req.url is None and (not req.specifier or req.specifier.contains(installed, prereleases=True)):
            ok.append(text)
        else:
            missing.append(f"{text} (installed: {installed})")
    return ok, missing


def pip_magic(line: str, spec: dict[str, Any]) -> str:
    """Implementation of ``%pip``; returns text to print. See module docstring for policy."""
    args = shlex.split(line)
    if not args:
        raise MagicError("%pip: missing subcommand (install | list | freeze | show)")
    sub, rest = args[0], args[1:]
    if sub in ("list", "freeze"):
        rows = sorted(f"{d.metadata['Name']}=={d.version}" for d in md.distributions())
        return "\n".join(rows)
    if sub == "show":
        out = []
        for name in rest:
            try:
                d = md.distribution(name)
                out.append(f"Name: {d.metadata['Name']}\nVersion: {d.version}")
            except md.PackageNotFoundError:
                out.append(f"WARNING: Package(s) not found: {name}")
        return "\n".join(out)
    if sub != "install":
        raise DependencyBlocked(f"%pip {sub} is blocked: the runtime environment is immutable and declared in the Environment item")
    reqs: list[str] = []
    skip = False
    for a in rest:
        if skip:
            skip = False
            continue
        if a in ("-q", "--quiet", "-U", "--upgrade", "--no-deps", "--user", "--pre"):
            continue
        if a in ("-r", "--requirement", "-e", "--editable", "-i", "--index-url", "--extra-index-url", "-f", "--find-links", "-c", "--constraint"):
            raise DependencyBlocked(f"%pip install {a} is blocked: use the declared Environment (hash-pinned) instead")
        if a.startswith("-"):
            raise DependencyBlocked(f"%pip install option {a} is not supported")
        reqs.append(a)
    if not reqs:
        raise MagicError("%pip install: no requirements given")
    satisfied, missing = check_requirements(reqs)
    if not missing:
        return "Requirement(s) already satisfied by the declared environment: " + ", ".join(satisfied)
    policy = spec.get("policy") or {}
    if not policy.get("allow_pip_install"):
        raise DependencyBlocked(f"%pip install is blocked by policy: not in the declared environment: {', '.join(missing)}")
    layer = Path(os.environ.get("OLCF_JOB_DIR") or ".") / "pip-layer"
    layer.mkdir(parents=True, exist_ok=True)
    cmd = [sys.executable, "-m", "pip", "install", "--disable-pip-version-check", "--target", str(layer), *missing_names(missing)]
    res = subprocess.run(cmd, capture_output=True, text=True, timeout=1800, check=False)  # noqa: S603
    if res.returncode != 0:
        raise DependencyBlocked(f"pip install failed: {res.stderr[-800:]}")
    if str(layer) not in sys.path:
        sys.path.insert(0, str(layer))
    os.environ["PYTHONPATH"] = os.pathsep.join(filter(None, [str(layer), os.environ.get("PYTHONPATH", "")]))
    emit_event("pip_install", requirements=missing_names(missing), layer=str(layer))
    return "Installed into the per-job layer (driver only; executors do not see it): " + ", ".join(missing_names(missing))


def missing_names(missing: list[str]) -> list[str]:
    return [m.split(" (installed")[0] for m in missing]


# --------------------------------------------------------------------------------------------- notebook lookup for %run
def find_notebook(name: str, spec: dict[str, Any]) -> Path:
    """Resolve a ``%run`` target: ``spec['notebooks'][name]`` or a file below ``<job dir>/source``."""
    base = Path(os.environ.get("OLCF_JOB_DIR") or ".").resolve()
    mapped = (spec.get("notebooks") or {}).get(name)
    cand = base / mapped if mapped else None
    if cand is None:
        src = (base / "source").resolve()
        for ext in ("", ".ipynb", ".py"):
            p = (src / (name + ext)).resolve()
            if str(p).startswith(str(src) + os.sep) and p.is_file():
                cand = p
                break
    if cand is None or not cand.resolve().is_file() or not str(cand.resolve()).startswith(str(base)):
        raise MagicError(f"%run {name}: notebook not available to this job (not in spec.notebooks and not under source/); "
                         "referenced notebooks must be bundled with the capsule")
    return cand.resolve()


# --------------------------------------------------------------------------------------------- registration
def register_magics(ip: Any, spec: dict[str, Any], spark: LazySpark) -> None:
    """Register the OLCF magics on an IPython shell."""
    approved = (spec.get("spark") or {}).get("approved_conf")

    def sql(line: str, cell: str) -> Any:
        ap = argparse.ArgumentParser(prog="%%sql", add_help=False)
        ap.add_argument("-o", "--output", default=None)
        ap.add_argument("--limit", type=int, default=1000)
        ap.add_argument("-q", "--quiet", action="store_true")
        ns, _ = ap.parse_known_args(shlex.split(line))
        df = run_sql(spark, cell)
        if df is None:
            return None
        if ns.output:
            ip.user_ns[ns.output] = df
        if not ns.quiet and df.columns:
            from olcf.runtime_lib.bootstrap import display

            display(df, n=ns.limit)
        return None

    def pyspark(line: str, cell: str) -> Any:
        res = ip.run_cell(cell, store_history=False)
        res.raise_error()

    def configure(line: str, cell: str) -> None:
        force = "-f" in shlex.split(line) or "--force" in shlex.split(line)
        try:
            cfg = json.loads(cell) if cell.strip() else {}
        except ValueError as exc:
            raise MagicError(f"%%configure: invalid JSON: {exc}") from exc
        conf, extras = configure_to_conf(cfg, approved)
        if spark.materialized:
            if not force:
                raise MagicError("%%configure: the Spark session is already running; use `%%configure -f` to restart it with the new settings")
            spark.reset()
        spark.overrides.update(conf)
        if "defaultLakehouse" in extras:
            ref = extras["defaultLakehouse"]
            lh = paths.set_default_lakehouse(ref.get("name") or ref.get("id") if isinstance(ref, dict) else str(ref))
            object.__getattribute__(spark, "_spec")["default_lakehouse"] = lh
        emit_event("spark_configured", conf=sorted(conf), restarted=force)
        print("Spark configuration recorded" + (" (session restarted)" if force else "") + ": " + ", ".join(sorted(conf)))

    def pip(line: str) -> None:
        print(pip_magic(line, spec))

    def run(line: str) -> None:
        parts = line.strip().split(None, 1)
        if not parts:
            raise MagicError("%run: notebook name required")
        name = parts[0].strip("'\"")
        params: dict[str, Any] = {}
        if len(parts) > 1 and parts[1].strip():
            try:
                params = json.loads(parts[1])
            except ValueError as exc:
                raise MagicError(f"%run: parameters must be a JSON object: {exc}") from exc
        from olcf.runtime_lib.executor import load_notebook_cells

        cells = load_notebook_cells(find_notebook(name, spec))
        if params:
            ip.run_cell("\n".join(f"{k} = {v!r}" for k, v in params.items()), store_history=False).raise_error()
        for cell in cells:
            if cell["cell_type"] == "code" and cell["source"].strip():
                ip.run_cell(cell["source"], store_history=False).raise_error()

    # NB: not the decorator API - those need a global get_ipython(); here we hold the shell object explicitly
    ip.register_magic_function(sql, "cell", "sql")
    ip.register_magic_function(pyspark, "cell", "pyspark")
    ip.register_magic_function(configure, "cell", "configure")
    ip.register_magic_function(pip, "line", "pip")
    ip.register_magic_function(run, "line", "run")
