# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Batch notebook execution through a real Jupyter kernel (papermill-style, stdlib + ``jupyter_client`` + ``nbformat``).

* Notebook sources: ``ipynb``, plain ``py`` (``# %%`` percent cells or one cell) and Fabric Git source (``notebook-content.py``:
  ``# CELL ****``, ``# MARKDOWN ****``, ``# PARAMETERS CELL ****``, ``# META`` blocks; ``--``/``//`` comment prefixes too).
* Parameters are injected papermill-style: a new cell tagged ``injected-parameters`` is inserted right after the first cell
  tagged ``parameters`` (or at the top when there is none).
* Each code cell is executed in order in one kernel (``olcf.runtime_lib.kernel_host``, which pre-loads ``spark``, ``display``,
  ``notebookutils`` and the ``%%sql`` / ``%%pyspark`` / ``%%configure`` / ``%run`` / ``%pip`` magics). Execution **stops at the first
  error**; the failing cell index (in the *original* notebook) and the line inside that cell are extracted into the failure record.
* ``notebookutils.notebook.exit(value)`` is captured through the reserved display-data MIME type
  ``application/vnd.olcf.exit+json`` and ends the run successfully with that exit value.
* Output size is limited by ``limits.max_output_bytes`` (visible truncation marker); the executed notebook is written to
  ``output.ipynb``.
* Per-cell timeouts interrupt the kernel; :meth:`NotebookExecutor.stop` (used on cancel/SIGTERM) interrupts and shuts it down.
"""

from __future__ import annotations

import copy
import importlib.util
import json
import keyword
import math
import os
import queue
import re
import sys
import threading
import time
import uuid
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from olcf.core.errors import Unsupported
from olcf.runtime_lib import failures
from olcf.runtime_lib.bootstrap import EXIT_MIME, emit_event
from olcf.runtime_lib.logbuf import OutputBudget

NOTEBOOK_FORMAT = 4
_FABRIC_HEADER = re.compile(r"^(#|--|//)\s*Fabric notebook source\s*$")
_SECTION = re.compile(r"^(?P<p>#|--|//)\s*(?P<kind>METADATA|CELL|MARKDOWN|PARAMETERS CELL|INJECTED PARAMETERS)\s*\*{10,}\s*$")


# --------------------------------------------------------------------------------------------- loading
def _new_cell(cell_type: str, source: str, metadata: dict[str, Any] | None = None) -> dict[str, Any]:
    cell: dict[str, Any] = {"cell_type": cell_type, "id": uuid.uuid4().hex[:8], "metadata": metadata or {}, "source": source}
    if cell_type == "code":
        cell["outputs"] = []
        cell["execution_count"] = None
    return cell


def new_notebook(cells: list[dict[str, Any]], metadata: dict[str, Any] | None = None) -> dict[str, Any]:
    return {"nbformat": NOTEBOOK_FORMAT, "nbformat_minor": 5, "metadata": metadata or {}, "cells": cells}


def _join(src: Any) -> str:
    return "".join(src) if isinstance(src, list) else str(src or "")


def parse_ipynb(text: str) -> dict[str, Any]:
    nb = json.loads(text)
    if not isinstance(nb, dict) or "cells" not in nb:
        raise Unsupported("not an nbformat notebook (no 'cells')", remediation="Provide a valid .ipynb (nbformat 4).")
    if int(nb.get("nbformat", 4)) < 4:
        raise Unsupported(f"nbformat {nb.get('nbformat')} is not supported", remediation="Upgrade the notebook to nbformat 4.")
    for c in nb["cells"]:
        c["source"] = _join(c.get("source"))
        c.setdefault("metadata", {})
        c.setdefault("id", uuid.uuid4().hex[:8])
        if c["cell_type"] == "code":
            c.setdefault("outputs", [])
            c.setdefault("execution_count", None)
    nb.setdefault("metadata", {})
    nb.setdefault("nbformat_minor", 5)
    return nb


def parse_py_percent(text: str) -> dict[str, Any]:
    """``# %%`` cell markers (optionally ``[markdown]`` and ``tags=[...]``); no markers -> one code cell."""
    cells: list[dict[str, Any]] = []
    cur: list[str] = []
    ctype, tags = "code", []
    started = False

    def flush() -> None:
        body = "\n".join(cur).strip("\n")
        if started or body.strip():
            if ctype == "markdown":
                body = "\n".join(re.sub(r"^# ?", "", ln) for ln in body.splitlines())
            cells.append(_new_cell(ctype, body, {"tags": list(tags)} if tags else {}))

    for line in text.splitlines():
        m = re.match(r"^#\s*%%(?P<rest>.*)$", line)
        if m:
            if started or cur:
                flush()
            cur, started = [], True
            rest = m.group("rest")
            ctype = "markdown" if re.search(r"\[(markdown|md)\]", rest) else "code"
            tm = re.search(r"tags=(\[[^\]]*\])", rest)
            tags = json.loads(tm.group(1).replace("'", '"')) if tm else []
        else:
            cur.append(line)
    if started or cur:
        flush()
    return new_notebook(cells)


def parse_fabric_git_source(text: str) -> dict[str, Any]:
    """Fabric Git ``notebook-content.py`` (``# Fabric notebook source``) -> nbformat notebook."""
    lines = text.splitlines()
    if not lines or not _FABRIC_HEADER.match(lines[0].strip()):
        raise Unsupported("not a Fabric Git notebook source (missing '# Fabric notebook source' header)")
    prefix = _FABRIC_HEADER.match(lines[0].strip()).group(1)  # type: ignore[union-attr]
    sections: list[tuple[str, list[str]]] = []
    for line in lines[1:]:
        m = _SECTION.match(line.strip())
        if m and m.group("p") == prefix:
            sections.append((m.group("kind"), []))
        elif sections:
            sections[-1][1].append(line)
    nb_meta: dict[str, Any] = {}
    cells: list[dict[str, Any]] = []

    def meta_json(body: list[str]) -> dict[str, Any]:
        js = "\n".join(re.sub(rf"^{re.escape(prefix)}\s?META ?", "", ln) for ln in body if ln.lstrip().startswith(f"{prefix} META") or ln.lstrip().startswith(f"{prefix}META"))
        return json.loads(js) if js.strip() else {}

    for kind, body in sections:
        if kind == "METADATA":
            md = meta_json(body)
            if cells:
                cells[-1]["metadata"].update({"fabric": md})
                lang = md.get("language")
                if lang:
                    cells[-1]["metadata"]["language"] = lang
            else:
                nb_meta.update(md)
        elif kind in ("CELL", "PARAMETERS CELL", "INJECTED PARAMETERS"):
            src = "\n".join(body).strip("\n")
            tags = ["parameters"] if kind == "PARAMETERS CELL" else (["injected-parameters"] if kind == "INJECTED PARAMETERS" else [])
            cells.append(_new_cell("code", src, {"tags": tags} if tags else {}))
        elif kind == "MARKDOWN":
            src = "\n".join(re.sub(rf"^{re.escape(prefix)} ?", "", ln, count=1) for ln in body).strip("\n")
            cells.append(_new_cell("markdown", src))
    for i, c in enumerate(cells):
        lang = str(c["metadata"].get("language", "python")).lower()
        if c["cell_type"] != "code":
            continue
        if lang in ("sparksql", "sql"):
            if not c["source"].lstrip().startswith("%%sql"):
                c["source"] = "%%sql\n" + c["source"]
        elif lang in ("scala", "r", "sparkr", "csharp"):
            raise Unsupported(f"cell {i} uses language {lang!r}, which OLCF notebooks do not execute (python, pyspark and sql only)",
                              remediation="Port the cell to PySpark or SQL, or run it as a Spark job definition with a compiled jar.")
    nb_meta.setdefault("kernelspec", {"name": "python3", "display_name": "Python 3", "language": "python"})
    return new_notebook(cells, nb_meta)


def load_notebook(path: str | Path, fmt: str | None = None) -> dict[str, Any]:
    """Load a notebook source file into an nbformat dict. ``fmt``: ``ipynb`` | ``py`` | ``fabric_git_source`` (default: sniff)."""
    p = Path(path)
    text = p.read_text(encoding="utf-8")
    fmt = (fmt or "").lower()
    if fmt == "fabric_git_source" or (not fmt and _FABRIC_HEADER.match(text.lstrip().splitlines()[0].strip() if text.strip() else "")):
        return parse_fabric_git_source(text)
    if fmt == "ipynb" or (not fmt and p.suffix == ".ipynb") or (not fmt and text.lstrip().startswith("{")):
        return parse_ipynb(text)
    if fmt in ("py", "script", ""):
        if text.lstrip().splitlines()[:1] and _FABRIC_HEADER.match(text.lstrip().splitlines()[0].strip()):
            return parse_fabric_git_source(text)
        return parse_py_percent(text)
    raise Unsupported(f"unsupported notebook format {fmt!r}", remediation="Use ipynb, py or fabric_git_source.")


def load_notebook_cells(path: str | Path) -> list[dict[str, Any]]:
    return load_notebook(path)["cells"]


# --------------------------------------------------------------------------------------------- parameters
def has_parameters_cell(nb: dict[str, Any]) -> bool:
    return any("parameters" in (c.get("metadata", {}).get("tags") or []) for c in nb["cells"])


_MAX_PARAM_DEPTH = 40


def py_literal(value: Any, _depth: int = 0) -> str:
    """Render a JSON-like value as a self-contained Python literal (same rules as ``olcf.notebooks.params.py_literal``; this file must stay stdlib-only).

    ``repr`` alone is wrong for non-finite floats (``nan`` / ``inf`` are names, not literals) and would stringify arbitrary objects; anything outside the
    JSON value set is refused instead of being turned into code.
    """
    if _depth > _MAX_PARAM_DEPTH:
        raise Unsupported("parameter value is nested too deeply")
    if value is None:
        return "None"
    if isinstance(value, bool):
        return "True" if value else "False"
    if isinstance(value, int):
        return str(int(value))
    if isinstance(value, float):
        if math.isnan(value):
            return 'float("nan")'
        if math.isinf(value):
            return 'float("inf")' if value > 0 else 'float("-inf")'
        return repr(float(value))
    if isinstance(value, str):
        return repr(str(value))
    if isinstance(value, (list, tuple)):
        return "[" + ", ".join(py_literal(v, _depth + 1) for v in value) + "]"
    if isinstance(value, dict):
        items = []
        for k, v in value.items():
            if not isinstance(k, str):
                raise Unsupported(f"cannot inject a mapping with non-string key {k!r}")
            items.append(f"{py_literal(k, _depth + 1)}: {py_literal(v, _depth + 1)}")
        return "{" + ", ".join(items) + "}"
    raise Unsupported(f"cannot inject a parameter value of type {type(value).__name__}", remediation="Parameters must be JSON values.")


def parameter_source(params: dict[str, Any]) -> str:
    lines = ["# Parameters injected by OLCF (papermill-compatible)"]
    for name, value in params.items():
        if not isinstance(name, str) or not name.isidentifier() or keyword.iskeyword(name):
            raise Unsupported(f"parameter name {name!r} is not a valid Python identifier")
        lines.append(f"{name} = {py_literal(value)}")
    return "\n".join(lines)


def inject_parameters(nb: dict[str, Any], params: dict[str, Any]) -> dict[str, Any]:
    """Return a deep copy of ``nb`` with an ``injected-parameters`` cell after the ``parameters`` cell (else at the top).
    Every cell carries ``metadata.olcf_index`` (original index or ``None`` for the injected cell)."""
    out = copy.deepcopy(nb)
    for i, c in enumerate(out["cells"]):
        c["metadata"]["olcf_index"] = i
    if not params:
        return out
    cell = _new_cell("code", parameter_source(params), {"tags": ["injected-parameters"], "olcf_index": None})
    idx = next((i for i, c in enumerate(out["cells"]) if "parameters" in (c["metadata"].get("tags") or [])), None)
    out["cells"].insert(0 if idx is None else idx + 1, cell)
    return out


# --------------------------------------------------------------------------------------------- execution
class CellTimeout(Exception):
    """A cell ran longer than its timeout."""
    olcf_category = "timeout"


class KernelDied(Exception):
    """The kernel process died while running a cell, often through lack of memory."""
    olcf_category = "resource"
    olcf_remediation = "The kernel process died (often out of memory): increase driver memory or reduce the data pulled into the driver."


@dataclass
class ExecResult:
    """Result of executing a notebook: final state, exit value, whether ``exit`` was called and failure details."""
    state: str = "succeeded"
    exit_value: Any = None
    exit_called: bool = False
    failure: dict[str, Any] | None = None
    notebook: dict[str, Any] = field(default_factory=dict)
    cells_executed: int = 0


class NotebookExecutor:
    """Execute a notebook cell by cell in a fresh kernel. One instance = one run."""

    def __init__(self, spec: dict[str, Any], workdir: str | Path, kernel_spec_path: str | Path, *, max_output_bytes: int = 50 * 1024 * 1024,
                 cell_timeout: float | None = None, startup_timeout: float = 180.0, python: str | None = None) -> None:
        self.spec = spec
        self.workdir = Path(workdir)
        self.kernel_spec_path = str(kernel_spec_path)
        self.budget = OutputBudget(max_output_bytes)
        self.cell_timeout = cell_timeout
        self.startup_timeout = startup_timeout
        self.python = python or sys.executable
        self._km: Any = None
        self._kc: Any = None
        self._stop = threading.Event()
        self._noted_trunc = False

    # ---- lifecycle
    def _start_kernel(self) -> None:
        from jupyter_client import KernelManager
        from jupyter_client.kernelspec import KernelSpec

        km = KernelManager()
        km._kernel_spec = KernelSpec(  # noqa: SLF001 - supported way to bypass spec discovery
            argv=[self.python, "-m", "olcf.runtime_lib.kernel_host", "-f", "{connection_file}", "--spec", self.kernel_spec_path],
            display_name="OLCF PySpark", language="python", env={})
        env = dict(os.environ)
        # figures are captured as image/png outputs of the executed notebook (the inline backend ships with ipykernel); without it nothing could be saved, so "Agg"
        backend = "module://matplotlib_inline.backend_inline" if importlib.util.find_spec("matplotlib_inline") else "Agg"
        env.update({"OLCF_JOB_DIR": str(self.workdir), "MPLBACKEND": backend, "PYTHONUNBUFFERED": "1"})
        km.start_kernel(cwd=str(self.workdir), env=env)
        kc = km.client()
        kc.start_channels()
        try:
            kc.wait_for_ready(timeout=self.startup_timeout)
        except RuntimeError as exc:
            self._shutdown(km, kc)
            raise KernelDied(f"kernel did not become ready (waited up to {self.startup_timeout:g}s): {exc}") from exc
        self._km, self._kc = km, kc

    @staticmethod
    def _shutdown(km: Any, kc: Any) -> None:
        try:
            if kc is not None:
                kc.stop_channels()
        except Exception:  # noqa: BLE001, S110
            pass
        try:
            if km is not None:
                km.shutdown_kernel(now=True)
        except Exception:  # noqa: BLE001, S110
            pass

    def stop(self) -> None:
        """Cancel: interrupt the running cell and shut the kernel down (safe to call from a signal handler thread)."""
        self._stop.set()
        km = self._km
        if km is not None:
            try:
                km.interrupt_kernel()
            except Exception:  # noqa: BLE001, S110
                pass

    # ---- main entry
    def run(self, nb: dict[str, Any], parameters: dict[str, Any] | None = None) -> ExecResult:
        prepared = inject_parameters(nb, parameters or {})
        result = ExecResult(notebook=prepared)
        try:
            self._start_kernel()
        except Exception as exc:  # noqa: BLE001
            result.state = "failed"
            result.failure = failures.failure_from_exception(exc)
            return result
        try:
            for cell in prepared["cells"]:
                if cell["cell_type"] != "code":
                    continue
                if self._stop.is_set():
                    result.state, result.failure = "failed", failures.failure_dict("cancellation", "execution cancelled")
                    break
                code = cell["source"]
                orig = cell["metadata"].get("olcf_index")
                if not code.strip():
                    continue
                emit_event("cell_started", cell=orig)
                try:
                    outputs, reply = self._execute_cell(code)
                except CellTimeout as exc:
                    cell["outputs"] = []
                    result.state = "failed"
                    result.failure = failures.failure_dict("timeout", str(exc), cell=orig, remediation="Raise the cell/job timeout or optimise the cell.")
                    break
                except Exception as exc:  # noqa: BLE001 - kernel died, protocol problem...
                    result.state = "failed"
                    result.failure = failures.failure_from_exception(exc, cell=orig)
                    break
                cell["outputs"] = outputs
                cell["execution_count"] = reply.get("execution_count")
                result.cells_executed += 1
                exit_out = next((o for o in outputs if o.get("output_type") == "display_data" and EXIT_MIME in (o.get("data") or {})), None)
                if exit_out is not None:
                    result.exit_called = True
                    result.exit_value = exit_out["data"][EXIT_MIME].get("value")
                    break  # notebookutils.notebook.exit ends the run successfully, even though the cell reports an error
                if reply.get("status") != "ok":
                    result.state = "failed"
                    err = next((o for o in outputs if o.get("output_type") == "error"), None)
                    ename = reply.get("ename") or (err or {}).get("ename") or "Error"
                    evalue = reply.get("evalue") or (err or {}).get("evalue") or ""
                    tb = reply.get("traceback") or (err or {}).get("traceback") or []
                    cat, remediation = failures.classify(ename, evalue, "\n".join(tb))
                    if getattr(sys.modules.get("olcf.runtime_lib.magics"), "DependencyBlocked", None) and ename == "DependencyBlocked":
                        cat = "dependency"
                    result.failure = failures.failure_dict(cat, f"{ename}: {evalue}", cell=orig, line=failures.extract_line(tb),
                                                           traceback="\n".join(tb), remediation=remediation)
                    break
        finally:
            self._shutdown(self._km, self._kc)
            self._km = self._kc = None
        return result

    # ---- one cell
    def _execute_cell(self, code: str) -> tuple[list[dict[str, Any]], dict[str, Any]]:
        kc, km = self._kc, self._km
        msg_id = kc.execute(code, store_history=True, allow_stdin=False, stop_on_error=False)
        deadline = (time.monotonic() + self.cell_timeout) if self.cell_timeout else None
        outputs: list[dict[str, Any]] = []
        display_index: dict[str, int] = {}
        while True:
            if self._stop.is_set():
                raise KeyboardInterrupt("execution cancelled")
            wait = 1.0
            if deadline is not None:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    self._interrupt_and_drain(msg_id)
                    raise CellTimeout(f"cell exceeded the per-cell timeout of {self.cell_timeout}s")
                wait = min(wait, remaining)
            try:
                msg = kc.get_iopub_msg(timeout=wait)
            except queue.Empty:
                if not km.is_alive():
                    raise KernelDied("kernel process exited unexpectedly") from None
                continue
            if msg["parent_header"].get("msg_id") != msg_id:
                continue
            mtype, content = msg["msg_type"], msg["content"]
            if mtype == "status" and content.get("execution_state") == "idle":
                break
            self._handle_iopub(mtype, content, outputs, display_index)
        reply = self._get_reply(msg_id)
        return outputs, reply

    def _get_reply(self, msg_id: str) -> dict[str, Any]:
        deadline = time.monotonic() + 30
        while time.monotonic() < deadline:
            try:
                msg = self._kc.get_shell_msg(timeout=1.0)
            except queue.Empty:
                continue
            if msg["parent_header"].get("msg_id") == msg_id:
                return msg["content"]
        raise KernelDied("no execute_reply from kernel")

    def _interrupt_and_drain(self, msg_id: str) -> None:
        try:
            self._km.interrupt_kernel()
        except Exception:  # noqa: BLE001, S110
            pass
        end = time.monotonic() + 15
        while time.monotonic() < end:
            try:
                m = self._kc.get_iopub_msg(timeout=0.5)
            except queue.Empty:
                continue
            if m["parent_header"].get("msg_id") == msg_id and m["msg_type"] == "status" and m["content"].get("execution_state") == "idle":
                return
        # kernel did not come back: it will be shut down by run()

    def _handle_iopub(self, mtype: str, content: dict[str, Any], outputs: list[dict[str, Any]], display_index: dict[str, int]) -> None:
        if mtype == "stream":
            text = content.get("text", "")
            raw = text.encode("utf-8", "replace")
            allowed = self.budget.take(len(raw))
            if allowed < len(raw):
                text = raw[:allowed].decode("utf-8", "ignore")
                self._note_truncation(outputs)
            if not text:
                return
            if outputs and outputs[-1].get("output_type") == "stream" and outputs[-1].get("name") == content.get("name"):
                outputs[-1]["text"] += text
            else:
                outputs.append({"output_type": "stream", "name": content.get("name", "stdout"), "text": text})
        elif mtype in ("display_data", "execute_result", "update_display_data"):
            data = content.get("data", {})
            size = len(json.dumps(data, default=str))
            is_exit = EXIT_MIME in data
            if not is_exit and self.budget.take(size) < size:
                self._note_truncation(outputs)
                return
            out: dict[str, Any] = {"output_type": "display_data" if mtype != "execute_result" else "execute_result", "data": data,
                                   "metadata": content.get("metadata", {})}
            if mtype == "execute_result":
                out["execution_count"] = content.get("execution_count")
            did = (content.get("transient") or {}).get("display_id")
            if mtype == "update_display_data" and did in display_index:
                outputs[display_index[did]] = out
            else:
                if did:
                    display_index[did] = len(outputs)
                outputs.append(out)
        elif mtype == "error":
            tb = [t if isinstance(t, str) else str(t) for t in content.get("traceback", [])]
            outputs.append({"output_type": "error", "ename": content.get("ename", ""), "evalue": content.get("evalue", ""), "traceback": tb})
        elif mtype == "clear_output":
            outputs.clear()
            display_index.clear()

    def _note_truncation(self, outputs: list[dict[str, Any]]) -> None:
        if not self._noted_trunc:
            self._noted_trunc = True
            outputs.append({"output_type": "stream", "name": "stderr",
                            "text": f"\n[... olcf: further output omitted (limits.max_output_bytes={self.budget.max_bytes}) ...]\n"})


def write_notebook(nb: dict[str, Any], path: str | Path) -> None:
    """Write nbformat JSON (validated by ``nbformat`` when installed; the olcf_index helper metadata is stripped)."""
    out = copy.deepcopy(nb)
    for c in out["cells"]:
        c.get("metadata", {}).pop("olcf_index", None)
    try:
        import nbformat
    except ImportError:
        Path(path).write_text(json.dumps(out, indent=1), encoding="utf-8")
        return
    node = nbformat.from_dict(out)
    try:
        nbformat.validate(node)
    except nbformat.ValidationError as exc:
        # a notebook that does not validate is still useful as a record of what ran: keep it, say so
        emit_event("output_notebook_invalid", detail=str(exc)[:300])
    nbformat.write(node, str(path), validate=False) if _accepts_validate() else Path(path).write_text(json.dumps(out, indent=1), encoding="utf-8")


def _accepts_validate() -> bool:
    import inspect

    import nbformat

    return "validate" in inspect.signature(nbformat.write).parameters
