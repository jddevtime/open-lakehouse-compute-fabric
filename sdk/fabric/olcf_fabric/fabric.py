# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Everything that touches the Fabric notebook session: ``notebookutils`` access, the runtime context, notebook acquisition and exit.

Microsoft's ``notebookutils`` surface is only available inside a Fabric session, and some details (the exact ``notebook.getDefinition`` signature and the values of its ``format``
parameter, which audiences ``credentials.getToken`` accepts) are not fully specified in Microsoft's public documentation. Every call here is therefore *defensive*: it tries the documented spellings in
order, accepts the plausible return shapes and degrades to *pull mode* (only identifiers travel; the control plane fetches the definition itself) when nothing works. The
assumptions are listed in docs/guides/fabric-assumptions.md.
"""

from __future__ import annotations

import ast
import base64
import json
import re
from typing import Any

from ._canonical import sha256_hex

CONTEXT_KEYS = (
    "currentNotebookName", "currentNotebookId", "currentWorkspaceName", "currentWorkspaceId", "defaultLakehouseName", "defaultLakehouseId",
    "defaultLakehouseWorkspaceName", "defaultLakehouseWorkspaceId", "currentRunId", "parentRunId", "rootRunId", "isForPipeline", "isReferenceRun",
    "referenceTreePath", "userName", "userId", "productType", "activityId",
)
_SENSITIVE = re.compile(r"(?i)token|secret|password|passwd|credential|apikey|api_key|authorization|bearer")
SUBMIT_CELL_MARKERS = ("olcf_fabric",)
_SUBMIT_CALL = re.compile(r"\b(?:run_remote|extend)\s*\(")
_GET_SECRET = re.compile(r"""get[Ss]ecret\(\s*['"]([^'"]+)['"]\s*,\s*['"]([^'"]+)['"]""")


def notebookutils_module() -> Any | None:
    """The ``notebookutils`` object of this session (``mssparkutils`` as a fallback), or ``None`` outside Fabric."""
    import builtins
    import sys

    for name in ("notebookutils", "mssparkutils"):
        mod = sys.modules.get(name) or getattr(builtins, name, None)
        if mod is not None:
            return mod
        try:
            return __import__(name)
        except ImportError:
            continue
    return None


def _as_dict(obj: Any) -> dict[str, Any]:
    if obj is None:
        return {}
    if isinstance(obj, dict):
        return dict(obj)
    for attr in ("to_dict", "asDict", "_asdict"):
        fn = getattr(obj, attr, None)
        if callable(fn):
            try:
                return dict(fn())
            except Exception:  # noqa: BLE001
                pass
    try:
        return dict(obj)
    except Exception:  # noqa: BLE001
        pass
    d = getattr(obj, "__dict__", None)
    return {k: v for k, v in d.items() if not k.startswith("_")} if isinstance(d, dict) else {}


def runtime_context() -> dict[str, Any]:
    """``notebookutils.runtime.context`` as a plain dict (empty outside Fabric)."""
    nbu = notebookutils_module()
    runtime = getattr(nbu, "runtime", None) if nbu is not None else None
    return _as_dict(getattr(runtime, "context", None)) if runtime is not None else {}


def normalise_context(raw: dict[str, Any]) -> dict[str, Any]:
    return {k: raw.get(k) for k in CONTEXT_KEYS if isinstance(raw.get(k), (str, int, bool)) or raw.get(k) is None}


def raw_context(raw: dict[str, Any]) -> dict[str, Any]:
    """The context as the session returned it, minus anything credential-like and anything that is not a plain scalar/list/dict of scalars."""
    out: dict[str, Any] = {}
    for k, v in raw.items():
        if _SENSITIVE.search(str(k)):
            continue
        try:
            json.dumps(v)
        except (TypeError, ValueError):
            v = str(v)
        out[str(k)] = v
    return out


# ------------------------------------------------------------------------------------------------- definition acquisition
def _text_of(value: Any) -> str | None:
    """Normalise the plausible return shapes of ``getDefinition`` to notebook text."""
    if value is None:
        return None
    if isinstance(value, bytes):
        return value.decode("utf-8", "replace")
    if isinstance(value, str):
        return value
    if isinstance(value, dict):
        parts = (value.get("definition") or {}).get("parts") if isinstance(value.get("definition"), dict) else value.get("parts")
        if isinstance(parts, list):
            for p in parts:
                if str(p.get("path", "")).startswith("notebook-content"):
                    payload = p.get("payload", "")
                    return base64.b64decode(payload).decode("utf-8") if p.get("payloadType", "InlineBase64") == "InlineBase64" else str(payload)
        if "cells" in value and "nbformat" in value:
            return json.dumps(value)
        return None
    if isinstance(value, (list, tuple)):
        return None
    return str(value)


def detect_format(text: str) -> str:
    s = text.lstrip()
    if s.startswith("{"):
        try:
            doc = json.loads(text)
            if isinstance(doc, dict) and "cells" in doc:
                return "ipynb"
        except ValueError:
            pass
    if "# Fabric notebook source" in text[:400] or "# CELL ********************" in text:
        return "fabric_git_source"
    return "py"


def acquire_definition(name: str, workspace_id: str | None, formats: list[str | None]) -> tuple[str, str, str | None] | None:
    """Try ``notebookutils.notebook.getDefinition`` with the documented spellings. Returns ``(format, text, how)`` or ``None`` (then the caller uses pull mode)."""
    nbu = notebookutils_module()
    nb = getattr(nbu, "notebook", None) if nbu is not None else None
    fn = getattr(nb, "getDefinition", None) if nb is not None else None
    if not callable(fn):
        return None
    for fmt in formats:
        attempts: list[tuple[tuple[Any, ...], dict[str, Any]]] = []
        if fmt:
            attempts += [((name,), {"workspaceId": workspace_id or "", "format": fmt}), ((name, workspace_id or "", fmt), {}), ((name,), {"format": fmt})]
        else:
            attempts += [((name,), {"workspaceId": workspace_id or ""}), ((name,), {})]
        for args, kwargs in attempts:
            try:
                text = _text_of(fn(*args, **kwargs))
            except TypeError:
                continue  # signature mismatch: try the next spelling
            except Exception:  # noqa: BLE001 - the call exists but failed (permissions, not found): next format
                break
            if text and text.strip():
                return detect_format(text), text, f"getDefinition(format={fmt!r})"
    return None


# ------------------------------------------------------------------------------------------------- light notebook handling
_SECTION_RE = re.compile(r"^# (CELL|PARAMETERS CELL|INJECTED PARAMETERS|MARKDOWN|METADATA) \*{20}[ \t]*$", re.M)


def _git_groups(text: str) -> tuple[str, list[dict[str, Any]]]:
    """Split Fabric Git source into ``(header, groups)``. A group is one cell: its section text plus the METADATA section that follows it."""
    marks = list(_SECTION_RE.finditer(text))
    if not marks:
        return text, []
    header = text[: marks[0].start()]
    groups: list[dict[str, Any]] = []
    for i, m in enumerate(marks):
        end = marks[i + 1].start() if i + 1 < len(marks) else len(text)
        chunk = text[m.start():end]
        if m.group(1) == "METADATA" and not groups:
            header += chunk  # notebook-level metadata belongs to the header
        elif m.group(1) == "METADATA":
            groups[-1]["chunks"].append(chunk)
            groups[-1]["meta"] += chunk
        else:
            groups.append({"kind": m.group(1), "chunks": [chunk], "body": chunk.split("\n", 1)[1] if "\n" in chunk else "", "meta": ""})
    return header, groups


def _is_submit(src: str) -> bool:
    return any(m in src for m in SUBMIT_CELL_MARKERS) and bool(_SUBMIT_CALL.search(src))


def drop_submit_cells(text: str, fmt: str) -> tuple[str, list[int]]:
    """Remove the cell(s) that call ``olcf.run_remote()`` / ``olcf.extend()``; returns ``(text, removed_indexes)``."""
    removed: list[int] = []
    if fmt == "ipynb":
        try:
            doc = json.loads(text)
        except ValueError:
            return text, removed
        keep = []
        for i, c in enumerate(doc.get("cells", [])):
            src = c.get("source", "")
            src = "".join(src) if isinstance(src, list) else str(src)
            if c.get("cell_type") == "code" and _is_submit(src):
                removed.append(i)
            else:
                keep.append(c)
        if removed:
            doc["cells"] = keep
            return json.dumps(doc, ensure_ascii=False, indent=1), removed
        return text, removed
    if fmt == "fabric_git_source":
        header, groups = _git_groups(text)
        kept: list[str] = []
        for i, g in enumerate(groups):
            if g["kind"] != "MARKDOWN" and _is_submit(g["body"]):
                removed.append(i)
            else:
                kept.extend(g["chunks"])
        return (header + "".join(kept) if removed else text), removed
    pieces = re.split(r"(?m)^(?=# %%)", text)  # percent-format .py
    keep_p = []
    for i, p in enumerate(pieces):
        if i and _is_submit(p):
            removed.append(i - 1)
        else:
            keep_p.append(p)
    return ("".join(keep_p) if removed else text), removed


def parameter_cells(text: str, fmt: str) -> list[str]:
    """Source of the cells Fabric treats as the parameter cell (tag ``parameters`` / ``# PARAMETERS CELL`` section)."""
    out: list[str] = []
    if fmt == "ipynb":
        try:
            doc = json.loads(text)
        except ValueError:
            return out
        for c in doc.get("cells", []):
            if "parameters" in ((c.get("metadata") or {}).get("tags") or []):
                src = c.get("source", "")
                out.append("".join(src) if isinstance(src, list) else str(src))
    elif fmt == "fabric_git_source":
        _header, groups = _git_groups(text)
        for g in groups:
            if g["kind"] == "PARAMETERS CELL" or (g["kind"] == "CELL" and '"parameters"' in g["meta"]):
                out.append(g["body"])
    return out


def declared_parameter_names(text: str, fmt: str) -> list[str]:
    """Names assigned at the top level of the parameter cell(s): the *declared* parameters. Nothing else is ever read from the interpreter."""
    names: list[str] = []
    for src in parameter_cells(text, fmt):
        body = "\n".join(line for line in src.split("\n") if not line.lstrip().startswith(("%", "!", "# META", "# METADATA")))
        try:
            tree = ast.parse(body)
        except SyntaxError:
            continue
        for node in tree.body:
            targets: list[ast.expr] = []
            if isinstance(node, ast.Assign):
                targets = list(node.targets)
            elif isinstance(node, ast.AnnAssign) and node.value is not None:
                targets = [node.target]
            for t in targets:
                if isinstance(t, ast.Name) and t.id not in names:
                    names.append(t.id)
    return names


def secret_refs_in(text: str) -> list[dict[str, str]]:
    return [{"vault": v, "name": n} for v, n in sorted(set(_GET_SECRET.findall(text)))]


def json_safe(value: Any) -> tuple[bool, Any]:
    try:
        json.dumps(value, allow_nan=False)
        return True, value
    except (TypeError, ValueError):
        return False, None


def exit_notebook(value: Any) -> None:
    """``notebookutils.notebook.exit(json)``: hands the value to the calling pipeline/notebook. Raises whatever ``exit`` raises to stop the cell stack."""
    nbu = notebookutils_module()
    nb = getattr(nbu, "notebook", None) if nbu is not None else None
    fn = getattr(nb, "exit", None) if nb is not None else None
    if not callable(fn):
        return
    fn(value if isinstance(value, str) else json.dumps(value, default=str))


def content_digest(text: str) -> str:
    return sha256_hex(text)
