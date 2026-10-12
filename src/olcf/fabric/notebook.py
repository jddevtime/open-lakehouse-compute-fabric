# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Notebook formats used by Microsoft Fabric: parse, normalise and serialise (round-trip safe).

Why this module exists: the compatibility analyzer, the adapter and the tests must agree on *one* in-memory notebook model
regardless of how the notebook arrived:

``ipynb``              nbformat 4 JSON as returned by ``getDefinition?format=ipynb`` (Fabric stores cell language in
                       ``metadata.microsoft.language`` and uses the ``parameters`` cell tag);
``fabric_git_source``  the text format of ``notebook-content.py`` / ``.sql`` / ``.scala`` / ``.r``::

                           # Fabric notebook source

                           # METADATA ********************

                           # META {
                           # META   "kernel_info": {"name": "synapse_pyspark"}
                           # META }

                           # CELL ********************

                           df = spark.range(3)

                           # METADATA ********************

                           # META {
                           # META   "language": "python"
                           # META }

                           # MARKDOWN ********************

                           # # Title

                           # PARAMETERS CELL ********************

                           # MAGIC ... (non-python cells are prefixed with ``# MAGIC `` in Fabric exports)

``py``                 a plain script; ``# %%`` percent markers split cells, otherwise one cell.

The model is deliberately small (:class:`Cell` / :class:`Notebook`). Cell numbering is the **position in the cell list including
markdown cells** because that is exactly how the NODE executor (``olcf.runtime_lib.executor``) numbers cells in failure records,
so the analyzer's ``cell`` field and the runner's ``failure.cell`` always point at the same cell.

Line numbers are 1-based and relative to the cell's code; :attr:`Cell.file_line` is the 1-based line of the first code line in the
original text file (git source / py) so editors can jump to it.
"""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass, field
from typing import Any

from olcf.core.errors import BadRequest

GIT_HEADER = "Fabric notebook source"
_SECTION = re.compile(r"^(?P<p>#|--|//)\s*(?P<kind>METADATA|CELL|MARKDOWN|PARAMETERS CELL|INJECTED PARAMETERS)\s*\*{10,}\s*$")
_HEADER = re.compile(r"^(?P<p>#|--|//)\s*Fabric notebook source\s*$")
_PERCENT = re.compile(r"^#\s*%%(?P<rest>.*)$")
FORMATS = ("ipynb", "fabric_git_source", "py")

#: Fabric language names (cell metadata) -> canonical names used by the analyzer.
LANGUAGE_ALIASES = {
    "python": "python", "pyspark": "python", "synapse_pyspark": "python", "sparksql": "sql", "sql": "sql", "spark": "scala", "scala": "scala",
    "sparkr": "r", "r": "r", "csharp": "csharp", "dotnet": "csharp", "markdown": "markdown",
}


@dataclass
class Cell:
    """One notebook cell. ``index`` is assigned by :meth:`Notebook.renumber` (position in the list)."""

    cell_type: str  # code | markdown | raw
    source: str
    index: int = 0
    metadata: dict[str, Any] = field(default_factory=dict)
    is_parameters: bool = False
    #: language declared by Fabric metadata (canonical: python | sql | scala | r | csharp); "python" when unknown
    language: str = "python"
    #: the git source prefixed every line with ``# MAGIC `` (round-trip flag; never part of ``source``)
    magic_prefixed: bool = False
    #: 1-based line in the original text file where ``source`` begins (git source / py), or None (ipynb)
    file_line: int | None = None
    #: provenance after deterministic rewrites that restructure cells: ``{"notebook": name, "cell": i}``
    origin: dict[str, Any] | None = None
    outputs: list[Any] = field(default_factory=list)

    @property
    def lines(self) -> list[str]:
        return self.source.split("\n") if self.source else []


@dataclass
class Notebook:
    """A parsed notebook in the Fabric Git source format or ipynb: its format, cells, metadata and the comment prefix of the source language.
    """
    format: str
    cells: list[Cell] = field(default_factory=list)
    metadata: dict[str, Any] = field(default_factory=dict)
    #: comment prefix of the git source file (``#`` for python, ``--`` for sql, ``//`` for scala)
    prefix: str = "#"
    nbformat_minor: int = 5

    def renumber(self) -> Notebook:
        for i, c in enumerate(self.cells):
            c.index = i
        return self

    @property
    def code_cells(self) -> list[Cell]:
        return [c for c in self.cells if c.cell_type == "code"]

    def parameters_cell(self) -> Cell | None:
        return next((c for c in self.cells if c.is_parameters), None)

    def default_lakehouse_hint(self) -> dict[str, Any] | None:
        """Default lakehouse recorded in notebook metadata (``dependencies.lakehouse``), if any."""
        dep = (self.metadata.get("dependencies") or {}).get("lakehouse") or {}
        if not isinstance(dep, dict) or not (dep.get("default_lakehouse") or dep.get("default_lakehouse_name")):
            return None
        return {"id": dep.get("default_lakehouse"), "name": dep.get("default_lakehouse_name"), "workspace_id": dep.get("default_lakehouse_workspace_id"),
                "known": [k for k in dep.get("known_lakehouses") or [] if isinstance(k, dict)]}


# --------------------------------------------------------------------------------------------- detection
def detect_format(text: str, hint: str | None = None) -> str:
    """Sniff the notebook format of ``text`` (``hint`` wins when it is a known format)."""
    h = (hint or "").lower().replace("fabricgitsource", "fabric_git_source")
    if h in FORMATS:
        return h
    head = text.lstrip()[:2000]
    first = head.splitlines()[0].strip() if head else ""
    if _HEADER.match(first):
        return "fabric_git_source"
    if head.startswith("{"):
        return "ipynb"
    return "py"


def _join(src: Any) -> str:
    return "".join(src) if isinstance(src, list) else str(src or "")


def canonical_language(value: Any) -> str:
    return LANGUAGE_ALIASES.get(str(value or "python").lower(), str(value).lower())


# --------------------------------------------------------------------------------------------- ipynb
def parse_ipynb(text: str) -> Notebook:
    try:
        doc = json.loads(text)
    except ValueError as exc:
        raise BadRequest(f"the notebook is not valid JSON: {exc}", remediation="Provide an .ipynb (nbformat 4) document.") from exc
    if not isinstance(doc, dict) or not isinstance(doc.get("cells"), list):
        raise BadRequest("not an nbformat notebook (no 'cells' list)", remediation="Provide an .ipynb (nbformat 4) document.")
    if int(doc.get("nbformat", 4)) < 4:
        raise BadRequest(f"nbformat {doc.get('nbformat')} is not supported", remediation="Upgrade the notebook to nbformat 4.")
    nb = Notebook(format="ipynb", metadata=dict(doc.get("metadata") or {}), nbformat_minor=int(doc.get("nbformat_minor", 5)))
    for raw in doc["cells"]:
        if not isinstance(raw, dict):
            raise BadRequest("malformed notebook: a cell is not an object")
        meta = dict(raw.get("metadata") or {})
        tags = meta.get("tags") or []
        lang = (meta.get("microsoft") or {}).get("language") if isinstance(meta.get("microsoft"), dict) else None
        nb.cells.append(Cell(cell_type=str(raw.get("cell_type", "code")), source=_join(raw.get("source")), metadata=meta,
                             is_parameters="parameters" in tags if isinstance(tags, list) else False,
                             language=canonical_language(lang) if lang else "python", outputs=list(raw.get("outputs") or [])))
    return nb.renumber()


def _cell_id(i: int, source: str) -> str:
    return f"c{i:03d}-{hashlib.sha256(source.encode('utf-8', 'replace')).hexdigest()[:8]}"


def to_ipynb(nb: Notebook, *, indent: int | None = 1) -> str:
    """nbformat 4.5 JSON for the runner. Spark SQL cells get the ``%%sql`` magic (the runner executes magics, not metadata)."""
    cells: list[dict[str, Any]] = []
    for c in nb.cells:
        meta = {k: v for k, v in c.metadata.items() if k != "olcf_origin"}
        if c.is_parameters:
            tags = list(meta.get("tags") or [])
            if "parameters" not in tags:
                tags.append("parameters")
            meta["tags"] = tags
        if c.origin:
            meta["olcf_origin"] = c.origin
        src = c.source
        if c.cell_type == "code" and c.language == "sql" and not re.match(r"\s*%%(sql|configure|pyspark|sh|html|markdown)", src):
            src = "%%sql\n" + src
        item: dict[str, Any] = {"cell_type": c.cell_type if c.cell_type in ("code", "markdown", "raw") else "code", "id": _cell_id(c.index, src),
                                "metadata": meta, "source": src}
        if item["cell_type"] == "code":
            item["outputs"] = []
            item["execution_count"] = None
        cells.append(item)
    meta = dict(nb.metadata)
    meta.setdefault("kernelspec", {"name": "python3", "display_name": "Python 3", "language": "python"})
    return json.dumps({"nbformat": 4, "nbformat_minor": max(5, nb.nbformat_minor), "metadata": meta, "cells": cells}, indent=indent, sort_keys=True)


# --------------------------------------------------------------------------------------------- git source
def _strip_magic(lines: list[str], prefix: str) -> tuple[list[str], bool]:
    """Remove the ``# MAGIC `` prefix Fabric adds to non-python cell bodies; returns (lines, was_prefixed)."""
    tag = f"{prefix} MAGIC"
    nonblank = [ln for ln in lines if ln.strip()]
    if not nonblank or not all(ln.startswith(tag) for ln in nonblank):
        return lines, False
    out = []
    for ln in lines:
        if ln.startswith(tag + " "):
            out.append(ln[len(tag) + 1:])
        elif ln.startswith(tag):
            out.append(ln[len(tag):])
        else:
            out.append(ln)
    return out, True


def _trim_blank(lines: list[str]) -> tuple[list[str], int]:
    """Drop blank lines around a cell body; returns (lines, number of leading lines dropped)."""
    lead = 0
    while lines and not lines[0].strip():
        lines = lines[1:]
        lead += 1
    while lines and not lines[-1].strip():
        lines = lines[:-1]
    return lines, lead


def _meta_json(body: list[str], prefix: str) -> dict[str, Any]:
    tag = f"{prefix} META"
    parts = []
    for ln in body:
        s = ln.rstrip()
        if s.startswith(tag):
            parts.append(s[len(tag):][1:] if s[len(tag):].startswith(" ") else s[len(tag):])
    js = "\n".join(parts).strip()
    if not js:
        return {}
    try:
        val = json.loads(js)
    except ValueError as exc:
        raise BadRequest(f"malformed '{tag}' metadata block: {exc}", remediation="Re-export the notebook definition from Fabric.") from exc
    return val if isinstance(val, dict) else {}


def parse_git_source(text: str) -> Notebook:
    """Parse ``notebook-content.py`` (or .sql/.scala/.r) into the notebook model."""
    lines = text.replace("\r\n", "\n").split("\n")
    first = next((i for i, ln in enumerate(lines) if ln.strip()), None)
    m = _HEADER.match(lines[first].strip()) if first is not None else None
    if m is None:
        raise BadRequest("not a Fabric Git notebook source (missing the '# Fabric notebook source' header)",
                         remediation="Pass the content of notebook-content.py exactly as Fabric returns it.")
    prefix = m.group("p")
    sections: list[tuple[str, list[str], int]] = []  # (kind, body lines, 1-based line of first body line)
    for n, line in enumerate(lines[first + 1:], start=first + 2):
        sm = _SECTION.match(line.strip())
        if sm and sm.group("p") == prefix:
            sections.append((sm.group("kind"), [], n + 1))
        elif sections:
            sections[-1][1].append(line)
    nb = Notebook(format="fabric_git_source", prefix=prefix)
    for kind, body, start in sections:
        if kind == "METADATA":
            md = _meta_json(body, prefix)
            if nb.cells:
                cell = nb.cells[-1]
                cell.metadata["fabric"] = md
                if md.get("language"):
                    cell.language = canonical_language(md["language"])
            else:
                nb.metadata.update(md)
            continue
        if kind == "MARKDOWN":
            trimmed, lead = _trim_blank(body)
            md_lines = [re.sub(rf"^{re.escape(prefix)} ?", "", ln, count=1) for ln in trimmed]
            nb.cells.append(Cell(cell_type="markdown", source="\n".join(md_lines), file_line=start + lead, language="markdown"))
            continue
        trimmed, lead = _trim_blank(body)
        trimmed, magic = _strip_magic(trimmed, prefix)
        nb.cells.append(Cell(cell_type="code", source="\n".join(trimmed), is_parameters=kind == "PARAMETERS CELL", magic_prefixed=magic,
                             file_line=start + lead, metadata={"tags": ["injected-parameters"]} if kind == "INJECTED PARAMETERS" else {},
                             language="python" if prefix == "#" else ("sql" if prefix == "--" else "scala")))
    return nb.renumber()


def _emit_meta(doc: dict[str, Any], prefix: str) -> list[str]:
    body = json.dumps(doc, indent=2, sort_keys=False).split("\n")
    return [f"{prefix} META {ln}" for ln in body]


def to_git_source(nb: Notebook) -> str:
    """Serialise to the Fabric Git text format. ``parse_git_source(to_git_source(nb))`` reproduces the notebook (tested)."""
    p = nb.prefix or "#"
    out: list[str] = [f"{p} {GIT_HEADER}", ""]
    if nb.metadata:
        out += [f"{p} METADATA ********************", ""] + _emit_meta(nb.metadata, p) + [""]
    for c in nb.cells:
        if c.cell_type == "markdown":
            out += [f"{p} MARKDOWN ********************", ""]
            out += [(f"{p} {ln}" if ln else p) for ln in c.source.split("\n")] + [""]
            continue
        head = "PARAMETERS CELL" if c.is_parameters else "CELL"
        body = c.source.split("\n") if c.source else [""]
        if c.magic_prefixed:
            body = [f"{p} MAGIC {ln}" if ln else f"{p} MAGIC" for ln in body]
        out += [f"{p} {head} ********************", ""] + body + [""]
        fab = c.metadata.get("fabric")
        if fab:
            out += [f"{p} METADATA ********************", ""] + _emit_meta(fab, p) + [""]
    return "\n".join(out).rstrip("\n") + "\n"


# --------------------------------------------------------------------------------------------- plain python
def parse_py(text: str) -> Notebook:
    """Plain script; ``# %%`` splits cells (``[markdown]`` and ``tags=[...]`` supported), otherwise one code cell."""
    text = text.replace("\r\n", "\n")
    nb = Notebook(format="py")
    cur: list[str] = []
    cur_start = 1
    ctype, tags, started = "code", [], False

    def flush() -> None:
        body, lead = _trim_blank(cur[:])
        if started or body:
            src = "\n".join(body)
            if ctype == "markdown":
                src = "\n".join(re.sub(r"^# ?", "", ln) for ln in body)
            nb.cells.append(Cell(cell_type=ctype, source=src, file_line=cur_start + lead, is_parameters="parameters" in tags,
                                 metadata={"tags": list(tags)} if tags else {}, language="markdown" if ctype == "markdown" else "python"))

    for n, line in enumerate(text.split("\n"), start=1):
        m = _PERCENT.match(line)
        if m:
            if started or cur:
                flush()
            cur, cur_start, started = [], n + 1, True
            rest = m.group("rest")
            ctype = "markdown" if re.search(r"\[(markdown|md)\]", rest) else "code"
            tm = re.search(r"tags=(\[[^\]]*\])", rest)
            try:
                tags = json.loads(tm.group(1).replace("'", '"')) if tm else []
            except ValueError:
                tags = []
        else:
            cur.append(line)
    if started or cur:
        flush()
    return nb.renumber()


def to_py(nb: Notebook) -> str:
    out: list[str] = []
    for c in nb.cells:
        tags = ", tags=" + json.dumps(["parameters"]) if c.is_parameters else ""
        if c.cell_type == "markdown":
            out.append("# %% [markdown]\n" + "\n".join(f"# {ln}" for ln in c.source.split("\n")))
        else:
            out.append(f"# %%{tags}\n{c.source}")
    return "\n\n".join(out) + "\n"


# --------------------------------------------------------------------------------------------- facade
def parse(text: str, fmt: str | None = None) -> Notebook:
    """Parse ``text`` as ``fmt`` (``ipynb`` | ``fabric_git_source`` | ``py``; sniffed when omitted)."""
    fmt = detect_format(text, fmt)
    if fmt == "ipynb":
        return parse_ipynb(text)
    if fmt == "fabric_git_source":
        return parse_git_source(text)
    return parse_py(text)


def serialise(nb: Notebook, fmt: str | None = None) -> str:
    fmt = fmt or nb.format
    if fmt == "ipynb":
        return to_ipynb(nb)
    if fmt == "fabric_git_source":
        return to_git_source(nb)
    return to_py(nb)


def sha256_text(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def drop_cells(nb: Notebook, predicate) -> list[int]:  # noqa: ANN001 - small helper, callable(Cell) -> bool
    """Remove cells for which ``predicate(cell)`` is true; returns the removed original indexes (used to exclude the submit cell)."""
    removed = [c.index for c in nb.cells if predicate(c)]
    nb.cells = [c for c in nb.cells if not predicate(c)]
    nb.renumber()
    return removed
