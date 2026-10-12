# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Notebook source formats: parse, normalise and serialise ``ipynb``, plain ``.py`` (percent) and the Fabric Git source.

Why this module exists
----------------------
The platform stores every notebook as a normalised nbformat v4 dictionary (CONTRACTS section 10) and must import / export
it as source that humans can read in a pull request.  The three supported spellings are:

``ipynb``
    Jupyter JSON.  Written deterministically (``indent=1``, sorted keys, multiline strings split into lines like
    ``nbformat`` does) so Git diffs show line changes instead of one long string.
``py`` (percent format)
    ``# %%`` cell markers, optionally ``# %% [markdown] id="c1" tags=["parameters"]``.  Markdown cells are commented
    (``# text``), magic lines (``%%sql``, ``%pip``, ``!ls``) are written as ``# MAGIC <line>`` so the file is valid Python.
``fabric``
    The Microsoft Fabric Git notebook source (``# Fabric notebook source`` / ``# METADATA ****`` / ``# CELL ****`` /
    ``# MARKDOWN ****`` / ``# PARAMETERS CELL ****`` / ``# META {json}`` / ``# MAGIC``).  Only the *format* is implemented here;
    compatibility analysis of the content belongs to the FABRIC package.

Guarantees (tested in ``tests/items/test_formats.py``)
-------------------------------------------------------
* ``parse(serialise(x)) == x`` for source, cell types, tags and ids (outputs and execution counts exist only in ``ipynb``);
  ``serialise(parse(text)) == text`` for text this module wrote.
* Lines that would be mistaken for a marker are escaped with ``#~ `` (reversible), so arbitrary code round-trips.
* Everything is deterministic: ids generated for cells that have none are derived from the cell position and content.
* Unsupported cell languages (Scala/R/C#) are *kept* (``%%scala`` prefix + ``metadata.language``) and reported as warnings; the
  runner and the analyzer reject them when somebody tries to run them (fail closed at execution, never silently dropped on import).
"""

from __future__ import annotations

import copy
import hashlib
import json
import re
import uuid
from typing import Any

from olcf.core.errors import BadRequest, Unsupported, ValidationFailed

NBFORMAT = 4
NBFORMAT_MINOR = 5
MAX_CELLS = 5000
MAX_SOURCE_BYTES = 25 * 1024 * 1024
CELL_TYPES = ("code", "markdown", "raw")
OUTPUT_TYPES = ("stream", "display_data", "execute_result", "error")
_ID_RE = re.compile(r"^[A-Za-z0-9_-]{1,64}$")
_DEFAULT_KERNELSPEC = {"name": "python3", "display_name": "Python 3 (ipykernel)", "language": "python"}
#: MIME types whose multiline-string form is joined (nbformat allows list-of-lines for these)
_JSONISH_MIMES = re.compile(r"(^application/(.+\+)?json$)|(^application/vnd\.olcf\.)")


# ============================================================================== normalisation
def plain(obj: Any) -> Any:
    """Recursively convert ``NotebookNode`` / dict subclasses into plain ``dict`` / ``list`` (JSON types only)."""
    if isinstance(obj, dict):
        return {str(k): plain(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple)):
        return [plain(v) for v in obj]
    return obj


def join_source(value: Any, where: str = "source") -> str:
    """nbformat allows ``source`` to be a string or a list of strings; the platform always stores a string."""
    if value is None:
        return ""
    if isinstance(value, str):
        return value
    if isinstance(value, list) and all(isinstance(x, str) for x in value):
        return "".join(value)
    raise ValidationFailed(f"{where} must be a string or a list of strings")


def derived_cell_id(index: int, cell_type: str, source: str) -> str:
    """Deterministic id for cells that come from a source format without ids (py / Fabric)."""
    h = hashlib.sha1(f"{index}\0{cell_type}\0{source}".encode(), usedforsecurity=False).hexdigest()  # noqa: S324 - identifier, not security
    return "c" + h[:11]


def new_cell_id() -> str:
    return uuid.uuid4().hex[:8]


def make_cell(cell_type: str, source: str, *, cell_id: str | None = None, metadata: dict[str, Any] | None = None) -> dict[str, Any]:
    """A normalised cell dictionary."""
    cell: dict[str, Any] = {"id": cell_id or new_cell_id(), "cell_type": cell_type, "metadata": metadata or {}, "source": source}
    if cell_type == "code":
        cell["execution_count"] = None
        cell["outputs"] = []
    return cell


def _join_text_outputs(out: dict[str, Any]) -> dict[str, Any]:
    if out.get("output_type") == "stream":
        out["text"] = join_source(out.get("text"), "stream output text")
        out.setdefault("name", "stdout")
    elif out.get("output_type") in ("display_data", "execute_result"):
        data = out.get("data")
        if not isinstance(data, dict):
            raise ValidationFailed(f"{out.get('output_type')} output needs a 'data' mapping")
        for mime, val in list(data.items()):
            if isinstance(val, list) and all(isinstance(x, str) for x in val) and not _JSONISH_MIMES.search(mime):
                data[mime] = "".join(val)
        out.setdefault("metadata", {})
        if out["output_type"] == "execute_result":
            out.setdefault("execution_count", None)
    elif out.get("output_type") == "error":
        out.setdefault("traceback", [])
    return out


def normalize_notebook(nb_in: Any, *, strip_outputs: bool = False) -> dict[str, Any]:
    """Return a validated, normalised nbformat v4.5 notebook as plain JSON types (idempotent).

    * ``source`` / text outputs become strings; every cell gets a unique ``id`` (existing valid ids are kept);
    * markdown / raw cells lose ``outputs`` / ``execution_count``; code cells always have both;
    * ``strip_outputs`` clears outputs and execution counts (used for Git and the ``strip_outputs`` item setting);
    * the result is checked against the official nbformat JSON schema; failures name the offending cell.
    """
    if not isinstance(nb_in, dict):
        raise ValidationFailed("a notebook must be a JSON object")
    nb = plain(nb_in)
    major = nb.get("nbformat")
    if major is None:
        raise ValidationFailed("notebook is missing 'nbformat'", remediation="use nbformat 4 (Jupyter notebook JSON)")
    if not isinstance(major, int) or isinstance(major, bool):
        raise ValidationFailed("'nbformat' must be an integer")
    if int(major) < 4:
        try:
            import nbformat

            nb = plain(nbformat.v4.upgrade(nbformat.from_dict(nb)))
        except Exception as exc:  # noqa: BLE001
            raise Unsupported(f"nbformat {major} notebooks cannot be upgraded automatically: {exc}", remediation="convert with 'jupyter nbconvert --to notebook' first") from exc
    elif int(major) > 4:
        raise Unsupported(f"nbformat {major} is not supported", remediation="export the notebook as nbformat 4")
    cells_in = nb.get("cells")
    if not isinstance(cells_in, list):
        raise ValidationFailed("notebook 'cells' must be a list")
    if len(cells_in) > MAX_CELLS:
        raise ValidationFailed(f"a notebook may have at most {MAX_CELLS} cells (has {len(cells_in)})")
    metadata = nb.get("metadata")
    if metadata is None:
        metadata = {}
    if not isinstance(metadata, dict):
        raise ValidationFailed("notebook 'metadata' must be an object")
    seen: set[str] = set()
    cells: list[dict[str, Any]] = []
    for i, c in enumerate(cells_in):
        if not isinstance(c, dict):
            raise ValidationFailed(f"cell {i} must be an object")
        ctype = c.get("cell_type")
        if ctype not in CELL_TYPES:
            raise ValidationFailed(f"cell {i}: unknown cell_type {ctype!r}", remediation="use code, markdown or raw")
        source = join_source(c.get("source"), f"cell {i} source")
        meta = c.get("metadata") if c.get("metadata") is not None else {}
        if not isinstance(meta, dict):
            raise ValidationFailed(f"cell {i}: metadata must be an object")
        tags = meta.get("tags")
        if tags is not None and not (isinstance(tags, list) and all(isinstance(t, str) for t in tags)):
            raise ValidationFailed(f"cell {i}: metadata.tags must be a list of strings")
        cid = c.get("id")
        if not isinstance(cid, str) or not _ID_RE.match(cid) or cid in seen:
            cid = derived_cell_id(i, ctype, source)
            while cid in seen:
                cid = new_cell_id()
        seen.add(cid)
        cell: dict[str, Any] = {"id": cid, "cell_type": ctype, "metadata": meta, "source": source}
        if ctype == "code":
            ec = c.get("execution_count")
            if ec is not None and not (isinstance(ec, int) and not isinstance(ec, bool)):
                raise ValidationFailed(f"cell {i}: execution_count must be an integer or null")
            outs = c.get("outputs") if c.get("outputs") is not None else []
            if not isinstance(outs, list):
                raise ValidationFailed(f"cell {i}: outputs must be a list")
            norm_outs = []
            for j, o in enumerate(outs):
                if not isinstance(o, dict) or o.get("output_type") not in OUTPUT_TYPES:
                    raise ValidationFailed(f"cell {i} output {j}: unknown output_type", remediation=f"use one of {', '.join(OUTPUT_TYPES)}")
                norm_outs.append(_join_text_outputs(o))
            cell["execution_count"] = None if strip_outputs else ec
            cell["outputs"] = [] if strip_outputs else norm_outs
        elif "attachments" in c and isinstance(c["attachments"], dict):
            cell["attachments"] = c["attachments"]
        cells.append(cell)
    out = {"nbformat": NBFORMAT, "nbformat_minor": max(NBFORMAT_MINOR, nb["nbformat_minor"] if isinstance(nb.get("nbformat_minor"), int) else 0), "metadata": metadata, "cells": cells}
    extra = set(nb) - {"nbformat", "nbformat_minor", "metadata", "cells"}
    if extra:
        raise ValidationFailed(f"unknown top-level notebook keys: {sorted(extra)}", remediation="notebook JSON may only contain nbformat, nbformat_minor, metadata and cells")
    _schema_validate(out)
    return out


def _schema_validate(nb: dict[str, Any]) -> None:
    import nbformat

    try:
        nbformat.validate(nbformat.from_dict(copy.deepcopy(nb)), version=4)
    except nbformat.ValidationError as exc:  # type: ignore[attr-defined]
        path = ".".join(str(p) for p in exc.path) if getattr(exc, "path", None) else ""
        raise ValidationFailed(f"invalid notebook ({path or 'root'}): {str(exc.message)[:300]}", details={"path": path}) from exc


def strip_outputs(nb: dict[str, Any]) -> dict[str, Any]:
    """A deep copy of ``nb`` without outputs / execution counts."""
    out = copy.deepcopy(nb)
    for c in out["cells"]:
        if c["cell_type"] == "code":
            c["outputs"] = []
            c["execution_count"] = None
    return out


def cell_tags(cell: dict[str, Any]) -> list[str]:
    return list((cell.get("metadata") or {}).get("tags") or [])


def with_tag(cell: dict[str, Any], tag: str) -> bool:
    return tag in cell_tags(cell)


# ============================================================================== ipynb
def _split_lines(text: str) -> list[str] | str:
    """nbformat's on-disk spelling of multiline strings: a list of lines that keeps the newlines."""
    if "\n" not in text:
        return text
    return text.splitlines(keepends=True) if text else text


def to_ipynb_text(nb: dict[str, Any], *, strip: bool = False, split: bool = True) -> str:
    """Deterministic ipynb JSON (what ``nbformat.writes`` produces, minus its validation round trip)."""
    doc = strip_outputs(nb) if strip else copy.deepcopy(nb)
    if split:
        for c in doc["cells"]:
            c["source"] = _split_lines(c["source"])
            for o in c.get("outputs", []) or []:
                if o.get("output_type") == "stream":
                    o["text"] = _split_lines(o["text"])
                elif isinstance(o.get("data"), dict):
                    for mime, val in list(o["data"].items()):
                        if isinstance(val, str) and not _JSONISH_MIMES.search(mime) and not mime.startswith("image/") and mime != "application/pdf":
                            o["data"][mime] = _split_lines(val)
    return json.dumps(doc, indent=1, sort_keys=True, ensure_ascii=False) + "\n"


def parse_ipynb(text: str | bytes) -> dict[str, Any]:
    if isinstance(text, bytes):
        text = text.decode("utf-8-sig")
    if len(text) > MAX_SOURCE_BYTES:
        raise BadRequest("notebook is too large")
    try:
        doc = json.loads(text)
    except ValueError as exc:
        raise ValidationFailed(f"not valid notebook JSON: {exc}") from exc
    return normalize_notebook(doc)


# ============================================================================== escaping shared by the two .py formats
_MARKER_LIKE = re.compile(r"^(#\s*%%(\s|$)|# MAGIC |#~ )")


def _is_magic(line: str) -> bool:
    return line.startswith(("%", "!"))


def encode_code_lines(source: str, prefix: str = "#", *, fabric: bool = False) -> list[str]:
    """Code cell source -> file lines (magics become ``# MAGIC ...``; marker look-alikes - including Fabric section lines when ``fabric`` - are escaped with ``#~ ``)."""
    if source == "":
        return []
    lines = source.split("\n")
    cell_magic = lines[0].startswith("%%")
    out = []
    for ln in lines:
        if cell_magic or _is_magic(ln):
            out.append(f"{prefix} MAGIC {ln}")
        elif ln.startswith(("#~ ", f"{prefix} MAGIC ")) or re.match(r"^#\s*%%(\s|$)", ln) or (fabric and _FABRIC_SECTION.match(ln.strip())):
            out.append(f"#~ {ln}")
        else:
            out.append(ln)
    return out


def decode_code_lines(lines: list[str], prefix: str = "#") -> str:
    out = []
    magic = f"{prefix} MAGIC "
    for ln in lines:
        if ln.startswith(magic):
            out.append(ln[len(magic):])
        elif ln.startswith("#~ "):
            out.append(ln[3:])
        else:
            out.append(ln)
    return "\n".join(out)


def _comment_lines(source: str, prefix: str = "#") -> list[str]:
    if source == "":
        return []
    return [prefix if ln == "" else f"{prefix} {ln}" for ln in source.split("\n")]


def _uncomment_lines(lines: list[str], prefix: str = "#") -> str:
    out = []
    for ln in lines:
        if ln.startswith("#~ "):  # an escaped line that would have looked like a Fabric section marker
            ln = ln[3:]
        if ln == prefix:
            out.append("")
        elif ln.startswith(prefix + " "):
            out.append(ln[len(prefix) + 1:])
        else:
            out.append(ln)
    return "\n".join(out)


def _split_text(text: str) -> list[str]:
    lines = text.replace("\r\n", "\n").replace("\r", "\n").split("\n")
    if lines and lines[-1] == "":
        lines.pop()
    return lines


# ============================================================================== percent format
_PCT_MARKER = re.compile(r"^#\s*%%(?P<rest>(\s.*)?)$")
_PCT_META_HEADER = "# OLCF-NOTEBOOK-METADATA: "
_PCT_TYPE = re.compile(r"^\s*\[(?P<t>markdown|md|raw|code)\]")
_PCT_KEYS = ("id", "tags", "name", "meta")


def _parse_pct_header(rest: str) -> tuple[str, dict[str, Any], str]:
    ctype = "code"
    m = _PCT_TYPE.match(rest)
    if m:
        t = m.group("t")
        ctype = {"md": "markdown"}.get(t, t)
        rest = rest[m.end():]
    kv: dict[str, Any] = {}
    title_parts: list[str] = []
    dec = json.JSONDecoder()
    pos = 0
    while pos < len(rest):
        km = re.compile(r"(?:^|\s)(" + "|".join(_PCT_KEYS) + r")=").search(rest, pos)
        if not km:
            title_parts.append(rest[pos:])
            break
        title_parts.append(rest[pos:km.start()])
        try:
            val, end = dec.raw_decode(rest, km.end())
        except ValueError:
            title_parts.append(rest[km.start():])
            break
        kv[km.group(1)] = val
        pos = end
    return ctype, kv, " ".join(p.strip() for p in title_parts if p.strip())


def parse_py_percent(text: str | bytes, *, warnings: list[str] | None = None) -> dict[str, Any]:
    """Percent-format ``.py`` -> normalised notebook.  A file without markers is a single code cell."""
    if isinstance(text, bytes):
        text = text.decode("utf-8-sig")
    if len(text) > MAX_SOURCE_BYTES:
        raise BadRequest("notebook source is too large")
    lines = _split_text(text)
    nb_meta: dict[str, Any] = {}
    if lines and lines[0].startswith(_PCT_META_HEADER):
        try:
            nb_meta = json.loads(lines[0][len(_PCT_META_HEADER):])
        except ValueError:
            (warnings if warnings is not None else []).append("ignored an unreadable OLCF-NOTEBOOK-METADATA header")
        lines = lines[1:]
    segments: list[tuple[dict[str, Any] | None, list[str]]] = [(None, [])]
    for ln in lines:
        m = _PCT_MARKER.match(ln)
        if m and not ln.startswith("#~ "):
            ctype, kv, title = _parse_pct_header(m.group("rest") or "")
            segments.append(({"type": ctype, "kv": kv, "title": title}, []))
        else:
            segments[-1][1].append(ln)
    cells: list[dict[str, Any]] = []
    for idx, (hdr, body) in enumerate(segments):
        last = idx == len(segments) - 1
        if hdr is None:
            if not any(b.strip() for b in body):
                continue  # preamble that is only blank lines
            if not last:
                body = body[:-1] if body and body[-1] == "" else body
            src = decode_code_lines(body)
            cells.append(make_cell("code", src, cell_id=derived_cell_id(len(cells), "code", src)))
            continue
        if not last and body and body[-1] == "":
            body = body[:-1]
        ctype = hdr["type"]
        src = decode_code_lines(body) if ctype == "code" else _uncomment_lines(body)
        meta: dict[str, Any] = {}
        kv = hdr["kv"]
        if isinstance(kv.get("meta"), dict):
            meta.update(kv["meta"])
        if isinstance(kv.get("tags"), list):
            meta["tags"] = [str(t) for t in kv["tags"]]
        if hdr["title"]:
            meta["title"] = hdr["title"]
        if isinstance(kv.get("name"), str):
            meta["name"] = kv["name"]
        cid = kv.get("id") if isinstance(kv.get("id"), str) and _ID_RE.match(kv["id"]) else derived_cell_id(len(cells), ctype, src)
        cells.append(make_cell(ctype, src, cell_id=cid, metadata=meta))
    nb_meta.setdefault("kernelspec", dict(_DEFAULT_KERNELSPEC))
    return normalize_notebook({"nbformat": 4, "nbformat_minor": 5, "metadata": nb_meta, "cells": cells})


def to_py_percent(nb: dict[str, Any], *, metadata_header: bool = False, ids: bool = True) -> str:
    """Normalised notebook -> percent-format source (outputs are not part of this format)."""
    out: list[str] = []
    if metadata_header and nb.get("metadata"):
        out.append(_PCT_META_HEADER + json.dumps(nb["metadata"], sort_keys=True, ensure_ascii=False, separators=(",", ":")))
    n = len(nb["cells"])
    for i, c in enumerate(nb["cells"]):
        ctype = c["cell_type"]
        meta = dict(c.get("metadata") or {})
        tags = meta.pop("tags", None)
        title = meta.pop("title", None)
        name = meta.pop("name", None)
        head = "# %%"
        if ctype != "code":
            head += f" [{ctype}]"
        if isinstance(title, str) and title and not re.search(r"(^|\s)(id|tags|name|meta)=", title) and "\n" not in title:
            head += f" {title}"
        elif title:
            meta["title"] = title
        if ids:
            head += f" id={json.dumps(c['id'], ensure_ascii=False)}"
        if tags:
            head += f" tags={json.dumps(tags, ensure_ascii=False)}"
        if isinstance(name, str):
            head += f" name={json.dumps(name, ensure_ascii=False)}"
        elif name is not None:
            meta["name"] = name
        if meta:
            head += f" meta={json.dumps(meta, sort_keys=True, ensure_ascii=False, separators=(',', ':'))}"
        out.append(head)
        body = encode_code_lines(c["source"]) if ctype == "code" else _comment_lines(c["source"])
        out.extend(body)
        if i != n - 1:
            out.append("")
    return "\n".join(out) + "\n"


# ============================================================================== Fabric Git source
_FABRIC_HEADER = re.compile(r"^(#|--|//) Fabric notebook source\s*$")
_FABRIC_SECTION = re.compile(r"^(?P<p>#|--|//) (?P<kind>METADATA|CELL|MARKDOWN|PARAMETERS CELL|INJECTED PARAMETERS) \*{4,}\s*$")
_LANG_FROM_MAGIC = {"sql": "sparksql", "sparksql": "sparksql", "pyspark": "python", "python": "python", "scala": "scala", "spark": "scala", "r": "r", "sparkr": "r", "csharp": "csharp"}
_UNSUPPORTED_LANGS = {"scala": "scala", "r": "r", "sparkr": "r", "csharp": "csharp"}


def is_fabric_source(text: str) -> bool:
    for ln in text.lstrip("﻿").splitlines()[:5]:
        if ln.strip():
            return bool(_FABRIC_HEADER.match(ln.strip()))
    return False


def _fabric_meta(body: list[str], prefix: str) -> dict[str, Any]:
    pat = re.compile(rf"^{re.escape(prefix)}\s*META ?")
    chunk = "\n".join(pat.sub("", ln, count=1) for ln in body if pat.match(ln))
    if not chunk.strip():
        return {}
    try:
        val = json.loads(chunk)
    except ValueError as exc:
        raise ValidationFailed(f"unreadable '{prefix} META' block in the Fabric notebook source: {exc}") from exc
    if not isinstance(val, dict):
        raise ValidationFailed("a Fabric META block must be a JSON object")
    return val


def _trim_blank(body: list[str]) -> list[str]:
    if body and body[0].strip() == "":
        body = body[1:]
    if body and body[-1].strip() == "":
        body = body[:-1]
    return body


def parse_fabric_git(text: str | bytes, *, warnings: list[str] | None = None) -> dict[str, Any]:
    """Fabric Git ``notebook-content.py`` -> normalised notebook (see module docstring)."""
    if isinstance(text, bytes):
        text = text.decode("utf-8-sig")
    if len(text) > MAX_SOURCE_BYTES:
        raise BadRequest("notebook source is too large")
    warn = warnings if warnings is not None else []
    lines = _split_text(text.lstrip("﻿"))
    first = next((i for i, ln in enumerate(lines) if ln.strip()), None)
    if first is None or not _FABRIC_HEADER.match(lines[first].strip()):
        raise ValidationFailed("not a Fabric Git notebook source (missing the '# Fabric notebook source' header line)")
    prefix = _FABRIC_HEADER.match(lines[first].strip()).group(1)  # type: ignore[union-attr]
    sections: list[tuple[str, list[str]]] = []
    for ln in lines[first + 1:]:
        m = _FABRIC_SECTION.match(ln.strip())
        if m and m.group("p") == prefix:
            sections.append((m.group("kind"), []))
        elif sections:
            sections[-1][1].append(ln)
    nb_meta: dict[str, Any] = {}
    cells: list[dict[str, Any]] = []
    for kind, body in sections:
        if kind == "METADATA":
            md = _fabric_meta(body, prefix)
            if cells:
                cell = cells[-1]
                cell["metadata"]["fabric"] = md
                lang = md.get("language")
                if lang and cell["cell_type"] == "code":
                    cell["metadata"]["language"] = lang
                extra = md.get("olcf") if isinstance(md.get("olcf"), dict) else {}
                if isinstance(extra.get("id"), str) and _ID_RE.match(extra["id"]):
                    cell["id"] = extra["id"]
                if isinstance(extra.get("tags"), list):
                    cell["metadata"]["tags"] = sorted({*cell_tags(cell), *[str(t) for t in extra["tags"]]})
            else:
                nb_meta.update(md)
            continue
        body = _trim_blank(body)
        idx = len(cells)
        if kind == "MARKDOWN":
            src = _uncomment_lines(body, prefix)
            cells.append(make_cell("markdown", src, cell_id=derived_cell_id(idx, "markdown", src)))
            continue
        src = decode_code_lines(body, prefix)
        meta: dict[str, Any] = {}
        if kind == "PARAMETERS CELL":
            meta["tags"] = ["parameters"]
        elif kind == "INJECTED PARAMETERS":
            meta["tags"] = ["injected-parameters"]
        cells.append(make_cell("code", src, cell_id=derived_cell_id(idx, "code", src), metadata=meta))
    for i, c in enumerate(cells):
        if c["cell_type"] != "code":
            continue
        lang = str(c["metadata"].get("language") or "python").lower()
        if lang in ("sparksql", "sql") and not c["source"].lstrip().startswith("%%"):
            c["source"] = "%%sql\n" + c["source"]
        elif lang in _UNSUPPORTED_LANGS:
            if not c["source"].lstrip().startswith("%%"):
                c["source"] = f"%%{_UNSUPPORTED_LANGS[lang]}\n" + c["source"]
            warn.append(f"cell {i} uses language '{lang}', which OLCF notebooks do not execute (python, pyspark and sql only); the cell was kept as '%%{_UNSUPPORTED_LANGS[lang]}'")
    # cells of an explicit id in META keep it; re-derive collisions through normalisation
    nb_meta.setdefault("kernelspec", dict(_DEFAULT_KERNELSPEC))
    return normalize_notebook({"nbformat": 4, "nbformat_minor": 5, "metadata": nb_meta, "cells": cells})


def _fabric_language(source: str, cell_meta: dict[str, Any]) -> str:
    first = source.split("\n", 1)[0].strip() if source else ""
    if first.startswith("%%"):
        magic = first[2:].split()[0].lower() if first[2:].split() else ""
        if magic in _LANG_FROM_MAGIC:
            return _LANG_FROM_MAGIC[magic]
    lang = cell_meta.get("language")
    if isinstance(lang, str) and lang and lang != "markdown":
        return lang
    return "python"


def to_fabric_git(nb: dict[str, Any], *, ids: bool = False) -> str:
    """Normalised notebook -> Fabric Git ``notebook-content.py``.

    ``ids=True`` records the cell ids (and non-parameter tags) in the cell's META block under ``"olcf"`` so Git history keeps stable cell identity;
    Fabric ignores unknown META keys.
    """
    out: list[str] = ["# Fabric notebook source", ""]

    def meta_block(md: dict[str, Any]) -> None:
        out.append("# METADATA ********************")
        out.append("")
        for ln in json.dumps(md, indent=2, ensure_ascii=False, sort_keys=False).split("\n"):
            out.append(f"# META {ln}")
        out.append("")

    meta = nb.get("metadata") or {}
    top = {k: copy.deepcopy(meta[k]) for k in ("kernel_info", "dependencies") if k in meta}
    top.setdefault("kernel_info", {"name": "synapse_pyspark"})
    meta_block(top)
    group = "jupyter_python" if str(top["kernel_info"].get("name", "")).startswith("jupyter") else "synapse_pyspark"
    for c in nb["cells"]:
        cmeta = dict(c.get("metadata") or {})
        tags = list(cmeta.get("tags") or [])
        if c["cell_type"] == "markdown" or c["cell_type"] == "raw":
            out.append("# MARKDOWN ********************")
            out.append("")
            out.extend(f"#~ {ln}" if _FABRIC_SECTION.match(ln.strip()) else ln for ln in _comment_lines(c["source"]))
            out.append("")
            md = copy.deepcopy(cmeta.get("fabric") or {})
            md["language"] = "markdown"
            md.setdefault("language_group", group)
        else:
            parameter = "parameters" in tags
            out.append("# PARAMETERS CELL ********************" if parameter else "# CELL ********************")
            out.append("")
            src = c["source"]
            out.extend(encode_code_lines(src, fabric=True))
            out.append("")
            md = copy.deepcopy(cmeta.get("fabric") or {})
            md["language"] = _fabric_language(src, {**cmeta, **({"language": md["language"]} if "language" in md else {})})
            md.setdefault("language_group", group)
        if ids:
            extra = dict(md.get("olcf") or {})
            extra["id"] = c["id"]
            other_tags = [t for t in tags if t != "parameters"]
            if other_tags:
                extra["tags"] = other_tags
            md["olcf"] = extra
        elif isinstance(md.get("olcf"), dict):
            md = {k: v for k, v in md.items() if k != "olcf"}
        meta_block(md)
    return "\n".join(out).rstrip("\n") + "\n"


# ============================================================================== front doors
FORMATS = ("ipynb", "py", "fabric")


def sniff_format(text: str | bytes, filename: str | None = None) -> str:
    """Guess the source format from content (preferred) and file name."""
    if isinstance(text, bytes):
        text = text.decode("utf-8-sig", errors="replace")
    head = text.lstrip("﻿ \t\r\n")
    if head.startswith("{"):
        return "ipynb"
    if is_fabric_source(text):
        return "fabric"
    if filename and filename.lower().endswith(".ipynb"):
        return "ipynb"
    return "py"


def parse_notebook(content: str | bytes, fmt: str | None = None, *, filename: str | None = None, warnings: list[str] | None = None) -> dict[str, Any]:
    """Parse any supported spelling into a normalised notebook; ``fmt`` of ``None`` / ``auto`` sniffs."""
    f = (fmt or "auto").lower()
    if f in ("fabric_git_source", "fabric-git", "fabric_git"):
        f = "fabric"
    if f == "auto":
        f = sniff_format(content, filename)
    if f == "ipynb":
        return parse_ipynb(content)
    if f == "py":
        return parse_py_percent(content, warnings=warnings)
    if f == "fabric":
        return parse_fabric_git(content, warnings=warnings)
    raise Unsupported(f"unsupported notebook format '{fmt}'", remediation=f"use one of {', '.join(FORMATS)}")


def serialize_notebook(nb: dict[str, Any], fmt: str, *, outputs: bool = True, ids: bool = True, metadata_header: bool = False) -> str:
    f = (fmt or "ipynb").lower()
    if f in ("fabric_git_source", "fabric-git", "fabric_git"):
        f = "fabric"
    if f == "ipynb":
        return to_ipynb_text(nb, strip=not outputs)
    if f == "py":
        return to_py_percent(nb, metadata_header=metadata_header, ids=ids)
    if f == "fabric":
        return to_fabric_git(nb, ids=ids)
    raise Unsupported(f"unsupported notebook format '{fmt}'", remediation=f"use one of {', '.join(FORMATS)}")
