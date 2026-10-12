# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""``NotebookService`` (``ctx.service("notebooks")``): the product-level notebook operations.

Responsibilities
----------------
* **Definition**: :func:`normalise_definition` is the ``validate`` item hook of the ``notebook`` type (CONTRACTS section 10).  It accepts the item
  definition produced by the DATA sample installer unchanged, additionally accepts ``{"source": "...", "format": "py|fabric|ipynb"}`` (a notebook given
  as text), validates the nbformat document with the official JSON schema and normalises ``settings`` (``default_lakehouse.id`` is rewritten to the
  ``LakehouseRef`` spelling ``lakehouse_id`` that the compute controller understands).  The normalisation is idempotent so saving an unchanged notebook
  never creates a new version.
* **Cross-package interface**: ``load(item) -> Notebook``, ``parse(content, format) -> Notebook``, ``to_ipynb / to_py / to_fabric``, ``detect_parameters``.
* **Editor API**: content (cells with stable ids + ETag), version history, restore, per-cell diff, import (ipynb / py / fabric / zip) and export (ipynb / py /
  fabric / sanitised html).
* **Execution**: batch run through ``ctx.service("jobs")`` (parameters coerced against the detected parameter cell, runtime / environment / default lakehouse resolved
  from the notebook settings, variable sets and the notebook ``%pip`` policy applied before the job exists) and interactive sessions through
  ``ctx.service("sessions")``.

Parameter *injection* itself happens in the runner (``olcf.runtime.executor.inject_parameters``, papermill semantics); :mod:`olcf.notebooks.params` provides the same
semantics for the control plane (preview endpoint, tests, export with parameters).
"""

from __future__ import annotations

import io
import json
import re
import shlex
import zipfile
from dataclasses import dataclass, field
from pathlib import PurePosixPath
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field
from sqlalchemy import select

from olcf.core.context import AppContext
from olcf.core.errors import BadRequest, NotFound, PolicyDenied, Unsupported, ValidationFailed
from olcf.core.models import (
    ItemCreate,
    ItemUpdate,
    JobKind,
    JobSubmission,
    LakehouseRef,
    Origin,
    OriginKind,
    PolicySpec,
    ResourceRequest,
    RuntimeRequest,
)
from olcf.core.redaction import redact_obj, redact_text
from olcf.db import schema as S
from olcf.db.util import fetch_all
from olcf.notebooks import diff as nbdiff
from olcf.notebooks import formats, htmlexport, params
from olcf.notebooks.model import Notebook
from olcf.security.rbac import Principal

MAX_IMPORT_FILES = 500
MAX_IMPORT_BYTES = 200 * 1024 * 1024
MAX_FILE_BYTES = formats.MAX_SOURCE_BYTES
SETTINGS_KEYS = {"engine", "runtime", "default_lakehouse", "environment_id", "timeout_seconds", "resources", "policy", "pip_policy", "variable_set", "strip_outputs", "fabric"}
PIP_ORDER = {"block": 0, "declared": 1, "allow": 2}
_PIP_LINE = re.compile(r"^\s*(?:%pip|%conda|!pip3?|!python3?\s+-m\s+pip|!uv\s+pip)\s+(?P<rest>.*)$")


# ============================================================================== settings / definition
def _model(cls: type[BaseModel], value: Any, what: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise ValidationFailed(f"settings.{what} must be an object")
    try:
        return cls.model_validate(value).model_dump(exclude_unset=True, exclude_none=True)
    except Exception as exc:  # noqa: BLE001 - pydantic ValidationError -> actionable message
        msg = str(exc).splitlines()
        raise ValidationFailed(f"invalid settings.{what}: {' '.join(msg[:3])[:300]}") from exc


def normalise_settings(raw: Any) -> dict[str, Any]:
    """Validate and normalise ``definition.settings``.  Unknown keys are rejected (except ``x-*``)."""
    if raw is None:
        return {}
    if not isinstance(raw, dict):
        raise ValidationFailed("notebook 'settings' must be an object")
    bad = [k for k in raw if k not in SETTINGS_KEYS and not str(k).startswith("x-")]
    if bad:
        raise ValidationFailed(f"unknown notebook settings {sorted(bad)}", remediation=f"supported: {sorted(SETTINGS_KEYS)}")
    out: dict[str, Any] = {}
    for k, v in raw.items():
        if v is None and k in ("default_lakehouse", "environment_id", "timeout_seconds", "variable_set", "pip_policy"):
            out[k] = None
            continue
        if k == "engine":
            if v not in ("spark", "python"):
                raise ValidationFailed("settings.engine must be 'spark' or 'python'")
            out[k] = v
        elif k == "runtime":
            out[k] = _model(RuntimeRequest, v, "runtime")
        elif k == "default_lakehouse":
            if not isinstance(v, dict):
                raise ValidationFailed("settings.default_lakehouse must be an object ({id|lakehouse_id|name})")
            d = dict(v)
            if "id" in d:  # CONTRACTS section 10 spelling -> LakehouseRef spelling
                if "lakehouse_id" in d and d["lakehouse_id"] != d["id"]:
                    raise ValidationFailed("settings.default_lakehouse has both 'id' and a different 'lakehouse_id'")
                d["lakehouse_id"] = d.pop("id")
            out[k] = _model(LakehouseRef, d, "default_lakehouse")
        elif k == "environment_id" or k == "variable_set":
            if not isinstance(v, str) or not v or len(v) > 128:
                raise ValidationFailed(f"settings.{k} must be an item id or name")
            out[k] = v
        elif k == "timeout_seconds":
            if not isinstance(v, int) or isinstance(v, bool) or v < 1:
                raise ValidationFailed("settings.timeout_seconds must be a positive integer")
            out[k] = v
        elif k == "resources":
            out[k] = _model(ResourceRequest, v, "resources")
        elif k == "policy":
            out[k] = _model(PolicySpec, v, "policy")
        elif k == "pip_policy":
            if v not in PIP_ORDER:
                raise ValidationFailed("settings.pip_policy must be block, declared or allow")
            out[k] = v
        elif k == "strip_outputs":
            out[k] = bool(v)
        else:  # fabric, x-*
            out[k] = v
    return out


def normalise_definition(defn: Any) -> dict[str, Any]:
    """The ``validate`` hook of the ``notebook`` item type (idempotent)."""
    if not isinstance(defn, dict):
        raise ValidationFailed("a notebook definition must be an object")
    settings = normalise_settings(defn.get("settings"))
    if "nbformat" in defn and isinstance(defn["nbformat"], dict):
        extra = set(defn) - {"nbformat", "settings"}
        if extra:
            raise ValidationFailed(f"unknown notebook definition keys {sorted(extra)}", remediation="a notebook definition has 'nbformat' and 'settings'")
        nb = formats.normalize_notebook(defn["nbformat"], strip_outputs=bool(settings.get("strip_outputs")))
    elif isinstance(defn.get("source"), str):
        extra = set(defn) - {"source", "format", "settings"}
        if extra:
            raise ValidationFailed(f"unknown notebook definition keys {sorted(extra)}")
        nb = formats.normalize_notebook(formats.parse_notebook(defn["source"], defn.get("format")), strip_outputs=bool(settings.get("strip_outputs")))
    elif not defn or set(defn) <= {"settings"}:
        nb = formats.normalize_notebook({"nbformat": 4, "nbformat_minor": 5, "metadata": {}, "cells": []})  # an empty notebook
    else:
        raise ValidationFailed("a notebook definition needs 'nbformat' (a Jupyter notebook object) or 'source' (+ 'format')")
    out: dict[str, Any] = {"nbformat": nb}
    if settings:
        out["settings"] = settings
    return out


# ============================================================================== request models
class _Req(BaseModel):
    model_config = ConfigDict(extra="forbid")


class RunRequest(_Req):
    """Body of ``POST /v1/notebooks/{id}/run``; anything not given comes from the notebook settings."""

    parameters: dict[str, Any] = Field(default_factory=dict)
    name: str | None = Field(None, max_length=255)
    engine: Literal["spark", "python"] | None = None
    runtime: dict[str, Any] | None = None
    resources: dict[str, Any] | None = None
    policy: dict[str, Any] | None = None
    default_lakehouse: dict[str, Any] | None = None
    environment_id: str | None = None
    variable_set: str | None = None
    stage: str | None = Field(None, pattern=r"^[A-Za-z0-9_-]{1,32}$")
    tags: dict[str, str] = Field(default_factory=dict)
    idempotency_key: str | None = Field(None, max_length=191)


class SessionRequest(_Req):
    """Body of ``POST /v1/notebooks/{id}/session``."""

    engine: Literal["spark", "python"] | None = None
    runtime: dict[str, Any] | None = None
    resources: dict[str, Any] | None = None
    default_lakehouse: dict[str, Any] | None = None
    placement: dict[str, Any] = Field(default_factory=dict)
    name: str | None = Field(None, max_length=255)
    idle_timeout_seconds: int | None = Field(None, ge=60)
    eager_spark: bool = True


class CellIn(_Req):
    """A notebook cell in a request: optional id, type (code, markdown or raw), source and metadata."""
    id: str | None = Field(None, pattern=r"^[A-Za-z0-9_-]{1,64}$")
    cell_type: Literal["code", "markdown", "raw"] = "code"
    source: str | list[str] = ""
    metadata: dict[str, Any] = Field(default_factory=dict)
    outputs: list[dict[str, Any]] | None = None
    execution_count: int | None = None
    attachments: dict[str, Any] | None = None


class ContentPut(_Req):
    """Body of ``PUT /v1/notebooks/{id}/content``: the whole cell list (cells keep their ids) and optionally metadata / settings."""

    cells: list[CellIn]
    metadata: dict[str, Any] | None = None
    settings: dict[str, Any] | None = None
    strip_outputs: bool | None = Field(None, description="true strips the outputs of this save; the notebook setting strip_outputs=true always applies")
    comment: str | None = Field(None, max_length=500)


@dataclass
class ImportItem:
    """A notebook prepared for import: name, parsed document, source format and original source."""
    name: str
    nb: dict[str, Any]
    format: str
    source: str
    settings: dict[str, Any] = field(default_factory=dict)
    warnings: list[str] = field(default_factory=list)


# ============================================================================== the service
class NotebookService:
    """See the module docstring.  Instances are stateless apart from the application context."""

    def __init__(self, ctx: AppContext) -> None:
        self.ctx = ctx

    # ------------------------------------------------------------------ cross-package interface (CONTRACTS section 2)
    def parse(self, content: str | bytes, fmt: str | None = None, *, filename: str | None = None, warnings: list[str] | None = None) -> Notebook:
        """Parse ipynb / percent ``.py`` / Fabric Git source into a normalised :class:`Notebook` (no item attached)."""
        nb = formats.normalize_notebook(formats.parse_notebook(content, fmt, filename=filename, warnings=warnings))
        return Notebook(nb=nb)

    def load(self, item: dict[str, Any] | str) -> Notebook:
        """The notebook of an item row (or item id)."""
        row = self.ctx.service("items").get(item) if isinstance(item, str) else item
        if row["type"] != "notebook":
            raise BadRequest(f"item {row['id']} is a {row['type']}, not a notebook")
        d = normalise_definition(row.get("definition") or {})
        return Notebook(nb=d["nbformat"], settings=d.get("settings") or {}, name=row.get("name"), item_id=row["id"], workspace_id=row["workspace_id"], version=row.get("version"))

    @staticmethod
    def _nb_dict(nb: Notebook | dict[str, Any]) -> dict[str, Any]:
        return nb.nb if isinstance(nb, Notebook) else nb

    def to_ipynb(self, nb: Notebook | dict[str, Any], *, outputs: bool = False) -> str:
        """Deterministic ipynb text.  The default is the *executable* form (no outputs, so the job source hash depends only on the code);
        pass ``outputs=True`` to keep stored outputs (user-facing export)."""
        return formats.to_ipynb_text(self._nb_dict(nb), strip=not outputs)

    def to_py(self, nb: Notebook | dict[str, Any], *, ids: bool = True, metadata_header: bool = False) -> str:
        return formats.to_py_percent(self._nb_dict(nb), ids=ids, metadata_header=metadata_header)

    def to_fabric(self, nb: Notebook | dict[str, Any], *, ids: bool = False) -> str:
        return formats.to_fabric_git(self._nb_dict(nb), ids=ids)

    def detect_parameters(self, nb: Notebook | dict[str, Any]) -> params.Detection:
        return params.detect_parameters(self._nb_dict(nb))

    def inject_parameters(self, nb: Notebook | dict[str, Any], values: dict[str, Any] | None) -> dict[str, Any]:
        return params.inject_parameters(self._nb_dict(nb), values)

    def validate_definition(self, defn: Any) -> dict[str, Any]:
        return normalise_definition(defn)

    # ------------------------------------------------------------------ item access (authz always from the stored item)
    def item(self, principal: Principal, item_id: str, perm: str, *, include_deleted: bool = False) -> dict[str, Any]:
        row = self.ctx.service("items").get(item_id, include_deleted=include_deleted)
        if row["type"] != "notebook":
            raise NotFound(f"notebook {item_id} not found")
        principal.require(perm, workspace_id=row["workspace_id"], item_id=row["id"])
        return row

    # ------------------------------------------------------------------ content (editor API)
    def content(self, row: dict[str, Any], *, outputs: bool = True) -> dict[str, Any]:
        nb = self.load(row)
        data = nb.nb if outputs else formats.strip_outputs(nb.nb)
        det = params.detect_parameters(nb.nb)
        return {"id": row["id"], "workspace_id": row["workspace_id"], "name": row["name"], "version": row["version"], "etag": row["etag"], "updated_at": row["updated_at"],
                "nbformat": data["nbformat"], "nbformat_minor": data["nbformat_minor"], "metadata": data["metadata"], "cells": data["cells"], "settings": nb.settings,
                "parameters": det.to_dict()}

    def put_content(self, principal: Principal, row: dict[str, Any], body: ContentPut, if_match: str | None) -> dict[str, Any]:
        cur = self.load(row)
        settings = normalise_settings(body.settings) if body.settings is not None else cur.settings
        strip = body.strip_outputs if body.strip_outputs is not None else bool(settings.get("strip_outputs"))
        nb_in = {"nbformat": 4, "nbformat_minor": cur.nb.get("nbformat_minor", 5), "metadata": body.metadata if body.metadata is not None else cur.nb["metadata"],
                 "cells": [self._cell_in(c) for c in body.cells]}
        nb = formats.normalize_notebook(nb_in, strip_outputs=strip)
        definition = {"nbformat": nb, **({"settings": settings} if settings else {})}
        item = self.ctx.service("items").update(principal, row["id"], ItemUpdate(definition=definition, comment=body.comment or "edited"), if_match)
        return item

    @staticmethod
    def _cell_in(c: CellIn) -> dict[str, Any]:
        d: dict[str, Any] = {"cell_type": c.cell_type, "source": c.source, "metadata": c.metadata}
        if c.id:
            d["id"] = c.id
        if c.cell_type == "code":
            d["outputs"] = c.outputs or []
            d["execution_count"] = c.execution_count
        if c.attachments:
            d["attachments"] = c.attachments
        return d

    # ------------------------------------------------------------------ versions / diff / restore
    def versions(self, row: dict[str, Any]) -> list[dict[str, Any]]:
        out = []
        for v in self.ctx.service("items").versions(row["id"]):
            out.append({"version": v["version"], "name": v["name"], "comment": v["comment"], "created_at": v["created_at"], "created_by": v["created_by"],
                        "current": v["version"] == row["version"]})
        return out

    def version_definition(self, row: dict[str, Any], ref: str | int | None) -> tuple[int, dict[str, Any]]:
        """``ref``: an integer version, ``current`` / ``head`` / None (the current version) or ``previous``."""
        if ref in (None, "", "current", "head"):
            return row["version"], row["definition"]
        if ref == "previous":
            ref = row["version"] - 1
        try:
            n = int(ref)  # type: ignore[arg-type]
        except (TypeError, ValueError):
            raise BadRequest(f"invalid version '{ref}'", remediation="use a version number, 'current' or 'previous'") from None
        if n == row["version"]:
            return n, row["definition"]
        for v in self.ctx.service("items").versions(row["id"]):
            if v["version"] == n:
                return n, v["definition"]
        raise NotFound(f"version {n} of notebook {row['id']} does not exist")

    def diff(self, row: dict[str, Any], a: str | int | None, b: str | int | None, *, context: int = 3) -> dict[str, Any]:
        """Per-cell diff of two versions (outputs and execution counts ignored).  Defaults: ``a`` = previous, ``b`` = current."""
        if a in (None, ""):
            a = "previous" if row["version"] > 1 else "current"
        vb, db_ = self.version_definition(row, b)
        va, da = self.version_definition(row, a)
        res = nbdiff.diff_notebooks(self._plain_def(da), self._plain_def(db_), context=max(0, min(context, 50)))
        res.update({"item_id": row["id"], "a": va, "b": vb, "text": nbdiff.render_text(res, f"{row['name']}@v{va}", f"{row['name']}@v{vb}")})
        return res

    @staticmethod
    def _plain_def(defn: dict[str, Any]) -> dict[str, Any]:
        d = normalise_definition(defn)
        return d

    def restore(self, principal: Principal, row: dict[str, Any], version: int) -> dict[str, Any]:
        _, defn = self.version_definition(row, version)
        return self.ctx.service("items").update(principal, row["id"], ItemUpdate(definition=defn, comment=f"restored from v{version}"), None)

    # ------------------------------------------------------------------ export
    def export(self, row: dict[str, Any], fmt: str, *, version: str | int | None = None, outputs: bool = True) -> tuple[bytes, str, str]:
        """Return ``(bytes, media_type, filename)``.  HTML is sanitised and carries no scripts (see :mod:`olcf.notebooks.htmlexport`)."""
        _, defn = self.version_definition(row, version)
        d = normalise_definition(defn)
        nb = d["nbformat"]
        base = _safe_filename(row["name"])
        f = (fmt or "ipynb").lower()
        if f in ("fabric_git_source", "fabric-git"):
            f = "fabric"
        if f == "ipynb":
            return formats.to_ipynb_text(nb, strip=not outputs).encode(), "application/x-ipynb+json", f"{base}.ipynb"
        if f == "py":
            return formats.to_py_percent(nb).encode(), "text/x-python; charset=utf-8", f"{base}.py"
        if f == "fabric":
            return formats.to_fabric_git(nb).encode(), "text/x-python; charset=utf-8", "notebook-content.py"
        if f == "html":
            return htmlexport.render_html(nb, row["name"], outputs=outputs).encode(), "text/html; charset=utf-8", f"{base}.html"
        raise Unsupported(f"unsupported export format '{fmt}'", remediation="use ipynb, py, fabric or html")

    # ------------------------------------------------------------------ import
    def read_import_files(self, files: list[tuple[str, bytes]], *, fmt: str | None = None) -> list[ImportItem]:
        """Parse uploaded files (and zip archives of them) into :class:`ImportItem`; every problem names the offending file."""
        total = 0
        found: list[tuple[str, bytes, dict[str, Any]]] = []  # (display name, content, platform metadata)
        for fname, data in files:
            total += len(data)
            if total > MAX_IMPORT_BYTES:
                raise BadRequest("the upload is too large")
            if len(data) > MAX_FILE_BYTES:
                raise BadRequest(f"'{fname}' is larger than {MAX_FILE_BYTES // 1024 // 1024} MiB")
            if fname.lower().endswith(".zip") or data[:4] == b"PK\x03\x04":
                found.extend(self._read_zip(fname, data))
            else:
                found.append((_stem(fname), data, {}))
        if not found:
            raise BadRequest("no notebook files were found in the upload")
        if len(found) > MAX_IMPORT_FILES:
            raise BadRequest(f"at most {MAX_IMPORT_FILES} notebooks can be imported at once")
        out: list[ImportItem] = []
        for name, data, plat in found:
            warnings: list[str] = []
            try:
                detected = fmt if fmt and fmt != "auto" else formats.sniff_format(data, plat.get("_filename") or name)
                nb = formats.normalize_notebook(formats.parse_notebook(data, detected, warnings=warnings))
            except (ValidationFailed, Unsupported, BadRequest) as exc:
                raise ValidationFailed(f"cannot import '{name}': {exc.message}", details={"file": name, **exc.details}, remediation=exc.remediation) from exc
            settings: dict[str, Any] = {}
            display = str(plat.get("displayName") or name)[:128]
            if plat.get("description"):
                warnings.append("description from .platform ignored")
            lang = ((nb.get("metadata") or {}).get("kernelspec") or {}).get("language")
            if lang and lang not in ("python",):
                warnings.append(f"kernel language '{lang}' is not Python; non-Python cells cannot run on OLCF runtimes")
            out.append(ImportItem(name=display, nb=nb, format=detected, source=name, settings=settings, warnings=warnings))
        return out

    def _read_zip(self, zname: str, data: bytes) -> list[tuple[str, bytes, dict[str, Any]]]:
        try:
            z = zipfile.ZipFile(io.BytesIO(data))
        except zipfile.BadZipFile as exc:
            raise BadRequest(f"'{zname}' is not a valid zip archive") from exc
        infos = z.infolist()
        if len(infos) > MAX_IMPORT_FILES * 4:
            raise BadRequest("the archive contains too many files")
        if sum(i.file_size for i in infos) > MAX_IMPORT_BYTES:
            raise BadRequest("the archive is too large when expanded")
        names = {i.filename for i in infos}
        for n in names:
            parts = n.split("/")
            if n.startswith("/") or "\\" in n or ".." in parts or "\x00" in n or (parts and re.match(r"^[A-Za-z]:", parts[0])):
                raise BadRequest(f"unsafe path in archive: {n[:100]!r}")
        out: list[tuple[str, bytes, dict[str, Any]]] = []
        for i in infos:
            if i.is_dir():
                continue
            p = PurePosixPath(i.filename)
            base = p.name
            if base.startswith(".") or "__MACOSX" in p.parts:
                continue
            fabric_dir = base == "notebook-content.py" or base == "notebook-content.ipynb" or base == "notebook-content.sql"
            if not (base.lower().endswith((".ipynb", ".py")) or fabric_dir):
                continue
            if i.file_size > MAX_FILE_BYTES:
                raise BadRequest(f"'{i.filename}' is larger than {MAX_FILE_BYTES // 1024 // 1024} MiB")
            with z.open(i) as fh:
                content = fh.read(MAX_FILE_BYTES + 1)  # bounded read: never trust the declared size
            if len(content) > MAX_FILE_BYTES:
                raise BadRequest(f"'{i.filename}' expands beyond the size limit")
            plat: dict[str, Any] = {"_filename": base}
            name = _stem(base)
            if fabric_dir:
                parent = p.parent.name
                name = re.sub(r"\.Notebook$", "", parent) or _stem(base)
                pf = str(p.parent / ".platform")
                if pf in names:
                    try:
                        meta = json.loads(z.read(pf)).get("metadata") or {}
                        plat.update({k: meta[k] for k in ("displayName", "description") if isinstance(meta.get(k), str)})
                    except (ValueError, AttributeError):
                        pass
            out.append((name, content, plat))
        return out

    def import_notebooks(self, principal: Principal, workspace_id: str, items: list[ImportItem], *, on_conflict: str = "rename", folder: str = "",
                         strip_outputs: bool = False, tags: list[str] | None = None) -> dict[str, Any]:
        """Create (or skip / overwrite) one notebook item per :class:`ImportItem`."""
        if on_conflict not in ("rename", "skip", "overwrite"):
            raise BadRequest("on_conflict must be rename, skip or overwrite")
        svc = self.ctx.service("items")
        created: list[dict[str, Any]] = []
        for it in items:
            nb = formats.normalize_notebook(it.nb, strip_outputs=strip_outputs)
            definition = {"nbformat": nb, **({"settings": it.settings} if it.settings else {})}
            existing = svc.find(workspace_id, "notebook", it.name)
            rec: dict[str, Any] = {"name": it.name, "source": it.source, "format": it.format, "warnings": it.warnings}
            if existing and on_conflict == "skip":
                rec.update(action="skipped", id=existing["id"])
            elif existing and on_conflict == "overwrite":
                principal.require("item.update", workspace_id=workspace_id, item_id=existing["id"])
                keep = (existing.get("definition") or {}).get("settings")
                row = svc.update(principal, existing["id"], ItemUpdate(definition={**definition, **({"settings": keep} if keep and not it.settings else {})}, comment="import"), None)
                rec.update(action="overwritten", id=row["id"], version=row["version"])
            else:
                name = it.name
                if existing:
                    k = 2
                    while svc.find(workspace_id, "notebook", f"{it.name} ({k})"):
                        k += 1
                    name = f"{it.name} ({k})"
                row = svc.create(principal, workspace_id, ItemCreate(type="notebook", name=name, folder=folder, tags=tags or [], definition=definition))  # type: ignore[arg-type]
                rec.update(action="created", id=row["id"], name=name, version=row["version"])
            created.append(rec)
        return {"workspace_id": workspace_id, "items": created, "created": sum(1 for r in created if r["action"] == "created"),
                "overwritten": sum(1 for r in created if r["action"] == "overwritten"), "skipped": sum(1 for r in created if r["action"] == "skipped")}

    # ------------------------------------------------------------------ %pip policy
    def pip_commands(self, nb: dict[str, Any]) -> list[dict[str, Any]]:
        """Every notebook-time package installation (``%pip install``, ``!pip install`` ...) as ``{cell_id, line, command, requirements}``."""
        found: list[dict[str, Any]] = []
        for c in nb["cells"]:
            if c["cell_type"] != "code":
                continue
            for n, line in enumerate(c["source"].split("\n"), start=1):
                m = _PIP_LINE.match(line)
                if not m:
                    continue
                words = _shell_words(m.group("rest"))
                if not words or words[0] not in ("install", "download", "wheel"):
                    found.append({"cell_id": c["id"], "line": n, "command": line.strip(), "requirements": [], "kind": words[0] if words else ""})
                    continue
                reqs = [w for w in words[1:] if not w.startswith("-") and not w.startswith("/") and "://" not in w]
                opts = [w for w in words[1:] if w.startswith("-")]
                direct = [w for w in words[1:] if "://" in w or w.startswith("/") or w.startswith(".")]
                found.append({"cell_id": c["id"], "line": n, "command": line.strip(), "requirements": reqs, "kind": "install", "options": opts, "direct": direct})
        return found

    def effective_pip_policy(self, nb_settings: dict[str, Any], env_policy: str | None) -> str:
        """notebook setting > environment setting > admin default (``declared``), capped by the admin maximum."""
        settings = self.ctx.maybe_service("settings")
        default = (settings.get("environments.pip_policy_default") if settings else None) or "declared"
        cap = (settings.get("environments.pip_policy_max") if settings else None) or "allow"
        want = nb_settings.get("pip_policy") or env_policy or default
        want = want if want in PIP_ORDER else "declared"
        cap = cap if cap in PIP_ORDER else "allow"
        return want if PIP_ORDER[want] <= PIP_ORDER[cap] else cap

    def check_pip_policy(self, nb: dict[str, Any], policy: str, declared: list[str] | None, *, where: str = "notebook") -> list[dict[str, Any]]:
        """Fail closed before a job exists: ``block`` rejects any install, ``declared`` allows requirements the environment declares, ``allow`` all."""
        cmds = [c for c in self.pip_commands(nb) if c.get("kind") == "install" or c.get("kind") not in ("freeze", "list", "show", "check")]
        if not cmds or policy == "allow":
            return cmds
        from packaging.requirements import InvalidRequirement, Requirement
        from packaging.utils import canonicalize_name

        have: dict[str, Any] = {}
        for r in declared or []:
            try:
                q = Requirement(r)
                have[canonicalize_name(q.name)] = q
            except InvalidRequirement:
                continue
        problems = []
        for c in cmds:
            if c.get("kind") != "install":
                problems.append({**c, "reason": f"'pip {c.get('kind')}' is not allowed"})
                continue
            if policy == "block":
                problems.append({**c, "reason": "notebook-time package installation is blocked (pip_policy=block)"})
                continue
            if c.get("direct") or not c["requirements"]:
                problems.append({**c, "reason": "only plain package names declared in the environment can be installed"})
                continue
            for r in c["requirements"]:
                try:
                    q = Requirement(r)
                except InvalidRequirement:
                    problems.append({**c, "reason": f"invalid requirement '{r[:60]}'"})
                    break
                d = have.get(canonicalize_name(q.name))
                if d is None:
                    problems.append({**c, "reason": f"'{q.name}' is not declared in the environment"})
                    break
        if problems:
            first = problems[0]
            raise PolicyDenied(f"{where}: cell {first['cell_id']} line {first['line']}: {first['reason']}",
                               details={"policy": policy, "violations": redact_obj(problems[:20])},
                               remediation="declare the package in the Environment item and publish it, or ask an administrator to allow notebook installs")
        return cmds

    # ------------------------------------------------------------------ execution
    def _items_by_ref(self, workspace_id: str, ref: str, type_: str) -> dict[str, Any]:
        return self.ctx.service("items").resolve(workspace_id, ref, type_)

    def build_submission(self, principal: Principal, row: dict[str, Any], body: RunRequest) -> tuple[JobSubmission, dict[str, Any]]:
        """Everything the job needs, resolved from the notebook settings; returns ``(submission, notes)``."""
        ws = row["workspace_id"]
        d = normalise_definition(row["definition"])
        nb, settings = d["nbformat"], d.get("settings") or {}
        notes: dict[str, Any] = {}
        # --- environment ------------------------------------------------------------------------------------
        env_ref = body.environment_id or settings.get("environment_id")
        env_spec: dict[str, Any] | None = None
        env_id: str | None = None
        if env_ref:
            envs = self.ctx.service("environments")
            env_item = self._items_by_ref(ws, env_ref, "environment")
            principal.require("item.read", workspace_id=ws, item_id=env_item["id"])
            env_id = env_item["id"]
            env_spec = envs.resolve(env_id, workspace_id=ws)
            notes["environment"] = {"id": env_id, "name": env_item["name"], "lock_hash": env_spec.get("lock_hash") if isinstance(env_spec, dict) else None}
        # --- %pip policy ------------------------------------------------------------------------------------
        declared = (env_spec or {}).get("requirements") if isinstance(env_spec, dict) else None
        pol = self.effective_pip_policy(settings, (env_spec or {}).get("pip_policy") if isinstance(env_spec, dict) else None)
        self.check_pip_policy(nb, pol, declared)
        notes["pip_policy"] = pol
        # --- parameters + variable set ---------------------------------------------------------------------------
        det = params.detect_parameters(nb)
        values = dict(body.parameters)
        env_vars: dict[str, str] = dict((env_spec or {}).get("variables") or {}) if isinstance(env_spec, dict) else {}
        secret_refs: dict[str, str] = {}
        vs_ref = body.variable_set or settings.get("variable_set")
        if vs_ref:
            vs = self.ctx.service("variables")
            res = vs.resolve(vs_ref, stage=body.stage, workspace_id=ws, principal=principal)
            declared_names = {p.name for p in det.parameters}
            for name, v in res["values"].items():
                if name in declared_names and name not in values:
                    values[name] = v
            for name, sid in res["secrets"].items():
                secret_refs[name] = sid
            notes["variable_set"] = {"id": res["id"], "name": res["name"], "stage": res["stage"]}
        coerced = params.coerce_parameters(det.parameters, values, allow_undeclared=True, require=True)
        # --- RuntimeRequest / resources / policy / lakehouse (request > settings), exactly like the controller's rules ---------------
        runtime_d = {**(settings.get("runtime") or {}), **(body.runtime or {})}
        if env_id:
            runtime_d["environment_id"] = env_id
        try:
            runtime = RuntimeRequest.model_validate(runtime_d)
            resources = ResourceRequest.model_validate({**(settings.get("resources") or {}), **(body.resources or {})})
            pdict = {**(settings.get("policy") or {}), **(body.policy or {})}
            if settings.get("timeout_seconds") and "timeout_seconds" not in pdict:
                pdict["timeout_seconds"] = settings["timeout_seconds"]
            if "allow_pip_install" not in pdict:  # the resolved %pip policy decides what the *node* lets a notebook install (declared packages come from the layer)
                pdict["allow_pip_install"] = pol == "allow"
            policy = PolicySpec.model_validate(pdict)
            lake_src = body.default_lakehouse if body.default_lakehouse is not None else settings.get("default_lakehouse")
            lake = None
            if lake_src:
                ld = dict(lake_src)
                if "id" in ld:
                    ld["lakehouse_id"] = ld.pop("id")
                lake = LakehouseRef.model_validate(ld)
        except Exception as exc:  # noqa: BLE001
            raise ValidationFailed(f"invalid run request: {' '.join(str(exc).splitlines()[:3])[:300]}") from exc
        if lake is not None and lake.lakehouse_id is None and lake.name and lake.workspace_id is None:
            lake = lake.model_copy(update={"workspace_id": ws})
        engine = body.engine or settings.get("engine") or "spark"
        sub = JobSubmission(
            kind=JobKind.NOTEBOOK, workspace_id=ws, name=body.name or row["name"], item_id=row["id"], parameters=coerced, default_lakehouse=lake,
            runtime=runtime, resources=resources, policy=policy, origin=Origin(kind=OriginKind.API, details={"notebook": row["name"], "notebook_version": row["version"]}),
            idempotency_key=body.idempotency_key, engine=engine, env={k: v for k, v in env_vars.items()}, secret_refs=secret_refs, tags=dict(body.tags))
        return sub, notes

    def run(self, principal: Principal, row: dict[str, Any], body: RunRequest) -> dict[str, Any]:
        principal.require("job.submit", workspace_id=row["workspace_id"])
        sub, notes = self.build_submission(principal, row, body)
        job = self.ctx.service("jobs").submit(principal, sub)
        self.ctx.audit.record(principal, "notebook.run", resource_type="notebook", resource_id=row["id"], workspace_id=row["workspace_id"],
                              details={"job_id": job.id, "parameters": sorted(sub.parameters), "environment": (notes.get("environment") or {}).get("id")})
        rec = job.model_dump(mode="json")
        rec["notes"] = notes
        return redact_obj(rec)

    def start_session(self, principal: Principal, row: dict[str, Any], body: SessionRequest) -> dict[str, Any]:
        from olcf.compute.sessions import SessionCreate

        principal.require("session.use", workspace_id=row["workspace_id"])
        kw: dict[str, Any] = {"workspace_id": row["workspace_id"], "notebook_id": row["id"], "placement": body.placement, "eager_spark": body.eager_spark}
        for k in ("engine", "name", "idle_timeout_seconds"):
            if getattr(body, k) is not None:
                kw[k] = getattr(body, k)
        try:
            if body.runtime is not None:
                kw["runtime"] = RuntimeRequest.model_validate(body.runtime)
            if body.resources is not None:
                kw["resources"] = ResourceRequest.model_validate(body.resources)
            if body.default_lakehouse is not None:
                ld = dict(body.default_lakehouse)
                if "id" in ld:
                    ld["lakehouse_id"] = ld.pop("id")
                kw["default_lakehouse"] = LakehouseRef.model_validate(ld)
        except Exception as exc:  # noqa: BLE001
            raise ValidationFailed(f"invalid session request: {' '.join(str(exc).splitlines()[:3])[:300]}") from exc
        # the notebook's environment is validated here (same workspace, readable) before the controller resolves it
        d = normalise_definition(row["definition"])
        env_ref = (d.get("settings") or {}).get("environment_id")
        env_policy = None
        if env_ref:
            env_item = self._items_by_ref(row["workspace_id"], env_ref, "environment")
            principal.require("item.read", workspace_id=row["workspace_id"], item_id=env_item["id"])
            kw["environment_id"] = env_item["id"]
            env_spec = self.ctx.service("environments").resolve(env_item["id"], workspace_id=row["workspace_id"])
            env_policy = env_spec.get("pip_policy") if isinstance(env_spec, dict) else None
        # the resolved %pip policy (notebook > environment > admin default, capped by the admin maximum) is what the kernel enforces
        kw["allow_pip_install"] = self.effective_pip_policy(d.get("settings") or {}, env_policy) == "allow"
        sess = self.ctx.service("sessions").create(principal, SessionCreate(**kw))
        self.ctx.audit.record(principal, "notebook.session", resource_type="notebook", resource_id=row["id"], workspace_id=row["workspace_id"], details={"session_id": sess.get("id") if isinstance(sess, dict) else None})
        return redact_obj(sess) if isinstance(sess, dict) else sess

    def runs(self, principal: Principal, row: dict[str, Any], *, limit: int, cursor: str | None, state: list[str] | None = None) -> dict[str, Any]:
        jobs = self.ctx.service("jobs")
        rows, nxt = jobs.list(principal, workspace_id=row["workspace_id"], item_id=row["id"], state=state, limit=limit, cursor=cursor)
        return {"items": [jobs.public(r) for r in rows], "next_cursor": nxt}

    # ------------------------------------------------------------------ health
    def health(self) -> dict[str, Any]:
        with self.ctx.db.connect() as conn:
            n = len(fetch_all(conn, select(S.items.c.id).where(S.items.c.type == "notebook", S.items.c.deleted_at.is_(None)).limit(1)))
        import nbformat

        return {"notebooks_present": bool(n), "nbformat": nbformat.__version__}


# ============================================================================== helpers
def _shell_words(text: str) -> list[str]:
    """Split a ``%pip`` / ``!pip`` argument string like a shell would (quotes honoured); falls back to whitespace splitting on unbalanced quotes."""
    try:
        return shlex.split(text, comments=True)
    except ValueError:
        return text.split()


def _stem(filename: str) -> str:
    base = PurePosixPath(filename.replace("\\", "/")).name
    stem = re.sub(r"\.(ipynb|py|zip|sql)$", "", base, flags=re.I)
    stem = re.sub(r"[\x00-\x1f]", "", stem).strip() or "notebook"
    return stem[:128]


def _safe_filename(name: str) -> str:
    return re.sub(r"[^A-Za-z0-9._-]+", "_", name).strip("._") or "notebook"


def redact(text: str) -> str:
    return redact_text(text)


__all__ = ["ContentPut", "ImportItem", "NotebookService", "RunRequest", "SessionRequest", "normalise_definition", "normalise_settings"]
