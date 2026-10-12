# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Path, URI and name handling shared by every storage provider.

Why this module exists: *every* file path that reaches a provider is derived from user input (REST query
parameters, notebook cells, SQL ``read_parquet('...')`` literals, MCP tool arguments).  Centralising the
traversal defence here means a provider can rely on receiving a normalised, root-relative path and never
has to re-implement the rules.  Rules (fail closed, never "fix up"):

* paths are *relative* to the lakehouse root and use ``/`` separators;
* ``..`` segments, absolute paths, backslashes, drive letters, NUL / control characters are rejected;
* ``.`` segments and duplicate slashes are collapsed;
* canonical ``olcf://<workspace>/<lakehouse>/<Files|Tables>/<path>`` URIs are percent-encoded per segment.
"""

from __future__ import annotations

import posixpath
import re
from dataclasses import dataclass
from urllib.parse import quote, unquote, urlparse

from olcf.core.errors import BadRequest

AREAS = ("Files", "Tables")
MAX_PATH_LEN = 1024
MAX_SEGMENT_LEN = 255
_DRIVE = re.compile(r"^[A-Za-z]:")
_CTRL = re.compile(r"[\x00-\x1f\x7f]")
_TABLE_NAME = re.compile(r"^[A-Za-z_][A-Za-z0-9_]{0,127}$")


def normalize_rel(path: str | None, *, what: str = "path") -> str:
    """Return a clean root-relative path (``""`` is the root) or raise :class:`BadRequest`.

    >>> normalize_rel("./Files//raw/a.csv")
    'Files/raw/a.csv'
    """
    if path is None:
        return ""
    if not isinstance(path, str):
        raise BadRequest(f"{what} must be a string")
    if _CTRL.search(path):
        raise BadRequest(f"{what} contains control characters")
    if "\\" in path:
        raise BadRequest(f"{what} must use '/' separators (backslash is not allowed)")
    if path.startswith("/") or _DRIVE.match(path):
        raise BadRequest(f"{what} must be relative to the lakehouse root, got an absolute path",
                         remediation="use paths such as 'Files/raw/data.csv'")
    if len(path) > MAX_PATH_LEN:
        raise BadRequest(f"{what} is too long")
    parts: list[str] = []
    for seg in path.split("/"):
        if seg in ("", "."):
            continue
        if seg == "..":
            raise BadRequest(f"{what} must not contain '..' segments")
        if len(seg) > MAX_SEGMENT_LEN:
            raise BadRequest(f"{what} has a segment that is too long")
        if _DRIVE.match(seg) and not parts:
            raise BadRequest(f"{what} must not start with a drive letter")
        parts.append(seg)
    return "/".join(parts)


def split_area(rel: str) -> tuple[str, str]:
    """Split a normalised relative path into ``(area, remainder)``; ``area`` is ``""`` for the root."""
    if not rel:
        return "", ""
    head, _, rest = rel.partition("/")
    return head, rest


def require_area(rel: str, areas: tuple[str, ...] = ("Files",), *, what: str = "path") -> str:
    """Ensure ``rel`` lies inside one of ``areas`` (e.g. mutating file operations are limited to ``Files``)."""
    head, _ = split_area(rel)
    if head not in areas:
        raise BadRequest(f"{what} must be inside {' or '.join(areas)}/ (got '{rel or '/'}')",
                         remediation="Tables/ is managed through the table APIs; use Files/ for unmanaged files")
    return rel


def join_rel(*parts: str) -> str:
    return normalize_rel("/".join(p for p in parts if p))


def validate_table_name(name: str, *, schema_enabled: bool = False) -> str:
    """Table names are identifiers; ``schema.table`` is accepted only for schema-enabled lakehouses."""
    segs = name.split(".")
    if len(segs) > (2 if schema_enabled else 1) or not all(_TABLE_NAME.match(s) for s in segs):
        raise BadRequest(f"invalid table name '{name}'",
                         remediation="use letters, digits and underscores, starting with a letter or underscore"
                                     + ("; 'schema.table' is allowed" if schema_enabled else ""))
    return name


def table_rel_path(name: str, *, schema_enabled: bool = False) -> str:
    validate_table_name(name, schema_enabled=schema_enabled)
    return "Tables/" + name.replace(".", "/")


def slug(name: str) -> str:
    """Filesystem-safe directory name for default local roots (spaces and punctuation become ``_``)."""
    s = re.sub(r"[^A-Za-z0-9._-]+", "_", name).strip("._")
    return s or "unnamed"


# ----------------------------------------------------------------------------- canonical URIs
@dataclass(frozen=True)
class CanonicalRef:
    """A parsed ``olcf://`` reference (``lakehouse`` may be ``None`` for relative forms)."""

    workspace: str | None
    lakehouse: str | None
    area: str
    path: str  # path below the area ("" for the area itself)

    @property
    def rel(self) -> str:
        return f"{self.area}/{self.path}".rstrip("/") if self.area else ""


def canonical_uri(workspace: str, lakehouse: str, rel: str = "") -> str:
    base = f"olcf://{quote(workspace, safe='')}/{quote(lakehouse, safe='')}"
    rel = normalize_rel(rel)
    return base + ("/" + "/".join(quote(s, safe="") for s in rel.split("/")) if rel else "")


def parse_canonical(uri: str) -> CanonicalRef:
    """Parse ``olcf://<workspace>/<lakehouse>/<Files|Tables>/<path>`` or a bare ``Files/...`` / ``Tables/...`` path."""
    if uri.startswith("olcf://"):
        u = urlparse(uri)
        ws = unquote(u.netloc)
        segs = [unquote(s) for s in u.path.split("/") if s != ""]
        if not ws or not segs:
            raise BadRequest(f"invalid olcf URI '{uri}'", remediation="expected olcf://<workspace>/<lakehouse>/Files/<path>")
        lh, rest = segs[0], segs[1:]
        rel = normalize_rel("/".join(rest), what="uri path")
        area, path = split_area(rel)
        if area and area not in AREAS:
            raise BadRequest(f"unknown area '{area}' in '{uri}' (expected Files or Tables)")
        return CanonicalRef(ws, lh, area, path)
    rel = normalize_rel(uri, what="path")
    area, path = split_area(rel)
    if area not in AREAS:
        raise BadRequest(f"'{uri}' is neither an olcf:// URI nor a Files/Tables relative path")
    return CanonicalRef(None, None, area, path)


def posix_join(base: str, rel: str) -> str:
    return posixpath.join(base.rstrip("/"), rel) if rel else base.rstrip("/")
