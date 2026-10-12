# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Asset selectors of policies: "which assets does this rule look at?".

A selector is ``{"types"?, "workspace_ids"?, "name_pattern"?, "tags"?, "domain"?, "outside_domain"?}``; **all given keys must match**. ``name_pattern`` is a case-insensitive glob
(``*``, ``?``). ``tags`` match the manual tags and the tags reported by the source (all listed tags must be present). ``domain`` / ``outside_domain`` use the effective domain
membership (direct, inherited from a parent asset, or through a data product, sub-domains included).

:func:`clean_selector` validates and normalises a selector when a policy is saved; :class:`Matcher` applies it to asset rows; :class:`Candidates` pages through the active assets
of the selected types in id order (bounded by ``governance.max_evaluation_assets``, reporting ``truncated`` instead of silently stopping).
"""

from __future__ import annotations

import fnmatch
from typing import Any

from sqlalchemy import func, or_, select

from olcf.core.errors import BadRequest
from olcf.db.util import fetch_all
from olcf.intelligence import model as M
from olcf.intelligence.governance.common import clean_str_list
from olcf.intelligence.schema import assets

SELECTOR_KEYS = ("types", "workspace_ids", "name_pattern", "tags", "domain", "outside_domain")
PAGE = 1000


def clean_selector(value: Any, *, allow_types: bool = True) -> dict[str, Any]:
    """Validate a selector object; returns the normalised form (unknown keys are refused)."""
    if value in (None, {}):
        return {}
    if not isinstance(value, dict):
        raise BadRequest("selector must be an object")
    extra = set(value) - set(SELECTOR_KEYS)
    if extra:
        raise BadRequest(f"unknown selector key(s) {sorted(extra)}", details={"supported": list(SELECTOR_KEYS)})
    out: dict[str, Any] = {}
    if value.get("types") is not None:
        types = clean_str_list(value["types"], "selector.types", max_items=20, max_len=32)
        bad = [t for t in types if t not in M.ASSET_TYPES]
        if bad:
            raise BadRequest(f"unknown asset type(s) {bad}", details={"supported": sorted(M.ASSET_TYPES)})
        if types:
            out["types"] = types
    if value.get("workspace_ids") is not None:
        ws = clean_str_list(value["workspace_ids"], "selector.workspace_ids", max_items=100, max_len=64)
        if ws:
            out["workspace_ids"] = ws
    if value.get("name_pattern") is not None:
        pat = value["name_pattern"]
        if not isinstance(pat, str) or not pat.strip() or len(pat) > 255:
            raise BadRequest("selector.name_pattern must be a non-empty string of at most 255 characters")
        out["name_pattern"] = pat.strip()
    if value.get("tags") is not None:
        tags = clean_str_list(value["tags"], "selector.tags", max_items=20, max_len=64, lower=True)
        if tags:
            out["tags"] = tags
    for key in ("domain", "outside_domain"):
        if value.get(key) is not None:
            if not isinstance(value[key], str) or not value[key].strip():
                raise BadRequest(f"selector.{key} must be a domain id or name")
            out[key] = value[key].strip()
    return out


def _literal(ch: str) -> str:
    return ch.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")


def glob_to_like(pattern: str) -> str:
    """A SQL ``LIKE`` pattern (escape ``\\``) that matches at least what the glob matches (``[...]`` classes become ``_``)."""
    out = []
    i = 0
    while i < len(pattern):
        ch = pattern[i]
        if ch == "*":
            out.append("%")
        elif ch == "?":
            out.append("_")
        elif ch == "[":
            j = pattern.find("]", i + 1)
            if j == -1:
                out.append("[")
            else:
                out.append("_")
                i = j
        else:
            out.append(_literal(ch))
        i += 1
    return "".join(out).lower()


def row_tags(row: dict[str, Any]) -> set[str]:
    """Manual tags plus tags reported by the source, lower-cased."""
    manual = row.get("tags") or []
    reported = (row.get("attrs") or {}).get("source_tags") or []
    return {str(t).lower() for t in (*manual, *reported) if t is not None}


class Matcher:
    """A compiled selector (``domain_sets`` are resolved once, by the caller, through the domain manager)."""

    def __init__(self, selector: dict[str, Any], default_types: tuple[str, ...], inside: set[str] | None = None, outside: set[str] | None = None, *, force_types: bool = False) -> None:
        self.selector = selector
        self.types = tuple(default_types) if force_types else tuple(selector.get("types") or default_types)
        self.workspaces = set(selector.get("workspace_ids") or [])
        self.pattern = (selector.get("name_pattern") or "").lower()
        self.tags = set(selector.get("tags") or [])
        self.inside = inside
        self.outside = outside

    def __call__(self, row: dict[str, Any]) -> bool:
        """True if the asset row satisfies the selector."""
        if row["type"] not in self.types or row["status"] != "active":
            return False
        if self.workspaces and row["workspace_id"] not in self.workspaces:
            return False
        if self.pattern and not fnmatch.fnmatchcase(row["name"].lower(), self.pattern):
            return False
        if self.tags and not self.tags <= row_tags(row):
            return False
        if self.inside is not None and row["id"] not in self.inside:
            return False
        return not (self.outside is not None and row["id"] in self.outside)


class Candidates:
    """Pages through the assets that match a :class:`Matcher` (plus optional extra name globs), in id order."""

    def __init__(self, db: Any, matcher: Matcher, *, name_globs: list[str] | None = None, limit: int = 20000) -> None:
        self.db = db
        self.matcher = matcher
        self.globs = [g.lower() for g in (name_globs or [])]
        self.limit = limit
        self.truncated = False

    def _iterate(self) -> Any:
        """Generator of lists of matching rows (each at most ``PAGE`` long)."""
        a = assets.c
        last = ""
        seen = 0
        pattern_sql = glob_to_like(self.matcher.pattern) if self.matcher.pattern else None
        while True:
            stmt = select(assets).where(a.status == "active", a.type.in_(list(self.matcher.types)), a.id > last)
            if self.matcher.workspaces:
                stmt = stmt.where(a.workspace_id.in_(sorted(self.matcher.workspaces)))
            if pattern_sql:
                stmt = stmt.where(func.lower(a.name).like(pattern_sql, escape="\\"))
            if self.globs:
                stmt = stmt.where(or_(*[func.lower(a.name).like(glob_to_like(g), escape="\\") for g in self.globs]))
            with self.db.connect() as c:
                rows = fetch_all(c, stmt.order_by(a.id).limit(PAGE))
            if not rows:
                return
            last = rows[-1]["id"]
            out = []
            for r in rows:
                if self.globs and not any(fnmatch.fnmatchcase(r["name"].lower(), g) for g in self.globs):
                    continue
                if not self.matcher(r):
                    continue
                if seen >= self.limit:
                    self.truncated = True
                    return
                seen += 1
                out.append(r)
            if out:
                yield out
            if len(rows) < PAGE:
                return

    def all(self) -> list[dict[str, Any]]:
        """Every matching row (bounded by ``limit``)."""
        out: list[dict[str, Any]] = []
        for page in self._iterate():
            out.extend(page)
        return out
