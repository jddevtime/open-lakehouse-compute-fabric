# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Shared vocabulary and helpers of the governance package: constants, validation, the :class:`Base` every manager derives from.

``Base`` gives a manager access to the platform context (database, audit log, event bus), the Intelligence service (asset visibility rules) and the other governance managers
through ``self.gov``; it also holds the small authorisation helpers so the rules of the contract (``docs/dev/GOVERNANCE_API.md`` section 0) are written once:

* an asset outside the caller's ``intelligence.read`` scope does not exist for the caller (``NotFound``);
* org-level objects (glossary, domains, products, classification catalog, policies) are readable by anyone who can read assets anywhere and written with an org-wide permission;
* every governance write records an audit action and, where the contract says so, publishes a platform event (best effort - a failing bus never fails a write).
"""

from __future__ import annotations

import logging
import re
from collections.abc import Iterable, Iterator, Sequence
from typing import TYPE_CHECKING, Any

from sqlalchemy import select

from olcf.core.errors import BadRequest, NotFound, PolicyDenied, PreconditionFailed
from olcf.core.timeutil import now_iso
from olcf.db.util import fetch_all
from olcf.intelligence import settings as rt
from olcf.security.rbac import Principal

if TYPE_CHECKING:  # pragma: no cover
    from olcf.intelligence.governance.service import GovernanceService

log = logging.getLogger("olcf.intelligence.governance")

#: Asset types that hold data or present it: the types governance coverage numbers and the default policies are about.
DATASET_ASSET_TYPES: tuple[str, ...] = ("table", "view", "model_table")
GOVERNED_ASSET_TYPES: tuple[str, ...] = ("table", "view", "model_table", "semantic_model", "report", "dashboard", "measure")
PRODUCT_MEMBER_TYPES: tuple[str, ...] = ("table", "view", "model_table", "measure", "report", "semantic_model", "dashboard")
#: Types that can receive a classification.
CLASSIFIABLE_TYPES: tuple[str, ...] = ("table", "view", "model_table", "column", "measure", "semantic_model", "report", "dashboard", "visual", "page", "file", "lakehouse",
                                       "warehouse", "schema", "source_system", "external", "dataflow")
SEVERITIES: tuple[str, ...] = ("low", "medium", "high", "critical")
CERT_STATES: tuple[str, ...] = ("none", "draft", "certified", "deprecated")
OWNER_ROLES: tuple[str, ...] = ("owner", "steward", "custodian", "contact")
SUBJECT_TYPES: tuple[str, ...] = ("asset", "domain", "product", "term")
MAX_CHUNK = 300


def chunks(seq: Sequence[Any], n: int = MAX_CHUNK) -> Iterator[list[Any]]:
    """Yield ``seq`` in lists of at most ``n`` items (IN-list sizes stay portable)."""
    for i in range(0, len(seq), n):
        yield list(seq[i:i + n])


def like_escape(text: str) -> str:
    """``%text%`` with LIKE wildcards in ``text`` escaped (use ``escape="\\\\"``)."""
    return "%" + text.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_") + "%"


def name_key(name: str) -> str:
    """The case-insensitive identity of a name (uniqueness and look-ups)."""
    return " ".join(name.split()).lower()


def clean_name(value: Any, what: str = "name", *, max_len: int = 255) -> str:
    """A trimmed, whitespace-collapsed, non-empty name of at most ``max_len`` characters."""
    if not isinstance(value, str) or not value.strip():
        raise BadRequest(f"{what} must be a non-empty string")
    out = " ".join(value.split())
    if len(out) > max_len:
        raise BadRequest(f"{what} is limited to {max_len} characters")
    return out


def clean_text(value: Any, what: str, *, max_len: int = 8000, allow_none: bool = True) -> str | None:
    """A text field (Markdown allowed): ``None`` passes when ``allow_none``; otherwise a string of at most ``max_len`` characters."""
    if value is None:
        if allow_none:
            return None
        raise BadRequest(f"{what} is required")
    if not isinstance(value, str):
        raise BadRequest(f"{what} must be a string")
    if len(value) > max_len:
        raise BadRequest(f"{what} is limited to {max_len} characters")
    return value


def clean_str_list(value: Any, what: str, *, max_items: int = 32, max_len: int = 128, lower: bool = False) -> list[str]:
    """A de-duplicated list of short strings (order kept)."""
    if value is None:
        return []
    if not isinstance(value, list) or not all(isinstance(x, str) for x in value):
        raise BadRequest(f"{what} must be a list of strings")
    out: list[str] = []
    for x in value:
        s = " ".join(x.split())
        if not s:
            continue
        if len(s) > max_len:
            raise BadRequest(f"each entry of {what} is limited to {max_len} characters")
        s = s.lower() if lower else s
        if s.lower() not in {o.lower() for o in out}:
            out.append(s)
    if len(out) > max_items:
        raise BadRequest(f"{what} is limited to {max_items} entries")
    return out


_COLOR = re.compile(r"^#[0-9a-fA-F]{6}$")


def clean_color(value: Any) -> str | None:
    """``#rrggbb`` or ``None``."""
    if value in (None, ""):
        return None
    if not isinstance(value, str) or not _COLOR.match(value):
        raise BadRequest("color must look like #1a2b3c")
    return value.lower()


def compact_asset(row: dict[str, Any]) -> dict[str, Any]:
    """``AssetRef``: the small form of an asset used inside governance answers."""
    return {"id": row["id"], "type": row["type"], "name": row["name"], "path": row.get("path"), "workspace_id": row.get("workspace_id")}


HIDDEN: dict[str, Any] = {"hidden": True}


class Base:
    """Common plumbing of the governance managers (see the module docstring)."""

    def __init__(self, gov: GovernanceService) -> None:
        self.gov = gov
        self.ctx = gov.ctx
        self.db = gov.db
        self.intel = gov.intel

    # ------------------------------------------------------------------------------------------------------------------------ settings
    def setting(self, key: str, default: Any) -> Any:
        """Current value of a ``governance.*`` runtime setting."""
        return rt.get(self.ctx, key, default)

    # ------------------------------------------------------------------------------------------------------------------------ audit and events
    def audit(self, principal: Principal | None, action: str, *, resource_type: str, resource_id: str, workspace_id: str | None = None, details: dict[str, Any] | None = None,
              outcome: str = "success") -> None:
        """Record an audit action (``principal`` ``None`` = the system acting on its own, for example the post-scan processing)."""
        try:
            self.ctx.audit.record(principal, action, resource_type=resource_type, resource_id=resource_id, workspace_id=workspace_id, details=details or {}, outcome=outcome)
        except Exception:  # noqa: BLE001 - an audit hiccup never undoes a governance decision that is already stored
            log.exception("could not record audit action %s", action)

    def emit(self, type_: str, payload: dict[str, Any], *, workspace_id: str | None = None, subject: str | None = None) -> None:
        """Publish a platform event, best effort."""
        try:
            self.ctx.events.publish(type_, payload, workspace_id=workspace_id, subject=subject)
        except Exception:  # noqa: BLE001
            log.exception("could not publish event %s", type_)

    # ------------------------------------------------------------------------------------------------------------------------ authorisation
    def org_reader(self, principal: Principal) -> None:
        """Org-level objects are readable by anyone with ``intelligence.read`` org-wide or in at least one workspace (``Forbidden`` otherwise)."""
        self.intel.scope(principal)

    def org_curator(self, principal: Principal) -> None:
        """Org-wide ``governance.curate`` (writes of glossary, domains, products, the classification catalog)."""
        self.org_reader(principal)
        principal.require("governance.curate")

    def org_approver(self, principal: Principal) -> None:
        """Org-wide ``governance.approve`` (policies, hard deletes, approving org-level changes)."""
        self.org_reader(principal)
        principal.require("governance.approve")

    def can(self, principal: Principal, perm: str, workspace_id: str | None) -> bool:
        """True if ``principal`` holds ``perm`` for ``workspace_id`` (``None`` = org-wide only)."""
        return principal.can(perm, workspace_id=workspace_id)

    def asset(self, principal: Principal, asset_id: str) -> dict[str, Any]:
        """The asset row if visible (a merged-away id leads to the survivor); ``NotFound`` otherwise."""
        return self.intel.row_for(principal, asset_id)

    def curate_asset(self, principal: Principal, asset_id: str) -> dict[str, Any]:
        """A visible asset the caller may curate (``governance.curate`` in its workspace)."""
        row = self.asset(principal, asset_id)
        principal.require("governance.curate", workspace_id=row["workspace_id"])
        return row

    def visible_rows(self, principal: Principal, ids: Iterable[str]) -> dict[str, dict[str, Any]]:
        """``{id: row}`` for the ids the caller can see (unknown or hidden ids are left out)."""
        ids = list(dict.fromkeys(ids))
        if not ids:
            return {}
        rows = self.intel.graph.asset_rows(ids)
        return {i: r for i, r in rows.items() if self.intel.can_see(principal, r["workspace_id"])}

    def scope_ids(self, principal: Principal, workspace_id: str | None = None) -> Any:
        """A sub-select of the asset ids the caller can see (optionally one workspace), for ``IN`` filters on governance tables."""
        from olcf.intelligence.schema import assets

        return self.intel.scope_filter(select(assets.c.id), principal, workspace_id)

    def ref(self, principal: Principal | None, row: dict[str, Any] | None) -> dict[str, Any]:
        """``AssetRef`` of a row, or ``{"hidden": true}`` when the caller cannot see it (``principal=None``: no filtering)."""
        if row is None:
            return dict(HIDDEN)
        if principal is not None and not self.intel.can_see(principal, row["workspace_id"]):
            return dict(HIDDEN)
        return compact_asset(row)

    # ------------------------------------------------------------------------------------------------------------------------ concurrency
    @staticmethod
    def check_etag(if_match: str | None, etag: str) -> None:
        """Honour ``If-Match`` (``*`` or the current etag)."""
        if if_match is None:
            return
        candidates = {x.strip().strip('"') for x in if_match.split(",")}
        if "*" not in candidates and etag not in candidates:
            raise PreconditionFailed("the resource was modified by someone else", details={"current_etag": etag})

    @staticmethod
    def denied(message: str, remediation: str | None = None) -> PolicyDenied:
        """A ``policy_denied`` error (a configured governance rule forbids the operation, as opposed to a missing permission)."""
        return PolicyDenied(message, remediation=remediation)

    # ------------------------------------------------------------------------------------------------------------------------ small reads
    def principal_names(self, ids: Iterable[str | None]) -> dict[str, str]:
        """``{principal id: display name}`` for the given ids (unknown ids map to themselves)."""
        from olcf.db import schema as S

        want = [i for i in dict.fromkeys(ids) if i]
        out = {i: i for i in want}
        if not want:
            return out
        with self.db.connect() as c:
            for part in chunks(want):
                for r in fetch_all(c, select(S.principals.c.id, S.principals.c.name, S.principals.c.display_name).where(S.principals.c.id.in_(part))):
                    out[r["id"]] = r["display_name"] or r["name"]
        return out


def expect_dict(value: Any, what: str) -> dict[str, Any]:
    """A JSON object or ``BadRequest``."""
    if not isinstance(value, dict):
        raise BadRequest(f"{what} must be an object")
    return value


def not_found(kind: str, ident: str) -> NotFound:
    """The uniform not-found error of governance objects."""
    return NotFound(f"{kind} '{ident}' does not exist")


def timestamp() -> str:
    """ISO-8601 UTC now."""
    return now_iso()


__all__ = ["CERT_STATES", "CLASSIFIABLE_TYPES", "DATASET_ASSET_TYPES", "GOVERNED_ASSET_TYPES", "HIDDEN", "OWNER_ROLES", "PRODUCT_MEMBER_TYPES", "SEVERITIES", "SUBJECT_TYPES", "Base",
           "chunks", "clean_color", "clean_name", "clean_str_list", "clean_text", "compact_asset", "expect_dict", "like_escape", "name_key", "not_found", "timestamp"]
