# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Sampling policy rows and their resolution: ``global`` / ``workspace`` / ``source`` / ``asset`` scopes, the narrowest row wins, no row means "never read values".

Split out of :mod:`~olcf.intelligence.discovery.sampling` so that the shared data-read policy service (:mod:`~olcf.intelligence.discovery.datapolicy`) and the sampling service can both
use it without importing each other. :mod:`~olcf.intelligence.discovery.sampling` re-exports every name here.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

from sqlalchemy import insert, select, update

from olcf.core.errors import BadRequest, NotFound, ValidationFailed
from olcf.core.ids import new_id
from olcf.core.timeutil import now_iso
from olcf.db.util import fetch_all, fetch_one, new_etag
from olcf.intelligence.discovery.schema import MASKING_MODES, MAX_SAMPLE_ROWS, sampling_policies

SCOPE_TYPES = ("global", "workspace", "source", "asset")

@dataclass(frozen=True)
class EffectivePolicy:
    """The policy that applies to one asset: the narrowest matching row, or "off" when none exists."""

    max_rows: int
    masking: str
    exclude_classified: bool
    scope: str                       # the scope_key of the row that decided, or "none"
    reason: str                      # why sampling is off, when it is

    @property
    def allowed(self) -> bool:
        """True when at least one row may be sampled."""
        return self.max_rows > 0

    def to_dict(self) -> dict[str, Any]:
        """JSON form (used in sample responses and the coverage report)."""
        return {"max_rows": self.max_rows, "masking": self.masking, "exclude_classified": self.exclude_classified, "scope": self.scope, "allowed": self.allowed,
                "reason": self.reason or None}


OFF = EffectivePolicy(0, "partial", True, "none", "no sampling policy exists")


def scope_key(scope_type: str, scope_id: str | None) -> str:
    """The ``scope_key`` column value: ``global`` or ``<type>:<id>``."""
    if scope_type == "global":
        return "global"
    if scope_type not in SCOPE_TYPES or not scope_id:
        raise ValidationFailed(f"a '{scope_type}' sampling policy needs scope_id", details={"scope_types": list(SCOPE_TYPES)})
    return f"{scope_type}:{scope_id}"


class SamplingPolicyStore:
    """CRUD and resolution of ``sampling_policies`` rows. There is no default row: an empty table means "never sample"."""

    def __init__(self, ctx: Any) -> None:
        self.ctx = ctx

    def list(self, *, scope_type: str | None = None, workspace_id: str | None = None, visible_workspaces: set[str] | None = None) -> list[dict[str, Any]]:
        """Policies, optionally one scope type or one workspace; ``visible_workspaces`` (``None`` = all) hides rows of workspaces the caller cannot see."""
        t = sampling_policies.c
        stmt = select(sampling_policies).order_by(t.scope_key)
        if scope_type:
            if scope_type not in SCOPE_TYPES:
                raise BadRequest(f"scope_type must be one of {list(SCOPE_TYPES)}")
            stmt = stmt.where(t.scope_key == "global") if scope_type == "global" else stmt.where(t.scope_key.like(f"{scope_type}:%"))
        if workspace_id:
            stmt = stmt.where(t.workspace_id == workspace_id)
        with self.ctx.db.connect() as c:
            rows = fetch_all(c, stmt)
        if visible_workspaces is not None:
            rows = [r for r in rows if r["workspace_id"] in visible_workspaces]
        return [self.public(r) for r in rows]

    @staticmethod
    def public(row: Mapping[str, Any]) -> dict[str, Any]:
        """API form of a policy row."""
        key = str(row["scope_key"])
        stype, _, sid = key.partition(":")
        return {"id": row["id"], "scope_type": stype, "scope_id": sid or None, "scope_key": key, "workspace_id": row.get("workspace_id"), "source_id": row.get("source_id"),
                "asset_id": row.get("asset_id"), "max_rows": row["max_rows"], "masking": row["masking"], "exclude_classified": bool(row["exclude_classified"]),
                "enabled": bool(row["enabled"]), "etag": row["etag"], "created_at": row["created_at"], "updated_at": row["updated_at"]}

    def put(self, principal: Any, *, scope_type: str, scope_id: str | None, max_rows: int, masking: str = "partial", exclude_classified: bool = True, enabled: bool = True,
            workspace_id: str | None = None) -> dict[str, Any]:
        """Create or replace the policy of one scope. ``workspace_id`` (for source / asset scopes: the workspace the object belongs to, if any) lets the API authorise per workspace."""
        if isinstance(max_rows, bool) or not isinstance(max_rows, int) or not 0 <= max_rows <= MAX_SAMPLE_ROWS:
            raise ValidationFailed(f"max_rows must be an integer between 0 and {MAX_SAMPLE_ROWS} (0 disables sampling)")
        if masking not in MASKING_MODES:
            raise ValidationFailed(f"masking must be one of {list(MASKING_MODES)}")
        key = scope_key(scope_type, scope_id)
        ws = scope_id if scope_type == "workspace" else workspace_id
        values = {"workspace_id": ws, "source_id": scope_id if scope_type == "source" else None, "asset_id": scope_id if scope_type == "asset" else None,
                  "max_rows": max_rows, "masking": masking, "exclude_classified": bool(exclude_classified), "enabled": bool(enabled)}
        now = now_iso()
        with self.ctx.db.begin() as c:
            cur = fetch_one(c, select(sampling_policies).where(sampling_policies.c.scope_key == key))
            if cur is None:
                c.execute(insert(sampling_policies).values(id=new_id("smp"), scope_key=key, etag=new_etag(), created_by=getattr(principal, "id", None), created_at=now, updated_at=now, **values))
            else:
                c.execute(update(sampling_policies).where(sampling_policies.c.id == cur["id"]).values(etag=new_etag(), updated_at=now, **values))
            row = fetch_one(c, select(sampling_policies).where(sampling_policies.c.scope_key == key))
        assert row is not None
        self.ctx.audit.record(principal, "config.changed", resource_type="sampling_policy", resource_id=row["id"], workspace_id=row["workspace_id"],
                              details={"scope": key, "max_rows": max_rows, "masking": masking, "exclude_classified": bool(exclude_classified), "enabled": bool(enabled)})
        return self.public(row)

    def get(self, policy_id: str) -> dict[str, Any]:
        """One policy row (internal form); ``NotFound`` if absent."""
        with self.ctx.db.connect() as c:
            row = fetch_one(c, select(sampling_policies).where(sampling_policies.c.id == policy_id))
        if row is None:
            raise NotFound(f"sampling policy '{policy_id}' does not exist")
        return row

    def delete(self, principal: Any, policy_id: str) -> dict[str, Any]:
        """Remove a policy row (the scope then falls back to the next wider policy, and finally to "off"). Audited as ``config.changed``."""
        row = self.get(policy_id)
        with self.ctx.db.begin() as c:
            c.execute(sampling_policies.delete().where(sampling_policies.c.id == policy_id))
        self.ctx.audit.record(principal, "config.changed", resource_type="sampling_policy", resource_id=policy_id, workspace_id=row.get("workspace_id"),
                              details={"scope": row["scope_key"], "deleted": True})
        return self.public(row)

    def effective(self, *, workspace_id: str | None = None, source_id: str | None = None, asset_id: str | None = None) -> EffectivePolicy:
        """The narrowest enabled-or-disabled row for an asset: asset, then source, then workspace, then global. A row with ``enabled = false`` or ``max_rows = 0`` is an explicit "off"."""
        keys = [k for k in (f"asset:{asset_id}" if asset_id else None, f"source:{source_id}" if source_id else None, f"workspace:{workspace_id}" if workspace_id else None, "global") if k]
        with self.ctx.db.connect() as c:
            rows = {r["scope_key"]: r for r in fetch_all(c, select(sampling_policies).where(sampling_policies.c.scope_key.in_(keys)))}
        for k in keys:
            r = rows.get(k)
            if r is None:
                continue
            if not r["enabled"]:
                return EffectivePolicy(0, r["masking"], bool(r["exclude_classified"]), k, f"the sampling policy '{k}' is disabled")
            if r["max_rows"] <= 0:
                return EffectivePolicy(0, r["masking"], bool(r["exclude_classified"]), k, f"the sampling policy '{k}' allows 0 rows")
            return EffectivePolicy(min(int(r["max_rows"]), MAX_SAMPLE_ROWS), r["masking"], bool(r["exclude_classified"]), k, "")
        return OFF

    def any_enabled(self) -> bool:
        """True when at least one policy row allows rows (used by the status/coverage report)."""
        with self.ctx.db.connect() as c:
            row = fetch_one(c, select(sampling_policies.c.id).where(sampling_policies.c.enabled.is_(True), sampling_policies.c.max_rows > 0).limit(1))
        return row is not None
