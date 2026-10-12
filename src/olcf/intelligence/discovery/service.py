# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""``service.discovery``: what the REST routes, the CLI and other modules call to administer discovery.

It owns the source store, the scan runner, the scheduler and the sampling service, and enforces authorisation:

* **Reading** (``intelligence.read``): sources, scans, gaps and the coverage report are limited to what the caller can see (an organisation-wide reader sees every source; a
  workspace-scoped reader sees the sources that have at least one asset in their workspaces). Only a caller with the organisation-wide ``intelligence.manage`` sees a source's
  ``config`` (identities, endpoints, schedule); everyone else gets the summary.
* **Managing** (organisation-wide ``intelligence.manage``): create / change / delete / test / scan sources, import scanner results, cancel scans, global and per-source sampling
  policies. Sampling policies of a workspace or an asset need ``intelligence.manage`` in that workspace.
* **Sampling** (``intelligence.sample`` in the asset's workspace): see :class:`~olcf.intelligence.discovery.sampling.SamplingService`.

Credentials are write-only everywhere: no method here returns, logs or audits one.
"""

from __future__ import annotations

import logging
from collections.abc import Mapping
from typing import Any

from sqlalchemy import func, select

from olcf.core.context import AppContext
from olcf.core.errors import BadRequest, Conflict, Forbidden, NotFound, ValidationFailed
from olcf.core.health import ComponentHealth, Status
from olcf.core.timeutil import age_seconds
from olcf.db.util import fetch_all, paginate
from olcf.intelligence import settings as rt
from olcf.intelligence.discovery import detect, identities, scanner_import
from olcf.intelligence.discovery.base import (
    ResolvedSecrets,
    connector_for,
    registered_kinds,
    safe_message,
    steps_ok,
)
from olcf.intelligence.discovery.sampling import SamplingService, scope_key
from olcf.intelligence.discovery.scans import ScanRunner
from olcf.intelligence.discovery.scheduler import DiscoveryScheduler
from olcf.intelligence.discovery.schema import SOURCE_KINDS, sampling_policies
from olcf.intelligence.discovery.sources import LOCAL_SYSTEM_KEY, SourceStore
from olcf.intelligence.engine import RUNTIME_SOURCE, scan_stats
from olcf.intelligence.queries import _gap_out
from olcf.intelligence.schema import assets, gaps, scans
from olcf.security.rbac import Principal

log = logging.getLogger("olcf.intelligence.discovery")

#: (key, description, default, (min, max)) - runtime settings added by discovery (the scan intervals themselves are registered by the Intelligence core)
SETTINGS: list[tuple[str, str, int | float, tuple[float, float]]] = [
    ("intelligence.local_full_scan_interval_seconds", "Seconds between full scans of the local OLCF source (a full scan also marks vanished assets removed; 0 = only on demand)", 86400, (0, 2592000)),
    ("intelligence.max_concurrent_scans", "Most discovery scans that run at the same time in one control-plane process", 2, (1, 16)),
    ("intelligence.scan_lease_seconds", "Seconds a running scan keeps its source reserved without a heartbeat before another process may take over (takes effect at start-up)", 120, (30, 3600)),
    ("intelligence.fabric_scanner_batch_size", "Workspaces per Power BI admin metadata scan request (the API accepts at most 100)", 100, (1, 100)),
    ("intelligence.fabric_getinfo_requests_per_hour", "Most Power BI admin getInfo requests a Fabric source makes per hour (the API allows 500)", 500, (1, 100000)),
    ("intelligence.fabric_modified_requests_per_hour", "Most Power BI admin Get Modified Workspaces requests per hour for one tenant, shared by all sources and replicas (the API allows 30)", 30, (1, 1000)),
    ("intelligence.fabric_max_wait_seconds", "Longest a Fabric scan waits out one throttling response (Retry-After) before it records a gap and moves on", 120, (1, 3600)),
]
KNOWN_SCAN_MODES = ("full", "incremental")


def _validator(lo: float, hi: float, integer: bool):  # noqa: ANN202
    def check(v: Any) -> Any:
        if isinstance(v, bool) or not isinstance(v, (int, float)) or (integer and not isinstance(v, int)) or not lo <= v <= hi:
            raise BadRequest(f"must be {'an integer' if integer else 'a number'} between {lo:g} and {hi:g}")
        return v

    return check


def register_settings(settings: Any) -> None:
    """Register the discovery runtime settings (a key some other component already registered is left alone)."""
    for key, desc, default, (lo, hi) in SETTINGS:
        if key not in getattr(settings, "registry", {}):
            settings.register(key, description=desc, default=default, validator=_validator(lo, hi, isinstance(default, int)))


class DiscoveryService:
    """Facade of the discovery package (``service.discovery``)."""

    def __init__(self, ctx: AppContext, service: Any) -> None:
        self.ctx, self.intel = ctx, service
        self.engine = service.engine
        lease = int(rt.get(ctx, "intelligence.scan_lease_seconds", 120))
        self.sources = SourceStore(ctx, lease_seconds=lease)
        self.runner = ScanRunner(ctx, service, self.sources)
        self.sampling = SamplingService(ctx, service.registry)
        self.policies = self.sampling.policies
        self.scheduler = DiscoveryScheduler(ctx, self.sources, self.runner)

    # ------------------------------------------------------------------------------------------------------------------------ lifecycle
    def ensure_default_source(self) -> dict[str, Any] | None:
        """Create the enabled ``local`` source if it does not exist (idempotent, safe on every replica). Returns the row, or ``None`` if the database was not ready."""
        try:
            return self.sources.ensure_system_source(LOCAL_SYSTEM_KEY, name="local", kind="local", config={})
        except Exception as exc:  # noqa: BLE001 - never block start-up
            log.warning("the default local discovery source could not be created: %s", safe_message(exc))
            return None

    def start(self) -> None:
        """Make sure the default source exists and start the scheduler."""
        self.ensure_default_source()
        self.scheduler.start()

    def stop(self) -> None:
        """Stop the scheduler and cancel / wait for the scans of this process."""
        self.scheduler.stop()
        self.runner.shutdown()

    # ------------------------------------------------------------------------------------------------------------------------ authorisation
    def _manage(self, principal: Principal) -> None:
        principal.require("intelligence.manage")

    def _is_manager(self, principal: Principal) -> bool:
        return principal.can("intelligence.manage")

    def _visible_source_ids(self, principal: Principal) -> set[str] | None:
        """``None`` = every source (organisation-wide reader); otherwise the ids with at least one visible asset."""
        vis = self.intel.scope(principal)
        if vis is None:
            return None
        return self.intel.visible_sources(principal)

    # ------------------------------------------------------------------------------------------------------------------------ sources
    def view(self, principal: Principal, row: Mapping[str, Any]) -> dict[str, Any]:
        """A source as the API shows it (no credentials, ever); only managers see ``config`` and the effective schedule."""
        out = self.sources.public(row)
        inc, full = self.scheduler.intervals(dict(row))
        out["next_due_at"] = self.scheduler.next_due(dict(row))
        if self._is_manager(principal):
            out["effective_schedule"] = {"incremental_seconds": inc, "full_seconds": full}
        else:
            out["config"] = None
            out["schedule"] = {}
        return out

    def list_sources(self, principal: Principal, *, kind: str | None = None) -> list[dict[str, Any]]:
        """Sources the caller may see."""
        if kind is not None and kind not in SOURCE_KINDS:
            raise BadRequest(f"kind must be one of {list(SOURCE_KINDS)}")
        allowed = self._visible_source_ids(principal)
        return [self.view(principal, r) for r in self.sources.list(kind=kind) if allowed is None or r["id"] in allowed]

    def get_source(self, principal: Principal, ref: str) -> dict[str, Any]:
        """One source (by id or name); hidden (404) when the caller cannot see any of its assets."""
        row = self.sources.find(ref)
        allowed = self._visible_source_ids(principal)
        if allowed is not None and row["id"] not in allowed:
            raise NotFound(f"discovery source '{ref}' does not exist")
        return self.view(principal, row)

    def create_source(self, principal: Principal, body: Mapping[str, Any]) -> dict[str, Any]:
        """Create a source from ``{name, kind, config, schedule, enabled, credentials, sampling_credentials}``."""
        self._manage(principal)
        unknown = sorted(set(body) - {"name", "kind", "config", "schedule", "enabled", "credentials", "sampling_credentials"})
        if unknown:
            raise ValidationFailed(f"unknown field(s) {unknown}")
        row = self.sources.create(principal, name=body.get("name") or "", kind=body.get("kind") or "", config=body.get("config"), schedule=body.get("schedule"),
                                  enabled=bool(body.get("enabled", True)), credentials=body.get("credentials"), sampling_credentials=body.get("sampling_credentials"))
        return self.view(principal, self.sources.get(row["id"]))

    def update_source(self, principal: Principal, ref: str, changes: Mapping[str, Any], if_match: str | None = None) -> dict[str, Any]:
        """Change a source (see :meth:`SourceStore.update`)."""
        self._manage(principal)
        row = self.sources.find(ref)
        self.sources.update(principal, row["id"], changes, if_match)
        return self.view(principal, self.sources.get(row["id"]))

    def delete_source(self, principal: Principal, ref: str) -> dict[str, Any]:
        """Delete a source, its secrets and - with a final empty full scan - mark everything only that source reported as ``removed`` (history stays)."""
        self._manage(principal)
        row = self.sources.find(ref)
        self.sources.delete(principal, row["id"])
        sweep: dict[str, Any] = {}
        try:
            sid = self.engine.begin_scan(row["id"], "full", "retired", principal)
            done = self.engine.finish_scan(sid, "succeeded", {"retired_source": True})
            sweep = (done.get("stats") or {}).get("sweep") or {}
        except Exception as exc:  # noqa: BLE001 - the source is gone; the assets can be retired by hand
            log.warning("assets of deleted source %s could not be retired: %s", row["id"], safe_message(exc))
        return {"id": row["id"], "name": row["name"], "deleted": True, "retired": sweep}

    # ---- connectivity tests
    def test_source(self, principal: Principal, ref: str) -> dict[str, Any]:
        """Run the connector's ordered connectivity steps against a stored source. Never raises for source problems: failures are steps with remediation."""
        self._manage(principal)
        row = self.sources.find(ref)
        kind = row["kind"]
        secrets = identities.resolve_secrets(self.ctx, row) if kind == "fabric" else ResolvedSecrets()
        connector = connector_for(kind)
        kwargs: dict[str, Any] = {"app": self.ctx}
        if kind == "scanner_import":
            kwargs["source"] = row
        steps = connector.test(row.get("config") or {}, secrets, **kwargs)  # type: ignore[call-arg]
        return self._tested(principal, row["id"], row["name"], kind, steps)

    def test_config(self, principal: Principal, body: Mapping[str, Any]) -> dict[str, Any]:
        """Test a source *before* it is saved: ``{kind, config, credentials, sampling_credentials}`` (a ``name`` is accepted and ignored). Credentials are used in memory only and never stored."""
        self._manage(principal)
        kind = body.get("kind") or ""
        if kind not in registered_kinds():
            raise BadRequest(f"unknown source kind '{kind}'", details={"kinds": registered_kinds()})
        extra = sorted(set(body) - {"kind", "name", "config", "credentials", "sampling_credentials"})
        if extra:
            raise ValidationFailed(f"unknown field(s) {extra}")
        cfg = self.sources.validate_config(principal, kind, body.get("config") or {})
        secrets = ResolvedSecrets()
        if kind == "fabric":
            supplied = {"metadata_identity": body.get("credentials"), "sampling_identity": body.get("sampling_credentials")}
            for name in identities.IDENTITY_NAMES:
                block = cfg.get(name)
                if not block:
                    continue
                secrets.configured.add(name)
                try:
                    if block.get("connection_id") or block.get("secret_id"):
                        secrets.values[name] = identities._resolve_one(self.ctx, block, None)
                    elif supplied[name] is not None:
                        secrets.values[name] = {"tenant_id": block["tenant_id"], "client_id": block["client_id"], **identities.check_credentials(supplied[name], label=f"{name} credentials")}
                    else:
                        secrets.errors[name] = "no credentials were supplied for this identity"
                except Exception as exc:  # noqa: BLE001
                    secrets.errors[name] = getattr(exc, "message", None) or safe_message(exc)
        connector = connector_for(kind)
        kwargs: dict[str, Any] = {"app": self.ctx}
        if kind == "scanner_import":
            kwargs["source"] = {"payload": None}
        steps = connector.test(cfg, secrets, **kwargs)  # type: ignore[call-arg]
        return self._tested(principal, None, str(body.get("name") or "(unsaved)"), kind, steps)

    def _tested(self, principal: Principal, source_id: str | None, name: str, kind: str, steps: list[Any]) -> dict[str, Any]:
        ok = steps_ok(steps)
        failed = next((s.name for s in steps if not s.ok and not s.skipped), None)
        self.ctx.audit.record(principal, "intelligence.source_tested", resource_type="discovery_source", resource_id=source_id, details={"name": name, "kind": kind, "ok": ok, "failed_step": failed},
                              outcome="success" if ok else "failure")
        return {"source_id": source_id, "kind": kind, "ok": ok, "failed_step": failed, "steps": [s.to_dict() for s in steps]}

    # ---- scans
    def scan(self, principal: Principal, ref: str, *, mode: str = "incremental", workspace_ids: list[str] | None = None, wait: bool = False) -> dict[str, Any]:
        """Start a scan of a source (``202`` semantics: it runs in the background unless ``wait``)."""
        self._manage(principal)
        row = self.runner.start(principal, ref, mode=mode, trigger="api", workspace_filter=workspace_ids, wait=wait)
        return self.scan_view(row)

    def cancel_scan(self, principal: Principal, scan_id: str) -> dict[str, Any]:
        """Ask a running scan to stop."""
        self._manage(principal)
        return self.scan_view(self.runner.cancel(principal, scan_id))

    @staticmethod
    def scan_view(row: Mapping[str, Any]) -> dict[str, Any]:
        """A ``scans`` row as the API shows it."""
        out = {k: row.get(k) for k in ("id", "source_id", "mode", "trigger", "status", "version", "started_at", "finished_at", "stats", "error")}
        out["stats"] = scan_stats(out["stats"])
        if "cancel_requested" in row:
            out["cancel_requested"] = row["cancel_requested"]
        return out

    def get_scan(self, principal: Principal, scan_id: str) -> dict[str, Any]:
        """One scan with its gap counts by kind; hidden (404) when its source is not visible to the caller."""
        row = self.engine.scan_row(scan_id)
        allowed = self._visible_source_ids(principal)
        if allowed is not None and row["source_id"] not in allowed:
            raise NotFound(f"scan '{scan_id}' does not exist")
        with self.ctx.db.connect() as c:
            by_kind = {k: n for k, n in c.execute(select(gaps.c.kind, func.count()).where(gaps.c.last_scan_id == scan_id).group_by(gaps.c.kind))}
        out = self.scan_view(row)
        out["gaps"] = {"total": sum(by_kind.values()), "by_kind": by_kind}
        return out

    def list_scans(self, principal: Principal, *, source_id: str | None = None, status: str | None = None, limit: int = 50, cursor: str | None = None) -> tuple[list[dict[str, Any]], str | None]:
        """Scans of the sources the caller may see, newest first."""
        allowed = self._visible_source_ids(principal)
        stmt = select(scans).where(scans.c.source_id != RUNTIME_SOURCE)
        if allowed is not None:
            if not allowed:
                return [], None
            stmt = stmt.where(scans.c.source_id.in_(sorted(allowed)))
        if source_id:
            stmt = stmt.where(scans.c.source_id == source_id)
        if status:
            stmt = stmt.where(scans.c.status == status)
        with self.ctx.db.connect() as c:
            rows, nxt = paginate(c, stmt, scans, limit=limit, cursor=cursor, descending=True)
        return [self.scan_view(r) for r in rows], nxt

    def list_gaps(self, principal: Principal, *, source_id: str | None = None, scan_id: str | None = None, kind: str | None = None, asset_id: str | None = None,
                  resolved: bool = False, limit: int = 100, cursor: str | None = None) -> tuple[list[dict[str, Any]], str | None]:
        """Gaps (what discovery could not read) the caller may see: gaps of visible assets, plus gaps without an asset for an organisation-wide reader."""
        vis = self.intel.scope(principal)
        stmt = select(gaps)
        stmt = stmt.where(gaps.c.resolved_at.is_not(None)) if resolved else stmt.where(gaps.c.resolved_at.is_(None))
        if vis is not None:
            stmt = stmt.where(gaps.c.asset_id.in_(self.intel.scope_filter(select(assets.c.id), principal, None)))
        if source_id:
            stmt = stmt.where(gaps.c.source_id == source_id)
        if scan_id:
            stmt = stmt.where(gaps.c.last_scan_id == scan_id)
        if kind:
            stmt = stmt.where(gaps.c.kind == kind)
        if asset_id:
            stmt = stmt.where(gaps.c.asset_id == asset_id)
        with self.ctx.db.connect() as c:
            rows, nxt = paginate(c, stmt, gaps, limit=limit, cursor=cursor, descending=True)
        return [{**_gap_out(g), "resolved_at": g["resolved_at"], "scan_id": g["last_scan_id"]} for g in rows], nxt

    # ---- scanner import
    def import_scanner(self, principal: Principal, document: bytes | str | Mapping[str, Any], *, name: str | None = None, tenant_id: str | None = None,
                       workspace_id: str | None = None, scan: bool = True, wait: bool = False) -> dict[str, Any]:
        """Store a saved Power BI scanner result in a ``scanner_import`` source (created on first use) and, unless ``scan=False``, run a full scan of it."""
        self._manage(principal)
        doc = scanner_import.validate_scan_document(document)
        blob, sha = scanner_import.pack(doc)
        cfg: dict[str, Any] = {}
        if tenant_id:
            cfg["tenant_id"] = tenant_id
        if workspace_id:
            cfg["workspace_id"] = str(self.ctx.service("workspaces").resolve(workspace_id)["id"])
        label = (name or "Power BI scanner import").strip()
        try:
            row = self.sources.find(label)
        except NotFound:
            row = None
        if row is not None and row["kind"] != "scanner_import":
            raise Conflict(f"a source named '{label}' exists and is not a scanner import", remediation="choose another name")
        if row is None:
            created = self.sources.create(principal, name=label, kind="scanner_import", config=cfg)
            row = self.sources.get(created["id"])
        elif cfg:
            self.sources.update(principal, row["id"], {"config": cfg})
        self.sources.store_payload(row["id"], blob, sha)
        n_ds = len(doc.get("datasourceInstances") or [])
        self.ctx.audit.record(principal, "intelligence.source_updated", resource_type="discovery_source", resource_id=row["id"],
                              details={"name": row["name"], "fields": ["import"], "workspaces": len(doc["workspaces"]), "datasource_instances": n_ds, "bytes": len(blob), "sha256": sha})
        out: dict[str, Any] = {"source": self.view(principal, self.sources.get(row["id"])), "workspaces": len(doc["workspaces"]), "datasource_instances": n_ds, "sha256": sha, "scan": None}
        if scan:
            out["scan"] = self.scan_view(self.runner.start(principal, row["id"], mode="full", trigger="import", wait=wait))
        return out

    # ------------------------------------------------------------------------------------------------------------------------ sampling policies
    def _policy_authorise(self, principal: Principal, scope_type: str, scope_id: str | None) -> str | None:
        """Authorise a policy change for its scope and return the workspace it belongs to (if any). A malformed scope is a validation error before anything is looked up."""
        scope_key(scope_type, scope_id)
        if scope_type in ("global", "source"):
            self._manage(principal)
            if scope_type == "source":
                self.sources.get(str(scope_id))
            return None
        if scope_type == "workspace":
            principal.require("intelligence.manage", workspace_id=scope_id)
            self.ctx.service("workspaces").resolve(str(scope_id))
            return str(scope_id)
        if scope_type == "asset":
            row = self.intel.row_for(principal, str(scope_id))
            principal.require("intelligence.manage", workspace_id=row["workspace_id"])
            return row["workspace_id"]
        raise BadRequest("scope_type must be one of global, workspace, source, asset")

    def list_policies(self, principal: Principal, *, scope_type: str | None = None) -> list[dict[str, Any]]:
        """Sampling policies the caller manages (all for an organisation-wide manager; those of their workspaces otherwise)."""
        vis = principal.visible_workspaces("intelligence.manage")
        if vis is not None and not vis:
            raise Forbidden("permission 'intelligence.manage' is required", details={"permission": "intelligence.manage"})
        return self.policies.list(scope_type=scope_type, visible_workspaces=vis)

    def put_policy(self, principal: Principal, *, scope_type: str, scope_id: str | None, max_rows: int, masking: str = "partial", exclude_classified: bool = True,
                   enabled: bool = True) -> dict[str, Any]:
        """Create or replace the sampling policy of a scope (``max_rows`` 0..100; there is no policy until someone makes one)."""
        ws = self._policy_authorise(principal, scope_type, scope_id)
        return self.policies.put(principal, scope_type=scope_type, scope_id=scope_id, max_rows=max_rows, masking=masking, exclude_classified=exclude_classified, enabled=enabled,
                                 workspace_id=ws)

    def delete_policy(self, principal: Principal, policy_id: str) -> dict[str, Any]:
        """Remove a sampling policy."""
        row = self.policies.get(policy_id)
        stype, _, sid = str(row["scope_key"]).partition(":")
        self._policy_authorise(principal, stype, sid or None)
        return self.policies.delete(principal, policy_id)

    def effective_policy(self, principal: Principal, asset_id: str) -> dict[str, Any]:
        """The sampling policy that applies to an asset (for the UI: "why can / can't I sample this?")."""
        row = self.intel.row_for(principal, asset_id)
        table = row
        column_id: str | None = None
        if row["type"] == "column" and row.get("parent_id"):                  # a column is governed by its table (see datapolicy.py)
            parent = self.sampling.registry.get(row["parent_id"])
            if parent is not None:
                table, column_id = parent, asset_id
        return self.sampling.access.effective(workspace_id=table["workspace_id"], source_id=table["source_id"], table_asset_ids=[table["id"]], column_asset_id=column_id).to_dict()

    def sample(self, principal: Principal, asset_row: Mapping[str, Any], *, rows: int | None = None, columns: list[str] | None = None) -> dict[str, Any]:
        """Entry point of ``IntelligenceService.sample`` (which authorises, hides invisible assets and audits): policy-limited, masked rows; values are never stored."""
        return self.sampling.sample(principal, str(asset_row["id"]), rows, columns=columns, audit=False)

    # ------------------------------------------------------------------------------------------------------------------------ suggestions
    def suggest(self, principal: Principal, asset_id: str) -> dict[str, Any]:
        """Suggested sensitive-data classifications of a table / view / column, from column names and types only (values are not read)."""
        row = self.intel.row_for(principal, asset_id)
        attrs = row.get("attrs") or {}
        if row["type"] in ("table", "view", "model_table"):
            subject: Any = {"name": row["name"], "columns": [c for c in attrs.get("columns") or [] if isinstance(c, dict)]}
            found = detect.suggest_classifications(subject)
            stored = {str(c["name"]): c["suggested_classifications"] for c in subject["columns"] if c.get("suggested_classifications")}
        elif row["type"] == "column":
            found = [detect.Suggestion(s["classification"], s["confidence"], s["evidence"], s["rule"], row["name"], None, s["category"], tuple(s["evidences"]))
                     for s in detect.classify_column(row["name"], attrs.get("data_type") or attrs.get("type"), None)]
            stored = {row["name"]: attrs["suggested_classifications"]} if attrs.get("suggested_classifications") else {}
        else:
            raise BadRequest(f"a {row['type']} has no columns to classify")
        return {"asset_id": asset_id, "suggestions": [s.to_dict() for s in found], "recorded_by_scans": stored,
                "note": "suggestions only; nothing is classified until a person accepts one in governance. No data values were read."}

    # ------------------------------------------------------------------------------------------------------------------------ coverage and status
    def coverage(self, principal: Principal) -> dict[str, Any]:
        """What discovery knows and what it could not read, per source: assets by type, open gaps by kind, last scan, schedule, sampling state."""
        allowed = self._visible_source_ids(principal)
        out: list[dict[str, Any]] = []
        with self.ctx.db.connect() as c:
            for row in self.sources.list():
                if allowed is not None and row["id"] not in allowed:
                    continue
                a_stmt = self.intel.scope_filter(select(assets.c.type, func.count()).where(assets.c.source_id == row["id"], assets.c.status == "active"), principal, None).group_by(assets.c.type)
                by_type = {t: n for t, n in c.execute(a_stmt)}
                g_stmt = select(gaps.c.kind, func.count()).where(gaps.c.source_id == row["id"], gaps.c.resolved_at.is_(None)).group_by(gaps.c.kind)
                if self.intel.scope(principal) is not None:
                    g_stmt = g_stmt.where(gaps.c.asset_id.in_(self.intel.scope_filter(select(assets.c.id), principal, None)))
                by_gap = {k: n for k, n in c.execute(g_stmt)}
                last = fetch_all(c, select(scans).where(scans.c.source_id == row["id"]).order_by(scans.c.version.desc()).limit(1))
                out.append({"source_id": row["id"], "name": row["name"], "kind": row["kind"], "enabled": bool(row["enabled"]), "assets": {"active": sum(by_type.values()), "by_type": by_type},
                            "open_gaps": {"total": sum(by_gap.values()), "by_kind": by_gap}, "last_scan": self.scan_view(last[0]) if last else None,
                            "last_status": row.get("last_status"), "last_full_at": row.get("last_full_at"), "last_incremental_at": row.get("last_incremental_at"),
                            "next_due_at": self.scheduler.next_due(row), "running": self.sources.is_leased(row)})
        sampling = {"enabled": self.policies.any_enabled(), "note": "sampling is off unless an administrator creates a policy with max_rows > 0"}
        return {"sources": out, "sampling": sampling, "connectors": registered_kinds()}

    def status(self) -> dict[str, Any]:
        """Machine-readable state of discovery (health check, ``/intelligence/summary``-style reports). Reads the local database only."""
        rows = self.sources.list()
        with self.ctx.db.connect() as c:
            pol = int(c.execute(select(func.count()).select_from(sampling_policies)).scalar() or 0)
        stale = []
        for r in rows:
            if not r["enabled"] or r["kind"] == "scanner_import":
                continue
            inc, full = self.scheduler.intervals(r)
            horizon = max(inc, full)
            last = r.get("last_scan_at")
            if horizon and (last is None or (age_seconds(last) or 0.0) > 3 * horizon + 300) and not self.sources.is_leased(r):
                stale.append(r["name"])
        return {"sources": len(rows), "enabled": sum(1 for r in rows if r["enabled"]), "kinds": sorted({r["kind"] for r in rows}),
                "running": self.runner.active_scan_ids(), "failed": [r["name"] for r in rows if r.get("last_status") == "failed"],
                "stale": stale, "scheduler": {"running": self.scheduler.running, "last_tick_at": self.scheduler.last_tick_at, "last_error": self.scheduler.last_error},
                "sampling": {"policies": pol, "any_enabled": self.policies.any_enabled()}, "connectors": registered_kinds()}

    def health(self) -> ComponentHealth:
        """The ``intelligence-discovery`` health component (database only, never the network)."""
        cfg = self.ctx.settings.intelligence
        if not cfg.enabled:
            return ComponentHealth("intelligence-discovery", Status.UNKNOWN, "disabled (intelligence.enabled = false): no scans are scheduled", category="data")
        st = self.status()
        problems = []
        if st["failed"]:
            problems.append("last scan failed: " + ", ".join(st["failed"]))
        if st["stale"]:
            problems.append("no recent scan: " + ", ".join(st["stale"]))
        if not st["scheduler"]["running"]:
            problems.append("scheduler is not running")
        if st["scheduler"]["last_error"]:
            problems.append(f"scheduler: {st['scheduler']['last_error']}")
        detail = f"{st['enabled']}/{st['sources']} discovery source(s) enabled; {len(st['running'])} scan(s) running; sampling {'on' if st['sampling']['any_enabled'] else 'off'}"
        return ComponentHealth("intelligence-discovery", Status.DEGRADED if problems else Status.OK, detail + ("; " + "; ".join(problems) if problems else ""), category="data", labels=st)
