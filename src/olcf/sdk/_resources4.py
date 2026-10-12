# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Wrappers of OLCF Intelligence: the asset graph, lineage, discovery sources, scans, gaps, coverage and sampling policies (``client.intelligence``).

Same conventions as :mod:`olcf.sdk._resources`: every method is one documented call, JSON objects come back as :class:`~olcf.sdk._core.Resource`
(with ``.etag``), list answers as :class:`~olcf.sdk._core.Page` (a ``list`` with ``.next_cursor`` and the endpoint's extra fields in ``.extra``).
The synchronous and the asynchronous client share these classes; ``client.intelligence.iter_assets(...)`` and friends (paginating iterators) are
added by :class:`olcf.sdk.client.IntelligenceApi` / :class:`olcf.sdk.aio.AsyncIntelligenceApi` because they differ between the two.

Asset ids look like ``ast_<ULID>``; every ``asset_id`` argument also accepts any alias the server resolves for that route.  An asset the caller may
not see answers ``404`` exactly like one that does not exist.  Credentials of discovery sources are write-only: the server never returns them.
"""

from __future__ import annotations

import builtins
import json
from collections.abc import Mapping, Sequence
from typing import Any, Literal

from olcf.sdk._resources import JSON, Body, _Res

Confidence = Literal["verified", "derived", "inferred", "unknown"]
Direction = Literal["up", "down", "both"]
LineageLevel = Literal["table", "column"]
AssetStatus = Literal["active", "removed", "placeholder", "any"]
ScanMode = Literal["full", "incremental"]
ScanStatus = Literal["running", "succeeded", "partial", "failed", "cancelled"]
MaskingMode = Literal["none", "hash", "redact", "partial"]
PolicyScope = Literal["global", "workspace", "source", "asset"]


def _clean(**fields: Any) -> dict[str, Any]:
    """Drop ``None`` values so the server's own defaults apply."""
    return {k: v for k, v in fields.items() if v is not None}


class Intelligence(_Res):
    """OLCF Intelligence: assets, lineage (table and column level), origin, impact, staleness, documentation, discovery sources, scans, gaps and sampling policies."""

    # ------------------------------------------------------------------------------------------------------------------------ overview
    def summary(self) -> JSON:
        """Counts of assets, edges by confidence, open gaps and the latest scan per source, limited to what the caller may see."""
        return self._call("GET", "/intelligence/summary")

    # ------------------------------------------------------------------------------------------------------------------------ assets
    def list_assets(self, *, q: str | None = None, type: str | builtins.list[str] | None = None, workspace_id: str | None = None,  # noqa: A002
                    domain: str | None = None, classification: str | None = None, certification: str | None = None, owner: str | None = None, tag: str | None = None,
                    status: AssetStatus | None = None, limit: int = 50, cursor: str | None = None) -> JSON:
        """Assets the caller can see, oldest first.  ``q`` matches name, path, description and aliases; ``type`` is one type or a list (comma separated on the wire)."""
        t = ",".join(type) if isinstance(type, (list, tuple)) else type
        return self._call("GET", "/intelligence/assets", params={"q": q, "type": t, "workspace_id": workspace_id, "domain": domain, "classification": classification,
                                                                 "certification": certification, "owner": owner, "tag": tag, "status": status, "limit": limit, "cursor": cursor})

    def get_asset(self, asset_id: str) -> JSON:
        """One asset with aliases, parent, child count, lineage edge counts and open gaps; the resource's ``.etag`` is the value for ``if_match`` of :meth:`update_asset`."""
        return self._call("GET", "/intelligence/assets/{id}", path={"id": asset_id})

    def update_asset(self, asset_id: str, if_match: Any = None, *, description: str | None = None, tags: Sequence[str] | None = None) -> JSON:
        """Describe / tag an asset (needs ``governance.curate``).  An empty ``description`` hands the field back to the scans.  Pass the asset you read as ``if_match``."""
        return self._call("PATCH", "/intelligence/assets/{id}", path={"id": asset_id}, if_match=if_match,
                          json=_clean(description=description, tags=list(tags) if tags is not None else None))

    def children(self, asset_id: str, *, type: str | None = None, status: AssetStatus | None = None, limit: int = 100, cursor: str | None = None) -> JSON:  # noqa: A002
        """Child assets (tables of a lakehouse, columns of a table ...), by name."""
        return self._call("GET", "/intelligence/assets/{id}/children", path={"id": asset_id}, params={"type": type, "status": status, "limit": limit, "cursor": cursor})

    def lineage(self, asset_id: str, *, direction: Direction = "both", depth: int = 3, level: LineageLevel = "table", min_confidence: Confidence | None = None,
                types: Sequence[str] | None = None, column: str | None = None, include_removed: bool = False) -> JSON:
        """Lineage around an asset: ``nodes`` and ``edges`` (``from``/``to`` follow the data flow), ``gaps`` and ``truncated``.  ``level="column"`` follows ``TRANSFORMS`` edges."""
        return self._call("GET", "/intelligence/assets/{id}/lineage", path={"id": asset_id},
                          params={"direction": direction, "depth": depth, "level": level, "min_confidence": min_confidence, "types": list(types) if types else None,
                                  "column": column, "include_removed": include_removed or None})

    def origin(self, asset_id: str, *, depth: int = 10, min_confidence: Confidence | None = None, level: LineageLevel = "table", column: str | None = None) -> JSON:
        """Where does this come from: the upstream roots, every path from them (source first) and the confidence of each path (its weakest edge)."""
        return self._call("GET", "/intelligence/assets/{id}/origin", path={"id": asset_id}, params={"depth": depth, "min_confidence": min_confidence, "level": level, "column": column})

    def impact(self, asset_id: str, *, depth: int = 5, min_confidence: Confidence | None = None) -> JSON:
        """What breaks if this changes: downstream consumers grouped by type (tables, models, measures, reports, visuals), with owners when governance data exists."""
        return self._call("GET", "/intelligence/assets/{id}/impact", path={"id": asset_id}, params={"depth": depth, "min_confidence": min_confidence})

    def staleness(self, asset_id: str, *, depth: int = 5, stale_after_hours: float | None = None) -> JSON:
        """Is the data fresh?  Last observed write, the outcome of the run that wrote it and the most upstream stale producer (the root cause)."""
        return self._call("GET", "/intelligence/assets/{id}/staleness", path={"id": asset_id}, params={"depth": depth, "stale_after_hours": stale_after_hours})

    def documentation(self, asset_id: str) -> JSON:
        """Generated documentation of an asset as JSON (``markdown`` holds the text)."""
        return self._call("GET", "/intelligence/assets/{id}/documentation", path={"id": asset_id}, params={"format": "json"})

    def documentation_markdown(self, asset_id: str) -> str:
        """Generated documentation of an asset as Markdown text."""
        return self._call("GET", "/intelligence/assets/{id}/documentation", path={"id": asset_id}, params={"format": "markdown"})

    def history(self, asset_id: str, *, schema_only: bool = False, limit: int = 50, cursor: str | None = None) -> JSON:
        """Attribute versions of an asset, newest first, with what changed (columns added / removed / retyped)."""
        return self._call("GET", "/intelligence/assets/{id}/history", path={"id": asset_id}, params={"schema_only": schema_only or None, "limit": limit, "cursor": cursor})

    def sample(self, asset_id: str, *, rows: int | None = None, columns: Sequence[str] | None = None) -> JSON:
        """Sample rows of a table, fetched on demand under the sampling policy (masked, limited, audited; never stored).  Needs ``intelligence.sample``; ``403 policy_denied`` until a policy allows it."""
        return self._call("GET", "/intelligence/assets/{id}/sample", path={"id": asset_id}, params={"rows": rows, "columns": list(columns) if columns else None})

    def resolve(self, type: str, *, aliases: Mapping[str, str] | None = None, name: str | None = None, scope: Mapping[str, str] | None = None,  # noqa: A002
                create: bool = False) -> JSON:
        """Which asset does this reference mean?  Resolves aliases, then the name qualified by ``scope`` (``workspace_id``, ``lakehouse``, ``table`` ...); ``create`` makes a placeholder (needs ``intelligence.manage``)."""
        return self._call("POST", "/intelligence/resolve", json={"type": type, "aliases": dict(aliases or {}), "name": name, "scope": dict(scope or {}), "create": create})

    def get_edge(self, edge_id: str) -> JSON:
        """One lineage edge with all of its evidence (who saw it, where, when) and the history of its confidence."""
        return self._call("GET", "/intelligence/edges/{id}", path={"id": edge_id})

    def suggestions(self, asset_id: str) -> JSON:
        """Suggested sensitive-data classifications for a table or column (from names and types; no data is read)."""
        return self._call("GET", "/intelligence/assets/{id}/suggestions", path={"id": asset_id})

    def asset_sampling_policy(self, asset_id: str) -> JSON:
        """The sampling policy that applies to an asset (``allowed: false`` with the reason when sampling is off, the default)."""
        return self._call("GET", "/intelligence/assets/{id}/sampling-policy", path={"id": asset_id})

    # ------------------------------------------------------------------------------------------------------------------------ snapshots and unused
    def snapshots(self, *, source_id: str | None = None, status: ScanStatus | None = None, limit: int = 50, cursor: str | None = None) -> JSON:
        """Metadata snapshots (scans) of the sources the caller can see, newest first; the ids feed :meth:`diff`."""
        return self._call("GET", "/intelligence/snapshots", params={"source_id": source_id, "status": status, "limit": limit, "cursor": cursor})

    def diff(self, from_scan: str, to_scan: str, *, limit: int = 500) -> JSON:
        """Assets and edges added, removed and changed between two scans of one source (columns added / removed / retyped are listed per table)."""
        return self._call("GET", "/intelligence/snapshots/diff", params={"from": from_scan, "to": to_scan, "limit": limit})

    def unused(self, *, days: int | None = None, type: str | None = None, workspace_id: str | None = None, limit: int = 50, cursor: str | None = None) -> JSON:  # noqa: A002
        """Tables and views nothing was seen reading within ``days`` days (no downstream consumer observed by a scan or at run time)."""
        return self._call("GET", "/intelligence/unused", params={"days": days, "type": type, "workspace_id": workspace_id, "limit": limit, "cursor": cursor})

    def requeue_inbox(self) -> JSON:
        """Queue every event the Intelligence inbox gave up on (status ``failed``) for another set of attempts (needs ``intelligence.manage``); returns ``{"requeued": n, "inbox": {...}}``."""
        return self._call("POST", "/intelligence/inbox/requeue")

    # ------------------------------------------------------------------------------------------------------------------------ sources
    def list_sources(self, *, kind: str | None = None) -> JSON:
        """Discovery sources the caller can see.  Credentials are never returned; ``has_credentials`` says whether any are stored."""
        return self._call("GET", "/intelligence/sources", params={"kind": kind})

    def create_source(self, name: str, kind: str, *, config: Body | None = None, schedule: Body | None = None, enabled: bool = True, credentials: Body | None = None,
                      sampling_credentials: Body | None = None) -> JSON:
        """Create a source (kinds ``local``, ``catalog_mirror``, ``fabric``, ``scanner_import``).  ``credentials`` (``{client_secret}`` or ``{private_key_pem, certificate_pem}``) are write-only."""
        return self._call("POST", "/intelligence/sources", json=_clean(name=name, kind=kind, config=dict(config or {}), schedule=dict(schedule) if schedule else None, enabled=enabled,
                                                                       credentials=dict(credentials) if credentials else None,
                                                                       sampling_credentials=dict(sampling_credentials) if sampling_credentials else None))

    def test_config(self, kind: str, *, config: Body | None = None, name: str | None = None, credentials: Body | None = None, sampling_credentials: Body | None = None) -> JSON:
        """Run the connectivity steps for a configuration that has not been saved; credentials are used in memory only."""
        return self._call("POST", "/intelligence/sources/test", json=_clean(kind=kind, config=dict(config or {}), name=name, credentials=dict(credentials) if credentials else None,
                                                                            sampling_credentials=dict(sampling_credentials) if sampling_credentials else None))

    def get_source(self, source_id: str) -> JSON:
        """One source by id or name (``config`` only for managers); the resource's ``.etag`` is the value for ``if_match`` of :meth:`update_source`."""
        return self._call("GET", "/intelligence/sources/{id}", path={"id": source_id})

    def update_source(self, source_id: str, if_match: Any = None, *, name: str | None = None, enabled: bool | None = None, config: Body | None = None,
                      schedule: Body | None = None, credentials: Body | None = None, sampling_credentials: Body | None = None) -> JSON:
        """Change a source; only the fields given change, ``config`` is merged key by key (``None`` value removes a key), credentials replace the stored secret."""
        return self._call("PATCH", "/intelligence/sources/{id}", path={"id": source_id}, if_match=if_match,
                          json=_clean(name=name, enabled=enabled, config=dict(config) if config is not None else None, schedule=dict(schedule) if schedule is not None else None,
                                      credentials=dict(credentials) if credentials else None, sampling_credentials=dict(sampling_credentials) if sampling_credentials else None))

    def delete_source(self, source_id: str) -> JSON:
        """Delete a source (not the built-in local source, not while it is scanning); assets only it reported are marked ``removed``, history is kept."""
        return self._call("DELETE", "/intelligence/sources/{id}", path={"id": source_id})

    def test_source(self, source_id: str) -> JSON:
        """Run the ordered connectivity steps of a saved source; each step has ``ok``, ``detail``, ``remediation`` and ``duration_ms``."""
        return self._call("POST", "/intelligence/sources/{id}/test", path={"id": source_id})

    def scan_source(self, source_id: str, *, mode: ScanMode = "incremental", workspace_ids: Sequence[str] | None = None, wait: bool = False) -> JSON:
        """Start a scan (``202``; poll :meth:`get_scan`).  ``wait=True`` holds the request until it ends.  ``409`` when a scan of the source is already running."""
        return self._call("POST", "/intelligence/sources/{id}/scan", path={"id": source_id}, params={"wait": wait or None},
                          json=_clean(mode=mode, workspace_ids=list(workspace_ids) if workspace_ids else None))

    # ------------------------------------------------------------------------------------------------------------------------ scans, gaps, coverage
    def list_scans(self, *, source_id: str | None = None, status: ScanStatus | None = None, limit: int = 50, cursor: str | None = None) -> JSON:
        """Scans of the sources the caller can see, newest first."""
        return self._call("GET", "/intelligence/scans", params={"source_id": source_id, "status": status, "limit": limit, "cursor": cursor})

    def get_scan(self, scan_id: str) -> JSON:
        """One scan: status, statistics (including the removal sweep of a full scan) and the number of gaps by kind."""
        return self._call("GET", "/intelligence/scans/{id}", path={"id": scan_id})

    def cancel_scan(self, scan_id: str) -> JSON:
        """Ask a running scan to stop; it ends ``cancelled`` after the batch in progress (``409`` if it already ended)."""
        return self._call("POST", "/intelligence/scans/{id}/cancel", path={"id": scan_id})

    def list_gaps(self, *, source_id: str | None = None, scan_id: str | None = None, asset_id: str | None = None, kind: str | None = None, resolved: bool = False,
                  limit: int = 50, cursor: str | None = None) -> JSON:
        """What discovery or analysis could not read (permission denied, API unavailable, dynamic references ...); open ones by default, each says how to fix it."""
        return self._call("GET", "/intelligence/gaps", params={"source_id": source_id, "scan_id": scan_id, "asset_id": asset_id, "kind": kind, "resolved": resolved or None,
                                                              "limit": limit, "cursor": cursor})

    def coverage(self) -> JSON:
        """Per source: active assets by type, open gaps by kind, last scan, next scan due; plus whether any sampling policy is enabled."""
        return self._call("GET", "/intelligence/coverage")

    def import_scanner(self, document: bytes | str | Mapping[str, Any], *, name: str | None = None, tenant_id: str | None = None, workspace_id: str | None = None,
                       scan: bool = True, wait: bool = False) -> JSON:
        """Import a saved Power BI admin scanner result (the JSON body of ``GET /admin/workspaces/scanResult/{scanId}``) as a ``scanner_import`` source and scan it."""
        raw = document if isinstance(document, bytes) else (document.encode("utf-8") if isinstance(document, str) else json.dumps(dict(document)).encode("utf-8"))
        return self._call("POST", "/intelligence/import/scanner", content=raw, headers={"Content-Type": "application/json"},
                          params={"name": name, "tenant_id": tenant_id, "workspace_id": workspace_id, "scan": scan, "wait": wait or None})

    # ------------------------------------------------------------------------------------------------------------------------ sampling policies
    def list_sampling_policies(self, *, scope_type: PolicyScope | None = None) -> JSON:
        """Sampling policies the caller manages.  With none, nothing is ever sampled."""
        return self._call("GET", "/intelligence/sampling-policies", params={"scope_type": scope_type})

    def put_sampling_policy(self, scope_type: PolicyScope, max_rows: int, *, scope_id: str | None = None, masking: MaskingMode = "partial", exclude_classified: bool = True,
                            enabled: bool = True) -> JSON:
        """Create or replace the policy of one scope: ``max_rows`` 0..100 (0 turns sampling off), ``masking`` and whether classified columns are excluded.  The narrowest scope wins."""
        return self._call("PUT", "/intelligence/sampling-policies", json=_clean(scope_type=scope_type, scope_id=scope_id, max_rows=max_rows, masking=masking,
                                                                                exclude_classified=exclude_classified, enabled=enabled))

    def delete_sampling_policy(self, policy_id: str) -> JSON:
        """Remove a sampling policy (the scope falls back to the next wider policy, and finally to off)."""
        return self._call("DELETE", "/intelligence/sampling-policies/{id}", path={"id": policy_id})
