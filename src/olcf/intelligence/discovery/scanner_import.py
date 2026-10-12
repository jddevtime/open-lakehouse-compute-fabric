# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""The ``scanner_import`` connector and the helpers shared with the Fabric connector for Power BI admin *scanner* results.

A Power BI admin metadata scan (Learn: *Admin - WorkspaceInfo GetScanResult*) returns one JSON document ``{"workspaces": [...], "datasourceInstances": [...],
"misconfiguredDatasourceInstances": [...]}``. Organisations that cannot give a service principal the admin API (or that run the scan elsewhere, with PowerShell or a
scheduled job) save that document and import it here: ``POST /v1/intelligence/import/scanner`` creates (or refreshes) a ``scanner_import`` source holding the validated
document and runs a scan of it **like any other connector** - same runner, same gaps, same removal sweep on full scans, same lineage analyzers.

Rules:

* The document is **validated** (a JSON object with a ``workspaces`` array of objects that have an ``id``; at most :data:`MAX_IMPORT_BYTES` bytes and :data:`MAX_IMPORT_WORKSPACES`
  workspaces) and **refused** if it looks like it contains credentials (a connection string with a password, a ``client_secret``, a bearer token). Secrets never belong in the file;
  the check is a safety net, not a licence to include them.
* The document is stored compressed in the source row (``payload``) so a later full or incremental scan can be repeated without the upload; importing again replaces it.
* Each workspace becomes its own :class:`~olcf.intelligence.model.DefinitionFact` of kind ``scanner_result`` (:func:`split_scan_result`): the payload is a scanResult document of that
  one workspace plus only the data-source instances it uses, and the etag is a hash of it, so an unchanged workspace is not analysed again.
* **OLCF extension: attached report definitions.** The Power BI admin scanner does not return report pages or visuals. A document that is imported (never one fetched from a Fabric tenant)
  may attach the definition of a report to its entry in ``reports[]`` as ``"olcfReportDefinition": {"<part path>": <JSON object or text>, ...}`` (the PBIR parts, ``definition.pbir``
  and ``definition/pages/...``). Each attached definition becomes a ``report_pbi`` :class:`~olcf.intelligence.model.DefinitionFact` for the same report asset, so visuals and
  the model fields they use are linked exactly as they are for a report read through the Fabric API. The key is ignored by the scanner analyzer and by the Fabric connector.
* Follows the documented shape of the scanner result.
"""

from __future__ import annotations

import hashlib
import json
import re
import zlib
from collections.abc import Iterator, Mapping
from typing import Any

from olcf.core.errors import BadRequest, ValidationFailed
from olcf.intelligence.discovery.base import (
    Batch,
    ConnectorFailure,
    ResolvedSecrets,
    ScanContext,
    Step,
    StepFailure,
    StepSpec,
    run_steps,
)
from olcf.intelligence.model import AssetFact, AssetRef, DefinitionFact, Facts, fabric_alias, looks_like_guid

KIND = "scanner_import"
REPORT_DEFINITION_KEY = "olcfReportDefinition"
MAX_IMPORT_BYTES = 50 * 1024 * 1024
MAX_IMPORT_WORKSPACES = 5000
_SECRET_PATTERNS = (
    re.compile(r"(?i)\b(password|pwd)\s*=\s*[^;\"'\s]{3,}"),
    re.compile(r"(?i)\"(client_?secret|access_?token|bearer|api_?key|accountkey|sharedaccesssignature)\"\s*:\s*\"[^\"]{6,}\""),
    re.compile(r"(?i)\bbearer\s+[A-Za-z0-9._~+/=-]{20,}"),
    re.compile(r"\beyJ[A-Za-z0-9_-]{10,}\.[A-Za-z0-9_-]{10,}\.[A-Za-z0-9_-]{5,}"),
    re.compile(r"-----BEGIN [A-Z ]*PRIVATE KEY-----"),
)


# --------------------------------------------------------------------------------------------- validation and splitting (pure)
def validate_scan_document(raw: bytes | str | Mapping[str, Any]) -> dict[str, Any]:
    """Parse and validate a saved scanResult. Returns the document; raises :class:`~olcf.core.errors.BadRequest` / :class:`~olcf.core.errors.ValidationFailed` with an actionable message."""
    if isinstance(raw, Mapping):
        text = json.dumps(raw)
        doc: Any = dict(raw)
    else:
        data = raw.encode() if isinstance(raw, str) else raw
        if len(data) > MAX_IMPORT_BYTES:
            raise BadRequest(f"the scanner result is {len(data) / 1e6:.1f} MB; the limit is {MAX_IMPORT_BYTES // (1024 * 1024)} MB", remediation="export fewer workspaces per file")
        text = data.decode("utf-8-sig", errors="replace")
        try:
            doc = json.loads(text)
        except ValueError as exc:
            raise BadRequest(f"the scanner result is not valid JSON: {exc}", remediation="save the body of GET admin/workspaces/scanResult/{scanId} unchanged") from None
    if len(text) > MAX_IMPORT_BYTES:
        raise BadRequest(f"the scanner result is larger than {MAX_IMPORT_BYTES // (1024 * 1024)} MB")
    if not isinstance(doc, dict) or not isinstance(doc.get("workspaces"), list):
        raise ValidationFailed("a scanner result is a JSON object with a 'workspaces' array", remediation="save the body of GET admin/workspaces/scanResult/{scanId} unchanged")
    wss = doc["workspaces"]
    if not wss:
        raise ValidationFailed("the scanner result contains no workspaces")
    if len(wss) > MAX_IMPORT_WORKSPACES:
        raise ValidationFailed(f"the scanner result contains {len(wss)} workspaces; the limit is {MAX_IMPORT_WORKSPACES}")
    bad = [i for i, w in enumerate(wss) if not isinstance(w, dict) or not str(w.get("id") or "").strip()]
    if bad:
        raise ValidationFailed(f"workspaces[{bad[0]}] has no 'id' (an object with the workspace GUID is expected)", details={"invalid_indices": bad[:20]})
    haystacks = (text, "\n".join(_strings(doc)))                  # credentials embedded in a string value are JSON-escaped in ``text``, so look at the raw strings too
    for pat in _SECRET_PATTERNS:
        if any(pat.search(h) for h in haystacks):
            raise ValidationFailed("the scanner result appears to contain a credential (password, secret, token or private key)",
                                   remediation="remove the credential from the file; scanner results do not need any, and secrets must never be imported")
    for key in ("datasourceInstances", "misconfiguredDatasourceInstances"):
        if key in doc and not isinstance(doc[key], list):
            raise ValidationFailed(f"'{key}' must be an array")
    return doc


def _strings(node: Any) -> Iterator[str]:
    """Every string key and value of a JSON document (iteratively, so deeply nested input cannot exhaust the stack)."""
    stack: list[Any] = [node]
    while stack:
        cur = stack.pop()
        if isinstance(cur, str):
            yield cur
        elif isinstance(cur, dict):
            for k, v in cur.items():
                yield str(k)
                stack.append(v)
        elif isinstance(cur, list):
            stack.extend(cur)


def pack(doc: Mapping[str, Any]) -> tuple[bytes, str]:
    """Compress a validated document for storage: ``(zlib bytes, sha256 of the canonical JSON)``."""
    canon = json.dumps(doc, sort_keys=True, separators=(",", ":"), default=str).encode()
    return zlib.compress(canon, 6), hashlib.sha256(canon).hexdigest()


def unpack(blob: bytes) -> dict[str, Any]:
    """Inverse of :func:`pack`."""
    return dict(json.loads(zlib.decompress(blob)))


def _datasource_ids(ws: Mapping[str, Any]) -> set[str]:
    ids: set[str] = set()
    for key in ("datasets", "dataflows", "datamarts", "reports", "dashboards"):
        for obj in ws.get(key) or []:
            if not isinstance(obj, dict):
                continue
            for u in obj.get("datasourceUsages") or []:
                if isinstance(u, dict) and u.get("datasourceInstanceId"):
                    ids.add(str(u["datasourceInstanceId"]))
    return ids


def split_scan_result(doc: Mapping[str, Any]) -> list[dict[str, Any]]:
    """One scanResult-shaped document per workspace: ``{"workspaces": [ws], "datasourceInstances": [only those ws uses], "misconfiguredDatasourceInstances": [...]}``."""
    ds_by_id = {str(d.get("datasourceId") or d.get("id")): d for d in doc.get("datasourceInstances") or [] if isinstance(d, dict)}
    mis_by_id = {str(d.get("datasourceId") or d.get("id")): d for d in doc.get("misconfiguredDatasourceInstances") or [] if isinstance(d, dict)}
    out: list[dict[str, Any]] = []
    for ws in doc.get("workspaces") or []:
        used = _datasource_ids(ws)
        out.append({"workspaces": [ws], "datasourceInstances": [ds_by_id[i] for i in sorted(used) if i in ds_by_id],
                    "misconfiguredDatasourceInstances": [mis_by_id[i] for i in sorted(used) if i in mis_by_id]})
    return out


def scanner_facts(source_id: str, tenant: str | None, docs: list[dict[str, Any]], *, origin: str, workspace_id: str | None = None) -> Iterator[Batch]:
    """For each split document: the (non-placeholder) asset that stands for "the scan of this workspace" and the ``scanner_result`` definition the lineage analyzer reads.

    The process asset is an ``external`` asset keyed by ``source:<source id>:scanner:<workspace guid>``; its ``item_etag`` is the hash of the document, which is also the definition etag.
    """
    for d in docs:
        ws = d["workspaces"][0]
        guid = str(ws["id"]).lower()
        name = str(ws.get("name") or guid)
        payload_hash = hashlib.sha256(json.dumps(d, sort_keys=True, separators=(",", ":"), default=str).encode()).hexdigest()
        ref = AssetRef.make("external", {"source": f"{source_id}:scanner:{guid}"}, f"Power BI scan of workspace {name}", {"fabric_workspace": guid, **({"tenant": tenant} if tenant else {})})
        facts = Facts()
        facts.assets.append(AssetFact(ref, f"Power BI scan of workspace {name}", {"source_kind": origin, "kind": "scanner_result", "fabric_workspace": guid, "fabric_workspace_name": name,
                                                                                       "item_etag": payload_hash, "datasets": len(ws.get("datasets") or []), "reports": len(ws.get("reports") or [])},
                                      workspace_id=workspace_id))
        yield facts
        ctx_ = {"source": "fabric", "source_id": source_id, "fabric_workspace": guid, "fabric_workspace_name": name, "item_type": "scanner_result", "dialect": "tsql"}
        if tenant:
            ctx_["tenant"] = tenant
        if workspace_id:
            ctx_["workspace_id"] = workspace_id
        yield DefinitionFact("scanner_result", ref, f"Power BI scan of workspace {name}", d, payload_hash, ctx_)
        if origin == KIND:
            yield from attached_report_definitions(source_id, tenant, ws, workspace_id=workspace_id)


def attached_report_definitions(source_id: str, tenant: str | None, ws: Mapping[str, Any], *, workspace_id: str | None = None) -> Iterator[DefinitionFact]:
    """The ``report_pbi`` definitions attached to the reports of one scanner workspace (see the module docstring, "attached report definitions")."""
    guid, wname = str(ws.get("id") or "").lower(), str(ws.get("name") or ws.get("id") or "")
    for rep in ws.get("reports") or []:
        parts = rep.get(REPORT_DEFINITION_KEY) if isinstance(rep, dict) else None
        rid = str(rep.get("id") or "").lower() if isinstance(rep, dict) else ""
        if not isinstance(parts, dict) or not parts or not looks_like_guid(rid):
            continue
        rname = str(rep.get("name") or rid)
        # the same aliases the scanner analyzer gives the report (``pbi`` = ``<workspace>/<report>``, see ``lineage.base.pbi_alias``), so both facts land on one asset
        ref = AssetRef.make("report", {"fabric": fabric_alias(tenant, guid, rid), "pbi": f"{guid}/{rid}"}, rname, {"fabric_workspace": guid})
        payload = {str(k): v for k, v in parts.items()}
        etag = hashlib.sha256(json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str).encode()).hexdigest()
        context: dict[str, Any] = {"source": "fabric", "source_id": source_id, "item_id": rid, "item_name": rname, "item_type": "Report", "fabric_workspace": guid,
                                   "fabric_workspace_name": wname, "dialect": "tsql"}
        if tenant:
            context["tenant"] = tenant
        if workspace_id:
            context["workspace_id"] = workspace_id
        yield DefinitionFact("report_pbi", ref, rname, payload, etag, context)


# --------------------------------------------------------------------------------------------- the connector
class ScannerImportConnector:
    """Connector over a saved scanner result held in the source row (``ScanContext.payload`` is the stored, validated document)."""

    kind = KIND
    label = "A saved Power BI admin scanner result (scanResult JSON) imported from a file or request body"

    def validate_config(self, config: Mapping[str, Any]) -> dict[str, Any]:
        """Only an optional ``tenant_id`` (used for Fabric aliases) is accepted; the document itself is uploaded separately and never lives in ``config``."""
        extra = sorted(set(config) - {"tenant_id", "workspace_id"})
        if extra:
            raise ValidationFailed(f"unknown setting(s) {extra} for a scanner import source; allowed: ['tenant_id', 'workspace_id']")
        out: dict[str, Any] = {}
        for k in ("tenant_id", "workspace_id"):
            if config.get(k):
                if not isinstance(config[k], str):
                    raise ValidationFailed(f"{k} must be a string")
                out[k] = str(config[k])
        return out

    def test(self, config: Mapping[str, Any], secrets: ResolvedSecrets, *, app: Any = None, source: Mapping[str, Any] | None = None) -> list[Step]:
        """Steps: 1 a document is stored, 2 it is valid and its size / workspace count."""
        holder: dict[str, Any] = {}

        def stored() -> str:
            blob = (source or {}).get("payload")
            if not blob:
                raise StepFailure("no scanner result has been imported for this source", "POST the saved scanResult JSON to /v1/intelligence/import/scanner")
            holder["doc"] = unpack(blob)
            return f"a scanner result of {len(blob)} compressed bytes is stored"

        def valid() -> str:
            doc = validate_scan_document(holder["doc"])
            return f"{len(doc['workspaces'])} workspace(s), {len(doc.get('datasourceInstances') or [])} data source instance(s)"

        return run_steps([StepSpec("stored document", stored), StepSpec("document validity", valid)])

    def discover(self, ctx: ScanContext) -> Iterator[Batch]:
        """Yield the per-workspace scanner assets and definitions of the stored document (nothing else exists in this source)."""
        if not ctx.payload:
            raise ConnectorFailure("no scanner result is stored for this source", "import one with POST /v1/intelligence/import/scanner")
        doc = validate_scan_document(unpack(ctx.payload))
        docs = split_scan_result(doc)
        flt = ctx.workspace_filter
        tenant = ctx.config.get("tenant_id")
        for d in docs:
            ctx.check_cancelled()
            guid = str(d["workspaces"][0]["id"]).lower()
            if flt is not None and guid not in {x.lower() for x in flt}:
                continue
            ctx.count("scanner_workspaces")
            yield from scanner_facts(ctx.source_id, tenant, [d], origin="scanner_import", workspace_id=ctx.config.get("workspace_id"))
