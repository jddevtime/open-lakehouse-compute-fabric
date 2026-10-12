# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""The ``fabric`` connector: Microsoft Fabric / Power BI metadata discovery.

.. note::

   This connector follows the documented behaviour of the APIs named below (Microsoft Learn page titles in parentheses) and is covered by tests against a local
   HTTP server in ``tests/fabric/fabric_fakes.py`` that reproduces those documents. Field names, quotas and tenant-setting behaviour can
   differ between tenants, so run the connection test first.

What it calls (identity ``metadata_identity``: a service principal; tokens from the platform's :class:`~olcf.fabric.entra.TokenBroker`):

========================================  =============================================================================================================
Fabric REST (scope ``api.fabric.microsoft.com``)
``GET /v1/workspaces``                    list workspaces, ``continuationToken`` paging (*Workspaces - List Workspaces*)
``GET /v1/workspaces/{ws}/items``         items of a workspace, optional ``type`` filter, paging (*Items - List Items*)
``GET /v1/workspaces/{ws}/lakehouses/{lh}/tables``   tables of a lakehouse (*Tables - List Tables*; not available for schema-enabled lakehouses)
``POST /v1/workspaces/{ws}/items/{id}/getDefinition`` long-running operation: ``202`` + ``Location`` + ``Retry-After``, poll the operation, ``GET .../result``
                                          (*Items - Get Item Definition*, *Long running operations*)
``POST /v1/workspaces/{ws}/notebooks/{id}/getDefinition?format=ipynb``   notebooks (*Notebooks - Get Notebook Definition*)
Power BI admin (scope ``analysis.windows.net/powerbi/api``)
``GET /v1.0/myorg/admin/workspaces/modified``        workspaces modified since a timestamp, for incremental scans (*Admin - Get Modified Workspaces*)
``POST /v1.0/myorg/admin/workspaces/getInfo?lineage=True&datasourceDetails=True&datasetSchema=True&datasetExpressions=True&getArtifactUsers=True``
                                          start a metadata scan of up to 100 workspaces (*Admin - WorkspaceInfo PostWorkspaceInfo*)
``GET .../workspaces/scanStatus/{id}``    poll (*Admin - WorkspaceInfo GetScanStatus*)
``GET .../workspaces/scanResult/{id}``    the result (*Admin - WorkspaceInfo GetScanResult*)
OneLake (ADLS Gen2 DFS endpoint, storage scope)
``GET {dfs}/{ws}?resource=filesystem&directory=..``   directory listing, ``x-ms-continuation`` paging (*OneLake access with Azure Data Lake Storage APIs*; ADLS *Path - List*)
``GET {dfs}/{ws}/{lakehouse}/Tables/{t}/_delta_log/*.json``   the Delta log, to read a table's schema (the tables API has no columns)
========================================  =============================================================================================================

Behaviour worth knowing:

* **Per-workspace failures never abort the scan.** A 401/403 on one workspace's items, tables or definitions becomes a ``permission_denied`` gap for that workspace / item; 404 / 501 /
  throttling that does not clear becomes ``api_unavailable``; the scan continues and ends ``partial``.
* **Tables API fallback.** If ``Tables - List Tables`` answers 400 (schema-enabled lakehouse), 404 or 501, the connector lists ``Tables/`` through OneLake instead and records an
  ``api_unavailable`` gap that says so. Columns always come from the Delta log: only ``metaData``/``protocol`` actions are parsed; ``add`` actions (which carry per-file min/max
  values) are skipped unread, and no value is stored. Reading the log needs OneLake read access for the *metadata* identity; without it the table is reported without columns and a ``permission_denied`` gap.
* **Rate limits.** ``429`` is retried after ``Retry-After`` (cancellation-aware). ``getInfo`` is budgeted per hour (``intelligence.fabric_getinfo_requests_per_hour``, default 500) and limited to
  16 in flight (the documented concurrency limit); a ``Retry-After`` longer than ``intelligence.fabric_max_wait_seconds`` ends the scanner phase with an ``api_unavailable`` gap
  instead of sleeping for an hour.
* **Incremental scans** call *Get Modified Workspaces* with the previous scan's start time and only revisit those workspaces (and workspaces never seen). Table drift inside an
  unmodified workspace is found by the next full scan - documented, not hidden.
* **Two identities.** ``metadata_identity`` reads metadata. ``sampling_identity`` (optional) is only ever used by the connectivity test (step 8) and, later, by the sampling service; the
  scanner never reads data with it.
"""

from __future__ import annotations

import hashlib
import io
import json
import logging
import time
from collections import deque
from collections.abc import Callable, Iterator, Mapping
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from typing import Any

from olcf.core.errors import ValidationFailed
from olcf.fabric import notebook as nbconv
from olcf.fabric.entra import EntraTokenError, TokenBroker, summarise_token
from olcf.fabric.http import Network
from olcf.fabric.rest import (
    FABRIC_SCOPE,
    POWERBI_SCOPE,
    STORAGE_SCOPE,
    FabricApiError,
    FabricRestClient,
    IncompleteInventory,
)
from olcf.intelligence.discovery import detect, netguard
from olcf.intelligence.discovery.base import (
    Batch,
    ConnectorFailure,
    ResolvedSecrets,
    ScanContext,
    Step,
    StepFailure,
    StepSkip,
    StepSpec,
    run_steps,
    safe_message,
)
from olcf.intelligence.discovery.identities import IDENTITY_NAMES, NETWORK_KEYS, build_network
from olcf.intelligence.discovery.ratelimit import SharedWindowLimit
from olcf.intelligence.discovery.scanner_import import scanner_facts, split_scan_result
from olcf.intelligence.model import (
    AssetFact,
    AssetRef,
    DefinitionFact,
    Facts,
    fabric_alias,
    normalize_path,
    normalize_qname,
)

log = logging.getLogger("olcf.intelligence.discovery.fabric")

KIND = "fabric"
ORIGIN = "discovery:fabric"
#: Fabric item types the connector models -> graph asset type
ITEM_ASSET_TYPES: dict[str, str] = {
    "Lakehouse": "lakehouse", "Warehouse": "warehouse", "Notebook": "notebook", "DataPipeline": "pipeline", "SemanticModel": "semantic_model", "Report": "report",
    "PaginatedReport": "report", "SparkJobDefinition": "spark_job", "Dataflow": "dataflow",
}
#: Item types whose definition is fetched -> lineage definition kind
DEFINITION_KINDS: dict[str, str] = {"Notebook": "notebook", "DataPipeline": "pipeline_fabric", "SemanticModel": "semantic_model", "Report": "report_pbi"}
CONFIG_KEYS = frozenset({"tenant_id", "metadata_identity", "sampling_identity", "network", "workspace_ids", "workspace_map", "item_types", "definitions", "tables", "scanner",
                         "include_personal", "max_workspaces"})
SCANNER_KEYS = frozenset({"enabled", "lineage", "datasource_details", "dataset_schema", "dataset_expressions", "artifact_users"})
#: Limits used when the runner passes none (the ``intelligence.fabric_*`` settings override them)
DEFAULT_LIMITS: dict[str, float] = {
    "scanner_batch_size": 100, "getinfo_requests_per_hour": 500, "max_concurrent_scans": 16, "scan_poll_seconds": 5.0, "scan_timeout_seconds": 1800.0, "max_wait_seconds": 120.0,
    "definition_timeout_seconds": 120.0, "modified_requests_per_hour": 30,
}
#: *Admin - Get Modified Workspaces* accepts a ``modifiedSince`` between 30 minutes and 30 days before "now" (Microsoft Learn). The connector never asks for a value outside that window:
#: the checkpoint watermark is kept :data:`WATERMARK_DELAY` behind the scan start, and the value actually sent is also clamped to ``now - WATERMARK_DELAY``.
MODIFIED_MIN_AGE = timedelta(minutes=30)
#: extra distance from the 30-minute limit: clock skew between this host and the service, and the service's own lag in reporting a change
SKEW_MARGIN = timedelta(minutes=5)
WATERMARK_DELAY = MODIFIED_MIN_AGE + SKEW_MARGIN
#: the previous watermark is re-queried from this far before it (overlap, so a change that happened while the last scan ran is not missed)
MODIFIED_OVERLAP = timedelta(minutes=2)
#: the oldest ``modifiedSince`` sent; the documented limit is 30 days, one hour of safety
MODIFIED_MAX_AGE = timedelta(days=30) - timedelta(hours=1)
#: a workspace whose read failed is retried by later incremental scans after ``RETRY_BASE`` seconds, doubling per consecutive failure up to ``RETRY_CAP`` (never abandoned)
RETRY_BASE = 300.0
RETRY_CAP = 6 * 3600.0
MAX_LOG_FILES = 100
MAX_CHECKPOINT_BYTES = 16 * 1024 * 1024
_TENANT_SETTINGS_HELP = ("In the Fabric admin portal (Tenant settings > Admin API settings) enable 'Service principals can access read-only admin APIs' and "
                         "'Enhance admin APIs responses with detailed metadata' / 'Enhance admin APIs responses with DAX and mashup expressions' for a security group that contains the service principal. "
                         "Changes can take up to 15 minutes to apply.")
_FABRIC_API_HELP = ("In the Fabric admin portal enable 'Service principals can use Fabric APIs' (Tenant settings > Developer settings) for a security group that contains the service "
                    "principal, and add the service principal to the workspaces you want discovered (Viewer or higher; Contributor for item definitions).")


# --------------------------------------------------------------------------------------------- aliases (shared names with the lineage analyzers)
def fabric_table_alias(tenant: str | None, workspace_guid: str, lakehouse_guid: str, table: str) -> str:
    """``<tenant>/<workspace>/<lakehouse>/<normalised table name>`` - the same spelling the lineage analyzers use (``lineage.base.fabric_table_alias``)."""
    return f"{fabric_alias(tenant, workspace_guid, lakehouse_guid)}/{normalize_qname(table)}"


def fabric_column_alias(table_alias: str, column: str) -> str:
    """``<fabric table alias>:<column lower-cased>`` (``lineage.base.fabric_column_alias``)."""
    return f"{table_alias}:{column.lower()}"


def item_ref(asset_type: str, tenant: str | None, ws: str, item: str, name: str | None, scope: Mapping[str, str], *, also_any_tenant: bool = False) -> AssetRef:
    """Reference to a Fabric item. ``also_any_tenant`` adds the tenant-less (``*``) alias too, which is what a local lakehouse that records its Fabric mapping uses."""
    aliases = [("fabric", fabric_alias(tenant, ws, item))]
    if also_any_tenant and tenant:
        aliases.append(("fabric", fabric_alias(None, ws, item)))
    return AssetRef(asset_type, tuple(aliases), name, tuple(sorted(scope.items())))


# --------------------------------------------------------------------------------------------- limits
class HourlyBudget:
    """A sliding one-hour request budget (the documented ``getInfo`` limit is 500 requests per hour). ``clock`` is injectable for tests."""

    def __init__(self, limit: int, clock: Callable[[], float] = time.monotonic, window: float = 3600.0) -> None:
        self.limit, self.clock, self.window = max(1, int(limit)), clock, window
        self._stamps: deque[float] = deque()

    def wait_needed(self) -> float:
        """Seconds to wait before one more request fits in the budget (0 = go ahead)."""
        now = self.clock()
        while self._stamps and now - self._stamps[0] >= self.window:
            self._stamps.popleft()
        if len(self._stamps) < self.limit:
            return 0.0
        return max(0.0, self._stamps[0] + self.window - now)

    def record(self) -> None:
        """Count one request now."""
        self._stamps.append(self.clock())


class QuotaExhausted(Exception):
    """The scanner's hourly quota (or a server ``Retry-After``) would require waiting longer than the configured maximum."""

    def __init__(self, wait: float) -> None:
        super().__init__(f"quota exhausted; {int(wait)} s until it resets")
        self.wait = wait


@dataclass
class _Scan:
    """One in-flight metadata scan."""

    id: str
    workspaces: list[str]
    started: float
    polls: int = 0


@dataclass
class Selection:
    """Which workspaces an incremental scan visits. ``ids`` ``None`` = every listed workspace. ``advance`` False = the modified-workspaces question could not be answered this time, so
    the checkpoint watermark must stay where it is (nothing is skipped, the next scan asks again from the same point)."""

    ids: set[str] | None
    advance: bool = True


@dataclass
class _Failure:
    kind: str
    wait: float = 0.0


@dataclass
class _Env:
    """Everything one ``discover`` call shares."""

    ctx: ScanContext
    client: FabricRestClient
    tenant: str | None
    cfg: dict[str, Any]
    limits: dict[str, float]
    mapped: dict[str, str] = field(default_factory=dict)
    budget: HourlyBudget | None = None
    limiter: SharedWindowLimit | None = None
    #: workspaces (lower-case GUID) whose reading failed in a way a later scan can repair (permission, connectivity, throttling, scanner phase, cut-off listing)
    failed: dict[str, _Failure] = field(default_factory=dict)
    #: the workspace listing itself was cut off: the set of workspaces is not known completely
    listing_incomplete: bool = False


def classify(exc: FabricApiError) -> tuple[str, str | None]:
    """``(gap kind, remediation)`` for a Fabric / Power BI API error: 401/403 -> ``permission_denied``; everything else (404, 501, 429, 5xx, network) -> ``api_unavailable``."""
    if isinstance(exc, IncompleteInventory):
        return "api_unavailable", ("the listing was cut off (page limit reached with a continuation token outstanding, or a repeated token), so the inventory is incomplete and nothing missing from it is "
                                   "treated as removed; the workspace is retried by the next scan")
    if exc.status in (401, 403):
        return "permission_denied", exc.remediation or _FABRIC_API_HELP
    if exc.status == 429:
        return "api_unavailable", "the API is throttling this tenant; the next scan retries (lower the scan frequency or the number of workspaces per scan if it persists)"
    if exc.status in (404, 501, 405):
        return "api_unavailable", exc.remediation or "this API is not available for the item or tenant; the connector falls back where it can"
    return "api_unavailable", exc.remediation or "retry later; if this persists check the Microsoft Fabric service health"


# --------------------------------------------------------------------------------------------- Delta log
def parse_schema_string(schema_string: str) -> list[dict[str, Any]]:
    """Columns of a Delta ``schemaString``: ``[{name, type, nullable, ordinal}]`` (complex types are kept as their JSON)."""
    doc = json.loads(schema_string)
    out = []
    for i, f in enumerate(doc.get("fields") or []):
        t = f.get("type")
        out.append({"name": str(f.get("name")), "type": t if isinstance(t, str) else json.dumps(t, sort_keys=True), "nullable": bool(f.get("nullable", True)), "ordinal": i})
    return out


def read_delta_schema(client: FabricRestClient, workspace: str, lakehouse: str, rel: str) -> dict[str, Any]:
    """Schema of a Delta table from its log in OneLake: ``{"columns", "partition_columns", "properties", "log_version", "last_commit_at", "source"}``.

    Walks the JSON commits from the newest backwards until one carries a ``metaData`` action (only ``metaData``/``protocol`` lines are parsed, ``add`` lines are never JSON-decoded);
    when the commits were cleaned up it reads ``metaData`` from the newest checkpoint Parquet (at most :data:`MAX_CHECKPOINT_BYTES`). Raises :class:`LookupError` when neither is
    found and :class:`~olcf.fabric.rest.FabricApiError` for access problems.
    """
    base = f"{lakehouse}/{rel}/_delta_log"
    listing = client.onelake_list(workspace, base)
    files = {e["name"].rsplit("/", 1)[-1]: e for e in listing if str(e.get("isDirectory", "false")).lower() != "true"}
    commits = sorted((n for n in files if n.endswith(".json") and n[:-5].isdigit()), reverse=True)
    version = int(commits[0][:-5]) if commits else None
    last_commit = files[commits[0]].get("lastModified") if commits else None
    protocol: dict[str, Any] | None = None
    for name in commits[:MAX_LOG_FILES]:
        raw = client.onelake_read(workspace, f"{base}/{name}").decode("utf-8", errors="replace")
        for line in raw.splitlines():
            if '"metaData"' in line or '"protocol"' in line:
                try:
                    action = json.loads(line)
                except ValueError:
                    continue
                if "protocol" in action and protocol is None:
                    protocol = action["protocol"]
                if "metaData" in action:
                    md = action["metaData"]
                    return {"columns": parse_schema_string(md["schemaString"]), "partition_columns": list(md.get("partitionColumns") or []), "properties": dict(md.get("configuration") or {}),
                            "log_version": version, "last_commit_at": last_commit, "protocol": protocol, "source": f"commit {name}"}
    md = _checkpoint_metadata(client, workspace, base, files)
    if md is not None:
        return {"columns": parse_schema_string(md["schemaString"]), "partition_columns": list(md.get("partitionColumns") or []), "properties": dict(md.get("configuration") or {}),
                "log_version": version, "last_commit_at": last_commit, "protocol": protocol, "source": "checkpoint"}
    raise LookupError("no metaData action in the JSON commits or the checkpoint of the Delta log")


def _checkpoint_metadata(client: FabricRestClient, workspace: str, base: str, files: Mapping[str, Any]) -> dict[str, Any] | None:
    cps = sorted((n for n in files if ".checkpoint." in n and n.endswith(".parquet")), reverse=True)
    if not cps:
        return None
    import pyarrow.parquet as pq

    newest = cps[0].split(".", 1)[0]
    for name in [n for n in cps if n.startswith(newest)][:3]:
        if int(files[name].get("contentLength") or 0) > MAX_CHECKPOINT_BYTES:
            continue
        data = client.onelake_read(workspace, f"{base}/{name}", max_bytes=MAX_CHECKPOINT_BYTES)
        try:
            table = pq.read_table(io.BytesIO(data), columns=["metaData"])
        except Exception as exc:  # noqa: BLE001 - a checkpoint part without that column is simply not the one
            log.debug("checkpoint part %s has no readable metaData column: %s", name, safe_message(exc))
            continue
        for row in table.column("metaData").to_pylist():
            if row and row.get("schemaString"):
                return {"schemaString": row["schemaString"], "partitionColumns": row.get("partitionColumns") or [], "configuration": dict(row.get("configuration") or {})}
    return None


def list_onelake_tables(client: FabricRestClient, workspace: str, lakehouse: str) -> list[dict[str, Any]]:
    """Fallback for the tables API: tables found by listing ``<lakehouse>/Tables`` (``Tables/<table>`` or ``Tables/<schema>/<table>`` holding a ``_delta_log`` directory)."""
    out: list[dict[str, Any]] = []
    for d in client.onelake_list(workspace, f"{lakehouse}/Tables"):
        if str(d.get("isDirectory", "false")).lower() != "true":
            continue
        name = d["name"].rsplit("/", 1)[-1]
        if name.startswith(("_", ".")):
            continue
        children = {c["name"].rsplit("/", 1)[-1]: c for c in client.onelake_list(workspace, f"{lakehouse}/Tables/{name}")}
        if "_delta_log" in children:
            out.append({"name": name, "path": f"Tables/{name}", "format": "delta", "schema": None})
            continue
        for sub, entry in children.items():
            if str(entry.get("isDirectory", "false")).lower() == "true" and not sub.startswith(("_", ".")):
                inner = {c["name"].rsplit("/", 1)[-1] for c in client.onelake_list(workspace, f"{lakehouse}/Tables/{name}/{sub}")}
                if "_delta_log" in inner:
                    out.append({"name": f"{name}.{sub}", "path": f"Tables/{name}/{sub}", "format": "delta", "schema": name})
    return out


# --------------------------------------------------------------------------------------------- the connector
class FabricConnector:
    """Microsoft Fabric / Power BI discovery (see the module docstring)."""

    kind = KIND
    label = "A Microsoft Fabric / Power BI tenant: workspaces, items, lakehouse tables, item definitions and the Power BI admin metadata scanner"

    def __init__(self, *, sleep: Callable[[float], None] | None = None) -> None:
        self._sleep = sleep

    # ------------------------------------------------------------------------------------------------------------------------ configuration
    def validate_config(self, config: Mapping[str, Any]) -> dict[str, Any]:
        """Check the shape of the non-secret settings (references and endpoint overrides are checked by the API layer, which has the platform context)."""
        extra = sorted(set(config) - CONFIG_KEYS)
        if extra:
            raise ValidationFailed(f"unknown setting(s) {extra} for a Fabric source; allowed: {sorted(CONFIG_KEYS)}")
        out: dict[str, Any] = {}
        tenant = config.get("tenant_id")
        if tenant is not None:
            if not isinstance(tenant, str) or not tenant.strip():
                raise ValidationFailed("tenant_id must be the tenant GUID or domain")
            out["tenant_id"] = tenant.strip().lower()
        for name in IDENTITY_NAMES:
            if name in config:
                if not isinstance(config[name], Mapping):
                    raise ValidationFailed(f"{name} must be an object ({{connection_id}} or {{tenant_id, client_id, secret_id}})")
                out[name] = dict(config[name])
        if "metadata_identity" not in out:
            raise ValidationFailed("a Fabric source needs a metadata_identity", remediation="give {connection_id} of a service-principal connection, or tenant_id + client_id with credentials")
        if "network" in config:
            if not isinstance(config["network"], Mapping) or set(config["network"]) - set(NETWORK_KEYS):
                raise ValidationFailed(f"network must be an object with only {list(NETWORK_KEYS)}")
            out["network"] = dict(config["network"])
        if "workspace_ids" in config:
            ids = config["workspace_ids"]
            if not isinstance(ids, list) or not all(isinstance(x, str) and x for x in ids):
                raise ValidationFailed("workspace_ids must be a list of Fabric workspace GUIDs or display names")
            out["workspace_ids"] = sorted(set(ids))
        if "workspace_map" in config:
            wm = config["workspace_map"]
            if not isinstance(wm, Mapping) or not all(isinstance(k, str) and isinstance(v, str) and v for k, v in wm.items()):
                raise ValidationFailed("workspace_map must map Fabric workspace GUIDs to OLCF workspace ids")
            out["workspace_map"] = {k.lower(): v for k, v in wm.items()}
        if "item_types" in config:
            it = config["item_types"]
            if not isinstance(it, list) or not all(isinstance(x, str) for x in it):
                raise ValidationFailed("item_types must be a list of Fabric item types")
            unknown = sorted(set(it) - set(ITEM_ASSET_TYPES))
            if unknown:
                raise ValidationFailed(f"item_types has unsupported type(s) {unknown}; supported: {sorted(ITEM_ASSET_TYPES)}")
            out["item_types"] = sorted(set(it))
        for key in ("definitions", "tables", "include_personal"):
            if key in config:
                if not isinstance(config[key], bool):
                    raise ValidationFailed(f"{key} must be true or false")
                out[key] = config[key]
        if "scanner" in config:
            sc = config["scanner"]
            if not isinstance(sc, Mapping) or set(sc) - SCANNER_KEYS or not all(isinstance(v, bool) for v in sc.values()):
                raise ValidationFailed(f"scanner must be an object of booleans with only {sorted(SCANNER_KEYS)}")
            out["scanner"] = dict(sc)
        if "max_workspaces" in config:
            mw = config["max_workspaces"]
            if isinstance(mw, bool) or not isinstance(mw, int) or not 1 <= mw <= 100_000:
                raise ValidationFailed("max_workspaces must be an integer between 1 and 100000")
            out["max_workspaces"] = mw
        return out

    # ------------------------------------------------------------------------------------------------------------------------ plumbing
    def _client(self, config: Mapping[str, Any], identity: Mapping[str, Any], app: Any, *, ctx: ScanContext | None = None) -> FabricRestClient:
        """The REST client of a source. Every endpoint it will contact is checked first (:mod:`~olcf.intelligence.discovery.netguard`: https, no loopback / private / link-local /
        metadata destination unless ``intelligence.allow_private_endpoints``) and its connections are resolved-and-pinned, so the credential is never sent to a refused endpoint.
        Raises :class:`~olcf.intelligence.discovery.netguard.EndpointNotAllowed` (before any request) when an endpoint is refused."""
        network: Network = build_network(app, config) if app is not None else Network.from_config(config.get("network") or {})
        factory = netguard.client_factory(app, network)
        secret = {k: v for k, v in identity.items() if k in ("client_secret", "private_key_pem", "certificate_pem")}
        broker = TokenBroker(tenant_id=str(identity.get("tenant_id") or ""), client_id=str(identity.get("client_id") or ""), secret=secret, network=network, client_factory=factory)
        sleeper: Callable[[float], None]
        if ctx is not None:
            def sleeper(seconds: float) -> None:        # interruptible by cancellation, replaceable in tests
                ctx.pause(seconds)
        else:
            sleeper = self._sleep or time.sleep
        client = FabricRestClient(broker, network, sleep=sleeper, client_factory=factory)
        return client

    # ------------------------------------------------------------------------------------------------------------------------ connectivity test
    def test(self, config: Mapping[str, Any], secrets: ResolvedSecrets, *, app: Any = None) -> list[Step]:
        """The eight ordered steps (stops at the first failure; the rest are reported skipped):

        1 resolve credentials - 2 acquire a Fabric token - 3 list workspaces (count) - 4 acquire a Power BI token - 5 scanner permission probe (admin API) - 6 item read probe -
        7 definition read probe - 8 optional sampling identity probe.
        """
        st: dict[str, Any] = {}
        scanner_on = (config.get("scanner") or {}).get("enabled", True)

        def creds() -> str:
            if "metadata_identity" in secrets.errors:
                raise StepFailure(f"the metadata identity cannot be resolved: {secrets.errors['metadata_identity']}",
                                  "check the connection / secret the source references (POST /v1/connections/{id}/test) or send the credentials again")
            ident = secrets.get("metadata_identity")
            if ident is None:
                raise StepFailure("the source has no metadata identity", "set config.metadata_identity and send its credentials")
            st["identity"] = ident
            how = "client secret" if ident.get("client_secret") else "certificate"
            return f"service principal {ident.get('client_id')} in tenant {ident.get('tenant_id')} ({how}; value not shown)"

        def fabric_token() -> str:
            try:
                st["client"] = self._client(config, st["identity"], app)
            except netguard.EndpointNotAllowed as exc:
                raise StepFailure(f"{exc.message}; no request was sent and the credential was not used", exc.remediation) from None
            try:
                tok = st["client"].broker.get_token(FABRIC_SCOPE)
            except EntraTokenError as exc:
                raise StepFailure(f"Entra rejected the token request for the Fabric API: {exc}", exc.remediation or "check tenant_id, client_id and the credential") from None
            return "token acquired " + summarise_token(st["client"].broker.decode_unverified(tok))

        def list_ws() -> str:
            try:
                rows: list[dict[str, Any]] = []
                for w in st["client"].iter_workspaces():
                    rows.append(w)
                    if len(rows) >= 5000:
                        break
            except FabricApiError as exc:
                raise StepFailure(f"listing workspaces failed: {safe_message(exc)}", exc.remediation or _FABRIC_API_HELP) from None
            if not rows:
                raise StepFailure("the service principal can authenticate but sees no workspace", "add it to at least one workspace (Viewer or higher), or enable 'Service principals can use Fabric APIs' for its security group and wait up to 15 minutes")
            st["workspaces"] = rows
            return f"{len(rows)} workspace(s) visible"

        def pbi_token() -> str:
            if not scanner_on:
                raise StepSkip("the Power BI admin scanner is turned off for this source; skipped")
            try:
                st["client"].broker.get_token(POWERBI_SCOPE)
            except EntraTokenError as exc:
                raise StepFailure(f"Entra rejected the token request for the Power BI API: {exc}", exc.remediation or "check the app registration's API permissions") from None
            return "token acquired for the Power BI API"

        def scanner_probe() -> str:
            if not scanner_on:
                raise StepSkip("the Power BI admin scanner is turned off for this source; skipped")
            since = (datetime.now(UTC) - timedelta(hours=1)).strftime("%Y-%m-%dT%H:%M:%S.000Z")
            try:
                ids = st["client"].powerbi_modified_workspaces(since)
            except FabricApiError as exc:
                if exc.status in (401, 403):
                    raise StepFailure(f"the Power BI admin API refused the service principal ({exc.status}): {safe_message(exc)}", _TENANT_SETTINGS_HELP) from None
                raise StepFailure(f"the Power BI admin API call failed: {safe_message(exc)}", exc.remediation or "retry later") from None
            return f"the admin metadata API answers ({len(ids)} workspace(s) modified in the last hour); getInfo/scanStatus/scanResult use the same permission"

        def items_probe() -> str:
            ws = st["workspaces"][0]
            try:
                items = st["client"].list_items(ws["id"])
            except FabricApiError as exc:
                raise StepFailure(f"listing the items of workspace '{ws.get('displayName', ws['id'])}' failed: {safe_message(exc)}",
                                  "add the service principal to that workspace (Viewer or higher)" if exc.status in (401, 403) else (exc.remediation or "retry later")) from None
            st["items"] = (ws, items)
            return f"{len(items)} item(s) in workspace '{ws.get('displayName', ws['id'])}'"

        def definition_probe() -> str:
            ws, items = st["items"]
            cand = next((i for i in items if i.get("type") in DEFINITION_KINDS), None)
            if cand is None:
                raise StepSkip("the first workspace has no notebook, pipeline, semantic model or report to probe; skipped")
            try:
                if cand["type"] == "Notebook":
                    st["client"].get_definition(ws["id"], cand["id"], "ipynb", timeout_seconds=60.0)
                else:
                    st["client"].get_item_definition(ws["id"], cand["id"], timeout_seconds=60.0)
            except FabricApiError as exc:
                raise StepFailure(f"reading the definition of {cand['type']} '{cand.get('displayName')}' failed: {safe_message(exc)}",
                                  "getDefinition needs read AND write permission on the item: make the service principal a Contributor (or higher) of the workspace" if exc.status in (401, 403)
                                  else (exc.remediation or "retry later")) from None
            return f"definition of {cand['type']} '{cand.get('displayName')}' readable"

        def sampling_probe() -> str:
            if "sampling_identity" not in secrets.configured:
                raise StepSkip("no sampling identity configured; data sampling of Fabric tables stays off")
            if "sampling_identity" in secrets.errors:
                raise StepFailure(f"the sampling identity cannot be resolved: {secrets.errors['sampling_identity']}", "check its secret or connection, or remove the sampling identity")
            ident = secrets.get("sampling_identity") or {}
            try:
                sclient = self._client(config, ident, app)
            except netguard.EndpointNotAllowed as exc:
                raise StepFailure(f"{exc.message}; no request was sent and the credential was not used", exc.remediation) from None
            try:
                sclient.broker.get_token(STORAGE_SCOPE)
            except EntraTokenError as exc:
                raise StepFailure(f"Entra rejected the token request for OneLake (storage) with the sampling identity: {exc}", exc.remediation or "check its credentials") from None
            ws, items = st["items"]
            lh = next((i for i in items if i.get("type") == "Lakehouse"), None)
            if lh is None:
                return "a OneLake token was acquired; no lakehouse in the first workspace to test reading"
            try:
                sclient.onelake_list(ws["id"], f"{lh['id']}/Tables")
            except FabricApiError as exc:
                raise StepFailure(f"the sampling identity cannot read OneLake of lakehouse '{lh.get('displayName')}': {safe_message(exc)}",
                                  "grant the identity a OneLake data access role (or workspace Viewer) on the lakehouse" if exc.status in (401, 403) else (exc.remediation or "retry later")) from None
            return f"sampling identity {ident.get('client_id')} can list OneLake of lakehouse '{lh.get('displayName')}' (no data was read)"

        return run_steps([
            StepSpec("resolve credentials", creds), StepSpec("acquire Fabric token", fabric_token), StepSpec("list workspaces", list_ws), StepSpec("acquire Power BI token", pbi_token),
            StepSpec("scanner permission probe", scanner_probe), StepSpec("item read probe", items_probe), StepSpec("definition read probe", definition_probe),
            StepSpec("sampling identity probe", sampling_probe),
        ])

    # ------------------------------------------------------------------------------------------------------------------------ discovery
    def discover(self, ctx: ScanContext) -> Iterator[Batch]:
        """Stream the tenant: tenant asset, then per workspace its assets, tables and definitions, then the Power BI scanner results (see the module docstring)."""
        ident = ctx.secrets.get("metadata_identity")
        if ident is None:
            raise ConnectorFailure(f"the metadata identity cannot be resolved: {ctx.secrets.errors.get('metadata_identity', 'no credentials configured')}",
                                   "fix the source's credentials (POST /v1/intelligence/sources/{id}/test shows the failing step)")
        cfg = dict(ctx.config)
        limits = {**DEFAULT_LIMITS, **{k: v for k, v in (ctx.limits or {}).items() if k in DEFAULT_LIMITS}}
        try:
            client = self._client(cfg, ident, ctx.app, ctx=ctx)
        except netguard.EndpointNotAllowed as exc:
            raise ConnectorFailure(f"{exc.message}; no request was sent and the credential was not used", exc.remediation) from None
        client.give_up_retry_after = float(limits["max_wait_seconds"])
        client.cancel_check = ctx.check_cancelled
        tenant = (cfg.get("tenant_id") or ident.get("tenant_id") or "").lower() or None
        started = (ctx.now or _utc_now)()
        env = _Env(ctx, client, tenant, cfg, limits, {k.lower(): v for k, v in (cfg.get("workspace_map") or {}).items()},
                   HourlyBudget(int(limits["getinfo_requests_per_hour"]), ctx.clock),
                   SharedWindowLimit(ctx.app, f"fabric.modified_workspaces:{tenant or '*'}", int(limits["modified_requests_per_hour"]), clock=lambda: _epoch(ctx.now() if ctx.now else _utc_now())))
        tenant_ref = AssetRef.make("tenant", {"source": f"{ctx.source_id}:tenant:{tenant or 'unknown'}"}, tenant or "Microsoft Fabric tenant", {"tenant": tenant} if tenant else None)
        yield Facts(assets=[AssetFact(tenant_ref, f"Microsoft Fabric tenant {tenant}" if tenant else "Microsoft Fabric tenant", {"source_kind": "fabric", "tenant_id": tenant})])
        workspaces = self._workspaces(env)
        sel = self._select(env, workspaces, started)
        todo = [w for w in workspaces if sel.ids is None or w["id"].lower() in sel.ids]
        ctx.count("workspaces_listed", len(workspaces))
        ctx.count("workspaces_scanned", len(todo))
        for ws in todo:
            ctx.check_cancelled()
            try:
                yield from self._workspace(env, ws, tenant_ref)
            except FabricApiError as exc:
                kind, hint = classify(exc)
                self._fail(env, ws["id"], kind, exc)
                ctx.record_gap(kind, ws.get("displayName") or ws["id"], f"workspace '{ws.get('displayName') or ws['id']}' could not be fully read: {safe_message(exc)}", hint,
                               location={"fabric_workspace": ws["id"]})
            except Exception as exc:  # noqa: BLE001 - a failing workspace never aborts the scan
                log.warning("workspace %s failed: %s", ws["id"], safe_message(exc))
                self._fail(env, ws["id"], "not_inspected")
                ctx.record_gap("not_inspected", ws.get("displayName") or ws["id"], f"workspace '{ws.get('displayName') or ws['id']}' failed unexpectedly: {type(exc).__name__}: {safe_message(exc)}",
                               "report this if it persists", location={"fabric_workspace": ws["id"]})
        if (cfg.get("scanner") or {}).get("enabled", True) and todo:
            yield from self._scanner(env, [w["id"].lower() for w in todo], {w["id"].lower(): w.get("displayName") or w["id"] for w in todo})
        self._checkpoint(env, workspaces, {w["id"].lower() for w in todo}, sel, started)

    # ---- which workspaces
    def _workspaces(self, env: _Env) -> list[dict[str, Any]]:
        ctx, cfg = env.ctx, env.cfg
        wanted = list(cfg.get("workspace_ids") or [])
        flt = ctx.workspace_filter
        listed: list[dict[str, Any]] = []
        try:
            for w in env.client.iter_workspaces():
                ctx.check_cancelled()
                listed.append(w)
                if len(listed) >= int(cfg.get("max_workspaces") or 100_000):
                    ctx.record_gap("not_inspected", "workspaces", f"only the first {len(listed)} workspaces are scanned (max_workspaces)", "raise max_workspaces on the source")
                    break
        except IncompleteInventory as exc:
            # keep what arrived, but the set of workspaces is not known completely: a blocking gap (the scan ends partial and removes nothing) and the checkpoint forgets nothing
            env.listing_incomplete = True
            kind, hint = classify(exc)
            ctx.record_gap(kind, "workspaces", f"the workspace list is incomplete ({safe_message(exc)}); the {len(listed)} workspace(s) received are scanned, none is treated as removed", hint)
        except FabricApiError as exc:
            guids = [x for x in wanted if _looks_guid(x)]
            kind, hint = classify(exc)
            if not guids:
                raise ConnectorFailure(f"the workspaces of the tenant cannot be listed: {safe_message(exc)}", hint or _FABRIC_API_HELP) from None
            ctx.record_gap(kind, "workspaces", f"the workspace list could not be read ({safe_message(exc)}); the {len(guids)} configured workspace id(s) are used as given", hint)
            listed = [{"id": g.lower(), "displayName": g, "type": "Workspace"} for g in guids]
        out = []
        for w in listed:
            gid, name = str(w["id"]).lower(), str(w.get("displayName") or w["id"])
            if str(w.get("type") or "Workspace").lower() == "personal" and not cfg.get("include_personal"):
                continue
            if wanted and not ({x.lower() for x in wanted} & {gid, name.lower()}):
                continue
            if flt is not None and not ({x.lower() for x in flt} & {gid, name.lower()}):
                continue
            out.append({**w, "id": gid, "displayName": name})
        if wanted:
            found = {w["id"] for w in out} | {w["displayName"].lower() for w in out}
            for missing in sorted(x for x in wanted if x.lower() not in found):
                env.ctx.record_gap("not_inspected", missing, f"configured workspace '{missing}' is not visible to the service principal", "add the service principal to the workspace or fix the id",
                                   location={"fabric_workspace": missing})
        return out

    def _select(self, env: _Env, workspaces: list[dict[str, Any]], started: str) -> Selection:
        """Which workspaces to visit. Full scans, a missing watermark, a watermark older than the documented 30-day window and a turned-off scanner all mean "everything". Otherwise:
        the workspaces *Get Modified Workspaces* reports, plus workspaces never seen, plus workspaces whose earlier read failed and whose retry time has come (the durable retry set).

        ``modifiedSince`` is always inside the documented window: ``min(previous watermark - overlap, now - 35 minutes)`` and never older than 30 days (less an hour). The request budget
        (``intelligence.fabric_modified_requests_per_hour``, shared by every source of the tenant and every replica) is respected; when the budget is spent or the API fails
        transiently the scan still visits the new and retry workspaces but leaves the watermark where it was, so nothing is skipped and no full scan is forced."""
        ctx = env.ctx
        mark = ctx.state.get("pbi_watermark")
        if ctx.is_full or not mark or not (env.cfg.get("scanner") or {}).get("enabled", True):
            return Selection(None)
        try:
            now = _parse(started)
            wm = _parse(str(mark))
        except ValueError:
            return Selection(None)
        known = set(ctx.state.get("workspaces") or {})
        listed = {w["id"] for w in workspaces}
        carry = {w["id"] for w in workspaces if w["id"] not in known} | self._due_retries(ctx, listed, now)
        candidate = wm - MODIFIED_OVERLAP
        if now - candidate > MODIFIED_MAX_AGE:
            ctx.record_gap("not_inspected", "incremental", "the last complete scan is older than the 30 days the modified-workspaces API can cover; this scan reads every workspace", None,
                           location={"informational": True})
            return Selection(None)
        since = min(candidate, now - WATERMARK_DELAY)
        retried_clock_fix = False
        while True:
            wait = env.limiter.reserve() if env.limiter is not None else 0.0
            if wait > 0:
                ctx.record_gap("not_inspected", "admin/workspaces/modified", f"the modified-workspaces request budget is spent (about {int(wait)} s until a slot frees); changes are looked for by a later scan, which asks from the same point",
                               None, location={"informational": True})
                return Selection(carry, advance=False)
            try:
                changed = {i.lower() for i in env.client.powerbi_modified_workspaces(_iso(since), exclude_personal=not env.cfg.get("include_personal"))}
                break
            except FabricApiError as exc:
                if exc.status == 400 and not retried_clock_fix:
                    # the service judged the timestamp outside its window (our clock may be ahead of its clock): once more, far inside the window
                    retried_clock_fix = True
                    since = min(since, now - timedelta(hours=1))
                    continue
                kind, hint = classify(exc)
                if exc.status in (401, 403):
                    ctx.record_gap(kind, "admin/workspaces/modified", f"the modified-workspaces API failed ({safe_message(exc)}); this incremental scan reads every workspace", _TENANT_SETTINGS_HELP)
                    return Selection(None)
                ctx.record_gap(kind, "admin/workspaces/modified", f"the modified-workspaces API failed ({safe_message(exc)}); only new and previously failed workspaces were read, the next scan asks again from the same point", hint)
                return Selection(carry, advance=False)
        return Selection(changed | carry)

    @staticmethod
    def _due_retries(ctx: ScanContext, listed: set[str], now: datetime) -> set[str]:
        """Workspaces of the durable retry set that are listed now and whose back-off has elapsed."""
        out: set[str] = set()
        for gid, entry in (ctx.state.get("retry") or {}).items():
            if gid not in listed or not isinstance(entry, dict):
                continue
            try:
                due = _parse(str(entry.get("next_attempt") or "")) <= now
            except ValueError:
                due = True
            if due:
                out.add(gid)
        return out

    def _fail(self, env: _Env, gid: str, kind: str, exc: Exception | None = None, *, wait: float = 0.0) -> None:
        """Remember that reading workspace ``gid`` failed in a way a later scan can repair (it goes to the durable retry set when the scan ends)."""
        wait = max(wait, float(getattr(exc, "retry_after", None) or 0.0))
        cur = env.failed.get(gid.lower())
        if cur is None or wait > cur.wait:
            env.failed[gid.lower()] = _Failure(kind, wait)

    def _checkpoint(self, env: _Env, workspaces: list[dict[str, Any]], visited: set[str], sel: Selection, started: str) -> None:
        """Write the connector's checkpoint (``ctx.state``): the delayed watermark, the known workspaces and the durable retry set.

        A global watermark never implies that every workspace was read: workspaces that failed are *in the retry set* (with attempts, first failure and the earliest next try), which every
        later incremental scan unions into its selection - after a restart, an empty modified list or recovered permissions included. Entries leave the set when the workspace is read
        successfully or is no longer listed."""
        ctx = env.ctx
        now = _parse(started)
        names = {w["id"].lower(): w.get("displayName") or w["id"] for w in workspaces}
        old = {k: v for k, v in (ctx.state.get("retry") or {}).items() if isinstance(v, dict)}
        retry: dict[str, dict[str, Any]] = {}
        for gid, entry in old.items():
            if gid in visited:
                continue                                               # read again now: it stays only if it failed again (below)
            if gid in names or ctx.workspace_filter is not None or env.listing_incomplete:
                retry[gid] = entry                                       # not due yet, or outside this scan's view
        for gid, failure in env.failed.items():
            prev = old.get(gid) or {}
            attempts = int(prev.get("attempts") or 0) + 1
            delay = max(failure.wait, min(RETRY_CAP, RETRY_BASE * (2 ** min(attempts - 1, 20))))
            retry[gid] = {"name": names.get(gid, gid), "kind": failure.kind, "attempts": attempts, "first_failure": prev.get("first_failure") or started, "last_attempt": started,
                          "next_attempt": _iso(now + timedelta(seconds=delay))}
        ctx.state["retry"] = retry
        ctx.count("workspaces_retry_pending", len(retry))
        if sel.advance:
            ctx.state["pbi_watermark"] = _iso(now - WATERMARK_DELAY)
        full_view = (ctx.is_full or sel.ids is None) and not env.listing_incomplete and ctx.workspace_filter is None
        ctx.state["workspaces"] = names if full_view else {**(ctx.state.get("workspaces") or {}), **names}

    # ---- one workspace
    def _workspace(self, env: _Env, ws: dict[str, Any], tenant_ref: AssetRef) -> Iterator[Batch]:
        ctx, client, tenant = env.ctx, env.client, env.tenant
        gid, name = ws["id"], ws["displayName"]
        scope = {"fabric_workspace": gid, **({"tenant": tenant} if tenant else {})}
        olcf_ws = env.mapped.get(gid)
        ws_ref = AssetRef.make("workspace", {"fabric": fabric_alias(tenant, gid)}, name, scope)
        head = Facts()
        head.assets.append(AssetFact(ws_ref, name, {"source_kind": "fabric", "fabric_workspace": gid, "tenant_id": tenant, "capacity_id": ws.get("capacityId"), "workspace_type": ws.get("type")},
                                     parent=tenant_ref, description=ws.get("description") or None, workspace_id=olcf_ws, path=name))
        try:
            items = client.list_items(gid)
        except IncompleteInventory as exc:
            kind, hint = classify(exc)
            self._fail(env, gid, kind, exc)
            ctx.record_gap(kind, name, f"the item list of workspace '{name}' is incomplete: {safe_message(exc)}; the {len(exc.partial)} item(s) received are kept", hint, location={"fabric_workspace": gid})
            items = list(exc.partial)
        except FabricApiError as exc:
            kind, hint = classify(exc)
            self._fail(env, gid, kind, exc)
            ctx.record_gap(kind, name, f"items of workspace '{name}' could not be listed: {safe_message(exc)}", hint, location={"fabric_workspace": gid})
            yield head
            return
        wanted_types = set(env.cfg.get("item_types") or ITEM_ASSET_TYPES)
        lakehouses: list[tuple[dict[str, Any], AssetRef]] = []
        modelled: list[tuple[dict[str, Any], AssetRef]] = []
        for it in items:
            t = str(it.get("type"))
            atype = ITEM_ASSET_TYPES.get(t)
            if atype is None:
                ctx.count("items_unmodelled")
                continue
            if t not in wanted_types:
                continue
            ref = item_ref(atype, tenant, gid, it["id"], it.get("displayName"), scope, also_any_tenant=t in ("Lakehouse", "Warehouse"))
            attrs = {"source_kind": "fabric", "fabric_item_id": it["id"], "fabric_item_type": t, "fabric_workspace": gid, "fabric_workspace_name": name, "tenant_id": tenant}
            head.assets.append(AssetFact(ref, it.get("displayName") or it["id"], attrs, parent=ws_ref, description=it.get("description") or None, workspace_id=olcf_ws,
                                         path=f"{name} / {it.get('displayName') or it['id']}"))
            modelled.append((it, ref))
            if t == "Lakehouse":
                lakehouses.append((it, ref))
        ctx.count("items", len(modelled))
        yield head
        if env.cfg.get("tables", True):
            for it, ref in lakehouses:
                ctx.check_cancelled()
                yield from self._tables(env, ws, it, ref, olcf_ws)
        if env.cfg.get("definitions", True):
            default_lh = {"id": lakehouses[0][0]["id"], "name": lakehouses[0][0].get("displayName")} if len(lakehouses) == 1 else None
            for it, ref in modelled:
                if it["type"] in DEFINITION_KINDS and it["type"] in wanted_types:
                    ctx.check_cancelled()
                    d = self._definition(env, ws, it, ref, default_lh, olcf_ws)
                    if d is not None:
                        ctx.count("definitions")
                        yield d

    # ---- tables
    def _tables(self, env: _Env, ws: dict[str, Any], lh: dict[str, Any], lh_ref: AssetRef, olcf_ws: str | None) -> Iterator[Batch]:
        ctx, client, tenant = env.ctx, env.client, env.tenant
        gid, lname = ws["id"], lh.get("displayName") or lh["id"]
        loc = {"fabric_workspace": gid, "fabric_item": lh["id"]}
        tables: list[dict[str, Any]]
        def shape(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
            return [{"name": t["name"], "path": t.get("path") or f"Tables/{t['name']}", "format": str(t.get("format") or "delta").lower(), "location": t.get("location"), "schema": None} for t in rows]

        try:
            tables = shape(client.list_lakehouse_tables(gid, lh["id"]))
        except IncompleteInventory as exc:
            kind, hint = classify(exc)
            self._fail(env, gid, kind, exc)
            ctx.record_gap(kind, f"{ws['displayName']}/{lname}", f"the table list of lakehouse '{lname}' is incomplete: {safe_message(exc)}; the {len(exc.partial)} table(s) received are kept", hint, location=loc)
            tables = shape(exc.partial)
        except FabricApiError as exc:
            if exc.status in (401, 403):
                self._fail(env, gid, "permission_denied", exc)
                ctx.record_gap("permission_denied", f"{ws['displayName']}/{lname}", f"tables of lakehouse '{lname}' could not be listed: {safe_message(exc)}", exc.remediation or _FABRIC_API_HELP, location=loc)
                return
            reason = "it is a lakehouse with schemas enabled" if exc.error_code == "UnsupportedOperationForSchemasEnabledLakehouse" else f"the API answered {exc.status}"
            ctx.record_gap("api_unavailable", f"{ws['displayName']}/{lname}", f"the Tables API is not available for lakehouse '{lname}' ({reason}); tables were listed through OneLake instead",
                           "Informational: columns and tables come from the OneLake directory listing and Delta logs.", location={**loc, "informational": True})
            try:
                tables = list_onelake_tables(client, gid, lh["id"])
            except FabricApiError as exc2:
                kind, hint = classify(exc2)
                self._fail(env, gid, kind, exc2)
                ctx.record_gap(kind, f"{ws['displayName']}/{lname}", f"OneLake listing of lakehouse '{lname}' failed: {safe_message(exc2)}", hint, location=loc)
                return
        facts = Facts()
        for t in tables:
            ctx.check_cancelled()
            name = str(t["name"])
            t_alias = fabric_table_alias(tenant, gid, lh["id"], name)
            location = t.get("location") or f"abfss://{gid}@onelake.dfs.fabric.microsoft.com/{lh['id']}/{t['path']}"
            aliases = {"fabric": t_alias, "delta_location": normalize_path(location), "onelake_path": normalize_path(location)}
            attrs: dict[str, Any] = {"source_kind": "fabric", "fabric_workspace": gid, "fabric_lakehouse": lh["id"], "lakehouse": lname, "table_name": name, "format": t["format"], "location": location,
                                     "tenant_id": tenant}
            columns: list[dict[str, Any]] = []
            if t["format"] == "delta":
                try:
                    sch = read_delta_schema(client, gid, lh["id"], t["path"])
                    columns = [{**c, "partition": c["name"] in sch["partition_columns"]} for c in sch["columns"]]
                    attrs.update({"partition_columns": sch["partition_columns"], "properties": sch["properties"], "delta_version": sch["log_version"], "last_modified": sch["last_commit_at"],
                                  "schema_source": sch["source"]})
                    if sch.get("protocol"):
                        attrs["protocol"] = sch["protocol"]
                except FabricApiError as exc:
                    kind, hint = classify(exc)
                    self._fail(env, gid, kind, exc)
                    ctx.record_gap(kind, f"{lname}.{name}", f"the Delta log of '{name}' could not be read, so its columns are unknown: {safe_message(exc)}",
                                   "grant the metadata identity read access to the lakehouse in OneLake (workspace Viewer is enough)" if exc.status in (401, 403) else hint,
                                   location={**loc, "table": name})
                except LookupError as exc:
                    ctx.record_gap("not_inspected", f"{lname}.{name}", f"columns of '{name}' unknown: {exc}", "run OPTIMIZE or write a checkpoint with a Delta writer that includes the table metadata", location={**loc, "table": name, "informational": True})
            else:
                ctx.record_gap("unsupported_construct", f"{lname}.{name}", f"table '{name}' is a {t['format']} table; only Delta schemas are read", None, location={**loc, "table": name, "informational": True})
            attrs["columns"], attrs["column_count"] = columns, len(columns)
            tref = AssetRef.make("table", aliases, name, {"fabric_workspace": gid, "lakehouse": lname, **({"tenant": tenant} if tenant else {})})
            facts.assets.append(AssetFact(tref, name, attrs, parent=lh_ref, workspace_id=olcf_ws, path=f"{ws['displayName']} / {lname} / {name}"))
            for col in columns:
                cattrs = {"data_type": col["type"], "nullable": col["nullable"], "ordinal": col["ordinal"], "partition": col["partition"], "source_kind": "fabric"}
                cref = AssetRef.make("column", {"fabric_column": fabric_column_alias(t_alias, col["name"])}, col["name"], {"fabric_workspace": gid, "table": f"fabric:{t_alias}", "table_type": "table",
                                                                                                                         **({"tenant": tenant} if tenant else {})})
                facts.assets.append(AssetFact(cref, col["name"], cattrs, parent=tref, workspace_id=olcf_ws, path=f"{ws['displayName']} / {lname} / {name} / {col['name']}"))
            ctx.count("tables")
        detect.annotate_facts(facts)
        if facts.assets:
            yield facts

    # ---- definitions
    def _definition(self, env: _Env, ws: dict[str, Any], it: dict[str, Any], ref: AssetRef, default_lh: dict[str, Any] | None, olcf_ws: str | None) -> DefinitionFact | None:
        ctx, client, tenant = env.ctx, env.client, env.tenant
        gid, t = ws["id"], it["type"]
        name = it.get("displayName") or it["id"]
        loc = {"fabric_workspace": gid, "fabric_item": it["id"]}
        timeout = float(env.limits["definition_timeout_seconds"])
        try:
            if t == "Notebook":
                nd = client.get_definition(gid, it["id"], "ipynb", timeout_seconds=timeout)
                payload: Any = json.loads(nbconv.serialise(nbconv.parse(nd.content, nd.format), "ipynb"))
                etag = hashlib.sha256(nd.content.encode()).hexdigest()
            else:
                idf = client.get_item_definition(gid, it["id"], timeout_seconds=timeout)
                if t == "DataPipeline":
                    part = next((p for p in idf.parts if p.endswith("pipeline-content.json")), None)
                    if part is None:
                        ctx.record_gap("unsupported_construct", name, f"pipeline '{name}' has no pipeline-content.json part (parts: {sorted(idf.parts)[:6]})", None, location={**loc, "informational": True})
                        return None
                    payload = json.loads(idf.parts[part])
                else:
                    payload = {p: v for p, v in idf.parts.items() if p != ".platform"}
                canon = json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str)
                etag = hashlib.sha256(canon.encode()).hexdigest()
                if idf.binary_parts:
                    ctx.count("definition_binary_parts_skipped", idf.binary_parts)
        except FabricApiError as exc:
            kind, hint = classify(exc)
            self._fail(env, gid, kind, exc)
            ctx.record_gap(kind, name, f"the definition of {t} '{name}' could not be read: {safe_message(exc)}",
                           "getDefinition needs read and write permission on the item: make the service principal a Contributor of the workspace" if exc.status in (401, 403) else hint, location=loc)
            return None
        except (ValueError, KeyError) as exc:
            ctx.record_gap("parse_error", name, f"the definition of {t} '{name}' is not in the expected format: {safe_message(exc)}", None, location=loc)
            return None
        context: dict[str, Any] = {"source": "fabric", "source_id": ctx.source_id, "item_id": it["id"], "item_name": name, "item_type": t, "fabric_workspace": gid,
                                   "fabric_workspace_name": ws["displayName"], "dialect": "spark"}
        if tenant:
            context["tenant"] = tenant
        if olcf_ws:
            context["workspace_id"] = olcf_ws
        if default_lh:
            context["default_lakehouse"] = {"name": default_lh["name"]}
        return DefinitionFact(DEFINITION_KINDS[t], ref, name, payload, etag, context)

    # ---- Power BI admin scanner
    def _scanner(self, env: _Env, guids: list[str], names: Mapping[str, str]) -> Iterator[Batch]:
        ctx, client = env.ctx, env.client
        sc = {"lineage": True, "datasource_details": True, "dataset_schema": True, "dataset_expressions": True, "artifact_users": True, **(env.cfg.get("scanner") or {})}
        size = max(1, min(100, int(env.limits["scanner_batch_size"])))
        concurrent = max(1, min(16, int(env.limits["max_concurrent_scans"])))
        poll = max(0.0, float(env.limits["scan_poll_seconds"]))
        timeout = float(env.limits["scan_timeout_seconds"])
        max_wait = float(env.limits["max_wait_seconds"])
        assert env.budget is not None
        pending: deque[list[str]] = deque(guids[i:i + size] for i in range(0, len(guids), size))
        inflight: dict[str, _Scan] = {}
        denied = False
        stopped = False                     # quota exhausted: the rest of the work waits for a later scan
        quota_wait = 0.0
        while (pending or inflight) and not denied and not stopped:
            ctx.check_cancelled()
            while pending and len(inflight) < concurrent and not denied and not stopped:
                ids = pending[0]
                try:
                    wait = env.budget.wait_needed()
                    if wait > max_wait:
                        raise QuotaExhausted(wait)
                    if wait > 0:
                        ctx.pause(wait)
                    info = client.powerbi_scan_workspaces(ids, lineage=sc["lineage"], datasource_details=sc["datasource_details"], dataset_schema=sc["dataset_schema"],
                                                          dataset_expressions=sc["dataset_expressions"], artifact_users=sc["artifact_users"])
                    env.budget.record()
                    ctx.count("getinfo_requests")
                except QuotaExhausted as q:
                    self._quota_gap(ctx, q.wait, len(pending) + len(inflight))
                    stopped, quota_wait = True, q.wait
                    break
                except FabricApiError as exc:
                    if exc.status == 429:
                        env.budget.record()
                        if inflight:
                            break                                      # the concurrency limit is the other scans: finish those first
                        ra = exc.retry_after if exc.retry_after is not None else 5.0
                        if ra > max_wait:
                            self._quota_gap(ctx, ra, len(pending))
                            stopped, quota_wait = True, ra
                            break
                        ctx.pause(ra)
                        continue
                    kind, hint = classify(exc)
                    if exc.status in (401, 403):
                        ctx.record_gap("permission_denied", "admin/workspaces/getInfo", f"the Power BI admin scanner refused the service principal: {safe_message(exc)}", _TENANT_SETTINGS_HELP)
                        denied = True
                    else:
                        ctx.record_gap(kind, "admin/workspaces/getInfo", f"a metadata scan of {len(ids)} workspace(s) could not be started: {safe_message(exc)}", hint,
                                       location={"fabric_workspaces": ids[:5]})
                        self._fail_batch(env, ids, kind, exc)
                        pending.popleft()
                    continue
                pending.popleft()
                inflight[str(info["id"])] = _Scan(str(info["id"]), ids, ctx.clock())
            progressed = False
            for sid, scan in list(inflight.items()):
                ctx.check_cancelled()
                try:
                    status = str(client.powerbi_scan_status(sid).get("status", "")).lower()
                    scan.polls += 1
                    if status == "succeeded":
                        result = client.powerbi_scan_result(sid)
                        del inflight[sid]
                        progressed = True
                        yield from self._scan_result(env, scan, result, names)
                    elif status in ("notstarted", "running", ""):
                        if ctx.clock() - scan.started > timeout:
                            del inflight[sid]
                            progressed = True
                            self._fail_batch(env, scan.workspaces, "api_unavailable")
                            ctx.record_gap("api_unavailable", "admin/workspaces/scanStatus", f"a metadata scan of {len(scan.workspaces)} workspace(s) did not finish within {int(timeout)} s", "retry later; "
                                           "lower intelligence.fabric_scanner_batch_size if scans of large workspaces time out", location={"fabric_workspaces": scan.workspaces[:5]})
                    else:
                        del inflight[sid]
                        progressed = True
                        self._fail_batch(env, scan.workspaces, "api_unavailable")
                        ctx.record_gap("api_unavailable", "admin/workspaces/scanStatus", f"a metadata scan ended with status '{status}'", "retry later", location={"fabric_workspaces": scan.workspaces[:5]})
                except FabricApiError as exc:
                    del inflight[sid]
                    progressed = True
                    kind, hint = classify(exc)
                    self._fail_batch(env, scan.workspaces, kind, exc)
                    ctx.record_gap(kind, "admin/workspaces/scanResult", f"the result of a metadata scan could not be read: {safe_message(exc)}", _TENANT_SETTINGS_HELP if exc.status in (401, 403) else hint,
                                   location={"fabric_workspaces": scan.workspaces[:5]})
            if inflight and not progressed:
                ctx.pause(poll)
        # whatever was not started or not collected (quota exhausted, admin API refused) is retried by a later scan
        for ids in pending:
            self._fail_batch(env, ids, "permission_denied" if denied else "api_unavailable", wait=quota_wait)
        for scan in inflight.values():
            self._fail_batch(env, scan.workspaces, "permission_denied" if denied else "api_unavailable", wait=quota_wait)

    def _fail_batch(self, env: _Env, guids: list[str], kind: str, exc: Exception | None = None, *, wait: float = 0.0) -> None:
        for g in guids:
            self._fail(env, g, kind, exc, wait=wait)

    @staticmethod
    def _quota_gap(ctx: ScanContext, wait: float, remaining: int) -> None:
        ctx.record_gap("api_unavailable", "admin/workspaces/getInfo", f"the metadata-scan quota is exhausted; it resets in about {int(wait)} s. {remaining} batch(es) were not scanned",
                       "The documented limit is 500 getInfo requests per hour per tenant. The next scan continues; raise intelligence.fabric_getinfo_requests_per_hour only if your tenant's limit is higher, "
                       "or scan less often.")

    def _scan_result(self, env: _Env, scan: _Scan, result: dict[str, Any], names: Mapping[str, str]) -> Iterator[Batch]:
        ctx, tenant = env.ctx, env.tenant
        returned = {str(w.get("id", "")).lower() for w in result.get("workspaces") or [] if isinstance(w, dict)}
        for g in scan.workspaces:
            if g not in returned:
                ctx.record_gap("not_inspected", names.get(g, g), f"the metadata scanner returned nothing for workspace '{names.get(g, g)}' (not scannable, or not visible to the admin API)",
                               "Personal workspaces and workspaces the admin API cannot see are not scanned.", location={"fabric_workspace": g})
        docs = split_scan_result(result)
        ctx.count("scanner_workspaces", len(docs))
        for d in docs:
            ctx.check_cancelled()
            guid = str(d["workspaces"][0]["id"]).lower()
            yield from scanner_facts(ctx.source_id, tenant, [d], origin="fabric", workspace_id=env.mapped.get(guid))


def _utc_now() -> str:
    return datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%S.%fZ")


def _parse(value: str) -> datetime:
    """An ISO-8601 timestamp (``Z`` or an offset; naive means UTC) as an aware UTC datetime. Raises ``ValueError``."""
    dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
    return dt.replace(tzinfo=UTC) if dt.tzinfo is None else dt.astimezone(UTC)


def _iso(dt: datetime) -> str:
    return dt.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%S.000Z")


def _epoch(value: str) -> float:
    return _parse(value).timestamp()


def _looks_guid(value: str) -> bool:
    try:
        return len(value) == 36 and value.count("-") == 4 and int(value.replace("-", ""), 16) >= 0
    except ValueError:
        return False


__all__ = ["FabricConnector", "HourlyBudget", "QuotaExhausted", "classify", "fabric_column_alias", "fabric_table_alias", "item_ref", "list_onelake_tables", "parse_schema_string", "read_delta_schema"]
