# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Local doubles for everything Microsoft: Entra ID (token endpoint + JWKS), the Fabric REST API (workspaces, getDefinition long-running operation, throttling) and the OneLake
DFS endpoint, all behind one real HTTP server on 127.0.0.1, plus a ``notebookutils`` test module for the SDK.

OLCF Intelligence discovery additions (all opt-in; the defaults leave the original behaviour untouched): paged workspace/item listing with continuation tokens, lakehouse
tables, the generic item ``getDefinition`` long-running operation, per-workspace permission failures (403), ``429`` throttling with ``Retry-After``, OneLake directory
listing and file reads, and the Power BI admin metadata scanner (``workspaces/modified``, ``getInfo``, ``scanStatus``, ``scanResult``) with a realistic ``scanResult`` shape
(see :func:`scanner_workspace`).

These are *contract tests* against the documented shapes of those APIs; the assumptions about Microsoft's behaviour are listed in docs/guides/fabric-assumptions.md.
"""

from __future__ import annotations

import base64
import json
import threading
import time
import urllib.parse
import uuid
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any

import jwt
from cryptography.hazmat.primitives.asymmetric import rsa

TENANT = "11111111-1111-4111-8111-111111111111"
OTHER_TENANT = "99999999-9999-4999-8999-999999999999"
FABRIC_WS = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
OTHER_FABRIC_WS = "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb"
LAKEHOUSE = "cccccccc-cccc-4ccc-8ccc-cccccccccccc"
NOTEBOOK = "dddddddd-dddd-4ddd-8ddd-dddddddddddd"
API_AUDIENCE = "api://olcf-test-api"
BROKER_CLIENT_ID = "eeeeeeee-eeee-4eee-8eee-eeeeeeeeeeee"
BROKER_SECRET = "Zx9~broker-client-secret-do-not-leak-0123456789"  # noqa: S105 - a fake, used to prove it never leaks
SAMPLING_CLIENT_ID = "ffffffff-ffff-4fff-8fff-ffffffffffff"
SAMPLING_SECRET = "Qy7~sampling-client-secret-do-not-leak-9876543210"  # noqa: S105 - a fake, used to prove it never leaks
PBI_AUDIENCE = "https://analysis.windows.net/powerbi/api"
SECOND_FABRIC_WS = "12121212-1212-4212-8212-121212121212"
THIRD_FABRIC_WS = "34343434-3434-4434-8434-343434343434"


def allow_private_endpoints(ctx: Any) -> None:
    """Let Intelligence discovery sources reach the loopback ``FakeFabricWorld``: sets the platform setting ``intelligence.allow_private_endpoints`` (default ``false``, which refuses
    every loopback / private / link-local endpoint of a Fabric source). Call it in the fixture of every test that points a source at a fake on 127.0.0.1."""
    ctx.service("settings").set("intelligence.allow_private_endpoints", True)


def new_rsa() -> rsa.RSAPrivateKey:
    return rsa.generate_private_key(public_exponent=65537, key_size=2048)


def jwk_of(key: rsa.RSAPrivateKey, kid: str) -> dict[str, Any]:
    return json.loads(jwt.algorithms.RSAAlgorithm.to_jwk(key.public_key())) | {"kid": kid, "use": "sig", "alg": "RS256"}


def pbi_table(name: str, columns: list[tuple[str, str]], *, measures: list[tuple[str, str]] | None = None, source_expression: str | None = None) -> dict[str, Any]:
    """A semantic-model table as the Power BI metadata scanner reports it (``datasetSchema=True``; ``source`` carries the M expression with ``datasetExpressions=True``)."""
    out: dict[str, Any] = {"name": name, "isHidden": False,
                           "columns": [{"name": c, "dataType": t, "isHidden": False, "columnType": "Data"} for c, t in columns],
                           "measures": [{"name": m, "expression": e, "isHidden": False} for m, e in (measures or [])]}
    if source_expression is not None:
        out["source"] = [{"expression": source_expression}]
    return out


def scanner_workspace(ws_id: str, name: str, *, datasets: list[dict[str, Any]] | None = None, reports: list[dict[str, Any]] | None = None,
                      lakehouses: list[dict[str, Any]] | None = None, dataflows: list[dict[str, Any]] | None = None, capacity_id: str | None = None) -> dict[str, Any]:
    """One element of the ``workspaces`` array of a Power BI admin metadata ``scanResult`` (shape as documented in Learn: *Admin - WorkspaceInfo GetScanResult*).

    ``datasets`` are built with :func:`pbi_dataset`, ``reports`` with :func:`pbi_report`. User lists contain a fake principal (the real ones carry e-mail addresses).
    """
    user = {"reportUserAccessRight": "Owner", "emailAddress": "owner@example.com", "displayName": "Owner", "identifier": "owner@example.com", "graphId": str(uuid.uuid4()), "principalType": "User"}
    doc: dict[str, Any] = {"id": ws_id, "name": name, "type": "Workspace", "state": "Active", "isOnDedicatedCapacity": capacity_id is not None, "reports": reports or [],
                           "dashboards": [], "datasets": datasets or [], "dataflows": dataflows or [], "datamarts": [], "users": [{**user, "groupUserAccessRight": "Admin"}]}
    if capacity_id:
        doc["capacityId"] = capacity_id
    if lakehouses:
        doc["Lakehouse"] = lakehouses
    return doc


def pbi_dataset(ds_id: str, name: str, tables: list[dict[str, Any]], *, datasource_ids: list[str] | None = None, expressions: list[tuple[str, str]] | None = None,
                upstream_dataflows: list[str] | None = None) -> dict[str, Any]:
    """A dataset (semantic model) element of a scanner workspace: tables with columns / measures, shared M expressions and its data source usages."""
    return {"id": ds_id, "name": name, "tables": tables, "configuredBy": "owner@example.com", "targetStorageMode": "Abf", "contentProviderType": "PbixInImportMode",
            "expressions": [{"name": n, "expression": e} for n, e in (expressions or [])], "datasourceUsages": [{"datasourceInstanceId": d} for d in (datasource_ids or [])],
            "upstreamDataflows": [{"targetDataflowId": d, "groupId": ""} for d in (upstream_dataflows or [])], "users": [], "createdDate": "2025-01-01T00:00:00.000"}


def pbi_report(rep_id: str, name: str, dataset_id: str | None, *, report_type: str = "PowerBIReport") -> dict[str, Any]:
    """A report element of a scanner workspace, bound to a dataset."""
    return {"reportType": report_type, "id": rep_id, "name": name, "datasetId": dataset_id, "createdDateTime": "2025-01-01T00:00:00.000", "modifiedDateTime": "2025-02-01T00:00:00.000",
            "modifiedBy": "owner@example.com", "createdBy": "owner@example.com", "users": []}


class FakeFabricWorld:
    """Entra + Fabric + OneLake in one process. Attributes are tweakable per test."""

    def __init__(self) -> None:
        self.key = new_rsa()
        self.kid = "kid-1"
        self.old_keys: list[tuple[rsa.RSAPrivateKey, str]] = []
        self.hide_current_key = False
        self.client_secret = BROKER_SECRET
        self.client_id = BROKER_CLIENT_ID
        self.tenant = TENANT
        self.workspaces: list[dict[str, Any]] = [{"id": FABRIC_WS, "displayName": "Sales", "type": "Workspace"}]
        self.notebooks: dict[tuple[str, str], dict[str, str]] = {}
        self.throttle_first = 0           # answer the first N getDefinition calls with 429
        self.lro_polls = 2                # operation polls that say Running before Succeeded
        self.fail_operation = False
        self.onelake_status = 200
        self.fabric_status = 200          # status for GET /v1/workspaces (set 403 to simulate a missing tenant setting)
        self.token_status = 200
        self.calls: list[tuple[str, str]] = []
        self.token_requests: list[dict[str, str]] = []
        self.auth_headers: list[str] = []
        self.dir_puts: list[str] = []
        self._ops: dict[str, dict[str, Any]] = {}
        # ---- OLCF Intelligence discovery emulation (see module docstring)
        self.extra_clients: dict[str, str] = {}                  # additional service principals accepted by the token endpoint: client id -> secret
        self.denied_apps: set[str] = set()                       # service principals (appid) answered with 403 by every discovery endpoint
        self.app_calls: list[tuple[str, str, str]] = []          # (appid, method, path) of every authorised discovery call
        self.workspaces_page_size = 0                            # 0 = one page; N = N workspaces per page with a continuationToken
        self.fabric_items: dict[str, list[dict[str, Any]]] = {}  # workspace guid -> items ({id, displayName, type, description})
        self.items_page_size = 100
        self.workspace_denied: set[str] = set()                  # workspace guids whose item/table/definition calls answer 403
        self.item_denied: set[tuple[str, str]] = set()           # (workspace, item) whose getDefinition answers 403
        self.item_definitions: dict[tuple[str, str], dict[str, str | bytes]] = {}   # (workspace, item) -> {part path: text or bytes}
        self.item_definition_status: dict[tuple[str, str], int] = {}                # (workspace, item) -> forced status for getDefinition
        self.lakehouse_tables: dict[tuple[str, str], list[dict[str, Any]]] = {}     # (workspace, lakehouse) -> table entries
        self.tables_page_size = 100
        self.tables_api_status = 200                             # force the lakehouse tables API to this status (404/501 = "feature not available")
        self.schema_enabled_lakehouses: set[str] = set()         # lakehouse guids whose tables API answers 400 UnsupportedOperationForSchemasEnabledLakehouse
        self.onelake_files: dict[str, bytes] = {}                # "<workspace>/<path>" -> bytes; directories are implied by the paths
        self.onelake_page_size = 1000
        self.throttle_paths: dict[str, int] = {}                 # path substring -> number of next matching requests answered 429
        self.throttle_retry_after = "0"                          # the Retry-After header of those 429 answers
        self.pbi_status = 200                                    # force every Power BI admin endpoint to this status (401/403: tenant setting off)
        self.pbi_modified: dict[str, str] = {}                   # workspace guid -> ISO timestamp of its last modification
        self.pbi_workspaces: dict[str, dict[str, Any]] = {}      # workspace guid -> scanner workspace document (see scanner_workspace)
        self.pbi_unscannable: set[str] = set()                   # requested workspaces the scanner silently leaves out of the result
        self.pbi_datasources: list[dict[str, Any]] = []
        self.pbi_scan_polls = 1                                  # scanStatus answers Running this many times before Succeeded
        self.pbi_scan_stuck = False                              # scanStatus never leaves Running
        self.pbi_concurrent_limit = 16                           # getInfo beyond this many unfinished scans answers 429
        self.pbi_hourly_quota: int | None = None                 # getInfo beyond this many requests answers 429 with a long Retry-After
        self.pbi_quota_retry_after = "3600"
        self.pbi_scan_requests: list[list[str]] = []             # the workspace lists of every getInfo request
        self.pbi_getinfo_queries: list[dict[str, list[str]]] = []
        self.pbi_modified_calls: list[dict[str, list[str]]] = []
        #: when set, ``workspaces/modified`` enforces the documented window (Learn: *Admin - Get Modified Workspaces*): ``modifiedSince`` must be between 30 minutes and 30 days before
        #: ``pbi_now()`` (a ``datetime``), else 400. ``pbi_modified_ages`` records the age of every value that was sent.
        self.pbi_now: Any = None
        self.pbi_modified_ages: list[float] = []
        self.pbi_modified_failures = 0                           # the next N modified calls answer 503
        self.pbi_visible_at: dict[str, Any] = {}                 # workspace guid -> datetime from which ``workspaces/modified`` reports it (service-side reporting lag)
        self.pbi_peak_concurrent = 0
        self._scans: dict[str, dict[str, Any]] = {}
        self._lock = threading.Lock()
        self.server = ThreadingHTTPServer(("127.0.0.1", 0), self._handler())
        self.server.daemon_threads = True
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)

    # ------------------------------------------------------------------ lifecycle
    def start(self) -> FakeFabricWorld:
        self.thread.start()
        return self

    def stop(self) -> None:
        self.server.shutdown()
        self.server.server_close()

    @property
    def base(self) -> str:
        return f"http://127.0.0.1:{self.server.server_address[1]}"

    def network_config(self) -> dict[str, Any]:
        return {"authority_host": self.base, "fabric_api": self.base, "onelake_dfs": self.base, "trust_env": False}

    # ------------------------------------------------------------------ tokens
    def mint(self, *, tid: str | None = None, aud: str = API_AUDIENCE, oid: str | None = None, ver: str = "2.0", exp_in: int = 3600, nbf_in: int = -10, key: rsa.RSAPrivateKey | None = None,
             kid: str | None = None, iss: str | None = None, alg: str = "RS256", **claims: Any) -> str:
        tid = tid or self.tenant
        now = int(time.time())
        body: dict[str, Any] = {"iss": iss or (f"{self.base}/{tid}/v2.0" if ver == "2.0" else f"https://sts.windows.net/{tid}/"), "aud": aud, "tid": tid, "oid": oid or str(uuid.uuid4()),
                                "sub": claims.pop("sub", None) or str(uuid.uuid4()), "iat": now, "nbf": now + nbf_in, "exp": now + exp_in, "ver": ver}
        body.update(claims)
        if alg == "none":
            return jwt.encode(body, key=None, algorithm="none", headers={"kid": kid or self.kid})
        return jwt.encode(body, key or self.key, algorithm=alg, headers={"kid": kid or self.kid})

    def rotate_key(self, keep_old: bool = True) -> None:
        if keep_old:
            self.old_keys.append((self.key, self.kid))
        self.key = new_rsa()
        self.kid = f"kid-{len(self.old_keys) + 1}"

    def add_notebook(self, ws: str, nb: str, *, git: str | None = None, ipynb: str | None = None) -> None:
        self.notebooks[(ws, nb)] = {"git": git or "", "ipynb": ipynb or ""}

    # ------------------------------------------------------------------ discovery emulation: test helpers
    def add_item(self, ws: str, item_id: str, name: str, item_type: str, *, description: str = "", parts: dict[str, str | bytes] | None = None) -> dict[str, Any]:
        """Register a Fabric item (and optionally its ``getDefinition`` parts) in a workspace."""
        item = {"id": item_id, "displayName": name, "type": item_type, "description": description, "workspaceId": ws}
        self.fabric_items.setdefault(ws, []).append(item)
        if parts is not None:
            self.item_definitions[(ws, item_id)] = dict(parts)
        return item

    def add_lakehouse_table(self, ws: str, lh: str, name: str, *, schema: list[tuple[str, str]] | None = None, fmt: str = "delta", table_type: str = "Managed",
                            commits: int = 1, schema_name: str | None = None) -> None:
        """Register a lakehouse table in the tables API and, when ``schema`` is given, write a real ``_delta_log`` for it into the emulated OneLake."""
        rel = f"Tables/{schema_name + '/' if schema_name else ''}{name}"
        entry = {"name": name, "location": f"abfss://{ws}@onelake.dfs.fabric.microsoft.com/{lh}/{rel}", "format": fmt, "type": table_type, "path": rel}
        self.lakehouse_tables.setdefault((ws, lh), []).append(entry)
        if schema is not None:
            self.write_delta_log(ws, lh, rel, schema, commits=commits)

    def write_delta_log(self, ws: str, lh: str, rel: str, schema: list[tuple[str, str]], *, commits: int = 1) -> None:
        """Write ``commits`` JSON commits under ``<ws>/<lh>/<rel>/_delta_log``; the schema is in commit 0 only (the reader must walk back to find it)."""
        fields = [{"name": n, "type": t, "nullable": True, "metadata": {}} for n, t in schema]
        meta = {"metaData": {"id": uuid.uuid4().hex, "format": {"provider": "parquet", "options": {}}, "schemaString": json.dumps({"type": "struct", "fields": fields}),
                             "partitionColumns": [], "configuration": {}, "createdTime": 1_700_000_000_000}}
        base = f"{ws}/{lh}/{rel}/_delta_log"
        first = [{"protocol": {"minReaderVersion": 1, "minWriterVersion": 2}}, meta, {"commitInfo": {"timestamp": 1_700_000_000_000, "operation": "CREATE TABLE"}}]
        self.onelake_files[f"{base}/{0:020d}.json"] = ("\n".join(json.dumps(a) for a in first) + "\n").encode()
        for v in range(1, commits):
            body = [{"add": {"path": f"part-{v}.parquet", "size": 10, "modificationTime": 1_700_000_000_000 + v, "dataChange": True}}, {"commitInfo": {"timestamp": 1_700_000_000_000 + v, "operation": "WRITE"}}]
            self.onelake_files[f"{base}/{v:020d}.json"] = ("\n".join(json.dumps(a) for a in body) + "\n").encode()

    @staticmethod
    def _encode_parts(parts: dict[str, str | bytes]) -> list[dict[str, str]]:
        return [{"path": k, "payload": base64.b64encode(v if isinstance(v, bytes) else v.encode()).decode(), "payloadType": "InlineBase64"} for k, v in parts.items()]

    def discovery_network_config(self) -> dict[str, Any]:
        """Like :meth:`network_config` plus the Power BI host (the fabric adapter's ``NetworkConfig`` forbids unknown keys, so the original method is left alone)."""
        return {**self.network_config(), "powerbi_api": self.base}

    def _discovery(self, h: Any, method: str, u: Any, parts: list[str], q: dict[str, list[str]], raw: bytes) -> bool:
        """Route the discovery additions. Returns True when the request was answered here."""
        world = self
        path = u.path

        def throttled() -> bool:
            for frag, left in list(world.throttle_paths.items()):
                if left > 0 and frag in path:
                    world.throttle_paths[frag] = left - 1
                    h._send(429, {"errorCode": "TooManyRequests", "message": "slow down"}, {"Retry-After": world.throttle_retry_after})
                    return True
            return False

        def authed(audience: str) -> dict[str, Any] | None:
            claims = h._bearer(audience)
            if claims is None:
                h._send(401, {"errorCode": "InvalidToken", "message": "Access token is invalid"})
                return None
            app = str(claims.get("appid") or "")
            with world._lock:
                world.app_calls.append((app, method, path))
            if app in world.denied_apps:
                h._send(403, {"errorCode": "InsufficientPrivileges", "message": "The caller does not have permission to perform the action"})
                return None
            return claims

        # ---------------------------------------------------------------- Power BI admin (metadata scanner)
        if parts[:3] == ["v1.0", "myorg", "admin"]:
            if authed(PBI_AUDIENCE) is None:
                return True
            if throttled():
                return True
            if world.pbi_status != 200:
                h._send(world.pbi_status, {"error": {"code": "PowerBINotAuthorizedException", "message": "Not authorized"}})
                return True
            tail = parts[3:]
            if tail == ["workspaces", "modified"] and method == "GET":
                world.pbi_modified_calls.append(q)
                since = (q.get("modifiedSince") or [""])[0]
                if world.pbi_modified_failures > 0:
                    world.pbi_modified_failures -= 1
                    h._send(503, {"error": {"code": "ServiceUnavailable", "message": "try again"}})
                    return True
                if world.pbi_now is not None and since:
                    import datetime as _dt

                    age = (world.pbi_now() - _dt.datetime.fromisoformat(since.replace("Z", "+00:00"))).total_seconds()
                    world.pbi_modified_ages.append(age)
                    if not 30 * 60 <= age <= 30 * 86400:
                        h._send(400, {"error": {"code": "InvalidRequest", "message": "modifiedSince must be between 30 minutes and 30 days earlier than the current time"}})
                        return True
                lagging = {w for w, at in world.pbi_visible_at.items() if world.pbi_now is not None and world.pbi_now() < at}
                rows = [{"id": w} for w, ts in sorted(world.pbi_modified.items()) if (not since or ts >= since) and w not in lagging]
                h._send(200, rows)
                return True
            if tail == ["workspaces", "getInfo"] and method == "POST":
                body = json.loads(raw or b"{}")
                ids = list(body.get("workspaces") or [])
                world.pbi_scan_requests.append(ids)
                world.pbi_getinfo_queries.append(q)
                if not 1 <= len(ids) <= 100:
                    h._send(400, {"error": {"code": "InvalidRequest", "message": "between 1 and 100 workspaces are allowed"}})
                    return True
                if world.pbi_hourly_quota is not None and len(world.pbi_scan_requests) > world.pbi_hourly_quota:
                    h._send(429, {"error": {"code": "TooManyRequests", "message": "API is not accessible for application"}}, {"Retry-After": world.pbi_quota_retry_after})
                    return True
                with world._lock:
                    active = sum(1 for sc in world._scans.values() if not sc["fetched"])
                    if active >= world.pbi_concurrent_limit:
                        h._send(429, {"error": {"code": "TooManyRequests", "message": "too many concurrent scans"}}, {"Retry-After": "0"})
                        return True
                    sid = uuid.uuid4().hex
                    world._scans[sid] = {"ids": ids, "polls": 0, "fetched": False, "query": q}
                    world.pbi_peak_concurrent = max(world.pbi_peak_concurrent, active + 1)
                h._send(202, {"id": sid, "createdDateTime": "2026-01-01T00:00:00.000", "status": "NotStarted"})
                return True
            if len(tail) == 3 and tail[:2] == ["workspaces", "scanStatus"] and method == "GET":
                sc = world._scans.get(tail[2])
                if sc is None:
                    h._send(404, {"error": {"code": "NotFound", "message": "scan not found"}})
                    return True
                sc["polls"] += 1
                done = not world.pbi_scan_stuck and sc["polls"] > world.pbi_scan_polls
                h._send(200, {"id": tail[2], "createdDateTime": "2026-01-01T00:00:00.000", "status": "Succeeded" if done else "Running"}, {} if done else {"Retry-After": "0"})
                return True
            if len(tail) == 3 and tail[:2] == ["workspaces", "scanResult"] and method == "GET":
                sc = world._scans.get(tail[2])
                if sc is None or sc["polls"] <= world.pbi_scan_polls:
                    h._send(404, {"error": {"code": "NotFound", "message": "scan result not available"}})
                    return True
                sc["fetched"] = True
                docs = [world.pbi_workspaces.get(w) or {"id": w, "name": w, "type": "Workspace", "state": "Active"} for w in sc["ids"] if w not in world.pbi_unscannable]
                h._send(200, {"workspaces": docs, "datasourceInstances": world.pbi_datasources, "misconfiguredDatasourceInstances": []})
                return True
            h._send(404, {"error": {"code": "NotFound", "message": path}})
            return True

        # ---------------------------------------------------------------- Fabric items / tables / generic getDefinition
        if len(parts) >= 4 and parts[:2] == ["v1", "workspaces"] and parts[3] in ("items", "lakehouses"):
            ws = parts[2]
            if authed("https://api.fabric.microsoft.com") is None:
                return True
            if throttled():
                return True
            if ws in world.workspace_denied:
                h._send(403, {"errorCode": "InsufficientPrivileges", "message": "The caller does not have permission to perform the action"})
                return True
            if parts[3] == "items" and len(parts) == 4 and method == "GET":
                items = list(world.fabric_items.get(ws, []))
                want = (q.get("type") or [""])[0]
                if want:
                    items = [i for i in items if i["type"].lower() == want.lower()]
                start = int((q.get("continuationToken") or ["0"])[0])
                page = items[start:start + world.items_page_size]
                nxt = start + world.items_page_size
                h._send(200, {"value": page, **({"continuationToken": str(nxt), "continuationUri": f"{world.base}{path}?continuationToken={nxt}"} if nxt < len(items) else {})})
                return True
            if parts[3] == "items" and len(parts) == 6 and parts[5] == "getDefinition" and method == "POST":
                item = parts[4]
                key = (ws, item)
                if key in world.item_denied:
                    h._send(403, {"errorCode": "InsufficientPrivileges", "message": "The caller does not have permission to perform the action"})
                    return True
                if key in world.item_definition_status:
                    st = world.item_definition_status[key]
                    h._send(st, {"errorCode": "OperationNotSupportedForItem" if st < 500 else "InternalError", "message": "getDefinition is not supported for this item"})
                    return True
                if key not in world.item_definitions:
                    h._send(404, {"errorCode": "ItemNotFound", "message": "item not found", "requestId": "req-2"})
                    return True
                op = uuid.uuid4().hex
                world._ops[op] = {"polls": 0, "parts": world.item_definitions[key], "format": (q.get("format") or [""])[0]}
                h._send(202, None, {"Location": f"{world.base}/v1/operations/{op}", "Retry-After": "0", "x-ms-operation-id": op})
                return True
            if parts[3] == "lakehouses" and len(parts) == 6 and parts[5] == "tables" and method == "GET":
                lh = parts[4]
                if world.tables_api_status != 200:
                    h._send(world.tables_api_status, {"errorCode": "FeatureNotAvailable", "message": "the tables API is not available"})
                    return True
                if lh in world.schema_enabled_lakehouses:
                    h._send(400, {"errorCode": "UnsupportedOperationForSchemasEnabledLakehouse", "message": "The operation is not supported for Lakehouse with schemas enabled"})
                    return True
                tables = world.lakehouse_tables.get((ws, lh))
                if tables is None:
                    h._send(404, {"errorCode": "ItemNotFound", "message": "lakehouse not found"})
                    return True
                start = int((q.get("continuationToken") or ["0"])[0])
                page = tables[start:start + world.tables_page_size]
                nxt = start + world.tables_page_size
                h._send(200, {"data": page, **({"continuationToken": str(nxt), "continuationUri": f"{world.base}{path}?continuationToken={nxt}"} if nxt < len(tables) else {})})
                return True

        # ---------------------------------------------------------------- OneLake file read
        if method == "GET" and len(parts) >= 2 and parts[0] not in ("v1", "v1.0") and "resource" not in q:
            if authed("https://storage.azure.com") is None:
                return True
            if world.onelake_status != 200:
                h._send(world.onelake_status, {"error": {"code": "AuthorizationPermissionMismatch", "message": "This request is not authorized to perform this operation using this permission."}})
                return True
            blob = world.onelake_files.get("/".join(urllib.parse.unquote(p) for p in parts))
            if blob is None:
                h._send(404, {"error": {"code": "PathNotFound", "message": "The specified path does not exist."}})
                return True
            h._send_bytes(blob)
            return True
        return False

    def _onelake_list(self, h: Any, ws: str, q: dict[str, list[str]]) -> None:
        """ADLS Gen2 ``Path - List`` over :attr:`onelake_files`: direct children (or all descendants when ``recursive=true``) of ``directory``, paged by ``x-ms-continuation``."""
        directory = (q.get("directory") or [""])[0].strip("/")
        recursive = (q.get("recursive") or ["false"])[0] == "true"
        prefix = f"{ws}/{directory}/"
        names: dict[str, bool] = {}
        for full in self.onelake_files:
            if not full.startswith(prefix):
                continue
            rest = full[len(prefix):]
            segs = rest.split("/")
            if recursive:
                for i in range(1, len(segs)):
                    names[f"{directory}/{'/'.join(segs[:i])}"] = True
                names[f"{directory}/{rest}"] = False
            elif len(segs) == 1:
                names[f"{directory}/{rest}"] = False
            else:
                names[f"{directory}/{segs[0]}"] = True
        rows = [{"name": n, "isDirectory": "true" if is_dir else "false", "contentLength": "0" if is_dir else str(len(self.onelake_files[f"{ws}/{n}"])),
                 "lastModified": "Mon, 01 Jan 2026 00:00:00 GMT"} for n, is_dir in sorted(names.items())]
        start = int((q.get("continuation") or ["0"])[0])
        page = rows[start:start + self.onelake_page_size]
        nxt = start + self.onelake_page_size
        h._send(200, {"paths": page}, {"x-ms-continuation": str(nxt)} if nxt < len(rows) else {})

    # ------------------------------------------------------------------ http
    def _handler(self) -> type[BaseHTTPRequestHandler]:
        world = self

        class H(BaseHTTPRequestHandler):
            protocol_version = "HTTP/1.1"

            def log_message(self, *a: Any) -> None:  # noqa: D401
                pass

            def _send(self, status: int, doc: Any = None, headers: dict[str, str] | None = None) -> None:
                body = b"" if doc is None else json.dumps(doc).encode()
                self.send_response(status)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(body)))
                for k, v in (headers or {}).items():
                    self.send_header(k, v)
                self.end_headers()
                self.wfile.write(body)

            def _send_bytes(self, body: bytes) -> None:
                self.send_response(200)
                self.send_header("Content-Type", "application/octet-stream")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

            def _bearer(self, audience_prefix: str) -> dict[str, Any] | None:
                auth = self.headers.get("Authorization", "")
                world.auth_headers.append(auth)
                if not auth.startswith("Bearer "):
                    return None
                try:
                    claims = jwt.decode(auth[7:], world.key.public_key(), algorithms=["RS256"], audience=audience_prefix)
                    return claims
                except jwt.PyJWTError:
                    return None

            def do_PUT(self) -> None:  # noqa: N802
                self._route("PUT")

            def do_POST(self) -> None:  # noqa: N802
                self._route("POST")

            def do_GET(self) -> None:  # noqa: N802
                self._route("GET")

            def _route(self, method: str) -> None:
                u = urllib.parse.urlsplit(self.path)
                n = int(self.headers.get("Content-Length") or 0)
                raw = self.rfile.read(n) if n else b""
                with world._lock:
                    world.calls.append((method, u.path))
                parts = [p for p in u.path.split("/") if p]
                q = urllib.parse.parse_qs(u.query)
                # ---- Entra
                if len(parts) == 4 and parts[1:] == ["oauth2", "v2.0", "token"] and method == "POST":
                    form = {k: v[0] for k, v in urllib.parse.parse_qs(raw.decode()).items()}
                    world.token_requests.append(form)
                    if world.token_status != 200:
                        return self._send(world.token_status, {"error": "invalid_client", "error_description": "AADSTS7000215: Invalid client secret provided.", "error_codes": [7000215]})
                    cid = form.get("client_id", "")
                    extra_ok = cid in world.extra_clients and world.extra_clients[cid] == form.get("client_secret")
                    if not extra_ok and (form.get("client_secret") != world.client_secret or cid != world.client_id):
                        return self._send(401, {"error": "invalid_client", "error_description": "AADSTS7000215: Invalid client secret provided.", "error_codes": [7000215]})
                    scope = form.get("scope", "")
                    aud = scope[: -len("/.default")] if scope.endswith("/.default") else scope
                    tok = world.mint(tid=parts[0], aud=aud, oid=cid, appid=cid, azp=cid, idtyp="app", roles=["Task.Execute"])
                    return self._send(200, {"token_type": "Bearer", "expires_in": 3599, "access_token": tok})
                if len(parts) == 4 and parts[1:] == ["discovery", "v2.0", "keys"]:
                    keys = [] if world.hide_current_key else [jwk_of(world.key, world.kid)]
                    keys += [jwk_of(k, kid) for k, kid in world.old_keys]
                    return self._send(200, {"keys": keys})
                if world._discovery(self, method, u, parts, q, raw):
                    return
                # ---- Fabric
                if parts[:2] == ["v1", "workspaces"] and len(parts) == 2 and method == "GET":
                    if self._bearer("https://api.fabric.microsoft.com") is None:
                        return self._send(401, {"errorCode": "InvalidToken", "message": "Access token is invalid"})
                    if world.fabric_status != 200:
                        return self._send(world.fabric_status, {"errorCode": "PrincipalTypeNotSupported", "message": "service principals are not allowed"})
                    if world.workspaces_page_size:
                        start = int((q.get("continuationToken") or ["0"])[0])
                        page = world.workspaces[start:start + world.workspaces_page_size]
                        nxt = start + world.workspaces_page_size
                        return self._send(200, {"value": page, **({"continuationToken": str(nxt)} if nxt < len(world.workspaces) else {})})
                    return self._send(200, {"value": world.workspaces})
                if len(parts) == 6 and parts[0] == "v1" and parts[1] == "workspaces" and parts[3] == "notebooks" and parts[5] == "getDefinition" and method == "POST":
                    if self._bearer("https://api.fabric.microsoft.com") is None:
                        return self._send(401, {"errorCode": "InvalidToken", "message": "Access token is invalid"})
                    if world.throttle_first > 0:
                        world.throttle_first -= 1
                        return self._send(429, {"errorCode": "TooManyRequests", "message": "slow down"}, {"Retry-After": "0"})
                    nb = world.notebooks.get((parts[2], parts[4]))
                    if nb is None:
                        return self._send(404, {"errorCode": "ItemNotFound", "message": "notebook not found", "requestId": "req-1"})
                    fmt = (q.get("format") or ["ipynb"])[0]
                    op = uuid.uuid4().hex
                    world._ops[op] = {"polls": 0, "fmt": fmt, "nb": nb}
                    return self._send(202, None, {"Location": f"{world.base}/v1/operations/{op}", "Retry-After": "0", "x-ms-operation-id": op})
                if len(parts) >= 3 and parts[:2] == ["v1", "operations"]:
                    op = world._ops.get(parts[2])
                    if op is None:
                        return self._send(404, {"errorCode": "OperationNotFound"})
                    if len(parts) == 4 and parts[3] == "result" and "parts" in op:
                        return self._send(200, {"definition": {"parts": world._encode_parts(op["parts"])}})
                    if len(parts) == 4 and parts[3] == "result":
                        nb, fmt = op["nb"], op["fmt"]
                        text = nb["git"] if fmt == "fabricGitSource" else nb["ipynb"]
                        if not text:
                            return self._send(400, {"errorCode": "InvalidFormat", "message": f"format {fmt} is not available for this item"})
                        name = "notebook-content.py" if fmt == "fabricGitSource" else "notebook-content.ipynb"
                        platform = json.dumps({"metadata": {"type": "Notebook", "displayName": "From Fabric"}})
                        return self._send(200, {"definition": {"parts": [
                            {"path": name, "payload": base64.b64encode(text.encode()).decode(), "payloadType": "InlineBase64"},
                            {"path": ".platform", "payload": base64.b64encode(platform.encode()).decode(), "payloadType": "InlineBase64"}]}})
                    op["polls"] += 1
                    if world.fail_operation:
                        return self._send(200, {"status": "Failed", "error": {"errorCode": "Boom", "message": "the definition could not be produced"}})
                    if op["polls"] <= world.lro_polls:
                        return self._send(200, {"status": "Running", "percentComplete": 40}, {"Retry-After": "0"})
                    return self._send(200, {"status": "Succeeded"}, {"Location": f"{world.base}/v1/operations/{parts[2]}/result"})
                # ---- OneLake: creating a directory (idempotent in OneLake for Files/ and Tables/)
                if method == "PUT" and q.get("resource") == ["directory"]:
                    if self._bearer("https://storage.azure.com") is None:
                        return self._send(401, {"error": {"code": "InvalidAuthenticationInfo", "message": "bad token"}})
                    world.dir_puts.append(u.path)
                    return self._send(201, None)
                # ---- OneLake (ADLS Gen2 style): GET /<workspace>?resource=filesystem&recursive=false
                if method == "GET" and len(parts) == 1 and q.get("resource") == ["filesystem"]:
                    if self._bearer("https://storage.azure.com") is None:
                        return self._send(401, {"error": {"code": "InvalidAuthenticationInfo", "message": "bad token"}})
                    if q.get("directory") and world.onelake_status == 200:
                        return world._onelake_list(self, parts[0], q)
                    if world.onelake_status != 200:
                        return self._send(world.onelake_status, {"error": {"code": "AuthorizationPermissionMismatch", "message": "This request is not authorized to perform this operation using this permission."}})
                    return self._send(200, {"paths": []})
                self._send(404, {"error": "not found", "path": u.path})

        return H


class FakeNotebookUtils:
    """Just enough of Fabric's ``notebookutils`` for the SDK: ``runtime.context``, ``notebook.getDefinition/exit``, ``credentials.getSecret/getToken``."""

    class NotebookExit(BaseException):
        def __init__(self, value: str) -> None:
            super().__init__(value)
            self.value = value

    def __init__(self, *, context: dict[str, Any], definitions: dict[str, dict[str | None, str]] | None = None, secrets: dict[tuple[str, str], str] | None = None,
                 tokens: dict[str, str] | None = None, definition_signature: str = "kwargs") -> None:
        outer = self
        self.exits: list[str] = []
        self.secret_calls: list[tuple[str, str]] = []
        self.token_calls: list[str] = []
        self.definition_calls: list[tuple[Any, ...]] = []
        self._definitions = definitions or {}
        self._secrets = secrets or {}
        self._tokens = tokens or {}
        self.signature = definition_signature  # kwargs | positional | none (getDefinition raises) | dict

        ctx_copy = dict(context)

        class Runtime:
            context = ctx_copy

        class Notebook:
            @staticmethod
            def getDefinition(name: str, workspaceId: str = "", format: str | None = None) -> Any:  # noqa: A002, N803
                outer.definition_calls.append((name, workspaceId, format))
                if outer.signature == "none":
                    raise RuntimeError("getDefinition is not available in this runtime")
                by_format = outer._definitions.get(name, {})
                text = by_format.get(format) if format in by_format else by_format.get(None)
                if text is None:
                    raise RuntimeError(f"format {format!r} is not supported")
                if outer.signature == "dict":
                    return {"definition": {"parts": [{"path": "notebook-content.py" if format == "fabricGitSource" else "notebook-content.ipynb",
                                                      "payload": base64.b64encode(text.encode()).decode(), "payloadType": "InlineBase64"}]}}
                return text

            @staticmethod
            def exit(value: str) -> None:
                outer.exits.append(value)
                raise FakeNotebookUtils.NotebookExit(value)

        class Credentials:
            @staticmethod
            def getSecret(vault: str, name: str) -> str:  # noqa: N802
                outer.secret_calls.append((vault, name))
                try:
                    return outer._secrets[(vault, name)]
                except KeyError:
                    raise RuntimeError(f"secret {name} not found in {vault}") from None

            @staticmethod
            def getToken(audience: str) -> str:  # noqa: N802
                outer.token_calls.append(audience)
                try:
                    return outer._tokens[audience]
                except KeyError:
                    raise RuntimeError(f"audience {audience} is not supported") from None

        self.runtime, self.notebook, self.credentials = Runtime, Notebook, Credentials
