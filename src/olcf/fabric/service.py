# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""``FabricService``: the control-plane side of "run this Fabric notebook on the on-prem fabric" (registered as service ``fabric``).

The submit pipeline (``POST /v1/integrations/fabric/submit``)::

    verify signature + nonce + window  ->  schema  ->  secret scan  ->  tenant / workspace mapping  ->  acquire source (inline or pull)
      ->  analyze (pass 1)  ->  resolve data plane (lakehouses)  ->  analyze (pass 2, knows where /lakehouse/default points)
      ->  blockers? reject before anything runs  ->  JobService.submit(kind="capsule", origin="fabric")  ->  record + audit + events

Every step either passes or rejects with a stable error code; rejections are recorded (``fabric_submissions.state = 'rejected'``), audited and published as
``fabric.submission.rejected`` so an operator can see *why* Fabric callers fail. Nothing from the capsule is trusted for authorisation: the bearer token identifies the caller,
the administrator's workspace mapping decides where it may run, and the analyzer policy (pip/shell) comes from the administrator's configuration, never from the capsule.
"""

from __future__ import annotations

import json
import logging
import secrets as _stdsecrets
import threading
import time
from dataclasses import dataclass, field
from typing import Any
from urllib.parse import urlsplit

from pydantic import ValidationError
from sqlalchemy import select
from sqlalchemy.exc import IntegrityError

from olcf.core.context import AppContext
from olcf.core.errors import (
    BadRequest,
    CompatibilityBlocked,
    FailureCategory,
    Forbidden,
    NotFound,
    OlcfError,
    Unavailable,
    ValidationFailed,
)
from olcf.core.models import (
    JobKind,
    JobSource,
    JobSubmission,
    Origin,
    OriginKind,
    PolicySpec,
    ResourceRequest,
    RuntimeRequest,
)
from olcf.core.redaction import redact_obj, redact_text
from olcf.core.timeutil import now_iso
from olcf.db.util import fetch_all, fetch_one, insert_row, update_row
from olcf.fabric import notebook as nbm
from olcf.fabric import schema as S
from olcf.fabric.analyzer import AnalysisOptions, analyze_text
from olcf.fabric.auth import FabricAuth
from olcf.fabric.capsule import (
    Capsule,
    CapsuleContext,
    CapsulePolicy,
    CapsuleSource,
    DataReferences,
    RuntimeRequestModel,
    find_secrets,
    normalise_context,
)
from olcf.fabric.config import FabricConfig, parse_config
from olcf.fabric.diagnostics import DiagnosticsInput, run_diagnostics
from olcf.fabric.entra import EntraTokenError, TokenBroker
from olcf.fabric.errors import (
    CapsuleContainsSecret,
    CapsuleExpired,
    CapsuleReplay,
    CapsuleSignatureInvalid,
    CapsuleTooLarge,
    FabricNotConfigured,
    NonceInvalid,
    TenantNotAllowed,
    TokenInvalid,
    WorkspaceNotAllowed,
)
from olcf.fabric.http import Network
from olcf.fabric.onelake import DataPlan, DataPlane
from olcf.fabric.rest import FabricApiError, FabricRestClient
from olcf.fabric.signing import ChallengeService
from olcf.runtime_lib.notebookutils.naming import secret_env_name
from olcf.security.rbac import Binding, Principal
from olcf.version import COMPAT_RULESET_VERSION, __version__

log = logging.getLogger("olcf.fabric")

ADAPTER_VERSION = f"olcf-fabric-adapter/{__version__}"
TERMINAL = ("succeeded", "failed", "cancelled", "timed_out")
_HIGH = "high"

#: rejection class -> failure category recorded for the operator's dashboards
_CATEGORY: list[tuple[type[Exception], str]] = [
    (CompatibilityBlocked, FailureCategory.COMPATIBILITY.value),
    (CapsuleSignatureInvalid, FailureCategory.AUTHN.value), (NonceInvalid, FailureCategory.AUTHN.value), (CapsuleReplay, FailureCategory.AUTHN.value),
    (CapsuleExpired, FailureCategory.AUTHN.value), (TokenInvalid, FailureCategory.AUTHN.value),
    (TenantNotAllowed, FailureCategory.AUTHZ.value), (WorkspaceNotAllowed, FailureCategory.AUTHZ.value), (CapsuleContainsSecret, FailureCategory.AUTHZ.value),
    (Forbidden, FailureCategory.AUTHZ.value), (CapsuleTooLarge, FailureCategory.RESOURCE.value),
    (FabricNotConfigured, FailureCategory.INTERNAL.value), (Unavailable, FailureCategory.PROVIDER.value),
]


def _category_of(exc: Exception) -> str:
    for cls, cat in _CATEGORY:
        if isinstance(exc, cls):
            return cat
    return FailureCategory.USER_CODE.value if isinstance(exc, (ValidationFailed, BadRequest)) else FailureCategory.INTERNAL.value


@dataclass
class Parts:
    """The pieces of a capsule (or of a ``/validate`` request) the pipeline works with."""

    source: CapsuleSource
    context: CapsuleContext
    runtime: RuntimeRequestModel
    parameters: dict[str, Any]
    data: DataReferences
    policy: CapsulePolicy
    identity: dict[str, Any] = field(default_factory=dict)
    secret_refs: list[dict[str, str]] = field(default_factory=list)
    submission_id: str | None = None
    idempotency_key: str | None = None
    correlation_id: str | None = None
    sdk_version: str | None = None

    @property
    def ctx_norm(self) -> dict[str, Any]:
        return self.context.normalized or normalise_context(self.context.raw or {})


@dataclass
class Plan:
    """Everything decided before a job is created."""

    parts: Parts
    cfg: FabricConfig
    tenant_id: str | None
    fabric_workspace_id: str | None
    olcf_workspace_id: str
    source_format: str
    source_text: str
    original: dict[str, Any] | None
    analysis: Any  # AnalysisResult
    data: DataPlan
    secret_env: dict[str, str]
    lakehouse_info: dict[str, Any] | None
    excluded_cells: list[int]
    warnings: list[str] = field(default_factory=list)


def _extract(obj: Any, *path: str) -> Any:
    for p in path:
        if not isinstance(obj, dict):
            return None
        obj = obj.get(p)
    return obj


def _trunc(v: Any, n: int = 200) -> Any:
    if isinstance(v, str):
        return v if len(v) <= n else v[:n] + "..."
    if isinstance(v, (int, float, bool)) or v is None:
        return v
    s = json.dumps(v, default=str)
    return s if len(s) <= n else s[:n] + "..."


def _is_submit_cell(cell: nbm.Cell) -> bool:
    src = cell.source or ""
    return "olcf_fabric" in src and ("run_remote(" in src or ".extend(" in src)


class FabricService:
    """Configuration, signing, analysis, submission and reporting for the Fabric integration."""

    def __init__(self, ctx: AppContext) -> None:
        self.ctx = ctx
        self.adapter_version = ADAPTER_VERSION
        self._cfg_cache: tuple[float, FabricConfig] | None = None
        self._cfg_lock = threading.Lock()
        self._master_cache: bytes | None = None
        self._master_lock = threading.Lock()
        self._broker_cache: tuple[tuple[Any, ...], TokenBroker] | None = None
        self.signing = ChallengeService(ctx, self.signing_material)
        self.auth = FabricAuth(ctx, self.config, self.network)
        self.data = DataPlane(ctx)
        self._unsubscribe: Any = None
        #: tests may inject an ``httpx.Client`` factory for outbound calls (local Fabric/Entra/OneLake test servers)
        self.client_factory: Any = None

    # ================================================================== configuration
    def _load_row(self) -> dict[str, Any] | None:
        with self.ctx.db.connect() as conn:
            return fetch_one(conn, select(S.fabric_config).where(S.fabric_config.c.id == "default"))

    def config(self) -> FabricConfig:
        with self._cfg_lock:
            if self._cfg_cache and time.monotonic() - self._cfg_cache[0] < 2.0:
                return self._cfg_cache[1]
        row = self._load_row()
        doc = dict((row or {}).get("config") or {})
        env = self.ctx.settings.fabric
        if not doc.get("allowed_tenants") and env.allowed_tenants:
            doc["allowed_tenants"] = list(env.allowed_tenants)
        if not doc.get("audiences") and env.audiences:
            doc["audiences"] = list(env.audiences)
        if "enabled" not in doc:
            doc["enabled"] = bool(env.enabled)
        try:
            cfg = parse_config(doc)
        except ValidationFailed:
            log.exception("stored Fabric configuration is invalid; using the strict defaults")
            cfg = FabricConfig(enabled=False)
        with self._cfg_lock:
            self._cfg_cache = (time.monotonic(), cfg)
        return cfg

    def invalidate_config(self) -> None:
        with self._cfg_lock:
            self._cfg_cache = None
        self._broker_cache = None

    def put_config(self, principal: Principal, doc: dict[str, Any]) -> dict[str, Any]:
        principal.require("integration.fabric.manage")
        cfg = parse_config({k: v for k, v in doc.items() if k != "network_effective"})      # computed read-only field: lets a GET result be PUT back unchanged
        self._validate_references(cfg)
        data = cfg.model_dump(mode="json")
        now = now_iso()
        with self.ctx.db.begin() as conn:
            if conn.execute(select(S.fabric_config.c.id).where(S.fabric_config.c.id == "default")).first():
                update_row(conn, S.fabric_config, "default", {"config": data, "updated_at": now, "updated_by": principal.id})
            else:
                insert_row(conn, S.fabric_config, {"id": "default", "config": data, "signing_secret_id": None, "updated_at": now, "updated_by": principal.id})
        self.invalidate_config()
        self.ctx.audit.record(principal, "fabric.config.updated", resource_type="fabric_integration", resource_id="default",
                              details={"allowed_tenants": cfg.allowed_tenants, "audiences": cfg.audiences, "workspaces": [m.fabric_workspace_id for m in cfg.workspace_map],
                                       "broker_connection": cfg.broker_connection, "enabled": cfg.enabled})
        return self.config_public()

    def config_public(self) -> dict[str, Any]:
        """Configuration (never a secret), the readiness checklist and the counters: the document of ``GET /v1/integrations/fabric``."""
        from olcf.fabric.metrics import counters_summary, readiness

        cfg = self.config()
        pub = cfg.model_dump(mode="json")
        pub["network_effective"] = self.network(cfg).describe()
        return {"config": pub, "readiness": readiness(self, cfg), "counters": counters_summary(self.ctx), "adapter_version": self.adapter_version, "public_url": self.public_url(),
                "endpoints": {"challenge": "/v1/integrations/fabric/challenge", "submit": "/v1/integrations/fabric/submit", "validate": "/v1/integrations/fabric/validate",
                              "handshake": "/v1/integrations/fabric/handshake"}}

    def _validate_references(self, cfg: FabricConfig) -> None:
        for m in cfg.workspace_map:
            try:
                self.ctx.service("workspaces").resolve(m.olcf_workspace)
            except NotFound:
                raise ValidationFailed(f"the OLCF workspace '{m.olcf_workspace}' in workspace_map does not exist", remediation="create it first (POST /v1/workspaces)") from None
        if cfg.broker_connection:
            try:
                conn = self.ctx.service("connections").get(cfg.broker_connection)
            except NotFound:
                raise ValidationFailed(f"the broker connection '{cfg.broker_connection}' does not exist") from None
            if conn["type"] != "onelake":
                raise ValidationFailed(f"the broker connection must have type 'onelake' (it is '{conn['type']}')")
        for g in cfg.secret_grants:
            try:
                self.ctx.secrets.get_meta(g.secret_id)
            except OlcfError:
                raise ValidationFailed(f"the secret '{g.secret_id}' granted for {g.vault}/{g.name} does not exist") from None
        for field_name in ("authority_host", "fabric_api", "onelake_dfs"):
            url = getattr(cfg.network, field_name)
            if url:
                u = urlsplit(url)
                if u.scheme != "https" and not (u.scheme == "http" and u.hostname in ("localhost", "127.0.0.1", "::1")):
                    raise ValidationFailed(f"network.{field_name} must be an https URL (plain http is accepted for loopback test doubles only)")
        if cfg.network.proxy and "@" in cfg.network.proxy.split("://", 1)[-1].split("/", 1)[0]:
            raise ValidationFailed("network.proxy must not contain credentials (they would be stored in the configuration)",
                                   remediation="put the credentials in the HTTPS_PROXY environment variable of the control-plane process")
        if cfg.network.ca_bundle:
            from pathlib import Path

            if not Path(cfg.network.ca_bundle).is_file():
                raise ValidationFailed(f"network.ca_bundle '{cfg.network.ca_bundle}' does not exist on the control-plane host")

    def network(self, cfg: FabricConfig | None = None) -> Network:
        cfg = cfg or self.config()
        return Network.from_config({k: v for k, v in cfg.network.model_dump().items() if v is not None})

    def public_url(self) -> str:
        return self.ctx.settings.base_url

    # ================================================================== signing material
    def signing_material(self) -> bytes:
        """The key-derivation master for capsule signing. Stored as an OLCF secret; created on first use."""
        if self._master_cache is not None:
            return self._master_cache
        with self._master_lock:
            if self._master_cache is not None:
                return self._master_cache
            row = self._load_row()
            sid = (row or {}).get("signing_secret_id")
            value: str | None = None
            if sid:
                try:
                    value = str(self.ctx.secrets.resolve(sid))
                except OlcfError:
                    value = None
            if not value:
                value = _stdsecrets.token_hex(32)
                meta = self.ctx.secrets.create("fabric-capsule-signing", value, meta={"purpose": "Fabric capsule key derivation (managed by the Fabric integration)"})
                with self.ctx.db.begin() as conn:
                    if row is None:
                        insert_row(conn, S.fabric_config, {"id": "default", "config": {}, "signing_secret_id": meta["id"], "updated_at": now_iso(), "updated_by": None})
                    else:
                        update_row(conn, S.fabric_config, "default", {"signing_secret_id": meta["id"]})
            self._master_cache = bytes.fromhex(value)
            return self._master_cache

    def rotate_signing(self, principal: Principal) -> dict[str, Any]:
        principal.require("integration.fabric.manage")
        row = self._load_row()
        sid = (row or {}).get("signing_secret_id")
        self.signing_material()
        row = self._load_row()
        sid = (row or {}).get("signing_secret_id")
        new = _stdsecrets.token_hex(32)
        self.ctx.secrets.rotate(sid, new)
        self._master_cache = bytes.fromhex(new)
        self.ctx.audit.record(principal, "fabric.signing.rotated", resource_type="fabric_integration", resource_id="default", details={})
        return {"rotated": True, "note": "outstanding challenges are no longer valid; SDKs obtain a new key on their next call"}

    # ================================================================== broker / Fabric REST
    def broker(self, cfg: FabricConfig | None = None) -> tuple[TokenBroker | None, str | None]:
        """``(broker, problem)``: the token broker for the configured broker connection, or why there is none."""
        cfg = cfg or self.config()
        if not cfg.broker_connection:
            return None, "no broker connection is configured"
        try:
            conn = self.ctx.service("connections").get(cfg.broker_connection)
        except NotFound:
            return None, f"broker connection {cfg.broker_connection} no longer exists"
        c = conn.get("config") or {}
        if (c.get("auth") or "service_principal") != "service_principal":
            return None, "the broker connection must use auth=service_principal"
        if not conn.get("secret_id"):
            return None, "the broker connection has no secret"
        try:
            meta = self.ctx.secrets.get_meta(conn["secret_id"])
            key = (conn["id"], conn.get("updated_at"), meta.get("version"), repr(self.network(cfg).describe()))
            if self._broker_cache and self._broker_cache[0] == key:
                return self._broker_cache[1], None
            secret = self.ctx.secrets.resolve(conn["secret_id"])
            broker = TokenBroker(tenant_id=c.get("tenant_id") or "", client_id=c.get("client_id") or "", secret=secret, network=self.network(cfg), client_factory=self.client_factory)
        except EntraTokenError as exc:
            return None, str(exc)
        except OlcfError as exc:
            return None, f"the broker secret cannot be read: {exc.message}"
        self._broker_cache = (key, broker)
        return broker, None

    def rest_client(self, cfg: FabricConfig | None = None) -> FabricRestClient:
        cfg = cfg or self.config()
        broker, problem = self.broker(cfg)
        if broker is None:
            raise FabricNotConfigured(f"pull mode needs the broker identity: {problem}", remediation="set broker_connection to an OLCF 'onelake' connection holding the service principal")
        return FabricRestClient(broker, self.network(cfg), client_factory=self.client_factory)

    def _known_secrets(self) -> list[str]:
        """Exact secret values that must never appear in a capsule (the broker service principal's secret material)."""
        cfg = self.config()
        out: list[str] = []
        try:
            if cfg.broker_connection:
                conn = self.ctx.service("connections").get(cfg.broker_connection)
                if conn.get("secret_id"):
                    val = self.ctx.secrets.resolve(conn["secret_id"])
                    vals = [val] if isinstance(val, str) else [v for v in val.values() if isinstance(v, str)]
                    out.extend(v for v in vals if len(v) >= 8)
        except OlcfError:
            pass
        return out

    # ================================================================== analysis
    def _options(self, cfg: FabricConfig, *, family: str | None, lakehouse: dict[str, Any] | None, bundled: dict[str, str] | None = None,
                 notebook_name: str | None = None) -> AnalysisOptions:
        return AnalysisOptions(runtime_family=family or str(cfg.default_runtime.get("family") or "fabric-2.0-compatible"),
                               allow_pip_install=cfg.default_policy.allow_pip_install, allow_shell=cfg.default_policy.allow_shell, lakehouse=lakehouse,
                               bundled_notebooks=bundled or {}, notebook_name=notebook_name)

    def analyze(self, text: str, *, format: str | None = None, runtime_family: str | None = None, origin: dict[str, Any] | None = None) -> dict[str, Any]:  # noqa: A002
        """The hook ``JobService`` calls for Fabric-shaped sources. The administrator's policy decides pip/shell; the origin carries the lakehouse facts."""
        cfg = self.config()
        lake = _extract(origin or {}, "details", "fabric", "lakehouse")
        opts = self._options(cfg, family=runtime_family, lakehouse=lake if isinstance(lake, dict) else None)
        return analyze_text(text, format=format, options=opts).report

    def analyze_document(self, text: str, *, format: str | None = None, lakehouse_kind: str | None = None, runtime_family: str | None = None) -> dict[str, Any]:  # noqa: A002
        """``POST /analyze``: a what-if analysis of pasted or uploaded content (no job, no data plane)."""
        cfg = self.config()
        lake = {"kind": lakehouse_kind, "root_path": "/lakehouse-mount-placeholder" if lakehouse_kind == "local" else None, "name": "default"} if lakehouse_kind in ("local", "remote") else None
        return analyze_text(text, format=format, options=self._options(cfg, family=runtime_family, lakehouse=lake)).report

    # ================================================================== challenge / handshake / diagnostics
    def challenge(self, principal: Principal) -> dict[str, Any]:
        self._gate(principal)
        out = self.signing.issue(principal.id)
        out.update({"adapterVersion": self.adapter_version, "serverVersion": __version__, "hints": self.config().sdk_hints, "compatRulesetVersion": COMPAT_RULESET_VERSION})
        return out

    def _gate(self, principal: Principal, *, perms: tuple[str, ...] = ("integration.fabric.submit",)) -> None:
        """The caller must hold at least one of ``perms`` somewhere (org-wide or in some workspace)."""
        for p in perms:
            vis = principal.visible_workspaces(p)
            if vis is None or vis:
                return
        raise Forbidden(f"permission '{perms[0]}' is required", details={"permission": perms[0]})

    def handshake(self, principal: Principal, body: dict[str, Any], *, ip: str | None = None) -> dict[str, Any]:
        """Record what the SDK's ``test_connection()`` saw: proof that a Fabric notebook reached this control plane."""
        self._gate(principal)
        cfg = self.config()
        ctxn = normalise_context(_extract(body, "context", "normalized") or body.get("context") or {})
        tenant = principal.claims.get("tid") or _extract(body, "identity", "tenantId")
        fws = ctxn.get("currentWorkspaceId")
        mapping = cfg.workspace_for(fws, tenant)
        problems: list[str] = []
        if cfg.allowed_tenants and tenant and str(tenant).lower() not in cfg.allowed_tenants:
            problems.append(f"tenant {tenant} is not in allowed_tenants")
        if fws and mapping is None:
            problems.append(f"Fabric workspace {fws} is not mapped to an OLCF workspace")
        if not fws:
            problems.append("the Fabric workspace id was not seen (is this a Fabric notebook session?)")
        olcf_ws = self._olcf_ws_id(mapping.olcf_workspace) if mapping else None
        if olcf_ws and not principal.can("integration.fabric.submit", workspace_id=olcf_ws):
            problems.append("this credential cannot submit to the mapped OLCF workspace")
        checks = body.get("checks") if isinstance(body.get("checks"), list) else []
        ok = not problems and all(c.get("ok", False) for c in checks if isinstance(c, dict))
        hid = "hsk_" + _stdsecrets.token_hex(10)
        row = {"id": hid, "received_at": now_iso(), "principal_id": principal.id, "ok": ok, "tenant_id": tenant, "fabric_workspace_id": fws,
               "fabric_workspace_name": ctxn.get("currentWorkspaceName"), "notebook_id": ctxn.get("currentNotebookId"), "notebook_name": ctxn.get("currentNotebookName"),
               "user": {"name": ctxn.get("userName"), "id": ctxn.get("userId")}, "sdk_version": _trunc(body.get("sdkVersion"), 40),
               "latency_ms": float(body["latencyMs"]) if isinstance(body.get("latencyMs"), (int, float)) else None,
               "checks": redact_obj(checks)[:40], "context_keys": sorted(k for k, v in ctxn.items() if v not in (None, "")), "server_version": __version__, "source_ip": ip}
        with self.ctx.db.begin() as conn:
            insert_row(conn, S.fabric_handshakes, row)
        self.ctx.events.publish("fabric.handshake", {"ok": ok, "tenant_id": tenant, "fabric_workspace_id": fws, "notebook_id": row["notebook_id"], "sdk_version": row["sdk_version"],
                                                     "problems": problems}, workspace_id=olcf_ws, subject=hid)
        self.ctx.audit.record(principal, "fabric.handshake", resource_type="fabric_handshake", resource_id=hid, workspace_id=olcf_ws, outcome="success" if ok else "failure",
                              details={"fabric_workspace_id": fws, "problems": problems})
        return {"ok": ok, "handshakeId": hid, "serverVersion": __version__, "adapterVersion": self.adapter_version, "serverTime": now_iso(), "authorized": not problems,
                "problems": problems, "olcfWorkspace": olcf_ws, "principal": {"id": principal.id, "name": principal.name, "kind": principal.kind},
                "contextKeysSeen": row["context_keys"], "hints": cfg.sdk_hints}

    def last_handshake(self) -> dict[str, Any] | None:
        with self.ctx.db.connect() as conn:
            return fetch_one(conn, select(S.fabric_handshakes).order_by(S.fabric_handshakes.c.received_at.desc()).limit(1))

    def diagnose(self, principal: Principal) -> dict[str, Any]:
        """``POST /test``: DNS -> TLS -> proxy -> Entra token -> Fabric API -> OneLake -> inbound path."""
        principal.require("integration.fabric.manage")
        cfg = self.config()
        broker, problem = self.broker(cfg)
        inp = DiagnosticsInput(network=self.network(cfg), broker=broker, broker_problem=problem, fabric_workspace_ids=[m.fabric_workspace_id for m in cfg.workspace_map],
                               allowed_tenants=cfg.allowed_tenants, audiences=cfg.audiences, public_url=self.public_url(), last_handshake=self.last_handshake(),
                               client_factory=self.client_factory)
        result = run_diagnostics(inp)
        self.ctx.audit.record(principal, "fabric.diagnostics.run", resource_type="fabric_integration", resource_id="default", outcome="success" if result["ok"] else "failure",
                              details={"failed": result["failed"]})
        return result

    # ================================================================== helpers
    def _olcf_ws_id(self, ref: str) -> str | None:
        try:
            return self.ctx.service("workspaces").resolve(ref)["id"]
        except NotFound:
            return None

    @staticmethod
    def _parts_from_capsule(c: Capsule) -> Parts:
        return Parts(source=c.source, context=c.context, runtime=c.runtime, parameters=c.parameters, data=c.data_references, policy=c.policy, identity=dict(c.security.identity),
                     secret_refs=[{"vault": r.vault, "name": r.name} for r in c.security.secret_refs], submission_id=c.submission_id, idempotency_key=c.idempotency_key,
                     correlation_id=c.security.correlation_id, sdk_version=c.sdk_version)

    @staticmethod
    def _parts_from_request(body: dict[str, Any]) -> Parts:
        try:
            sec = body.get("security") if isinstance(body.get("security"), dict) else {}
            return Parts(source=CapsuleSource.model_validate(body.get("source") or {}), context=CapsuleContext.model_validate(body.get("context") or {}),
                         runtime=RuntimeRequestModel.model_validate(body.get("runtime") or {}), parameters=dict(body.get("parameters") or {}),
                         data=DataReferences.model_validate(body.get("dataReferences") or {}), policy=CapsulePolicy.model_validate(body.get("policy") or {}),
                         identity=dict(sec.get("identity") or {}), secret_refs=[r for r in (sec.get("secretRefs") or []) if isinstance(r, dict) and r.get("vault") and r.get("name")],
                         sdk_version=_trunc(body.get("sdkVersion"), 40))
        except ValidationError as exc:
            raise _schema_error(exc) from None

    # ================================================================== the plan (shared by /validate and /submit)
    def _acquire(self, cfg: FabricConfig, parts: Parts, fabric_ws: str | None) -> tuple[str, str, dict[str, Any] | None]:
        """``(format, text, original)``: inline content or a pulled definition."""
        src = parts.source
        if src.mode == "pull":
            nb_id = src.notebook_id or parts.ctx_norm.get("currentNotebookId")
            if not (nb_id and fabric_ws):
                raise ValidationFailed("pull mode needs source.notebookId and source.workspaceId", remediation="send the Fabric notebook and workspace ids, or submit the content inline")
            client = self.rest_client(cfg)
            last: FabricApiError | None = None
            for fmt in [("fabric_git_source" if f.lower() == "fabricgitsource" else f) for f in cfg.sdk_hints.get("definition_formats", ["fabricGitSource", "ipynb"])] or ["ipynb"]:
                try:
                    d = client.get_definition(fabric_ws, nb_id, fmt, timeout_seconds=cfg.pull_timeout_seconds)
                    return d.format, d.content, {"format": d.format, "sha256": nbm.sha256_text(d.content), "pulled": True, "parts": d.parts}
                except FabricApiError as exc:
                    last = exc
                    if exc.status not in (400, 422):
                        break
            assert last is not None
            raise _fabric_error(last)
        if not src.content:
            raise ValidationFailed("source.content is empty", remediation="send the notebook content (mode 'inline') or the notebook ids (mode 'pull')")
        text = src.content
        if src.sha256 and nbm.sha256_text(text) != src.sha256:
            raise BadRequest("source.sha256 does not match source.content")
        return src.format, text, ({"format": src.original.format, "sha256": src.original.sha256} if src.original else None)

    def _tenant_and_workspace(self, cfg: FabricConfig, principal: Principal, parts: Parts) -> tuple[str | None, str | None, str]:
        token_tid = principal.claims.get("tid")
        claimed = parts.identity.get("tenantId") or parts.identity.get("tid")
        tenant = str(token_tid or claimed or "").lower() or None
        if cfg.allowed_tenants:
            if token_tid is None and tenant is None:
                raise TenantNotAllowed("the capsule does not say which Entra tenant it comes from", remediation="the SDK reads the tenant from notebookutils.runtime.context; use a recent SDK")
            if tenant not in cfg.allowed_tenants:
                raise TenantNotAllowed(f"tenant {tenant} is not allowed to submit", remediation="ask an administrator to add the tenant (PUT /v1/integrations/fabric allowed_tenants)")
            if token_tid and claimed and str(claimed).lower() != str(token_tid).lower():
                raise TenantNotAllowed("the tenant claimed in the capsule differs from the tenant of the authenticated token",
                                       details={"token_tenant": token_tid, "capsule_tenant": claimed})
        fws = (parts.source.workspace_id or parts.ctx_norm.get("currentWorkspaceId") or "")
        fws = str(fws).lower() or None
        mapping = cfg.workspace_for(fws, tenant)
        if mapping is None:
            raise WorkspaceNotAllowed(f"Fabric workspace {fws or '(unknown)'} is not mapped to an OLCF workspace", remediation="map it with PUT /v1/integrations/fabric (workspace_map)",
                                      details={"fabric_workspace_id": fws})
        olcf_ws = self._olcf_ws_id(mapping.olcf_workspace)
        if olcf_ws is None:
            raise FabricNotConfigured(f"the mapped OLCF workspace '{mapping.olcf_workspace}' no longer exists", remediation="fix workspace_map")
        principal.require("integration.fabric.submit", workspace_id=olcf_ws)
        return tenant, fws, olcf_ws

    def _plan(self, principal: Principal, parts: Parts, *, dry_run: bool) -> Plan:
        cfg = self.config()
        if not cfg.enabled:
            raise FabricNotConfigured("the Fabric integration is disabled", remediation="enable it (PUT /v1/integrations/fabric enabled=true)")
        tenant, fws, olcf_ws = self._tenant_and_workspace(cfg, principal, parts)
        fmt, text, original = self._acquire(cfg, parts, fws)
        family = parts.runtime.family or str(cfg.default_runtime.get("family") or "fabric-2.0-compatible")
        # the submit cell must never run remotely (it would recurse): drop it defensively even if the SDK already did
        excluded = list(parts.source.excluded_cells)
        try:
            nb0 = nbm.parse(text, fmt)
        except BadRequest:
            raise
        except Exception as exc:  # noqa: BLE001
            raise ValidationFailed(f"the notebook could not be parsed: {exc}", remediation="send a valid ipynb, Fabric Git source (.py) or percent-format .py") from exc
        removed = nbm.drop_cells(nb0, _is_submit_cell)
        if removed:
            excluded.extend(removed)
            text, fmt = nbm.serialise(nb0, nb0.format), nb0.format
        bundled = {n: d.content for n, d in parts.source.bundled.items()}
        name = parts.source.notebook_name or parts.ctx_norm.get("currentNotebookName")
        # pass 1: find the data references; pass 2 (below) knows where the default lakehouse lives
        first = analyze_text(text, format=fmt, options=self._options(cfg, family=family, lakehouse=None, bundled=bundled, notebook_name=name))
        logical = list(parts.data.logical)
        ctxn = dict(parts.ctx_norm)
        if not (ctxn.get("defaultLakehouseId") or ctxn.get("defaultLakehouseName")):
            hint = nb0.default_lakehouse_hint()
            if hint:
                ctxn.update({"defaultLakehouseId": hint.get("id"), "defaultLakehouseName": hint.get("name"), "defaultLakehouseWorkspaceId": hint.get("workspace_id")})
        data = self.data.plan(cfg, olcf_workspace_id=olcf_ws, tenant_id=tenant, context=ctxn, logical=logical, analysis=first.report, current_workspace_id=fws, dry_run=dry_run,
                              actor=self._internal(principal, olcf_ws))
        lake = self.data.default_lakehouse_info(data.default_item) if data.default_item else None
        final = analyze_text(text, format=fmt, options=self._options(cfg, family=family, lakehouse=lake, bundled=bundled, notebook_name=name))
        # secrets: references only; the administrator's grants decide what is readable
        refs = {(r["vault"], r["name"]) for r in parts.secret_refs}
        refs |= {(r["vault"], r["name"]) for r in final.report.get("secret_refs", []) if r.get("vault") and r.get("name") and not r.get("dynamic")}
        secret_env: dict[str, str] = {}
        missing = []
        for vault, nm in sorted(refs):
            grant = cfg.grant_for(vault, nm)
            if grant is None:
                missing.append(f"{vault}/{nm}")
            else:
                secret_env[secret_env_name(vault, nm)] = grant.secret_id
        warnings = []
        if missing:
            warnings.append("secret references without an administrator grant: " + ", ".join(missing))
        return Plan(parts=parts, cfg=cfg, tenant_id=tenant, fabric_workspace_id=fws, olcf_workspace_id=olcf_ws, source_format=fmt, source_text=text, original=original, analysis=final,
                    data=data, secret_env=secret_env, lakehouse_info=lake, excluded_cells=sorted(set(excluded)), warnings=warnings)

    @staticmethod
    def _internal(principal: Principal, olcf_workspace_id: str) -> Principal:
        """The caller's identity narrowed to what the adapter itself needs on its behalf (scope ``fabric-internal``, one Contributor binding in the mapped workspace)."""
        return Principal(id=principal.id, name=principal.name, kind=principal.kind, source=principal.source, claims=principal.claims, ip=principal.ip,
                         correlation_id=principal.correlation_id, display_name=principal.display_name, token_scopes=["fabric-internal"],
                         bindings=[Binding("Contributor", "workspace", olcf_workspace_id, "fabric-internal")])

    # ================================================================== validate
    def validate(self, principal: Principal, body: dict[str, Any]) -> dict[str, Any]:
        """``POST /validate``: everything ``/submit`` checks except the signature, without creating a job. Blockers are reported, not raised."""
        self._gate(principal)
        parts = self._parts_from_request(body)
        plan = self._plan(principal, parts, dry_run=True)
        report = plan.analysis.report
        issues: list[dict[str, Any]] = []
        for p in plan.data.problems:
            issues.append({"code": "lakehouse_not_attachable", **p})
        for w in plan.warnings:
            issues.append({"code": "secret_not_granted", "reason": w, "remediation": "add a secret_grants entry (vault, name -> OLCF secret) in the integration settings"})
        secret_hits = [f.to_dict() for f in find_secrets(parts.parameters) + find_secrets(plan.source_text, known=self._known_secrets()) if f.severity == _HIGH]
        if secret_hits:
            issues.append({"code": "capsule_contains_secret", "reason": "credential-like content was found in the notebook or parameters", "findings": secret_hits,
                           "remediation": "use notebookutils.credentials.getSecret(vault, name) and an administrator grant instead of literals"})
        ok = not report["blocked"] and not plan.data.problems and not secret_hits and not any(i["code"] == "secret_not_granted" for i in issues)
        return {"ok": ok, "blocked": bool(report["blocked"]), "compatibility": report, "issues": issues, "plan": {"olcfWorkspaceId": plan.olcf_workspace_id,
                "fabricWorkspaceId": plan.fabric_workspace_id, "tenantId": plan.tenant_id, "runtimeFamily": parts.runtime.family, "dataPlane": plan.data.to_public(),
                "sourceFormat": plan.source_format, "excludedCells": plan.excluded_cells}, "adapterVersion": self.adapter_version}

    # ================================================================== submit
    def submit(self, principal: Principal, body: dict[str, Any], *, ip: str | None = None, raw_bytes: int | None = None) -> dict[str, Any]:
        t0 = time.perf_counter()
        self._gate(principal)
        peek = _peek(body)
        try:
            return self._submit(principal, body, ip=ip, raw_bytes=raw_bytes, t0=t0)
        except OlcfError as exc:
            self._record_rejection(principal, peek, exc, ip=ip, raw_bytes=raw_bytes)
            raise

    def _submit(self, principal: Principal, body: dict[str, Any], *, ip: str | None, raw_bytes: int | None, t0: float) -> dict[str, Any]:
        verified = self.signing.verify(principal.id, body)
        if verified["replay"] is not None:
            return self._public(self._sync(verified["replay"]), replayed=True)
        try:
            capsule = Capsule.model_validate(body["capsule"])
        except ValidationError as exc:
            raise _schema_error(exc) from None
        parts = self._parts_from_capsule(capsule)
        findings = find_secrets(body["capsule"], known=self._known_secrets())
        high = [f for f in findings if f.severity == _HIGH]
        if high:
            raise CapsuleContainsSecret("the capsule contains credential-like content and was rejected", details={"findings": [f.to_dict() for f in high]},
                                        remediation=("remove the credential from the notebook and use notebookutils.credentials.getSecret(vault, name) with an administrator "
                                                     "grant; the broker service principal secret never belongs in a notebook"))
        plan = self._plan(principal, parts, dry_run=False)
        report = plan.analysis.report
        if report["blocked"]:
            raise CompatibilityBlocked("the notebook uses constructs that OLCF cannot run: " + ", ".join(sorted({b["rule"] for b in report["blockers"]})[:8]),
                                       details={"report": _compat_public(report), "blockers": report["blockers"][:50]},
                                       remediation="see the compatibility report; replace or remove the blocking constructs, then resubmit")
        if plan.data.problems:
            p0 = plan.data.problems[0]
            raise ValidationFailed(f"cannot attach the Fabric lakehouse '{p0['lakehouse']}': {p0['reason']}", remediation=p0.get("remediation"), details={"problems": plan.data.problems})
        if any(w.startswith("secret references") for w in plan.warnings):
            raise ValidationFailed(plan.warnings[0], remediation="add secret_grants entries (vault, name -> OLCF secret) in the integration settings")
        sub = self._build_submission(plan, capsule)
        internal = self._internal(principal, plan.olcf_workspace_id)
        row = self._insert_submission(principal, capsule, plan, verified["digest"], raw_bytes, ip)
        self.ctx.events.publish("fabric.submission.received", {"submission_id": row["id"], "notebook": row["notebook_name"], "fabric_workspace_id": row["fabric_workspace_id"]},
                                workspace_id=plan.olcf_workspace_id, subject=row["id"])
        self.ctx.events.publish("fabric.submission.validated", {"submission_id": row["id"], "blocked": False, "counts": report["counts"]}, workspace_id=plan.olcf_workspace_id, subject=row["id"])
        try:
            job = self.ctx.service("jobs").submit(internal, sub, correlation_id=capsule.security.correlation_id or principal.correlation_id)
        except OlcfError as exc:
            self._finish_row(row["id"], state="rejected", category=_category_of(exc), error=f"[{exc.code}] {exc.message}")
            self.ctx.events.publish("fabric.submission.rejected", {"submission_id": row["id"], "code": exc.code, "reason": exc.message[:300]}, workspace_id=plan.olcf_workspace_id, subject=row["id"])
            self.ctx.audit.record(principal, "fabric.submission.rejected", resource_type="fabric_submission", resource_id=row["id"], workspace_id=plan.olcf_workspace_id, outcome="failure",
                                  details={"code": exc.code, "message": exc.message[:300], "stage": "job-submit"})
            raise
        with self.ctx.db.begin() as conn:
            update_row(conn, S.fabric_submissions, row["id"], {"job_id": job.id, "state": job.state, "updated_at": now_iso()})
        self.ctx.audit.record(principal, "fabric.submission.accepted", resource_type="fabric_submission", resource_id=row["id"], workspace_id=plan.olcf_workspace_id,
                              details={"job_id": job.id, "notebook_id": row["notebook_id"], "fabric_workspace_id": row["fabric_workspace_id"], "tenant_id": plan.tenant_id,
                                       "capsule_sha256": verified["digest"], "source_sha256": parts.source.sha256, "compat": row["compat_summary"]})
        fresh = self._sync(self._get_row(row["id"]))
        return self._public(fresh, replayed=False)

    def _build_submission(self, plan: Plan, capsule: Capsule) -> JobSubmission:
        cfg, parts = plan.cfg, plan.parts
        dp = cfg.default_policy
        pol = parts.policy
        timeout = min(x for x in (pol.timeout_seconds, dp.timeout_seconds) if x) if (pol.timeout_seconds or dp.timeout_seconds) else None
        res_in = {k: v for k, v in (pol.resources or {}).items() if k in ResourceRequest.model_fields}
        if dp.max_executors and res_in.get("max_executors"):
            res_in["max_executors"] = min(int(res_in["max_executors"]), dp.max_executors)
        if dp.max_executor_memory_mb and res_in.get("executor_memory_mb"):
            res_in["executor_memory_mb"] = min(int(res_in["executor_memory_mb"]), dp.max_executor_memory_mb)
        try:
            resources = ResourceRequest.model_validate(res_in)
            policy = PolicySpec(queue=pol.queue or dp.queue, priority=min(pol.priority if pol.priority is not None else dp.priority, dp.priority), timeout_seconds=timeout,
                                retries=min(pol.retries or dp.retries, 3), retention_days=pol.retention_days if pol.retention_days is not None else dp.retention_days,
                                placement=dict(pol.placement or {}), allow_pip_install=bool(dp.allow_pip_install and pol.allow_pip_install))
        except ValidationError as exc:
            raise _schema_error(exc) from None
        rt_in = parts.runtime
        constraints = {k: v for k, v in (rt_in.constraints or {}).items() if k in ("spark", "python", "java", "scala") and isinstance(v, str)}
        engine = str((rt_in.constraints or {}).get("engine") or cfg.default_runtime.get("engine") or "spark")
        if engine not in ("spark", "python"):
            raise ValidationFailed(f"runtime.constraints.engine must be 'spark' or 'python' (got '{engine}')")
        runtime = RuntimeRequest(family=rt_in.family or str(cfg.default_runtime.get("family") or "fabric-2.0-compatible"), runtime_id=cfg.default_runtime.get("runtime_id"),
                                 dependency_lock=rt_in.dependency_lock,
                                 spark_conf={str(k): str(v) for k, v in (rt_in.spark_conf or {}).items()}, **constraints)
        report = plan.analysis.report
        ipynb = plan.analysis.source("ipynb")
        ctxn = parts.ctx_norm
        name = (parts.source.notebook_name or ctxn.get("currentNotebookName") or "fabric-notebook")
        fabric_origin = {"tenant_id": plan.tenant_id, "workspace_id": plan.fabric_workspace_id, "workspace_name": parts.source.workspace_name or ctxn.get("currentWorkspaceName"),
                         "notebook_id": parts.source.notebook_id or ctxn.get("currentNotebookId"), "notebook_name": name, "run_id": ctxn.get("currentRunId"),
                         "parent_run_id": ctxn.get("parentRunId"), "is_for_pipeline": bool(ctxn.get("isForPipeline")), "submission_id": capsule.submission_id,
                         "sdk_version": capsule.sdk_version, "user": {"name": ctxn.get("userName"), "id": ctxn.get("userId")}, "context": ctxn, "lakehouse": plan.lakehouse_info,
                         "default_lakehouse": {"id": ctxn.get("defaultLakehouseId"), "name": ctxn.get("defaultLakehouseName"), "workspace_id": ctxn.get("defaultLakehouseWorkspaceId")}}
        return JobSubmission(
            kind=JobKind.CAPSULE, workspace_id=plan.olcf_workspace_id, name=f"{name} (Fabric)"[:200],
            source=JobSource(format="ipynb", content=ipynb, filename=f"{name}.ipynb", sha256=nbm.sha256_text(ipynb)),
            parameters=json.loads(json.dumps(parts.parameters, default=str)), default_lakehouse=plan.data.default_lakehouse, data_refs=plan.data.data_refs, runtime=runtime,
            resources=resources, policy=policy, origin=Origin(kind=OriginKind.FABRIC, details={"fabric": fabric_origin, "submission_id": capsule.submission_id,
                                                                                               "caller": {"name": None, "tenant_id": plan.tenant_id}}),
            engine=engine, idempotency_key=capsule.idempotency_key, compatibility=_compat_public(report), parent_run_id=ctxn.get("parentRunId") if ctxn.get("isForPipeline") else None,
            secret_refs=dict(plan.secret_env), tags={"fabric.submission": capsule.submission_id, "fabric.notebook": str(fabric_origin["notebook_id"] or "")[:60]})

    # ================================================================== persistence
    def _insert_submission(self, principal: Principal, capsule: Capsule, plan: Plan, digest: str, raw_bytes: int | None, ip: str | None) -> dict[str, Any]:
        ctxn = plan.parts.ctx_norm
        report = plan.analysis.report
        row = {"id": capsule.submission_id, "received_at": now_iso(), "principal_id": principal.id, "idempotency_key": capsule.idempotency_key, "tenant_id": plan.tenant_id,
               "fabric_workspace_id": plan.fabric_workspace_id, "fabric_workspace_name": plan.parts.source.workspace_name or ctxn.get("currentWorkspaceName"),
               "notebook_id": plan.parts.source.notebook_id or ctxn.get("currentNotebookId"), "notebook_name": plan.parts.source.notebook_name or ctxn.get("currentNotebookName"),
               "default_lakehouse": {"id": ctxn.get("defaultLakehouseId"), "name": ctxn.get("defaultLakehouseName"), "workspace_id": ctxn.get("defaultLakehouseWorkspaceId")},
               "run_id": ctxn.get("currentRunId"), "parent_run_id": ctxn.get("parentRunId"), "is_for_pipeline": bool(ctxn.get("isForPipeline")),
               "caller": {"principal_id": principal.id, "name": principal.name, "kind": principal.kind, "source": principal.source, "ip": ip,
                          "claims": {k: principal.claims.get(k) for k in ("tid", "oid", "appid", "azp", "upn") if principal.claims.get(k)}},
               "sdk_version": capsule.sdk_version, "adapter_version": capsule.adapter_version, "source_mode": plan.parts.source.mode,
               "capsule_bytes": int(raw_bytes if raw_bytes is not None else len(json.dumps(capsule.model_dump(by_alias=True, mode="json")))), "capsule_sha256": digest,
               "parameters": {str(k): _trunc(redact_obj({str(k): v})[str(k)]) for k, v in list(plan.parts.parameters.items())[:200]},
               "compatibility": redact_obj(_compat_public(report)), "compat_summary": _compat_summary(report), "olcf_workspace_id": plan.olcf_workspace_id, "job_id": None,
               "state": "validated", "failure_category": None, "error": None, "round_trip_ms": None, "secret_findings": [], "correlation_id": capsule.security.correlation_id,
               "finished_at": None, "updated_at": now_iso()}
        try:
            with self.ctx.db.begin() as conn:
                insert_row(conn, S.fabric_submissions, row)
        except IntegrityError:
            with self.ctx.db.connect() as conn:
                existing = fetch_one(conn, select(S.fabric_submissions).where(S.fabric_submissions.c.principal_id == principal.id,
                                                                              S.fabric_submissions.c.idempotency_key == capsule.idempotency_key))
            if existing is not None and existing["capsule_sha256"] != digest:
                raise CapsuleReplay("this idempotency key was already used for a different capsule", remediation="use a new idempotency key for a changed notebook") from None
            raise CapsuleReplay("this submission was already recorded", details={"submission_id": capsule.submission_id}) from None
        return row

    def _record_rejection(self, principal: Principal, peek: dict[str, Any], exc: OlcfError, *, ip: str | None, raw_bytes: int | None) -> None:
        sid = "sub_rej_" + _stdsecrets.token_hex(10)
        cat = _category_of(exc)
        try:
            row = {"id": sid, "received_at": now_iso(), "principal_id": principal.id, "idempotency_key": f"rejected:{sid}", "tenant_id": peek.get("tenant_id"),
                   "fabric_workspace_id": peek.get("fabric_workspace_id"), "fabric_workspace_name": peek.get("fabric_workspace_name"), "notebook_id": peek.get("notebook_id"),
                   "notebook_name": peek.get("notebook_name"), "default_lakehouse": None, "run_id": peek.get("run_id"), "parent_run_id": None, "is_for_pipeline": False,
                   "caller": {"principal_id": principal.id, "name": principal.name, "kind": principal.kind, "source": principal.source, "ip": ip,
                              "claimed_submission_id": peek.get("submission_id")}, "sdk_version": peek.get("sdk_version"), "adapter_version": peek.get("adapter_version"),
                   "source_mode": peek.get("source_mode") or "inline", "capsule_bytes": int(raw_bytes or 0), "capsule_sha256": None, "parameters": {}, "compatibility": None,
                   "compat_summary": None, "olcf_workspace_id": None, "job_id": None, "state": "rejected", "failure_category": cat,
                   "error": redact_text(f"[{exc.code}] {exc.message}")[:1000], "round_trip_ms": None, "secret_findings": (exc.details or {}).get("findings"),
                   "correlation_id": peek.get("correlation_id"), "finished_at": now_iso(), "updated_at": now_iso()}
            if isinstance(exc, CompatibilityBlocked):
                rep = (exc.details or {}).get("report")
                row["compatibility"] = redact_obj(rep) if rep else None
                row["compat_summary"] = _compat_summary(rep) if rep else None
            with self.ctx.db.begin() as conn:
                insert_row(conn, S.fabric_submissions, row)
        except Exception:  # noqa: BLE001 - recording a rejection must never mask it
            log.exception("could not record the rejected submission")
        self.ctx.events.publish("fabric.submission.rejected", {"submission_id": sid, "code": exc.code, "category": cat, "reason": exc.message[:300]}, subject=sid)
        self.ctx.audit.record(principal, "fabric.submission.rejected", resource_type="fabric_submission", resource_id=sid, outcome="failure",
                              details={"code": exc.code, "message": redact_text(exc.message)[:300], "category": cat, "notebook_id": peek.get("notebook_id"),
                                       "fabric_workspace_id": peek.get("fabric_workspace_id")})

    def _get_row(self, sid: str) -> dict[str, Any]:
        with self.ctx.db.connect() as conn:
            row = fetch_one(conn, select(S.fabric_submissions).where(S.fabric_submissions.c.id == sid))
        if row is None:
            raise NotFound(f"submission {sid} not found")
        return row

    def _finish_row(self, sid: str, *, state: str, category: str | None, error: str | None, exit_ok: bool = False) -> None:
        row = self._get_row(sid)
        from olcf.fabric.capsule import parse_ts

        rt = None
        try:
            rt = max(0.0, (parse_ts(now_iso()) - parse_ts(row["received_at"])).total_seconds() * 1000)
        except ValueError:
            pass
        with self.ctx.db.begin() as conn:
            update_row(conn, S.fabric_submissions, sid, {"state": state, "failure_category": category, "error": redact_text(error)[:1000] if error else None, "finished_at": now_iso(),
                                                         "round_trip_ms": rt, "updated_at": now_iso()})

    # ================================================================== job state
    def _sync(self, row: dict[str, Any]) -> dict[str, Any]:
        """Bring a submission row up to date with its job (idempotent; cheap)."""
        if not row.get("job_id") or row["state"] in TERMINAL or row["state"] == "rejected":
            return row
        jobs = self.ctx.maybe_service("jobs")
        if jobs is None:
            return row
        try:
            jr = jobs.get_row(row["job_id"])
        except NotFound:
            return row
        if jr["state"] == row["state"]:
            return row
        vals: dict[str, Any] = {"state": jr["state"], "updated_at": now_iso()}
        done = jr["state"] in TERMINAL
        if done:
            fail = jr.get("failure") if isinstance(jr.get("failure"), dict) else {}
            if jr["state"] == "succeeded":
                cat = None
            else:
                cat = (fail or {}).get("category") or jr.get("failure_category") or {"timed_out": "timeout", "cancelled": "cancellation"}.get(jr["state"])
            vals.update({"failure_category": cat, "error": redact_text(str((fail or {}).get("message") or ""))[:1000] or None, "finished_at": jr.get("finished_at") or now_iso()})
            from olcf.fabric.capsule import parse_ts

            try:
                vals["round_trip_ms"] = max(0.0, (parse_ts(vals["finished_at"]) - parse_ts(row["received_at"])).total_seconds() * 1000)
            except ValueError:
                pass
        with self.ctx.db.begin() as conn:
            changed = conn.execute(S.fabric_submissions.update().where(S.fabric_submissions.c.id == row["id"], S.fabric_submissions.c.state != jr["state"]).values(**vals)).rowcount
        fresh = self._get_row(row["id"])
        if done and changed:
            self.ctx.events.publish("fabric.submission.completed", {"submission_id": row["id"], "job_id": row["job_id"], "state": jr["state"], "failure_category": vals.get("failure_category"),
                                                                  "round_trip_ms": vals.get("round_trip_ms")}, workspace_id=row.get("olcf_workspace_id"), subject=row["id"])
        return fresh

    def on_event(self, evt: dict[str, Any]) -> None:
        """EventBus subscriber: keep submissions in step with their jobs."""
        if evt.get("type") != "job.state_changed":
            return
        job_id = (evt.get("payload") or {}).get("job_id")
        if not job_id:
            return
        try:
            with self.ctx.db.connect() as conn:
                row = fetch_one(conn, select(S.fabric_submissions).where(S.fabric_submissions.c.job_id == job_id))
            if row:
                self._sync(row)
        except Exception:  # noqa: BLE001
            log.exception("could not sync Fabric submission for job %s", job_id)

    def reconcile(self) -> int:
        """Sync every non-terminal submission (covers events missed during a restart)."""
        with self.ctx.db.connect() as conn:
            rows = fetch_all(conn, select(S.fabric_submissions).where(S.fabric_submissions.c.job_id.is_not(None), S.fabric_submissions.c.state.not_in(list(TERMINAL) + ["rejected"])).limit(500))
        n = 0
        for r in rows:
            if self._sync(r)["state"] != r["state"]:
                n += 1
        return n

    # ================================================================== reads
    def _public(self, row: dict[str, Any], *, replayed: bool | None = None, detail: bool = False) -> dict[str, Any]:
        out = {k: row[k] for k in ("id", "received_at", "state", "failure_category", "error", "round_trip_ms", "tenant_id", "fabric_workspace_id", "fabric_workspace_name", "notebook_id",
                                   "notebook_name", "run_id", "parent_run_id", "is_for_pipeline", "sdk_version", "adapter_version", "source_mode", "capsule_bytes", "capsule_sha256",
                                   "olcf_workspace_id", "job_id", "compat_summary", "finished_at", "correlation_id")}
        out["parameters"] = row["parameters"]
        out["caller"] = {k: v for k, v in (row.get("caller") or {}).items() if k != "ip"}
        if replayed is not None:
            out["replayed"] = replayed
        if detail:
            out["compatibility"] = row.get("compatibility")
            out["default_lakehouse"] = row.get("default_lakehouse")
        job = None
        if row.get("job_id"):
            jobs = self.ctx.maybe_service("jobs")
            if jobs is not None:
                try:
                    jr = jobs.get_row(row["job_id"])
                    rec = jobs.to_record(jr)
                    job = {"id": rec.id, "state": rec.state, "exit_value": rec.exit_value, "failure": rec.failure.model_dump(mode="json") if rec.failure else None,
                           "started_at": rec.started_at, "finished_at": rec.finished_at, "queued_at": rec.queued_at, "result": redact_obj(rec.result or {}), "attempt": rec.attempt}
                except NotFound:
                    job = None
        out["job"] = job
        out["links"] = {"self": f"/v1/integrations/fabric/submissions/{row['id']}", "logs": f"/v1/integrations/fabric/submissions/{row['id']}/logs",
                        "cancel": f"/v1/integrations/fabric/submissions/{row['id']}/cancel"}
        return redact_obj(out)

    def _authorize_row(self, principal: Principal, row: dict[str, Any], perm: str = "job.read") -> None:
        ws = row.get("olcf_workspace_id")
        if principal.can("integration.fabric.read"):
            return
        if ws and principal.can(perm, workspace_id=ws):
            return
        raise NotFound(f"submission {row['id']} not found")  # do not reveal other workspaces' submissions

    def get_submission(self, principal: Principal, sid: str) -> dict[str, Any]:
        row = self._get_row(sid)
        self._authorize_row(principal, row)
        return self._public(self._sync(row), detail=True)

    def list_submissions(self, principal: Principal, *, limit: int = 50, state: str | None = None, notebook_id: str | None = None, fabric_workspace_id: str | None = None,
                         before: str | None = None) -> list[dict[str, Any]]:
        self._gate(principal, perms=("integration.fabric.read", "job.read"))
        stmt = select(S.fabric_submissions).order_by(S.fabric_submissions.c.received_at.desc(), S.fabric_submissions.c.id.desc()).limit(min(max(limit, 1), 500))
        if state:
            stmt = stmt.where(S.fabric_submissions.c.state == state)
        if notebook_id:
            stmt = stmt.where(S.fabric_submissions.c.notebook_id == notebook_id)
        if fabric_workspace_id:
            stmt = stmt.where(S.fabric_submissions.c.fabric_workspace_id == fabric_workspace_id.lower())
        if before:
            stmt = stmt.where(S.fabric_submissions.c.received_at < before)
        if not principal.can("integration.fabric.read"):
            vis = principal.visible_workspaces("job.read")
            if vis is not None:
                if not vis:
                    return []
                stmt = stmt.where(S.fabric_submissions.c.olcf_workspace_id.in_(list(vis)))
        with self.ctx.db.connect() as conn:
            rows = fetch_all(conn, stmt)
        return [self._public(self._sync(r)) for r in rows]

    def submission_logs(self, principal: Principal, sid: str, *, offset: int = 0, limit: int = 65536) -> dict[str, Any]:
        row = self._get_row(sid)
        self._authorize_row(principal, row)
        if not row.get("job_id"):
            return {"submission_id": sid, "data": "", "offset": offset, "next_offset": offset, "eof": True, "size": 0}
        page = self.ctx.service("jobs").logs_page(row["job_id"], offset=offset, limit=limit)
        page = redact_obj(page)
        page["submission_id"] = sid
        page["state"] = self._sync(row)["state"]
        return page

    def submission_output(self, principal: Principal, sid: str, *, max_bytes: int = 262144) -> dict[str, Any]:
        """Condensed cell output of the executed notebook (``output.ipynb``): stream text, text results and errors per cell, redacted and size-limited.

        The job log carries the runner's own messages; what the notebook *printed* is recorded by the kernel in the output notebook, so the SDK reads it here.
        """
        from olcf.compute.jobs_api import artifact_path

        row = self._get_row(sid)
        self._authorize_row(principal, row)
        empty: dict[str, Any] = {"submission_id": sid, "available": False, "cells": [], "truncated": False}
        if not row.get("job_id"):
            return empty
        path = artifact_path(self.ctx, row["job_id"], "output.ipynb")
        if not path.is_file() or path.stat().st_size > 64 * 1024 * 1024:
            return empty
        try:
            nb = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, ValueError):
            return empty
        cells: list[dict[str, Any]] = []
        used = 0
        truncated = False
        for idx, cell in enumerate(nb.get("cells") or []):
            if cell.get("cell_type") != "code":
                continue
            parts: list[dict[str, str]] = []
            for out in cell.get("outputs") or []:
                kind = out.get("output_type")
                if kind == "stream":
                    text, label = out.get("text", ""), out.get("name", "stdout")
                elif kind in ("execute_result", "display_data"):
                    text, label = (out.get("data") or {}).get("text/plain", ""), "result"
                elif kind == "error":
                    text, label = f"{out.get('ename', '')}: {out.get('evalue', '')}", "error"
                else:
                    continue
                text = "".join(text) if isinstance(text, list) else str(text)
                if not text:
                    continue
                if used + len(text) > max_bytes:
                    text, truncated = text[: max(0, max_bytes - used)], True
                used += len(text)
                parts.append({"stream": label, "text": redact_text(text)})
                if truncated:
                    break
            if parts:
                cells.append({"index": idx, "outputs": parts})
            if truncated:
                break
        return {"submission_id": sid, "available": True, "cells": cells, "truncated": truncated}

    def cancel_submission(self, principal: Principal, sid: str) -> dict[str, Any]:
        row = self._get_row(sid)
        self._authorize_row(principal, row, "job.cancel")
        if not row.get("job_id"):
            raise BadRequest("this submission has no job to cancel")
        principal.require("job.cancel", workspace_id=row["olcf_workspace_id"])
        self.ctx.service("jobs").cancel(principal, row["job_id"], reason="cancelled from Fabric")
        self.ctx.audit.record(principal, "fabric.submission.cancelled", resource_type="fabric_submission", resource_id=sid, workspace_id=row["olcf_workspace_id"], details={"job_id": row["job_id"]})
        return self._public(self._sync(self._get_row(sid)))

    # ================================================================== lifecycle
    def start(self) -> None:
        self._unsubscribe = self.ctx.events.subscribe(self.on_event)
        try:
            self.reconcile()
        except Exception:  # noqa: BLE001
            log.exception("initial Fabric submission reconcile failed")

    def stop(self) -> None:
        if self._unsubscribe:
            self._unsubscribe()
            self._unsubscribe = None


# ===================================================================================== module-level helpers
def _schema_error(exc: ValidationError) -> ValidationFailed:
    errs = [{"field": ".".join(str(p) for p in e["loc"]), "message": e["msg"]} for e in exc.errors()][:20]
    return ValidationFailed("the capsule does not match the schema (schemas/capsule-v1.json)", details={"errors": errs}, remediation="update the olcf-fabric SDK, or fix the listed fields")


def _fabric_error(exc: FabricApiError) -> OlcfError:
    details = {"status": exc.status, "error_code": exc.error_code, "request_id": exc.request_id}
    if exc.status in (401, 403):
        return Forbidden(f"the Fabric API refused the broker identity: {exc}", details=details, remediation=exc.remediation)
    if exc.status == 404:
        return NotFound(f"the notebook was not found in Fabric: {exc}", details=details, remediation=exc.remediation)
    return Unavailable(f"the Fabric API call failed: {exc}", details=details, remediation=exc.remediation or "retry; see POST /v1/integrations/fabric/test")


def _peek(body: Any) -> dict[str, Any]:
    """Untrusted, best-effort facts for the rejection record (never used for decisions)."""
    c = body.get("capsule") if isinstance(body, dict) else None
    c = c if isinstance(c, dict) else {}
    ctxn = _extract(c, "context", "normalized") or {}
    return {"submission_id": _trunc(c.get("submissionId"), 40), "tenant_id": _trunc(_extract(c, "security", "identity", "tenantId"), 64),
            "fabric_workspace_id": _trunc(_extract(c, "source", "workspaceId") or ctxn.get("currentWorkspaceId"), 64),
            "fabric_workspace_name": _trunc(_extract(c, "source", "workspaceName") or ctxn.get("currentWorkspaceName"), 200),
            "notebook_id": _trunc(_extract(c, "source", "notebookId") or ctxn.get("currentNotebookId"), 64),
            "notebook_name": _trunc(_extract(c, "source", "notebookName") or ctxn.get("currentNotebookName"), 200), "run_id": _trunc(ctxn.get("currentRunId"), 120),
            "sdk_version": _trunc(c.get("sdkVersion"), 40), "adapter_version": _trunc(c.get("adapterVersion"), 80), "source_mode": _trunc(_extract(c, "source", "mode"), 8),
            "correlation_id": _trunc(_extract(c, "security", "correlationId"), 200)}


def _compat_public(report: dict[str, Any] | None) -> dict[str, Any]:
    """The report as stored with the job and returned to callers (findings capped so one huge notebook cannot bloat the record)."""
    if not report:
        return {}
    out = dict(report)
    f = out.get("findings") or []
    if len(f) > 500:
        out["findings"] = f[:500]
        out["findings_truncated"] = len(f) - 500
    return out


def _compat_summary(report: dict[str, Any] | None) -> dict[str, Any] | None:
    if not report:
        return None
    return {"analyzer_version": report.get("analyzer_version"), "ruleset_version": report.get("ruleset_version"), "counts": report.get("counts"), "blocked": bool(report.get("blocked")),
            "blockers": sorted({b.get("rule") for b in (report.get("blockers") or [])}), "shims_required": report.get("shims_required"), "rewrites": len(report.get("rewrites") or []),
            "cells": (report.get("cells") or {}).get("total")}


__all__ = ["ADAPTER_VERSION", "FabricService", "Plan"]
