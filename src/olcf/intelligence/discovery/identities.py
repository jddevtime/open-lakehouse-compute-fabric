# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Connector identities: how a discovery source names the credentials it uses, and how they are resolved without ever being stored in a source row.

A Microsoft Fabric / Power BI source has up to two identities (design note: "metadata discovery and data sampling use separate identities"):

``metadata_identity``
    The service principal the scanner uses to read *metadata* (workspaces, items, definitions, lakehouse tables, the Power BI admin scanner). Required.
``sampling_identity``
    An optional, separate service principal used **only** to read data for sampling / value profiling (Delta files in OneLake). Absent means sampling of Fabric data is impossible,
    whatever the sampling policy says.

Each identity block in ``source.config`` is one of

* ``{"connection_id": "con_..."}`` - an existing platform connection with ``auth = service_principal`` (``tenant_id`` / ``client_id`` in its config and its secret), the same
  mechanism the Fabric integration's broker uses; or
* ``{"tenant_id": "...", "client_id": "...", "secret_id": "sec_..."}`` - ids plus a *reference* to a secret in the platform secret store. The credential itself
  (``{"client_secret": ...}`` or ``{"private_key_pem": ..., "certificate_pem": ...}``) is accepted only as write-only input of the API (``credentials`` /
  ``sampling_credentials``), is stored with :class:`~olcf.security.secrets.SecretStore` and is never returned, logged or audited.

:func:`resolve_secrets` turns the blocks into in-memory :class:`~olcf.intelligence.discovery.base.ResolvedSecrets` for the duration of one scan or test.
"""

from __future__ import annotations

import logging
from collections.abc import Mapping
from typing import Any

from olcf.core.errors import NotFound, OlcfError, ValidationFailed
from olcf.fabric.http import Network
from olcf.intelligence.discovery import netguard
from olcf.intelligence.discovery.base import ResolvedSecrets, remember_secret, safe_message

log = logging.getLogger("olcf.intelligence.discovery.identities")

IDENTITY_NAMES = ("metadata_identity", "sampling_identity")
#: Network endpoint overrides a Fabric source may carry (sovereign clouds, regional endpoints, private link, test doubles). Setting them needs ``integration.fabric.manage``.
NETWORK_KEYS = ("authority_host", "fabric_api", "powerbi_api", "onelake_dfs", "proxy", "ca_bundle", "timeout_seconds", "trust_env")
SECRET_FIELDS = ("client_secret", "private_key_pem", "certificate_pem")


def check_credentials(creds: Any, *, label: str = "credentials") -> dict[str, str]:
    """Validate write-only credential input: ``{"client_secret"}`` or ``{"private_key_pem", "certificate_pem"}``. Returns the cleaned dict (never logged)."""
    if not isinstance(creds, Mapping):
        raise ValidationFailed(f"{label} must be an object with client_secret, or private_key_pem and certificate_pem")
    extra = sorted(set(creds) - set(SECRET_FIELDS))
    if extra:
        raise ValidationFailed(f"{label} has unknown field(s) {extra}; allowed: {list(SECRET_FIELDS)}")
    out = {k: str(v) for k, v in creds.items() if v not in (None, "")}
    if out.get("client_secret"):
        if "private_key_pem" in out or "certificate_pem" in out:
            raise ValidationFailed(f"{label}: give either client_secret or a certificate (private_key_pem + certificate_pem), not both")
        return {"client_secret": out["client_secret"]}
    if out.get("private_key_pem") and out.get("certificate_pem"):
        return {"private_key_pem": out["private_key_pem"], "certificate_pem": out["certificate_pem"]}
    raise ValidationFailed(f"{label} needs client_secret, or both private_key_pem and certificate_pem")


def store_credentials(ctx: Any, principal: Any, source_name: str, which: str, creds: Mapping[str, Any]) -> str:
    """Store write-only credentials in the secret store and return the ``sec_`` id (the only thing a source row keeps)."""
    clean = check_credentials(creds, label=f"{which} credentials")
    meta = ctx.secrets.create(f"intelligence-source-{source_name}-{which}", clean, scope_type="org", created_by=getattr(principal, "id", None),
                              meta={"purpose": f"OLCF Intelligence discovery source '{source_name}' ({which})"})
    return str(meta["id"])


def delete_secret(ctx: Any, secret_id: str | None) -> None:
    """Best-effort removal of a secret created for a source (a missing secret is fine)."""
    if not secret_id:
        return
    try:
        ctx.secrets.delete(secret_id)
    except OlcfError:
        log.info("secret %s of a discovery source was already gone", secret_id)


def validate_identity_block(ctx: Any, principal: Any, block: Any, *, name: str) -> dict[str, Any]:
    """Check an identity block of a source's config and return it normalised (field-level :class:`ValidationFailed` messages; references are checked to exist).
    A block with ids but no ``secret_id`` is valid here: the credential may arrive with the same request as write-only input and be attached by the caller."""
    if not isinstance(block, Mapping):
        raise ValidationFailed(f"config.{name} must be an object")
    allowed = {"connection_id", "tenant_id", "client_id", "secret_id"}
    extra = sorted(set(block) - allowed)
    if extra:
        raise ValidationFailed(f"config.{name} has unknown field(s) {extra}; allowed: {sorted(allowed)}")
    if block.get("connection_id"):
        cid = str(block["connection_id"])
        try:
            conn = ctx.service("connections").get(cid)
        except NotFound:
            raise ValidationFailed(f"config.{name}.connection_id '{cid}' does not exist", remediation="create the connection first (POST /v1/connections)") from None
        if principal is not None:
            principal.require("connection.read", workspace_id=conn.get("workspace_id"), connection_id=cid)
        cfg = conn.get("config") or {}
        if (cfg.get("auth") or "service_principal") != "service_principal":
            raise ValidationFailed(f"config.{name}.connection_id must reference a connection with auth=service_principal")
        if not conn.get("secret_id"):
            raise ValidationFailed(f"the connection '{cid}' has no secret", remediation="set the service principal's secret on the connection")
        return {"connection_id": cid}
    tenant, client = str(block.get("tenant_id") or ""), str(block.get("client_id") or "")
    if not tenant or not client:
        raise ValidationFailed(f"config.{name} needs connection_id, or tenant_id and client_id")
    out: dict[str, Any] = {"tenant_id": tenant, "client_id": client}
    sid = block.get("secret_id")
    if sid:
        try:
            ctx.secrets.get_meta(str(sid))
        except OlcfError:
            raise ValidationFailed(f"config.{name}.secret_id '{sid}' does not exist") from None
        out["secret_id"] = str(sid)
    return out


def validate_network(principal: Any, net: Any, ctx: Any = None) -> dict[str, Any]:
    """Check ``config.network`` (endpoint overrides). Changing them needs ``integration.fabric.manage`` because the service principal's credential is sent to those hosts.

    Every endpoint must be an https URL without credentials that does not point at a loopback, private, link-local or metadata destination (see :mod:`~olcf.intelligence.discovery.netguard`;
    private destinations and plain http to them are accepted only while the runtime setting ``intelligence.allow_private_endpoints`` is true, which needs ``ctx``, the platform
    context; without it the strict policy applies). A proxy may not carry credentials. A refusal is a :class:`ValidationFailed` (422) with a remediation.
    """
    if net in (None, {}):
        return {}
    if not isinstance(net, Mapping):
        raise ValidationFailed("config.network must be an object")
    extra = sorted(set(net) - set(NETWORK_KEYS))
    if extra:
        raise ValidationFailed(f"config.network has unknown field(s) {extra}; allowed: {list(NETWORK_KEYS)}")
    if principal is not None:
        principal.require("integration.fabric.manage")
    out = {k: v for k, v in net.items() if v not in (None, "")}
    proxy = out.get("proxy")
    if proxy and "@" in str(proxy).split("://", 1)[-1].split("/", 1)[0]:
        raise ValidationFailed("config.network.proxy must not contain credentials", remediation="put them in the HTTPS_PROXY environment variable of the control-plane process")
    try:
        netguard.check_overrides(ctx, out)
    except netguard.EndpointNotAllowed as exc:
        raise ValidationFailed(exc.message, remediation=exc.remediation) from None
    return out


def build_network(ctx: Any, config: Mapping[str, Any]) -> Network:
    """The :class:`~olcf.fabric.http.Network` for a source: its own ``config.network`` overrides on top of the Fabric integration's network settings (when that module is
    installed and configured), on top of the defaults."""
    base: dict[str, Any] = {}
    try:
        fabric = ctx.maybe_service("fabric")
        if fabric is not None:
            base = {k: v for k, v in fabric.config().network.model_dump().items() if v is not None}
    except Exception as exc:  # noqa: BLE001 - an unreadable Fabric configuration never blocks discovery
        log.info("the Fabric integration network settings are not available (%s)", safe_message(exc))
    base.update({k: v for k, v in (config.get("network") or {}).items() if v not in (None, "")})
    return Network.from_config(base)


def resolve_secrets(ctx: Any, source: Mapping[str, Any]) -> ResolvedSecrets:
    """Resolve the identities of ``source`` into in-memory credentials. Problems are collected per identity in ``ResolvedSecrets.errors`` (the connectivity test reports them as
    step 1) instead of being raised. Nothing is logged except identity names and error summaries."""
    out = ResolvedSecrets()
    cfg = source.get("config") or {}
    for name in IDENTITY_NAMES:
        block = cfg.get(name)
        if not block:
            continue
        out.configured.add(name)
        try:
            out.values[name] = _resolve_one(ctx, block, source if name == "metadata_identity" else None)
        except OlcfError as exc:
            out.errors[name] = exc.message
        except Exception as exc:  # noqa: BLE001
            out.errors[name] = f"{type(exc).__name__}: {safe_message(exc)}"
    return out


def _resolve_one(ctx: Any, block: Mapping[str, Any], source: Mapping[str, Any] | None) -> dict[str, Any]:
    if block.get("connection_id"):
        conn = ctx.service("connections").get(str(block["connection_id"]))
        c = conn.get("config") or {}
        if not conn.get("secret_id"):
            raise ValidationFailed(f"the connection '{conn['id']}' has no secret")
        secret = ctx.secrets.resolve(conn["secret_id"])
        tenant, client = str(c.get("tenant_id") or ""), str(c.get("client_id") or "")
    else:
        sid = block.get("secret_id") or (source or {}).get("secret_ref")
        if not sid:
            raise ValidationFailed("no credentials are stored for this identity", remediation="send credentials (client_secret, or private_key_pem + certificate_pem) with the source")
        secret = ctx.secrets.resolve(str(sid))
        tenant, client = str(block.get("tenant_id") or ""), str(block.get("client_id") or "")
    material = {"client_secret": secret} if isinstance(secret, str) else dict(secret)
    for value in material.values():
        remember_secret(value)                                                       # so that an upstream error that echoes it is scrubbed from steps, gaps and logs
    if not tenant or not client:
        raise ValidationFailed("the identity has no tenant_id / client_id")
    return {"tenant_id": tenant, "client_id": client, **material}
