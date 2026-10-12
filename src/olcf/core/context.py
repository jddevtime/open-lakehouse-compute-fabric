# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Application context and the module/plug-in contract.

``AppContext`` is the dependency-injection container shared by every service. Feature packages
expose ``install(ctx) -> Module`` from ``olcf.<pkg>.module``; the API factory discovers them through
``olcf.api.modules.MODULES``. Cross-module calls go through ``ctx.service(name)`` so packages stay
decoupled and individually testable (see ``docs/dev/CONTRACTS.md`` for the registered service names).
"""

from __future__ import annotations

import logging
import os
import socket
import time
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

from olcf.core.config import Settings
from olcf.core.errors import Unavailable
from olcf.core.events import EventBus
from olcf.core.health import HealthRegistry
from olcf.core.ids import ulid
from olcf.core.leader import LeaderElector
from olcf.core.ratelimit import RateLimiter
from olcf.db.engine import Database
from olcf.security.audit import AuditLog
from olcf.security.pki import PKI
from olcf.security.rbac import Authorizer
from olcf.security.secrets import (
    FileSecretProvider,
    MasterKey,
    SecretProvider,
    SecretStore,
    VaultSecretProvider,
)
from olcf.security.tokens import SigningKeys, TokenService

if TYPE_CHECKING:  # pragma: no cover
    from fastapi import APIRouter

log = logging.getLogger("olcf.context")


class Module:
    """A feature package plugged into the control plane."""

    name: str = "module"

    def __init__(self) -> None:
        self.routers: list[tuple[APIRouter, str]] = []  # (router, prefix) - prefix relative to /v1 (e.g. "/jobs" or "")
        self.public_routers: list[tuple[APIRouter, str]] = []  # mounted at the app root (e.g. /mcp, /.well-known)

    def start(self) -> None:
        """Called once after migrations (start background threads here)."""

    def stop(self) -> None:
        """Called on shutdown."""


@dataclass
class AppContext:
    """Bundle of the foundation services (settings, database, secrets, audit log, event bus, health registry, token service, authoriser, PKI and others) that routers and modules receive instead of reaching for globals.
    """
    settings: Settings
    db: Database
    secrets: SecretStore
    audit: AuditLog
    events: EventBus
    health: HealthRegistry
    tokens: TokenService
    authz: Authorizer
    pki: PKI
    limiter: RateLimiter
    node_id: str
    leader: LeaderElector
    started_at: float = field(default_factory=time.time)
    services: dict[str, Any] = field(default_factory=dict)
    modules: list[Module] = field(default_factory=list)
    #: hooks keyed by item type: {"validate": fn(definition)->definition, "on_create": fn(item), "on_delete": fn(item),
    #: "export": fn(item)->dict[str, bytes], "import": fn(files)->definition}
    item_hooks: dict[str, dict[str, Any]] = field(default_factory=dict)
    #: connection testers keyed by connection type: fn(ctx, connection_row, secret_value) -> dict(ok, detail, steps)
    connection_testers: dict[str, Any] = field(default_factory=dict)

    def register_service(self, name: str, obj: Any) -> None:
        self.services[name] = obj

    def service(self, name: str) -> Any:
        try:
            return self.services[name]
        except KeyError:
            raise Unavailable(f"service '{name}' is not available in this deployment") from None

    def maybe_service(self, name: str) -> Any | None:
        return self.services.get(name)

    def register_item_hooks(self, item_type: str, **hooks: Any) -> None:
        self.item_hooks.setdefault(item_type, {}).update(hooks)

    @property
    def uptime(self) -> float:
        return time.time() - self.started_at


def build_context(settings: Settings, *, migrate: bool = True) -> AppContext:
    """Construct all foundation services. Feature modules are installed separately (see ``api.app``)."""
    settings.ensure_dirs()
    db = Database(settings.database_url, echo=settings.db.echo, pool_size=settings.db.pool_size)
    if migrate:
        db.migrate()
    master = MasterKey(settings.path("secrets"), use_keyring=settings.security.use_keyring, explicit_file=settings.security.master_key_file)
    providers: dict[str, SecretProvider] = {"file": FileSecretProvider(master)}
    vault_addr = os.environ.get("OLCF_VAULT_ADDR")
    if vault_addr:
        providers["vault"] = VaultSecretProvider(
            vault_addr, mount=os.environ.get("OLCF_VAULT_MOUNT", "secret"), prefix=os.environ.get("OLCF_VAULT_PREFIX", "olcf"),
            namespace=os.environ.get("OLCF_VAULT_NAMESPACE"), verify=os.environ.get("OLCF_VAULT_CACERT", True) or True,
        )
    keys = SigningKeys(settings.path("secrets"), master=master)
    node_id = f"{socket.gethostname()}-{ulid()[-6:].lower()}"
    return AppContext(
        settings=settings, db=db,
        secrets=SecretStore(db, providers, default=settings.security.secrets_provider if settings.security.secrets_provider in providers else "file", master=master),
        audit=AuditLog(db, hash_chain=settings.security.audit_hash_chain),
        events=EventBus(db), health=HealthRegistry(),
        tokens=TokenService(db, keys, access_ttl=settings.auth.access_token_ttl_seconds, refresh_ttl=settings.auth.refresh_token_ttl_seconds),
        authz=Authorizer(db), pki=PKI(settings.path("pki")), limiter=RateLimiter(), node_id=node_id,
        leader=LeaderElector(db, "control-plane", node_id),
    )
