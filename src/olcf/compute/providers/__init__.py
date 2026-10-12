# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Provider registry: built-in provider types, third-party plug-ins and the instances configured in the ``providers`` table.

Plug-ins register through the entry-point group ``olcf.compute_providers``; the entry point must resolve to a
:class:`~olcf.compute.providers.base.ComputeProvider` subclass whose ``type`` is unique::

    [project.entry-points."olcf.compute_providers"]
    my_cloud = "my_pkg.provider:MyCloudProvider"
"""

from __future__ import annotations

import logging
import threading
from importlib import metadata
from typing import Any

from olcf.compute.providers.base import AppStatus, ComputeProvider, Discovery, ProviderEnv
from olcf.compute.providers.external import ExternalProvider
from olcf.compute.providers.fabric import FabricProvider
from olcf.compute.providers.kubernetes import KubernetesProvider
from olcf.compute.providers.local import LocalProvider
from olcf.compute.providers.standalone import StandaloneProvider
from olcf.core.errors import BadRequest, NotFound

log = logging.getLogger("olcf.compute.providers")

ENTRY_POINT_GROUP = "olcf.compute_providers"
BUILTIN_TYPES: tuple[type[ComputeProvider], ...] = (StandaloneProvider, LocalProvider, ExternalProvider, KubernetesProvider, FabricProvider)


class ProviderManager:
    """Builds and caches provider instances from database rows (rebuilt whenever a row's ``etag`` changes)."""

    def __init__(self, env: ProviderEnv, *, load_plugins: bool = True) -> None:
        self.env = env
        self._types: dict[str, type[ComputeProvider]] = {}
        self._instances: dict[str, tuple[str, ComputeProvider]] = {}  # name -> (etag, instance)
        self._lock = threading.RLock()
        for cls in BUILTIN_TYPES:
            self.register_type(cls)
        if load_plugins:
            self.load_entry_points()

    # ---- types ------------------------------------------------------------------------------
    def register_type(self, cls: type[ComputeProvider]) -> None:
        if not (isinstance(cls, type) and issubclass(cls, ComputeProvider)) or not cls.type:
            raise BadRequest(f"{cls!r} is not a ComputeProvider subclass with a non-empty 'type'")
        existing = self._types.get(cls.type)
        if existing is not None and existing is not cls:
            raise BadRequest(f"provider type '{cls.type}' is already registered by {existing.__module__}.{existing.__qualname__}")
        self._types[cls.type] = cls

    def load_entry_points(self) -> list[str]:
        loaded: list[str] = []
        for ep in metadata.entry_points(group=ENTRY_POINT_GROUP):
            try:
                self.register_type(ep.load())
                loaded.append(ep.name)
            except Exception as exc:  # noqa: BLE001 - a broken plug-in must never stop the control plane
                log.warning("provider plug-in '%s' ignored: %s", ep.name, exc)
        return loaded

    def types(self) -> list[dict[str, Any]]:
        return [{"type": t, "execution": c.execution_model, "live_verified": c.live_verified, "capabilities": list(c.capabilities),
                 "builtin": c in BUILTIN_TYPES} for t, c in sorted(self._types.items())]

    # ---- instances -----------------------------------------------------------------------------
    def instantiate(self, ptype: str, name: str, config: dict[str, Any], secret_id: str | None = None) -> ComputeProvider:
        """Create (without caching) a provider and validate its configuration."""
        cls = self._types.get(ptype)
        if cls is None:
            raise BadRequest(f"unknown provider type '{ptype}'", details={"known": sorted(self._types)})
        cfg = dict(config or {})
        if secret_id:
            cfg["secret_id"] = secret_id
        inst = cls(name, cfg, self.env)
        inst.validate_config()
        return inst

    def from_row(self, row: dict[str, Any]) -> ComputeProvider:
        with self._lock:
            hit = self._instances.get(row["name"])
            if hit and hit[0] == row["etag"]:
                return hit[1]
            inst = self.instantiate(row["type"], row["name"], row.get("config") or {}, row.get("secret_id"))
            if hit:
                self._close(hit[1])
            self._instances[row["name"]] = (row["etag"], inst)
            return inst

    def get(self, ref: str) -> ComputeProvider:
        row = self.env.registry.get_provider_row(ref)
        if not row["enabled"]:
            raise NotFound(f"provider '{row['name']}' is disabled")
        return self.from_row(row)

    def all(self) -> list[ComputeProvider]:
        out = []
        for row in self.env.registry.list_providers():
            if not row["enabled"]:
                continue
            try:
                out.append(self.from_row(row))
            except Exception as exc:  # noqa: BLE001
                log.warning("provider '%s' is misconfigured and was skipped: %s", row["name"], exc)
        return out

    def invalidate(self, name: str | None = None) -> None:
        with self._lock:
            for n in [name] if name else list(self._instances):
                hit = self._instances.pop(n, None)
                if hit:
                    self._close(hit[1])

    def default_name(self) -> str:
        rows = self.env.registry.list_providers()
        for r in rows:
            if r["is_default"] and r["enabled"]:
                return str(r["name"])
        want = str(self.env.registry.setting("compute.default_provider", "standalone") or "standalone")
        return want if any(r["name"] == want and r["enabled"] for r in rows) else "standalone"

    def remote_names(self) -> frozenset[str]:
        """Providers that execute outside OLCF worker nodes (``PlannerConfig.remote_providers``)."""
        return frozenset(p.name for p in self.all() if p.execution == "remote")

    # ---- fleet operations --------------------------------------------------------------------
    def discover_all(self) -> list[Discovery]:
        out: list[Discovery] = []
        for p in self.all():
            try:
                out.append(p.discover())
            except Exception as exc:  # noqa: BLE001
                out.append(Discovery(p.name, p.type, False, f"discovery failed: {exc}", list(p.capabilities), p.execution, live_verified=p.live_verified))
        return out

    def reconcile_all(self) -> dict[str, list[AppStatus]]:
        out: dict[str, list[AppStatus]] = {}
        for p in self.all():
            try:
                out[p.name] = p.reconcile()
            except Exception as exc:  # noqa: BLE001
                log.warning("reconcile of provider '%s' failed: %s", p.name, exc)
        return out

    def maintain_all(self) -> None:
        for p in self.all():
            try:
                p.maintain()
            except Exception:  # noqa: BLE001
                log.exception("maintenance of provider '%s' failed", p.name)

    def close(self) -> None:
        self.invalidate()

    @staticmethod
    def _close(p: ComputeProvider) -> None:
        fn = getattr(p, "close", None)
        if callable(fn):
            try:
                fn()
            except Exception:  # noqa: BLE001
                log.debug("provider close failed", exc_info=True)
