# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Storage provider registry.

Built-in providers (``local``, ``s3``, ``adls``, ``onelake``) are registered eagerly.  Third parties add providers
through the ``olcf.storage_providers`` entry-point group; an entry point may resolve to a
:class:`~olcf.storage.providers.base.StorageProvider` subclass or to a zero-argument factory returning an instance::

    [project.entry-points."olcf.storage_providers"]
    gcs = "my_pkg.gcs:GcsProvider"

Plug-ins cannot replace a built-in provider id (fail closed); a broken plug-in is logged and skipped so it can never
take the platform down.
"""

from __future__ import annotations

import logging
from importlib import metadata

from olcf.core.errors import NotFound
from olcf.storage.providers.adls import AdlsProvider
from olcf.storage.providers.base import StorageProvider
from olcf.storage.providers.local import LocalProvider
from olcf.storage.providers.onelake import OneLakeProvider
from olcf.storage.providers.s3 import S3Provider

log = logging.getLogger("olcf.storage")
ENTRY_POINT_GROUP = "olcf.storage_providers"
BUILTIN: tuple[type[StorageProvider], ...] = (LocalProvider, S3Provider, AdlsProvider, OneLakeProvider)


class ProviderRegistry:
    """Maps provider ids, URI schemes and connection types to provider instances."""

    def __init__(self, *, load_entry_points: bool = True) -> None:
        self._by_id: dict[str, StorageProvider] = {}
        for cls in BUILTIN:
            self.register(cls(), builtin=True)
        self.plugin_errors: dict[str, str] = {}
        if load_entry_points:
            self._load_entry_points()

    def register(self, provider: StorageProvider, *, builtin: bool = False) -> None:
        if provider.id in self._by_id and not builtin:
            raise ValueError(f"storage provider id '{provider.id}' is already registered and cannot be replaced")
        self._by_id[provider.id] = provider

    def _load_entry_points(self) -> None:
        try:
            eps = metadata.entry_points(group=ENTRY_POINT_GROUP)
        except Exception:  # noqa: BLE001
            return
        for ep in eps:
            try:
                obj = ep.load()
                inst = obj() if callable(obj) else obj
                if not isinstance(inst, StorageProvider):
                    raise TypeError(f"{ep.value} did not produce a StorageProvider")
                self.register(inst)
                log.info("loaded storage provider plug-in '%s' from %s", inst.id, ep.value)
            except Exception as exc:  # noqa: BLE001
                self.plugin_errors[ep.name] = f"{type(exc).__name__}: {exc}"
                log.warning("storage provider plug-in %s failed to load: %s", ep.name, exc)

    def get(self, provider_id: str) -> StorageProvider:
        try:
            return self._by_id[provider_id]
        except KeyError:
            raise NotFound(f"storage provider '{provider_id}' is not available", details={"available": sorted(self._by_id)}) from None

    def for_scheme(self, scheme: str) -> StorageProvider:
        for p in self._by_id.values():
            if scheme in p.schemes:
                return p
        raise NotFound(f"no storage provider handles the '{scheme}://' scheme", details={"available": sorted(self._by_id)})

    def for_root(self, root_uri: str) -> StorageProvider:
        """Provider that owns a lakehouse root URI (scheme, refined by host for ADLS vs OneLake)."""
        for p in self._by_id.values():
            if p.claims(root_uri):
                return p
        raise NotFound(f"no storage provider handles the root '{root_uri.split('?')[0]}'", details={"available": sorted(self._by_id)})

    def for_connection_type(self, ctype: str) -> StorageProvider | None:
        for p in self._by_id.values():
            if ctype in p.connection_types:
                return p
        return None

    def all(self) -> list[StorageProvider]:
        return [self._by_id[k] for k in sorted(self._by_id)]
