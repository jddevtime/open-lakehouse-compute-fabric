# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Connection testers and ``storage:<provider>`` health checks.

``ctx.connection_testers[type]`` powers ``POST /v1/connections/{id}/test`` (the "Test connectivity" button).  The
health checks give the control panel a traffic light per provider: *quick* is passive (SDK present, connections
configured, last recorded test result); *deep* actively tests **every** connection of that provider type end to end
(DNS -> connect -> auth -> list -> write -> read -> delete), and for ``local`` the platform storage root.
"""

from __future__ import annotations

from typing import Any

from sqlalchemy import select

from olcf.core.context import AppContext
from olcf.core.health import ComponentHealth, HealthCheck, Status
from olcf.db import schema as S
from olcf.db.util import fetch_all
from olcf.storage.providers import ProviderRegistry
from olcf.storage.service import StorageService


def _sdk_status(provider_id: str) -> tuple[bool, str]:
    mod = {"s3": "boto3", "adls": "azure.storage.filedatalake", "onelake": "azure.storage.filedatalake"}.get(provider_id)
    if not mod:
        return True, "built in"
    try:
        __import__(mod)
        return True, f"{mod} available"
    except ImportError:
        return False, f"{mod} is not installed (pip install 'olcf[{'s3' if provider_id == 's3' else 'azure'}]')"


def install_testers(ctx: AppContext, svc: StorageService, registry: ProviderRegistry) -> None:
    def make(ctype: str):  # noqa: ANN202
        def tester(_ctx: AppContext, conn: dict[str, Any], secret: Any) -> dict[str, Any]:
            res = svc.test_connection(conn, secret=secret if secret is not None else None, resolve_secret=False)
            return res

        return tester

    for p in registry.all():
        for t in p.connection_types:
            ctx.connection_testers[t] = make(t)


def _connections_of(ctx: AppContext, types: tuple[str, ...]) -> list[dict[str, Any]]:
    with ctx.db.connect() as conn:
        return fetch_all(conn, select(S.connections).where(S.connections.c.type.in_(list(types)), S.connections.c.deleted_at.is_(None)).order_by(S.connections.c.name))


def install_health(ctx: AppContext, svc: StorageService, registry: ProviderRegistry) -> None:
    for p in registry.all():
        name = f"storage:{p.id}"

        def quick(p=p) -> ComponentHealth:  # noqa: ANN001
            ok, detail = _sdk_status(p.id)
            conns = _connections_of(ctx, p.connection_types)
            if not ok:
                return ComponentHealth(f"storage:{p.id}", Status.DOWN, detail, labels={"connections": len(conns)})
            if p.id == "local":
                import shutil

                u = shutil.disk_usage(ctx.settings.local_storage_root if ctx.settings.local_storage_root.exists() else ctx.settings.data_dir)
                return ComponentHealth("storage:local", Status.OK if u.free / u.total > 0.05 else Status.DEGRADED,
                                       f"{u.free / 1e9:.1f} GB free under {ctx.settings.local_storage_root}", labels={"connections": len(conns)})
            bad = [c["name"] for c in conns if c.get("last_test") and not c["last_test"].get("ok")]
            if bad:
                return ComponentHealth(f"storage:{p.id}", Status.DEGRADED, f"last connectivity test failed for: {', '.join(bad)}", labels={"connections": len(conns), "failing": bad})
            return ComponentHealth(f"storage:{p.id}", Status.OK if conns else Status.UNKNOWN,
                                   f"{detail}; {len(conns)} connection(s) configured" if conns else f"{detail}; no connections configured", labels={"connections": len(conns)})

        def deep(p=p) -> ComponentHealth:  # noqa: ANN001
            ok, detail = _sdk_status(p.id)
            if not ok:
                return ComponentHealth(f"storage:{p.id}", Status.DOWN, detail)
            steps: list[dict[str, Any]] = []
            failures: list[str] = []
            if p.id == "local":
                root = svc.default_root_for("_health", "probe")
                res = p.test(root, {}, None)
                steps += [{**s, "target": "platform local root"} for s in res["steps"]]
                if not res["ok"]:
                    failures.append("platform local root")
            tested = 0
            for c in _connections_of(ctx, p.connection_types):
                if p.id == "local" and not (c.get("config") or {}).get("root"):
                    continue
                tested += 1
                res = svc.test_connection(c)
                steps += [{**s, "target": c["name"]} for s in res["steps"]]
                if not res["ok"]:
                    failures.append(c["name"])
            if p.id != "local" and tested == 0:
                return ComponentHealth(f"storage:{p.id}", Status.UNKNOWN, "no connections of this type are configured, nothing to test", steps=[])
            status = Status.DOWN if failures else Status.OK
            return ComponentHealth(f"storage:{p.id}", status, ("failed: " + ", ".join(failures)) if failures else f"deep test passed for {max(tested, 1)} target(s)", steps=steps)

        ctx.health.register(HealthCheck(name, "storage", f"{p.label} reachability and read/write round trip", quick, deep, ttl=20))
