# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""External provider: an existing Spark cluster registered by an administrator.

Two modes, chosen by the configuration:

* ``master_url`` (``spark://host:7077``): the OLCF runner executes on a driver node (scheduled like any other job) and connects its
  Spark driver to the existing master. OLCF does **not** manage that cluster; it reads ``<ui_url>/json`` for monitoring.
  Optional ``livy_url`` adds Livy-based monitoring/reconcile.
* ``livy_url`` only: the job is executed on that cluster through Livy sessions (see :mod:`olcf.compute.providers.livy`);
  no worker node is involved.

Other master schemes (``yarn``, ``mesos``, ``k8s://``) are rejected: use the kubernetes provider, or register a plug-in provider.
"""

from __future__ import annotations

import re
from typing import Any

import httpx

from olcf.compute.providers.agent import AgentExecutedProvider
from olcf.compute.providers.base import (
    AppStatus,
    Discovery,
    PrepareRequest,
    base_spark_conf,
    dynamic_allocation_conf,
    spark_memory,
)
from olcf.compute.providers.livy import LivyClient, LivyRemoteProvider
from olcf.compute.providers.standalone import parse_master_json
from olcf.core.errors import BadRequest, Unsupported

_MASTER_RE = re.compile(r"^spark://[A-Za-z0-9._-]+(:\d+)?(,[A-Za-z0-9._-]+(:\d+)?)*$")


def validate_external_config(cfg: dict[str, Any]) -> None:
    master, livy = cfg.get("master_url"), cfg.get("livy_url")
    if not master and not livy:
        raise BadRequest("an external provider needs 'master_url' (spark://host:port) and/or 'livy_url'")
    if master and not _MASTER_RE.match(str(master)):
        raise Unsupported(f"unsupported master '{master}': only spark:// standalone masters are supported by the external provider",
                          remediation="use the kubernetes provider for k8s:// masters")
    for key in ("livy_url", "ui_url"):
        if cfg.get(key) and not str(cfg[key]).startswith(("http://", "https://")):
            raise BadRequest(f"'{key}' must be an http(s) URL")


class ExternalProvider(AgentExecutedProvider, LivyRemoteProvider):
    """Runs jobs against a Spark cluster that someone else operates, through its standalone master or a Livy endpoint."""
    type = "external"
    capabilities = ("existing-cluster", "spark-standalone-master", "livy")

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self._livy: LivyClient | None = None

    @property
    def uses_runtimes(self) -> bool:  # type: ignore[override]
        return self.execution == "agent"  # Livy-only registrations run on the foreign cluster's own runtime

    @property
    def prepares_spark(self) -> bool:  # type: ignore[override]
        return self.execution == "agent"

    def validate_config(self) -> None:
        validate_external_config(self.config)

    @property
    def execution(self) -> str:
        return "agent" if self.config.get("master_url") else "remote"

    # --- remote (Livy-only) mode delegates to the Livy base class; agent mode to AgentExecutedProvider ---------
    def _client(self) -> LivyClient:
        if self._livy is None:
            url = self.config.get("livy_url")
            if not url:
                raise Unsupported("no livy_url configured for this external provider")
            headers: dict[str, str] = {}
            secret = self.env.secret(self.config.get("secret_id")) if self.config.get("secret_id") else None
            if isinstance(secret, dict) and secret.get("bearer_token"):
                headers["Authorization"] = f"Bearer {secret['bearer_token']}"
            self._livy = LivyClient(url, lambda: headers, verify=self.config.get("ca_bundle") or True)
        return self._livy

    def submit(self, req):  # noqa: ANN001, ANN201
        return (AgentExecutedProvider if self.execution == "agent" else LivyRemoteProvider).submit(self, req)

    def status(self, handle):  # noqa: ANN001, ANN201
        return (AgentExecutedProvider if self.execution == "agent" else LivyRemoteProvider).status(self, handle)

    def logs(self, handle, offset=0, limit=65536, *, cursor=None):  # noqa: ANN001, ANN201
        return (AgentExecutedProvider if self.execution == "agent" else LivyRemoteProvider).logs(self, handle, offset, limit, cursor=cursor)

    def cancel(self, handle, *, force=False, grace_seconds=15):  # noqa: ANN001, ANN201
        return (AgentExecutedProvider if self.execution == "agent" else LivyRemoteProvider).cancel(self, handle, force=force, grace_seconds=grace_seconds)

    def ui(self, handle):  # noqa: ANN001, ANN201
        return (AgentExecutedProvider if self.execution == "agent" else LivyRemoteProvider).ui(self, handle)

    # --- agent mode ------------------------------------------------------------------------------------
    def prepare(self, req: PrepareRequest) -> dict[str, Any]:
        if self.execution != "agent":
            raise Unsupported("Livy-only external providers do not run an OLCF runner")
        g, node = req.placement, req.node
        driver_host = node.advertise_host(self.env.ctx.settings)
        conf = base_spark_conf(req)
        conf.update({
            "spark.executor.cores": str(g.get("executor_cores") or 1), "spark.executor.memory": spark_memory(g.get("executor_memory_mb") or 1024),
            "spark.driver.host": driver_host, "spark.driver.bindAddress": "0.0.0.0", "spark.driver.port": str(g["driver_port"]),  # noqa: S104 - the driver must accept executor connections
            "spark.blockManager.port": str(g["block_manager_port"]),
        })
        if g.get("executor_cores_total"):
            conf["spark.cores.max"] = str(g["executor_cores_total"])
        conf.update(dynamic_allocation_conf(req.job.get("resources_request") or {}, g))
        return {"master": self.config["master_url"], "app_name": req.app_name, "driver_host": driver_host, "driver_port": g["driver_port"],
                "block_manager_port": g["block_manager_port"], "conf": conf, "jars": [], "packages": []}

    def _master_ui(self) -> str | None:
        if self.config.get("ui_url"):
            return str(self.config["ui_url"]).rstrip("/")
        m = re.match(r"spark://([^:,/]+)", str(self.config.get("master_url") or ""))
        return f"http://{m.group(1)}:8080" if m else None

    def discover(self) -> Discovery:
        details: dict[str, Any] = {"mode": self.execution, "master_url": self.config.get("master_url"), "livy_url": self.config.get("livy_url")}
        ok, detail = True, ""
        ui = self._master_ui()
        if self.config.get("master_url") and ui:
            try:
                r = httpx.get(ui.rstrip("/") + "/json/", timeout=4.0)
                st = parse_master_json(r.json())
                detail = f"master {st['state']}: {st['alive_workers']} workers, {st['cores_total']} cores"
                details["status"] = {k: st[k] for k in ("state", "alive_workers", "cores_total", "cores_used", "memory_total_mb")}
                ok = st["state"] == "ALIVE"
            except (httpx.HTTPError, ValueError) as exc:
                ok, detail = False, f"master UI {ui}/json/ unreachable: {exc}"
        if self.config.get("livy_url") and self.execution == "remote":
            d = LivyRemoteProvider.discover(self)
            ok, detail = d.ok, d.detail
        return Discovery(self.name, self.type, ok, detail, list(self.capabilities), self.execution, extra=details)

    def reconcile(self) -> list[AppStatus]:
        if self.execution == "remote":
            return LivyRemoteProvider.reconcile(self)
        out = AgentExecutedProvider.reconcile(self)
        if self.config.get("livy_url"):
            try:
                out += LivyRemoteProvider.reconcile(self)
            except Exception:  # noqa: BLE001 - Livy monitoring is optional
                pass
        return out
