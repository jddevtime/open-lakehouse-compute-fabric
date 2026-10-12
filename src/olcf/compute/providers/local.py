# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Local provider: the driver runs Spark in ``local[N]`` mode on a single node, no cluster, no executors."""

from __future__ import annotations

from typing import Any

from olcf.compute.providers.agent import AgentExecutedProvider
from olcf.compute.providers.base import Discovery, PrepareRequest, base_spark_conf
from olcf.compute.registry import NodeInfo


class LocalProvider(AgentExecutedProvider):
    """Runs a job in Spark local mode on one worker, with no cluster."""
    type = "local"
    capabilities = ("local-mode", "single-node", "reconcile")

    def prepare(self, req: PrepareRequest) -> dict[str, Any]:
        g = req.placement
        cores = max(1, int(g.get("driver_cores") or 1))
        conf = base_spark_conf(req)
        conf.update({"spark.driver.host": "127.0.0.1", "spark.driver.bindAddress": "127.0.0.1"})
        if g.get("driver_port"):
            conf["spark.driver.port"] = str(g["driver_port"])
        if g.get("block_manager_port"):
            conf["spark.blockManager.port"] = str(g["block_manager_port"])
        return {"master": f"local[{cores}]", "app_name": req.app_name, "driver_host": "127.0.0.1", "driver_port": g.get("driver_port"),
                "block_manager_port": g.get("block_manager_port"), "conf": conf, "jars": [], "packages": []}

    def discover(self) -> Discovery:
        nodes = [n for n in self.env.registry.node_infos() if n.state in ("online", "degraded")]
        ready: dict[str, int] = {}
        for n in nodes:
            for rid in n.ready_runtimes:
                ready[rid] = ready.get(rid, 0) + 1
        return Discovery(self.name, self.type, bool(nodes), f"{len(nodes)} node(s) online", list(self.capabilities),
                         workers=[_brief(n) for n in nodes], runtimes=[{"id": r, "ready_nodes": c} for r, c in sorted(ready.items())])


def _brief(n: NodeInfo) -> dict[str, Any]:
    return {"id": n.id, "name": n.name, "pool": n.pool, "state": n.state, "scheduling": n.scheduling}
