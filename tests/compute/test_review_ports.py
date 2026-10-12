# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Review F05 (control-plane side): every Spark daemon port comes from a configured range, so the Helm NetworkPolicy - generated from the same ranges - can name it."""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any

import pytest

from olcf.compute.errors import ProviderError
from olcf.compute.providers.standalone import StandaloneProvider
from olcf.core.config import ComputeSettings


def stub(clusters: list[dict[str, Any]], **settings: Any) -> Any:
    compute = ComputeSettings(**settings)
    return SimpleNamespace(env=SimpleNamespace(ctx=SimpleNamespace(settings=SimpleNamespace(compute=compute))), clusters=lambda: clusters,
                           _free_port=StandaloneProvider._free_port)


NODE = SimpleNamespace(id="wrk_a")


def worker_ports(self: Any, cl: dict[str, Any]) -> tuple[int, int]:
    return StandaloneProvider._worker_ports(self, cl, NODE)


def test_worker_daemons_get_distinct_ports_from_the_configured_ranges_and_keep_them() -> None:
    a = {"id": "c1", "spark_workers": {}}
    b = {"id": "c2", "spark_workers": {}}
    s = stub([a, b], worker_port_range=(45000, 45003), worker_ui_port_range=(45100, 45103))
    assert worker_ports(s, a) == (45000, 45100)
    a["spark_workers"]["wrk_a"] = {"state": "running", "port": 45000, "ui_port": 45100}
    assert worker_ports(s, b) == (45001, 45101), "a second runtime family on the same node takes the next free port"
    assert worker_ports(s, a) == (45000, 45100), "an existing daemon keeps its ports (no needless restart)"
    b["spark_workers"]["wrk_a"] = {"state": "running", "port": 45001, "ui_port": 45101}
    a["spark_workers"]["wrk_a"]["state"] = "stopped"  # stopped daemons release their ports
    assert worker_ports(s, {"id": "c3", "spark_workers": {}}) == (45000, 45100)


def test_exhausting_a_range_is_an_actionable_error_not_a_port_outside_the_policy() -> None:
    clusters = [{"id": f"c{i}", "spark_workers": {"wrk_a": {"state": "running", "port": 45000 + i, "ui_port": 45100 + i}}} for i in range(2)]
    s = stub(clusters, worker_port_range=(45000, 45001), worker_ui_port_range=(45100, 45101))
    with pytest.raises(ProviderError, match="all 2 Spark worker RPC ports"):
        worker_ports(s, {"id": "c9", "spark_workers": {}})


def test_master_ports_stay_inside_their_span() -> None:
    free = StandaloneProvider._free_port
    assert free(7077, set(), 16) == 7077 and free(7077, {7077, 7078}, 16) == 7079
    with pytest.raises(ProviderError, match="Spark master RPC"):
        free(7077, set(range(7077, 7093)), 16, role="Spark master RPC")
    assert free(7077, set(range(7077, 7093))) == 7093, "without a span the old unbounded behaviour is kept"


def test_port_ranges_are_validated_and_cannot_overlap() -> None:
    ok = ComputeSettings().port_ranges()
    assert set(ok) == {"master_rpc", "master_ui", "worker_rpc", "worker_ui", "driver_ui", "driver", "block_manager"}
    with pytest.raises(ValueError, match="overlap"):
        ComputeSettings(worker_port_range=(41100, 41250))  # collides with the block manager range
    with pytest.raises(ValueError, match="valid range"):
        ComputeSettings(driver_port_range=(41099, 41000))
    with pytest.raises(ValueError, match="valid range"):
        ComputeSettings(worker_ui_port_range=(65000, 70000))
    with pytest.raises(ValueError, match="at least 1"):
        ComputeSettings(spark_master_port_span=0)


def test_admin_ports_report_lists_every_range(stack: Any) -> None:
    text = stack.http.get("/v1/admin/ports", headers=stack.admin).text
    r = stack.settings.compute.port_ranges()
    for role in ("master_rpc", "master_ui", "worker_rpc", "worker_ui", "driver_ui", "driver", "block_manager"):
        lo, hi = r[role]
        assert f"tcp/{lo}-{hi}" in text, (role, text)
