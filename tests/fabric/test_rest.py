# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Pull mode client against the fake Fabric API: getDefinition long-running operation, 429 / Retry-After, 401 refresh, redirect to a foreign host."""

from __future__ import annotations

import base64
import json
from typing import Any

import httpx
import pytest

from olcf.fabric.entra import TokenBroker
from olcf.fabric.http import Network
from olcf.fabric.rest import FabricApiError, FabricRestClient
from tests.fabric.fabric_fakes import (
    BROKER_CLIENT_ID,
    BROKER_SECRET,
    FABRIC_WS,
    LAKEHOUSE,
    NOTEBOOK,
    TENANT,
    FakeFabricWorld,
)
from tests.fabric.fabric_support import ipynb

NB = ipynb([("code", "print(1)")])
GIT = "# Fabric notebook source\n\n# CELL ********************\n\nprint(1)\n"


def make_client(world: FakeFabricWorld, **kw: Any) -> tuple[FabricRestClient, list[float]]:
    net = Network(**{k: v for k, v in world.network_config().items() if k != "trust_env"}, trust_env=False)
    broker = TokenBroker(tenant_id=TENANT, client_id=BROKER_CLIENT_ID, secret=BROKER_SECRET, network=net)
    sleeps: list[float] = []
    return FabricRestClient(broker, net, sleep=sleeps.append, **kw), sleeps


@pytest.fixture()
def fresh(world: FakeFabricWorld):
    world.notebooks.clear()
    world.calls.clear()
    world.throttle_first, world.lro_polls, world.fail_operation = 0, 2, False
    world.add_notebook(FABRIC_WS, NOTEBOOK, ipynb=NB, git=GIT)
    yield world
    world.notebooks.clear()
    world.throttle_first, world.lro_polls, world.fail_operation = 0, 2, False


def test_get_definition_polls_the_operation_and_decodes_both_formats(fresh: FakeFabricWorld) -> None:
    c, sleeps = make_client(fresh)
    d = c.get_definition(FABRIC_WS, NOTEBOOK, "ipynb")
    assert d.format == "ipynb" and json.loads(d.content)["cells"][0]["source"] == "print(1)" and d.display_name == "From Fabric"
    assert d.content_path == "notebook-content.ipynb" and ".platform" in d.parts
    g = c.get_definition(FABRIC_WS, NOTEBOOK, "fabric_git_source")
    assert g.format == "fabric_git_source" and "# CELL" in g.content
    assert len(sleeps) >= 2 and sum(1 for m, p in fresh.calls if p.endswith("getDefinition")) == 2


def test_throttling_honours_retry_after_and_gives_up_eventually(fresh: FakeFabricWorld) -> None:
    fresh.throttle_first = 2
    c, sleeps = make_client(fresh)
    assert c.get_definition(FABRIC_WS, NOTEBOOK).format == "ipynb"
    assert fresh.throttle_first == 0 and sleeps[:2] == [0.0, 0.0]            # Retry-After: 0 was honoured, not the exponential default
    fresh.throttle_first = 50
    c2, _ = make_client(fresh, max_attempts=3)
    with pytest.raises(FabricApiError) as ei:
        c2.get_definition(FABRIC_WS, NOTEBOOK)
    assert ei.value.status == 429


def test_failed_and_slow_operations_and_missing_items(fresh: FakeFabricWorld) -> None:
    fresh.fail_operation = True
    c, _ = make_client(fresh)
    with pytest.raises(FabricApiError) as ei:
        c.get_definition(FABRIC_WS, NOTEBOOK)
    assert "could not be produced" in str(ei.value) and ei.value.error_code == "Boom"
    fresh.fail_operation = False
    fresh.lro_polls = 10_000
    clock = [0.0]
    c2, sleeps = make_client(fresh, monotonic=lambda: clock[0])
    c2._sleep = lambda s: clock.__setitem__(0, clock[0] + 30)               # noqa: SLF001 - advance the fake clock instead of waiting
    with pytest.raises(FabricApiError) as ei2:
        c2.get_definition(FABRIC_WS, NOTEBOOK, timeout_seconds=100)
    assert "did not finish" in str(ei2.value)
    c3, _ = make_client(fresh)
    with pytest.raises(FabricApiError) as ei3:
        c3.get_definition(FABRIC_WS, "00000000-0000-4000-8000-000000000000")
    assert ei3.value.status == 404 and ei3.value.remediation
    with pytest.raises(FabricApiError):
        c3.get_definition(FABRIC_WS, NOTEBOOK, "docx")


def test_unavailable_format_is_a_clean_error(fresh: FakeFabricWorld) -> None:
    fresh.add_notebook(FABRIC_WS, NOTEBOOK, git=GIT)                          # no ipynb rendition
    c, _ = make_client(fresh)
    with pytest.raises(FabricApiError) as ei:
        c.get_definition(FABRIC_WS, NOTEBOOK, "ipynb")
    assert ei.value.status == 400 and ei.value.error_code == "InvalidFormat"
    assert c.get_definition(FABRIC_WS, NOTEBOOK, "fabric_git_source").format == "fabric_git_source"


def test_forbidden_carries_the_tenant_setting_remediation(fresh: FakeFabricWorld) -> None:
    fresh.fabric_status = 403
    try:
        c, _ = make_client(fresh)
        with pytest.raises(FabricApiError) as ei:
            c.list_workspaces()
        assert ei.value.status == 403 and "Service principals can use Fabric APIs" in (ei.value.remediation or "")
    finally:
        fresh.fabric_status = 200


def test_list_workspaces(fresh: FakeFabricWorld) -> None:
    c, _ = make_client(fresh)
    assert [w["id"] for w in c.list_workspaces()] == [FABRIC_WS]


def test_operation_url_on_a_foreign_host_is_never_followed(fresh: FakeFabricWorld) -> None:
    c, _ = make_client(fresh)
    first = httpx.Response(202, headers={"Location": "https://evil.example/v1/operations/x", "Retry-After": "0"})
    with pytest.raises(FabricApiError) as ei:
        c._await_operation(first, 10)                                          # noqa: SLF001
    assert "another host" in str(ei.value)
    assert not any("evil" in p for _, p in fresh.calls)
    with pytest.raises(FabricApiError):
        c._await_operation(httpx.Response(202), 10)                            # noqa: SLF001 - no Location header at all


def test_expired_token_is_refreshed_once_after_a_401(fresh: FakeFabricWorld) -> None:
    c, _ = make_client(fresh)
    c.list_workspaces()
    first = fresh.token_requests and len(fresh.token_requests)
    c.broker._cache["https://api.fabric.microsoft.com/.default"] = ("garbage", 9e12)         # noqa: SLF001 - a cached token the API no longer accepts
    assert [w["id"] for w in c.list_workspaces()] == [FABRIC_WS]
    assert len(fresh.token_requests) == (first or 0) + 1


def test_decode_definition_errors() -> None:
    with pytest.raises(FabricApiError):
        FabricRestClient.decode_definition({"definition": {"parts": []}}, "ipynb")
    payload = base64.b64encode(b"hello").decode()
    d = FabricRestClient.decode_definition({"definition": {"parts": [{"path": "notebook-content.py", "payload": payload, "payloadType": "InlineBase64"}]}}, "fabric_git_source")
    assert d.content == "hello" and LAKEHOUSE
