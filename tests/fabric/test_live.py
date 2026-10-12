# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Checks against a LIVE Microsoft Entra / Fabric / OneLake tenant. Never part of the default run: every test is marked ``live`` and skipped unless the variables below are set.

    OLCF_LIVE_FABRIC_TENANT, OLCF_LIVE_FABRIC_CLIENT_ID, OLCF_LIVE_FABRIC_CLIENT_SECRET   the broker service principal
    OLCF_LIVE_FABRIC_WORKSPACE                                                            a Fabric workspace guid the principal can read
    OLCF_LIVE_FABRIC_NOTEBOOK                                                             (optional) a notebook guid in that workspace

These exist so that the assumptions in docs/guides/fabric-assumptions.md can be confirmed on your own tenant.
"""

from __future__ import annotations

import os

import httpx
import pytest

from olcf.fabric import notebook as nbm
from olcf.fabric.entra import TokenBroker
from olcf.fabric.http import Network, make_client
from olcf.fabric.rest import FABRIC_SCOPE, FabricRestClient

pytestmark = pytest.mark.live
NEEDED = ("OLCF_LIVE_FABRIC_TENANT", "OLCF_LIVE_FABRIC_CLIENT_ID", "OLCF_LIVE_FABRIC_CLIENT_SECRET", "OLCF_LIVE_FABRIC_WORKSPACE")


def env(name: str) -> str:
    return os.environ[name]


@pytest.fixture(scope="module")
def broker() -> TokenBroker:
    missing = [n for n in NEEDED if not os.environ.get(n)]
    if missing:
        pytest.skip("live Fabric tests need " + ", ".join(missing))
    return TokenBroker(tenant_id=env("OLCF_LIVE_FABRIC_TENANT"), client_id=env("OLCF_LIVE_FABRIC_CLIENT_ID"), secret=env("OLCF_LIVE_FABRIC_CLIENT_SECRET"), network=Network.from_config({}))


def test_live_token_claims(broker: TokenBroker) -> None:
    claims = broker.decode_unverified(broker.get_token(FABRIC_SCOPE))
    assert claims["tid"] == env("OLCF_LIVE_FABRIC_TENANT") and (claims.get("appid") or claims.get("azp")) == env("OLCF_LIVE_FABRIC_CLIENT_ID")


def test_live_workspace_is_visible(broker: TokenBroker) -> None:
    ws = FabricRestClient(broker, Network.from_config({})).list_workspaces()
    assert env("OLCF_LIVE_FABRIC_WORKSPACE") in {w["id"] for w in ws}, "add the service principal to the workspace and enable 'Service principals can use Fabric APIs'"


@pytest.mark.parametrize("fmt", ["ipynb", "fabric_git_source"])
def test_live_get_definition(broker: TokenBroker, fmt: str) -> None:
    if not os.environ.get("OLCF_LIVE_FABRIC_NOTEBOOK"):
        pytest.skip("set OLCF_LIVE_FABRIC_NOTEBOOK")
    d = FabricRestClient(broker, Network.from_config({})).get_definition(env("OLCF_LIVE_FABRIC_WORKSPACE"), env("OLCF_LIVE_FABRIC_NOTEBOOK"), fmt)
    assert d.content.strip()
    nb = nbm.parse(d.content, fmt)                                              # our parser accepts what the real service returns
    assert nb.cells


def test_live_onelake_lists_the_workspace(broker: TokenBroker) -> None:
    net = Network.from_config({})
    tok = broker.get_token("https://storage.azure.com/.default")
    with make_client(net) as c:
        r = c.get(f"{net.onelake_dfs}/{env('OLCF_LIVE_FABRIC_WORKSPACE')}", params={"resource": "filesystem", "recursive": "false", "maxResults": "1"},
                  headers={"Authorization": f"Bearer {tok}", "x-ms-version": "2023-11-03"})
    assert r.status_code == httpx.codes.OK, f"OneLake answered {r.status_code}: enable 'Users can access data stored in OneLake with apps external to Fabric'"
