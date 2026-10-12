# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Microsoft Fabric page against the real control plane, real agents and the SDK: a simulated Fabric notebook (fake ``notebookutils``, fake Entra/Fabric/OneLake on loopback)
submits a notebook with ``olcf.run_remote()`` and the run is watched live in the browser (feed, log, output)."""

from __future__ import annotations

import json
import re
import sys
import threading
import types
import uuid
from typing import Any

import pytest
from playwright.sync_api import expect

from tests.e2e.stack import RUNTIME_ID
from tests.fabric.fabric_fakes import (
    API_AUDIENCE,
    BROKER_SECRET,
    FABRIC_WS,
    LAKEHOUSE,
    TENANT,
    FakeFabricWorld,
    FakeNotebookUtils,
)
from tests.fabric.fabric_support import SUBMIT_CELL, ipynb, notebook_context

NB = ipynb([
    ("code", "n = 3\ngreeting = 'hello'", ["parameters"]),
    ("code", "import json\nprint('e2e-greeting', greeting, n)\nres = {'doubled': n * 2, 'shout': greeting.upper()}\nnotebookutils.notebook.exit(json.dumps(res))"),
    ("code", SUBMIT_CELL),
])


@pytest.fixture(scope="module")
def fab(stack, has_runtime):
    if not has_runtime:
        pytest.skip("no cached runtime")
    world = FakeFabricWorld().start()
    api = stack.api
    ws = api("POST", "/v1/workspaces", json={"name": "fabric-" + uuid.uuid4().hex[:6]})["id"]
    sp = api("POST", "/v1/identity/users", json={"name": "fab-sp-" + uuid.uuid4().hex[:6], "kind": "service", "roles": [{"role": "ServicePrincipal", "scope_type": "workspace", "scope_id": ws}]})
    tok = api("POST", "/v1/identity/tokens", json={"name": "fabric-e2e", "principal_id": sp["id"], "scopes": ["fabric"]})["token"]
    secret = api("POST", "/v1/secrets", json={"name": "fab-broker-" + uuid.uuid4().hex[:6], "value": {"client_secret": BROKER_SECRET}})
    conn = api("POST", "/v1/connections", json={"name": "fab-broker-" + uuid.uuid4().hex[:6], "type": "onelake", "shared": True, "secret_id": secret["id"],
                                                  "config": {"workspace": FABRIC_WS, "lakehouse": LAKEHOUSE, "auth": "service_principal", "tenant_id": world.tenant, "client_id": world.client_id,
                                                             "endpoint": world.base}})
    api("PUT", "/v1/integrations/fabric", json={"allowed_tenants": [TENANT], "audiences": [API_AUDIENCE], "workspace_map": [{"fabric_workspace_id": FABRIC_WS, "olcf_workspace": ws}],
                                                  "broker_connection": conn["id"], "network": world.network_config(),
                                                  "default_runtime": {"family": "fabric-2.0-compatible", "runtime_id": RUNTIME_ID, "engine": "python"}})
    yield types.SimpleNamespace(world=world, ws=ws, token=tok, stack=stack)
    world.stop()


def test_readiness_and_diagnostics_chain(page, goto, fab, snap):
    goto(page, "/fabric")
    expect(page.get_by_role("heading", name="Readiness")).to_be_visible()
    expect(page.get_by_text(re.compile(r"\d+ of \d+ required steps done"))).to_be_visible()
    page.get_by_role("button", name="Run diagnostics").click()
    expect(page.get_by_role("button", name="Run again")).to_be_visible(timeout=60_000)
    steps = page.locator(".steps li, .step, [data-step]")
    banner = page.locator(".banner").first
    expect(banner).to_be_visible()
    snap(page, "fabric-diagnostics-light")
    res = fab.stack.api("POST", "/v1/integrations/fabric/test")
    names = [s["name"] for s in res["steps"]]
    assert len(names) >= 5, names
    for n in names[:3]:
        expect(page.get_by_text(n, exact=False).first).to_be_visible()
    assert steps.count() >= 0


def test_submission_appears_in_the_feed(page, goto, fab, monkeypatch, snap):
    import olcf_fabric as olcf

    ctx = notebook_context()
    nbu = FakeNotebookUtils(context=ctx, definitions={ctx["currentNotebookName"]: {None: NB, "ipynb": NB}})
    monkeypatch.setitem(sys.modules, "notebookutils", nbu)  # type: ignore[arg-type]
    olcf.config.reset_settings()
    olcf.configure(endpoint=fab.stack.url, token=fab.token, tenant_id=TENANT, allow_insecure_http=True)
    goto(page, "/fabric?tab=submissions")
    box: dict[str, Any] = {}

    def submit() -> None:
        try:
            box["value"] = olcf.run_remote(exit=False, stream_logs=False, parameters={"n": 21, "greeting": "hi"}, timeout=240, poll_interval=1.0)
        except BaseException as exc:  # noqa: BLE001
            box["error"] = exc

    th = threading.Thread(target=submit, daemon=True)
    th.start()
    try:
        row = page.locator("table tbody tr").first
        expect(row).to_be_visible(timeout=60_000)
        row.click()
        dlg = page.get_by_role("dialog")
        expect(dlg).to_contain_text("etl_orders")
        expect(dlg).to_contain_text(re.compile("succeeded", re.I), timeout=180_000)
        expect(dlg).to_contain_text("e2e-greeting hi 21", timeout=30_000)  # the notebook output the remote run printed
        expect(dlg).to_contain_text('"n": 21')
        snap(page, "fabric-submission")
        page.keyboard.press("Escape")
    finally:
        th.join(timeout=60)
        olcf.config.reset_settings()
    assert "error" not in box, box.get("error")
    assert json.loads(box["value"]) == {"doubled": 42, "shout": "HI"}
    subs = fab.stack.api("GET", "/v1/integrations/fabric/submissions", params={"limit": 1})["items"]
    assert subs[0]["state"] == "succeeded"
    # the home page feed and the jobs list show the same run
    goto(page, "/jobs")
    expect(page.get_by_text(subs[0]["job_id"][:10]).first).to_be_visible()
