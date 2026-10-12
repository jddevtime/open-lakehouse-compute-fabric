# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Pull mode against the fake Fabric API, and the scanner proving that the broker's service-principal secret never reaches a capsule, log, submission, audit record or support bundle."""

from __future__ import annotations

import io
import json
import zipfile
from pathlib import Path
from typing import Any

import pytest

from olcf.fabric.capsule import find_secrets
from olcf.worker.diagnostics import collect
from olcf_fabric import capsule as cap
from olcf_fabric.client import get_client
from olcf_fabric.errors import ServerError, SubmissionRejected
from tests.fabric.fabric_fakes import BROKER_SECRET, FABRIC_WS, NOTEBOOK
from tests.fabric.fabric_support import E2E, SUBMIT_CELL, ipynb

SERVER_NB = ipynb([("code", "n = 10", ["parameters"]), ("code", "print('pulled', n)\nnotebookutils.notebook.exit(str(n * 3))"), ("code", SUBMIT_CELL)])
GIT_NB = "# Fabric notebook source\n\n# METADATA ********************\n\n# META {\n# META   \"kernel_info\": {\n# META     \"name\": \"synapse_pyspark\"\n# META   }\n# META }\n\n# PARAMETERS CELL ********************\n\nn = 10\n\n# CELL ********************\n\nprint('git pulled', n)\nnotebookutils.notebook.exit(str(n + 1))\n"


@pytest.fixture()
def server_side(e2e: E2E):
    e2e.world.notebooks.clear()
    e2e.world.calls.clear()
    e2e.world.throttle_first = e2e.world.lro_polls = 0
    yield e2e.world
    e2e.world.notebooks.clear()
    e2e.world.throttle_first, e2e.world.lro_polls, e2e.world.fail_operation = 0, 2, False


def test_pull_mode_fetches_the_definition_server_side(e2e: E2E, session: Any, server_side: Any) -> None:
    server_side.add_notebook(FABRIC_WS, NOTEBOOK, ipynb=SERVER_NB)
    s = session.make(notebook="")
    assert s.olcf.run_remote(mode="pull", exit=False, stream_logs=False, parameters={"n": 7}) == "21"
    sub = e2e.st.http.get("/v1/integrations/fabric/submissions", headers=e2e.headers, params={"limit": 1}).json()["items"][0]
    assert sub["source_mode"] == "pull" and sub["state"] == "succeeded"
    assert any("getDefinition" in p for _, p in server_side.calls)
    assert server_side.token_requests and server_side.token_requests[-1]["client_id"] == server_side.client_id
    assert server_side.token_requests[-1]["scope"] == "https://api.fabric.microsoft.com/.default"


def test_pull_mode_falls_back_to_the_git_format_and_survives_throttling_and_slow_operations(e2e: E2E, session: Any, server_side: Any) -> None:
    server_side.add_notebook(FABRIC_WS, NOTEBOOK, git=GIT_NB)       # the ipynb format is "not available" for this item
    server_side.throttle_first, server_side.lro_polls = 2, 3          # two 429s with Retry-After, then a long-running operation
    s = session.make(notebook="")
    assert s.olcf.run_remote(mode="pull", exit=False, stream_logs=False) == "11"


def test_pull_mode_reports_a_missing_notebook_and_a_failed_operation(e2e: E2E, session: Any, server_side: Any) -> None:
    s = session.make(notebook="")
    with pytest.raises(SubmissionRejected) as ei:
        s.olcf.run_remote(mode="pull", wait=False)
    assert ei.value.remediation and ("not found" in str(ei.value).lower() or "404" in str(ei.value) or "ItemNotFound" in str(ei.value))
    server_side.add_notebook(FABRIC_WS, NOTEBOOK, ipynb=SERVER_NB)
    server_side.fail_operation = True
    with pytest.raises(ServerError) as ei2:
        s.olcf.run_remote(mode="pull", wait=False)
    assert ei2.value.status >= 400 and "could not be produced" in str(ei2.value)


def test_auto_mode_uses_getdefinition_in_the_notebook_session_first(e2e: E2E, session: Any, server_side: Any) -> None:
    s = session.make(notebook=SERVER_NB)
    assert s.olcf.run_remote(exit=False, stream_logs=False) == "30"
    sub = e2e.st.http.get("/v1/integrations/fabric/submissions", headers=e2e.headers, params={"limit": 1}).json()["items"][0]
    assert sub["source_mode"] == "inline"
    assert not any("getDefinition" in p for _, p in server_side.calls)


# ------------------------------------------------------------------------------------------------ secret scanner
def _texts(root: Path) -> list[tuple[str, bytes]]:
    out = []
    for p in root.rglob("*"):
        if p.is_file() and p.stat().st_size < 64 * 1024 * 1024:
            try:
                out.append((str(p), p.read_bytes()))
            except OSError:
                continue
    return out


def test_the_broker_secret_never_leaves_the_secret_store(e2e: E2E, session: Any, server_side: Any) -> None:
    # make sure the broker was used (pull mode: token request with the secret), then collect everything an operator or a notebook could see
    server_side.add_notebook(FABRIC_WS, NOTEBOOK, ipynb=SERVER_NB)
    s = session.make(notebook=SERVER_NB)
    s.olcf.run_remote(mode="pull", exit=False, stream_logs=False)
    assert any(r.get("client_secret") == BROKER_SECRET for r in server_side.token_requests)      # it WAS sent to Entra, and only there
    run = s.olcf.run_remote(wait=False)
    run.wait(stream_logs=False, out=io.StringIO())
    http, h, adm = e2e.st.http, e2e.headers, e2e.st.admin

    # 1. the capsule exactly as the SDK builds and signs it
    from tests.fabric.test_e2e_security import build_parts
    signed = cap.build_signed(build_parts(s, SERVER_NB), get_client().challenge(), idempotency_key=None, correlation_id=None)
    surfaces: dict[str, str] = {"capsule": json.dumps(signed)}
    # 2. API surfaces
    for name, path, hdr in [("config", "/v1/integrations/fabric", adm), ("metrics", "/v1/integrations/fabric/metrics", adm), ("submissions", "/v1/integrations/fabric/submissions", adm),
                            ("submission", f"/v1/integrations/fabric/submissions/{run.submission_id}", h), ("logs", f"/v1/integrations/fabric/submissions/{run.submission_id}/logs", h),
                            ("output", f"/v1/integrations/fabric/submissions/{run.submission_id}/output", h), ("job", f"/v1/jobs/{run.job_id}", adm),
                            ("job-logs", f"/v1/jobs/{run.job_id}/logs", adm), ("job-output", f"/v1/jobs/{run.job_id}/output", adm), ("secrets", "/v1/secrets", adm),
                            ("connections", "/v1/connections", adm), ("snippet", "/v1/integrations/fabric/snippet", h), ("audit", "/v1/audit?limit=500", adm)]:
        r = http.get(path, headers=hdr)
        surfaces[name] = r.text
    surfaces["diagnostics"] = http.post("/v1/integrations/fabric/test", headers=adm).text
    surfaces["audit-direct"] = json.dumps(e2e.st.ctx.audit.query(limit=1000), default=str)
    # 3. the worker's support bundle (redacted by its own code) and the raw job directories on the worker
    agent = e2e.st.agent
    assert agent is not None
    bundle = collect(agent.state_dir, {}, inline=False)
    with zipfile.ZipFile(bundle["path"]) as zf:
        for n in zf.namelist():
            surfaces["bundle:" + n] = zf.read(n).decode("utf-8", "replace")
    bad = {k: v for k, v in surfaces.items() if BROKER_SECRET in v}
    assert not bad, f"the broker secret appears in: {sorted(bad)}"
    # nothing in the logs / bundle matches the high-severity credential patterns either
    for k, v in surfaces.items():
        if k.startswith(("bundle:", "logs", "job-logs")):
            high = [f for f in find_secrets(v, known=[BROKER_SECRET]) if f.severity == "high" and f.kind not in ("jwt",)]
            assert not high, (k, [f.to_dict() for f in high])
    # 4. at rest: the secret exists encrypted in the store and nowhere in plain text on the worker or in job artifacts
    leaks = [path for path, data in _texts(Path(agent.state_dir)) + _texts(e2e.st.settings.path("jobs")) if BROKER_SECRET.encode() in data]
    assert not leaks, leaks
