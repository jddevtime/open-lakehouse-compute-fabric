# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""End to end with a OneLake default lakehouse: the lakehouse is attached from the capsule's context, the job gets the OneLake storage configuration through the (0600) job
spec only, and the broker secret appears nowhere an operator or a notebook can see."""

from __future__ import annotations

import io
import json
from pathlib import Path
from typing import Any

import pytest

from olcf.storage.providers.onelake import ONELAKE_HOST
from tests.fabric.fabric_fakes import BROKER_SECRET, FABRIC_WS, LAKEHOUSE
from tests.fabric.fabric_support import E2E, SUBMIT_CELL, ipynb

NB = ipynb([
    ("code", "import json\nc = notebookutils.runtime.context\nlh = notebookutils.lakehouse.get()\n"
             "print('lakehouse', c['defaultLakehouseName'], lh.get('displayName') or lh.get('name'))\nnotebookutils.notebook.exit(json.dumps({'name': c['defaultLakehouseName'], 'ws': c['defaultLakehouseWorkspaceId']}))"),
    ("code", SUBMIT_CELL),
])


@pytest.fixture()
def auto_create(e2e: E2E):
    http, adm = e2e.st.http, e2e.st.admin
    original = http.get("/v1/integrations/fabric", headers=adm).json()["config"]
    changed = dict(original, auto_create_lakehouses=True)
    e2e.st.put_fabric(**changed)
    yield
    e2e.st.put_fabric(**original)


def test_default_lakehouse_is_attached_for_the_run(e2e: E2E, session: Any, auto_create: None) -> None:
    s = session.make(notebook=NB, context={"defaultLakehouseName": "bronze", "defaultLakehouseId": LAKEHOUSE, "defaultLakehouseWorkspaceId": FABRIC_WS,
                                           "defaultLakehouseWorkspaceName": "Sales"})
    run = s.olcf.run_remote(wait=False)
    run.wait(stream_logs=False, out=io.StringIO())
    assert run.state == "succeeded", (run.data.get("error"), (run.data.get("job") or {}).get("failure"))
    job = e2e.st.http.get(f"/v1/jobs/{run.job_id}", headers=e2e.st.admin).json()
    assert json.loads(job["exit_value"]) == {"name": "bronze", "ws": FABRIC_WS}
    items = e2e.st.http.get(f"/v1/workspaces/{e2e.ws}/lakehouses", headers=e2e.st.admin).json()["items"]
    mine = next(x for x in items if (x.get("fabric") or {}).get("lakehouse_id") == LAKEHOUSE)
    assert mine["root_uri"] == f"abfss://{FABRIC_WS}@{ONELAKE_HOST}/{LAKEHOUSE}" and mine["name"] == "bronze"
    sub = e2e.st.http.get(f"/v1/integrations/fabric/submissions/{run.submission_id}", headers=e2e.headers).json()
    audit = e2e.st.http.get("/v1/audit?limit=200", headers=e2e.st.admin).json()
    assert any(r["action"] == "fabric.lakehouse.attached" for r in audit.get("items", audit if isinstance(audit, list) else []))

    # the secret reached the worker only inside the job spec, which is deleted at job end; nowhere else
    texts: list[tuple[str, str]] = [("job", json.dumps(job)), ("sub", json.dumps(sub)), ("lakehouses", json.dumps(items)),
                                    ("job-logs", e2e.st.http.get(f"/v1/jobs/{run.job_id}/logs", headers=e2e.st.admin).text),
                                    ("output", e2e.st.http.get(f"/v1/integrations/fabric/submissions/{run.submission_id}/output", headers=e2e.headers).text),
                                    ("audit", json.dumps(audit))]
    assert not [n for n, t in texts if BROKER_SECRET in t]
    agent = e2e.st.agent
    assert agent is not None
    leaked = [str(p) for p in Path(agent.state_dir).rglob("*") if p.is_file() and p.stat().st_size < 32 * 1024 * 1024 and BROKER_SECRET.encode() in p.read_bytes()]
    assert not leaked, leaked


def test_a_foreign_workspace_in_the_context_is_refused(e2e: E2E, session: Any, auto_create: None) -> None:
    from olcf_fabric.errors import SubmissionRejected
    from tests.fabric.fabric_fakes import OTHER_FABRIC_WS

    s = session.make(notebook=NB, context={"defaultLakehouseName": "x", "defaultLakehouseId": LAKEHOUSE, "defaultLakehouseWorkspaceId": OTHER_FABRIC_WS})
    with pytest.raises(SubmissionRejected) as ei:
        s.olcf.run_remote(wait=False)
    assert ei.value.code == "fabric_workspace_not_allowed"
