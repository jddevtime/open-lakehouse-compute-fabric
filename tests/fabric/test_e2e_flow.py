# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""End to end: a simulated Fabric notebook session (fake ``notebookutils``) with the real SDK, the real control plane and the real worker agent/runner (fake Spark-less runtime)."""

from __future__ import annotations

import io
import json
from typing import Any

import pytest

from tests.fabric.fabric_fakes import FakeNotebookUtils
from tests.fabric.fabric_support import E2E, SUBMIT_CELL, ipynb


def run_cell(code: str, ns: dict[str, Any]) -> Any:
    """Execute the submit cell the way a notebook kernel would (module-level code in the notebook's namespace)."""
    exec(compile(code, "<cell>", "exec"), ns)  # noqa: S102


NB = ipynb([
    ("code", "n = 3\ngreeting = 'hello'", ["parameters"]),
    ("code", "import json\nprint('greeting', greeting, n)\nres = {'doubled': n * 2, 'shout': greeting.upper()}\nnotebookutils.notebook.exit(json.dumps(res))"),
    ("code", SUBMIT_CELL),
])


def test_notebook_runs_remotely_and_returns_through_exit(e2e: E2E, session: Any, capsys: pytest.CaptureFixture[str]) -> None:
    s = session.make(notebook=NB)
    ns: dict[str, Any] = {"n": 21, "greeting": "hi", "secret_local_var": "must-not-travel"}
    with pytest.raises(FakeNotebookUtils.NotebookExit) as exit_:
        run_cell(SUBMIT_CELL, ns)
    assert json.loads(exit_.value.value) == {"doubled": 42, "shout": "HI"}  # the declared parameters (n, greeting) travelled, the rest did not
    out = capsys.readouterr().out
    assert "greeting hi 21" in out  # the remote log was streamed into the cell output
    subs = e2e.st.http.get("/v1/integrations/fabric/submissions", headers=e2e.headers).json()["items"]
    sub = subs[0]
    assert sub["state"] == "succeeded" and sub["parameters"] == {"n": 21, "greeting": "hi"} and "secret_local_var" not in json.dumps(sub)
    assert sub["compat_summary"]["blocked"] is False and sub["round_trip_ms"] > 0
    job = e2e.st.http.get(f"/v1/jobs/{sub['job_id']}", headers=e2e.st.admin).json()
    assert job["origin"]["kind"] == "fabric" and job["kind"] == "capsule" and job["state"] == "succeeded"
    assert s.nbu.exits  # notebookutils.notebook.exit was called with the result


def test_wait_false_returns_a_handle(e2e: E2E, session: Any) -> None:
    s = session.make(notebook=NB)
    run = s.olcf.run_remote(wait=False, parameters={"n": 4})
    assert run.submission_id.startswith("sub_") and run.state in ("validated", "queued", "provisioning", "running", "succeeded")
    run.wait(stream_logs=False, out=io.StringIO())
    assert run.state == "succeeded"
    assert s.olcf.status(run)["state"] == "succeeded"
    assert isinstance(s.olcf.logs(run, offset=0), str)
    printed = e2e.st.http.get(f"/v1/integrations/fabric/submissions/{run.submission_id}/output", headers=e2e.headers).json()
    assert printed["available"] and "greeting hello 4" in json.dumps(printed["cells"])  # what the remote notebook printed
    with pytest.raises(FakeNotebookUtils.NotebookExit):
        s.olcf.finish(run)
    assert json.loads(s.nbu.exits[-1]) == {"doubled": 8, "shout": "HELLO"}


def test_output_endpoint_redacts_and_is_confined(e2e: E2E, session: Any) -> None:
    """What a notebook prints may contain a secret it read from its environment: the ``/output`` endpoint masks it, and other principals cannot read it."""
    leaky = ipynb([("code", "print('aws_secret_access_key = ' + repr('" + "Zq9" * 13 + "A'))\nprint('visible-marker')"), ("code", SUBMIT_CELL)])
    s = session.make(notebook=leaky)
    run = s.olcf.run_remote(wait=False)
    run.wait(stream_logs=False, out=io.StringIO())
    res = e2e.st.http.get(f"/v1/integrations/fabric/submissions/{run.submission_id}/output", headers=e2e.headers)
    assert res.status_code == 200
    text = json.dumps(res.json())
    assert "visible-marker" in text and ("Zq9" * 13) not in text
    other = e2e.st.service_principal(e2e.st.workspace())
    assert e2e.st.http.get(f"/v1/integrations/fabric/submissions/{run.submission_id}/output", headers=other).status_code in (403, 404)
    assert e2e.st.http.get(f"/v1/integrations/fabric/submissions/{run.submission_id}/output").status_code == 401
