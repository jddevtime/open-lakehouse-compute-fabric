# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""The first-run path with NO Spark runtime, proven end to end through the shipped commands (FIRSTRUN).

An empty data directory, ``olcf quickstart --no-runtime --no-open`` as a subprocess on a free port, nothing else arranged:

1. the server, the embedded worker and the sample lakehouse are up; the built-in host-Python runtime is the only (and ``ready``) runtime;
2. ``olcf notebook run ... 02_Retail_Explore_Python --wait`` succeeds on host-Python; the executed notebook has outputs (tables/text, and a chart image when matplotlib
   is installed in the worker's Python); the job record and ``result.json`` carry the runtime fingerprint;
3. the Spark notebook (01) does not run and does not hang silently: it stays queued with an explicit reason and the exact command that fixes it;
4. the "Retail Sales Overview" dashboard runs through the API with every tile returning data;
5. an AI agent connects with the official ``mcp`` client and runs SQL;
6. DuckDB SQL through the CLI works.
"""

from __future__ import annotations

import asyncio
import importlib.util
import json
import sys
from collections.abc import Iterator
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
for _p in (str(ROOT), str(ROOT / "src")):
    if _p not in sys.path:
        sys.path.insert(0, _p)
sys.path.insert(0, str(ROOT / "examples"))

from tests.e2e.firstrun import FirstRunStack  # noqa: E402

WS = "Sample Retail"


@pytest.fixture(scope="module")
def fr(tmp_path_factory: pytest.TempPathFactory) -> Iterator[FirstRunStack]:
    st = FirstRunStack(tmp_path_factory.mktemp("firstrun")).start()
    try:
        yield st
    finally:
        st.stop()


def test_quickstart_output_is_a_working_state(fr: FirstRunStack) -> None:
    out = fr.quickstart.stdout  # type: ignore[union-attr]
    assert f"OLCF is running at {fr.url}" in out
    assert "Sample Retail" in out and "host-Python" in out
    assert f"{fr.url}/#/dashboards/itm_" in out
    assert f'olcf notebook run -w "{WS}" 02_Retail_Explore_Python --wait' in out
    assert "olcf mcp config" in out
    assert "Traceback" not in out + fr.quickstart.stderr  # type: ignore[union-attr]


def test_host_python_is_the_only_runtime_and_it_is_ready(fr: FirstRunStack) -> None:
    rows = {r["id"]: r for r in fr.api("GET", "/v1/runtimes")["items"]}
    host = rows["olcf-host-python"]
    assert host["state"] == "ready" and host["family"] == "host-python" and host["no_spark"] is True and host["builtin"] is True and host["ready_nodes"]
    assert not [r for r in rows.values() if r["family"] != "host-python" and r["state"] == "ready"], "no Spark runtime is installed"
    listing = fr.cli("runtime", "list")
    assert "olcf-host-python" in listing.stdout and "ready" in listing.stdout


def test_sql_over_the_sample_lakehouse(fr: FirstRunStack) -> None:
    res = fr.cli("--json", "sql", "run", "-w", WS, "select count(*) as n from retail_lakehouse.fact_sales", check=True)
    assert json.loads(res.stdout)["rows"][0][0] > 1000


def test_sample_notebook_02_runs_on_host_python(fr: FirstRunStack) -> None:
    res = fr.cli("notebook", "run", "-w", WS, "02_Retail_Explore_Python", "--wait", timeout=300)
    assert res.returncode == 0, res.stdout + res.stderr
    jid = next(line.split()[-1] for line in (res.stdout + res.stderr).splitlines() if "submitted job_" in line)
    job = fr.api("GET", f"/v1/jobs/{jid}", params={"detail": "true"})
    assert job["state"] == "succeeded" and job["runtime_id"] == "olcf-host-python" and job["queue_reason"] is None
    assert job["hashes"]["runtime_fingerprint"].startswith("sha256:")
    # the sample explicitly asks for the host-python runtime family, so this is a direct selection, NOT a fallback: no runtime_fallback event may be recorded
    assert job["request"]["runtime"]["family"] == "host-python", job["request"]
    kinds = [e.get("event") or e.get("kind") for e in job["timeline"]]
    assert "runtime_fallback" not in kinds, kinds
    # the executed notebook has real outputs
    r = fr.http.get(f"/v1/jobs/{jid}/output", headers=fr.admin)
    assert r.status_code == 200, r.text[:300]
    import nbformat

    nb = nbformat.reads(r.text, as_version=4)
    code = [c for c in nb.cells if c.cell_type == "code"]
    with_out = [c for c in code if c.get("outputs")]
    assert len(with_out) >= 8, f"{len(with_out)} of {len(code)} code cells have outputs"
    assert not [o for c in code for o in c.get("outputs", []) if o.output_type == "error"]
    text = json.dumps([o for c in code for o in c.get("outputs", [])])
    assert "reconciles with gold_daily_sales" in text
    has_png = "image/png" in text
    # the worker's own Python decides: with matplotlib there are charts, without it the notebook says how to get them and still shows the tables
    if importlib.util.find_spec("matplotlib") is not None:
        assert has_png, "matplotlib is installed, the notebook must draw its charts"
    else:
        assert not has_png and "matplotlib is not installed" in text and "text/plain" in text
    # result.json (the job's result record) carries the same fingerprint and the host-Python facts
    res_doc = fr.api("GET", f"/v1/jobs/{jid}")["result"]
    dep = res_doc["dependencies"]
    assert dep["runtime_id"] == "olcf-host-python" and dep["runtime_fingerprint"] == job["hashes"]["runtime_fingerprint"]
    assert dep["host_python"]["packages"]["duckdb"] and dep["host_python"]["python"]


def test_a_python_job_that_asks_for_an_unavailable_runtime_family_falls_back_with_an_event(fr: FirstRunStack) -> None:
    """The genuine fallback path: the request names a Spark runtime family nothing has installed, the engine is Python, so the job runs on host Python and says why."""
    ws = fr.api("GET", "/v1/sample/status")["workspace_id"]
    body = {"kind": "script", "workspace_id": ws, "engine": "python", "name": "fallback-probe", "source": {"format": "py", "content": "print('ran on host python')"},
            "runtime": {"family": "fabric-2.0-compatible"}}
    jid = fr.api("POST", "/v1/jobs", json=body)["id"]
    job = fr.wait_job(jid, 120)
    assert job["state"] == "succeeded" and job["runtime_id"] == "olcf-host-python", job
    detail = fr.api("GET", f"/v1/jobs/{jid}", params={"detail": "true"})
    fallbacks = [e for e in detail["timeline"] if (e.get("event") or e.get("kind")) == "runtime_fallback"]
    assert len(fallbacks) == 1, detail["timeline"]
    fb = fallbacks[0]
    assert fb["reason"].startswith("runtime 'olcf-rt-") and "is not installed on any worker" in fb["reason"], fb      # a registered runtime of that family exists but no worker has it
    assert "olcf-host-python" in fb["reason"] and "Install a Spark runtime" in fb["reason"], fb


def test_spark_notebook_waits_with_an_explicit_reason(fr: FirstRunStack) -> None:
    items = fr.api("GET", f"/v1/workspaces/{fr.api('GET', '/v1/sample/status')['workspace_id']}/items", params={"type": "notebook", "limit": 50})["items"]
    etl = next(i for i in items if i["name"] == "01_Retail_Medallion_ETL")
    job = fr.api("POST", "/v1/jobs", json={"kind": "notebook", "workspace_id": etl["workspace_id"], "item_id": etl["id"], "engine": "spark", "name": "needs-spark"})
    try:
        deadline = __import__("time").monotonic() + 40
        j: dict = {}
        while __import__("time").monotonic() < deadline:
            j = fr.api("GET", f"/v1/jobs/{job['id']}")
            if j.get("queue_reason"):
                break
            __import__("time").sleep(0.5)
        qr = j["queue_reason"]
        assert j["state"] == "queued" and qr["code"] == "no_runtime", j
        assert "not installed on any worker" in qr["message"] and "olcf runtime install olcf-rt-" in qr["message"]
        assert j["runtime_id"] != "olcf-host-python", "a Spark job is never placed on the host-Python runtime"
        status = fr.cli("job", "status", job["id"])
        assert status.returncode == 1 and "waiting: [no_runtime]" in status.stdout
    finally:
        fr.api("POST", f"/v1/jobs/{job['id']}/cancel", json={})


def test_dashboard_runs_with_every_tile_returning_data(fr: FirstRunStack) -> None:
    ws = fr.api("GET", "/v1/sample/status")["workspace_id"]
    rep = next(i for i in fr.api("GET", f"/v1/workspaces/{ws}/items", params={"type": "report", "limit": 50})["items"] if i["name"] == "Retail Sales Overview")
    run = fr.api("POST", f"/v1/reports/{rep['id']}/run", json={})
    assert run["tiles"], run
    for t in run["tiles"]:
        assert not t["error"], t
        assert t["row_count"] > 0 and t["rows"], f"tile {t['id']} returned no data"


def test_ai_agent_connects_over_mcp_and_runs_sql(fr: FirstRunStack) -> None:
    pytest.importorskip("mcp")
    cfg = json.loads(fr.cli("--json", "mcp", "config", "--client", "generic", check=True).stdout)["config"]
    spec = importlib.util.spec_from_file_location("agent_mcp_client_under_test", ROOT / "examples" / "agent_mcp_client.py")
    assert spec and spec.loader
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    token = cfg["headers"]["Authorization"].split(" ", 1)[1]

    async def walk() -> dict:
        async with mod.open_session(cfg["url"], token) as session:
            return await mod.explore(session, lakehouse=None, table=None, sql="select count(*) as n from fact_sales", max_rows=5)

    found = asyncio.run(walk())
    assert {"list_lakehouses", "run_sql"} <= {t["name"] for t in found["tools"]}
    assert found["result"]["rows"][0][0] > 1000
