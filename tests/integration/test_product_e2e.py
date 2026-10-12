# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
# ruff: noqa: S608 - the SQL texts are constants of this test (names of the sample lakehouse), not user input
"""Whole-product end to end: ONE real control plane (uvicorn, every module) and TWO real worker agents, each a separate OS process with its own state directory, the cached
Spark 4.1.1 runtime, the real runner / kernel / Spark daemons.  The tests are ordered steps of one story and share the stack:

1. bootstrap, enrolment of two agents, the runtime ready on both nodes, the sample retail lakehouse installed;
2. the sample's pure-Python notebook as a batch job on a per-job dependency layer (deltalake + duckdb + matplotlib, hash-pinned, built by the agent from a wheelhouse);
3. a pipeline of two notebook activities, the second failing once and succeeding on retry, writing / reading a Delta table of the sample lakehouse;
4. an MCP ``run_sql`` over the sample and over the table the pipeline wrote;
5. (spark) a PySpark job through the job service on a Spark Standalone cluster whose executors run on BOTH agents;
6. (spark) a simulated Fabric notebook submitted with the real SDK, running on that cluster and reading the local lakehouse through relative ``Files/`` paths;
7. cancellation of a job and of a pipeline run (the job it started is cancelled too, nothing keeps running);
8. the topology / connectivity matrix is green.

Spark here reads Parquet / CSV: the Delta jars cannot be downloaded in the sandbox (Spark + Delta is verified by ``scripts/verify_spark_delta.py`` on a networked machine); Delta
is written and read by delta-rs (the notebooks' ``deltalake``) and DuckDB.  Skipped (with the reason) when the cached runtime or the package wheels cannot be obtained.
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
import time
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest

ROOT = Path(__file__).resolve().parents[2]
for _p in (str(ROOT), str(ROOT / "src"), str(ROOT / "sdk" / "fabric"), str(ROOT / "tests" / "fabric")):
    if _p not in sys.path:
        sys.path.insert(0, _p)

from tests.e2e.stack import RUNTIME_ID, E2EStack  # noqa: E402

WHEELHOUSE = Path("/tmp/olcf-test-wheelhouse-e2e")  # noqa: S108 - shared, content-addressed by pip; re-used between runs
AGENT_TOML = 'max_memory_mb = 3000\ndaemon_memory = "256m"\nstop_children_on_exit = true\n'
LAKEHOUSE = "retail_lakehouse"
TERMINAL = ("succeeded", "failed", "cancelled", "timed_out")
SPARK_RES = {"driver_cores": 1, "driver_memory_mb": 768, "executor_cores": 1, "executor_memory_mb": 512, "min_executors": 2, "max_executors": 2}


# ================================================================================================================================ helpers
def ipynb(cells: list[tuple[str, str] | tuple[str, str, list[str]]]) -> dict[str, Any]:
    out = []
    for c in cells:
        tags = c[2] if len(c) > 2 else []
        cell: dict[str, Any] = {"cell_type": c[0], "metadata": {"tags": tags} if tags else {}, "source": c[1]}
        if c[0] == "code":
            cell.update({"execution_count": None, "outputs": []})
        out.append(cell)
    return {"nbformat": 4, "nbformat_minor": 5, "metadata": {"kernelspec": {"name": "python3", "display_name": "Python 3", "language": "python"}}, "cells": out}


def ensure_wheelhouse() -> Path:
    """Wheels for the environment layer (downloaded once, cached in /tmp); skips the tests that need them when PyPI is unreachable."""
    if len(list(WHEELHOUSE.glob("deltalake-*.whl"))) and len(list(WHEELHOUSE.glob("duckdb-*.whl"))) and len(list(WHEELHOUSE.glob("matplotlib-*.whl"))):
        return WHEELHOUSE
    WHEELHOUSE.mkdir(parents=True, exist_ok=True)
    res = subprocess.run([sys.executable, "-m", "pip", "download", "--only-binary=:all:", "-q", "-d", str(WHEELHOUSE), "deltalake", "duckdb", "matplotlib"],  # noqa: S603
                         capture_output=True, text=True, timeout=600, check=False)
    if res.returncode != 0:
        pytest.skip(f"the package wheels for the environment layer cannot be downloaded: {res.stderr[-300:]}")
    return WHEELHOUSE


class Product:
    """The running product plus a few helpers."""

    def __init__(self, st: E2EStack) -> None:
        self.st = st
        self.ws: str = ""
        self.lakehouse: str = ""
        self.env: str = ""
        self.settings: dict[str, Any] = {}
        self.items: dict[str, dict[str, Any]] = {}

    def api(self, method: str, path: str, **kw: Any) -> Any:
        return self.st.api(method, path, **kw)

    def wait_job(self, job_id: str, timeout: float = 300) -> dict[str, Any]:
        end = time.monotonic() + timeout
        job: dict[str, Any] = {}
        while time.monotonic() < end:
            job = self.api("GET", f"/v1/jobs/{job_id}")
            if job["state"] in TERMINAL:
                return job
            time.sleep(0.5)
        raise AssertionError(f"job {job_id} did not finish in {timeout}s: {job.get('state')} {job.get('wait_reason')}")

    def wait_run(self, run_id: str, timeout: float = 300) -> dict[str, Any]:
        end = time.monotonic() + timeout
        run: dict[str, Any] = {}
        while time.monotonic() < end:
            run = self.api("GET", f"/v1/pipeline-runs/{run_id}")
            if run["state"] in ("succeeded", "failed", "cancelled"):
                return run
            time.sleep(0.5)
        raise AssertionError(f"pipeline run {run_id} did not finish in {timeout}s: {run.get('state')}")

    def create_item(self, type_: str, name: str, definition: dict[str, Any]) -> dict[str, Any]:
        item = self.api("POST", f"/v1/workspaces/{self.ws}/items", json={"type": type_, "name": name, "definition": definition})
        self.items[name] = item
        return item

    def notebook(self, name: str, nb: dict[str, Any], **settings: Any) -> dict[str, Any]:
        return self.create_item("notebook", name, {"nbformat": nb, "settings": {**self.settings, **settings}})

    def job_failure(self, job: dict[str, Any]) -> str:
        return json.dumps({k: job.get(k) for k in ("state", "state_reason", "wait_reason", "failure")}, default=str)

    def agent_dirs(self) -> list[Path]:
        return [a.state_dir for a in self.st.agents]


class Mcp:
    """Minimal MCP client over the real HTTP endpoint (Streamable HTTP, JSON responses)."""

    def __init__(self, st: E2EStack) -> None:
        self.st = st
        self.h = {**st.admin, "Host": "localhost", "Content-Type": "application/json", "Accept": "application/json, text/event-stream"}
        self.n = 0
        r = self.rpc("initialize", {"protocolVersion": "2025-06-18", "capabilities": {}, "clientInfo": {"name": "product-e2e", "version": "1"}})
        assert r.status_code == 200, r.text
        self.h["Mcp-Session-Id"] = r.headers["Mcp-Session-Id"]
        n = st.http.post("/mcp", content=json.dumps({"jsonrpc": "2.0", "method": "notifications/initialized"}), headers=self.h)
        assert n.status_code == 202, n.text

    def rpc(self, method: str, params: dict[str, Any] | None = None) -> Any:
        self.n += 1
        body: dict[str, Any] = {"jsonrpc": "2.0", "id": self.n, "method": method}
        if params is not None:
            body["params"] = params
        return self.st.http.post("/mcp", content=json.dumps(body), headers=self.h)

    def call(self, name: str, **args: Any) -> dict[str, Any]:
        r = self.rpc("tools/call", {"name": name, "arguments": args})
        assert r.status_code == 200, r.text
        res = r.json()["result"]
        data = json.loads(res["content"][0]["text"])
        assert res["isError"] is False, data
        return data


# ================================================================================================================================ fixtures
@pytest.fixture(scope="module")
def product(tmp_path_factory: pytest.TempPathFactory) -> Iterator[Product]:
    root = tmp_path_factory.mktemp("product-e2e")
    previous = os.environ.get("OLCF_COMPUTE__RESERVE_CORES")
    os.environ["OLCF_COMPUTE__RESERVE_CORES"] = "0"  # two 2-core "nodes" share this 2-core sandbox: keep the planner's per-node headroom out of the way
    st = E2EStack(root)
    if not st.has_runtime_cache():
        pytest.skip("the cached Spark test runtime (/tmp/olcf-test-runtime) is not available; build it with tests/node/runtime_fixture.py")
    wheels = ensure_wheelhouse()
    st.start()
    try:
        p = Product(st)
        assert st.register_runtime() == RUNTIME_ID
        for name in ("agent-a", "agent-b"):
            sd = st.root / "agents" / name / "state"
            sd.mkdir(parents=True, exist_ok=True)
            (sd / "agent.toml").write_text(AGENT_TOML)  # a "small node": 3 GB handed out, small Spark daemons, daemons stop with the agent
            st.add_agent(name)
        st.wait_nodes_online(2, timeout=120)
        install = st.api("POST", "/v1/sample/install", json={"size": "small"})
        p.ws, p.lakehouse = install["workspace_id"], install["lakehouse_id"]
        p.settings = {"engine": "python", "default_lakehouse": {"name": LAKEHOUSE}}
        st.api("PUT", "/v1/admin/settings/environments.wheelhouses", json={"value": [str(wheels)]})
        env = p.create_item("environment", "e2e-env", {"python": {"requirements": ["deltalake", "duckdb", "matplotlib"], "index": {"no_index": True, "find_links": [str(wheels)]}}})
        pub = st.api("POST", f"/v1/environments/{env['id']}/publish", json={"comment": "product e2e"})
        assert pub["created"] and {x["name"] for x in pub["lock"]["python"]["packages"]} >= {"deltalake", "duckdb", "matplotlib"}
        p.env = env["id"]
        p.settings = {**p.settings, "environment_id": env["id"]}
        yield p
    finally:
        st.stop()
        if previous is None:
            os.environ.pop("OLCF_COMPUTE__RESERVE_CORES", None)
        else:
            os.environ["OLCF_COMPUTE__RESERVE_CORES"] = previous


# ================================================================================================================================ 1. bootstrap
def test_bootstrap_two_agents_runtime_ready_and_sample_installed(product: Product) -> None:
    workers = product.api("GET", "/v1/workers")["items"]
    assert sorted(w["name"] for w in workers if w["state"] == "online") == ["agent-a", "agent-b"]
    assert all(RUNTIME_ID in {r["id"] for r in w["runtimes"] if r.get("state") == "ready"} for w in workers)
    # each agent hands out only what its agent.toml allows, and the planner knows (not the machine's inventory)
    assert all(w["inventory"]["capacity"]["memory_mb"] == 3000 for w in workers)
    status = product.api("GET", "/v1/sample/status")
    assert status["installed"] is True


# ================================================================================================================================ 2. python notebook on a layer
def test_python_notebook_batch_job_on_a_dependency_layer(product: Product) -> None:
    nb = next(i for i in product.api("GET", f"/v1/workspaces/{product.ws}/items")["items"] if i["name"] == "02_Retail_Explore_Python")
    run = product.api("POST", f"/v1/notebooks/{nb['id']}/run", json={"environment_id": product.env})
    job = product.wait_job(run["id"], 300)
    assert job["state"] == "succeeded", product.job_failure(job)
    assert job["engine"] == "python" and job["runtime_id"] == RUNTIME_ID
    metrics = job["result"]["metrics"]
    assert metrics["cells_executed"] >= 10 and metrics["exit_called"] is False
    # the job record fingerprints the runtime AND the dependency layer it ran on
    dep = job["result"]["dependencies"]
    assert dep["runtime_id"] == RUNTIME_ID and dep["layer_key"].startswith("sha256:") and dep["environment"]["name"] == "e2e-env"
    assert dep["environment_lock_hash"] == dep["environment"]["lock_hash"]
    # the agent that ran it built the layer once below its state directory
    layer_dirs = [d / "layers" / dep["layer_key"].replace(":", "-") for d in product.agent_dirs()]
    assert any(d.is_dir() for d in layer_dirs), layer_dirs
    # the executed notebook is an artifact: the last cell's reconciliation printed, no cell raised
    out = product.st.http.get(f"/v1/jobs/{job['id']}/artifacts/output.ipynb", headers=product.st.admin)
    assert out.status_code == 200, out.text[:300]
    executed = out.json()
    text = json.dumps(executed)
    assert "reconciles with gold_daily_sales" in text
    assert not [o for c in executed["cells"] if c["cell_type"] == "code" for o in c.get("outputs", []) if o.get("output_type") == "error"]


# ================================================================================================================================ 3. pipeline with retry
PRODUCE = ipynb([
    ("code", "n = 3", ["parameters"]),
    ("code", """
import json
import pandas as pd
from deltalake import write_deltalake
from olcf.runtime_lib import paths

root = paths.resolve("Tables/e2e_scores", for_="local")
write_deltalake(root, pd.DataFrame({"k": list(range(n)), "v": [i * 10 for i in range(n)]}), mode="overwrite")
notebookutils.notebook.exit(json.dumps({"rows": n}))
"""),
])
CONSUME = ipynb([
    ("code", """
import json
from pathlib import Path

from deltalake import DeltaTable
from olcf.runtime_lib import paths

marker = Path(paths.resolve("Files/e2e_attempts.txt", for_="local"))
marker.parent.mkdir(parents=True, exist_ok=True)
seen = marker.read_text() if marker.exists() else ""
marker.write_text(seen + "x")
if not seen:
    raise RuntimeError("the first attempt fails on purpose")
tbl = DeltaTable(str(paths.resolve("Tables/e2e_scores", for_="local"))).to_pyarrow_table()
notebookutils.notebook.exit(json.dumps({"rows": tbl.num_rows, "sum": sum(tbl.column("v").to_pylist()), "attempts": len(seen) + 1}))
"""),
])


def test_pipeline_two_notebook_activities_with_retry(product: Product) -> None:
    product.notebook("e2e_produce", PRODUCE)
    product.notebook("e2e_consume", CONSUME)
    pipe = product.create_item("pipeline", "e2e_pipeline", {"schema": "olcf.pipeline/1", "activities": [
        {"name": "Produce", "type": "notebook", "typeProperties": {"notebook": "e2e_produce", "parameters": {"n": 4}}},
        {"name": "Consume", "type": "notebook", "dependsOn": ["Produce"], "typeProperties": {"notebook": "e2e_consume"}, "policy": {"retries": 1, "retryBackoffSeconds": 0}},
    ]})
    run = product.api("POST", f"/v1/pipelines/{pipe['id']}/runs", json={})
    done = product.wait_run(run["id"], 300)
    acts = {a["name"]: a for a in product.api("GET", f"/v1/pipeline-runs/{run['id']}/activities")["items"]}
    assert done["state"] == "succeeded", json.dumps({k: (a["state"], a["error"]) for k, a in acts.items()}, default=str)
    assert acts["Produce"]["attempt"] == 1 and len(acts["Produce"]["job_ids"]) == 1
    # the retry: attempt 1 failed with the user-code error, attempt 2 ran as a NEW job and succeeded
    c = acts["Consume"]
    assert c["attempt"] == 2 and [a["state"] for a in c["attempts"]] == ["failed", "succeeded"] and len(set(c["job_ids"])) == 2
    assert "first attempt fails on purpose" in json.dumps(c["attempts"][0]["error"])
    first, second = (product.api("GET", f"/v1/jobs/{j}") for j in c["job_ids"])
    assert first["state"] == "failed" and first["failure_category"] == "user-code" and second["state"] == "succeeded"
    assert first["origin"]["kind"] == "pipeline"
    # output tables / exit values travelled through the lakehouse and the job records
    produced = product.api("GET", f"/v1/jobs/{acts['Produce']['job_ids'][0]}")
    assert json.loads(produced["exit_value"]) == {"rows": 4}
    assert json.loads(second["exit_value"]) == {"rows": 4, "sum": 60, "attempts": 2}


# ================================================================================================================================ 4. MCP
def test_mcp_run_sql_over_the_sample_and_the_pipeline_output(product: Product) -> None:
    m = Mcp(product.st)
    tools = {t["name"] for t in m.rpc("tools/list").json()["result"]["tools"]}
    assert {"run_sql", "list_tables", "describe_table"} <= tools
    sales = m.call("run_sql", sql=f"SELECT count(*) AS n, round(sum(net_amount), 2) AS total FROM {LAKEHOUSE}.fact_sales", lakehouse=LAKEHOUSE)
    assert sales["rows"][0][0] == 6000 and sales["truncated"] is False and sales["tables"] == [f"{LAKEHOUSE}.fact_sales"]
    gold = m.call("run_sql", sql=f"SELECT round(sum(net_revenue), 2) FROM {LAKEHOUSE}.gold_daily_sales", lakehouse=LAKEHOUSE)
    assert abs(gold["rows"][0][0] - sales["rows"][0][1]) < 1.0  # the lakehouse reconciles (the sample's own invariant)
    mine = m.call("run_sql", sql=f"SELECT count(*) AS n, sum(v) AS s FROM {LAKEHOUSE}.e2e_scores", lakehouse=LAKEHOUSE)  # the table the pipeline's first activity wrote
    assert mine["rows"] == [[4, 60]]
    # run_sql scans storage, the logical catalog (list_tables / search) learns about tables a notebook wrote directly on the next refresh
    product.api("POST", f"/v1/lakehouses/{product.lakehouse}/refresh")
    names = {t["name"] for t in m.call("list_tables", lakehouse=LAKEHOUSE)["tables"]}
    assert {"fact_sales", "dim_customer", "gold_daily_sales", "e2e_scores"} <= names
    # read-only by construction
    bad = m.rpc("tools/call", {"name": "run_sql", "arguments": {"sql": f"DELETE FROM {LAKEHOUSE}.e2e_scores", "lakehouse": LAKEHOUSE}}).json()["result"]
    assert bad["isError"] is True


# ================================================================================================================================ 5. distributed PySpark
EXECUTOR_PROBE = '''
import os


def where(rows):
    yield os.getcwd()


sc = spark.sparkContext
cwds = sorted(set(sc.parallelize(range(16), 4).mapPartitions(where).collect()))
total = spark.range(1000).selectExpr("sum(id) AS s").collect()[0]["s"]
EXIT_VALUE = {"cwds": cwds, "total": int(total), "master": sc.master}
'''


@pytest.mark.spark
@pytest.mark.timeout(600)
def test_pyspark_job_runs_executors_on_both_agents(product: Product, spark_lock: None) -> None:  # noqa: ARG001
    job = product.api("POST", "/v1/jobs", json={"kind": "script", "workspace_id": product.ws, "engine": "spark", "name": "distributed-probe",
                                                "source": {"format": "py", "content": EXECUTOR_PROBE}, "resources": SPARK_RES,
                                                "runtime": {"runtime_id": RUNTIME_ID}, "policy": {"timeout_seconds": 420}})
    done = product.wait_job(job["id"], 480)
    assert done["state"] == "succeeded", product.job_failure(done)
    assert done["provider"] == "standalone" and done["application_id"] and done["cluster_id"]
    value = done["exit_value"]
    assert value["total"] == 499500 and value["master"].startswith("spark://")
    # the tasks ran in the work directories of the Spark workers of BOTH agents (separate processes, separate state directories)
    dirs = product.agent_dirs()
    for d in dirs:
        assert any(str(d) in cwd for cwd in value["cwds"]), (str(d), value["cwds"])
    apps = [a for a in product.api("GET", "/v1/spark-applications")["items"] if a["job_id"] == done["id"]]
    assert apps and apps[0]["cluster_id"] == done["cluster_id"] and apps[0]["application_id"] == done["application_id"]


# ================================================================================================================================ 6. Fabric via the SDK on Spark
FABRIC_SPARK_NB = ipynb([
    ("code", "limit = 10", ["parameters"]),
    ("code", """
import json
import os

df = spark.read.option("header", True).option("inferSchema", True).csv("Files/raw/sales/")
rows = df.count()
sums = {r["channel"]: round(r["revenue"], 2) for r in df.groupBy("channel").agg({"net_amount": "sum"}).withColumnRenamed("sum(net_amount)", "revenue").collect()}


def where(it):
    yield os.getcwd()


cwds = sorted(set(spark.sparkContext.parallelize(range(8), 4).mapPartitions(where).collect()))
print("spark", spark.version, "rows", rows)
notebookutils.notebook.exit(json.dumps({"rows": rows, "channels": sorted(sums), "cwds": cwds, "limit": limit}))
"""),
    ("code", "import olcf_fabric as olcf\nolcf.run_remote()\n"),
])


@pytest.mark.spark
@pytest.mark.timeout(600)
def test_simulated_fabric_notebook_runs_on_spark_over_relative_files_paths(product: Product, spark_lock: None, monkeypatch: pytest.MonkeyPatch) -> None:  # noqa: ARG001
    import olcf_fabric as olcf
    from tests.fabric.fabric_fakes import (
        API_AUDIENCE,
        FABRIC_WS,
        NOTEBOOK,
        TENANT,
        FakeFabricWorld,
        FakeNotebookUtils,
    )
    from tests.fabric.fabric_fakes import LAKEHOUSE as FAKE_LH

    st = product.st
    world = FakeFabricWorld().start()
    try:
        r = st.http.put("/v1/integrations/fabric", headers=st.admin, json={
            "allowed_tenants": [TENANT], "audiences": [API_AUDIENCE], "workspace_map": [{"fabric_workspace_id": FABRIC_WS, "olcf_workspace": product.ws}],
            "network": world.network_config(), "default_runtime": {"family": "fabric-2.0-compatible", "runtime_id": RUNTIME_ID, "engine": "spark"}})
        assert r.status_code == 200, r.text
        user = st.http.post("/v1/identity/users", headers=st.admin, json={"name": "fabric-sp", "kind": "service", "roles": [{"role": "ServicePrincipal", "scope_type": "workspace", "scope_id": product.ws}]})
        assert user.status_code == 201, user.text
        tok = st.http.post("/v1/identity/tokens", headers=st.admin, json={"name": "t", "principal_id": user.json()["id"], "scopes": ["fabric"]})
        assert tok.status_code == 201, tok.text
        ctx = {"currentNotebookName": "sales_by_channel", "currentNotebookId": NOTEBOOK, "currentWorkspaceName": "Sales", "currentWorkspaceId": FABRIC_WS,
               "defaultLakehouseName": LAKEHOUSE, "defaultLakehouseId": FAKE_LH, "defaultLakehouseWorkspaceName": "Sales", "defaultLakehouseWorkspaceId": FABRIC_WS,
               "currentRunId": "run-0001", "parentRunId": None, "rootRunId": "run-0001", "isForPipeline": False, "isReferenceRun": False,
               "userName": "alice@contoso.example", "userId": "00000000-0000-4000-8000-0000000000aa", "productType": "Fabric"}
        text = json.dumps(FABRIC_SPARK_NB)
        nbu = FakeNotebookUtils(context=ctx, definitions={ctx["currentNotebookName"]: {None: text, "ipynb": text}})
        monkeypatch.setitem(sys.modules, "notebookutils", nbu)  # type: ignore[arg-type]
        olcf.config.reset_settings()
        olcf.configure(endpoint=st.url, token=tok.json()["token"], tenant_id=TENANT, allow_insecure_http=True)
        try:
            value = olcf.run_remote(exit=False, stream_logs=False, parameters={"limit": 5}, timeout=480, poll_interval=1.0, timeout_seconds=420, resources=SPARK_RES)
        finally:
            olcf.config.reset_settings()
    finally:
        world.stop()
    out = json.loads(value)
    assert out["rows"] == 6000 and out["limit"] == 5 and len(out["channels"]) >= 2  # read through "Files/raw/sales/" - relative to the default lakehouse
    for d in product.agent_dirs():
        assert any(str(d) in cwd for cwd in out["cwds"]), (str(d), out["cwds"])
    sub = st.http.get("/v1/integrations/fabric/submissions", headers={"Authorization": f"Bearer {tok.json()['token']}"}, params={"limit": 1}).json()["items"][0]
    assert sub["state"] == "succeeded"
    job = product.api("GET", f"/v1/jobs/{sub['job_id']}")
    assert job["engine"] == "spark" and job["origin"]["kind"] == "fabric" and job["provider"] == "standalone"
    assert job["request"]["default_lakehouse"] == product.lakehouse, job["request"]["default_lakehouse"]  # resolved by name to the sample's own lakehouse item
    printed = st.http.get(f"/v1/integrations/fabric/submissions/{sub['id']}/output", headers={"Authorization": f"Bearer {tok.json()['token']}"}).json()
    assert "spark 4." in json.dumps(printed["cells"]) and "rows 6000" in json.dumps(printed["cells"])


# ================================================================================================================================ 7. cancellation
SLEEPER = "import time\nprint('sleeper started', flush=True)\ntime.sleep(600)\n"


def _wait_state(product: Product, job_id: str, state: str, timeout: float = 90) -> dict[str, Any]:
    end = time.monotonic() + timeout
    job: dict[str, Any] = {}
    while time.monotonic() < end:
        job = product.api("GET", f"/v1/jobs/{job_id}")
        if job["state"] == state or job["state"] in TERMINAL:
            return job
        time.sleep(0.3)
    raise AssertionError(f"job {job_id} never reached {state}: {job.get('state')}")


def _no_runner_left(job_id: str) -> bool:
    res = subprocess.run(["pgrep", "-f", f"olcf.runtime_lib.runner.*{job_id}"], capture_output=True, text=True, check=False)  # noqa: S603, S607
    return res.returncode != 0


def test_cancel_a_job_and_a_pipeline_run(product: Product) -> None:
    job = product.api("POST", "/v1/jobs", json={"kind": "script", "workspace_id": product.ws, "engine": "python", "name": "sleeper", "source": {"format": "py", "content": SLEEPER},
                                                "runtime": {"runtime_id": RUNTIME_ID}})
    assert _wait_state(product, job["id"], "running")["state"] == "running"
    product.api("POST", f"/v1/jobs/{job['id']}/cancel", json={})
    done = product.wait_job(job["id"], 60)
    assert done["state"] == "cancelled" and done["failure"]["category"] == "cancellation"
    # a pipeline run: cancelling the run cancels the job its activity started
    pipe = product.create_item("pipeline", "e2e_cancel", {"schema": "olcf.pipeline/1", "activities": [{"name": "Sleep", "type": "script", "typeProperties": {"code": SLEEPER}}]})
    run = product.api("POST", f"/v1/pipelines/{pipe['id']}/runs", json={})
    deadline = time.monotonic() + 60
    job_ids: list[str] = []
    while time.monotonic() < deadline and not job_ids:
        acts = product.api("GET", f"/v1/pipeline-runs/{run['id']}/activities")["items"]
        job_ids = acts[0]["job_ids"] if acts else []
        time.sleep(0.3)
    assert job_ids
    assert _wait_state(product, job_ids[0], "running")["state"] == "running"
    product.st.http.post(f"/v1/pipeline-runs/{run['id']}/cancel", headers=product.st.admin, json={}).raise_for_status()
    assert product.wait_run(run["id"], 60)["state"] == "cancelled"
    assert product.wait_job(job_ids[0], 60)["state"] == "cancelled"
    # nothing keeps running and the cores come back
    end = time.monotonic() + 30
    while time.monotonic() < end and not (_no_runner_left(done["id"]) and _no_runner_left(job_ids[0])):
        time.sleep(0.5)
    assert _no_runner_left(done["id"]) and _no_runner_left(job_ids[0])


# ================================================================================================================================ 8. topology
def test_topology_is_green(product: Product) -> None:
    topo = product.api("GET", "/v1/compute/topology")
    s = topo["summary"]
    assert s["nodes_total"] == 2 and s["nodes_online"] == 2 and s["jobs_running"] == 0 and s["jobs_queued"] == 0
    assert all(n["state"] == "online" for n in topo["nodes"])
    assert all(c["status"] not in ("down", "error", "failed") for n in topo["nodes"] for c in n.get("components", []))
    comps = {c["name"]: c for c in topo["control_plane"]["components"]}
    host_dependent = {"data-disk"}  # free space of the machine running the tests is not under test
    host_dependent_probes = {"disk"}
    # `runtime-trust` is "degraded" BY DESIGN while the development runtime-signing key is trusted (this product runs as a development installation, like quickstart):
    # a reminder, not a fault - but never "down" (that would mean the key is trusted in production)
    by_design = {"runtime-trust": "degraded"}
    bad = {k: (c["status"], c["detail"]) for k, c in comps.items() if k not in host_dependent and c["status"] not in ("ok", "unknown") and c["status"] != by_design.get(k)}
    assert not bad, bad
    assert comps["runtime-trust"]["status"] == "degraded" and "development runtime-signing key is trusted" in comps["runtime-trust"]["detail"]
    assert comps["runtimes"]["status"] == "ok" and comps["scheduler"]["status"] == "ok" and comps["database"]["status"] == "ok"
    # the Spark cluster the earlier steps created is up, its UI is reachable only through the control plane's proxy path
    clusters = [c for c in topo["clusters"] if c["runtime_id"] == RUNTIME_ID]
    if clusters:  # (absent when only the non-spark steps were selected)
        assert all(c["state"] == "up" for c in clusters)
        assert all("/v1/spark-ui/" in (c.get("ui_path") or c.get("ui_url") or "/v1/spark-ui/") for c in clusters)
    # node-perspective connectivity matrix: every probe from every node passes
    matrix = product.api("POST", "/v1/compute/test", json={})
    assert [row["node"] for row in matrix["matrix"]["rows"]] == ["agent-a", "agent-b"]
    failed = [(row["node"], name, r["detail"]) for row in matrix["matrix"]["rows"] for name, r in row["results"].items() if not r["ok"] and name not in host_dependent_probes]
    assert not failed, failed
