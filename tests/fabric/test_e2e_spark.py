# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""End to end with a REAL Spark session: a simulated Fabric notebook runs remotely on the cached pyspark 4.1.1 runtime through the real worker agent and runner.

The runtime (``/tmp/olcf-test-runtime``, built by ``tests/node/runtime_fixture.py``) is shared with the NODE tests: the agent here gets its own state directory with a symlink to
the cached runtime. The JVM is serialised with the sandbox-wide lock and kept small. If the runtime cannot be built (offline), the module is skipped with the reason.
"""

from __future__ import annotations

import json
import os
import sys
import types
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest

from tests.fabric.fabric_fakes import (
    API_AUDIENCE,
    FABRIC_WS,
    NOTEBOOK,
    TENANT,
    FakeFabricWorld,
    FakeNotebookUtils,
)
from tests.fabric.fabric_stack import FabricStack
from tests.fabric.fabric_support import SUBMIT_CELL, ipynb, notebook_context
from tests.spark_locking import spark_lock_held

pytestmark = pytest.mark.spark

SPARK_RT = "olcf-rt-test-4.1.1"
SPARK_NB = ipynb([
    ("code", "rows = 6", ["parameters"]),
    ("code", "import json\ndf = spark.range(rows).selectExpr('id', 'id % 2 as parity')\nagg = {str(r['parity']): r['n'] for r in df.groupBy('parity').count().withColumnRenamed('count', 'n').collect()}\n"
             "print('spark', spark.version, agg)"),
    ("code", "spark.createDataFrame([(1, 'a'), (2, 'b')], ['k', 'v']).createOrReplaceTempView('t')\n"
             "total = spark.sql('select sum(k) as s from t').collect()[0]['s']\n"
             "notebookutils.notebook.exit(json.dumps({'agg': agg, 'total': int(total)}, sort_keys=True))"),
    ("code", SUBMIT_CELL),
])
SMALL_JVM = {"spark.driver.memory": "512m", "spark.sql.shuffle.partitions": "2", "spark.ui.enabled": "false"}


@pytest.fixture(scope="module")
def spark_runtime() -> Path:
    try:
        from tests.node.runtime_fixture import CACHE, ensure_test_runtime

        ensure_test_runtime(lambda m: print(m, file=sys.stderr))
    except Exception as exc:  # noqa: BLE001
        pytest.skip(f"the real Spark test runtime is not available: {exc}")
    return CACHE


@pytest.fixture(scope="module")
def spark_exclusive(spark_runtime: Path) -> Iterator[None]:  # noqa: ARG001 - the runtime is built before the lock is taken (its smoke test uses the same lock)
    with spark_lock_held():
        yield


@pytest.fixture(scope="module")
def spark_stack(tmp_path_factory: pytest.TempPathFactory, spark_runtime: Path, spark_exclusive: None) -> Iterator[Any]:
    world = FakeFabricWorld().start()
    st = FabricStack(tmp_path_factory.mktemp("fabric-spark")).start()
    try:
        sd = tmp_path_factory.mktemp("fabric-spark-agent") / "state"
        (sd / "runtimes").mkdir(parents=True)
        os.symlink(spark_runtime / "state" / "runtimes" / SPARK_RT, sd / "runtimes" / SPARK_RT)
        shutil_state = spark_runtime / "state" / "runtimes" / "state.json"
        if shutil_state.exists():
            (sd / "runtimes" / "state.json").write_text(shutil_state.read_text())
        st.http.put("/v1/admin/settings/compute.allow_unsigned_runtimes", headers=st.admin, json={"value": True})
        manifest = json.loads((spark_runtime / "manifest.json").read_text())
        manifest.update({"family": "fabric-2.0-compatible"})
        manifest.pop("signature", None)
        r = st.http.post("/v1/runtimes", headers=st.admin, json={"manifest": manifest})
        assert r.status_code == 201, r.text
        st.start_agent(sd, name="fab-spark-agent")
        st.wait_agent_ready(SPARK_RT)
        ws = st.workspace("fabric-spark")
        sp = st.service_principal(ws)
        st.put_fabric(allowed_tenants=[TENANT], audiences=[API_AUDIENCE], workspace_map=[{"fabric_workspace_id": FABRIC_WS, "olcf_workspace": ws}], network=world.network_config(),
                      default_runtime={"family": "fabric-2.0-compatible", "runtime_id": SPARK_RT, "engine": "spark"})
        yield types.SimpleNamespace(st=st, world=world, ws=ws, headers=sp, token=sp["Authorization"].split()[1])
    finally:
        st.stop()
        world.stop()


def test_real_spark_notebook_runs_remotely(spark_stack: Any, monkeypatch: pytest.MonkeyPatch) -> None:
    import olcf_fabric as olcf

    ctx = notebook_context()
    nbu = FakeNotebookUtils(context=ctx, definitions={ctx["currentNotebookName"]: {None: SPARK_NB, "ipynb": SPARK_NB}})
    monkeypatch.setitem(sys.modules, "notebookutils", nbu)  # type: ignore[arg-type]
    olcf.config.reset_settings()
    olcf.configure(endpoint=spark_stack.st.url, token=spark_stack.token, tenant_id=TENANT, allow_insecure_http=True)
    try:
        value = olcf.run_remote(exit=False, stream_logs=False, parameters={"rows": 6}, timeout=420, poll_interval=1.0, spark_conf=SMALL_JVM,
                                resources={"driver_memory_mb": 512, "executor_memory_mb": 512, "max_executors": 1}, timeout_seconds=400, placement={"provider": "local"})
    finally:
        olcf.config.reset_settings()
    assert json.loads(value) == {"agg": {"0": 3, "1": 3}, "total": 3}
    sub = spark_stack.st.http.get("/v1/integrations/fabric/submissions", headers=spark_stack.headers, params={"limit": 1}).json()["items"][0]
    assert sub["state"] == "succeeded"
    job = spark_stack.st.http.get(f"/v1/jobs/{sub['job_id']}", headers=spark_stack.st.admin).json()
    assert job["engine"] == "spark" and job["origin"]["kind"] == "fabric"
    out = spark_stack.st.http.get(f"/v1/integrations/fabric/submissions/{sub['id']}/output", headers=spark_stack.headers).json()
    assert "spark 4." in json.dumps(out["cells"])
    assert NOTEBOOK
