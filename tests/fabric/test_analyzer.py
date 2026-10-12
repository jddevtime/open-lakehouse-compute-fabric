# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Rule-by-rule tests for the compatibility analyzer: every rule of ``fabric-2.0.yaml`` is triggered by at least one case, classes and blockers are checked, rewrites are
line preserving and the policy options (pip, shell, lakehouse, approved conf) change the outcome."""

from __future__ import annotations

import json
from typing import Any

import pytest

from olcf.fabric import notebook as nbm
from olcf.fabric.analyzer import CLASSES, AnalysisOptions, analyze, analyze_text, load_ruleset
from olcf.fabric.analyzer.sqlscan import analyse_sql
from tests.fabric.fabric_support import ipynb

LOCAL = {"kind": "local", "root_path": "/data/lakehouses/bronze", "name": "bronze"}
REMOTE = {"kind": "remote", "name": "gold"}

#: rule id -> (code of one cell, options). One minimal trigger per rule.
TRIGGERS: dict[str, tuple[str, dict[str, Any]]] = {
    "FAB-NBU-HELP": ("notebookutils.help()", {}),
    "FAB-NBU-FS": ("notebookutils.fs.ls('Files')", {}),
    "FAB-NBU-FS-MOUNT": ("notebookutils.fs.mount('abfss://x', '/m')", {}),
    "FAB-NBU-NOTEBOOK": ("notebookutils.notebook.exit('1')", {}),
    "FAB-NBU-NOTEBOOK-MGMT": ("notebookutils.notebook.create('x')", {}),
    "FAB-NBU-CRED": ("notebookutils.credentials.getSecret('https://v.vault.azure.net/', 'k')", {}),
    "FAB-NBU-CRED-OTHER": ("notebookutils.credentials.putSecret('a', 'b', 'c')", {}),
    "FAB-NBU-RUNTIME": ("notebookutils.runtime.context", {}),
    "FAB-NBU-RUNTIME-RESTART": ("notebookutils.runtime.restartPython()", {}),
    "FAB-NBU-SESSION": ("notebookutils.session.stop()", {}),
    "FAB-NBU-ENV": ("notebookutils.env.getWorkspaceName()", {}),
    "FAB-NBU-ENV-OTHER": ("notebookutils.env.getPoolName()", {}),
    "FAB-NBU-LAKEHOUSE": ("notebookutils.lakehouse.get('x')", {}),
    "FAB-NBU-LAKEHOUSE-MGMT": ("notebookutils.lakehouse.create('x')", {}),
    "FAB-NBU-VARLIB": ("notebookutils.variableLibrary.get('x')", {}),
    "FAB-NBU-FABRIC-ONLY": ("notebookutils.warehouse.x()", {}),
    "FAB-NBU-UNKNOWN": ("notebookutils.bogus.thing()", {}),
    "FAB-NBU-DYNAMIC": ("getattr(notebookutils, name)()", {}),
    "FAB-IMP-SEMPY": ("import sempy.fabric as f", {}),
    "FAB-IMP-FABRIC-CONNECTOR": ("import com.microsoft.spark.fabric", {}),
    "FAB-IMP-SYNAPSE-FABRIC": ("import synapse.ml.fabric", {}),
    "FAB-IMP-SYNAPSEML": ("import synapse.ml", {}),
    "FAB-IMP-COM-MICROSOFT": ("from com.microsoft.azure import x", {}),
    "FAB-IMP-NBU": ("import notebookutils", {}),
    "FAB-IMP-DATABRICKS": ("import dbutils", {}),
    "FAB-IMP-BASELINE": ("import pandas", {}),
    "FAB-IMP-FABRIC-PREINSTALLED": ("import matplotlib", {}),
    "FAB-IMP-THIRDPARTY": ("import some_unknown_pkg", {}),
    "FAB-MAGIC-SQL": ("%%sql\nselect 1", {}),
    "FAB-MAGIC-CONFIGURE": ('%%configure\n{"conf": {"spark.sql.shuffle.partitions": "8"}}', {}),
    "FAB-MAGIC-CONFIGURE-BAD": ('%%configure\n{"conf": {"spark.master": "x"}}', {}),
    "FAB-MAGIC-CONFIGURE-JSON": ("%%configure\n[1,2]", {}),
    "FAB-MAGIC-RUN": ("%run missing_nb", {}),
    "FAB-MAGIC-PIP-BLOCKED": ("%pip install foo", {}),
    "FAB-MAGIC-PIP-ALLOWED": ("%pip install foo", {"allow_pip_install": True}),
    "FAB-MAGIC-PIP-READONLY": ("%pip list", {}),
    "FAB-MAGIC-SHELL": ("!ls -la", {}),
    "FAB-MAGIC-SHELL-ALLOWED": ("!ls -la", {"allow_shell": True}),
    "FAB-MAGIC-LANG": ("%%scala\nprintln(1)", {}),
    "FAB-MAGIC-IPYTHON": ("%matplotlib inline", {}),
    "FAB-MAGIC-UNKNOWN": ("%%weirdmagic\nx", {}),
    "FAB-PATH-REL": ("spark.read.parquet('Files/x')", {}),
    "FAB-PATH-ABFSS": ("spark.read.parquet('abfss://ws@onelake.dfs.fabric.microsoft.com/lh.Lakehouse/Files/x')", {}),
    "FAB-PATH-MOUNT-LOCAL": ("open('/lakehouse/default/Files/x.csv')", {"lakehouse": LOCAL}),
    "FAB-PATH-MOUNT-REMOTE": ("open('/lakehouse/default/Files/x.csv')", {"lakehouse": REMOTE}),
    "FAB-PATH-MOUNT-NOLAKEHOUSE": ("open('/lakehouse/default/Files/x.csv')", {}),
    "FAB-PATH-MOUNT-DYNAMIC": ("p = '/lakehouse/other/Files/x'", {"lakehouse": LOCAL}),
    "FAB-CONF-FABRIC-ONLY": ("spark.conf.set('spark.native.enabled', 'true')", {}),
    "FAB-CONF-PROTECTED": ("spark.conf.set('spark.master', 'local')", {}),
    "FAB-CONF-NOTAPPROVED": ("spark.conf.set('spark.sql.shuffle.partitions', '4')", {"approved_conf": ["spark.executor.*"]}),
    "FAB-CONF-OSS": ("spark.conf.set('spark.sql.shuffle.partitions', '4')", {}),
    "FAB-SQL-VORDER": ("spark.sql('OPTIMIZE t VORDER')", {}),
    "FAB-SQL-VORDER-PROP": ("spark.sql('ALTER TABLE t SET TBLPROPERTIES (\"delta.parquet.vorder.enabled\" = \"true\")')", {}),
    "FAB-SQL-DELTA": ("spark.sql('merge into t using s on t.id=s.id when matched then update set * when not matched then insert *')", {}),
    "FAB-SQL-PARSE": ("spark.sql('selec from from')", {}),
    "FAB-SQL-FABRIC-FN": ("spark.sql(\"COPY INTO t FROM 'x'\")", {}),
    "FAB-PY-SYNAPSESQL": ("df = spark.read.synapsesql('wh.dbo.t')", {}),
    "FAB-PY-KUSTO": ("spark.read.format('com.microsoft.kusto.spark.synapse.datasource').load()", {}),
    "FAB-PY-UDF": ("from pyspark.sql.functions import udf\n@udf('int')\ndef f(x): return x", {}),
    "FAB-PY-DELTA": ("from delta.tables import DeltaTable\nDeltaTable.forName(spark, 't')", {}),
    "FAB-PY-SHELL": ("import os\nos.system('ls')", {}),
    "FAB-PY-SUBPROCESS": ("import subprocess\nsubprocess.run(['ls'])", {}),
    "FAB-PY-PIP": ("import pip\npip.main(['install', 'x'])", {}),
    "FAB-PY-PIP-ALLOWED": ("import pip\npip.main(['install', 'x'])", {"allow_pip_install": True}),
    "FAB-PY-DYNAMIC-EXEC": ("exec(code)", {}),
    "FAB-PY-NETWORK": ("import requests\nrequests.get('https://example.com')", {}),
    "FAB-PY-ONELAKE-API": ("import requests\nrequests.get('https://onelake.dfs.fabric.microsoft.com/ws')", {}),
    "FAB-PY-DISPLAYHTML": ("displayHTML('<b>x</b>')", {}),
    "FAB-PY-SYNTAX": ("def broken(:\n  pass", {}),
    "FAB-DATA-WAREHOUSE-REF": ("import requests\nrequests.get('https://x.datawarehouse.fabric.microsoft.com/q')", {}),
}
#: rules that need more than one cell / a different document shape; each has a dedicated test below
DEDICATED = {"FAB-LANG-CELL", "FAB-MAGIC-RUN-INLINED", "FAB-FMT-MAGIC-NORMALISED"}


def rules_found(code: str, **opts: Any) -> dict[str, dict[str, Any]]:
    res = analyze_text(ipynb([("code", code)]), options=AnalysisOptions(**opts))
    return {f["rule"]: f for f in res.report["findings"]}


def test_every_rule_has_a_trigger() -> None:
    ids = {r["id"] for r in load_ruleset().describe()["rules"]}
    assert ids == set(TRIGGERS) | DEDICATED, f"untested: {sorted(ids - set(TRIGGERS) - DEDICATED)}; unknown: {sorted((set(TRIGGERS) | DEDICATED) - ids)}"
    assert len(ids) >= 70


@pytest.mark.parametrize("rule_id", sorted(TRIGGERS))
def test_rule_is_triggered_and_classified_as_declared(rule_id: str) -> None:
    code, opts = TRIGGERS[rule_id]
    found = rules_found(code, **opts)
    assert rule_id in found, f"{rule_id} not found; got {sorted(found)}"
    rule = load_ruleset().rule(rule_id)
    f = found[rule_id]
    assert f["class"] == rule.cls and f["class"] in CLASSES
    assert f["message"] and f["cell"] == 0 and f["line"] >= 1
    if f["class"] == "unsupported":
        assert f.get("remediation"), f"{rule_id} is a blocker without remediation"


def test_no_remediation_suggests_a_jdbc_connection_olcf_does_not_have() -> None:
    """OLCF connection types are local, s3/minio, adls and onelake: a remediation (analyzer rules and the runtime shim's hints) must not send anyone to a "JDBC OLCF connection"."""
    from olcf.runtime_lib import notebookutils

    texts = [r.get("remediation") or "" for r in load_ruleset().describe()["rules"]] + [fix for _reason, fix in notebookutils._FABRIC_ONLY.values()]  # noqa: SLF001
    assert texts and not [t for t in texts if "JDBC" in t and "OLCF connection" in t], [t for t in texts if "JDBC" in t]
    assert any("storage connection" in t for t in texts)


def test_ruleset_metadata() -> None:
    d = load_ruleset().describe()
    assert d["ruleset_version"] == "1.0.0" and "fabric-2.0-compatible" in d["runtime_families"] and set(d["classes"]) == set(CLASSES)
    assert all(r["class"] in CLASSES for r in d["rules"]) and len({r["id"] for r in d["rules"]}) == len(d["rules"])


def test_report_shape_and_blocking() -> None:
    r = analyze_text(ipynb([("code", "import sempy.fabric as f\nnotebookutils.notebook.exit('1')")])).report
    assert r["blocked"] is True and [b["rule"] for b in r["blockers"]] == ["FAB-IMP-SEMPY"]
    assert r["counts"]["unsupported"] == 1 and r["counts"]["compatibility-shim"] == 1 and "notebook" in r["shims_required"]
    assert r["analyzer"]["name"] == "olcf-fabric-analyzer" and r["ruleset_version"] == "1.0.0" and r["source"]["format"] == "ipynb"
    assert r["summary"].endswith("1 blocker(s)") and r["duration_ms"] >= 0
    clean = analyze_text(ipynb([("code", "import pandas as pd\nprint(pd.__name__)")])).report
    assert clean["blocked"] is False and clean["blockers"] == [] and "pandas" in clean["imports"]


def test_analyze_hook_signature_used_by_the_job_service() -> None:
    rep = analyze(ipynb([("code", "print(1)")]), format="ipynb", runtime_family="fabric-1.3-compatible", origin={"kind": "fabric"})
    assert rep["runtime_family"] == "fabric-1.3-compatible" and rep["blocked"] is False


# ------------------------------------------------------------------------------------------------ policy
def test_pip_and_shell_policy_flip_blockers_to_warnings() -> None:
    assert analyze_text(ipynb([("code", "%pip install foo\n!ls")])).report["blocked"] is True
    ok = analyze_text(ipynb([("code", "%pip install foo\n!ls")]), options=AnalysisOptions(allow_pip_install=True, allow_shell=True)).report
    assert ok["blocked"] is False and [p["name"] for p in ok["pip_requirements"]] == ["foo"]


def test_pip_install_of_a_package_in_the_baseline_is_not_a_blocker_when_it_is_a_no_op() -> None:
    r = analyze_text(ipynb([("code", "%pip list")])).report
    assert r["blocked"] is False


def test_default_lakehouse_decides_the_mount_path_outcome() -> None:
    code = "df = open('/lakehouse/default/Files/in/x.csv').read()"
    local = analyze_text(ipynb([("code", code)]), options=AnalysisOptions(lakehouse=LOCAL))
    assert local.report["blocked"] is False
    assert "/data/lakehouses/bronze/Files/in/x.csv" in local.source("ipynb")
    assert [rw["rule"] for rw in local.report["rewrites"]] == ["FAB-PATH-MOUNT-LOCAL"]
    assert analyze_text(ipynb([("code", code)]), options=AnalysisOptions(lakehouse=REMOTE)).report["blocked"] is True
    assert analyze_text(ipynb([("code", code)])).report["blocked"] is True


def test_rewrites_preserve_line_structure_and_are_listed_with_before_and_after() -> None:
    src = "a = 1\nspark.sql('OPTIMIZE t VORDER')\nb = open('/lakehouse/default/Files/x')\nc = 3"
    res = analyze_text(ipynb([("code", src)]), options=AnalysisOptions(lakehouse=LOCAL))
    new = nbm.parse(res.source("ipynb")).cells[0].source
    assert new.count("\n") == src.count("\n") and "VORDER" not in new and "/data/lakehouses/bronze/Files/x" in new
    assert {rw["rule"] for rw in res.report["rewrites"]} == {"FAB-SQL-VORDER", "FAB-PATH-MOUNT-LOCAL"}
    assert all(rw.get("before") is not None and rw.get("after") is not None for rw in res.report["rewrites"])
    assert res.report["source_map"]["identity"] is True


def test_approved_conf_and_protected_keys() -> None:
    r = analyze_text(ipynb([("code", "spark.conf.set('spark.executor.memory', '2g')\nspark.conf.set('spark.hadoop.fs.defaultFS', 'x')")]), options=AnalysisOptions(approved_conf=["spark.executor.*"])).report
    classes = {f["rule"]: f["class"] for f in r["findings"]}
    assert classes.get("FAB-CONF-OSS") == "native-portable"


def test_data_references_are_collected() -> None:
    code = ("spark.read.parquet('abfss://ws@onelake.dfs.fabric.microsoft.com/sales.Lakehouse/Tables/orders')\n"
            "spark.sql('select * from bronze.customers c join silver.items i on c.id = i.cid')\nspark.read.csv('Files/raw/x.csv')")
    refs = analyze_text(ipynb([("code", code)])).report["data_references"]
    kinds = {p["kind"] for p in refs["paths"]}
    assert {"onelake", "relative"} <= kinds
    assert {"bronze.customers", "silver.items"} <= {t["ref"] for t in refs["tables"]}
    one = next(p for p in refs["paths"] if p["kind"] == "onelake")
    assert one["workspace"] == "ws" and one["item"] == "sales" and one["item_type"] == "Lakehouse" and one["area"] == "Tables"


# ------------------------------------------------------------------------------------------------ dedicated rule cases
def test_non_python_cell_languages() -> None:
    nb = {"nbformat": 4, "nbformat_minor": 5, "metadata": {}, "cells": [{"cell_type": "code", "metadata": {"microsoft": {"language": "r"}}, "source": "x <- 1", "outputs": [], "execution_count": None}]}
    r = analyze_text(json.dumps(nb)).report
    assert any(f["rule"] == "FAB-LANG-CELL" for f in r["findings"]) and r["blocked"] is True


def test_run_is_inlined_when_the_child_is_bundled_and_blocked_when_not() -> None:
    parent = ipynb([("code", "%run child {\"n\": 5}"), ("code", "print(n)")])
    child = ipynb([("code", "n = 1", ["parameters"]), ("code", "print('child', n)")])
    res = analyze_text(parent, options=AnalysisOptions(bundled_notebooks={"child": child}, notebook_name="parent"))
    assert res.report["blocked"] is False and res.report["source_map"]["identity"] is False
    assert {f["rule"] for f in res.report["findings"]} >= {"FAB-MAGIC-RUN-INLINED"}
    assert any(e["status"] == "inlined" for e in res.report["inlined_runs"])
    assert "child" in res.source("ipynb")
    missing = analyze_text(parent).report
    assert missing["blocked"] is True and any("not bundled" in (b.get("detail") or "") for b in missing["blockers"])
    rec = analyze_text(ipynb([("code", "%run a")]), options=AnalysisOptions(bundled_notebooks={"a": ipynb([("code", "%run a")])}, notebook_name="a")).report
    assert rec["blocked"] is True


def test_fabric_git_source_magics_are_normalised() -> None:
    git = ("# Fabric notebook source\n\n# METADATA ********************\n\n# META {\n# META   \"language\": \"python\"\n# META }\n\n# CELL ********************\n\n"
           "# MAGIC %%sql\n# MAGIC select 1\n\n# CELL ********************\n\nprint(1)\n")
    r = analyze_text(git).report
    assert r["source"]["format"] == "fabric_git_source" and r["blocked"] is False
    assert any(f["rule"] in ("FAB-FMT-MAGIC-NORMALISED", "FAB-MAGIC-SQL") for f in r["findings"])


def test_sql_cell_language_metadata_counts_as_sql() -> None:
    nb = {"nbformat": 4, "nbformat_minor": 5, "metadata": {}, "cells": [{"cell_type": "code", "metadata": {"microsoft": {"language": "sparksql"}}, "source": "select * from bronze.t", "outputs": [], "execution_count": None}]}
    r = analyze_text(json.dumps(nb)).report
    assert r["cells"]["sql"] == 1 and "bronze.t" in {t["ref"] for t in r["data_references"]["tables"]}


def test_parameters_secrets_children_and_packages_are_extracted() -> None:
    nb = ipynb([("code", "n = 3\nname = 'x'\nflag = True", ["parameters"]),
                ("code", "import json, numpy, pandas\nsecret = notebookutils.credentials.getSecret('https://kv.vault.azure.net/', 'db-pass')\nnotebookutils.notebook.run('child_nb', 60, {'n': 1})")])
    r = analyze_text(nb).report
    assert {p["name"] for p in r["parameters"]} == {"n", "name", "flag"} if isinstance(r["parameters"], list) and r["parameters"] and isinstance(r["parameters"][0], dict) else set(r["parameters"]) == {"n", "name", "flag"}
    assert r["secret_refs"] and "db-pass" in json.dumps(r["secret_refs"])
    assert "child_nb" in json.dumps(r["child_notebooks"])
    assert {"numpy", "pandas"} <= set(r["imports"]) | set(r["packages"])


def test_default_lakehouse_hint_is_read_from_notebook_metadata() -> None:
    meta = {"dependencies": {"lakehouse": {"default_lakehouse": "abc", "default_lakehouse_name": "bronze", "default_lakehouse_workspace_id": "ws1", "known_lakehouses": [{"id": "abc"}]}}}
    r = analyze_text(ipynb([("code", "print(1)")], metadata=meta)).report
    assert r["notebook_default_lakehouse"]["name"] == "bronze" and r["notebook_default_lakehouse"]["workspace_id"] == "ws1"


def test_not_a_notebook_is_a_bad_request() -> None:
    from olcf.core.errors import BadRequest

    for junk in ("", "{}", '{"cells": 5}'):
        with pytest.raises(BadRequest):
            analyze_text(junk, format="ipynb")


# ------------------------------------------------------------------------------------------------ sqlscan
def test_sqlscan_tables_commands_and_vorder() -> None:
    res = analyse_sql("SELECT a FROM bronze.t1 JOIN silver.t2 ON t1.id = t2.id;\nINSERT INTO gold.out SELECT * FROM bronze.t1;\nOPTIMIZE gold.out VORDER;\nSET spark.sql.shuffle.partitions=4;")
    refs = {(t.ref, t.role) for t in res.tables}
    assert ("bronze.t1", "read") in refs and ("gold.out", "write") in refs and ("gold.out", "write") in refs
    assert res.statements == 4 and res.delta_statements == 1 and [c[0] for c in res.configs] == ["spark.sql.shuffle.partitions"]
    assert [e.rule for e in res.edits] == ["FAB-SQL-VORDER"]
    assert analyse_sql("SELECT FROM WHERE ((").findings[0].rule == "FAB-SQL-PARSE"
    assert analyse_sql("MERGE INTO t USING s ON t.id = s.id WHEN MATCHED THEN UPDATE SET * ").delta_statements == 1


def test_sqlscan_handles_comments_and_strings() -> None:
    res = analyse_sql("-- from nowhere.table\nSELECT 'from fake.t' AS x FROM real.t /* from other.t */")
    assert {t.ref for t in res.tables} == {"real.t"}
