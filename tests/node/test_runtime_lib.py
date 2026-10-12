# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Unit tests for the runtime-side library (no JVM, no kernel): Spark conf generation, path resolution, notebook parsing,
parameter injection, magics helpers, failure classification and bounded output buffers.

The Delta / Iceberg configuration generation is covered **here only**: the sandbox has no Delta jars, so the real
Delta-on-Spark behaviour is exercised by ``scripts/verify_spark_delta.py`` on a machine with network access.
"""

# ruff: noqa: S108
from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest

from olcf.core.errors import Unsupported
from olcf.runtime_lib import failures, paths
from olcf.runtime_lib import spark as sparkmod
from olcf.runtime_lib.executor import (
    has_parameters_cell,
    inject_parameters,
    load_notebook,
    parameter_source,
    parse_fabric_git_source,
    parse_ipynb,
    parse_py_percent,
)
from olcf.runtime_lib.logbuf import BoundedBuffer, OutputBudget, truncate_text
from olcf.runtime_lib.magics import (
    DependencyBlocked,
    MagicError,
    configure_to_conf,
    find_notebook,
    pip_magic,
    split_statements,
)

DELTA_LOCK = {"jars": [
    {"coordinate": "io.delta:delta-spark_2.13:4.0.0", "file": "delta-spark_2.13-4.0.0.jar", "linked": "symlink"},
    {"coordinate": "io.delta:delta-storage:4.0.0", "file": "delta-storage-4.0.0.jar", "linked": "symlink"},
]}
ICEBERG_LOCK = {"jars": [{"coordinate": "org.apache.iceberg:iceberg-spark-runtime-4.0_2.13:1.10.0",
                          "file": "iceberg-spark-runtime-4.0_2.13-1.10.0.jar", "linked": "symlink"}]}
LH = {"name": "sales lh", "id": "lh-1", "root_uri": "abfss://fs@acct.dfs.core.windows.net/lh",
      "files_uri": "abfss://fs@acct.dfs.core.windows.net/lh/Files", "tables_uri": "abfss://fs@acct.dfs.core.windows.net/lh/Tables",
      "format": "delta"}
OTHER = {"name": "other", "id": "lh-2", "root_uri": "file:///data/other", "files_uri": "file:///data/other/Files",
         "tables_uri": "file:///data/other/Tables"}


# --------------------------------------------------------------------------------------------- spark conf
class TestSparkConf:
    def test_local_defaults(self):
        conf = sparkmod.build_spark_conf({"job_id": "j1"})
        assert conf["spark.master"] == "local[*]"
        assert conf["spark.app.name"] == "j1"
        assert "spark.driver.host" not in conf

    def test_runtime_defaults_then_job_conf_then_overrides(self):
        manifest = {"spark_conf_defaults": {"spark.sql.shuffle.partitions": "8", "spark.ui.enabled": True}}
        spec = {"spark": {"conf": {"spark.sql.shuffle.partitions": "4"}}}
        conf = sparkmod.build_spark_conf(spec, manifest=manifest, overrides={"spark.sql.shuffle.partitions": 2})
        assert conf["spark.sql.shuffle.partitions"] == "2"  # override wins over job conf wins over default
        assert conf["spark.ui.enabled"] == "true"  # bools rendered the Spark way

    def test_cluster_networking(self):
        spec = {"spark": {"master": "spark://10.0.0.1:7077", "driver_host": "10.0.0.5", "driver_port": 40001, "block_manager_port": 40002}}
        conf = sparkmod.build_spark_conf(spec)
        assert conf["spark.master"] == "spark://10.0.0.1:7077"
        assert conf["spark.driver.host"] == "10.0.0.5"
        assert conf["spark.driver.bindAddress"] == "10.0.0.5"
        assert conf["spark.driver.port"] == "40001"
        assert conf["spark.blockManager.port"] == "40002" == conf["spark.driver.blockManager.port"]
        assert conf["spark.port.maxRetries"] == "32"

    def test_delta_generation(self):
        conf = sparkmod.build_spark_conf({}, lock=DELTA_LOCK, runtime_root=Path("/rt"))
        assert sparkmod.DELTA_EXTENSION in conf["spark.sql.extensions"].split(",")
        assert conf["spark.sql.catalog.spark_catalog"] == sparkmod.DELTA_CATALOG
        assert conf["spark.sql.sources.default"] == "delta"
        assert "spark.jars" not in conf  # symlinked into $SPARK_HOME/jars: not shipped again

    def test_delta_extension_survives_conf_that_replaces_extensions(self):
        spec = {"spark": {"conf": {"spark.sql.extensions": "com.example.Ext"}}}
        conf = sparkmod.build_spark_conf(spec, lock=DELTA_LOCK)
        exts = conf["spark.sql.extensions"].split(",")
        assert "com.example.Ext" in exts and sparkmod.DELTA_EXTENSION in exts

    def test_iceberg_and_delta_together(self):
        lock = {"jars": DELTA_LOCK["jars"] + ICEBERG_LOCK["jars"]}
        exts = sparkmod.build_spark_conf({}, lock=lock)["spark.sql.extensions"].split(",")
        assert exts == [sparkmod.DELTA_EXTENSION, sparkmod.ICEBERG_EXTENSION]

    def test_no_delta_without_jars(self):
        conf = sparkmod.build_spark_conf({}, lock={"jars": [{"coordinate": "org.apache.hadoop:hadoop-azure:3.4.1", "file": "x.jar"}]})
        assert "spark.sql.extensions" not in conf
        assert "spark.sql.sources.default" not in conf

    def test_unlinked_jars_listed_in_spark_jars(self, tmp_path):
        lock = {"jars": [{"coordinate": "g:a:1", "file": "a-1.jar"}, {"coordinate": "g:b:1", "file": "b-1.jar", "linked": "symlink"}]}
        conf = sparkmod.build_spark_conf({"spark": {"jars": ["/opt/extra.jar", "s3a://b/c.jar"]}}, lock=lock, runtime_root=tmp_path)
        jars = conf["spark.jars"].split(",")
        assert (tmp_path / "jars" / "a-1.jar").resolve().as_uri() in jars
        assert "file:///opt/extra.jar" in jars and "s3a://b/c.jar" in jars
        assert not any("b-1.jar" in j for j in jars)

    def test_storage_conf_prefixing_and_warehouse(self):
        spec = {"storage_conf": {"spark": {"fs.azure.account.key.acct.dfs.core.windows.net": "k", "spark.hadoop.fs.s3a.endpoint": "http://x"}},
                "default_lakehouse": LH}
        conf = sparkmod.build_spark_conf(spec)
        assert conf["spark.hadoop.fs.azure.account.key.acct.dfs.core.windows.net"] == "k"
        assert conf["spark.hadoop.fs.s3a.endpoint"] == "http://x"
        assert conf["spark.sql.warehouse.dir"] == LH["tables_uri"]

    @pytest.mark.parametrize("key", ["spark.master", "spark.driver.host", "spark.authenticate", "spark.authenticate.secret",
                                     "spark.ssl.enabled", "spark.jars.packages", "spark.network.crypto.enabled"])
    def test_protected_keys_cannot_be_overridden(self, key):
        with pytest.raises(PermissionError):
            sparkmod.build_spark_conf({}, overrides={key: "x"})

    def test_redacted_conf_masks_secret_keys(self):
        red = sparkmod.redacted_conf({"spark.hadoop.fs.azure.account.key.x": "TOPSECRET", "spark.executor.memory": "1g",
                                      "spark.hadoop.fs.s3a.secret.key": "S3SECRET"})
        assert "TOPSECRET" not in json.dumps(red) and "S3SECRET" not in json.dumps(red)
        assert red["spark.executor.memory"] == "1g"

    def test_db_name_is_sanitised(self):
        assert sparkmod.db_name_for(LH) == "sales_lh"
        assert sparkmod.db_name_for({}) == "lakehouse"

    def test_sql_string_escaping(self):
        assert sparkmod._sql_str("a'b\\c") == "a\\'b\\\\c"


# --------------------------------------------------------------------------------------------- paths
@pytest.fixture
def lakehouse_spec():
    paths.configure({"default_lakehouse": LH, "lakehouses": [OTHER]})
    yield
    paths.configure({})


class TestPaths:
    def test_relative_files_and_tables(self, lakehouse_spec):
        assert paths.resolve("Files/raw/a.csv") == LH["files_uri"] + "/raw/a.csv"
        assert paths.resolve("./Files/raw") == LH["files_uri"] + "/raw"
        assert paths.resolve("Tables/t1") == LH["tables_uri"] + "/t1"
        assert paths.resolve("Files") == LH["files_uri"]

    def test_fabric_mount_spelling(self, lakehouse_spec):
        assert paths.resolve("/lakehouse/default/Files/x.txt") == LH["files_uri"] + "/x.txt"
        assert paths.resolve("/lakehouse/other/Tables/t") == "file:///data/other/Tables/t"
        assert paths.resolve("/lakehouse/other/Files/x", for_="local") == "/data/other/Files/x"

    def test_olcf_scheme(self, lakehouse_spec):
        assert paths.resolve("olcf://ws/other/Files/a/b") == "file:///data/other/Files/a/b"
        with pytest.raises(ValueError):
            paths.resolve("olcf://ws/nope/Files/a")
        with pytest.raises(ValueError):
            paths.resolve("olcf://ws/other/Elsewhere/a")

    def test_absolute_and_uri_paths_unchanged(self, lakehouse_spec):
        for p in ("abfss://c@a.dfs.core.windows.net/x", "s3a://b/k", "/tmp/x", "hdfs://nn/x", "other/relative"):
            assert paths.resolve(p) == p
        assert paths.resolve("file:///tmp/x", for_="local") == "/tmp/x"

    @pytest.mark.parametrize("bad", ["Files/../../etc", "Files/../Tables/../..", "/lakehouse/default/Files/../../x"])
    def test_traversal_rejected(self, lakehouse_spec, bad):
        with pytest.raises(ValueError):
            paths.resolve(bad)

    def test_inner_dotdot_that_stays_inside_is_normalised(self, lakehouse_spec):
        assert paths.resolve("Files/a/../b") == LH["files_uri"] + "/b"

    def test_no_default_lakehouse(self):
        paths.configure({})
        with pytest.raises(ValueError):
            paths.resolve("Files/x")
        assert paths.resolve("/tmp/y") == "/tmp/y"

    def test_unknown_lakehouse_in_mount(self, lakehouse_spec):
        with pytest.raises(ValueError):
            paths.resolve("/lakehouse/ghost/Files/x")

    def test_switch_default(self, lakehouse_spec):
        paths.set_default_lakehouse("other")
        assert paths.resolve("Tables/x") == "file:///data/other/Tables/x"
        with pytest.raises(ValueError):
            paths.set_default_lakehouse("unknown")

    def test_notebookutils_hook_wins_and_may_decline(self, lakehouse_spec, monkeypatch):
        import types

        mod = types.ModuleType("olcf.runtime_lib.notebookutils")
        seen = []

        def resolve_path(path, *, for_, spec):
            seen.append((path, for_, bool(spec)))
            return "hooked://" + path if path.startswith("Files/") else None

        mod.resolve_path = resolve_path
        monkeypatch.setitem(sys.modules, "olcf.runtime_lib.notebookutils", mod)
        assert paths.resolve("Files/a") == "hooked://Files/a"
        assert paths.resolve("Tables/a") == LH["tables_uri"] + "/a"  # hook declined -> built-in rules
        assert seen[0] == ("Files/a", "spark", True)


# --------------------------------------------------------------------------------------------- notebook parsing
IPYNB = {"nbformat": 4, "nbformat_minor": 5, "metadata": {}, "cells": [
    {"cell_type": "markdown", "source": ["# t\n", "x"], "metadata": {}},
    {"cell_type": "code", "source": ["a = 1\n", "print(a)"], "metadata": {"tags": ["parameters"]}},
    {"cell_type": "code", "source": "b = a + 1", "metadata": {}}]}

PERCENT = """\
# %% [markdown]
# # Title
# text
# %% tags=["parameters"]
n = 3
# %%
print(n)
"""

FABRIC = """\
# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {"name": "synapse_pyspark"}
# META }

# MARKDOWN ********************

# # Hello

# PARAMETERS CELL ********************

n = 1

# METADATA ********************

# META {
# META   "language": "python"
# META }

# CELL ********************

print(n)

# METADATA ********************

# META {
# META   "language": "python"
# META }

# CELL ********************

select 1

# METADATA ********************

# META {
# META   "language": "sparksql"
# META }
"""


class TestNotebooks:
    def test_ipynb_normalises_sources(self):
        nb = parse_ipynb(json.dumps(IPYNB))
        assert nb["cells"][1]["source"] == "a = 1\nprint(a)"
        assert all("id" in c for c in nb["cells"])
        assert nb["cells"][1]["outputs"] == [] and nb["cells"][1]["execution_count"] is None
        assert has_parameters_cell(nb)

    def test_ipynb_rejects_garbage(self):
        with pytest.raises(Unsupported):
            parse_ipynb(json.dumps({"foo": 1}))
        with pytest.raises(Unsupported):
            parse_ipynb(json.dumps({"nbformat": 3, "cells": []}))

    def test_py_percent(self):
        nb = parse_py_percent(PERCENT)
        types = [c["cell_type"] for c in nb["cells"]]
        assert types == ["markdown", "code", "code"]
        assert nb["cells"][0]["source"].startswith("# Title")
        assert nb["cells"][1]["metadata"]["tags"] == ["parameters"]
        assert nb["cells"][2]["source"] == "print(n)"

    def test_py_without_markers_is_one_cell(self):
        nb = parse_py_percent("x = 1\nprint(x)\n")
        assert len(nb["cells"]) == 1 and nb["cells"][0]["source"] == "x = 1\nprint(x)"

    def test_fabric_git_source(self):
        nb = parse_fabric_git_source(FABRIC)
        assert nb["metadata"]["kernel_info"] == {"name": "synapse_pyspark"}
        kinds = [(c["cell_type"], c["source"]) for c in nb["cells"]]
        assert kinds[0] == ("markdown", "# Hello")
        assert nb["cells"][1]["metadata"]["tags"] == ["parameters"]
        assert kinds[2] == ("code", "print(n)")
        assert kinds[3][1].startswith("%%sql\nselect 1")  # sparksql cell language -> %%sql magic
        assert has_parameters_cell(nb)

    def test_fabric_scala_cell_is_unsupported(self):
        text = FABRIC.replace('"language": "python"', '"language": "scala"')
        with pytest.raises(Unsupported) as ei:
            parse_fabric_git_source(text)
        assert "scala" in str(ei.value)

    def test_fabric_header_required(self):
        with pytest.raises(Unsupported):
            parse_fabric_git_source("print(1)\n")

    def test_load_notebook_sniffs_format(self, tmp_path):
        (tmp_path / "a.ipynb").write_text(json.dumps(IPYNB))
        (tmp_path / "b.py").write_text(PERCENT)
        (tmp_path / "c.py").write_text(FABRIC)
        (tmp_path / "d.txt").write_text(json.dumps(IPYNB))
        assert len(load_notebook(tmp_path / "a.ipynb")["cells"]) == 3
        assert len(load_notebook(tmp_path / "b.py")["cells"]) == 3
        assert load_notebook(tmp_path / "c.py")["cells"][0]["cell_type"] == "markdown"
        assert len(load_notebook(tmp_path / "d.txt")["cells"]) == 3  # JSON sniffed
        assert len(load_notebook(tmp_path / "b.py", "py")["cells"]) == 3
        with pytest.raises(Unsupported):
            load_notebook(tmp_path / "b.py", "docx")


class TestParameters:
    def test_injection_after_parameters_cell(self):
        nb = parse_ipynb(json.dumps(IPYNB))
        out = inject_parameters(nb, {"a": 5, "s": "x'y", "flag": True, "none": None, "lst": [1, 2]})
        assert [c["metadata"].get("olcf_index") for c in out["cells"]] == [0, 1, None, 2]
        inj = out["cells"][2]
        assert inj["metadata"]["tags"] == ["injected-parameters"]
        ns: dict = {}
        exec(inj["source"], ns)  # noqa: S102 - generated source of a literal assignment list
        assert (ns["a"], ns["s"], ns["flag"], ns["none"], ns["lst"]) == (5, "x'y", True, None, [1, 2])
        assert nb["cells"][1]["source"] == "a = 1\nprint(a)"  # input notebook untouched
        assert len(nb["cells"]) == 3

    def test_injection_at_top_without_parameters_cell(self):
        nb = parse_py_percent("print(1)\n")
        out = inject_parameters(nb, {"x": 1})
        assert out["cells"][0]["metadata"]["tags"] == ["injected-parameters"]
        assert out["cells"][1]["metadata"]["olcf_index"] == 0

    def test_empty_params_adds_nothing(self):
        out = inject_parameters(parse_ipynb(json.dumps(IPYNB)), {})
        assert len(out["cells"]) == 3

    @pytest.mark.parametrize("bad", ["1x", "with space", "class", "a-b", ""])
    def test_invalid_parameter_names(self, bad):
        with pytest.raises(Unsupported):
            parameter_source({bad: 1})

    def test_parameter_values_cannot_inject_code(self):
        src = parameter_source({"p": "'); import os; os.system('x') #"})
        ns: dict = {}
        exec(src, ns)  # noqa: S102
        assert ns["p"] == "'); import os; os.system('x') #"


# --------------------------------------------------------------------------------------------- magics helpers
class TestMagics:
    def test_split_statements(self):
        assert split_statements("select 1; select 2;") == ["select 1", "select 2"]
        assert split_statements("select ';' as a; select \"x;y\"; select `a;b`") == ["select ';' as a", 'select "x;y"', "select `a;b`"]
        assert split_statements("-- c1; c2\nselect 1; /* ; */ select 2") == ["-- c1; c2\nselect 1", "/* ; */ select 2"]
        assert split_statements(" ; ;; -- only comment\n") == []
        assert split_statements("select 'it''s;ok'; select 2") == ["select 'it''s;ok'", "select 2"]
        assert split_statements("select 'a\\';b'; select 2") == ["select 'a\\';b'", "select 2"]

    def test_configure_mapping(self):
        conf, extras = configure_to_conf({"driverMemory": "2g", "executorCores": 2, "numExecutors": 3, "name": "n",
                                          "jars": ["a.jar", "b.jar"], "conf": {"spark.sql.shuffle.partitions": 4, "spark.x": True},
                                          "defaultLakehouse": {"name": "lh"}, "useStarterPool": True})
        assert conf["spark.driver.memory"] == "2g" and conf["spark.executor.cores"] == "2" and conf["spark.executor.instances"] == "3"
        assert conf["spark.jars"] == "a.jar,b.jar" and conf["spark.sql.shuffle.partitions"] == "4" and conf["spark.x"] == "true"
        assert extras == {"name": "n", "defaultLakehouse": {"name": "lh"}}

    def test_configure_rejects_unknown_and_protected(self):
        with pytest.raises(MagicError, match="unsupported key"):
            configure_to_conf({"bogus": 1})
        with pytest.raises(MagicError, match="managed by the platform"):
            configure_to_conf({"conf": {"spark.master": "local"}})
        with pytest.raises(MagicError, match="managed by the platform"):
            configure_to_conf({"conf": {"spark.authenticate.secret": "x"}})

    def test_configure_approved_list(self):
        conf, _ = configure_to_conf({"conf": {"spark.sql.shuffle.partitions": 4}}, approved=["spark.sql.*"])
        assert conf == {"spark.sql.shuffle.partitions": "4"}
        with pytest.raises(MagicError, match="approved"):
            configure_to_conf({"conf": {"spark.executor.extraJavaOptions": "-Dx"}}, approved=["spark.sql.*"])

    def test_pip_blocked_by_default(self):
        with pytest.raises(DependencyBlocked):
            pip_magic("install definitely-not-installed-pkg-xyz", {})
        with pytest.raises(DependencyBlocked):
            pip_magic("uninstall pytest", {})
        for flag in ("-r reqs.txt", "-e .", "--index-url http://x", "--extra-index-url http://x", "-f http://x"):
            with pytest.raises(DependencyBlocked):
                pip_magic(f"install {flag} pytest", {})
        with pytest.raises(DependencyBlocked):
            pip_magic("install --trusted-host x pytest", {})

    def test_pip_already_satisfied_is_a_noop(self):
        out = pip_magic("install pytest", {})
        assert "already satisfied" in out
        assert "already satisfied" in pip_magic("install -q 'pytest>=1'", {})

    def test_pip_version_mismatch_is_missing(self):
        with pytest.raises(DependencyBlocked, match="installed"):
            pip_magic("install pytest==0.0.1", {})

    def test_pip_list_and_show(self):
        assert "pytest==" in pip_magic("list", {})
        assert "Name:" in pip_magic("show pytest", {})
        assert "not found" in pip_magic("show nope-nope-xyz", {})

    def test_pip_empty(self):
        with pytest.raises(MagicError):
            pip_magic("", {})
        with pytest.raises(MagicError):
            pip_magic("install -q", {})

    def test_find_notebook_confinement(self, tmp_path, monkeypatch):
        monkeypatch.setenv("OLCF_JOB_DIR", str(tmp_path))
        (tmp_path / "source").mkdir()
        (tmp_path / "source" / "child.ipynb").write_text("{}")
        (tmp_path / "bundle").mkdir()
        (tmp_path / "bundle" / "other.py").write_text("")
        (tmp_path.parent / "outside.py").write_text("")
        assert find_notebook("child", {}).name == "child.ipynb"
        assert find_notebook("o", {"notebooks": {"o": "bundle/other.py"}}).name == "other.py"
        for bad in ("../outside", "nope", "/etc/passwd"):
            with pytest.raises(MagicError):
                find_notebook(bad, {})
        with pytest.raises(MagicError):
            find_notebook("x", {"notebooks": {"x": "../outside.py"}})


# --------------------------------------------------------------------------------------------- failures
class TestFailures:
    @pytest.mark.parametrize(("ename", "evalue", "cat"), [
        ("KeyboardInterrupt", "", "cancellation"),
        ("ModuleNotFoundError", "No module named 'foo'", "dependency"),
        ("Py4JJavaError", "java.lang.ClassNotFoundException: io.delta.Foo", "dependency"),
        ("Py4JJavaError", "An error occurred ... java.lang.OutOfMemoryError: Java heap space", "resource"),
        ("AnalysisException", "[TABLE_OR_VIEW_NOT_FOUND] The table or view `t` cannot be found", "catalog"),
        ("Py4JJavaError", "AbfsRestOperationException: AuthorizationPermissionMismatch", "authz"),
        ("Exception", "HTTP 401 Unauthorized", "authn"),
        ("Py4JJavaError", "java.io.FileNotFoundException: abfss://x", "storage"),
        ("Py4JJavaError", "org.apache.spark.SparkException: Job aborted", "spark"),
        ("ZeroDivisionError", "division by zero", "user-code"),
        ("TimeoutError", "cell timed out after 5s", "timeout"),
        ("Unsupported", "cell uses scala", "compatibility"),
    ])
    def test_classify(self, ename, evalue, cat):
        got, remediation = failures.classify(ename, evalue)
        assert got == cat
        assert remediation

    def test_classify_strips_ansi(self):
        got, _ = failures.classify("Error", "x", "\x1b[0;31mModuleNotFoundError\x1b[0m: No module named 'q'")
        assert got == "dependency"

    @pytest.mark.parametrize(("tb", "line"), [
        (["\x1b[0;36mCell \x1b[0;32mIn[3], line 7\x1b[0m"], 7),
        (["File \"<ipython-input-4-abcdef>\", line 12"], 12),
        (["  File \"x.py\", line 3, in f", "SyntaxError ... line 9"], 9),
        (["nothing here"], None),
    ])
    def test_extract_line(self, tb, line):
        assert failures.extract_line(tb) == line

    def test_failure_dict_redacts(self):
        d = failures.failure_dict("storage", "failed with password=hunter2 for user", traceback="Bearer abcdefghijklmnopqrstuvwxyz0123456789")
        assert "hunter2" not in d["message"]
        assert "abcdefghijklmnopqrstuvwxyz0123456789" not in (d["traceback"] or "")

    def test_failure_from_exception_honours_forced_category(self):
        try:
            pip_magic("install no-such-package-abc", {})
        except DependencyBlocked as exc:
            d = failures.failure_from_exception(exc, cell=2)
        assert d["category"] == "dependency" and d["cell"] == 2 and d["remediation"]
        try:
            1 / 0  # noqa: B018
        except ZeroDivisionError as exc:
            d = failures.failure_from_exception(exc)
        assert d["category"] == "user-code" and "ZeroDivisionError" in d["traceback"]


# --------------------------------------------------------------------------------------------- logbuf
class TestLogbuf:
    def test_under_limit_untouched(self):
        b = BoundedBuffer(100)
        b.write("hello ")
        b.write("world")
        assert b.getvalue() == "hello world" and not b.truncated

    def test_head_and_tail_kept_with_marker(self):
        b = BoundedBuffer(100)
        for i in range(100):
            b.write(f"line{i:03d}\n")
        out = b.getvalue()
        assert out.startswith("line000") and out.rstrip().endswith("line099")
        assert b.truncated and f"{b.dropped} bytes" in out
        assert len(out.encode()) < 100 + 200  # budget + marker

    def test_multibyte_boundaries_do_not_raise(self):
        b = BoundedBuffer(10)
        b.write("é" * 50)
        b.getvalue().encode("utf-8")

    def test_truncate_text(self):
        t, cut = truncate_text("a" * 1000, 100)
        assert cut and "truncated" in t
        assert truncate_text("abc", 100) == ("abc", False)

    def test_output_budget(self):
        ob = OutputBudget(10)
        assert ob.take(6) == 6 and not ob.truncated
        assert ob.take(6) == 4 and ob.truncated and ob.exhausted
        assert ob.take(1) == 0


# --------------------------------------------------------------------------------------------- CONTRACTS section 12
def test_runtime_lib_imports_only_what_a_runtime_venv_has():
    """Importing every runtime_lib module must pull in nothing but the stdlib and the permitted olcf.core leaf modules (section 12)."""
    import subprocess
    from pathlib import Path as P

    code = (
        "import sys; sys.path.insert(0, sys.argv[1]);"
        "import olcf.runtime_lib.runner, olcf.runtime_lib.executor, olcf.runtime_lib.kernel_host, olcf.runtime_lib.spark, olcf.runtime_lib.magics,"
        "olcf.runtime_lib.bootstrap, olcf.runtime_lib.paths, olcf.runtime_lib.failures, olcf.runtime_lib.logbuf, olcf.runtime_lib.lineage_probe;"
        "std = set(sys.stdlib_module_names);"
        "print(sorted({m.split('.')[0] for m in sys.modules if m.split('.')[0] not in std and not m.startswith('_')} - {'olcf', 'sitecustomize'}));"
        "print(sorted(m for m in sys.modules if m.startswith('olcf.core')))"
    )
    src = str(P(__file__).resolve().parents[2] / "src")
    out = subprocess.run([sys.executable, "-I", "-c", code, src], capture_output=True, text=True, check=True).stdout.splitlines()  # noqa: S603
    assert out[0] == "[]", f"third-party imports at module load: {out[0]}"
    allowed = {"olcf.core", "olcf.core.errors", "olcf.core.hostruntime", "olcf.core.redaction"}  # hostruntime: stdlib-only constants (FIRSTRUN)
    assert set(eval(out[1])) <= allowed, out[1]  # noqa: S307 - our own subprocess output
