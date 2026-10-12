# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Runtime lineage probe (``olcf.runtime_lib.lineage_probe``): resolution, de-duplication, caps, PySpark hooks (fake classes), real deltalake / duckdb in a child process."""

from __future__ import annotations

import json
import os
import subprocess
import sys
import textwrap
import threading
import types
from pathlib import Path
from typing import Any

import pytest

from olcf.runtime_lib import lineage_probe as lp

SRC = str(Path(lp.__file__).resolve().parents[2])
SPEC = {
    "default_lakehouse": {"id": "itm_silver", "name": "Silver", "root_uri": "abfss://ws@onelake.example/Silver.Lakehouse", "schema_enabled": False},
    "lakehouses": [{"id": "itm_gold", "name": "gold-lh", "root_uri": "file:///data/gold", "schema_enabled": True}],
    "lakehouse_ids": ["itm_silver", "itm_gold"],
}


class Sink:
    """Collects ``emit(event, **data)`` calls."""

    def __init__(self) -> None:
        self.events: list[dict[str, Any]] = []
        self._lock = threading.Lock()

    def __call__(self, event: str, **data: Any) -> None:
        with self._lock:
            self.events.append({"event": event, **data})

    def of(self, name: str) -> list[dict[str, Any]]:
        return [e for e in self.events if e["event"] == name]


@pytest.fixture()
def probe():
    sink = Sink()
    p = lp.DatasetProbe(json.loads(json.dumps(SPEC)), emit=sink)
    p.sink = sink  # type: ignore[attr-defined]
    return p


# --------------------------------------------------------------------------------------------- resolution
def test_table_names_resolve_against_the_attached_lakehouses(probe):
    assert probe.table("sales") == {"kind": "table", "lakehouse_id": "itm_silver", "name": "sales"}
    assert probe.table("`Silver`.`sales`", "delta") == {"kind": "table", "lakehouse_id": "itm_silver", "name": "sales", "format": "delta"}
    assert probe.table("gold_lh.dim_customer") == {"kind": "table", "lakehouse_id": "itm_gold", "name": "dim_customer"}      # db name of 'gold-lh'
    assert probe.table("spark_catalog.Silver.sales") == {"kind": "table", "lakehouse_id": "itm_silver", "name": "sales"}
    assert probe.table("gold-lh.sch.t") == {"kind": "table", "lakehouse_id": "itm_gold", "name": "sch.t"}
    assert probe.table("other.t") == {"kind": "table", "lakehouse_id": "itm_silver", "name": "other.t"}                       # unknown qualifier: kept as written
    assert probe.table("") is None and probe.table(None) is None and probe.table(5) is None and probe.table("  ") is None


def test_paths_resolve_to_files_tables_and_plain_locations(probe):
    assert probe.path("Files/raw/a.csv", "csv") == {"kind": "file", "lakehouse_id": "itm_silver", "path": "Files/raw/a.csv", "format": "csv"}
    assert probe.path("Tables/sales") == {"kind": "table", "lakehouse_id": "itm_silver", "name": "sales"}
    assert probe.path("abfss://ws@onelake.example/Silver.Lakehouse/Tables/sales/_delta_log?sig=SECRET", "delta") == \
        {"kind": "table", "lakehouse_id": "itm_silver", "name": "sales", "uri": "abfss://ws@onelake.example/Silver.Lakehouse/Tables/sales/_delta_log", "format": "delta"}
    assert probe.path("/data/gold/Tables/dbo/orders") == {"kind": "table", "lakehouse_id": "itm_gold", "name": "dbo.orders", "uri": "/data/gold/Tables/dbo/orders"}   # schema-enabled
    assert probe.path("file:///data/gold/Files/x.json") == {"kind": "file", "lakehouse_id": "itm_gold", "path": "Files/x.json", "uri": "file:///data/gold/Files/x.json"}
    assert probe.path("s3://bucket/key.parquet") == {"kind": "file", "uri": "s3://bucket/key.parquet"}
    assert probe.path("relative/thing.csv") == {"kind": "file", "path": "relative/thing.csv"}
    assert probe.path("") is None and probe.path(None) is None and probe.path(5) is None
    assert probe.path(Path("Files/p.csv"))["path"] == "Files/p.csv"


def test_a_job_without_lakehouses_still_reports_tables_and_files():
    p = lp.DatasetProbe({}, emit=Sink())
    assert p.table("db.t") == {"kind": "table", "name": "db.t"}
    assert p.path("Files/x") == {"kind": "file", "path": "Files/x"}
    assert p.default_lakehouse_id() is None


# --------------------------------------------------------------------------------------------- emission rules
def test_events_have_timestamps_modes_and_clean_datasets(probe):
    probe.read([probe.table("sales"), None])
    probe.written([probe.table("gold_lh.out")], "overwrite")
    probe.written([probe.table("gold_lh.out2")], "errorifexists")
    probe.written([probe.table("gold_lh.out3")])
    s = probe.sink
    assert [e["event"] for e in s.events] == ["dataset_read", "dataset_written", "dataset_written", "dataset_written"]
    assert s.events[0]["datasets"] == [{"kind": "table", "lakehouse_id": "itm_silver", "name": "sales"}] and "mode" not in s.events[0]
    assert [e["mode"] for e in s.events[1:]] == ["overwrite", "create", "create"]
    assert all(e["at"].endswith("Z") and "T" in e["at"] for e in s.events)


def test_duplicates_are_reported_once_but_different_modes_are_not_duplicates(probe):
    for _ in range(5):
        probe.read([probe.table("sales")])
    probe.written([probe.table("t")], "append")
    probe.written([probe.table("t")], "append")
    probe.written([probe.table("t")], "overwrite")
    assert [e["event"] for e in probe.sink.events] == ["dataset_read", "dataset_written", "dataset_written"]
    assert probe.stats() == {"emitted": 3, "duplicates": 5, "dropped": 0, "errors": 0}


def test_empty_dataset_lists_report_nothing(probe):
    probe.read([])
    probe.read([None, None])
    probe.written([None])
    assert probe.sink.events == []


def test_cap_emits_one_truncated_marker_then_drops(probe):
    small = lp.DatasetProbe({}, emit=probe.sink, limit=5)
    for i in range(30):
        small.read([small.table(f"t{i}")])
    assert len(probe.sink.of("dataset_read")) == 5
    assert probe.sink.of("dataset_events_truncated") == [{"event": "dataset_events_truncated", "limit": 5}]
    assert small.stats()["dropped"] == 25
    small.read([small.table("t0")])          # still a duplicate of an already reported one
    assert small.stats()["duplicates"] == 1


def test_default_cap_is_200():
    assert lp.MAX_EVENTS == 200
    sink = Sink()
    p = lp.DatasetProbe({}, emit=sink)
    for i in range(260):
        p.read([p.table(f"t{i}")])
    assert len(sink.of("dataset_read")) == 200 and len(sink.of("dataset_events_truncated")) == 1


def test_thread_safe_dedupe_and_cap():
    sink = Sink()
    p = lp.DatasetProbe({}, emit=sink, limit=50)
    barrier = threading.Barrier(8)

    def work(n: int) -> None:
        barrier.wait()
        for i in range(100):
            p.read([p.table(f"shared{i % 20}")])      # the same 20 tables from every thread
            p.read([p.table(f"own{n}_{i}")])           # plus unique ones that overflow the cap

    threads = [threading.Thread(target=work, args=(n,)) for n in range(8)]
    [t.start() for t in threads]
    [t.join() for t in threads]
    names = [e["datasets"][0]["name"] for e in sink.of("dataset_read")]
    assert len(names) == 50 and len(set(names)) == 50
    assert len(sink.of("dataset_events_truncated")) == 1
    assert p.stats()["errors"] == 0


def test_statements_are_filtered_redacted_clipped_and_deduplicated(probe):
    probe.statement("SELECT 1")
    probe.statement("SET spark.sql.shuffle.partitions=4")
    probe.statement("USE Silver")
    probe.statement("-- hello\nSHOW TABLES FROM Silver")
    probe.statement("   ")
    probe.statement(None)
    probe.statement(42)
    assert probe.sink.events == []
    probe.statement("SELECT * FROM sales WHERE token = 'password=hunter2hunter2'")
    probe.statement("select  *  from sales where token = 'password=hunter2hunter2'   ")        # whitespace-only difference: same statement, but case differs -> kept apart
    probe.statement("SELECT * FROM t WHERE x = '" + "y" * 30000 + "'", "duckdb")
    sql_events = probe.sink.of("dataset_statement")
    assert len(sql_events) == 3
    assert "hunter2hunter2" not in sql_events[0]["sql"] and sql_events[0]["dialect"] == "spark" and sql_events[0]["default_lakehouse_id"] == "itm_silver"
    assert len(sql_events[2]["sql"]) == lp.MAX_SQL_CHARS and sql_events[2]["dialect"] == "duckdb"
    probe.statement("SELECT * FROM sales WHERE token = 'password=hunter2hunter2'")
    assert len(probe.sink.of("dataset_statement")) == 3


@pytest.mark.parametrize(("sql", "word"), [("  /* c */ INSERT INTO t VALUES (1)", "insert"), ("(SELECT 1) UNION (SELECT 2)", "select"), ("-- x\n-- y\nWITH a AS (SELECT 1) SELECT * FROM a", "with"),
                                           ("", None), ("123", None)])
def test_leading_keyword(sql, word):
    assert lp.leading_keyword(sql) == word


def test_an_emit_that_raises_never_reaches_the_caller():
    def broken(event: str, **data: Any) -> None:
        raise RuntimeError("stdout closed")

    p = lp.DatasetProbe({}, emit=broken)
    p.read([p.table("t")])
    p.statement("SELECT * FROM t")
    p.written([p.table("t")], "append")
    assert p.stats()["errors"] >= 1


def test_enabled_switch(monkeypatch):
    monkeypatch.setenv("OLCF_DATASET_EVENTS", "0")
    assert not lp.enabled() and lp.install({}) is None
    monkeypatch.setenv("OLCF_DATASET_EVENTS", "off")
    assert not lp.enabled()
    monkeypatch.delenv("OLCF_DATASET_EVENTS")
    assert lp.enabled()


# --------------------------------------------------------------------------------------------- PySpark hooks on fake classes
@pytest.fixture()
def fake_pyspark(monkeypatch):
    """Minimal stand-ins for ``pyspark.sql.session`` and ``pyspark.sql.readwriter`` with the real class and method names."""
    calls: list[tuple] = []

    class DataFrameReader:
        def __init__(self, spark=None):
            self.spark = spark

        def format(self, source):
            return self

        def table(self, tableName):
            calls.append(("reader.table", tableName))
            return "df"

        def load(self, path=None, format=None, schema=None, **options):
            return "df"

        def parquet(self, *paths, **options):
            return "df"

        def orc(self, *paths, **options):
            return "df"

        def csv(self, path, schema=None, **options):
            return "df"

        def json(self, path, schema=None, **options):
            return "df"

        def text(self, paths, wholetext=False, **options):
            return "df"

    class DataFrameWriter:
        fail = False

        def mode(self, saveMode):
            return self

        def format(self, source):
            return self

        def _go(self):
            if DataFrameWriter.fail:
                raise RuntimeError("write failed")

        def saveAsTable(self, name, format=None, mode=None, partitionBy=None, **options):
            self._go()

        def insertInto(self, tableName, overwrite=None):
            self._go()

        def save(self, path=None, format=None, mode=None, partitionBy=None, **options):
            self._go()

        def parquet(self, path, mode=None, partitionBy=None, compression=None):
            self._go()

        def csv(self, path, mode=None, **options):
            self._go()

        def json(self, path, mode=None, **options):
            self._go()

        def orc(self, path, mode=None, partitionBy=None, compression=None):
            self._go()

        def text(self, path, compression=None, lineSep=None):
            self._go()

    class DataFrameWriterV2:
        fail = False

        def __init__(self, df, table):
            self.df, self.table = df, table

        def _go(self, *a):
            if DataFrameWriterV2.fail:
                raise RuntimeError("write failed")

        create = createOrReplace = replace = append = overwrite = overwritePartitions = _go

    class SparkSession:
        def __init__(self):
            self.statements: list[str] = []

        @property
        def read(self):
            return DataFrameReader(self)

        def table(self, tableName):
            return "df"

        def sql(self, sqlQuery, args=None, **kwargs):
            self.statements.append(sqlQuery)
            return "df"

    pkg = types.ModuleType("pyspark")
    sql = types.ModuleType("pyspark.sql")
    session = types.ModuleType("pyspark.sql.session")
    rw = types.ModuleType("pyspark.sql.readwriter")
    session.SparkSession = SparkSession
    rw.DataFrameReader, rw.DataFrameWriter, rw.DataFrameWriterV2 = DataFrameReader, DataFrameWriter, DataFrameWriterV2
    for name, mod in (("pyspark", pkg), ("pyspark.sql", sql), ("pyspark.sql.session", session), ("pyspark.sql.readwriter", rw)):
        monkeypatch.setitem(sys.modules, name, mod)
    sink = Sink()
    old = lp.set_probe(lp.DatasetProbe(json.loads(json.dumps(SPEC)), emit=sink))
    n = lp.install_spark()
    ns = types.SimpleNamespace(spark=SparkSession(), Reader=DataFrameReader, Writer=DataFrameWriter, V2=DataFrameWriterV2, sink=sink, calls=calls, n=n, SparkSession=SparkSession)
    yield ns
    lp.set_probe(old)


def test_spark_reads_are_reported_when_the_plan_is_created(fake_pyspark):
    s, sink = fake_pyspark.spark, fake_pyspark.sink
    assert fake_pyspark.n >= 25
    assert s.table("sales") == "df" and s.read.table("gold_lh.dim") == "df"
    s.read.format("delta").load("Tables/events")
    s.read.parquet("Files/a.parquet", "Files/b.parquet")
    s.read.csv("Files/raw/c.csv", header=True)
    s.read.json(path="Files/raw/j.json")
    s.read.text("Files/t.txt")
    s.read.orc("/data/gold/Files/o.orc")
    s.read.format("csv").load(path=["Files/m1.csv", "Files/m2.csv"])
    names = [(d.get("name") or d.get("path"), d.get("format")) for e in sink.of("dataset_read") for d in e["datasets"]]
    assert names == [("sales", None), ("dim", None), ("events", "delta"), ("Files/a.parquet", "parquet"), ("Files/b.parquet", "parquet"), ("Files/raw/c.csv", "csv"),
                     ("Files/raw/j.json", "json"), ("Files/t.txt", "text"), ("Files/o.orc", "orc"), ("Files/m1.csv", "csv"), ("Files/m2.csv", "csv")]
    assert fake_pyspark.calls == [("reader.table", "gold_lh.dim")], "the wrapped methods still run and return what they always returned"


def test_spark_sql_reports_the_statement_only(fake_pyspark):
    s = fake_pyspark.spark
    assert s.sql("INSERT INTO gold_lh.t SELECT * FROM sales") == "df"
    assert s.sql("USE Silver") == "df"
    assert s.sql(sqlQuery="SELECT * FROM sales") == "df"
    assert s.statements == ["INSERT INTO gold_lh.t SELECT * FROM sales", "USE Silver", "SELECT * FROM sales"]
    stmts = fake_pyspark.sink.of("dataset_statement")
    assert [e["sql"] for e in stmts] == ["INSERT INTO gold_lh.t SELECT * FROM sales", "SELECT * FROM sales"]
    assert fake_pyspark.sink.of("dataset_read") == [] and fake_pyspark.sink.of("dataset_written") == []


def test_spark_writes_are_reported_after_success_with_their_mode(fake_pyspark):
    W, sink = fake_pyspark.Writer, fake_pyspark.sink
    W().mode("overwrite").saveAsTable("gold_lh.out")
    W().format("delta").mode("append").saveAsTable("Silver.fact")
    W().saveAsTable("plain")
    W().insertInto("gold_lh.ins")
    W().mode("overwrite").insertInto("gold_lh.ins2")
    W().insertInto("gold_lh.ins3", overwrite=True)
    W().format("delta").mode("append").save("Tables/delta_out")
    W().parquet("Files/out/p", mode="overwrite")
    W().mode("append").csv("Files/out/c")
    W().json("Files/out/j")
    W().orc("Files/out/o", "append")
    W().text("Files/out/t")
    got = [(d.get("name") or d.get("path"), e["mode"], d.get("format")) for e in sink.of("dataset_written") for d in e["datasets"]]
    assert got == [("out", "overwrite", None), ("fact", "append", "delta"), ("plain", "create", None), ("ins", "append", None), ("ins2", "overwrite", None), ("ins3", "overwrite", None),
                   ("delta_out", "append", "delta"), ("Files/out/p", "overwrite", "parquet"), ("Files/out/c", "append", "csv"), ("Files/out/j", "create", "json"),
                   ("Files/out/o", "append", "orc"), ("Files/out/t", "create", "text")]


def test_failed_spark_writes_report_nothing_and_the_exception_is_untouched(fake_pyspark):
    W, V2 = fake_pyspark.Writer, fake_pyspark.V2
    W.fail = V2.fail = True
    with pytest.raises(RuntimeError, match="write failed"):
        W().mode("overwrite").saveAsTable("t")
    with pytest.raises(RuntimeError, match="write failed"):
        V2(None, "gold_lh.t").append()
    assert fake_pyspark.sink.of("dataset_written") == []


def test_spark_dataframe_writer_v2_modes(fake_pyspark):
    V2 = fake_pyspark.V2
    for method in ("create", "createOrReplace", "replace", "append", "overwrite", "overwritePartitions"):
        getattr(V2(None, f"gold_lh.t_{method}"), method)()
    got = [(e["datasets"][0]["name"], e["mode"]) for e in fake_pyspark.sink.of("dataset_written")]
    assert got == [("t_create", "create"), ("t_createOrReplace", "overwrite"), ("t_replace", "overwrite"), ("t_append", "append"), ("t_overwrite", "overwrite"),
                   ("t_overwritePartitions", "overwrite")]


def test_install_spark_is_idempotent_and_types_are_preserved(fake_pyspark):
    assert lp.install_spark() == 0
    assert type(fake_pyspark.spark) is fake_pyspark.SparkSession
    assert isinstance(fake_pyspark.spark.read, fake_pyspark.Reader)


def test_hook_errors_never_reach_user_code(fake_pyspark):
    class Weird:
        def __fspath__(self):
            raise OSError("boom")

    fake_pyspark.spark.read.csv(Weird())          # the wrapper swallows the failure of its own hook
    fake_pyspark.spark.table(object())            # unusable argument types are ignored
    probe = lp.current()
    assert probe is not None and probe.stats()["errors"] >= 1


# --------------------------------------------------------------------------------------------- a real Python process with real deltalake and duckdb
def child(script: str, spec: dict[str, Any] | None = None, env: dict[str, str] | None = None) -> list[dict[str, Any]]:
    """Run ``script`` in a fresh interpreter with the probe installed first; return the ``@@OLCF@@`` events it printed (stdout of the child)."""
    code = textwrap.dedent("""
        import json, sys
        from olcf.runtime_lib import lineage_probe
        spec = json.loads(sys.argv[1])
        lineage_probe.install(spec)
    """) + textwrap.dedent(script) + textwrap.dedent("""
        # deltalake's native runtime can hang while the interpreter shuts down (also without the probe): leave the child directly
        import os
        sys.stdout.flush()
        sys.stderr.flush()
        os._exit(0)
    """)
    proc = subprocess.run([sys.executable, "-I", "-c", code, json.dumps(spec if spec is not None else {})], capture_output=True, text=True, timeout=120,   # noqa: S603
                          env={"PYTHONPATH": SRC, "PATH": os.environ.get("PATH", ""), **(env or {})}, check=False)
    assert proc.returncode == 0, proc.stderr[-3000:]
    out = []
    for line in proc.stdout.splitlines():
        if line.startswith("@@OLCF@@ "):
            out.append(json.loads(line[len("@@OLCF@@ "):]))
    return out


def test_deltalake_reads_and_writes_in_a_real_process(tmp_path):
    root = tmp_path / "lh"
    (root / "Tables").mkdir(parents=True)
    spec = {"default_lakehouse": {"id": "itm_x", "name": "x", "root_uri": str(root)}}
    events = child(f"""
        import pyarrow as pa
        import deltalake
        from deltalake import DeltaTable, write_deltalake
        t = pa.table({{"a": [1, 2]}})
        write_deltalake({str(root / 'Tables' / 'sales')!r}, t)
        write_deltalake({str(root / 'Tables' / 'sales')!r}, t, mode="append")
        try:
            write_deltalake({str(root / 'Tables' / 'sales')!r}, t, mode="error")
        except Exception as exc:
            print("expected failure:", type(exc).__name__)
        dt = DeltaTable({str(root / 'Tables' / 'sales')!r})
        assert dt.to_pyarrow_table().num_rows == 4
        assert deltalake.DeltaTable is DeltaTable
    """, spec)
    kinds = [(e["event"], e["datasets"][0].get("name"), e.get("mode")) for e in events if e["event"] != "dataset_statement"]
    assert kinds == [("dataset_written", "sales", "create"), ("dataset_written", "sales", "append"), ("dataset_read", "sales", None)], kinds
    assert all(e["datasets"][0]["lakehouse_id"] == "itm_x" and e["datasets"][0]["format"] == "delta" for e in events)


def test_duckdb_statements_and_proxy_behaviour_in_a_real_process():
    events = child("""
        import duckdb
        con = duckdb.connect()
        assert isinstance(con, duckdb.DuckDBPyConnection)
        con.execute("CREATE TABLE t AS SELECT 1 AS a")
        assert con.execute("SELECT * FROM t").fetchall() == [(1,)]
        assert con.sql("SELECT a FROM t").fetchall() == [(1,)]
        assert con.query("SELECT a FROM t WHERE a = ?", params=[1]).fetchall() == [(1,)]
        with duckdb.connect() as c2:
            c2.execute("SELECT 42 AS answer")        # no table reference: not reported
        assert duckdb.sql("SELECT 5 AS five").fetchall() == [(5,)]
        cur = con.cursor()
        assert cur.execute("SELECT a FROM t").fetchone() == (1,)
        try:
            con.execute("SELECT * FROM missing_table")
        except duckdb.Error:
            print("expected duckdb error")
    """)
    stmts = [(e["sql"], e["dialect"]) for e in events if e["event"] == "dataset_statement"]
    assert ("SELECT * FROM t", "duckdb") in stmts and ("SELECT a FROM t", "duckdb") in stmts
    assert ("CREATE TABLE t AS SELECT 1 AS a", "duckdb") in stmts
    assert not any("answer" in s or "five" in s for s, _ in stmts), "statements without a table reference are not reported"
    assert not any("missing_table" in s for s, _ in stmts), "a statement that failed is not reported"
    assert len([1 for s, _ in stmts if s == "SELECT a FROM t"]) == 1, "the same statement is reported once"


def test_no_events_when_disabled_and_no_cost_for_jobs_that_never_import_the_libraries():
    off = child("import duckdb\nduckdb.connect().execute('SELECT * FROM (SELECT 1) x')", env={"OLCF_DATASET_EVENTS": "0"})
    assert off == []
    lazy = child("""
        import sys
        assert 'duckdb' not in sys.modules and 'deltalake' not in sys.modules and 'pyspark' not in sys.modules
        print("lazy ok")
    """)
    assert lazy == []


def test_runner_environment_installs_the_probe_via_build_namespace(tmp_path):
    events = child("""
        from olcf.runtime_lib import bootstrap
        bootstrap.build_namespace(spec, spark=None)
        import duckdb
        duckdb.connect().execute("SELECT * FROM (SELECT 1 AS a) q")
    """, {"default_lakehouse": None})
    assert [e["event"] for e in events] == ["dataset_statement"]
