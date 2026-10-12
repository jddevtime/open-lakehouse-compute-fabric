# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
# ruff: noqa: S105, S106, S603
"""Sample retail lakehouse: deterministic data, Delta layout, installer idempotency and RBAC, the saved queries, the report and the MCP tools running
end to end through the API, the notebooks (validated, parsed, and the Python one executed), the standalone generator and the committed schema files."""

from __future__ import annotations

import ast
import json
import os
import subprocess
import sys
import types
from pathlib import Path
from typing import Any

import nbformat
import pyarrow as pa
import pyarrow.compute as pc
import pytest
from deltalake import DeltaTable

from olcf.sample import generate, installer, notebooks, reports, spec

from .test_mcp import Mcp

REPO = Path(__file__).resolve().parents[2]
SCHEMAS = REPO / "schemas"


# ----------------------------------------------------------------------------- the generator
def test_generation_is_deterministic_and_pinned():
    a, b = generate.generate("small"), generate.generate("small")
    assert list(a) == spec.table_order() == list(b)
    for name in a:
        assert a[name].equals(b[name]), name
    assert {k: v.num_rows for k, v in a.items()} == {"dim_customer": 300, "dim_product": 60, "dim_store": 8, "dim_date": 1096, "fact_sales": 6000, "fact_web_events": 4000,
                                                      "gold_daily_sales": 1011, "gold_category_summary": 18, "gold_customer_ltv": 292}
    assert pc.sum(a["fact_sales"]["net_amount"]).as_py() == pytest.approx(773378.31, abs=0.05)
    assert pc.sum(a["gold_daily_sales"]["net_revenue"]).as_py() == pytest.approx(773378.31, abs=0.05)       # gold reconciles with the fact
    assert generate.generate("small", seed=7)["fact_sales"].equals(a["fact_sales"]) is False                  # the seed matters


def test_standard_size_has_the_documented_shape():
    t = generate.generate("standard")
    rows = {k: v.num_rows for k, v in t.items()}
    assert rows["dim_customer"] == 2000 and rows["dim_product"] == 200 and rows["dim_store"] == 25 and rows["dim_date"] == 1096
    assert 79_000 <= rows["fact_sales"] <= 81_000 and rows["fact_web_events"] == 50_000
    assert rows["gold_daily_sales"] == 1096 and rows["gold_category_summary"] == 18
    assert pc.sum(t["fact_sales"]["net_amount"]).as_py() == pytest.approx(10297889.63, abs=0.5)
    sales = t["fact_sales"]
    assert sorted(pc.unique(sales["year"]).to_pylist()) == [2023, 2024, 2025] and sorted(pc.unique(sales["month"]).to_pylist()) == list(range(1, 13))
    # referential integrity
    assert set(sales["customer_id"].to_pylist()) <= set(t["dim_customer"]["customer_id"].to_pylist())
    assert set(sales["product_id"].to_pylist()) <= set(t["dim_product"]["product_id"].to_pylist())
    assert set(sales["store_id"].to_pylist()) <= set(t["dim_store"]["store_id"].to_pylist())
    assert set(sales["date_key"].to_pylist()) <= set(t["dim_date"]["date_key"].to_pylist())
    assert len(set(sales["sale_id"].to_pylist())) == rows["fact_sales"]
    # arithmetic of the fact
    gross = pc.multiply(sales["quantity"].cast(pa.float64()), sales["unit_price"])
    assert pc.max(pc.abs(pc.subtract(gross, sales["gross_amount"]))).as_py() < 0.02
    assert pc.max(pc.abs(pc.subtract(pc.subtract(sales["net_amount"], sales["cost_amount"]), sales["profit"]))).as_py() < 0.02


def test_delta_schemas_match_the_spec_exactly():
    for name, table in generate.generate("small").items():
        want = spec.TABLES[name]["columns"]
        assert [f.name for f in table.schema] == [c["name"] for c in want], name
        for f, c in zip(table.schema, want, strict=True):
            want_type = spec.ARROW_TYPES[c["type"]]
            assert f.type == (pa.timestamp("us", tz="UTC") if want_type.startswith("timestamp") else pa.type_for_alias(want_type)), (name, f.name)
            assert f.nullable is c["nullable"], (name, f.name)
            if c["nullable"] is False:
                assert table[f.name].null_count == 0, (name, f.name)
        if (ts := [f for f in table.schema if pa.types.is_timestamp(f.type)]):
            assert all(f.type.tz == "UTC" for f in ts)         # timezone-aware only: keeps the Delta protocol minimal


def test_standalone_cli_writes_readable_delta_tables_and_files(tmp_path):
    out = tmp_path / "retail"
    proc = subprocess.run([sys.executable, "-m", "olcf.sample.generate", "--out", str(out), "--size", "small"], capture_output=True, text=True,
                          env={**os.environ, "PYTHONPATH": str(REPO / "src")}, timeout=240, check=False)
    assert proc.returncode == 0, proc.stderr
    for name in spec.TABLES:
        dt = DeltaTable(str(out / "Tables" / name))
        assert dt.to_pyarrow_table().num_rows > 0
    fact = DeltaTable(str(out / "Tables" / "fact_sales"))
    assert fact.metadata().partition_columns == ["year", "month"]
    proto = fact.protocol()
    assert (proto.min_reader_version, proto.min_writer_version) <= (1, 2) and not proto.reader_features and not proto.writer_features
    assert sorted(p.name for p in (out / "Files" / "raw").iterdir()) == ["customers", "products", "sales", "stores"]
    assert sorted(p.name for p in (out / "Files" / "raw" / "sales").iterdir()) == ["sales_2023.csv", "sales_2024.csv", "sales_2025.csv"]
    assert "retail_lakehouse" in (out / "Files" / "README.md").read_text()
    again = subprocess.run([sys.executable, "-m", "olcf.sample.generate", "--out", str(tmp_path / "bad"), "--size", "huge"], capture_output=True, text=True,
                           env={**os.environ, "PYTHONPATH": str(REPO / "src")}, timeout=60, check=False)
    assert again.returncode != 0


# ----------------------------------------------------------------------------- notebooks
def test_notebooks_are_valid_nbformat_with_the_documented_structure():
    items = {i["name"]: i for i in notebooks.manifest()}
    assert list(items) == ["00_README", "01_Retail_Medallion_ETL", "02_Retail_Explore_Python"]
    for name, item in items.items():
        nb = nbformat.from_dict(item["definition"]["nbformat"])
        nbformat.validate(nb)
        assert nb.nbformat == 4 and item["definition"]["settings"]["default_lakehouse"] == {"name": spec.LAKEHOUSE}
        assert len({c["id"] for c in nb.cells}) == len(nb.cells), name
        assert all(not c.get("outputs") for c in nb.cells if c.cell_type == "code")
        json.dumps(item["definition"])
    etl, explore = items["01_Retail_Medallion_ETL"]["definition"], items["02_Retail_Explore_Python"]["definition"]
    assert etl["settings"]["engine"] == "spark" and explore["settings"]["engine"] == "python"
    # the Python notebook needs deltalake/duckdb/pandas: it asks for the workers' own Python, never for a Spark family whose runtime may lack them
    assert explore["settings"]["runtime"] == {"family": "host-python"} and etl["settings"]["runtime"] == {"family": "fabric-2.0-compatible"}
    params = [c for c in etl["nbformat"]["cells"] if "parameters" in c["metadata"].get("tags", [])]
    assert len(params) == 1 and "run_date" in params[0]["source"] and "min_amount" in params[0]["source"]
    ast.parse(params[0]["source"])
    code = "\n".join(c["source"] for c in etl["nbformat"]["cells"] if c["cell_type"] == "code")
    ast.parse(code)
    for needle in ("saveAsTable", "spark.sql(", "MERGE INTO gold_daily_sales", "DESCRIBE HISTORY", "partitionBy(\"year\", \"month\")", "Files/raw/sales/"):
        assert needle in code, needle
    assert "pyspark" not in "\n".join(c["source"] for c in explore["nbformat"]["cells"]) and "spark" not in "\n".join(c["source"] for c in explore["nbformat"]["cells"] if c["cell_type"] == "code")
    mods = "\n".join(c["source"] for c in explore["nbformat"]["cells"])
    for needle in ("deltalake", "duckdb", "pandas", "matplotlib"):
        assert needle in mods


def test_spark_gold_sql_in_the_etl_notebook_parses_as_spark_sql():
    import sqlglot

    for name in generate.GOLD_SQL:
        q = generate.gold_sql(name, "spark").format(fact="silver_sales_enriched", product="dim_product", as_of="2025-12-31")
        sqlglot.parse_one(q, read="spark")
        assert "datediff('day'" not in q                     # DuckDB-only spelling must not leak into the Spark notebook


def run_notebook(nb: dict[str, Any], env: dict[str, str]) -> dict[str, Any]:
    """Executes the code cells in one namespace (like a kernel would) and also evaluates each cell's trailing expression."""
    ns: dict[str, Any] = {"__name__": "__main__"}
    old = {k: os.environ.get(k) for k in env}
    os.environ.update(env)
    try:
        for cell in nb["cells"]:
            if cell["cell_type"] != "code":
                continue
            tree = ast.parse(cell["source"])
            last = tree.body.pop() if tree.body and isinstance(tree.body[-1], ast.Expr) else None
            exec(compile(tree, f"<cell {cell['id']}>", "exec"), ns)  # noqa: S102
            if last is not None:
                eval(compile(ast.Expression(last.value), f"<cell {cell['id']}>", "eval"), ns)  # noqa: S307
    finally:
        for k, v in old.items():
            if v is None:
                os.environ.pop(k, None)
            else:
                os.environ[k] = v
    return ns


@pytest.fixture()
def maybe_matplotlib(monkeypatch):
    """Real matplotlib when installed, otherwise a recording stub (the notebook logic still runs)."""
    try:
        import matplotlib  # noqa: F401

        return "real"
    except ImportError:
        calls: list[str] = []

        class Anything:
            def __getattr__(self, name):
                return lambda *a, **k: Anything() if name not in ("subplots",) else (Anything(), Anything())

        pyplot = types.ModuleType("matplotlib.pyplot")
        pyplot.__dict__["__getattr__"] = lambda name: (lambda *a, **k: (calls.append(name), (Anything(), Anything()) if name == "subplots" else Anything())[1])
        mpl = types.ModuleType("matplotlib")
        mpl.use = lambda *a, **k: None
        mpl.pyplot = pyplot
        monkeypatch.setitem(sys.modules, "matplotlib", mpl)
        monkeypatch.setitem(sys.modules, "matplotlib.pyplot", pyplot)
        pyplot.calls = calls
        return "stub"


def test_python_notebook_runs_end_to_end_without_spark(tmp_path, maybe_matplotlib):
    pytest.importorskip("pandas")
    generate.write_dataset(tmp_path / "lake", "small")
    nb = {i["name"]: i for i in notebooks.manifest()}["02_Retail_Explore_Python"]["definition"]["nbformat"]
    ns = run_notebook(nb, {"RETAIL_TABLES_ROOT": str(tmp_path / "lake" / "Tables"), "MPLBACKEND": "Agg"})
    assert str(ns["ROOT"]) == str(tmp_path / "lake" / "Tables")
    assert not any(m in sys.modules for m in ("pyspark", "py4j"))
    assert len(ns["monthly"]) == 36 and ns["funnel"]["sessions"].sum() > 0 and set(ns["cat"].columns) == {2023, 2024, 2025}
    assert ns["total_sales"] == pytest.approx(ns["total_gold"], abs=1)


def test_python_notebook_still_runs_and_shows_its_tables_when_matplotlib_is_missing(tmp_path, monkeypatch, capsys):
    """Charts are optional (FIRSTRUN): without matplotlib every cell still runs, the data frames are produced, and the notebook says how to get the charts."""
    pytest.importorskip("pandas")
    generate.write_dataset(tmp_path / "lake", "small")
    monkeypatch.setitem(sys.modules, "matplotlib", None)  # `import matplotlib` raises ImportError
    monkeypatch.setitem(sys.modules, "matplotlib.pyplot", None)
    nb = {i["name"]: i for i in notebooks.manifest()}["02_Retail_Explore_Python"]["definition"]["nbformat"]
    ns = run_notebook(nb, {"RETAIL_TABLES_ROOT": str(tmp_path / "lake" / "Tables")})
    assert ns["plt"] is None
    assert len(ns["monthly"]) == 36 and len(ns["cat"]) > 0 and len(ns["ltv"]) > 0 and ns["total_sales"] == pytest.approx(ns["total_gold"], abs=1)
    out = capsys.readouterr().out
    assert "matplotlib is not installed" in out and "olcf[plots]" in out


def test_python_notebook_resolves_tables_through_the_runtime_path_hook(tmp_path, maybe_matplotlib):
    """Inside an OLCF runtime the default lakehouse comes from the job spec; no environment override is needed."""
    from olcf.runtime_lib import paths

    pytest.importorskip("pandas")
    root = tmp_path / "lh"
    generate.write_dataset(root, "small")
    paths.configure({"default_lakehouse": {"id": "itm_x", "name": spec.LAKEHOUSE, "root_uri": f"file://{root}", "tables_uri": f"file://{root}/Tables", "files_uri": f"file://{root}/Files"}})
    try:
        nb = {i["name"]: i for i in notebooks.manifest()}["02_Retail_Explore_Python"]["definition"]["nbformat"]
        ns = run_notebook(nb, {"MPLBACKEND": "Agg"})
    finally:
        paths.configure({})
    assert str(ns["ROOT"]) == str(root / "Tables") and ns["total_sales"] > 0


def test_python_notebook_falls_back_to_the_lakehouse_mount_when_nothing_is_configured(tmp_path, maybe_matplotlib):
    nb = {i["name"]: i for i in notebooks.manifest()}["02_Retail_Explore_Python"]["definition"]["nbformat"]
    cells = [c for c in nb["cells"] if c["id"] in ("exp-setup", "exp-root")]
    ns = run_notebook({"cells": cells}, {})
    assert str(ns["ROOT"]).endswith("Tables")


def test_committed_schema_files_match_the_generator(tmp_path):
    written = generate.write_schema_files(tmp_path)
    assert sorted(p.name for p in written) == ["sample-retail-notebooks.json", "sample-retail-powerbi-scan.json", "sample-retail-queries.json", "sample-retail-report.json", "sample-retail.schema.json"]
    for p in written:
        committed = SCHEMAS / p.name
        assert committed.exists(), f"{committed} is missing; run `python -m olcf.sample.generate --write-schemas`"
        assert json.loads(committed.read_text()) == json.loads(p.read_text()), f"{committed.name} is out of date; run `python -m olcf.sample.generate --write-schemas`"
    doc = json.loads((SCHEMAS / "sample-retail.schema.json").read_text())
    assert [t["name"] for t in doc["tables"]] == list(spec.TABLES)


# ----------------------------------------------------------------------------- installer + API
@pytest.fixture()
def sample(dc):
    r = dc.post("/v1/sample/install", json={"size": "small"}, headers=dc.admin)
    assert r.status_code == 201, r.text
    body = r.json()
    lh = dc.get(f"/v1/lakehouses/{body['lakehouse_id']}", headers=dc.admin).json()
    return {"summary": body, "ws": body["workspace_id"], "lh": lh, "lh_id": body["lakehouse_id"], "root": Path(lh["root_uri"][len("file://"):])}


def items_by_type(dc, ws: str, type_: str, headers=None) -> dict[str, dict[str, Any]]:
    r = dc.get(f"/v1/workspaces/{ws}/items", params={"type": type_, "limit": 200, "with_definition": True}, headers=headers or dc.admin)
    assert r.status_code == 200, r.text
    return {i["name"]: i for i in r.json()["items"]}


def test_install_creates_everything(dc, sample):
    s = sample["summary"]
    assert s["action"] == "installed" and s["workspace_created"] is True and s["workspace"] == "Sample Retail" and s["lakehouse"] == "retail_lakehouse"
    assert {t["name"]: t["rows"] for t in s["tables"]}["fact_sales"] == 6000
    assert set(items_by_type(dc, sample["ws"], "notebook")) == {"00_README", "01_Retail_Medallion_ETL", "02_Retail_Explore_Python"}
    assert len(items_by_type(dc, sample["ws"], "sql_query")) == len(reports.saved_queries()) >= 8
    assert set(items_by_type(dc, sample["ws"], "report")) == {"Retail Sales Overview"}
    tables = {t["name"]: t for t in dc.get(f"/v1/lakehouses/{sample['lh_id']}/tables", headers=dc.admin).json()["items"]}
    assert set(tables) == set(spec.TABLES) and all(t["format"] == "delta" for t in tables.values())
    # the files the notebooks and the dashboard read
    names = {f["name"] for f in dc.get(f"/v1/lakehouses/{sample['lh_id']}/files", params={"path": "Files/raw/sales"}, headers=dc.admin).json()["items"]}
    assert names == {"sales_2023.csv", "sales_2024.csv", "sales_2025.csv"}
    ws = dc.get(f"/v1/workspaces/{sample['ws']}", headers=dc.admin).json()
    assert ws["settings"]["default_lakehouse"] == sample["lh_id"]


def test_installed_delta_tables_have_the_specified_layout(sample):
    for name, t in spec.TABLES.items():
        dt = DeltaTable(str(sample["root"] / "Tables" / name))
        arrow = dt.schema().to_arrow()
        assert [f.name for f in arrow] == [c["name"] for c in t["columns"]], name
        assert dt.metadata().partition_columns == t["partition_by"], name
        proto = dt.protocol()
        assert not proto.reader_features and not proto.writer_features, (name, proto)
        assert dt.metadata().description == t["description"]
    fact = DeltaTable(str(sample["root"] / "Tables" / "fact_sales"))
    assert fact.to_pyarrow_table().num_rows == 6000 and len(fact.file_uris()) >= 36        # one or more files per year/month


def test_catalog_has_descriptions_tags_and_lineage(dc, sample):
    entries = {e["name"]: e for e in dc.get("/v1/catalog/tables", params={"lakehouse_id": sample["lh_id"], "include": "columns"}, headers=dc.admin).json()["items"]}
    daily = entries["gold_daily_sales"]
    assert daily["description"] == spec.TABLES["gold_daily_sales"]["description"] and "gold" in daily["tags"] and "sample" in daily["tags"]
    assert {c["name"]: c.get("description") for c in daily["columns"]}["net_revenue"] == "Sum of net_amount"
    ds = "olcf://Sample%20Retail/retail_lakehouse/Tables/gold_daily_sales"
    lin = dc.get("/v1/catalog/lineage", params={"dataset": ds, "direction": "up", "depth": 3}, headers=dc.admin)
    assert lin.status_code == 200, lin.text
    text = json.dumps(lin.json())
    assert "fact_sales" in text and "Files/raw/sales" in text and "01_Retail_Medallion_ETL" in text
    col = dc.get("/v1/catalog/lineage", params={"dataset": ds, "direction": "up", "column": "net_revenue"}, headers=dc.admin)
    assert col.status_code == 200 and "net_amount" in json.dumps(col.json())


def test_install_is_idempotent_and_leaves_user_edits_alone(dc, sample):
    nb = items_by_type(dc, sample["ws"], "notebook")["01_Retail_Medallion_ETL"]
    edited = {**nb["definition"], "settings": {**nb["definition"]["settings"], "timeout_seconds": 99}}
    r = dc.put(f"/v1/items/{nb['id']}", json={"definition": edited}, headers=dc.admin)
    assert r.status_code == 200, r.text
    before = dc.get(f"/v1/lakehouses/{sample['lh_id']}/tables/fact_sales/history", headers=dc.admin).json()
    r = dc.post("/v1/sample/install", json={"size": "small"}, headers=dc.admin)
    assert r.status_code == 200 and r.json()["action"] == "unchanged" and not r.json()["created"] and not r.json()["updated"], r.text
    assert dc.get(f"/v1/items/{nb['id']}", headers=dc.admin).json()["definition"]["settings"]["timeout_seconds"] == 99
    assert dc.get(f"/v1/lakehouses/{sample['lh_id']}/tables/fact_sales/history", headers=dc.admin).json() == before
    assert len(dc.get("/v1/workspaces", headers=dc.admin).json()["items"]) == 1
    assert len(items_by_type(dc, sample["ws"], "sql_query")) == len(reports.saved_queries())


def test_install_recreates_deleted_items_but_reinstall_resets_edits(dc, sample):
    queries = items_by_type(dc, sample["ws"], "sql_query")
    assert dc.delete(f"/v1/items/{queries['Revenue by month']['id']}", headers=dc.admin).status_code == 204
    r = dc.post("/v1/sample/install", json={"size": "small"}, headers=dc.admin).json()
    assert r["action"] == "updated" and r["created"] == ["sql_query:Revenue by month"]
    nb = items_by_type(dc, sample["ws"], "notebook")["02_Retail_Explore_Python"]
    dc.put(f"/v1/items/{nb['id']}", json={"description": "mine", "definition": {**nb["definition"], "settings": {"engine": "python"}}}, headers=dc.admin)
    r = dc.post("/v1/sample/install", json={"size": "small", "reinstall": True}, headers=dc.admin)
    assert r.status_code == 201 and r.json()["action"] == "reinstalled", r.text
    after = dc.get(f"/v1/items/{nb['id']}", headers=dc.admin).json()
    assert after["definition"]["settings"]["default_lakehouse"] == {"name": "retail_lakehouse"} and after["description"] != "mine"
    hist = dc.get(f"/v1/lakehouses/{sample['lh_id']}/tables/fact_sales/history", headers=dc.admin).json()
    assert len(hist["items"] if isinstance(hist, dict) else hist) >= 2


def test_install_into_a_foreign_lakehouse_is_refused_unless_reinstall(dc):
    ws = dc.workspace("Sample Retail")
    lh = dc.lakehouse("Sample Retail", "retail_lakehouse")
    dc.write_table(lh["id"], "mine", pa.table({"a": [1]}))
    r = dc.post("/v1/sample/install", json={"size": "small"}, headers=dc.admin)
    assert r.status_code == 409 and "not the sample" in r.text, r.text
    ok = dc.post("/v1/sample/install", json={"size": "small", "reinstall": True}, headers=dc.admin)
    assert ok.status_code == 201 and ok.json()["workspace_id"] == ws["id"] and ok.json()["workspace_created"] is False, ok.text
    names = {t["name"] for t in dc.get(f"/v1/lakehouses/{lh['id']}/tables", headers=dc.admin).json()["items"]}
    assert "mine" in names and "fact_sales" in names                      # other tables are never touched


def test_custom_names_and_validation(dc):
    r = dc.post("/v1/sample/install", json={"size": "small", "workspace_name": "Demo Shop", "lakehouse_name": "shop"}, headers=dc.admin)
    assert r.status_code == 201, r.text
    assert r.json()["workspace"] == "Demo Shop"
    q = items_by_type(dc, r.json()["workspace_id"], "sql_query")["Revenue by month"]
    assert q["definition"]["lakehouse_id"] == r.json()["lakehouse_id"]
    for body in ({"lakehouse_name": "has space"}, {"lakehouse_name": "1abc"}, {"workspace_name": "-bad"}, {"size": "gigantic"}, {"unknown": 1}):
        assert dc.post("/v1/sample/install", json=body, headers=dc.admin).status_code in (400, 422), body


def test_status_before_and_after(dc):
    s = dc.get("/v1/sample/status", headers=dc.admin).json()
    assert s["installed"] is False and s["current_version"] == spec.VERSION and s["running"] is False
    assert dc.post("/v1/sample/install", json={"size": "small"}, headers=dc.admin).status_code == 201
    s = dc.get("/v1/sample/status", headers=dc.admin).json()
    assert s["installed"] is True and s["up_to_date"] is True and s["installed_version"] == spec.VERSION and s["size"] == "small"
    assert {t["name"]: t["rows"] for t in s["tables"]}["dim_store"] == 8 and len(s["items"]["queries"]) >= 8 and s["items"]["reports"] == ["Retail Sales Overview"]


def test_rbac(dc, sample):
    ws = sample["ws"]
    viewer = dc.user("sv", "Viewer", workspace_id=ws)
    assert dc.post("/v1/sample/install", json={"size": "small"}, headers=viewer).status_code == 403
    assert dc.post("/v1/sample/install", json={"size": "small", "reinstall": True}, headers=viewer).status_code == 403
    assert dc.get("/v1/sample/status", headers=viewer).json()["installed"] is True          # viewers may look
    stranger = dc.user("nobody", "Viewer", workspace_id=dc.workspace("other")["id"])
    st = dc.get("/v1/sample/status", headers=stranger).json()
    assert st["installed"] is False and "tables" not in st and "lakehouse_id" not in st    # existence of the workspace is not revealed
    assert dc.post("/v1/sample/install", json={"size": "small"}, headers=stranger).status_code == 403
    assert dc.post("/v1/sample/install", json={"size": "small"}).status_code == 401
    ro = dc.user("ro-token", "Contributor", scopes=["read"])
    assert dc.post("/v1/sample/install", json={"size": "small", "workspace_name": "Another"}, headers=ro).status_code == 403


def test_contributor_creates_a_workspace_and_can_use_it_straight_away(dc):
    contrib = dc.user("maker", "Contributor")
    r = dc.post("/v1/sample/install", json={"size": "small", "workspace_name": "Makers Retail"}, headers=contrib)
    assert r.status_code == 201, r.text
    ws = r.json()["workspace_id"]
    assert dc.get(f"/v1/workspaces/{ws}", headers=contrib).status_code == 200            # now WorkspaceAdmin there
    rep = items_by_type(dc, ws, "report", contrib)["Retail Sales Overview"]
    out = dc.post(f"/v1/reports/{rep['id']}/run", json={}, headers=contrib)
    assert out.status_code == 200 and out.json()["errors"] == 0, out.text


def test_concurrent_installs_do_not_duplicate(dc):
    import threading

    results: list[int] = []

    def go():
        results.append(dc.post("/v1/sample/install", json={"size": "small"}, headers=dc.admin).status_code)

    threads = [threading.Thread(target=go) for _ in range(3)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(120)
    assert sorted(results)[0] in (200, 201) and set(results) <= {200, 201, 409} and 201 in results
    assert len(dc.get("/v1/workspaces", headers=dc.admin).json()["items"]) == 1
    ws = dc.get("/v1/sample/status", headers=dc.admin).json()["workspace_id"]
    assert len(items_by_type(dc, ws, "lakehouse")) == 1 and len(items_by_type(dc, ws, "report")) == 1


# ----------------------------------------------------------------------------- end to end: report, saved queries, MCP
def run_query(dc, qid: str, params: dict[str, Any] | None = None, headers=None):
    return dc.post(f"/v1/sql/queries/{qid}/run", json={"parameters": params or {}}, headers=headers or dc.admin)


def test_report_runs_and_answers_filters(dc, sample):
    rep = items_by_type(dc, sample["ws"], "report")["Retail Sales Overview"]
    out = dc.post(f"/v1/reports/{rep['id']}/run", json={}, headers=dc.admin)
    assert out.status_code == 200, out.text
    out = out.json()
    assert out["errors"] == 0 and out["partial"] is False, [t["error"] for t in out["tiles"] if t["error"]]
    tiles = {t["id"]: t for t in out["tiles"]}
    assert list(tiles) == [t["id"] for t in reports.report_definition()["tiles"]]
    revenue, orders, customers, aov = (tiles[k]["rows"][0][0] for k in ("revenue", "orders", "customers", "aov"))
    assert revenue > 0 and orders > 0 and customers > 0 and aov == pytest.approx(revenue / orders, rel=0.01)
    assert len(tiles["top_products"]["rows"]) == 10 and len(tiles["revenue_by_month"]["rows"]) == 12
    assert {r[0] for r in tiles["channel_mix"]["rows"]} == {"store", "web", "mobile"}
    assert {r[0] for r in tiles["revenue_by_category"]["rows"]} == {"Electronics", "Home & Kitchen", "Apparel", "Sports & Outdoors", "Beauty", "Books & Media"}
    flt = {f["name"]: f for f in out["filters"]}
    assert flt["period"]["value"] == {"from": "2025-01-01", "to": "2025-12-31"} and flt["category"]["value"] is None
    assert set(flt["category"]["options"]) == {"Electronics", "Home & Kitchen", "Apparel", "Sports & Outdoors", "Beauty", "Books & Media"}
    # filters change the numbers
    cat = dc.post(f"/v1/reports/{rep['id']}/run", json={"filters": {"category": "Beauty", "period": {"from": "2024-01-01", "to": "2024-06-30"}}}, headers=dc.admin).json()
    assert cat["errors"] == 0
    beauty = {t["id"]: t for t in cat["tiles"]}["revenue"]["rows"][0][0]
    assert 0 < beauty < revenue
    # the report numbers equal a direct query
    direct = dc.post("/v1/sql/execute", json={"sql": "SELECT round(sum(net_amount), 2) FROM retail_lakehouse.fact_sales WHERE order_date BETWEEN DATE '2025-01-01' AND DATE '2025-12-31'",
                                              "workspace_id": sample["ws"]}, headers=dc.admin).json()
    assert direct["rows"][0][0] == pytest.approx(revenue, abs=0.01)


def test_every_saved_query_runs_with_its_defaults_and_with_parameters(dc, sample):
    queries = items_by_type(dc, sample["ws"], "sql_query")
    assert set(queries) == {q["name"] for q in reports.saved_queries()}
    for name, q in queries.items():
        r = run_query(dc, q["id"])
        assert r.status_code == 200, (name, r.text)
        body = r.json()
        assert body["row_count"] > 0 and body["columns"] and not body["truncated"], name
    by_month = run_query(dc, queries["Revenue by month"]["id"], {"year": 2024}).json()
    assert [r[0] for r in by_month["rows"]] == list(range(1, 13))
    top = run_query(dc, queries["Top products in a category"]["id"], {"category": "Beauty", "top_n": 3}).json()
    assert top["row_count"] == 3 and top["rows"][0][3] >= top["rows"][1][3] >= top["rows"][2][3]
    days = run_query(dc, queries["Daily sales, last N days"]["id"], {"days": 7}).json()
    assert days["row_count"] == 7 and days["rows"][-1][0].startswith("2025-12-31")
    east = run_query(dc, queries["Store performance ranking"]["id"], {"region": "East"}).json()
    assert east["row_count"] > 0 and {r[2] for r in east["rows"]} == {"East"}
    all_regions = run_query(dc, queries["Store performance ranking"]["id"]).json()
    assert {"East", "South", "Online"} == {r[2] for r in all_regions["rows"]}          # the small dataset has 8 stores
    rec = run_query(dc, queries["Raw files reconcile with the table"]["id"]).json()
    csv_lines, table_lines, csv_rev, table_rev = rec["rows"][0]
    assert csv_lines == table_lines == 6000 and csv_rev == pytest.approx(table_rev, abs=0.05)
    bad = run_query(dc, queries["Revenue by month"]["id"], {"year": "twenty"})
    assert bad.status_code == 400


def test_funnel_and_ltv_queries_have_sensible_content(dc, sample):
    queries = items_by_type(dc, sample["ws"], "sql_query")
    funnel = run_query(dc, queries["Web funnel by device"]["id"]).json()
    for _device, sessions, carts, checkouts, purchases, conv in funnel["rows"]:
        assert sessions >= carts >= checkouts >= purchases > 0 and 0 < conv < 100
    ltv = run_query(dc, queries["Customer value segments"]["id"]).json()
    assert {r[0] for r in ltv["rows"]} == {"Top 10%", "Upper 40%", "Lower 50%"}
    top = [r for r in ltv["rows"] if r[0] == "Top 10%"]
    low = [r for r in ltv["rows"] if r[0] == "Lower 50%"]
    assert min(r[3] for r in top) > max(r[3] for r in low)
    discount = run_query(dc, queries["Discount impact on margin"]["id"]).json()
    assert [r[0] for r in discount["rows"]] == sorted(r[0] for r in discount["rows"]) and discount["rows"][0][0] == 0


def test_a_viewer_can_run_the_sample_report_and_queries(dc, sample):
    viewer = dc.user("rv", "Viewer", workspace_id=sample["ws"])
    queries = items_by_type(dc, sample["ws"], "sql_query", viewer)
    assert run_query(dc, queries["Channel mix by quarter"]["id"], headers=viewer).status_code == 200
    rep = items_by_type(dc, sample["ws"], "report", viewer)["Retail Sales Overview"]
    assert dc.post(f"/v1/reports/{rep['id']}/run", json={}, headers=viewer).json()["errors"] == 0


def test_mcp_agent_explores_the_sample(dc, sample):
    agent = dc.user("sample-agent", "Contributor", workspace_id=sample["ws"], scopes=["mcp"])
    m = Mcp(dc, agent)
    m.init()
    lakes = m.call("list_lakehouses", workspace="Sample Retail")
    assert [x["name"] for x in lakes["lakehouses"]] == ["retail_lakehouse"]
    tables = m.call("list_tables", workspace="Sample Retail", lakehouse="retail_lakehouse")
    assert {t["name"] for t in tables["tables"]} == set(spec.TABLES)
    d = m.call("describe_table", workspace="Sample Retail", lakehouse="retail_lakehouse", table="fact_sales")
    assert [c["name"] for c in d["columns"]][:3] == ["sale_id", "order_id", "order_date"] and d["partition_columns"] == ["year", "month"]
    assert d["description"] == spec.TABLES["fact_sales"]["description"]
    assert next(c for c in d["columns"] if c["name"] == "net_amount")["description"] == "Revenue: gross_amount - discount_amount"
    q = m.call("run_sql", workspace="Sample Retail", lakehouse="retail_lakehouse",
               sql="SELECT p.category, round(sum(f.net_amount), 2) AS revenue FROM retail_lakehouse.fact_sales f JOIN retail_lakehouse.dim_product p USING (product_id) "
                   "WHERE f.year = :y GROUP BY 1 ORDER BY 2 DESC", parameters={"y": 2025})
    assert q["row_count"] == 6 and q["rows"][0][1] >= q["rows"][-1][1]
    w = m.call("run_sql", expect_error=True, workspace="Sample Retail", lakehouse="retail_lakehouse", sql="DELETE FROM retail_lakehouse.fact_sales")
    assert "read-only" in json.dumps(w).lower()
    rep = m.call("run_report", workspace="Sample Retail", report="Retail Sales Overview")
    assert rep["errors"] == 0
    s = m.call("search_catalog", query="customer lifetime value")
    assert any(r.get("name") == "gold_customer_ltv" for r in s["results"]), s
    lin = m.call("get_lineage", dataset="retail_lakehouse.gold_category_summary", workspace="Sample Retail", direction="up")
    assert "fact_sales" in json.dumps(lin)
    res = m.result("resources/list")
    assert any(r["uri"].endswith("/Tables/gold_daily_sales") for r in res["resources"])


def test_sample_runs_on_an_installed_default_lakehouse_without_qualification(dc, sample):
    r = dc.post("/v1/sql/execute", json={"sql": "SELECT count(*) AS n FROM fact_sales", "workspace_id": sample["ws"]}, headers=dc.admin)
    assert r.status_code == 200 and r.json()["rows"] == [[6000]], r.text


def test_installer_status_and_progress_helpers(dc):
    assert installer.status(dc.ctx, types.SimpleNamespace(can=lambda *a, **k: False))["installed"] is False
    with pytest.raises(Exception, match="size"):
        installer.install_sample(dc.ctx, None, size="nope")
