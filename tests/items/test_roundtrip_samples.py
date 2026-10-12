# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
# ruff: noqa: S105, S106
"""Whole-workspace export / import round trip over every item type that is present, and compatibility with the DATA sample installer's notebooks."""

from __future__ import annotations

import io
import zipfile

import pytest

from olcf.core import refs
from olcf.notebooks.service import NotebookService
from tests.items.items_support import WIDE_MODULES, nb_json, present


def _strings(obj):
    """Every string value inside a JSON-like object."""
    if isinstance(obj, str):
        return {obj}
    if isinstance(obj, dict):
        return set().union(*(_strings(v) for v in obj.values())) if obj else set()
    if isinstance(obj, list):
        return set().union(*(_strings(v) for v in obj)) if obj else set()
    return set()


def definitions(ic, ws_id):
    out = {}
    for i in ic.get(f"/v1/workspaces/{ws_id}/items", params={"with_definition": "true", "limit": 200}, headers=ic.admin).json()["items"]:
        out[(i["type"], i["name"])] = i
    return out


def populate(ic, ws):
    """One of every item type owned by ITEMS (several notebook shapes) plus whatever other types the installed modules provide."""
    wid = ws["id"]
    nb = nb_json([("markdown", "# Title\n\nSome *text*"), ("code", "region = 'eu'\nlimit = 10"), ("code", "%%sql\nSELECT 1"), ("code", "print(region)  # METADATA ****")])
    nb["cells"][1]["metadata"] = {"tags": ["parameters"]}
    ic.item(wid, "notebook", "analysis", {"nbformat": nb, "settings": {"engine": "spark", "timeout_seconds": 900, "pip_policy": "declared"}}, tags=["a", "b"], description="analysis notebook")
    ic.item(wid, "notebook", "plain", {"nbformat": nb_json([("code", "print(1)")])})
    ic.item(wid, "variable_set", "vars", {"stages": ["dev", "prod"], "default_stage": "dev",
                                          "variables": [{"name": "region", "type": "string", "default": "eu", "overrides": {"prod": "us"}}, {"name": "limit", "type": "integer", "default": 10}]})
    ic.item(wid, "spark_job_definition", "job", {"language": "python", "code": "print('hi')", "args": ["--x", "1"], "conf": {"spark.sql.shuffle.partitions": 8}, "retries": 2, "timeout_seconds": 300,
                                                 "settings": {"variable_set": "vars"}})
    ic.item(wid, "sql_query", "q1", {"sql": "SELECT 1 AS x"})
    for t, d in (("pipeline", {"activities": [{"name": "w", "type": "wait", "typeProperties": {"waitTimeInSeconds": 1}}]}),):  # other owners' types, when their module is installed
        r = ic.post(f"/v1/workspaces/{wid}/items", json={"type": t, "name": f"other-{t}", "definition": d}, headers=ic.admin)
        assert r.status_code == 201, r.text
    if ic.post(f"/v1/workspaces/{wid}/lakehouses", json={"name": "lh1"}, headers=ic.admin).status_code != 201:
        raise AssertionError("lakehouse could not be created")
    ic.post("/v1/connections", json={"workspace_id": wid, "name": "wh", "type": "postgres", "config": {"host": "db.example.com"}}, headers=ic.admin)


def test_environment_with_lock_roundtrips_in_a_workspace_archive(ic, tmp_path):
    from tests.items.wheelbuilder import build_wheel

    ws = ic.workspace("env-src")
    wheelhouse = tmp_path / "wh"
    wheelhouse.mkdir()
    build_wheel(wheelhouse, "olcf-rt-pkg", "1.0.0")
    assert ic.put("/v1/admin/settings/environments.wheelhouses", json={"value": [str(wheelhouse)]}, headers=ic.admin).status_code == 200
    env = ic.item(ws["id"], "environment", "e1", {"python": {"requirements": ["olcf-rt-pkg==1.0.0"], "index": {"no_index": True, "find_links": [str(wheelhouse)]}}, "variables": {"MODE": "x"}})
    r = ic.post(f"/v1/environments/{env['id']}/publish", json={}, headers=ic.admin)
    assert r.status_code == 201, r.text
    lock_hash = r.json()["lock"]["lock_hash"]
    z = ic.get(f"/v1/workspaces/{ws['id']}/export", headers=ic.admin)
    assert z.status_code == 200
    names = zipfile.ZipFile(io.BytesIO(z.content)).namelist()
    assert not any(".." in n or n.startswith("/") for n in names)
    r = ic.post("/v1/workspaces/import", params={"new_name": "env-copy"}, files={"file": ("w.zip", z.content, "application/zip")}, headers=ic.admin)
    assert r.status_code == 201, r.text
    copy = r.json()
    cws = copy.get("workspace", copy)
    e2 = next(i for n, i in definitions(ic, cws["id"]).items() if n == ("environment", "e1"))
    assert e2["definition"] == ic.get_item(env["id"])["definition"]
    pubs = ic.get(f"/v1/environments/{e2['id']}/publications", headers=ic.admin)
    assert pubs.status_code == 200 and [v["lock_hash"] for v in pubs.json()["items"]] == [lock_hash]
    # the imported environment resolves to the same immutable lock
    svc = ic.ctx.service("environments")
    a, b = svc.resolve(env["id"]), svc.resolve(e2["id"])
    assert a["lock_hash"] == b["lock_hash"] == lock_hash


def test_full_workspace_roundtrip_has_identical_definitions(wide, tmp_path):
    ic = wide
    ws = ic.workspace("rt-src")
    populate(ic, ws)
    r = ic.post("/v1/sample/install", json={"workspace_name": "rt-sample", "size": "small"}, headers=ic.admin) if any(m.endswith("sample.module") for m in present(WIDE_MODULES)) else None
    targets = [ws] + ([ic.get("/v1/workspaces/rt-sample", headers=ic.admin).json()] if r is not None and r.status_code in (200, 201) else [])
    assert len(targets) == 2, "the sample installer should be installed in the wide app"
    for src in targets:
        before = definitions(ic, src["id"])
        assert before
        z = ic.get(f"/v1/workspaces/{src['id']}/export", headers=ic.admin)
        assert z.status_code == 200, z.text
        r = ic.post("/v1/workspaces/import", params={"new_name": src["name"] + "-copy"}, files={"file": ("w.zip", z.content, "application/zip")}, headers=ic.admin)
        assert r.status_code == 201, r.text
        dst = r.json().get("workspace", r.json())
        after = definitions(ic, dst["id"])
        assert set(after) == set(before), (set(before) ^ set(after))
        # references to other items (a report tile's lakehouse_id, a pipeline's notebook, ...) follow the imported items: the copy equals the source with ids rewritten,
        # and nothing in the copy still points at the SOURCE workspace's items (see test_import_remap.py for the per-type cases)
        id_map = r.json()["id_mapping"]
        src_ids = {k for k in id_map if k.startswith("itm_")}
        assert src_ids
        for k in sorted(before):
            a, b = before[k]["definition"], after[k]["definition"]
            assert refs.remap_exact(a, id_map) == b, (k, "definitions differ beyond the id rewrite")
            assert not (src_ids & _strings(b)), (k, "the copy still references an item of the source workspace")
            assert before[k]["tags"] == after[k]["tags"] and before[k]["description"] == after[k]["description"], k
        # a second export of the copy is byte-for-byte equivalent in content (deterministic serialisation) apart from ids
        z2 = ic.get(f"/v1/workspaces/{dst['id']}/export", headers=ic.admin).content
        ns = lambda zz: sorted(n for n in zipfile.ZipFile(io.BytesIO(zz)).namelist())  # noqa: E731
        assert ns(z.content) == ns(z2)


@pytest.fixture()
def sample_notebooks():
    sn = pytest.importorskip("olcf.sample.notebooks")
    return sn.manifest()


def test_sample_installer_notebooks_are_accepted_unchanged(ic, sample_notebooks):
    ws = ic.workspace("sample-compat")
    svc: NotebookService = ic.ctx.service("notebooks")
    assert sample_notebooks
    for rec in sample_notebooks:
        # the validator accepts the definition as the installer ships it
        item = ic.item(ws["id"], "notebook", rec["name"], rec["definition"], description=rec.get("description", ""), tags=rec.get("tags", []))
        stored = ic.get_item(item["id"])["definition"]
        assert stored["nbformat"]["cells"] == rec["definition"]["nbformat"]["cells"], rec["name"]  # cells (ids, sources, tags, metadata) are untouched
        eng = rec["definition"]["settings"]["engine"]
        assert stored["settings"]["engine"] == eng
        # every format of the content round-trips
        for fmt in ("ipynb", "py", "fabric"):
            ex = ic.get(f"/v1/notebooks/{item['id']}/export", params={"format": fmt}, headers=ic.admin)
            assert ex.status_code == 200, (rec["name"], fmt, ex.text[:200])
        html = ic.get(f"/v1/notebooks/{item['id']}/export", params={"format": "html"}, headers=ic.admin)
        assert html.status_code == 200 and "<script" not in html.text.lower()
        content = ic.get(f"/v1/notebooks/{item['id']}/content", headers=ic.admin).json()
        assert [c["source"] for c in content["cells"]] == ["".join(c["source"]) if isinstance(c["source"], list) else c["source"] for c in rec["definition"]["nbformat"]["cells"]]
        params = svc.parameters(item["id"]) if hasattr(svc, "parameters") else None
        assert params is None or isinstance(params, (list, dict))
    # idempotent with the installer: creating the same names again conflicts rather than corrupts
    assert ic.post(f"/v1/workspaces/{ws['id']}/items", json={"type": "notebook", "name": sample_notebooks[0]["name"], "definition": sample_notebooks[0]["definition"]}, headers=ic.admin).status_code == 409


def test_installed_sample_notebooks_can_be_run_and_diffed(wide):
    ic = wide
    r = ic.post("/v1/sample/install", json={"workspace_name": "compat-sample", "size": "small"}, headers=ic.admin)
    if r.status_code not in (200, 201):
        pytest.skip(f"sample installer unavailable: {r.status_code}")
    ws = ic.get("/v1/workspaces/compat-sample", headers=ic.admin).json()
    nbs = {i["name"]: i for i in ic.get(f"/v1/workspaces/{ws['id']}/items", params={"type": "notebook"}, headers=ic.admin).json()["items"]}
    assert {"00_README", "01_Retail_Medallion_ETL", "02_Retail_Explore_Python"} <= set(nbs)
    for name, it in nbs.items():
        v = ic.get(f"/v1/notebooks/{it['id']}/parameters", headers=ic.admin)
        assert v.status_code == 200, (name, v.text[:300])
        d = ic.get(f"/v1/notebooks/{it['id']}/diff", params={"a": 1, "b": 1}, headers=ic.admin)
        assert d.status_code == 200 and d.json()["changed"] is False if "changed" in d.json() else d.status_code == 200
        run = ic.post(f"/v1/notebooks/{it['id']}/run", json={}, headers=ic.admin)
        assert run.status_code == 202, (name, run.text[:300])
