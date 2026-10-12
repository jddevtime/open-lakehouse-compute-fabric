# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Workspace export -> import re-points every cross-item id reference (report tiles, saved queries, notebook settings, pipelines, schedules) at the
imported items - the imported report must read the *imported* lakehouse, not the source workspace's."""

from __future__ import annotations

import io
import zipfile
from typing import Any

import pytest

from olcf.core import refs
from tests.items.items_support import nb_json


def items_by_type(wide, ws: str, type_: str) -> dict[str, dict[str, Any]]:
    r = wide.get(f"/v1/workspaces/{ws}/items", params={"type": type_, "limit": 200, "with_definition": True}, headers=wide.admin)
    assert r.status_code == 200, r.text
    return {i["name"]: i for i in r.json()["items"]}


def _walk(obj: Any):
    if isinstance(obj, dict):
        for v in obj.values():
            yield from _walk(v)
    elif isinstance(obj, list):
        for v in obj:
            yield from _walk(v)
    else:
        yield obj


def _export_import(wide, ws: str, **params: Any) -> dict[str, Any]:
    z = wide.get(f"/v1/workspaces/{ws}/export", headers=wide.admin)
    assert z.status_code == 200, z.text
    r = wide.post("/v1/workspaces/import", files={"file": ("w.zip", z.content, "application/zip")}, params=params, headers=wide.admin)
    assert r.status_code == 201, r.text
    return r.json()


def test_sample_workspace_round_trip_runs_the_imported_report(wide):
    src = wide.post("/v1/sample/install", json={"size": "small"}, headers=wide.admin).json()
    out = _export_import(wide, src["workspace_id"], new_name="Sample Copy")
    new_ws = out["workspace"]["id"]
    assert new_ws != src["workspace_id"]
    new_lh = items_by_type(wide, new_ws, "lakehouse")["retail_lakehouse"]
    assert new_lh["id"] != src["lakehouse_id"]
    # nothing in the imported workspace still points at the source lakehouse
    for typ in ("report", "sql_query", "notebook"):
        for it in items_by_type(wide, new_ws, typ).values():
            assert src["lakehouse_id"] not in set(_walk(it["definition"])), (typ, it["name"])
    rep = items_by_type(wide, new_ws, "report")["Retail Sales Overview"]
    assert any(t.get("lakehouse_id") == new_lh["id"] for t in rep["definition"]["tiles"])
    run = wide.post(f"/v1/reports/{rep['id']}/run", json={}, headers=wide.admin)
    assert run.status_code == 200 and run.json()["errors"] == 0, run.text
    # the id mapping reports every item, the source lakehouse included
    assert out["id_mapping"][src["lakehouse_id"]] == new_lh["id"]


def test_hooks_remap_only_reference_fields(wide):
    ids = {"nb_old": "nb_new", "lh_old": "lh_new"}
    hooks = wide.ctx.item_hooks
    rep = {"tiles": [{"title": "lh_old", "lakehouse_id": "lh_old", "sql": "select 'lh_old'"}], "filters": [{"options_lakehouse_id": "lh_old"}]}
    out = refs.remap_definition(hooks, "report", rep, ids)
    assert out["tiles"][0] == {"title": "lh_old", "lakehouse_id": "lh_new", "sql": "select 'lh_old'"} and out["filters"][0]["options_lakehouse_id"] == "lh_new"
    assert rep["tiles"][0]["lakehouse_id"] == "lh_old"                       # input untouched
    sch = refs.remap_definition(hooks, "schedule", {"cron": "0 * * * *", "target": {"type": "notebook", "ref": "nb_old", "parameters": {"x": "nb_old"}}}, ids)
    assert sch["target"]["ref"] == "nb_new" and sch["target"]["parameters"] == {"x": "nb_old"}
    pipe = {"activities": [{"name": "a", "type": "notebook", "typeProperties": {"notebook": "nb_old", "lakehouse": "lh_old"}},
                           {"name": "b", "type": "if", "typeProperties": {"expression": "@true", "ifTrueActivities": [
                               {"name": "c", "type": "notebook", "typeProperties": {"notebook": "nb_old"}}], "ifFalseActivities": []}}]}
    out = refs.remap_definition(hooks, "pipeline", pipe, ids)
    assert out["activities"][0]["typeProperties"] == {"notebook": "nb_new", "lakehouse": "lh_new"}
    assert out["activities"][1]["typeProperties"]["ifTrueActivities"][0]["typeProperties"]["notebook"] == "nb_new"
    nb = {"cells": [{"source": "nb_old"}], "settings": {"default_lakehouse": {"lakehouse_id": "lh_old", "workspace_id": "ws_src"}}}
    out = refs.remap_definition(hooks, "notebook", nb, ids)
    assert out["settings"]["default_lakehouse"] == {"lakehouse_id": "lh_new"} and out["cells"] == nb["cells"]
    env = {"python": {"requirements": ["lh_old"]}}
    assert refs.remap_definition(hooks, "environment", env, ids) == env


def test_pipeline_and_schedule_follow_the_imported_notebook(wide):
    ws = wide.workspace("src-ws")["id"]
    nbid = wide.item(ws, "notebook", "n1", {"nbformat": nb_json([("code", "print(1)")])})["id"]
    pl = wide.post(f"/v1/workspaces/{ws}/items", json={"type": "pipeline", "name": "p1", "definition": {"activities": [
        {"name": "Run", "type": "notebook", "typeProperties": {"notebook": nbid}}]}}, headers=wide.admin)
    assert pl.status_code == 201, pl.text
    plid = pl.json()["id"]
    sc = wide.post(f"/v1/workspaces/{ws}/items", json={"type": "schedule", "name": "s1", "definition": {"cron": "0 3 * * *", "enabled": False, "target": {"type": "pipeline", "ref": plid}}}, headers=wide.admin)
    assert sc.status_code == 201, sc.text
    out = _export_import(wide, ws, new_name="dst-ws")
    new_ws = out["workspace"]["id"]
    new_nb = items_by_type(wide, new_ws, "notebook")["n1"]["id"]
    new_pl = items_by_type(wide, new_ws, "pipeline")["p1"]
    assert new_nb != nbid and new_pl["definition"]["activities"][0]["typeProperties"]["notebook"] == new_nb
    assert items_by_type(wide, new_ws, "schedule")["s1"]["definition"]["target"]["ref"] == new_pl["id"]


def _minio(wide, ws: str, name: str) -> dict[str, Any]:
    r = wide.post("/v1/connections", json={"name": name, "type": "minio", "workspace_id": ws, "config": {"bucket": "lake", "prefix": name, "endpoint_url": "http://127.0.0.1:9", "path_style": True}},
                  headers=wide.admin)
    assert r.status_code == 201, r.text
    return r.json()


def test_connections_are_created_first_and_land_in_the_id_mapping(wide):
    ws = wide.workspace("with-conn")["id"]
    con = _minio(wide, ws, "store")
    z = wide.get(f"/v1/workspaces/{ws}/export", headers=wide.admin).content
    assert any(n.startswith("connections/") for n in zipfile.ZipFile(io.BytesIO(z)).namelist())
    out = _export_import(wide, ws, new_name="with-conn-2")
    new_con = out["id_mapping"][con["id"]]
    assert new_con.startswith("con_") and new_con != con["id"]
    assert [c["name"] for c in out["connections_created"]] == ["store"]
    # the lakehouse hook rewrites storage.connection_id with that mapping (no storage is touched here)
    hook = wide.ctx.item_hooks["lakehouse"]["remap_ids"]
    d = {"storage": {"connection_id": con["id"], "root_uri": "s3://lake/store"}}
    assert hook(d, out["id_mapping"])["storage"] == {"connection_id": new_con, "root_uri": "s3://lake/store"} and d["storage"]["connection_id"] == con["id"]


def test_lakehouse_on_update_hook_requires_connection_use_to_repoint(wide, monkeypatch):
    from olcf.core.errors import Forbidden
    from olcf.security import rbac

    ws = wide.workspace("upd")["id"]
    con = _minio(wide, ws, "other")
    monkeypatch.setitem(rbac.ROLE_PERMISSIONS, "Editor", {"item.update"})            # may edit items but not use connections
    editor = rbac.Principal(id="usr_e", name="e", bindings=[rbac.Binding("Editor", "workspace", ws)])
    hook = wide.ctx.item_hooks["lakehouse"]["on_update"]
    old = {"workspace_id": ws, "definition": {"storage": {"connection_id": None, "root_uri": "file:///x"}}}
    new = {"workspace_id": ws, "definition": {"storage": {"connection_id": con["id"], "root_uri": ""}}}
    with pytest.raises(Forbidden):
        hook(editor, old, new)
    hook(editor, old, old)                                                              # unchanged connection: nothing to check
    monkeypatch.setitem(rbac.ROLE_PERMISSIONS, "Editor", {"item.update", "connection.use"})
    hook(editor, old, new)
    hook(rbac.SYSTEM_PRINCIPAL, old, new)
