# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""FIX J: ``GET /v1/workspaces/{ws}/pipelines`` (last run + trigger summary per pipeline, optional size-capped definitions) and ``?include=definition`` on the items list."""

from __future__ import annotations

import json

from tests.orch.orch_support import OrchStack
from tests.orch.test_api_e2e import A


def _list(stack: OrchStack, ws: str, **params):
    r = stack.http.get(f"/v1/workspaces/{ws}/pipelines", headers=stack.admin, params=params)
    assert r.status_code == 200, r.text
    return r.json()


def test_workspace_pipelines_summarise_last_run_and_triggers(stack: OrchStack, ws: str) -> None:
    ok = stack.pipeline(ws, "p-ok", [A("W", "wait", waitTimeInSeconds=0)])
    stack.pipeline(ws, "p-never-run", [A("W", "wait", waitTimeInSeconds=0)])
    run1 = stack.start_run(ok["id"])
    stack.wait_run(run1["id"])
    run2 = stack.start_run(ok["id"])
    done = stack.wait_run(run2["id"])
    assert stack.http.post("/v1/triggers", headers=stack.admin, json={"workspace_id": ws, "name": "hook", "type": "webhook", "target": {"type": "pipeline", "ref": "p-ok"}}).status_code == 201
    sch = stack.item(ws, "schedule", "nightly", {"cron": "0 3 * * *", "timezone": "UTC", "target": {"type": "pipeline", "ref": "p-ok"}})
    assert sch["id"]

    page = _list(stack, ws)
    by_name = {i["name"]: i for i in page["items"]}
    assert set(by_name) == {"p-ok", "p-never-run"} and page["next_cursor"] is None
    a, b = by_name["p-ok"], by_name["p-never-run"]
    assert a["last_run"]["id"] == run2["id"] and a["last_run"]["state"] == done["state"] == "succeeded" and a["last_run"]["finished_at"]
    assert a["triggers"]["total"] == 2 and a["triggers"]["enabled"] == 2 and a["triggers"]["by_type"] == {"webhook": 1, "schedule": 1} and a["triggers"]["next_fire"]
    assert b["last_run"] is None and b["triggers"] == {"total": 0, "enabled": 0, "by_type": {}, "next_fire": None}
    assert "definition" not in a and {"id", "workspace_id", "name", "version", "etag", "updated_at"} <= set(a)
    # filter, pagination
    assert [i["name"] for i in _list(stack, ws, q="never")["items"]] == ["p-never-run"]
    first = _list(stack, ws, limit=1)
    assert len(first["items"]) == 1 and first["next_cursor"]
    second = _list(stack, ws, limit=1, cursor=first["next_cursor"])
    assert {first["items"][0]["name"], second["items"][0]["name"]} == {"p-ok", "p-never-run"}
    # names resolve like everywhere else
    name = stack.http.get(f"/v1/workspaces/{ws}", headers=stack.admin).json()["name"]
    assert len(_list(stack, name)["items"]) == 2


def test_definitions_are_opt_in_and_size_capped(stack: OrchStack, ws: str, monkeypatch) -> None:
    from olcf.pipelines import service as svc

    stack.pipeline(ws, "p-a", [A("W", "wait", waitTimeInSeconds=0)])
    stack.pipeline(ws, "p-b", [A("W", "wait", waitTimeInSeconds=0)])
    full = _list(stack, ws, include="definition")["items"]
    assert all(i["definition"]["activities"][0]["name"] == "W" and "definition_omitted" not in i for i in full)
    size = len(json.dumps(full[0]["definition"], default=str))
    monkeypatch.setattr(svc, "LIST_DEFINITION_CAP_BYTES", size * 3 // 2)  # room for one definition only
    capped = _list(stack, ws, include="definition")["items"]
    assert [bool(i["definition"]) for i in capped].count(True) == 1
    assert [i["definition_omitted"] for i in capped if i["definition"] is None] == ["size"]
    r = stack.http.get(f"/v1/workspaces/{ws}/pipelines", headers=stack.admin, params={"include": "secrets"})
    assert r.status_code == 400 and "include" in r.json()["error"]["message"]


def test_workspace_pipelines_need_read_access_to_that_workspace(stack: OrchStack, ws: str) -> None:
    other = stack.workspace()
    stack.pipeline(ws, "hidden", [A("W", "wait", waitTimeInSeconds=0)])
    outsider = stack.user("Viewer", other)
    assert stack.http.get(f"/v1/workspaces/{ws}/pipelines", headers=outsider).status_code == 403
    assert stack.http.get(f"/v1/workspaces/{ws}/pipelines", headers=outsider, params={"include": "definition"}).status_code == 403
    insider = stack.user("Viewer", ws)
    assert [i["name"] for i in stack.http.get(f"/v1/workspaces/{ws}/pipelines", headers=insider, params={"include": "definition"}).json()["items"]] == ["hidden"]
    assert stack.http.get("/v1/workspaces/" + ws + "/pipelines").status_code == 401


def test_items_list_include_definition_is_the_same_as_with_definition_and_capped(stack: OrchStack, ws: str, monkeypatch) -> None:
    from olcf.api.core import workspaces as wsmod

    stack.pipeline(ws, "i-a", [A("W", "wait", waitTimeInSeconds=0)])
    stack.pipeline(ws, "i-b", [A("W", "wait", waitTimeInSeconds=0)])
    h = stack.admin
    plain = stack.http.get(f"/v1/workspaces/{ws}/items", headers=h, params={"type": "pipeline"}).json()["items"]
    assert all("definition" not in i for i in plain)
    inc = stack.http.get(f"/v1/workspaces/{ws}/items", headers=h, params={"type": "pipeline", "include": "definition"}).json()["items"]
    legacy = stack.http.get(f"/v1/workspaces/{ws}/items", headers=h, params={"type": "pipeline", "with_definition": "true"}).json()["items"]
    assert inc == legacy and all(i["definition"]["activities"] for i in inc)
    monkeypatch.setattr(wsmod, "LIST_DEFINITION_CAP_BYTES", len(json.dumps(inc[0]["definition"], default=str)) * 3 // 2)
    capped = stack.http.get(f"/v1/workspaces/{ws}/items", headers=h, params={"type": "pipeline", "include": "definition"}).json()["items"]
    assert sorted((i["definition"] is None, i.get("definition_omitted")) for i in capped) == [(False, None), (True, "size")]
    bad = stack.http.get(f"/v1/workspaces/{ws}/items", headers=h, params={"include": "everything"})
    assert bad.status_code == 400
    viewer_other = stack.user("Viewer", stack.workspace())
    assert stack.http.get(f"/v1/workspaces/{ws}/items", headers=viewer_other, params={"include": "definition"}).status_code == 403


def test_sdk_methods_for_the_new_endpoints(stack: OrchStack, ws: str) -> None:
    from olcf.sdk import Client

    p = stack.pipeline(ws, "sdk-p", [A("W", "wait", waitTimeInSeconds=0)])
    stack.wait_run(stack.start_run(p["id"])["id"])
    token = stack.admin["Authorization"].split(" ", 1)[1]
    with Client(stack.url, token=token, max_retries=0) as c:
        (row,) = list(c.pipelines.list_pipelines(ws))
        assert row["name"] == "sdk-p" and row["last_run"]["state"] == "succeeded" and "definition" not in row
        assert c.pipelines.list_pipelines(ws, include_definition=True)[0]["definition"]["activities"]
        assert list(c.pipelines.list_pipelines(ws, q="nomatch")) == []
        assert c.items.list(ws, type="pipeline", include="definition")[0]["definition"]["activities"]
        assert "definition" not in c.items.list(ws, type="pipeline")[0]
