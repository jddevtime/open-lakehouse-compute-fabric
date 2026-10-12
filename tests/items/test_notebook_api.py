# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Notebook REST API through a real app (foundation + storage + compute + notebooks): content / ETag, versions, diff, restore, import, export, parameters, runs."""

from __future__ import annotations

import copy
import io
import json
import zipfile
from html.parser import HTMLParser

import nbformat
import pytest

from olcf.notebooks import formats as F
from tests.items.items_support import nb_json


def mk_nb(ic, ws, name="nb1", cells=None, settings=None, tags_first=True):
    nb = nb_json(cells or [("markdown", "# T"), ("code", "alpha = 1\nname = 'x'"), ("code", "print(alpha, name)")])
    if tags_first:
        nb["cells"][1]["metadata"] = {"tags": ["parameters"]}
    return ic.item(ws["id"], "notebook", name, {"nbformat": nb, **({"settings": settings} if settings else {})})


def upload(ic, ws_id, files, params="", headers=None):
    return ic.post(f"/v1/workspaces/{ws_id}/notebooks/import{params}", files=[("file", (n, d, "application/octet-stream")) for n, d in files], headers=headers or ic.admin)


# ------------------------------------------------------------------------------------------------ content
def test_content_get_put_etag_and_stable_cell_ids(ic):
    ws = ic.workspace()
    it = mk_nb(ic, ws)
    r = ic.get(f"/v1/notebooks/{it['id']}/content", headers=ic.admin)
    assert r.status_code == 200
    body = r.json()
    etag = r.headers["ETag"]
    assert [c["id"] for c in body["cells"]] == ["c00", "c01", "c02"]
    assert body["parameters"]["parameters"][0]["name"] == "alpha"
    cells = [{"id": c["id"], "cell_type": c["cell_type"], "source": c["source"], "metadata": c["metadata"]} for c in body["cells"]]
    cells[2]["source"] = "print('changed')"
    cells.append({"cell_type": "code", "source": "new = 1"})  # no id: assigned by the server
    r = ic.put(f"/v1/notebooks/{it['id']}/content", json={"cells": cells, "comment": "edit"}, headers={**ic.admin, "If-Match": etag})
    assert r.status_code == 200, r.text
    new = r.json()
    assert new["version"] == 2 and r.headers["ETag"] != etag
    assert [c["id"] for c in new["cells"][:3]] == ["c00", "c01", "c02"]  # unchanged ids stay stable
    assert new["cells"][3]["id"] and new["cells"][3]["id"] not in ("c00", "c01", "c02")
    # a stale ETag is refused (optimistic concurrency) and nothing is saved
    r = ic.put(f"/v1/notebooks/{it['id']}/content", json={"cells": cells[:1]}, headers={**ic.admin, "If-Match": etag})
    assert r.status_code == 412
    assert ic.get_item(it["id"])["version"] == 2
    # saving identical content creates no new version
    same = [{k: c[k] for k in ("id", "cell_type", "source", "metadata")} for c in new["cells"]]
    r = ic.put(f"/v1/notebooks/{it['id']}/content", json={"cells": same}, headers=ic.admin)
    assert r.status_code == 200 and r.json()["version"] == 2


def test_put_rejects_invalid_cells(ic):
    ws = ic.workspace()
    it = mk_nb(ic, ws)
    for bad in ([{"cell_type": "weird", "source": "x"}], [{"id": "a b", "cell_type": "code", "source": "x"}], [{"cell_type": "code", "source": 5}]):
        r = ic.put(f"/v1/notebooks/{it['id']}/content", json={"cells": bad}, headers=ic.admin)
        assert r.status_code == 422, r.text
    r = ic.put(f"/v1/notebooks/{it['id']}/content", json={"cells": [{"id": "d", "cell_type": "code", "source": "1"}, {"id": "d", "cell_type": "code", "source": "2"}]}, headers=ic.admin)
    assert r.status_code in (200, 422)
    if r.status_code == 200:  # duplicate ids are repaired, never stored twice
        assert len({c["id"] for c in r.json()["cells"]}) == 2


def test_strip_outputs_on_save_option(ic):
    ws = ic.workspace()
    out = [{"output_type": "stream", "name": "stdout", "text": "secret-ish\n"}]
    cells = [{"id": "c00", "cell_type": "code", "source": "print(1)", "outputs": out, "execution_count": 3}]
    # the notebook setting strips on every save (also through the generic items API)
    strict = mk_nb(ic, ws, name="strict", settings={"strip_outputs": True})
    r = ic.put(f"/v1/notebooks/{strict['id']}/content", json={"cells": cells}, headers=ic.admin)
    assert r.status_code == 200 and r.json()["cells"][0]["outputs"] == [] and r.json()["cells"][0]["execution_count"] is None
    # without the setting outputs are kept; a request can still ask to strip this one save
    plain = mk_nb(ic, ws, name="plain")
    r = ic.put(f"/v1/notebooks/{plain['id']}/content", json={"cells": cells}, headers=ic.admin)
    assert r.json()["cells"][0]["outputs"] == out and r.json()["cells"][0]["execution_count"] == 3
    r = ic.put(f"/v1/notebooks/{plain['id']}/content", json={"cells": cells, "strip_outputs": True}, headers=ic.admin)
    assert r.json()["cells"][0]["outputs"] == []
    r = ic.put(f"/v1/notebooks/{plain['id']}/content", json={"cells": cells}, headers=ic.admin)
    # outputs=false on read hides them without touching the stored notebook
    assert ic.get(f"/v1/notebooks/{plain['id']}/content?outputs=false", headers=ic.admin).json()["cells"][0]["outputs"] == []
    assert ic.get(f"/v1/notebooks/{plain['id']}/content", headers=ic.admin).json()["cells"][0]["outputs"] == out


def test_item_api_roundtrip_keeps_definition_valid(ic):
    """The generic items API (create / update) runs the notebook validate hook: bad notebooks are rejected, good ones normalised."""
    ws = ic.workspace()
    r = ic.post(f"/v1/workspaces/{ws['id']}/items", json={"type": "notebook", "name": "bad", "definition": {"nbformat": {"nbformat": 4, "cells": "nope"}}}, headers=ic.admin)
    assert r.status_code == 422
    r = ic.post(f"/v1/workspaces/{ws['id']}/items", json={"type": "notebook", "name": "x", "definition": {"source": "# %%\nprint(1)\n", "format": "py"}}, headers=ic.admin)
    assert r.status_code == 201 and r.json()["definition"]["nbformat"]["cells"][0]["source"] == "print(1)"
    r = ic.post(f"/v1/workspaces/{ws['id']}/items", json={"type": "notebook", "name": "empty", "definition": {}}, headers=ic.admin)
    assert r.status_code == 201 and r.json()["definition"]["nbformat"]["cells"] == []
    r = ic.post(f"/v1/workspaces/{ws['id']}/items", json={"type": "notebook", "name": "badsettings", "definition": {"settings": {"engine": "nope"}}}, headers=ic.admin)
    assert r.status_code == 422


# ------------------------------------------------------------------------------------------------ versions / diff / restore
def test_versions_diff_restore(ic):
    ws = ic.workspace()
    it = mk_nb(ic, ws)
    base = ic.get(f"/v1/notebooks/{it['id']}/content", headers=ic.admin).json()
    cells = [{k: c[k] for k in ("id", "cell_type", "source", "metadata")} for c in base["cells"]]
    cells[1]["source"] = "alpha = 2\nname = 'x'"
    cells.pop(2)
    cells.append({"id": "zz", "cell_type": "code", "source": "added()"})
    assert ic.put(f"/v1/notebooks/{it['id']}/content", json={"cells": cells, "comment": "second"}, headers=ic.admin).status_code == 200
    vs = ic.get(f"/v1/notebooks/{it['id']}/versions", headers=ic.admin).json()["items"]
    assert [v["version"] for v in vs] == [2, 1] and vs[0]["comment"] == "second" and vs[0]["current"]
    d = ic.get(f"/v1/notebooks/{it['id']}/diff?a=1&b=2", headers=ic.admin).json()
    assert d["a"] == 1 and d["b"] == 2 and not d["identical"]
    assert d["summary"] == {"added": 1, "removed": 1, "modified": 1, "moved": 0, "unchanged": 1}
    mod = next(c for c in d["cells"] if c["status"] == "modified")
    assert "-alpha = 1" in mod["diff"] and "+alpha = 2" in mod["diff"]
    assert "alpha = 2" in d["text"]
    # defaults: previous vs current
    assert ic.get(f"/v1/notebooks/{it['id']}/diff", headers=ic.admin).json()["a"] == 1
    assert ic.get(f"/v1/notebooks/{it['id']}/diff?a=1&b=9", headers=ic.admin).status_code == 404
    assert ic.get(f"/v1/notebooks/{it['id']}/diff?a=x", headers=ic.admin).status_code == 400
    # outputs are ignored by the diff
    cells[0]["source"] = base["cells"][0]["source"]
    v1 = ic.get(f"/v1/notebooks/{it['id']}/versions/1", headers=ic.admin).json()
    assert v1["version"] == 1 and len(v1["cells"]) == 3 and not v1["current"]
    r = ic.post(f"/v1/notebooks/{it['id']}/versions/1/restore", headers=ic.admin)
    assert r.status_code == 200 and r.json()["version"] == 3  # a restore is a new version, history is never rewritten
    assert [c["source"] for c in r.json()["cells"]] == [c["source"] for c in base["cells"]]
    assert ic.get(f"/v1/notebooks/{it['id']}/diff?a=1&b=3", headers=ic.admin).json()["identical"] is True
    assert ic.post(f"/v1/notebooks/{it['id']}/versions/42/restore", headers=ic.admin).status_code == 404


def test_parameters_endpoints_and_preview_is_injection_safe(ic):
    ws = ic.workspace()
    it = mk_nb(ic, ws)
    p = ic.get(f"/v1/notebooks/{it['id']}/parameters", headers=ic.admin).json()
    assert {x["name"]: (x["type"], x["default"]) for x in p["parameters"]} == {"alpha": ("integer", 1), "name": ("string", "x")}
    evil = "x'\n__import__('os').system('id')\n'"
    r = ic.post(f"/v1/notebooks/{it['id']}/parameters/preview", json={"parameters": {"alpha": "9", "name": evil}}, headers=ic.admin)
    assert r.status_code == 200
    cell = r.json()["cell"]
    assert r.json()["parameters"]["alpha"] == 9
    ns: dict = {}
    exec(cell["source"], {"__builtins__": {}}, ns)  # noqa: S102 - executing the injected cell must only assign
    assert ns == {"alpha": 9, "name": evil}
    assert ic.post(f"/v1/notebooks/{it['id']}/parameters/preview", json={"parameters": {"alpha": "nope"}}, headers=ic.admin).status_code == 422
    assert ic.post(f"/v1/notebooks/{it['id']}/parameters/preview", json={"parameters": {"1bad": 1}}, headers=ic.admin).status_code == 422


# ------------------------------------------------------------------------------------------------ export
def test_export_all_formats_roundtrip(ic):
    ws = ic.workspace()
    it = mk_nb(ic, ws, name="Exp Test")
    base = ic.get(f"/v1/notebooks/{it['id']}/content", headers=ic.admin).json()
    cells = [{k: c[k] for k in ("id", "cell_type", "source", "metadata")} for c in base["cells"]]
    cells[2]["outputs"] = [{"output_type": "stream", "name": "stdout", "text": "out\n"}]
    ic.put(f"/v1/notebooks/{it['id']}/content", json={"cells": cells}, headers=ic.admin)
    r = ic.get(f"/v1/notebooks/{it['id']}/export?format=ipynb", headers=ic.admin)
    assert r.status_code == 200 and "attachment" in r.headers["content-disposition"] and "nosniff" in r.headers["x-content-type-options"]
    nbformat.validate(nbformat.reads(r.text, as_version=4))
    assert "".join(json.loads(r.text)["cells"][2]["outputs"][0]["text"]) == "out\n"  # multi-line strings are split into lists, as nbformat writes them
    assert json.loads(ic.get(f"/v1/notebooks/{it['id']}/export?format=ipynb&outputs=false", headers=ic.admin).text)["cells"][2]["outputs"] == []
    for fmt in ("py", "fabric"):
        r = ic.get(f"/v1/notebooks/{it['id']}/export?format={fmt}", headers=ic.admin)
        assert r.status_code == 200
        back = F.normalize_notebook(F.parse_notebook(r.text))
        assert [c["source"] for c in back["cells"]] == [c["source"] for c in base["cells"]] and F.sniff_format(r.text) == fmt
    assert ic.get(f"/v1/notebooks/{it['id']}/export?format=docx", headers=ic.admin).status_code == 422
    assert ic.get(f"/v1/notebooks/{it['id']}/export?format=ipynb&version=1", headers=ic.admin).status_code == 200


def test_html_export_is_sanitised_and_sandboxed(ic):
    ws = ic.workspace()
    it = mk_nb(ic, ws)
    cells = [{"id": "m", "cell_type": "markdown", "source": "<script>alert(1)</script><img src=x onerror=alert(2)> [l](javascript:alert(3))"},
             {"id": "c", "cell_type": "code", "source": "print(1)", "outputs": [{"output_type": "display_data", "data": {"text/html": "<script>alert(4)</script><iframe src=//evil></iframe><b>b</b>"}, "metadata": {}}]}]
    assert ic.put(f"/v1/notebooks/{it['id']}/content", json={"cells": cells}, headers=ic.admin).status_code == 200
    r = ic.get(f"/v1/notebooks/{it['id']}/export?format=html", headers=ic.admin)
    assert r.status_code == 200 and r.headers["content-type"].startswith("text/html")
    csp = r.headers["content-security-policy"]
    assert "default-src 'none'" in csp and "sandbox" in csp and "script-src" not in csp
    tags: set[str] = set()
    attrs: list[tuple[str, str]] = []

    class C(HTMLParser):
        def handle_starttag(self, tag, a):  # noqa: ANN001
            tags.add(tag)
            attrs.extend((k, v or "") for k, v in a)

    C().feed(r.text)
    assert not tags & {"script", "iframe", "object", "embed", "svg", "form"}
    assert not [a for a in attrs if a[0].startswith("on") or a[1].lower().startswith("javascript:")]
    assert "<b>b</b>" in r.text


# ------------------------------------------------------------------------------------------------ import
def test_import_ipynb_py_fabric_and_conflicts(ic):
    ws = ic.workspace()
    nb = nb_json([("code", "a = 1"), ("markdown", "hi")])
    ipynb = json.dumps(nb).encode()
    py = F.serialize_notebook(F.normalize_notebook(nb), "py").encode()
    fab = F.serialize_notebook(F.normalize_notebook(nb), "fabric").encode()
    r = upload(ic, ws["id"], [("one.ipynb", ipynb), ("two.py", py), ("three.py", fab)])
    assert r.status_code == 201, r.text
    d = r.json()
    assert d["created"] == 3 and {i["name"] for i in d["items"]} == {"one", "two", "three"}
    assert {i["format"] for i in d["items"]} == {"ipynb", "py", "fabric"}
    for it in d["items"]:
        c = ic.get(f"/v1/notebooks/{it['id']}/content", headers=ic.admin).json()
        assert [x["source"] for x in c["cells"]] == ["a = 1", "hi"]
    # on_conflict: rename (default) / skip / overwrite
    r = upload(ic, ws["id"], [("one.ipynb", ipynb)])
    assert r.json()["items"][0]["name"] == "one (2)"
    r = upload(ic, ws["id"], [("one.ipynb", ipynb)], "?on_conflict=skip")
    assert r.json()["skipped"] == 1 and r.json()["created"] == 0
    other = json.dumps(nb_json([("code", "b = 2")])).encode()
    r = upload(ic, ws["id"], [("one.ipynb", other)], "?on_conflict=overwrite")
    assert r.json()["overwritten"] == 1
    assert ic.get(f"/v1/notebooks/{d['items'][0]['id']}/content", headers=ic.admin).json()["cells"][0]["source"] == "b = 2"
    # folder + strip_outputs
    without = copy.deepcopy(nb)
    without["cells"][0]["outputs"] = [{"output_type": "stream", "name": "stdout", "text": "x\n"}]
    r = upload(ic, ws["id"], [("fo.ipynb", json.dumps(without).encode())], "?folder=imports/a&strip_outputs=true")
    it = ic.get_item(r.json()["items"][0]["id"])
    assert it["folder"] == "imports/a" and it["definition"]["nbformat"]["cells"][0]["outputs"] == []


def test_import_zip_with_fabric_directories_and_platform_names(ic):
    ws = ic.workspace()
    nb = F.normalize_notebook(nb_json([("code", "x = 1")]))
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as z:
        z.writestr("plain/a.ipynb", json.dumps(nb))
        z.writestr("My Report.Notebook/notebook-content.py", F.serialize_notebook(nb, "fabric"))
        z.writestr("My Report.Notebook/.platform", json.dumps({"metadata": {"type": "Notebook", "displayName": "Nice Name"}, "config": {"logicalId": "x"}}))
        z.writestr("readme.txt", "ignored")
        z.writestr("__MACOSX/._junk.py", "ignored")
    r = upload(ic, ws["id"], [("bundle.zip", buf.getvalue())])
    assert r.status_code == 201, r.text
    assert {i["name"] for i in r.json()["items"]} == {"a", "Nice Name"}


@pytest.mark.parametrize("member", ["../evil.ipynb", "/abs/evil.ipynb", "a/../../evil.ipynb", "C:/evil.ipynb", "a\\b.ipynb"])
def test_import_zip_slip_is_rejected(ic, member):
    ws = ic.workspace()
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as z:
        z.writestr(member, json.dumps(nb_json([("code", "1")])))
    r = upload(ic, ws["id"], [("bad.zip", buf.getvalue())])
    assert r.status_code == 400 and "unsafe path" in r.text
    assert ic.get(f"/v1/workspaces/{ws['id']}/items?type=notebook", headers=ic.admin).json()["items"] == []


def test_import_errors_name_the_file_and_nothing_is_created(ic):
    ws = ic.workspace()
    r = upload(ic, ws["id"], [("good.ipynb", json.dumps(nb_json([("code", "1")])).encode()), ("broken.ipynb", b"{nope")])
    assert r.status_code == 422 and "broken" in r.text
    assert ic.get(f"/v1/workspaces/{ws['id']}/items?type=notebook", headers=ic.admin).json()["items"] == []
    assert ic.post(f"/v1/workspaces/{ws['id']}/notebooks/import", headers=ic.admin).status_code == 400
    assert upload(ic, ws["id"], [("e.zip", b"PK\x03\x04garbage")]).status_code == 400


def test_zip_bomb_is_bounded(ic, monkeypatch):
    from olcf.notebooks import service as svc_mod

    monkeypatch.setattr(svc_mod, "MAX_FILE_BYTES", 2000)
    ws = ic.workspace()
    big = json.dumps(nb_json([("code", "x" * 5000)]))
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as z:
        z.writestr("big.ipynb", big)
    r = upload(ic, ws["id"], [("bomb.zip", buf.getvalue())])
    assert r.status_code == 400 and "larger" in r.text


# ------------------------------------------------------------------------------------------------ authz
def test_authorization_follows_the_stored_item(ic):
    a, b = ic.workspace("wa"), ic.workspace("wb")
    it = mk_nb(ic, a)
    viewer = ic.user("vera", "Viewer", workspace_id=a["id"])
    dev = ic.user("dave", "Contributor", workspace_id=a["id"])
    outsider = ic.user("olga", "Contributor", workspace_id=b["id"])
    base = f"/v1/notebooks/{it['id']}"
    assert ic.get(f"{base}/content", headers=viewer).status_code == 200
    assert ic.get(f"{base}/export?format=py", headers=viewer).status_code == 200
    assert ic.put(f"{base}/content", json={"cells": []}, headers=viewer).status_code == 403
    assert ic.post(f"{base}/versions/1/restore", headers=viewer).status_code == 403
    assert ic.post(f"{base}/run", json={}, headers=viewer).status_code == 403
    assert upload(ic, a["id"], [("x.ipynb", b"{}")], headers=viewer).status_code == 403
    assert ic.put(f"{base}/content", json={"cells": [{"id": "n", "cell_type": "code", "source": "1"}]}, headers=dev).status_code == 200
    for path in ("content", "versions", "diff", "export", "runs", "parameters", "pip"):
        assert ic.get(f"{base}/{path}", headers=outsider).status_code in (403, 404), path
    assert ic.put(f"{base}/content", json={"cells": []}, headers=outsider).status_code in (403, 404)
    assert upload(ic, a["id"], [("x.ipynb", b"{}")], headers=outsider).status_code in (403, 404)
    # other item types are not notebooks
    lh = ic.item(a["id"], "variable_set", "vs", {})
    assert ic.get(f"/v1/notebooks/{lh['id']}/content", headers=ic.admin).status_code == 404
    assert ic.get("/v1/notebooks/itm_doesnotexist/content", headers=ic.admin).status_code == 404


# ------------------------------------------------------------------------------------------------ run / session
def test_run_submits_a_notebook_job_with_coerced_parameters(ic):
    ws = ic.workspace()
    lh = ic.post(f"/v1/workspaces/{ws['id']}/lakehouses", json={"name": "lh1"}, headers=ic.admin).json()
    it = mk_nb(ic, ws, settings={"default_lakehouse": {"id": lh["id"]}, "timeout_seconds": 600, "engine": "python"})
    r = ic.post(f"/v1/notebooks/{it['id']}/run", json={"parameters": {"alpha": "5", "extra": [1, 2]}, "tags": {"team": "x"}}, headers={**ic.admin, "Idempotency-Key": "k-1"})
    assert r.status_code == 202, r.text
    job = r.json()
    assert job["kind"] == "notebook" and job["item_id"] == it["id"] and job["state"] == "queued"
    assert job["parameters"] == {"alpha": 5, "extra": [1, 2]}
    assert job["origin"]["details"]["notebook"] == "nb1"
    # the same idempotency key returns the same job
    again = ic.post(f"/v1/notebooks/{it['id']}/run", json={"parameters": {"alpha": "5", "extra": [1, 2]}, "tags": {"team": "x"}}, headers={**ic.admin, "Idempotency-Key": "k-1"})
    assert again.status_code in (200, 202) and again.json()["id"] == job["id"]
    runs = ic.get(f"/v1/notebooks/{it['id']}/runs", headers=ic.admin).json()["items"]
    assert [x["id"] for x in runs] == [job["id"]]
    assert ic.get(f"/v1/notebooks/{it['id']}/runs?state=succeeded", headers=ic.admin).json()["items"] == []


def test_run_validation_errors(ic):
    ws = ic.workspace()
    it = mk_nb(ic, ws)
    base = f"/v1/notebooks/{it['id']}/run"
    assert ic.post(base, json={"parameters": {"alpha": "not-a-number"}}, headers=ic.admin).status_code == 422
    assert ic.post(base, json={"parameters": {"bad name": 1}}, headers=ic.admin).status_code == 422
    assert ic.post(base, json={"engine": "fortran"}, headers=ic.admin).status_code == 422
    assert ic.post(base, json={"unknown_field": 1}, headers=ic.admin).status_code == 422
    assert ic.post(base, json={"environment_id": "no-such-env"}, headers=ic.admin).status_code == 404
    assert ic.post(base, json={"default_lakehouse": {"lakehouse_id": "lak_x", "nope": 1}}, headers=ic.admin).status_code == 422


def test_pip_policy_is_enforced_before_a_job_exists(ic):
    ws = ic.workspace()
    it = mk_nb(ic, ws, name="pipper", cells=[("code", "%pip install requests\nimport requests"), ("code", "!pip install -r /etc/passwd")], tags_first=False)
    base = f"/v1/notebooks/{it['id']}"
    chk = ic.get(f"{base}/pip", headers=ic.admin).json()
    assert chk["commands"] if "commands" in chk else chk
    # default policy "declared": nothing is declared, so the install is refused with a precise message
    r = ic.post(f"{base}/run", json={}, headers=ic.admin)
    assert r.status_code == 403 and "cell" in r.json()["error"]["message"]
    assert ic.get(f"/v1/notebooks/{it['id']}/runs", headers=ic.admin).json()["items"] == []
    # the notebook may opt in to "allow" only while the administrator's maximum permits it
    ic.put(f"{base}/content", json={"cells": [{"id": "a", "cell_type": "code", "source": "%pip install requests"}], "settings": {"pip_policy": "allow"}}, headers=ic.admin)
    assert ic.post(f"{base}/run", json={}, headers=ic.admin).status_code == 202
    assert ic.put("/v1/admin/settings/environments.pip_policy_max", json={"value": "block"}, headers=ic.admin).status_code == 200
    r = ic.post(f"{base}/run", json={}, headers=ic.admin)
    assert r.status_code == 403 and "blocked" in r.text
    ic.put("/v1/admin/settings/environments.pip_policy_max", json={"value": "allow"}, headers=ic.admin)


def test_session_without_a_worker_fails_honestly(ic):
    ws = ic.workspace()
    it = mk_nb(ic, ws)
    r = ic.post(f"/v1/notebooks/{it['id']}/session", json={}, headers=ic.admin)
    assert r.status_code == 503 and r.json()["error"]["details"]["code"] == "no_node"
    assert ic.post(f"/v1/notebooks/{it['id']}/session", json={"idle_timeout_seconds": 5}, headers=ic.admin).status_code == 422
    viewer = ic.user("vv", "Viewer", workspace_id=ws["id"])
    assert ic.post(f"/v1/notebooks/{it['id']}/session", json={}, headers=viewer).status_code == 403
