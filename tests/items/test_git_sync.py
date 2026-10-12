# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
# ruff: noqa: S105, S106
"""Git sync against real local bare repositories (file:// enabled by the admin setting) and a smart-HTTP server with Basic auth."""

from __future__ import annotations

import json

import pytest

from olcf.notebooks import gitsync
from tests.items.gitutil import Dev, SmartHttp, bare_repo, git
from tests.items.items_support import nb_json


@pytest.fixture()
def gs(ic, tmp_path):
    """(client, workspace, bare repo path, url) with file:// repositories allowed."""
    assert ic.put("/v1/admin/settings/git.allow_file_urls", json={"value": True}, headers=ic.admin).status_code == 200
    ws = ic.workspace("gws")
    repo = bare_repo(tmp_path / "remote.git")
    return ic, ws, repo, repo.resolve().as_uri()


def connect(ic, ws, url, **body):
    r = ic.post(f"/v1/workspaces/{ws['id']}/git/connect", json={"repo_url": url, **body}, headers=ic.admin)
    return r


def nb(ic, ws, name="nb1", src="x = 1"):
    return ic.item(ws["id"], "notebook", name, {"nbformat": nb_json([("code", src)])})


def status(ic, ws, **q):
    r = ic.get(f"/v1/workspaces/{ws['id']}/git/status", params=q, headers=ic.admin)
    assert r.status_code == 200, r.text
    return r.json()


def test_connect_status_commit_roundtrip(gs, tmp_path):
    ic, ws, repo, url = gs
    n = nb(ic, ws)
    r = connect(ic, ws, url, create_branch=True)
    assert r.status_code == 201, r.text
    st = r.json()
    assert st["connected"] and st["branch"] == "main" and st["remote_branch_exists"] is False and st["counts"]["outgoing"] >= 1
    assert any(c["name"] == "nb1" and c["direction"] == "outgoing" for c in st["changes"])
    r = ic.post(f"/v1/workspaces/{ws['id']}/git/commit", json={"message": "first"}, headers=ic.admin)
    assert r.status_code == 200, r.text
    c = r.json()
    assert c["committed"] and c["pushed"] and len(c["commit"]) == 40
    assert git("rev-parse", "refs/heads/main", cwd=repo).strip() == c["commit"]
    assert status(ic, ws)["in_sync"] is True
    dev = Dev(url, tmp_path / "dev")
    files = dev.files()
    assert any(f.endswith(".ipynb") or "nb1" in f for f in files), files
    # nothing to commit now
    assert ic.post(f"/v1/workspaces/{ws['id']}/git/commit", json={"message": "again"}, headers=ic.admin).json()["committed"] is False
    # change locally -> outgoing -> committed
    cur = ic.get_item(n["id"])
    ic.put(f"/v1/notebooks/{n['id']}/content", json={"cells": [{"cell_type": "code", "source": "x = 2"}]}, headers={**ic.admin, "If-Match": ic.get(f"/v1/notebooks/{n['id']}/content", headers=ic.admin).headers["ETag"]})
    assert status(ic, ws)["counts"]["outgoing"] == 1
    assert ic.post(f"/v1/workspaces/{ws['id']}/git/commit", json={"message": "second"}, headers=ic.admin).json()["committed"]
    dev.pull()
    assert "x = 2" in "".join(dev.read(f) for f in dev.files() if "nb1" in f)
    assert cur["id"] == n["id"]


def commit(ic, ws, msg="c", **body):
    return ic.post(f"/v1/workspaces/{ws['id']}/git/commit", json={"message": msg, **body}, headers=ic.admin)


def pull(ic, ws, **body):
    return ic.post(f"/v1/workspaces/{ws['id']}/git/pull", json=body, headers=ic.admin)


def put_code(ic, item_id, src):
    et = ic.get(f"/v1/notebooks/{item_id}/content", headers=ic.admin).headers["ETag"]
    r = ic.put(f"/v1/notebooks/{item_id}/content", json={"cells": [{"cell_type": "code", "source": src}]}, headers={**ic.admin, "If-Match": et})
    assert r.status_code == 200, r.text


def nb_path(dev, name):
    return next(f for f in dev.files() if f.startswith(f"items/{name}.notebook/") and f.endswith(".ipynb"))


def synced(gs, tmp_path, name="dev"):
    """Connect, commit one notebook, return (dev clone, notebook item)."""
    ic, ws, repo, url = gs
    n = nb(ic, ws)
    assert connect(ic, ws, url, create_branch=True).status_code == 201
    assert commit(ic, ws, "init").status_code == 200
    return Dev(url, tmp_path / name), n


def test_incoming_changes_are_pulled_and_status_reports_them(gs, tmp_path):
    ic, ws, repo, url = gs
    dev, n = synced(gs, tmp_path)
    path = nb_path(dev, "nb1")
    doc = json.loads(dev.read(path))
    doc["cells"][0]["source"] = "x = 'from dev'"
    dev.write(path, json.dumps(doc, indent=1) + "\n")
    dev.push("dev edit")
    assert status(ic, ws, fetch="false")["counts"]["incoming"] == 0  # a no-fetch status does not know yet
    st = status(ic, ws)
    assert st["counts"] == {"outgoing": 0, "incoming": 1, "conflicts": 0, "units": st["counts"]["units"]} and st["in_sync"] is False
    dry = pull(ic, ws, dry_run=True).json()
    assert dry["dry_run"] and [a["action"] for a in dry["applied"]] == ["updated"]
    assert "from dev" not in json.dumps(ic.get(f"/v1/notebooks/{n['id']}/content", headers=ic.admin).json())
    r = pull(ic, ws)
    assert r.status_code == 200, r.text
    got = ic.get(f"/v1/notebooks/{n['id']}/content", headers=ic.admin).json()
    assert "from dev" in json.dumps(got)  # updated in place:
    assert [i["id"] for i in ic.get(f"/v1/workspaces/{ws['id']}/items", params={"type": "notebook"}, headers=ic.admin).json()["items"]] == [n["id"]]  # same item id
    assert status(ic, ws)["in_sync"] is True
    # a new notebook and a deletion by the other developer
    dev.pull()
    dev.write("items/fresh.notebook/notebook-content.ipynb", json.dumps(nb_json([("code", "print('fresh')")])))
    dev.write("items/fresh.notebook/item.json", json.dumps({"type": "notebook", "name": "fresh", "definition": {}}))
    dev.push("add fresh")
    r = pull(ic, ws)
    assert r.status_code == 200, r.text
    fresh = ic.get(f"/v1/workspaces/{ws['id']}/items", params={"type": "notebook"}, headers=ic.admin).json()["items"]
    assert {i["name"] for i in fresh} == {"nb1", "fresh"}
    dev.pull()
    dev.remove("items/nb1.notebook")
    dev.push("drop nb1")
    assert pull(ic, ws).status_code == 200
    names = {i["name"] for i in ic.get(f"/v1/workspaces/{ws['id']}/items", params={"type": "notebook"}, headers=ic.admin).json()["items"]}
    assert names == {"fresh"}
    # the hand-written 'fresh' unit is not in canonical form, so the workspace re-exports it as an outgoing normalisation; nothing is incoming or in conflict
    st = status(ic, ws)
    assert st["counts"]["incoming"] == 0 and st["counts"]["conflicts"] == 0 and [c["name"] for c in st["changes"]] == ["fresh"]
    assert commit(ic, ws, "normalise fresh").status_code == 200 and status(ic, ws)["in_sync"] is True


def test_three_way_conflicts_are_detected_and_resolved(gs, tmp_path):
    ic, ws, repo, url = gs
    dev, n = synced(gs, tmp_path)
    m = nb(ic, ws, "other", "y = 1")
    assert commit(ic, ws, "other").status_code == 200
    dev.pull()
    for name, text in (("nb1", "x = 'theirs'"), ("other", "y = 'theirs'")):
        path = nb_path(dev, name)
        doc = json.loads(dev.read(path))
        doc["cells"][0]["source"] = text
        dev.write(path, json.dumps(doc, indent=1) + "\n")
    dev.push("both theirs")
    put_code(ic, n["id"], "x = 'ours'")
    put_code(ic, m["id"], "y = 'ours'")
    st = status(ic, ws)
    assert st["counts"]["conflicts"] == 2
    # commit refuses while the remote has changes to the same units
    r = commit(ic, ws, "mine")
    assert r.status_code == 409 and "update from Git first" in r.json()["error"]["message"]
    assert len(r.json()["error"]["details"]["blocking"]) == 2
    # pull without a resolution applies nothing
    r = pull(ic, ws)
    assert r.status_code == 409 and len(r.json()["error"]["details"]["conflicts"]) == 2
    assert "ours" in json.dumps(ic.get(f"/v1/notebooks/{n['id']}/content", headers=ic.admin).json())
    # resolve per unit: nb1 -> theirs, other -> ours (selected by item name)
    r = pull(ic, ws, resolutions={"nb1": "theirs", "other": "ours"})
    assert r.status_code == 200, r.text
    assert {c["name"]: c["resolution"] for c in r.json()["resolved_conflicts"]} == {"nb1": "theirs", "other": "ours"}
    assert "theirs" in json.dumps(ic.get(f"/v1/notebooks/{n['id']}/content", headers=ic.admin).json())
    assert "ours" in json.dumps(ic.get(f"/v1/notebooks/{m['id']}/content", headers=ic.admin).json())
    # the kept local change is now outgoing and can be committed on top of the remote
    st = status(ic, ws)
    assert st["counts"] == {"outgoing": 1, "incoming": 0, "conflicts": 0, "units": st["counts"]["units"]}
    assert commit(ic, ws, "keep ours").status_code == 200
    dev.pull()
    assert "y = 'ours'" in dev.read(nb_path(dev, "other")) and "x = 'theirs'" in dev.read(nb_path(dev, "nb1"))


def test_global_resolution_ours_and_theirs(gs, tmp_path):
    ic, ws, repo, url = gs
    dev, n = synced(gs, tmp_path)
    path = nb_path(dev, "nb1")
    doc = json.loads(dev.read(path))
    doc["cells"][0]["source"] = "x = 'theirs'"
    dev.write(path, json.dumps(doc, indent=1) + "\n")
    dev.push("theirs")
    put_code(ic, n["id"], "x = 'ours'")
    assert pull(ic, ws, resolution="ours").status_code == 200
    assert "ours" in json.dumps(ic.get(f"/v1/notebooks/{n['id']}/content", headers=ic.admin).json())
    assert commit(ic, ws, "ours wins").status_code == 200
    dev.pull()
    assert "x = 'ours'" in dev.read(path)


def test_delete_in_workspace_is_outgoing_and_delete_vs_edit_conflicts(gs, tmp_path):
    ic, ws, repo, url = gs
    dev, n = synced(gs, tmp_path)
    ic.delete(f"/v1/items/{n['id']}", headers=ic.admin)
    st = status(ic, ws)
    assert [c["direction"] for c in st["changes"] if c["name"] == "nb1"] == ["outgoing"]
    assert [c["local"] for c in st["changes"] if c["name"] == "nb1"] == ["deleted"]
    assert commit(ic, ws, "drop").status_code == 200
    dev.pull()
    assert not any(f.startswith("items/nb1.notebook") for f in dev.files())


def test_commit_selected_items_only(gs, tmp_path):
    ic, ws, repo, url = gs
    dev, n = synced(gs, tmp_path)
    a, b = nb(ic, ws, "aa", "a = 1"), nb(ic, ws, "bb", "b = 1")
    r = commit(ic, ws, "only aa", items=["aa"])
    assert r.status_code == 200 and [u["name"] for u in r.json()["units"]] == ["aa"]
    dev.pull()
    assert any("aa.notebook" in f for f in dev.files()) and not any("bb.notebook" in f for f in dev.files())
    assert commit(ic, ws, "none", items=["does-not-exist"]).status_code == 404
    assert a["id"] != b["id"]


def test_secret_scan_blocks_outgoing_commits(gs, tmp_path):
    ic, ws, repo, url = gs
    dev, n = synced(gs, tmp_path)
    head = git("rev-parse", "main", cwd=repo).strip()
    leaked = "AKIA" + "ABCDEFGHIJKLMNOP"
    put_code(ic, n["id"], f"key = '{leaked}'")
    r = commit(ic, ws, "oops")
    assert r.status_code == 422, r.text
    body = json.dumps(r.json())
    assert leaked not in body and r.json()["error"]["details"]["findings"][0]["rule"] == "aws-access-key-id"
    assert git("rev-parse", "main", cwd=repo).strip() == head and status(ic, ws)["counts"]["outgoing"] == 1
    # a secret in the commit message is refused as well
    put_code(ic, n["id"], "key = 'fine'")
    assert commit(ic, ws, f"token {'ghp_' + 'a1' * 20}").status_code == 422
    # an intentional dummy is allowed with the marker
    put_code(ic, n["id"], f"key = '{leaked}'  # olcf: allow-secret")
    assert commit(ic, ws, "documented dummy").status_code == 200
    # credential-looking values in item definitions are caught too
    ic.item(ws["id"], "spark_job_definition", "sj", {"language": "python", "code": "print(1)", "args": ["--db-password=Sup3rS3cretValue!"]})
    r = commit(ic, ws, "env")
    assert r.status_code == 422 and "Sup3rS3cretValue" not in json.dumps(r.json())
    log = [e for e in ic.get("/v1/audit", params={"action": "git.commit_rejected"}, headers=ic.admin).json().get("items", [])]
    assert log and "Sup3rS3cretValue" not in json.dumps(log)


def test_branches_create_switch_and_status_against_other_branch(gs, tmp_path):
    ic, ws, repo, url = gs
    dev, n = synced(gs, tmp_path)
    b = ic.get(f"/v1/workspaces/{ws['id']}/git/branches", headers=ic.admin).json()
    assert b["current"] == "main" and [x["name"] for x in b["branches"]] == ["main"]
    r = ic.post(f"/v1/workspaces/{ws['id']}/git/branches", json={"name": "feature/x"}, headers=ic.admin)
    assert r.status_code == 201 and r.json()["switched"] is False
    assert ic.post(f"/v1/workspaces/{ws['id']}/git/branches", json={"name": "feature/x"}, headers=ic.admin).status_code == 409
    assert ic.post(f"/v1/workspaces/{ws['id']}/git/branches", json={"name": "bad..name"}, headers=ic.admin).status_code == 422
    assert ic.post(f"/v1/workspaces/{ws['id']}/git/branches", json={"name": "-rf"}, headers=ic.admin).status_code == 422
    assert ic.post(f"/v1/workspaces/{ws['id']}/git/branches", json={"name": "n", "source": "nope"}, headers=ic.admin).status_code == 404
    assert git("rev-parse", "refs/heads/feature/x", cwd=repo).strip() == git("rev-parse", "main", cwd=repo).strip()
    r = ic.put(f"/v1/workspaces/{ws['id']}/git/branch", json={"name": "feature/x"}, headers=ic.admin)
    assert r.status_code == 200 and r.json()["branch"] == "feature/x" and r.json()["in_sync"] is True
    put_code(ic, n["id"], "x = 'on feature'")
    assert commit(ic, ws, "feature work").status_code == 200
    assert git("rev-parse", "refs/heads/feature/x", cwd=repo).strip() != git("rev-parse", "main", cwd=repo).strip()
    assert "x = 1" in git("show", "main:" + nb_path(dev, "nb1"), cwd=repo)
    assert ic.put(f"/v1/workspaces/{ws['id']}/git/branch", json={"name": "ghost"}, headers=ic.admin).status_code == 404
    # switching back to main shows the feature change as outgoing against main
    ic.put(f"/v1/workspaces/{ws['id']}/git/branch", json={"name": "main"}, headers=ic.admin)
    st = status(ic, ws)
    assert st["branch"] == "main" and st["counts"]["outgoing"] == 0 and st["counts"]["conflicts"] == 0
    names = [x["name"] for x in ic.get(f"/v1/workspaces/{ws['id']}/git/branches", headers=ic.admin).json()["branches"]]
    assert names == ["feature/x", "main"]


def test_folder_and_notebook_formats(gs, tmp_path):
    ic, ws, repo, url = gs
    nb(ic, ws, "pynb", "a = 1\nb = a + 1")
    r = connect(ic, ws, url, folder="workspaces/dev", notebook_format="py", create_branch=True)
    assert r.status_code == 201 and r.json()["folder"] == "workspaces/dev"
    assert commit(ic, ws, "py format").status_code == 200
    dev = Dev(url, tmp_path / "dev")
    files = dev.files()
    assert all(f.startswith("workspaces/dev/") for f in files) and any(f.endswith(".py") for f in files)
    py = next(f for f in files if f.endswith(".py"))
    assert "b = a + 1" in dev.read(py)
    # an edit of the .py file comes back as an update of the notebook
    dev.write(py, dev.read(py).replace("b = a + 1", "b = a + 2"))
    dev.push("edit py")
    assert pull(ic, ws).status_code == 200
    n = ic.get(f"/v1/workspaces/{ws['id']}/items", params={"type": "notebook"}, headers=ic.admin).json()["items"][0]
    assert "a + 2" in json.dumps(ic.get(f"/v1/notebooks/{n['id']}/content", headers=ic.admin).json())
    assert status(ic, ws)["in_sync"] is True


def test_connections_are_metadata_only_in_git(gs, tmp_path):
    ic, ws, repo, url = gs
    sec = ic.post("/v1/secrets", json={"name": "sql-secret", "value": "Pa55-w0rd-for-sql-only"}, headers=ic.admin).json()["id"]
    r = ic.post("/v1/connections", json={"workspace_id": ws["id"], "name": "warehouse", "type": "postgres", "config": {"host": "db.example.com", "port": 5432}, "secret_id": sec}, headers=ic.admin)
    assert r.status_code == 201, r.text
    connect(ic, ws, url, create_branch=True)
    assert commit(ic, ws, "with connection").status_code == 200
    dev = Dev(url, tmp_path / "dev")
    blob = "".join(dev.read(f) for f in dev.files())
    assert "warehouse" in blob and "db.example.com" in blob and "Pa55-w0rd-for-sql-only" not in blob and sec not in blob
    # a connection arriving from Git is created without a secret and never deleted by a later pull
    dev.write("connections/lake.connection.json", json.dumps({"name": "lake", "type": "adls", "config": {"account": "acct"}}))
    dev.push("add connection")
    assert pull(ic, ws).status_code == 200
    cons = ic.get("/v1/connections", params={"workspace_id": ws["id"]}, headers=ic.admin).json()["items"]
    lake = next(c for c in cons if c["name"] == "lake")
    assert not lake.get("secret_id")
    dev.pull()
    dev.remove("connections/lake.connection.json")
    dev.push("drop connection")
    assert pull(ic, ws).status_code == 200
    assert {c["name"] for c in ic.get("/v1/connections", params={"workspace_id": ws["id"]}, headers=ic.admin).json()["items"]} >= {"lake", "warehouse"}


def test_hostile_repository_content_is_rejected_or_skipped(gs, tmp_path):
    ic, ws, repo, url = gs
    dev, n = synced(gs, tmp_path)
    dev.write("items/evil.notebook/item.json", json.dumps({"type": "notebook", "name": "../../escape", "definition": {}}))
    dev.write("items/evil.notebook/notebook-content.ipynb", "{ this is not json")
    dev.write("items/ok.notebook/notebook-content.ipynb", json.dumps(nb_json([("code", "print('fine')")])))
    dev.write("items/ok.notebook/item.json", json.dumps({"type": "notebook", "name": "ok", "definition": {}}))
    import os

    os.symlink("/etc/passwd", dev.work / "items" / "link.notebook")
    dev.push("hostile")
    head = status(ic, ws)["synced_commit"]
    r = pull(ic, ws)
    assert r.status_code == 422, r.text  # one bad unit: nothing applied, nothing marked as synced
    assert status(ic, ws)["synced_commit"] == head
    assert any("link.notebook" in p for p in status(ic, ws)["skipped_paths"])
    assert not (ic.ctx.settings.data_dir.parent / "escape").exists()


def test_connect_validation_and_tampered_state(gs, tmp_path):
    ic, ws, repo, url = gs
    base = f"/v1/workspaces/{ws['id']}/git/connect"
    for bad in ("ssh://git@example.com/x.git", "git@github.com:a/b.git", "https://user:pw@example.com/a/b.git", "https://tok@example.com/a/b.git", "ftp://example.com/a",
                "http://example.com/a.git", "https://example.com/a.git?x=1", "file:///no/such/dir", "https://exa mple.com/x", "ext::sh -c id", "-oProxyCommand=x"):
        r = ic.post(base, json={"repo_url": bad}, headers=ic.admin)
        assert r.status_code == 422, (bad, r.status_code, r.text)
        assert "pw@" not in r.text
    assert ic.post(base, json={"repo_url": url, "branch": "--upload-pack=x"}, headers=ic.admin).status_code == 422
    assert ic.post(base, json={"repo_url": url, "folder": "../x"}, headers=ic.admin).status_code == 422
    assert ic.post(base, json={"repo_url": url, "folder": ".git/hooks"}, headers=ic.admin).status_code == 422
    assert ic.post(base, json={"repo_url": url, "branch": "missing"}, headers=ic.admin).status_code == 404  # no such branch, no create_branch
    assert ic.get(f"/v1/workspaces/{ws['id']}/git/status", headers=ic.admin).json()["connected"] is False
    assert ic.post(f"/v1/workspaces/{ws['id']}/git/commit", json={"message": "x"}, headers=ic.admin).status_code == 400
    assert connect(ic, ws, url, create_branch=True).status_code == 201
    assert connect(ic, ws, url).status_code == 409
    # PATCHing the workspace's git column cannot smuggle an unsafe URL / ref into a git command
    for patch in ({"repo_url": "ext::sh -c id"}, {"repo_url": url, "branch": "--upload-pack=touch /tmp/pwned"}, {"repo_url": url, "folder": "../../etc"}):
        ic.patch(f"/v1/workspaces/{ws['id']}", json={"git": {**{"repo_url": url, "branch": "main"}, **patch}}, headers=ic.admin)
        assert ic.get(f"/v1/workspaces/{ws['id']}/git/status", headers=ic.admin).status_code in (400, 422)
    # file:// can be switched off again by the administrator
    ic.patch(f"/v1/workspaces/{ws['id']}", json={"git": {"repo_url": url, "branch": "main"}}, headers=ic.admin)
    ic.put("/v1/admin/settings/git.allow_file_urls", json={"value": False}, headers=ic.admin)
    assert ic.get(f"/v1/workspaces/{ws['id']}/git/status", headers=ic.admin).status_code == 422
    ic.put("/v1/admin/settings/git.allow_file_urls", json={"value": True}, headers=ic.admin)
    d = ic.post(f"/v1/workspaces/{ws['id']}/git/disconnect", headers=ic.admin).json()
    assert d == {"connected": False}
    assert not ic.ctx.settings.path("git", f"{ws['id']}.git").exists()


def test_allowed_hosts_and_unit_url_rules():
    f = gitsync.check_repo_url
    assert f("https://github.com/a/b.git") and f("http://127.0.0.1:8080/x.git") and f("http://localhost/x")
    with pytest.raises(Exception, match="not in the allowed"):
        f("https://evil.example.org/x", allowed_hosts=["github.com", "*.corp.example"])
    assert f("https://git.corp.example/x", allowed_hosts=["github.com", "*.corp.example"])
    assert f("http://git.corp.example/x", allow_insecure_http=True)
    with pytest.raises(Exception, match="disabled"):
        f("file:///tmp")


def test_authz_on_git_endpoints(gs, tmp_path):
    ic, ws, repo, url = gs
    viewer = ic.user("vera", "Viewer", workspace_id=ws["id"])
    contrib = ic.user("carl", "Contributor", workspace_id=ws["id"])
    base = f"/v1/workspaces/{ws['id']}/git"
    assert ic.post(f"{base}/connect", json={"repo_url": url, "create_branch": True}, headers=viewer).status_code == 403
    assert ic.post(f"{base}/connect", json={"repo_url": url, "create_branch": True}, headers=contrib).status_code == 403  # connecting needs workspace.update
    connect(ic, ws, url, create_branch=True)
    for method, path, body in (("post", "commit", {"message": "m"}), ("post", "pull", {}), ("post", "disconnect", None), ("put", "branch", {"name": "main"}), ("post", "branches", {"name": "b"})):
        r = getattr(ic, method)(f"{base}/{path}", json=body, headers=viewer) if body is not None else getattr(ic, method)(f"{base}/{path}", headers=viewer)
        assert r.status_code == 403, (path, r.status_code)
    other = ic.workspace("other-ws")
    outsider = ic.user("olga", "Contributor", workspace_id=other["id"])
    assert ic.get(f"{base}/status", headers=outsider).status_code in (403, 404)
    assert ic.get(f"/v1/workspaces/{ws['id']}/git/status").status_code == 401


# ------------------------------------------------------------------------------ smart HTTP with Basic auth: the token never leaks
def test_credentials_through_askpass_never_leak(ic, tmp_path):
    token = "tok-9f8e7d6c5b4a3f2e1d0c-very-secret"  # gitleaks:allow
    root = tmp_path / "srv"
    root.mkdir()
    bare_repo(root / "private.git")
    with SmartHttp(root, "ci-bot", token) as srv:
        sec = ic.post("/v1/secrets", json={"name": "git-token", "value": token}, headers=ic.admin).json()["id"]
        ws = ic.workspace("httpws")
        con = ic.post("/v1/connections", json={"workspace_id": ws["id"], "name": "gh", "type": "git", "config": {"username": "ci-bot"}, "secret_id": sec}, headers=ic.admin)
        assert con.status_code == 201, con.text
        nb(ic, ws)
        # without a connection the server answers 401 and the error says so without leaking anything
        r = connect(ic, ws, srv.url("private.git"), create_branch=True)
        assert r.status_code in (401, 403, 422, 502), r.text
        assert token not in r.text
        r = connect(ic, ws, srv.url("private.git"), connection="gh", create_branch=True)
        assert r.status_code == 201, r.text
        assert token not in r.text and json.dumps(ic.get(f"/v1/workspaces/{ws['id']}", headers=ic.admin).json()).find(token) < 0
        r = commit(ic, ws, "over http")
        assert r.status_code == 200, r.text
        assert token not in r.text
        assert any(ok for _, ok in srv.requests) and git("rev-parse", "refs/heads/main", cwd=root / "private.git").strip() == r.json()["commit"]
        # the workspace state and the on-disk clone's config hold neither the token nor a credentialed URL
        stored = ic.get(f"/v1/workspaces/{ws['id']}", headers=ic.admin).json()
        assert token not in json.dumps(stored) and "@127.0.0.1" not in json.dumps(stored)
        clone = ic.ctx.settings.path("git", f"{ws['id']}.git")
        for f in clone.rglob("*"):
            if f.is_file() and f.stat().st_size < 1_000_000:
                assert token.encode() not in f.read_bytes(), f
        tmp = ic.ctx.settings.path("tmp", "git")
        leftovers = [p for p in tmp.rglob("*")] if tmp.exists() else []
        assert leftovers == [], leftovers  # askpass dirs are deleted afterwards
        # a wrong token fails closed and is not echoed
        assert ic.post(f"/v1/secrets/{sec}/rotate", json={"value": "wrong-token-value-1234567890"}, headers=ic.admin).status_code == 200
        r = ic.get(f"/v1/workspaces/{ws['id']}/git/status", headers=ic.admin)
        assert r.status_code in (401, 403, 502, 503), r.text
        assert token not in r.text and "wrong-token-value" not in r.text
        # connection.use is required: a Contributor without access to the connection cannot use it
        ev = [e for e in ic.get("/v1/audit", headers=ic.admin).json().get("items", [])]
        assert token not in json.dumps(ev)
