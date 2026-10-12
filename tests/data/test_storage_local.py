# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Lakehouses on the local provider: lifecycle, files API, traversal defence, tables."""

from __future__ import annotations

import os

import pytest


@pytest.fixture()
def lh(dc):
    dc.workspace("ws1")
    return dc.lakehouse("ws1", "lh1")


def test_create_lakehouse_defaults_and_folders(dc, lh):
    assert lh["provider"] == "local"
    assert lh["root_uri"].startswith("file://") and lh["root_uri"].endswith("ws1/lh1.Lakehouse")
    assert lh["canonical_uri"] == "olcf://ws1/lh1"
    root = lh["root_uri"][len("file://"):]
    assert os.path.isdir(root + "/Files") and os.path.isdir(root + "/Tables")
    r = dc.get("/v1/lakehouses", headers=dc.admin)
    assert [x["id"] for x in r.json()["items"]] == [lh["id"]]
    assert dc.get("/v1/workspaces/ws1/lakehouses", headers=dc.admin).json()["items"][0]["name"] == "lh1"
    # generic items API sees it too, with a normalised definition
    item = dc.get(f"/v1/items/{lh['id']}", headers=dc.admin).json()
    assert item["definition"]["storage"]["root_uri"] == lh["root_uri"]


def test_files_roundtrip(dc, lh):
    h, p = dc.admin, f"/v1/lakehouses/{lh['id']}"
    assert dc.post(f"{p}/files/mkdir", json={"path": "Files/raw"}, headers=h).status_code == 201
    up = dc.put(f"{p}/files/content", params={"path": "Files/raw/a.csv"}, content=b"id,name\n1,x\n2,y\n", headers=h)
    assert up.status_code == 201 and up.json()["size"] == 16
    ls = dc.get(f"{p}/files", params={"path": "Files/raw"}, headers=h).json()
    assert [f["name"] for f in ls["items"]] == ["a.csv"]
    assert dc.get(f"{p}/files/content", params={"path": "Files/raw/a.csv"}, headers=h).content == b"id,name\n1,x\n2,y\n"
    part = dc.get(f"{p}/files/content", params={"path": "Files/raw/a.csv"}, headers={**h, "Range": "bytes=3-6"})
    assert part.status_code == 206 and part.content == b"name" and part.headers["content-range"] == "bytes 3-6/16"
    # multipart upload
    mp = dc.post(f"{p}/files/upload", params={"path": "Files/raw"}, files={"file": ("b.txt", b"hello")}, headers=h)
    assert mp.status_code == 201 and mp.json()["uploaded"][0]["path"] == "Files/raw/b.txt"
    # move / copy / delete
    assert dc.post(f"{p}/files/copy", json={"src": "Files/raw/b.txt", "dst": "Files/raw/c.txt"}, headers=h).status_code == 200
    assert dc.post(f"{p}/files/move", json={"src": "Files/raw/c.txt", "dst": "Files/d/c.txt"}, headers=h).status_code == 200
    assert dc.post(f"{p}/files/move", json={"src": "Files/d/c.txt", "dst": "Files/raw/b.txt"}, headers=h).status_code == 409
    assert dc.delete(f"{p}/files", params={"path": "Files/d/c.txt"}, headers=h).json()["deleted"] == 1
    assert dc.delete(f"{p}/files", params={"path": "Files/d/c.txt"}, headers=h).status_code == 404
    # preview of CSV
    pv = dc.get(f"{p}/files/preview", params={"path": "Files/raw/a.csv"}, headers=h).json()
    assert pv["kind"] == "table" and pv["rows"] == [[1, "x"], [2, "y"]]
    assert dc.get(f"{p}/files/preview", params={"path": "Files/raw/b.txt"}, headers=h).json()["text"] == "hello"
    # root listing shows both managed areas
    assert [f["name"] for f in dc.get(f"{p}/files", headers=h).json()["items"]] == ["Files", "Tables"]


def test_mutations_are_limited_to_files_area(dc, lh):
    h, p = dc.admin, f"/v1/lakehouses/{lh['id']}"
    for path in ("Tables/x", "other/x.txt", "x.txt"):
        r = dc.put(f"{p}/files/content", params={"path": path}, content=b"x", headers=h)
        assert r.status_code == 400, (path, r.text)
    assert dc.post(f"{p}/files/mkdir", json={"path": "Tables/evil"}, headers=h).status_code == 400


@pytest.mark.parametrize("bad", ["../etc/passwd", "Files/../../x", "/etc/passwd", "Files/a/../../../x", "C:/x", "Files\\x", "Files/\x00x"])
def test_path_traversal_is_rejected(dc, lh, bad):
    h, p = dc.admin, f"/v1/lakehouses/{lh['id']}"
    assert dc.get(f"{p}/files", params={"path": bad}, headers=h).status_code == 400
    assert dc.put(f"{p}/files/content", params={"path": bad}, content=b"x", headers=h).status_code == 400
    assert dc.get(f"{p}/files/content", params={"path": bad}, headers=h).status_code == 400
    assert dc.delete(f"{p}/files", params={"path": bad}, headers=h).status_code == 400


def test_symlink_escape_is_refused(dc, lh, tmp_path):
    secret = tmp_path / "secret.txt"
    secret.write_text("top secret")
    root = lh["root_uri"][len("file://"):]
    os.symlink(secret, root + "/Files/link.txt")
    os.symlink(tmp_path, root + "/Files/linkdir")
    h, p = dc.admin, f"/v1/lakehouses/{lh['id']}"
    assert dc.get(f"{p}/files/content", params={"path": "Files/link.txt"}, headers=h).status_code == 403
    assert dc.get(f"{p}/files/content", params={"path": "Files/linkdir/secret.txt"}, headers=h).status_code == 403
    assert dc.put(f"{p}/files/content", params={"path": "Files/linkdir/new.txt"}, content=b"x", headers=h).status_code == 403
    assert not (tmp_path / "new.txt").exists()
    # symlinks are not even listed
    names = [f["name"] for f in dc.get(f"{p}/files", params={"path": "Files"}, headers=h).json()["items"]]
    assert "link.txt" not in names and "linkdir" not in names


def test_upload_limit_enforced_and_no_partial_file(dc, lh):
    dc.ctx.settings.storage.max_upload_mb = 1
    h, p = dc.admin, f"/v1/lakehouses/{lh['id']}"
    r = dc.put(f"{p}/files/content", params={"path": "Files/big.bin"}, content=b"0" * (2 * 1024 * 1024), headers=h)
    assert r.status_code == 413
    assert dc.get(f"{p}/files/stat", params={"path": "Files/big.bin"}, headers=h).status_code == 404
    root = lh["root_uri"][len("file://"):]
    assert not [n for n in os.listdir(root + "/Files") if n.startswith(".olcf-upload")]


def test_local_root_outside_allowed_roots_rejected(dc):
    dc.workspace("ws1")
    r = dc.post("/v1/workspaces/ws1/lakehouses", json={"name": "evil", "root_uri": "file:///etc"}, headers=dc.admin)
    assert r.status_code == 422 and "storage root" in r.text
    r = dc.post("/v1/workspaces/ws1/items", json={"type": "lakehouse", "name": "evil2", "definition": {"storage": {"root_uri": "file:///tmp/x"}}}, headers=dc.admin)
    assert r.status_code == 422
    # symlink inside the storage root pointing outside is not accepted either
    link = dc.ctx.settings.local_storage_root / "escape"
    link.parent.mkdir(parents=True, exist_ok=True)
    os.symlink("/etc", link)
    r = dc.post("/v1/workspaces/ws1/lakehouses", json={"name": "evil3", "root_uri": f"file://{link}"}, headers=dc.admin)
    assert r.status_code == 422


def test_remote_root_requires_connection(dc):
    dc.workspace("ws1")
    r = dc.post("/v1/workspaces/ws1/lakehouses", json={"name": "s3lh", "root_uri": "s3://bucket/prefix"}, headers=dc.admin)
    assert r.status_code == 422 and "connection" in r.text


def test_rbac_on_lakehouse_endpoints(dc, lh):
    p = f"/v1/lakehouses/{lh['id']}"
    ws_id = lh["workspace_id"]
    viewer = dc.user("vera", "Viewer", workspace_id=ws_id)
    contrib = dc.user("carl", "Contributor", workspace_id=ws_id)
    outsider = dc.user("olga", "Viewer", workspace_id="wks_other")
    assert dc.get(p, headers=viewer).status_code == 200
    assert dc.get(p, headers=outsider).status_code == 403
    assert dc.get(f"{p}/files", headers=outsider).status_code == 403
    assert dc.put(f"{p}/files/content", params={"path": "Files/x"}, content=b"1", headers=viewer).status_code == 403
    assert dc.put(f"{p}/files/content", params={"path": "Files/x"}, content=b"1", headers=contrib).status_code == 201
    # recursive delete needs lakehouse.destructive (WorkspaceAdmin only)
    dc.post(f"{p}/files/mkdir", json={"path": "Files/dir"}, headers=contrib)
    assert dc.delete(f"{p}/files", params={"path": "Files/dir", "recursive": "true"}, headers=contrib).status_code == 403
    assert dc.delete(f"{p}/files", params={"path": "Files/dir", "recursive": "true"}, headers=dc.admin).status_code == 200
    assert [x["id"] for x in dc.get("/v1/lakehouses", headers=outsider).json()["items"]] == []
    assert [x["id"] for x in dc.get("/v1/lakehouses", headers=viewer).json()["items"]] == [lh["id"]]


def test_providers_catalogue(dc):
    r = dc.get("/v1/storage/providers", headers=dc.admin).json()
    ids = {p["id"]: p for p in r["items"]}
    assert set(ids) >= {"local", "s3", "adls", "onelake"}
    assert any(f["name"] == "endpoint_url" for f in ids["s3"]["fields"])
    assert ids["s3"]["capabilities"]["delta_commits"] == "conditional_put" and ids["adls"]["capabilities"]["atomic_rename"] is True
    assert "minio" in ids["s3"]["connection_types"]


def test_lakehouse_test_steps_local(dc, lh):
    r = dc.post(f"/v1/lakehouses/{lh['id']}/test", headers=dc.admin).json()
    assert r["ok"] is True
    assert [s["name"] for s in r["steps"]] == ["resolve DNS", "connect", "auth", "list", "write", "read", "delete"]
    root = lh["root_uri"][len("file://"):]
    assert os.listdir(root + "/Files") == []  # probe cleaned up
