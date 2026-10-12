# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Spec 35.4: path traversal and host-filesystem access through every file-ish surface: lakehouse file API, lakehouse/connection roots, SQL, git sync, workspace and
notebook archives, job artifacts, the Spark UI proxy, runtime jars / bundles, the static UI and the ops endpoints (backups, support bundles).

Findings fixed while writing these tests (kept as regression tests):

* a workspace administrator could create a ``local`` connection rooted at ``/`` (or the storage root) and read every other tenant's files and the server's secrets;
* any contributor could create a lakehouse whose ``root_uri`` was another tenant's lakehouse directory (it lies "inside the storage root");
* a malformed ``workspace.json`` / ``item.json`` inside an uploaded archive produced HTTP 500.
"""

from __future__ import annotations

import io
import shutil
import subprocess
import zipfile
from pathlib import Path

import pytest

from tests.security.secenv import CANARY_A, CANARY_B

CLIENT_ERRORS = (400, 401, 403, 404, 405, 409, 413, 415, 422)


def zipped(entries: list[tuple[str, bytes]]) -> bytes:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as z:
        for name, data in entries:
            z.writestr(name, data)
    return buf.getvalue()


@pytest.fixture(scope="module")
def env(two_tenants):
    return two_tenants


def tree_names(root: Path) -> set[str]:
    return {p.name for p in root.rglob("*")} if root.exists() else set()


# ---------------------------------------------------------------------------------------------------------------- lakehouse file API
READ_PATHS = ["Files/../../x", "Files/../../../../etc/passwd", "/etc/passwd", "Files//etc/passwd", "Files\\..\\..\\x", "Files/%2e%2e/%2e%2e/x", "Files/a\x00b", "Files/./../Tables",
              "../lh_b/Files/secret.txt", "Files/../../../lakehouses/tenant-b/lh_b.Lakehouse/Files/secret.txt", "file:///etc/passwd", "olcf://tenant-b/lh_b/Files/secret.txt"]


@pytest.mark.parametrize("path", READ_PATHS)
def test_lakehouse_read_cannot_leave_the_lakehouse(env, path):
    h = env.headers("a_contrib")
    r = env.c.get(f"/v1/lakehouses/{env.tenants['A'].ids['lh_id']}/files/content", params={"path": path}, headers=h)
    assert r.status_code in CLIENT_ERRORS, (path, r.status_code, r.text[:200])
    assert CANARY_B not in r.text and "root:x:0:0" not in r.text


@pytest.mark.parametrize("path", ["Files/../../pwn", "/tmp/olcf-pwn", "Files/..\\pwn", "../pwn", "Tables/../../../pwn"])  # noqa: S108 - deliberate fixed path: the test asserts on / needs exactly this location
def test_lakehouse_write_cannot_leave_the_lakehouse(env, path):
    h = env.headers("a_contrib")
    lh = env.tenants["A"].ids["lh_id"]
    r = env.c.put(f"/v1/lakehouses/{lh}/files/content", params={"path": path}, content=b"x", headers=h)
    assert r.status_code in CLIENT_ERRORS, (path, r.status_code, r.text[:200])
    assert not Path("/tmp/olcf-pwn").exists()  # noqa: S108 - deliberate fixed path: the test asserts on / needs exactly this location
    root = env.ctx.settings.local_storage_root
    assert not list(root.rglob("pwn")) and not list(root.parent.glob("pwn"))


def test_move_copy_upload_cannot_escape(env):
    h = env.headers("a_contrib")
    lh = env.tenants["A"].ids["lh_id"]
    for body in ({"src": "Files/secret.txt", "dst": "Files/../../moved"}, {"src": "../../etc/passwd", "dst": "Files/p"},
                 {"src": "olcf://tenant-b/lh_b/Files/secret.txt", "dst": "Files/stolen.txt"}):
        for op in ("move", "copy"):
            r = env.c.post(f"/v1/lakehouses/{lh}/files/{op}", json=body, headers=h)
            assert r.status_code in CLIENT_ERRORS, (op, body, r.status_code, r.text[:200])
    assert env.c.get(f"/v1/lakehouses/{lh}/files/content", params={"path": "Files/stolen.txt"}, headers=h).status_code == 404
    r = env.c.post(f"/v1/lakehouses/{lh}/files/upload", files={"file": ("../../evil.txt", b"x")}, params={"path": "Files"}, headers=h)
    assert r.status_code in (200, 201) or r.status_code in CLIENT_ERRORS
    assert not list(env.ctx.settings.local_storage_root.parent.glob("evil.txt")) and not list(env.ctx.settings.local_storage_root.glob("evil.txt"))


def test_symlink_inside_a_lakehouse_does_not_escape(env, tmp_path):
    """A planted symlink (e.g. through a git checkout or an archive) must not turn the lakehouse into a window on the host."""
    t = env.tenants["A"]
    h = env.headers("a_contrib")
    r = env.c.get(f"/v1/lakehouses/{t.ids['lh_id']}", headers=h)
    root = Path(r.json()["root_uri"].removeprefix("file://"))
    secret = tmp_path / "outside.txt"
    secret.write_text("OUTSIDE-SECRET")
    link = root / "Files" / "link.txt"
    link.symlink_to(secret)
    dlink = root / "Files" / "dirlink"
    dlink.symlink_to(tmp_path, target_is_directory=True)
    try:
        for p in ("Files/link.txt", "Files/dirlink/outside.txt"):
            rr = env.c.get(f"/v1/lakehouses/{t.ids['lh_id']}/files/content", params={"path": p}, headers=h)
            assert "OUTSIDE-SECRET" not in rr.text, p
            assert rr.status_code in CLIENT_ERRORS, (p, rr.status_code)
        rr = env.c.put(f"/v1/lakehouses/{t.ids['lh_id']}/files/content", params={"path": "Files/dirlink/new.txt"}, content=b"x", headers=h)
        assert rr.status_code in CLIENT_ERRORS
        assert not (tmp_path / "new.txt").exists()
    finally:
        link.unlink()
        dlink.unlink()


# ---------------------------------------------------------------------------------------------------------------- roots: lakehouses and connections
@pytest.mark.parametrize("who", ["a_admin", "a_contrib"])
@pytest.mark.parametrize("label", ["b_dir", "storage_root", "data_dir", "etc", "traversal", "tmp", "root"])
def test_lakehouse_root_cannot_cover_other_tenants_or_the_host(env, who, label):
    """Regression: any contributor could create a lakehouse over tenant B's directory (it is "inside the storage root")."""
    storage = env.ctx.settings.local_storage_root
    uri = {"b_dir": f"file://{storage}/lakehouses/tenant-b/lh_b.Lakehouse", "storage_root": f"file://{storage}", "data_dir": f"file://{env.ctx.settings.data_dir}",
           "etc": "file:///etc", "traversal": f"file://{storage}/lakehouses/tenant-a/../tenant-b", "tmp": "/tmp", "root": "file:///"}[label]  # noqa: S108 - deliberate fixed path: the test asserts on / needs exactly this location
    t = env.tenants["A"]
    r = env.c.post(f"/v1/workspaces/{t.ws}/lakehouses", json={"name": f"x_{label}_{who[2:]}", "root_uri": uri}, headers=env.headers(who))
    assert r.status_code in (400, 403, 422), (label, r.status_code, r.text[:200])
    assert CANARY_B not in r.text
    items = env.c.get(f"/v1/workspaces/{t.ws}/items", params={"type": "lakehouse"}, headers=env.admin).json()["items"]
    assert not [i for i in items if i["name"] == f"x_{label}_{who[2:]}"], "the rejected lakehouse must not be left behind"


def test_workspace_admin_cannot_create_local_connection(env):
    """Regression: ``connection.manage`` in one workspace let its administrator root a ``local`` connection at ``/`` and read the host."""
    t = env.tenants["A"]
    for who in ("a_admin", "a_contrib"):
        for root in ("/", "/etc", str(env.ctx.settings.local_storage_root)):
            r = env.c.post("/v1/connections", json={"workspace_id": t.ws, "name": f"evil-{who}-{abs(hash(root))}", "type": "local", "config": {"root": root}}, headers=env.headers(who))
            assert r.status_code == 403, (who, root, r.status_code, r.text[:200])
    # editing an existing (platform-created) local connection is platform-only as well
    r = env.c.patch(f"/v1/connections/{t.ids['cid']}", json={"config": {"root": "/etc"}}, headers=env.headers("a_admin"))
    assert r.status_code == 403, r.text
    cur = env.c.get(f"/v1/connections/{t.ids['cid']}", headers=env.admin).json()
    assert cur["config"]["root"] == f"/tmp/{CANARY_A}"  # noqa: S108 - deliberate fixed path: the test asserts on / needs exactly this location


def test_platform_admin_can_still_approve_a_local_root(env, tmp_path):
    """The legitimate path stays open: a platform administrator creates the local connection, the workspace uses it."""
    t = env.tenants["A"]
    root = tmp_path / "approved"
    root.mkdir()
    (root / "ok.txt").write_text("approved-data")
    c = env.c.post("/v1/connections", json={"workspace_id": t.ws, "name": "approved-root", "type": "local", "config": {"root": str(root)}}, headers=env.admin)
    assert c.status_code == 201, c.text
    r = env.c.post(f"/v1/workspaces/{t.ws}/lakehouses", json={"name": "approved_lh", "root_uri": f"file://{root}", "connection_id": c.json()["id"]}, headers=env.headers("a_admin"))
    assert r.status_code == 201, r.text
    got = env.c.get(f"/v1/lakehouses/{r.json()['id']}/files/content", params={"path": "ok.txt"}, headers=env.headers("a_contrib"))
    assert got.status_code == 200 and got.text == "approved-data"
    # ... and the approval does not extend to the neighbour directory
    r2 = env.c.post(f"/v1/workspaces/{t.ws}/lakehouses", json={"name": "approved_up", "root_uri": f"file://{root.parent}", "connection_id": c.json()["id"]}, headers=env.headers("a_admin"))
    assert r2.status_code == 422, r2.text


def test_repointing_an_existing_lakehouse_is_checked(env):
    """Updating a lakehouse definition must not be a way around the root check."""
    t = env.tenants["A"]
    storage = env.ctx.settings.local_storage_root
    r = env.c.put(f"/v1/items/{t.ids['lh_id']}", json={"definition": {"storage": {"root_uri": f"file://{storage}/lakehouses/tenant-b/lh_b.Lakehouse"}}}, headers=env.headers("a_admin"))
    assert r.status_code in (403, 422), (r.status_code, r.text[:200])
    cur = env.c.get(f"/v1/lakehouses/{t.ids['lh_id']}", headers=env.admin).json()
    assert "tenant-b" not in cur["root_uri"]


def test_imported_archive_cannot_smuggle_a_local_connection_or_root(env):
    t = env.tenants["A"]
    z = zipped([("workspace.json", b'{"name":"zz-smuggle"}'),
                ("connections/a.json", b'{"name":"smuggled","type":"local","config":{"root":"/etc"}}')])
    r = env.c.post("/v1/workspaces/import", params={"target_workspace": t.ws}, files={"file": ("x.zip", z)}, headers=env.headers("a_admin"))
    assert r.status_code in (403, 422), (r.status_code, r.text[:200])
    z = zipped([("items/a/item.json", b'{"type":"lakehouse","name":"smug","definition":{"storage":{"root_uri":"file:///etc"}}}')])
    r = env.c.post("/v1/workspaces/import", params={"target_workspace": t.ws}, files={"file": ("x.zip", z)}, headers=env.headers("a_admin"))
    assert r.status_code in CLIENT_ERRORS, (r.status_code, r.text[:200])
    items = env.c.get(f"/v1/workspaces/{t.ws}/items", headers=env.admin).json()["items"]
    assert not [i for i in items if i["name"] == "smug"]
    cons = env.c.get("/v1/connections", params={"workspace_id": t.ws}, headers=env.admin).json()["items"]
    assert not [c for c in cons if c["name"] == "smuggled"]


# ---------------------------------------------------------------------------------------------------------------- SQL
SQL_ATTACKS = ["SELECT * FROM read_csv('/etc/passwd')", "SELECT * FROM read_csv('Files/../../x')", "SELECT * FROM read_csv('olcf://tenant-b/lh_b/Files/secret.txt')",
               "SELECT * FROM read_text('/etc/passwd')", "COPY (SELECT 1) TO '/tmp/olcf-pwn.csv'", "ATTACH '/tmp/olcf-x.db'", "INSTALL httpfs", "SELECT * FROM glob('/*')",
               "SELECT getenv('HOME')", "SELECT * FROM duckdb_settings()", "SET enable_external_access=true", "SELECT * FROM read_parquet('/etc/*')",
               "SELECT * FROM read_json_auto('file:///etc/passwd')", "CREATE TABLE t AS SELECT * FROM read_csv('/etc/hostname')", "PRAGMA database_list",
               "SELECT * FROM sniff_csv('/etc/passwd')", "EXPORT DATABASE '/tmp/olcf-export'", "LOAD '/tmp/evil.duckdb_extension'"]


@pytest.mark.parametrize("sql", SQL_ATTACKS)
def test_sql_cannot_touch_the_host_or_other_tenants(env, sql):
    t = env.tenants["A"]
    r = env.c.post("/v1/sql/execute", json={"sql": sql, "lakehouse": "lh_a", "workspace_id": t.ws}, headers=env.headers("a_contrib"))
    assert r.status_code in CLIENT_ERRORS, (sql, r.status_code, r.text[:200])
    assert "root:x:0:0" not in r.text and CANARY_B not in r.text
    assert not Path("/tmp/olcf-pwn.csv").exists() and not Path("/tmp/olcf-x.db").exists() and not Path("/tmp/olcf-export").exists()  # noqa: S108 - deliberate fixed path: the test asserts on / needs exactly this location


# ---------------------------------------------------------------------------------------------------------------- git sync
@pytest.mark.parametrize("url", ["ext::sh -c 'touch /tmp/olcf-pwn' %S", "file:///etc", "/etc", "-upload-pack=touch /tmp/x", "ssh://x/y", "git://x/y", "http://169.254.169.254/latest",
                                  "https://user:pw@example.com/r.git", "https://example.com/r.git\nfoo", "https://example.com/r.git --upload-pack=x", "fd::3", "https://[::1"])
def test_git_connect_rejects_dangerous_urls(env, url):
    r = env.c.post(f"/v1/workspaces/{env.tenants['A'].ws}/git/connect", json={"repo_url": url}, headers=env.headers("a_admin"))
    assert r.status_code == 422, (url, r.status_code, r.text[:200])
    assert not Path("/tmp/olcf-pwn").exists()  # noqa: S108 - deliberate fixed path: the test asserts on / needs exactly this location


@pytest.mark.parametrize("branch", ["../x", "-x", "a..b", "x;rm -rf", "a b", "refs/../x", "--upload-pack=x", "x.lock", "@{u}", "a\nb"])
def test_git_branch_names_are_validated(env, branch):
    r = env.c.post(f"/v1/workspaces/{env.tenants['A'].ws}/git/connect", json={"repo_url": "https://example.invalid/r.git", "branch": branch}, headers=env.headers("a_admin"))
    assert r.status_code == 422, (branch, r.status_code, r.text[:200])


@pytest.mark.parametrize("folder", ["../x", "a/../../b", "a/./b", ".git", "x/.git/hooks", "a\\b"])
def test_git_folder_is_validated(env, folder):
    r = env.c.post(f"/v1/workspaces/{env.tenants['A'].ws}/git/connect", json={"repo_url": "https://example.invalid/r.git", "folder": folder}, headers=env.headers("a_admin"))
    assert r.status_code == 422, (folder, r.status_code, r.text[:200])


def test_git_connection_of_another_tenant_cannot_be_used(env):
    r = env.c.post(f"/v1/workspaces/{env.tenants['A'].ws}/git/connect", json={"repo_url": "https://example.invalid/r.git", "connection": env.tenants["B"].ids["cid"]}, headers=env.headers("a_admin"))
    assert r.status_code in (403, 404, 422), r.text[:200]


@pytest.mark.skipif(shutil.which("git") is None, reason="git is not installed")
def test_git_commit_tree_is_confined_and_secret_free(env, tmp_path):
    """Items with hostile names are committed under sanitised paths inside the configured folder; no secret value reaches the repository."""
    c, adm = env.c, env.admin
    assert c.put("/v1/admin/settings/git.allow_file_urls", json={"value": True}, headers=adm).status_code == 200
    try:
        ws = c.post("/v1/workspaces", json={"name": "git-sec"}, headers=adm).json()["id"]
        bare = tmp_path / "remote.git"
        subprocess.run(["git", "init", "--bare", "-b", "main", str(bare)], check=True, capture_output=True)
        r = c.post(f"/v1/workspaces/{ws}/git/connect", json={"repo_url": f"file://{bare}", "branch": "main", "create_branch": True, "folder": "ws"}, headers=adm)
        assert r.status_code == 201, r.text
        nb = {"type": "notebook", "definition": {"nbformat": {"nbformat": 4, "nbformat_minor": 5, "metadata": {}, "cells": []}}}
        for name in ["../../evil", "a/b", ".git", "-rf", "x y", "..", "$(id)", "nul\x00x", "~/x", "con"]:
            c.post(f"/v1/workspaces/{ws}/items", json={**nb, "name": name}, headers=adm)
        sec = c.post("/v1/secrets", json={"name": "git-sec-secret", "value": "GIT-SECRET-VALUE-91ac3f", "workspace_id": ws}, headers=adm).json()
        c.post("/v1/connections", json={"workspace_id": ws, "name": "git-sec-conn", "type": "s3", "config": {"bucket": "b"}, "secret_id": sec["id"]}, headers=adm)
        r = c.post(f"/v1/workspaces/{ws}/git/commit", json={"message": "--amend\n\nSigned-off-by: x"}, headers=adm)
        assert r.status_code == 200, r.text
        files = subprocess.run(["git", "--git-dir", str(bare), "ls-tree", "-r", "--name-only", "main"], capture_output=True, text=True, check=True).stdout.splitlines()
        assert files and all(f.startswith("ws/") for f in files), files
        for f in files:
            parts = f.split("/")
            assert ".." not in parts and ".git" not in parts and not any(p.startswith("-") for p in parts[1:2]) and "\x00" not in f, f
        dump = subprocess.run(["git", "--git-dir", str(bare), "log", "-p", "--all"], capture_output=True, text=True, check=True).stdout
        assert "GIT-SECRET-VALUE-91ac3f" not in dump
        assert subprocess.run(["git", "--git-dir", str(bare), "log", "-1", "--format=%s"], capture_output=True, text=True, check=True).stdout.startswith("--amend")
        for it in (["../../x"], ["--all"], ["-A"]):
            r = c.post(f"/v1/workspaces/{ws}/git/commit", json={"message": "m", "items": it}, headers=adm)
            assert r.status_code in CLIENT_ERRORS, (it, r.status_code)
    finally:
        c.put("/v1/admin/settings/git.allow_file_urls", json={"value": False}, headers=adm)


# ---------------------------------------------------------------------------------------------------------------- archives
ARCHIVES = {
    "dotdot": [("../evil.txt", b"x"), ("workspace.json", b'{"name":"zz"}')],
    "absolute": [("/etc/evil", b"x")],
    "backslash": [("..\\evil", b"x")],
    "items-dotdot": [("workspace.json", b'{"name":"zz2"}'), ("items/../../x/item.json", b"{}")],
    "bad-manifest": [("manifest.json", b'{"files":{"a":"00"}}')],
    "manifest-list": [("manifest.json", b"[]")],
    "manifest-files-list": [("manifest.json", b'{"files":[]}')],
    "bad-json": [("workspace.json", b"not json")],
    "ws-json-list": [("workspace.json", b"[]")],
    "ws-name-bad": [("workspace.json", b'{"name":"../../zz6"}')],
    "item-empty": [("workspace.json", b'{"name":"zz3"}'), ("items/a/item.json", b"{}")],
    "item-not-json": [("workspace.json", b'{"name":"zz3b"}'), ("items/a/item.json", b"zzz")],
    "item-type-bogus": [("workspace.json", b'{"name":"zz4"}'), ("items/a/item.json", b'{"type":"../../etc","name":"x"}')],
    "item-name-slash": [("workspace.json", b'{"name":"zz5"}'), ("items/a/item.json", b'{"type":"notebook","name":"../../x","definition":{}}')],
    "conn-missing": [("workspace.json", b'{"name":"zz7"}'), ("connections/a.json", b"{}")],
    "conn-not-object": [("workspace.json", b'{"name":"zz7b"}'), ("connections/a.json", b"[1]")],
    "empty": [],
}


@pytest.mark.parametrize("label", sorted(ARCHIVES))
def test_workspace_import_survives_hostile_archives(env, label):
    """Never a 5xx, never a file outside the platform (zip-slip), never data in a foreign workspace."""
    data = zipped(ARCHIVES[label])
    for who, params in (("admin", {}), ("a_admin", {"target_workspace": env.tenants["A"].ws}), ("a_admin", {"target_workspace": env.tenants["B"].ws})):
        r = env.c.post("/v1/workspaces/import", params=params, files={"file": ("x.zip", data)}, headers=env.headers(who))
        assert r.status_code < 500, (label, who, r.status_code, r.text[:200])
        if params.get("target_workspace") == env.tenants["B"].ws:
            assert r.status_code == 403
    assert not Path("/etc/evil").exists()
    assert not list(env.ctx.settings.data_dir.parent.glob("evil*")) and not list(env.ctx.settings.data_dir.glob("evil*"))


def test_workspace_import_rejects_non_zip_and_bombs(env):
    h = env.headers("admin")
    assert env.c.post("/v1/workspaces/import", files={"file": ("x.zip", b"not a zip")}, headers=h).status_code in (400, 422)
    assert env.c.post("/v1/workspaces/import", files={"file": ("x.zip", b"")}, headers=h).status_code in CLIENT_ERRORS
    big = zipped([("items/a/blob.bin", b"\0" * (64 * 1024 * 1024))])  # compresses to ~64 KiB; below the expansion limit but must not crash the server
    assert len(big) < 1024 * 1024
    r = env.c.post("/v1/workspaces/import", files={"file": ("x.zip", big)}, headers=h)
    assert r.status_code < 500, r.text[:200]


def test_notebook_import_archive_paths(env):
    t = env.tenants["A"]
    h = env.headers("a_admin")
    for name in ("../x.ipynb", "/x.py", "a/../../b.py"):
        r = env.c.post(f"/v1/workspaces/{t.ws}/notebooks/import", files={"file": ("x.zip", zipped([(name, b"print(1)")]))}, headers=h)
        assert r.status_code in CLIENT_ERRORS, (name, r.status_code, r.text[:200])
    r = env.c.post(f"/v1/workspaces/{env.tenants['B'].ws}/notebooks/import", files={"file": ("x.py", b"print(1)")}, headers=h)
    assert r.status_code == 403
    r = env.c.post(f"/v1/workspaces/{t.ws}/notebooks/import", params={"folder": "../../x"}, files={"file": ("x.py", b"print(1)")}, headers=h)
    assert r.status_code < 500
    if r.status_code == 201:  # folders are virtual (item metadata), they never touch the filesystem
        assert not list(env.ctx.settings.data_dir.parent.glob("x"))


# ---------------------------------------------------------------------------------------------------------------- artifacts, spark UI, jars, bundles
@pytest.mark.parametrize("name", ["%2e%2e", "a%5cb", ".hidden", "x" * 200, "a%00b", "%2e%2e%2f%2e%2e%2fx", "..%2f..%2fx", "a%2fb", "%2Fetc%2Fpasswd"])
def test_job_artifact_names_cannot_traverse(env, name):
    jid = env.tenants["A"].ids["job_id"]
    for method in ("get", "post"):
        r = getattr(env.c, method)(f"/v1/jobs/{jid}/artifacts/{name}", headers=env.headers("a_contrib"), **({"content": b"x"} if method == "post" else {}))
        assert r.status_code in CLIENT_ERRORS, (method, name, r.status_code, r.text[:200])
        assert "root:x:0:0" not in r.text
    assert not list(env.ctx.settings.path("jobs").rglob("passwd"))


def test_job_output_name_cannot_traverse_and_artifacts_stay_in_their_job(env):
    a, b = env.tenants["A"].ids["job_id"], env.tenants["B"].ids["job_id"]
    h = env.headers("a_contrib")
    for n in ("../../x", f"../{b}/artifacts/x", "/etc/passwd", "..\\x"):
        assert env.c.get(f"/v1/jobs/{a}/output", params={"name": n}, headers=h).status_code in CLIENT_ERRORS, n
    assert env.c.get(f"/v1/jobs/{b}/artifacts", headers=h).status_code == 403
    assert env.c.get(f"/v1/jobs/{b}/output", headers=h).status_code == 403
    # a contributor may not upload artifacts (only the driver worker / job token may)
    assert env.c.post(f"/v1/jobs/{a}/artifacts/planted.txt", content=b"x", headers=h).status_code == 403


@pytest.mark.parametrize("path", ["../x", "%2e%2e/x", "a%5cb", "a/../b", "..%2f..%2fetc/passwd"])
def test_spark_ui_proxy_rejects_traversal(env, path):
    r = env.c.get(f"/v1/spark-ui/nonexistent/{path}", headers=env.headers("admin"))
    assert r.status_code in CLIENT_ERRORS, (path, r.status_code, r.text[:200])
    assert env.c.get("/v1/spark-ui/nonexistent/x").status_code == 401


@pytest.mark.parametrize("sha", ["../x", "a" * 64, "%2e%2e%2f", "A" * 64, "zz"])
def test_runtime_jar_download(env, sha):
    r = env.c.get(f"/v1/environments/jars/{sha}", headers=env.headers("admin"))
    assert r.status_code in (400, 404, 422)
    assert env.c.get(f"/v1/environments/jars/{sha}", headers=env.headers("a_admin")).status_code in (403, 404)


@pytest.mark.parametrize("path", ["/v1/runtimes/bundles/..%2f..%2fx/download", "/v1/admin/support-bundle/..%2f..%2fx/download", "/v1/admin/backups/..%2f..%2fx/download",
                                  "/v1/admin/backups/..%2f..%2fx", "/v1/admin/updates/..%2f..%2fx"])
def test_download_ids_cannot_traverse(env, path):
    r = env.c.get(path, headers=env.headers("admin"))
    assert r.status_code in (400, 404, 422), (path, r.status_code, r.text[:200])


@pytest.mark.parametrize("path", ["/ui/../../etc/passwd", "/ui/%2e%2e/%2e%2e/etc/passwd", "/ui/..%2f..%2fetc/passwd", "/../etc/passwd", "/%2e%2e/etc/passwd", "/static/../../etc/passwd",
                                  "//etc/passwd", "/%252e%252e/etc/passwd"])
def test_static_ui_cannot_serve_host_files(env, path):
    r = env.c.get(path)
    assert "root:x:0:0" not in r.text
    assert r.status_code < 500


def test_ops_inputs_cannot_traverse(env, tmp_path):
    """Backup / update / support-bundle ids and names from the request are never used as paths."""
    h = env.headers("admin")
    for name in ("../../x", "a/b", "..", "x" * 300):
        r = env.c.post("/v1/admin/backup/targets", json={"name": name, "type": "local", "config": {"path": str(tmp_path)}}, headers=h)
        assert r.status_code in CLIENT_ERRORS, (name, r.status_code, r.text[:200])
    r = env.c.post("/v1/admin/backup/targets", json={"name": "okname", "type": "local", "config": {"path": str(env.ctx.settings.path("secrets"))}}, headers=h)
    assert r.status_code in CLIENT_ERRORS, "a backup target inside the secrets directory must be refused"
    r = env.c.post("/v1/admin/backup/import", files={"file": ("../../evil.olcfbak", b"garbage")}, headers=h)
    assert r.status_code in CLIENT_ERRORS
    assert not list(env.ctx.settings.data_dir.parent.glob("evil*"))
