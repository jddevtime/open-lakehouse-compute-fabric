# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Git sync on a machine without a usable git (e.g. a Mac with only the /usr/bin installer shim): clean 503 + capabilities flag."""

from __future__ import annotations

import stat
from pathlib import Path

import pytest

from olcf.core import hostbin
from olcf.notebooks import gitrunner


@pytest.fixture()
def no_git(tmp_path, monkeypatch):
    marker = tmp_path / "STUB-WAS-EXECUTED"
    usr_bin = tmp_path / "usr" / "bin"
    usr_bin.mkdir(parents=True)
    stub = usr_bin / "git"
    stub.write_text(f"#!/bin/sh\ntouch '{marker}'\nexit 99\n")
    stub.chmod(stub.stat().st_mode | stat.S_IXUSR)
    xs = tmp_path / "xcode-select"
    xs.write_text("#!/bin/sh\nexit 2\n")
    xs.chmod(xs.stat().st_mode | stat.S_IXUSR)
    monkeypatch.setattr(hostbin, "is_macos", lambda: True)
    monkeypatch.setattr(hostbin, "STUB_DIRS", (str(usr_bin),))
    monkeypatch.setattr(hostbin, "XCODE_SELECT", str(xs))
    monkeypatch.setenv("PATH", str(usr_bin))
    hostbin.clear_cache()
    yield Path(marker)
    hostbin.clear_cache()


def test_capabilities_flag_and_clean_503(ic, no_git):
    ws = ic.workspace("gitws")
    r = ic.get(f"/v1/workspaces/{ws['id']}/git/capabilities", headers=ic.admin)
    assert r.status_code == 200, r.text
    caps = r.json()
    assert caps["available"] is False and caps["path"] is None
    assert "git" in caps["remediation"] and caps["workspace_id"] == ws["id"] and caps["connected"] is False

    for method, url, body in (("post", f"/v1/workspaces/{ws['id']}/git/connect", {"repo_url": "https://example.invalid/r.git", "branch": "main"}),
                              ("get", f"/v1/workspaces/{ws['id']}/git/status", None),
                              ("get", f"/v1/workspaces/{ws['id']}/git/branches", None),
                              ("post", f"/v1/workspaces/{ws['id']}/git/pull", None)):
        r = getattr(ic, method)(url, headers=ic.admin, **({"json": body} if body is not None else {}))
        assert r.status_code == 503, (url, r.text)
        err = r.json()["error"]
        assert err["code"] == "git_unavailable" and err["remediation"] and "brew install git" in err["remediation"]
    # leaving Git sync never needs git
    assert ic.post(f"/v1/workspaces/{ws['id']}/git/disconnect", headers=ic.admin).status_code == 200
    assert not no_git.exists()


def test_capabilities_with_git_and_authorisation(ic):
    ws = ic.workspace("gitws2")
    caps = ic.get(f"/v1/workspaces/{ws['id']}/git/capabilities", headers=ic.admin).json()
    assert caps["available"] == (gitrunner.git_binary() is not None)
    outsider = ic.user("nobody", "Viewer", workspace_id="wks_does_not_exist_000000")
    assert ic.get(f"/v1/workspaces/{ws['id']}/git/capabilities", headers=outsider).status_code in (403, 404)
