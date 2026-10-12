# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Authorization: the role matrix, token scopes, workspace-scoped versus organisation-wide readers and hidden existence."""

from __future__ import annotations

from types import SimpleNamespace

import pytest

from olcf.intelligence import model as M
from tests.intelligence.conftest import PASSWORD, Builder

BASE = "/v1/intelligence"

#: role -> (read, curate, sample, manage) for an organisation-level binding
MATRIX = {
    "PlatformAdmin": (True, True, True, True),
    "SecurityAdmin": (True, False, False, True),
    "WorkspaceAdmin": (True, True, True, True),
    "Contributor": (True, True, False, False),
    "Viewer": (True, False, False, False),
    "ServicePrincipal": (True, False, False, False),
    "Operator": (False, False, False, False),
    "RuntimeAdmin": (False, False, False, False),
}


@pytest.fixture()
def vis(intel):
    ws = intel.workspace("alpha")
    b = Builder(ws["id"])
    lh = b.lakehouse("itm_lh", "lh")
    t = b.table(lh, "t", [("id", "int")])
    intel.scan(M.Facts([lh, t]))
    intel.service.discovery = SimpleNamespace(sample=lambda principal, row, **kw: {"columns": [], "rows": []})      # only the permission gates /sample here
    intel.aid = intel.asset_id("t")
    intel.wsid = ws["id"]
    return intel


def probe(c, headers, aid):
    """(read, curate, sample, manage) as the server answers them: True when the call was not refused with 403/401."""
    read = c.get(f"{BASE}/summary", headers=headers).status_code
    curate = c.patch(f"{BASE}/assets/{aid}", json={"tags": ["x"]}, headers=headers).status_code
    sample = c.get(f"{BASE}/assets/{aid}/sample", headers=headers).status_code
    manage = c.post(f"{BASE}/resolve", json={"type": "table", "name": "lh.p", "scope": {"workspace_id": c.wsid}, "create": True}, headers=headers).status_code
    return tuple(code not in (401, 403) for code in (read, curate, sample, manage))


@pytest.mark.parametrize("role", sorted(MATRIX))
def test_role_matrix_org_scope(vis, role):
    if role == "PlatformAdmin":
        headers = vis.admin
    else:
        headers = vis.user(f"u_{role}", role)
    assert probe(vis, headers, vis.aid) == MATRIX[role]


@pytest.mark.parametrize("role", ["Viewer", "Contributor", "WorkspaceAdmin"])
def test_role_matrix_workspace_scope_matches_in_that_workspace_only(vis, role):
    mine = vis.user(f"m_{role}", role, workspace_id=vis.wsid)
    other = vis.user(f"o_{role}", role, workspace_id=vis.workspace("beta")["id"])
    assert probe(vis, mine, vis.aid) == MATRIX[role]
    # in another workspace the asset does not exist for the caller: always 404, never 403, for every operation that names the asset
    for call in (vis.get(f"{BASE}/assets/{vis.aid}", headers=other), vis.get(f"{BASE}/assets/{vis.aid}/lineage", headers=other),
                 vis.patch(f"{BASE}/assets/{vis.aid}", json={"tags": ["x"]}, headers=other), vis.get(f"{BASE}/assets/{vis.aid}/sample", headers=other)):
        assert call.status_code == 404
    assert vis.get(f"{BASE}/assets", headers=other).json()["items"] == []
    assert vis.get(f"{BASE}/summary", headers=other).json()["assets"]["total"] == 0


@pytest.mark.parametrize(("scopes", "expected"), [
    (["read"], (True, False, False, False)),
    (["write"], (True, True, True, True)),
    (["mcp"], (True, False, False, False)),
    (["mcp:write"], (True, False, False, False)),
    (["*"], (True, True, True, True)),
    (["job"], (False, False, False, False)),
])
def test_token_scopes_narrow_a_principal(vis, scopes, expected):
    headers = vis.user("tok_" + "_".join(s.replace(":", "") for s in scopes).replace("*", "all"), "WorkspaceAdmin", scopes=None if scopes == ["*"] else scopes)
    assert probe(vis, headers, vis.aid) == expected


def test_scoped_token_cannot_exceed_its_owners_role(vis):
    headers = vis.user("viewer_with_write", "Viewer", scopes=["write"])        # scope 'write' offers more than a Viewer holds: the intersection wins
    assert probe(vis, headers, vis.aid) == (True, False, False, False)


def test_unauthenticated_and_bad_tokens(vis):
    for path in ("/summary", "/assets", f"/assets/{vis.aid}", "/unused", "/snapshots"):
        assert vis.c.get(f"{BASE}{path}").status_code == 401
        assert vis.c.get(f"{BASE}{path}", headers={"Authorization": "Bearer nope"}).status_code == 401
    assert vis.c.post(f"{BASE}/resolve", json={"type": "table", "name": "x"}).status_code == 401


def test_org_wide_reader_sees_assets_without_a_workspace(vis):
    ext = M.AssetFact(M.AssetRef.make("source_system", {"source": "crm:main"}), "crm", workspace_id=None)
    vis.scan(M.Facts([ext]), source="ext", mode="incremental")
    eid = vis.asset_id("crm")
    scoped = vis.user("scoped", "Viewer", workspace_id=vis.wsid)
    org = vis.user("orgwide", "Viewer")
    assert vis.get(f"{BASE}/assets/{eid}", headers=scoped).status_code == 404
    assert vis.get(f"{BASE}/assets/{eid}", headers=org).status_code == 200
    assert "crm" not in {a["name"] for a in vis.get(f"{BASE}/assets", params={"limit": 100}, headers=scoped).json()["items"]}
    assert "crm" in {a["name"] for a in vis.get(f"{BASE}/assets", params={"limit": 100}, headers=org).json()["items"]}
    # sources: a scan source is visible when at least one of its assets is
    assert {s["source_id"] for s in vis.get(f"{BASE}/snapshots", headers=scoped).json()["items"]} == {"local"}
    assert {s["source_id"] for s in vis.get(f"{BASE}/snapshots", headers=org).json()["items"]} == {"local", "ext"}


def test_two_roles_in_two_workspaces_see_the_union(vis):
    beta = vis.workspace("beta")
    b2 = Builder(beta["id"])
    lh2 = b2.lakehouse("itm_lh2", "lh2")
    vis.scan(M.Facts([lh2, b2.table(lh2, "t2")]), source="other", mode="incremental")
    ident = vis.ctx.service("identity")
    row = ident.create_principal("both", kind="user", password=PASSWORD)
    ident.bind(row["id"], "Viewer", scope_type="workspace", scope_id=vis.wsid)
    ident.bind(row["id"], "Viewer", scope_type="workspace", scope_id=beta["id"])
    r = vis.c.post("/v1/identity/login", json={"username": "both", "password": PASSWORD})
    h = {"Authorization": f"Bearer {r.json()['access_token']}"}
    names = {a["name"] for a in vis.get(f"{BASE}/assets", params={"type": "table", "limit": 100}, headers=h).json()["items"]}
    assert names == {"t", "t2"}


def test_edges_and_diffs_do_not_leak_across_workspaces(vis):
    beta = vis.workspace("beta")
    b2 = Builder(beta["id"])
    lh2 = b2.lakehouse("itm_lh2", "lh2")
    nb = b2.notebook("itm_nb", "reader")
    t = M.AssetRef.make("table", {"lakehouse_table": "itm_lh:t"})
    s1 = vis.scan(M.Facts([lh2, nb], [b2.edge(nb, t)]), source="beta_src", mode="incremental")
    s2 = vis.scan(M.Facts([b2.table(lh2, "added")]), source="beta_src", mode="incremental")
    alpha_only = vis.user("only_alpha", "Viewer", workspace_id=vis.wsid)
    cross = next(e for e in vis.get(f"{BASE}/assets/{vis.aid}/lineage", params={"depth": 2}, headers=vis.admin).json()["edges"])
    assert vis.get(f"{BASE}/edges/{cross['id']}", headers=vis.admin).status_code == 200
    assert vis.get(f"{BASE}/edges/{cross['id']}", headers=alpha_only).status_code == 404
    assert vis.get(f"{BASE}/snapshots/diff", params={"from": s1, "to": s2}, headers=alpha_only).status_code == 404       # a source it cannot see
    assert vis.get(f"{BASE}/snapshots/diff", params={"from": s1, "to": s2}, headers=vis.admin).status_code == 200
    lineage = vis.get(f"{BASE}/assets/{vis.aid}/lineage", params={"depth": 3}, headers=alpha_only).json()
    assert "reader" not in {n["name"] for n in lineage["nodes"]} and lineage["edges"] == []
    impact = vis.get(f"{BASE}/assets/{vis.aid}/impact", headers=alpha_only).json()
    assert impact["total"] == 0 and impact["processes"] == []
