# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Routes and service calls that write or depend on runtime state: PATCH, merged-asset redirects, sample, resolve+create, unused and staleness against real data."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any

import pytest
from sqlalchemy import text

from olcf.core.errors import BadRequest, Forbidden, NotFound
from olcf.intelligence import model as M
from olcf.security.rbac import Binding, Principal
from tests.intelligence.conftest import Builder

BASE = "/v1/intelligence"


def iso(delta: timedelta) -> str:
    return (datetime.now(UTC) + delta).strftime("%Y-%m-%dT%H:%M:%S.%fZ")


@pytest.fixture()
def alpha(intel):
    """Workspace + a small graph (lakehouse, two tables, a notebook that moves data) and the callers."""
    ws = intel.workspace("alpha")
    b = Builder(ws["id"])
    lh = b.lakehouse("itm_lh", "lh")
    src, dst = b.table(lh, "src", [("id", "int")]), b.table(lh, "dst", [("id", "int")])
    nb = b.notebook("itm_nb", "job_nb")
    sid = intel.scan(M.Facts([lh, src, dst, nb], [b.edge(nb, src), b.edge(nb, dst, "WRITES_TO", "verified", kind="runtime", source="runtime:job", ref="run_1", location={})]))
    intel.ws, intel.b, intel.scan_id = ws, b, sid
    intel.ids = {n: intel.asset_id(n) for n in ("src", "dst", "job_nb", "lh")}
    intel.h = {"viewer": intel.user("viewer", "Viewer", workspace_id=ws["id"]), "curator": intel.user("curator", "Contributor", workspace_id=ws["id"]),
               "wsadmin": intel.user("wsadmin", "WorkspaceAdmin", workspace_id=ws["id"]), "agent": intel.user("agent", "Contributor", workspace_id=ws["id"], scopes=["mcp"]),
               "other": intel.user("other", "Contributor", workspace_id=intel.workspace("beta")["id"])}
    return intel


def patch(c, aid, body, who="admin", **headers):
    h = dict(c.admin if who == "admin" else c.h[who])
    h.update(headers)
    return c.patch(f"{BASE}/assets/{aid}", json=body, headers=h)


def test_patch_description_and_tags(alpha):
    c, aid = alpha, alpha.ids["dst"]
    before = c.get(f"{BASE}/assets/{aid}", headers=c.admin)
    etag = before.headers["ETag"]
    r = patch(c, aid, {"description": "Cleaned orders", "tags": ["PII ", "finance", "pii", ""]}, "curator", **{"If-Match": etag})
    j = r.json()
    assert r.status_code == 200 and j["description"] == "Cleaned orders" and j["description_source"] == "manual" and j["tags"] == ["finance", "pii"]
    assert j["etag"] != before.json()["etag"] and r.headers["ETag"].strip('"') == j["etag"]
    # tag filter finds it, name search does not need it
    assert [a["name"] for a in c.get(f"{BASE}/assets", params={"tag": "PII"}, headers=c.admin).json()["items"]] == ["dst"]
    assert [a["name"] for a in c.get(f"{BASE}/assets", params={"q": "cleaned"}, headers=c.admin).json()["items"]] == ["dst"]
    # the audit log has the update
    rows = c.ctx.audit.query(action="intelligence.asset_updated")
    assert rows and rows[0]["resource_id"] == aid and rows[0]["actor_name"] == "curator" and rows[0]["details"]["fields"] == ["description", "tags"]


def test_patch_optimistic_concurrency(alpha):
    c, aid = alpha, alpha.ids["dst"]
    etag = c.get(f"{BASE}/assets/{aid}", headers=c.admin).headers["ETag"]
    assert patch(c, aid, {"tags": ["a"]}, "curator", **{"If-Match": etag}).status_code == 200
    stale = patch(c, aid, {"tags": ["b"]}, "curator", **{"If-Match": etag})
    assert stale.status_code == 412 and stale.json()["error"]["code"] == "precondition_failed" and "current_etag" in stale.json()["error"]["details"]
    assert patch(c, aid, {"tags": ["c"]}, "curator", **{"If-Match": "*"}).status_code == 200
    assert patch(c, aid, {"tags": ["d"]}, "curator").status_code == 200                                        # no If-Match: last writer wins


def test_manual_description_survives_a_scan_and_can_be_handed_back(alpha):
    c, aid, b = alpha, alpha.ids["dst"], alpha.b
    patch(c, aid, {"description": "mine"}, "curator")
    lh = b.lakehouse("itm_lh", "lh")
    t = b.table(lh, "dst", [("id", "int")])
    t.description = "from the scan"
    c.scan(M.Facts([lh, t]), mode="incremental")
    assert c.get(f"{BASE}/assets/{aid}", headers=c.admin).json()["description"] == "mine"
    j = patch(c, aid, {"description": "   "}, "curator").json()
    assert j["description"] == "" and j["description_source"] == "scan"
    c.scan(M.Facts([lh, t]), mode="incremental")
    assert c.get(f"{BASE}/assets/{aid}", headers=c.admin).json()["description"] == "from the scan"


def test_patch_errors_and_permissions(alpha):
    c, aid = alpha, alpha.ids["dst"]
    assert patch(c, aid, {}, "curator").status_code == 400
    assert patch(c, aid, {"description": "x" * 8001}, "curator").status_code == 400 or patch(c, aid, {"description": "x" * 8001}, "curator").status_code == 422
    assert patch(c, aid, {"tags": [f"t{i}" for i in range(40)]}, "curator").status_code == 400
    assert patch(c, aid, {"tags": ["x" * 65]}, "curator").status_code == 400
    assert patch(c, aid, {"tags": "nope"}, "curator").status_code == 422
    assert patch(c, aid, {"owner": "me"}, "curator").status_code == 422                                          # unknown field
    assert patch(c, aid, {"tags": ["a"]}, "viewer").status_code == 403                                            # read-only role
    assert patch(c, aid, {"tags": ["a"]}, "agent").status_code == 403                                             # token scope 'mcp' does not include governance.curate
    assert c.get(f"{BASE}/assets/{aid}", headers=c.h["agent"]).status_code == 200                                 # ... but reads
    assert patch(c, aid, {"tags": ["a"]}, "other").status_code == 404                                             # another workspace: existence hidden
    assert patch(c, "ast_nope", {"tags": ["a"]}).status_code == 404
    assert c.patch(f"{BASE}/assets/{aid}", json={"tags": ["a"]}).status_code == 401


def test_merged_asset_ids_redirect_to_the_survivor(alpha):
    c = alpha
    reg = c.service.registry
    ph = reg.resolve(M.AssetRef.make("table", None, "lh.late", {"workspace_id": alpha.ws["id"]}))
    real = c.b.table(c.b.lakehouse("itm_lh", "lh"), "late", [("z", "int")])
    c.scan(M.Facts([c.b.lakehouse("itm_lh", "lh"), real]), mode="incremental")
    survivor = c.asset_id("late")
    assert survivor == ph                                              # promoted in place
    other = reg.ensure(M.AssetRef.make("table", {"lakehouse_table": "itm_lh:late_copy"}), name="late_copy", workspace_id=alpha.ws["id"]).asset_id
    reg.merge(survivor, other)
    r = c.get(f"{BASE}/assets/{other}", headers=c.admin)
    assert r.status_code == 200 and r.json()["id"] == survivor
    assert c.get(f"{BASE}/assets/{other}/lineage", headers=c.admin).json()["root"]["id"] == survivor
    listed = {a["id"] for a in c.get(f"{BASE}/assets", params={"limit": 200}, headers=c.admin).json()["items"]}
    assert other not in listed and survivor in listed
    assert other in {a["id"] for a in c.get(f"{BASE}/assets", params={"limit": 200, "status": "removed"}, headers=c.admin).json()["items"]}


# ------------------------------------------------------------------------------------------------------------------------------ sample
class FakeDiscovery:
    def __init__(self) -> None:
        self.calls: list[tuple[Any, ...]] = []
        self.fail: Exception | None = None

    def sample(self, principal, row, *, rows=None, columns=None):
        self.calls.append((principal.name, row["name"], rows, columns))
        if self.fail:
            raise self.fail
        return {"columns": ["id"], "rows": [[1], [2]], "masking": {"id": "none"}}


def test_sample_needs_discovery_and_permission(alpha):
    c, aid = alpha, alpha.ids["src"]
    get = lambda who: c.get(f"{BASE}/assets/{aid}/sample", headers=c.admin if who == "admin" else c.h[who])  # noqa: E731
    c.service.discovery = None                              # a build without the discovery package
    r = get("wsadmin")
    assert r.status_code == 422 and r.json()["error"]["code"] == "unsupported" and "discovery" in r.json()["error"]["message"]
    fake = FakeDiscovery()
    c.service.discovery = fake
    assert get("viewer").status_code == 403 and get("curator").status_code == 403 and get("agent").status_code == 403           # sampling is its own permission
    assert get("other").status_code == 404
    ok = c.get(f"{BASE}/assets/{aid}/sample", params={"rows": 5, "columns": ["id"]}, headers=c.h["wsadmin"])
    assert ok.status_code == 200 and ok.json()["rows"] == [[1], [2]]
    assert fake.calls == [("wsadmin", "src", 5, ["id"])]
    audit = c.ctx.audit.query(action="intelligence.sample_read")
    assert audit[0]["outcome"] == "success" and audit[0]["details"] == {"rows": 2, "columns": 1, "masking": {"id": "none"}} and "1" not in str(audit[0]["details"]["rows"]) + "x"
    assert c.get(f"{BASE}/assets/{aid}/sample", params={"rows": 0}, headers=c.h["wsadmin"]).status_code == 422
    assert c.get(f"{BASE}/assets/{aid}/sample", params={"rows": 101}, headers=c.h["wsadmin"]).status_code == 422
    assert c.get(f"{BASE}/assets/{aid}/sample", headers=c.admin).status_code == 200                                              # platform admin


def test_sample_denial_is_audited(alpha):
    c, aid = alpha, alpha.ids["src"]
    fake = FakeDiscovery()
    fake.fail = Forbidden("sampling is disabled for this workspace")
    c.service.discovery = fake
    r = c.get(f"{BASE}/assets/{aid}/sample", headers=c.h["wsadmin"])
    assert r.status_code == 403
    audit = c.ctx.audit.query(action="intelligence.sample_read")
    assert audit[0]["outcome"] == "denied" and audit[0]["details"]["reason"] == "forbidden"


# ------------------------------------------------------------------------------------------------------------------------------ resolve + create
def test_resolve_create_makes_a_placeholder_for_those_who_manage(alpha):
    c = alpha
    body = {"type": "table", "name": "lh.planned", "scope": {"workspace_id": alpha.ws["id"]}, "create": True}
    deny = c.post(f"{BASE}/resolve", json=body, headers=c.h["curator"])
    assert deny.status_code == 403
    r = c.post(f"{BASE}/resolve", json=body, headers=c.h["wsadmin"])
    j = r.json()
    assert r.status_code == 200 and j["created"] is True and j["placeholder"] is True and j["asset"]["status"] == "placeholder" and j["asset"]["attrs"]["unresolved"] is True
    again = c.post(f"{BASE}/resolve", json=body, headers=c.h["wsadmin"]).json()
    assert again["created"] is False and again["asset"]["id"] == j["asset"]["id"]
    found = c.post(f"{BASE}/resolve", json={k: v for k, v in body.items() if k != "create"}, headers=c.h["viewer"])
    assert found.status_code == 200 and found.json()["placeholder"] is True
    # a manager of another workspace cannot create one in this workspace
    assert c.post(f"{BASE}/resolve", json=body, headers=c.h["other"]).status_code == 403
    # placeholders are listed when asked for
    ph = c.get(f"{BASE}/assets", params={"status": "placeholder"}, headers=c.admin).json()["items"]
    assert [a["id"] for a in ph] == [j["asset"]["id"]]
    assert c.get(f"{BASE}/summary", headers=c.admin).json()["placeholders"] == 1


# ------------------------------------------------------------------------------------------------------------------------------ unused
def test_unused_logic_with_real_ages(alpha):
    c = alpha
    old = iso(-timedelta(days=60))
    recent = iso(-timedelta(days=2))
    with c.ctx.db.begin() as conn:                      # everything was created 60 days ago ...
        conn.execute(text("update assets set created_at=:t"), {"t": old})
    # ... src is read by job_nb (an old edge), dst has no reader at all
    with c.ctx.db.begin() as conn:
        conn.execute(text("update edges set last_seen_at=:t where type='READS_FROM'"), {"t": old})
    r = c.get(f"{BASE}/unused", params={"days": 30}, headers=c.admin).json()
    assert {i["name"] for i in r["items"]} == {"src", "dst"}
    src = next(i for i in r["items"] if i["name"] == "src")
    assert src["last_consumer_seen_at"] == old and src["consumer_edges_ever"] == 1
    with c.ctx.db.begin() as conn:                      # a read seen 2 days ago makes src used
        conn.execute(text("update edges set last_seen_at=:t where type='READS_FROM'"), {"t": recent})
    assert {i["name"] for i in c.get(f"{BASE}/unused", params={"days": 30}, headers=c.admin).json()["items"]} == {"dst"}
    assert {i["name"] for i in c.get(f"{BASE}/unused", params={"days": 1}, headers=c.admin).json()["items"]} == {"src", "dst"}
    # an asset younger than the window is never reported
    with c.ctx.db.begin() as conn:
        conn.execute(text("update assets set created_at=:t where name='dst'"), {"t": recent})
    assert c.get(f"{BASE}/unused", params={"days": 30}, headers=c.admin).json()["items"] == []
    # the default window comes from the runtime setting
    assert c.get(f"{BASE}/unused", headers=c.admin).json()["days"] == 30
    c.ctx.service("settings").set("intelligence.default_unused_days", 7)
    assert c.get(f"{BASE}/unused", headers=c.admin).json()["days"] == 7


# ------------------------------------------------------------------------------------------------------------------------------ staleness
def test_staleness_from_runtime_evidence_and_run_outcomes(alpha):
    c = alpha
    dst = alpha.ids["dst"]
    # no failed run: the write was just observed
    j = c.get(f"{BASE}/assets/{dst}/staleness", headers=c.admin).json()
    assert j["stale"] is False and "looks fresh" in j["explanation"]
    # the write is old: stale by age, and the setting-less override can relax the threshold
    with c.ctx.db.begin() as conn:
        conn.execute(text("update edges set last_verified_at=:t where type='WRITES_TO'"), {"t": iso(-timedelta(hours=30))})
    j = c.get(f"{BASE}/assets/{dst}/staleness", headers=c.admin).json()
    assert j["stale"] is True and "threshold 24 h" in j["nodes"][0]["reasons"][0] or any("threshold" in r for n in j["nodes"] for r in n["reasons"])
    assert c.get(f"{BASE}/assets/{dst}/staleness", params={"stale_after_hours": 48}, headers=c.admin).json()["stale"] is False
    # the run that wrote it later failed: stale with the failing run named
    c.ctx.events.publish("job.state_changed", {"from": "running", "to": "failed"}, subject="run_1")
    j = c.get(f"{BASE}/assets/{dst}/staleness", params={"stale_after_hours": 48}, headers=c.admin).json()
    me = next(n for n in j["nodes"] if n["asset"]["id"] == dst)
    assert j["stale"] is True and me["last_run"]["ref"] == "run_1" and me["last_run"]["state"] == "failed" and any("run_1 ended failed" in r for r in me["reasons"])
    assert j["root_causes"] == [dst] and "dst" in j["explanation"]


def test_staleness_names_the_upstream_root_cause(alpha):
    c, b = alpha, alpha.b
    lh = b.lakehouse("itm_lh", "lh")
    mid = b.table(lh, "mid")
    nb2 = b.notebook("itm_nb2", "nb2")
    dst_ref = M.AssetRef.make("table", {"lakehouse_table": "itm_lh:dst"})
    c.scan(M.Facts([lh, mid, nb2], [b.edge(nb2, dst_ref, "READS_FROM"), b.edge(nb2, mid, "WRITES_TO")]), mode="incremental")
    # dst's last run failed; mid has a producer but no observed write
    c.ctx.events.publish("job.state_changed", {"to": "failed"}, subject="run_1")
    with c.ctx.db.begin() as conn:
        conn.execute(text("update edges set last_verified_at=:t where type='WRITES_TO' and last_verified_at is not null"), {"t": iso(-timedelta(hours=1))})
    j = c.get(f"{BASE}/assets/{c.asset_id('mid')}/staleness", headers=c.admin).json()
    by = {n["asset"]["name"]: n for n in j["nodes"]}
    assert j["stale"] is True and by["mid"]["stale"] is True and by["mid"]["root_cause"] is False
    assert by["dst"]["stale"] is True and by["dst"]["root_cause"] is True and j["root_causes"] == [c.ids["dst"]]
    assert "'mid' is stale" in j["explanation"] and "most upstream stale producer is 'dst'" in j["explanation"]
    assert by["src"]["is_source"] is True and by["src"]["stale"] is False


# ------------------------------------------------------------------------------------------------------------------------------ service level
def _who(name: str, *bindings: Binding) -> Principal:
    return Principal(id=f"usr_{name}", name=name, bindings=list(bindings))


def test_service_visibility_filters(alpha):
    c = alpha
    svc, ws = c.service, alpha.ws["id"]
    c.engine.record_gaps(c.scan_id, [M.Gap("parse_error", "x", "job_nb", "bad cell", None, {"cell": 2})], "local", asset_id=c.ids["job_nb"])
    viewer, other, nobody = _who("v", Binding("Viewer", "workspace", ws)), _who("o", Binding("Viewer", "workspace", "wks_other")), _who("n")
    assert [g["kind"] for g in svc.gaps(viewer)] == ["parse_error"] and svc.gaps(other) == []
    with pytest.raises(Forbidden):
        svc.gaps(nobody)
    assert svc.visible_sources(viewer) == {"local"} and svc.visible_sources(other) == set()
    assert svc.visible_asset_ids(viewer, [c.ids["src"], "ast_nope"]) == {c.ids["src"]} and svc.visible_asset_ids(other, [c.ids["src"]]) == set()
    assert svc.can_see(viewer, ws) and not svc.can_see(viewer, None) and not svc.can_see(other, ws)
    assert svc.can_see(_who("org", Binding("Viewer")), None)
    # ranked search: exact name beats prefix beats substring; alias hit counts
    ranked = svc.search(viewer, q="src")
    assert ranked[0]["name"] == "src" and ranked[0]["score"] >= 100
    assert svc.search(viewer, q="itm_lh:src")[0]["name"] == "src"
    with pytest.raises(NotFound):
        svc.get_asset(other, c.ids["src"])
    with pytest.raises(BadRequest):
        svc.search(viewer, q="   ")
    assert svc.inbox_stats()["pending"] >= 0 and svc.resolution_of(svc.registry.resolve_ex(M.AssetRef.make("table", {"lakehouse_table": "itm_lh:src"})))["via"] == "alias"
    assert svc.resolve(M.AssetRef.make("table", {"lakehouse_table": "itm_lh:src"})) == c.ids["src"]
