# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Every Intelligence REST route: the happy path and its error paths (read-only tests share the module-scoped ``world``)."""

from __future__ import annotations

from olcf.intelligence import model as M

BASE = "/v1/intelligence"


def code(r) -> str:
    return r.json()["error"]["code"]


# ------------------------------------------------------------------------------------------------------------------------------ summary
def test_summary(world):
    r = world.get("/summary")
    assert r.status_code == 200
    j = r.json()
    assert j["assets"]["by_type"]["table"] == 7 and j["assets"]["by_type"]["notebook"] == 3 and j["edges"]["total"] == 12
    assert j["edges"]["by_confidence"]["verified"] == 1 and j["gaps"]["by_kind"] == {"dynamic_reference": 1}
    assert j["organisation_wide"] is True and "inbox" in j and isinstance(j["discovery"], bool) and isinstance(j["governance"], bool)
    assert j["last_scans"][0]["source_id"] == "local" and j["last_scans"][0]["version"] == 2


def test_summary_is_scoped_to_the_callers_workspaces(world):
    j = world.get("/summary", "alpha").json()
    assert j["organisation_wide"] is False and "inbox" not in j
    assert j["assets"]["by_type"]["notebook"] == 2 and "source_system" not in j["assets"]["by_type"]       # beta's notebook and the org-level source are hidden
    assert j["edges"]["total"] < 12 and j["gaps"]["open"] == 1
    assert world.get("/summary", "org").json()["assets"]["total"] == world.get("/summary").json()["assets"]["total"]


def test_no_permission_at_all_is_forbidden(world):
    for path in ("/summary", "/assets", "/unused", "/snapshots", f"/assets/{world.ids['clean']}", f"/assets/{world.ids['clean']}/lineage"):
        r = world.get(path, "none")
        assert r.status_code == 403 and code(r) == "forbidden", path


def test_unauthenticated_is_401(world):
    assert world.c.get(f"{BASE}/summary").status_code == 401


# ------------------------------------------------------------------------------------------------------------------------------ assets
def test_list_assets_filters_and_paging(world):
    r = world.get("/assets", limit=500)
    j = r.json()
    assert r.status_code == 200 and len(j["items"]) == 19 and j["next_cursor"] is None
    seen, cursor = [], None
    while True:
        p = world.get("/assets", limit=5, **({"cursor": cursor} if cursor else {})).json()
        seen += [a["id"] for a in p["items"]]
        cursor = p["next_cursor"]
        if not cursor:
            break
    assert len(seen) == 19 and len(set(seen)) == 19
    assert {a["name"] for a in world.get("/assets", type="notebook").json()["items"]} == {"etl", "agg", "beta_nb"}
    assert {a["name"] for a in world.get("/assets", type="notebook,report").json()["items"]} == {"etl", "agg", "beta_nb", "sales_report"}
    assert [a["name"] for a in world.get("/assets", q="SALES_report").json()["items"]] == ["sales_report"]
    assert {a["name"] for a in world.get("/assets", q="sales").json()["items"]} == {"sales_model", "sales_report"}
    assert {a["name"] for a in world.get("/assets", q="itm_lh1:clean").json()["items"]} == {"clean", "id", "amount"}    # matches an alias (also the columns'')
    assert [a["name"] for a in world.get("/assets", q="lakehouse_a/mart").json()["items"]] == ["mart"]    # matches the path
    assert world.get("/assets", q="no-such-thing").json()["items"] == []
    assert {a["name"] for a in world.get("/assets", workspace_id=world.ws2["id"], type="table").json()["items"]} == {"secret", "secret_out"}


def test_list_assets_errors(world):
    assert world.get("/assets", type="spaceship").status_code == 400
    assert world.get("/assets", status="nope").status_code == 422
    assert world.get("/assets", limit=0).status_code == 422
    r = world.get("/assets", domain="finance")           # served by the governance package; an unknown domain matches nothing
    assert r.status_code == 200 and r.json()["items"] == []
    assert world.get("/assets", certification="nope").status_code == 400


def test_list_assets_is_visibility_filtered(world):
    names = {a["name"] for a in world.get("/assets", "alpha", limit=500).json()["items"]}
    assert "secret" not in names and "beta_nb" not in names and "crm_system" not in names and "clean" in names
    assert world.get("/assets", "alpha", workspace_id=world.ws2["id"]).json()["items"] == []          # a foreign workspace filter yields nothing, not an error
    assert {a["name"] for a in world.get("/assets", "beta", limit=500).json()["items"]} >= {"secret", "beta_nb"}
    assert "crm_system" in {a["name"] for a in world.get("/assets", "org", limit=500).json()["items"]}


def test_get_asset(world):
    r = world.get(f"/assets/{world.ids['clean']}")
    j = r.json()
    assert r.status_code == 200 and r.headers["ETag"].strip('"') == j["etag"]
    assert j["name"] == "clean" and j["type"] == "table" and j["placeholder"] is False and j["child_count"] == 2
    assert {"kind": "lakehouse_table", "value": "itm_lh1:clean"} in j["aliases"]
    assert j["parent"]["name"] == "lakehouse_a" and j["lineage_edges"] == {"upstream": 1, "downstream": 2}
    assert j["attrs"]["columns"][0]["name"] == "id" and j["open_gaps"] == 0
    assert world.get(f"/assets/{world.ids['etl']}").json()["open_gaps"] == 1


def test_get_asset_errors_and_hidden_existence(world):
    missing = world.get("/assets/ast_00000000000000000000000000")
    hidden = world.get(f"/assets/{world.ids['secret']}", "alpha")
    assert missing.status_code == 404 and hidden.status_code == 404 and code(hidden) == "not_found"
    assert missing.json()["error"]["message"].replace("ast_00000000000000000000000000", "X") == hidden.json()["error"]["message"].replace(world.ids["secret"], "X")
    assert world.get(f"/assets/{world.ids['crm_system']}", "alpha").status_code == 404           # organisation-level asset needs an org-wide reader
    assert world.get(f"/assets/{world.ids['crm_system']}", "org").status_code == 200
    assert world.get(f"/assets/{world.ids['secret']}", "beta").status_code == 200


def test_children(world):
    j = world.get(f"/assets/{world.ids['clean']}/children").json()
    assert [c["name"] for c in j["items"]] == ["amount", "id"] and all(c["type"] == "column" for c in j["items"])
    page = world.get(f"/assets/{world.ids['lakehouse_a']}/children", limit=2).json()
    assert len(page["items"]) == 2 and page["next_cursor"]
    rest = world.get(f"/assets/{world.ids['lakehouse_a']}/children", limit=50, cursor=page["next_cursor"]).json()
    assert {c["name"] for c in page["items"] + rest["items"]} == {"raw", "clean", "mart", "lonely", "fresh"}
    assert world.get(f"/assets/{world.ids['lakehouse_a']}/children", type="column").json()["items"] == []
    assert world.get(f"/assets/{world.ids['lakehouse_a']}/children", "beta").status_code == 404


# ------------------------------------------------------------------------------------------------------------------------------ lineage
def _names(j):
    return {n["name"] for n in j["nodes"]}


def test_lineage_both_directions_with_processes(world):
    j = world.get(f"/assets/{world.ids['clean']}/lineage", depth=3).json()
    assert j["root"]["name"] == "clean" and j["level"] == "table" and j["truncated"] is False
    assert {"raw", "etl", "agg", "mart", "sales_model", "sales_report", "crm_system"} <= _names(j)
    dirs = {n["name"]: n["direction"] for n in j["nodes"]}
    assert dirs["raw"] == "upstream" and dirs["mart"] == "downstream" and dirs["clean"] == "root"
    levels = {n["name"]: n["level"] for n in j["nodes"]}
    assert levels["etl"] == 0 and levels["agg"] == 0 and levels["raw"] == 1 and levels["mart"] == 1 and levels["sales_model"] == 2     # processes are free
    assert {e["type"] for e in j["edges"]} >= {"READS_FROM", "WRITES_TO", "DEPENDS_ON"}
    assert [g["kind"] for g in j["gaps"]] == ["dynamic_reference"] and j["gaps"][0]["subject"] == "etl"
    floors = {n["name"]: n["confidence_floor"] for n in j["nodes"]}
    assert floors["raw"] == "derived" and floors["sales_report"] == "inferred"


def test_lineage_depth_direction_and_confidence(world):
    cid = world.ids["clean"]
    shallow = world.get(f"/assets/{cid}/lineage", direction="down", depth=1).json()
    assert "mart" in _names(shallow) and "sales_model" not in _names(shallow) and "raw" not in _names(shallow)
    up = world.get(f"/assets/{cid}/lineage", direction="up").json()
    assert {"raw", "etl", "crm_system"} <= _names(up) and "mart" not in _names(up)
    strong = world.get(f"/assets/{cid}/lineage", min_confidence="derived").json()
    assert "crm_system" not in _names(strong) and "sales_report" not in _names(strong) and "raw" in _names(strong)
    assert world.get(f"/assets/{cid}/lineage", min_confidence="verified", direction="up").json()["nodes"][0]["name"] in ("clean", "etl")


def test_lineage_types_filter_bridges_hidden_nodes(world):
    j = world.get(f"/assets/{world.ids['raw']}/lineage", direction="down", depth=5, types=["table", "report"]).json()
    assert _names(j) >= {"raw", "clean", "mart", "sales_report"} and not ({"etl", "agg", "sales_model"} & _names(j))
    bridged = [e for e in j["edges"] if e.get("virtual")]
    assert bridged and all(e["type"] == "FLOWS_TO" for e in bridged)
    j2 = world.get(f"/assets/{world.ids['raw']}/lineage", direction="down", types="table,report").json()       # comma separated works too
    assert _names(j2) == _names(world.get(f"/assets/{world.ids['raw']}/lineage", direction="down", types=["table", "report"]).json())
    assert world.get(f"/assets/{world.ids['raw']}/lineage", types="spaceship").status_code == 400


def test_lineage_column_level(world):
    j = world.get(f"/assets/{world.ids['clean']}/lineage", level="column", direction="up").json()
    assert j["level"] == "column" and {n["name"] for n in j["nodes"] if n["type"] == "column"} == {"id", "amount"}
    one = world.get(f"/assets/{world.ids['clean']}/lineage", column="amount", direction="up").json()
    assert one["level"] == "column" and [e["transformation"] for e in one["edges"]] == ["amount * 1.0"] and any(n["name"] == "etl" and n.get("via_only") for n in one["nodes"])
    direct = world.get(f"/assets/{world.ids['clean.amount']}/lineage", direction="up", level="column").json()
    assert direct["root"]["name"] == "amount" and any(e["transformation"] == "amount * 1.0" for e in direct["edges"])
    as_table = world.get(f"/assets/{world.ids['clean.amount']}/lineage", direction="up").json()               # table level of a column is its table's lineage
    assert as_table["level"] == "table" and "raw" in _names(as_table)
    assert world.get(f"/assets/{world.ids['clean']}/lineage", column="nope").status_code in (400, 404)


def test_lineage_errors(world):
    cid = world.ids["clean"]
    for params in ({"direction": "sideways"}, {"depth": 0}, {"depth": 11}, {"level": "row"}, {"min_confidence": "maybe"}):
        assert world.get(f"/assets/{cid}/lineage", **params).status_code == 422, params
    assert world.get("/assets/ast_nope/lineage").status_code == 404


def test_lineage_never_shows_hidden_workspaces(world):
    j = world.get(f"/assets/{world.ids['clean']}/lineage", "alpha", depth=5).json()
    assert not ({"beta_nb", "secret", "secret_out", "crm_system"} & _names(j))
    assert all(e["src"] in {n["id"] for n in j["nodes"]} and e["dst"] in {n["id"] for n in j["nodes"]} for e in j["edges"])
    b = world.get(f"/assets/{world.ids['clean']}/lineage", "beta", depth=5)
    assert b.status_code == 404                                                      # clean itself belongs to alpha
    full = world.get(f"/assets/{world.ids['secret']}/lineage", "beta", depth=5).json()
    assert "beta_nb" in _names(full) and "clean" not in _names(full)               # beta's view of a path through alpha stops at the boundary
    assert "beta_nb" in _names(world.get(f"/assets/{world.ids['clean']}/lineage", "org", depth=5).json())


def test_lineage_removed_edges_need_include_removed(world):
    # (the module-scoped world is never swept; this checks the parameter is accepted and idempotent)
    a = world.get(f"/assets/{world.ids['clean']}/lineage", include_removed=True).json()
    b = world.get(f"/assets/{world.ids['clean']}/lineage").json()
    assert _names(a) == _names(b)


# ------------------------------------------------------------------------------------------------------------------------------ origin, impact, staleness
def test_origin(world):
    j = world.get(f"/assets/{world.ids['mart']}/origin").json()
    assert {r["asset"]["name"] for r in j["roots"]} == {"crm_system", "raw"}
    src = {r["asset"]["name"]: r for r in j["roots"]}
    assert src["crm_system"]["is_source"] is True and src["crm_system"]["best_confidence"] == "inferred" and src["raw"]["best_confidence"] == "derived"
    for p in j["paths"]:
        assert p["nodes"][-1] == world.ids["mart"] and p["source"] == p["nodes"][0] and p["confidence"] in M.CONFIDENCE_LEVELS and p["length"] >= 1
    by = {p["source"]: p for p in j["paths"]}
    assert by[world.ids["raw"]]["processes"] and len(by[world.ids["raw"]]["edges"]) == 4 and by[world.ids["raw"]]["length"] == 2
    assert [r["asset"]["name"] for r in world.get(f"/assets/{world.ids['mart']}/origin", min_confidence="derived").json()["roots"]] == ["raw"]
    none = world.get(f"/assets/{world.ids['raw']}/origin").json()
    assert none["roots"] == [] and none["paths"] == [] and none["has_upstream"] is False                       # raw is itself a root
    col = world.get(f"/assets/{world.ids['clean']}/origin", column="amount").json()
    assert [r["asset"]["name"] for r in col["roots"]] == ["amount"] and col["paths"][0]["confidence"] == "derived"


def test_origin_of_a_source_is_itself_and_errors(world):
    j = world.get(f"/assets/{world.ids['crm_system']}/origin").json()
    assert j["paths"] == [] and j["roots"] == [] and j["asset"]["name"] == "crm_system"
    assert world.get(f"/assets/{world.ids['mart']}/origin", depth=11).status_code == 422
    assert world.get(f"/assets/{world.ids['mart']}/origin", min_confidence="x").status_code == 422
    assert world.get("/assets/ast_nope/origin").status_code == 404
    assert world.get(f"/assets/{world.ids['secret']}/origin", "alpha").status_code == 404


def test_impact(world):
    j = world.get(f"/assets/{world.ids['raw']}/impact").json()
    assert j["asset"]["name"] == "raw" and j["total"] == 5
    assert {k: v["count"] for k, v in j["by_type"].items()} == {"table": 3, "semantic_model": 1, "report": 1}
    assert [r["name"] for r in j["reports"]] == ["sales_model", "sales_report"] or {r["name"] for r in j["reports"]} == {"sales_model", "sales_report"}
    assert {p["name"] for p in j["processes"]} >= {"etl", "agg"} and j["owners_known"] is False
    limited = world.get(f"/assets/{world.ids['raw']}/impact", depth=1).json()
    assert limited["total"] == 1 and limited["by_type"]["table"]["items"][0]["name"] == "clean"
    assert world.get(f"/assets/{world.ids['sales_report']}/impact").json()["total"] == 0
    alpha = world.get(f"/assets/{world.ids['raw']}/impact", "alpha").json()
    assert alpha["total"] == 4 and "secret_out" not in {n["name"] for n in alpha["nodes"]}
    assert world.get(f"/assets/{world.ids['raw']}/impact", depth=0).status_code == 422 and world.get("/assets/nope/impact").status_code == 404


def test_impact_of_a_column_does_not_list_its_own_table(world):
    """The walk of a column starts at its table; the table is where the column lives, not a consumer of it (it used to be listed at hop 0)."""
    j = world.get(f"/assets/{world.ids['raw.amount']}/impact").json()
    names = {n["name"] for g in j["by_type"].values() for n in g["items"]}
    assert "raw" not in names and {"clean", "mart", "sales_report"} <= names
    assert j["total"] == sum(g["count"] for g in j["by_type"].values()) == len(names)
    assert j["total"] == world.get(f"/assets/{world.ids['raw']}/impact").json()["total"]          # the same consumers as the table has


def test_staleness(world):
    j = world.get(f"/assets/{world.ids['mart']}/staleness").json()
    assert j["threshold_hours"] == 24 and j["stale"] is True and j["last_write_at"] is None
    rows = {r["asset"]["name"]: r for r in j["nodes"]}
    assert rows["mart"]["root_cause"] is True and rows["mart"]["reasons"] and rows["clean"]["stale"] is False and rows["clean"]["last_run"]["ref"] == "job_1"
    assert rows["clean"]["last_write_source"] == "observed write"
    fresh = world.get(f"/assets/{world.ids['clean']}/staleness").json()
    assert fresh["stale"] is False and fresh["last_write_at"]
    tiny = world.get(f"/assets/{world.ids['clean']}/staleness", stale_after_hours=0.0001)
    assert tiny.status_code == 200
    assert world.get(f"/assets/{world.ids['clean']}/staleness", stale_after_hours=0).status_code == 422
    assert world.get(f"/assets/{world.ids['clean']}/staleness", depth=11).status_code == 422
    assert world.get("/assets/nope/staleness").status_code == 404 and world.get(f"/assets/{world.ids['secret']}/staleness", "alpha").status_code == 404


# ------------------------------------------------------------------------------------------------------------------------------ documentation, history
def test_documentation(world):
    j = world.get(f"/assets/{world.ids['raw']}/documentation").json()
    md = j["markdown"]
    assert j["name"] == "raw" and md.startswith("# raw") and "## Schema" in md and "| `region` | string |" in md and "## Lineage" in md
    assert "## Recent schema changes" in md and "added: region" in md and "changed: id" in md
    assert "etl" in md and "Lineage is incomplete" in md
    r = world.get(f"/assets/{world.ids['clean']}/documentation", format="markdown")
    assert r.status_code == 200 and r.headers["content-type"].startswith("text/markdown") and r.text.startswith("# clean")
    assert "Last write:" in r.text
    assert world.get(f"/assets/{world.ids['clean']}/documentation", format="pdf").status_code == 422
    assert world.get(f"/assets/{world.ids['secret']}/documentation", "alpha").status_code == 404 and world.get("/assets/nope/documentation").status_code == 404
    colmd = world.get(f"/assets/{world.ids['clean.amount']}/documentation").json()["markdown"]
    assert colmd.startswith("# amount")


def test_history(world):
    j = world.get(f"/assets/{world.ids['raw']}/history").json()
    assert j["asset_id"] == world.ids["raw"] and len(j["items"]) == 2 and j["items"][0]["changes"]["columns_added"] == ["region"]
    assert j["items"][1]["changes"] == {"created": True} and j["next_cursor"] is None
    only = world.get(f"/assets/{world.ids['raw']}/history", schema_only=True).json()["items"]
    assert len(only) == 1 and "columns_added" in only[0]["changes"]
    p1 = world.get(f"/assets/{world.ids['raw']}/history", limit=1).json()
    assert len(p1["items"]) == 1 and p1["next_cursor"]
    p2 = world.get(f"/assets/{world.ids['raw']}/history", limit=1, cursor=p1["next_cursor"]).json()
    assert p2["items"][0]["id"] < p1["items"][0]["id"] and p2["next_cursor"] is None
    assert world.get(f"/assets/{world.ids['secret']}/history", "alpha").status_code == 404 and world.get("/assets/nope/history").status_code == 404


# ------------------------------------------------------------------------------------------------------------------------------ resolve
def test_resolve(world):
    ws = world.ws1["id"]

    def post(body, who="admin"):
        return world.c.post(f"{BASE}/resolve", json=body, headers=world.admin if who == "admin" else world.h[who])

    r = post({"type": "table", "aliases": {"lakehouse_table": "itm_lh1:clean"}})
    assert r.status_code == 200 and r.json()["asset"]["id"] == world.ids["clean"] and r.json()["created"] is False and r.json()["via"] == "alias"
    r = post({"type": "table", "name": "mart", "scope": {"workspace_id": ws}})
    assert r.json()["asset"]["id"] == world.ids["mart"] and r.json()["via"] in ("name", "qname", "lakehouse")
    r = post({"type": "table", "name": "clean", "scope": {"workspace_id": ws, "lakehouse_id": "itm_lh1"}})
    assert r.json()["asset"]["id"] == world.ids["clean"]
    r = post({"type": "column", "name": "AMOUNT", "scope": {"table": world.ids["clean"], "workspace_id": ws}})
    assert r.json()["asset"]["id"] == world.ids["clean.amount"]
    assert post({"type": "table", "name": "nothing_here", "scope": {"workspace_id": ws}}).status_code == 404
    assert post({"type": "table", "aliases": {"lakehouse_table": "itm_lh1:clean"}}, "beta").status_code == 404            # exists, but not for beta
    assert post({"type": "table", "aliases": {"lakehouse_table": "itm_lh1:clean"}}, "alpha").status_code == 200
    assert post({"type": "table", "aliases": {"lakehouse_table": "itm_lh1:clean"}}, "none").status_code == 403


def test_resolve_errors(world):
    def post(body, who="admin"):
        return world.c.post(f"{BASE}/resolve", json=body, headers=world.admin if who == "admin" else world.h[who])

    assert post({"type": "spaceship", "name": "x"}).status_code == 400
    assert post({"type": "table", "aliases": {"nope": "x"}}).status_code == 400
    assert post({"type": "table"}).status_code in (400, 404)
    assert post({"type": "table", "name": "x", "bogus": 1}).status_code == 422
    assert post({}).status_code == 422
    # creating a placeholder is a write: needs intelligence.manage
    body = {"type": "table", "name": "lakehouse_a.future", "scope": {"workspace_id": world.ws1["id"]}, "create": True}
    assert post(body, "alpha").status_code == 403 and post(body, "curator").status_code == 403


# ------------------------------------------------------------------------------------------------------------------------------ edges, snapshots
def test_edge_detail(world):
    lin = world.get(f"/assets/{world.ids['clean']}/lineage", direction="up", depth=1).json()
    wr = next(e for e in lin["edges"] if e["type"] == "WRITES_TO")
    r = world.get(f"/edges/{wr['id']}")
    j = r.json()
    assert r.status_code == 200 and j["confidence"] == "verified" and j["evidence"][0]["kind"] == "runtime" and j["evidence"][0]["ref"] == "job_1"
    assert j["src_asset"]["name"] == "etl" and j["dst_asset"]["name"] == "clean" and j["changes"] == [] and j["observation_count"] == 1
    tr = next(e for e in world.get(f"/assets/{world.ids['clean.amount']}/lineage", direction="up", level="column").json()["edges"])
    d = world.get(f"/edges/{tr['id']}").json()
    assert d["transformation"] == "amount * 1.0" and d["via_asset"]["name"] == "etl"
    assert world.get(f"/edges/{wr['id']}", "alpha").status_code == 200
    assert world.get("/edges/edg_nope").status_code == 404
    assert world.get(f"/edges/{wr['id']}", "none").status_code == 403


def test_edge_between_workspaces_is_hidden_from_a_reader_of_one(world):
    cross = world.c.get(f"{BASE}/assets/{world.ids['clean']}/lineage", params={"depth": 1}, headers=world.admin).json()
    edge = next(e for e in cross["edges"] if "beta_nb" in (n["name"] for n in cross["nodes"] if n["id"] in (e["src"], e["dst"])))
    assert world.get(f"/edges/{edge['id']}").status_code == 200
    assert world.get(f"/edges/{edge['id']}", "alpha").status_code == 404 and world.get(f"/edges/{edge['id']}", "beta").status_code == 404
    assert world.get(f"/edges/{edge['id']}", "org").status_code == 200


def test_snapshots_and_diff(world):
    j = world.get("/snapshots").json()
    assert [s["version"] for s in j["items"]] == [2, 1] and j["items"][0]["mode"] == "incremental" and j["next_cursor"] is None
    assert [s["id"] for s in world.get("/snapshots", status="failed").json()["items"]] == []
    assert len(world.get("/snapshots", source_id="local").json()["items"]) == 2 and world.get("/snapshots", source_id="elsewhere").json()["items"] == []
    assert len(world.get("/snapshots", limit=1).json()["items"]) == 1 and world.get("/snapshots", limit=1).json()["next_cursor"]
    assert world.get("/snapshots", status="bogus").status_code == 422
    d = world.c.get(f"{BASE}/snapshots/diff", params={"from": world.scans["s1"], "to": world.scans["s2"]}, headers=world.admin)
    assert d.status_code == 200
    j = d.json()
    assert [a["name"] for a in j["assets"]["added"]] == ["fresh"] and [a["name"] for a in j["assets"]["changed"]] == ["raw"]
    assert j["assets"]["changed"][0]["changes"]["columns_added"] == ["region"] and j["summary"]["assets_removed"] == 0 and j["source_id"] == "local"
    assert j["from"]["version"] == 1 and j["to"]["version"] == 2


def test_diff_errors(world):
    def diff(a, b, who="admin"):
        return world.c.get(f"{BASE}/snapshots/diff", params={"from": a, "to": b}, headers=world.admin if who == "admin" else world.h[who])

    assert diff(world.scans["s2"], world.scans["s1"]).status_code == 400
    assert diff("scn_nope", world.scans["s1"]).status_code == 404
    assert world.c.get(f"{BASE}/snapshots/diff", params={"to": "x"}, headers=world.admin).status_code == 422
    assert world.c.get(f"{BASE}/snapshots/diff", params={"from": "a", "to": "b", "limit": 0}, headers=world.admin).status_code == 422
    assert diff(world.scans["s1"], world.scans["s2"], "none").status_code == 403
    alpha = diff(world.scans["s1"], world.scans["s2"], "alpha").json()
    assert "fresh" in [a["name"] for a in alpha["assets"]["added"]]
    hidden = diff(world.scans["s1"], world.scans["s2"], "beta").json()
    assert hidden["assets"]["added"] == [] and hidden["assets"]["changed"] == []          # alpha-only changes are invisible to beta


def test_unused_route(world):
    j = world.get("/unused", days=0).json()
    assert j["days"] == 0 and {i["name"] for i in j["items"]} >= {"lonely", "mart"} and all(i["type"] in ("table", "view") for i in j["items"])
    assert world.get("/unused").json()["items"] == []                                  # default 30 days: everything is younger than the window
    assert world.get("/unused", days=-1).status_code == 422 and world.get("/unused", days=99999).status_code == 422
    assert {i["name"] for i in world.get("/unused", days=0, type="notebook").json()["items"]} == {"etl", "agg", "beta_nb"}
    assert {i["name"] for i in world.get("/unused", "beta", days=0).json()["items"]} == {"secret", "secret_out"}
    assert world.get("/unused", "alpha", days=0, workspace_id=world.ws2["id"]).json()["items"] == []
    page = world.get("/unused", days=0, limit=2).json()
    assert len(page["items"]) == 2 and page["next_cursor"]
    assert world.get("/unused", days=0, type="spaceship").status_code == 400
