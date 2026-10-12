# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
# ruff: noqa: S105, S106
"""Who may see what, end to end: a Viewer of workspace *alpha* never learns anything about workspace *beta* through any surface - REST search, lineage, origin, impact, staleness,
documentation, history, edges, gaps and summary; the MCP tools; the ``olcf intelligence`` commands; the Python SDK.

The graph is the real platform's (all modules, the sample installed in *alpha*) plus a chain that crosses the boundary twice, applied through the real engine as a connector would::

    zz_alpha_raw --nb_beta_in--> zz_beta_mid --nb_beta_out--> zz_alpha_out          (zz_beta_private: a table of beta nobody reads)

**What the implementation does with a hidden middle node** (asserted below, not just hoped): a node the caller may not see is skipped *as if it did not exist* (``GraphStore.walk``'s
``keep_node``). The walk therefore does not hop over it: from ``zz_alpha_raw`` the caller sees no downstream at all and from ``zz_alpha_out`` no upstream, so the two alpha tables
look unrelated to an alpha-only viewer. Nothing about beta leaks - not a name, an id, a count, a confidence, a gap or the existence of the missing link - at the price that
"alpha_out is fed by alpha_raw (through beta)" is not shown. Someone who may see the middle node (an org-wide viewer, or alpha's viewer once granted beta) sees the whole chain.
The alternative - bridging the gap with an anonymous "hidden" node - would reveal that something exists and how many hops it is away; the platform chooses not to.
"""

from __future__ import annotations

import json
from collections.abc import Iterator
from dataclasses import dataclass
from typing import Any

import pytest
from sqlalchemy import select

from olcf.intelligence import model as M
from olcf.intelligence.schema import edges
from olcf.sdk.errors import NotFoundError as NotFound
from tests.intelligence.conftest import Builder
from tests.intelligence.system.conftest import PASSWORD, Platform, build_platform

SECRET = ("zz_beta_mid", "zz_beta_private", "nb_beta_in", "nb_beta_out", "lh_beta", "Beta Finance", "zz_beta_gap")


@dataclass
class World:
    p: Platform
    alpha: str
    beta: str
    ids: dict[str, str]
    va: dict[str, str]                      # Viewer of alpha
    vb: dict[str, str]                      # Viewer of beta
    org: dict[str, str]                     # Viewer of the whole organisation
    va_principal: str
    cross_edge: str

    def leaks(self, blob: Any) -> list[str]:
        """Everything in ``blob`` (any JSON-able value or text) that belongs to beta: its names, its workspace id and name, the ids of its assets."""
        text = blob if isinstance(blob, str) else json.dumps(blob, default=str)
        needles = [*SECRET, self.beta, *(v for k, v in self.ids.items() if k.startswith(("beta_", "nb_beta")))]
        return [n for n in needles if n in text]


@pytest.fixture(scope="module")
def w(tmp_path_factory: pytest.TempPathFactory) -> Iterator[World]:
    with build_platform(tmp_path_factory.mktemp("authz"), None, install_sample=True) as p:
        alpha = p.sample["workspace_id"]                                                      # type: ignore[attr-defined]
        r = p.http.post("/v1/workspaces", json={"name": "Beta Finance"}, headers=p.admin)
        assert r.status_code == 201, r.text
        beta = r.json()["id"]
        ba, bb = Builder(alpha), Builder(beta)
        lh_a, lh_b = ba.lakehouse("itm_lh_alpha", "lh_alpha"), bb.lakehouse("itm_lh_beta", "lh_beta")
        raw, out = ba.table(lh_a, "zz_alpha_raw", [("k", "int")]), ba.table(lh_a, "zz_alpha_out", [("k", "int")])
        mid, private = bb.table(lh_b, "zz_beta_mid", [("k", "int")]), bb.table(lh_b, "zz_beta_private", [("secret_k", "int")])
        n_in, n_out = bb.notebook("itm_nb_beta_in", "nb_beta_in"), bb.notebook("itm_nb_beta_out", "nb_beta_out")
        links = [bb.edge(n_in, raw, "READS_FROM"), bb.edge(n_in, mid, "WRITES_TO"), bb.edge(n_out, mid, "READS_FROM"), bb.edge(n_out, out, "WRITES_TO")]
        svc = p.intel
        source = next(s["id"] for s in p.get("/v1/intelligence/sources")["items"] if s["kind"] == "local")
        scan = svc.engine.begin_scan(source, "incremental", "manual")
        stats = svc.engine.apply_facts(M.Facts([lh_a, lh_b, raw, out, mid, private, n_in, n_out], links), scan_id=scan, source_id=source)
        assert not stats.errors, stats.error_samples
        mid_id = svc.registry.resolve(mid.ref, create=False)
        svc.engine.record_gaps(scan, [M.Gap("not_inspected", "test", "zz_beta_gap", "zz_beta_mid has a column that could not be read", "grant read access")], source, asset_id=mid_id)
        svc.engine.finish_scan(scan, "succeeded", stats.as_dict())
        ids = {name: svc.registry.resolve(f.ref, create=False) for name, f in
               {"alpha_raw": raw, "alpha_out": out, "beta_mid": mid, "beta_private": private, "nb_beta_in": n_in, "nb_beta_out": n_out, "beta_lh": lh_b}.items()}
        with p.ctx.db.connect() as c:
            cross = c.execute(select(edges.c.id).where(edges.c.src_id == ids["nb_beta_in"], edges.c.dst_id == ids["alpha_raw"])).scalar_one()
        ident = p.ctx.service("identity")
        row = ident.create_principal("viewer_alpha", kind="user", password=PASSWORD)
        ident.bind(row["id"], "Viewer", scope_type="workspace", scope_id=alpha)
        r = p.http.post("/v1/identity/login", json={"username": "viewer_alpha", "password": PASSWORD})
        yield World(p, alpha, beta, ids, {"Authorization": f"Bearer {r.json()['access_token']}"}, p.user("viewer_beta", "Viewer", workspace_id=beta),
                    p.user("viewer_org", "Viewer"), row["id"], cross)


def names(doc: dict[str, Any]) -> set[str]:
    return {n["name"] for n in doc["nodes"]}


# ================================================================================================================ REST
def test_the_fixture_is_what_the_tests_assume(w: World) -> None:
    p = w.p
    full = p.lineage(w.ids["alpha_raw"], direction="down", depth=5)                           # the administrator sees the chain across both workspaces
    assert {"nb_beta_in", "zz_beta_mid", "nb_beta_out", "zz_alpha_out"} <= names(full)
    assert w.leaks(full)                                                                      # ... and the leak detector sees beta in it
    assert w.leaks("a harmless sentence") == []


def test_search_finds_alpha_and_never_beta(w: World) -> None:
    p = w.p
    assert {a["name"] for a in p.get("/v1/intelligence/assets", w.va, q="zz_")["items"]} == {"zz_alpha_raw", "zz_alpha_out"}
    for q in ("zz_beta", "nb_beta", "lh_beta", "secret_k", "Beta"):
        doc = p.get("/v1/intelligence/assets", w.va, q=q)
        assert doc["items"] == [] and not w.leaks(doc), q
    assert p.get("/v1/intelligence/assets", w.va, q="zz_beta", workspace_id=w.beta)["items"] == []          # asking for beta's workspace by id is not a way in
    assert {a["name"] for a in p.get("/v1/intelligence/assets", w.vb, q="zz_")["items"]} == {"zz_beta_mid", "zz_beta_private"}
    assert {a["name"] for a in p.get("/v1/intelligence/assets", w.org, q="zz_")["items"]} == {"zz_alpha_raw", "zz_alpha_out", "zz_beta_mid", "zz_beta_private"}


@pytest.mark.parametrize("endpoint", ["", "/lineage", "/origin", "/impact", "/staleness", "/documentation", "/history", "/children", "/sample"])
def test_a_hidden_asset_does_not_exist_for_any_per_asset_endpoint(w: World, endpoint: str) -> None:
    r = w.p.http.get(f"/v1/intelligence/assets/{w.ids['beta_mid']}{endpoint}", headers=w.va)
    assert r.status_code == 404, (endpoint, r.status_code, r.text[:200])                     # not 403: the existence of the asset is not revealed
    assert r.json()["error"]["code"] == "not_found" and not [n for n in w.leaks(r.text) if n != w.ids["beta_mid"]]
    same = w.p.http.get(f"/v1/intelligence/assets/ast_00000000000000000000000000{endpoint}", headers=w.va)         # ... and it looks like an asset that never existed
    assert (same.status_code, same.json()["error"]["code"]) == (r.status_code, r.json()["error"]["code"])


def test_hidden_governance_and_edges_are_not_found_either(w: World) -> None:
    p = w.p
    assert p.http.get(f"/v1/governance/assets/{w.ids['beta_mid']}/certification", headers=w.va).status_code == 404
    assert p.http.get(f"/v1/intelligence/edges/{w.cross_edge}", headers=w.va).status_code == 404       # one end is beta's
    assert p.http.get(f"/v1/intelligence/edges/{w.cross_edge}", headers=w.vb).status_code == 404       # ... and the other end is alpha's
    assert p.http.get(f"/v1/intelligence/edges/{w.cross_edge}", headers=w.org).status_code == 200


def test_a_hidden_middle_node_is_skipped_not_bridged_and_leaks_nothing(w: World) -> None:
    p = w.p
    down = p.get(f"/v1/intelligence/assets/{w.ids['alpha_raw']}/lineage", w.va, direction="down", depth=10)
    assert names(down) == {"zz_alpha_raw"} and down["edges"] == [] and down["truncated"] is False and not w.leaks(down)
    up = p.get(f"/v1/intelligence/assets/{w.ids['alpha_out']}/lineage", w.va, direction="up", depth=10)
    assert names(up) == {"zz_alpha_out"} and up["edges"] == [] and not w.leaks(up)
    origin = p.get(f"/v1/intelligence/assets/{w.ids['alpha_out']}/origin", w.va)
    assert origin["paths"] == [] and origin["roots"] == [] and origin["has_upstream"] is False and not w.leaks(origin)
    impact = p.get(f"/v1/intelligence/assets/{w.ids['alpha_raw']}/impact", w.va)
    assert impact["total"] == 0 and impact["processes"] == [] and impact["reports"] == [] and not w.leaks(impact)
    assert not w.leaks(p.get(f"/v1/intelligence/assets/{w.ids['alpha_raw']}/staleness", w.va))
    assert not w.leaks(p.get(f"/v1/intelligence/assets/{w.ids['alpha_out']}/documentation", w.va))
    assert not w.leaks(p.get(f"/v1/intelligence/assets/{w.ids['alpha_raw']}/history", w.va))
    # the column level and the edge/node statistics say nothing either
    assert not w.leaks(p.get(f"/v1/intelligence/assets/{w.ids['alpha_raw']}/lineage", w.va, direction="both", level="column", depth=10))
    assert down["stats"] == {"nodes": 1, "edges": 0, "max_nodes": down["stats"]["max_nodes"]}


def test_whoever_may_see_the_middle_sees_the_whole_chain(w: World) -> None:
    p = w.p
    for who in (w.org, p.admin):
        origin = p.get(f"/v1/intelligence/assets/{w.ids['alpha_out']}/origin", who)
        nodes = {n["id"]: n["name"] for n in origin["nodes"]}
        chains = [[nodes[i] for i in path["nodes"]] for path in origin["paths"]]
        assert ["zz_alpha_raw", "nb_beta_in", "zz_beta_mid", "nb_beta_out", "zz_alpha_out"] in chains
        assert {x["total"] for x in [p.get(f"/v1/intelligence/assets/{w.ids['alpha_raw']}/impact", who)]} == {2}      # zz_beta_mid and zz_alpha_out


def test_symmetrically_beta_does_not_see_alpha(w: World) -> None:
    p = w.p
    up = p.get(f"/v1/intelligence/assets/{w.ids['beta_mid']}/lineage", w.vb, direction="up", depth=10)
    assert names(up) == {"zz_beta_mid", "nb_beta_in"} and len(up["edges"]) == 1               # beta's own notebook is there; the alpha table it reads is not (the walk stops at it)
    down = p.get(f"/v1/intelligence/assets/{w.ids['beta_mid']}/lineage", w.vb, direction="down", depth=10)
    assert "zz_alpha_out" not in names(down) and "zz_alpha_raw" not in names(down)
    blob = json.dumps([up, down, p.get("/v1/intelligence/assets", w.vb, q="alpha")])
    assert "zz_alpha" not in blob and "lh_alpha" not in blob
    assert p.http.get(f"/v1/intelligence/assets/{w.ids['alpha_raw']}", headers=w.vb).status_code == 404


def test_gaps_summary_and_coverage_are_filtered_to_what_the_caller_may_see(w: World) -> None:
    p = w.p
    admin_gaps = p.get("/v1/intelligence/gaps", limit=200)
    assert any(g["subject"] == "zz_beta_gap" for g in admin_gaps["items"])
    mine = p.get("/v1/intelligence/gaps", w.va, limit=200)
    assert not w.leaks(mine) and not any(g["subject"] == "zz_beta_gap" for g in mine["items"])
    assert [g["subject"] for g in p.get("/v1/intelligence/gaps", w.vb, limit=200)["items"] if g["subject"] == "zz_beta_gap"] == ["zz_beta_gap"]
    s_admin, s_alpha, s_beta = (p.get("/v1/intelligence/summary", who)["assets"] for who in (p.admin, w.va, w.vb))
    assert s_admin["by_type"]["table"] - s_alpha["by_type"]["table"] == 2                    # exactly beta's two tables
    assert s_alpha["by_type"]["table"] - 2 == s_admin["by_type"]["table"] - 4 and s_beta["by_type"]["table"] == 2
    assert s_admin["by_type"].get("notebook", 0) - s_alpha["by_type"].get("notebook", 0) == 2 and s_beta["by_type"]["notebook"] == 2
    assert not w.leaks(p.get("/v1/intelligence/summary", w.va)) and not w.leaks(p.get("/v1/intelligence/unused", w.va))
    assert not w.leaks(p.get("/v1/intelligence/snapshots", w.va))


def test_unauthenticated_and_unbound_callers_get_nothing(w: World) -> None:
    p = w.p
    assert p.http.get("/v1/intelligence/assets", params={"q": "zz_"}).status_code == 401
    nobody = p.user("nobody_bound", None)
    r = p.http.get("/v1/intelligence/assets", params={"q": "zz_"}, headers=nobody)
    assert r.status_code in (200, 403) and (r.status_code == 403 or r.json()["items"] == [])
    assert not w.leaks(r.text) and "zz_alpha" not in r.text


# ================================================================================================================ MCP
def _agent(w: World, name: str, workspace: str) -> Any:
    return w.p.mcp(w.p.agent(name, "Contributor", workspace_id=workspace))


def test_every_mcp_tool_keeps_beta_out_of_alphas_answers(w: World) -> None:
    a = _agent(w, "agent_alpha", w.alpha)
    tools = {t["name"] for t in a.result("tools/list")["tools"]}
    assert {"intel_search_assets", "intel_get_asset", "intel_lineage", "intel_where_does_this_come_from", "intel_impact_analysis", "intel_staleness", "intel_documentation",
            "intel_coverage_gaps", "intel_summary", "intel_unused_assets"} <= tools
    answers: list[Any] = [
        a.call("intel_search_assets", query="zz_"),
        a.call("intel_search_assets", query="zz_beta"),
        a.call("intel_search_assets", query="nb_beta"),
        a.call("intel_get_asset", asset="zz_alpha_raw"),
        a.call("intel_lineage", asset="zz_alpha_raw", direction="down", depth=6),
        a.call("intel_lineage", asset="zz_alpha_out", direction="up", depth=6),
        a.call("intel_where_does_this_come_from", asset="zz_alpha_out"),
        a.call("intel_impact_analysis", asset="zz_alpha_raw"),
        a.call("intel_staleness", asset="zz_alpha_out"),
        a.call("intel_documentation", asset="zz_alpha_out"),
        a.call("intel_coverage_gaps"),
        a.call("intel_summary"),
        a.call("intel_unused_assets"),
    ]
    assert not w.leaks(answers)
    assert {x["name"] for x in answers[0]["assets"]} == {"zz_alpha_raw", "zz_alpha_out"}
    assert answers[1]["assets"] == [] and answers[2]["assets"] == [] and answers[1]["count"] == 0
    # by name, by id, by an id that exists elsewhere: the answer is the same "not found", and it does not repeat what was asked beyond the argument
    for ref in ("zz_beta_mid", w.ids["beta_mid"], w.ids["nb_beta_in"]):
        for tool in ("intel_get_asset", "intel_lineage", "intel_where_does_this_come_from", "intel_impact_analysis", "intel_staleness", "intel_documentation"):
            err = a.call(tool, error=True, asset=ref)
            assert err["code"] in ("not_found", "asset_not_found") or "not" in err["message"].lower(), (tool, err)
            assert not [n for n in w.leaks(err) if n not in (ref,)], (tool, err)
    nothing = a.call("intel_where_does_this_come_from", asset="zz_alpha_out")
    assert nothing.get("paths", []) == [] and nothing.get("roots", []) == []


def test_an_agent_of_beta_sees_beta_and_not_alpha(w: World) -> None:
    b = _agent(w, "agent_beta", w.beta)
    assert {x["name"] for x in b.call("intel_search_assets", query="zz_")["assets"]} == {"zz_beta_mid", "zz_beta_private"}
    blob = json.dumps([b.call("intel_search_assets", query="alpha"), b.call("intel_lineage", asset="zz_beta_mid", direction="both", depth=6), b.call("intel_summary"),
                       b.call("intel_coverage_gaps")])
    assert "zz_alpha" not in blob and "lh_alpha" not in blob


# ================================================================================================================ CLI
def test_the_cli_commands_show_alpha_only(w: World) -> None:
    cli = w.p.cli(w.va)
    outputs = [cli.run("intelligence", "search", "zz_").output, cli.run("intelligence", "search", "zz_beta").output, cli.run("intelligence", "lineage", w.ids["alpha_raw"]).output,
               cli.run("intelligence", "where-from", w.ids["alpha_out"]).output, cli.run("intelligence", "impact", w.ids["alpha_raw"]).output,
               cli.run("intelligence", "gaps").output, cli.run("intelligence", "summary").output, cli.run("intelligence", "coverage").output, cli.run("intelligence", "unused").output,
               cli.run("intelligence", "assets", "list").output]
    assert "zz_alpha_raw" in outputs[0] and "zz_alpha_out" in outputs[0]
    assert not w.leaks("\n".join(outputs)), [o for o in outputs if w.leaks(o)]
    asset_json = cli.json("intelligence", "search", "zz_beta")
    assert asset_json == [] or asset_json.get("items") == [] if isinstance(asset_json, dict) else asset_json == []
    for ref in (w.ids["beta_mid"], "zz_beta_mid"):
        for args in (("assets", "show", ref), ("lineage", ref), ("where-from", ref), ("impact", ref), ("assets", "docs", ref)):
            res = cli.run("intelligence", *args, expect=None)
            assert res.exit_code != 0, (args, res.output)
            assert not [n for n in w.leaks(res.output) if n != ref], (args, res.output)


# ================================================================================================================ SDK
def test_the_sdk_raises_not_found_for_beta_and_returns_alpha(w: World) -> None:
    client = w.p.sdk(w.va["Authorization"].split(" ", 1)[1])
    api = client.intelligence
    assert {a["name"] for a in api.list_assets(q="zz_")} == {"zz_alpha_raw", "zz_alpha_out"}
    assert list(api.list_assets(q="zz_beta")) == []
    answers = [api.lineage(w.ids["alpha_raw"], direction="down", depth=10), api.origin(w.ids["alpha_out"]), api.impact(w.ids["alpha_raw"]), api.summary(),
               api.staleness(w.ids["alpha_raw"]), api.documentation(w.ids["alpha_out"]), api.unused(), api.snapshots()]
    assert not w.leaks(answers)
    for call in (api.get_asset, api.lineage, api.origin, api.impact, api.staleness, api.documentation, api.history, api.children, api.sample):
        with pytest.raises(NotFound) as err:
            call(w.ids["beta_mid"])
        assert not [n for n in w.leaks(str(err.value)) if n != w.ids["beta_mid"]]
    with pytest.raises(NotFound):
        api.get_edge(w.cross_edge)


# ================================================================================================================ access changes take effect
def test_granting_beta_to_alpha_viewer_shows_the_chain_and_revoking_hides_it_again(w: World) -> None:
    p = w.p
    ident = p.ctx.service("identity")
    assert names(p.get(f"/v1/intelligence/assets/{w.ids['alpha_raw']}/lineage", w.va, direction="down", depth=10)) == {"zz_alpha_raw"}
    binding = ident.bind(w.va_principal, "Viewer", scope_type="workspace", scope_id=w.beta)
    try:
        granted = p.get(f"/v1/intelligence/assets/{w.ids['alpha_raw']}/lineage", w.va, direction="down", depth=10)
        assert {"nb_beta_in", "zz_beta_mid", "nb_beta_out", "zz_alpha_out"} <= names(granted)           # the same token, no re-login: the bridge is there
        assert p.get(f"/v1/intelligence/assets/{w.ids['beta_mid']}", w.va)["name"] == "zz_beta_mid"
    finally:
        assert ident.unbind(binding["id"])
    after = p.get(f"/v1/intelligence/assets/{w.ids['alpha_raw']}/lineage", w.va, direction="down", depth=10)
    assert names(after) == {"zz_alpha_raw"} and not w.leaks(after)
    assert p.http.get(f"/v1/intelligence/assets/{w.ids['beta_mid']}", headers=w.va).status_code == 404
