# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
# ruff: noqa: S105, S106
"""The read-only Intelligence MCP tools over the real MCP transport (``POST /mcp`` with scoped platform tokens) against the populated two-workspace world.

Proves what ADR 0014 promises: the ``mcp`` scope reads the asset graph and nothing else.  The write endpoints and the sample endpoint of the REST API refuse an ``mcp`` token
(even one that belongs to a platform administrator), no MCP tool reaches them, results are compact and size-bounded with ``truncated`` set, asset visibility is the same as in
REST (a hidden asset does not exist), and the answers carry confidence, evidence and gaps.
"""

from __future__ import annotations

import json
import re

import pytest

from olcf.intelligence import model as M
from tests.intelligence.conftest import PASSWORD, Builder
from tests.intelligence.surfaces_support import McpClient, Stack, intel_stack

INTEL_TOOLS = ["intel_search_assets", "intel_get_asset", "intel_lineage", "intel_where_does_this_come_from", "intel_impact_analysis", "intel_staleness", "intel_documentation",
               "intel_coverage_gaps", "intel_summary", "intel_unused_assets"]


class Agents:
    """The stack, MCP clients of several callers and the ids of the extra graph that exercises limits, ambiguity and column statistics."""

    def __init__(self, s: Stack) -> None:
        self.s = s
        self.ids = s.ids
        ident = s.ctx.service("identity")
        a, b = s.world.ws1["id"], s.world.ws2["id"]
        # an organisation-wide platform administrator: its login token may do anything, the same account's `mcp` token may only read
        row = ident.create_principal("mcp_boss", kind="user", password=PASSWORD)
        ident.bind(row["id"], "PlatformAdmin", scope_type="org", scope_id="")
        self.boss_mcp = {"Authorization": "Bearer " + s.ctx.tokens.create_pat(row["id"], "boss-mcp", scopes=["mcp"], ttl_days=1)[0]}
        self.boss_write = {"Authorization": "Bearer " + s.ctx.tokens.create_pat(row["id"], "boss-mcp-write", scopes=["mcp:write"], ttl_days=1)[0]}
        r = s.http.post("/v1/identity/login", json={"username": "mcp_boss", "password": PASSWORD})
        assert r.status_code == 200, r.text
        self.boss_login = {"Authorization": f"Bearer {r.json()['access_token']}"}
        self.org = McpClient(s.http, s.world.h["agent"])
        self.boss = McpClient(s.http, self.boss_mcp)
        self.alpha = McpClient(s.http, s.intel.user("mcp_alpha", "Contributor", workspace_id=a, scopes=["mcp"]))
        self.beta = McpClient(s.http, s.intel.user("mcp_beta", "Contributor", workspace_id=b, scopes=["mcp"]))
        self.viewer_headers = s.intel.user("mcp_viewer", "Viewer", workspace_id=a, scopes=["mcp"])
        for m in (self.org, self.boss, self.alpha, self.beta):
            m.init()
        self._extra_graph(a, b)

    def _extra_graph(self, alpha: str, beta: str) -> None:
        """120 tables fed by one notebook from ``hub`` (limits), a table whose scan kept column statistics (they must stay out), and ``dup`` in both workspaces (ambiguity)."""
        s = self.s
        bd, bb = Builder(alpha), Builder(beta)
        lh = bd.lakehouse("itm_lhfan", "lakehouse_fan")
        hub = bd.table(lh, "hub", [("k", "int")])
        nb = bd.notebook("itm_nbfan", "fan_nb")
        outs = [bd.table(lh, f"out_{i:03d}", [("k", "int")]) for i in range(120)]
        profiled = bd.table(lh, "profiled")
        profiled.attrs["columns"] = [{"name": "salary", "type": "int", "nullable": False, "min": 12345, "max": 987654, "null_count": 0,
                                      "value_profile": {"rows": 99, "non_null": 99, "distinct": 7}}]
        dup_a = bd.table(lh, "dup", [("x", "int")])
        lhb = bb.lakehouse("itm_lhb2", "lakehouse_b2")
        dup_b = bb.table(lhb, "dup", [("x", "int")])
        edges = [bd.edge(nb, hub, "READS_FROM", "derived")] + [bd.edge(nb, o, "WRITES_TO", "derived") for o in outs]
        s.intel.scan(M.Facts([lh, hub, nb, *outs, profiled, dup_a, lhb, dup_b], edges), source="local", mode="incremental")
        for n in ("hub", "profiled", "fan_nb"):
            self.ids[n] = s.intel.asset_id(n)


@pytest.fixture(scope="module")
def ag(tmp_path_factory):
    with intel_stack(tmp_path_factory.mktemp("mcp_intel"), extra_modules=("olcf.mcp.module",)) as s:
        yield Agents(s)


# ============================================================================================================================ catalogue
def test_tool_catalogue_is_read_only_and_asks_the_right_questions(ag):
    tools = {t["name"]: t for t in ag.org.result("tools/list")["tools"]}
    assert [n for n in tools if n.startswith("intel_")] == INTEL_TOOLS
    for name in INTEL_TOOLS:
        t = tools[name]
        assert t["annotations"]["readOnlyHint"] is True and t["annotations"]["destructiveHint"] is False and t["annotations"]["openWorldHint"] is False
        assert t["inputSchema"]["additionalProperties"] is False and len(t["description"]) > 150 and t["title"]
    blob = " ".join(t["description"] for t in tools.values())
    for question in ("Where does Revenue originate?", "What breaks if I change this column?", "Which tables can we retire?", "What can't the lineage see?"):
        assert question in blob
    assert "?" in tools["intel_where_does_this_come_from"]["description"] and "Revenue" in tools["intel_where_does_this_come_from"]["description"]
    # nothing in the Intelligence namespace writes, scans, samples or configures
    forbidden = re.compile(r"sample|update|patch|create|delete|scan|import|set_|put_|policy|curate|approve|reject|certify|source|credential")
    assert [n for n in tools if n.startswith(("intel_", "governance_")) and forbidden.search(n)] == []
    # the three catalogue tools that predate Intelligence are exactly as they were
    assert set(tools["search_catalog"]["inputSchema"]["properties"]) == {"query", "workspace", "kinds", "limit"}
    assert set(tools["describe_table"]["inputSchema"]["properties"]) == {"lakehouse", "workspace", "table", "include_history"}
    assert set(tools["get_lineage"]["inputSchema"]["properties"]) == {"dataset", "lakehouse", "workspace", "direction", "depth", "column"}
    assert tools["get_lineage"]["inputSchema"]["required"] == ["dataset"]


def test_viewers_and_unscoped_tokens_cannot_use_the_tools(ag):
    viewer = McpClient(ag.s.http, ag.viewer_headers)
    r = viewer.rpc("initialize", {"protocolVersion": "2025-06-18", "capabilities": {}, "clientInfo": {"name": "v", "version": "1"}}, session=False)
    assert r.status_code == 403                                   # Viewers cannot use MCP at all
    read_only = McpClient(ag.s.http, ag.s.intel.user("mcp_readscope", "Contributor", workspace_id=ag.s.world.ws1["id"], scopes=["read"]))
    r = read_only.rpc("initialize", {"protocolVersion": "2025-06-18", "capabilities": {}, "clientInfo": {"name": "v", "version": "1"}}, session=False)
    assert r.status_code == 403                                   # a token without the mcp scope is refused too


# ============================================================================================================================ writes and samples are unreachable
def test_write_and_sample_endpoints_are_unreachable_with_an_mcp_token(ag):
    aid, http = ag.ids["lonely"], ag.s.http
    for headers in (ag.boss_mcp, ag.boss_write):
        # reads work ...
        assert http.get(f"/v1/intelligence/assets/{aid}", headers=headers).status_code == 200
        # ... everything that writes or samples is refused for the very same administrator account, because the token scope has no such permission
        probes = [
            ("GET", f"/v1/intelligence/assets/{aid}/sample", None),
            ("PATCH", f"/v1/intelligence/assets/{aid}", {"description": "changed through MCP"}),
            ("POST", "/v1/intelligence/sources", {"name": "evil", "kind": "local", "config": {}}),
            ("PUT", "/v1/intelligence/sampling-policies", {"scope_type": "global", "max_rows": 50}),
            ("POST", "/v1/intelligence/resolve", {"type": "table", "name": "ghost", "create": True}),
            ("POST", "/v1/intelligence/import/scanner", None),
            ("POST", "/v1/intelligence/scans/scn_nothing/cancel", None),
        ]
        for method, url, body in probes:
            r = http.request(method, url, json=body, headers=headers) if body is not None else http.request(method, url, headers=headers)
            assert r.status_code == 403, (method, url, r.status_code, r.text[:200])
    # the control: the same account with a full login token is allowed to curate, so the 403 above comes from the scope
    r = http.patch(f"/v1/intelligence/assets/{aid}", json={"description": "curated by hand"}, headers=ag.boss_login)
    assert r.status_code == 200
    assert http.patch(f"/v1/intelligence/assets/{aid}", json={"description": ""}, headers=ag.boss_login).status_code == 200


def test_no_tool_reaches_a_write_or_the_sample_endpoint(ag):
    for name in ("intel_sample_asset", "intel_sample", "intel_update_asset", "intel_scan_source", "intel_create_source", "intel_set_sampling_policy", "intel_import_scanner",
                 "governance_certify", "governance_approve", "governance_create_term"):
        err = ag.boss.error("tools/call", {"name": name, "arguments": {}})
        assert err["code"] == -32602 and "unknown tool" in err["message"]
    # the arguments cannot be bent into a write either: unknown keys are refused by the schema
    bad = ag.boss.call("intel_get_asset", asset="lonely", description="x", error=True)
    assert bad["code"] == "invalid_arguments" and "unknown argument" in bad["message"]
    # a full tour of every tool leaves no write and no sample read in the audit trail
    before = len(ag.s.ctx.audit.query(action="intelligence.asset_updated"))
    tour(ag.boss)
    assert ag.s.ctx.audit.query(action="intelligence.sample_read") == []
    assert len(ag.s.ctx.audit.query(action="intelligence.asset_updated")) == before
    calls = ag.s.http.get("/v1/mcp/calls", params={"limit": 200, "mine": "false"}, headers=ag.s.headers()).json()
    names = {c["tool"] for c in calls["items"] if c.get("tool")}
    assert set(INTEL_TOOLS) <= names                               # the calls were audited like every MCP call


def tour(m: McpClient) -> None:
    """Call every Intelligence tool once with a sensible argument."""
    m.call("intel_summary")
    m.call("intel_search_assets", query="clean")
    m.call("intel_get_asset", asset="clean")
    m.call("intel_lineage", asset="clean")
    m.call("intel_where_does_this_come_from", asset="sales_report")
    m.call("intel_impact_analysis", asset="raw")
    m.call("intel_staleness", asset="clean")
    m.call("intel_documentation", asset="clean")
    m.call("intel_coverage_gaps")
    m.call("intel_unused_assets")


# ============================================================================================================================ search and resolution
def test_search_by_name_type_and_filters(ag):
    r = ag.org.call("intel_search_assets", query="clean")
    assert r["truncated"] is False and r["assets"][0]["name"] == "clean" and r["assets"][0]["path"] == "lakehouse_a/clean" and r["assets"][0]["workspace"] == "alpha"
    assert ag.org.call("intel_search_assets", query="amount", type="column")["assets"][0]["label"] in ("raw.amount", "clean.amount")
    reports = ag.org.call("intel_search_assets", type="report")
    assert [a["name"] for a in reports["assets"]] == ["sales_report"]
    nothing = ag.org.call("intel_search_assets", query="zzz-nothing")
    assert nothing["count"] == 0 and "nothing matched" in nothing["note"]
    needs_something = ag.org.call("intel_search_assets", error=True)
    assert needs_something["code"] == "bad_request" and "query or at least one filter" in needs_something["message"]
    wrong_ws = ag.org.call("intel_search_assets", query="clean", workspace="no-such-workspace", error=True)
    assert wrong_ws["code"] == "not_found"
    limited = ag.org.call("intel_search_assets", query="out_", limit=5)
    assert limited["count"] == 5 and limited["truncated"] is True and "narrow" in limited["note"]


def test_assets_resolve_by_id_name_path_and_column_and_ambiguity_is_explained(ag):
    clean = ag.ids["clean"]
    for ref in (clean, "clean", "lakehouse_a/clean", "lakehouse_a.clean", "Lakehouse_A.CLEAN"):
        assert ag.org.call("intel_get_asset", asset=ref)["id"] == clean, ref
    col = ag.org.call("intel_get_asset", asset="clean.amount")
    assert col["type"] == "column" and col["id"] == ag.ids["clean.amount"]
    amb = ag.org.call("intel_get_asset", asset="dup", error=True)
    assert amb["code"] == "bad_request" and "matches 2 assets" in amb["message"] and len(amb["details"]["candidates"]) == 2
    assert {c["path"] for c in amb["details"]["candidates"]} == {"lakehouse_fan/dup", "lakehouse_b2/dup"}
    assert ag.org.call("intel_get_asset", asset="dup", workspace="alpha")["path"] == "lakehouse_fan/dup"
    assert ag.org.call("intel_get_asset", asset="lakehouse_b2.dup")["path"] == "lakehouse_b2/dup"
    missing = ag.org.call("intel_get_asset", asset="clena", error=True)
    assert missing["code"] == "not_found" and "intel_search_assets" in missing["remediation"]
    assert ag.org.call("intel_get_asset", asset="  ", error=True)["code"] in ("invalid_arguments", "bad_request")


def test_get_asset_is_compact_and_never_carries_column_statistics(ag):
    a = ag.org.call("intel_get_asset", asset="clean")
    assert [c["name"] for c in a["columns"]] == ["id", "amount"] and a["column_count"] == 2 and a["truncated"] is False
    assert a["parent"]["name"] == "lakehouse_a" and a["lineage_edges"] == {"upstream": 1, "downstream": 2} and a["workspace"] == "alpha"
    assert "intel_lineage" in a["next"]
    p = ag.org.call("intel_get_asset", asset="profiled")
    col = p["columns"][0]
    assert col == {"name": "salary", "type": "int", "nullable": False}
    text = json.dumps(p)
    for leaked in ("12345", "987654", "value_profile", "null_count", '"min"', '"max"'):
        assert leaked not in text
    # the same holds for the documentation and the search listing
    assert "987654" not in json.dumps(ag.org.call("intel_documentation", asset="profiled")) and "987654" not in json.dumps(ag.org.call("intel_search_assets", query="profiled"))
    capped = ag.org.call("intel_get_asset", asset="clean", max_columns=1)
    assert len(capped["columns"]) == 1 and capped["column_count"] == 2 and capped["truncated"] is True


# ============================================================================================================================ lineage
def test_lineage_table_level_has_confidence_evidence_and_gaps(ag):
    r = ag.org.call("intel_lineage", asset="clean", depth=2)
    assert r["asset"]["name"] == "clean" and r["level"] == "table" and r["direction"] == "both" and r["column"] is None
    by_name = {n["name"]: n for n in r["nodes"]}
    assert {"etl", "raw", "crm_system", "agg", "mart", "sales_model"} <= set(by_name)
    assert by_name["raw"]["direction"] == "upstream" and by_name["raw"]["hops"] == 1 and by_name["mart"]["direction"] == "downstream"
    assert by_name["crm_system"]["confidence"] == "inferred" and by_name["etl"]["confidence"] == "verified"
    writes = next(e for e in r["edges"] if e["type"] == "WRITES_TO" and e["from_name"] == "etl")
    assert writes["confidence"] == "verified" and writes["to_name"] == "lakehouse_a/clean" and writes["evidence_count"] == 1 and writes["last_verified_at"]
    assert r["weakest_confidence"] == "inferred" and r["edges_by_confidence"]["verified"] == 1
    assert r["gap_count"] == 1 and r["gaps"][0]["kind"] == "dynamic_reference" and r["gaps"][0]["fix"] == "use a literal table name"
    assert r["complete"] is False and any("incomplete" in n for n in r["notes"])
    assert "links: 1 verified" in r["summary"]
    up = ag.org.call("intel_lineage", asset="clean", direction="up", depth=1)
    assert {n["name"] for n in up["nodes"] if n["direction"] == "upstream"} >= {"raw", "etl"} and not [n for n in up["nodes"] if n["direction"] == "downstream"]
    strong = ag.org.call("intel_lineage", asset="clean", min_confidence="derived", depth=3)
    assert "crm_system" not in {n["name"] for n in strong["nodes"]} and strong["min_confidence"] == "derived"
    only_tables = ag.org.call("intel_lineage", asset="clean", types=["table"], depth=3)
    assert {n["type"] for n in only_tables["nodes"]} <= {"table"}


def test_lineage_column_level(ag):
    r = ag.org.call("intel_lineage", asset="clean.amount", direction="up")
    assert r["level"] == "column" and r["column"] == "amount" and r["asset"]["name"] == "clean"
    edge = r["edges"][0]
    assert (edge["from_name"], edge["to_name"], edge["type"]) == ("raw.amount", "clean.amount", "TRANSFORMS")
    assert edge["transformation"] == "amount * 1.0" and edge["via"] == "etl" and edge["confidence"] == "derived"
    same = ag.org.call("intel_lineage", asset="clean", column="amount", direction="up")
    assert [e["from_name"] for e in same["edges"]] == ["raw.amount"] and same["level"] == "column"
    down = ag.org.call("intel_lineage", asset="raw.amount", direction="down")
    assert [e["to_name"] for e in down["edges"]] == ["clean.amount"]


def test_lineage_is_cut_to_the_requested_limits_and_says_so(ag):
    r = ag.org.call("intel_lineage", asset="hub", direction="down", depth=2, max_nodes=10, max_edges=15)
    assert r["truncated"] is True and len(r["nodes"]) == 10 and len(r["edges"]) <= 15 and r["omitted"]["nodes"] > 100
    assert r["complete"] is False and any("cut" in n for n in r["notes"])
    ids = {n["id"] for n in r["nodes"]} | {r["asset"]["id"]}
    assert all(e["from"] in ids and e["to"] in ids for e in r["edges"])      # no dangling edges
    roomy = ag.org.call("intel_lineage", asset="hub", direction="down", depth=2, max_nodes=200, max_edges=400)
    assert len(roomy["nodes"]) > 100 and roomy["omitted"] == {"nodes": 0, "edges": 0}


# ============================================================================================================================ origin
def test_where_does_this_come_from(ag):
    r = ag.org.call("intel_where_does_this_come_from", asset="sales_report")
    assert r["has_upstream"] is True and r["source_systems"] == ["crm_system"]
    assert {o["name"] for o in r["origins"]} == {"crm_system", "raw"} and next(o for o in r["origins"] if o["name"] == "crm_system")["is_source"] is True
    chains = {p["chain"][0]: p["chain"] for p in r["paths"]}
    assert chains["crm_system"] == ["crm_system", "etl", "lakehouse_a/clean", "agg", "lakehouse_a/mart", "sales_model", "sales_report"]
    assert chains["lakehouse_a/raw"][0:3] == ["lakehouse_a/raw", "etl", "lakehouse_a/clean"]
    p = r["paths"][0]
    assert p["confidence"] == "inferred" and p["hops"] == 4 and p["through"] == ["etl", "agg"] and p["evidence"]["links"] >= 1 and p["evidence"]["last_seen_at"]
    assert r["weakest_confidence"] == "inferred" and "originates from" in r["summary"] and "crm_system" in r["summary"]
    assert r["gap_count"] == 1 and r["complete"] is False
    strong = ag.org.call("intel_where_does_this_come_from", asset="sales_report", min_confidence="derived")
    assert strong["has_upstream"] is False or "crm_system" not in strong["source_systems"]


def test_origin_of_a_column_and_of_a_source(ag):
    c = ag.org.call("intel_where_does_this_come_from", asset="clean.amount")
    assert c["column"] == "amount" and c["has_upstream"] is True and c["paths"][0]["chain"][0] == "raw.amount"
    src = ag.org.call("intel_where_does_this_come_from", asset="crm_system")
    assert src["has_upstream"] is False and "no upstream lineage" in src["summary"] and src["paths"] == []
    capped = ag.org.call("intel_where_does_this_come_from", asset="sales_report", max_paths=1)
    assert len(capped["paths"]) == 1 and capped["truncated"] is True and capped["omitted"]["paths"] == 1


# ============================================================================================================================ impact
def test_impact_analysis_of_a_table_and_a_column(ag):
    r = ag.org.call("intel_impact_analysis", asset="raw")
    assert r["counts_by_type"] == {"table": 3, "semantic_model": 1, "report": 1} and r["total"] == 5
    assert [c["name"] for c in r["consumers"]][:2] == ["clean", "mart"] or r["consumers"][0]["name"] == "clean"
    assert {x["name"] for x in r["reports_and_models"]} == {"sales_model", "sales_report"} and r["processes"] == ["etl", "agg", "beta_nb"]
    assert next(c for c in r["consumers"] if c["name"] == "sales_report")["confidence"] == "inferred" and r["weakest_confidence"] == "inferred"
    assert "1 report/dashboard" in r["summary"] and r["complete"] is False and r["gap_count"] == 1
    col = ag.org.call("intel_impact_analysis", asset="raw.amount")
    assert [d["label"] for d in col["dependent_columns"]] == ["clean.amount"] and col["dependent_columns"][0]["transformation"] == "amount * 1.0"
    assert "raw" not in {c["name"] for c in col["consumers"]}            # the column's own table is not a consumer of it
    assert {c["name"] for c in col["consumers"]} >= {"clean", "mart", "sales_report"} and any("table-level" in n for n in col["notes"])
    assert "feeds 1 column" in col["summary"]
    leaf = ag.org.call("intel_impact_analysis", asset="sales_report")
    assert leaf["total"] == 0 and leaf["consumers"] == [] and any("not proof" in n for n in leaf["notes"])


def test_impact_is_bounded(ag):
    r = ag.org.call("intel_impact_analysis", asset="hub")
    assert r["total"] == 120 and len(r["consumers"]) == 40 and r["truncated"] is True and r["omitted"] == {"consumers": 80} and r["counts_by_type"] == {"table": 120}
    assert r["complete"] is False
    more = ag.org.call("intel_impact_analysis", asset="hub", max_items=200)
    assert len(more["consumers"]) == 120 and more["truncated"] is False and more["omitted"]["consumers"] == 0
    # the service-level backstop: even when the tool is asked for everything, the platform's result limit applies and says so
    ag.s.ctx.service("settings").set("mcp.max_result_bytes", 4096)
    try:
        cut = ag.org.call("intel_impact_analysis", asset="hub", max_items=200)
        assert cut["truncated"] is True and len(json.dumps(cut, separators=(",", ":")).encode()) <= 4096 and 0 < len(cut["consumers"]) < 120
    finally:
        ag.s.ctx.service("settings").set("mcp.max_result_bytes", 262_144)


# ============================================================================================================================ staleness, documentation, coverage, summary, unused
def test_staleness(ag):
    r = ag.org.call("intel_staleness", asset="clean")
    assert r["stale"] is False and r["threshold_hours"] == 24.0 and r["last_write_at"] and "fresh" in r["summary"]
    me = next(c for c in r["checked"] if c["name"] == "clean")
    assert me["evidence"] == "observed write" and me["last_run"]["by"] == "runtime:job" and r["root_causes"] == []
    never = ag.org.call("intel_staleness", asset="lonely")
    assert never["last_write_at"] is None and any("unknown" in n for n in never["notes"])
    tight = ag.org.call("intel_staleness", asset="clean", stale_after_hours=0.0000001)
    assert tight["threshold_hours"] == pytest.approx(0.0000001)


def test_documentation_is_markdown_and_cut_to_max_chars(ag):
    r = ag.org.call("intel_documentation", asset="clean")
    assert r["format"] == "markdown" and r["markdown"].startswith("# clean") and "## Schema" in r["markdown"] and "## Lineage" in r["markdown"] and r["truncated"] is False
    short = ag.org.call("intel_documentation", asset="clean", max_chars=500)
    assert short["truncated"] is True and len(short["markdown"]) < 700 and "cut" in short["markdown"].splitlines()[-1] and short["chars"] == r["chars"]
    assert ag.org.call("intel_documentation", asset="clean", max_chars=100, error=True)["code"] == "invalid_arguments"


def test_coverage_gaps_and_summary(ag):
    cov = ag.org.call("intel_coverage_gaps")
    assert cov["gaps"][0]["kind"] == "dynamic_reference" and cov["gaps"][0]["asset"] == "etl" and cov["gaps"][0]["fix"] and cov["listed_by_kind"] == {"dynamic_reference": 1}
    assert [s["name"] for s in cov["sources"]] == ["local"] and "1 open gap" in cov["summary"]
    assert "config" not in json.dumps(cov) and "credential" not in json.dumps(cov).lower().replace("credentials", "")
    assert ag.org.call("intel_coverage_gaps", kind="permission_denied")["gaps"] == []
    one = ag.org.call("intel_coverage_gaps", asset="etl")
    assert len(one["gaps"]) == 1
    assert ag.org.call("intel_coverage_gaps", source="nope", error=True)["code"] == "not_found"
    s = ag.org.call("intel_summary")
    assert s["assets"]["total"] >= 19 and s["assets"]["by_type"]["table"] >= 7 and s["edges"]["by_confidence"]["verified"] >= 1 and s["gaps"]["open"] == 1
    assert 0 < s["edges"]["verified_or_derived_pct"] <= 100 and s["discovery_installed"] is True and "lineage link" in s["summary"]


def test_unused_assets(ag):
    r = ag.org.call("intel_unused_assets", days=0)
    names = {i["name"] for i in r["unused"]}
    assert "lonely" in names and "mart" in names and r["caveat"]
    lonely = next(i for i in r["unused"] if i["name"] == "lonely")
    assert lonely["ever_read"] is False and lonely["workspace"] == "alpha"
    assert ag.org.call("intel_unused_assets", days=0, limit=2)["truncated"] is True
    assert ag.org.call("intel_unused_assets", days=30)["unused"] == []          # everything was created a moment ago


# ============================================================================================================================ visibility
def test_a_hidden_asset_does_not_exist_for_a_scoped_agent(ag):
    secret = ag.ids["secret"]
    ghost = "ast_" + "0" * 26
    hidden = ag.alpha.call("intel_get_asset", asset=secret, error=True)
    nothing = ag.alpha.call("intel_get_asset", asset=ghost, error=True)
    assert hidden["code"] == "not_found" and nothing["code"] == "not_found"
    assert hidden["message"].replace(secret, "X") == nothing["message"].replace(ghost, "X")      # indistinguishable
    assert ag.alpha.call("intel_search_assets", query="secret")["count"] == 0
    by_name = ag.alpha.call("intel_get_asset", asset="secret", error=True)
    assert by_name["code"] == "not_found" and "lakehouse_b" not in json.dumps(by_name)
    # the owner of the workspace sees it
    assert ag.beta.call("intel_get_asset", asset="secret")["workspace"] == "beta"
    assert ag.org.call("intel_get_asset", asset="secret")["workspace"] == "beta"


def test_analysis_leaves_out_what_the_caller_cannot_see(ag):
    org = ag.org.call("intel_impact_analysis", asset="clean")
    alpha = ag.alpha.call("intel_impact_analysis", asset="clean")
    assert "secret_out" in {c["name"] for c in org["consumers"]} and "beta_nb" in org["processes"]
    assert "secret_out" not in json.dumps(alpha) and "beta_nb" not in json.dumps(alpha) and "lakehouse_b" not in json.dumps(alpha)
    lin = ag.alpha.call("intel_lineage", asset="clean", direction="down", depth=3)
    assert "secret_out" not in json.dumps(lin) and "beta_nb" not in json.dumps(lin)
    # an organisation-level asset (the CRM source system) needs an organisation-wide reader
    assert ag.org.call("intel_get_asset", asset="crm_system")["type"] == "source_system"
    assert ag.alpha.call("intel_get_asset", asset="crm_system", error=True)["code"] == "not_found"
    origin = ag.alpha.call("intel_where_does_this_come_from", asset="sales_report")
    assert "crm_system" not in json.dumps(origin)
    # summary and unused are limited to the caller's workspaces
    assert ag.alpha.call("intel_summary")["assets"]["total"] < ag.org.call("intel_summary")["assets"]["total"]
    assert "secret" not in {i["name"] for i in ag.alpha.call("intel_unused_assets", days=0)["unused"]}
    # the ambiguity message of a scoped agent names only what it can see
    assert ag.alpha.call("intel_get_asset", asset="dup")["path"] == "lakehouse_fan/dup"


def test_a_token_scoped_agent_gets_the_same_answers_as_its_user(ag):
    """The token does not widen visibility: the administrator's MCP token sees everything, the workspace agent only its workspace."""
    assert ag.boss.call("intel_get_asset", asset="secret")["workspace"] == "beta"
    assert ag.alpha.call("intel_search_assets", query="lakehouse_b")["count"] == 0
    cov = ag.alpha.call("intel_coverage_gaps")
    assert all(g["asset"] != "beta_nb" for g in cov["gaps"])


# ============================================================================================================================ availability
def test_tools_are_listed_only_when_intelligence_is_installed(tmp_path):
    from fastapi.testclient import TestClient

    from olcf.api.app import create_app
    from olcf.core.config import load_settings
    from tests.intelligence.conftest import INTEL_MODULES

    s = load_settings(tmp_path / "data", environment="test")
    s.compute.embedded_worker = False
    app = create_app(s, module_names=[m for m in INTEL_MODULES if m != "olcf.intelligence.module"] + ["olcf.mcp.module"])
    with TestClient(app) as c:
        pw = app.state.ctx.settings.path("secrets", "bootstrap-admin.txt").read_text().split("password=")[1].strip()
        tok = c.post("/v1/identity/login", json={"username": "admin", "password": pw}).json()["access_token"]
        ident = app.state.ctx.service("identity")
        row = ident.create_principal("noint", kind="user", password=PASSWORD)
        ident.bind(row["id"], "Contributor", scope_type="org", scope_id="")
        pat = app.state.ctx.tokens.create_pat(row["id"], "p", scopes=["mcp"], ttl_days=1)[0]
        m = McpClient(c, {"Authorization": f"Bearer {pat}"})
        m.init()
        names = {t["name"] for t in m.result("tools/list")["tools"]}
        assert "run_sql" in names and not [n for n in names if n.startswith("intel_")]
        assert m.error("tools/call", {"name": "intel_summary", "arguments": {}})["code"] == -32602
        assert tok
