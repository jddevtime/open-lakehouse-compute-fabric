# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
# ruff: noqa: S105, S106
"""``olcf intelligence ...`` through the real command tree (click's CliRunner) against a live in-process application with a populated asset graph.

The CLI builds its SDK client from the saved context; here that factory is replaced by one that returns an SDK client bound to the in-process transport, so every command
runs the full path CLI -> SDK -> HTTP -> API -> service -> database.  Covered: ``--help`` with examples for every command, human and machine output (``--json``, ``-o json|yaml|csv``),
the exact lineage tree, column lineage, ambiguity and not-found handling and their exit codes, permission failures, the Fabric source flow against ``FakeFabricWorld``
(add, test with and without ``--test-only``, a failing test with its ``fix:`` line, scan ``--wait``) with the guarantee that the secret never reaches any output, scanner import,
sampling policies and a sample read through a real local scan.
"""

from __future__ import annotations

import json
import os
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import click
import pytest
from click.testing import CliRunner, Result

import olcf.cli.state as cli_state
from olcf.cli.main import cli
from olcf.sdk import Client
from tests.fabric.fabric_fakes import (
    BROKER_CLIENT_ID,
    BROKER_SECRET,
    FABRIC_WS,
    TENANT,
    FakeFabricWorld,
    allow_private_endpoints,
    pbi_dataset,
    pbi_table,
    scanner_workspace,
)
from tests.intelligence.surfaces_support import Stack, intel_stack

EXIT_FAIL, EXIT_USAGE, EXIT_AUTH, EXIT_DENIED, EXIT_NOT_FOUND, EXIT_CONFLICT_OR_INVALID, EXIT_AMBIGUOUS = 1, 2, 3, 4, 5, 6, 7


class Cli:
    """Runs ``olcf`` commands in-process as one of the world's callers and remembers everything that was printed (to prove a secret never appears)."""

    def __init__(self, stack: Stack) -> None:
        self.stack = stack
        self.who = "admin"
        self.runner = CliRunner()
        self.transcript: list[str] = []

    def client(self, *a: Any, **k: Any) -> Client:
        if self.who == "anon":
            return Client("http://testserver", http_client=self.stack.http, token="olcf_pat_bogus", max_retries=0)
        return self.stack.sdk(self.who)

    def run(self, *args: str, who: str = "admin", expect: int | None = 0, input: str | bytes | None = None, env: dict[str, str] | None = None) -> Result:  # noqa: A002
        self.who = who
        res = self.runner.invoke(cli, list(args), input=input, env=env, catch_exceptions=False, prog_name="olcf")
        self.transcript.append(res.output)
        if expect is not None:
            assert res.exit_code == expect, f"olcf {' '.join(args)} -> exit {res.exit_code}, expected {expect}\n{res.output}"
        return res

    def json(self, *args: str, who: str = "admin") -> Any:
        return json.loads(self.run("--json", *args, who=who).stdout)

    def lines(self, *args: str, who: str = "admin") -> list[str]:
        return [ln.rstrip() for ln in self.run(*args, who=who).stdout.splitlines()]


@pytest.fixture(scope="module")
def cl(tmp_path_factory) -> Iterator[Cli]:
    cfg = tmp_path_factory.mktemp("cli_config")
    mp = pytest.MonkeyPatch()
    mp.setenv("NO_COLOR", "1")
    mp.setenv("OLCF_CONFIG_DIR", str(cfg))
    for var in ("OLCF_URL", "OLCF_TOKEN", "OLCF_FABRIC_CLIENT_SECRET", "OLCF_FABRIC_SAMPLING_CLIENT_SECRET"):
        mp.delenv(var, raising=False)
    with intel_stack(tmp_path_factory.mktemp("cli_intel")) as stack:
        c = Cli(stack)
        mp.setattr(cli_state, "Client", c.client)
        try:
            yield c
        finally:
            mp.undo()


@pytest.fixture()
def fabric_world(cl) -> Iterator[FakeFabricWorld]:
    world = FakeFabricWorld().start()
    allow_private_endpoints(cl.stack.ctx)                   # the fake tenant listens on 127.0.0.1
    lakehouse = "aaaaaaaa-0000-4000-8000-0000000000a1"
    world.workspaces = [{"id": FABRIC_WS, "displayName": "Sales", "type": "Workspace"}]
    world.add_item(FABRIC_WS, lakehouse, "lh_sales", "Lakehouse")
    world.add_lakehouse_table(FABRIC_WS, lakehouse, "orders", schema=[("id", "long"), ("email", "string")], commits=2)
    world.throttle_retry_after = "0"
    try:
        yield world
    finally:
        world.stop()


def commands(group: click.Group, path: tuple[str, ...] = ()) -> Iterator[tuple[tuple[str, ...], click.Command]]:
    """Every command of a click tree with its path."""
    for name, sub in group.commands.items():
        yield (*path, name), sub
        if isinstance(sub, click.Group):
            yield from commands(sub, (*path, name))


# ============================================================================================================================ help
def test_every_intelligence_command_has_help_with_examples(cl):
    from olcf.cli.commands.intelligence import intelligence_group

    seen = list(commands(intelligence_group, ("intelligence",)))
    assert len(seen) >= 38
    for path, sub in seen:
        out = cl.run(*path, "--help").stdout
        assert "Usage: olcf " + " ".join(path) in out, path
        assert "Example" in out and f"olcf {' '.join(path[:2])}" in out, f"{' '.join(path)}: help has no example"
        assert (sub.help or "").strip(), path
    top = cl.run("intelligence", "--help").stdout
    for name in ("search", "lineage", "where-from", "impact", "sources", "scans", "coverage", "gaps", "sampling-policy", "import", "assets", "summary", "diff", "snapshots", "unused"):
        assert name in top
    assert "documented exit codes" in top and "--json" in top
    root = cl.run("--help").stdout
    assert "intelligence" in root


def test_list_commands_take_the_documented_paging_options(cl):
    out = cl.run("intelligence", "assets", "list", "--help").stdout
    for opt in ("--limit", "--all", "--json", "--type", "--classification", "--status"):
        assert opt in out
    assert "--secret-stdin" in cl.run("intelligence", "sources", "add", "--help").stdout and "--test-only" in cl.run("intelligence", "sources", "add", "--help").stdout


# ============================================================================================================================ reading the graph
def test_summary_human_and_machine_output(cl):
    human = cl.run("intelligence", "summary").stdout
    assert "assets:" in human and "open gaps:    1  (dynamic_reference 1)" in human and "governance:   yes" in human and "SOURCE" in human
    data = cl.json("intelligence", "summary")
    assert data["assets"]["total"] >= 19 and data["gaps"]["open"] == 1 and data["edges"]["by_confidence"]["verified"] >= 1
    via_o = json.loads(cl.run("-o", "json", "intelligence", "summary").stdout)
    via_flag = json.loads(cl.run("intelligence", "summary", "--json").stdout)
    assert via_o["assets"] == via_flag["assets"] and via_o["edges"] == via_flag["edges"]
    yml = cl.run("-o", "yaml", "intelligence", "summary").stdout
    assert yml.startswith("assets:") or "\nassets:" in yml


def test_search_and_assets_list_and_show(cl):
    out = cl.lines("intelligence", "search", "clean")
    assert out[0].split() == ["ID", "TYPE", "NAME", "PATH", "WORKSPACE", "STATUS"]
    assert any(ln.split()[1:4] == ["table", "clean", "lakehouse_a/clean"] for ln in out[1:])
    assert cl.run("intelligence", "search", "zzz-no-such-thing").stdout.strip() == "nothing matches 'zzz-no-such-thing'"
    assert [a["name"] for a in cl.json("intelligence", "search", "clean", "--type", "table")] == ["clean"]
    rows = cl.json("intelligence", "assets", "list", "--type", "report")
    assert [a["name"] for a in rows] == ["sales_report"]
    several = {a["type"] for a in cl.json("intelligence", "assets", "list", "--type", "report,semantic_model")}
    assert several == {"report", "semantic_model"}
    page = cl.json("intelligence", "assets", "list", "--limit", "3")
    everything = cl.json("intelligence", "assets", "list", "--all")
    assert len(page) == 3 and len(everything) >= 19 and [a["id"] for a in page] == [a["id"] for a in everything[:3]]
    csv_out = cl.run("-o", "csv", "intelligence", "assets", "list", "--type", "report").stdout.splitlines()
    assert csv_out[0].startswith("ID,TYPE,NAME") or "id" in csv_out[0].lower()
    show = cl.run("intelligence", "assets", "show", "lakehouse_a/clean").stdout
    assert "name:          clean" in show and "path:          lakehouse_a/clean" in show and "lineage edges: 1 upstream, 2 downstream" in show
    assert "COLUMN" in show and "amount  double" in show and "ALIAS KIND" in show
    j = cl.json("intelligence", "assets", "show", "lakehouse_a/clean")
    assert j["id"] == cl.stack.ids["clean"] and j["child_count"] == 2
    local_json = cl.run("intelligence", "assets", "show", "clean", "--json").stdout
    assert json.loads(local_json)["id"] == j["id"]
    kids = cl.json("intelligence", "assets", "children", "lakehouse_a", "--child-type", "table")
    assert {"raw", "clean", "mart", "lonely", "fresh"} <= {k["name"] for k in kids}


def test_asset_references_resolve_by_id_name_path_and_column(cl):
    clean = cl.stack.ids["clean"]
    for ref in (clean, "clean", "lakehouse_a/clean", "lakehouse_a.clean", "CLEAN"):
        assert cl.json("intelligence", "assets", "show", ref)["id"] == clean, ref
    col = cl.json("intelligence", "assets", "show", "clean.amount")
    assert col["type"] == "column" and col["id"] == cl.stack.ids["clean.amount"]


def test_ambiguous_and_missing_assets_have_distinct_exit_codes_and_hints(cl):
    amb = cl.run("intelligence", "assets", "show", "amount", expect=EXIT_AMBIGUOUS)
    assert "'amount' matches 2 assets" in amb.output and "hint:" in amb.output and cl.stack.ids["clean.amount"] in amb.output and cl.stack.ids["raw.amount"] in amb.output
    missing = cl.run("intelligence", "lineage", "nosuch", expect=EXIT_NOT_FOUND)
    assert "no asset matches 'nosuch'" in missing.output and "olcf intelligence search" in missing.output
    assert cl.run("intelligence", "where-from", "ast_00000000000000000000000000", expect=EXIT_NOT_FOUND).exit_code == EXIT_NOT_FOUND
    bad_option = cl.run("intelligence", "lineage", "clean", "--direction", "sideways", expect=EXIT_USAGE)
    assert "Invalid value for '--direction'" in bad_option.output
    assert cl.run("intelligence", "lineage", expect=EXIT_USAGE).exit_code == EXIT_USAGE            # missing argument
    # JSON mode keeps stdout clean on errors: the message goes to stderr
    err = cl.run("--json", "intelligence", "assets", "show", "nosuch", expect=EXIT_NOT_FOUND)
    assert err.stdout.strip() == "" and "no asset matches" in err.stderr


LINEAGE_CLEAN = [
    "clean (table)  lakehouse_a/clean",
    "├── upstream (flows into it)",
    "│   └── <- etl (notebook)  [verified]",
    "│       ├── <- crm_system (source_system)  [inferred]",
    "│       └── <- raw (table)  [derived]",
    "└── downstream (it flows into)",
    "    ├── -> agg (notebook)  [derived]",
    "    │   └── -> mart (table)  [derived]",
    "    │       └── -> sales_model (semantic_model)  [derived]",
    "    └── -> beta_nb (notebook)  [derived]",
    "        └── -> secret_out (table)  [derived]",
    "lineage is incomplete: 1 gap(s) (dynamic_reference x1); see `olcf intelligence gaps`",
]


def test_lineage_tree_is_exact(cl):
    assert cl.lines("intelligence", "lineage", "clean", "--depth", "2") == LINEAGE_CLEAN
    assert cl.lines("intelligence", "assets", "lineage", "clean", "--depth", "2") == LINEAGE_CLEAN                 # the same command under `assets`
    up = cl.lines("intelligence", "lineage", "clean", "--direction", "up", "--depth", "2")
    assert up == [
        "clean (table)  lakehouse_a/clean",
        "└── upstream (flows into it)",
        "    └── <- etl (notebook)  [verified]",
        "        ├── <- crm_system (source_system)  [inferred]",
        "        └── <- raw (table)  [derived]",
        "lineage is incomplete: 1 gap(s) (dynamic_reference x1); see `olcf intelligence gaps`",
    ]
    strong = "\n".join(cl.lines("intelligence", "lineage", "clean", "--depth", "2", "--min-confidence", "derived"))
    assert "crm_system" not in strong and "raw (table)" in strong
    ids = "\n".join(cl.lines("intelligence", "lineage", "clean", "--depth", "1", "--ids"))
    assert cl.stack.ids["raw"] in ids and cl.stack.ids["etl"] in ids
    tables = "\n".join(cl.lines("intelligence", "lineage", "clean", "--depth", "3", "--types", "table"))
    assert "(notebook)" not in tables and "(table)" in tables
    leaf = cl.lines("intelligence", "lineage", "lonely")
    assert leaf[0] == "lonely (table)  lakehouse_a/lonely" and leaf[1:] == ["├── upstream (flows into it): none found", "└── downstream (it flows into): none found"]


def test_lineage_json_matches_the_api(cl):
    doc = cl.json("intelligence", "lineage", "clean", "--depth", "2")
    api = cl.stack.sdk().intelligence.lineage(cl.stack.ids["clean"], depth=2)
    assert doc["root"]["id"] == api["root"]["id"] and {n["id"] for n in doc["nodes"]} == {n["id"] for n in api["nodes"]}
    assert doc["stats"]["edges"] == api["stats"]["edges"] and doc["gaps"][0]["kind"] == "dynamic_reference"


def test_column_lineage(cl):
    out = cl.lines("intelligence", "lineage", "clean.amount", "--level", "column")
    assert out[0] == "Column lineage around clean.amount (1 edge(s), direction both, depth 3)"
    assert out[1].split() == ["DIRECTION", "FROM", "TO", "VIA", "TRANSFORMATION", "CONFIDENCE"]
    assert out[2].split(None, 2)[:2] == ["upstream", "raw.amount"] and "amount * 1.0" in out[2] and out[2].endswith("derived")
    same = cl.lines("intelligence", "lineage", "clean", "--column", "amount")
    assert same[:3] == out[:3]
    down = cl.json("intelligence", "lineage", "raw.amount", "--level", "column", "--direction", "down")
    assert down["level"] == "column" and [e["type"] for e in down["edges"]] == ["TRANSFORMS"]
    origin = cl.lines("intelligence", "where-from", "clean.amount", "--level", "column")
    assert origin[0] == "Where clean.amount (column) comes from (1 origin(s), 1 path(s)):" and "  raw.amount -> clean.amount  [derived, 1 hop(s)]" in origin


def test_where_from_impact_staleness_docs_history(cl):
    out = cl.run("intelligence", "where-from", "sales_report").stdout
    assert out.startswith("Where sales_report (report) comes from (2 origin(s), 2 path(s)):")
    assert "crm_system" in out and "source_system" in out and "yes" in out.split("crm_system")[1].splitlines()[0]
    assert "  crm_system -> etl -> clean -> agg -> mart -> sales_model -> sales_report  [inferred, 4 hop(s), via etl, agg]" in out
    assert "lineage is incomplete" in out
    j = cl.json("intelligence", "assets", "origin", "sales_report")
    assert {r["asset"]["name"] for r in j["roots"]} == {"crm_system", "raw"}
    imp = cl.run("intelligence", "impact", "raw").stdout
    assert imp.startswith("If raw (table) changes, 5 downstream asset(s) are affected (table 3, semantic_model 1, report 1).")
    assert "sales_report" in imp and "Processes that read it: etl (notebook), agg (notebook), beta_nb (notebook)" in imp and "owners are unknown" in imp
    assert cl.json("intelligence", "assets", "impact", "raw")["total"] == 5
    st = cl.run("intelligence", "assets", "staleness", "clean").stdout
    assert "stale:       no" in st and "PRODUCER" in st
    only = cl.run("intelligence", "assets", "staleness", "clean", "--stale-only").stdout
    assert "nothing stale" in only
    docs = cl.run("intelligence", "assets", "docs", "clean").stdout
    assert docs.startswith("# clean") and "## Schema" in docs
    hist = cl.lines("intelligence", "assets", "history", "raw")
    assert hist[0].split() == ["WHEN", "SCAN", "CHANGES"] and "+region; id: int -> bigint" in hist[1] and hist[2].endswith("created")
    assert len(cl.json("intelligence", "assets", "history", "raw", "--schema-only")) >= 1


def test_snapshots_diff_unused_gaps_coverage(cl):
    snaps = cl.lines("intelligence", "snapshots")
    assert snaps[0].split()[:3] == ["SNAPSHOT", "SOURCE", "VERSION"] and "+1 assets, 1 updated, +0 edges" in snaps[1] and "+18 assets, 0 updated, +12 edges" in snaps[2]
    assert cl.run("intelligence", "snapshots", "--status", "failed").stdout.strip() == "no snapshots yet"
    diff = cl.run("intelligence", "diff").stdout
    assert "(v1) -> " in diff and "(v2) of source local" in diff and "+ table lakehouse_a/fresh" in diff and "~ table lakehouse_a/raw: +region; id: int -> bigint" in diff
    both = cl.json("intelligence", "snapshots")
    explicit = cl.run("intelligence", "diff", both[1]["id"], both[0]["id"]).stdout
    assert explicit == diff
    assert cl.run("intelligence", "diff", both[0]["id"], expect=EXIT_USAGE).exit_code == EXIT_USAGE
    unused = cl.json("intelligence", "unused", "--days", "0")
    assert {"lonely", "mart"} <= {u["name"] for u in unused}
    assert cl.json("intelligence", "unused", "--days", "30") == []
    assert cl.run("intelligence", "unused", "--days", "30").stdout.strip() == "no unused tables in that window"
    gaps = cl.lines("intelligence", "gaps")
    assert gaps[0].split()[:2] == ["KIND", "SUBJECT"] and "dynamic_reference" in gaps[1] and "use a literal table name" in gaps[1]
    assert cl.json("intelligence", "gaps", "--kind", "permission_denied") == []
    assert [g["kind"] for g in cl.json("intelligence", "gaps", "--asset", "etl")] == ["dynamic_reference"]
    cov = cl.run("intelligence", "coverage").stdout
    assert "SOURCE" in cov and "sampling: off" in cov and "connectors:" in cov
    assert cl.json("intelligence", "coverage")["sampling"]["enabled"] is False


# ============================================================================================================================ permissions and authentication
def test_permission_failures_exit_4_and_hidden_assets_exit_5(cl):
    for args in (("sources", "add", "viewer-src", "--kind", "local"), ("sampling-policy", "set", "--scope", "global", "--max-rows", "5"), ("sources", "scan", "local"), ("scans", "cancel", "scn_x"),
                 ("sampling-policy", "list")):
        res = cl.run("intelligence", *args, who="alpha", expect=None)
        assert res.exit_code == EXIT_DENIED, (args, res.exit_code, res.output)
        assert "intelligence.manage" in res.output
    assert cl.run("intelligence", "sources", "list", who="alpha").stdout.strip() == "no sources"                # sources are only listed to those who may manage them
    # a viewer of workspace alpha cannot see beta's assets: they do not exist for it
    assert cl.run("intelligence", "assets", "show", cl.stack.ids["secret"], who="alpha", expect=EXIT_NOT_FOUND).exit_code == EXIT_NOT_FOUND
    assert cl.run("intelligence", "search", "secret", who="alpha").stdout.strip() == "nothing matches 'secret'"
    assert "beta_nb" not in cl.run("intelligence", "impact", "clean", who="alpha").stdout
    assert cl.run("intelligence", "assets", "show", "clean", who="none", expect=EXIT_DENIED).exit_code == EXIT_DENIED


def test_bad_token_is_an_authentication_error_and_not_echoed(cl):
    res = cl.run("intelligence", "summary", who="anon", expect=EXIT_AUTH)
    assert "olcf_pat_bogus" not in res.output


# ============================================================================================================================ sources
def test_local_and_mirror_sources_lifecycle(cl):
    listed = cl.json("intelligence", "sources", "list")
    assert any(s["kind"] == "local" and s["system"] for s in listed)
    human = cl.lines("intelligence", "sources", "list")
    assert human[0].split()[:3] == ["NAME", "KIND", "ENABLED"] and any(ln.startswith("local") for ln in human)
    out = cl.run("intelligence", "sources", "add", "mirror", "--kind", "catalog_mirror", "--incremental-every", "3600", "--full-every", "0").stdout
    assert "created catalog_mirror source mirror (src_" in out
    shown = cl.run("intelligence", "sources", "show", "mirror").stdout
    assert "name:               mirror" in shown and "credentials stored: no" in shown and "kind:               catalog_mirror" in shown
    assert cl.json("intelligence", "sources", "show", "mirror")["schedule"]["incremental_seconds"] == 3600
    assert [s["name"] for s in cl.json("intelligence", "sources", "list", "--kind", "catalog_mirror")] == ["mirror"]
    dup = cl.run("intelligence", "sources", "add", "mirror", "--kind", "catalog_mirror", expect=None)
    assert dup.exit_code == EXIT_CONFLICT_OR_INVALID
    test = cl.run("intelligence", "sources", "test", "mirror").stdout
    assert "[ ok ]" in test and test.rstrip().endswith("connectivity OK")
    ask = cl.run("intelligence", "sources", "delete", "mirror", input="n\n", expect=None)
    assert ask.exit_code != 0 and cl.json("intelligence", "sources", "show", "mirror")["name"] == "mirror"            # not confirmed: still there
    assert "deleted source mirror" in cl.run("intelligence", "sources", "delete", "mirror", "--yes").stdout
    assert cl.run("intelligence", "sources", "show", "mirror", expect=EXIT_NOT_FOUND).exit_code == EXIT_NOT_FOUND
    builtin = cl.run("intelligence", "sources", "delete", "local", "--yes", expect=None)
    assert builtin.exit_code != 0 and any(s["kind"] == "local" for s in cl.json("intelligence", "sources", "list"))


def fabric_args(world: FakeFabricWorld, tmp_path: Path, name: str = "tenant") -> list[str]:
    net = tmp_path / f"{name}-net.json"
    net.write_text(json.dumps({"network": world.discovery_network_config()}))
    return ["intelligence", "sources", "add", name, "--kind", "fabric", "--tenant-id", TENANT, "--client-id", BROKER_CLIENT_ID, "--config-file", str(net)]


def test_fabric_source_flow_add_test_scan_and_the_secret_never_appears(cl, fabric_world, tmp_path):
    env = {"OLCF_FABRIC_CLIENT_SECRET": BROKER_SECRET}
    dry = cl.run(*fabric_args(fabric_world, tmp_path), "--test-only", env=env).stdout
    assert dry.count("[ ok ]") == 6 and "[skip] sampling identity probe" in dry and dry.rstrip().endswith("connectivity OK")
    assert not [s for s in cl.json("intelligence", "sources", "list") if s["name"] == "tenant"]                      # --test-only saved nothing
    created = cl.run(*fabric_args(fabric_world, tmp_path), env=env)
    assert "created fabric source tenant (src_" in created.stdout and "olcf intelligence sources test tenant" in created.stdout
    shown = cl.run("intelligence", "sources", "show", "tenant").stdout
    assert "credentials stored: yes" in shown and BROKER_SECRET not in shown
    stored = json.dumps(cl.json("intelligence", "sources", "show", "tenant"))
    assert BROKER_SECRET not in stored and "client_secret" not in stored
    test = cl.run("intelligence", "sources", "test", "tenant").stdout
    assert test.splitlines()[0].startswith("1. [ ok ] resolve credentials") and "value not shown" in test and test.rstrip().endswith("connectivity OK")
    scan = cl.run("intelligence", "sources", "scan", "tenant", "--mode", "full", "--wait", "--timeout", "120")
    assert "==> scan scn_" in scan.output and "status:   succeeded" in scan.stdout and "assets:   7 created, 1 updated, 0 unchanged" in scan.stdout
    found = cl.json("intelligence", "search", "orders")
    assert {a["type"] for a in found} == {"table", "column"} and any(a["path"] == "Sales / lh_sales / orders" for a in found)
    cov = cl.json("intelligence", "coverage")
    fab = next(s for s in cov["sources"] if s["name"] == "tenant")
    assert fab["last_status"] == "succeeded" and fab["assets"]["active"] == 7
    scans = cl.json("intelligence", "scans", "list", "--source", "tenant")
    assert scans[0]["status"] == "succeeded" and cl.run("intelligence", "scans", "show", scans[0]["id"]).stdout.count("succeeded") >= 1
    assert cl.run("intelligence", "scans", "cancel", scans[0]["id"], expect=None).exit_code != 0              # a finished scan cannot be cancelled
    json_scan = json.loads(cl.run("intelligence", "sources", "scan", "tenant", "--wait", "--timeout", "120", "--json").stdout)
    assert json_scan["status"] == "succeeded" and json_scan["mode"] == "incremental"
    assert "deleted source tenant" in cl.run("intelligence", "sources", "delete", "tenant", "--yes").stdout
    assert BROKER_SECRET not in "\n".join(cl.transcript)


def test_failing_connectivity_test_prints_a_fix_and_exits_1_without_leaking_the_secret(cl, fabric_world, tmp_path):
    wrong = "wrong-secret-value-0123456789"
    res = cl.run(*fabric_args(fabric_world, tmp_path, "bad"), "--test-only", env={"OLCF_FABRIC_CLIENT_SECRET": wrong}, expect=EXIT_FAIL)
    lines = res.stdout.splitlines()
    assert lines[0].startswith("1. [ ok ] resolve credentials") and lines[1].startswith("2. [FAIL] acquire Fabric token")
    assert lines[2].startswith("        fix: ") and "client secret" in lines[2].lower()
    assert all("skipped because an earlier step failed" in ln for ln in lines[3:9]) and res.stdout.rstrip().endswith("connectivity FAILED at step: acquire Fabric token")
    as_json = json.loads(cl.run("--json", *fabric_args(fabric_world, tmp_path, "bad"), "--test-only", env={"OLCF_FABRIC_CLIENT_SECRET": wrong}, expect=EXIT_FAIL).stdout)
    assert as_json["ok"] is False and as_json["failed_step"] == "acquire Fabric token" and as_json["steps"][1]["remediation"]
    # a saved source whose secret is wrong fails `sources test` the same way
    cl.run(*fabric_args(fabric_world, tmp_path, "bad"), env={"OLCF_FABRIC_CLIENT_SECRET": wrong})
    again = cl.run("intelligence", "sources", "test", "bad", expect=EXIT_FAIL)
    assert "[FAIL] acquire Fabric token" in again.stdout and "fix:" in again.stdout
    cl.run("intelligence", "sources", "delete", "bad", "--yes")
    assert wrong not in "\n".join(cl.transcript)


def test_secret_from_stdin_missing_secret_and_certificate_options(cl, fabric_world, tmp_path):
    out = cl.run(*fabric_args(fabric_world, tmp_path, "viastdin"), "--secret-stdin", input=BROKER_SECRET + "\n")
    assert "created fabric source viastdin" in out.stdout and BROKER_SECRET not in out.output
    assert cl.run("intelligence", "sources", "test", "viastdin").stdout.rstrip().endswith("connectivity OK")
    cl.run("intelligence", "sources", "delete", "viastdin", "--yes")
    # no secret in the environment and nothing on stdin: the command asks (here it gets an empty answer) and refuses
    nothing = cl.run(*fabric_args(fabric_world, tmp_path, "nosecret"), "--test-only", input="", expect=None)
    assert nothing.exit_code != 0 and BROKER_SECRET not in nothing.output
    incomplete = cl.run("intelligence", "sources", "add", "x", "--kind", "fabric", expect=EXIT_USAGE)
    assert "--connection-id, or --tenant-id and --client-id" in incomplete.output
    pem = tmp_path / "key.pem"
    pem.write_text("-----BEGIN PRIVATE KEY-----\nAAAA\n-----END PRIVATE KEY-----\n")
    half = cl.run(*fabric_args(fabric_world, tmp_path, "certless"), "--private-key-file", str(pem), "--test-only", expect=None)
    assert half.exit_code != 0 and "AAAA" not in half.output
    no_file = cl.run("intelligence", "sources", "add", "p", "--kind", "scanner_import", expect=EXIT_USAGE)
    assert "--file RESULT.json" in no_file.output
    assert "--test-only does not apply" in cl.run("intelligence", "sources", "add", "p", "--kind", "scanner_import", "--file", str(pem), "--test-only", expect=EXIT_USAGE).output


# ============================================================================================================================ scanner import
def scanner_document() -> dict[str, Any]:
    return {"workspaces": [scanner_workspace(FABRIC_WS, "Imported", datasets=[pbi_dataset("d1d1d1d1-0000-4000-8000-000000000001", "Imported model", [pbi_table("t", [("c", "Int64")])])])],
            "datasourceInstances": []}


def test_import_scanner_command_and_scanner_import_source(cl, tmp_path):
    f = tmp_path / "scan.json"
    f.write_text(json.dumps(scanner_document()))
    out = cl.run("intelligence", "import", "scanner", str(f), "--name", "cli-import", "--wait").stdout
    assert "imported 1 workspace(s), 0 data source instance(s) into cli-import (src_" in out and "; scan scn_" in out and out.rstrip().endswith("succeeded")
    assert cl.json("intelligence", "sources", "show", "cli-import")["kind"] == "scanner_import"
    assert any(a["name"] == "Imported model" for a in cl.json("intelligence", "search", "Imported model"))
    res = cl.json("intelligence", "import", "scanner", str(f), "--name", "cli-import", "--no-scan")
    assert res["scan"] is None and res["workspaces"] == 1
    via_source = cl.run("intelligence", "sources", "add", "cli-import-2", "--kind", "scanner_import", "--file", str(f), "--no-scan").stdout
    assert "imported 1 workspace(s), 0 data source instance(s) into cli-import-2 (src_" in via_source
    stdin = cl.run("intelligence", "import", "scanner", "-", "--name", "cli-import-3", "--no-scan", input=f.read_bytes()).stdout
    assert "imported 1 workspace(s), 0 data source instance(s) into cli-import-3 (src_" in stdin
    poisoned = scanner_document()
    poisoned["workspaces"][0]["connectionString"] = "Server=x;Password=hunter2hunter2;"
    bad = tmp_path / "bad.json"
    bad.write_text(json.dumps(poisoned))
    refused = cl.run("intelligence", "import", "scanner", str(bad), "--name", "cli-poisoned", expect=None)
    assert refused.exit_code != 0 and "credential" in refused.output.lower() and "hunter2hunter2" not in refused.output
    broken = tmp_path / "broken.json"
    broken.write_text("{not json")
    assert cl.run("intelligence", "import", "scanner", str(broken), "--name", "cli-broken", expect=None).exit_code != 0
    for name in ("cli-import", "cli-import-2", "cli-import-3"):
        cl.run("intelligence", "sources", "delete", name, "--yes")


# ============================================================================================================================ sampling
def test_sampling_policies_and_a_sample_through_a_real_local_scan(cl):
    import pyarrow as pa

    admin = cl.stack.sdk()
    ws = admin.workspaces.create("cli-sample-ws")
    lh = admin.lakehouses.create(ws.id, "clilh")
    svc = cl.stack.ctx.service("storage")
    svc.tables.write_table(svc.get_lakehouse(lh.id), "people", pa.table({"id": pa.array([1, 2, 3], pa.int64()), "email": ["alice@example.org", "bob@example.org", "carol@example.org"]}))
    scan = cl.run("intelligence", "sources", "scan", "local", "--mode", "full", "--wait", "--timeout", "120")
    assert "status:   succeeded" in scan.stdout or "status:   partial" in scan.stdout
    assert cl.run("intelligence", "sampling-policy", "list").stdout.strip() == "no sampling policies: sampling is off everywhere"
    off = cl.run("intelligence", "assets", "sample", "clilh/people", expect=EXIT_DENIED)
    assert "sampling" in off.output.lower() and "alice@example.org" not in off.output
    out = cl.run("intelligence", "sampling-policy", "set", "--scope", "workspace", "--scope-id", "cli-sample-ws", "--max-rows", "2", "--masking", "redact").stdout
    assert "sampling policy" in out.lower() or "plc" in out or "workspace" in out
    pols = cl.json("intelligence", "sampling-policy", "list")
    assert len(pols) == 1 and pols[0]["scope_type"] == "workspace" and pols[0]["max_rows"] == 2 and pols[0]["masking"] == "redact"
    human = cl.lines("intelligence", "sampling-policy", "list", "--scope", "workspace")
    assert human[0].split()[0] in ("ID", "SCOPE", "POLICY") and any("redact" in ln for ln in human)
    sample = cl.json("intelligence", "assets", "sample", "clilh/people", "--rows", "5")
    assert sample["row_count"] <= 2
    flat = json.dumps(sample)
    assert "alice@example.org" not in flat and "bob@example.org" not in flat
    table = cl.run("intelligence", "assets", "sample", "clilh/people", "--column", "id").stdout
    assert "id" in table.splitlines()[0] and "email" not in table.splitlines()[0]
    # 0 rows turns the scope off again, deleting removes it
    cl.run("intelligence", "sampling-policy", "set", "--scope", "workspace", "--scope-id", "cli-sample-ws", "--max-rows", "0")
    cl.run("intelligence", "assets", "sample", "clilh/people", expect=EXIT_DENIED)
    pid = cl.json("intelligence", "sampling-policy", "list")[0]["id"]
    assert cl.run("intelligence", "sampling-policy", "delete", pid, "--yes").exit_code == 0
    assert cl.json("intelligence", "sampling-policy", "list") == []
    assert cl.run("intelligence", "sampling-policy", "delete", pid, "--yes", expect=EXIT_NOT_FOUND).exit_code == EXIT_NOT_FOUND
    bad_scope = cl.run("intelligence", "sampling-policy", "set", "--scope", "workspace", "--max-rows", "2", expect=None)
    assert bad_scope.exit_code != 0
    too_many = cl.run("intelligence", "sampling-policy", "set", "--scope", "global", "--max-rows", "101", expect=EXIT_USAGE)
    assert "101" in too_many.output
    assert os.environ.get("OLCF_FABRIC_CLIENT_SECRET") is None


def test_inbox_requeue_command(cl):
    out = cl.run("intelligence", "inbox", "requeue").stdout
    assert "0 failed event(s) queued again" in out and "pending" in out and "failed" in out
    doc = cl.json("intelligence", "inbox", "requeue")
    assert doc["requeued"] == 0 and set(doc["inbox"]) >= {"pending", "done", "failed", "skipped"}
    assert "Example" in cl.run("intelligence", "inbox", "requeue", "--help").stdout
    assert cl.run("intelligence", "inbox", "requeue", who="alpha", expect=EXIT_DENIED).exit_code == EXIT_DENIED
