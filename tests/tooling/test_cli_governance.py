# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
# ruff: noqa: F811
"""``olcf governance ...`` through click's CliRunner against the real application running in-process: every command group is exercised on the populated asset graph
(table output, ``--json``, exit codes for denied / missing / ambiguous / conflicting requests, ``--if-match``, the confirmation guard of deletes) and the output of the
important views (exposure, quality, review queue) is asserted line by line."""

from __future__ import annotations

import json
import re

import click

from olcf.cli.main import cli  # noqa: F401  (registers the commands)
from tests.tooling.test_cli_intelligence import (  # noqa: F401
    EXIT_AMBIGUOUS,
    EXIT_AUTH,
    EXIT_DENIED,
    EXIT_FAIL,
    EXIT_NOT_FOUND,
    EXIT_USAGE,
    cl,
    commands,
)

EXIT_CONFLICT, EXIT_INVALID = 6, 7
ID = re.compile(r"\((?P<kind>trm|dom|dpr|cls|plc|chg|vio)_[0-9A-Z]{26}\)")


# ============================================================================================================================ help
def test_every_governance_command_has_help_with_examples(cl):
    from olcf.cli.commands.governance import governance_group

    seen = list(commands(governance_group, ("governance",)))
    assert len(seen) >= 75
    for path, sub in seen:
        out = cl.run(*path, "--help").stdout
        assert "Usage: olcf " + " ".join(path) in out, path
        assert "Example" in out and f"olcf {' '.join(path[:2])}" in out, f"{' '.join(path)}: help has no example"
        assert (sub.help or "").strip(), path
        if not isinstance(sub, click.Group):
            assert "--json" in out, f"{' '.join(path)} lacks --json"
    top = cl.run("governance", "--help").stdout
    for name in ("summary", "asset", "terms", "domains", "products", "owners", "classifications", "review-queue", "certify", "requests", "policies", "violations", "quality", "exposure"):
        assert name in top
    assert "documented exit codes" in top
    assert "governance" in cl.run("--help").stdout


# ============================================================================================================================ glossary
def test_terms_lifecycle(cl):
    created = cl.run("governance", "terms", "create", "Net revenue", "--definition", "Revenue after returns", "--abbreviation", "NR", "--synonym", "turnover", "--synonym", "net sales")
    assert re.fullmatch(r"OK: created term Net revenue \(trm_[0-9A-Z]{26}\), status draft\n", created.stdout)
    term_id = re.search(r"(trm_[0-9A-Z]{26})", created.stdout).group(1)
    dup = cl.run("governance", "terms", "create", "net REVENUE", expect=EXIT_CONFLICT)
    assert "409 conflict" in dup.stderr and dup.stdout == ""

    show = cl.run("governance", "terms", "show", "net revenue").stdout.splitlines()                     # a name is case-insensitive, an id works too
    assert show[:8] == [f"id:            {term_id}", "name:          Net revenue", "glossary:      default", "status:        draft", "certification: none",
                        "abbreviation:  NR", "synonyms:      turnover, net sales", "hierarchy:     -"] and show[-1] == "Revenue after returns"
    assert cl.json("governance", "terms", "show", "NR")["id"] == term_id                                # an exact abbreviation names a term, too
    assert cl.json("governance", "terms", "show", "turnover")["id"] == term_id                          # ... and an exact synonym

    listing = cl.lines("governance", "terms", "list")
    assert listing[0].split() == ["ID", "NAME", "ABBR", "STATUS", "CERTIFICATION", "LINKS", "DEFINITION"]
    assert listing[1].split()[:2] == [term_id, "Net"] and "Revenue after returns" in listing[1]
    assert cl.json("governance", "terms", "list", "--status", "approved") == []
    assert [t["name"] for t in cl.json("governance", "terms", "list", "-q", "turnover")] == ["Net revenue"]
    search = cl.lines("governance", "terms", "search", "nr")
    assert search[0].split() == ["ID", "TERM", "STATUS", "SCORE", "MATCHED", "ON", "DEFINITION"] and search[1].split()[4:7] == ["80", "abbreviation", "Revenue"]
    assert cl.run("governance", "terms", "search", "zzz-no-term").stdout.strip() == "no term matches 'zzz-no-term'"

    cl.run("governance", "terms", "create", "Booked revenue", "--parent", "Net revenue", "--related", "NR")
    child = cl.json("governance", "terms", "show", "Booked revenue")
    assert child["parent_id"] == term_id and [a["name"] for a in child["ancestors"]] == ["Net revenue"] and [r["name"] for r in child["related_terms"]] == ["Net revenue"]
    assert "hierarchy:     Net revenue > Booked revenue" in cl.run("governance", "terms", "show", "Booked revenue").stdout

    updated = cl.run("governance", "terms", "update", "Net revenue", "--definition", "Revenue after returns and credits")
    assert updated.stdout == f"OK: updated term Net revenue ({term_id}); status draft\n"
    nothing = cl.run("governance", "terms", "update", "Net revenue", expect=EXIT_USAGE)
    assert "nothing to change" in nothing.output
    etag = cl.json("governance", "terms", "show", "Net revenue")["etag"]
    cl.run("governance", "terms", "update", "Net revenue", "--abbreviation", "NETREV", "--if-match", etag)
    stale = cl.run("governance", "terms", "update", "Net revenue", "--abbreviation", "NR", "--if-match", etag, expect=EXIT_CONFLICT)
    assert "412" in stale.stderr and cl.json("governance", "terms", "show", "Net revenue")["abbreviation"] == "NETREV"
    cl.run("governance", "terms", "update", "Net revenue", "--abbreviation", "NR", "--status", "approved")
    assert cl.json("governance", "terms", "show", "Net revenue")["status"] == "approved"

    refused = cl.run("governance", "terms", "delete", "Booked revenue", expect=EXIT_FAIL)                # a delete asks first; a script must pass --yes
    assert "pass --yes" in refused.output and cl.json("governance", "terms", "show", "Booked revenue")["status"] == "draft"
    assert cl.run("governance", "terms", "delete", "Booked revenue", "--yes").stdout == "OK: deprecated term Booked revenue (0 link(s) removed)\n"
    assert cl.json("governance", "terms", "show", "Booked revenue")["status"] == "deprecated"
    assert cl.run("governance", "terms", "delete", "Booked revenue", "--hard", "--yes").stdout == "OK: deleted term Booked revenue (0 link(s) removed)\n"
    gone = cl.run("governance", "terms", "show", "Booked revenue", expect=EXIT_NOT_FOUND)
    assert "no term is named 'Booked revenue'" in gone.stderr and "olcf governance terms list" in gone.stderr


def test_term_links_in_both_directions_and_ambiguous_names(cl):
    out = cl.run("governance", "terms", "link", "Net revenue", "lakehouse_a.mart", "--comment", "the mart")
    assert out.stdout == "OK: linked Net revenue to lakehouse_a/mart (link 1, approved)\n"
    cl.run("governance", "terms", "link", "NR", "clean")
    links = cl.lines("governance", "terms", "links", "Net revenue")
    assert links[0].split() == ["LINK", "TERM", "KIND", "TARGET", "STATUS", "SOURCE", "CONFIDENCE"]
    assert [ln.split()[1:] for ln in links[1:]] == [["Net", "revenue", "asset", "lakehouse_a/mart", "approved", "manual", "-"], ["Net", "revenue", "asset", "lakehouse_a/clean", "approved", "manual", "-"]]
    assert [lk["asset"]["name"] for lk in cl.json("governance", "terms", "links", "Net revenue", "--all", "--limit", "1")] == ["mart", "clean"]
    assert cl.run("governance", "terms", "unlink", "Net revenue", "clean").stdout == "OK: unlinked Net revenue from clean\n"
    assert [lk["asset"]["name"] for lk in cl.json("governance", "terms", "links", "Net revenue")] == ["mart"]
    assert [lk["status"] for lk in cl.json("governance", "terms", "links", "Net revenue", "--status", "unlinked")] == ["unlinked"]
    cl.run("governance", "products", "create", "Terms product")
    assert cl.run("governance", "terms", "link", "Net revenue", "Terms product", "--product").stdout.startswith("OK: linked Net revenue to Terms product (link ")
    cl.run("governance", "terms", "create", "Net revenue", "--glossary", "finance")                      # the same name in another glossary
    amb = cl.run("governance", "terms", "show", "Net revenue", expect=EXIT_AMBIGUOUS)
    assert "'Net revenue' names 2 terms:" in amb.stderr and "(finance)" in amb.stderr and "pass the id (trm_...)" in amb.stderr
    assert cl.json("governance", "terms", "list", "--glossary", "finance")[0]["glossary"] == "finance"
    no_such = cl.run("governance", "terms", "link", "Net revenue", "no-such-asset", expect=EXIT_AMBIGUOUS)
    assert "names 2 terms" in no_such.stderr                                                             # the term is resolved first
    t = [x for x in cl.json("governance", "terms", "list") if x["glossary"] == "default"][0]["id"]
    assert cl.run("governance", "terms", "link", t, "no-such-asset", expect=EXIT_NOT_FOUND).exit_code == EXIT_NOT_FOUND


# ============================================================================================================================ domains and products
def test_domains_assignment_and_products_with_consumers(cl):
    d = cl.run("governance", "domains", "create", "Finance", "--description", "Money")
    assert ID.search(d.stdout).group("kind") == "dom"
    cl.run("governance", "domains", "create", "Sales analytics", "--parent", "Finance")
    listing = cl.lines("governance", "domains", "list", "--parent", "root")
    assert listing[0].split() == ["ID", "NAME", "ASSETS", "PRODUCTS", "SUB-DOMAINS", "DESCRIPTION"] and listing[1].split()[1:5] == ["Finance", "0", "0", "1"]
    show = cl.run("governance", "domains", "show", "Sales analytics").stdout
    assert "path:          Finance > Sales analytics" in show and "etag:" in show
    assert cl.run("governance", "domains", "assign", "mart", "Sales analytics").stdout == "OK: assigned mart to domain Sales analytics\n"
    assert cl.run("governance", "domains", "assign", "mart", expect=EXIT_USAGE).exit_code == EXIT_USAGE
    assigned = cl.lines("governance", "domains", "assets", "Finance")
    assert assigned[0].split() == ["ID", "TYPE", "NAME", "PATH", "VIA"] and assigned[1].split()[1:] == ["table", "mart", "lakehouse_a/mart", "direct"]
    assert cl.json("governance", "domains", "assets", "Finance", "--no-descendants") == []
    refused = cl.run("governance", "domains", "delete", "Sales analytics", "--yes", expect=EXIT_CONFLICT)
    assert "409" in refused.stderr
    updated = cl.run("governance", "domains", "update", "Sales analytics", "--description", "Sales numbers")
    assert updated.stdout.startswith("OK: updated domain Sales analytics")

    created = cl.run("governance", "products", "create", "Revenue mart", "--domain", "Finance", "--member", "mart", "--freshness-hours", "24", "--availability", "99.5%", "--description", "Monthly revenue")
    assert re.fullmatch(r"OK: created data product Revenue mart \(dpr_[0-9A-Z]{26}\), status draft, 1 member\(s\)\n", created.stdout)
    show = cl.run("governance", "products", "show", "Revenue mart").stdout
    assert "domain:        Finance" in show and 'sla:           {"availability": "99.5%", "freshness_hours": 24.0}' in show and "Members (1):" in show
    assert re.search(r"sales_model\s+semantic_model\s+mart\s+1\s+derived", show) and re.search(r"sales_report\s+report\s+mart\s+2\s+inferred", show)
    consumers = cl.lines("governance", "products", "consumers", "Revenue mart")
    assert consumers[0].split() == ["CONSUMER", "TYPE", "READS", "HOPS", "CONFIDENCE"] and len(consumers) == 3
    assert [c["asset"]["name"] for c in cl.json("governance", "products", "consumers", "Revenue mart", "--depth", "1")["items"]] == ["sales_model"]
    assert [c["asset"]["name"] for c in cl.json("governance", "products", "consumers", "Revenue mart", "--min-confidence", "derived")["items"]] == ["sales_model"]
    cl.run("governance", "products", "add-member", "Revenue mart", "clean")
    assert cl.json("governance", "products", "show", "Revenue mart")["member_count"] == 2
    cl.run("governance", "products", "remove-member", "Revenue mart", "clean")
    assert cl.run("governance", "products", "set-members", "Revenue mart", "mart", "clean").stdout == "OK: Revenue mart now has 2 member(s)\n"
    assert [p["name"] for p in cl.json("governance", "products", "list", "--domain", "Finance")] == ["Revenue mart"]
    plist = cl.lines("governance", "products", "list")
    assert plist[0].split() == ["ID", "NAME", "STATUS", "CERTIFICATION", "DOMAIN", "MEMBERS"]
    contract = json.dumps({"columns": [{"name": "total", "type": "double"}], "expectations": [{"name": "no negatives"}]})
    cl.run("governance", "products", "create", "Contracted", "--contract-file", "-", input=contract)
    assert "contract:      1 column(s), 1 expectation(s)" in cl.run("governance", "products", "show", "Contracted").stdout
    denied = cl.run("governance", "products", "create", "By curator", "--status", "published", who="curator_org", expect=EXIT_DENIED)
    assert "governance.approve" in denied.stderr
    assert cl.run("governance", "products", "update", "Revenue mart", "--status", "published").stdout.endswith("status published\n")
    assert cl.run("governance", "products", "delete", "Revenue mart", "--yes", expect=EXIT_CONFLICT).exit_code == EXIT_CONFLICT
    cl.run("governance", "products", "update", "Revenue mart", "--status", "deprecated")
    assert cl.run("governance", "products", "delete", "Revenue mart", "--yes").stdout == "OK: deleted data product Revenue mart\n"
    cl.run("governance", "domains", "assign", "mart", "--clear")
    cl.run("governance", "domains", "delete", "Sales analytics", "--yes")
    cl.run("governance", "domains", "delete", "Finance", "--yes")
    for name in ("Terms product", "Contracted"):
        cl.run("governance", "products", "delete", name, "--yes")


# ============================================================================================================================ owners
def test_owners_add_list_inherit_remove_and_set(cl):
    assert cl.run("governance", "owners", "add", "asset", "raw", "--role", "owner", "--user", "admin").stdout.endswith("as owner of raw (owner id 1)\n")
    ext = cl.run("governance", "owners", "add", "asset", "raw", "--role", "steward", "--name", "Data Office", "--email", "data-office@example.com")
    assert ext.stdout == "OK: added Data Office as steward of raw (owner id 2)\n"
    assert cl.run("governance", "owners", "add", "asset", "raw", "--user", "admin", expect=EXIT_CONFLICT).exit_code == EXIT_CONFLICT
    both = cl.run("governance", "owners", "add", "asset", "raw", "--user", "admin", "--name", "x", expect=EXIT_USAGE)
    assert "--user USER" in both.output and "--name NAME" in both.output
    assert cl.run("governance", "owners", "add", "asset", "raw", expect=EXIT_USAGE).exit_code == EXIT_USAGE
    direct = cl.lines("governance", "owners", "list", "asset", "raw")
    assert direct[0].split() == ["ID", "ROLE", "NAME", "EMAIL", "KIND", "SCOPE", "FROM"] and [ln.split()[1] for ln in direct[1:]] == ["owner", "steward"]
    inherited = cl.lines("governance", "owners", "list", "asset", "raw.amount")
    assert [ln.split()[-3:] for ln in inherited[1:]] == [["user", "inherited", "raw"], ["external", "inherited", "raw"]]
    assert cl.run("governance", "owners", "remove", "asset", "raw", "2").stdout == "OK: removed owner 2 from raw\n"
    assert [o["role"] for o in cl.json("governance", "owners", "list", "asset", "raw")["direct"]] == ["owner"]
    cl.run("governance", "owners", "set", "asset", "raw", "-", input='[{"role": "custodian", "name": "Platform team"}]')
    assert [(o["role"], o["name"]) for o in cl.json("governance", "owners", "list", "asset", "raw")["direct"]] == [("custodian", "Platform team")]
    bad = cl.run("governance", "owners", "set", "asset", "raw", "-", input="{}", expect=EXIT_USAGE)
    assert "JSON list" in bad.output
    cl.run("governance", "owners", "set", "asset", "raw", "-", input="[]")
    for kind, ref in (("domain", "Owned domain"), ("product", "Owned product"), ("term", "Owned term")):
        cl.run("governance", {"domain": "domains", "product": "products", "term": "terms"}[kind], "create", ref)
        cl.run("governance", "owners", "add", kind, ref, "--role", "steward", "--user", "admin")
        assert [o["role"] for o in cl.json("governance", "owners", "list", kind, ref)["direct"]] == ["steward"]
    assert cl.run("governance", "owners", "add", "asset", "raw", "--user", "admin", who="org", expect=EXIT_DENIED).exit_code == EXIT_DENIED
    assert cl.run("governance", "owners", "list", "asset", "secret", who="alpha", expect=EXIT_NOT_FOUND).exit_code == EXIT_NOT_FOUND
    assert cl.run("governance", "owners", "list", "workspace", "x", expect=EXIT_USAGE).exit_code == EXIT_USAGE


# ============================================================================================================================ classifications and exposure
def test_classifications_assign_inherit_and_exposure_output(cl):
    cat = cl.lines("governance", "classifications", "list")
    assert cat[0].split() == ["ID", "NAME", "CATEGORY", "LEVEL", "BUILT-IN", "ASSIGNED", "DESCRIPTION"]
    assert {ln.split()[1] for ln in cat[1:]} >= {"Public", "Internal", "Confidential", "PII", "PCI", "Restricted", "Credentials"}
    assert cl.json("governance", "classifications", "show", "pii")["name"] == "PII"
    assert cl.run("governance", "classifications", "assign", "raw.amount", "PII", "--comment", "personal amounts").stdout == "OK: labelled raw.amount as PII (approved)\n"
    assert cl.run("governance", "classifications", "assign", "raw", "No such label", expect=EXIT_NOT_FOUND).exit_code == EXIT_NOT_FOUND

    inherited = cl.lines("governance", "classifications", "of", "clean.amount")
    assert inherited[0].split() == ["LABEL", "HOW", "STATUS", "CONFIDENCE", "PRECISION", "EVIDENCE"]
    assert inherited[1].split() == ["PII", "inherited", "approved", "0.75", "column", "raw.amount", "->", "clean.amount"]                   # columns are named table.column
    contained = cl.lines("governance", "classifications", "of", "raw")
    assert contained[1].split() == ["PII", "contained", "-", "1", "column(s)"]
    held = cl.lines("governance", "classifications", "assets", "PII")
    assert [ln.split()[:3] for ln in held[1:]] == [["lakehouse_a/clean", "table", "inherited"], ["raw.amount", "column", "direct"], ["clean.amount", "column", "inherited"]]
    assert [ln.split()[0] for ln in cl.lines("governance", "classifications", "assets", "PII", "--direct")[1:]] == ["raw.amount"]

    out = cl.lines("governance", "exposure", "PII")
    assert out[0] == "Data labelled PII reaches 5 asset(s): 2 proven, 3 possible; 0 report(s), 1 semantic model(s), 0 data product(s)."
    assert out[1] == "Labelled directly: raw.amount"
    assert out[3].split() == ["ASSET", "TYPE", "PRECISION", "PROVEN", "CONFIDENCE", "FROM", "HOPS"]
    rows = [ln.split() for ln in out[4:9]]
    assert rows == [["lakehouse_a/clean", "table", "contains", "yes", "derived", "raw.amount", "1"], ["clean.amount", "column", "column", "yes", "derived", "raw.amount", "1"],
                    ["lakehouse_a/mart", "table", "table", "possible", "inferred", "lakehouse_a/raw", "4"],
                    ["lakehouse_b/secret_out", "table", "table", "possible", "inferred", "lakehouse_a/raw", "4"], ["sales_model", "semantic_model", "table", "possible", "inferred", "lakehouse_a/raw", "5"]]
    assert any(ln.startswith("note: Entries with proven=false are possible exposures") for ln in out)

    paths = cl.lines("governance", "exposure", "PII", "--type", "report", "--min-confidence", "inferred", "--paths")
    assert paths[0].startswith("Data labelled PII reaches 1 asset(s): 0 proven, 1 possible; 1 report(s)")
    assert "  raw -> etl -> clean -> agg -> mart -> sales_model -> sales_report  [inferred, possible]" in paths
    assert "sales_report" not in cl.run("governance", "exposure", "PII").stdout                         # the report is only inferred: not shown at the default confidence
    proven = cl.run("governance", "classifications", "exposure", "PII", "--proven-only", "--paths").stdout
    assert "lakehouse_a/mart" not in proven and "  raw.amount -> clean.amount  [derived, proven]" in proven
    as_json = cl.json("governance", "exposure", "PII", "--type", "report", "--min-confidence", "inferred")
    assert as_json["summary"]["reports"] == 1 and as_json["exposures"][0]["asset"]["name"] == "sales_report"
    csv_out = cl.run("-o", "csv", "governance", "classifications", "assets", "PII").stdout.splitlines()
    assert csv_out[0] == "ASSET,TYPE,HOW,STATUS,CONFIDENCE,PRECISION" and len(csv_out) == 4

    # what the viewer of workspace alpha is allowed to see
    alpha = cl.run("governance", "exposure", "PII", who="alpha").stdout
    assert "secret_out" not in alpha and "lakehouse_a/clean" in alpha
    assert cl.run("governance", "classifications", "scan").stdout.startswith("OK: examined ")

    custom = cl.run("governance", "classifications", "create", "Embargoed", "--category", "handling", "--level", "4", "--color", "#112233")
    assert ID.search(custom.stdout).group("kind") == "cls"
    cl.run("governance", "classifications", "update", "Embargoed", "--color", "#445566", "--description", "Not before the announcement")
    assert cl.json("governance", "classifications", "show", "Embargoed")["color"] == "#445566"
    assert cl.run("governance", "classifications", "update", "PII", "--name", "Renamed", expect=EXIT_INVALID).exit_code == EXIT_INVALID
    cl.run("governance", "classifications", "assign", "lonely", "Embargoed")
    assert cl.run("governance", "classifications", "delete", "Embargoed", "--yes", expect=EXIT_CONFLICT).exit_code == EXIT_CONFLICT
    cl.run("governance", "classifications", "remove", "lonely", "Embargoed")
    assert cl.run("governance", "classifications", "delete", "Embargoed", "--yes").stdout == "OK: deleted label Embargoed\n"
    cl.run("governance", "classifications", "remove", "raw.amount", "PII")
    assert cl.run("governance", "classifications", "assets", "PII").stdout.strip() == "no assets carry this label"
    assert cl.run("governance", "classifications", "delete", "PII", "--yes", expect=EXIT_CONFLICT).exit_code == EXIT_CONFLICT          # a built-in label
    assert cl.run("governance", "classifications", "assign", "raw", "PII", who="org", expect=EXIT_DENIED).exit_code == EXIT_DENIED


# ============================================================================================================================ review queue
def test_review_queue_list_approve_reject_and_bulk(cl):
    for name in ("lonely", "fresh", "mart"):
        cl.run("governance", "terms", "create", name, "--definition", f"the {name} table")
    sug = cl.run("governance", "terms", "suggest-links").stdout
    assert sug.startswith("OK: examined ") and ", 3 new suggestion(s); review them with `olcf governance review-queue list`" in sug
    queue = cl.lines("governance", "review-queue", "list")
    assert queue[0].split() == ["ID", "KIND", "ASSET", "PROPOSAL", "CONFIDENCE", "SOURCE", "NEEDS", "CREATED"] and len(queue) == 4
    assert {ln.split()[2] + " " + ln.split()[3] for ln in queue[1:]} == {"lakehouse_a/lonely lonely", "lakehouse_a/fresh fresh", "lakehouse_a/mart mart"}
    assert all(ln.split()[1] == "term_link" and ln.split()[6] == "governance.curate" for ln in queue[1:])
    summary = cl.lines("governance", "review-queue", "summary")
    assert summary[:3] == ["pending:                    3", "classification suggestions: 0", "term-link suggestions:      3"]
    assert [i["asset"]["name"] for i in cl.json("governance", "review-queue", "list", "--asset", "fresh")] == ["fresh"]
    assert cl.json("governance", "review-queue", "list", "--kind", "classification") == []
    assert cl.run("governance", "review-queue", "list", "--min-confidence", "2", expect=EXIT_USAGE).exit_code == EXIT_USAGE
    by_asset = {i["asset"]["name"]: i["id"] for i in cl.json("governance", "review-queue", "list")}
    assert cl.run("governance", "review-queue", "approve", by_asset["lonely"], "--comment", "yes").stdout == f"OK: approved {by_asset['lonely']} (lonely)\n"
    assert [lk["asset"]["name"] for lk in cl.json("governance", "terms", "links", "lonely")] == ["lonely"]
    assert cl.run("governance", "review-queue", "reject", by_asset["fresh"], "--comment", "other meaning").stdout == f"OK: rejected {by_asset['fresh']} (fresh)\n"
    cl.run("governance", "terms", "suggest-links")
    assert cl.json("governance", "review-queue", "list", "--asset", "fresh") == []                      # not proposed again
    mixed = cl.run("governance", "review-queue", "approve", by_asset["mart"], "term_link:999999", expect=EXIT_FAIL)
    lines = mixed.stdout.splitlines()
    assert lines[0].split() == ["ITEM", "RESULT", "REASON"] and lines[1].split()[:2] == [by_asset["mart"], "ok"]
    assert lines[2].split()[:2] == ["term_link:999999", "failed"] and "does not exist" in lines[2] and lines[3] == "approved: 1, failed: 1"
    assert cl.run("governance", "review-queue", "approve", "term_link:424242", expect=EXIT_NOT_FOUND).exit_code == EXIT_NOT_FOUND
    assert cl.run("governance", "review-queue", "approve", "term_link:1", who="org", expect=EXIT_DENIED).exit_code == EXIT_DENIED
    assert cl.run("governance", "review-queue", "approve", expect=EXIT_USAGE).exit_code == EXIT_USAGE
    assert cl.run("governance", "review-queue", "list", "--json").stdout.strip() == "[]"
    assert cl.run("governance", "review-queue", "list").stdout.strip() == "the review queue is empty"


# ============================================================================================================================ certification and change requests
def test_certify_certification_and_permissions(cl):
    assert cl.run("governance", "certify", "mart", "--comment", "reviewed with finance").stdout == "OK: mart is now certified\n"
    state = cl.lines("governance", "certification", "mart")
    assert state[0] == "subject:              mart (asset)" and state[1] == "state:                certified" and "comment:              reviewed with finance" in state
    listing = cl.lines("governance", "certifications")
    assert listing[0].split()[:5] == ["SUBJECT", "KIND", "STATE", "CERTIFIED", "AT"] and listing[1].split()[:3] == ["lakehouse_a/mart", "asset", "certified"]
    assert cl.json("governance", "certifications", "--state", "deprecated") == []
    denied = cl.run("governance", "certify", "fresh", who="curator_org", expect=EXIT_DENIED)
    assert "governance.approve" in denied.stderr
    assert cl.run("governance", "certify", "fresh", "--state", "draft", who="curator_org").stdout == "OK: fresh is now draft\n"
    etag = cl.json("governance", "certification", "fresh")["etag"]
    cl.run("governance", "certify", "fresh", "--state", "none")
    stale = cl.run("governance", "certify", "fresh", "--state", "deprecated", "--if-match", etag, expect=EXIT_CONFLICT)
    assert "412" in stale.stderr
    cl.run("governance", "products", "create", "Cert product")
    assert cl.run("governance", "certify", "Cert product", "--kind", "product").stdout == "OK: Cert product is now certified\n"
    assert cl.json("governance", "certification", "Cert product", "--kind", "product")["state"] == "certified"
    cl.run("governance", "products", "delete", "Cert product", "--yes")                                  # certification does not block a delete, only `published` does
    term = cl.run("governance", "terms", "create", "Certifiable").stdout
    assert "draft" in term
    needs_approval = cl.run("governance", "certify", "Certifiable", "--kind", "term", expect=EXIT_INVALID)        # a term must be approved before it can be certified
    assert "approved" in needs_approval.stderr
    cl.run("governance", "terms", "update", "Certifiable", "--status", "approved")
    assert cl.run("governance", "certify", "Certifiable", "--kind", "term").stdout == "OK: Certifiable is now certified\n"
    cl.run("governance", "certify", "mart", "--state", "none")


def test_change_requests_create_comment_approve_reject_withdraw(cl):
    made = cl.run("governance", "requests", "create", "certify", "lonely", "--comment", "please certify", who="curator_org")
    chg = re.search(r"opened certify request (chg_[0-9A-Z]{26}) \(pending\)", made.stdout).group(1)
    listing = cl.lines("governance", "requests", "list", "--status", "pending")
    assert listing[0].split() == ["ID", "TYPE", "SUBJECT", "STATUS", "REQUESTER", "CREATED"] and listing[1].split()[:5] == [chg, "certify", "lakehouse_a/lonely", "pending", "c_org"]
    assert [c["id"] for c in cl.json("governance", "requests", "list", "--requester", "me", who="curator_org")] == [chg]
    assert cl.json("governance", "requests", "list", "--requester", "me") == []
    assert cl.run("governance", "requests", "comment", chg, "ping", who="curator_org").stdout == f"OK: commented on {chg}\n"
    show = cl.run("governance", "requests", "show", chg).stdout
    assert "type:             certify" in show and 'payload:          {"state": "certified"}' in show and "[comment]  ping" in show and "[created]" in show
    assert "governance.approve" in cl.run("governance", "requests", "approve", chg, who="curator_org", expect=EXIT_DENIED).stderr
    assert cl.run("governance", "requests", "reject", chg, expect=EXIT_USAGE).exit_code == EXIT_USAGE            # a reason is required
    done = cl.run("governance", "requests", "approve", chg, "--comment", "looks right")
    assert re.fullmatch(rf"OK: approved {chg} and applied it at \S+\n", done.stdout)
    assert cl.json("governance", "certification", "lonely")["state"] == "certified"
    assert cl.run("governance", "requests", "apply", chg).stdout.startswith(f"OK: {chg}: approved, applied at ")
    assert cl.run("governance", "requests", "approve", chg, expect=EXIT_CONFLICT).exit_code == EXIT_CONFLICT

    second = cl.run("governance", "requests", "create", "classification_change", "lonely", "--payload", '{"add": ["Internal"]}', who="curator_org").stdout
    chg2 = re.search(r"(chg_[0-9A-Z]{26})", second).group(1)
    assert cl.run("governance", "requests", "reject", chg2, "--comment", "not now").stdout == f"OK: rejected {chg2}\n"
    third = cl.run("governance", "requests", "create", "classification_change", "lonely", "--set", "add=[\"Internal\"]", who="curator_org").stdout
    chg3 = re.search(r"(chg_[0-9A-Z]{26})", third).group(1)
    assert cl.run("governance", "requests", "withdraw", chg3, who="curator_org").stdout == f"OK: withdrew {chg3}\n"
    own = cl.run("governance", "requests", "create", "certify", "mart").stdout
    own_id = re.search(r"(chg_[0-9A-Z]{26})", own).group(1)
    assert cl.run("governance", "requests", "approve", own_id, expect=EXIT_DENIED).exit_code == EXIT_DENIED         # not your own request
    assert {c["id"] for c in cl.json("governance", "requests", "list", "--kind", "asset", "--subject", "lonely", "--all", "--limit", "1")} == {chg, chg2, chg3}
    assert cl.run("governance", "requests", "list", "--subject", "lonely", expect=EXIT_USAGE).exit_code == EXIT_USAGE
    bad = cl.run("governance", "requests", "create", "certify", "lonely", "--payload", "[1]", expect=EXIT_USAGE)
    assert "must be a JSON object" in bad.output
    assert cl.run("governance", "requests", "create", "teleport", "lonely", expect=EXIT_USAGE).exit_code == EXIT_USAGE
    exc = cl.run("governance", "requests", "create", "policy_exception", "vio_00000000000000000000000000", "--kind", "violation", "--set", "reason=x", who="curator_org", expect=None)
    assert exc.exit_code == EXIT_NOT_FOUND
    cl.run("governance", "certify", "lonely", "--state", "none")


# ============================================================================================================================ policies and violations
def test_policies_evaluate_violations_acknowledge_preview_and_delete(cl):
    rules = cl.run("governance", "policies", "rule-types").stdout
    assert "required_owner\n  Assets must have an effective owner (direct or inherited from a parent asset)." in rules and "quality_min" in rules
    create = cl.run("governance", "policies", "create", "Tables have owners", "--rule-type", "required_owner", "--param", 'types=["table"]', "--severity", "high",
                    "--description", "every table has an owner")
    pid = re.search(r"created policy Tables have owners \((plc_[0-9A-Z]{26})\), high, observe; evaluate it with `olcf governance policies evaluate plc_[0-9A-Z]{26}`", create.stdout).group(1)
    assert cl.run("governance", "policies", "create", "Blocking", "--rule-type", "required_owner", "--mode", "enforce", expect=EXIT_INVALID).exit_code == EXIT_INVALID
    assert cl.run("governance", "policies", "create", "Bad rule", "--rule-type", "nonsense", expect=EXIT_INVALID).exit_code == EXIT_INVALID
    assert cl.run("governance", "policies", "create", "Not allowed", "--rule-type", "required_owner", who="curator_org", expect=EXIT_DENIED).exit_code == EXIT_DENIED
    params_file_err = cl.run("governance", "policies", "create", "Bad json", "--rule-type", "required_owner", "--params-file", "-", input="{", expect=EXIT_USAGE)
    assert "not valid JSON" in params_file_err.output

    ev = cl.lines("governance", "policies", "evaluate", "Tables have owners")
    assert ev[0].split() == ["POLICY", "EVALUATED", "OPENED", "UPDATED", "RESOLVED", "REOPENED", "UNKNOWN", "MS"] and ev[1].split()[:6] == [pid, "7", "7", "0", "0", "0"]
    again = cl.lines("governance", "policies", "evaluate", pid)
    assert again[1].split()[:4] == [pid, "7", "0", "7"]                                                  # idempotent: the same violations are updated
    assert pid in cl.run("governance", "policies", "evaluate").stdout
    lst = cl.lines("governance", "policies", "list")
    assert lst[0].split() == ["ID", "NAME", "RULE", "SEVERITY", "MODE", "ENABLED", "OPEN", "LAST", "EVALUATED"] and lst[1].split()[1:8] == ["Tables", "have", "owners", "required_owner", "high", "observe", "yes"]
    show = cl.run("governance", "policies", "show", "Tables have owners").stdout
    assert 'params:          {"roles": ["owner"], "selector": {}, "types": ["table"]}' in show and "open violations: 7" in show

    vio = cl.lines("governance", "violations", "list", "--severity", "high")
    assert vio[0].split() == ["ID", "SEVERITY", "STATUS", "POLICY", "SUBJECT", "MESSAGE"] and len(vio) == 8
    assert [ln.split()[4] for ln in vio[1:]] == sorted(ln.split()[4] for ln in vio[1:])                  # worst first, then by subject
    assert cl.json("governance", "violations", "list", "--severity", "critical") == []
    one = cl.json("governance", "violations", "list", "--asset", "lonely")[0]
    detail = cl.run("governance", "violations", "show", one["id"]).stdout
    assert re.search(rf"^id:\s+{one['id']}$", detail, re.M) and re.search(r"^policy:\s+Tables have owners \(required_owner\)$", detail, re.M)
    assert re.search(r"^message:\s+'lonely' has no owner \(direct or inherited\)$", detail, re.M) and re.search(r"^status:\s+open$", detail, re.M)
    assert cl.run("governance", "violations", "acknowledge", one["id"], "--comment", "known").stdout == f"OK: acknowledged {one['id']}\n"
    assert cl.json("governance", "violations", "show", one["id"])["status"] == "acknowledged"
    assert one["id"] in {v["id"] for v in cl.json("governance", "violations", "list", "--policy", "Tables have owners")}
    assert cl.run("governance", "violations", "reopen", one["id"]).stdout == f"OK: reopened {one['id']}\n"
    cl.run("governance", "owners", "add", "asset", "lonely", "--user", "admin")
    assert cl.lines("governance", "policies", "evaluate", pid)[1].split()[4] == "1"
    assert [v["status"] for v in cl.json("governance", "violations", "list", "--asset", "lonely", "--status", "resolved")] == ["resolved"]
    assert cl.json("governance", "violations", "list", "--asset", "lonely") == []
    cl.run("governance", "owners", "set", "asset", "lonely", "-", input="[]")

    pre = cl.lines("governance", "policies", "preview", "--rule-type", "required_description", "--param", "min_length=5", "--limit", "2")
    assert pre[0] == "9 violation(s) would be reported among 9 evaluated asset(s) (0 unknown)"
    assert pre[1].split() == ["SUBJECT", "SEVERITY", "MESSAGE"] and len(pre) == 4
    assert cl.json("governance", "policies", "list", "--rule-type", "required_description") == []         # a preview stores nothing

    upd = cl.run("governance", "policies", "update", "Tables have owners", "--severity", "critical", "--disabled")
    assert upd.stdout == f"OK: updated policy Tables have owners ({pid})\n"
    assert cl.json("governance", "policies", "show", pid)["enabled"] is False and [p["id"] for p in cl.json("governance", "policies", "list", "--disabled")] == [pid]
    assert cl.run("governance", "policies", "update", pid, expect=EXIT_USAGE).exit_code == EXIT_USAGE
    refused = cl.run("governance", "policies", "delete", pid, expect=EXIT_FAIL)
    assert "pass --yes" in refused.output
    assert cl.run("governance", "policies", "delete", "Tables have owners", "--yes").stdout == "OK: deleted policy Tables have owners\n"
    assert cl.run("governance", "policies", "show", pid, expect=EXIT_NOT_FOUND).exit_code == EXIT_NOT_FOUND
    assert cl.run("governance", "violations", "list", "--status", "any").stdout.strip() == "no violations"


def test_a_policy_exception_through_a_change_request(cl):
    cl.run("governance", "policies", "create", "Low owners", "--rule-type", "required_owner", "--param", 'types=["table"]', "--severity", "low")
    cl.run("governance", "policies", "evaluate")
    vio = cl.json("governance", "violations", "list", "--asset", "fresh")[0]["id"]
    made = cl.run("governance", "requests", "create", "policy_exception", vio, "--kind", "violation", "--set", "reason=scratch table", who="curator_org").stdout
    chg = re.search(r"(chg_[0-9A-Z]{26})", made).group(1)
    cl.run("governance", "requests", "approve", chg)
    shown = cl.run("governance", "violations", "show", vio).stdout
    assert re.search(r"^status:\s+excepted$", shown, re.M) and re.search(r"^exception:\s+scratch table \(until ", shown, re.M)
    cl.run("governance", "violations", "reopen", vio)
    cl.run("governance", "policies", "delete", "Low owners", "--yes")


# ============================================================================================================================ quality, overview, asset
def test_quality_summary_asset_and_graph_views(cl):
    q = cl.lines("governance", "quality", "clean")
    assert re.fullmatch(r"Quality of clean: \d+\.\d+ / 100  \(own \d+\.\d+\)", q[0])
    names = [ln.split()[0] for ln in q[1:9]]
    assert names == ["documentation", "ownership", "classification", "freshness", "certification", "lineage", "violations", "schema_stability"]
    assert q[1] == "  documentation          0 / 22.22  -22.22 no description" and q[3] == "  classification     n/a (does not apply)"
    assert q[-1].startswith("  columns: 2 column(s), mean 50.0 (weight 0.3)")
    as_json = cl.json("governance", "quality", "clean")
    assert as_json["asset_id"] == cl.stack.ids["clean"] and {c["name"] for c in as_json["components"]} == set(names)
    cl.run("governance", "products", "create", "Quality product", "--member", "clean", "--member", "mart")
    pq = cl.run("governance", "quality", "Quality product", "--product").stdout.splitlines()
    assert pq[0].startswith("Quality of Quality product: ") and pq[-1].startswith("  members: ") and {m.rsplit(" ", 1)[0] for m in pq[-1].removeprefix("  members: ").split(", ")} == {"lakehouse_a/clean", "lakehouse_a/mart"}
    cl.run("governance", "products", "delete", "Quality product", "--yes")
    assert cl.run("governance", "quality", "secret", who="alpha", expect=EXIT_NOT_FOUND).exit_code == EXIT_NOT_FOUND

    s = cl.lines("governance", "summary")
    assert s[0].startswith("glossary terms:") and s[1].startswith("domains:") and any(ln.startswith("coverage of datasets:   7 datasets: owner ") for ln in s)
    assert cl.json("governance", "summary")["coverage"]["datasets"] == 7
    assert cl.json("governance", "summary", "--workspace-id", cl.stack.world.ws2["id"])["coverage"]["datasets"] == 2
    assert cl.run("governance", "summary", who="none", expect=EXIT_DENIED).exit_code == EXIT_DENIED

    cl.run("governance", "domains", "create", "View domain")
    cl.run("governance", "domains", "assign", "mart", "View domain")
    cl.run("governance", "classifications", "assign", "mart", "Internal")
    asset = cl.lines("governance", "asset", "mart")
    assert asset[0] == "asset:              mart (table)  lakehouse_a/mart" and "domain:             View domain (direct)" in asset
    assert any(ln.startswith("labels:             direct Internal; inherited none") for ln in asset) and "owners:             none" in asset
    graph = cl.lines("governance", "graph", "mart")
    assert graph[0].split() == ["FROM", "EDGE", "TO", "VIA"] and any("CLASSIFIED_AS" in ln and "Internal (classification)" in ln for ln in graph) and any("GOVERNED_BY" in ln and "View domain (domain)" in ln for ln in graph)
    cl.run("governance", "classifications", "remove", "mart", "Internal")
    cl.run("governance", "domains", "assign", "mart", "--clear")
    cl.run("governance", "domains", "delete", "View domain", "--yes")
    assert cl.run("governance", "asset", "secret", who="alpha", expect=EXIT_NOT_FOUND).exit_code == EXIT_NOT_FOUND
    assert cl.run("governance", "asset", "amount", expect=EXIT_AMBIGUOUS).exit_code == EXIT_AMBIGUOUS


def test_authentication_failures_and_json_errors_keep_stdout_clean(cl):
    res = cl.run("governance", "summary", who="anon", expect=EXIT_AUTH)
    assert "olcf_pat_bogus" not in res.output and res.stdout == ""
    err = cl.run("--json", "governance", "terms", "show", "no-such-term", expect=EXIT_NOT_FOUND)
    assert err.stdout.strip() == "" and "no term is named 'no-such-term'" in err.stderr
    assert cl.run("governance", "terms", "list", "--limit", "0", expect=EXIT_USAGE).exit_code == EXIT_USAGE
    assert cl.run("governance", "nonsense", expect=EXIT_USAGE).exit_code == EXIT_USAGE
