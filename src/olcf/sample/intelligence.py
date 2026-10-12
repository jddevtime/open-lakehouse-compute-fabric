# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""The Intelligence part of the sample: what a freshly installed sample shows in OLCF Intelligence on the first day.

:func:`integrate` runs after the sample's lakehouse, tables, notebooks, queries and report exist and **only when the Intelligence module is installed**
(``ctx.maybe_service("intelligence")``); without it the sample installs exactly as before. It

* scans the sample workspace with the *local* source (assets, columns, lineage from the notebooks / queries / report, classification suggestions);
* imports the SYNTHETIC Power BI scan of :mod:`olcf.sample.powerbi` (source kind ``scanner_import``, labelled as invented everywhere) and scans it, so the lineage of the model and report
  reaches the sample's gold tables through the normal name resolution;
* adds a business glossary for the sample's vocabulary (terms with real definitions that match the data) and links the terms to the columns, tables and measures they describe;
* creates the domain *Retail Sales* and the data product *Retail Sales Analytics* (the gold tables, a freshness SLA), with the installing user as owner and steward;
* approves the PII label on the e-mail column of ``dim_customer`` and leaves every other suggestion (the name columns, glossary links by name) in the review queue;
* certifies the three gold tables.

**Nothing here may fail the install.** Every step runs on its own; an error is logged and reported in the result (``problems``) and the other steps carry on. A caller without the
organisation-wide rights these steps need (``intelligence.manage``, ``governance.curate``) gets the steps that need them reported as problems. Without ``governance.approve`` the glossary terms
and the data product are created as ``draft`` (approving and publishing are for an approver).

Idempotent: terms, the domain and the product are get-or-create; owners, links, approvals and certifications are made once, while the product does not exist yet (the product is the last
thing created, so an install that stopped half-way finishes on the next call and one that finished is not touched again, which keeps a later change by a curator). Scans run when the sample
data was (re)written or when the Power BI source does not exist yet.
"""

from __future__ import annotations

import logging
from collections.abc import Callable
from typing import Any

from olcf.core.errors import BadRequest, Conflict, Forbidden, NotFound
from olcf.sample import powerbi
from olcf.security.rbac import Principal

log = logging.getLogger("olcf.sample")

DOMAIN = "Retail Sales"
PRODUCT = "Retail Sales Analytics"
GOLD_TABLES = ("gold_daily_sales", "gold_category_summary", "gold_customer_ltv")
CERTIFIED_COMMENT = "Certified by the sample installer: the gold tables are generated from the sample data and rebuilt by notebook 01_Retail_Medallion_ETL."
PII_COLUMNS = (("dim_customer", "email"),)
SLA = {"freshness_hours": 24, "availability": "Sample data: rebuilt only when the sample is reinstalled."}

#: glossary: (name, abbreviation, synonyms, definition, [(kind, table or model table, column or measure)])
#: kind: lh_table / lh_column are objects of the sample lakehouse, measure / model_table objects of the synthetic Power BI model.
TERMS: list[tuple[str, str | None, list[str], str, list[tuple[str, str, str | None]]]] = [
    ("Revenue", None, ["Sales", "Turnover"],
     "Money earned from selling products, after discounts and before costs. In the sample company revenue always means net revenue: the sum of net_amount over order lines "
     "(quantity x unit_price minus the discount). Returns, tax and shipping are not modelled.",
     [("lh_table", "gold_daily_sales", None), ("lh_table", "gold_category_summary", None), ("measure", "Daily Sales", "Total Revenue"), ("measure", "Category Summary", "Category Revenue")]),
    ("Net Revenue", None, ["Net Sales"],
     "Gross revenue minus discounts for an order line: net_amount = gross_amount - discount_amount. Stored as net_amount in fact_sales and as net_revenue in the gold tables.",
     [("lh_column", "fact_sales", "net_amount"), ("lh_column", "gold_daily_sales", "net_revenue"), ("lh_column", "gold_category_summary", "net_revenue")]),
    ("Gross Revenue", None, ["Gross Sales"],
     "Revenue before discounts: quantity x unit_price of an order line (gross_amount). Summed per day as gross_revenue in gold_daily_sales.",
     [("lh_column", "fact_sales", "gross_amount"), ("lh_column", "gold_daily_sales", "gross_revenue"), ("measure", "Daily Sales", "Gross Revenue")]),
    ("Discount", None, ["Markdown"],
     "Reduction of the list price granted on an order line, 0 to 25 percent (discount_pct); discount_amount = gross_amount x discount_pct / 100.",
     [("lh_column", "fact_sales", "discount_pct"), ("lh_column", "fact_sales", "discount_amount"), ("measure", "Daily Sales", "Discount Amount")]),
    ("Profit", None, ["Gross Profit"],
     "Net revenue minus the cost of the goods sold: profit = net_amount - cost_amount, where cost_amount = quantity x unit_cost of the product.",
     [("lh_column", "fact_sales", "profit"), ("lh_column", "gold_daily_sales", "profit"), ("measure", "Daily Sales", "Total Profit")]),
    ("Gross Margin", "GM", ["Margin", "Profit Margin"],
     "Profit as a share of net revenue: profit / net_revenue, shown in percent (margin_pct in gold_category_summary, the measure Gross Margin % in the Power BI model).",
     [("lh_column", "gold_category_summary", "margin_pct"), ("measure", "Daily Sales", "Gross Margin %")]),
    ("Order", None, ["Basket", "Transaction"],
     "One purchase by one customer on one date through one channel. An order has one to five lines; the lines share an order_id in fact_sales, and orders are counted as distinct order_id.",
     [("lh_column", "fact_sales", "order_id"), ("lh_column", "gold_daily_sales", "orders"), ("measure", "Daily Sales", "Total Orders")]),
    ("Average Order Value", "AOV", ["Basket Size"],
     "Net revenue divided by the number of orders (avg_order_value in the gold tables, the measure Average Order Value in the Power BI model).",
     [("lh_column", "gold_daily_sales", "avg_order_value"), ("measure", "Daily Sales", "Average Order Value")]),
    ("Customer Segment", None, ["Segment"],
     "The commercial type of a customer: Consumer, Corporate or Home Office (dim_customer.segment). Not the same as the value segment of Customer Lifetime Value.",
     [("lh_column", "dim_customer", "segment")]),
    ("Customer Lifetime Value", "LTV", ["Lifetime Revenue", "CLV"],
     "Total net revenue a customer has generated since the first order up to the end of the dataset (lifetime_revenue). Customers are grouped into the value segments Top 10%, "
     "Upper 40% and Lower 50% (ltv_segment).",
     [("lh_column", "gold_customer_ltv", "lifetime_revenue"), ("lh_column", "gold_customer_ltv", "ltv_segment"), ("measure", "Customer LTV", "Average Lifetime Revenue")]),
    ("Store", None, ["Shop", "Outlet"],
     "A selling location of the company: one of the physical stores (format Flagship, Mall or Outlet) or the Online store (format Online) to which web and mobile orders are booked.",
     [("lh_table", "dim_store", None), ("lh_column", "fact_sales", "store_id")]),
    ("Loyalty Tier", None, ["Membership Level"],
     "The loyalty programme level of a customer: Bronze, Silver, Gold or Platinum (dim_customer.loyalty_tier).",
     [("lh_column", "dim_customer", "loyalty_tier")]),
]


class Report:
    """What the integration did: ``steps`` (per step a small dict), ``created`` (what is new) and ``problems`` (what did not work, in words)."""

    def __init__(self) -> None:
        self.steps: dict[str, Any] = {}
        self.created: list[str] = []
        self.problems: list[str] = []

    def run(self, name: str, fn: Callable[[], Any]) -> Any:
        """Run one step; a failure is logged and recorded, never raised."""
        try:
            out = fn()
        except (Forbidden, NotFound, BadRequest, Conflict) as exc:
            self.steps[name] = {"ok": False, "error": f"{type(exc).__name__}: {exc}"}
            self.problems.append(f"{name}: {exc}")
            log.warning("sample intelligence step '%s' did not complete: %s", name, exc)
            return None
        except Exception as exc:  # noqa: BLE001 - the sample install must never fail because of Intelligence
            log.exception("sample intelligence step '%s' failed", name)
            self.steps[name] = {"ok": False, "error": f"{type(exc).__name__}: {exc}"}
            self.problems.append(f"{name}: {type(exc).__name__}: {exc}")
            return None
        self.steps[name] = {"ok": True, **(out if isinstance(out, dict) else {})}
        return out

    def view(self) -> dict[str, Any]:
        return {"installed": True, "ok": not self.problems, "steps": self.steps, "created": self.created, "problems": self.problems}


class _Assets:
    """Finds the graph assets the steps need (by the aliases the local scan gives them, or by name inside the sample workspace)."""

    def __init__(self, intel: Any, principal: Principal, workspace_id: str, lakehouse_id: str) -> None:
        self.intel, self.principal, self.wid, self.lh_id = intel, principal, workspace_id, lakehouse_id
        self._cache: dict[tuple[str, ...], str | None] = {}

    def lakehouse(self) -> str | None:
        return self._alias(("olcf_item", self.lh_id))

    def table(self, name: str) -> str | None:
        return self._alias(("lakehouse_table", f"{self.lh_id}:{name}"))

    def column(self, table: str, name: str) -> str | None:
        return self._alias(("lakehouse_column", f"{self.lh_id}:{table}:{name}"))

    def _alias(self, key: tuple[str, str]) -> str | None:
        if key not in self._cache:
            self._cache[key] = self.intel.registry.find(*key)
        return self._cache[key]

    def named(self, type_: str, name: str, parent: str | None = None, parent_type: str | None = None) -> str | None:
        """An active asset of the sample workspace by type and name (optionally under a parent of that name)."""
        key = (type_, name, parent or "", parent_type or "")
        if key not in self._cache:
            items, _ = self.intel.list_assets(self.principal, q=name, type=type_, workspace_id=self.wid, status="active", limit=200)
            found: str | None = None
            for item in items:
                if item["name"] != name:
                    continue
                if parent is None:
                    found = item["id"]
                    break
                par = self.intel.graph.asset_rows([item["parent_id"]]).get(item["parent_id"]) if item.get("parent_id") else None
                if par and par["name"] == parent and (parent_type is None or par["type"] == parent_type):
                    found = item["id"]
                    break
            self._cache[key] = found
        return self._cache[key]

    def term_target(self, kind: str, obj: str, member: str | None) -> str | None:
        if kind == "lh_table":
            return self.table(obj)
        if kind == "lh_column":
            return self.column(obj, member or "")
        if kind == "measure":
            return self.named("measure", member or "", parent=obj, parent_type="model_table")
        return self.named("model_table", obj)


def integrate(ctx: Any, principal: Principal, *, workspace_id: str, workspace_name: str, lakehouse_id: str, lakehouse_name: str, scan: bool) -> dict[str, Any] | None:
    """Add the sample to OLCF Intelligence; ``None`` when the module is not installed. Never raises (see the module docstring).

    ``scan``: the sample data was written by this call, so the graph is scanned again. Scans also run when the synthetic Power BI source does not exist yet.
    """
    intel = ctx.maybe_service("intelligence")
    if intel is None:
        return None
    report = Report()
    try:
        _integrate(ctx, intel, principal, report, workspace_id, workspace_name, lakehouse_id, lakehouse_name, scan)
    except Exception as exc:  # noqa: BLE001 - last line of defence
        log.exception("sample intelligence integration failed")
        report.problems.append(f"integration: {type(exc).__name__}: {exc}")
    return report.view()


def _integrate(ctx: Any, intel: Any, principal: Principal, rep: Report, wid: str, wname: str, lh_id: str, lh_name: str, scan: bool) -> None:
    disc, gov = intel.discovery, intel.governance
    source_known = _source_exists(disc, powerbi.SOURCE_NAME)
    scan = scan or not source_known
    if disc is None:
        rep.problems.append("discovery: the Intelligence discovery service is not available")
    elif scan:
        rep.run("scan_local", lambda: _scan_local(disc, principal, wid))
        if rep.run("import_powerbi", lambda: _import_powerbi(disc, principal, wid, lh_name)) is not None and not source_known:
            rep.created.append(f"source:{powerbi.SOURCE_NAME}")
    else:
        rep.steps["scan"] = {"ok": True, "skipped": "already scanned; reinstall to scan again"}
    assets = _Assets(intel, principal, wid, lh_id)
    if gov is None:
        rep.problems.append("governance: the governance service is not available (glossary, domain, data product, classification and certification were not set up)")
        return
    product = _find(gov.products.list(principal, q=PRODUCT, limit=50)[0], PRODUCT) if _can_read(gov, principal) else None
    fresh = product is None
    if scan or fresh:
        rep.run("classification_scan", lambda: _classification_scan(gov, principal, wid))
    rep.run("glossary", lambda: _glossary(gov, principal, assets, rep, fresh))
    if fresh:
        rep.run("term_link_suggestions", lambda: _classification_scan(gov, principal, wid))
    domain = rep.run("domain", lambda: _domain(gov, principal, assets, rep, fresh))
    if fresh:
        rep.run("owners", lambda: _owners(gov, principal, assets, domain))
        rep.run("classifications", lambda: _classifications(gov, principal, assets))
        rep.run("certification", lambda: _certify(gov, principal, assets))
    else:
        rep.steps["curation"] = {"ok": True, "skipped": "the data product exists: owners, classifications and certification are left as the curators set them"}
    rep.run("data_product", lambda: _data_product(gov, principal, assets, domain, product, rep, wname))


def _can_read(gov: Any, principal: Principal) -> bool:
    try:
        gov.products.org_reader(principal)
    except Exception:  # noqa: BLE001
        return False
    return True


def _source_exists(disc: Any, name: str) -> bool:
    if disc is None:
        return False
    try:
        disc.sources.find(name)
    except NotFound:
        return False
    except Exception:  # noqa: BLE001 - unknown: scan, the scan reports the real problem
        return False
    return True


def _scan_local(disc: Any, principal: Principal, wid: str) -> dict[str, Any]:
    row = disc.runner.start(principal, "local", mode="incremental", trigger="manual", workspace_filter=[wid], wait=True)
    stats = row.get("stats") or {}
    if row.get("status") not in ("succeeded", "partial"):
        raise BadRequest(f"the local scan ended {row.get('status')}: {row.get('error') or 'see GET /v1/intelligence/scans'}")
    return {"scan_id": row["id"], "status": row["status"], "assets": stats.get("assets") or stats.get("assets_seen")}


def _import_powerbi(disc: Any, principal: Principal, wid: str, lh_name: str) -> dict[str, Any]:
    out = disc.import_scanner(principal, powerbi.scan_document(lh_name), name=powerbi.SOURCE_NAME, workspace_id=wid, scan=True, wait=True)
    sc = out.get("scan") or {}
    if sc.get("status") not in ("succeeded", "partial"):
        raise BadRequest(f"the scan of the synthetic Power BI result ended {sc.get('status')}: {sc.get('error') or 'see GET /v1/intelligence/scans'}")
    return {"source": out["source"]["name"], "scan_id": sc.get("id"), "status": sc.get("status"), "label": powerbi.SYNTHETIC_LABEL}


def _find(rows: list[dict[str, Any]], name: str) -> dict[str, Any] | None:
    return next((r for r in rows if r.get("name", "").lower() == name.lower()), None)


def _me(gov: Any, principal: Principal) -> dict[str, Any] | None:
    """The owner spec of the installing user (``None`` when the caller is not an OLCF user, for example the system principal)."""
    spec_ = {"user": principal.id}
    try:
        gov.owners.parse(spec_)
    except BadRequest:
        return None
    return spec_


def _classification_scan(gov: Any, principal: Principal, wid: str) -> dict[str, Any]:
    out = gov.classifications.scan(principal, workspace_id=wid)
    return {"columns_examined": out["assets_examined"], "suggested": out["classification"]["created"], "term_links_suggested": out["term_links"]["suggested"]}


def _glossary(gov: Any, principal: Principal, assets: _Assets, rep: Report, fresh: bool) -> dict[str, Any]:
    made = linked = missing = 0
    existing = {t["name"].lower(): t for t in gov.glossary.list(principal, glossary="default", limit=500)[0]}
    me = _me(gov, principal)
    for name, abbr, synonyms, definition, targets in TERMS:
        term = existing.get(name.lower())
        if term is None:
            body: dict[str, Any] = {"name": name, "definition": definition, "synonyms": synonyms, "status": "approved" if principal.can("governance.approve") else "draft",
                                    "glossary": "default"}
            if abbr:
                body["abbreviation"] = abbr
            if me:
                body["owners"] = [{**me, "role": "steward"}]
            try:
                term = gov.glossary.create(principal, body)
                made += 1
                rep.created.append(f"term:{name}")
            except Conflict:
                term = _find(gov.glossary.list(principal, q=name, glossary="default", limit=50)[0], name)
            if term is None:
                continue
            made_now = True
        else:
            made_now = False
        if made_now or fresh:
            for kind, obj, member in targets:
                aid = assets.term_target(kind, obj, member)
                if aid is None:
                    missing += 1
                    continue
                try:
                    gov.glossary.link(principal, term["id"], asset_id=aid, comment="Linked by the sample installer.")
                    linked += 1
                except Conflict:
                    pass
    return {"terms": len(TERMS), "created": made, "links": linked, "targets_not_found": missing}


def _domain(gov: Any, principal: Principal, assets: _Assets, rep: Report, fresh: bool) -> dict[str, Any]:
    dom = _find(gov.domains.list(principal, q=DOMAIN, limit=50)[0], DOMAIN)
    if dom is None:
        try:
            dom = gov.domains.create(principal, {"name": DOMAIN, "description": "Sales, customers, products and stores of the sample retail company (synthetic data)."})
            rep.created.append(f"domain:{DOMAIN}")
        except Conflict:
            dom = _find(gov.domains.list(principal, q=DOMAIN, limit=50)[0], DOMAIN)
    if dom is None:
        raise NotFound(f"domain '{DOMAIN}' could not be created or found")
    if fresh:
        for aid in (assets.lakehouse(), assets.named("semantic_model", powerbi.MODEL_NAME), assets.named("report", powerbi.REPORT_NAME)):
            if aid:
                gov.domains.set_asset_domain(principal, aid, dom["id"])
    return {"id": dom["id"], "name": dom["name"]}


def _owners(gov: Any, principal: Principal, assets: _Assets, domain: dict[str, Any] | None) -> dict[str, Any]:
    me = _me(gov, principal)
    if me is None:
        return {"skipped": "the installing principal is not an OLCF user, so there is nobody to make owner"}
    n = 0
    subjects: list[tuple[str, str | None, str]] = []
    if domain:
        subjects.append(("domain", domain["id"], "owner"))
    for t in GOLD_TABLES:
        subjects.append(("asset", assets.table(t), "owner"))
    subjects.append(("asset", assets.table("dim_customer"), "steward"))
    for stype, sid, role in subjects:
        if not sid:
            continue
        try:
            gov.owners.add(principal, stype, sid, {**me, "role": role})
            n += 1
        except Conflict:
            pass
    return {"owners_added": n, "user": principal.name}


def _classifications(gov: Any, principal: Principal, assets: _Assets) -> dict[str, Any]:
    approved = absent = 0
    for table, column in PII_COLUMNS:
        aid = assets.column(table, column)
        if aid is None:
            absent += 1
            continue
        try:
            row = gov.classifications.assign(principal, aid, "PII", comment="Approved by the sample installer: personal data (synthetic people, example.com addresses).")
        except Conflict:
            continue
        approved += 1 if row.get("status") == "approved" else 0
    counts = gov.classifications.queue_counts(principal, None)
    return {"approved": approved, "columns_not_found": absent, "left_in_review_queue": counts}


def _certify(gov: Any, principal: Principal, assets: _Assets) -> dict[str, Any]:
    done = []
    for t in GOLD_TABLES:
        aid = assets.table(t)
        if aid is None:
            continue
        gov.certification.put(principal, "asset", aid, "certified", CERTIFIED_COMMENT)
        done.append(t)
    return {"certified": done}


def _data_product(gov: Any, principal: Principal, assets: _Assets, domain: dict[str, Any] | None, product: dict[str, Any] | None, rep: Report, workspace_name: str) -> dict[str, Any]:
    members = [a for a in (assets.table(t) for t in GOLD_TABLES) if a]
    if len(members) != len(GOLD_TABLES):
        raise NotFound("the gold tables are not in the Intelligence graph yet, so the data product cannot list them", remediation="run a scan and install the sample again")
    if product is None:
        me = _me(gov, principal)
        body: dict[str, Any] = {"name": PRODUCT, "status": "published" if principal.can("governance.approve") else "draft", "member_asset_ids": members, "sla": SLA,
                                "description": f"The gold tables of the sample retail company ({workspace_name}): daily sales, category summary and customer lifetime value, with the Power BI model and report built on them."}
        if domain:
            body["domain_id"] = domain["id"]
        if me:
            body["owners"] = [{**me, "role": "owner"}, {**me, "role": "steward"}]
        product = gov.products.create(principal, body)
        rep.created.append(f"data_product:{PRODUCT}")
        return {"created": True, "members": len(members)}
    gov.products.set_members(principal, product["id"], sorted(set(members) | set(gov.products.member_ids(product["id"]))))
    return {"created": False, "members": len(members)}


__all__ = ["DOMAIN", "GOLD_TABLES", "PRODUCT", "TERMS", "integrate"]
