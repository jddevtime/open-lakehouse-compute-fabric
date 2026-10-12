# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Deterministic generator of the sample retail dataset.

Pure Python + PyArrow + DuckDB (no NumPy): every random decision comes from ``random.Random(seed).random()``, the one method whose output
Python guarantees across versions, so the same seed gives the same bytes everywhere.  The gold tables are computed with the *same SQL*
that notebook ``01_Retail_Medallion_ETL`` runs on Spark (``GOLD_SQL``), so the pre-installed gold tables match what the notebook produces.

Standalone use::

    python -m olcf.sample.generate --out ./retail            # Tables/<name> (Delta) + Files/raw/... CSVs
    python -m olcf.sample.generate --write-schemas           # regenerate schemas/sample-retail.schema.json
"""

from __future__ import annotations

import argparse
import bisect
import datetime as dt
import io
import json
import math
import random
import sys
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.csv as pacsv

from olcf.sample import spec

UTC = dt.UTC
FIRST = ["Olivia", "Liam", "Emma", "Noah", "Ava", "Elijah", "Sophia", "Lucas", "Isabella", "Mason", "Mia", "Ethan", "Amelia", "Logan", "Harper", "James", "Evelyn", "Aiden",
         "Abigail", "Jackson", "Ella", "Sebastian", "Scarlett", "Mateo", "Grace", "Henry", "Chloe", "Owen", "Camila", "Wyatt", "Priya", "Arjun", "Mei", "Hiro", "Fatima", "Omar",
         "Sofia", "Diego", "Nora", "Isaac"]
LAST = ["Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller", "Davis", "Rodriguez", "Martinez", "Hernandez", "Lopez", "Gonzalez", "Wilson", "Anderson", "Thomas",
        "Taylor", "Moore", "Jackson", "Martin", "Lee", "Perez", "Thompson", "White", "Harris", "Sanchez", "Clark", "Ramirez", "Lewis", "Robinson", "Walker", "Young", "Allen", "King",
        "Wright", "Scott", "Torres", "Nguyen", "Hill", "Patel"]
CITIES = [("New York", "NY", "East"), ("Boston", "MA", "East"), ("Philadelphia", "PA", "East"), ("Atlanta", "GA", "South"), ("Miami", "FL", "South"), ("Charlotte", "NC", "South"),
          ("Houston", "TX", "South"), ("Dallas", "TX", "South"), ("Chicago", "IL", "Central"), ("Minneapolis", "MN", "Central"), ("Denver", "CO", "Central"),
          ("Kansas City", "MO", "Central"), ("Seattle", "WA", "West"), ("Portland", "OR", "West"), ("San Francisco", "CA", "West"), ("Los Angeles", "CA", "West"),
          ("San Diego", "CA", "West"), ("Phoenix", "AZ", "West"), ("Salt Lake City", "UT", "West"), ("Nashville", "TN", "South"), ("Detroit", "MI", "Central"),
          ("Pittsburgh", "PA", "East"), ("Austin", "TX", "South"), ("Columbus", "OH", "Central")]
CATEGORIES: dict[str, dict[str, Any]] = {
    "Electronics": {"subs": ["Phones", "Laptops", "Audio", "Accessories"], "cost": (18, 260), "weight": 0.8, "brands": ["Voltix", "Nexora", "Sonaro", "Pixelon"]},
    "Home & Kitchen": {"subs": ["Cookware", "Appliances", "Bedding", "Storage"], "cost": (8, 140), "weight": 1.3, "brands": ["Hearthly", "Kitchora", "Nestwell", "Domo"]},
    "Apparel": {"subs": ["Shirts", "Jeans", "Outerwear", "Footwear"], "cost": (6, 90), "weight": 1.6, "brands": ["Threadly", "Northbay", "Urbana", "Stride"]},
    "Sports & Outdoors": {"subs": ["Fitness", "Camping", "Cycling", "Team Sports"], "cost": (7, 220), "weight": 0.9, "brands": ["Summitor", "Trailix", "Velora", "Athlo"]},
    "Beauty": {"subs": ["Skincare", "Haircare", "Fragrance", "Makeup"], "cost": (3, 60), "weight": 1.0, "brands": ["Lumiere", "Verdea", "Auralis", "Bloomé"]},
    "Books & Media": {"subs": ["Fiction", "Non-fiction", "Kids", "Games"], "cost": (2, 35), "weight": 0.8, "brands": ["Pageturn", "Inkwell", "Brightleaf", "Playmate"]},
}
MONTH_FACTOR = [0.85, 0.80, 0.90, 0.95, 1.00, 1.00, 1.00, 1.05, 0.95, 1.05, 1.35, 1.60]
WEEKDAY_FACTOR = [0.90, 0.90, 0.95, 1.00, 1.15, 1.30, 1.20]            # Monday first
YEAR_GROWTH = {2023: 1.00, 2024: 1.18, 2025: 1.35}
CHANNEL_MIX = {2023: (0.62, 0.28, 0.10), 2024: (0.52, 0.34, 0.14), 2025: (0.45, 0.38, 0.17)}   # store, web, mobile
CHANNELS = ("store", "web", "mobile")
MONTH_NAMES = ["January", "February", "March", "April", "May", "June", "July", "August", "September", "October", "November", "December"]
DAY_NAMES = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]

#: SQL shared with the Spark notebook.  ``{fact}``, ``{product}`` etc. are replaced by table names (DuckDB: registered views; Spark: lakehouse tables).
GOLD_SQL = {
    "gold_daily_sales": """
SELECT order_date AS sale_date,
       CAST(count(DISTINCT order_id) AS BIGINT) AS orders,
       CAST(sum(quantity) AS BIGINT) AS units,
       CAST(count(DISTINCT customer_id) AS BIGINT) AS customers,
       round(sum(gross_amount), 2) AS gross_revenue,
       round(sum(net_amount), 2) AS net_revenue,
       round(sum(profit), 2) AS profit,
       round(sum(net_amount) / count(DISTINCT order_id), 2) AS avg_order_value
FROM {fact}
GROUP BY order_date
""",
    "gold_category_summary": """
WITH by_cat AS (
  SELECT p.category AS category, CAST(f.year AS INT) AS year,
         CAST(count(DISTINCT f.order_id) AS BIGINT) AS orders, CAST(sum(f.quantity) AS BIGINT) AS units,
         round(sum(f.net_amount), 2) AS net_revenue, round(sum(f.profit), 2) AS profit
  FROM {fact} f JOIN {product} p ON p.product_id = f.product_id
  GROUP BY p.category, f.year)
SELECT category, year, orders, units, net_revenue, profit,
       round(100.0 * profit / net_revenue, 2) AS margin_pct,
       round(100.0 * net_revenue / sum(net_revenue) OVER (PARTITION BY year), 2) AS revenue_share_pct,
       CAST(rank() OVER (PARTITION BY year ORDER BY net_revenue DESC) AS INT) AS revenue_rank
FROM by_cat
""",
    "gold_customer_ltv": """
WITH per_customer AS (
  SELECT customer_id, min(order_date) AS first_order_date, max(order_date) AS last_order_date,
         CAST(count(DISTINCT order_id) AS BIGINT) AS orders, CAST(sum(quantity) AS BIGINT) AS units,
         round(sum(net_amount), 2) AS lifetime_revenue, round(sum(profit), 2) AS lifetime_profit
  FROM {fact} GROUP BY customer_id),
tiles AS (
  SELECT *, ntile(10) OVER (ORDER BY lifetime_revenue DESC, customer_id) AS decile FROM per_customer)
SELECT customer_id, first_order_date, last_order_date, orders, units, lifetime_revenue, lifetime_profit,
       round(lifetime_revenue / orders, 2) AS avg_order_value,
       CAST({recency} AS INT) AS recency_days,
       CASE WHEN decile = 1 THEN 'Top 10%' WHEN decile <= 5 THEN 'Upper 40%' ELSE 'Lower 50%' END AS ltv_segment
FROM tiles
""",
}
#: Dialect specific day difference used by ``{recency}`` in ``GOLD_SQL`` (everything else in the gold SQL is common DuckDB/Spark SQL).
RECENCY = {"duckdb": "datediff('day', last_order_date, DATE '{as_of}')", "spark": "datediff(DATE '{as_of}', last_order_date)"}


def gold_sql(name: str, dialect: str) -> str:
    """The gold query ``name`` for ``duckdb`` or ``spark`` with ``{fact}``, ``{product}`` and ``{as_of}`` left as placeholders."""
    return GOLD_SQL[name].replace("{recency}", RECENCY[dialect])


class Rng:
    """Version-stable random helpers built on ``random()`` only."""

    def __init__(self, seed: int) -> None:
        self._r = random.Random(seed)

    def u(self) -> float:
        return self._r.random()

    def integer(self, lo: int, hi: int) -> int:
        return lo + int(self.u() * (hi - lo + 1))

    def pick(self, seq: list[Any] | tuple[Any, ...]) -> Any:
        return seq[int(self.u() * len(seq))]

    def uniform(self, lo: float, hi: float) -> float:
        return lo + (hi - lo) * self.u()

    def gauss(self) -> float:
        return math.sqrt(-2.0 * math.log(1.0 - self.u())) * math.cos(2.0 * math.pi * self.u())

    def date(self, lo: dt.date, hi: dt.date) -> dt.date:
        return lo + dt.timedelta(days=self.integer(0, (hi - lo).days))


class Chooser:
    """Weighted choice with an O(log n) draw."""

    def __init__(self, weights: list[float]) -> None:
        self.cum: list[float] = []
        total = 0.0
        for w in weights:
            total += w
            self.cum.append(total)
        self.total = total

    def draw(self, rng: Rng) -> int:
        return min(bisect.bisect_left(self.cum, rng.u() * self.total), len(self.cum) - 1)


def _schema(table: str) -> pa.Schema:
    fields = []
    for c in spec.TABLES[table]["columns"]:
        t = spec.ARROW_TYPES[c["type"]]
        typ = pa.timestamp("us", tz="UTC") if t.startswith("timestamp") else pa.type_for_alias(t)
        fields.append(pa.field(c["name"], typ, nullable=c["nullable"]))
    return pa.schema(fields)


def _table(name: str, cols: dict[str, list[Any]]) -> pa.Table:
    schema = _schema(name)
    return pa.table({f.name: pa.array(cols[f.name], type=f.type) for f in schema}, schema=schema)


def _price_points(lo: float, hi: float, rng: Rng) -> float:
    return round(lo + (hi - lo) * rng.u() ** 2.2, 2)           # skewed to cheap items


def generate(size: str = "standard", seed: int = spec.SEED) -> dict[str, pa.Table]:
    """All tables of the dataset (dimensions, facts, gold) as Arrow tables keyed by name."""
    if size not in spec.SIZES:
        raise ValueError(f"unknown size '{size}'; choose one of {', '.join(spec.SIZES)}")
    n = spec.SIZES[size]
    rng = Rng(seed)
    start, end = dt.date.fromisoformat(spec.START_DATE), dt.date.fromisoformat(spec.END_DATE)

    # ---- dim_date
    days: list[dt.date] = [start + dt.timedelta(days=i) for i in range((end - start).days + 1)]
    holidays = {(1, 1), (7, 4), (11, 11), (12, 25), (6, 19)}
    dim_date = _table("dim_date", {
        "date_key": [d.year * 10000 + d.month * 100 + d.day for d in days], "date": days, "year": [d.year for d in days], "quarter": [(d.month - 1) // 3 + 1 for d in days],
        "month": [d.month for d in days], "month_name": [MONTH_NAMES[d.month - 1] for d in days], "day": [d.day for d in days], "day_of_week": [d.isoweekday() for d in days],
        "day_name": [DAY_NAMES[d.weekday()] for d in days], "week_of_year": [d.isocalendar()[1] for d in days], "is_weekend": [d.weekday() >= 5 for d in days],
        "is_holiday": [(d.month, d.day) in holidays for d in days]})
    day_weights = [MONTH_FACTOR[d.month - 1] * WEEKDAY_FACTOR[d.weekday()] * YEAR_GROWTH[d.year] for d in days]
    day_chooser = Chooser(day_weights)

    # ---- dim_store (last store is the online store)
    n_phys = n["stores"] - 1
    stores: dict[str, list[Any]] = {k: [] for k in ("store_id", "store_name", "city", "state", "region", "format", "opened_date", "sq_ft")}
    for i in range(n_phys):
        city, state, region = CITIES[i % len(CITIES)]
        fmt = ["Flagship", "Mall", "Outlet"][int(rng.u() * 3)] if i >= 2 else "Flagship"
        stores["store_id"].append(i + 1)
        stores["store_name"].append(f"{city} {fmt} #{i // len(CITIES) + 1}")
        stores["city"].append(city)
        stores["state"].append(state)
        stores["region"].append(region)
        stores["format"].append(fmt)
        stores["opened_date"].append(rng.date(dt.date(2008, 1, 1), dt.date(2022, 12, 31)))
        stores["sq_ft"].append({"Flagship": 24000, "Mall": 9000, "Outlet": 14000}[fmt] + rng.integer(-1500, 2500))
    online_id = n_phys + 1
    for k, v in (("store_id", online_id), ("store_name", "Online Store"), ("city", "Seattle"), ("state", "WA"), ("region", "Online"), ("format", "Online"),
                 ("opened_date", dt.date(2015, 3, 1)), ("sq_ft", 0)):
        stores[k].append(v)
    dim_store = _table("dim_store", stores)
    store_chooser = Chooser([1.6 if f == "Flagship" else 1.0 for f in stores["format"][:n_phys]])

    # ---- dim_product
    cats = list(CATEGORIES)
    prod: dict[str, list[Any]] = {k: [] for k in ("product_id", "sku", "product_name", "category", "subcategory", "brand", "unit_cost", "list_price", "launch_date")}
    prod_weights: list[float] = []
    for i in range(n["products"]):
        cat = cats[i % len(cats)]
        c = CATEGORIES[cat]
        sub = c["subs"][(i // len(cats)) % len(c["subs"])]
        cost = _price_points(*c["cost"], rng)
        price = round(cost * rng.uniform(1.35, 2.3), 2)
        price = float(int(price)) + 0.99 if price >= 10 else price
        launch = rng.date(dt.date(2019, 1, 1), dt.date(2022, 12, 31)) if rng.u() < 0.88 else rng.date(start, dt.date(2025, 6, 30))
        prod["product_id"].append(1000 + i)
        prod["sku"].append(f"SKU-{cat[:3].upper()}-{i:04d}")
        prod["product_name"].append(f"{rng.pick(c['brands'])} {sub[:-1] if sub.endswith('s') else sub} {100 + i}")
        prod["category"].append(cat)
        prod["subcategory"].append(sub)
        prod["brand"].append(prod["product_name"][-1].split(" ")[0])
        prod["unit_cost"].append(cost)
        prod["list_price"].append(max(price, round(cost * 1.1, 2)))
        prod["launch_date"].append(launch)
        prod_weights.append(c["weight"] / (1.0 + (i // len(cats)) * 0.35))        # a few best sellers per category, then a long tail
    dim_product = _table("dim_product", prod)
    prod_chooser = Chooser(prod_weights)
    old_products = [i for i, d in enumerate(prod["launch_date"]) if d < start]

    # ---- customers (is_active is filled in after the sales exist)
    cust: dict[str, list[Any]] = {k: [] for k in ("customer_id", "first_name", "last_name", "email", "city", "state", "country", "segment", "signup_date", "loyalty_tier", "is_active")}
    for i in range(n["customers"]):
        first, last = rng.pick(FIRST), rng.pick(LAST)
        city, state, _ = CITIES[int(rng.u() ** 1.4 * len(CITIES))]
        cid = 100_000 + i
        cust["customer_id"].append(cid)
        cust["first_name"].append(first)
        cust["last_name"].append(last)
        cust["email"].append(f"{first}.{last}.{cid}@example.com".lower())
        cust["city"].append(city)
        cust["state"].append(state)
        cust["country"].append("US")
        cust["segment"].append(["Consumer", "Corporate", "Home Office"][Chooser([0.6, 0.25, 0.15]).draw(rng)])
        cust["signup_date"].append(rng.date(dt.date(2018, 1, 1), dt.date(2022, 12, 31)) if rng.u() < 0.62 else rng.date(start, dt.date(2025, 9, 30)))
        cust["loyalty_tier"].append(["Bronze", "Silver", "Gold", "Platinum"][Chooser([0.5, 0.3, 0.15, 0.05]).draw(rng)])
        cust["is_active"].append(False)
    cust_weights = [(i + 1) ** -0.55 for i in range(n["customers"])]
    # shuffle the popularity ranking so loyal customers are not simply the lowest ids
    order = sorted(range(n["customers"]), key=lambda _i: rng.u())
    cust_w = [0.0] * n["customers"]
    for rank, idx in enumerate(order):
        cust_w[idx] = cust_weights[rank]
    cust_chooser = Chooser(cust_w)
    old_customers = [i for i, d in enumerate(cust["signup_date"]) if d < start]

    # ---- fact_sales
    sales: dict[str, list[Any]] = {c["name"]: [] for c in spec.TABLES["fact_sales"]["columns"]}
    line_chooser = Chooser([0.45, 0.30, 0.15, 0.07, 0.03])
    qty_chooser = Chooser([0.70, 0.20, 0.07, 0.03])
    disc_normal = [0, 0, 0, 0, 5, 5, 10, 15]
    disc_peak = [0, 5, 10, 15, 20, 25, 25, 20]
    pay_chooser = Chooser([0.55, 0.12, 0.2, 0.13])
    pays = ["card", "cash", "paypal", "gift_card"]
    sale_id, order_id = 1, 5_000_000
    last_active = end - dt.timedelta(days=182)
    active: set[int] = set()
    target = n["sales_lines"]
    while sale_id <= target:
        di = day_chooser.draw(rng)
        day = days[di]
        ci = cust_chooser.draw(rng)
        for _ in range(8):
            if cust["signup_date"][ci] <= day:
                break
            ci = cust_chooser.draw(rng)
        else:
            ci = rng.pick(old_customers)
        mix = CHANNEL_MIX[day.year]
        channel = CHANNELS[Chooser(list(mix)).draw(rng)]
        store = store_chooser.draw(rng) + 1 if channel == "store" else online_id
        secs = rng.integer(8 * 3600, 22 * 3600) if channel == "store" else rng.integer(0, 86399)
        ts = dt.datetime(day.year, day.month, day.day, tzinfo=UTC) + dt.timedelta(seconds=secs)
        pay = pays[pay_chooser.draw(rng)] if channel == "store" else pays[[0, 2, 3][int(rng.u() * 3)]]
        order_id += 1
        peak = day.month in (11, 12)
        if day >= last_active:
            active.add(ci)
        for _ in range(line_chooser.draw(rng) + 1):
            pi = prod_chooser.draw(rng)
            for _ in range(6):
                if prod["launch_date"][pi] <= day:
                    break
                pi = prod_chooser.draw(rng)
            else:
                pi = rng.pick(old_products)
            q = qty_chooser.draw(rng) + 1
            price = round(prod["list_price"][pi] * rng.uniform(0.97, 1.03), 2)
            pct = float(rng.pick(disc_peak if peak else disc_normal))
            gross = round(q * price, 2)
            disc = round(gross * pct / 100.0, 2)
            net = round(gross - disc, 2)
            cost = round(q * prod["unit_cost"][pi], 2)
            row = {"sale_id": sale_id, "order_id": order_id, "order_date": day, "order_ts": ts, "date_key": day.year * 10000 + day.month * 100 + day.day, "year": day.year, "month": day.month,
                   "customer_id": cust["customer_id"][ci], "product_id": prod["product_id"][pi], "store_id": store, "channel": channel, "quantity": q, "unit_price": price,
                   "discount_pct": pct, "gross_amount": gross, "discount_amount": disc, "net_amount": net, "cost_amount": cost, "profit": round(net - cost, 2), "payment_method": pay}
            for k, v in row.items():
                sales[k].append(v)
            sale_id += 1
    # order rows chronologically (stable), then renumber so ids increase with time
    idx = sorted(range(len(sales["sale_id"])), key=lambda i: (sales["order_date"][i], sales["order_id"][i], sales["sale_id"][i]))
    for k in sales:
        sales[k] = [sales[k][i] for i in idx]
    sales["sale_id"] = list(range(1, len(idx) + 1))
    for i in range(len(cust["customer_id"])):
        cust["is_active"][i] = i in active
    dim_customer = _table("dim_customer", cust)
    fact_sales = _table("fact_sales", sales)

    # ---- fact_web_events: one visit at a time, each following a browse -> cart -> checkout -> purchase funnel
    ev: dict[str, list[Any]] = {c["name"]: [] for c in spec.TABLES["fact_web_events"]["columns"]}
    device_chooser, browser_chooser = Chooser([0.42, 0.48, 0.10]), Chooser([0.55, 0.28, 0.1, 0.07])
    ref_chooser = Chooser([0.3, 0.32, 0.14, 0.1, 0.14])
    conversion = {"desktop": 1.25, "mobile": 0.8, "tablet": 1.0}
    eid, sid = 1, 0
    target_ev = n["web_events"]
    while eid <= target_ev:
        sid += 1
        day = days[day_chooser.draw(rng)]
        ci = cust_chooser.draw(rng)
        customer = cust["customer_id"][ci] if rng.u() < 0.65 and cust["signup_date"][ci] <= day else None
        device = ["desktop", "mobile", "tablet"][device_chooser.draw(rng)]
        browser = ["Chrome", "Safari", "Edge", "Firefox"][browser_chooser.draw(rng)]
        referrer = ["direct", "search", "social", "email", "ads"][ref_chooser.draw(rng)]
        country = "US" if rng.u() < 0.87 else rng.pick(["CA", "GB", "DE", "MX"])
        k = conversion[device]
        visit: list[tuple[str, str, bool]] = []                    # (event_type, page, refers to a product)
        if rng.u() < 0.6:
            visit.append(("page_view", "home", False))
        visit.append(("page_view", "category", False))
        if rng.u() < 0.3:
            visit.append(("search", "search", False))
        views = 1 + int(rng.u() * 3.5)
        visit += [("page_view", "product", True)] * views
        if rng.u() < 0.24 * k:
            visit.append(("add_to_cart", "cart", True))
            if rng.u() < 0.55:
                visit.append(("checkout_start", "checkout", False))
                if rng.u() < 0.65:
                    visit.append(("purchase", "checkout", False))
        t = rng.integer(0, 80000)
        for et, page, with_product in visit:
            if eid > target_ev:
                break
            t = min(t + rng.integer(3, 120), 86399)
            ts = dt.datetime(day.year, day.month, day.day, tzinfo=UTC) + dt.timedelta(seconds=t)
            pid = prod["product_id"][prod_chooser.draw(rng)] if with_product else None
            for key, val in (("event_id", eid), ("event_ts", ts), ("event_date", day), ("session_id", f"S{sid:07d}"), ("customer_id", customer), ("event_type", et), ("page", page),
                             ("product_id", pid), ("device", device), ("browser", browser), ("referrer", referrer), ("country", country),
                             ("duration_ms", int(math.exp(8.3 + 0.9 * rng.gauss())))):
                ev[key].append(val)
            eid += 1
    order = sorted(range(len(ev["event_id"])), key=lambda i: (ev["event_ts"][i], ev["event_id"][i]))
    for key in ev:
        ev[key] = [ev[key][i] for i in order]
    ev["event_id"] = list(range(1, len(order) + 1))
    fact_web_events = _table("fact_web_events", ev)

    out: dict[str, pa.Table] = {"dim_customer": dim_customer, "dim_product": dim_product, "dim_store": dim_store, "dim_date": dim_date,
                                "fact_sales": fact_sales, "fact_web_events": fact_web_events}
    out.update(gold_tables(fact_sales, dim_product))
    return out


def gold_tables(fact_sales: pa.Table, dim_product: pa.Table) -> dict[str, pa.Table]:
    """Gold aggregates computed with DuckDB from the facts (single thread, rounded: results are reproducible to the cent)."""
    import duckdb

    con = duckdb.connect(":memory:", config={"threads": "1", "enable_external_access": "false"})
    try:
        con.register("fact_sales", fact_sales)
        con.register("dim_product", dim_product)
        out: dict[str, pa.Table] = {}
        for name in GOLD_SQL:
            q = gold_sql(name, "duckdb").format(fact="fact_sales", product="dim_product", as_of=spec.AS_OF)
            res = con.execute(q + " ORDER BY 1, 2" if name == "gold_category_summary" else q + " ORDER BY 1").to_arrow_table()
            schema = _schema(name)
            out[name] = pa.table({f.name: res.column(f.name).cast(f.type) for f in schema}, schema=schema)
        return out
    finally:
        con.close()


# ----------------------------------------------------------------------------- raw files
def raw_csvs(tables: dict[str, pa.Table]) -> dict[str, bytes]:
    """The ``Files/raw/...`` CSV exports: dimensions as one file each, sales one file per year."""
    out: dict[str, bytes] = {}

    def to_csv(t: pa.Table) -> bytes:
        buf = io.BytesIO()
        pacsv.write_csv(t, buf)
        return buf.getvalue()

    for path, table in spec.RAW_FILES.items():
        out[path] = to_csv(tables[table])
    sales = tables["fact_sales"]
    years = sorted({int(y) for y in sales.column("year").unique().to_pylist()})
    flat = sales.drop_columns(["year", "month"])           # a raw export has no partition columns; the notebook derives them
    for y in years:
        mask = pc.equal(sales.column("year"), y)
        out[f"Files/raw/sales/sales_{y}.csv"] = to_csv(flat.filter(mask))
    return out


def readme_text(tables: dict[str, pa.Table], size: str) -> str:
    rows = {k: v.num_rows for k, v in tables.items()}
    lines = [f"# {spec.WORKSPACE}: {spec.LAKEHOUSE}", "", "A synthetic retail company (no real data), three years of trading from "
             f"{spec.START_DATE} to {spec.END_DATE}. Generated deterministically (seed {spec.SEED}, dataset version {spec.VERSION}, size `{size}`).", "", "## Tables", "",
             "| table | layer | rows | description |", "|---|---|---:|---|"]
    for name, t in spec.TABLES.items():
        lines.append(f"| `{name}` | {t['layer']} | {rows.get(name, 0):,} | {t['description']} |")
    lines += ["", "## Raw files", "", "`Files/raw/customers`, `products`, `stores` (CSV) and `Files/raw/sales/sales_<year>.csv`; the ETL notebook reads these as the bronze layer.", "",
              "## Try it", "", f"* SQL: `SELECT category, round(sum(net_amount)) FROM {spec.LAKEHOUSE}.fact_sales f JOIN {spec.LAKEHOUSE}.dim_product USING (product_id) GROUP BY 1`",  # noqa: S608
              "* Files: `SELECT * FROM read_csv('Files/raw/stores/stores.csv')`", "* Report: *Retail Sales Overview*", "* Notebooks: `01_Retail_Medallion_ETL` (Spark), `02_Retail_Explore_Python` (no Spark)", ""]
    return "\n".join(lines)


# ----------------------------------------------------------------------------- standalone output
def write_dataset(out: Path, size: str = "standard", seed: int = spec.SEED, fmt: str = "delta") -> dict[str, int]:
    """Write ``Tables/<name>`` (Delta or Parquet) and ``Files/raw/...`` below ``out``; returns row counts."""
    tables = generate(size, seed)
    out.mkdir(parents=True, exist_ok=True)
    for name, t in tables.items():
        target = out / "Tables" / name
        if fmt == "delta":
            from deltalake import write_deltalake

            write_deltalake(str(target), t, mode="overwrite", partition_by=spec.TABLES[name]["partition_by"] or None, description=spec.TABLES[name]["description"])
        else:
            import pyarrow.parquet as pq

            target.mkdir(parents=True, exist_ok=True)
            pq.write_table(t, target / "part-0.parquet")
    for rel, data in raw_csvs(tables).items():
        p = out / rel
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_bytes(data)
    (out / "Files").mkdir(exist_ok=True)
    (out / "Files" / "README.md").write_text(readme_text(tables, size), encoding="utf-8")
    return {k: v.num_rows for k, v in tables.items()}


def write_schema_files(directory: Path) -> list[Path]:
    from olcf.sample import notebooks, powerbi, reports

    directory.mkdir(parents=True, exist_ok=True)
    files = {"sample-retail.schema.json": spec.schema_document(), "sample-retail-report.json": reports.report_definition("<lakehouse-id>"),
             "sample-retail-queries.json": reports.saved_queries(), "sample-retail-notebooks.json": notebooks.manifest(),
             "sample-retail-powerbi-scan.json": powerbi.scan_document()}
    out = []
    for name, doc in files.items():
        p = directory / name
        p.write_text(json.dumps(doc, indent=2, ensure_ascii=False, sort_keys=False) + "\n", encoding="utf-8")
        out.append(p)
    return out


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(prog="python -m olcf.sample.generate", description="Generate the OLCF sample retail dataset.")
    p.add_argument("--out", type=Path, help="directory to write Tables/ and Files/ into")
    p.add_argument("--size", choices=sorted(spec.SIZES), default="standard")
    p.add_argument("--seed", type=int, default=spec.SEED)
    p.add_argument("--format", choices=["delta", "parquet"], default="delta")
    p.add_argument("--write-schemas", nargs="?", const="", metavar="DIR", help="(re)generate the schemas/sample-*.json files (default: the repository's schemas/ directory)")
    a = p.parse_args(argv)
    if a.write_schemas is None and a.out is None:
        p.error("give --out DIR and/or --write-schemas")
    if a.write_schemas is not None:
        target = Path(a.write_schemas) if a.write_schemas else Path(__file__).resolve().parents[3] / "schemas"
        for f in write_schema_files(target):
            sys.stdout.write(f"wrote {f}\n")
    if a.out is not None:
        counts = write_dataset(a.out, a.size, a.seed, a.format)
        for k, v in counts.items():
            sys.stdout.write(f"{k:<24}{v:>10,} rows\n")
    return 0


if __name__ == "__main__":
    sys.exit(main())
