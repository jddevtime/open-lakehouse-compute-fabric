# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""The registry merge hook: when two assets are merged, governance rows follow the survivor.

The Intelligence registry calls ``hook(conn, from_id, to_id)`` inside the merge transaction (so everything here uses the given connection, never a new one). Every governance table
that stores an asset id is re-pointed from the merged-away asset (``from_id``) to the survivor (``to_id``). Where both assets already have a row under a unique key the survivor keeps
the stronger of the two (an approved link beats a pending one, an approved classification beats a suggestion ...) and the other is dropped; violations of the merged-away asset
are deleted because their identity derives from the asset id and the next evaluation re-creates what still holds.
"""

from __future__ import annotations

from typing import Any

from sqlalchemy import delete, inspect, select, update
from sqlalchemy.engine import Connection

from olcf.intelligence.governance.schema import (
    gov_asset_domains,
    gov_certifications,
    gov_changes,
    gov_class_assignments,
    gov_owners,
    gov_product_members,
    gov_term_links,
    gov_violations,
)

LINK_RANK = {"approved": 3, "pending": 2, "unlinked": 1, "rejected": 0}
ASSIGNMENT_RANK = {"approved": 3, "suggested": 2, "rejected": 1}


def merge_assets(conn: Connection, from_id: str, to_id: str) -> dict[str, int]:
    """Re-point governance rows from ``from_id`` to ``to_id``; returns how many rows moved per table (for diagnostics and tests)."""
    moved: dict[str, int] = {}
    insp = inspect(conn)
    if not all(insp.has_table(tb.name) for tb in (gov_term_links, gov_asset_domains, gov_product_members, gov_owners, gov_class_assignments, gov_certifications, gov_violations, gov_changes)):
        return moved     # governance storage is not installed: the merge must not depend on it

    def done(name: str, n: int = 1) -> None:
        moved[name] = moved.get(name, 0) + n

    # term links (unique: term, subject)
    t = gov_term_links.c
    for r in conn.execute(select(gov_term_links).where(t.subject_type == "asset", t.subject_id == from_id)).mappings().all():
        clash = conn.execute(select(gov_term_links).where(t.term_id == r["term_id"], t.subject_type == "asset", t.subject_id == to_id)).mappings().first()
        if clash is not None:
            if LINK_RANK.get(r["status"], 0) > LINK_RANK.get(clash["status"], 0):
                conn.execute(delete(gov_term_links).where(t.id == clash["id"]))
            else:
                conn.execute(delete(gov_term_links).where(t.id == r["id"]))
                continue
        conn.execute(update(gov_term_links).where(t.id == r["id"]).values(subject_id=to_id))
        done("gov_term_links")
    # domain (primary key: asset)
    d = gov_asset_domains.c
    if conn.execute(select(d.asset_id).where(d.asset_id == to_id)).first() is not None:
        conn.execute(delete(gov_asset_domains).where(d.asset_id == from_id))
    elif conn.execute(update(gov_asset_domains).where(d.asset_id == from_id).values(asset_id=to_id)).rowcount:
        done("gov_asset_domains")
    # product membership (primary key: product, asset)
    m = gov_product_members.c
    for (pid,) in conn.execute(select(m.product_id).where(m.asset_id == from_id)).all():
        if conn.execute(select(m.product_id).where(m.product_id == pid, m.asset_id == to_id)).first() is not None:
            conn.execute(delete(gov_product_members).where(m.product_id == pid, m.asset_id == from_id))
        else:
            conn.execute(update(gov_product_members).where(m.product_id == pid, m.asset_id == from_id).values(asset_id=to_id))
            done("gov_product_members")
    # owners (unique: subject, role, owner key)
    o = gov_owners.c
    for r in conn.execute(select(gov_owners).where(o.subject_type == "asset", o.subject_id == from_id)).mappings().all():
        if conn.execute(select(o.id).where(o.subject_type == "asset", o.subject_id == to_id, o.role == r["role"], o.owner_key == r["owner_key"])).first() is not None:
            conn.execute(delete(gov_owners).where(o.id == r["id"]))
        else:
            conn.execute(update(gov_owners).where(o.id == r["id"]).values(subject_id=to_id))
            done("gov_owners")
    # classification assignments (unique: asset, class)
    a = gov_class_assignments.c
    for r in conn.execute(select(gov_class_assignments).where(a.asset_id == from_id)).mappings().all():
        clash = conn.execute(select(gov_class_assignments).where(a.asset_id == to_id, a.class_id == r["class_id"])).mappings().first()
        if clash is not None:
            if ASSIGNMENT_RANK.get(r["status"], 0) > ASSIGNMENT_RANK.get(clash["status"], 0):
                conn.execute(delete(gov_class_assignments).where(a.id == clash["id"]))
            else:
                conn.execute(delete(gov_class_assignments).where(a.id == r["id"]))
                continue
        conn.execute(update(gov_class_assignments).where(a.id == r["id"]).values(asset_id=to_id))
        done("gov_class_assignments")
    # certification (primary key: subject): the survivor's own certification wins
    c = gov_certifications.c
    if conn.execute(select(c.subject_id).where(c.subject_type == "asset", c.subject_id == to_id)).first() is not None:
        conn.execute(delete(gov_certifications).where(c.subject_type == "asset", c.subject_id == from_id))
    elif conn.execute(update(gov_certifications).where(c.subject_type == "asset", c.subject_id == from_id).values(subject_id=to_id)).rowcount:
        done("gov_certifications")
    # violations derive their identity from the asset id: drop, the next evaluation re-creates what still holds
    v = gov_violations.c
    n = conn.execute(delete(gov_violations).where(v.subject_type == "asset", v.subject_id == from_id)).rowcount
    if n:
        done("gov_violations_dropped", int(n))
    # change requests keep their history under the survivor
    ch = gov_changes.c
    n = conn.execute(update(gov_changes).where(ch.subject_type == "asset", ch.subject_id == from_id).values(subject_id=to_id)).rowcount
    if n:
        done("gov_changes", int(n))
    return moved


def make_hook(_: Any = None) -> Any:
    """The callable registered in ``registry.merge_hooks`` (``hook(conn, from_id, to_id)``)."""

    def hook(conn: Connection, from_id: str, to_id: str) -> None:
        merge_assets(conn, from_id, to_id)

    return hook
