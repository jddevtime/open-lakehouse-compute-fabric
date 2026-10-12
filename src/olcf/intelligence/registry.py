# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""The asset registry: stable identity for everything the graph talks about.

Every object gets one ``ast_<ULID>``; every other identifier (OLCF item id, catalog entry id, Fabric GUID, OneLake path, qualified name ...) is an *alias*
that points at it (``asset_aliases``, primary key ``(kind, value)``). The registry answers three questions:

* :meth:`AssetRegistry.resolve` - which asset does this reference mean? (contract section 3: alias hit, name qualified with the default lakehouse, unique
  name inside the workspace, otherwise a **placeholder** keyed by its qualified name; a column reference resolves its table first and then the child column,
  case-insensitively)
* :meth:`AssetRegistry.ensure` - a connector found a real object: find or create its asset, learn its aliases, and merge any placeholder it touches
* :meth:`AssetRegistry.merge` - fold one asset into another (aliases move, edges are rewired and collapsed with their evidence unioned, children merge by name)

Concurrency: all writes run inside ``Database.begin_serialized("intelligence.graph")`` (a cluster wide mutual exclusion lock: ``BEGIN IMMEDIATE`` on SQLite,
an advisory lock on PostgreSQL), and the alias primary key is the backstop - an ``IntegrityError`` makes the whole unit retry, find the winner's row and
return it. Two threads resolving the same alias therefore always get the same asset.

Qualified names are stored per *scope* (``<workspace id>|silver.sales`` or ``fab:<tenant>/<guid>|silver.sales``) because two workspaces can both have a
lakehouse called ``Silver``; connectors should not supply ``qname`` aliases themselves (a bare one is scoped by the registry).
"""

from __future__ import annotations

import hashlib
import logging
import threading
from collections.abc import Callable, Iterable, Iterator, Sequence
from contextlib import contextmanager
from dataclasses import dataclass, field
from typing import Any

from sqlalchemy import Connection, delete, func, insert, select, tuple_, update
from sqlalchemy.exc import IntegrityError

from olcf.core.errors import BadRequest, NotFound
from olcf.core.ids import new_id
from olcf.core.timeutil import now_iso
from olcf.db.engine import Database
from olcf.db.util import fetch_all, fetch_one, new_etag
from olcf.intelligence import model as M
from olcf.intelligence.graph import merge_evidence, strongest_confidence
from olcf.intelligence.schema import analysis_cache, asset_aliases, assets, edges, gaps

log = logging.getLogger("olcf.intelligence.registry")

#: The serialisation lock every graph/registry writer takes.
GRAPH_LOCK = "intelligence.graph"
#: Alias values longer than this are stored as ``h:<sha256>`` (btree key limits).
ALIAS_MAX = 700
#: Alias kinds whose values are compared case-insensitively.
CASE_INSENSITIVE_KINDS = frozenset({"lakehouse_table", "lakehouse_column", "fabric", "fabric_column", "qname", "pbi"})
#: Types that sit below a lakehouse / warehouse and are addressed by a qualified name in code.
QNAME_TYPES = frozenset({"table", "view"})
PATH_TYPES = frozenset({"file"})            # named by a lakehouse-relative path (``Files/raw/sales``): qualified by the lakehouse like tables, but the whole path is the name
CONTAINER_TYPES = frozenset({"lakehouse", "warehouse"})
_RETRIES = 4

#: Alias kinds that name a real object (as opposed to ``qname``, a name that can be shared or reused): they are *authoritative*. Two assets that carry
#: different values of the same authoritative kind are different objects, whatever their names say.
STRICT_KINDS = frozenset(M.ALIAS_KINDS) - {"qname"}
#: The kinds an asset normally holds one value of (a table has one Fabric identity); more than one is reported by :meth:`AssetRegistry.identity_conflicts`.
SINGLE_VALUED_KINDS = frozenset({"olcf_item", "olcf_workspace", "catalog_entry", "lakehouse_table", "lakehouse_column", "fabric", "fabric_column", "pbi"})

EventHook = Callable[[str, str, dict[str, Any]], None]
#: ``audit(conn, action, asset_id, details)``: records an audit-worthy change *inside* the transaction that makes it (see :class:`AssetRegistry`).
AuditHook = Callable[[Connection, str, str, dict[str, Any]], None]


def alias_value(kind: str, value: str) -> str:
    """The stored form of an alias value: stripped, lower-cased for case-insensitive kinds, hashed when it would not fit an index key."""
    v = value.strip()
    if kind in CASE_INSENSITIVE_KINDS:
        v = v.lower()
    if len(v) > ALIAS_MAX:
        v = "h:" + hashlib.sha256(v.encode("utf-8")).hexdigest()
    return v


def _identity(kind: str, value: str) -> tuple[str, str | None]:
    """``(identity, tenant)`` of an authoritative alias: the Fabric kinds start with the tenant (``*`` = unknown), which is a qualifier, not part of the object."""
    if kind in ("fabric", "fabric_column"):
        tenant, _, rest = value.partition("/")
        return rest, (None if tenant in ("", "*") else tenant)
    return value, None


def same_identity(kind: str, a: str, b: str) -> bool:
    """Do two values of an authoritative alias kind name the same object? (An unknown Fabric tenant matches any tenant; two known tenants must agree.)"""
    ia, ta = _identity(kind, a)
    ib, tb = _identity(kind, b)
    return ia == ib and (ta is None or tb is None or ta == tb)


def identities_conflict(left: Iterable[tuple[str, str]], right: Iterable[tuple[str, str]]) -> bool:
    """True when the two alias sets prove two different objects: they carry the same authoritative kind but not one common value of it. A kind only one side
    has says nothing (a placeholder, or a source that knows the object by another identifier)."""
    lk: dict[str, list[str]] = {}
    rk: dict[str, list[str]] = {}
    for k, v in left:
        if k in STRICT_KINDS:
            lk.setdefault(k, []).append(v)
    for k, v in right:
        if k in STRICT_KINDS:
            rk.setdefault(k, []).append(v)
    for k, lv in lk.items():
        rv = rk.get(k)
        if rv and not any(same_identity(k, a, b) for a in lv for b in rv):
            return True
    return False


def scope_keys(scope: dict[str, str], workspace_id: str | None = None) -> list[str]:
    """The scope prefixes a reference can be qualified with: its OLCF workspace, then its Fabric workspace, else ``*`` (unscoped)."""
    keys: list[str] = []
    ws = scope.get("workspace_id") or workspace_id
    if ws:
        keys.append(ws)
    fab = scope.get("fabric_workspace")
    if fab:
        keys.append(f"fab:{(scope.get('tenant') or '*').lower()}/{fab.lower()}")
    return keys or ["*"]


def qname_alias(key: str, qname: str) -> str:
    """The stored ``qname`` alias value for a normalised qualified name within a scope key."""
    return f"{key}|{qname}"


@dataclass
class Resolution:
    """The outcome of resolving a reference: the asset id, whether it was created by this call, whether it is a placeholder and how it was found
    (``alias``, ``qname``, ``lakehouse``, ``name``, ``column``, ``placeholder``)."""

    asset_id: str
    created: bool = False
    placeholder: bool = False
    via: str = "alias"


@dataclass
class EnsureResult:
    """The outcome of :meth:`AssetRegistry.ensure`: the asset id, whether it is new, whether a placeholder was promoted, the assets merged into it and
    the alias collisions that could not be resolved (``[{"alias": [kind, value], "asset_id": ..., "reason": ...}]``)."""

    asset_id: str
    created: bool = False
    promoted: bool = False
    merged: list[str] = field(default_factory=list)
    conflicts: list[dict[str, Any]] = field(default_factory=list)


class AssetRegistry:
    """Resolve, ensure and merge assets by alias.

    Audit-worthy changes (``intelligence.asset_merged``) are reported twice, deliberately in different places:

    * ``audit(conn, action, asset_id, details)`` runs *inside* the write transaction, on its connection, so the record commits or rolls back with the change and no
      second writer is opened while the graph lock is held (that deadlocked SQLite until the busy timeout and lost the record);
    * ``on_event(action, asset_id, details)`` is a notification, queued during the transaction and delivered **after it committed**, never inside a critical
      section; a rolled-back unit delivers nothing. The registry delivers it itself when it owns the transaction; when the caller passes its own ``conn`` the
      caller calls :meth:`flush_events` after its commit (or :meth:`discard_events` after a rollback), as :class:`~olcf.intelligence.engine.Engine` does.

    ``merge_hooks`` are called as ``hook(conn, from_id, to_id)`` inside the merge transaction so other tables that store asset ids (governance) can follow.
    """

    def __init__(self, db: Database, *, on_event: EventHook | None = None, audit: AuditHook | None = None) -> None:
        self.db = db
        self.on_event = on_event
        self.audit = audit
        self.merge_hooks: list[Callable[[Connection, str, str], None]] = []
        self._tl = threading.local()

    # ------------------------------------------------------------------------------------------------------------------------ notification outbox
    def _outbox(self) -> list[tuple[str, str, dict[str, Any]]]:
        box = getattr(self._tl, "box", None)
        if box is None:
            box = self._tl.box = []
        return box

    def flush_events(self) -> int:
        """Deliver the notifications queued by this thread's committed transactions; returns how many. Call it after the commit, never inside a transaction."""
        box = self._outbox()
        pending, box[:] = list(box), []
        for action, asset_id, details in pending:
            if self.on_event is None:
                continue
            try:
                self.on_event(action, asset_id, details)
            except Exception:  # noqa: BLE001 - a notification problem never blocks identity
                log.exception("asset notification failed")
        return len(pending)

    def discard_events(self, mark: int = 0) -> None:
        """Drop the queued notifications of a transaction that was rolled back (those queued after ``mark``)."""
        del self._outbox()[mark:]

    # ------------------------------------------------------------------------------------------------------------------------ transactions
    @contextmanager
    def _writing(self, conn: Connection | None) -> Iterator[Connection]:
        if conn is not None:
            yield conn
            return
        mark = len(self._outbox())
        try:
            with self.db.begin_serialized(GRAPH_LOCK) as c:
                yield c
        except BaseException:
            self.discard_events(mark)
            raise
        self.flush_events()

    def _retry(self, fn: Callable[[], Any]) -> Any:
        last: Exception | None = None
        for _ in range(_RETRIES):
            try:
                return fn()
            except IntegrityError as exc:  # another writer won the alias race: the next attempt finds its row
                last = exc
        assert last is not None
        raise last

    # ------------------------------------------------------------------------------------------------------------------------ alias lookups
    def lookup(self, aliases: Iterable[tuple[str, str]], conn: Connection | None = None) -> dict[tuple[str, str], str]:
        """``{(kind, stored value): asset id}`` for the aliases that exist."""
        pairs = list(dict.fromkeys((k, alias_value(k, v)) for k, v in aliases))
        if not pairs:
            return {}
        t = asset_aliases.c
        out: dict[tuple[str, str], str] = {}
        for i in range(0, len(pairs), 200):
            stmt = select(t.kind, t.value, t.asset_id).where(tuple_(t.kind, t.value).in_(pairs[i:i + 200]))
            if conn is not None:
                rows = conn.execute(stmt).all()
            else:
                with self.db.connect() as c:
                    rows = c.execute(stmt).all()
            out.update({(r[0], r[1]): r[2] for r in rows})
        return out

    def find(self, kind: str, value: str) -> str | None:
        """The asset id an alias points at, or ``None``."""
        return self.lookup([(kind, value)]).get((kind, alias_value(kind, value)))

    def aliases_of(self, asset_id: str, conn: Connection | None = None) -> list[tuple[str, str]]:
        """All ``(kind, value)`` aliases of an asset (stored form)."""
        stmt = select(asset_aliases.c.kind, asset_aliases.c.value).where(asset_aliases.c.asset_id == asset_id).order_by(asset_aliases.c.kind, asset_aliases.c.value)
        if conn is not None:
            return [(r[0], r[1]) for r in conn.execute(stmt)]
        with self.db.connect() as c:
            return [(r[0], r[1]) for r in c.execute(stmt)]

    def get(self, asset_id: str, conn: Connection | None = None) -> dict[str, Any] | None:
        """The ``assets`` row, or ``None``."""
        stmt = select(assets).where(assets.c.id == asset_id)
        if conn is not None:
            return fetch_one(conn, stmt)
        with self.db.connect() as c:
            return fetch_one(c, stmt)

    # ------------------------------------------------------------------------------------------------------------------------ resolution
    def resolve(self, ref: M.AssetRef, *, create: bool = True, conn: Connection | None = None, scan_id: str | None = None, source_id: str | None = None) -> str:
        """The ``ast_`` id a reference means (contract section 3). With ``create`` a placeholder is made when nothing matches; without it ``NotFound`` is raised."""
        return self.resolve_ex(ref, create=create, conn=conn, scan_id=scan_id, source_id=source_id).asset_id

    def resolve_ex(self, ref: M.AssetRef, *, create: bool = True, conn: Connection | None = None, scan_id: str | None = None,
                   source_id: str | None = None) -> Resolution:
        """Like :meth:`resolve` but says how the asset was found and whether it is a placeholder."""
        if ref.type not in M.ASSET_TYPES:
            raise BadRequest(f"unknown asset type '{ref.type}'", details={"supported": list(M.ASSET_TYPES)})
        bad = [k for k, _ in ref.aliases if k not in M.ALIAS_KINDS]
        if bad:
            raise BadRequest(f"unknown alias kind(s) {sorted(set(bad))}", details={"supported": list(M.ALIAS_KINDS)})
        if conn is None:
            hit, pending = self._lookup_ref(None, ref)
            if hit is not None and (not pending or not create):
                return hit
            if not create:
                raise NotFound(f"no asset matches {self._describe(ref)}")
            return self._retry(lambda: self._resolve_unit(None, ref, scan_id, source_id))
        if not create:
            found, _ = self._lookup_ref(conn, ref)
            if found is None:
                raise NotFound(f"no asset matches {self._describe(ref)}")
            return found
        return self._resolve_write(conn, ref, scan_id, source_id)

    def _resolve_unit(self, conn: Connection | None, ref: M.AssetRef, scan_id: str | None, source_id: str | None) -> Resolution:
        with self._writing(conn) as c:
            return self._resolve_write(c, ref, scan_id, source_id)

    @staticmethod
    def _describe(ref: M.AssetRef) -> str:
        bits = [f"{k}={v}" for k, v in ref.aliases][:4]
        if ref.name:
            bits.append(f"name={ref.name}")
        return f"{ref.type} ({', '.join(bits)})"

    def _ref_aliases(self, ref: M.AssetRef) -> list[tuple[str, str]]:
        """Alias pairs of a reference; a bare ``qname`` gets scoped by the reference's scope."""
        sc = ref.scope_map
        out: list[tuple[str, str]] = []
        for k, v in ref.aliases:
            if k == "qname" and "|" not in v:
                qn = M.normalize_qname(v)
                out.extend((k, qname_alias(sk, qn)) for sk in scope_keys(sc))
            else:
                out.append((k, v))
        return out

    def _lookup_ref(self, conn: Connection | None, ref: M.AssetRef) -> tuple[Resolution | None, list[tuple[str, str]]]:
        """Read-only part of resolution: ``(resolution or None, aliases of the reference that are not attached to that asset yet)``.

        Authoritative aliases decide first. A hit through a qualified name (an alias derived from names) or through the name rules is only a hint: it is refused when
        the asset it points at carries a different authoritative identity than the reference (two Fabric tables that are both called ``Silver.orders``)."""
        aliases = self._ref_aliases(ref)
        hits = self.lookup(aliases, conn)
        by_alias = [(k, alias_value(k, v)) for k, v in aliases]
        strict = [a for a in by_alias if a[0] in STRICT_KINDS]
        found = next((hits[a] for a in strict if a in hits), None)
        if found is None:
            found = next(iter(self._tenant_peers(conn, strict).values()), None)
        via = "alias"
        if found is None:
            found = next((hits[a] for a in by_alias if a in hits and a[0] not in STRICT_KINDS and not self._conflicts_with(conn, hits[a], strict)), None)
        if found is None:
            found, via = self._lookup_by_name(conn, ref)
            if found is not None and strict and self._conflicts_with(conn, found, strict):
                found = None
        if found is None:
            return None, []
        row = self.get(found, conn)
        res = Resolution(found, False, bool(row and row["status"] == "placeholder"), via)
        pending = [a for a in by_alias if a not in hits]
        return res, pending

    def _tenant_peers(self, conn: Connection | None, strict: Sequence[tuple[str, str]]) -> dict[tuple[str, str], str]:
        """Existing Fabric aliases that name the same object as one of ``strict`` but carry the tenant the other side lacks (``*`` = unknown): ``{(kind, stored value): asset id}``.

        ``tenant1/ws/item`` and ``*/ws/item`` are one item; the stored values differ, so the plain alias lookup cannot join them. Only an unambiguous match (one owner) counts."""
        out: dict[tuple[str, str], str] = {}
        t = asset_aliases.c
        for k, v in strict:
            if k not in ("fabric", "fabric_column"):
                continue
            rest, tenant = _identity(k, v)
            if tenant is not None:
                where = [t.value == f"*/{rest}"]
            else:
                esc = rest.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")
                where = [t.value.like(f"%/{esc}", escape="\\"), t.value != v]
            stmt = select(t.value, t.asset_id).where(t.kind == k, *where).limit(8)
            if conn is not None:
                rows = conn.execute(stmt).all()
            else:
                with self.db.connect() as c:
                    rows = c.execute(stmt).all()
            found = {(k, r[0]): r[1] for r in rows if same_identity(k, v, r[0])}
            if len(set(found.values())) == 1:
                out.update(found)
        return out

    def identity_conflicts(self, *, limit: int = 500) -> list[dict[str, Any]]:
        """Diagnose assets that were conflated by an older build: one asset that carries two *different* authoritative identities of a kind it should hold one value of
        (two Fabric tables, two Power BI items). Read-only. Each row is ``{asset_id, type, name, workspace_id, kinds: {alias kind: [values]}}``; the values show which
        objects were joined, so an operator can split them by re-scanning the sources after removing the wrong alias (``move_alias``) - ``['*/ws/item', 'tenant/ws/item']`` is
        one object with an unknown tenant and is not reported."""
        t = asset_aliases.c
        many = (select(t.asset_id).where(t.kind.in_(sorted(SINGLE_VALUED_KINDS))).group_by(t.asset_id, t.kind).having(func.count() > 1).subquery())
        stmt = (select(t.asset_id, t.kind, t.value).where(t.kind.in_(sorted(SINGLE_VALUED_KINDS)), t.asset_id.in_(select(many.c.asset_id))).order_by(t.asset_id, t.kind, t.value))
        with self.db.connect() as c:
            rows = c.execute(stmt).all()
            per: dict[str, dict[str, list[str]]] = {}
            for aid, kind, value in rows:
                per.setdefault(aid, {}).setdefault(kind, []).append(value)
            out: list[dict[str, Any]] = []
            for aid, kinds in per.items():
                bad = {k: vs for k, vs in kinds.items() if any(not same_identity(k, a, b) for i, a in enumerate(vs) for b in vs[i + 1:])}
                if not bad:
                    continue
                row = self.get(aid, c)
                if row is None or row["status"] == "removed":
                    continue
                out.append({"asset_id": aid, "type": row["type"], "name": row["name"], "workspace_id": row["workspace_id"], "kinds": bad})
                if len(out) >= limit:
                    break
        return out

    def _strict_aliases_of(self, asset_id: str, conn: Connection | None) -> list[tuple[str, str]]:
        return [(k, v) for k, v in self.aliases_of(asset_id, conn) if k in STRICT_KINDS]

    def _conflicts_with(self, conn: Connection | None, asset_id: str, strict: Sequence[tuple[str, str]]) -> bool:
        """Does the asset carry an authoritative identity that is different from ``strict`` (see :func:`identities_conflict`)?"""
        return bool(strict) and identities_conflict(strict, self._strict_aliases_of(asset_id, conn))

    def _lookup_by_name(self, conn: Connection | None, ref: M.AssetRef) -> tuple[str | None, str]:
        """Steps (2) and (3) of the resolution rules plus the column rule. ``(asset id or None, how)``."""
        if ref.type == "column":
            table = self._table_for_column(conn, ref, create=False, scan_id=None, source_id=None)
            if table is None:
                return None, "column"
            child = self._child(conn, table.asset_id, ref.name or "", "column")
            return (child, "column") if child else (None, "column")
        if not ref.name:
            return None, "alias"
        sc = ref.scope_map
        qn = M.normalize_qname(ref.name)
        if not qn:
            return None, "alias"
        parts = qn.split(".")
        keys = scope_keys(sc)
        lh_prefixes = self._lakehouse_prefixes(conn, sc)
        if ref.type in PATH_TYPES and lh_prefixes:
            # a lakehouse-relative path (dots are part of file names): the file below the scope's default lakehouse
            for _, lh_id in lh_prefixes:
                child = self._child(conn, lh_id, qn, None, types=PATH_TYPES) if lh_id else None
                if child:
                    return child, "lakehouse"
            cands = [qname_alias(k, f"{p}.{qn}") for k in keys for p, _ in lh_prefixes]
        elif len(parts) < 2 and lh_prefixes:
            # (2) the bare name qualified with the scope's default lakehouse
            lh_id = lh_prefixes[0][1]
            if lh_id:
                child = self._child(conn, lh_id, qn, None, types=QNAME_TYPES)
                if child:
                    return child, "lakehouse"
            cands = [qname_alias(k, f"{p}.{qn}") for k in keys for p, _ in lh_prefixes]
        else:
            cands = [qname_alias(k, qn) for k in keys]
        hits = self.lookup([("qname", c) for c in cands], conn)
        for c in cands:
            if ("qname", alias_value("qname", c)) in hits:
                return hits[("qname", alias_value("qname", c))], "qname"
        # (3) a unique name match inside the scope's workspace
        ws = sc.get("workspace_id")
        if ws and not (ref.type in PATH_TYPES and lh_prefixes):         # a relative path names a file of ITS lakehouse, never "the only file of that name in the workspace"
            t = assets.c
            stmt = select(t.id).where(t.workspace_id == ws, t.type == ref.type, t.status != "removed", func.lower(t.name).in_([qn, parts[-1]] if len(parts) == 1 else [qn]))
            rows = (conn.execute(stmt).all() if conn is not None else self._read(stmt))
            ids = list(dict.fromkeys(r[0] for r in rows))
            if len(ids) == 1:
                return ids[0], "name"
        return None, "alias"

    def _read(self, stmt: Any) -> list[Any]:
        with self.db.connect() as c:
            return list(c.execute(stmt).all())

    def _lakehouse_prefixes(self, conn: Connection | None, sc: dict[str, str]) -> list[tuple[str, str | None]]:
        """``[(qname prefix, lakehouse asset id or None)]`` for the scope's default lakehouse: its name and, when only the id is known, the lower-cased id."""
        out: list[tuple[str, str | None]] = []
        lh_id = sc.get("lakehouse_id")
        asset_id: str | None = None
        if lh_id:
            asset_id = self.lookup([("olcf_item", lh_id)], conn).get(("olcf_item", lh_id))
            if asset_id:
                row = self.get(asset_id, conn)
                if row:
                    out.append((M.normalize_qname(row["name"]), asset_id))
            out.append((lh_id.lower(), asset_id))
        name = sc.get("lakehouse")
        if name:
            norm = M.normalize_qname(name)
            if not any(p == norm for p, _ in out):
                out.append((norm, self._lakehouse_by_name(conn, norm, sc.get("workspace_id"))))
        return out

    def _lakehouse_by_name(self, conn: Connection | None, norm: str, workspace_id: str | None) -> str | None:
        if not workspace_id:
            return None
        t = assets.c
        stmt = select(t.id).where(t.workspace_id == workspace_id, t.type.in_(sorted(CONTAINER_TYPES)), t.status != "removed", func.lower(t.name) == norm)
        rows = conn.execute(stmt).all() if conn is not None else self._read(stmt)
        return rows[0][0] if len(rows) == 1 else None

    def _child(self, conn: Connection | None, parent_id: str, name: str, type_: str | None, types: Iterable[str] | None = None) -> str | None:
        t = assets.c
        stmt = select(t.id, t.status).where(t.parent_id == parent_id, func.lower(t.name) == name.lower()).order_by(t.status == "removed", t.id)
        if type_:
            stmt = stmt.where(t.type == type_)
        elif types:
            stmt = stmt.where(t.type.in_(sorted(types)))
        rows = conn.execute(stmt).all() if conn is not None else self._read(stmt)
        live = [r for r in rows if r[1] != "removed"]
        return (live or rows)[0][0] if rows else None

    def _table_for_column(self, conn: Connection | None, ref: M.AssetRef, *, create: bool, scan_id: str | None, source_id: str | None) -> Resolution | None:
        """Resolve the table a column reference lives in (``scope["table"]``: an ``ast_`` id, ``<alias kind>:<value>`` or a qualified name)."""
        sc = ref.scope_map
        spec = sc.get("table")
        if not spec:
            col = dict(ref.aliases).get("lakehouse_column")
            if col and col.count(":") >= 2:
                lh, rest = col.split(":", 1)
                tbl = rest.rsplit(":", 1)[0]
                tref = M.AssetRef.make("table", {"lakehouse_table": f"{lh}:{tbl}"}, tbl, {k: v for k, v in sc.items() if k != "table"})
            else:
                return None
        elif spec.startswith("ast_"):
            row = self.get(spec, conn)
            return Resolution(spec, False, bool(row and row["status"] == "placeholder"), "alias") if row else None
        else:
            kind, _, rest = spec.partition(":")
            rest_scope = {k: v for k, v in sc.items() if k not in ("table", "table_alias")}
            if kind in M.ALIAS_KINDS and rest:
                tref = M.AssetRef.make(sc.get("table_type", "table"), {kind: rest}, rest.rsplit(":", 1)[-1] if kind != "qname" else rest, rest_scope)
            else:
                # a table known by its physical location (``table_alias`` = ``<kind>=<value>``, see lineage.base.column_ref) is found by that location first
                akind, _, avalue = sc.get("table_alias", "").partition("=")
                tref = M.AssetRef.make(sc.get("table_type", "table"), {akind: avalue} if akind in M.ALIAS_KINDS and avalue else None, spec, rest_scope)
        if not create:
            res, _ = self._lookup_ref(conn, tref)
            return res
        return self._resolve_write(conn, tref, scan_id, source_id)  # type: ignore[arg-type]

    # ---- write path of resolution
    def _resolve_write(self, conn: Connection, ref: M.AssetRef, scan_id: str | None, source_id: str | None) -> Resolution:
        res, pending = self._lookup_ref(conn, ref)
        if res is not None:
            if pending:
                row = self.get(res.asset_id, conn) or {}
                if row and any(k == "qname" for k, _ in pending):
                    pending = self._admit_qnames(conn, res.asset_id, row["type"], row["name"], row["workspace_id"], row["parent_id"], pending, self._strict_aliases_of(res.asset_id, conn))
                self._attach(conn, res.asset_id, pending)
            return res
        if ref.type == "column":
            return self._create_column_placeholder(conn, ref, scan_id, source_id)
        return self._create_placeholder(conn, ref, scan_id, source_id)

    def _placeholder_qnames(self, conn: Connection, ref: M.AssetRef) -> list[tuple[str, str]]:
        """The scoped qualified-name aliases a placeholder is keyed by."""
        if not ref.name:
            return []
        sc = ref.scope_map
        qn = M.normalize_qname(ref.name)
        if not qn:
            return []
        keys = scope_keys(sc)
        if ref.type in PATH_TYPES or ("." not in qn and ref.type in QNAME_TYPES):
            prefixes = self._lakehouse_prefixes(conn, sc)
            if prefixes:
                return [("qname", qname_alias(k, f"{p}.{qn}")) for k in keys for p, _ in prefixes]
        return [("qname", qname_alias(k, qn)) for k in keys]

    def _create_placeholder(self, conn: Connection, ref: M.AssetRef, scan_id: str | None, source_id: str | None) -> Resolution:
        if not ref.name and not ref.aliases:
            raise BadRequest("a reference needs aliases or a name")
        name = (ref.name or ref.aliases[0][1]).strip()
        sc = ref.scope_map
        parent_id: str | None = None
        if ref.type in QNAME_TYPES:
            pref = self._lakehouse_prefixes(conn, sc)
            if pref and pref[0][1]:
                parent_id = pref[0][1]
        aliases = self._ref_aliases(ref) + self._placeholder_qnames(conn, ref)
        aid = self._insert_asset(conn, type_=ref.type, name=name, workspace_id=sc.get("workspace_id"), source_id=source_id, scan_id=scan_id, parent_id=parent_id,
                                 status="placeholder", attrs={"unresolved": True, "referenced_as": ref.name or name})
        self._attach(conn, aid, aliases)
        return Resolution(aid, True, True, "placeholder")

    def _create_column_placeholder(self, conn: Connection, ref: M.AssetRef, scan_id: str | None, source_id: str | None) -> Resolution:
        if not ref.name:
            raise BadRequest("a column reference needs the column name", remediation="set AssetRef.name to the column and scope['table'] to its table")
        table = self._table_for_column(conn, ref, create=True, scan_id=scan_id, source_id=source_id)
        if table is None:
            raise BadRequest("a column reference needs scope['table'] (a qualified table name, '<alias kind>:<value>' or an ast_ id) or a lakehouse_column alias")
        trow = self.get(table.asset_id, conn) or {}
        child = self._child(conn, table.asset_id, ref.name, "column")
        if child:
            self._attach(conn, child, self._ref_aliases(ref))
            crow = self.get(child, conn) or {}
            return Resolution(child, False, crow.get("status") == "placeholder", "column")
        aid = self._insert_asset(conn, type_="column", name=ref.name.strip(), workspace_id=trow.get("workspace_id") or ref.scope_map.get("workspace_id"), source_id=source_id,
                                 scan_id=scan_id, parent_id=table.asset_id, status="placeholder", attrs={"unresolved": True})
        self._attach(conn, aid, self._ref_aliases(ref))
        return Resolution(aid, True, True, "placeholder")

    # ------------------------------------------------------------------------------------------------------------------------ rows and aliases
    def _insert_asset(self, conn: Connection, *, type_: str, name: str, workspace_id: str | None, source_id: str | None, scan_id: str | None, parent_id: str | None,
                      status: str, attrs: dict[str, Any] | None = None, path: str | None = None, description: str = "") -> str:
        aid = new_id("ast")
        now = now_iso()
        conn.execute(insert(assets).values(
            id=aid, type=type_, name=name[:512], path=path, workspace_id=workspace_id, source_id=source_id, parent_id=parent_id, attrs=attrs or {}, description=description,
            description_source="scan", tags=[], status=status, first_seen_scan=scan_id, last_seen_scan=scan_id, removed_scan=None, etag=new_etag(), created_at=now, updated_at=now))
        return aid

    def _attach(self, conn: Connection, asset_id: str, aliases: Iterable[tuple[str, str]]) -> list[dict[str, Any]]:
        """Attach aliases to an asset; those that belong to another asset are returned as conflicts and left alone."""
        pairs = list(dict.fromkeys((k, alias_value(k, v)) for k, v in aliases))
        if not pairs:
            return []
        owned = self.lookup(pairs, conn)
        conflicts: list[dict[str, Any]] = []
        now = now_iso()
        for k, v in pairs:
            owner = owned.get((k, v))
            if owner is None:
                conn.execute(insert(asset_aliases).values(kind=k, value=v, asset_id=asset_id, created_at=now))
            elif owner != asset_id:
                conflicts.append({"alias": [k, v], "asset_id": owner, "reason": "alias belongs to another asset"})
        return conflicts

    def add_alias(self, asset_id: str, kind: str, value: str, *, conn: Connection | None = None) -> bool:
        """Attach one alias; ``True`` when the alias is on this asset afterwards, ``False`` when it belongs to another asset (use :meth:`move_alias` to take it over)."""
        if kind not in M.ALIAS_KINDS:
            raise BadRequest(f"unknown alias kind '{kind}'", details={"supported": list(M.ALIAS_KINDS)})

        def run() -> bool:
            with self._writing(conn) as c:
                if self.get(asset_id, c) is None:
                    raise NotFound(f"asset '{asset_id}' does not exist")
                return not self._attach(c, asset_id, [(kind, value)])

        return self._retry(run) if conn is None else run()

    def move_alias(self, kind: str, value: str, to_asset_id: str, *, conn: Connection | None = None) -> str | None:
        """Re-point an alias at another asset (an item that was deleted and recreated, a renamed table). Returns the previous owner, if any."""
        v = alias_value(kind, value)

        def run() -> str | None:
            with self._writing(conn) as c:
                if self.get(to_asset_id, c) is None:
                    raise NotFound(f"asset '{to_asset_id}' does not exist")
                prev = self.lookup([(kind, v)], c).get((kind, v))
                if prev == to_asset_id:
                    return prev
                if prev is None:
                    c.execute(insert(asset_aliases).values(kind=kind, value=v, asset_id=to_asset_id, created_at=now_iso()))
                else:
                    c.execute(update(asset_aliases).where(asset_aliases.c.kind == kind, asset_aliases.c.value == v).values(asset_id=to_asset_id))
                return prev

        return self._retry(run) if conn is None else run()

    # ------------------------------------------------------------------------------------------------------------------------ ensure
    def _derived_qnames(self, conn: Connection, type_: str, name: str, parent_id: str | None, keys: Sequence[str]) -> list[tuple[str, str]]:
        """The scoped qualified names a real table/view is known by in code: ``<lakehouse>.<table>`` (name and, when it has one, item id form)."""
        if (type_ not in QNAME_TYPES and type_ not in PATH_TYPES) or not parent_id:
            return []
        trail: list[str] = []
        lh_aliases: list[str] = []
        cur: str | None = parent_id
        for _ in range(4):
            row = self.get(cur, conn) if cur else None
            if row is None:
                return []
            if row["type"] in CONTAINER_TYPES:
                for k, v in self.aliases_of(row["id"], conn):
                    if k == "olcf_item":
                        lh_aliases.append(v.lower())
                prefix = M.normalize_qname(row["name"])
                body = ".".join([*reversed(trail), M.normalize_qname(name)])
                out = [qname_alias(k, f"{prefix}.{body}") for k in keys]
                out += [qname_alias(k, f"{a}.{body}") for k in keys for a in lh_aliases]
                return [("qname", q) for q in out]
            if row["type"] == "schema":
                trail.append(M.normalize_qname(row["name"]))
                cur = row["parent_id"]
                continue
            return []
        return []

    def ensure(self, ref: M.AssetRef, *, name: str, workspace_id: str | None = None, source_id: str | None = None, scan_id: str | None = None,
               parent_id: str | None = None, path: str | None = None, conn: Connection | None = None) -> EnsureResult:
        """Find or create the real asset a connector reports and make its aliases point at it.

        Every alias of ``ref`` (plus the qualified names derived from its place in the lakehouse tree) is looked up. If nothing matches a new ``active`` asset is
        created. If a **placeholder** is touched it is promoted and, when several assets match, merged: placeholders into the real asset (same type, or both
        datasets); two real assets only when they have the same type (audited as ``intelligence.asset_merged``) - a collision across types is left alone and
        reported in ``conflicts``. A column with no alias hit is matched to a child of ``parent_id`` with the same name (case-insensitive).
        """
        if ref.type not in M.ASSET_TYPES:
            raise BadRequest(f"unknown asset type '{ref.type}'", details={"supported": list(M.ASSET_TYPES)})
        bad = [k for k, _ in ref.aliases if k not in M.ALIAS_KINDS]
        if bad:
            raise BadRequest(f"unknown alias kind(s) {sorted(set(bad))}", details={"supported": list(M.ALIAS_KINDS)})

        def run() -> EnsureResult:
            with self._writing(conn) as c:
                return self._ensure(c, ref, name, workspace_id, source_id, scan_id, parent_id, path)

        return self._retry(run) if conn is None else run()

    def _ensure(self, c: Connection, ref: M.AssetRef, name: str, workspace_id: str | None, source_id: str | None, scan_id: str | None, parent_id: str | None,
                path: str | None) -> EnsureResult:
        sc = ref.scope_map
        keys = scope_keys(sc, workspace_id)
        # a bare qname alias of an asset fact is scoped by its workspace too
        aliases = [(k, v) if not (k == "qname" and "|" not in v) else (k, qname_alias(keys[0], M.normalize_qname(v))) for k, v in ref.aliases]
        aliases += self._derived_qnames(c, ref.type, name, parent_id, keys)
        pairs = [(k, alias_value(k, v)) for k, v in aliases]
        strict = [p for p in pairs if p[0] in STRICT_KINDS]
        hits = self.lookup(pairs, c)
        # authoritative aliases identify; a qualified-name hit is a hint that must not join two objects whose authoritative identities differ
        ids = list(dict.fromkeys([hits[p] for p in strict if p in hits] + list(self._tenant_peers(c, strict).values())))
        refused: list[str] = []
        for p in pairs:
            if p[0] in STRICT_KINDS or p not in hits or hits[p] in ids or hits[p] in refused:
                continue
            if self._conflicts_with(c, hits[p], strict):
                refused.append(hits[p])
            else:
                ids.append(hits[p])
        if not ids and ref.type == "column" and parent_id:
            child = self._child(c, parent_id, name, "column")
            if child and not self._conflicts_with(c, child, strict):
                ids = [child]
        conflicts = [{"alias": None, "asset_id": other, "reason": "identity_conflict: another asset has the same qualified name but a different authoritative identity; both are kept apart",
                      "identity": True} for other in refused]
        if not ids:
            aid = self._insert_asset(c, type_=ref.type, name=name, workspace_id=workspace_id, source_id=source_id, scan_id=scan_id, parent_id=parent_id, status="active", path=path)
            conflicts += self._attach(c, aid, self._admit_qnames(c, aid, ref.type, name, workspace_id, parent_id, pairs, strict))
            return EnsureResult(aid, created=True, conflicts=conflicts)
        fetched = {i: self.get(i, c) for i in ids}
        rows: dict[str, dict[str, Any]] = {i: r for i, r in fetched.items() if r is not None}
        real = sorted(i for i, r in rows.items() if r["status"] != "placeholder")
        phs = sorted(i for i, r in rows.items() if r["status"] == "placeholder")
        promoted = False
        if real:
            primary = real[0]
        else:
            primary = phs[0]
            phs = phs[1:]
            promoted = True
        result = EnsureResult(primary, promoted=promoted, conflicts=conflicts)
        prow = rows[primary]
        for other in [*phs, *real[1:]]:
            orow = rows[other]
            both_real = orow["status"] != "placeholder" and prow["status"] != "placeholder"
            if both_real and orow["type"] != prow["type"]:
                result.conflicts.append({"alias": None, "asset_id": other, "reason": f"another {orow['type']} asset shares an alias with this {prow['type']}; not merged"})
                continue
            if not both_real and not _compatible(prow["type"], orow["type"]):
                result.conflicts.append({"alias": None, "asset_id": other, "reason": f"placeholder of type {orow['type']} cannot stand for a {prow['type']}"})
                continue
            self.merge(primary, other, scan_id=scan_id, conn=c, reason="alias collision" if both_real else "placeholder resolved")
            result.merged.append(other)
        result.conflicts.extend(self._attach(c, primary, self._admit_qnames(c, primary, ref.type, name, workspace_id, parent_id, pairs, strict)))
        return result

    def _admit_qnames(self, c: Connection, asset_id: str, type_: str, name: str, workspace_id: str | None, parent_id: str | None, pairs: Sequence[tuple[str, str]],
                      strict: Sequence[tuple[str, str]]) -> list[tuple[str, str]]:
        """The aliases of ``pairs`` that may be attached to ``asset_id``: all the authoritative ones, and the qualified names that are not shared with a different object.

        A qualified name derived from names (``<workspace>|silver.orders``) is one asset's alias only while it is unambiguous. If another object with a *different*
        authoritative identity currently derives the same name (two remote Fabric workspaces mapped to one OLCF workspace), nobody keeps the local name: it is removed
        from the earlier owner and not given to this asset, so a reference that only knows the local name resolves to nothing (a placeholder and a gap) instead of to the
        wrong table, while the remote-scoped names (``fab:<tenant>/<workspace>|...``) keep selecting the right one. A name the earlier owner no longer derives (it was renamed
        or moved) is stale and simply passes to this asset."""
        out: list[tuple[str, str]] = []
        for k, v in pairs:
            if k != "qname" or not strict:
                out.append((k, v))
                continue
            owner = self.lookup([(k, v)], c).get((k, v))
            if owner and owner != asset_id:
                if not self._conflicts_with(c, owner, strict):
                    out.append((k, v))
                    continue
                orow = self.get(owner, c)
                if orow is not None and orow["status"] != "removed" and self._derives(c, orow, v):
                    c.execute(delete(asset_aliases).where(asset_aliases.c.kind == k, asset_aliases.c.value == v))
                    continue
                c.execute(delete(asset_aliases).where(asset_aliases.c.kind == k, asset_aliases.c.value == v))       # stale: the owner no longer has this name
                out.append((k, v))
            elif owner is None and not v.startswith("fab:") and self._ambiguous_sibling(c, asset_id, type_, name, workspace_id, parent_id, v, strict):
                continue
            else:
                out.append((k, v))
        return out

    def _derives(self, c: Connection, row: dict[str, Any], qname_value: str) -> bool:
        """Does the asset *currently* derive this scoped qualified name (from its name and the containers above it)?"""
        key = qname_value.split("|", 1)[0]
        mine = {alias_value("qname", v) for _, v in self._derived_qnames(c, row["type"], row["name"], row["parent_id"], [key])}
        return qname_value in mine

    def _ambiguous_sibling(self, c: Connection, asset_id: str, type_: str, name: str, workspace_id: str | None, parent_id: str | None, qname_value: str,
                           strict: Sequence[tuple[str, str]]) -> bool:
        """Is there another live asset of the same type and name, below a container of the same name, with a different authoritative identity?"""
        parent = self.get(parent_id, c) if parent_id else None
        if not workspace_id or parent is None or parent["type"] not in CONTAINER_TYPES:
            return False
        t = assets.c
        twins = [r[0] for r in c.execute(select(t.id).where(t.workspace_id == workspace_id, t.type.in_(sorted(CONTAINER_TYPES)), func.lower(t.name) == parent["name"].lower(),
                                                              t.status != "removed", t.id != parent["id"]))]
        if not twins:
            return False
        for r in fetch_all(c, select(assets).where(t.parent_id.in_(twins), t.type == type_, func.lower(t.name) == name.lower(), t.status != "removed", t.id != asset_id)):
            if self._conflicts_with(c, r["id"], strict) and self._derives(c, r, qname_value):
                return True
        return False

    # ------------------------------------------------------------------------------------------------------------------------ merge
    def merge(self, primary_id: str, other_id: str, *, scan_id: str | None = None, conn: Connection | None = None, reason: str = "merge") -> dict[str, Any]:
        """Fold ``other_id`` into ``primary_id``: aliases move, edges are rewired (duplicates collapse, evidence is unioned), children merge by name, the other
        row becomes ``removed`` with ``attrs.merged_into``. Returns counts. Merging two non-placeholder assets is reported through ``on_event`` (audit)."""
        if primary_id == other_id:
            return {"aliases": 0, "edges": 0, "children": 0}

        def run() -> dict[str, Any]:
            with self._writing(conn) as c:
                return self._merge(c, primary_id, other_id, scan_id, reason)

        return self._retry(run) if conn is None else run()

    def _merge(self, c: Connection, primary_id: str, other_id: str, scan_id: str | None, reason: str) -> dict[str, Any]:
        prow, orow = self.get(primary_id, c), self.get(other_id, c)
        if prow is None or orow is None:
            raise NotFound("cannot merge: asset does not exist")
        now = now_iso()
        counts = {"aliases": 0, "edges": 0, "children": 0}
        counts["aliases"] = c.execute(update(asset_aliases).where(asset_aliases.c.asset_id == other_id).values(asset_id=primary_id)).rowcount
        # children merge by name (case-insensitive, same type); the rest is re-parented
        t = assets.c
        for child in fetch_all(c, select(assets).where(t.parent_id == other_id).order_by(t.id)):
            twin = self._child(c, primary_id, child["name"], child["type"])
            if twin and twin != child["id"]:
                self._merge(c, twin, child["id"], scan_id, "child of merged asset")
            else:
                c.execute(update(assets).where(t.id == child["id"]).values(parent_id=primary_id, updated_at=now))
            counts["children"] += 1
        counts["edges"] = self._rewire_edges(c, primary_id, other_id, now)
        # analysis cache rows follow the asset when the primary has none of that kind
        for row in fetch_all(c, select(analysis_cache.c.kind).where(analysis_cache.c.asset_id == other_id)):
            if fetch_one(c, select(analysis_cache.c.kind).where(analysis_cache.c.asset_id == primary_id, analysis_cache.c.kind == row["kind"])):
                c.execute(delete(analysis_cache).where(analysis_cache.c.asset_id == other_id, analysis_cache.c.kind == row["kind"]))
            else:
                c.execute(update(analysis_cache).where(analysis_cache.c.asset_id == other_id, analysis_cache.c.kind == row["kind"]).values(asset_id=primary_id))
        # gaps about the placeholder follow the real asset; an unresolved-reference gap is resolved by the merge itself
        c.execute(update(gaps).where(gaps.c.asset_id == other_id, gaps.c.kind == "unresolved_reference", gaps.c.resolved_at.is_(None)).values(resolved_at=now, asset_id=primary_id))
        c.execute(update(gaps).where(gaps.c.asset_id == other_id).values(asset_id=primary_id))
        for hook in self.merge_hooks:
            hook(c, other_id, primary_id)
        # the primary keeps its own facts; empty fields are filled from the other asset
        fill: dict[str, Any] = {}
        if not prow["description"] and orow["description"]:
            fill.update(description=orow["description"], description_source=orow["description_source"])
        tags = sorted(set(prow["tags"] or []) | set(orow["tags"] or []))
        if tags != sorted(prow["tags"] or []):
            fill["tags"] = tags
        if not prow["path"] and orow["path"]:
            fill["path"] = orow["path"]
        if not prow["workspace_id"] and orow["workspace_id"]:
            fill["workspace_id"] = orow["workspace_id"]
        if fill or prow["status"] == "placeholder":
            c.execute(update(assets).where(t.id == primary_id).values(updated_at=now, etag=new_etag(), **fill))
        merged_attrs = {**(orow["attrs"] or {}), "merged_into": primary_id, "merge_reason": reason}
        merged_attrs.pop("unresolved", None)
        c.execute(update(assets).where(t.id == other_id).values(status="removed", removed_scan=scan_id, attrs=merged_attrs, updated_at=now, etag=new_etag()))
        if orow["status"] != "placeholder" and prow["status"] != "placeholder":
            details = {"merged": other_id, "type": prow["type"], "reason": reason, "name": prow["name"], "workspace_id": prow["workspace_id"]}
            if self.audit is not None:
                self.audit(c, "intelligence.asset_merged", primary_id, details)           # in the transaction: it commits or rolls back with the merge
            if self.on_event is not None:
                self._outbox().append(("intelligence.asset_merged", primary_id, details))   # delivered after the commit
        return counts

    def _rewire_edges(self, c: Connection, primary_id: str, other_id: str, now: str) -> int:
        e = edges.c
        rows = fetch_all(c, select(edges).where((e.src_id == other_id) | (e.dst_id == other_id) | (e.via_id == other_id)).order_by(e.id))
        moved = 0
        for r in rows:
            src = primary_id if r["src_id"] == other_id else r["src_id"]
            dst = primary_id if r["dst_id"] == other_id else r["dst_id"]
            via = primary_id if r["via_id"] == other_id else r["via_id"]
            if src == dst and r["type"] != "TRANSFORMS":
                c.execute(delete(edges).where(e.id == r["id"]))  # an edge between the placeholder and the real asset is now a self loop
                moved += 1
                continue
            h = M.edge_hash(src, dst, r["type"], via)
            twin = fetch_one(c, select(edges).where(e.edge_hash == h, e.id != r["id"]))
            if twin is None:
                c.execute(update(edges).where(e.id == r["id"]).values(src_id=src, dst_id=dst, via_id=via, edge_hash=h))
            else:
                ev = merge_evidence(twin["evidence"] or [], r["evidence"] or [])
                c.execute(update(edges).where(e.id == twin["id"]).values(
                    evidence=ev, confidence=strongest_confidence(ev, twin["confidence"], r["confidence"]), observation_count=(twin["observation_count"] or 0) + (r["observation_count"] or 0),
                    first_seen_at=min(twin["first_seen_at"], r["first_seen_at"]), last_seen_at=max(twin["last_seen_at"], r["last_seen_at"]),
                    last_verified_at=max(filter(None, [twin["last_verified_at"], r["last_verified_at"]]), default=None),
                    status="active" if "active" in (twin["status"], r["status"]) else twin["status"], transformation=twin["transformation"] or r["transformation"]))
                c.execute(delete(edges).where(e.id == r["id"]))
            moved += 1
        return moved

    # ------------------------------------------------------------------------------------------------------------------------ helpers for callers
    def merged_target(self, asset_id: str) -> str | None:
        """The asset a merged-away asset was folded into (``attrs.merged_into``), following the chain; ``None`` if it was not merged."""
        seen: set[str] = set()
        cur = asset_id
        target: str | None = None
        while cur not in seen:
            seen.add(cur)
            row = self.get(cur)
            nxt = (row or {}).get("attrs", {}).get("merged_into") if row and row["status"] == "removed" else None
            if not nxt:
                break
            target, cur = nxt, nxt
        return target


def _compatible(real_type: str, placeholder_type: str) -> bool:
    """A placeholder may stand for a real asset of the same type, or any dataset (a reference to ``Silver.Sales`` may turn out to be a view)."""
    if real_type == placeholder_type:
        return True
    return real_type in M.DATASET_TYPES and placeholder_type in M.DATASET_TYPES and "column" not in (real_type, placeholder_type)
