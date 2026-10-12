# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""The Engine: the only place that turns facts (assets, edges, gaps produced by connectors, analyzers and the runtime translator) into graph rows.

Public API (what discovery, runtime and tests call)::

    scan_id = engine.begin_scan(source_id, mode, trigger, principal=None)
    stats   = engine.apply_facts(facts, scan_id=scan_id, source_id=source_id)
    stats   = engine.analyze_and_apply(definition, scan_id=scan_id, source_id=source_id)
    n       = engine.record_gaps(scan_id, gaps, source_id)
    result  = engine.finish_scan(scan_id, "succeeded", stats_dict)

A *full* scan that ends ``succeeded`` sweeps: assets and edges of that source that the scan did not report become ``removed`` (never deleted), unseen gaps are
resolved. ``partial``, ``failed`` and ``cancelled`` scans, and every *incremental* scan, never sweep. One fact is applied in one serialised transaction, so a
fact that fails is counted (``ApplyStats.errors``) without losing the others, and concurrent writers (a scan and the runtime inbox) never interleave inside
an edge's read-modify-write.

``analyze_and_apply`` calls the pure analyzers (``olcf.intelligence.lineage.engine.analyze_definition``) and skips a definition whose etag and analyzer version
are unchanged since its last analysis. A skipped definition is *kept alive*: the assets, edges and gaps its last analysis produced are marked as seen by the
current scan, so a full scan does not sweep them. A re-analysed definition retires whatever it no longer produces - *if the analysis was complete*. An analysis that could
not read the whole definition (a parse error, an analyzer exception, an analysis cap, an unsupported construct that made known edges disappear: the *blocking* gaps) reports
``ApplyStats.incomplete`` / ``blocking_gaps``, keeps the last known edges, assets and gaps (edges are flagged ``attrs.stale``) and is analysed again by the next scan; only a complete
analysis retires. What makes an analysis reusable is the definition's etag, the analyzer version and a fingerprint of the resolution context
(:func:`context_fingerprint`): a changed default lakehouse, schema, workspace mapping or model binding re-analyses a definition whose text did not change.
"""

from __future__ import annotations

import hashlib
import importlib
import json
import logging
import re
import threading
import time
from collections.abc import Callable, Iterable, Mapping
from contextlib import contextmanager
from dataclasses import dataclass, field, replace
from functools import partial
from typing import Any

from sqlalchemy import Connection, insert, select, update
from sqlalchemy.exc import IntegrityError

from olcf.core.errors import BadRequest, Conflict, NotFound
from olcf.core.ids import new_id
from olcf.core.timeutil import now_iso
from olcf.db.engine import Database
from olcf.db.util import fetch_all, fetch_one, new_etag
from olcf.intelligence import model as M
from olcf.intelligence.graph import GraphStore
from olcf.intelligence.registry import GRAPH_LOCK, AssetRegistry, Resolution, alias_value
from olcf.intelligence.schema import analysis_cache, asset_aliases, assets, gaps, scans
from olcf.security.audit import AuditLog
from olcf.security.rbac import Principal

log = logging.getLogger("olcf.intelligence.engine")

SCAN_MODES = ("full", "incremental")
SCAN_FINAL = ("succeeded", "partial", "failed", "cancelled")
RUNTIME_SOURCE = "runtime"
_RETRIES = 4
_MAX_ERRORS_KEPT = 20
#: Rows of each kind one transaction of a removal sweep / retirement changes: a sweep over a very large graph is many short write transactions, never one long one.
SWEEP_CHUNK = 5000
RETIRE_CHUNK = 1000


@dataclass
class Touched:
    """Everything one analysis produced (ids and gap hashes); stored in ``analysis_cache.touched`` so an unchanged definition can be kept alive."""

    assets: set[str] = field(default_factory=set)
    edges: set[str] = field(default_factory=set)
    gaps: set[str] = field(default_factory=set)

    def as_dict(self) -> dict[str, list[str]]:
        """JSON form."""
        return {"assets": sorted(self.assets), "edges": sorted(self.edges), "gaps": sorted(self.gaps)}


@dataclass
class ApplyStats:
    """Counters of one :meth:`Engine.apply_facts` / :meth:`Engine.analyze_and_apply` call."""

    assets_created: int = 0
    assets_updated: int = 0
    assets_unchanged: int = 0
    placeholders_created: int = 0
    placeholders_merged: int = 0
    edges_created: int = 0
    edges_updated: int = 0
    edges_unchanged: int = 0
    edges_rejected: int = 0
    gaps: int = 0
    analyzed: int = 0
    analysis_skipped: int = 0
    errors: int = 0
    blocking_gaps: int = 0               # gaps that mean "the definition was not analysed completely" (see :func:`is_blocking_gap`)
    incomplete: bool = False             # True when any analysis behind these counters was incomplete: the last known lineage was kept, nothing may be swept on its strength
    error_samples: list[str] = field(default_factory=list)

    def error(self, message: str) -> None:
        """Count an error and keep the first few messages."""
        self.errors += 1
        if len(self.error_samples) < _MAX_ERRORS_KEPT:
            self.error_samples.append(message[:300])

    def add(self, other: ApplyStats) -> ApplyStats:
        """Add another set of counters in place and return self."""
        for k, v in other.__dict__.items():
            if k == "error_samples":
                self.error_samples.extend(v[: max(0, _MAX_ERRORS_KEPT - len(self.error_samples))])
            elif k == "incomplete":
                self.incomplete = self.incomplete or bool(v)
            else:
                setattr(self, k, getattr(self, k) + v)
        return self

    def as_dict(self) -> dict[str, Any]:
        """Plain dict (JSON friendly, for scan statistics)."""
        return {k: (list(v) if isinstance(v, list) else v) for k, v in self.__dict__.items()}


#: The counters of :class:`ApplyStats`: the keys a scan's ``stats`` carries flat (and, for discovery scans, also under ``stats.apply``).
APPLY_COUNTERS = tuple(f for f in ApplyStats.__dataclass_fields__)


def scan_stats(stats: Mapping[str, Any] | None) -> dict[str, Any]:
    """A scan's ``stats`` with the apply counters (``assets_created``, ``edges_created`` ...) available at both levels: flat at the top and under ``apply``.

    Local, Fabric and run-time scans store the counters flat, discovery scans store them under ``apply`` next to ``facts`` and ``counters``; readers (CLI,
    console, SDK) should not have to know which. Additive: nothing is removed or renamed."""
    out: dict[str, Any] = dict(stats or {})
    nested = out.get("apply")
    if isinstance(nested, dict):
        for k in APPLY_COUNTERS:
            if k in nested:
                out.setdefault(k, nested[k])
    else:
        flat = {k: out[k] for k in APPLY_COUNTERS if k in out}
        if flat:
            out["apply"] = flat
    return out


#: Gap kinds that always mean the definition could not be read: the analysis result is incomplete.
BLOCKING_GAP_KINDS = frozenset({"parse_error"})
#: Wording of the analyzers' "I stopped here" markers (an analysis cap or size limit was hit): ``more than 200000 edges``, ``larger than ... was not analysed``, ``only the first 6000 cells``.
_CAP_HIT = re.compile(r"\b(more than \d+|(larger|longer) than \d+|only the first \d+|deeper than \d+|\d+ levels deep|rest (was|were) not|(was|were) not (analy[sz]ed|read|parsed|recorded))", re.IGNORECASE)


def is_blocking_gap(gap: M.Gap) -> bool:
    """Does this analysis gap mean "something known may have been missed"? A parse error, or a cap-hit marker (an ``unsupported_construct`` that says the analysis stopped at a limit).
    Informational gaps never block. (An ordinary unsupported construct blocks only when known edges disappeared with it; see :meth:`Engine.analyze_and_apply`.)"""
    if (gap.location or {}).get("informational"):
        return False
    if gap.kind in BLOCKING_GAP_KINDS:
        return True
    return gap.kind == "unsupported_construct" and bool(_CAP_HIT.search(gap.detail or ""))


#: The keys of ``DefinitionFact.context`` the analyzers read (``lineage.base.LineageContext``); ``location`` only decorates evidence and ``process`` is the definition's own ref.
LINEAGE_CONTEXT_KEYS = ("workspace_id", "item_id", "item_name", "item_type", "default_lakehouse", "tenant", "fabric_workspace", "fabric_workspace_name", "source", "dialect", "source_id",
                        "semantic_models", "parameters", "known_notebooks", "schemas")


def context_fingerprint(defn: M.DefinitionFact, analyzer_version: str) -> str:
    """A deterministic fingerprint of everything that decides *what lineage* a definition resolves to: the analyzer version, the definition's kind and name, the default lakehouse
    binding, the Fabric tenant / workspace mapping, the dialect, the semantic-model name -> GUID map, the parameters and known notebooks, and the schemas used to expand ``SELECT *``.

    Only the schemas of tables the definition mentions are included (a column added to a table nobody here reads does not re-analyse a thousand notebooks); the match is a
    case-insensitive search for the bare table name in the definition text and the parameters. Volatile context (``location``, timestamps, any key the analyzers do not read) is
    left out, so it never forces a re-analysis. Keys are normalised the way the analyzers read them (lower-cased schema and model names)."""
    ctx = defn.context if isinstance(defn.context, Mapping) else {}
    doc: dict[str, Any] = {"v": analyzer_version, "kind": defn.kind, "name": defn.name}
    for key in LINEAGE_CONTEXT_KEYS:
        val = ctx.get(key)
        if val in (None, "", {}, []) or key == "schemas":
            continue
        if key == "semantic_models" and isinstance(val, Mapping):
            val = {str(k).lower(): str(v).lower() for k, v in val.items()}
        doc[key] = val
    schemas = ctx.get("schemas")
    if isinstance(schemas, Mapping) and schemas:
        text = (json.dumps(defn.payload, default=str, sort_keys=True) + json.dumps(ctx.get("parameters") or {}, default=str, sort_keys=True)).lower()
        doc["schemas"] = {str(k).lower(): [str(c) for c in v] for k, v in sorted(schemas.items(), key=lambda kv: str(kv[0]).lower())
                          if isinstance(v, (list, tuple)) and str(k).lower().rsplit(".", 1)[-1] in text}
    raw = json.dumps(doc, sort_keys=True, default=str, separators=(",", ":"))
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:32]


def gap_hash(source_id: str, kind: str, subject: str, detail: str, location: dict[str, Any] | None) -> str:
    """Identity of a gap: the same gap seen by the next scan is the same row."""
    raw = json.dumps([source_id, kind, subject, detail, location or {}], sort_keys=True, default=str, separators=(",", ":"))
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


class WritePacer:
    """Keeps a graph writer from monopolising the single SQLite writer lock.

    SQLite admits one writer. A waiting writer (a job state change, the event bus persisting an event) retries from a sleeping busy handler whose longest sleep is 100 ms, so
    a writer that commits back to back - however short each transaction - is almost never interrupted: measured with a 4,000 asset scan, 29 event publishes saw a worst case of
    4.2 seconds although no transaction held the lock for more than 51 ms. After ``max_rows`` write units or ``max_ms`` milliseconds of continuous writing the pacer therefore
    pauses the writing thread for ``yield_ms`` (outside any transaction), long enough for every waiting writer to get in. Other writers wait at most about ``max_ms + yield_ms``.
    The limits are the runtime settings ``intelligence.write_batch_max_rows`` / ``write_batch_max_ms`` / ``write_yield_ms``; ``0`` disables a limit. PostgreSQL has no
    single-writer lock, so nothing is paused there. The window is per thread; a thread that was idle for ``yield_ms`` starts a new one."""

    def __init__(self, db: Database, limits: Callable[[], tuple[int, float, float]] | None = None) -> None:
        self.db = db
        self.limits = limits
        self.enabled = True
        self.pauses = 0                              # statistics: how often and how long writers paused (diagnostics, tests)
        self.paused_seconds = 0.0
        self._tl = threading.local()
        self._cached: tuple[float, tuple[int, float, float]] = (0.0, (500, 250.0, 100.0))

    def _limits(self) -> tuple[int, float, float]:
        at, val = self._cached
        now = time.monotonic()
        if self.limits is not None and now - at > 2.0:           # settings are read from the database: not once per write unit
            try:
                val = self.limits()
            except Exception:  # noqa: BLE001 - a settings hiccup never stops the graph
                pass
            self._cached = (now, val)
        return val

    def tick(self) -> None:
        """Count one write unit that ended just now (for maintenance loops that do not go through :meth:`Engine._unit`)."""
        self.after_unit(time.monotonic())

    def after_unit(self, began: float) -> None:
        """Called after each committed write unit (``began`` = ``time.monotonic()`` when it started): pause when the writer has been going for too long."""
        if not self.enabled or not self.db.is_sqlite:
            return
        rows_max, ms_max, yield_ms = self._limits()
        if yield_ms <= 0 or (rows_max <= 0 and ms_max <= 0):
            return
        tl, now = self._tl, time.monotonic()
        if getattr(tl, "start", None) is None or began - getattr(tl, "last", 0.0) > yield_ms / 1000.0:
            tl.start, tl.rows = began, 0
        tl.rows += 1
        tl.last = now
        if (rows_max > 0 and tl.rows >= rows_max) or (ms_max > 0 and (now - tl.start) * 1000.0 >= ms_max):
            time.sleep(yield_ms / 1000.0)
            self.pauses += 1
            self.paused_seconds += yield_ms / 1000.0
            tl.start = None


class Engine:
    """Applies facts to the graph and manages scans (see the module docstring). ``audit`` and ``events`` are optional; with them ``begin_scan`` /
    ``finish_scan`` record ``intelligence.scan_started|finished`` and publish ``intelligence.scan.finished`` (never for the synthetic ``runtime`` scans)."""

    def __init__(self, db: Database, registry: AssetRegistry, graph: GraphStore, *, audit: AuditLog | None = None, events: Any = None) -> None:
        self.db, self.registry, self.graph = db, registry, graph
        self.audit, self.events = audit, events
        #: paces the write units of this engine (see :class:`WritePacer`); the module wires the runtime settings in
        self.pacer = WritePacer(db)
        #: returns ``(analyze_definition, ANALYZER_VERSION)``; replaceable in tests
        self.analyzer_loader: Callable[[], tuple[Callable[[M.DefinitionFact], M.Facts], str]] = self._load_analyzer

    # ------------------------------------------------------------------------------------------------------------------------ plumbing
    @staticmethod
    def _load_analyzer() -> tuple[Callable[[M.DefinitionFact], M.Facts], str]:
        mod = importlib.import_module("olcf.intelligence.lineage.engine")
        return mod.analyze_definition, str(mod.ANALYZER_VERSION)

    @contextmanager
    def _serial(self) -> Any:
        with self.db.begin_serialized(GRAPH_LOCK) as conn:
            yield conn

    def _unit(self, fn: Callable[[Connection], Any]) -> Any:
        """Run ``fn(conn)`` in one serialised transaction; an alias race (IntegrityError) retries the unit, the winner's rows are then found."""
        last: Exception | None = None
        for _ in range(_RETRIES):
            began = time.monotonic()
            try:
                with self._serial() as conn:
                    out = fn(conn)
            except IntegrityError as exc:
                self.registry.discard_events()
                last = exc
                continue
            except BaseException:
                self.registry.discard_events()          # the unit rolled back: its queued notifications never happened
                raise
            self.registry.flush_events()                # committed: now (outside the critical section) tell the world
            self.pacer.after_unit(began)                # and let other writers in when this thread has been writing for too long
            return out
        assert last is not None
        raise last

    def scan_row(self, scan_id: str) -> dict[str, Any]:
        """The ``scans`` row (``NotFound`` if there is none)."""
        return self.graph.scan_row(scan_id)

    def latest_scan(self, source_id: str, *, status: str | None = None) -> dict[str, Any] | None:
        """The newest scan of a source (optionally with a given status)."""
        stmt = select(scans).where(scans.c.source_id == source_id)
        if status:
            stmt = stmt.where(scans.c.status == status)
        with self.db.connect() as c:
            return fetch_one(c, stmt.order_by(scans.c.version.desc()).limit(1))

    # ------------------------------------------------------------------------------------------------------------------------ scans
    def begin_scan(self, source_id: str, mode: str, trigger: str, principal: Principal | None = None) -> str:
        """Open a scan of ``source_id`` (``full`` or ``incremental``) and return its ``scn_`` id. The scan's ``version`` is the next number of that source."""
        if mode not in SCAN_MODES:
            raise BadRequest(f"scan mode must be one of {list(SCAN_MODES)}")
        if not source_id or not trigger:
            raise BadRequest("source_id and trigger are required")
        sid = new_id("scn")

        def run(conn: Connection) -> None:
            cur = conn.execute(select(scans.c.version).where(scans.c.source_id == source_id).order_by(scans.c.version.desc()).limit(1)).first()
            conn.execute(insert(scans).values(id=sid, source_id=source_id, mode=mode, trigger=trigger, status="running", version=(cur[0] if cur else 0) + 1,
                                              principal_id=principal.id if principal else None, started_at=now_iso(), stats={}, error=None))

        self._unit(run)
        audit = self.audit
        if source_id != RUNTIME_SOURCE and audit is not None:
            self._safely(lambda: audit.record(principal, "intelligence.scan_started", resource_type="scan", resource_id=sid,
                                              details={"source_id": source_id, "mode": mode, "trigger": trigger}))
        return sid

    def finish_scan(self, scan_id: str, status: str, stats: dict[str, Any] | None = None, error: str | None = None) -> dict[str, Any]:
        """Close a scan. ``succeeded`` + ``full`` runs the removal sweep (its counts are added to ``stats.sweep``). Returns the scan row."""
        if status not in SCAN_FINAL:
            raise BadRequest(f"final scan status must be one of {list(SCAN_FINAL)}")

        swept = self._sweep_in_chunks(scan_id) if status == "succeeded" else None

        def run(conn: Connection) -> dict[str, Any]:
            row = fetch_one(conn, select(scans).where(scans.c.id == scan_id))
            if row is None:
                raise NotFound(f"scan '{scan_id}' does not exist")
            if row["status"] != "running":
                raise Conflict(f"scan '{scan_id}' is already {row['status']}")
            merged = scan_stats(stats)
            if row["mode"] == "full" and status == "succeeded":
                last = self.graph.sweep(conn, row["source_id"], scan_id, limit=SWEEP_CHUNK)
                merged["sweep"] = {k: v + (swept or {}).get(k, 0) for k, v in last.items()}
            finished = now_iso()
            conn.execute(update(scans).where(scans.c.id == scan_id).values(status=status, finished_at=finished, stats=merged, error=(error[:2000] if error else None)))
            if row["source_id"] != RUNTIME_SOURCE:
                self._mark_tenants(conn, row["source_id"], status, finished)
            return {**row, "status": status, "stats": merged, "error": error}

        done = self._unit(run)
        if done["source_id"] != RUNTIME_SOURCE:
            payload = {"scan_id": scan_id, "source_id": done["source_id"], "mode": done["mode"], "status": status, "version": done["version"], "stats": done["stats"]}
            audit, events = self.audit, self.events
            if audit is not None:
                self._safely(lambda: audit.record(None, "intelligence.scan_finished", resource_type="scan", resource_id=scan_id, details=payload))
            if events is not None:
                self._safely(lambda: events.publish("intelligence.scan.finished", payload, subject=scan_id))
        return done

    def _sweep_in_chunks(self, scan_id: str) -> dict[str, int] | None:
        """The bulk of a full scan's removal sweep, ``SWEEP_CHUNK`` rows per write transaction (the closing transaction of :meth:`finish_scan` sweeps what is left). The sweep is
        idempotent - it removes what the scan did not see - so a scan that dies in between leaves nothing wrong behind. ``None`` for scans that do not sweep."""
        row = self.graph.scan_row(scan_id)
        if row["mode"] != "full" or row["status"] != "running":
            return None
        total = {"assets_removed": 0, "edges_removed": 0, "gaps_resolved": 0}
        while True:
            counts = self._unit(lambda conn: self.graph.sweep(conn, row["source_id"], scan_id, limit=SWEEP_CHUNK))
            for k, v in counts.items():
                total[k] += v
            if all(v < SWEEP_CHUNK for v in counts.values()):
                return total

    @staticmethod
    def _mark_tenants(conn: Connection, source_id: str, status: str, when: str) -> None:
        """Record the outcome of a scan on the source's tenant asset (``attrs.last_scan_status`` / ``last_scanned_at``): a scan that failed after its first batch still
        leaves the tenant root asset behind, and this is how the console and CLI can tell. Not a content change, so no new version and no new etag."""
        for r in fetch_all(conn, select(assets.c.id, assets.c.attrs).where(assets.c.source_id == source_id, assets.c.type == "tenant", assets.c.status != "removed")):
            conn.execute(update(assets).where(assets.c.id == r["id"]).values(attrs={**(r["attrs"] or {}), "last_scan_status": status, "last_scanned_at": when}))

    @staticmethod
    def _safely(fn: Callable[[], Any]) -> None:
        try:
            fn()
        except Exception:  # noqa: BLE001 - bookkeeping never fails a scan
            log.exception("intelligence bookkeeping failed")

    def _require_running(self, scan_id: str) -> dict[str, Any]:
        row = self.scan_row(scan_id)
        if row["status"] != "running":
            raise Conflict(f"scan '{scan_id}' is {row['status']}; facts can only be applied to a running scan")
        return row

    def retire_by_alias(self, kind: str, value: str, *, scan_id: str | None = None) -> int:
        """Mark the asset an alias points at, and everything below it, ``removed`` (an item, lakehouse or workspace was deleted). Returns the assets changed.
        The tree is read first; the change is made ``RETIRE_CHUNK`` assets per write transaction."""
        with self.db.connect() as conn:
            row = conn.execute(select(asset_aliases.c.asset_id).where(asset_aliases.c.kind == kind, asset_aliases.c.value == alias_value(kind, value))).first()
            if row is None:
                return 0
            ids, seen, frontier = [row[0]], {row[0]}, [row[0]]
            while frontier:
                kids: list[str] = []
                for i in range(0, len(frontier), 300):
                    kids.extend(r[0] for r in conn.execute(select(assets.c.id).where(assets.c.parent_id.in_(frontier[i:i + 300]))) if r[0] not in seen)
                seen.update(kids)
                ids.extend(kids)
                frontier = kids
        n = 0
        for start in range(0, len(ids), RETIRE_CHUNK):
            chunk = ids[start:start + RETIRE_CHUNK]

            def run(conn: Connection, chunk: list[str] = chunk) -> int:
                total = 0
                for i in range(0, len(chunk), 300):
                    total += conn.execute(update(assets).where(assets.c.id.in_(chunk[i:i + 300]), assets.c.status.in_(("active", "placeholder")))
                                          .values(status="removed", removed_scan=scan_id, updated_at=now_iso(), etag=new_etag())).rowcount
                return total

            n += int(self._unit(run))
        return n

    def prune_runtime_evidence(self, batch: int = 500) -> int:
        """Drop expired run-time evidence from the edges that carry some, ``batch`` edges per write transaction. Returns the edges changed."""
        total = 0
        while True:
            n = int(self._unit(lambda conn: self.graph.prune_runtime_evidence(conn, limit=batch)))
            total += n
            if n < batch:
                return total

    def trim_versions(self, keep_per_asset: int) -> int:
        """Trim attribute histories longer than ``keep_per_asset`` (a few assets per write transaction, with the writer pauses of :class:`WritePacer` in between)."""
        return self.graph.purge_versions(keep_per_asset, after_batch=self.pacer.tick)

    # ------------------------------------------------------------------------------------------------------------------------ facts
    def apply_facts(self, facts: M.Facts, *, scan_id: str, source_id: str, touched: Touched | None = None, gap_asset_id: str | None = None) -> ApplyStats:
        """Apply assets, then edges, then gaps. Idempotent: applying the same facts again creates nothing new (only ``last_seen_*`` / ``observation_count`` move).
        ``gap_asset_id`` attaches the gaps of ``facts`` to that asset (the definition they were found in) so they show up in its lineage and documentation."""
        self._require_running(scan_id)
        stats = ApplyStats()
        stats.blocking_gaps = sum(1 for g in facts.gaps if is_blocking_gap(g))
        stats.incomplete = stats.blocking_gaps > 0
        touched = touched if touched is not None else Touched()
        for af in facts.assets:
            try:
                self._unit(partial(self._apply_asset, fact=af, scan_id=scan_id, source_id=source_id, stats=stats, touched=touched))
            except Exception as exc:  # noqa: BLE001 - one bad fact does not lose the rest of the batch
                log.warning("asset fact %r failed: %s", af.name, exc)
                stats.error(f"asset {af.name!r}: {type(exc).__name__}: {exc}")
        for ef in facts.edges:
            try:
                self._unit(partial(self._apply_edge, ef=ef, scan_id=scan_id, source_id=source_id, stats=stats, touched=touched))
            except Exception as exc:  # noqa: BLE001
                log.warning("edge fact %s failed: %s", ef.type, exc)
                stats.error(f"edge {ef.type}: {type(exc).__name__}: {exc}")
        if facts.gaps:
            try:
                gap_list = list(facts.gaps)
                stats.gaps += self._unit(lambda conn: self._record_gaps(conn, scan_id, gap_list, source_id, gap_asset_id, touched))
            except Exception as exc:  # noqa: BLE001
                stats.error(f"gaps: {type(exc).__name__}: {exc}")
        return stats

    def _apply_asset(self, conn: Connection, *, fact: M.AssetFact, scan_id: str, source_id: str, stats: ApplyStats, touched: Touched) -> None:
        parent_id: str | None = None
        parent_ws: str | None = None
        if fact.parent is not None:
            pres = self.registry.resolve_ex(fact.parent, create=True, conn=conn, scan_id=scan_id, source_id=source_id)
            parent_id = pres.asset_id
            prow = self.registry.get(parent_id, conn)
            parent_ws = prow["workspace_id"] if prow else None
            if pres.created:
                stats.placeholders_created += 1
        ws = fact.workspace_id or parent_ws
        res = self.registry.ensure(fact.ref, name=fact.name, workspace_id=ws, source_id=source_id, scan_id=scan_id, parent_id=parent_id, path=fact.path, conn=conn)
        out = self.graph.upsert_asset(conn, res.asset_id, replace(fact, workspace_id=ws), scan_id=scan_id, source_id=source_id, parent_id=parent_id, created=res.created)
        touched.assets.add(res.asset_id)
        if res.promoted or res.merged:
            conn.execute(update(gaps).where(gaps.c.asset_id.in_([res.asset_id, *res.merged]), gaps.c.kind == "unresolved_reference", gaps.c.resolved_at.is_(None))
                         .values(resolved_at=now_iso()))
        stats.placeholders_merged += len(res.merged) + (1 if res.promoted else 0)
        if out.kind == "created":
            stats.assets_created += 1
        elif out.kind == "updated":
            stats.assets_updated += 1
        else:
            stats.assets_unchanged += 1
        for c in res.conflicts:
            if c.get("identity"):
                gap = M.Gap("identity_conflict", "engine", fact.name, f"{fact.ref.type} '{fact.name}' has the same qualified name as {c['asset_id']} but a different authoritative identity; the two are kept as separate assets",
                            "Code that mentions the name alone cannot be resolved to either: qualify the reference with its Fabric workspace, or map the workspaces to different OLCF workspaces.",
                            {"informational": True, "other_asset": c["asset_id"]})
            else:
                gap = M.Gap("unresolved_reference", "engine", fact.name, f"{fact.ref.type} '{fact.name}': {c['reason']} ({c['asset_id']})",
                            "Two assets of different types share an identifier; check which source reports the wrong type.", {})
            touched.gaps.add(self._gap_row(conn, scan_id, source_id, gap, res.asset_id))

    def _apply_edge(self, conn: Connection, *, ef: M.EdgeFact, scan_id: str, source_id: str, stats: ApplyStats, touched: Touched) -> None:
        if ef.type not in M.STORED_EDGE_TYPES:
            stats.edges_rejected += 1
            stats.error(f"edge type {ef.type!r} is not stored (GOVERNED_BY / CLASSIFIED_AS are served from governance tables)")
            return
        src = self.registry.resolve_ex(ef.src, create=True, conn=conn, scan_id=scan_id, source_id=source_id)
        dst = self.registry.resolve_ex(ef.dst, create=True, conn=conn, scan_id=scan_id, source_id=source_id)
        via = self.registry.resolve_ex(ef.via, create=True, conn=conn, scan_id=scan_id, source_id=source_id) if ef.via is not None else None
        stats.placeholders_created += sum(1 for r in (src, dst, via) if r is not None and r.created)
        if src.asset_id == dst.asset_id and ef.type != "TRANSFORMS":
            stats.edges_rejected += 1
            return
        res = self.graph.upsert_edge(conn, src_id=src.asset_id, dst_id=dst.asset_id, type_=ef.type, via_id=via.asset_id if via else None, evidence=ef.evidence,
                                     confidence=ef.confidence, transformation=ef.transformation, attrs=ef.attrs, scan_id=scan_id, source_id=source_id)
        touched.edges.add(res.id)
        if res.created:
            stats.edges_created += 1
        elif res.changed:
            stats.edges_updated += 1
        else:
            stats.edges_unchanged += 1
        if source_id != RUNTIME_SOURCE:
            loc = dict(ef.evidence[0].location) if ef.evidence else {}
            for r, ref in ((src, ef.src), (dst, ef.dst), (via, ef.via)):
                if r is not None and ref is not None and r.placeholder:
                    touched.gaps.add(self._unresolved_gap(conn, scan_id, source_id, r, ref, ef, loc))

    def _unresolved_gap(self, conn: Connection, scan_id: str, source_id: str, res: Resolution, ref: M.AssetRef, ef: M.EdgeFact, loc: dict[str, Any]) -> str:
        row = self.registry.get(res.asset_id, conn) or {}
        subject = row.get("attrs", {}).get("referenced_as") or row.get("name") or ref.name or res.asset_id
        gap = M.Gap("unresolved_reference", "engine", str(subject),
                    f"{ref.type} '{subject}' is used by a {ef.type} relationship but no discovery source has reported it",
                    "Run a scan of the source that holds it, or check the name and the default lakehouse of the code that mentions it.", loc)
        return self._gap_row(conn, scan_id, source_id, gap, res.asset_id)

    # ------------------------------------------------------------------------------------------------------------------------ gaps
    def record_gaps(self, scan_id: str, gaps_: Iterable[M.Gap], source_id: str, *, asset_id: str | None = None) -> int:
        """Store gaps reported by a connector or analyzer (the same gap seen again is one row whose ``last_seen_at`` moves; a resolved one reopens). Returns the count."""
        items = list(gaps_)
        if not items:
            return 0
        return int(self._unit(lambda conn: self._record_gaps(conn, scan_id, items, source_id, asset_id, None)))

    def report_identity_conflicts(self, scan_id: str, source_id: str) -> int:
        """Record an ``identity_conflict`` gap for every asset that carries two different authoritative identities (assets conflated by an older build; see
        :meth:`AssetRegistry.identity_conflicts`). Idempotent: the same conflict is one gap row. Returns the number of conflicts."""
        found = self.registry.identity_conflicts()
        items = [M.Gap("identity_conflict", "registry", f"{r['type']} {r['name']}",
                       "the asset holds different authoritative identities (" + "; ".join(f"{k}: {', '.join(vs)}" for k, vs in sorted(r["kinds"].items())) + ")",
                       "re-scan the sources after removing the identity that does not belong to this asset; the registry no longer joins such objects", {"asset": r["asset_id"]})
                 for r in found]
        for r, g in zip(found, items, strict=True):
            self.record_gaps(scan_id, [g], source_id, asset_id=r["asset_id"])
        return len(found)

    def _record_gaps(self, conn: Connection, scan_id: str, items: Iterable[M.Gap], source_id: str, asset_id: str | None, touched: Touched | None) -> int:
        n = 0
        for g in items:
            aid = asset_id or (g.subject if isinstance(g.subject, str) and g.subject.startswith("ast_") else None)
            h = self._gap_row(conn, scan_id, source_id, g, aid)
            if touched is not None:
                touched.gaps.add(h)
            n += 1
        return n

    def _gap_row(self, conn: Connection, scan_id: str, source_id: str, g: M.Gap, asset_id: str | None) -> str:
        h = gap_hash(source_id, g.kind, g.subject, g.detail, g.location)
        now = now_iso()
        t = gaps.c
        row = fetch_one(conn, select(t.id).where(t.source_id == source_id, t.gap_hash == h))
        if row is None:
            conn.execute(insert(gaps).values(gap_hash=h, scan_id=scan_id, last_scan_id=scan_id, source_id=source_id, asset_id=asset_id, kind=g.kind[:32], origin=(g.source or "")[:64],
                                             subject=g.subject[:1024], detail=g.detail, remediation=g.remediation, location=dict(g.location or {}), first_seen_at=now,
                                             last_seen_at=now, resolved_at=None))
        else:
            vals: dict[str, Any] = {"last_scan_id": scan_id, "last_seen_at": now, "resolved_at": None}
            if asset_id:
                vals["asset_id"] = asset_id
            conn.execute(update(gaps).where(t.id == row["id"]).values(**vals))
        return h

    # ------------------------------------------------------------------------------------------------------------------------ definitions
    def analyze_and_apply(self, defn: M.DefinitionFact, *, scan_id: str, source_id: str, force: bool = False) -> ApplyStats:
        """Run the lineage analyzer over a stored definition and apply its facts. Skipped (and kept alive) when ``defn.etag``, the analyzer version and the resolution
        context (:func:`context_fingerprint`) are unchanged since the last *complete* analysis, unless ``force``.

        An analysis that is incomplete (``stats.incomplete``: a parse error, an analyzer exception, an analysis cap, or an unsupported construct next to edges the previous analysis
        knew and this one did not find) applies what it did find, keeps everything the previous analysis produced that this one did not (edges are flagged ``attrs.stale``, with the
        scan that last confirmed them) and does not cache the result, so the next scan analyses the definition again. Only a complete analysis retires what is gone."""
        self._require_running(scan_id)
        stats = ApplyStats()
        try:
            analyze, version = self.analyzer_loader()
        except ModuleNotFoundError as exc:
            if exc.name and (exc.name == "olcf.intelligence.lineage" or exc.name.startswith("olcf.intelligence.lineage.")):
                stats.error("the lineage analyzers are not installed in this deployment")
                self.record_gaps(scan_id, [M.Gap("not_inspected", "engine", defn.name, "lineage analyzers are not installed in this deployment",
                                                 "Install a build that includes olcf.intelligence.lineage.", {"kind": defn.kind})], source_id)
                return stats
            raise
        process = self._unit(lambda conn: self.registry.resolve_ex(defn.ref, create=True, conn=conn, scan_id=scan_id, source_id=source_id))
        pid = process.asset_id
        with self.db.connect() as c:
            cached = fetch_one(c, select(analysis_cache).where(analysis_cache.c.asset_id == pid, analysis_cache.c.kind == defn.kind))
        fingerprint = context_fingerprint(defn, version)
        old = (cached or {}).get("touched") or {}
        if (not force and defn.etag and cached and cached["etag"] == defn.etag and cached["analyzer_version"] == version and cached.get("context_hash") == fingerprint
                and not old.get("incomplete")):
            self._unit(lambda conn: self._keep_alive(conn, old, pid, scan_id, source_id))
            stats.analysis_skipped += 1
            return stats
        try:
            facts = analyze(defn)
        except Exception as exc:  # noqa: BLE001 - an analyzer bug is a gap, not a failed scan
            log.warning("analysis of %r failed: %s", defn.name, exc)
            stats.error(f"analysis of {defn.name!r}: {type(exc).__name__}: {exc}")
            stats.blocking_gaps, stats.incomplete = 1, True
            self.record_gaps(scan_id, [M.Gap("parse_error", "engine", defn.name, f"the analyzer raised {type(exc).__name__}: {str(exc)[:300]}",
                                             "Report this definition to the OLCF maintainers; lineage for it is incomplete.", {"kind": defn.kind})], source_id, asset_id=pid)
            why = f"the analyzer raised {type(exc).__name__}"
            self._unit(lambda conn: self._retain(conn, old, set(), pid, scan_id, source_id, why))
            return stats
        touched = Touched(assets={pid})
        applied = self.apply_facts(facts, scan_id=scan_id, source_id=source_id, touched=touched, gap_asset_id=pid)
        stats.add(applied)
        stats.analyzed += 1
        gone_edges = set(old.get("edges", [])) - touched.edges
        if not stats.incomplete and gone_edges:
            lost = [g for g in facts.gaps if g.kind == "unsupported_construct" and not (g.location or {}).get("informational")]
            if lost:                                       # edges that were there are gone and the analysis says it skipped something: it cannot tell "removed" from "not understood"
                stats.blocking_gaps += len(lost)
                stats.incomplete = True
        if applied.errors:
            stats.incomplete = True
        reason = next((f"{g.kind}: {g.detail}" for g in facts.gaps if is_blocking_gap(g)), None) or "the analysis was incomplete"
        self._unit(lambda conn: self._finish_analysis(conn, defn, pid, version, fingerprint, touched, old, scan_id, source_id, incomplete=stats.incomplete, reason=reason))
        return stats

    def _retain(self, conn: Connection, old: dict[str, Any], keep: set[str], pid: str, scan_id: str, source_id: str, reason: str) -> None:
        """Keep the last known output of a definition that could not be analysed completely: its assets and gaps count as seen, its edges as well but flagged stale."""
        gone_edges = sorted(set(old.get("edges", [])) - keep)
        self.graph.touch_assets(conn, [pid, *old.get("assets", [])], scan_id)
        if gone_edges:
            self.graph.mark_edges_stale(conn, gone_edges, scan_id, reason)
        hashes = old.get("gaps", [])
        for i in range(0, len(hashes), 300):
            conn.execute(update(gaps).where(gaps.c.source_id == source_id, gaps.c.gap_hash.in_(hashes[i:i + 300])).values(last_scan_id=scan_id, last_seen_at=now_iso()))

    def _keep_alive(self, conn: Connection, touched: dict[str, Any], pid: str, scan_id: str, source_id: str) -> None:
        self.graph.touch_assets(conn, [pid, *touched.get("assets", [])], scan_id)
        self.graph.touch_edges(conn, touched.get("edges", []), scan_id)
        hashes = touched.get("gaps", [])
        for i in range(0, len(hashes), 300):
            conn.execute(update(gaps).where(gaps.c.source_id == source_id, gaps.c.gap_hash.in_(hashes[i:i + 300])).values(last_scan_id=scan_id, last_seen_at=now_iso()))

    def _finish_analysis(self, conn: Connection, defn: M.DefinitionFact, pid: str, version: str, fingerprint: str, touched: Touched, old: dict[str, Any], scan_id: str,
                         source_id: str, *, incomplete: bool = False, reason: str = "") -> None:
        if incomplete:
            self._retain(conn, old, touched.edges, pid, scan_id, source_id, reason)
            stored: dict[str, Any] = dict(Touched(assets=touched.assets | set(old.get("assets", [])), edges=touched.edges | set(old.get("edges", [])),
                                                  gaps=touched.gaps | set(old.get("gaps", []))).as_dict())
            stored["incomplete"] = True                    # the next scan analyses again (never skipped), and a complete analysis then retires what really went
        else:
            gone_edges = sorted(set(old.get("edges", [])) - touched.edges)
            gone_assets = sorted(set(old.get("assets", [])) - touched.assets)
            gone_gaps = sorted(set(old.get("gaps", [])) - touched.gaps)
            if gone_edges:
                self.graph.retire_edges(conn, gone_edges, scan_id, source_id)
            if gone_assets:
                self.graph.retire_assets(conn, gone_assets, scan_id, source_id)
            for i in range(0, len(gone_gaps), 300):
                conn.execute(update(gaps).where(gaps.c.source_id == source_id, gaps.c.gap_hash.in_(gone_gaps[i:i + 300]), gaps.c.resolved_at.is_(None)).values(resolved_at=now_iso()))
            stored = dict(touched.as_dict())
        vals = {"analyzer_version": version, "etag": defn.etag, "context_hash": fingerprint, "analyzed_at": now_iso(), "edge_count": len(stored["edges"]), "touched": stored}
        n = conn.execute(update(analysis_cache).where(analysis_cache.c.asset_id == pid, analysis_cache.c.kind == defn.kind).values(**vals)).rowcount
        if not n:
            conn.execute(insert(analysis_cache).values(asset_id=pid, kind=defn.kind, **vals))
