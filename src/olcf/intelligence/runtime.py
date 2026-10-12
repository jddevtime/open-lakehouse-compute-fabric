# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Runtime lineage: translates platform events into :class:`~olcf.intelligence.model.Facts` (**pure**: no database, no network, no clock).

The compute side (SQL, jobs and notebooks, pipelines, the catalog) publishes ordinary platform events and never imports this package (contract
``docs/dev/INTELLIGENCE.md`` sections 1, 6 and 7). The inbox processor of the core hands every stored event to :func:`facts_from_event` and applies the returned facts
with the ``runtime`` source; what the event *proves* is the point: a process **really** read or wrote a dataset, so those edges are ``verified`` and carry runtime
evidence (``Evidence(kind="runtime", source="runtime:job|sql|pipeline", confidence="verified", ref=<job / query / run id>)``).

Events handled
--------------
``dataset.read`` / ``dataset.written``
    ``READS_FROM`` / ``WRITES_TO`` edges from the **process** asset to every dataset of the event. The process is

    * an ``olcf_item`` (notebook, Spark job definition, saved query, report, pipeline) when the event names the item;
    * for ad-hoc SQL the ``sql_query`` asset keyed by the ``query`` alias ``sha1:<statement hash>`` (the same statement is the same asset, wherever it ran);
    * **a run that came from a Microsoft Fabric notebook** (``process.fabric`` carries the Entra tenant, the Fabric workspace GUID and the notebook's item GUID, copied from the job's
      Fabric ``origin`` by the control plane): the Fabric notebook asset the tenant scan discovered, keyed by the ``fabric`` alias ``<tenant>/<workspace>/<notebook>``
      (``fabric_alias``), so the runtime edges attach to the same ``ast_`` asset in both orders (scan first, or events first and the scan promotes the placeholder).
      When the identifiers are incomplete (no notebook GUID or no tenant) but the notebook's name is known, the process is matched **by name** inside the OLCF workspace
      the Fabric workspace maps to and the edges are only ``inferred``: what ran is certain, which notebook asset it was is a guess;
    * for a job run without an item an ``spark_job`` asset keyed by the job id (``source`` alias ``olcf-job:<job id>``).

    Datasets are ``table`` assets (alias ``lakehouse_table = <lakehouse id>:<name>`` when the lakehouse is known, otherwise resolved by name inside the workspace scope)
    or ``file`` assets (alias ``onelake_path`` / ``delta_location`` from :func:`~olcf.intelligence.model.normalize_path` of the URI, plus ``source = olcf-file:<lakehouse id>:<path>``
    for lakehouse-relative paths). A dataset whose lakehouse is attached to a Fabric lakehouse (``datasets[].fabric`` = the Fabric workspace and lakehouse GUIDs of the OLCF
    lakehouse's ``definition.fabric``: the data lives in OneLake) also carries the ``onelake_path`` alias of ``abfss://<ws>@onelake.dfs.fabric.microsoft.com/<lakehouse>/Tables/<table>``
    and, when the tenant is known, the ``fabric`` table alias the tenant scan gives the same table - so the edge lands on the asset the Fabric scan discovered. A lakehouse that
    is only *mapped* to a Fabric one (``lakehouse_map`` to an on-premises copy) has no ``fabric`` block and keeps its own table assets.
``dataset.statement``
    The SQL text that really ran (for a Fabric run the process is the Fabric notebook, and bare table names are given the Fabric aliases of the default lakehouse). It is analysed with the static SQL analyzer (``olcf.intelligence.lineage.engine.analyze_definition``, imported lazily) and every
    resulting edge is then **upgraded to runtime evidence**: table-level reads and writes become ``verified``, column mappings the analyzer derived become ``verified`` too (the
    statement executed), mappings it could only guess (``inferred``) or not determine (``unknown``) keep their level. The analyzer's location and snippet are kept.
``dataset.schema_changed``
    An asset update for the table (``attrs.columns`` = the new column list, ``attrs.schema_changed_at``) and column assets: added columns (with type), changed columns (new
    type / nullability) and dropped columns (``attrs.dropped = true``; a column asset is never deleted).
``pipeline.run.succeeded|failed|cancelled``
    An asset update for the pipeline with ``attrs.last_run_state`` (state only: no ids or timestamps, so attribute history records state *flips*, not every run).
``job.state_changed``
    Recognised and deliberately empty: the event carries the job's name, not the item's, and an asset fact would rename the notebook after its last job. What a job did
    is reported by its ``dataset.*`` events.

Anything else returns ``None``. Malformed payloads are never raised on: an event that names nothing usable returns ``None``.
"""

from __future__ import annotations

import importlib
import posixpath
import re
from dataclasses import dataclass, field
from typing import Any
from urllib.parse import urlparse

import olcf.core.dataset_events as dsev
from olcf.core.dataset_events import DIALECTS, statement_hash
from olcf.intelligence.model import (
    AssetFact,
    AssetRef,
    DefinitionFact,
    EdgeFact,
    Evidence,
    Facts,
    Gap,
    fabric_alias,
    normalize_path,
    normalize_qname,
    stronger,
)

#: Source id the inbox applies runtime facts under.
RUNTIME_SOURCE = "runtime"
#: Event types the inbox should hand to :func:`facts_from_event` (``pipeline.run.*`` is matched by prefix, see :func:`handles`).
RUNTIME_EVENT_TYPES: tuple[str, ...] = (
    "dataset.read", "dataset.written", "dataset.statement", "dataset.schema_changed", "job.state_changed",
    "pipeline.run.succeeded", "pipeline.run.failed", "pipeline.run.cancelled",
)
_TERMINAL_PIPELINE = ("succeeded", "failed", "cancelled")
_MAX_COLUMNS = 500
_ONELAKE_HOST = "onelake.dfs.fabric.microsoft.com"
_SHA = re.compile(r"^(?:sha1:)?([0-9a-f]{40})$")


def handles(event_type: str) -> bool:
    """True if :func:`facts_from_event` knows ``event_type`` (so the inbox can skip the others without calling it)."""
    return event_type in RUNTIME_EVENT_TYPES or event_type.startswith("pipeline.run.")


# --------------------------------------------------------------------------------------------- process identity
@dataclass
class Process:
    """What read, wrote or ran: its asset reference and how to describe the evidence."""

    ref: AssetRef
    name: str
    type: str
    source: str                       # evidence source: runtime:job | runtime:sql | runtime:pipeline
    ref_id: str | None                # job id / query id / run id (the evidence ``ref``)
    owned: bool                       # True when the runtime is the only source of this asset (ad-hoc query, item-less job): it may name and describe it
    location: dict[str, Any] = field(default_factory=dict)
    attrs: dict[str, Any] = field(default_factory=dict)
    confidence: str = "verified"      # the strongest confidence the runtime evidence may claim; ``inferred`` when the process asset itself was matched by name
    tenant: str | None = None         # Entra tenant of a Fabric run (for the Fabric aliases of its datasets)
    fabric_workspace: str | None = None


def _s(value: Any) -> str | None:
    """``value`` as a non-empty string, else ``None``."""
    return value.strip() if isinstance(value, str) and value.strip() else None


def _list(value: Any) -> list[Any]:
    return value if isinstance(value, list) else []


def _hash(payload: dict[str, Any], sql: str | None = None) -> str | None:
    """The statement hash of an event: its ``statement_hash`` (with or without the ``sha1:`` prefix), else computed from the SQL text."""
    raw = _s(payload.get("statement_hash"))
    m = _SHA.match(raw.lower()) if raw else None
    if m:
        return m.group(1)
    return statement_hash(sql) if sql else None


def _process(payload: dict[str, Any], *, sql: str | None = None) -> Process | None:
    """Resolve the ``process`` block of a ``dataset.*`` event to a :class:`Process` (``None`` when it names nothing identifiable)."""
    proc = payload.get("process")
    if not isinstance(proc, dict):
        return None
    ws, kind = _s(payload.get("workspace_id")), _s(proc.get("kind")) or "job"
    item_id, job_id, query_id, run_id, pname = _s(proc.get("item_id")), _s(proc.get("job_id")), _s(proc.get("query_id")), _s(proc.get("run_id")), _s(proc.get("name"))
    engine = _s(payload.get("engine")) or _s(payload.get("dialect"))
    scope = {"workspace_id": ws} if ws else {}
    loc: dict[str, Any] = {k: v for k, v in (("workspace_id", ws), ("item_id", item_id), ("job_id", job_id), ("query_id", query_id), ("run_id", run_id),
                                            ("engine", engine), ("process", pname)) if v}
    h = _hash(payload, sql)
    if h:
        loc["statement_hash"] = h
    if kind in ("sql_query", "query", "report") and not item_id:
        if h:
            return Process(AssetRef.make("sql_query", {"query": f"sha1:{h}"}, name=f"SQL {h[:10]}", scope=scope), f"SQL {h[:10]}", "sql_query", "runtime:sql", query_id, True, loc,
                           {"engine": engine, "statement_hash": h, "observed": "runtime"})
        if query_id:
            return Process(AssetRef.make("sql_query", {"source": f"olcf-query:{query_id}"}, name=f"query {query_id}", scope=scope), f"query {query_id}", "sql_query", "runtime:sql",
                           query_id, True, loc, {"engine": engine, "observed": "runtime"})
        return None
    if kind in ("sql_query", "query", "report") and item_id:
        atype = "report" if kind == "report" else "sql_query"
        return Process(AssetRef.make(atype, {"olcf_item": item_id}, name=pname, scope=scope), pname or item_id, atype, "runtime:sql", query_id, False, loc)
    if kind == "pipeline":
        if not item_id:
            return None
        return Process(AssetRef.make("pipeline", {"olcf_item": item_id}, name=pname, scope=scope), pname or item_id, "pipeline", "runtime:pipeline", run_id, False, loc)
    fab = dsev.fabric_process_block(proc.get("fabric")) if kind in ("notebook", "job", "spark_job", "script") else None
    if fab is not None:
        fp = _fabric_process(fab, scope, loc, item_id, job_id or run_id)
        if fp is not None:
            return fp
    # notebook | spark_job | job | script ...
    if item_id:
        atype = "notebook" if kind == "notebook" else ("spark_job" if (kind == "spark_job" or engine == "spark") else "script")
        return Process(AssetRef.make(atype, {"olcf_item": item_id}, name=pname, scope=scope), pname or item_id, atype, "runtime:job", job_id or run_id, False, loc)
    if job_id:
        name = pname or job_id
        atype = "script" if engine and engine != "spark" else "spark_job"      # a python script run as a job is not a Spark job
        return Process(AssetRef.make(atype, {"source": f"olcf-job:{job_id}"}, name=name, scope=scope), name, atype, "runtime:job", job_id, True, loc,
                       {"engine": engine, "observed": "runtime"})
    return None


def _fabric_process(fab: dict[str, Any], scope: dict[str, str], loc: dict[str, Any], item_id: str | None, ref_id: str | None) -> Process | None:
    """The Fabric notebook a Fabric-originated run executed (``None`` when the run names neither the notebook's GUID nor its name).

    Tenant, workspace GUID and notebook GUID all known: the notebook is the asset with that ``fabric`` alias (what the tenant scan gives it), the edges are ``verified``. Otherwise,
    with only a name: a name reference scoped by the workspace (the registry matches a unique notebook of that name in the OLCF workspace the Fabric workspace is mapped to) and
    ``inferred`` edges. The asset is never named or described by the runtime (discovery owns it).
    """
    tenant, fws, fitem, fname = fab.get("tenant_id"), fab.get("workspace_id"), fab.get("item_id"), fab.get("item_name")
    fscope = {**scope, **({"fabric_workspace": fws} if fws else {}), **({"tenant": tenant} if tenant else {})}
    floc = {**loc, **{k: v for k, v in (("fabric_tenant", tenant), ("fabric_workspace", fws), ("fabric_item", fitem), ("fabric_run", fab.get("run_id"))) if v}}
    if tenant and fws and fitem:
        aliases = {"fabric": fabric_alias(tenant, fws, fitem), **({"olcf_item": item_id} if item_id else {})}
        return Process(AssetRef.make("notebook", aliases, name=fname, scope=fscope), fname or fitem, "notebook", "runtime:job", ref_id, False, floc, {}, "verified", tenant, fws)
    if fname:
        return Process(AssetRef.make("notebook", {"olcf_item": item_id} if item_id else {}, name=fname, scope=fscope), fname, "notebook", "runtime:job", ref_id, False,
                       {**floc, "attribution": "name"}, {}, "inferred", tenant, fws)
    return None


def _process_fact(p: Process, ws: str | None) -> list[AssetFact]:
    """The asset fact for a process the runtime owns; none for item-backed processes (discovery owns their names)."""
    if not p.owned:
        return []
    return [AssetFact(p.ref, p.name, attrs={k: v for k, v in p.attrs.items() if v is not None}, workspace_id=ws)]


# --------------------------------------------------------------------------------------------- dataset identity
def _host(uri: str) -> str:
    try:
        return (urlparse(uri).hostname or "").lower()
    except ValueError:
        return ""


def fabric_table_alias(tenant: str, workspace_guid: str, lakehouse_guid: str, table: str) -> str:
    """The ``fabric`` alias the tenant scan gives a lakehouse table: ``<tenant>/<workspace>/<lakehouse>/<normalised table name>`` (same spelling as the connector and the analyzers)."""
    return f"{fabric_alias(tenant, workspace_guid, lakehouse_guid)}/{normalize_qname(table)}"


def onelake_uri(workspace_guid: str, lakehouse_guid: str, rel: str) -> str:
    """``abfss://<workspace>@onelake.dfs.fabric.microsoft.com/<lakehouse>/<rel>`` - the location the tenant scan records for a table (``rel`` = ``Tables/<name>``)."""
    return f"abfss://{workspace_guid}@{_ONELAKE_HOST}/{lakehouse_guid}/{rel.strip('/')}"


def dataset_ref(ds: Any, workspace_id: str | None, tenant: str | None = None) -> AssetRef | None:
    """The asset reference of one dataset descriptor of an event (``{kind, lakehouse_id, name, path, uri}``); ``None`` when it names nothing.

    ``ds["fabric"]`` (``{workspace_id, lakehouse_id}`` GUIDs, set by the control plane when the dataset's lakehouse is attached to a Fabric lakehouse) adds the aliases the Fabric
    tenant scan gives the same table - the ``fabric`` alias (needs ``tenant``) and the ``onelake_path`` - ahead of the OLCF ones, so a table the scan already discovered wins.
    """
    if not isinstance(ds, dict):
        return None
    kind, lh = ds.get("kind"), _s(ds.get("lakehouse_id"))
    name, path, uri = _s(ds.get("name")), _s(ds.get("path")), _s(ds.get("uri"))
    fab = dsev.fabric_lakehouse_block(ds.get("fabric"))
    scope = {k: v for k, v in (("workspace_id", workspace_id), ("lakehouse_id", lh)) if v}
    if fab:
        scope.update({"fabric_workspace": fab["workspace_id"], "fabric_lakehouse": fab["lakehouse_id"], **({"tenant": tenant} if tenant else {})})
    if kind == "table":
        if not name:
            return None
        aliases: dict[str, str] = {}
        if fab:
            if tenant:
                aliases["fabric"] = fabric_table_alias(tenant, fab["workspace_id"], fab["lakehouse_id"], name)
            aliases["onelake_path"] = normalize_path(onelake_uri(fab["workspace_id"], fab["lakehouse_id"], "Tables/" + name.replace(".", "/")))
        if lh:
            aliases["lakehouse_table"] = f"{lh}:{name}"
        return AssetRef.make("table", aliases, name=name, scope=scope)
    if kind == "file":
        aliases = {}
        if uri:
            aliases["onelake_path" if "onelake" in _host(uri) else "delta_location"] = normalize_path(uri)
        elif fab and path:
            aliases["onelake_path"] = normalize_path(onelake_uri(fab["workspace_id"], fab["lakehouse_id"], path))
        if lh and path:
            aliases["source"] = f"olcf-file:{lh}:{path.strip('/')}"
        if not aliases and path:
            aliases["delta_location"] = normalize_path(path)
        if not aliases:
            return None
        label = posixpath.basename((path or uri or "").rstrip("/")) or (path or uri or "")
        return AssetRef.make("file", aliases, name=label, scope=scope)
    return None


# --------------------------------------------------------------------------------------------- dataset.read / dataset.written
def _cap(level: str, p: Process) -> str:
    """``level`` limited to what the process identity allows (a process matched by name makes every edge ``inferred`` at best)."""
    return level if p.confidence == "verified" else (p.confidence if stronger(level, p.confidence) == level else level)


def _evidence(p: Process, at: str | None, extra: dict[str, Any] | None = None) -> Evidence:
    return Evidence(kind="runtime", source=p.source, confidence=p.confidence, ref=p.ref_id, location={**p.location, **(extra or {})}, observed_at=at)


def _dataset_flow(payload: dict[str, Any], edge_type: str) -> Facts | None:
    p = _process(payload)
    datasets = payload.get("datasets")
    if p is None or not isinstance(datasets, list):
        return None
    ws, at = _s(payload.get("workspace_id")), _s(payload.get("at"))
    mode = _s(payload.get("mode"))
    facts = Facts(assets=_process_fact(p, ws))
    seen: set[AssetRef] = set()
    for d in datasets:
        ref = dataset_ref(d, ws, p.tenant)
        if ref is None or ref in seen:
            continue
        seen.add(ref)
        facts.edges.append(EdgeFact(src=p.ref, dst=ref, type=edge_type, evidence=[_evidence(p, at)], confidence=p.confidence,
                                    attrs={"mode": mode} if edge_type == "WRITES_TO" and mode else {}))
    return facts if facts.edges else None


# --------------------------------------------------------------------------------------------- dataset.statement
def _lift(confidence: str) -> str:
    """A static result seen executing: ``derived`` becomes ``verified``; guesses (``inferred``) and unknowns stay what they were."""
    return "verified" if confidence in ("verified", "derived") else confidence


def _fabric_table(ref: AssetRef, fab_lh: dict[str, str], tenant: str) -> AssetRef:
    """A table reference of an analysed statement that names its table with a bare name (so it lives in the default lakehouse) gets the Fabric aliases of that table;
    everything else (qualified names, columns, other types) is returned unchanged."""
    if ref.type != "table" or not ref.name or ref.alias_map.get("fabric") or "." in normalize_qname(ref.name):
        return ref
    extra = (("fabric", fabric_table_alias(tenant, fab_lh["workspace_id"], fab_lh["lakehouse_id"], ref.name)),
             ("onelake_path", normalize_path(onelake_uri(fab_lh["workspace_id"], fab_lh["lakehouse_id"], "Tables/" + ref.name))))
    return AssetRef(ref.type, (*extra, *ref.aliases), ref.name, ref.scope)


def _statement(payload: dict[str, Any]) -> Facts | None:
    sql = _s(payload.get("sql"))
    if sql is None:
        return None
    p = _process(payload, sql=sql)
    if p is None:
        return None
    ws, at = _s(payload.get("workspace_id")), _s(payload.get("at"))
    dialect = payload.get("dialect") if payload.get("dialect") in DIALECTS else "spark"
    default_lh = _s(payload.get("default_lakehouse_id"))
    fab_lh = dsev.fabric_lakehouse_block(payload.get("default_lakehouse_fabric")) if default_lh else None
    default_ctx: dict[str, Any] | None = {"id": default_lh, "name": None, **({"fabric_id": fab_lh["lakehouse_id"]} if fab_lh else {})} if default_lh else None
    context: dict[str, Any] = {"workspace_id": ws, "item_id": _s((payload.get("process") or {}).get("item_id")), "item_name": p.name, "item_type": p.type,
                               "default_lakehouse": default_ctx, "source": "olcf", "dialect": dialect, "source_id": RUNTIME_SOURCE}
    if p.tenant:
        context["tenant"] = p.tenant
    if p.fabric_workspace:
        context["fabric_workspace"] = p.fabric_workspace
    defn = DefinitionFact(kind="sql_query", ref=p.ref, name=p.name, payload={"sql": sql, "engine": dialect, "lakehouse_id": default_lh}, etag=None, context=context)
    facts = Facts(assets=_process_fact(p, ws))
    try:
        # lazy: the analyzers are a separate package that may be absent in a stripped deployment or still be loading
        analysis = importlib.import_module("olcf.intelligence.lineage.engine").analyze_definition(defn)
    except Exception as exc:  # noqa: BLE001 - an analyzer bug must not lose the event; it becomes data
        facts.gaps.append(Gap("parse_error", "runtime:statement", p.name, f"the statement could not be analysed: {type(exc).__name__}: {str(exc)[:200]}",
                              "The statement ran, but its tables and columns could not be derived; table-level reads and writes of the same run are still recorded from the dataset events.",
                              {k: v for k, v in p.location.items() if k in ("query_id", "job_id", "run_id")}))
        return facts
    for af in analysis.assets:
        if af.ref == p.ref and not p.owned:
            continue                      # discovery owns the name of an item-backed process; the analyzer would rename it to the job's name
        if fab_lh and p.tenant:           # the same table the edges below name: it must be found by the alias the tenant scan gave it, not made anew
            af.ref = _fabric_table(af.ref, fab_lh, p.tenant)
            af.parent = _fabric_table(af.parent, fab_lh, p.tenant) if af.parent is not None else None
        facts.assets.append(af)
    fab_loc = {k: v for k, v in p.location.items() if k.startswith("fabric_")}
    for ef in analysis.edges:
        ef.evidence = [Evidence(kind="runtime", source=p.source, confidence=_cap(_lift(ev.confidence), p), ref=p.ref_id, location={**ev.location, **fab_loc}, snippet=ev.snippet,
                                observed_at=at) for ev in ef.evidence] or [_evidence(p, at)]
        ef.confidence = _cap("verified" if ef.type in ("READS_FROM", "WRITES_TO") else _lift(ef.effective_confidence()), p)
        if fab_lh and p.tenant:
            ef.src, ef.dst = _fabric_table(ef.src, fab_lh, p.tenant), _fabric_table(ef.dst, fab_lh, p.tenant)
        facts.edges.append(ef)
    facts.gaps.extend(analysis.gaps)
    return facts


# --------------------------------------------------------------------------------------------- dataset.schema_changed
def _column_fact(table_ref: AssetRef, ws: str | None, lh: str, table: str, col: dict[str, Any], attrs: dict[str, Any]) -> AssetFact | None:
    name = _s(col.get("name"))
    if name is None:
        return None
    ref = AssetRef.make("column", {"lakehouse_column": f"{lh}:{table}:{name}"}, name=name, scope={k: v for k, v in (("workspace_id", ws), ("lakehouse_id", lh), ("table", table)) if v})
    return AssetFact(ref, name, attrs=attrs, parent=table_ref, workspace_id=ws)


def _schema_changed(payload: dict[str, Any]) -> Facts | None:
    ws, lh, table = _s(payload.get("workspace_id")), _s(payload.get("lakehouse_id")), _s(payload.get("table"))
    if lh is None or table is None:
        return None
    at = _s(payload.get("at"))
    added, removed, changed = (_list(payload.get(k)) for k in ("columns_added", "columns_removed", "columns_changed"))
    table_ref = AssetRef.make("table", {"lakehouse_table": f"{lh}:{table}"}, name=table, scope={k: v for k, v in (("workspace_id", ws), ("lakehouse_id", lh)) if v})
    attrs: dict[str, Any] = {}
    full = payload.get("columns")
    if isinstance(full, list):
        attrs["columns"] = [{"name": c.get("name"), "type": c.get("type"), "nullable": c.get("nullable", True)} for c in full[:_MAX_COLUMNS] if isinstance(c, dict) and c.get("name")]
    if at:
        attrs["schema_changed_at"] = at
    parent = AssetRef.make("lakehouse", {"olcf_item": lh}, scope={"workspace_id": ws} if ws else {})
    facts = Facts(assets=[AssetFact(table_ref, table, attrs=attrs, parent=parent, workspace_id=ws)])
    for col in added:
        if isinstance(col, dict):
            f = _column_fact(table_ref, ws, lh, table, col, {"type": col.get("type"), "nullable": col.get("nullable", True)})
            if f:
                facts.assets.append(f)
    for col in changed:
        if isinstance(col, dict):
            f = _column_fact(table_ref, ws, lh, table, col, {"type": col.get("to"), "nullable": col.get("nullable_to", True)})
            if f:
                facts.assets.append(f)
    for col in removed:
        if isinstance(col, dict):
            f = _column_fact(table_ref, ws, lh, table, col, {"type": col.get("type"), "dropped": True})
            if f:
                facts.assets.append(f)
    return facts


# --------------------------------------------------------------------------------------------- pipeline runs
def _pipeline_run(event_type: str, payload: dict[str, Any]) -> Facts | None:
    state = event_type.rsplit(".", 1)[-1]
    pipeline_id, name = _s(payload.get("pipeline_id")), _s(payload.get("pipeline"))
    if state not in _TERMINAL_PIPELINE or pipeline_id is None or name is None:
        return None
    ws = _s(payload.get("workspace_id"))
    ref = AssetRef.make("pipeline", {"olcf_item": pipeline_id}, name=name, scope={"workspace_id": ws} if ws else {})
    return Facts(assets=[AssetFact(ref, name, attrs={"last_run_state": state}, workspace_id=ws)])


# --------------------------------------------------------------------------------------------- entry point
def facts_from_event(event_type: str, payload: dict[str, Any]) -> Facts | None:
    """Translate one platform event into facts, or ``None`` when the event says nothing about lineage (unknown type, unusable payload, not a terminal state ...).

    Pure and deterministic: the same event always gives the same facts. ``dataset.statement`` calls the static SQL analyzer (imported on demand); everything else only
    reads the payload. See the module docstring for what each event type yields.
    """
    if not isinstance(payload, dict):
        return None
    if event_type == "dataset.read":
        return _dataset_flow(payload, "READS_FROM")
    if event_type == "dataset.written":
        return _dataset_flow(payload, "WRITES_TO")
    if event_type == "dataset.statement":
        return _statement(payload)
    if event_type == "dataset.schema_changed":
        return _schema_changed(payload)
    if event_type.startswith("pipeline.run."):
        return _pipeline_run(event_type, payload)
    return None   # job.state_changed (see the module docstring) and every other event type
