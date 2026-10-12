# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""The shared vocabulary of OLCF Intelligence: asset types, edge types, confidence levels, evidence and the *fact* records that connectors and analyzers produce.

This module is deliberately **pure**: it imports nothing from the rest of OLCF (no database, no FastAPI, no services). Discovery connectors and lineage
analyzers return the plain dataclasses defined here, and the core (:mod:`olcf.intelligence.engine`) is the only place that turns them into rows. That is what
keeps discovery and lineage separate systems that can be tested, replaced and extended independently (design note: "discovery collects everything available;
the lineage engine determines how those discovered objects relate").

Vocabulary
==========

*Asset*: anything the platform knows about: a workspace, lakehouse, table, column, notebook, pipeline, semantic model, measure, report, visual ... Every
asset has one **globally stable internal id** ``ast_<ULID>``. Everything else that names the same object (an OLCF item id ``itm_...``, a catalog entry id
``cat_...``, a Fabric GUID, a OneLake path, a physical Delta location, a ``lakehouse.table`` name) is an **alias** of that id. Compute, governance, lineage and
discovery all store and exchange the ``ast_`` id, never a private one.

*Edge*: a typed, directed relationship between two assets, with a **confidence** and a list of **evidence** records (who saw it, where, when).

Direction convention
--------------------
Edges are stored the way they read in English (``notebook WRITES_TO table``). Data-flow direction is *derived* with :func:`flow` so that traversal code never
has to special-case a type: for ``READS_FROM`` the data flows from the destination to the source, for ``WRITES_TO`` from source to destination, and so on.

Confidence
----------
``verified`` (observed at run time by the platform: a job, query or pipeline run really read or wrote the dataset, or a statement that really ran was parsed), ``derived``
(static analysis of code, SQL or a model definition, and authoritative definition bindings such as a pipeline's ``dependsOn`` or a report's model binding), ``inferred``
(suggested by names or other indirect evidence), ``unknown`` (a relationship exists but the precise transformation cannot be determined).
Only the run-time translator (``olcf.intelligence.runtime``) produces ``verified`` evidence; the static analyzers and the discovery connectors emit ``derived``, ``inferred``
and ``unknown``. An edge's confidence is the strongest of its evidence; it never claims more than its best evidence supports.
"""

from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass, field
from typing import Any

# --------------------------------------------------------------------------------------------- vocabulary
#: Every kind of asset. ``column`` assets are children of a ``table`` / ``view`` / ``model_table`` / ``visual`` (their ``parent`` link is a CONTAINS edge).
ASSET_TYPES: tuple[str, ...] = (
    "tenant", "workspace", "domain_scope",
    "lakehouse", "warehouse", "schema", "table", "view", "column", "file", "source_system",
    "notebook", "pipeline", "activity", "sql_query", "spark_job", "script", "dataflow",
    "semantic_model", "model_table", "measure", "report", "page", "visual", "dashboard",
    "external",
)
#: Types that *run* and move data between datasets (the nodes that sit between two datasets in a lineage picture).
PROCESS_TYPES: frozenset[str] = frozenset({"notebook", "pipeline", "activity", "sql_query", "spark_job", "script", "dataflow"})
#: Types that hold data.
DATASET_TYPES: frozenset[str] = frozenset({"table", "view", "file", "model_table", "source_system", "external"})
#: Types that present data to people.
CONSUMER_TYPES: frozenset[str] = frozenset({"semantic_model", "measure", "report", "page", "visual", "dashboard"})

EDGE_TYPES: tuple[str, ...] = (
    "CONTAINS", "READS_FROM", "WRITES_TO", "TRANSFORMS", "EXECUTES", "DEPENDS_ON", "USES_MEASURE", "DISPLAYS",
    # served from the governance tables through the same graph API ("virtual" edges, never stored in the edge table):
    "GOVERNED_BY", "CLASSIFIED_AS",
)
STORED_EDGE_TYPES: tuple[str, ...] = tuple(t for t in EDGE_TYPES if t not in ("GOVERNED_BY", "CLASSIFIED_AS"))
#: Edge types along which *data* flows (the others are structure or control).
FLOW_EDGE_TYPES: frozenset[str] = frozenset({"READS_FROM", "WRITES_TO", "TRANSFORMS", "DEPENDS_ON", "USES_MEASURE", "DISPLAYS"})

CONFIDENCE_LEVELS: tuple[str, ...] = ("verified", "derived", "inferred", "unknown")
_CONF_RANK = {c: i for i, c in enumerate(CONFIDENCE_LEVELS)}   # lower index = stronger

EVIDENCE_KINDS: tuple[str, ...] = ("runtime", "definition", "static", "metadata", "naming", "manual", "api")
GAP_KINDS: tuple[str, ...] = ("permission_denied", "api_unavailable", "unsupported_construct", "dynamic_reference", "parse_error", "not_inspected", "unresolved_reference", "sampling_disabled", "identity_conflict")


def stronger(a: str, b: str) -> str:
    """The stronger of two confidence levels (``verified`` beats ``derived`` beats ``inferred`` beats ``unknown``)."""
    return a if _CONF_RANK[a] <= _CONF_RANK[b] else b


def weaker_or_equal(level: str, floor: str) -> bool:
    """True if ``level`` is at least as strong as ``floor`` (used for the ``min_confidence`` filter: ``derived`` keeps verified and derived edges)."""
    return _CONF_RANK[level] <= _CONF_RANK[floor]


def flow(edge_type: str, src: str, dst: str) -> tuple[str, str] | None:
    """Direction of *data* for an edge, as ``(from_asset, to_asset)``; ``None`` when the edge carries no data (CONTAINS, EXECUTES, GOVERNED_BY, CLASSIFIED_AS).

    ``a READS_FROM b``: b -> a.  ``a WRITES_TO b``: a -> b.  ``a TRANSFORMS b``: a -> b (column to column).  ``a DEPENDS_ON b``: b -> a.
    ``a USES_MEASURE b``: b -> a.  ``a DISPLAYS b``: b -> a (the measure or field flows into the visual).
    """
    if edge_type in ("WRITES_TO", "TRANSFORMS"):
        return src, dst
    if edge_type in ("READS_FROM", "DEPENDS_ON", "USES_MEASURE", "DISPLAYS"):
        return dst, src
    return None


# --------------------------------------------------------------------------------------------- aliases and references
#: Alias kinds. An alias is ``(kind, value)``; ``(kind, value)`` is unique across all assets, so two sources that name the same object end up on one asset.
ALIAS_KINDS: tuple[str, ...] = (
    "olcf_item",          # itm_<ULID>
    "olcf_workspace",     # wks_<ULID>
    "catalog_entry",      # cat_<ULID>
    "lakehouse_table",    # "<lakehouse item id>:<table name>"
    "lakehouse_column",   # "<lakehouse item id>:<table name>:<column>"
    "fabric",             # "<tenant or *>/<fabric workspace guid>/<item guid>" (lower-case); workspace-level "<tenant>/<ws guid>"
    "fabric_column",      # "<fabric table alias>:<column>"
    "onelake_path",       # normalised OneLake / ADLS path, see normalize_path()
    "delta_location",     # normalised physical storage location, see normalize_path()
    "qname",              # normalised qualified name, see normalize_qname()
    "pbi",                # Power BI object: "<workspace guid>/<dataset or report guid>[/<table>[/<column|measure>]]"
    "query",              # "sha1:<hash>" of a normalised ad-hoc statement
    "source",             # "<source id>:<native id>" for objects a connector can only name
)

_GUID = re.compile(r"[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}")


def normalize_qname(name: str) -> str:
    """Normalise a qualified table/column name for alias matching: unquote, strip back-ticks / brackets / double quotes, collapse dots, lower-case.

    ``"[Silver].[Sales]"`` / ```Silver`.`Sales``` / ``silver.sales`` all give ``silver.sales``. Unqualified names stay unqualified; resolving them against a default
    lakehouse is the registry's job, not this function's.
    """
    parts = [p.strip().strip('`"[]').strip() for p in re.split(r"\.(?=(?:[^`\"\]]*[`\"\[][^`\"\]]*[`\"\]])*[^`\"\]]*$)", name.strip())]
    return ".".join(p.lower() for p in parts if p)


def normalize_path(uri: str) -> str:
    """Normalise a storage location for alias matching: lower-case scheme and host, no trailing slash, ``%20`` decoded, OneLake GUID/name host forms kept as written.

    ``abfss://WS@onelake.dfs.fabric.microsoft.com/LH/Tables/Sales/`` -> ``abfss://ws@onelake.dfs.fabric.microsoft.com/lh/tables/sales``. Path components are lower-cased
    because OneLake and ADLS Gen2 paths are case-insensitive for the names used here; local file paths keep their case (POSIX paths are case-sensitive).
    """
    u = uri.strip().replace("\\", "/")
    m = re.match(r"^([a-zA-Z][a-zA-Z0-9+.-]*)://([^/]*)(/.*)?$", u)
    if not m:
        return u.rstrip("/") or "/"
    scheme, host, rest = m.group(1).lower(), m.group(2).lower(), (m.group(3) or "")
    rest = rest.replace("%20", " ").rstrip("/")
    if scheme in ("file", "local"):
        return f"{scheme}://{host}{rest}"
    return f"{scheme}://{host}{rest.lower()}"


def fabric_alias(tenant: str | None, workspace_guid: str, item_guid: str | None = None) -> str:
    """The ``fabric`` alias value for a Fabric workspace or item: ``<tenant>/<workspace>[/<item>]``, GUIDs lower-cased, tenant ``*`` if unknown."""
    parts = [(tenant or "*").lower(), workspace_guid.lower()] + ([item_guid.lower()] if item_guid else [])
    return "/".join(parts)


@dataclass(frozen=True)
class AssetRef:
    """A reference to an asset that has *not* been resolved to an ``ast_`` id yet.

    Connectors and analyzers only know the names and ids their own source uses, so they hand the core a reference: the asset ``type``, any number of
    ``aliases`` (``{alias_kind: value}``) and, for references found in code ("``spark.read.table('Silver.Sales')``"), a ``name`` plus a ``scope`` that says how
    to resolve a partial name (``{"workspace_id": ..., "lakehouse_id": ..., "lakehouse": ..., "tenant": ..., "fabric_workspace": ...}``). The core's
    ``AssetRegistry.resolve`` rules, in order: (1) any alias hit; (2) ``name`` qualified with the scope's default lakehouse; (3) a unique ``qname`` match across
    the scope's workspace; (4) otherwise a **placeholder** asset (``attrs.unresolved = true``) keyed by its qname alias, which is merged into the real asset the
    moment a discovery scan finds one. Edges to a placeholder are kept (and reported as an ``unresolved_reference`` gap) instead of being dropped.
    """

    type: str
    aliases: tuple[tuple[str, str], ...] = ()
    name: str | None = None
    scope: tuple[tuple[str, str], ...] = ()

    @staticmethod
    def make(type: str, aliases: dict[str, str] | None = None, name: str | None = None, scope: dict[str, str] | None = None) -> AssetRef:
        """Build a reference from plain dicts (dataclass fields are tuples so the reference is hashable)."""
        return AssetRef(type, tuple(sorted((aliases or {}).items())), name, tuple(sorted((scope or {}).items())))

    @property
    def alias_map(self) -> dict[str, str]:
        """The aliases as a dict."""
        return dict(self.aliases)

    @property
    def scope_map(self) -> dict[str, str]:
        """The resolution scope as a dict."""
        return dict(self.scope)


@dataclass
class Evidence:
    """One observation supporting an edge: who saw it, where in the source, and when. Evidence never contains secrets or data values."""

    kind: str                              # one of EVIDENCE_KINDS
    source: str                            # connector / analyzer / runtime emitter name, e.g. "pyspark-analyzer", "runtime:job", "fabric-scanner"
    confidence: str = "derived"            # the confidence this single observation justifies
    ref: str | None = None                 # job id, scan id, definition etag, query id ...
    location: dict[str, Any] = field(default_factory=dict)   # {"item": "itm_..", "cell": 3, "line": 12} or {"part": "model.bim", "path": "tables[2].measures[0]"}
    snippet: str | None = None             # the code / expression, truncated to 400 chars
    observed_at: str | None = None         # ISO-8601 UTC; the engine fills it in when absent

    def key(self) -> tuple[str, str, str | None, str]:
        """Identity used to de-duplicate evidence on an edge."""
        return (self.kind, self.source, self.ref, repr(sorted(self.location.items())))


@dataclass
class AssetFact:
    """An asset a connector or analyzer saw, with its attributes and its parent (for the CONTAINS tree)."""

    ref: AssetRef
    name: str
    attrs: dict[str, Any] = field(default_factory=dict)      # type-specific attributes: columns, format, row_count, expression, definition_etag ...
    parent: AssetRef | None = None
    description: str | None = None
    workspace_id: str | None = None        # the OLCF workspace the asset belongs to (None: external / tenant level)
    source_id: str | None = None           # set by the engine from the scan, connectors need not fill it
    path: str | None = None                # human path, e.g. "Sample Retail / retail_lakehouse / fact_sales"
    tags: list[str] = field(default_factory=list)


@dataclass
class EdgeFact:
    """A relationship between two asset references, with its evidence. ``via`` names the process asset for TRANSFORMS (which notebook / query produced the mapping)."""

    src: AssetRef
    dst: AssetRef
    type: str
    evidence: list[Evidence] = field(default_factory=list)
    confidence: str | None = None          # default: the strongest evidence confidence
    transformation: str | None = None      # e.g. "GrossRevenue - Discount"
    via: AssetRef | None = None
    attrs: dict[str, Any] = field(default_factory=dict)

    def effective_confidence(self) -> str:
        """The stated confidence, or the strongest evidence confidence, or ``unknown``."""
        if self.confidence:
            return self.confidence
        best = "unknown"
        for ev in self.evidence:
            best = stronger(best, ev.confidence) if best != "unknown" or ev.confidence != "unknown" else best
        return best


@dataclass
class Gap:
    """Something discovery or analysis could not do. Gaps are first-class output: the platform reports them instead of pretending to complete coverage."""

    kind: str                              # one of GAP_KINDS
    source: str                            # connector or analyzer
    subject: str                           # what it was about: an asset name, an item id, an API path
    detail: str
    remediation: str | None = None
    location: dict[str, Any] = field(default_factory=dict)


@dataclass
class Facts:
    """The complete output of one connector run or one analysis: assets, edges and gaps (all unresolved references)."""

    assets: list[AssetFact] = field(default_factory=list)
    edges: list[EdgeFact] = field(default_factory=list)
    gaps: list[Gap] = field(default_factory=list)

    def extend(self, other: Facts) -> Facts:
        """Append another result in place and return self."""
        self.assets.extend(other.assets)
        self.edges.extend(other.edges)
        self.gaps.extend(other.gaps)
        return self


# --------------------------------------------------------------------------------------------- helpers
def edge_hash(src_id: str, dst_id: str, edge_type: str, via_id: str | None = None, attrs_key: str | None = None) -> str:
    """Stable identity of a stored edge: sha256 over (source asset id, destination asset id, type, process asset id, optional discriminator)."""
    raw = "\x1f".join([src_id, dst_id, edge_type, via_id or "", attrs_key or ""])
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def clip(text: str | None, limit: int = 400) -> str | None:
    """Truncate a code or expression snippet for evidence."""
    if text is None:
        return None
    t = " ".join(text.split())
    return t if len(t) <= limit else t[: limit - 1] + "…"


def looks_like_guid(value: str) -> bool:
    """True if ``value`` is a GUID (Fabric / Power BI object ids)."""
    return bool(_GUID.fullmatch(value.strip()))

#: Kinds of definition a connector can hand to the lineage engine for analysis (the analyzers live in ``olcf.intelligence.lineage``).
DEFINITION_KINDS: tuple[str, ...] = (
    "notebook",            # payload: nbformat dict (OLCF or Fabric notebook converted to ipynb)
    "pipeline_olcf",       # payload: olcf.pipeline/1 definition dict
    "pipeline_fabric",     # payload: Fabric DataPipeline content (pipeline-content.json) dict
    "sql_query",           # payload: {"sql": str, "engine": str, "lakehouse_id": str|None}
    "report_olcf",         # payload: OLCF report (dashboard) definition dict with tiles
    "spark_job_olcf",      # payload: spark_job_definition dict (main file / class, args)
    "semantic_model",      # payload: {part path: text} (TMDL files and/or model.bim / definition.pbism)
    "report_pbi",          # payload: {part path: text} (PBIR definition/*.json or legacy report.json)
    "scanner_result",      # payload: Power BI admin scanner "scanResult" JSON (or an exported copy of it)
)


@dataclass
class DefinitionFact:
    """A stored definition (notebook code, pipeline JSON, model parts ...) that the lineage engine should analyze.

    ``etag`` is the source's version marker (item etag, Fabric definition hash, content hash); the engine skips analysis when the etag is unchanged since the last
    scan, which is what makes incremental scans cheap. ``context`` is plain JSON: ``workspace_id``, ``item_id``, ``item_name``, ``item_type``, ``default_lakehouse``
    (``{"id", "name"}``), ``tenant``, ``fabric_workspace`` (guid), ``fabric_workspace_name``, ``source`` (``"olcf"`` or ``"fabric"``), ``dialect`` (``spark`` | ``duckdb`` |
    ``tsql``), ``source_id``.
    """

    kind: str
    ref: AssetRef
    name: str
    payload: Any
    etag: str | None = None
    context: dict[str, Any] = field(default_factory=dict)

