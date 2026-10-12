# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Shared plumbing of the lineage analyzers: the analysis context, reference builders, path classification and the :class:`FactBuilder`.

Design rules every analyzer follows
-----------------------------------
* **Pure and deterministic.** No I/O, no clocks, no randomness; sets are sorted before they are iterated, so the same definition always produces byte-identical
  :class:`~olcf.intelligence.model.Facts`.
* **Honest.** Every edge carries :class:`~olcf.intelligence.model.Evidence` whose ``source`` is ``"<analyzer>@<ANALYZER_VERSION>"``. Analyzers never
  emit ``verified``: that level means *observed at run time by the platform* and belongs to the run-time translator (``olcf.intelligence.runtime``). ``derived`` is
  used for parsed code and for authoritative declarations (pipeline ``dependsOn``, a report's model binding, a TMDL partition naming its entity), ``inferred`` for
  name- or default-parameter-based guesses and ``unknown`` for "a dependency exists but the mapping cannot be determined".
  Whatever cannot be resolved becomes a :class:`~olcf.intelligence.model.Gap`, never a guess presented as fact.
* **Bounded.** Hard caps (:data:`MAX_EDGES`, :data:`MAX_SQL_CHARS`, ...) keep a hostile or enormous definition from exhausting memory or time; hitting one
  records a gap saying what was skipped.
* **No secrets in facts.** Evidence snippets, gap texts, asset names / attributes and URI-shaped aliases pass through :func:`redact_secrets` / :func:`strip_uri_secrets`:
  ``password=``, ``token``, ``sig`` (SAS), ``Bearer`` values and the password of ``scheme://user:password@host`` never reach the graph.

References
----------
Datasets are referenced the way the registry resolves them (see ``docs/dev/INTELLIGENCE.md`` section 3): ``AssetRef.make("table", name=..., scope=...)`` with the
default lakehouse in the scope; columns are ``AssetRef.make("column", name=<column>, scope={..., "table": <normalised table qname>})``.
"""

from __future__ import annotations

import hashlib
import re
from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field, replace
from functools import lru_cache
from typing import Any

from olcf.intelligence.model import (
    CONFIDENCE_LEVELS,
    AssetFact,
    AssetRef,
    EdgeFact,
    Evidence,
    Facts,
    Gap,
    clip,
    fabric_alias,
    looks_like_guid,
    normalize_path,
    normalize_qname,
    stronger,
)

#: Bump when the output of any analyzer changes (the engine re-analyzes definitions whose cached analysis used an older version).
ANALYZER_VERSION = "1"

# ---- secrets -------------------------------------------------------------------------------------------------------------------------------------
_CRED_KEYS = (r"(?:password|passwd|pwd|pass|secret|client[_-]?secret|account[_-]?key|shared[_-]?access[_-]?(?:key|signature)|access[_-]?key|secret[_-]?(?:access[_-]?)?key|"
                r"api[_-]?key|apikey|auth[_-]?token|access[_-]?token|refresh[_-]?token|id[_-]?token|sas[_-]?token|token|sig|signature|private[_-]?key|authorization)")
_CRED_VALUE = r"\"[^\"]*\"|'[^']*'|[^\s;&,'\")\]\}]+"
#: ``password=hunter2`` / ``password = "x y"`` / ``"password": "x"`` / ``?sig=...`` (a qualified ``t.password = ...`` is a column, not a credential)
_SECRET_KV = re.compile(rf"(?<![\w.])((?P<q>[\"']?){_CRED_KEYS}(?P=q)\s*(?:=+|(?<=[\"']):)\s*)({_CRED_VALUE})", re.IGNORECASE)
#: ``.option("password", "hunter2")`` / ``("token", "x")`` / ``spark.conf.set("fs.azure.account.key.<acct>.dfs.core.windows.net", "<key>")``
_SECRET_PAIR = re.compile(rf"([\"'](?:{_CRED_KEYS}|[^\"'\s]*(?:account\.key|\.sas\.|secret|password|credential|access[._]key|connection[._]?string)[^\"'\s]*)[\"']\s*,\s*)([\"'])[^\"']*\2", re.IGNORECASE)
_URI_USERINFO = re.compile(r"([A-Za-z][A-Za-z0-9+.\-]*://)[^/\s:@'\"?#]+:[^/\s@'\"?#]*@")
_BEARER = re.compile(r"\b(bearer|basic)\s+[A-Za-z0-9._~+/=\-]{8,}", re.IGNORECASE)
_AWS_KEY = re.compile(r"\bA[KS]IA[0-9A-Z]{16}\b")


def redact_secrets(text: str | None) -> str | None:
    """Replace credential-looking values in ``text`` by ``***`` so that evidence, gaps and asset attributes never carry secrets.

    Handled: ``password=...`` / ``pwd`` / ``secret`` / ``token`` / ``api_key`` / ``account_key`` / ``sig`` ... as ``key=value``, ``"key": "value"`` and ``("key", "value")`` pairs
    (connection strings, ``.option("password", ...)``, SAS query strings, M record fields), the password of ``scheme://user:password@host``, ``Bearer`` / ``Basic`` tokens and
    AWS access key ids. Anything else is returned unchanged, so ordinary code and SQL keep their full text.
    """
    if not text:
        return text
    return _redact_cached(text)


@lru_cache(maxsize=1024)
def _redact_cached(text: str) -> str:
    """The work of :func:`redact_secrets`, memoised: one statement text is the snippet of every edge it produces."""
    out = _SECRET_KV.sub(lambda m: m.group(1) + (m.group(3)[0] + "***" + m.group(3)[0] if m.group(3)[0] in ("\"", "'") else "***"), text)
    out = _SECRET_PAIR.sub(lambda m: f"{m.group(1)}{m.group(2)}***{m.group(2)}", out)
    out = _URI_USERINFO.sub(lambda m: m.group(1) + "***@", out)
    out = _BEARER.sub(lambda m: m.group(1) + " ***", out)
    return _AWS_KEY.sub("***", out)


def strip_uri_secrets(path: str) -> str:
    """The URI without credentials: ``scheme://user:password@host/p?query#frag`` becomes ``scheme://host/p`` (a user name without a colon, as in ``abfss://container@account...``, is kept)."""
    m = re.match(r"^([A-Za-z][A-Za-z0-9+.\-]*://)((?:<\?>|[^/?#])*)(.*)$", path, re.DOTALL)  # "<?>" is the analyzers' marker of a value only known at run time
    if not m:
        return path
    scheme, authority, rest = m.groups()
    user, at, host = authority.rpartition("@")
    if at and ":" in user:
        authority = host
    return scheme + authority + re.split(r"(?<!<)\?(?!>)|#", rest, maxsplit=1)[0]


def _redact_value(value: Any) -> Any:
    """:func:`redact_secrets` applied to a string, or to the strings inside a list / dict."""
    if isinstance(value, str):
        return redact_secrets(value)
    if isinstance(value, list):
        return [_redact_value(v) for v in value]
    if isinstance(value, dict):
        return {k: _redact_value(v) for k, v in value.items()}
    return value


# ---- hard caps ------------------------------------------------------------------------------------------------------------------------------------
MAX_EDGES = 200_000            # edges per analysis
MAX_GAPS = 5_000               # gaps per analysis (after de-duplication)
MAX_EVIDENCE = 20              # evidence records kept per edge (matches the graph store)
MAX_SQL_CHARS = 2_000_000      # SQL text analysed per call
MAX_STATEMENTS = 5_000         # statements per script
MAX_PY_CHARS = 8_000_000       # python source analysed per call
MAX_CELLS = 6_000              # notebook cells analysed (formats.MAX_CELLS is 5000)
MAX_JSON_BYTES = 64_000_000    # serialised definition size accepted by the JSON based analyzers
MAX_PARTS = 5_000              # parts of a model / report definition
MAX_DEPTH = 40                 # nesting depth of JSON / expression walks
SNIPPET = 400                  # evidence snippet length (model.clip default)

#: Analyzer names used in evidence (``name@version``) and gaps (``name``).
SQL = "sql-analyzer"
PYSPARK = "pyspark-analyzer"
NOTEBOOK = "notebook-analyzer"
PIPELINE_OLCF = "olcf-pipeline-analyzer"
PIPELINE_FABRIC = "fabric-pipeline-analyzer"
TMDL = "tmdl-analyzer"
SEMANTIC_MODEL = "semantic-model-analyzer"
DAX = "dax-analyzer"
MCODE = "mcode-analyzer"
REPORT_PBI = "pbi-report-analyzer"
REPORT_OLCF = "olcf-report-analyzer"
SCANNER = "pbi-scanner-analyzer"
SPARK_JOB = "spark-job-analyzer"
ENGINE = "lineage-engine"

_CONF_ORDER = {c: i for i, c in enumerate(CONFIDENCE_LEVELS)}


def evidence_source(analyzer: str) -> str:
    """The ``Evidence.source`` string of ``analyzer``: ``"<name>@<ANALYZER_VERSION>"``."""
    return f"{analyzer}@{ANALYZER_VERSION}"


def weaker(a: str, b: str) -> str:
    """The weaker of two confidence levels (``unknown`` is weaker than ``inferred`` is weaker than ``derived`` is weaker than ``verified``)."""
    return a if _CONF_ORDER[a] >= _CONF_ORDER[b] else b


# =================================================================================================================================== context
@dataclass
class LineageContext:
    """What an analyzer may know about the definition it is looking at (all plain data, mirrors ``DefinitionFact.context``).

    ``default_lakehouse`` is ``{"id": <OLCF lakehouse item id>, "name": <lakehouse name>, "fabric_id": <Fabric lakehouse GUID>}`` (any key may be missing); ``schemas`` maps a lower-case
    qualified table name (``"retail_lakehouse.fact_sales"``, or just ``"fact_sales"``) to its column names and is the only thing that allows ``SELECT *`` to be
    expanded; ``process`` is the asset that performs the work when the caller already has one (the notebook, the pipeline activity, the saved query);
    ``location`` is merged into every evidence location (``{"item": "itm_x"}``); ``semantic_models`` maps lower-case semantic model names to their item GUIDs in the same workspace
    (lets the report analyzer resolve a ``byPath`` dataset reference); ``parameters`` overrides the values of a notebook's parameters cell (a pipeline or job run that passes
    ``table = "silver.orders"`` can be analysed with that value); ``known_notebooks`` maps notebook names to their item ids so that ``%run`` / ``notebookutils.notebook.run`` and
    pipeline notebook activities can point at the exact asset.
    """

    workspace_id: str | None = None
    item_id: str | None = None
    item_name: str | None = None
    item_type: str | None = None
    default_lakehouse: dict[str, Any] | None = None
    tenant: str | None = None
    fabric_workspace: str | None = None
    fabric_workspace_name: str | None = None
    source: str = "olcf"
    dialect: str = "spark"
    source_id: str | None = None
    schemas: dict[str, list[str]] = field(default_factory=dict)
    process: AssetRef | None = None
    location: dict[str, Any] = field(default_factory=dict)
    semantic_models: dict[str, str] = field(default_factory=dict)
    parameters: dict[str, Any] = field(default_factory=dict)
    known_notebooks: dict[str, str] = field(default_factory=dict)

    @classmethod
    def coerce(cls, obj: Any) -> LineageContext:
        """Accept a context object of any subclass, a plain ``dict`` (``DefinitionFact.context``) or ``None``; never raises."""
        if isinstance(obj, cls):
            return obj
        out = cls()
        data: Mapping[str, Any] = obj if isinstance(obj, Mapping) else {}
        out._load(data)
        return out

    def _load(self, data: Mapping[str, Any]) -> None:
        """Copy the known keys of a plain context mapping, ignoring values of the wrong type."""
        for key in ("workspace_id", "item_id", "item_name", "item_type", "tenant", "fabric_workspace", "fabric_workspace_name", "source_id"):
            val = data.get(key)
            if isinstance(val, str) and val:
                setattr(self, key, val)
        if isinstance(data.get("source"), str):
            self.source = data["source"]
        if isinstance(data.get("dialect"), str) and data["dialect"]:
            self.dialect = data["dialect"]
        lh = data.get("default_lakehouse")
        if isinstance(lh, Mapping):
            self.default_lakehouse = {str(k): v for k, v in lh.items() if isinstance(v, str) and v}
        elif isinstance(lh, str) and lh:
            self.default_lakehouse = {"name": lh}
        models = data.get("semantic_models")
        if isinstance(models, Mapping):
            self.semantic_models = {str(k).lower(): str(v).lower() for k, v in models.items() if isinstance(v, str) and v}
        params = data.get("parameters")
        if isinstance(params, Mapping):
            self.parameters = {str(k): v for k, v in params.items()}
        known = data.get("known_notebooks")
        if isinstance(known, Mapping):
            self.known_notebooks = {str(k): str(v) for k, v in known.items() if isinstance(v, str) and v}
        schemas = data.get("schemas")
        if isinstance(schemas, Mapping):
            self.schemas = {str(k).lower(): [str(c) for c in v] for k, v in schemas.items() if isinstance(v, (list, tuple))}

    @classmethod
    def from_definition(cls, defn: Any) -> LineageContext:
        """Build the context for a :class:`~olcf.intelligence.model.DefinitionFact` (the process is the definition's own asset)."""
        out = cls.coerce(getattr(defn, "context", None) or {})
        out.process = getattr(defn, "ref", None)
        if not out.item_name:
            out.item_name = getattr(defn, "name", None)
        if not out.item_id:
            out.item_id = out.process.alias_map.get("olcf_item") if out.process is not None else None
        return out

    def scope(self, **overrides: str | None) -> dict[str, str]:
        """The resolution scope placed on dataset references: default lakehouse, workspace, tenant and Fabric workspace (``None`` values are dropped)."""
        lh = self.default_lakehouse or {}
        raw: dict[str, str | None] = {
            "workspace_id": self.workspace_id, "lakehouse_id": lh.get("id"), "lakehouse": lh.get("name"),
            "tenant": self.tenant, "fabric_workspace": self.fabric_workspace, "fabric_lakehouse": lh.get("fabric_id"),
        }
        raw.update(overrides)
        return {k: v for k, v in raw.items() if v}

    def loc(self, **parts: Any) -> dict[str, Any]:
        """An evidence location: the context's own ``location`` (and item id) plus ``parts`` (``cell``, ``line``, ``path`` ...)."""
        out: dict[str, Any] = dict(self.location)
        if self.item_id and "item" not in out:
            out["item"] = self.item_id
        out.update({k: v for k, v in parts.items() if v is not None})
        return out

    def lookup_schema(self, table_name: str) -> list[str] | None:
        """Column names for ``table_name`` from :attr:`schemas`, or ``None``.

        Tries the normalised name, and for a one-part name also ``<default lakehouse>.<name>``; for ``<default lakehouse>.<name>`` also the bare name. It never
        strips a different schema or lakehouse, because ``bronze.customers`` and ``silver.customers`` are different tables.
        """
        if not self.schemas:
            return None
        q = normalize_qname(table_name)
        if q in self.schemas:
            return self.schemas[q]
        lh = str((self.default_lakehouse or {}).get("name") or "").lower()
        if lh:
            if "." not in q and f"{lh}.{q}" in self.schemas:
                return self.schemas[f"{lh}.{q}"]
            if q.startswith(lh + ".") and q.count(".") == 1 and q[len(lh) + 1:] in self.schemas:
                return self.schemas[q[len(lh) + 1:]]
        return None


@dataclass
class SqlContext(LineageContext):
    """Context for :func:`olcf.intelligence.lineage.sql.analyze_sql` (adds nothing to :class:`LineageContext`; the type documents the entry point)."""


@dataclass
class PyContext(LineageContext):
    """Context for :func:`olcf.intelligence.lineage.pyspark.analyze_python` and the notebook analyzer (adds nothing to :class:`LineageContext`, whose ``parameters`` and
    ``known_notebooks`` fields it uses; the type documents the entry point)."""


# =================================================================================================================================== references
def table_ref(name: str, ctx: LineageContext, **scope_overrides: str | None) -> AssetRef:
    """Reference to a table (or view) by the name used in code; partial names are resolved by the registry against the default lakehouse in the scope."""
    return AssetRef.make("table", name=strip_quotes(name), scope=ctx.scope(**scope_overrides))


def fabric_table_ref(ctx: LineageContext, workspace_guid: str, item_guid: str, name: str) -> AssetRef:
    """Reference to a table of a Fabric lakehouse / warehouse identified by GUIDs: carries the ``fabric`` table alias and a scope with the Fabric ids (no OLCF lakehouse)."""
    alias = fabric_table_alias(ctx.tenant, workspace_guid, item_guid, name)
    scope = ctx.scope(fabric_workspace=workspace_guid.lower(), fabric_lakehouse=item_guid.lower(), lakehouse=None, lakehouse_id=None)
    return AssetRef.make("table", aliases={"fabric": alias}, name=name, scope=scope)


def ctx_for_lakehouse(ctx: LineageContext, lakehouse: str | None, *, workspace_guid: str | None = None, fabric_lakehouse: str | None = None) -> LineageContext:
    """A copy of ``ctx`` whose default lakehouse is ``lakehouse`` (an OLCF lakehouse item id ``itm_...``, a name, or ``None`` for no default).

    ``fabric_lakehouse`` / ``workspace_guid`` (Fabric GUIDs) are put into the scope so that references can be matched with Fabric-discovered tables.
    """
    out = replace(ctx)
    lh: dict[str, Any] = {}
    if lakehouse:
        if lakehouse.startswith("itm_"):
            lh["id"] = lakehouse
        else:
            lh["name"] = lakehouse
    if fabric_lakehouse:
        lh["fabric_id"] = fabric_lakehouse.lower()
    out.default_lakehouse = lh or None
    if workspace_guid:
        out.fabric_workspace = workspace_guid.lower()
    return out


def view_ref(name: str, ctx: LineageContext, **scope_overrides: str | None) -> AssetRef:
    """Reference to a persistent view by name."""
    return AssetRef.make("view", name=strip_quotes(name), scope=ctx.scope(**scope_overrides))


def column_ref(table: AssetRef, column: str) -> AssetRef:
    """Reference to ``column`` of the table ``table`` (a :func:`table_ref` / :func:`path_ref` result): ``scope["table"]`` carries the normalised table name.

    A table that is only known by a physical location (an ``onelake_path`` alias) carries that alias in ``scope["table_alias"]`` as ``<kind>=<value>`` so that
    the registry can resolve it the same way.
    """
    scope = dict(table.scope_map)
    scope["table"] = normalize_qname(table.name) if table.name else ""
    if not scope["table"]:
        scope.pop("table")
    for kind, value in table.aliases:
        scope["table_alias"] = f"{kind}={value}"
        break
    return AssetRef.make("column", name=column, scope=scope)


def ref_key(ref: AssetRef) -> tuple[Any, ...]:
    """Case-insensitive identity of a reference inside one analysis (two spellings of one table or column collapse)."""
    name = None
    if ref.name:
        name = normalize_qname(ref.name) if ref.type in ("table", "view", "model_table") else ref.name.lower()
    scope = tuple((k, v.lower() if k == "table" else v) for k, v in ref.scope)
    return (ref.type, ref.aliases, name, scope)


def strip_quotes(name: str) -> str:
    """Remove back-tick / bracket / double-quote quoting from a dotted name, keeping the case: ``"[dbo].[Sales]"`` -> ``dbo.Sales``."""
    parts = [p.strip().strip('`"[]').strip() for p in re.split(r"\.(?=(?:[^`\"\]]*[`\"\[][^`\"\]]*[`\"\]])*[^`\"\]]*$)", name.strip())]
    return ".".join(p for p in parts if p)


def statement_alias(text: str) -> str:
    """The ``query`` alias value for a statement: ``sha1:<hash>`` of its whitespace-normalised text."""
    norm = " ".join(text.split())
    return "sha1:" + hashlib.sha1(norm.encode("utf-8"), usedforsecurity=False).hexdigest()  # noqa: S324 - identifier, not security


def query_ref(normalised_sql: str) -> AssetRef:
    """The process reference of an ad-hoc statement (``sql_query`` keyed by the ``query`` alias)."""
    return AssetRef.make("sql_query", aliases={"query": statement_alias(normalised_sql)})


def notebook_ref(ctx: LineageContext) -> AssetRef:
    """The notebook asset of ``ctx``: the supplied process reference or the ``olcf_item`` alias."""
    if ctx.process is not None:
        return ctx.process
    return AssetRef.make("notebook", aliases={"olcf_item": ctx.item_id} if ctx.item_id else None, name=ctx.item_name, scope=ctx.scope())


def fabric_table_alias(tenant: str | None, workspace_guid: str, lakehouse_guid: str, table: str) -> str:
    """The ``fabric`` alias of a lakehouse table: ``<tenant>/<workspace>/<lakehouse>/<normalised table name>`` (GUIDs lower-cased).

    Analyzers that read Fabric definitions (pipelines, scanner results) use this so that two analyzers that see the same table agree on its alias.
    """
    return f"{fabric_alias(tenant, workspace_guid, lakehouse_guid)}/{normalize_qname(table)}"


def pbi_alias(workspace_guid: str, dataset_guid: str, table: str | None = None, obj: str | None = None) -> str:
    """The ``pbi`` alias of a semantic model object: ``<workspace guid>/<dataset guid>[/<table>[/<column or measure>]]``, everything lower-cased, ``/`` in names escaped as ``%2f``.

    The semantic-model analyzer, the PBIR report analyzer and the scanner analyzer all build model-object aliases with this function so that they agree.
    """
    parts = [workspace_guid.strip().lower(), dataset_guid.strip().lower()]
    if table is not None:
        parts.append(table.strip().lower().replace("/", "%2f"))
        if obj is not None:
            parts.append(obj.strip().lower().replace("/", "%2f"))
    return "/".join(parts)


def fabric_column_alias(table_alias: str, column: str) -> str:
    """The ``fabric_column`` alias value: ``<fabric table alias>:<column lower-cased>``."""
    return f"{table_alias}:{column.lower()}"


# =================================================================================================================================== paths
_SCHEME = re.compile(r"^([A-Za-z][A-Za-z0-9+.-]*)://")
_ONELAKE_HOST = re.compile(r"(^|\.)onelake\.(dfs|blob)\.fabric\.microsoft\.com$", re.IGNORECASE)
_AREAS = ("Tables", "Files")


@dataclass(frozen=True)
class PathTarget:
    """A storage path classified into the asset it denotes (``kind``: ``table`` | ``file`` | ``external``).

    ``name`` is the dotted table name or the path; ``lakehouse`` / ``workspace`` are filled when the URL names them (OneLake URLs, ``/lakehouse/<name>/...``).
    ``uri`` is set for locations with a URL scheme (they also get a ``onelake_path`` / ``delta_location`` alias so that discovery can match them physically).
    """

    kind: str
    name: str
    area: str | None = None
    lakehouse: str | None = None
    workspace: str | None = None
    uri: str | None = None

    def ref(self, ctx: LineageContext, fmt: str | None = None) -> AssetRef:
        """The :class:`AssetRef` for this target. ``fmt == "delta"`` makes a cloud location outside a lakehouse a table (its identity is the physical location)."""
        overrides: dict[str, str | None] = {}
        aliases: dict[str, str] = {}
        kind = self.kind
        if self.area is None and not self.lakehouse:
            overrides["lakehouse_id"] = None      # a location outside any lakehouse is not relative to the default lakehouse
            overrides["lakehouse"] = None
        if self.lakehouse:
            overrides["lakehouse_id"] = None
            if looks_like_guid(self.lakehouse):
                overrides["lakehouse"] = None
                overrides["fabric_lakehouse"] = self.lakehouse.lower()
            else:
                overrides["lakehouse"] = self.lakehouse
        if self.workspace:
            if looks_like_guid(self.workspace):
                overrides["fabric_workspace"] = self.workspace.lower()
            else:
                overrides["fabric_workspace_name"] = self.workspace
        if self.uri:
            if self.lakehouse is not None or self.area in _AREAS:
                aliases["onelake_path"] = normalize_path(self.uri)
            elif fmt == "delta":
                kind = "table"
                aliases["delta_location"] = normalize_path(self.uri)
            else:
                aliases["onelake_path"] = normalize_path(self.uri)
        return AssetRef.make(kind, aliases=aliases or None, name=self.name, scope=ctx.scope(**overrides))


def _table_segments(segs: list[str]) -> list[str]:
    """The name segments of a ``Tables/...`` path: stop at Delta/partition internals (``_delta_log``, ``date=...``, data files)."""
    out: list[str] = []
    for seg in segs:
        if seg.startswith("_") or "=" in seg or seg.lower().endswith((".parquet", ".json", ".crc", ".csv")) or len(out) == 2:
            break
        out.append(seg)
    return out


def classify_path(path: str, *, fmt: str | None = None) -> PathTarget:
    """Classify a Spark / pandas / DuckDB path.

    * ``Tables/<name>``, ``./Tables/<schema>/<name>``, ``/lakehouse/default/Tables/<name>`` and OneLake URLs with a ``/Tables/`` segment denote **tables**
      (``<schema>.<name>`` when two name segments follow ``Tables``, the shape of schema-enabled lakehouses);
    * ``Files/...`` in the same spellings denote **files** of the lakehouse;
    * ``abfss://`` / ``s3://`` / ``gs://`` / ``wasbs://`` / ``hdfs://`` / ``file://`` locations outside a lakehouse are files (tables when ``fmt == "delta"``, see :meth:`PathTarget.ref`);
    * any other path is a plain (local) file.
    """
    p = strip_uri_secrets(path.strip().replace("\\", "/"))
    m = _SCHEME.match(p)
    if m:
        scheme = m.group(1).lower()
        rest = p[m.end():]
        host, _, tail = rest.partition("/")
        user, _, hostname = host.rpartition("@")
        if scheme in ("abfss", "abfs", "https", "http") and _ONELAKE_HOST.search(hostname):
            segs = [s for s in tail.split("/") if s]
            ws = user or (segs.pop(0) if segs and scheme in ("https", "http") else "")
            if ws == "<?>":                      # workspace only known at run time (notebookutils.lakehouse.get): no URL identity, the lakehouse name still counts
                ws, p = "", ""
            lh = segs.pop(0) if segs else ""
            lh_name = re.sub(r"\.(Lakehouse|Warehouse)$", "", lh, flags=re.IGNORECASE)
            if segs and segs[0] in _AREAS:
                area, sub = segs[0], segs[1:]
                names = _table_segments(sub) if area == "Tables" else []
                if names:
                    return PathTarget("table", ".".join(names), "Tables", lh_name or None, ws or None, p or None)
                return PathTarget("file", "/".join([area, *sub]), area, lh_name or None, ws or None, p or None)
            return PathTarget("file", "/".join(segs), None, lh_name or None, ws or None, p or None)
        if scheme in ("file", "local"):
            return PathTarget("file", p)
        return PathTarget("file", p, None, None, None, p)
    lakehouse: str | None = None
    m2 = re.match(r"^/lakehouse/([^/]+)/(Tables|Files)(?:/(.*))?$", p)
    if m2:
        lakehouse = None if m2.group(1) == "default" else m2.group(1)
        area, rest2 = m2.group(2), m2.group(3) or ""
    else:
        m3 = re.match(r"^(?:\./)?(Tables|Files)(?:/(.*))?$", p)
        if not m3:
            return PathTarget("file", p)
        area, rest2 = m3.group(1), m3.group(2) or ""
    segs2 = [s for s in rest2.split("/") if s]
    names2 = _table_segments(segs2) if area == "Tables" else []
    if names2:
        return PathTarget("table", ".".join(names2), "Tables", lakehouse)
    return PathTarget("file", "/".join([area, *segs2]), area, lakehouse)


# =================================================================================================================================== FactBuilder
class FactBuilder:
    """Collects assets, edges and gaps of one analysis, merging duplicates (same edge from several places -> one edge with several evidence records).

    ``analyzer`` is the evidence source name (``"sql-analyzer"``). :meth:`edge` is the only way analyzers add edges, so confidence, evidence caps and the global
    edge cap are enforced in one place; :meth:`gap` de-duplicates identical gaps (the same dynamic reference met in a loop reports once).
    """

    def __init__(self, analyzer: str, ctx: LineageContext) -> None:
        self.analyzer = analyzer
        self.ctx = ctx
        self._assets: dict[tuple[Any, ...], AssetFact] = {}
        self._edges: dict[tuple[Any, ...], EdgeFact] = {}
        self._gaps: dict[tuple[Any, ...], Gap] = {}
        self._capped = False
        self._gaps_capped = False
        self.edge_total = 0
        self.muted = False  # when True, gaps are dropped (speculative analysis of code that is never called)

    # -- evidence
    def evidence(self, kind: str, confidence: str, location: dict[str, Any] | None = None, snippet: str | None = None, analyzer: str | None = None) -> Evidence:
        """An :class:`Evidence` stamped with this analyzer (or ``analyzer``) and version."""
        return Evidence(kind=kind, source=evidence_source(analyzer or self.analyzer), confidence=confidence, location=dict(location or {}), snippet=clip(redact_secrets(snippet), SNIPPET))

    # -- assets
    def asset(self, fact: AssetFact) -> AssetFact:
        """Add (or return the existing) asset fact for ``fact.ref``; attributes of a later duplicate are merged without overwriting."""
        key = ref_key(fact.ref)
        have = self._assets.get(key)
        if have is None:
            fact.name = redact_secrets(fact.name) or fact.name
            fact.description = redact_secrets(fact.description)
            fact.path = redact_secrets(fact.path)
            fact.attrs = _redact_value(fact.attrs)
            self._assets[key] = fact
            return fact
        for k, v in fact.attrs.items():
            have.attrs.setdefault(k, _redact_value(v))
        if have.description is None:
            have.description = fact.description
        return have

    def has_asset(self, ref: AssetRef) -> bool:
        """True if an asset fact for ``ref`` was added."""
        return ref_key(ref) in self._assets

    # -- edges
    def edge(self, src: AssetRef, dst: AssetRef, etype: str, *, confidence: str, kind: str, location: dict[str, Any] | None = None, snippet: str | None = None,
             transformation: str | None = None, via: AssetRef | None = None, attrs: dict[str, Any] | None = None, analyzer: str | None = None) -> EdgeFact | None:
        """Add an edge or merge its evidence into the existing identical edge (same type, endpoints and ``via``). Returns ``None`` once the cap is reached."""
        key = (etype, ref_key(src), ref_key(dst), ref_key(via) if via is not None else None)
        ev = self.evidence(kind, confidence, location, snippet, analyzer)
        have = self._edges.get(key)
        if have is None:
            if self.edge_total >= MAX_EDGES:
                if not self._capped:
                    self._capped = True
                    self.gap("unsupported_construct", "analysis", f"more than {MAX_EDGES} edges: the remaining relationships were not recorded",
                             "Split the definition into smaller notebooks or queries.")
                return None
            self.edge_total += 1
            have = EdgeFact(src=src, dst=dst, type=etype, evidence=[ev], confidence=confidence, transformation=transformation, via=via, attrs=dict(attrs or {}))
            self._edges[key] = have
            return have
        if ev.key() not in {e.key() for e in have.evidence} and len(have.evidence) < MAX_EVIDENCE:
            have.evidence.append(ev)
        have.confidence = stronger(have.confidence or confidence, confidence)
        if transformation and transformation != have.transformation:
            if have.transformation is None:
                have.transformation = transformation
            elif transformation not in have.transformation.split(" | "):
                have.transformation = clip(have.transformation + " | " + transformation, 1000)
        for k, v in (attrs or {}).items():
            have.attrs.setdefault(k, v)
        return have

    # -- gaps
    def gap(self, kind: str, subject: str, detail: str, remediation: str | None = None, location: dict[str, Any] | None = None, analyzer: str | None = None) -> None:
        """Record a gap once per (kind, subject, detail, location)."""
        if self.muted:
            return
        loc = dict(location or {})
        subject, detail, remediation = redact_secrets(subject) or subject, redact_secrets(detail) or detail, redact_secrets(remediation)
        key = (kind, subject, detail, repr(sorted(loc.items())))
        if key in self._gaps:
            return
        if len(self._gaps) >= MAX_GAPS:
            if not self._gaps_capped:
                self._gaps_capped = True
                note = f"more than {MAX_GAPS} distinct gaps: the remaining ones are not listed"
                self._gaps[("unsupported_construct", "analysis", note, "[]")] = Gap(kind="unsupported_construct", source=self.analyzer, subject="analysis", detail=note,
                                                                                   remediation="Fix the first gaps or split the definition.", location={})
            return
        self._gaps[key] = Gap(kind=kind, source=analyzer or self.analyzer, subject=subject, detail=detail, remediation=remediation, location=loc)

    # -- result
    def facts(self) -> Facts:
        """The collected :class:`Facts` (insertion order, which is deterministic)."""
        return Facts(assets=list(self._assets.values()), edges=list(self._edges.values()), gaps=list(self._gaps.values()))

    def merge(self, other: Facts) -> None:
        """Add the facts of a sub-analysis (assets, edges with their evidence, gaps) through the normal de-duplication."""
        for a in other.assets:
            self.asset(a)
        for e in other.edges:
            first = True
            for ev in e.evidence or [Evidence(kind="static", source=evidence_source(self.analyzer), confidence=e.confidence or "unknown")]:
                got = self.edge(e.src, e.dst, e.type, confidence=ev.confidence, kind=ev.kind, location=ev.location, snippet=ev.snippet,
                                transformation=e.transformation if first else None, via=e.via, attrs=e.attrs,
                                analyzer=ev.source.split("@")[0])
                first = False
                if got is None:
                    break
        for g in other.gaps:
            self.gap(g.kind, g.subject, g.detail, g.remediation, g.location, g.source)


def facts_with_gap(analyzer: str, kind: str, subject: str, detail: str, remediation: str | None = None, location: dict[str, Any] | None = None) -> Facts:
    """A :class:`Facts` holding just one gap (the result for an unsupported or unparseable input)."""
    return Facts(gaps=[Gap(kind=kind, source=analyzer, subject=subject, detail=detail, remediation=remediation, location=dict(location or {}))])


def as_text(value: Any) -> str:
    """Join an nbformat ``source`` (string or list of strings) into one string; anything else becomes ``""``."""
    if isinstance(value, str):
        return value
    if isinstance(value, list):
        return "".join(x for x in value if isinstance(x, str))
    return ""


def sorted_unique(items: Iterable[str]) -> list[str]:
    """Sorted, de-duplicated list (case-sensitive) used wherever a set would otherwise be iterated."""
    return sorted(set(items))
