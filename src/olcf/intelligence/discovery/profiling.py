# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Profiling for discovery: what a scan may say about the *content* of a table, and what it deliberately does not.

Two levels, kept apart on purpose (design note: "sampling is opt-in"):

``metadata`` profiling (:func:`profile_delta_log`, always on)
    Read from the Delta transaction log only - the ``add`` actions' file statistics - without reading a single data file: row count, size, file count, table version and, per
    column, the **null count** (plus min / max for numeric, date and timestamp columns that do not look sensitive). The log stores per-file min / max values, which *are* data
    values: they are therefore never stored for text / binary columns or for a column :mod:`~olcf.intelligence.discovery.detect` suggests is sensitive. Whatever cannot be
    determined is reported as an honest ``not_inspected`` :class:`~olcf.intelligence.model.Gap` (statistics missing for some files, column beyond the first 32 statistics
    columns, a table whose log cannot be read), never as a made-up zero.

``value`` profiling (:func:`profile_values`, opt-in, gated by a sampling policy)
    Looks at actual cell values of at most ``policy.max_rows`` rows (0 = disabled, which is the default) and records only *aggregates*: non-null count, distinct-value count
    (capped), and - through :func:`~olcf.intelligence.discovery.detect.classify_column` - the **suggested classifications** the values justify (for example "these look like
    e-mail addresses"). Values themselves are discarded; they never enter the graph, a log line or an audit record. Value profiling never runs unless
    :meth:`~olcf.intelligence.discovery.sampling.SamplingPolicyStore.effective` says the table's scope has ``max_rows > 0``.
"""

from __future__ import annotations

import contextlib
import datetime as dt
import decimal
import math
from collections.abc import Iterable, Sequence
from typing import Any

from olcf.intelligence.discovery import detect
from olcf.intelligence.model import Gap

#: Delta writes statistics for the first ``delta.dataSkippingNumIndexedCols`` (default 32) columns only.
DEFAULT_STATS_COLUMNS = 32
#: A distinct-value count above this is reported as ``">= N"`` (the exact count is not needed to judge cardinality).
DISTINCT_CAP = 10_000
#: Type families whose min / max are safe to keep (they are numbers or points in time, never free text).
_RANGE_FAMILIES = frozenset({"integer", "floating", "date", "timestamp"})


def _json_scalar(v: Any) -> Any:
    """A JSON-safe scalar for a min / max statistic (dates become ISO strings, non-finite numbers ``None``)."""
    if v is None or isinstance(v, bool):
        return v
    if isinstance(v, (dt.datetime, dt.date)):
        return v.isoformat()
    if isinstance(v, decimal.Decimal):
        return float(v) if v.is_finite() else None
    if isinstance(v, float):
        return v if math.isfinite(v) else None
    if isinstance(v, int):
        return v if abs(v) <= 2**53 - 1 else str(v)
    return str(v)


def profile_delta_log(table: Any, columns: Sequence[dict[str, Any]], *, subject: str, source: str = "discovery:local", location: dict[str, Any] | None = None,
                      stats_columns: int = DEFAULT_STATS_COLUMNS) -> tuple[dict[str, Any], list[Gap]]:
    """Profile an open ``deltalake.DeltaTable`` from its log. ``columns`` is the schema as ``[{"name", "type", ...}]`` (used to decide which min / max are safe).

    Returns ``(profile, gaps)``. ``profile`` is ``{"row_count", "size_bytes", "num_files", "version", "last_modified", "columns": {name: {"null_count", "min", "max"}}, "source": "delta_log",
    "complete": bool}`` where unknown numbers are ``None``. ``gaps`` holds one ``not_inspected`` gap per distinct limitation. Reads the log only; no data file is opened.
    """
    gaps: list[Gap] = []
    loc = dict(location or {})
    try:
        import pyarrow as pa

        actions = pa.table(table.get_add_actions(flatten=True))
    except Exception as exc:  # noqa: BLE001 - a log we cannot read is a gap, not a failed scan
        gaps.append(Gap("not_inspected", source, subject, f"the Delta log of '{subject}' could not be read for statistics ({type(exc).__name__})",
                        "Check the table with the Storage test; statistics are filled in again by the next scan.", loc))
        return ({"row_count": None, "size_bytes": None, "num_files": None, "version": _version(table), "last_modified": None, "columns": {}, "source": "delta_log", "complete": False}, gaps)

    names = actions.column_names
    n_files = actions.num_rows
    size = sum(v for v in actions.column("size_bytes").to_pylist() if v is not None) if "size_bytes" in names else None
    rows: int | None = None
    if "num_records" in names:
        vals = actions.column("num_records").to_pylist()
        known = [v for v in vals if v is not None]
        rows = sum(known) if len(known) == len(vals) else None
        if rows is None and n_files:
            gaps.append(Gap("not_inspected", source, subject, f"row count of '{subject}' is unknown: {len(vals) - len(known)} of {len(vals)} data files carry no record count in the log",
                            "Rewrite or OPTIMIZE the table with a writer that records file statistics.", loc))
    elif n_files:
        gaps.append(Gap("not_inspected", source, subject, f"row count of '{subject}' is unknown: the log carries no record counts", "Rewrite the table with a writer that records file statistics.", loc))
    last_modified: str | None = None
    if "modification_time" in names and n_files:
        stamps = [v for v in actions.column("modification_time").to_pylist() if v is not None]
        if stamps:
            last_modified = dt.datetime.fromtimestamp(max(stamps) / 1000, dt.UTC).strftime("%Y-%m-%dT%H:%M:%S.%fZ")

    out_cols: dict[str, dict[str, Any]] = {}
    sensitive = detect.sensitive_column_names(list(columns))
    beyond: list[str] = []
    missing_stats: list[str] = []
    for idx, col in enumerate(columns):
        cname = str(col.get("name"))
        fam = detect.type_family(col.get("type"))
        nulls_name, min_name, max_name = f"null_count.{cname}", f"min.{cname}", f"max.{cname}"
        entry: dict[str, Any] = {"null_count": None, "min": None, "max": None}
        if nulls_name in names and n_files:
            vals = actions.column(nulls_name).to_pylist()
            known = [v for v in vals if v is not None]
            if len(known) == len(vals):
                entry["null_count"] = sum(known)
            else:
                missing_stats.append(cname)
        elif n_files and idx >= stats_columns:
            beyond.append(cname)
        elif n_files:
            missing_stats.append(cname)
        if fam in _RANGE_FAMILIES and cname.lower() not in sensitive and min_name in names and max_name in names and n_files:
            mins = [v for v in actions.column(min_name).to_pylist() if v is not None]
            maxs = [v for v in actions.column(max_name).to_pylist() if v is not None]
            if mins and maxs and len(mins) == n_files:
                with contextlib.suppress(TypeError):
                    entry["min"], entry["max"] = _json_scalar(min(mins)), _json_scalar(max(maxs))
        out_cols[cname] = entry
    if beyond:
        gaps.append(Gap("not_inspected", source, subject, f"{len(beyond)} column(s) of '{subject}' lie beyond the first {stats_columns} statistics columns and have no null counts: "
                        f"{', '.join(beyond[:8])}{' ...' if len(beyond) > 8 else ''}", "Raise delta.dataSkippingNumIndexedCols on the table to collect more statistics.", loc))
    if missing_stats:
        gaps.append(Gap("not_inspected", source, subject, f"null counts of {len(missing_stats)} column(s) of '{subject}' are incomplete in the log: "
                        f"{', '.join(missing_stats[:8])}{' ...' if len(missing_stats) > 8 else ''}", "Statistics are written by Delta writers that collect them; OPTIMIZE or rewrite the table.", loc))
    complete = not gaps
    return ({"row_count": rows, "size_bytes": size, "num_files": n_files, "version": _version(table), "last_modified": last_modified, "columns": out_cols, "source": "delta_log",
             "complete": complete}, gaps)


def _version(table: Any) -> int | None:
    try:
        return int(table.version())
    except Exception:  # noqa: BLE001
        return None


def profile_values(arrow_table: Any, columns: Sequence[dict[str, Any]] | None = None) -> dict[str, dict[str, Any]]:
    """Value profile of the rows in ``arrow_table`` (a ``pyarrow.Table`` holding at most the policy's ``max_rows`` rows): ``{column: {"rows", "non_null", "distinct", "distinct_capped",
    "suggested_classifications"}}``. Only aggregates and suggestions are returned - **no cell value is kept**. ``columns`` optionally supplies the declared types."""
    declared = {str(c.get("name")): c.get("type") for c in (columns or [])}
    out: dict[str, dict[str, Any]] = {}
    n = arrow_table.num_rows
    for name in arrow_table.column_names:
        col = arrow_table.column(name)
        values = col.to_pylist()
        present = [v for v in values if v is not None]
        try:
            distinct = len({v if isinstance(v, (str, int, float, bool, bytes, dt.date, dt.datetime, decimal.Decimal)) else repr(v) for v in present})
        except TypeError:
            distinct = len({repr(v) for v in present})
        suggestions = detect.classify_column(name, declared.get(name) or str(col.type), present[: detect.MAX_VALUES])
        out[name] = {"rows": n, "non_null": len(present), "distinct": min(distinct, DISTINCT_CAP), "distinct_capped": distinct > DISTINCT_CAP,
                     "suggested_classifications": [s for s in suggestions if "pattern" in s.get("evidences", ())]}
    return out


def merge_value_profile(columns: Iterable[dict[str, Any]], profile: dict[str, dict[str, Any]]) -> list[dict[str, Any]]:
    """Return ``columns`` with the aggregates and value-derived suggestions of :func:`profile_values` merged into each column dict (a copy; the input is not modified)."""
    merged: list[dict[str, Any]] = []
    for c in columns:
        c2 = dict(c)
        p = profile.get(str(c.get("name")))
        if p:
            c2["value_profile"] = {k: p[k] for k in ("rows", "non_null", "distinct", "distinct_capped")}
            if p["suggested_classifications"]:
                existing = {s["classification"]: s for s in c2.get("suggested_classifications") or [] if isinstance(s, dict)}
                for s in p["suggested_classifications"]:
                    cur = existing.get(s["classification"])
                    if cur is None or s["confidence"] >= cur.get("confidence", 0):
                        existing[s["classification"]] = s
                c2["suggested_classifications"] = sorted(existing.values(), key=lambda s: (-float(s.get("confidence", 0)), s["classification"]))
        merged.append(c2)
    return merged
