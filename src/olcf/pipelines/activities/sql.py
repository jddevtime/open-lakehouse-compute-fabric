# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""``sql`` activity: run one statement through the DATA ``sql`` service (DuckDB by default, Spark on request) as the run's principal.

The SQL service enforces ``sql.execute`` / ``sql.write``, table authorisation, read-only protection of the embedded engine, row and time limits and
query history. Parameters are bound by the service as typed literals - they are never concatenated into the statement text.
"""

from __future__ import annotations

import json
import re
from typing import Any

from olcf.core.errors import OlcfError
from olcf.pipelines.activities.base import (
    Activity,
    ActivityCancelled,
    ActivityContext,
    ActivityError,
    obj,
    s_int,
    s_obj,
    s_str,
)

MAX_OUTPUT_BYTES = 1024 * 1024
HARD_ROW_CAP = 100_000
_READ_FIRST = {"select", "with", "show", "describe", "desc", "explain", "summarize", "values", "from", "table"}
_WRITE_WORDS = re.compile(r"\b(insert|update|delete|merge|create|drop|alter|truncate|copy|attach|detach|install|load|set|pragma|call|export|import|replace|grant|revoke|optimize|vacuum)\b", re.I)
_COMMENTS = re.compile(r"--[^\n]*|/\*.*?\*/", re.S)
_STRINGS = re.compile(r"'(?:[^']|'')*'|\"(?:[^\"]|\"\")*\"")


def is_read_only(sql: str) -> bool:
    """Conservative textual check used only to decide whether a crashed statement may be re-run (the engine enforces real read-only-ness)."""
    s = _STRINGS.sub("''", _COMMENTS.sub(" ", sql)).strip().rstrip(";").strip()
    if not s or ";" in s:
        return False
    first = s.split(None, 1)[0].lower().lstrip("(")
    return first in _READ_FIRST and not _WRITE_WORDS.search(s)


class SqlActivity(Activity):
    """Run SQL against a lakehouse and expose rows / scalar / first row to downstream expressions."""

    type = "sql"
    title = "SQL"
    description = "Run a SQL statement (duckdb or spark). Output: rowCount, columns, firstRow, rows (<= maxRows), scalar, truncated."
    mode = "blocking"
    default_timeout_seconds = 3600
    properties_schema = obj({
        "sql": s_str(minLength=1), "engine": {"enum": ["duckdb", "spark"]}, "lakehouse": s_str(), "parameters": s_obj(), "maxRows": s_int(1, HARD_ROW_CAP),
        "timeoutSeconds": s_int(1, 86400),
    }, ["sql"])

    def idempotent_for(self, props: dict[str, Any]) -> bool:
        return isinstance(props.get("sql"), str) and is_read_only(props["sql"])

    def execute(self, actx: ActivityContext) -> dict[str, Any]:
        from olcf.sql.model import SqlRequest

        p = actx.props
        text = p.get("sql")
        if not isinstance(text, str) or not text.strip():
            raise ActivityError("sql: 'sql' must be a non-empty string", category="configuration", retryable=False)
        params = p.get("parameters") or {}
        if not isinstance(params, dict):
            raise ActivityError("sql: 'parameters' must be an object", category="configuration", retryable=False)
        max_rows = int(p.get("maxRows") or 1000)
        lakehouse = None
        if p.get("lakehouse"):
            try:
                lakehouse = actx.service("items").resolve(actx.workspace_id, str(p["lakehouse"]), "lakehouse")["id"]
            except OlcfError as exc:
                raise ActivityError(f"sql: lakehouse: {exc.message}", category="configuration", retryable=False) from None
        qid = f"pl-{actx.activity_run_id}-{actx.attempt}"
        actx.save_state(query_id=qid)
        req = SqlRequest(sql=text, engine=str(p.get("engine") or "duckdb"), workspace_id=actx.workspace_id, lakehouse=lakehouse, parameters=params, max_rows=min(max_rows, HARD_ROW_CAP),
                         timeout_seconds=int(p["timeoutSeconds"]) if p.get("timeoutSeconds") else None, query_id=qid, source="pipeline", label=f"{actx.run.get('pipeline_name')}/{actx.name}")
        try:
            res = actx.service("sql").execute(actx.principal, req)
        except OlcfError as exc:
            if actx.is_cancelled() or exc.code == "query_cancelled":
                raise ActivityCancelled() from None
            transient = exc.code in ("query_timeout", "unavailable", "internal") or exc.status >= 500
            raise ActivityError(exc.message, category=exc.code, retryable=transient, details={**exc.details, **({"remediation": exc.remediation} if exc.remediation else {})}) from None
        names = [str(c.get("name")) for c in res.columns]
        rows = [dict(zip(names, r, strict=False)) for r in res.rows[:max_rows]]
        truncated = bool(res.truncated) or len(res.rows) > max_rows
        while rows and len(json.dumps(rows, default=str)) > MAX_OUTPUT_BYTES:
            rows = rows[: max(0, len(rows) // 2)]
            truncated = True
        first = rows[0] if rows else None
        scalar = res.rows[0][0] if res.rows and res.rows[0] else None
        return {"queryId": res.query_id, "engine": res.engine, "statement": res.statement, "rowCount": len(res.rows), "columns": res.columns, "rows": rows, "firstRow": first,
                "scalar": scalar, "truncated": truncated, "elapsedMs": (res.stats or {}).get("elapsed_ms"), "warnings": res.warnings}

    def cancel(self, actx: ActivityContext) -> None:
        qid = actx.state.get("query_id")
        if qid:
            try:
                actx.service("sql").cancel(actx.principal, qid)
            except Exception:  # noqa: BLE001 - the statement may already be over
                pass


SQL_ACTIVITIES: list[type[Activity]] = [SqlActivity]
