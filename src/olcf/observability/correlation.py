# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Structured-log correlation helpers.

A request, a pipeline run or an activity attempt *binds* identifiers (``correlation_id``, ``trace_id``, ``run_id``, ``job_id`` ...) to the current context
(:func:`bind`, a ``contextvars`` scope that follows ``asyncio`` tasks and worker threads started through ``copy_context``). :class:`CorrelationFilter` appends them to
every log record (``... [cid=abc trace=def run=run_01H...]``) so one grep follows a request through the API, the engine, jobs and webhooks - for the plain *and* the
JSON formatter, because the identifiers become part of the message text. W3C ``traceparent`` is parsed / produced so traces started by a caller continue through the
control plane, and :func:`outgoing_headers` lets outbound HTTP calls (http activity, webhooks) propagate them.
"""

from __future__ import annotations

import contextlib
import logging
import re
import secrets
from collections.abc import Iterator
from contextvars import ContextVar
from typing import Any

_FIELDS: ContextVar[dict[str, str] | None] = ContextVar("olcf_log_fields", default=None)
_TRACEPARENT = re.compile(r"^00-([0-9a-f]{32})-([0-9a-f]{16})-([0-9a-f]{2})$")
_SHORT = {"correlation_id": "cid", "trace_id": "trace", "run_id": "run", "activity_id": "act", "job_id": "job", "workspace_id": "ws", "principal": "who", "webhook_id": "hook",
          "trigger_id": "trg", "alert_id": "alert"}
ORDER = list(_SHORT)


def new_trace_id() -> str:
    return secrets.token_hex(16)


def new_span_id() -> str:
    return secrets.token_hex(8)


def parse_traceparent(header: str | None) -> tuple[str, str, bool] | None:
    """``(trace_id, parent_span_id, sampled)`` of a valid W3C ``traceparent`` header, else ``None`` (all-zero ids are invalid)."""
    if not header:
        return None
    m = _TRACEPARENT.match(header.strip().lower())
    if not m or set(m.group(1)) == {"0"} or set(m.group(2)) == {"0"}:
        return None
    return m.group(1), m.group(2), bool(int(m.group(3), 16) & 1)


def make_traceparent(trace_id: str, span_id: str, sampled: bool = True) -> str:
    return f"00-{trace_id}-{span_id}-{'01' if sampled else '00'}"


def current() -> dict[str, str]:
    return dict(_FIELDS.get() or {})


def get(name: str, default: str | None = None) -> str | None:
    return (_FIELDS.get() or {}).get(name, default)


@contextlib.contextmanager
def bind(**fields: Any) -> Iterator[dict[str, str]]:
    """Add identifiers to the logging context for the duration of the ``with`` block (``None`` values are ignored)."""
    cur = dict(_FIELDS.get() or {})
    cur.update({k: str(v) for k, v in fields.items() if v is not None and v != ""})
    token = _FIELDS.set(cur)
    try:
        yield cur
    finally:
        _FIELDS.reset(token)


def outgoing_headers(extra: dict[str, str] | None = None) -> dict[str, str]:
    """Headers to put on outbound HTTP calls so the receiver can correlate: ``X-Correlation-ID`` and ``traceparent`` (when a trace is active)."""
    h = dict(extra or {})
    f = _FIELDS.get() or {}
    if f.get("correlation_id"):
        h.setdefault("X-Correlation-ID", f["correlation_id"])
    if f.get("trace_id"):
        h.setdefault("traceparent", make_traceparent(f["trace_id"], f.get("span_id") or new_span_id()))
    return h


def suffix(fields: dict[str, str] | None = None) -> str:
    f = fields if fields is not None else (_FIELDS.get() or {})
    parts = [f"{_SHORT[k]}={f[k]}" for k in ORDER if f.get(k)]
    return " [" + " ".join(parts) + "]" if parts else ""


class CorrelationFilter(logging.Filter):
    """Appends the bound identifiers to the message (once) and exposes them as ``record.olcf`` for custom formatters."""

    def filter(self, record: logging.LogRecord) -> bool:
        f = _FIELDS.get() or {}
        if f and not getattr(record, "_olcf_correlated", False):
            record.olcf = dict(f)  # type: ignore[attr-defined]
            try:
                msg = record.getMessage()
            except Exception:  # noqa: BLE001 - malformed format args must not hide the record
                msg = str(record.msg)
            record.msg = msg + suffix(f)
            record.args = ()
            record._olcf_correlated = True  # type: ignore[attr-defined]
        return True


_installed = CorrelationFilter()


def install_log_filter() -> int:
    """Attach the filter to every OLCF root handler (idempotent). Returns the number of handlers touched."""
    n = 0
    for h in logging.getLogger().handlers:
        if getattr(h, "_olcf", False) and _installed not in h.filters:
            h.addFilter(_installed)
            n += 1
    return n


def uninstall_log_filter() -> None:
    for h in logging.getLogger().handlers:
        if _installed in h.filters:
            h.removeFilter(_installed)
