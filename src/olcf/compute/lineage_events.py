# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Validation of the dataset events a job's runner reports (``@@OLCF@@`` ``dataset_read`` / ``dataset_written`` / ``dataset_statement``).

The runner (:mod:`olcf.runtime_lib.lineage_probe`) runs user code, so everything it reports is **untrusted input**: a hostile or buggy job can print any JSON line.
:func:`sanitize_runner_event` turns one runner event into the payload of the platform event the control plane publishes (``dataset.read`` / ``dataset.written`` /
``dataset.statement``, contract ``docs/dev/INTELLIGENCE.md`` sections 6 and 7) or raises :class:`MalformedDatasetEvent` with a short reason code. Nothing here raises
anything else, touches the database or imports ``olcf.intelligence``.

What is enforced
----------------
* **shape**: only the documented keys are read (unknown keys are ignored, never forwarded), ``datasets`` must be a list of 1..:data:`MAX_DATASETS` objects of kind
  ``table`` or ``file``, tables need a name, files a path or a URI;
* **size**: every string is stripped of control characters and clipped (names 512, paths and URIs 2048, SQL 20000 characters), URIs lose their query string and fragment
  (SAS tokens live there), the whole payload is capped at :data:`MAX_PAYLOAD_BYTES`;
* **identity**: a ``lakehouse_id`` is kept only when it is one of the lakehouses attached to this job (a job cannot claim to have read another lakehouse's table by id);
  the process, workspace and engine come from the job row, never from the event. The Fabric identifiers (``process.fabric``: tenant, workspace GUID, notebook GUID; ``datasets[].fabric``:
  workspace and lakehouse GUID) are likewise copied from the job (its Fabric ``origin`` and the ``definition.fabric`` of its attached lakehouses), never from what the runner prints;
* **budget**: at most :data:`MAX_EVENTS_PER_JOB` events are forwarded per job attempt (:class:`EventBudget`), whatever the runner prints.
"""

from __future__ import annotations

import json
import re
import threading
from collections import OrderedDict
from collections.abc import Mapping
from typing import Any

from olcf.core import dataset_events as dsev
from olcf.core.timeutil import now_iso

MAX_DATASETS = 50
MAX_NAME_CHARS = 512
MAX_PATH_CHARS = 2048
MAX_FORMAT_CHARS = 32
MAX_PAYLOAD_BYTES = 96 * 1024
MAX_EVENTS_PER_JOB = 200
RUNNER_EVENTS: dict[str, str] = {"dataset_read": "dataset.read", "dataset_written": "dataset.written", "dataset_statement": "dataset.statement"}

_CONTROL = re.compile(r"[\x00-\x1f\x7f]")
_ID = re.compile(r"^[A-Za-z0-9_.:-]{1,80}$")
_AT = re.compile(r"^\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d(\.\d{1,9})?(Z|[+-]\d\d:\d\d)?$")
_FORMAT = re.compile(r"^[A-Za-z0-9_.+-]{1,32}$")


class MalformedDatasetEvent(ValueError):
    """A runner dataset event that cannot be forwarded; ``reason`` is a short stable code (used as a metric label)."""

    def __init__(self, reason: str) -> None:
        super().__init__(reason)
        self.reason = reason


def _text(value: Any, limit: int) -> str | None:
    """``value`` as a clean string (control characters removed, trimmed, clipped) or ``None`` when it is not a non-empty string."""
    if not isinstance(value, str):
        return None
    out = _CONTROL.sub("", value).strip()
    return out[:limit] if out else None


def _strip_query(uri: str) -> str:
    """A URI without query string and fragment (they may carry SAS tokens or signatures)."""
    return re.split(r"[?#]", uri, maxsplit=1)[0]


def _lakehouse(value: Any, allowed: set[str]) -> str | None:
    """The lakehouse id when it is one of this job's lakehouses, else ``None``."""
    text = _text(value, 80)
    return text if text and _ID.match(text) and text in allowed else None


def _dataset(raw: Any, allowed: set[str], default_format: str | None, fabric_lakehouses: Mapping[str, dict[str, str]] | None = None) -> dict[str, Any]:
    if not isinstance(raw, dict):
        raise MalformedDatasetEvent("dataset_not_object")
    kind = raw.get("kind")
    if kind not in dsev.DATASET_KINDS:
        raise MalformedDatasetEvent("dataset_kind")
    name, path = _text(raw.get("name"), MAX_NAME_CHARS), _text(raw.get("path"), MAX_PATH_CHARS)
    uri_raw = _text(raw.get("uri"), MAX_PATH_CHARS)
    uri = _strip_query(uri_raw) if uri_raw else None
    if kind == "table" and not name:
        raise MalformedDatasetEvent("table_without_name")
    if kind == "file" and not (path or uri):
        raise MalformedDatasetEvent("file_without_path")
    fmt = _text(raw.get("format"), MAX_FORMAT_CHARS) or default_format
    fmt = fmt if fmt and _FORMAT.match(fmt) else None
    lakehouse = _lakehouse(raw.get("lakehouse_id"), allowed)
    return dsev.dataset_descriptor(str(kind), lakehouse_id=lakehouse, name=name, path=path, uri=uri, format=fmt, fabric=(fabric_lakehouses or {}).get(lakehouse) if lakehouse else None)


def _at(data: dict[str, Any]) -> str:
    at = data.get("at")
    return at if isinstance(at, str) and _AT.match(at) else now_iso()


def fabric_lakehouses_of(spec: Any) -> dict[str, dict[str, str]]:
    """``{OLCF lakehouse id: {"workspace_id", "lakehouse_id"}}`` for the lakehouses of a job spec that are attached to a Fabric lakehouse (``definition.fabric`` as the storage service
    reports it in the spec's lakehouse descriptors; only GUID pairs count)."""
    out: dict[str, dict[str, str]] = {}
    if not isinstance(spec, dict):
        return out
    attached = spec.get("lakehouses")
    descriptors = [spec.get("default_lakehouse"), *(attached if isinstance(attached, list) else [])]
    for d in descriptors:
        if isinstance(d, dict) and isinstance(d.get("id"), str):
            block = dsev.fabric_lakehouse_block(d.get("fabric"))
            if block:
                out[d["id"]] = block
    return out


def sanitize_runner_event(event: str, data: Any, *, row: dict[str, Any], allowed_lakehouses: set[str], default_lakehouse: str | None = None,
                          fabric_lakehouses: Mapping[str, dict[str, str]] | None = None) -> tuple[str, dict[str, Any]]:
    """Validate runner event ``event`` (``dataset_read`` / ``dataset_written`` / ``dataset_statement``) and build the platform event.

    ``row`` is the job the event came from (or, for an interactive session, a dict with ``kind == "session"``, the session id as ``id`` and the notebook as ``item_id``): workspace, item, id,
    name, engine and kind are taken from it. ``allowed_lakehouses`` are the lakehouse ids attached to
    the job and ``default_lakehouse`` the id unqualified names resolve against. ``fabric_lakehouses`` (see :func:`fabric_lakehouses_of`) says which of them are Fabric lakehouses.
    A job whose origin is Fabric (``row["origin_kind"] == "fabric"``, the adapter's ``origin.fabric`` record) gets ``process.fabric``. Returns ``(platform event type, payload)`` or
    raises :class:`MalformedDatasetEvent`.
    """
    etype = RUNNER_EVENTS.get(event)
    if etype is None:
        raise MalformedDatasetEvent("unknown_event")
    if not isinstance(data, dict):
        raise MalformedDatasetEvent("not_an_object")
    session = row.get("kind") == "session"
    kind = {"notebook": "notebook", "capsule": "notebook", "session": "notebook", "spark_job": "spark_job"}.get(str(row.get("kind")), "job")
    fabric = None
    if not session and str(row.get("origin_kind") or "") == "fabric" and isinstance(row.get("origin"), dict):
        fabric = dsev.fabric_process_block(row["origin"].get("fabric"))
    proc = dsev.process_descriptor(kind, item_id=row.get("item_id"), job_id=None if session else row.get("id"), name=row.get("name"),
                                   run_id=row.get("id") if session else row.get("parent_run_id"), fabric=fabric)
    base: dict[str, Any] = {"workspace_id": row.get("workspace_id"), "process": proc}
    if etype == "dataset.statement":
        sql = data.get("sql")
        if not isinstance(sql, str) or not sql.strip():
            raise MalformedDatasetEvent("statement_without_sql")
        dialect = data.get("dialect")
        default_id = _lakehouse(data.get("default_lakehouse_id"), allowed_lakehouses) or default_lakehouse
        payload = {**base, "sql": dsev.clip_statement(_CONTROL.sub(lambda m: m.group(0) if m.group(0) in "\n\t" else " ", sql)),
                   "dialect": dialect if dialect in dsev.DIALECTS else "spark", "default_lakehouse_id": default_id, "at": _at(data)}
        if default_id and (fabric_lakehouses or {}).get(default_id):
            payload["default_lakehouse_fabric"] = dict((fabric_lakehouses or {})[default_id])
    else:
        raw = data.get("datasets")
        if not isinstance(raw, list) or not raw:
            raise MalformedDatasetEvent("no_datasets")
        if len(raw) > MAX_DATASETS:
            raw = raw[:MAX_DATASETS]
        default_format = _text(data.get("format"), MAX_FORMAT_CHARS)
        mode = data.get("mode")
        payload = {**base, "datasets": [_dataset(d, allowed_lakehouses, default_format, fabric_lakehouses) for d in raw], "engine": str(row.get("engine") or "spark")[:16],
                   "mode": mode if etype == "dataset.written" and mode in dsev.WRITE_MODES else None, "statement_hash": None, "at": _at(data)}
    if len(json.dumps(payload, default=str)) > MAX_PAYLOAD_BYTES:
        raise MalformedDatasetEvent("too_large")
    return etype, payload


def compact_for_job_log(etype: str, payload: dict[str, Any]) -> dict[str, Any]:
    """What is stored in the job's own event log: the payload without the SQL text (replaced by its length and hash) so ``job_events`` stays small."""
    if etype != "dataset.statement":
        return {"datasets": payload.get("datasets"), "mode": payload.get("mode")}
    sql = str(payload.get("sql") or "")
    return {"sql_chars": len(sql), "statement_hash": dsev.statement_hash(sql), "dialect": payload.get("dialect")}


class EventBudget:
    """Thread-safe per-job-attempt event budget: :meth:`take` is ``True`` for the first :data:`MAX_EVENTS_PER_JOB` events of a key, ``False`` afterwards.

    Remembers the newest ``max_keys`` keys only (a long-running control plane must not grow a counter per job forever).
    """

    def __init__(self, limit: int = MAX_EVENTS_PER_JOB, max_keys: int = 4096) -> None:
        self.limit, self.max_keys = limit, max_keys
        self._counts: OrderedDict[str, int] = OrderedDict()
        self._lock = threading.Lock()

    def take(self, key: str) -> bool:
        """Count one event for ``key``; ``False`` when its budget is spent."""
        with self._lock:
            n = self._counts.pop(key, 0)
            self._counts[key] = n + 1
            while len(self._counts) > self.max_keys:
                self._counts.popitem(last=False)
            return n < self.limit


class EventStats:
    """Thread-safe counters of forwarded and dropped dataset events (``dropped`` by reason); exposed for diagnostics and tests."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self.forwarded = 0
        self.dropped: dict[str, int] = {}

    def forward(self) -> None:
        """Count one forwarded event."""
        with self._lock:
            self.forwarded += 1

    def drop(self, reason: str) -> None:
        """Count one dropped event under ``reason``."""
        with self._lock:
            self.dropped[reason] = self.dropped.get(reason, 0) + 1

    def snapshot(self) -> dict[str, Any]:
        """``{"forwarded": n, "dropped": {reason: n}}`` (a copy)."""
        with self._lock:
            return {"forwarded": self.forwarded, "dropped": dict(self.dropped)}
