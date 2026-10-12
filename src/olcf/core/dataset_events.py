# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Building blocks for the ``dataset.*`` platform events published by the compute side (SQL, jobs, pipelines, the catalog).

OLCF Intelligence (``olcf.intelligence``) learns what actually ran from ordinary platform events: ``dataset.read``, ``dataset.written``, ``dataset.statement`` and
``dataset.schema_changed`` (contract ``docs/dev/INTELLIGENCE.md`` section 6). This module is the **only** thing the publishing modules share; it deliberately
knows nothing about Intelligence (nothing in the compute path may import ``olcf.intelligence``) and it is stdlib-only apart from the redaction helper, so the
runtime-side code may reuse the same helpers.

Rules every emitter follows
---------------------------
* The event is published by a call of ``ctx.events.publish`` with the event name as a string literal at the call site (the reference generator finds event names by scanning for those literals),
  *inside* a :func:`best_effort` block: an exception is logged (a warning the first time per kind of event, debug afterwards) and swallowed, so a broken event store or
  subscriber can never change the result of a query, a job or a pipeline.
* Payloads are plain JSON-able dicts. They carry identifiers and names only, never data values, credentials or secrets; SQL text appears only in ``dataset.statement``
  (:func:`clip_statement`: redacted, at most :data:`MAX_STATEMENT_CHARS` characters).
* The ``process`` block says *what* did the reading or writing (:func:`process_descriptor`); each entry of ``datasets`` says *what* was touched
  (:func:`dataset_descriptor`).
"""

from __future__ import annotations

import hashlib
import logging
import re
import threading
from collections.abc import Iterator
from contextlib import contextmanager
from typing import Any

from olcf.core.redaction import redact_text

#: Upper bound of the SQL text carried by ``dataset.statement`` (the same bound the SQL history uses for stored text).
MAX_STATEMENT_CHARS = 20_000
#: Kinds of process a ``dataset.*`` event may name (contract section 6).
PROCESS_KINDS: tuple[str, ...] = ("sql_query", "notebook", "spark_job", "pipeline", "report", "job", "query")
#: Kinds of dataset a ``dataset.*`` event may name.
DATASET_KINDS: tuple[str, ...] = ("table", "file")
#: Write modes carried by ``dataset.written``. ``create``, ``append``, ``overwrite`` and ``merge`` are the contract values; ``update`` and ``delete`` are added for SQL
#: ``UPDATE`` / ``DELETE`` statements (they change a table without being any of the four).
WRITE_MODES: tuple[str, ...] = ("create", "append", "overwrite", "merge", "update", "delete")
#: SQL dialects ``dataset.statement`` may name.
DIALECTS: tuple[str, ...] = ("spark", "duckdb", "tsql")

_WS = re.compile(r"\s+")
_GUID = re.compile(r"^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$")
_TENANT = re.compile(r"^[A-Za-z0-9][A-Za-z0-9.-]{0,252}$")
_RUN = re.compile(r"^[A-Za-z0-9_.:-]{1,80}$")
_warned: set[str] = set()
_failures: dict[str, int] = {}
_state_lock = threading.Lock()


def normalise_statement(sql: str) -> str:
    """The statement text used for hashing: whitespace runs collapsed to one space, trailing ``;`` and surrounding blanks removed (case and literals are kept)."""
    return _WS.sub(" ", sql or "").strip().rstrip(";").strip()


def statement_hash(sql: str) -> str:
    """``sha1`` hex digest of :func:`normalise_statement` (the identity of an ad-hoc statement; Intelligence stores it as the alias ``query = sha1:<digest>``)."""
    return hashlib.sha1(normalise_statement(sql).encode("utf-8", "replace"), usedforsecurity=False).hexdigest()  # noqa: S324 - an identity, not a security control


def clip_statement(sql: str, limit: int = MAX_STATEMENT_CHARS) -> str:
    """SQL text safe to put into an event: secrets redacted with the platform redaction rules, then cut to ``limit`` characters."""
    text = redact_text(sql or "")
    return text if len(text) <= limit else text[:limit]


def process_descriptor(kind: str, *, item_id: str | None = None, job_id: str | None = None, query_id: str | None = None, name: str | None = None,
                       run_id: str | None = None, fabric: dict[str, Any] | None = None) -> dict[str, Any]:
    """The ``process`` block of a ``dataset.*`` event: what read or wrote the data (all keys present, unknown ones ``None``).

    ``fabric`` (additive, see :func:`fabric_process_block`) is present only for a run that came from Microsoft Fabric: events of every other process are unchanged.
    """
    out: dict[str, Any] = {"kind": kind, "item_id": item_id, "job_id": job_id, "query_id": query_id, "name": str(name)[:255] if name else None, "run_id": run_id}
    if fabric:
        out["fabric"] = fabric
    return out


def dataset_descriptor(kind: str, *, lakehouse_id: str | None = None, name: str | None = None, path: str | None = None, uri: str | None = None,
                       format: str | None = None, fabric: dict[str, Any] | None = None) -> dict[str, Any]:  # noqa: A002 - the payload key is "format"
    """One entry of the ``datasets`` list of a ``dataset.*`` event (all keys present, unknown ones ``None``).

    ``fabric`` (additive, see :func:`fabric_lakehouse_block`) is present only when the lakehouse is the Fabric lakehouse whose OneLake storage the data lives in.
    """
    out: dict[str, Any] = {"kind": kind, "lakehouse_id": lakehouse_id, "name": name, "path": path, "uri": uri, "format": format}
    if fabric:
        out["fabric"] = fabric
    return out


def _guid(value: Any) -> str | None:
    """``value`` as a lower-case GUID, else ``None``."""
    return value.strip().lower() if isinstance(value, str) and _GUID.match(value.strip()) else None


def fabric_process_block(fabric: Any) -> dict[str, Any] | None:
    """The ``process.fabric`` block of a run that came from a Fabric notebook, built from the ``origin.fabric`` record the Fabric adapter stores on the job.

    ``{"tenant_id", "workspace_id", "item_id", "item_name", "item_type", "run_id"}``: the Entra tenant, the Fabric workspace GUID, the notebook's item GUID, its display name and the
    Fabric run id. A value that is missing or not shaped like what it claims to be (a GUID for the workspace and the item) is omitted. ``None`` unless the block says which workspace, notebook id or notebook name the run is (a tenant or a run id alone identifies nothing).

    These are **labels for lineage, never an authorisation input**: the tenant and workspace were checked by the adapter against its allow-list and workspace map, the notebook id is
    what the (authenticated) caller's capsule says.
    """
    if not isinstance(fabric, dict):
        return None
    tenant_raw = fabric.get("tenant_id")
    tenant = tenant_raw.strip().lower() if isinstance(tenant_raw, str) and _TENANT.match(tenant_raw.strip()) else None
    name = fabric.get("notebook_name") or fabric.get("item_name")
    run = fabric.get("run_id")
    out: dict[str, Any] = {"tenant_id": tenant, "workspace_id": _guid(fabric.get("workspace_id")), "item_id": _guid(fabric.get("notebook_id") or fabric.get("item_id")),
                           "item_name": " ".join(name.split())[:255] if isinstance(name, str) and name.strip() else None, "item_type": "Notebook",
                           "run_id": run if isinstance(run, str) and _RUN.match(run) else None}
    out = {k: v for k, v in out.items() if v}
    return out if out.keys() & {"workspace_id", "item_id", "item_name"} else None


def fabric_lakehouse_block(fabric: Any) -> dict[str, str] | None:
    """The ``fabric`` block of a dataset: ``{"workspace_id", "lakehouse_id"}`` GUIDs of the Fabric lakehouse an OLCF lakehouse item is attached to (its ``definition.fabric``);
    ``None`` unless both are GUIDs (a lakehouse recorded by name cannot be turned into a OneLake path)."""
    if not isinstance(fabric, dict):
        return None
    ws, lh = _guid(fabric.get("workspace_id")), _guid(fabric.get("lakehouse_id"))
    return {"workspace_id": ws, "lakehouse_id": lh} if ws and lh else None


def from_catalog_dataset(d: dict[str, Any]) -> dict[str, Any] | None:
    """Translate a catalog-style dataset (``{kind: table|file, lakehouse_id, name | path}``, as the SQL plan and the pipeline activities carry them) to a descriptor.

    Returns ``None`` for kinds that are not tables or files (reports, items ...), which are not datasets in this sense.
    """
    kind = d.get("kind")
    if kind not in DATASET_KINDS:
        return None
    return dataset_descriptor(str(kind), lakehouse_id=d.get("lakehouse_id"), name=d.get("name"), path=d.get("path"), uri=d.get("uri"), format=d.get("format"))


@contextmanager
def best_effort(logger: logging.Logger, what: str) -> Iterator[None]:
    """Run the body; swallow any ``Exception`` (never ``KeyboardInterrupt`` / ``SystemExit``).

    The first failure per ``what`` is logged as a warning (with the traceback at debug level), later ones at debug level only, so a persistently broken subscriber
    does not flood the log. :func:`emission_failures` exposes the counts (tests and diagnostics).
    """
    try:
        yield
    except Exception as exc:  # noqa: BLE001 - emission must never change the outcome of the operation that triggered it
        with _state_lock:
            _failures[what] = _failures.get(what, 0) + 1
            first = what not in _warned
            _warned.add(what)
        if first:
            logger.warning("%s could not be published (further failures of this kind are logged at debug level): %s: %s", what, type(exc).__name__, exc)
        else:
            logger.debug("%s could not be published: %s: %s", what, type(exc).__name__, exc)


def emission_failures() -> dict[str, int]:
    """How many emissions of each kind were swallowed since the process started (a copy)."""
    with _state_lock:
        return dict(_failures)


def reset_emission_state() -> None:
    """Forget the failure counters and the warned-once memory (tests)."""
    with _state_lock:
        _failures.clear()
        _warned.clear()
