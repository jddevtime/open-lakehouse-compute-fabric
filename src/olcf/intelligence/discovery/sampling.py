# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Sampling policies and the on-demand, policy-limited, masked, audited sample of an asset (design note: "sampling is opt-in").

Rules, all enforced here and nowhere else:

* **Default off.** With no policy row - or ``max_rows = 0`` - nothing is ever sampled and :meth:`SamplingService.sample` raises a clear error that says how to enable it.
  Having the *permission* ``intelligence.sample`` is not enough; a policy must also allow it.
* **Scopes.** A policy is ``global``, per ``workspace``, per discovery ``source`` or per ``asset``; the narrowest scope that has a row wins (an asset row with
  ``max_rows = 0`` switches sampling off for that asset even if the workspace allows it). The row count is clamped to 0..100 whatever a caller asks for.
* **Masking** of every returned cell: ``none`` | ``hash`` (salted, stable within the installation so equal values stay equal) | ``redact`` | ``partial`` (first/last characters, year of
  a date, leading digit of a number).
* **Classified columns are left out** (``exclude_classified``, default true): columns classified in governance (providers registered with
  :meth:`SamplingService.add_classified_provider`), columns carrying a classification in the asset's attributes, and columns that
  :mod:`~olcf.intelligence.discovery.detect` suggests are sensitive.
* **Nothing is stored.** Sample values are read, masked, clipped (:data:`MAX_CELL_CHARS` per cell) and returned; they are not cached, logged or audited. The audit record
  ``intelligence.sample_read`` holds the asset, the row count, the column *names*, the masking mode and the policy scope - never a value.
* **No Spark, no worker.** Local Delta tables are read with delta-rs through the storage service; Fabric tables only with the source's separate ``sampling_identity``
  (delta-rs over the OneLake ``abfss://`` URL, see :func:`fabric_delta_reader`).
"""

from __future__ import annotations

import datetime as dt
import decimal
import hashlib
import hmac
import json
import secrets as _stdsecrets
from collections.abc import Callable
from typing import Any

from sqlalchemy import select

from olcf.core.errors import BadRequest, Forbidden, NotFound, PolicyDenied, Unsupported
from olcf.db.util import fetch_one
from olcf.intelligence.discovery import datapolicy
from olcf.intelligence.discovery.datapolicy import DataReadPolicy
from olcf.intelligence.discovery.sampling_policy import (
    OFF,
    SCOPE_TYPES,
    EffectivePolicy,
    SamplingPolicyStore,
    scope_key,
)
from olcf.intelligence.discovery.schema import MAX_SAMPLE_ROWS

__all__ = ["DEFAULT_SAMPLE_ROWS", "MAX_CELL_CHARS", "OFF", "SCOPE_TYPES", "EffectivePolicy", "SamplingPolicyStore", "SamplingService", "fabric_delta_reader", "mask_value", "scope_key"]

#: Longest cell value (characters) returned by a sample; longer values are cut and end with an ellipsis.
MAX_CELL_CHARS = 200
#: Rows returned when the caller does not say how many.
DEFAULT_SAMPLE_ROWS = 10
_SALT_KEY = "intelligence.sampling.hash_salt"


# --------------------------------------------------------------------------------------------- masking (pure)
def mask_value(value: Any, mode: str, *, salt: bytes = b"", column: str = "") -> Any:
    """Mask one cell according to ``mode`` (``none`` | ``hash`` | ``redact`` | ``partial``). ``None`` stays ``None`` (a null reveals nothing). Result is JSON-safe and clipped to
    :data:`MAX_CELL_CHARS`."""
    if value is None:
        return None
    if mode == "none":
        return _clip(_plain(value))
    if mode == "redact":
        return "***"
    if mode == "hash":
        digest = hmac.new(salt or b"olcf", f"{column}\x1f{_text(value)}".encode(), hashlib.sha256).hexdigest()
        return f"h:{digest[:16]}"
    if mode == "partial":
        return _partial(value)
    raise ValueError(f"unknown masking mode {mode!r}")


def _text(value: Any) -> str:
    if isinstance(value, (bytes, bytearray)):
        return value.hex()
    if isinstance(value, (dt.datetime, dt.date)):
        return value.isoformat()
    if isinstance(value, (dict, list, tuple)):
        return json.dumps(value, sort_keys=True, default=str)
    return str(value)


def _plain(value: Any) -> Any:
    """JSON-safe form of a cell value (no masking)."""
    if isinstance(value, bool) or value is None:
        return value
    if isinstance(value, int):
        return value if abs(value) <= 2**53 - 1 else str(value)
    if isinstance(value, float):
        return value if value == value and value not in (float("inf"), float("-inf")) else None
    if isinstance(value, decimal.Decimal):
        return str(value)
    if isinstance(value, (bytes, bytearray)):
        return f"<{len(value)} bytes>"
    return _text(value)


def _clip(value: Any) -> Any:
    if isinstance(value, str) and len(value) > MAX_CELL_CHARS:
        return value[: MAX_CELL_CHARS - 1] + "…"
    return value


def _partial(value: Any) -> str:
    if isinstance(value, bool):
        return "***"
    if isinstance(value, (dt.datetime, dt.date)):
        return f"{value.year:04d}-**-**"
    if isinstance(value, (bytes, bytearray)):
        return f"<{len(value)} bytes>"
    s = _text(value)
    if isinstance(value, (int, float, decimal.Decimal)):
        digits = s.lstrip("-")
        return ("-" if s.startswith("-") else "") + digits[0] + "*" * min(len(digits) - 1, 12) if digits else "***"
    if len(s) <= 4:
        return "*" * len(s)
    return _clip(s[0] + "*" * min(len(s) - 2, 24) + s[-1])


# --------------------------------------------------------------------------------------------- policies (see sampling_policy.py)
# --------------------------------------------------------------------------------------------- the service
FabricReader = Callable[[dict[str, Any], dict[str, Any], dict[str, Any], int, list[str]], Any]


def fabric_delta_reader(source: dict[str, Any], identity: dict[str, Any], attrs: dict[str, Any], rows: int, columns: list[str]) -> Any:
    """Read ``rows`` rows of a Fabric lakehouse Delta table with delta-rs, authenticating as the source's ``sampling_identity`` (``abfss://<workspace>@onelake.dfs.fabric.microsoft.com/<lakehouse>/Tables/<table>``;
    storage options ``azure_client_id`` / ``azure_client_secret`` / ``azure_tenant_id`` and ``use_fabric_endpoint``). Returns a ``pyarrow.Table``.

    Needs a client-secret identity (delta-rs' Azure backend takes a secret, not a certificate) and a OneLake data-plane role for the identity.
    """
    secret = identity.get("client_secret")
    if not secret:
        raise Unsupported("Fabric sampling needs a sampling identity with a client secret (certificate identities cannot be passed to the Delta reader)",
                          remediation="store a client_secret credential for the sampling identity of this source")
    location = attrs.get("location")
    if not location:
        raise Unsupported("the table has no OneLake location recorded by the scan", remediation="run a full scan of the source")
    from deltalake import DeltaTable

    options = {"azure_client_id": identity["client_id"], "azure_tenant_id": identity["tenant_id"], "azure_client_secret": str(secret), "use_fabric_endpoint": "true"}
    table = DeltaTable(str(location), storage_options=options)
    return table.to_pyarrow_dataset().scanner(columns=columns or None).head(rows)


def _column_excluded(column: str) -> PolicyDenied:
    return PolicyDenied(f"column '{column}' is excluded from sampling (classified or sensitive)", details={"reason": "column_excluded"},
                        remediation="Turn off exclude_classified in the sampling policy if this column may be shown, or sample another column.")


class SamplingService:
    """``service.discovery.sampling``: policy-limited, masked, audited sample rows of an asset. See the module docstring for the rules.

    Every decision about *whether* values may be read, *which* policy applies and *which columns* are left out is taken by the shared :attr:`access`
    (:class:`~olcf.intelligence.discovery.datapolicy.DataReadPolicy`), which value profiling during a scan uses as well."""

    def __init__(self, ctx: Any, registry: Any, store: SamplingPolicyStore | None = None) -> None:
        self.ctx = ctx
        self.registry = registry
        self.policies = store or SamplingPolicyStore(ctx)
        #: the shared data-read policy service (sampling and automatic value profiling both ask it)
        self.access = DataReadPolicy(registry, self.policies)
        #: replaceable reader for Fabric tables (tests inject a local one); see :func:`fabric_delta_reader`
        self.fabric_reader: FabricReader = fabric_delta_reader
        self._salt: bytes | None = None

    # ---- extension points
    def add_classified_provider(self, provider: Callable[[str], set[str]]) -> None:
        """Register ``provider(asset_id) -> {lower-case column names classified in governance}``. Governance calls this when it installs; sampling and value profiling exclude those columns."""
        self.access.add_classified_provider(provider)

    def _hash_salt(self) -> bytes:
        if self._salt is None:
            from olcf.pipelines.common import kv_get, kv_set_if_absent

            kv_set_if_absent(self.ctx, _SALT_KEY, _stdsecrets.token_hex(16))
            self._salt = str(kv_get(self.ctx, _SALT_KEY, "olcf")).encode()
        return self._salt

    # ---- the operation
    def sample(self, principal: Any, asset_id: str, rows: int | None = None, *, columns: list[str] | None = None, audit: bool = True) -> dict[str, Any]:
        """Return up to ``rows`` masked rows of a table (or one column of it) asset; ``columns`` narrows the answer to those columns (excluded ones are still refused).
        ``audit=False`` is used by the Intelligence service, which records ``intelligence.sample_read`` itself (so a call is never audited twice).

        Order of checks: the asset exists and is visible (hidden existence: ``NotFound``), the caller holds ``intelligence.sample`` for its workspace, the *table's* sampling policy allows rows
        (a column asset is resolved through its parent table: it inherits the table's restriction), the asset is something that can be sampled, and the reader can open it. Raises
        :class:`~olcf.core.errors.PolicyDenied` (403 ``policy_denied``) with remediation when the policy is off - before anything is read.

        The answer holds exactly the requested, permitted columns: a column asset yields that one column whether or not the scan recorded the table's schema; a column the table does not have is a
        ``BadRequest``, never a widening to the whole table.
        """
        row = self.registry.get(asset_id)
        if row is None or row["status"] == "placeholder":
            raise NotFound(f"asset '{asset_id}' does not exist")
        ws = row.get("workspace_id")
        if not (principal.can("intelligence.read", workspace_id=ws) and (ws is not None or principal.visible_workspaces("intelligence.read") is None)):
            raise NotFound(f"asset '{asset_id}' does not exist")
        principal.require("intelligence.sample", workspace_id=ws)
        requested = DEFAULT_SAMPLE_ROWS if rows is None else rows
        if isinstance(requested, bool) or not isinstance(requested, int) or requested < 1:
            raise BadRequest("rows must be a positive integer")
        table_row, only_column = self._table_of(row)
        attrs = table_row.get("attrs") or {}
        decision = self.access.decide(workspace_id=table_row.get("workspace_id"), source_id=table_row.get("source_id"), table_asset_ids=[table_row["id"]],
                                      column_asset_id=asset_id if only_column is not None else None)
        policy = decision.policy
        if not decision.allowed:
            if decision.code == datapolicy.CLASSIFICATION_UNAVAILABLE:
                raise Unsupported("the classification lookup failed, so the sample cannot be checked for classified columns",
                                  remediation="retry; sampling stays blocked while the exclusion list is unknown")
            raise PolicyDenied(f"sampling is disabled for '{table_row['name']}': {policy.reason}", details={"reason": "sampling_disabled", "policy": policy.to_dict()},
                               remediation="An administrator can allow sampling with PUT /v1/intelligence/sampling-policies (max_rows 1..100, masking, scope); the default is off.")
        n = min(requested, policy.max_rows, MAX_SAMPLE_ROWS)
        declared = [c for c in (attrs.get("columns") or []) if isinstance(c, dict) and c.get("name")]
        target = self._target(only_column, columns, declared, table_row)
        keep = self._plan(decision, declared, target, only_column, table_row)
        table, skipped = self._read(table_row, n, keep, decision, target, only_column)
        table, dropped = self.access.project(table, decision, declared=declared, requested=target, require=only_column)
        if not table.column_names:
            raise BadRequest(f"every column of '{table_row['name']}' is excluded from sampling (classified or sensitive)", details={"reason": "all_columns_excluded"},
                             remediation="Set exclude_classified=false in the sampling policy if the table's columns may be shown.")
        excluded_declared = decision.excluded_in(declared)
        excluded_names = sorted(set(dropped) | set(skipped) | {str(c["name"]) for c in declared if str(c["name"]).lower() in excluded_declared and (target is None or str(c["name"]).lower() in target)})
        salt = self._hash_salt() if policy.masking == "hash" else b""
        data = table.to_pylist()[:n]
        out_rows = [{c: mask_value(r.get(c), policy.masking, salt=salt, column=c) for c in table.column_names} for r in data]
        types = {str(c["name"]): c.get("type") for c in declared}
        result = {"asset_id": asset_id, "table": table_row["name"], "columns": [{"name": c, "type": types.get(c) or str(table.schema.field(c).type)} for c in table.column_names],
                  "rows": out_rows, "row_count": len(out_rows), "excluded_columns": excluded_names,
                  "masking": policy.masking, "max_rows": policy.max_rows, "policy_scope": policy.scope, "truncated": len(data) >= n,
                  "note": "values are masked per the sampling policy and are not stored anywhere"}
        if audit:
            self.ctx.audit.record(principal, "intelligence.sample_read", resource_type=row["type"], resource_id=asset_id, workspace_id=ws,
                                  details={"rows": len(out_rows), "requested": requested, "columns": list(table.column_names), "excluded_columns": len(result["excluded_columns"]),
                                           "masking": policy.masking, "policy_scope": policy.scope, "asset_name": table_row["name"]})
        return result

    # ---- helpers
    def _table_of(self, row: dict[str, Any]) -> tuple[dict[str, Any], str | None]:
        """The table row to read for an asset and, for a column asset, the column name."""
        if row["type"] in ("table", "view", "model_table"):
            return row, None
        if row["type"] == "column" and row.get("parent_id"):
            parent = self.registry.get(row["parent_id"])
            if parent is not None and parent["type"] in ("table", "view"):
                return parent, row["name"]
        raise BadRequest(f"a {row['type']} cannot be sampled; only tables, views and their columns can", details={"reason": "not_sampleable"})

    @staticmethod
    def _target(only_column: str | None, columns: list[str] | None, declared: list[dict[str, Any]], table_row: dict[str, Any]) -> set[str] | None:
        """The lower-case column names the caller asked for (``None`` = the whole table). A column asset narrows to its own column; ``columns`` may only repeat that column."""
        wanted = {str(c).lower() for c in columns} if columns else None
        if only_column is not None:
            own = only_column.lower()
            if wanted is not None and wanted - {own}:
                raise BadRequest(f"unknown column(s) {sorted(wanted - {own})} for '{table_row['name']}'")
            return {own}
        if wanted is not None and declared:
            unknown = sorted(w for w in wanted if w not in {str(c["name"]).lower() for c in declared})
            if unknown:
                raise BadRequest(f"unknown column(s) {unknown} for '{table_row['name']}'")
        return wanted

    @staticmethod
    def _plan(decision: datapolicy.ReadDecision, declared: list[dict[str, Any]], target: set[str] | None, only_column: str | None, table_row: dict[str, Any]) -> list[str] | None:
        """The columns to ask the reader for when the scan recorded the schema (``None`` = the schema is unknown; the reader schema decides after the read). Refuses before reading when
        the requested column is excluded or nothing would be left."""
        if not declared:
            return None
        excluded = decision.excluded_in(declared)
        names = [str(c["name"]) for c in declared if target is None or str(c["name"]).lower() in target]
        if only_column is not None and only_column.lower() in excluded:
            raise _column_excluded(only_column)
        keep = [c for c in names if c.lower() not in excluded]
        if not keep:
            raise BadRequest(f"every column of '{table_row['name']}' is excluded from sampling (classified or sensitive)", details={"reason": "all_columns_excluded"},
                             remediation="Set exclude_classified=false in the sampling policy if the table's columns may be shown.")
        return keep

    def _read(self, table_row: dict[str, Any], n: int, columns: list[str] | None, decision: datapolicy.ReadDecision, target: set[str] | None, only_column: str | None = None) -> tuple[Any, list[str]]:
        attrs = table_row.get("attrs") or {}
        kind = attrs.get("source_kind") or ("local" if attrs.get("lakehouse_id") else None)
        if kind == "local":
            storage = self.ctx.service("storage")
            lh = storage.get_lakehouse(str(attrs["lakehouse_id"]))
            dataset = storage.tables.scan_source(lh, str(attrs.get("table_name") or table_row["name"]))
            physical = [{"name": str(f.name), "type": str(f.type)} for f in dataset.schema]
            by_lower = {p["name"].lower(): p["name"] for p in physical}
            skipped = sorted(p["name"] for p in physical if p["name"].lower() in decision.excluded_in(physical) and (target is None or p["name"].lower() in target))     # never opened
            if columns is None:
                # the scan recorded no schema: plan the projection from the reader's schema, before any data is read
                excluded = decision.excluded_in(physical)
                if only_column is not None and only_column.lower() in excluded:
                    raise _column_excluded(only_column)
                columns = [p["name"] for p in physical if (target is None or p["name"].lower() in target) and p["name"].lower() not in excluded]
                missing = sorted(t for t in (target or set()) if t not in by_lower and t not in excluded)
            else:
                # the scan recorded a schema: the table may have changed since - keep what still exists and say so when the caller asked for what is gone
                missing = sorted(t for t in (target or set()) if t not in by_lower)
                columns = [by_lower[c.lower()] for c in columns if c.lower() in by_lower]
            if missing:
                raise BadRequest(f"unknown column(s) {missing} for '{table_row['name']}'", details={"reason": "unknown_column"})
            if not columns:
                raise BadRequest(f"every column of '{table_row['name']}' is excluded from sampling (classified or sensitive)", details={"reason": "all_columns_excluded"},
                                 remediation="Set exclude_classified=false in the sampling policy if the table's columns may be shown.")
            return dataset.scanner(columns=columns).head(n), skipped
        if kind == "fabric":
            return self._read_fabric(table_row, attrs, n, columns or []), []
        raise Unsupported(f"'{table_row['name']}' was not discovered from a source that can be sampled", details={"reason": "not_sampleable"},
                          remediation="only tables of the local platform and of Fabric sources with a sampling identity can be sampled")

    def _read_fabric(self, table_row: dict[str, Any], attrs: dict[str, Any], n: int, columns: list[str]) -> Any:
        from olcf.intelligence.discovery.identities import resolve_secrets
        from olcf.intelligence.discovery.schema import discovery_sources

        sid = table_row.get("source_id")
        with self.ctx.db.connect() as c:
            source = fetch_one(c, select(discovery_sources).where(discovery_sources.c.id == sid)) if sid else None
        if source is None or "sampling_identity" not in (source.get("config") or {}):
            raise Unsupported("this source has no sampling identity, so Fabric data cannot be read (metadata discovery never reads data)", details={"reason": "no_sampling_identity"},
                              remediation="configure config.sampling_identity (a separate service principal with a OneLake data role) on the source")
        res = resolve_secrets(self.ctx, source)
        ident = res.get("sampling_identity")
        if ident is None:
            raise Unsupported(f"the sampling identity could not be resolved: {res.errors.get('sampling_identity', 'unknown reason')}", details={"reason": "no_sampling_identity"})
        try:
            return self.fabric_reader(source, ident, attrs, n, columns)
        except (Unsupported, Forbidden, BadRequest, NotFound):
            raise
        except Exception as exc:  # noqa: BLE001
            from olcf.intelligence.discovery.base import safe_message

            raise Unsupported(f"the Fabric table could not be read with the sampling identity ({type(exc).__name__}: {safe_message(exc)})", details={"reason": "read_failed"},
                              remediation="check the identity's OneLake data access role on the lakehouse (POST /v1/intelligence/sources/{id}/test, step 8)") from None
