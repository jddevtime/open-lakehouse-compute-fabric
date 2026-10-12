# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Runtime settings of the Intelligence module (``olcf settings``, ``PUT /v1/admin/settings/{key}``).

The static (process) settings live in :class:`olcf.core.config.IntelligenceSettings`; the values here are stored in the database and change while the server
runs. Every registered key has a description, a default and a bounds validator. :func:`get` reads one with a fallback so the module also works when the
settings service is absent.
"""

from __future__ import annotations

from typing import Any

from olcf.core.errors import BadRequest

#: (key, description, default, (min, max))
SETTINGS: list[tuple[str, str, int | float, tuple[float, float]]] = [
    ("intelligence.retention_days", "Days to keep processed rows of the Intelligence event inbox", 30, (1, 3650)),
    ("intelligence.verified_evidence_days", "Days a run-time observation keeps an edge 'verified' once the code stops showing it (older run-time evidence is dropped)", 90, (1, 3650)),
    ("intelligence.max_lineage_nodes", "Most nodes a lineage, origin or impact answer contains before it is marked truncated", 500, (10, 20000)),
    ("intelligence.max_impact_nodes", "Most downstream assets an impact analysis counts before its totals are marked truncated (the lists stay capped by max_lineage_nodes)", 20000, (100, 500000)),
    ("intelligence.max_versions_per_asset", "Attribute versions (history entries) kept per asset; older ones are trimmed by the hourly housekeeping", 200, (10, 100000)),
    ("intelligence.inbox_retry_after_minutes", "Minutes after which a failed event-inbox row is queued once more (0 = never retry automatically)", 30, (0, 10080)),
    ("intelligence.write_batch_max_rows", "Most graph write transactions one writer thread commits back to back before it pauses (SQLite only; 0 = no row limit). Bounds how long a scan can monopolise the single SQLite writer", 500, (0, 1000000)),
    ("intelligence.write_batch_max_ms", "Most milliseconds one writer thread writes to the graph without a pause (SQLite only; 0 = no time limit). Other writers - job states, the event bus - wait at most about this long plus the pause", 250, (0, 60000)),
    ("intelligence.write_yield_ms", "Milliseconds a graph writer pauses after a full batch so waiting writers can take the SQLite write lock (0 = never pause). Should be at least SQLite's longest retry sleep, 100 ms", 100, (0, 10000)),
    ("intelligence.default_unused_days", "Default 'no read within N days' window of the unused-tables report", 30, (0, 3650)),
    ("intelligence.stale_after_hours", "Default age after which a producing dataset counts as stale in the staleness analysis", 24, (1, 87600)),
    ("intelligence.local_scan_interval_seconds", "Seconds between incremental scans of the local OLCF source (0 = only on demand)", 900, (0, 604800)),
    ("intelligence.fabric_full_scan_interval_seconds", "Seconds between full scans of a Microsoft Fabric / Power BI source (0 = only on demand)", 86400, (0, 2592000)),
    ("intelligence.fabric_incremental_interval_seconds", "Seconds between incremental (modified-since) scans of a Microsoft Fabric / Power BI source (0 = only on demand)", 3600, (0, 604800)),
    ("intelligence.schema_check_interval_seconds", "Seconds between table schema checks that detect added, removed and retyped columns (0 = only on events)", 300, (0, 604800)),
]

#: Boolean runtime setting: may a Fabric / Power BI discovery source contact loopback and private-network endpoints (and plain ``http`` to them)? Off by default.
ALLOW_PRIVATE_ENDPOINTS = "intelligence.allow_private_endpoints"
ALLOW_PRIVATE_ENDPOINTS_HELP = ("Let Microsoft Fabric / Power BI discovery sources contact loopback and private-network endpoints (and plain http to them) through config.network overrides or a "
                                "proxy. WARNING: the source's service principal credential is sent to those endpoints, so enable it only for private-link gateways or test doubles you "
                                "control. Cloud metadata and link-local addresses stay blocked either way. Default false")


#: The write pacing keys read by :class:`olcf.intelligence.engine.WritePacer` (rows, milliseconds of continuous writing, pause in milliseconds) and their defaults.
WRITE_PACING = (("intelligence.write_batch_max_rows", 500), ("intelligence.write_batch_max_ms", 250), ("intelligence.write_yield_ms", 100))


def _bounded(lo: float, hi: float, integer: bool):  # noqa: ANN202
    def check(v: Any) -> Any:
        if isinstance(v, bool) or not isinstance(v, (int, float)) or (integer and not isinstance(v, int)) or not lo <= v <= hi:
            raise BadRequest(f"must be {'an integer' if integer else 'a number'} between {lo:g} and {hi:g}")
        return v

    return check


def _flag(v: Any) -> bool:
    if not isinstance(v, bool):
        raise BadRequest("must be true or false")
    return v


def register(settings: Any) -> None:
    """Register the Intelligence runtime settings with the platform settings service."""
    for key, desc, default, (lo, hi) in SETTINGS:
        settings.register(key, description=desc, default=default, validator=_bounded(lo, hi, isinstance(default, int)))
    settings.register(ALLOW_PRIVATE_ENDPOINTS, description=ALLOW_PRIVATE_ENDPOINTS_HELP, default=False, validator=_flag)


def get(ctx: Any, key: str, default: Any) -> Any:
    """Current value of a runtime setting; ``default`` when the setting is unknown, unset or the settings service is not installed."""
    try:
        st = ctx.maybe_service("settings")
        if st is not None and key in st.registry:
            v = st.get(key)
            return default if v is None else v
    except Exception:  # noqa: BLE001 - a settings hiccup never breaks the graph
        pass
    return default
