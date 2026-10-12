# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Observability of the Fabric adapter: the readiness checklist, counters and the ``GET /metrics`` document.

The numbers answer the operator's questions: *Is Fabric reaching us? What did it send? How often does it work, how long does a round trip take, and why do the failures
fail?* Everything is computed from ``fabric_submissions`` / ``fabric_handshakes``, so it survives restarts and works on every API replica.
"""

from __future__ import annotations

import math
from collections import Counter
from datetime import UTC, datetime, timedelta
from typing import Any
from urllib.parse import urlsplit

from sqlalchemy import func, select

from olcf.core.errors import OlcfError
from olcf.db.util import fetch_all, fetch_one
from olcf.fabric import schema as S
from olcf.fabric.config import FabricConfig

WINDOWS = {"1h": 3600, "24h": 86400, "7d": 7 * 86400}
TERMINAL_OK = ("succeeded",)
TERMINAL_BAD = ("failed", "timed_out")
MAX_ROWS = 20000


def percentile(values: list[float], q: float) -> float | None:
    if not values:
        return None
    s = sorted(values)
    k = (len(s) - 1) * q
    lo, hi = math.floor(k), math.ceil(k)
    if lo == hi:
        return round(s[int(k)], 1)
    return round(s[lo] + (s[hi] - s[lo]) * (k - lo), 1)


def _iso(dt: datetime) -> str:
    return dt.astimezone(UTC).isoformat()


def _pub_sub(r: dict[str, Any] | None) -> dict[str, Any] | None:
    if r is None:
        return None
    return {k: r.get(k) for k in ("id", "received_at", "state", "notebook_name", "notebook_id", "fabric_workspace_id", "job_id", "failure_category", "round_trip_ms")}


def _pub_hs(r: dict[str, Any] | None) -> dict[str, Any] | None:
    if r is None:
        return None
    return {k: r.get(k) for k in ("id", "received_at", "ok", "tenant_id", "fabric_workspace_id", "fabric_workspace_name", "notebook_name", "sdk_version", "latency_ms", "context_keys")}


def _counters(rows: list[dict[str, Any]]) -> dict[str, Any]:
    states = Counter(r["state"] for r in rows)
    terminal = [r for r in rows if r["state"] in TERMINAL_OK + TERMINAL_BAD + ("cancelled",)]
    done = states["succeeded"] + sum(states[s] for s in TERMINAL_BAD)
    return {"received": len(rows), "rejected": states["rejected"], "accepted": len(rows) - states["rejected"], "succeeded": states["succeeded"], "failed": states["failed"],
            "timed_out": states["timed_out"], "cancelled": states["cancelled"], "in_flight": len(rows) - states["rejected"] - len(terminal),
            "success_rate": round(states["succeeded"] / done, 4) if done else None}


def compute_metrics(ctx: Any, window: str = "24h", *, now: datetime | None = None) -> dict[str, Any]:
    if window not in WINDOWS:
        window = "24h"
    now = now or datetime.now(UTC)
    since7 = _iso(now - timedelta(seconds=WINDOWS["7d"]))
    with ctx.db.connect() as conn:
        rows = fetch_all(conn, select(S.fabric_submissions).where(S.fabric_submissions.c.received_at >= since7).order_by(S.fabric_submissions.c.received_at.desc()).limit(MAX_ROWS))
        hs = fetch_all(conn, select(S.fabric_handshakes).where(S.fabric_handshakes.c.received_at >= since7).order_by(S.fabric_handshakes.c.received_at.desc()).limit(MAX_ROWS))
        last_sub = fetch_one(conn, select(S.fabric_submissions).order_by(S.fabric_submissions.c.received_at.desc()).limit(1))
        last_hs = fetch_one(conn, select(S.fabric_handshakes).order_by(S.fabric_handshakes.c.received_at.desc()).limit(1))
        total_sub = conn.execute(select(func.count()).select_from(S.fabric_submissions)).scalar() or 0
        total_hs = conn.execute(select(func.count()).select_from(S.fabric_handshakes)).scalar() or 0

    def within(rs: list[dict[str, Any]], seconds: int) -> list[dict[str, Any]]:
        cut = _iso(now - timedelta(seconds=seconds))
        return [r for r in rs if r["received_at"] >= cut]

    per_window = {w: {**_counters(within(rows, sec)), "handshakes": len(within(hs, sec))} for w, sec in WINDOWS.items()}
    cur = within(rows, WINDOWS[window])
    cur_hs = within(hs, WINDOWS[window])
    lat = [float(r["round_trip_ms"]) for r in cur if r.get("round_trip_ms") is not None and r["state"] in TERMINAL_OK + TERMINAL_BAD]
    errors = Counter(r["failure_category"] or "unknown" for r in cur if r["state"] in ("rejected",) + TERMINAL_BAD + ("cancelled",))
    rejections = Counter((r["error"] or "").split("]")[0].lstrip("[") or "unknown" for r in cur if r["state"] == "rejected")
    nb = Counter()
    nb_names: dict[str, str] = {}
    ws = Counter()
    ws_names: dict[str, str] = {}
    for r in cur:
        if r["notebook_id"]:
            nb[r["notebook_id"]] += 1
            nb_names[r["notebook_id"]] = r["notebook_name"] or r["notebook_id"]
        if r["fabric_workspace_id"]:
            ws[r["fabric_workspace_id"]] += 1
            ws_names[r["fabric_workspace_id"]] = r["fabric_workspace_name"] or r["fabric_workspace_id"]
    ctx_keys: Counter[str] = Counter()
    for h in cur_hs:
        ctx_keys.update(h.get("context_keys") or [])
    accepted = [r for r in cur if r["state"] != "rejected"]
    classes: Counter[str] = Counter()
    for r in accepted:
        for k, v in ((r.get("compat_summary") or {}).get("counts") or {}).items():
            classes[k] += int(v or 0)
    passed = {
        "sdk_versions": dict(Counter(r["sdk_version"] or "unknown" for r in cur)),
        "source_modes": dict(Counter(r["source_mode"] for r in cur)),
        "pipeline_runs": sum(1 for r in cur if r["is_for_pipeline"]),
        "with_default_lakehouse": sum(1 for r in cur if (r.get("default_lakehouse") or {}).get("id") or (r.get("default_lakehouse") or {}).get("name")),
        "parameters_per_submission_avg": round(sum(len(r["parameters"] or {}) for r in cur) / len(cur), 2) if cur else 0,
        "capsule_bytes_avg": int(sum(r["capsule_bytes"] for r in cur) / len(cur)) if cur else 0,
        "capsule_bytes_max": max((r["capsule_bytes"] for r in cur), default=0),
        "context_keys_seen_in_handshakes": dict(ctx_keys.most_common()),
        "compatibility_findings_by_class": dict(classes),
        "tenants": dict(Counter(r["tenant_id"] or "unknown" for r in cur)),
    }
    return {"window": window, "generated_at": _iso(now), "since": _iso(now - timedelta(seconds=WINDOWS[window])),
            "counters": per_window[window], "windows": per_window, "success_rate": per_window[window]["success_rate"],
            "latency_ms": {"n": len(lat), "p50": percentile(lat, 0.50), "p95": percentile(lat, 0.95), "p99": percentile(lat, 0.99), "mean": round(sum(lat) / len(lat), 1) if lat else None},
            "errors_by_category": dict(errors), "rejections_by_code": dict(rejections),
            "last_handshake": _pub_hs(last_hs), "last_submission": _pub_sub(last_sub),
            "top_notebooks": [{"notebook_id": k, "name": nb_names[k], "count": n} for k, n in nb.most_common(10)],
            "top_workspaces": [{"fabric_workspace_id": k, "name": ws_names[k], "count": n} for k, n in ws.most_common(10)],
            "passed_along": passed, "totals": {"submissions": total_sub, "handshakes": total_hs}}


# ===================================================================================== readiness
def readiness(svc: Any, cfg: FabricConfig) -> list[dict[str, Any]]:
    """The "is the integration ready?" checklist shown on the Fabric page. Each item: ``{id, title, ok, required, detail, remediation}``."""
    ctx = svc.ctx
    items: list[dict[str, Any]] = []

    def add(id_: str, title: str, ok: bool, detail: str, remediation: str | None = None, required: bool = True) -> None:
        items.append({"id": id_, "title": title, "ok": ok, "required": required, "detail": detail, "remediation": None if ok else remediation})

    add("enabled", "Integration enabled", cfg.enabled, "enabled" if cfg.enabled else "disabled", "PUT /v1/integrations/fabric with enabled=true")
    add("tenants", "Allowed Entra tenants", bool(cfg.allowed_tenants), f"{len(cfg.allowed_tenants)} tenant(s)" if cfg.allowed_tenants else "none configured",
        "Set allowed_tenants to the Entra tenant id(s) of your Fabric capacity (Entra tokens are rejected until then; platform tokens still work).", required=False)
    add("audiences", "API audience", bool(cfg.audiences), ", ".join(cfg.audiences) or "none configured",
        "Set audiences to the application id URI / client id of the Entra app registration that represents OLCF.", required=False)
    problems = []
    for m in cfg.workspace_map:
        try:
            ctx.service("workspaces").resolve(m.olcf_workspace)
        except OlcfError:
            problems.append(m.olcf_workspace)
    add("workspace_map", "Fabric workspace mapping", bool(cfg.workspace_map) and not problems,
        f"{len(cfg.workspace_map)} mapping(s)" + (f"; unknown OLCF workspace(s): {', '.join(problems)}" if problems else ""),
        "Map each Fabric workspace id to an OLCF workspace (workspace_map). Submissions from unmapped workspaces are rejected.")
    broker, problem = svc.broker(cfg)
    add("broker", "Broker connection (Fabric/OneLake identity)", broker is not None, "ready" if broker else (problem or "not configured"),
        "Create an OLCF connection of type 'onelake' (service principal) and set broker_connection. Needed for pull mode and OneLake data access.", required=False)
    u = urlsplit(svc.public_url())
    add("public_url", "Reachable HTTPS endpoint", u.scheme == "https" and u.hostname not in ("localhost", "127.0.0.1", "::1"), svc.public_url(),
        "Set server.public_url to an https URL that Fabric can reach (reverse proxy, VPN or private endpoint).", required=False)
    with ctx.db.connect() as conn:
        row = fetch_one(conn, select(S.fabric_config.c.signing_secret_id).where(S.fabric_config.c.id == "default"))
    add("signing", "Capsule signing material", bool(row and row.get("signing_secret_id")), "present" if row and row.get("signing_secret_id") else "created on first challenge",
        "It is created automatically the first time an SDK asks for a challenge.", required=False)
    jobs = ctx.maybe_service("jobs")
    if jobs is None:
        add("compute", "Compute module", False, "the compute service is not installed", "Install the compute module.")
    else:
        from olcf.core.models import RuntimeRequest

        try:
            sel = jobs.runtimes.resolve(RuntimeRequest(family=str(cfg.default_runtime.get("family") or "fabric-2.0-compatible")))
            add("runtime", "Compatible runtime installed", True, f"{sel.runtime_id}")
        except OlcfError as exc:
            add("runtime", "Compatible runtime installed", False, exc.message, "Install a runtime of the Fabric Runtime 2.0 Compatible family (Admin > Runtimes).")
    hs = svc.last_handshake()
    ok_hs = False
    detail = "no handshake yet"
    if hs:
        age = (datetime.now(UTC) - datetime.fromisoformat(hs["received_at"])).total_seconds()
        ok_hs = bool(hs["ok"]) and age < 7 * 86400
        detail = f"{'ok' if hs['ok'] else 'failed'}, {int(age)} s ago, SDK {hs.get('sdk_version')}, workspace {hs.get('fabric_workspace_id')}"
    add("handshake", "A Fabric notebook has connected", ok_hs, detail, "Run olcf.test_connection() in a Fabric notebook (see GET /v1/integrations/fabric/snippet).", required=False)
    with ctx.db.connect() as conn:
        ok_n = conn.execute(select(func.count()).select_from(S.fabric_submissions).where(S.fabric_submissions.c.state == "succeeded")).scalar() or 0
    add("first_run", "A notebook has run to completion", ok_n > 0, f"{ok_n} successful submission(s)", "Submit a small notebook with olcf.run_remote().", required=False)
    return items


def counters_summary(ctx: Any) -> dict[str, Any]:
    with ctx.db.connect() as conn:
        by_state = dict(conn.execute(select(S.fabric_submissions.c.state, func.count()).group_by(S.fabric_submissions.c.state)).all())
        hs = conn.execute(select(func.count()).select_from(S.fabric_handshakes)).scalar() or 0
    return {"submissions_by_state": by_state, "submissions_total": sum(by_state.values()), "handshakes_total": hs}


__all__ = ["WINDOWS", "compute_metrics", "counters_summary", "percentile", "readiness"]
