# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Job reconciliation after a restore into a clean environment (CONTRACTS addendum L5).

A backup is a snapshot of the control-plane database. The processes that belong to its queued and running jobs live on worker machines, not in the snapshot: after a restore
into a new environment no worker is connected and none of those processes exists. Left alone, such a job would stay ``running`` until the worker-loss grace period, or - for a
queued job whose source file lives in the data directory, which is not part of the backup - fail late and obscurely at launch.

:func:`reconcile_restored_jobs` makes the outcome explicit and immediate: every job that was not in a terminal state at backup time and has **no live worker attempt** becomes
``failed`` with the failure category ``restore_reconcile`` (a reason on the job and a timeline event), its open attempts are closed and its job-scoped credential is revoked.
The report says which jobs were failed (by the state they were in) and which were left alone, so the operator can resubmit what matters (``olcf job retry <id>``).

A *live* attempt is one on a worker that is connected right now (``live_workers`` - only meaningful for an online restore on a running server; the offline CLI restore and the
throw-away restore-test have no live workers) and, for jobs run by a remote provider (Kubernetes, Livy), the provider's own monitor keeps ownership: those are left as they are
and reported as ``left_to_provider``.
"""

from __future__ import annotations

from collections.abc import Collection
from typing import Any

from sqlalchemy import select, update

from olcf.core.errors import FailureCategory
from olcf.core.timeutil import now_iso
from olcf.db import schema as S
from olcf.db.engine import Database
from olcf.db.util import fetch_all

CATEGORY = FailureCategory.RESTORE_RECONCILE.value
NON_TERMINAL = ("accepted", "validating", "queued", "provisioning", "running", "cancel_requested")
#: providers that run the job through an agent on a worker; any other provider with a handle that was ``sent`` is a remote system the provider monitor looks up
AGENT_PROVIDERS = ("local", "standalone")
MAX_LISTED = 200


def empty_report() -> dict[str, Any]:
    return {"applied": False, "category": CATEGORY, "considered": 0, "failed": 0, "kept": 0, "by_state": {}, "jobs": [], "left_to_provider": [], "live_workers": 0, "truncated": False, "events_added": 0,
            "message": "no jobs were queued or running at backup time"}


def reconcile_restored_jobs(db: Database, *, live_workers: Collection[str] = (), backup_id: str | None = None, apply: bool = True) -> dict[str, Any]:
    """Fail every non-terminal job without a live worker attempt. Idempotent. ``apply=False`` only reports what would happen."""
    from sqlalchemy import inspect

    from olcf.compute import schema as C

    report = empty_report()
    report["live_workers"] = len(live_workers)
    with db.connect() as conn:
        if "jobs" not in set(inspect(conn).get_table_names()):
            report["message"] = "this installation has no compute tables"
            return report
        rows = fetch_all(conn, select(C.jobs).where(C.jobs.c.state.in_(NON_TERMINAL)).order_by(C.jobs.c.created_at.asc()))
        attempts = fetch_all(conn, select(C.job_attempts).where(C.job_attempts.c.job_id.in_([r["id"] for r in rows]))) if rows else []
    live = set(live_workers)
    by_job: dict[str, list[dict[str, Any]]] = {}
    for a in attempts:
        by_job.setdefault(a["job_id"], []).append(a)
    report["applied"] = bool(apply)
    report["considered"] = len(rows)
    by_state: dict[str, int] = {}
    for row in rows:
        mine = by_job.get(row["id"], [])
        current = next((a for a in mine if a["attempt"] == row["attempt"]), None)
        handle = row.get("provider_handle") or {}
        if row["provider"] and row["provider"] not in AGENT_PROVIDERS and handle.get("sent"):
            report["left_to_provider"].append({"id": row["id"], "name": row["name"], "provider": row["provider"], "state": row["state"]})
            report["kept"] += 1
            continue
        if row["driver_worker_id"] in live and current is not None and current["worker_id"] == row["driver_worker_id"] and current["state"] in ("provisioning", "running"):
            report["kept"] += 1
            continue
        by_state[row["state"]] = by_state.get(row["state"], 0) + 1
        report["failed"] += 1
        if len(report["jobs"]) < MAX_LISTED:
            report["jobs"].append({"id": row["id"], "name": row["name"], "workspace_id": row["workspace_id"], "state_at_backup": row["state"], "attempt": row["attempt"],
                                   "worker_id": row["driver_worker_id"], "queue": row["queue"]})
        else:
            report["truncated"] = True
        if apply and _fail(db, row, mine, backup_id):
            report["events_added"] += 2  # a state change and a ``restore_reconcile`` event on the job's timeline
    report["by_state"] = by_state
    if report["failed"]:
        parts = ", ".join(f"{n} {s}" for s, n in sorted(by_state.items()))
        report["message"] = (f"{report['failed']} job(s) were not finished when the backup was taken ({parts}) and have no live worker attempt: "
                             f"{'marked failed' if apply else 'would be marked failed'} with category {CATEGORY}; resubmit what is still needed (olcf job retry <id>)")
    elif rows:
        report["message"] = f"{len(rows)} unfinished job(s) at backup time; all of them still have a live owner"
    return report


def _fail(db: Database, row: dict[str, Any], attempts: list[dict[str, Any]], backup_id: str | None) -> bool:
    from olcf.compute import schema as C

    now = now_iso()
    origin = f" from backup {backup_id}" if backup_id else ""
    reason = f"restored{origin}: the job was {row['state']} when the backup was taken and no worker attempt of it is alive in this environment"
    failure = {"category": CATEGORY, "message": reason, "state_at_backup": row["state"], "worker_id": row["driver_worker_id"], "attempt": row["attempt"],
               "remediation": "resubmit the job if its result is still needed (olcf job retry <id>); results of work that was in flight at backup time were not restored"}
    tokens = [a["token_id"] for a in attempts if a.get("token_id")]
    with db.begin() as conn:
        n = conn.execute(update(C.jobs).where(C.jobs.c.id == row["id"], C.jobs.c.state == row["state"]).values(
            state="failed", state_reason=reason[:512], failure=failure, failure_category=CATEGORY, finished_at=now, updated_at=now, wait_reason=None, not_before=None, lost_since=None)).rowcount
        if not n:
            return False  # something else moved the job meanwhile
        conn.execute(C.job_events.insert().values(job_id=row["id"], ts=now, kind="state", from_state=row["state"], to_state="failed", reason=reason[:512], data={"restore_reconcile": True}))
        conn.execute(C.job_events.insert().values(job_id=row["id"], ts=now, kind="event", event="restore_reconcile", reason=reason[:512],
                                                  data={"state_at_backup": row["state"], "backup_id": backup_id, "worker_id": row["driver_worker_id"], "attempt": row["attempt"]}))
        for a in attempts:
            if a["state"] not in ("succeeded", "failed", "cancelled", "timed_out", "lost"):
                conn.execute(update(C.job_attempts).where(C.job_attempts.c.id == a["id"]).values(state="failed", failure=failure, finished_at=now))
        if tokens:  # the job-scoped credential of a dead attempt must not outlive it
            conn.execute(update(S.api_tokens).where(S.api_tokens.c.id.in_(tokens), S.api_tokens.c.revoked_at.is_(None)).values(revoked_at=now))
    return True
