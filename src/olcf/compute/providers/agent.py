# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Shared behaviour of providers whose application runs inside an OLCF runner on a worker node (standalone, local, external)."""

from __future__ import annotations

import base64
import hashlib
from typing import Any

from sqlalchemy import select

from olcf.compute import schema as C
from olcf.compute.providers.base import AppHandle, AppStatus, ComputeProvider, LogsPage, SubmitRequest, UiInfo
from olcf.compute.registry import ACTIVE_JOB_STATES
from olcf.core.errors import NotFound
from olcf.db.util import fetch_all, fetch_one

_JOB_TO_APP = {
    "accepted": "pending", "validating": "pending", "queued": "pending", "provisioning": "pending", "running": "running",
    "cancel_requested": "running", "succeeded": "succeeded", "failed": "failed", "timed_out": "failed", "cancelled": "cancelled",
}


class AgentExecutedProvider(ComputeProvider):
    """``submit`` ships ``run_job`` to the driver node's agent; status/logs/ui come from controller state."""

    @property
    def execution(self) -> str:
        return "agent"

    def submit(self, req: SubmitRequest) -> AppHandle:
        if not req.worker_id:
            raise NotFound("no driver worker was selected for this job")
        payload: dict[str, Any] = {"spec": req.spec}
        if req.source_bytes is not None:
            try:
                content, enc = req.source_bytes.decode("utf-8"), "utf8"
            except UnicodeDecodeError:
                content, enc = base64.b64encode(req.source_bytes).decode(), "base64"
            payload["source"] = {"path": req.spec["source"]["path"], "encoding": enc, "content": content,
                                 "sha256": hashlib.sha256(req.source_bytes).hexdigest()}
        # run_job is keyed by (job_id, attempt) on the agent: re-sending the same attempt never starts a second process
        self.env.hub.request(req.worker_id, "run_job", payload, timeout=req.timeout)
        return AppHandle(self.name, req.job["id"], int(req.job["attempt"]), worker_id=req.worker_id, cluster_id=req.cluster_id)

    def status(self, handle: AppHandle) -> AppStatus:
        with self.env.ctx.db.connect() as conn:
            row = fetch_one(conn, select(C.jobs).where(C.jobs.c.id == handle.job_id))
        if not row:
            return AppStatus("unknown", handle.application_id, "job not found", job_id=handle.job_id)
        return AppStatus(_JOB_TO_APP.get(row["state"], "unknown"), row["application_id"], row.get("state_reason") or "", resources=row["resources_granted"] or {}, job_id=row["id"],
                         native={"state": row["state"], "worker_id": row["driver_worker_id"]})

    def logs(self, handle: AppHandle, offset: int = 0, limit: int = 65536, *, cursor: Any = None) -> LogsPage:
        page = self.env.logs.read(handle.job_id, offset, limit)
        return LogsPage(page.data, page.next_offset, page.eof)

    def cancel(self, handle: AppHandle, *, force: bool = False, grace_seconds: int = 15) -> None:
        if not handle.worker_id:
            return
        self.env.hub.request(handle.worker_id, "cancel_job", {"job_id": handle.job_id, "attempt": handle.attempt, "grace_seconds": 0 if force else grace_seconds, "force": force},
                             timeout=max(10.0, grace_seconds + 10.0))

    def ui(self, handle: AppHandle) -> UiInfo:
        with self.env.ctx.db.connect() as conn:
            row = fetch_one(conn, select(C.spark_apps).where(C.spark_apps.c.job_id == handle.job_id).order_by(C.spark_apps.c.created_at.desc()).limit(1))
        if not row or not row.get("ui_url"):
            return UiInfo(None, False, "the application has not published a UI address yet (or has finished)")
        return UiInfo(row["ui_url"], row["state"] in ("RUNNING", "WAITING"), proxy_hint=f"/v1/spark-applications/{row['id']}/ui/")

    def reconcile(self) -> list[AppStatus]:
        """What the connected agents say is running (the first half of post-restart recovery)."""
        out: list[AppStatus] = []
        with self.env.ctx.db.connect() as conn:
            rows = fetch_all(conn, select(C.jobs.c.id, C.jobs.c.application_id, C.jobs.c.driver_worker_id, C.jobs.c.provider).where(
                C.jobs.c.state.in_(ACTIVE_JOB_STATES), C.jobs.c.provider == self.name))
            workers = {w["id"]: w for w in fetch_all(conn, select(C.workers.c.id, C.workers.c.active_jobs, C.workers.c.connected))}
        for r in rows:
            w = workers.get(r["driver_worker_id"] or "")
            reported = {a.get("job_id") for a in (w or {}).get("active_jobs") or []}
            out.append(AppStatus("running" if r["id"] in reported else "unknown", r["application_id"], "reported by agent" if r["id"] in reported else "not reported by agent", job_id=r["id"]))
        return out
