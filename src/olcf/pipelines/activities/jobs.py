# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Job-backed activities: ``notebook``, ``spark_job`` and ``script``.

All three submit a real job through the CTRL ``jobs`` service (``origin.kind = pipeline``), so they get queueing, placement, runtime resolution,
compatibility analysis, logs and cancellation exactly like jobs submitted through the API. They are *asynchronous*: ``start`` submits and persists
the job id in the activity state, ``poll`` reads the job row. Nothing is held while the job runs, a restarted control plane re-attaches by job id,
and the idempotency key ``pl:<activity_run_id>:<attempt>`` guarantees one job per attempt even if the control plane dies between "submit" and
"remember the job id".
"""

from __future__ import annotations

import json
from typing import Any

from olcf.core.errors import Conflict, NotFound, OlcfError
from olcf.core.models import (
    JobKind,
    JobSource,
    JobSubmission,
    LakehouseRef,
    Origin,
    OriginKind,
    PolicySpec,
    ResourceRequest,
    RuntimeRequest,
)
from olcf.core.redaction import redact_obj
from olcf.pipelines.activities.base import (
    Activity,
    ActivityCancelled,
    ActivityContext,
    ActivityError,
    ValidationIssue,
    obj,
    s_int,
    s_obj,
    s_str,
)

MAX_SCRIPT_BYTES = 1024 * 1024
MAX_RESULT_BYTES = 256 * 1024
NON_RETRYABLE = {"compatibility", "authn", "authz", "cancellation"}
_COMMON = {
    "parameters": s_obj(), "engine": {"enum": ["python", "spark"]}, "lakehouse": s_str(), "environment": s_str(), "runtime": s_obj(), "resources": s_obj(),
    "queue": s_str(), "priority": s_int(0, 100), "timeoutSeconds": s_int(1, 30 * 86400), "env": s_obj(), "secrets": s_obj(), "tags": s_obj(),
    "placement": s_obj(),
}


def _cap(value: Any) -> Any:
    try:
        s = json.dumps(value, default=str)
    except (TypeError, ValueError):
        return {"truncated": True}
    if len(s) > MAX_RESULT_BYTES:
        return {"truncated": True, "bytes": len(s)}
    return value


class JobActivity(Activity):
    """Shared implementation: build a :class:`JobSubmission` from the evaluated properties, submit it, poll it, cancel it."""

    mode = "async"
    kind: JobKind = JobKind.NOTEBOOK
    default_timeout_seconds = None

    # -- subclass hooks
    def item_type(self) -> str | None:
        return None

    def item_ref(self, props: dict[str, Any]) -> str | None:
        return None

    def source(self, actx: ActivityContext) -> JobSource | None:
        return None

    # -- submission
    def build(self, actx: ActivityContext) -> JobSubmission:
        p = actx.props
        ws = actx.workspace_id
        kw: dict[str, Any] = {"kind": self.kind, "workspace_id": ws}
        ref = self.item_ref(p)
        if ref:
            items = actx.service("items")
            try:
                item = items.resolve(ws, str(ref), self.item_type())
            except OlcfError as exc:
                raise ActivityError(f"{self.type}: {exc.message}", category="configuration", retryable=False) from None
            kw["item_id"] = item["id"]
            kw["name"] = f"{actx.run.get('pipeline_name', 'pipeline')}/{actx.name}: {item['name']}"[:255]
        else:
            kw["name"] = f"{actx.run.get('pipeline_name', 'pipeline')}/{actx.name}"[:255]
        src = self.source(actx)
        if src is not None:
            kw["source"] = src
        params = p.get("parameters") or {}
        if not isinstance(params, dict):
            raise ActivityError(f"{self.type}: 'parameters' must be an object", category="configuration", retryable=False)
        kw["parameters"] = params
        if p.get("engine"):
            kw["engine"] = p["engine"]
        if p.get("lakehouse"):
            lh = str(p["lakehouse"])
            try:
                row = actx.service("items").resolve(ws, lh, "lakehouse")
            except OlcfError as exc:
                raise ActivityError(f"{self.type}: default lakehouse: {exc.message}", category="configuration", retryable=False) from None
            kw["default_lakehouse"] = LakehouseRef(lakehouse_id=row["id"], name=row["name"], workspace_id=ws)
        runtime = dict(p.get("runtime") or {})
        if p.get("environment"):
            try:
                env = actx.service("items").resolve(ws, str(p["environment"]), "environment")
            except OlcfError as exc:
                raise ActivityError(f"{self.type}: environment: {exc.message}", category="configuration", retryable=False) from None
            runtime["environment_id"] = env["id"]
        try:
            if runtime:
                kw["runtime"] = RuntimeRequest(**runtime)
            if p.get("resources"):
                kw["resources"] = ResourceRequest(**p["resources"])
            policy: dict[str, Any] = {}
            for src_key, dst in (("queue", "queue"), ("priority", "priority"), ("timeoutSeconds", "timeout_seconds")):
                if p.get(src_key) is not None:
                    policy[dst] = p[src_key]
            if p.get("placement"):
                policy["placement"] = p["placement"]
            if policy:
                kw["policy"] = PolicySpec(**policy)
            env_vars = {str(k): str(v) for k, v in (p.get("env") or {}).items()}
            if env_vars:
                kw["env"] = env_vars
            if p.get("secrets"):
                kw["secret_refs"] = {str(k): str(v) for k, v in p["secrets"].items()}
        except Exception as exc:  # noqa: BLE001 - pydantic validation
            raise ActivityError(f"{self.type}: invalid job settings: {exc}", category="configuration", retryable=False) from None
        tags = {str(k): str(v) for k, v in (p.get("tags") or {}).items()}
        tags.update({"pipeline_run": actx.run_id, "pipeline_activity": actx.name, "pipeline": str(actx.run.get("pipeline_name", ""))})
        kw["tags"] = tags
        kw["origin"] = Origin(kind=OriginKind.PIPELINE, details={"run_id": actx.run_id, "activity_run_id": actx.activity_run_id, "activity": actx.name,
                                                                 "pipeline": actx.run.get("pipeline_name"), "pipeline_id": actx.run.get("pipeline_id")})
        kw["idempotency_key"] = f"pl:{actx.activity_run_id}:{actx.attempt}"
        kw["parent_run_id"] = actx.activity_run_id
        try:
            return JobSubmission(**kw)
        except Exception as exc:  # noqa: BLE001
            raise ActivityError(f"{self.type}: invalid job request: {exc}", category="configuration", retryable=False) from None

    def start(self, actx: ActivityContext) -> None:
        jobs = actx.service("jobs")
        sub = self.build(actx)
        if sub.item_id:
            actx.save_state(item_id=sub.item_id)  # remembered for lineage (pipeline -> notebook / spark job definition)
        try:
            rec = jobs.submit(actx.principal, sub, correlation_id=actx.run.get("correlation_id"))
        except Conflict:
            # the same attempt was submitted before (crash between submit and save_state) with slightly different evaluated parameters: re-attach
            existing = jobs._by_idempotency(actx.principal.id, sub.idempotency_key or "")  # noqa: SLF001
            if not existing:
                raise ActivityError("the job for this attempt already exists but cannot be found", category="internal", retryable=False) from None
            rec_id = existing["id"]
        except OlcfError as exc:
            retry = exc.status >= 500 or exc.code in ("rate_limited", "unavailable")
            raise ActivityError(exc.message, category=exc.code, retryable=retry and exc.code not in NON_RETRYABLE,
                                details={**exc.details, **({"remediation": exc.remediation} if exc.remediation else {})}) from None
        else:
            rec_id = rec.id
        actx.save_state(job_id=rec_id)
        actx.link_job(rec_id)
        actx.log(f"submitted job {rec_id}")

    def resume(self, actx: ActivityContext) -> dict[str, Any] | None:
        if not actx.state.get("job_id"):
            # crashed before the job id was recorded: submit again - the idempotency key makes this exactly-once
            self.start(actx)
        else:
            actx.link_job(actx.state["job_id"])
        return self.poll(actx)

    def poll(self, actx: ActivityContext) -> dict[str, Any] | None:
        job_id = actx.state.get("job_id")
        if not job_id:
            raise ActivityError("job id missing from the activity state", category="internal", retryable=False)
        jobs = actx.service("jobs")
        try:
            row = jobs.get_row(job_id)
        except NotFound:
            raise ActivityError(f"job {job_id} no longer exists (retention?)", category="internal", retryable=False) from None
        st = row["state"]
        if st not in ("succeeded", "failed", "cancelled", "timed_out"):
            return None
        out = {"jobId": job_id, "state": st, "exitValue": row.get("exit_value"), "result": _cap(redact_obj(row.get("result") or {})), "attempt": row.get("attempt"),
               "runtimeId": row.get("runtime_id"), "applicationId": row.get("application_id"), "startedAt": row.get("started_at"), "finishedAt": row.get("finished_at")}
        if st == "succeeded":
            return out
        fail = row.get("failure") or {}
        cat = str(row.get("failure_category") or fail.get("category") or "internal")
        msg = str(fail.get("message") or row.get("state_reason") or f"job {st}")
        details = {"job_id": job_id, "job_state": st, "failure_category": cat}
        for k in ("remediation", "cell", "line"):
            if fail.get(k) is not None:
                details[k] = fail[k]
        if st == "cancelled":
            if actx.is_cancelled():
                raise ActivityCancelled()
            raise ActivityError(f"job {job_id} was cancelled: {msg}", category="cancellation", retryable=False, details=details)
        if st == "timed_out":
            raise ActivityError(f"job {job_id} timed out: {msg}", category="timeout", retryable=True, details=details)
        raise ActivityError(f"job {job_id} failed ({cat}): {msg}", category=cat, retryable=cat not in NON_RETRYABLE, details=details)

    def poll_interval(self, actx: ActivityContext) -> float:
        return 0.5

    def cancel(self, actx: ActivityContext) -> None:
        job_id = actx.state.get("job_id")
        if not job_id:
            return
        try:
            actx.service("jobs").cancel(actx.principal, job_id, reason=f"pipeline run {actx.run_id} activity '{actx.name}' cancelled")
        except Exception as exc:  # noqa: BLE001 - best effort; the job's own timeout/cleanup remains
            actx.log(f"could not cancel job {job_id}: {exc}")


class NotebookActivity(JobActivity):
    """Run a notebook item as a job (parameters override the notebook's ``parameters`` cell, papermill style)."""

    type = "notebook"
    title = "Notebook"
    description = "Run a notebook of this workspace as a job on the compute fabric."
    kind = JobKind.NOTEBOOK
    properties_schema = obj({"notebook": s_str(minLength=1), **_COMMON}, ["notebook"])

    def item_type(self) -> str:
        return "notebook"

    def item_ref(self, props: dict[str, Any]) -> str | None:
        return props.get("notebook")


class SparkJobActivity(JobActivity):
    """Run a Spark job definition item (python file, jar or scala main class) as a job."""

    type = "spark_job"
    title = "Spark job"
    description = "Run a Spark job definition of this workspace as a job on the compute fabric."
    kind = JobKind.SPARK_JOB
    properties_schema = obj({"sparkJob": s_str(minLength=1), **_COMMON}, ["sparkJob"])

    def item_type(self) -> str:
        return "spark_job_definition"

    def item_ref(self, props: dict[str, Any]) -> str | None:
        return props.get("sparkJob")


class ScriptActivity(JobActivity):
    """Run inline Python (or shell) code as a job. The code runs on a worker, never inside the control plane."""

    type = "script"
    title = "Script"
    description = "Run inline Python or shell code as a job on a worker. 'parameters' are exposed to the script like job parameters."
    kind = JobKind.SCRIPT
    properties_schema = obj({"code": {"type": "string", "minLength": 1}, "language": {"enum": ["python", "shell"]}, **{k: v for k, v in _COMMON.items()}}, ["code"])

    def validate(self, props: dict[str, Any], path: str) -> list[ValidationIssue]:
        out: list[ValidationIssue] = []
        code = props.get("code")
        if isinstance(code, str) and len(code.encode()) > MAX_SCRIPT_BYTES:
            out.append(ValidationIssue(f"{path}.code", f"script is larger than {MAX_SCRIPT_BYTES // 1024} KiB", "too_large"))
        if props.get("language") == "shell" and props.get("engine") == "spark":
            out.append(ValidationIssue(f"{path}.engine", "shell scripts cannot use the spark engine", "incompatible"))
        return out

    def source(self, actx: ActivityContext) -> JobSource:
        code = actx.props.get("code")
        if not isinstance(code, str) or not code.strip():
            raise ActivityError("script: 'code' must be a non-empty string", category="configuration", retryable=False)
        if len(code.encode()) > MAX_SCRIPT_BYTES:
            raise ActivityError(f"script: code is larger than {MAX_SCRIPT_BYTES // 1024} KiB", category="limit", retryable=False)
        fmt = "script" if actx.props.get("language") == "shell" else "py"
        return JobSource(format=fmt, content=code, filename=f"{actx.name}.{'sh' if fmt == 'script' else 'py'}")

    def build(self, actx: ActivityContext) -> JobSubmission:
        if "engine" not in actx.props:
            actx.props = {**actx.props, "engine": "python"}  # scripts are plain Python unless they ask for Spark
        return super().build(actx)


JOB_ACTIVITIES: list[type[Activity]] = [NotebookActivity, SparkJobActivity, ScriptActivity]
