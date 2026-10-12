# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Spark Job Definitions (``spark_job_definition`` items): validation, run endpoint (job kind ``spark_job``) and run history.

Definition (the field names are exactly those the compute controller reads from the item when it builds the job source)::

    {"language": "python|scala|java",
     "main_file": "Files/jobs/etl.py" | "abfss://..." | "olcf://ws/lh/Files/app.jar",   # python: main_file or inline "code"
     "code": "print('hi')",                                                            # python only, alternative to main_file
     "main_class": "com.acme.Main",                                                    # scala / java
     "args": ["--date", "2026-01-01"],
     "reference_files": ["Files/libs/helpers.py", "Files/libs/udf.jar"],
     "settings": {"engine": "spark", "runtime": {..RuntimeRequest.., "spark_conf": {...}}, "default_lakehouse": {"lakehouse_id"|"name"},
                  "environment_id": "...", "resources": {...}, "policy": {"retries": 2, "retry_backoff_seconds": 30, "timeout_seconds": 3600, "idempotent": true},
                  "variable_set": "..."}}

``conf`` is accepted as an alias for ``settings.runtime.spark_conf`` and ``timeout_seconds`` / ``retries`` at the top level for ``settings.policy``; the
normalised form never duplicates them, so a definition run through ``POST /v1/jobs`` (item_id) and through ``POST /v1/spark-job-definitions/{id}/run`` behaves
identically.  Per-run variation goes through ``parameters`` (predefined globals in Python jobs, see N7).
"""

from __future__ import annotations

import ast
import re
from typing import Any

from fastapi import APIRouter, Query
from pydantic import BaseModel, ConfigDict, Field

from olcf.api.deps import Authed, Ctx, IdempotencyKey
from olcf.core.context import AppContext
from olcf.core.errors import NotFound, ValidationFailed
from olcf.core.models import (
    JobKind,
    JobSubmission,
    LakehouseRef,
    Origin,
    OriginKind,
    PolicySpec,
    ResourceRequest,
    RuntimeRequest,
)
from olcf.core.redaction import redact_obj
from olcf.notebooks import params as nbparams
from olcf.notebooks.service import normalise_settings
from olcf.security.rbac import Principal

MAX_CODE = 5 * 1024 * 1024
_CLASS_RE = re.compile(r"^[A-Za-z_$][A-Za-z0-9_$]*(\.[A-Za-z_$][A-Za-z0-9_$]*)*$")
_URI_RE = re.compile(r"^(?:(?P<scheme>[a-z][a-z0-9+.-]*)://)?[^\x00-\x1f\s]{1,1024}$", re.I)
_SCHEMES = {"abfss", "abfs", "wasbs", "wasb", "s3", "s3a", "gs", "olcf", "https"}
_TOP = {"language", "main_file", "code", "main_class", "args", "reference_files", "settings", "conf", "timeout_seconds", "retries", "description"}


def check_uri(uri: Any, what: str, exts: tuple[str, ...] | None = None) -> str:
    if not isinstance(uri, str) or not _URI_RE.match(uri):
        raise ValidationFailed(f"{what} must be a lakehouse path or storage URI")
    m = _URI_RE.match(uri)
    scheme = (m.group("scheme") or "").lower() if m else ""
    if scheme and scheme not in _SCHEMES:
        raise ValidationFailed(f"{what}: scheme '{scheme}' is not allowed", remediation=f"use a lakehouse path (Files/...) or one of {sorted(_SCHEMES)}")
    rest = uri.split("://", 1)[1] if scheme else uri
    if ".." in rest.split("/") or rest.startswith("/") and not scheme:
        raise ValidationFailed(f"{what} must not contain '..' or be an absolute local path")
    if exts and not uri.lower().split("?")[0].endswith(exts):
        raise ValidationFailed(f"{what} must end with {' or '.join(exts)}")
    return uri


def normalise_definition(defn: Any) -> dict[str, Any]:
    """``validate`` hook of the ``spark_job_definition`` type (idempotent)."""
    if not isinstance(defn, dict):
        raise ValidationFailed("a spark job definition must be an object")
    bad = [k for k in defn if k not in _TOP and not str(k).startswith("x-")]
    if bad:
        raise ValidationFailed(f"unknown spark job definition keys {sorted(bad)}", remediation=f"supported: {sorted(_TOP)}")
    lang = str(defn.get("language") or "python").lower()
    if lang not in ("python", "scala", "java"):
        raise ValidationFailed("language must be python, scala or java")
    out: dict[str, Any] = {"language": lang}
    settings = dict(normalise_settings(defn.get("settings")))
    if settings.get("engine") not in (None, "spark"):
        raise ValidationFailed("a spark job definition always runs on the spark engine")
    settings["engine"] = "spark"
    main_file, code, main_class = defn.get("main_file"), defn.get("code"), defn.get("main_class")
    if lang == "python":
        if main_class:
            raise ValidationFailed("main_class is only valid for scala / java jobs")
        if bool(main_file) == (code is not None and code != ""):
            raise ValidationFailed("a python job needs exactly one of 'main_file' (a .py file) or inline 'code'")
        if main_file:
            out["main_file"] = check_uri(main_file, "main_file", (".py",))
        else:
            if not isinstance(code, str) or len(code.encode()) > MAX_CODE:
                raise ValidationFailed("code must be a string of at most 5 MiB")
            try:
                ast.parse(code)
            except SyntaxError as exc:
                raise ValidationFailed(f"the inline code has a syntax error on line {exc.lineno}: {exc.msg}") from exc
            out["code"] = code
    else:
        if code:
            raise ValidationFailed(f"inline code is only valid for python jobs ({lang} jobs run a jar)")
        if not main_file or not main_class:
            raise ValidationFailed(f"a {lang} job needs 'main_file' (a .jar) and 'main_class'")
        out["main_file"] = check_uri(main_file, "main_file", (".jar",))
        if not isinstance(main_class, str) or not _CLASS_RE.match(main_class):
            raise ValidationFailed("main_class is not a valid class name")
        out["main_class"] = main_class
    args = defn.get("args")
    if args is not None:
        if not isinstance(args, list) or not all(isinstance(a, str) and "\x00" not in a and len(a) <= 4096 for a in args) or sum(len(a) for a in args) > 65536:
            raise ValidationFailed("args must be a list of strings (at most 4096 characters each, 64 KiB in total)")
        out["args"] = args
    refs = defn.get("reference_files")
    if refs is not None:
        if not isinstance(refs, list) or len(refs) > 200:
            raise ValidationFailed("reference_files must be a list of at most 200 entries")
        out["reference_files"] = [check_uri(r, "reference_files[]") for r in refs]
    if isinstance(defn.get("description"), str):
        out["description"] = defn["description"]
    # aliases -> canonical settings (never duplicated)
    if defn.get("conf") is not None:
        conf = defn["conf"]
        if not isinstance(conf, dict):
            raise ValidationFailed("conf must be an object")
        norm: dict[str, str] = {}
        for k, v in conf.items():
            if not isinstance(k, str) or not k.startswith("spark."):
                raise ValidationFailed(f"conf key '{k}' must start with 'spark.'")
            if isinstance(v, (dict, list)) or v is None:
                raise ValidationFailed(f"conf value of '{k}' must be a scalar")
            norm[k] = "true" if v is True else "false" if v is False else str(v)
        rt = dict(settings.get("runtime") or {})
        rt["spark_conf"] = {**(rt.get("spark_conf") or {}), **norm}
        settings["runtime"] = rt
    pol = dict(settings.get("policy") or {})
    if defn.get("timeout_seconds") is not None:
        if not isinstance(defn["timeout_seconds"], int) or isinstance(defn["timeout_seconds"], bool) or defn["timeout_seconds"] < 1:
            raise ValidationFailed("timeout_seconds must be a positive integer")
        pol["timeout_seconds"] = defn["timeout_seconds"]
    if defn.get("retries") is not None:
        if not isinstance(defn["retries"], int) or isinstance(defn["retries"], bool) or not 0 <= defn["retries"] <= 10:
            raise ValidationFailed("retries must be an integer between 0 and 10")
        pol["retries"] = defn["retries"]
    if pol:
        settings["policy"] = PolicySpec.model_validate(pol).model_dump(exclude_unset=True, exclude_none=True)
    if settings.get("timeout_seconds") and not (settings.get("policy") or {}).get("timeout_seconds"):
        settings["policy"] = {**(settings.get("policy") or {}), "timeout_seconds": settings.pop("timeout_seconds")}
    settings.pop("timeout_seconds", None)
    out["settings"] = settings
    for k, v in defn.items():
        if str(k).startswith("x-"):
            out[k] = v
    return out


class SparkRunRequest(BaseModel):
    """Body of ``POST /v1/spark-job-definitions/{id}/run``."""

    model_config = ConfigDict(extra="forbid")
    parameters: dict[str, Any] = Field(default_factory=dict)
    conf: dict[str, str] = Field(default_factory=dict)
    name: str | None = Field(None, max_length=255)
    runtime: dict[str, Any] | None = None
    resources: dict[str, Any] | None = None
    policy: dict[str, Any] | None = None
    default_lakehouse: dict[str, Any] | None = None
    environment_id: str | None = None
    variable_set: str | None = None
    stage: str | None = Field(None, pattern=r"^[A-Za-z0-9_-]{1,32}$")
    tags: dict[str, str] = Field(default_factory=dict)
    idempotency_key: str | None = Field(None, max_length=191)


class SparkJobService:
    """Runs and lists Spark job definitions (``ctx.service("spark_jobs")``)."""

    def __init__(self, ctx: AppContext) -> None:
        self.ctx = ctx

    def item(self, principal: Principal, item_id: str, perm: str) -> dict[str, Any]:
        row = self.ctx.service("items").get(item_id)
        if row["type"] != "spark_job_definition":
            raise NotFound(f"spark job definition {item_id} not found")
        principal.require(perm, workspace_id=row["workspace_id"], item_id=row["id"])
        return row

    def build_submission(self, principal: Principal, row: dict[str, Any], body: SparkRunRequest) -> tuple[JobSubmission, dict[str, Any]]:
        ws = row["workspace_id"]
        d = normalise_definition(row["definition"])
        settings = d["settings"]
        notes: dict[str, Any] = {}
        runtime_d = {**(settings.get("runtime") or {}), **(body.runtime or {})}
        runtime_d["spark_conf"] = {**(runtime_d.get("spark_conf") or {}), **body.conf}
        env_vars: dict[str, str] = {}
        env_ref = body.environment_id or settings.get("environment_id")
        if env_ref:
            env_item = self.ctx.service("items").resolve(ws, env_ref, "environment")
            principal.require("item.read", workspace_id=ws, item_id=env_item["id"])
            spec = self.ctx.service("environments").resolve(env_item["id"], workspace_id=ws)
            runtime_d["environment_id"] = env_item["id"]
            env_vars = dict(spec.get("variables") or {})
            notes["environment"] = {"id": env_item["id"], "name": env_item["name"], "lock_hash": spec.get("lock_hash")}
        values = dict(body.parameters)
        secret_refs: dict[str, str] = {}
        vs_ref = body.variable_set or settings.get("variable_set")
        if vs_ref:
            res = self.ctx.service("variables").resolve(vs_ref, stage=body.stage, workspace_id=ws, principal=principal)
            for k, v in res["values"].items():
                values.setdefault(k, v)
            secret_refs.update(res["secrets"])
            notes["variable_set"] = {"id": res["id"], "name": res["name"], "stage": res["stage"]}
        for k, v in values.items():
            nbparams.validate_name(k)
            nbparams.check_json_value(v, f"parameter '{k}'")
        try:
            runtime = RuntimeRequest.model_validate(runtime_d)
            resources = ResourceRequest.model_validate({**(settings.get("resources") or {}), **(body.resources or {})})
            policy = PolicySpec.model_validate({**(settings.get("policy") or {}), **(body.policy or {})})
            lake_src = body.default_lakehouse if body.default_lakehouse is not None else settings.get("default_lakehouse")
            lake = None
            if lake_src:
                ld = dict(lake_src)
                if "id" in ld:
                    ld["lakehouse_id"] = ld.pop("id")
                lake = LakehouseRef.model_validate(ld)
                if lake.lakehouse_id is None and lake.name and lake.workspace_id is None:
                    lake = lake.model_copy(update={"workspace_id": ws})
        except Exception as exc:  # noqa: BLE001
            raise ValidationFailed(f"invalid run request: {' '.join(str(exc).splitlines()[:3])[:300]}") from exc
        sub = JobSubmission(kind=JobKind.SPARK_JOB, workspace_id=ws, name=body.name or row["name"], item_id=row["id"], parameters=values, default_lakehouse=lake, runtime=runtime,
                            resources=resources, policy=policy, origin=Origin(kind=OriginKind.API, details={"spark_job_definition": row["name"], "version": row["version"]}),
                            idempotency_key=body.idempotency_key, engine="spark", env=env_vars, secret_refs=secret_refs, tags=dict(body.tags))
        return sub, notes

    def run(self, principal: Principal, row: dict[str, Any], body: SparkRunRequest) -> dict[str, Any]:
        principal.require("job.submit", workspace_id=row["workspace_id"])
        sub, notes = self.build_submission(principal, row, body)
        job = self.ctx.service("jobs").submit(principal, sub)
        self.ctx.audit.record(principal, "spark_job_definition.run", resource_type="spark_job_definition", resource_id=row["id"], workspace_id=row["workspace_id"], details={"job_id": job.id})
        rec = job.model_dump(mode="json")
        rec["notes"] = notes
        return redact_obj(rec)

    def runs(self, principal: Principal, row: dict[str, Any], *, limit: int, cursor: str | None) -> dict[str, Any]:
        jobs = self.ctx.service("jobs")
        rows, nxt = jobs.list(principal, workspace_id=row["workspace_id"], item_id=row["id"], limit=limit, cursor=cursor)
        return {"items": [jobs.public(r) for r in rows], "next_cursor": nxt}


router = APIRouter(tags=["spark-job-definitions"])


@router.post("/spark-job-definitions/{item_id}/run", status_code=202)
def run(item_id: str, body: SparkRunRequest, principal: Authed, ctx: Ctx, idem: IdempotencyKey = None) -> dict[str, Any]:
    """Submit the definition as a ``spark_job`` job; returns the job record."""
    s: SparkJobService = ctx.service("spark_jobs")
    row = s.item(principal, item_id, "item.read")
    if idem and not body.idempotency_key:
        body = body.model_copy(update={"idempotency_key": idem})
    return s.run(principal, row, body)


@router.get("/spark-job-definitions/{item_id}/runs")
def runs(item_id: str, principal: Authed, ctx: Ctx, limit: int = Query(50, ge=1, le=200), cursor: str | None = None) -> dict[str, Any]:
    s: SparkJobService = ctx.service("spark_jobs")
    row = s.item(principal, item_id, "item.read")
    return redact_obj(s.runs(principal, row, limit=limit, cursor=cursor))


@router.get("/spark-job-definitions/{item_id}/validate")
def validate(item_id: str, principal: Authed, ctx: Ctx) -> dict[str, Any]:
    """The normalised definition and the job submission it would produce (nothing is submitted)."""
    s: SparkJobService = ctx.service("spark_jobs")
    row = s.item(principal, item_id, "item.read")
    sub, notes = s.build_submission(principal, row, SparkRunRequest())
    return {"definition": normalise_definition(row["definition"]), "submission": redact_obj(sub.model_dump(mode="json", exclude_none=True)), "notes": notes}


__all__ = ["SparkJobService", "SparkRunRequest", "normalise_definition", "router"]
