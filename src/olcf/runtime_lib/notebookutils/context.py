# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""``notebookutils.runtime.context``: rebuilt from the job spec (stdlib only).

Fabric exposes the notebook's identity as a dict (``currentNotebookName``, ``currentWorkspaceId``, ``defaultLakehouseName`` ...). The SDK captures that dict
in the Fabric session and ships it in the capsule; the control plane places it in ``job_spec["context"]["fabric"]["context"]``. This module answers with
exactly those values (what the notebook saw in Fabric), filling the gaps from OLCF's own knowledge (job id, lakehouse, workspace) so that code written
against the Fabric keys keeps working on a job that did not come from Fabric at all.
"""

from __future__ import annotations

from typing import Any

#: Fabric keys that are always present in the rebuilt context (value ``None`` / ``False`` when unknown).
CONTEXT_KEYS = (
    "currentNotebookName", "currentNotebookId", "currentWorkspaceName", "currentWorkspaceId", "defaultLakehouseName", "defaultLakehouseId",
    "defaultLakehouseWorkspaceName", "defaultLakehouseWorkspaceId", "currentRunId", "parentRunId", "rootRunId", "isForPipeline", "isReferenceRun",
    "referenceTreePath", "userName", "userId", "productType", "activityId", "isForInteractive",
)


def build(spec: dict[str, Any]) -> dict[str, Any]:
    ctx = spec.get("context") or {}
    fab = ctx.get("fabric") or {}
    raw = dict(fab.get("context") or {}) if isinstance(fab, dict) else {}
    lake = spec.get("default_lakehouse") or {}
    lfab = lake.get("fabric") or {}
    ws = spec.get("workspace") or {}
    job_id = spec.get("job_id") or ctx.get("job_id")
    out: dict[str, Any] = {k: None for k in CONTEXT_KEYS}
    out.update({
        "currentNotebookName": fab.get("notebook_name") or spec.get("name"),
        "currentNotebookId": fab.get("notebook_id") or ctx.get("item_id"),
        "currentWorkspaceName": fab.get("workspace_name") or ctx.get("workspace_name") or ws.get("name"),
        "currentWorkspaceId": fab.get("workspace_id") or ctx.get("workspace_id") or ws.get("id"),
        "defaultLakehouseName": lake.get("name"),
        "defaultLakehouseId": lfab.get("lakehouse_id") or lake.get("id"),
        "defaultLakehouseWorkspaceId": lfab.get("workspace_id") or lake.get("workspace_id"),
        "currentRunId": job_id,
        "parentRunId": ctx.get("parent_run_id"),
        "rootRunId": ctx.get("parent_run_id") or job_id,
        "isForPipeline": bool(fab.get("is_for_pipeline", False)),
        "isReferenceRun": bool(ctx.get("parent_run_id")),
        "isForInteractive": False,
        "userName": fab.get("user_name") or ctx.get("submitted_by"),
        "userId": fab.get("user_id"),
        "productType": "olcf",
    })
    for k, v in raw.items():  # what the notebook saw in Fabric wins, key for key
        if v is not None and (k in CONTEXT_KEYS or k not in out):
            out[k] = v
    out["olcfJobId"] = job_id
    return out
