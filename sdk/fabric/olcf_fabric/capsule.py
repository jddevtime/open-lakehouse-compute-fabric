# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Build and sign the execution capsule (``schemas/capsule-v1.json``). A capsule is *declared* content only: normalised notebook source, selected Fabric context keys, explicit
parameters, references and policy. It never contains an interpreter dump, a token or a secret value (secrets travel as ``{vault, name}`` references)."""

from __future__ import annotations

import datetime as _dt
from typing import Any

from . import __version__
from ._canonical import sha256_hex, sign_capsule

SCHEMA_VERSION = "1.0"


def iso(ts: float | _dt.datetime) -> str:
    dt = ts if isinstance(ts, _dt.datetime) else _dt.datetime.fromtimestamp(ts, tz=_dt.timezone.utc)  # noqa: UP017 - Python 3.10 compatible
    return dt.astimezone(_dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"  # noqa: UP017


def parse_iso(value: str) -> _dt.datetime:
    return _dt.datetime.fromisoformat(value.replace("Z", "+00:00"))


def request_parts(*, source: dict[str, Any], context: dict[str, Any], runtime: dict[str, Any], parameters: dict[str, Any], data_references: dict[str, Any], policy: dict[str, Any],
                  identity: dict[str, Any], secret_refs: list[dict[str, str]]) -> dict[str, Any]:
    """The body of ``POST /validate``: a capsule without its signing material."""
    return {"sdkVersion": __version__, "source": source, "context": context, "runtime": runtime, "parameters": parameters, "dataReferences": data_references, "policy": policy,
            "security": {"identity": identity, "secretRefs": secret_refs}}


def build_signed(parts: dict[str, Any], challenge: dict[str, Any], *, idempotency_key: str | None, correlation_id: str | None, elapsed_since_challenge: float = 0.0) -> dict[str, Any]:
    """Assemble the capsule from validated ``parts`` and sign it with the challenge key. ``createdAt`` is derived from the *server's* clock (``serverTime`` of the challenge plus the time
    that has passed), so a Fabric session with a skewed clock still lands inside the server's timestamp window."""
    created = parse_iso(challenge["serverTime"]) + _dt.timedelta(seconds=max(0.0, elapsed_since_challenge))
    sid = challenge["submissionId"]
    security = dict(parts["security"])
    security.update({"nonce": challenge["nonce"], "keyId": challenge["keyId"], "correlationId": correlation_id})
    if correlation_id is None:
        security.pop("correlationId")
    capsule: dict[str, Any] = {
        "schemaVersion": SCHEMA_VERSION, "sdkVersion": __version__, "adapterVersion": str(challenge.get("adapterVersion") or "unknown"), "submissionId": sid,
        "idempotencyKey": idempotency_key or f"sdk-{sid}", "createdAt": iso(created), "source": parts["source"], "context": parts["context"], "runtime": parts["runtime"],
        "parameters": parts["parameters"], "dataReferences": parts["dataReferences"], "policy": parts["policy"], "security": security,
    }
    if parts.get("compatibility"):
        capsule["compatibility"] = parts["compatibility"]
    return {"capsule": capsule, "signature": sign_capsule(capsule, challenge["key"], challenge["keyId"])}


def source_block(*, mode: str, fmt: str, content: str | None, notebook_id: str | None, notebook_name: str | None, workspace_id: str | None, workspace_name: str | None,
                 original: tuple[str, str] | None, excluded: list[int]) -> dict[str, Any]:
    """``original`` is ``(format, text)`` of the definition as Fabric returned it (kept for audit)."""
    block: dict[str, Any] = {"mode": mode, "notebookId": notebook_id, "notebookName": notebook_name, "workspaceId": workspace_id, "workspaceName": workspace_name, "format": fmt,
                             "excludedCells": excluded}
    if mode == "inline" and content is not None:
        block["content"] = content
        block["sha256"] = sha256_hex(content)
        if original is not None:
            block["original"] = {"format": original[0], "content": original[1], "sha256": sha256_hex(original[1])}
    return {k: v for k, v in block.items() if v is not None}
