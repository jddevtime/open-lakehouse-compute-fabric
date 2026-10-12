# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Job artifacts on the control plane: where they live and how they arrive from an agent.

Two delivery paths exist (both documented in CONTRACTS, "Controller addenda (CTRL)"):

1. **Inline** (the NODE agent's default): the terminal ``job_state`` carries ``result.artifacts[]`` entries
   ``{name, path, media_type, size, sha256, data_b64?, truncated?}``. Entries with ``data_b64`` are decoded, verified against ``size``/``sha256`` and stored by
   :func:`ingest_inline` *before* the job is finished, so ``GET /v1/jobs/{id}/output`` works the moment the job is terminal. The base64 payload never reaches the
   database: the stored ``result`` keeps metadata only (``stored: true``).
2. **On demand**: entries flagged ``truncated`` (too large to inline) are pulled afterwards with the additive agent command ``fetch_artifact``
   (:func:`fetch_remote`, paged, sha256-verified, run on the job pool). The agent keeps the job directory for ``job_retention_hours``.

The HTTP upload endpoint ``POST /v1/jobs/{id}/artifacts/{name}`` (see :mod:`olcf.compute.jobs_api`) remains available for runners that prefer to push files.
All names are restricted to ``[A-Za-z0-9][A-Za-z0-9._-]{0,127}`` and every path is resolved below the job's artifact directory.
"""

from __future__ import annotations

import base64
import binascii
import hashlib
import logging
import os
import re
import tempfile
from collections.abc import Callable
from pathlib import Path
from typing import Any

from olcf.core.errors import BadRequest, OlcfError

log = logging.getLogger("olcf.compute.artifacts")

ARTIFACT_NAME = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$")
HARD_ARTIFACT_CAP = 2 * 1024 * 1024 * 1024
DEFAULT_ARTIFACT_CAP = 512 * 1024 * 1024
#: Largest inline artifact the controller accepts from a frame (the agent inlines at most 2 MiB each; this is the defensive ceiling).
MAX_INLINE_BYTES = 6 * 1024 * 1024
FETCH_CHUNK = 4 * 1024 * 1024
MAX_ARTIFACTS_PER_JOB = 256


def artifact_dir(jobs_root: Path, job_id: str) -> Path:
    base = (jobs_root / job_id).resolve()
    if base.parent != jobs_root.resolve():
        raise BadRequest("invalid job id")
    return base / "artifacts"


def artifact_path(jobs_root: Path, job_id: str, name: str) -> Path:
    if not ARTIFACT_NAME.match(name or ""):
        raise BadRequest("artifact names are 1-128 characters of letters, digits, '.', '_' or '-' (no path separators)")
    root = artifact_dir(jobs_root, job_id).resolve()
    p = (root / name).resolve()
    if p.parent != root:
        raise BadRequest("invalid artifact name")
    return p


def safe_name(entry: dict[str, Any]) -> str | None:
    """The stored name of a reported artifact: its ``name`` when valid, else the final component of its ``path``; ``None`` when neither is acceptable."""
    for cand in (entry.get("name"), os.path.basename(str(entry.get("path") or ""))):
        if isinstance(cand, str) and ARTIFACT_NAME.match(cand):
            return cand
    return None


def _write_atomic(dest: Path, data: bytes) -> None:
    dest.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp = tempfile.mkstemp(dir=dest.parent, prefix=".ingest-")
    try:
        with os.fdopen(fd, "wb") as f:
            f.write(data)
        os.replace(tmp, dest)
    except BaseException:
        try:
            os.unlink(tmp)
        except OSError:
            pass
        raise


def ingest_inline(jobs_root: Path, job_id: str, result: dict[str, Any] | None) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    """Store the inline artifacts of a terminal ``job_state`` result.

    Returns ``(clean_result, pending)``: ``clean_result`` has no ``data_b64`` (safe to persist); ``pending`` lists the entries that must still be fetched
    (``truncated``) with their validated ``name``. Invalid names, bad base64 and checksum/size mismatches are recorded on the entry (``error``) and skipped,
    never raised: a damaged artifact must not turn a finished job into a failure.
    """
    res = dict(result or {})
    arts = res.get("artifacts")
    if not isinstance(arts, list):
        return res, []
    clean: list[dict[str, Any]] = []
    pending: list[dict[str, Any]] = []
    for raw in arts[:MAX_ARTIFACTS_PER_JOB]:
        if not isinstance(raw, dict):
            continue
        entry = {k: v for k, v in raw.items() if k != "data_b64"}
        name = safe_name(raw)
        if name is None:
            entry["error"] = "invalid artifact name"
            clean.append(entry)
            continue
        entry["name"] = name
        b64 = raw.get("data_b64")
        if isinstance(b64, str):
            try:
                if len(b64) > MAX_INLINE_BYTES * 4 // 3 + 8:
                    raise ValueError("inline artifact too large")
                data = base64.b64decode(b64, validate=True)
                if raw.get("size") is not None and int(raw["size"]) != len(data):
                    raise ValueError(f"size mismatch (reported {raw['size']}, received {len(data)})")
                digest = hashlib.sha256(data).hexdigest()
                if raw.get("sha256") and str(raw["sha256"]).removeprefix("sha256:") != digest:
                    raise ValueError("sha256 mismatch")
                _write_atomic(artifact_path(jobs_root, job_id, name), data)
                entry.update(size=len(data), sha256=digest, stored=True)
                entry.pop("truncated", None)
            except (ValueError, binascii.Error, OSError, OlcfError) as exc:
                log.warning("artifact %s of %s rejected: %s", name, job_id, exc)
                entry.update(stored=False, error=str(exc))
        elif raw.get("truncated"):
            entry["stored"] = False
            entry["pending"] = True
            pending.append(entry)
        clean.append(entry)
    res["artifacts"] = clean
    return res, pending


def fetch_remote(jobs_root: Path, job_id: str, entry: dict[str, Any], request: Callable[[str, dict[str, Any]], Any], *, cap: int) -> dict[str, Any]:
    """Pull one ``truncated`` artifact with ``fetch_artifact`` (paged), verify it and store it. ``request(command, payload)`` performs the agent call.

    Returns an event payload ``{name, bytes}`` on success; raises ``ValueError`` on any inconsistency (the partial file is removed).
    """
    name = entry["name"]
    path = str(entry.get("path") or name)
    expected = entry.get("size")
    if expected is not None and int(expected) > cap:
        raise ValueError(f"artifact is {int(expected)} bytes, above the limit of {cap}")
    dest = artifact_path(jobs_root, job_id, name)
    dest.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp = tempfile.mkstemp(dir=dest.parent, prefix=".fetch-")
    h = hashlib.sha256()
    offset = 0
    try:
        with os.fdopen(fd, "wb") as f:
            while True:
                doc = request("fetch_artifact", {"job_id": job_id, "path": path, "offset": offset, "length": FETCH_CHUNK})
                if not isinstance(doc, dict):
                    raise ValueError("the agent returned no data")
                try:
                    chunk = base64.b64decode(str(doc.get("data_b64") or ""), validate=True)
                except (ValueError, binascii.Error) as exc:
                    raise ValueError("the agent returned invalid base64") from exc
                if int(doc.get("offset", offset)) != offset:
                    raise ValueError("the agent answered a different offset")
                f.write(chunk)
                h.update(chunk)
                offset += len(chunk)
                if offset > cap:
                    raise ValueError(f"artifact exceeds the limit of {cap} bytes")
                if doc.get("eof") or not chunk:
                    break
        if expected is not None and int(expected) != offset:
            raise ValueError(f"size mismatch (reported {expected}, received {offset})")
        if entry.get("sha256") and str(entry["sha256"]).removeprefix("sha256:") != h.hexdigest():
            raise ValueError("sha256 mismatch")
        os.replace(tmp, dest)
    except BaseException:
        try:
            os.unlink(tmp)
        except OSError:
            pass
        raise
    return {"name": name, "bytes": offset, "sha256": h.hexdigest()}


__all__ = ["ARTIFACT_NAME", "DEFAULT_ARTIFACT_CAP", "HARD_ARTIFACT_CAP", "artifact_dir", "artifact_path", "fetch_remote", "ingest_inline", "safe_name"]
