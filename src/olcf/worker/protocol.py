# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Wire helpers for the controller <-> agent protocol (``WORKER_PROTOCOL_VERSION = "1.0"``, CONTRACTS section 4.2).

Frames are JSON objects ``{"v": 1, "type": ..., "id": <uuid>, "ref": <id answered>, "ts": <ISO-8601 UTC>, "payload": {...}}``.
The agent tolerates ``ts`` as ISO string or epoch number (controller side) and always emits ISO strings.
"""

from __future__ import annotations

import json
import time
import uuid
from datetime import datetime
from typing import Any

from olcf.core.errors import OlcfError
from olcf.core.redaction import redact_text
from olcf.core.timeutil import from_iso, now_iso

FRAME_VERSION = 1

#: Commands the controller may send (each carries an ``id`` and is answered with ``result{ref=id}``).
COMMANDS = (
    "ping", "run_job", "cancel_job", "start_spark_master", "stop_spark_master", "start_spark_worker", "stop_spark_worker",
    "start_kernel", "kernel_execute", "kernel_interrupt", "kernel_restart", "kernel_shutdown", "kernel_complete",
    "probe", "install_runtime", "drain", "collect_diagnostics", "shutdown", "renew_credentials",
)
#: Frame types from the controller that need no answer.
NOTIFICATIONS = ("result", "error", "log_ack", "job_ack", "bye")


def make_frame(ftype: str, payload: dict[str, Any] | None = None, *, ref: str | None = None, fid: str | None = None, anon: bool = False) -> dict[str, Any]:
    """Build a frame. The controller acknowledges every agent frame that carries an ``id`` with ``result{ref=id}`` (this is how the
    agent learns that a ``job_log``/``job_state`` was accepted); ``anon=True`` omits the id for frames that need no acknowledgement
    (heartbeats, kernel events, results)."""
    f: dict[str, Any] = {"v": FRAME_VERSION, "type": ftype, "ref": ref, "ts": now_iso(), "payload": payload or {}}
    if not anon:
        f["id"] = fid or uuid.uuid4().hex
    return f


def encode(frame: dict[str, Any]) -> str:
    return json.dumps(frame, separators=(",", ":"), default=str)


def decode(raw: str | bytes) -> dict[str, Any]:
    obj = json.loads(raw)
    if not isinstance(obj, dict) or "type" not in obj:
        raise ValueError("frame is not an object with a 'type'")
    obj.setdefault("payload", {})
    return obj


def parse_ts(value: Any) -> float | None:
    """Epoch seconds from an ISO string or number; ``None`` if absent/unparseable."""
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value) / (1000.0 if value > 1e11 else 1.0)
    try:
        return from_iso(str(value)).timestamp()
    except ValueError:
        try:
            return datetime.fromisoformat(str(value)).timestamp()
        except ValueError:
            return None


def result_ok(ref: str | None, data: dict[str, Any] | None = None) -> dict[str, Any]:
    """``result`` frame: ``ref`` appears on the frame and inside the payload (both readings of the contract work)."""
    return make_frame("result", {"ref": ref, "ok": True, "data": data or {}}, ref=ref, anon=True)


def error_dict(exc: BaseException) -> dict[str, Any]:
    if isinstance(exc, OlcfError):
        d = exc.to_dict()
    else:
        d = {"code": getattr(exc, "code", "internal"), "message": f"{type(exc).__name__}: {exc}"}
        if getattr(exc, "remediation", None):
            d["remediation"] = exc.remediation
    d["message"] = redact_text(str(d.get("message", "")))
    return d


def result_err(ref: str | None, exc: BaseException) -> dict[str, Any]:
    return make_frame("result", {"ref": ref, "ok": False, "error": error_dict(exc)}, ref=ref, anon=True)


class CommandError(OlcfError):
    """A command could not be executed (carries a stable ``code`` and remediation for the controller)."""

    status = 422

    def __init__(self, code: str, message: str, *, remediation: str | None = None, details: dict[str, Any] | None = None) -> None:
        super().__init__(message, details=details, remediation=remediation)
        self.code = code


class ClockSkew:
    """Estimates controller-vs-local clock offset from the ``ts`` of received frames (min |offset| of recent samples)."""

    def __init__(self, keep: int = 30) -> None:
        self._samples: list[float] = []
        self._keep = keep

    def observe(self, frame_ts: Any) -> None:
        ts = parse_ts(frame_ts)
        if ts is not None:
            self._samples.append(time.time() - ts)  # positive: local clock ahead of the controller
            del self._samples[: -self._keep]

    @property
    def seconds(self) -> float | None:
        return min(self._samples, key=abs) if self._samples else None
