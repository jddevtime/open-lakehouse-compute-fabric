# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Small helpers shared by the ops services: tracked background operations."""

from __future__ import annotations

import logging
import threading
from collections.abc import Callable
from typing import Any

from olcf.core.context import AppContext
from olcf.security.rbac import Principal

log = logging.getLogger("olcf.ops")
Progress = Callable[[int, str], None]


def run_operation(ctx: AppContext, kind: str, principal: Principal | None, fn: Callable[[Progress], Any], *, message: str = "", wait: bool = False) -> dict[str, Any]:
    """Run ``fn(progress)`` as a tracked operation (``/v1/operations/{id}``): in a daemon thread, or inline with ``wait``.

    ``fn`` returns the JSON-able result; an exception with ``to_dict`` becomes a structured error. Returns ``{"operation_id": ...}`` (plus the
    final operation row under ``operation`` when ``wait``).
    """
    ops = ctx.service("operations")
    oid = ops.create(kind, principal.id if principal else None, message or f"{kind} started")

    def progress(p: int, m: str) -> None:
        ops.update(oid, progress=max(0, min(99, p)), message=m)

    def body() -> None:
        try:
            result = fn(progress)
            ops.update(oid, state="succeeded", progress=100, message=f"{kind} finished", result=result if result is not None else {})
        except Exception as exc:  # noqa: BLE001 - recorded on the operation
            log.exception("operation %s (%s) failed", oid, kind)
            err = exc.to_dict() if hasattr(exc, "to_dict") else {"code": "internal", "message": str(exc)}
            ops.update(oid, state="failed", progress=100, message=err.get("message", "failed"), error=err)

    if wait:
        body()
        return {"operation_id": oid, "operation": ops.get(oid)}
    threading.Thread(target=body, name=f"op-{oid}", daemon=True).start()
    return {"operation_id": oid}
