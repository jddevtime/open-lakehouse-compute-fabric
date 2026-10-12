# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Platform exception hierarchy and the canonical error envelope.

Every API error is rendered as::

    {"error": {"code": "not_found", "message": "...", "details": {...}, "correlation_id": "..."}}

Job failures use :class:`FailureCategory` so operators can filter by cause.
"""

from __future__ import annotations

from enum import StrEnum
from typing import Any


class OlcfError(Exception):
    """Base class. ``status`` maps to the HTTP status used by the API layer."""

    status = 500
    code = "internal"

    def __init__(self, message: str = "", *, details: dict[str, Any] | None = None, remediation: str | None = None):
        super().__init__(message or self.code)
        self.message = message or self.code
        self.details = details or {}
        self.remediation = remediation

    def to_dict(self) -> dict[str, Any]:
        out: dict[str, Any] = {"code": self.code, "message": self.message}
        if self.details:
            out["details"] = self.details
        if self.remediation:
            out["remediation"] = self.remediation
        return out


class ValidationFailed(OlcfError):
    """A request or document failed validation (HTTP 422, code ``validation_failed``)."""
    status = 422
    code = "validation_failed"


class BadRequest(OlcfError):
    """The request is malformed or cannot be processed as sent (HTTP 400, code ``bad_request``)."""
    status = 400
    code = "bad_request"


class Unauthenticated(OlcfError):
    """No valid credentials were presented (HTTP 401, code ``unauthenticated``)."""
    status = 401
    code = "unauthenticated"


class Forbidden(OlcfError):
    """The caller is authenticated but not permitted to do this (HTTP 403, code ``forbidden``)."""
    status = 403
    code = "forbidden"


class PasswordChangeRequired(Forbidden):
    """The account must choose a new password first: its (login) credentials may call nothing but ``POST /v1/identity/password``."""

    code = "password_change_required"


class NotFound(OlcfError):
    """The object does not exist, or the caller may not know that it does (HTTP 404, code ``not_found``)."""
    status = 404
    code = "not_found"


class Conflict(OlcfError):
    """The request conflicts with the current state of the object (HTTP 409, code ``conflict``)."""
    status = 409
    code = "conflict"


class PreconditionFailed(OlcfError):
    """ETag / If-Match mismatch (optimistic concurrency)."""

    status = 412
    code = "precondition_failed"


class TooManyRequests(OlcfError):
    """A rate limit was exceeded (HTTP 429, code ``rate_limited``)."""
    status = 429
    code = "rate_limited"


class Unsupported(OlcfError):
    """Fail-closed: the requested semantics are not supported (never silently substituted)."""

    status = 422
    code = "unsupported"


class Unavailable(OlcfError):
    """A required component is not available right now (HTTP 503, code ``unavailable``)."""
    status = 503
    code = "unavailable"


class CompatibilityBlocked(Unsupported):
    """The operation is blocked because it would not behave as the source platform does (code ``compatibility_blocked``); a kind of ``Unsupported``.
    """
    code = "compatibility_blocked"


class PolicyDenied(Forbidden):
    """A configured policy forbids the operation (HTTP 403, code ``policy_denied``)."""
    code = "policy_denied"


class FailureCategory(StrEnum):
    """Job failure classification (spec section 33)."""

    COMPATIBILITY = "compatibility"
    AUTHN = "authn"
    AUTHZ = "authz"
    STORAGE = "storage"
    CATALOG = "catalog"
    DEPENDENCY = "dependency"
    RESOURCE = "resource"
    SPARK = "spark"
    USER_CODE = "user-code"
    TIMEOUT = "timeout"
    CANCELLATION = "cancellation"
    PROVIDER = "provider"
    INTERNAL = "internal"
    RESTORE_RECONCILE = "restore_reconcile"  # the job was queued/running when a backup was taken and did not survive the restore into a new environment
