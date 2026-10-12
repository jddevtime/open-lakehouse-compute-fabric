# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Exception hierarchy of the OLCF Python SDK.

Every non-2xx answer of the REST API carries the canonical error envelope
(``{"error": {"code", "message", "details", "remediation", "correlation_id"}}``, CONTRACTS section 7).  The SDK turns it into
an :class:`ApiError` subclass chosen by HTTP status (and, for ``422``, by the error code) so callers can write
``except NotFoundError`` instead of inspecting status codes.  Messages, details and ``repr`` are passed through
:func:`olcf.core.redaction.redact_text` / ``redact_obj`` so a traceback pasted into a ticket never leaks a token.
"""

from __future__ import annotations

from typing import Any

from olcf.core.redaction import redact_obj, redact_text


class OlcfSdkError(Exception):
    """Base class of everything the SDK raises on purpose."""


class OlcfConnectionError(OlcfSdkError):
    """The control plane could not be reached (DNS, TCP, TLS, proxy, timeout) after the configured retries."""

    def __init__(self, message: str, *, url: str | None = None) -> None:
        super().__init__(redact_text(message))
        self.url = url


class OlcfTimeoutError(OlcfSdkError):
    """A ``wait`` helper ran out of time (the server-side work may still be running)."""

    def __init__(self, message: str, *, last: Any = None) -> None:
        super().__init__(redact_text(message))
        self.last = last


class ApiError(OlcfSdkError):
    """A non-2xx answer from the REST API."""

    status: int = 0

    def __init__(self, message: str, *, status: int, code: str = "http_error", details: dict[str, Any] | None = None,
                 remediation: str | None = None, correlation_id: str | None = None, headers: dict[str, str] | None = None,
                 method: str | None = None, url: str | None = None) -> None:
        super().__init__(redact_text(message))
        self.message = redact_text(message)
        self.status = status
        self.code = code
        self.details = redact_obj(details or {})
        self.remediation = remediation
        self.correlation_id = correlation_id
        self.headers = dict(headers or {})
        self.method = method
        self.url = url

    @property
    def retry_after(self) -> float | None:
        """Seconds the server asked us to wait (``Retry-After``), when present."""
        raw = self.headers.get("retry-after")
        try:
            return float(raw) if raw is not None else None
        except ValueError:
            return None

    def __str__(self) -> str:
        parts = [f"{self.status} {self.code}: {self.message}"]
        if self.remediation:
            parts.append(f"hint: {self.remediation}")
        if self.correlation_id:
            parts.append(f"(correlation id {self.correlation_id})")
        return " ".join(parts)

    def __repr__(self) -> str:
        return f"{type(self).__name__}(status={self.status}, code={self.code!r}, message={self.message!r})"


class BadRequestError(ApiError):
    """HTTP 400."""


class AuthenticationError(ApiError):
    """HTTP 401: missing, expired or invalid credentials."""


class PermissionDeniedError(ApiError):
    """HTTP 403: authenticated but not authorised (RBAC, token scope or policy)."""


class NotFoundError(ApiError):
    """HTTP 404."""


class ConflictError(ApiError):
    """HTTP 409 (duplicate name, invalid state transition, idempotency key reuse...)."""


class PreconditionFailedError(ApiError):
    """HTTP 412: the ``If-Match`` ETag no longer matches; re-read the resource and retry."""


class ValidationError(ApiError):
    """HTTP 422: the request is well-formed JSON but semantically invalid."""


class UnsupportedError(ValidationError):
    """HTTP 422 with code ``unsupported`` / ``compatibility_blocked``: fail-closed, the platform refuses to guess."""


class RateLimitError(ApiError):
    """HTTP 429."""


class ServerError(ApiError):
    """HTTP 5xx."""


class UnavailableError(ServerError):
    """HTTP 503: a service is not available in this deployment (or is starting)."""


class JobFailedError(OlcfSdkError):
    """Raised by ``jobs.wait(..., raise_on_failure=True)`` when the job ended in a state other than ``succeeded``."""

    def __init__(self, job: dict[str, Any]) -> None:
        failure = job.get("failure") or {}
        msg = f"job {job.get('id')} ended {job.get('state')}"
        if failure.get("message"):
            msg += f": {failure['message']}"
        super().__init__(redact_text(msg))
        self.job = job
        self.failure = failure


def error_from_response(status: int, body: Any, *, headers: dict[str, str], method: str, url: str) -> ApiError:
    """Build the right :class:`ApiError` subclass from a decoded error body."""
    env = body.get("error") if isinstance(body, dict) and isinstance(body.get("error"), dict) else None
    if env is None:
        text = body if isinstance(body, str) else (str(body.get("detail")) if isinstance(body, dict) and "detail" in body else "")
        env = {"code": "http_error", "message": (text or f"HTTP {status}")[:500]}
    code = str(env.get("code") or "http_error")
    kwargs: dict[str, Any] = {
        "status": status, "code": code, "details": env.get("details") if isinstance(env.get("details"), dict) else {},
        "remediation": env.get("remediation"), "correlation_id": env.get("correlation_id"),
        "headers": {k.lower(): v for k, v in headers.items()}, "method": method, "url": url,
    }
    message = str(env.get("message") or code)
    cls: type[ApiError]
    if status == 400:
        cls = BadRequestError
    elif status == 401:
        cls = AuthenticationError
    elif status == 403:
        cls = PermissionDeniedError
    elif status == 404:
        cls = NotFoundError
    elif status == 409:
        cls = ConflictError
    elif status == 412:
        cls = PreconditionFailedError
    elif status == 422:
        cls = UnsupportedError if code in ("unsupported", "compatibility_blocked") else ValidationError
    elif status == 429:
        cls = RateLimitError
    elif status == 503:
        cls = UnavailableError
    elif status >= 500:
        cls = ServerError
    else:
        cls = ApiError
    return cls(message, **kwargs)
