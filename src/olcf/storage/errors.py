# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Translate provider-library exceptions (boto3, Azure SDK, OS) into platform errors with actionable messages."""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager

from olcf.core.errors import Forbidden, NotFound, OlcfError, Unavailable
from olcf.core.redaction import redact_text


class PayloadTooLarge(OlcfError):
    """The data exceeds the allowed size (HTTP 413, code ``payload_too_large``)."""
    status = 413
    code = "payload_too_large"


def translate(exc: BaseException) -> OlcfError | None:
    """Return an :class:`OlcfError` for well-known library failures, else ``None`` (caller re-raises)."""
    if isinstance(exc, OlcfError):
        return None
    name = type(exc).__name__
    resp = getattr(exc, "response", None)
    if isinstance(resp, dict):  # botocore ClientError
        code = resp.get("Error", {}).get("Code", "")
        if code in ("404", "NoSuchKey", "NoSuchBucket", "NotFound"):
            return NotFound(redact_text(f"object store reports: {code}"))
        if code in ("403", "AccessDenied", "InvalidAccessKeyId", "SignatureDoesNotMatch", "ExpiredToken"):
            return Forbidden(f"the object store rejected the lakehouse credentials ({code})",
                             remediation="re-test the connection and check the access key, secret and bucket policy")
        return Unavailable(redact_text(f"object store error: {code or name}"), remediation="check connectivity with 'Test connectivity' on the connection")
    status = getattr(exc, "status_code", None)
    if name.endswith(("HttpResponseError", "ResourceNotFoundError", "ClientAuthenticationError", "ResourceExistsError")) or (status and name.startswith("Azure")):
        if status == 404 or name == "ResourceNotFoundError":
            return NotFound("Azure storage reports: path not found")
        if status in (401, 403) or name == "ClientAuthenticationError":
            return Forbidden("Azure storage rejected the credentials", remediation="check the service principal's role assignment on the storage account/workspace")
        return Unavailable(redact_text(f"Azure storage error: {getattr(exc, 'message', name)}"[:300]))
    if name in ("EndpointConnectionError", "ConnectTimeoutError", "ReadTimeoutError", "ConnectionError", "ServiceRequestError", "ConnectError"):
        return Unavailable("could not reach the storage endpoint", remediation="run 'Test connectivity' to see which step fails (DNS / connect / auth)")
    if isinstance(exc, PermissionError):
        return Forbidden("the service account has no permission on the storage location")
    if isinstance(exc, FileNotFoundError):
        return NotFound("path does not exist")
    return None


@contextmanager
def storage_errors() -> Iterator[None]:
    try:
        yield
    except OlcfError:
        raise
    except Exception as exc:  # noqa: BLE001
        t = translate(exc)
        if t is None:
            raise
        raise t from exc
