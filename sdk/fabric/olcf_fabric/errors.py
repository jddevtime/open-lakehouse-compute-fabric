# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Exceptions raised by the SDK. All derive from :class:`OlcfFabricError`; every one carries a human-readable ``remediation`` when the server (or the SDK) knows one."""

from __future__ import annotations

from typing import Any


class OlcfFabricError(Exception):
    """Base class."""

    def __init__(self, message: str, *, remediation: str | None = None, details: dict[str, Any] | None = None) -> None:
        super().__init__(message)
        self.message = message
        self.remediation = remediation
        self.details = details or {}

    def __str__(self) -> str:
        return self.message + (f"\n  -> {self.remediation}" if self.remediation else "")


class ConfigurationError(OlcfFabricError):
    """The SDK is not (fully) configured, or the configuration is inconsistent."""


class AuthenticationError(OlcfFabricError):
    """No credential could be obtained, or the server rejected it."""


class ConnectionFailed(OlcfFabricError):
    """The control plane could not be reached (DNS, TLS, network, timeout)."""


class ServerError(OlcfFabricError):
    """The control plane answered with an error. ``status`` is the HTTP status, ``code`` the stable error code (for example ``capsule_replay``)."""

    def __init__(self, message: str, *, status: int, code: str | None = None, remediation: str | None = None, details: dict[str, Any] | None = None) -> None:
        super().__init__(message, remediation=remediation, details=details)
        self.status = status
        self.code = code


class SubmissionRejected(ServerError):
    """The capsule was rejected before anything ran (signature, tenant, workspace, compatibility, policy ...)."""


class CompatibilityError(SubmissionRejected):
    """The notebook uses constructs the on-prem runtime cannot run. ``report`` is the analyzer report (blockers, findings, counts)."""

    def __init__(self, message: str, *, status: int = 422, code: str | None = "compatibility_blocked", remediation: str | None = None, details: dict[str, Any] | None = None,
                 report: dict[str, Any] | None = None) -> None:
        super().__init__(message, status=status, code=code, remediation=remediation, details=details)
        self.report = report or {}

    @property
    def blockers(self) -> list[dict[str, Any]]:
        return list(self.report.get("blockers") or self.details.get("blockers") or [])


class RemoteExecutionError(OlcfFabricError):
    """The remote run finished in a state other than ``succeeded``. Raising (instead of returning) makes a Fabric pipeline activity fail, as a local failure would.

    Attributes: ``state``, ``job_id``, ``submission_id``, ``failure`` (category, message, cell, line, traceback), ``logs`` (the tail of the remote log).
    """

    def __init__(self, message: str, *, state: str, job_id: str | None, submission_id: str | None, failure: dict[str, Any] | None = None, logs: str = "",
                 remediation: str | None = None) -> None:
        super().__init__(message, remediation=remediation, details={"state": state, "job_id": job_id, "submission_id": submission_id, "failure": failure or {}})
        self.state = state
        self.job_id = job_id
        self.submission_id = submission_id
        self.failure = failure or {}
        self.logs = logs

    @property
    def category(self) -> str | None:
        return self.failure.get("category")


__all__ = ["AuthenticationError", "CompatibilityError", "ConfigurationError", "ConnectionFailed", "OlcfFabricError", "RemoteExecutionError", "ServerError", "SubmissionRejected"]
