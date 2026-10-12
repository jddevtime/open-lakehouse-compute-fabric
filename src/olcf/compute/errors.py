# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Compute specific errors (all map onto the platform error envelope)."""

from __future__ import annotations

from olcf.core.errors import OlcfError, Unavailable, Unsupported


class WorkerOffline(Unavailable):
    """The command's target worker has no live connection (to this or any replica)."""
    code = "worker_offline"


class WorkerBusy(Unavailable):
    """The per-worker send queue is full (backpressure)."""

    code = "worker_busy"


class WorkerTimeout(Unavailable):
    """The worker did not answer a command within its timeout."""
    status = 504
    code = "worker_timeout"


class WorkerCommandFailed(OlcfError):
    """The agent answered a command with ``ok: false``."""

    status = 502
    code = "worker_command_failed"


class ResourceUnsatisfiable(Unsupported):
    """The request can never fit on the registered fleet (fail closed instead of queueing forever)."""

    code = "resource_unsatisfiable"


class ProviderError(OlcfError):
    """A compute provider failed to carry out an operation; the message and remediation come from the provider."""
    status = 502
    code = "provider_error"
    #: True when the failure left it unknown whether the remote system accepted a create request (see :class:`SubmissionUncertain`).
    uncertain = False


class SubmissionUncertain(ProviderError):
    """A request that creates remote work (a Livy session, statement or batch) may or may not have been accepted: the response was lost after the request could
    have been transmitted. Such a request must never be replayed blindly; the provider reconciles against the remote state instead (CONTRACTS, PROV addenda)."""
    code = "submission_uncertain"
    uncertain = True


class ArtifactTooLarge(OlcfError):
    """An uploaded job artifact is larger than the configured limit."""
    status = 413
    code = "payload_too_large"
