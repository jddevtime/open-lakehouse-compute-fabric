# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Python SDK for the Open Lakehouse Compute Fabric REST API.

``from olcf.sdk import Client`` (sync) or ``AsyncClient``.  Everything the API does is reachable through typed resource groups
(``client.workspaces``, ``.items``, ``.notebooks``, ``.lakehouses``, ``.sql``, ``.jobs``, ``.pipelines``, ``.connections``, ``.compute``,
``.runtimes``, ``.fabric``, ``.mcp``, ``.intelligence``, ``.governance`` ...).  Errors are subclasses of :class:`ApiError`; see :mod:`olcf.sdk.errors`.
"""

from __future__ import annotations

from olcf.sdk._core import Page, Resource
from olcf.sdk.aio import AsyncClient
from olcf.sdk.client import Client
from olcf.sdk.contexts import ContextStore
from olcf.sdk.errors import (
    ApiError,
    AuthenticationError,
    BadRequestError,
    ConflictError,
    JobFailedError,
    NotFoundError,
    OlcfConnectionError,
    OlcfSdkError,
    OlcfTimeoutError,
    PermissionDeniedError,
    PreconditionFailedError,
    RateLimitError,
    ServerError,
    UnavailableError,
    UnsupportedError,
    ValidationError,
)
from olcf.version import __version__

__all__ = [
    "ApiError", "AsyncClient", "AuthenticationError", "BadRequestError", "Client", "ConflictError", "ContextStore", "JobFailedError",
    "NotFoundError", "OlcfConnectionError", "OlcfSdkError", "OlcfTimeoutError", "Page", "PermissionDeniedError", "PreconditionFailedError",
    "RateLimitError", "Resource", "ServerError", "UnavailableError", "UnsupportedError", "ValidationError", "__version__",
]
