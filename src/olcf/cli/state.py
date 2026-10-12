# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Per-invocation CLI state, exit codes and error reporting.

Exit codes (stable, documented in ``docs/guides/cli.md`` and ``olcf --help``):

====  =================================================================
0     success
1     general failure / unexpected error / ``doctor`` found a failing check
2     usage error (bad option or argument)
3     authentication required or failed (HTTP 401) - run ``olcf login``
4     permission denied (HTTP 403)
5     not found (HTTP 404)
6     conflict or precondition failed (HTTP 409 / 412)
7     rejected request (HTTP 400 / 422), including fail-closed ``unsupported``
8     server error, service unavailable or server unreachable
9     a ``--wait`` ran out of time (the work may still be running)
10    the awaited job or run ended in a state other than ``succeeded``
11    prerequisite missing (Python / JDK / uv / disk / port)
130   interrupted (Ctrl-C)
====  =================================================================
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import click

from olcf.cli.output import Output
from olcf.sdk import (
    ApiError,
    AuthenticationError,
    BadRequestError,
    Client,
    ConflictError,
    JobFailedError,
    NotFoundError,
    OlcfConnectionError,
    OlcfSdkError,
    OlcfTimeoutError,
    PermissionDeniedError,
    PreconditionFailedError,
    ServerError,
    ValidationError,
)
from olcf.sdk.contexts import ContextStore

EXIT_OK, EXIT_FAIL, EXIT_USAGE, EXIT_AUTH, EXIT_FORBIDDEN, EXIT_NOT_FOUND, EXIT_CONFLICT, EXIT_INVALID, EXIT_SERVER, EXIT_TIMEOUT, EXIT_JOB_FAILED, EXIT_PREREQ = (
    0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11)

DEFAULT_URL = "http://127.0.0.1:8470"


class PrerequisiteError(click.ClickException):
    """A tool the command needs is missing (exit 11)."""

    exit_code = EXIT_PREREQ


def exit_code_for(exc: BaseException) -> int:
    if isinstance(exc, AuthenticationError):
        return EXIT_AUTH
    if isinstance(exc, PermissionDeniedError):
        return EXIT_FORBIDDEN
    if isinstance(exc, NotFoundError):
        return EXIT_NOT_FOUND
    if isinstance(exc, (ConflictError, PreconditionFailedError)):
        return EXIT_CONFLICT
    if isinstance(exc, (BadRequestError, ValidationError)):
        return EXIT_INVALID
    if isinstance(exc, (ServerError, OlcfConnectionError)):
        return EXIT_SERVER
    if isinstance(exc, OlcfTimeoutError):
        return EXIT_TIMEOUT
    if isinstance(exc, JobFailedError):
        return EXIT_JOB_FAILED
    if isinstance(exc, ApiError):
        return EXIT_FAIL
    return EXIT_FAIL


def hint_for(exc: BaseException) -> str | None:
    if isinstance(exc, ApiError) and exc.remediation:
        return exc.remediation
    if isinstance(exc, AuthenticationError):
        return "run `olcf login` (or set OLCF_TOKEN) and retry"
    if isinstance(exc, OlcfConnectionError):
        return "is the server running? try `olcf status`, `olcf serve`, or `olcf quickstart`; check --url / OLCF_URL"
    if isinstance(exc, PermissionDeniedError):
        return "your role does not allow this; ask an administrator or check `olcf whoami`"
    return None


@dataclass
class State:
    """What every command needs: output, connection overrides, local data dir."""

    out: Output
    fmt: str = "table"
    context: str | None = None
    url: str | None = None
    token: str | None = None
    ca_bundle: str | None = None
    data_dir: str | None = None
    timeout: float = 60.0
    verbose: int = 0
    _client: Client | None = field(default=None, repr=False)

    # ------------------------------------------------------------------ connection
    def resolve_connection(self) -> dict[str, Any]:
        """Precedence: flags > environment (OLCF_URL / OLCF_TOKEN) > selected context > local default."""
        store = ContextStore()
        ctx = store.get(self.context) if (self.context or os.environ.get("OLCF_CONTEXT") or not (self.url or os.environ.get("OLCF_URL"))) else None
        if (self.context or os.environ.get("OLCF_CONTEXT")) and ctx is None:
            raise click.ClickException(f"unknown context '{self.context or os.environ.get('OLCF_CONTEXT')}' (see `olcf context list`)")
        url = self.url or os.environ.get("OLCF_URL") or (ctx or {}).get("url") or DEFAULT_URL
        token = self.token or os.environ.get("OLCF_TOKEN") or ((ctx or {}).get("token") if (not self.url or (ctx or {}).get("url") == self.url) else None)
        return {"url": url, "token": token, "refresh_token": (ctx or {}).get("refresh_token") if token == (ctx or {}).get("token") else None,
                "ca_bundle": self.ca_bundle or os.environ.get("OLCF_CA_BUNDLE") or (ctx or {}).get("ca_bundle"), "context": (ctx or {}).get("name"),
                "workspace": (ctx or {}).get("workspace")}

    @property
    def client(self) -> Client:
        if self._client is None:
            conn = self.resolve_connection()
            c = Client(conn["url"], token=conn["token"], refresh_token=conn["refresh_token"], ca_bundle=conn["ca_bundle"], timeout=self.timeout,
                       user_agent=None)
            if conn["context"]:
                name = conn["context"]
                c.on_tokens = lambda access, refresh: ContextStore().set(name, make_current=False, token=access, refresh_token=refresh)
            self._client = c
        return self._client

    def default_workspace(self, explicit: str | None) -> str:
        ws = explicit or os.environ.get("OLCF_WORKSPACE") or self.resolve_connection().get("workspace")
        if not ws:
            raise click.UsageError("no workspace given: use -w/--workspace NAME, set OLCF_WORKSPACE, or run `olcf workspace use NAME`")
        return str(ws)

    # ------------------------------------------------------------------ local (no server) helpers
    def data_path(self) -> Path:
        """``--data-dir`` > ``$OLCF_DATA_DIR`` > the data directory the installer recorded in ``install.json`` > ``~/.olcf``."""
        from olcf.core.config import default_data_dir

        if self.data_dir:
            return Path(self.data_dir).expanduser().resolve()
        if not os.environ.get("OLCF_DATA_DIR"):
            recorded = _installed_data_dir()
            if recorded:
                return recorded
        return default_data_dir()

    def settings(self, **overrides: Any) -> Any:
        from olcf.core.config import load_settings

        return load_settings(self.data_path(), **overrides)


def _installed_data_dir() -> Path | None:
    """The ``data_dir`` field of the installer's ``install.json`` (found relative to the running interpreter: ``<prefix>/versions/<v>/venv``)."""
    import json
    import sys

    env = os.environ.get("OLCF_HOME")
    here = Path(sys.prefix).resolve()
    for cand in ([Path(env) / "install.json"] if env else []) + [p / "install.json" for p in list(here.parents)[:4]]:
        try:
            value = json.loads(cand.read_text(encoding="utf-8")).get("data_dir")
        except (OSError, ValueError, AttributeError):
            continue
        if isinstance(value, str) and value:
            return Path(value).expanduser()
    return None


def report_exception(out: Output, exc: BaseException) -> int:
    """Print an SDK error for humans (or as JSON) and return its exit code."""
    code = exit_code_for(exc)
    if out.json_mode:
        import json

        body: dict[str, Any] = {"error": {"type": type(exc).__name__, "message": str(exc)}}
        if isinstance(exc, ApiError):
            body["error"].update({"status": exc.status, "code": exc.code, "details": exc.details, "remediation": exc.remediation, "correlation_id": exc.correlation_id})
        body["error"]["exit_code"] = code
        print(json.dumps(body, default=str), file=out.err)
    else:
        out.error(str(exc), hint_for(exc))
    return code


SDK_ERRORS = (OlcfSdkError,)
