# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Synchronous OLCF client (``from olcf.sdk import Client``).

::

    from olcf.sdk import Client

    with Client("http://127.0.0.1:8470", token="olcf_pat_...") as c:
        ws = c.workspaces.create("analytics")
        job = c.notebooks.run(nb_id, parameters={"year": 2025})
        job = c.jobs.wait(job["id"], timeout=600, raise_on_failure=True)
        print(c.sql.execute("SELECT 1 AS x", workspace_id=ws["id"])["rows"])

Authentication is a personal access token (``token=``), a login (``Client.login``) or the environment (``OLCF_URL``, ``OLCF_TOKEN``, and
the CA bundle variables ``OLCF_CA_BUNDLE``/``SSL_CERT_FILE``/``REQUESTS_CA_BUNDLE``; ``HTTP(S)_PROXY`` is honoured for non-loopback URLs).  An expired
access token is refreshed transparently when a refresh token is known.  See :mod:`olcf.sdk._core` for the retry policy.
"""

from __future__ import annotations

import contextlib
import logging
import os
import threading
import time
from collections.abc import Callable, Iterator
from typing import Any

import httpx

from olcf.core.redaction import redact_text
from olcf.sdk import _core as core
from olcf.sdk._resources import (
    Admin,
    Catalog,
    Connections,
    Identity,
    Items,
    Jobs,
    Lakehouses,
    Notebooks,
    Sessions,
    Sql,
    Workspaces,
)
from olcf.sdk._resources2 import Compute, Environments, Fabric, Mcp, Pipelines, Runtimes, Sample
from olcf.sdk._resources3 import Backups, Diagnostics, Retention, Security, Updates
from olcf.sdk._resources4 import Intelligence
from olcf.sdk._resources5 import Governance
from olcf.sdk.errors import (
    AuthenticationError,
    JobFailedError,
    OlcfConnectionError,
    OlcfTimeoutError,
)
from olcf.version import __version__

log = logging.getLogger("olcf.sdk")
DEFAULT_URL = "http://127.0.0.1:8470"
TERMINAL_JOB_STATES = frozenset({"succeeded", "failed", "cancelled", "timed_out"})
NO_REFRESH_PATHS = frozenset({"/identity/login", "/identity/refresh", "/identity/logout"})
TERMINAL_OP_STATES = frozenset({"succeeded", "failed", "cancelled", "completed", "error", "done"})
TERMINAL_SCAN_STATES = frozenset({"succeeded", "partial", "failed", "cancelled"})


def build_config(url: str | None, *, token: str | None, refresh_token: str | None, timeout: float, max_retries: int, verify: Any, ca_bundle: str | None,
                 proxy: str | None, headers: dict[str, str] | None, user_agent: str | None, trust_env: bool, auto_refresh: bool) -> core.ClientConfig:
    base = core.normalise_base_url(url or os.environ.get("OLCF_URL") or DEFAULT_URL)
    return core.ClientConfig(
        base_url=base, token=token if token is not None else os.environ.get("OLCF_TOKEN"), refresh_token=refresh_token, timeout=timeout,
        max_retries=max_retries, verify=core.resolve_verify(ca_bundle if ca_bundle else verify), proxy=proxy, trust_env=trust_env,
        user_agent=user_agent or f"olcf-sdk/{__version__}", headers=dict(headers or {}), auto_refresh=auto_refresh)


class JobsApi(Jobs):
    """:class:`~olcf.sdk._resources.Jobs` plus the blocking helpers."""

    def iter(self, **filters: Any) -> Iterator[dict[str, Any]]:
        """Every job matching the filters, following ``next_cursor``."""
        return self._c.paginate(self.list, **filters)

    def wait(self, job_id: str, *, timeout: float | None = None, poll: float = 1.0, max_poll: float = 5.0, raise_on_failure: bool = False,
             on_update: Callable[[dict[str, Any]], None] | None = None) -> dict[str, Any]:
        """Poll until the job reaches a terminal state and return the final record.

        Raises :class:`~olcf.sdk.errors.OlcfTimeoutError` after ``timeout`` seconds (the job keeps running) and, with
        ``raise_on_failure``, :class:`~olcf.sdk.errors.JobFailedError` for any terminal state other than ``succeeded``.
        """
        deadline = None if timeout is None else time.monotonic() + timeout
        last_state: str | None = None
        interval = poll
        while True:
            job = self.get(job_id)
            if on_update and job.get("state") != last_state:
                on_update(job)
            last_state = job.get("state")
            if last_state in TERMINAL_JOB_STATES:
                if raise_on_failure and last_state != "succeeded":
                    raise JobFailedError(job)
                return job
            if deadline is not None and time.monotonic() >= deadline:
                raise OlcfTimeoutError(f"job {job_id} still {last_state} after {timeout:g}s", last=job)
            time.sleep(interval if deadline is None else max(0.0, min(interval, deadline - time.monotonic())))
            interval = min(max_poll, interval * 1.3)

    def logs(self, job_id: str, *, follow: bool = False, stream: str = "combined", offset: int = 0, poll: float = 1.0, page_size: int = 65536,
             timeout: float | None = None) -> Iterator[str]:
        """Yield log text chunks.  With ``follow`` the generator keeps polling until the job is finished and the log is drained."""
        pos = offset
        deadline = None if timeout is None else time.monotonic() + timeout
        while True:
            page = self.logs_page(job_id, offset=pos, limit=page_size, stream=stream)
            if page.get("data"):
                pos = int(page["next_offset"])
                yield page["data"]
                continue                       # drain without sleeping
            if not follow or page.get("eof"):
                return
            if deadline is not None and time.monotonic() >= deadline:
                raise OlcfTimeoutError(f"log of {job_id} still open after {timeout:g}s")
            time.sleep(poll)

    def submit_and_wait(self, submission: dict[str, Any] | None = None, *, timeout: float | None = None, raise_on_failure: bool = True, **fields: Any) -> dict[str, Any]:
        job = self.submit(submission, **fields)
        return self.wait(job["id"], timeout=timeout, raise_on_failure=raise_on_failure)


class IntelligenceApi(Intelligence):
    """:class:`~olcf.sdk._resources4.Intelligence` plus paginating iterators (``for a in client.intelligence.iter_assets(type="table"): ...``)."""

    def iter_assets(self, **filters: Any) -> Iterator[dict[str, Any]]:
        """Every asset matching the filters of :meth:`list_assets`, following ``next_cursor``."""
        return self._c.paginate(self.list_assets, **filters)

    def iter_children(self, asset_id: str, **filters: Any) -> Iterator[dict[str, Any]]:
        """Every child of ``asset_id`` (see :meth:`children`), following ``next_cursor``."""
        return self._c.paginate(self.children, asset_id, **filters)

    def iter_history(self, asset_id: str, **filters: Any) -> Iterator[dict[str, Any]]:
        """Every attribute version of ``asset_id`` (see :meth:`history`), newest first."""
        return self._c.paginate(self.history, asset_id, **filters)

    def iter_snapshots(self, **filters: Any) -> Iterator[dict[str, Any]]:
        """Every snapshot (scan) matching the filters of :meth:`snapshots`, newest first."""
        return self._c.paginate(self.snapshots, **filters)

    def iter_unused(self, **filters: Any) -> Iterator[dict[str, Any]]:
        """Every unused table matching the filters of :meth:`unused`."""
        return self._c.paginate(self.unused, **filters)

    def iter_scans(self, **filters: Any) -> Iterator[dict[str, Any]]:
        """Every scan matching the filters of :meth:`list_scans`, newest first."""
        return self._c.paginate(self.list_scans, **filters)

    def iter_gaps(self, **filters: Any) -> Iterator[dict[str, Any]]:
        """Every gap matching the filters of :meth:`list_gaps`."""
        return self._c.paginate(self.list_gaps, **filters)

    def wait_scan(self, scan_id: str, *, timeout: float | None = 600.0, poll: float = 1.0, max_poll: float = 5.0) -> dict[str, Any]:
        """Poll until the scan has ended (``succeeded``, ``partial``, ``failed`` or ``cancelled``) and return its final record.

        Raises :class:`~olcf.sdk.errors.OlcfTimeoutError` after ``timeout`` seconds (the scan keeps running); a non-successful end is returned, not raised.
        """
        deadline = None if timeout is None else time.monotonic() + timeout
        interval = poll
        while True:
            scan = self.get_scan(scan_id)
            if scan.get("status") in TERMINAL_SCAN_STATES:
                return scan
            if deadline is not None and time.monotonic() >= deadline:
                raise OlcfTimeoutError(f"scan {scan_id} still {scan.get('status')} after {timeout:g}s", last=scan)
            time.sleep(interval if deadline is None else max(0.0, min(interval, deadline - time.monotonic())))
            interval = min(max_poll, interval * 1.3)


class GovernanceApi(Governance):
    """:class:`~olcf.sdk._resources5.Governance` plus paginating iterators (``for t in client.governance.iter_terms(): ...``)."""

    def iter_terms(self, **filters: Any) -> Iterator[dict[str, Any]]:
        """Every glossary term matching the filters of :meth:`list_terms`."""
        return self._c.paginate(self.list_terms, **filters)

    def iter_term_links(self, subject_id: str, **filters: Any) -> Iterator[dict[str, Any]]:
        """Every link of a term (see :meth:`term_links`)."""
        return self._c.paginate(self.term_links, subject_id, **filters)

    def iter_domains(self, **filters: Any) -> Iterator[dict[str, Any]]:
        """Every domain matching the filters of :meth:`list_domains`."""
        return self._c.paginate(self.list_domains, **filters)

    def iter_domain_assets(self, subject_id: str, **filters: Any) -> Iterator[dict[str, Any]]:
        """Every asset of a domain (see :meth:`domain_assets`)."""
        return self._c.paginate(self.domain_assets, subject_id, **filters)

    def iter_products(self, **filters: Any) -> Iterator[dict[str, Any]]:
        """Every data product matching the filters of :meth:`list_products`."""
        return self._c.paginate(self.list_products, **filters)

    def iter_product_consumers(self, subject_id: str, **filters: Any) -> Iterator[dict[str, Any]]:
        """Every consumer of a data product the server returned (see :meth:`product_consumers`, whose answer is one bounded document)."""
        yield from self.product_consumers(subject_id, **filters)["items"]

    def iter_classifications(self) -> Iterator[dict[str, Any]]:
        """Every classification label (see :meth:`list_classifications`)."""
        yield from self.list_classifications()

    def iter_classification_assets(self, subject_id: str, **filters: Any) -> Iterator[dict[str, Any]]:
        """Every asset carrying a label (see :meth:`classification_assets`)."""
        return self._c.paginate(self.classification_assets, subject_id, **filters)

    def iter_review_queue(self, **filters: Any) -> Iterator[dict[str, Any]]:
        """Every pending review item matching the filters of :meth:`review_queue`."""
        return self._c.paginate(self.review_queue, **filters)

    def iter_certifications(self, **filters: Any) -> Iterator[dict[str, Any]]:
        """Every certification matching the filters of :meth:`list_certifications`."""
        return self._c.paginate(self.list_certifications, **filters)

    def iter_changes(self, **filters: Any) -> Iterator[dict[str, Any]]:
        """Every change request matching the filters of :meth:`list_changes`."""
        return self._c.paginate(self.list_changes, **filters)

    def iter_policies(self, **filters: Any) -> Iterator[dict[str, Any]]:
        """Every policy matching the filters of :meth:`list_policies`."""
        return self._c.paginate(self.list_policies, **filters)

    def iter_violations(self, **filters: Any) -> Iterator[dict[str, Any]]:
        """Every violation matching the filters of :meth:`list_violations`."""
        return self._c.paginate(self.list_violations, **filters)


class AdminApi(Admin):
    """:class:`~olcf.sdk._resources.Admin` plus ``wait_operation``."""

    def wait_operation(self, operation_id: str, *, timeout: float = 300.0, poll: float = 1.0) -> dict[str, Any]:
        deadline = time.monotonic() + timeout
        while True:
            op = self.operation(operation_id)
            if op.get("state") in TERMINAL_OP_STATES:
                return op
            if time.monotonic() >= deadline:
                raise OlcfTimeoutError(f"operation {operation_id} still {op.get('state')} after {timeout:g}s", last=op)
            time.sleep(poll)

    def wait_until_ready(self, *, timeout: float = 60.0, poll: float = 0.5) -> dict[str, Any]:
        """Block until ``GET /v1/health/ready`` answers (useful right after starting a server)."""
        deadline = time.monotonic() + timeout
        last: Exception | None = None
        while time.monotonic() < deadline:
            try:
                return self.ready()
            except Exception as exc:  # noqa: BLE001 - not reachable yet / still starting
                last = exc
                time.sleep(poll)
        raise OlcfTimeoutError(f"control plane not ready after {timeout:g}s ({last})")


class Client:
    """Synchronous client.  Thread-safe (httpx clients are); create one per process and share it."""

    def __init__(self, url: str | None = None, *, token: str | None = None, refresh_token: str | None = None, username: str | None = None,
                 password: str | None = None, timeout: float = core.DEFAULT_TIMEOUT, max_retries: int = 3, verify: bool | str = True,
                 ca_bundle: str | None = None, proxy: str | None = None, headers: dict[str, str] | None = None, user_agent: str | None = None,
                 trust_env: bool = True, auto_refresh: bool = True, http_client: httpx.Client | None = None) -> None:
        self.cfg = build_config(url, token=token, refresh_token=refresh_token, timeout=timeout, max_retries=max_retries, verify=verify,
                                ca_bundle=ca_bundle, proxy=proxy, headers=headers, user_agent=user_agent, trust_env=trust_env, auto_refresh=auto_refresh)
        self._owns_http = http_client is None
        self._http: httpx.Client = http_client or httpx.Client(**self.cfg.httpx_kwargs())
        self._auth_lock = threading.Lock()
        self.on_tokens: Callable[[str, str | None], None] | None = None   # called after login/refresh (the CLI persists them)
        self.identity = Identity(self)
        self.admin = AdminApi(self)
        self.workspaces = Workspaces(self)
        self.items = Items(self)
        self.notebooks = Notebooks(self)
        self.lakehouses = Lakehouses(self)
        self.sql = Sql(self)
        self.catalog = Catalog(self)
        self.connections = Connections(self)
        self.jobs = JobsApi(self)
        self.sessions = Sessions(self)
        self.pipelines = Pipelines(self)
        self.environments = Environments(self)
        self.compute = Compute(self)
        self.runtimes = Runtimes(self)
        self.fabric = Fabric(self)
        self.mcp = Mcp(self)
        self.sample = Sample(self)
        self.backups = Backups(self)
        self.retention = Retention(self)
        self.diagnostics = Diagnostics(self)
        self.updates = Updates(self)
        self.security = Security(self)
        self.intelligence = IntelligenceApi(self)
        self.governance = GovernanceApi(self)
        self._notebook_helpers: Any = None
        if username and password:
            self.login(username, password)

    # ------------------------------------------------------------------ construction helpers
    @classmethod
    def from_env(cls, **kw: Any) -> Client:
        """``OLCF_URL`` / ``OLCF_TOKEN`` (+ CA variables) from the environment."""
        return cls(**kw)

    @classmethod
    def from_context(cls, name: str | None = None, **kw: Any) -> Client:
        """Use the credentials stored by ``olcf login`` (the current context, or ``name``).  Refreshed tokens are written back."""
        from olcf.sdk.contexts import ContextStore

        store = ContextStore()
        ctx = store.get(name)
        if ctx is None:
            raise AuthenticationError("no such context; run `olcf login`", status=401, code="no_context")
        client = cls(kw.pop("url", None) or ctx.get("url"), token=kw.pop("token", None) or ctx.get("token"), refresh_token=ctx.get("refresh_token"),
                     ca_bundle=kw.pop("ca_bundle", None) or ctx.get("ca_bundle"), **kw)
        cname = ctx["name"]

        def persist(access: str, refresh: str | None) -> None:
            ContextStore().set(cname, make_current=False, token=access, refresh_token=refresh)

        client.on_tokens = persist
        return client

    # ------------------------------------------------------------------ plumbing
    def __enter__(self) -> Client:
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()

    def close(self) -> None:
        if self._owns_http:
            self._http.close()

    def __repr__(self) -> str:
        return f"Client(url={self.cfg.base_url!r}, authenticated={bool(self.cfg.token)})"

    @property
    def base_url(self) -> str:
        return self.cfg.base_url

    @property
    def token(self) -> str | None:
        return self.cfg.token

    def set_token(self, token: str | None, refresh_token: str | None = None) -> None:
        self.cfg.token, self.cfg.refresh_token = token, refresh_token

    def request(self, method: str, path: str, **kw: Any) -> Any:
        """Escape hatch for endpoints without a wrapper: ``client.request("GET", "/jobs/stats")`` (``path`` is relative to ``/v1``)."""
        return self.call(method, path, **kw)

    def call(self, method: str, template: str, *, path: dict[str, Any] | None = None, auth: bool = True, raw: bool = False, as_bytes: bool = False, expect_error: bool = False,
             idem: bool = False, idempotency_key: str | None = None, if_match: Any = None, **kw: Any) -> Any:
        """Perform one request with retries, token refresh and error mapping (see :mod:`olcf.sdk._core`)."""
        prep = core.prepare(self.cfg, method, template, path or {}, json_body=kw.pop("json", None), idem=idem, idempotency_key=idempotency_key,
                            if_match=if_match, token=None if auth else "", **kw)
        attempt = 0
        refreshed = False
        while True:
            try:
                resp = self._http.request(prep.method, prep.url, **prep.kwargs)
            except httpx.HTTPError as exc:
                if core.should_retry_exception(self.cfg, prep, exc, attempt):
                    delay = core.retry_delay(self.cfg, attempt)
                    log.debug("retrying %s %s after %s in %.1fs", prep.method, prep.url, type(exc).__name__, delay)
                    time.sleep(delay)
                    attempt += 1
                    continue
                raise OlcfConnectionError(f"cannot reach {self.cfg.base_url}: {type(exc).__name__}: {str(exc)[:200]}", url=self.cfg.base_url) from exc
            if resp.status_code == 401 and auth and not refreshed and self.cfg.auto_refresh and self.cfg.refresh_token and template not in NO_REFRESH_PATHS:
                refreshed = True
                if self._refresh_tokens(prep.kwargs["headers"].get("Authorization", "")):
                    prep.kwargs["headers"]["Authorization"] = f"Bearer {self.cfg.token}"
                    continue
            if core.should_retry_response(self.cfg, prep, resp, attempt):
                delay = core.retry_delay(self.cfg, attempt, resp)
                log.debug("retrying %s %s after HTTP %s in %.1fs", prep.method, prep.url, resp.status_code, delay)
                time.sleep(delay)
                attempt += 1
                continue
            if expect_error and resp.status_code >= 400:
                return resp if raw else None
            core.raise_for_error(resp, prep)
            return core.decode(resp, raw=raw, as_bytes=as_bytes)

    def _refresh_tokens(self, used_header: str) -> bool:
        with self._auth_lock:
            if f"Bearer {self.cfg.token}" != used_header:      # another thread already refreshed
                return True
            try:
                doc = self.call("POST", "/identity/refresh", json={"refresh_token": self.cfg.refresh_token}, auth=False)
            except Exception as exc:  # noqa: BLE001
                log.debug("token refresh failed: %s", redact_text(str(exc)))
                return False
            self._store_tokens(doc)
            return True

    def _store_tokens(self, doc: dict[str, Any]) -> None:
        self.cfg.token = doc.get("access_token") or self.cfg.token
        self.cfg.refresh_token = doc.get("refresh_token") or self.cfg.refresh_token
        if self.on_tokens and self.cfg.token:
            with contextlib.suppress(Exception):
                self.on_tokens(self.cfg.token, self.cfg.refresh_token)

    # ------------------------------------------------------------------ auth
    def login(self, username: str, password: str) -> dict[str, Any]:
        """Username/password login; stores the access and refresh tokens on the client (and returns the full answer)."""
        doc = self.identity.login(username, password)
        self._store_tokens(doc)
        return doc

    def logout(self) -> None:
        """Revoke the session on the server (refresh token and access tokens) and forget the tokens locally."""
        rt = self.cfg.refresh_token
        try:
            if rt or self.cfg.token:
                self.identity.logout(rt)
        finally:
            self.cfg.token = self.cfg.refresh_token = None

    def me(self) -> dict[str, Any]:
        return self.identity.me()

    def health(self) -> dict[str, Any]:
        return self.admin.health()

    def info(self) -> dict[str, Any]:
        return self.admin.info()

    # ------------------------------------------------------------------ pagination
    def paginate(self, fn: Callable[..., Any], *args: Any, **kw: Any) -> Iterator[Any]:
        """Iterate every item of a list method: ``for j in client.paginate(client.jobs.list, state="failed")``."""
        cursor = kw.pop("cursor", None)
        while True:
            page = fn(*args, cursor=cursor, **kw)
            yield from page
            cursor = getattr(page, "next_cursor", None)
            if not cursor:
                return

    # ------------------------------------------------------------------ notebooks
    @property
    def notebook_helpers(self) -> Any:
        """:class:`olcf.sdk.notebook_helpers.NotebookHelpers` bound to this client (for use inside notebooks)."""
        if self._notebook_helpers is None:
            from olcf.sdk.notebook_helpers import NotebookHelpers

            self._notebook_helpers = NotebookHelpers(self)
        return self._notebook_helpers
