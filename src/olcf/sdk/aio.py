# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Asynchronous OLCF client (``from olcf.sdk import AsyncClient``).

The resource groups are the very same classes as in the sync client; here every call returns an awaitable::

    async with AsyncClient(url, token=tok) as c:
        job = await c.notebooks.run(nb_id)
        job = await c.jobs.wait(job["id"])
        async for chunk in c.jobs.logs(job["id"]):
            print(chunk, end="")
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import time
from collections.abc import AsyncIterator, Awaitable, Callable
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
from olcf.sdk.client import (
    NO_REFRESH_PATHS,
    TERMINAL_JOB_STATES,
    TERMINAL_OP_STATES,
    TERMINAL_SCAN_STATES,
    build_config,
)
from olcf.sdk.errors import JobFailedError, OlcfConnectionError, OlcfTimeoutError

log = logging.getLogger("olcf.sdk")


class AsyncJobsApi(Jobs):
    """Jobs with awaitable helpers."""

    def iter(self, **filters: Any) -> AsyncIterator[dict[str, Any]]:
        return self._c.paginate(self.list, **filters)

    async def wait(self, job_id: str, *, timeout: float | None = None, poll: float = 1.0, max_poll: float = 5.0, raise_on_failure: bool = False,
                   on_update: Callable[[dict[str, Any]], Any] | None = None) -> dict[str, Any]:
        """Async twin of :meth:`olcf.sdk.client.JobsApi.wait`."""
        deadline = None if timeout is None else time.monotonic() + timeout
        last_state: str | None = None
        interval = poll
        while True:
            job = await self.get(job_id)
            if on_update and job.get("state") != last_state:
                res = on_update(job)
                if asyncio.iscoroutine(res):
                    await res
            last_state = job.get("state")
            if last_state in TERMINAL_JOB_STATES:
                if raise_on_failure and last_state != "succeeded":
                    raise JobFailedError(job)
                return job
            if deadline is not None and time.monotonic() >= deadline:
                raise OlcfTimeoutError(f"job {job_id} still {last_state} after {timeout:g}s", last=job)
            await asyncio.sleep(interval if deadline is None else max(0.0, min(interval, deadline - time.monotonic())))
            interval = min(max_poll, interval * 1.3)

    async def logs(self, job_id: str, *, follow: bool = False, stream: str = "combined", offset: int = 0, poll: float = 1.0, page_size: int = 65536,
                   timeout: float | None = None) -> AsyncIterator[str]:
        pos = offset
        deadline = None if timeout is None else time.monotonic() + timeout
        while True:
            page = await self.logs_page(job_id, offset=pos, limit=page_size, stream=stream)
            if page.get("data"):
                pos = int(page["next_offset"])
                yield page["data"]
                continue
            if not follow or page.get("eof"):
                return
            if deadline is not None and time.monotonic() >= deadline:
                raise OlcfTimeoutError(f"log of {job_id} still open after {timeout:g}s")
            await asyncio.sleep(poll)

    async def submit_and_wait(self, submission: dict[str, Any] | None = None, *, timeout: float | None = None, raise_on_failure: bool = True,
                              **fields: Any) -> dict[str, Any]:
        job = await self.submit(submission, **fields)
        return await self.wait(job["id"], timeout=timeout, raise_on_failure=raise_on_failure)


class AsyncIntelligenceApi(Intelligence):
    """Intelligence with async iterators (``async for a in client.intelligence.iter_assets(): ...``) and an awaitable scan wait."""

    def iter_assets(self, **filters: Any) -> AsyncIterator[dict[str, Any]]:
        """Every asset matching the filters of :meth:`list_assets`, following ``next_cursor``."""
        return self._c.paginate(self.list_assets, **filters)

    def iter_children(self, asset_id: str, **filters: Any) -> AsyncIterator[dict[str, Any]]:
        """Every child of ``asset_id`` (see :meth:`children`), following ``next_cursor``."""
        return self._c.paginate(self.children, asset_id, **filters)

    def iter_history(self, asset_id: str, **filters: Any) -> AsyncIterator[dict[str, Any]]:
        """Every attribute version of ``asset_id`` (see :meth:`history`), newest first."""
        return self._c.paginate(self.history, asset_id, **filters)

    def iter_snapshots(self, **filters: Any) -> AsyncIterator[dict[str, Any]]:
        """Every snapshot (scan) matching the filters of :meth:`snapshots`, newest first."""
        return self._c.paginate(self.snapshots, **filters)

    def iter_unused(self, **filters: Any) -> AsyncIterator[dict[str, Any]]:
        """Every unused table matching the filters of :meth:`unused`."""
        return self._c.paginate(self.unused, **filters)

    def iter_scans(self, **filters: Any) -> AsyncIterator[dict[str, Any]]:
        """Every scan matching the filters of :meth:`list_scans`, newest first."""
        return self._c.paginate(self.list_scans, **filters)

    def iter_gaps(self, **filters: Any) -> AsyncIterator[dict[str, Any]]:
        """Every gap matching the filters of :meth:`list_gaps`."""
        return self._c.paginate(self.list_gaps, **filters)

    async def wait_scan(self, scan_id: str, *, timeout: float | None = 600.0, poll: float = 1.0, max_poll: float = 5.0) -> dict[str, Any]:  # type: ignore[override]
        """Async twin of :meth:`olcf.sdk.client.IntelligenceApi.wait_scan`."""
        deadline = None if timeout is None else time.monotonic() + timeout
        interval = poll
        while True:
            scan = await self.get_scan(scan_id)
            if scan.get("status") in TERMINAL_SCAN_STATES:
                return scan
            if deadline is not None and time.monotonic() >= deadline:
                raise OlcfTimeoutError(f"scan {scan_id} still {scan.get('status')} after {timeout:g}s", last=scan)
            await asyncio.sleep(interval if deadline is None else max(0.0, min(interval, deadline - time.monotonic())))
            interval = min(max_poll, interval * 1.3)


class AsyncGovernanceApi(Governance):
    """Governance with async iterators (``async for t in client.governance.iter_terms(): ...``)."""

    def iter_terms(self, **filters: Any) -> AsyncIterator[dict[str, Any]]:
        """Every glossary term matching the filters of :meth:`list_terms`."""
        return self._c.paginate(self.list_terms, **filters)

    def iter_term_links(self, subject_id: str, **filters: Any) -> AsyncIterator[dict[str, Any]]:
        """Every link of a term (see :meth:`term_links`)."""
        return self._c.paginate(self.term_links, subject_id, **filters)

    def iter_domains(self, **filters: Any) -> AsyncIterator[dict[str, Any]]:
        """Every domain matching the filters of :meth:`list_domains`."""
        return self._c.paginate(self.list_domains, **filters)

    def iter_domain_assets(self, subject_id: str, **filters: Any) -> AsyncIterator[dict[str, Any]]:
        """Every asset of a domain (see :meth:`domain_assets`)."""
        return self._c.paginate(self.domain_assets, subject_id, **filters)

    def iter_products(self, **filters: Any) -> AsyncIterator[dict[str, Any]]:
        """Every data product matching the filters of :meth:`list_products`."""
        return self._c.paginate(self.list_products, **filters)

    async def iter_product_consumers(self, subject_id: str, **filters: Any) -> AsyncIterator[dict[str, Any]]:
        """Every consumer of a data product the server returned (see :meth:`product_consumers`, whose answer is one bounded document)."""
        for item in (await self.product_consumers(subject_id, **filters))["items"]:
            yield item

    async def iter_classifications(self) -> AsyncIterator[dict[str, Any]]:
        """Every classification label (see :meth:`list_classifications`)."""
        for item in await self.list_classifications():
            yield item

    def iter_classification_assets(self, subject_id: str, **filters: Any) -> AsyncIterator[dict[str, Any]]:
        """Every asset carrying a label (see :meth:`classification_assets`)."""
        return self._c.paginate(self.classification_assets, subject_id, **filters)

    def iter_review_queue(self, **filters: Any) -> AsyncIterator[dict[str, Any]]:
        """Every pending review item matching the filters of :meth:`review_queue`."""
        return self._c.paginate(self.review_queue, **filters)

    def iter_certifications(self, **filters: Any) -> AsyncIterator[dict[str, Any]]:
        """Every certification matching the filters of :meth:`list_certifications`."""
        return self._c.paginate(self.list_certifications, **filters)

    def iter_changes(self, **filters: Any) -> AsyncIterator[dict[str, Any]]:
        """Every change request matching the filters of :meth:`list_changes`."""
        return self._c.paginate(self.list_changes, **filters)

    def iter_policies(self, **filters: Any) -> AsyncIterator[dict[str, Any]]:
        """Every policy matching the filters of :meth:`list_policies`."""
        return self._c.paginate(self.list_policies, **filters)

    def iter_violations(self, **filters: Any) -> AsyncIterator[dict[str, Any]]:
        """Every violation matching the filters of :meth:`list_violations`."""
        return self._c.paginate(self.list_violations, **filters)


class AsyncAdminApi(Admin):
    """Asynchronous administration API: the admin operations plus an awaitable wait for long-running operations."""
    async def wait_operation(self, operation_id: str, *, timeout: float = 300.0, poll: float = 1.0) -> dict[str, Any]:
        deadline = time.monotonic() + timeout
        while True:
            op = await self.operation(operation_id)
            if op.get("state") in TERMINAL_OP_STATES:
                return op
            if time.monotonic() >= deadline:
                raise OlcfTimeoutError(f"operation {operation_id} still {op.get('state')} after {timeout:g}s", last=op)
            await asyncio.sleep(poll)

    async def wait_until_ready(self, *, timeout: float = 60.0, poll: float = 0.5) -> dict[str, Any]:
        deadline = time.monotonic() + timeout
        last: Exception | None = None
        while time.monotonic() < deadline:
            try:
                return await self.ready()
            except Exception as exc:  # noqa: BLE001 - not reachable yet / still starting
                last = exc
                await asyncio.sleep(poll)
        raise OlcfTimeoutError(f"control plane not ready after {timeout:g}s ({last})")


class AsyncClient:
    """Asynchronous client; see :class:`olcf.sdk.Client` for the constructor arguments."""

    def __init__(self, url: str | None = None, *, token: str | None = None, refresh_token: str | None = None, timeout: float = core.DEFAULT_TIMEOUT,
                 max_retries: int = 3, verify: bool | str = True, ca_bundle: str | None = None, proxy: str | None = None,
                 headers: dict[str, str] | None = None, user_agent: str | None = None, trust_env: bool = True, auto_refresh: bool = True,
                 http_client: httpx.AsyncClient | None = None) -> None:
        self.cfg = build_config(url, token=token, refresh_token=refresh_token, timeout=timeout, max_retries=max_retries, verify=verify,
                                ca_bundle=ca_bundle, proxy=proxy, headers=headers, user_agent=user_agent, trust_env=trust_env, auto_refresh=auto_refresh)
        self._owns_http = http_client is None
        self._http: httpx.AsyncClient = http_client or httpx.AsyncClient(**self.cfg.httpx_kwargs())
        self._auth_lock = asyncio.Lock()
        self.on_tokens: Callable[[str, str | None], Any] | None = None
        self.identity = Identity(self)
        self.admin = AsyncAdminApi(self)
        self.workspaces = Workspaces(self)
        self.items = Items(self)
        self.notebooks = Notebooks(self)
        self.lakehouses = Lakehouses(self)
        self.sql = Sql(self)
        self.catalog = Catalog(self)
        self.connections = Connections(self)
        self.jobs = AsyncJobsApi(self)
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
        self.intelligence = AsyncIntelligenceApi(self)
        self.governance = AsyncGovernanceApi(self)

    # ------------------------------------------------------------------ construction helpers
    @classmethod
    def from_env(cls, **kw: Any) -> AsyncClient:
        """``OLCF_URL`` / ``OLCF_TOKEN`` (+ CA variables) from the environment."""
        return cls(**kw)

    @classmethod
    def from_context(cls, name: str | None = None, **kw: Any) -> AsyncClient:
        """Use the credentials stored by ``olcf login`` (the current context, or ``name``).  Refreshed tokens are written back."""
        from olcf.sdk.contexts import ContextStore
        from olcf.sdk.errors import AuthenticationError

        ctx = ContextStore().get(name)
        if ctx is None:
            raise AuthenticationError("no such context; run `olcf login`", status=401, code="no_context")
        client = cls(kw.pop("url", None) or ctx.get("url"), token=kw.pop("token", None) or ctx.get("token"), refresh_token=ctx.get("refresh_token"),
                     ca_bundle=kw.pop("ca_bundle", None) or ctx.get("ca_bundle"), **kw)
        cname = ctx["name"]

        def persist(access: str, refresh: str | None) -> None:
            ContextStore().set(cname, make_current=False, token=access, refresh_token=refresh)

        client.on_tokens = persist
        return client

    async def __aenter__(self) -> AsyncClient:
        return self

    async def __aexit__(self, *exc: object) -> None:
        await self.aclose()

    async def aclose(self) -> None:
        if self._owns_http:
            await self._http.aclose()

    def __repr__(self) -> str:
        return f"AsyncClient(url={self.cfg.base_url!r}, authenticated={bool(self.cfg.token)})"

    @property
    def base_url(self) -> str:
        return self.cfg.base_url

    def set_token(self, token: str | None, refresh_token: str | None = None) -> None:
        self.cfg.token, self.cfg.refresh_token = token, refresh_token

    def request(self, method: str, path: str, **kw: Any) -> Awaitable[Any]:
        return self.call(method, path, **kw)

    async def call(self, method: str, template: str, *, path: dict[str, Any] | None = None, auth: bool = True, raw: bool = False,
                   as_bytes: bool = False, expect_error: bool = False, idem: bool = False, idempotency_key: str | None = None, if_match: Any = None, **kw: Any) -> Any:
        prep = core.prepare(self.cfg, method, template, path or {}, json_body=kw.pop("json", None), idem=idem, idempotency_key=idempotency_key,
                            if_match=if_match, token=None if auth else "", **kw)
        attempt = 0
        refreshed = False
        while True:
            try:
                resp = await self._http.request(prep.method, prep.url, **prep.kwargs)
            except httpx.HTTPError as exc:
                if core.should_retry_exception(self.cfg, prep, exc, attempt):
                    await asyncio.sleep(core.retry_delay(self.cfg, attempt))
                    attempt += 1
                    continue
                raise OlcfConnectionError(f"cannot reach {self.cfg.base_url}: {type(exc).__name__}: {str(exc)[:200]}", url=self.cfg.base_url) from exc
            if resp.status_code == 401 and auth and not refreshed and self.cfg.auto_refresh and self.cfg.refresh_token and template not in NO_REFRESH_PATHS:
                refreshed = True
                if await self._refresh_tokens(prep.kwargs["headers"].get("Authorization", "")):
                    prep.kwargs["headers"]["Authorization"] = f"Bearer {self.cfg.token}"
                    continue
            if core.should_retry_response(self.cfg, prep, resp, attempt):
                await asyncio.sleep(core.retry_delay(self.cfg, attempt, resp))
                attempt += 1
                continue
            if expect_error and resp.status_code >= 400:
                return resp if raw else None
            core.raise_for_error(resp, prep)
            return core.decode(resp, raw=raw, as_bytes=as_bytes)

    async def _refresh_tokens(self, used_header: str) -> bool:
        async with self._auth_lock:
            if f"Bearer {self.cfg.token}" != used_header:
                return True
            try:
                doc = await self.call("POST", "/identity/refresh", json={"refresh_token": self.cfg.refresh_token}, auth=False)
            except Exception as exc:  # noqa: BLE001
                log.debug("token refresh failed: %s", redact_text(str(exc)))
                return False
            await self._store_tokens(doc)
            return True

    async def _store_tokens(self, doc: dict[str, Any]) -> None:
        self.cfg.token = doc.get("access_token") or self.cfg.token
        self.cfg.refresh_token = doc.get("refresh_token") or self.cfg.refresh_token
        if self.on_tokens and self.cfg.token:
            with contextlib.suppress(Exception):
                res = self.on_tokens(self.cfg.token, self.cfg.refresh_token)
                if asyncio.iscoroutine(res):
                    await res

    async def login(self, username: str, password: str) -> dict[str, Any]:
        doc = await self.identity.login(username, password)
        await self._store_tokens(doc)
        return doc

    async def logout(self) -> None:
        rt = self.cfg.refresh_token
        try:
            if rt or self.cfg.token:
                await self.identity.logout(rt)  # type: ignore[misc, func-returns-value]  # the async facade awaits what the sync resource returns
        finally:
            self.cfg.token = self.cfg.refresh_token = None

    async def me(self) -> dict[str, Any]:
        return await self.identity.me()

    async def paginate(self, fn: Callable[..., Awaitable[Any]], *args: Any, **kw: Any) -> AsyncIterator[Any]:
        """``async for job in client.paginate(client.jobs.list, state="failed")``."""
        cursor = kw.pop("cursor", None)
        while True:
            page = await fn(*args, cursor=cursor, **kw)
            for item in page:
                yield item
            cursor = getattr(page, "next_cursor", None)
            if not cursor:
                return
