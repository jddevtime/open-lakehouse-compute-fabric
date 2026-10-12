# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Authenticated reverse proxies for Spark web UIs (spec 13, CONTRACTS 4.1).

``GET /v1/spark-ui/{cluster_id}/{path}``
    The Spark *master* UI of a managed or registered cluster (org-wide ``compute.read``: it lists applications of every workspace).
``GET /v1/spark-applications/{id}/ui/{path}``
    The UI of one application (its driver), authorised by the application's workspace (``job.read``).

Rules
-----
* **Never unauthenticated.** The credential is ``Authorization: Bearer``, ``?access_token=`` (first navigation from the browser) or the ``olcf_ui`` cookie that the first
  query-token request sets (``HttpOnly``, ``SameSite=Strict``, path-limited). The upstream address always comes from the database (set by the provider), never from the request.
* Only ``GET`` and ``HEAD``; path traversal and backslashes are refused; the query string is forwarded without the credential; request cookies and ``Authorization`` are
  never forwarded; ``Set-Cookie`` and hop-by-hop headers are dropped from responses.
* ``Location`` headers are rewritten to the proxy (a redirect to a foreign host is turned into the proxy root); root-relative URLs in HTML and ``setUIRoot('')`` are rewritten so
  Spark's own links keep working below the proxy prefix. Non-HTML responses are streamed unmodified.
* The controller must be able to reach the UI address; when it cannot (firewalled executors/drivers), the proxy answers ``502`` with a remediation instead of guessing.
"""

from __future__ import annotations

import asyncio
import logging
import re
from collections.abc import AsyncIterator
from typing import Any
from urllib.parse import parse_qsl, quote, urlsplit

import httpx
from fastapi import APIRouter, Request
from fastapi.responses import RedirectResponse, Response, StreamingResponse
from sqlalchemy import select

from olcf.api.deps import authenticate, bearer_token, get_ctx
from olcf.compute import schema as C
from olcf.compute.jobs_api import authorize_app, get_app_row
from olcf.core.errors import BadRequest, NotFound, Unauthenticated, Unavailable
from olcf.db.util import fetch_one
from olcf.security.rbac import Principal

log = logging.getLogger("olcf.compute.spark_ui")

ui_router = APIRouter(tags=["spark"])
COOKIE = "olcf_ui"
MAX_REWRITE_BYTES = 8 * 1024 * 1024
_HOP = {"connection", "keep-alive", "proxy-authenticate", "proxy-authorization", "te", "trailers", "transfer-encoding", "upgrade", "content-length", "content-encoding", "set-cookie",
        "server", "date"}
_ATTR = re.compile(r"""(\b(?:href|src|action|data-url)\s*=\s*)(["'])/(?!/)""", re.I)
_CSS_URL = re.compile(r"""(url\(\s*["']?)/(?!/)""", re.I)
_UIROOT = re.compile(r"""setUIRoot\(\s*(["'])\1\s*\)""")
_clients: dict[int, tuple[asyncio.AbstractEventLoop, httpx.AsyncClient]] = {}


def _http() -> httpx.AsyncClient:
    """One pooled client per running event loop (a client must never outlive or cross loops: servers and tests may restart the loop)."""
    loop = asyncio.get_running_loop()
    hit = _clients.get(id(loop))
    if hit is not None and hit[0] is loop:
        return hit[1]
    for k, (lp, _c) in list(_clients.items()):
        if lp.is_closed():
            _clients.pop(k, None)
    client = httpx.AsyncClient(timeout=httpx.Timeout(30.0, connect=5.0), follow_redirects=False, trust_env=False, limits=httpx.Limits(max_connections=64, max_keepalive_connections=16))
    _clients[id(loop)] = (loop, client)
    return client


# --------------------------------------------------------------------------------------------------------------- auth
def _credential(request: Request) -> tuple[str | None, bool]:
    """Return ``(token, from_query)``."""
    tok = bearer_token(request)
    if tok:
        return tok, False
    q = request.query_params.get("access_token")
    if q:
        return q, True
    return request.cookies.get(COOKIE), False


def _clean_path(path: str) -> str:
    if "\\" in path or "\x00" in path or any(seg == ".." for seg in path.split("/")):
        raise BadRequest("invalid proxy path")
    return path.lstrip("/")


def _upstream_query(request: Request) -> list[tuple[str, str]]:
    return [(k, v) for k, v in request.query_params.multi_items() if k != "access_token"]


# --------------------------------------------------------------------------------------------------------------- rewriting
def rewrite_location(location: str, upstream_base: str, proxy_base: str) -> str:
    """Map a redirect target of the upstream onto the proxy (never leak or follow foreign locations)."""
    up = urlsplit(upstream_base)
    loc = urlsplit(location)
    if loc.scheme or loc.netloc:
        if (loc.scheme, loc.netloc) != (up.scheme, up.netloc):
            return proxy_base + "/"
        path = loc.path
    elif location.startswith("/"):
        path = loc.path
    else:
        return location  # relative: resolves against the proxied URL already
    base_path = up.path.rstrip("/")
    if base_path and path.startswith(base_path):
        path = path[len(base_path):]
    out = proxy_base + (path if path.startswith("/") else "/" + path)
    if loc.query:
        out += "?" + loc.query
    if loc.fragment:
        out += "#" + loc.fragment
    return out


def rewrite_html(text: str, proxy_base: str) -> str:
    """Prefix root-relative URLs with the proxy base so Spark's pages work below ``/v1/spark-...``."""
    def attr(m: re.Match[str]) -> str:
        return f"{m.group(1)}{m.group(2)}{proxy_base}/"

    text = _ATTR.sub(attr, text)
    text = _CSS_URL.sub(lambda m: f"{m.group(1)}{proxy_base}/", text)
    return _UIROOT.sub(lambda m: f"setUIRoot({m.group(1)}{proxy_base}{m.group(1)})", text)


def already_prefixed(text: str, proxy_base: str) -> bool:
    """A UI started with ``spark.ui.proxyBase`` already emits prefixed links; rewriting again would double the prefix."""
    return f'"{proxy_base}/' in text or f"'{proxy_base}/" in text


# --------------------------------------------------------------------------------------------------------------- core
async def _proxy(request: Request, principal_token: tuple[str | None, bool], *, resolve, proxy_base: str, cookie_path: str) -> Response:  # noqa: ANN001
    ctx = get_ctx(request)
    token, from_query = principal_token
    if request.method not in ("GET", "HEAD"):
        return Response(status_code=405, headers={"Allow": "GET, HEAD"})
    if not token:
        raise Unauthenticated("authentication required", remediation="send 'Authorization: Bearer <token>' or open the link from the OLCF UI")
    def prepare() -> tuple[Principal, str, str]:
        principal = authenticate(ctx, token, ip=request.client.host if request.client else None)
        upstream, default_path = resolve(ctx, principal)
        return principal, upstream, default_path

    _principal, upstream, default_path = await asyncio.to_thread(prepare)
    path = _clean_path(request.path_params.get("path", ""))
    query = _upstream_query(request)
    if not path and default_path:
        path, _, q2 = default_path.partition("?")
        query = parse_qsl(q2) + query
    url = upstream.rstrip("/") + "/" + quote(path, safe="/%:@!$&'()*+,;=-._~")
    headers = {"Accept": request.headers.get("accept", "*/*"), "User-Agent": "olcf-spark-ui-proxy"}
    for h in ("range", "if-none-match", "if-modified-since", "accept-language"):
        if h in request.headers:
            headers[h] = request.headers[h]
    try:
        req = _http().build_request(request.method, url, params=query, headers=headers)
        resp = await _http().send(req, stream=True)
    except httpx.HTTPError as exc:
        raise Unavailable(f"the Spark UI is not reachable from the controller: {type(exc).__name__}",
                          remediation="the UI port of the master/driver must be reachable from the control plane; for finished applications use the history/event log") from exc
    out_headers = {k: v for k, v in resp.headers.items() if k.lower() not in _HOP}
    out_headers["X-Content-Type-Options"] = "nosniff"
    out_headers["Cache-Control"] = "no-store"
    out_headers["Content-Security-Policy"] = "frame-ancestors 'self'"
    if "location" in resp.headers:
        out_headers["location"] = rewrite_location(resp.headers["location"], upstream, proxy_base)
    ctype = resp.headers.get("content-type", "")
    set_cookie = from_query
    body: Response
    if "text/html" in ctype and request.method == "GET":
        raw = b""
        async for chunk in resp.aiter_bytes():
            raw += chunk
            if len(raw) > MAX_REWRITE_BYTES:
                break
        await resp.aclose()
        text = raw.decode(resp.encoding or "utf-8", errors="replace")
        if not already_prefixed(text, proxy_base):
            text = rewrite_html(text, proxy_base)
        body = Response(text, status_code=resp.status_code, headers=out_headers, media_type=ctype.split(";")[0])
    else:
        async def stream() -> AsyncIterator[bytes]:
            try:
                async for chunk in resp.aiter_raw():
                    yield chunk
            finally:
                await resp.aclose()

        if "content-length" in resp.headers and resp.headers.get("content-encoding") is None:
            out_headers["content-length"] = resp.headers["content-length"]
        if resp.headers.get("content-encoding"):
            out_headers["content-encoding"] = resp.headers["content-encoding"]
        body = StreamingResponse(stream() if request.method == "GET" else iter(()), status_code=resp.status_code, headers=out_headers,
                                 media_type=ctype.split(";")[0] or None)
        if request.method == "HEAD":
            await resp.aclose()
    if set_cookie:
        body.set_cookie(COOKIE, token, path=cookie_path, httponly=True, samesite="strict", secure=request.url.scheme == "https", max_age=3600)
    return body


def _prefix_of(request: Request, marker: str) -> str:
    return request.url.path.split(marker, 1)[0]


# --------------------------------------------------------------------------------------------------------------- routes
@ui_router.api_route("/spark-ui/{cluster_id}", methods=["GET", "HEAD"], include_in_schema=False)
async def cluster_ui_root(request: Request, cluster_id: str) -> Response:
    return RedirectResponse(request.url.path + "/" + (("?" + request.url.query) if request.url.query else ""), status_code=307)


@ui_router.head("/spark-ui/{cluster_id}/{path:path}", include_in_schema=False)
@ui_router.get("/spark-ui/{cluster_id}/{path:path}")
async def cluster_ui(request: Request, cluster_id: str, path: str = "") -> Response:
    """Spark master UI of a cluster (``compute.read``)."""
    def resolve(ctx: Any, principal: Principal) -> tuple[str, str]:
        principal.require("compute.read")
        with ctx.db.connect() as conn:
            cl = fetch_one(conn, select(C.clusters.c.ui_url, C.clusters.c.state).where(C.clusters.c.id == cluster_id))
        if not cl:
            raise NotFound(f"cluster {cluster_id} not found")
        if not cl["ui_url"]:
            raise Unavailable("the cluster has no running Spark master UI", details={"state": cl["state"]})
        return cl["ui_url"], ""

    prefix = _prefix_of(request, "/spark-ui/")
    base = f"{prefix}/spark-ui/{cluster_id}"
    return await _proxy(request, _credential(request), resolve=resolve, proxy_base=base, cookie_path=base)


@ui_router.api_route("/spark-applications/{app_id}/ui", methods=["GET", "HEAD"], include_in_schema=False)
async def app_ui_root(request: Request, app_id: str) -> Response:
    return RedirectResponse(request.url.path + "/" + (("?" + request.url.query) if request.url.query else ""), status_code=307)


@ui_router.head("/spark-applications/{app_id}/ui/{path:path}", include_in_schema=False)
@ui_router.get("/spark-applications/{app_id}/ui/{path:path}")
async def app_ui(request: Request, app_id: str, path: str = "") -> Response:
    """Spark UI of one application (``job.read`` on its workspace)."""
    def resolve(ctx: Any, principal: Principal) -> tuple[str, str]:
        app = get_app_row(ctx, app_id)
        authorize_app(principal, app)
        if app["state"] not in ("RUNNING", "WAITING", "UNKNOWN") and not app.get("ui_url"):
            raise Unavailable("the application has finished and its UI is gone", remediation="read the job logs; the driver UI is only available while the application runs")
        if app.get("ui_url"):
            return app["ui_url"], ""
        if app.get("cluster_id"):
            with ctx.db.connect() as conn:
                cl = fetch_one(conn, select(C.clusters.c.ui_url).where(C.clusters.c.id == app["cluster_id"]))
            if cl and cl["ui_url"]:
                return cl["ui_url"], f"app/?appId={quote(app['application_id'])}"
        raise Unavailable("no UI address is known for this application")

    prefix = _prefix_of(request, "/spark-applications/")
    base = f"{prefix}/spark-applications/{app_id}/ui"
    return await _proxy(request, _credential(request), resolve=resolve, proxy_base=base, cookie_path=base)


__all__ = ["rewrite_html", "rewrite_location", "ui_router"]
