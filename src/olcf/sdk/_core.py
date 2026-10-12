# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Transport core shared by the sync and the async SDK clients.

Everything that does not need I/O lives here so the two clients cannot drift apart: configuration (URL, token, CA bundle, proxy
from the environment), request preparation (path templating with percent-encoding, query clean-up, ``Idempotency-Key``), response
decoding (:class:`Resource` with its ``ETag``, :class:`Page`), the retry policy and the redaction of everything that could end
up in a log line.

Retry policy (documented in ``docs/guides``): connection errors are always retried (the request never reached the server);
``429`` honours ``Retry-After``; ``502/503/504`` and read timeouts are retried for idempotent methods (GET/HEAD/PUT/DELETE) and for
POST/PATCH calls that carry an ``Idempotency-Key`` the endpoint honours (``idem=True``).  Back-off is ``base * 2**attempt`` with full
jitter, capped at ``backoff_max``.
"""

from __future__ import annotations

import json
import logging
import os
import random
import ssl
import uuid
from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Any
from urllib.parse import quote, urlsplit

import httpx

from olcf.core.redaction import redact_text
from olcf.sdk.errors import ApiError, error_from_response

log = logging.getLogger("olcf.sdk")

API_PREFIX = "/v1"
DEFAULT_TIMEOUT = 60.0
SAFE_METHODS = frozenset({"GET", "HEAD", "OPTIONS", "PUT", "DELETE"})
RETRY_STATUSES = frozenset({502, 503, 504})
LOOPBACK = frozenset({"localhost", "127.0.0.1", "::1"})
#: A list answer is {"items": [...], "next_cursor": ...} (+ endpoint specific extras); without next_cursor only these keys may accompany
#: "items" - anything else (e.g. an import summary that also has "items") stays a Resource.
PAGE_KEYS = frozenset({"items", "next_cursor", "total", "count"})
CA_ENV_VARS = ("OLCF_CA_BUNDLE", "SSL_CERT_FILE", "REQUESTS_CA_BUNDLE", "CURL_CA_BUNDLE")


class Resource(dict):  # type: ignore[type-arg]
    """A JSON object returned by the API.  ``etag`` is the ``ETag`` header (pass the resource itself as ``if_match=`` to update it)."""

    etag: str | None = None
    correlation_id: str | None = None

    @property
    def id(self) -> str | None:
        v = self.get("id")
        return str(v) if v is not None else None


class Page(list):  # type: ignore[type-arg]
    """The ``items`` of a list endpoint; ``next_cursor`` is ``None`` on the last page."""

    next_cursor: str | None = None
    extra: dict[str, Any]

    def __init__(self, items: list[Any] | None = None, next_cursor: str | None = None, extra: dict[str, Any] | None = None) -> None:
        super().__init__(items or [])
        self.next_cursor = next_cursor
        self.extra = extra or {}


def etag_of(value: Any) -> str | None:
    """Accept a :class:`Resource`, a ``dict`` with an ``etag`` key, or a plain string and return the bare ETag."""
    if value is None:
        return None
    if isinstance(value, Resource) and value.etag:
        return value.etag
    if isinstance(value, Mapping):
        v = value.get("etag")
        return str(v) if v else None
    return str(value).strip('"')


def resolve_verify(verify: bool | str | ssl.SSLContext | None) -> bool | ssl.SSLContext:
    """Turn the ``verify`` argument / CA environment variables into something httpx accepts."""
    if isinstance(verify, ssl.SSLContext):
        return verify
    if verify is False:
        return False
    if isinstance(verify, str):
        return ssl.create_default_context(cafile=verify)
    for name in CA_ENV_VARS:
        path = os.environ.get(name)
        if path and os.path.isfile(path):
            return ssl.create_default_context(cafile=path)
    return True


def is_loopback(url: str) -> bool:
    host = (urlsplit(url).hostname or "").lower()
    return host in LOOPBACK


@dataclass
class ClientConfig:
    """Resolved connection settings (no I/O)."""

    base_url: str
    token: str | None = None
    refresh_token: str | None = None
    timeout: float = DEFAULT_TIMEOUT
    max_retries: int = 3
    backoff_base: float = 0.5
    backoff_max: float = 20.0
    verify: bool | ssl.SSLContext = True
    proxy: str | None = None
    trust_env: bool = True
    user_agent: str = ""
    headers: dict[str, str] = field(default_factory=dict)
    auto_refresh: bool = True

    def httpx_kwargs(self) -> dict[str, Any]:
        """Keyword arguments for ``httpx.Client`` / ``AsyncClient``.

        A proxy can never reach the user's own machine, so loopback URLs bypass ``HTTP(S)_PROXY`` (CA variables are still honoured
        because ``verify`` is resolved explicitly).
        """
        kw: dict[str, Any] = {"base_url": self.base_url, "timeout": httpx.Timeout(self.timeout, connect=min(self.timeout, 15.0)),
                              "verify": self.verify, "follow_redirects": False}
        if is_loopback(self.base_url):
            kw["trust_env"] = False
        else:
            kw["trust_env"] = self.trust_env
            if self.proxy:
                kw["proxy"] = self.proxy
        return kw


def normalise_base_url(url: str) -> str:
    """``http://host:8470`` / ``http://host:8470/`` / ``http://host:8470/v1`` all become ``http://host:8470``."""
    u = url.strip().rstrip("/")
    if u.endswith(API_PREFIX):
        u = u[: -len(API_PREFIX)]
    if "://" not in u:
        u = "http://" + u
    return u


@dataclass
class Prepared:
    """A fully built request (method, URL, keyword arguments and whether it can be retried) shared by the synchronous and asynchronous clients.
    """
    method: str
    url: str
    kwargs: dict[str, Any]
    retry_ok: bool


class PathVars(dict):  # type: ignore[type-arg]
    """``str.format_map`` helper that percent-encodes every value (``/`` included) so ids cannot escape their path segment."""

    def __missing__(self, key: str) -> str:
        raise KeyError(f"missing path variable {key!r}")

    def __getitem__(self, key: str) -> str:
        return quote(str(super().__getitem__(key)), safe="")


def build_path(template: str, path_vars: Mapping[str, Any]) -> str:
    """Expand ``/jobs/{id}`` to ``/v1/jobs/<id>``; templates that already start with ``/v1/`` (and the root-level ``/mcp`` endpoint) are used as they are."""
    if not template.startswith("/"):
        raise ValueError("path templates start with '/'")
    path = template.format_map(PathVars(path_vars))
    return path if path == "/mcp" or path.startswith(API_PREFIX + "/") else API_PREFIX + path


def clean_params(params: Mapping[str, Any] | None) -> dict[str, Any] | None:
    """Drop ``None`` values, render booleans the way FastAPI parses them and lists as repeated keys."""
    if not params:
        return None
    out: dict[str, Any] = {}
    for k, v in params.items():
        if v is None:
            continue
        if isinstance(v, bool):
            out[k] = "true" if v else "false"
        elif isinstance(v, (list, tuple)):
            out[k] = [str(x) for x in v]
        else:
            out[k] = v
    return out or None


def prepare(cfg: ClientConfig, method: str, template: str, path_vars: Mapping[str, Any], *, params: Mapping[str, Any] | None = None,
            json_body: Any = None, content: bytes | None = None, data: Mapping[str, Any] | None = None, files: Any = None,
            headers: Mapping[str, str] | None = None, if_match: Any = None, idempotency_key: str | None = None, idem: bool = False,
            token: str | None = None) -> Prepared:
    method = method.upper()
    hdrs: dict[str, str] = {"Accept": "application/json", "User-Agent": cfg.user_agent, **cfg.headers}
    tok = token if token is not None else cfg.token
    if tok:
        hdrs["Authorization"] = f"Bearer {tok}"
    mutating = method in ("POST", "PUT", "PATCH", "DELETE")
    key = idempotency_key
    if key is None and method == "POST" and idem:
        key = uuid.uuid4().hex
    if key:
        hdrs["Idempotency-Key"] = key
    et = etag_of(if_match)
    if et:
        hdrs["If-Match"] = et if et == "*" else f'"{et}"'
    if headers:
        hdrs.update(headers)
    kwargs: dict[str, Any] = {"headers": hdrs}
    p = clean_params(params)
    if p:
        kwargs["params"] = p
    if json_body is not None:
        kwargs["json"] = json_body
    if content is not None:
        kwargs["content"] = content
    if data is not None:
        kwargs["data"] = data
    if files is not None:
        kwargs["files"] = files
    url = build_path(template, path_vars)
    return Prepared(method, url, kwargs, retry_ok=(method in SAFE_METHODS) or bool(idem or idempotency_key) or not mutating)


def retry_delay(cfg: ClientConfig, attempt: int, resp: httpx.Response | None = None) -> float:
    """Seconds to sleep before retry number ``attempt`` (0-based)."""
    if resp is not None:
        ra = resp.headers.get("retry-after")
        if ra:
            try:
                return min(max(float(ra), 0.0), max(cfg.backoff_max, 30.0))
            except ValueError:
                pass
    cap = min(cfg.backoff_max, cfg.backoff_base * (2 ** attempt))
    return random.uniform(cap / 2, cap)  # noqa: S311 - jitter, not security


def should_retry_response(cfg: ClientConfig, prep: Prepared, resp: httpx.Response, attempt: int) -> bool:
    if attempt >= cfg.max_retries:
        return False
    if resp.status_code == 429:
        return True
    return resp.status_code in RETRY_STATUSES and prep.retry_ok


def should_retry_exception(cfg: ClientConfig, prep: Prepared, exc: Exception, attempt: int) -> bool:
    if attempt >= cfg.max_retries:
        return False
    if isinstance(exc, (httpx.ConnectError, httpx.ConnectTimeout)):
        return True
    return isinstance(exc, httpx.TransportError) and prep.retry_ok


def decode(resp: httpx.Response, *, raw: bool = False, as_bytes: bool = False) -> Any:
    """Decode a successful response (``as_bytes`` returns the body untouched, for downloads)."""
    if raw:
        return resp
    if as_bytes:
        return resp.content
    if resp.status_code == 204 or not resp.content:
        return None
    ctype = resp.headers.get("content-type", "")
    if "json" in ctype:
        body = resp.json()
        cid = resp.headers.get("x-correlation-id")
        if isinstance(body, dict):
            if isinstance(body.get("items"), list) and ("next_cursor" in body or set(body) <= PAGE_KEYS):
                extra = {k: v for k, v in body.items() if k not in ("items", "next_cursor")}
                return Page(body["items"], body.get("next_cursor"), extra)
            res = Resource(body)
            res.etag = (resp.headers.get("etag") or "").strip('"') or body.get("etag")
            res.correlation_id = cid
            return res
        return body
    return resp.text if ctype.startswith("text/") else resp.content


def raise_for_error(resp: httpx.Response, prep: Prepared) -> None:
    if resp.status_code < 400:
        return
    try:
        body: Any = resp.json()
    except (ValueError, json.JSONDecodeError):
        body = resp.text[:500]
    raise error_from_response(resp.status_code, body, headers=dict(resp.headers), method=prep.method, url=redact_text(str(resp.request.url)))


def is_auth_failure(err: ApiError) -> bool:
    return err.status == 401
