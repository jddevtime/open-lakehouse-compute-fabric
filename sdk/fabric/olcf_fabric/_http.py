# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Small HTTPS client on the standard library: retries with exponential backoff and jitter, ``Retry-After``, JSON in and out, a custom CA bundle and a clear error model.

TLS verification is always on. Plain ``http://`` is accepted only for loopback hosts (local development) unless ``allow_insecure_http`` is set explicitly.
"""

from __future__ import annotations

import json
import os
import random
import socket
import ssl
import time
import urllib.error
import urllib.parse
import urllib.request
from typing import Any

from .errors import ConfigurationError, ConnectionFailed, ServerError

RETRY_STATUS = {429, 502, 503, 504}
_LOOPBACK = {"localhost", "127.0.0.1", "::1"}


class Response:
    def __init__(self, status: int, headers: dict[str, str], body: bytes) -> None:
        self.status, self.headers, self.body = status, headers, body

    def json(self) -> Any:
        return json.loads(self.body.decode("utf-8")) if self.body else None


def check_endpoint(url: str, allow_insecure_http: bool = False) -> str:
    u = urllib.parse.urlsplit(url)
    if u.scheme not in ("https", "http") or not u.hostname:
        raise ConfigurationError(f"'{url}' is not a valid endpoint URL", remediation="use https://<host>[:port]")
    if u.scheme == "http" and u.hostname not in _LOOPBACK and not allow_insecure_http:
        raise ConfigurationError("the endpoint uses plain http; tokens must only travel over TLS", remediation="use https:// (plain http is allowed for localhost only)")
    return url.rstrip("/")


def ssl_context(ca_bundle: str | None = None) -> ssl.SSLContext:
    bundle = ca_bundle or os.environ.get("OLCF_CA_BUNDLE") or os.environ.get("SSL_CERT_FILE") or os.environ.get("REQUESTS_CA_BUNDLE")
    if bundle:
        if not os.path.isfile(bundle):
            raise ConfigurationError(f"the CA bundle '{bundle}' does not exist")
        return ssl.create_default_context(cafile=bundle)
    return ssl.create_default_context()


class HttpClient:
    def __init__(self, base_url: str, *, timeout: float = 30.0, max_attempts: int = 5, ca_bundle: str | None = None, allow_insecure_http: bool = False,
                 sleep: Any = time.sleep, user_agent: str = "olcf-fabric") -> None:
        self.base_url = check_endpoint(base_url, allow_insecure_http)
        self.timeout, self.max_attempts, self._sleep, self.user_agent = timeout, max_attempts, sleep, user_agent
        self._ctx = ssl_context(ca_bundle) if self.base_url.startswith("https") else None

    def request(self, method: str, path: str, *, token: str | None = None, body: Any = None, headers: dict[str, str] | None = None, retry: bool = True,
                timeout: float | None = None) -> Response:
        url = path if path.startswith("http") else self.base_url + path
        data = None if body is None else (body if isinstance(body, bytes) else json.dumps(body, ensure_ascii=False).encode("utf-8"))
        hdrs = {"Accept": "application/json", "User-Agent": self.user_agent, **(headers or {})}
        if data is not None:
            hdrs.setdefault("Content-Type", "application/json")
        if token:
            hdrs["Authorization"] = f"Bearer {token}"
        delay = 0.5
        attempt = 0
        while True:
            attempt += 1
            try:
                req = urllib.request.Request(url, data=data, method=method, headers=hdrs)  # noqa: S310 - scheme checked in check_endpoint
                with urllib.request.urlopen(req, timeout=timeout or self.timeout, context=self._ctx) as resp:  # noqa: S310
                    return Response(resp.status, {k.lower(): v for k, v in resp.headers.items()}, resp.read())
            except urllib.error.HTTPError as exc:
                payload = exc.read()
                resp = Response(exc.code, {k.lower(): v for k, v in (exc.headers or {}).items()}, payload)
                if retry and exc.code in RETRY_STATUS and attempt < self.max_attempts:
                    self._sleep(self._backoff(resp, delay))
                    delay = min(delay * 2, 20.0)
                    continue
                raise _server_error(resp) from None
            except (urllib.error.URLError, ConnectionError, ssl.SSLError, TimeoutError) as exc:
                if retry and attempt < self.max_attempts:
                    self._sleep(min(delay, 20.0) * (0.5 + random.random()))  # noqa: S311 - jitter, not security
                    delay = min(delay * 2, 20.0)
                    continue
                raise _connection_error(exc, url) from None

    @staticmethod
    def _backoff(resp: Response, delay: float) -> float:
        ra = resp.headers.get("retry-after")
        try:
            return max(0.0, min(float(ra), 60.0)) if ra is not None else delay * (0.5 + random.random())  # noqa: S311
        except ValueError:
            return delay


def _server_error(resp: Response) -> ServerError:
    try:
        doc = json.loads(resp.body.decode("utf-8"))
    except (ValueError, UnicodeDecodeError):
        doc = {}
    err = doc.get("error") if isinstance(doc, dict) and isinstance(doc.get("error"), dict) else (doc if isinstance(doc, dict) else {})
    msg = err.get("message") or (resp.body[:200].decode("utf-8", "replace") if resp.body else f"HTTP {resp.status}")
    return ServerError(str(msg), status=resp.status, code=err.get("code"), remediation=err.get("remediation"), details=err.get("details") or {})


def _connection_error(exc: BaseException, url: str) -> ConnectionFailed:
    host = urllib.parse.urlsplit(url).hostname
    reason = getattr(exc, "reason", exc)
    if isinstance(reason, ssl.SSLCertVerificationError) or isinstance(exc, ssl.SSLCertVerificationError):
        return ConnectionFailed(f"TLS verification failed for {host}: {reason}",
                                remediation="the server certificate is not trusted by this Fabric session; set ca_bundle in olcf.configure() to your CA bundle, or use a publicly trusted certificate")
    if isinstance(reason, socket.gaierror):
        return ConnectionFailed(f"the host name {host} does not resolve from this Fabric session", remediation="check the endpoint; the control plane must be resolvable from Fabric (private endpoint, VPN or public DNS)")
    return ConnectionFailed(f"cannot reach {host}: {type(reason).__name__}: {reason}", remediation="check the network path (firewall, private link, data gateway) between Fabric and the control plane")


__all__ = ["HttpClient", "Response", "check_endpoint", "ssl_context"]
