# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Tiny HTTP client for the job-scoped callbacks (``spec["callbacks"]``: ``api_url`` + short-lived ``job_token``). Stdlib only (urllib)."""

from __future__ import annotations

import json
import os
import ssl
import urllib.error
import urllib.request
from typing import Any


class ApiError(RuntimeError):
    """An API call made by ``notebookutils`` returned an error: carries the HTTP status and response body."""
    def __init__(self, status: int, message: str, body: Any = None) -> None:
        super().__init__(f"HTTP {status}: {message}")
        self.status = status
        self.body = body


class Callbacks:
    """Calls the OLCF API with the job token. Never logs the token."""

    def __init__(self, spec: dict[str, Any]) -> None:
        cb = spec.get("callbacks") or {}
        self.api_url = str(cb.get("api_url") or "").rstrip("/")
        self._token = str(cb.get("job_token") or "")

    @property
    def available(self) -> bool:
        return bool(self.api_url and self._token)

    def _ctx(self) -> ssl.SSLContext | None:
        if not self.api_url.startswith("https://"):
            return None
        ca = os.environ.get("OLCF_CA_BUNDLE") or os.environ.get("SSL_CERT_FILE")
        return ssl.create_default_context(cafile=ca) if ca else ssl.create_default_context()

    def request(self, method: str, path: str, body: Any = None, *, timeout: float = 30.0, headers: dict[str, str] | None = None) -> Any:
        if not self.available:
            raise ApiError(0, "this job has no callback credentials (callbacks.api_url / callbacks.job_token)")
        data = None if body is None else json.dumps(body).encode()
        req = urllib.request.Request(self.api_url + path, data=data, method=method)  # noqa: S310 - scheme is the configured control plane URL
        req.add_header("Authorization", "Bearer " + self._token)
        req.add_header("Accept", "application/json")
        if data is not None:
            req.add_header("Content-Type", "application/json")
        for k, v in (headers or {}).items():
            req.add_header(k, v)
        try:
            with urllib.request.urlopen(req, timeout=timeout, context=self._ctx()) as resp:  # noqa: S310
                raw = resp.read()
                return json.loads(raw) if raw else None
        except urllib.error.HTTPError as exc:
            raw = exc.read()
            try:
                payload = json.loads(raw)
            except ValueError:
                payload = None
            msg = (payload or {}).get("message") if isinstance(payload, dict) else None
            raise ApiError(exc.code, msg or (raw[:200].decode("utf-8", "replace") if raw else exc.reason), payload) from None
        except urllib.error.URLError as exc:
            raise ApiError(0, f"cannot reach the OLCF API at {self.api_url}: {exc.reason}") from None
