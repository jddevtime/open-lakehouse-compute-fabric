# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Fabric as a compute provider (optional): run work on Microsoft Fabric through the lakehouse Livy API.

Endpoint shape (Fabric Livy API)::

    {api_base}/v1/workspaces/{workspace_id}/lakehouses/{lakehouse_id}/livyapi/versions/{api_version}/{sessions|batches}

Authentication is an Entra bearer token: client-credentials with ``client_id`` + the ``client_secret`` stored in the OLCF secret store
(``secret_id``), or a pre-issued ``access_token`` in that secret. **Status: contract-tested against a local Livy test server.**
Provider-side Spark UI is never proxied.
"""

from __future__ import annotations

import threading
import time
from typing import Any

import httpx

from olcf.compute.errors import ProviderError
from olcf.compute.providers.livy import LivyClient, LivyRemoteProvider
from olcf.core.errors import BadRequest

DEFAULT_API = "https://api.fabric.microsoft.com"
DEFAULT_VERSION = "2023-12-01"
DEFAULT_SCOPE = "https://api.fabric.microsoft.com/.default"


class FabricProvider(LivyRemoteProvider):
    """Runs jobs on Microsoft Fabric's own Spark through the Fabric Livy API with an Entra token."""
    type = "fabric"
    live_verified = False
    execution_model = "remote"
    capabilities = ("livy-sessions", "livy-batches", "remote", "entra-auth")

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self._token: tuple[str, float] | None = None
        self._lock = threading.Lock()
        self._livy: LivyClient | None = None

    def validate_config(self) -> None:
        for k in ("workspace_id", "lakehouse_id"):
            if not self.config.get(k):
                raise BadRequest(f"fabric provider needs '{k}' (the Fabric workspace/lakehouse GUID that hosts the Livy endpoint)")
        if not self.config.get("secret_id"):
            raise BadRequest("fabric provider needs 'secret_id' (secret with client_secret or access_token)")

    def base_url(self) -> str:
        c = self.config
        return (f"{str(c.get('api_base') or DEFAULT_API).rstrip('/')}/v1/workspaces/{c['workspace_id']}/lakehouses/{c['lakehouse_id']}"
                f"/livyapi/versions/{c.get('api_version') or DEFAULT_VERSION}")

    def _bearer(self) -> str:
        secret = self.env.secret(self.config.get("secret_id"))
        if isinstance(secret, dict) and secret.get("access_token"):
            return str(secret["access_token"])
        if not isinstance(secret, dict) or not secret.get("client_secret"):
            raise ProviderError("the fabric provider secret must be JSON with 'client_secret' (or 'access_token')")
        with self._lock:
            if self._token and self._token[1] > time.time() + 60:
                return self._token[0]
            c = self.config
            url = c.get("token_url") or f"https://login.microsoftonline.com/{c['tenant_id']}/oauth2/v2.0/token"
            try:
                r = httpx.post(url, data={"grant_type": "client_credentials", "client_id": c["client_id"], "client_secret": secret["client_secret"],
                                          "scope": c.get("scope") or DEFAULT_SCOPE}, timeout=20.0)
            except httpx.HTTPError as exc:
                raise ProviderError(f"could not reach the Entra token endpoint: {exc}") from exc
            if r.status_code != 200:
                raise ProviderError(f"Entra token request failed: HTTP {r.status_code} {r.text[:200]}")
            body = r.json()
            self._token = (body["access_token"], time.time() + int(body.get("expires_in", 3000)))
            return self._token[0]

    def _client(self) -> LivyClient:
        if self._livy is None:
            self._livy = LivyClient(self.base_url(), lambda: {"Authorization": f"Bearer {self._bearer()}"}, verify=self.config.get("ca_bundle") or True)
        return self._livy
