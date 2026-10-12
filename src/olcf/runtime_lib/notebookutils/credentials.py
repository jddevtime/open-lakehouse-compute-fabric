# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""``notebookutils.credentials``: ``getSecret`` and ``getToken`` (stdlib only).

``getSecret(vault, name)`` resolves **only** secrets that the control plane explicitly granted to this job: they arrive as environment variables named
by :func:`olcf.runtime_lib.notebookutils.naming.secret_env_name` (derived from the Key Vault URL and secret name the notebook already uses, so the notebook
source does not change). Anything else raises :class:`SecretNotGranted`. No call leaves the machine.

``getToken(audience)`` supports the storage / OneLake audience using the job's own storage identity (client-credentials from the job's storage
configuration). Other audiences (``pbi``, ``kusto``, ``keyvault`` ...) belong to the interactive Fabric user and are not available.
"""

from __future__ import annotations

import json
import os
import threading
import time
import urllib.parse
import urllib.request
from typing import Any

from olcf.runtime_lib.notebookutils.errors import NotSupportedError, SecretNotGranted
from olcf.runtime_lib.notebookutils.naming import normalise_vault, secret_env_name

_STORAGE_AUDIENCES = {"storage", "https://storage.azure.com", "https://storage.azure.com/", "https://storage.azure.com/.default", "onelake",
                      "https://onelake.dfs.fabric.microsoft.com", "https://onelake.dfs.fabric.microsoft.com/"}


class CredentialsNS:
    """The ``notebookutils.credentials`` namespace."""

    def __init__(self, state: dict[str, Any]) -> None:
        self._state = state
        self._tokens: dict[str, tuple[str, float]] = {}
        self._lock = threading.Lock()

    def getSecret(self, akvName: str, secret: str) -> str:  # noqa: N802, N803 - Fabric names
        """Value of Key Vault secret ``secret`` of vault ``akvName`` (URL or name) if it was granted to this job."""
        env = secret_env_name(akvName, secret)
        val = os.environ.get(env)
        if val is None:
            raise SecretNotGranted(f"secret '{secret}' of vault '{normalise_vault(akvName)}' was not granted to this job. "
                                   "Grant it with olcf.run_remote(secrets=[...]) and map it to an OLCF secret in the Fabric integration settings.")
        return val

    def getToken(self, audience: str, name: str = "") -> str:  # noqa: N802
        """Access token for the storage / OneLake audience, minted with the job's storage identity."""
        aud = (audience or "").strip().lower()
        if aud not in _STORAGE_AUDIENCES:
            raise NotSupportedError(f"notebookutils.credentials.getToken({audience!r})", "only the storage/OneLake audience is available to remote runs",
                                    "Grant a credential for that service as a secret and read it with notebookutils.credentials.getSecret.")
        with self._lock:
            hit = self._tokens.get("storage")
            if hit and hit[1] - 60 > time.time():
                return hit[0]
        tok, exp = self._client_credentials()
        with self._lock:
            self._tokens["storage"] = (tok, exp)
        return tok

    def _storage_identity(self) -> dict[str, str]:
        conf = ((self._state["spec"].get("storage_conf") or {}).get("spark")) or {}
        found: dict[str, str] = {}
        for k, v in conf.items():
            key = k.removeprefix("spark.hadoop.")
            for part in ("client.id", "client.secret", "client.endpoint"):
                if key.startswith("fs.azure.account.oauth2." + part + ".") and "onelake" in key:
                    found[part] = str(v)
        if len(found) < 3:  # fall back to any ABFS OAuth identity of the job
            for k, v in conf.items():
                key = k.removeprefix("spark.hadoop.")
                for part in ("client.id", "client.secret", "client.endpoint"):
                    if key.startswith("fs.azure.account.oauth2." + part + ".") and part not in found:
                        found[part] = str(v)
        return found

    def _client_credentials(self) -> tuple[str, float]:
        ident = self._storage_identity()
        if len(ident) < 3:
            raise NotSupportedError("notebookutils.credentials.getToken('storage')", "this job has no OAuth storage identity (service principal)",
                                    "Attach a lakehouse backed by a OneLake/ADLS connection that uses a service principal.")
        endpoint = ident["client.endpoint"].replace("/oauth2/token", "/oauth2/v2.0/token") if "/oauth2/v2.0/" not in ident["client.endpoint"] else ident["client.endpoint"]
        body = urllib.parse.urlencode({"grant_type": "client_credentials", "client_id": ident["client.id"], "client_secret": ident["client.secret"],
                                       "scope": "https://storage.azure.com/.default"}).encode()
        req = urllib.request.Request(endpoint, data=body, method="POST", headers={"Content-Type": "application/x-www-form-urlencoded"})  # noqa: S310
        try:
            with urllib.request.urlopen(req, timeout=30) as resp:  # noqa: S310
                doc = json.loads(resp.read())
        except Exception as exc:  # noqa: BLE001
            raise RuntimeError(f"could not obtain a storage token: {type(exc).__name__}") from None  # never echo the request (it carries the secret)
        return str(doc["access_token"]), time.time() + float(doc.get("expires_in", 3600))

    def _unsupported(self, api: str) -> Any:
        def _raise(*args: Any, **kwargs: Any) -> Any:
            raise NotSupportedError(f"notebookutils.credentials.{api}", "this API needs the interactive Fabric session identity",
                                    "Use an OLCF connection/secret reference and read it with notebookutils.credentials.getSecret.")

        return _raise

    def __getattr__(self, name: str) -> Any:
        if name in {"putSecret", "isValidToken", "getConnectionStringOrCreds"}:
            return self._unsupported(name)
        if name.startswith("_"):
            raise AttributeError(name)
        raise NotSupportedError(f"notebookutils.credentials.{name}")

    def help(self, method_name: str = "") -> None:
        print("notebookutils.credentials (OLCF):\n  getSecret(akvName, secret) -> only secrets granted to this job\n"
              "  getToken('storage') -> token of the job's storage identity\n  putSecret/isValidToken/getConnectionStringOrCreds: not supported in OLCF")
