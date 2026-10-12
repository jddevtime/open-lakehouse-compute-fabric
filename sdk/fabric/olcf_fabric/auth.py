# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Bearer-token acquisition for the caller (never for the control plane's own broker identity).

Modes (see :func:`olcf_fabric.configure`): explicit token, Key Vault secret, Entra client credentials (secret held in Key Vault), ``notebookutils.credentials.getToken(audience)``.
Tokens are cached in memory until shortly before they expire (JWT ``exp`` is read without verification: the server validates the signature, the SDK only needs the clock).
"""

from __future__ import annotations

import base64
import json
import time
import urllib.parse
from typing import Any

from ._http import HttpClient
from .config import Settings
from .errors import AuthenticationError, ConfigurationError, OlcfFabricError, ServerError
from .fabric import notebookutils_module

REFRESH_MARGIN = 120.0


def decode_unverified(token: str) -> dict[str, Any]:
    """Claims of a JWT without verifying it (for expiry and tenant hints only). Empty dict for opaque tokens."""
    parts = token.split(".")
    if len(parts) != 3:
        return {}
    try:
        return json.loads(base64.urlsafe_b64decode(parts[1] + "=" * (-len(parts[1]) % 4)))
    except (ValueError, UnicodeDecodeError):
        return {}


class TokenProvider:
    """Resolves the bearer token according to the settings and caches it."""

    def __init__(self, settings: Settings) -> None:
        self.s = settings
        self._cache: tuple[str, float] | None = None
        self.mode_used: str | None = None

    # ------------------------------------------------------------------ helpers
    def _nbu(self) -> Any:
        nbu = notebookutils_module()
        if nbu is None:
            raise ConfigurationError("this authentication mode needs a Fabric notebook session (notebookutils is not available)",
                                     remediation="run inside Fabric, or use auth='token' with a token from the environment (OLCF_TOKEN)")
        return nbu

    def _secret(self, vault: str | None, name: str | None, what: str) -> str:
        if not vault or not name:
            raise ConfigurationError(f"{what}: keyvault and the secret name are required", remediation="olcf.configure(keyvault='https://<vault>.vault.azure.net/', secret_name='...')")
        try:
            value = self._nbu().credentials.getSecret(vault, name)
        except OlcfFabricError:
            raise
        except Exception as exc:  # noqa: BLE001
            raise AuthenticationError(f"could not read secret '{name}' from Key Vault '{vault}': {type(exc).__name__}",
                                      remediation="check that the notebook's identity (the user, or the workspace identity in a pipeline) has 'get' permission on the Key Vault secret") from None
        if not isinstance(value, str) or not value.strip():
            raise AuthenticationError(f"the Key Vault secret '{name}' is empty")
        return value.strip()

    # ------------------------------------------------------------------ modes
    def _from_keyvault(self) -> tuple[str, float | None]:
        tok = self._secret(self.s.keyvault, self.s.secret_name, "keyvault authentication")
        return tok, decode_unverified(tok).get("exp")

    def _from_notebookutils(self) -> tuple[str, float | None]:
        if not self.s.audience:
            raise ConfigurationError("auth='notebookutils' needs the audience", remediation="olcf.configure(auth='notebookutils', audience='api://<olcf-api-app-id>')")
        try:
            tok = self._nbu().credentials.getToken(self.s.audience)
        except OlcfFabricError:
            raise
        except Exception as exc:  # noqa: BLE001
            raise AuthenticationError(f"notebookutils.credentials.getToken('{self.s.audience}') failed: {type(exc).__name__}",
                                      remediation="not every audience is supported by getToken; use auth='client_credentials' or 'keyvault' instead") from None
        if not isinstance(tok, str) or not tok:
            raise AuthenticationError("notebookutils.credentials.getToken returned no token")
        return tok, decode_unverified(tok).get("exp")

    def _from_client_credentials(self) -> tuple[str, float | None]:
        s = self.s
        if not (s.tenant_id and s.client_id and s.audience):
            raise ConfigurationError("client_credentials needs tenant_id, client_id and audience", remediation="olcf.configure(auth='client_credentials', tenant_id=..., client_id=..., audience=..., keyvault=..., client_secret_name=...)")
        secret = s.client_secret or self._secret(s.keyvault, s.client_secret_name, "client_credentials authentication")
        aud = s.audience if s.audience.endswith("/.default") else s.audience.rstrip("/") + "/.default"
        http = HttpClient(s.authority, timeout=s.timeout, ca_bundle=s.ca_bundle, allow_insecure_http=s.allow_insecure_http)
        body = urllib.parse.urlencode({"grant_type": "client_credentials", "client_id": s.client_id, "client_secret": secret, "scope": aud}).encode()
        try:
            resp = http.request("POST", f"/{urllib.parse.quote(s.tenant_id)}/oauth2/v2.0/token", body=body, headers={"Content-Type": "application/x-www-form-urlencoded"})
        except ServerError as exc:
            code = (exc.details or {}).get("error_codes") or ""
            raise AuthenticationError(f"Entra rejected the client credentials ({exc.status}): {exc.message[:200]}",
                                      remediation="check tenant_id, client_id, the secret in Key Vault (it may have expired) and that the app registration exposes the audience", details={"codes": code}) from None
        doc = resp.json() or {}
        tok = doc.get("access_token")
        if not tok:
            raise AuthenticationError("Entra returned no access token")
        return tok, time.time() + float(doc.get("expires_in", 3000))

    # ------------------------------------------------------------------ public
    def mode(self) -> str:
        s = self.s
        if s.auth != "auto":
            return s.auth
        if s.token:
            return "token"
        if s.keyvault and s.secret_name:
            return "keyvault"
        if s.tenant_id and s.client_id and s.audience and (s.client_secret or (s.keyvault and s.client_secret_name)):
            return "client_credentials"
        if s.audience:
            return "notebookutils"
        raise ConfigurationError("no credential is configured", remediation="olcf.configure(endpoint=..., token=...) or auth='keyvault' (keyvault=, secret_name=) or auth='client_credentials'")

    def token(self, *, force: bool = False) -> str:
        if self._cache and not force and self._cache[1] - time.time() > REFRESH_MARGIN:
            return self._cache[0]
        mode = self.mode()
        if mode == "token":
            if not self.s.token:
                raise ConfigurationError("auth='token' but no token was given", remediation="olcf.configure(token=...) or set OLCF_TOKEN")
            tok, exp = self.s.token, decode_unverified(self.s.token).get("exp")
        elif mode == "keyvault":
            tok, exp = self._from_keyvault()
        elif mode == "client_credentials":
            tok, exp = self._from_client_credentials()
        elif mode == "notebookutils":
            tok, exp = self._from_notebookutils()
        else:  # pragma: no cover - guarded by configure()
            raise ConfigurationError(f"unknown auth mode '{mode}'")
        self.mode_used = mode
        self._cache = (tok, float(exp) if exp else time.time() + 300.0)
        return tok

    def claims(self) -> dict[str, Any]:
        try:
            return decode_unverified(self.token())
        except OlcfFabricError:
            return {}
