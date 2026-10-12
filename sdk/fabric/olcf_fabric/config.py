# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""SDK configuration: ``olcf.configure(...)`` and the process-wide settings object.

Environment fallbacks (useful for pipelines and Environments): ``OLCF_ENDPOINT``, ``OLCF_TOKEN``, ``OLCF_AUTH``, ``OLCF_TENANT_ID``, ``OLCF_AUDIENCE``, ``OLCF_CA_BUNDLE``.
A token is never written to disk and never printed; ``repr`` of the settings masks it.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from typing import Any

from .errors import ConfigurationError

AUTH_MODES = ("auto", "token", "keyvault", "client_credentials", "notebookutils")


@dataclass
class Settings:
    endpoint: str = ""
    auth: str = "auto"
    token: str | None = field(default=None, repr=False)
    keyvault: str | None = None
    secret_name: str | None = None
    tenant_id: str | None = None
    client_id: str | None = None
    client_secret_name: str | None = None
    client_secret: str | None = field(default=None, repr=False)
    audience: str | None = None
    authority: str = "https://login.microsoftonline.com"
    ca_bundle: str | None = None
    timeout: float = 30.0
    allow_insecure_http: bool = False
    runtime_family: str = "fabric-2.0-compatible"
    extra: dict[str, Any] = field(default_factory=dict)

    def require(self) -> Settings:
        if not self.endpoint:
            raise ConfigurationError("the control plane endpoint is not configured", remediation="call olcf.configure(endpoint='https://...', ...) first (or set OLCF_ENDPOINT)")
        return self


_SETTINGS = Settings()


def current() -> Settings:
    s = _SETTINGS
    if not s.endpoint and os.environ.get("OLCF_ENDPOINT"):
        s.endpoint = os.environ["OLCF_ENDPOINT"]
    if s.token is None and os.environ.get("OLCF_TOKEN"):
        s.token = os.environ["OLCF_TOKEN"]
    if s.auth == "auto" and os.environ.get("OLCF_AUTH") in AUTH_MODES:
        s.auth = os.environ["OLCF_AUTH"]  # type: ignore[assignment]
    s.tenant_id = s.tenant_id or os.environ.get("OLCF_TENANT_ID")
    s.audience = s.audience or os.environ.get("OLCF_AUDIENCE")
    s.ca_bundle = s.ca_bundle or os.environ.get("OLCF_CA_BUNDLE")
    return s


def configure(endpoint: str | None = None, token: str | None = None, *, auth: str = "auto", keyvault: str | None = None, secret_name: str | None = None,
              tenant_id: str | None = None, client_id: str | None = None, client_secret_name: str | None = None, client_secret: str | None = None,
              audience: str | None = None, authority: str | None = None, ca_bundle: str | None = None, timeout: float | None = None, allow_insecure_http: bool | None = None,
              runtime_family: str | None = None, **extra: Any) -> Settings:
    """Configure the SDK for this session.

    ``auth`` selects how the bearer token is obtained:

    * ``"token"`` - ``token`` is used as is (short-lived; prefer a Key Vault secret).
    * ``"keyvault"`` - ``notebookutils.credentials.getSecret(keyvault, secret_name)`` returns a platform token or an Entra access token.
    * ``"client_credentials"`` - an Entra app registration: ``tenant_id``, ``client_id`` and its secret from Key Vault (``client_secret_name``); the token is requested for ``audience``.
    * ``"notebookutils"`` - ``notebookutils.credentials.getToken(audience)`` (depends on what the Fabric tenant supports; see docs/guides/fabric-assumptions.md).
    * ``"auto"`` (default) - the first of the above whose inputs are present, in that order.

    A broker/service-principal secret of the control plane never belongs here: the SDK only ever holds the credential of the *caller*.
    """
    global _SETTINGS  # noqa: PLW0603
    s = _SETTINGS
    if auth not in AUTH_MODES:
        raise ConfigurationError(f"auth must be one of {AUTH_MODES}", remediation="see help(olcf.configure)")
    if endpoint is not None:
        s.endpoint = endpoint.rstrip("/")
    s.auth = auth
    for name, value in (("token", token), ("keyvault", keyvault), ("secret_name", secret_name), ("tenant_id", tenant_id), ("client_id", client_id), ("client_secret_name", client_secret_name),
                        ("client_secret", client_secret), ("audience", audience), ("ca_bundle", ca_bundle)):
        if value is not None:
            setattr(s, name, value)
    if authority:
        s.authority = authority.rstrip("/")
    if timeout is not None:
        s.timeout = float(timeout)
    if allow_insecure_http is not None:
        s.allow_insecure_http = bool(allow_insecure_http)
    if runtime_family:
        s.runtime_family = runtime_family
    s.extra.update(extra)
    from . import client as _client

    _client.reset()
    return s


def reset_settings() -> None:
    """Forget the configuration (tests, or switching control planes in one session)."""
    global _SETTINGS  # noqa: PLW0603
    _SETTINGS = Settings()
    from . import client as _client

    _client.reset()


__all__ = ["AUTH_MODES", "Settings", "configure", "current", "reset_settings"]
