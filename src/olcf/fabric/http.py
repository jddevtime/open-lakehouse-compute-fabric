# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Outbound HTTP for the Fabric adapter: enterprise proxy and custom CA support in one place.

Every call the adapter makes to Entra ID, the Fabric REST API, OneLake or a JWKS endpoint goes through :func:`make_client`, so the same
settings apply everywhere and the diagnostics (``POST /test``) exercise exactly the path production traffic uses.

Resolution order for each setting: the integration config (``network`` block) > environment (``HTTPS_PROXY`` / ``NO_PROXY`` /
``REQUESTS_CA_BUNDLE`` / ``SSL_CERT_FILE`` / ``CURL_CA_BUNDLE``) > defaults. TLS verification is always on; a custom CA *adds trust* (it replaces the bundle with the file
you name, which should contain your proxy's CA chain) - there is deliberately no switch to disable verification.
"""

from __future__ import annotations

import os
import ssl
from collections.abc import Mapping
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import httpx

DEFAULT_AUTHORITY = "https://login.microsoftonline.com"
DEFAULT_FABRIC_API = "https://api.fabric.microsoft.com"
DEFAULT_ONELAKE_DFS = "https://onelake.dfs.fabric.microsoft.com"
DEFAULT_POWERBI_API = "https://api.powerbi.com"
_CA_ENV = ("REQUESTS_CA_BUNDLE", "SSL_CERT_FILE", "CURL_CA_BUNDLE")
_PROXY_ENV = ("HTTPS_PROXY", "https_proxy", "ALL_PROXY", "all_proxy")


@dataclass
class Network:
    """Resolved network settings. Endpoints are overridable so sovereign clouds, regional OneLake endpoints and test doubles work."""

    proxy: str | None = None
    #: ``True`` when the proxy comes from the integration configuration (passed to the client explicitly); an environment proxy is left to httpx, which also honours ``NO_PROXY``
    proxy_explicit: bool = False
    ca_bundle: str | None = None
    timeout_seconds: float = 15.0
    trust_env: bool = True
    authority_host: str = DEFAULT_AUTHORITY
    fabric_api: str = DEFAULT_FABRIC_API
    onelake_dfs: str = DEFAULT_ONELAKE_DFS
    extra: dict[str, Any] = field(default_factory=dict)
    #: Power BI REST host used by the admin (metadata scanner) calls of OLCF Intelligence; not part of :meth:`describe` (the Fabric diagnostics output is unchanged)
    powerbi_api: str = DEFAULT_POWERBI_API

    @classmethod
    def from_config(cls, cfg: Mapping[str, Any] | None, env: Mapping[str, str] | None = None) -> Network:
        cfg = dict(cfg or {})
        env = os.environ if env is None else env
        trust_env = bool(cfg.get("trust_env", True))
        configured_proxy = cfg.get("proxy") or None
        env_proxy = next((env[k] for k in _PROXY_ENV if env.get(k)), None) if trust_env else None
        ca = cfg.get("ca_bundle") or (next((env[k] for k in _CA_ENV if env.get(k)), None) if trust_env else None)
        return cls(proxy=configured_proxy or env_proxy, proxy_explicit=bool(configured_proxy), ca_bundle=ca or None, timeout_seconds=float(cfg.get("timeout_seconds") or 15.0),
                   trust_env=trust_env, authority_host=str(cfg.get("authority_host") or DEFAULT_AUTHORITY).rstrip("/"),
                   fabric_api=str(cfg.get("fabric_api") or DEFAULT_FABRIC_API).rstrip("/"), onelake_dfs=str(cfg.get("onelake_dfs") or DEFAULT_ONELAKE_DFS).rstrip("/"),
                   powerbi_api=str(cfg.get("powerbi_api") or DEFAULT_POWERBI_API).rstrip("/"))

    def describe(self) -> dict[str, Any]:
        """Safe-to-display view (a proxy URL with credentials is masked)."""
        proxy = self.proxy
        if proxy and "@" in proxy:
            scheme, _, rest = proxy.partition("://")
            proxy = f"{scheme}://***@{rest.rsplit('@', 1)[1]}"
        return {"proxy": proxy, "ca_bundle": self.ca_bundle, "timeout_seconds": self.timeout_seconds, "authority_host": self.authority_host,
                "fabric_api": self.fabric_api, "onelake_dfs": self.onelake_dfs, "trust_env": self.trust_env}


def verify_for(net: Network) -> ssl.SSLContext | bool:
    """TLS verification setting: the named CA bundle, or the system store. Missing bundle files are an error, never a silent fallback."""
    if net.ca_bundle:
        p = Path(net.ca_bundle)
        if not p.is_file():
            raise FileNotFoundError(f"the configured CA bundle does not exist: {net.ca_bundle}")
        return ssl.create_default_context(cafile=str(p))
    return True


def make_client(net: Network, *, timeout: float | None = None, follow_redirects: bool = False) -> httpx.Client:
    """An ``httpx.Client`` honouring the proxy / CA settings. Redirects are not followed by default (the LRO protocol reads ``Location`` itself)."""
    kwargs: dict[str, Any] = {"timeout": timeout or net.timeout_seconds, "verify": verify_for(net), "follow_redirects": follow_redirects, "trust_env": net.trust_env}
    if net.proxy and net.proxy_explicit:
        kwargs["proxy"] = net.proxy
    return httpx.Client(**kwargs)
