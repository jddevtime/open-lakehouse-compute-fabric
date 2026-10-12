# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""SSRF and credential hygiene for the endpoints a Microsoft Fabric / Power BI discovery source contacts.

A Fabric source may override its endpoints (``config.network``: ``authority_host``, ``fabric_api``, ``powerbi_api``, ``onelake_dfs``, ``proxy``) for sovereign clouds, regional
OneLake endpoints and private link. The service principal's credential is sent to the authority host and its access tokens to the others, so an override is a place where a
confused or hostile value would make the control plane hand a credential to an internal service or to the cloud metadata endpoint. The guard is the platform's existing one
(:mod:`olcf.pipelines.ssrf`), applied at two moments:

* **create / update** (:func:`check_overrides`, called by ``identities.validate_network``): every URL must be ``https`` without credentials, must not point at a loopback,
  private, link-local, metadata, multicast or reserved destination (a host name is resolved and *every* address checked). A violation is a 422 with a remediation.
* **use** (:func:`client_factory`, called whenever the connector builds its HTTP stack): the same check runs again on the resolved :class:`~olcf.fabric.http.Network` (a stored
  source, the Fabric integration's network settings and the defaults all end up here) **before** a token is requested, and every connection then goes through the guarded
  transport, which resolves the host name itself, checks every address and connects to the address it checked (no gap for a DNS-rebinding answer). Redirects are never followed.

Private and loopback destinations (and plain ``http`` to them) are accepted only while the runtime setting ``intelligence.allow_private_endpoints`` is true (default false; for
private-link gateways and test doubles). The cloud metadata endpoint (169.254.169.254) and the rest of the link-local / reserved ranges stay blocked either way, and the platform's
``network.ssrf.deny`` list always applies. The ``network.ssrf.allow`` list of pipelines and webhooks is deliberately **not** honoured: it cannot widen what a credential-bearing
source may reach.

Limit: when a proxy is in use (``config.network.proxy`` or ``HTTPS_PROXY`` with ``trust_env``) the proxy resolves the destination, so the connect-time pin is not possible; the
destination host names are resolved and checked locally at the start of every client and the proxy itself is checked when it comes from the source's configuration (an
operator's ``HTTPS_PROXY`` is trusted).
"""

from __future__ import annotations

import ipaddress
from collections.abc import Callable, Mapping
from typing import Any
from urllib.parse import urlsplit

import httpx

from olcf.core.errors import OlcfError
from olcf.fabric.http import Network, make_client, verify_for
from olcf.intelligence import settings as isettings
from olcf.pipelines import ssrf

#: The URL-valued keys of ``config.network``: where tokens and the credential go.
URL_KEYS = ("authority_host", "fabric_api", "powerbi_api", "onelake_dfs")
SETTING = isettings.ALLOW_PRIVATE_ENDPOINTS
_ENABLE_HINT = "use a public https endpoint, or (for a private-link gateway you control) an administrator can set the runtime setting intelligence.allow_private_endpoints to true"
_NAMES = frozenset({"localhost", "metadata", "metadata.google.internal", "instance-data", "kubernetes.default.svc"})


class EndpointNotAllowed(Exception):
    """An endpoint of a Fabric / Power BI source is refused; the message names the key and the reason and never contains a credential. ``remediation`` says what to do."""

    def __init__(self, message: str, remediation: str | None = None) -> None:
        super().__init__(message)
        self.message, self.remediation = message, remediation


def allow_private(ctx: Any) -> bool:
    """Whether private / loopback endpoints are accepted (the runtime setting; ``False`` without a platform context or settings service)."""
    return bool(ctx is not None and isettings.get(ctx, SETTING, False))


def policy_for(ctx: Any, *, resolver: ssrf.Resolver | None = None) -> ssrf.SsrfPolicy:
    """The destination policy of a Fabric source: the platform's deny list, private ranges blocked unless the setting allows them, the allow list ignored."""
    deny: list[str] = []
    if ctx is not None:
        deny = list(ssrf.policy_from_ctx(ctx).deny)
    private_ok = allow_private(ctx)
    # the guard treats ::1 as 'reserved' (never reachable) rather than as loopback; the opt-in for loopback names it explicitly
    return ssrf.SsrfPolicy(allow=["::1/128"] if private_ok else [], deny=deny, block_private=not private_ok, schemes=("https", "http"), resolver=resolver or ssrf.default_resolver)


def _host_class(host: str) -> str | None:
    """The host name when it is one of the well-known internal names (a name that never needs resolving to be refused), else ``None``."""
    h = host.lower().rstrip(".")
    return h if h in _NAMES or h.endswith(".localhost") else None


def check_url(label: str, url: Any, policy: ssrf.SsrfPolicy, *, private_ok: bool) -> None:
    """Raise :class:`EndpointNotAllowed` unless ``url`` (the value of ``config.network.<label>``) may receive a credential or token."""
    if not isinstance(url, str) or not url.strip():
        raise EndpointNotAllowed(f"config.network.{label} must be a URL", "give an https URL such as https://api.fabric.microsoft.com")
    text = url.strip()
    try:
        scheme = urlsplit(text).scheme.lower()
    except ValueError as exc:
        raise EndpointNotAllowed(f"config.network.{label} is not a valid URL ({exc})", "give an https URL such as https://api.fabric.microsoft.com") from None
    if scheme not in ("https", "http"):
        raise EndpointNotAllowed(f"config.network.{label} must be an https URL (got scheme '{scheme or 'none'}')", _ENABLE_HINT)
    if scheme == "http" and not private_ok:
        raise EndpointNotAllowed(f"config.network.{label} must be an https URL: the credential would travel in clear text", _ENABLE_HINT)
    try:
        host, port = ssrf.split_url(text, policy)
    except OlcfError as exc:
        raise EndpointNotAllowed(f"config.network.{label} is refused: {exc.message}", exc.remediation or "give a plain https URL without user name or password") from None
    named = _host_class(host)
    if named is not None and not private_ok:
        raise EndpointNotAllowed(f"config.network.{label} points at '{host}', an internal host name; the credential is not sent to internal services", _ENABLE_HINT)
    try:
        ips = [host] if _is_ip(host) else policy.resolver(host, port)
    except OSError:
        ips = []                                                  # nothing to attack from a name that does not resolve; the connect-time guard checks again
    for ip in ips:
        try:
            policy.check_ip(ip, host)
        except OlcfError as exc:
            raise EndpointNotAllowed(f"config.network.{label} ({host}) is refused: {exc.message}", _ENABLE_HINT) from None
        if scheme == "http" and not _internal(ip):
            raise EndpointNotAllowed(f"config.network.{label} must be an https URL: plain http is accepted only for loopback / private test doubles", _ENABLE_HINT)


def _is_ip(host: str) -> bool:
    try:
        ipaddress.ip_address(host)
        return True
    except ValueError:
        return False


def _internal(ip_text: str) -> bool:
    ip = ssrf._unwrap(ipaddress.ip_address(ip_text))  # noqa: SLF001 - the guard's own normalisation
    return ip.is_private or ip.is_loopback


def check_overrides(ctx: Any, net: Mapping[str, Any], *, resolver: ssrf.Resolver | None = None) -> None:
    """Create / update time: check the URLs a source's ``config.network`` overrides (and its explicit proxy) before they are stored."""
    private_ok = allow_private(ctx)
    policy = policy_for(ctx, resolver=resolver)
    for key in URL_KEYS:
        if net.get(key) not in (None, ""):
            check_url(key, net[key], policy, private_ok=private_ok)
    if net.get("proxy"):
        check_proxy(net["proxy"], policy, private_ok=private_ok)


def check_proxy(proxy: Any, policy: ssrf.SsrfPolicy, *, private_ok: bool) -> None:
    """A proxy named in the source's configuration receives the request (the credential in the token request) so it gets the same check as an endpoint."""
    text = str(proxy).strip()
    try:
        scheme = urlsplit(text).scheme.lower()
    except ValueError:
        scheme = ""
    if scheme not in ("http", "https", "socks5", "socks5h"):
        raise EndpointNotAllowed("config.network.proxy must be an http(s) or socks5 URL", "give the proxy as http://host:port or https://host:port")
    # the proxy is reached over its own protocol; the check is about *where* it is, so use the https rules on an equivalent URL
    probe = "https://" + text.split("://", 1)[1]
    check_url("proxy", probe, policy, private_ok=private_ok)


def check_network(ctx: Any, network: Network, *, resolver: ssrf.Resolver | None = None) -> None:
    """Use time: every URL of the resolved ``network`` (overrides, Fabric integration settings or defaults) and an explicit proxy. Raises :class:`EndpointNotAllowed`."""
    private_ok = allow_private(ctx)
    policy = policy_for(ctx, resolver=resolver)
    for key in URL_KEYS:
        check_url(key, getattr(network, key), policy, private_ok=private_ok)
    if network.proxy and network.proxy_explicit:
        check_proxy(network.proxy, policy, private_ok=private_ok)


def client_factory(ctx: Any, network: Network, *, resolver: ssrf.Resolver | None = None) -> Callable[[], httpx.Client]:
    """Check ``network`` (raises :class:`EndpointNotAllowed` before anything is sent) and return the factory of HTTP clients the connector must use.

    Without a proxy every client is a :func:`~olcf.pipelines.ssrf.guarded_client`: resolve, check and connect in one step, no environment proxy, no redirects. With a proxy the
    ordinary client of :func:`olcf.fabric.http.make_client` is used (see the module docstring for what that means).
    """
    check_network(ctx, network, resolver=resolver)
    if network.proxy:
        return lambda: make_client(network)
    verify = verify_for(network)

    def make() -> httpx.Client:
        return ssrf.guarded_client(policy_for(ctx, resolver=resolver), timeout=network.timeout_seconds, follow_redirects=False, verify=verify)

    return make
