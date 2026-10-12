# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Server-side request forgery (SSRF) guard for every outbound HTTP call the orchestration layer makes.

Used by the ``http`` / ``webhook_wait`` pipeline activities, outbound event webhooks and webhook alert channels - anything where a
user-supplied URL makes the control plane open a socket.

Defence in depth
----------------
1. :func:`validate_url` (configuration time): scheme must be ``http``/``https``, no embedded credentials, a host, a sane port, and - when the
   host resolves - none of its addresses may be blocked. This gives early, actionable errors.
2. :class:`GuardedBackend` (connect time): the *socket layer* of the HTTP client resolves the host itself, checks **every** resolved address
   against the policy and connects to the address it checked. There is no gap between "check" and "use" that a DNS-rebinding attacker could
   exploit, and redirects are covered because every hop opens its connection through the same backend. Environment proxies are never used
   (``trust_env=False``): a proxy would resolve the destination outside of this check.

Policy
------
Blocked unless explicitly allowed: loopback, RFC 1918 private ranges, link-local (incl. the 169.254.169.254 cloud metadata endpoint), CGNAT
(100.64.0.0/10), unspecified, multicast, reserved and their IPv6 equivalents (IPv4-mapped IPv6 addresses are unwrapped first). Operators
extend/restrict it with the platform settings ``network.ssrf.allow`` (CIDRs, IPs, host names or ``*.suffix`` patterns that are reachable even
when private - e.g. an internal ticketing system) and ``network.ssrf.deny`` (CIDRs that are never reachable). ``network.ssrf.block_private=false``
disables the private-range block entirely (not recommended).
"""

from __future__ import annotations

import ipaddress
import socket
import threading
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any
from urllib.parse import urlsplit

import httpcore
import httpx

from olcf.core.errors import BadRequest, PolicyDenied

Resolver = Callable[[str, int], list[str]]

_ALWAYS_BLOCKED = [ipaddress.ip_network(n) for n in ("169.254.0.0/16", "fe80::/10", "0.0.0.0/8", "224.0.0.0/4", "ff00::/8", "240.0.0.0/4")]
_PRIVATE_BLOCKED = [ipaddress.ip_network(n) for n in (
    "127.0.0.0/8", "10.0.0.0/8", "172.16.0.0/12", "192.168.0.0/16", "100.64.0.0/10", "192.0.0.0/24", "198.18.0.0/15", "::1/128", "fc00::/7", "::/128",
)]


def default_resolver(host: str, port: int) -> list[str]:
    infos = socket.getaddrinfo(host, port, type=socket.SOCK_STREAM)
    seen: list[str] = []
    for fam, _t, _p, _c, sockaddr in infos:
        if fam in (socket.AF_INET, socket.AF_INET6):
            ip = sockaddr[0].split("%")[0]
            if ip not in seen:
                seen.append(ip)
    return seen


def _unwrap(ip: ipaddress.IPv4Address | ipaddress.IPv6Address) -> ipaddress.IPv4Address | ipaddress.IPv6Address:
    if isinstance(ip, ipaddress.IPv6Address):
        if ip.ipv4_mapped is not None:
            return ip.ipv4_mapped
        if ip.sixtofour is not None:
            return ip.sixtofour
    return ip


@dataclass
class SsrfPolicy:
    """Outbound destination policy (see module docstring)."""

    allow: list[str] = field(default_factory=list)
    deny: list[str] = field(default_factory=list)
    block_private: bool = True
    schemes: tuple[str, ...] = ("http", "https")
    resolver: Resolver = default_resolver

    def _networks(self, items: list[str]) -> list[Any]:
        out = []
        for it in items:
            try:
                out.append(ipaddress.ip_network(it.strip(), strict=False))
            except ValueError:
                continue
        return out

    def _host_allowed(self, host: str) -> bool:
        h = host.lower().rstrip(".")
        for a in self.allow:
            a = a.strip().lower().rstrip(".")
            if not a:
                continue
            if a == h or (a.startswith("*.") and h.endswith(a[1:])):
                return True
        return False

    def check_ip(self, ip_text: str, host: str = "") -> None:
        """Raise :class:`PolicyDenied` when connecting to ``ip_text`` (reached via ``host``) is not allowed."""
        try:
            ip = _unwrap(ipaddress.ip_address(ip_text))
        except ValueError:
            raise PolicyDenied(f"'{ip_text}' is not an IP address") from None
        if any(ip in n for n in self._networks(self.deny)):
            raise PolicyDenied(f"destination {ip} is blocked by network.ssrf.deny", remediation="remove the range from the deny list or use another endpoint")
        allowed = self._host_allowed(host) or any(ip in n for n in self._networks(self.allow))
        if allowed:
            return
        if any(ip in n for n in _ALWAYS_BLOCKED) or ip.is_unspecified or ip.is_multicast or ip.is_reserved or ip.is_link_local:
            raise PolicyDenied(f"destination {ip} (link-local/reserved/multicast) is not reachable from the control plane",
                               remediation="add the address or host to the platform setting network.ssrf.allow if this is intentional")
        if self.block_private and (ip.is_private or ip.is_loopback or any(ip in n for n in _PRIVATE_BLOCKED)):
            raise PolicyDenied(f"destination {ip} is a private/loopback address; outbound calls to internal networks are blocked",
                               remediation="add the CIDR or host name to the platform setting network.ssrf.allow if this is intentional")

    def resolve_checked(self, host: str, port: int) -> list[str]:
        """Resolve ``host`` and check *every* address; returns the addresses (all allowed)."""
        try:
            ips = [str(_unwrap(ipaddress.ip_address(host)))] if _is_ip(host) else self.resolver(host, port)
        except socket.gaierror as exc:
            raise BadRequest(f"cannot resolve host '{host}': {exc.strerror or exc}") from None
        if not ips:
            raise BadRequest(f"host '{host}' has no addresses")
        for ip in ips:
            self.check_ip(ip, host)
        return ips


def _is_ip(host: str) -> bool:
    try:
        ipaddress.ip_address(host)
        return True
    except ValueError:
        return False


def split_url(url: str, policy: SsrfPolicy) -> tuple[str, int]:
    """Syntactic validation shared by every entry point; returns ``(host, port)``."""
    if not isinstance(url, str) or not url.strip():
        raise BadRequest("url is required")
    if len(url) > 2048:
        raise BadRequest("url is longer than 2048 characters")
    try:
        parts = urlsplit(url.strip())
        port = parts.port
    except ValueError as exc:
        raise BadRequest(f"invalid url: {exc}") from None
    if parts.scheme.lower() not in policy.schemes:
        raise BadRequest(f"url scheme '{parts.scheme}' is not allowed (allowed: {', '.join(policy.schemes)})")
    if parts.username or parts.password:
        raise BadRequest("credentials in the url are not allowed", remediation="use the auth settings (secret reference) instead")
    if not parts.hostname:
        raise BadRequest("url has no host")
    if port is not None and not (0 < port < 65536):
        raise BadRequest("url port is out of range")
    return parts.hostname, port or (443 if parts.scheme.lower() == "https" else 80)


def validate_url(url: str, policy: SsrfPolicy, *, resolve: bool = True) -> str:
    """Configuration-time validation. Unresolvable hosts are accepted (nothing to attack); resolvable ones must be allowed."""
    host, port = split_url(url, policy)
    if _is_ip(host):
        policy.check_ip(host, host)
    elif resolve:
        try:
            ips = policy.resolver(host, port)
        except OSError:
            ips = []
        for ip in ips:
            policy.check_ip(ip, host)
    return url.strip()


class ConnectionTracker:
    """Remembers the connections of one outbound call so a blocked read can be aborted from another thread (``abort``) and tells whether anything was ever sent.

    A thread blocked in ``recv`` cannot be interrupted by closing its socket on every platform; ``shutdown`` wakes it.  ``connected`` is False while no connection was
    established, which is the proof that no request bytes left this process (so the call may be repeated without any risk).
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._streams: list[httpcore.NetworkStream] = []
        self.aborted = False
        self.connected = False

    def add(self, stream: httpcore.NetworkStream) -> None:
        with self._lock:
            aborted = self.aborted
            if not aborted:
                self._streams.append(stream)
                self.connected = True
        if aborted:  # cancelled while connecting: nothing may be sent. This runs on the thread that owns the connection, so it closes the stream itself
            self._kill(stream, owner=True)
            raise httpcore.ConnectError("the request was cancelled before it was sent")

    def abort(self) -> None:
        with self._lock:
            self.aborted = True
            streams = list(self._streams)
        for st in streams:
            self._kill(st, owner=False)

    @staticmethod
    def _kill(stream: httpcore.NetworkStream, *, owner: bool) -> None:
        """Wake whoever is blocked on ``stream``.

        The cancellation watcher (``owner=False``) only ``shutdown`` s the socket: closing a descriptor from another thread while the owner is blocked in ``recv`` races
        with it (the descriptor can be released, even reused, under the reader), so after a successful shutdown it returns and the I/O owner performs the normal cleanup.
        Without a usable socket (or when the shutdown fails) the stream is closed as a fallback.  The owning thread (``owner=True``) always closes.
        """
        shut = False
        try:
            sock = stream.get_extra_info("socket")
            if sock is not None:
                sock.shutdown(socket.SHUT_RDWR)  # wakes a thread blocked in recv/send
                shut = True
        except (OSError, ValueError):
            pass
        if shut and not owner:
            return
        try:
            stream.close()
        except Exception:  # noqa: BLE001
            pass


class _TrackedStream(httpcore.NetworkStream):
    """Delegating network stream; ``start_tls`` keeps tracking the upgraded stream (the plain one is detached by the TLS wrapper)."""

    def __init__(self, inner: httpcore.NetworkStream, tracker: ConnectionTracker) -> None:
        self._inner, self._tracker = inner, tracker

    def read(self, max_bytes: int, timeout: float | None = None) -> bytes:
        return self._inner.read(max_bytes, timeout)

    def write(self, buffer: bytes, timeout: float | None = None) -> None:
        self._inner.write(buffer, timeout)

    def close(self) -> None:
        self._inner.close()

    def start_tls(self, ssl_context: Any, server_hostname: str | None = None, timeout: float | None = None) -> httpcore.NetworkStream:
        upgraded = _TrackedStream(self._inner.start_tls(ssl_context, server_hostname, timeout), self._tracker)
        self._tracker.add(upgraded)
        return upgraded

    def get_extra_info(self, info: str) -> Any:
        return self._inner.get_extra_info(info)


class GuardedBackend(httpcore.SyncBackend):
    """httpcore network backend that resolves, checks and connects in one step (see module docstring)."""

    def __init__(self, policy: SsrfPolicy, tracker: ConnectionTracker | None = None) -> None:
        super().__init__()
        self.policy = policy
        self.tracker = tracker

    def connect_tcp(self, host: str, port: int, timeout: float | None = None, local_address: str | None = None,
                    socket_options: Any = None) -> httpcore.NetworkStream:
        ips = self.policy.resolve_checked(host, port)
        last: Exception | None = None
        for ip in ips:
            try:
                stream = super().connect_tcp(ip, port, timeout=timeout, local_address=local_address, socket_options=socket_options)
            except (httpcore.ConnectError, httpcore.ConnectTimeout) as exc:
                last = exc
                continue
            if self.tracker is None:
                return stream
            tracked = _TrackedStream(stream, self.tracker)
            self.tracker.add(tracked)
            return tracked
        assert last is not None
        raise last


class GuardedTransport(httpx.HTTPTransport):
    """``httpx`` transport whose connections go through :class:`GuardedBackend`."""

    def __init__(self, policy: SsrfPolicy, *, verify: Any = True, limits: httpx.Limits | None = None, tracker: ConnectionTracker | None = None) -> None:
        super().__init__(verify=verify, trust_env=False, limits=limits or httpx.Limits(max_connections=20, max_keepalive_connections=5, keepalive_expiry=15))
        lim = limits or httpx.Limits(max_connections=20, max_keepalive_connections=5, keepalive_expiry=15)
        try:
            self._pool.close()
        except Exception:  # noqa: BLE001
            pass
        ssl_ctx = verify if not isinstance(verify, bool) else httpx.create_ssl_context(verify=verify)
        self._pool = httpcore.ConnectionPool(
            ssl_context=ssl_ctx, max_connections=lim.max_connections, max_keepalive_connections=lim.max_keepalive_connections,
            keepalive_expiry=lim.keepalive_expiry, http1=True, http2=False, network_backend=GuardedBackend(policy, tracker))


def guarded_client(policy: SsrfPolicy, *, timeout: float = 30.0, follow_redirects: bool = False, max_redirects: int = 5, verify: Any = True,
                   headers: dict[str, str] | None = None, tracker: ConnectionTracker | None = None) -> httpx.Client:
    """An ``httpx.Client`` that can only reach destinations allowed by ``policy`` (also across redirects).

    With a ``tracker`` the caller can abort an in-flight call from another thread and learn whether a connection was ever made.
    """
    return httpx.Client(transport=GuardedTransport(policy, verify=verify, tracker=tracker), timeout=timeout, follow_redirects=follow_redirects, max_redirects=max_redirects,
                        headers=headers, trust_env=False)


def read_capped(response: httpx.Response, max_bytes: int) -> tuple[bytes, bool]:
    """Read a streamed response body up to ``max_bytes``; returns ``(data, truncated)``."""
    buf = bytearray()
    for chunk in response.iter_bytes():
        room = max_bytes - len(buf)
        if len(chunk) > room:
            buf += chunk[:room]
            return bytes(buf), True
        buf += chunk
    return bytes(buf), False


_CACHE: dict[str, Any] = {"at": 0.0, "policy": None}


def policy_from_ctx(ctx: Any, *, resolver: Resolver | None = None) -> SsrfPolicy:
    """Build the effective policy from platform settings (cached for a few seconds)."""
    now = time.monotonic()
    cached = _CACHE.get("policy")
    if cached is not None and now - _CACHE["at"] < 3.0 and _CACHE.get("owner") is ctx and resolver is None:
        return cached
    try:
        st = ctx.service("settings")
        allow = [str(x) for x in (st.get("network.ssrf.allow") or [])]
        deny = [str(x) for x in (st.get("network.ssrf.deny") or [])]
        block = st.get("network.ssrf.block_private")
        block = True if block is None else bool(block)
    except Exception:  # noqa: BLE001
        allow, deny, block = [], [], True
    pol = SsrfPolicy(allow=allow, deny=deny, block_private=block, resolver=resolver or default_resolver)
    if resolver is None:
        _CACHE.update(at=now, policy=pol, owner=ctx)
    return pol


def register_settings(ctx: Any) -> None:
    """Register the ``network.ssrf.*`` platform settings (idempotent)."""
    st = ctx.service("settings")

    def _list(v: Any) -> list[str]:
        if not isinstance(v, list) or not all(isinstance(x, str) for x in v):
            raise BadRequest("must be a list of strings (CIDRs, IPs or host names)")
        for x in v:
            if not x.strip() or len(x) > 255:
                raise BadRequest("entries must be non-empty strings up to 255 characters")
        _CACHE.update(at=0.0, policy=None)
        return [x.strip() for x in v]

    def _bool(v: Any) -> bool:
        if not isinstance(v, bool):
            raise BadRequest("must be true or false")
        _CACHE.update(at=0.0, policy=None)
        return v

    st.register("network.ssrf.allow", description="Destinations (CIDR, IP, host or *.suffix) that outbound HTTP calls of pipelines/webhooks/alerts may reach even when private", default=[], validator=_list)
    st.register("network.ssrf.deny", description="CIDR ranges that outbound HTTP calls may never reach", default=[], validator=_list)
    st.register("network.ssrf.block_private", description="Block loopback/private/link-local destinations for outbound HTTP calls (strongly recommended)", default=True, validator=_bool)
