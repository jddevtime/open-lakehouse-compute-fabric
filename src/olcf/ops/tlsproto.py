# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""uvicorn protocol classes that publish the verified TLS client certificate to the ASGI application.

Why this exists: the worker channel (``olcf.compute.hub``) enforces mutual TLS by reading ``scope["extensions"]["tls"]["client_cert_chain"]``
(the ASGI TLS extension). uvicorn (checked up to 0.54) verifies client certificates during the handshake but **does not put them into the ASGI
scope**; without this shim ``tls.require_client_cert_for_workers`` would reject every worker (and a presented certificate would be ignored
when it is not required). The classes below are the stock uvicorn HTTP / WebSocket protocols with one addition: for requests arriving on a TLS
connection that presented a client certificate, ``scope["extensions"]["tls"]`` is set to::

    {"client_cert_chain": [<PEM of the leaf certificate>], "tls_version": "TLSv1.3", "cipher_suite": "TLS_AES_256_GCM_SHA384"}

The certificate was already verified against ``ssl_ca_certs`` by OpenSSL during the handshake (a certificate from another CA never reaches the
application). The value is derived from the socket, never from request headers, so a client cannot forge it.

:func:`olcf.ops.tlsconfig.build_uvicorn_ssl_kwargs` selects these classes through uvicorn's ``http=`` / ``ws=`` import-string settings (which also
makes them work with ``uvicorn --workers N``).
"""

from __future__ import annotations

import importlib
import importlib.util
import ssl
from typing import Any


def peer_tls_info(transport: Any) -> dict[str, Any] | None:
    """The ASGI ``tls`` extension for a connection, or ``None`` when it is not TLS or the client presented no certificate."""
    if transport is None:
        return None
    sslobj = transport.get_extra_info("ssl_object")
    if sslobj is None:
        return None
    try:
        der = sslobj.getpeercert(binary_form=True)
    except (ValueError, ssl.SSLError):
        return None
    if not der:
        return None
    info: dict[str, Any] = {"client_cert_chain": [ssl.DER_cert_to_PEM_cert(der)]}
    try:
        info["tls_version"] = sslobj.version()
        cipher = sslobj.cipher()
        info["cipher_suite"] = cipher[0] if cipher else None
    except (ValueError, ssl.SSLError):
        pass
    return info


class _TlsExtension:
    """Mixin: wrap the ASGI app so the scope gets the TLS extension of *this* connection."""

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        inner = self.app  # type: ignore[attr-defined]

        async def app(scope: dict[str, Any], receive: Any, send: Any) -> Any:
            if scope.get("type") in ("http", "websocket"):
                ext = scope.get("extensions")
                ext = dict(ext) if ext else {}
                ext.pop("tls", None)  # never trust a pre-existing value
                tls = peer_tls_info(getattr(self, "transport", None))
                if tls:
                    ext["tls"] = tls
                scope["extensions"] = ext
            return await inner(scope, receive, send)

        self.app = app  # type: ignore[attr-defined]


#: class name -> (uvicorn module, uvicorn class, optional third-party module that must be installed)
_BASES: dict[str, tuple[str, str, str | None]] = {
    "HttpToolsTlsProtocol": ("uvicorn.protocols.http.httptools_impl", "HttpToolsProtocol", "httptools"),
    "H11TlsProtocol": ("uvicorn.protocols.http.h11_impl", "H11Protocol", "h11"),
    "WebSocketsSansIOTlsProtocol": ("uvicorn.protocols.websockets.websockets_sansio_impl", "WebSocketsSansIOProtocol", "websockets"),
    "WebSocketsTlsProtocol": ("uvicorn.protocols.websockets.websockets_impl", "WebSocketProtocol", "websockets"),
    "WSProtoTlsProtocol": ("uvicorn.protocols.websockets.wsproto_impl", "WSProtocol", "wsproto"),
}


def __getattr__(name: str) -> Any:
    """Build the protocol classes lazily: importing a uvicorn implementation that is not used (or deprecated) must not happen as a side effect."""
    if name in _BASES:
        mod, cls, _ = _BASES[name]
        base = getattr(importlib.import_module(mod), cls)
        built = type(name, (_TlsExtension, base), {"__module__": __name__, "__doc__": f"{cls} with the ASGI TLS extension."})
        globals()[name] = built
        return built
    raise AttributeError(name)


def _available(name: str) -> bool:
    dep = _BASES[name][2]
    return dep is None or importlib.util.find_spec(dep) is not None


def http_protocol_path() -> str:
    """Import string of the HTTP protocol class to use (httptools when installed, like uvicorn's ``auto``)."""
    return f"{__name__}:{'HttpToolsTlsProtocol' if _available('HttpToolsTlsProtocol') else 'H11TlsProtocol'}"


def ws_protocol_path() -> str | None:
    """Import string of the WebSocket protocol class (sans-IO ``websockets`` first, like uvicorn's ``auto``), ``None`` when no implementation is installed."""
    for name in ("WebSocketsSansIOTlsProtocol", "WSProtoTlsProtocol"):
        if _available(name):
            return f"{__name__}:{name}"
    return None
