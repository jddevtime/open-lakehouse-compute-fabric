# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""TLS / mutual-TLS configuration for the control plane's own listener (used by ``olcf serve``).

``build_uvicorn_ssl_kwargs(settings)`` turns ``settings.tls`` into keyword arguments for ``uvicorn.Config`` / ``uvicorn.run``::

    from olcf.ops.tlsconfig import build_uvicorn_ssl_kwargs
    uvicorn.run("olcf.cli.factory:create", factory=True, **build_uvicorn_ssl_kwargs(settings))

Returns ``{}`` when ``tls.enabled`` is false. Otherwise:

``ssl_certfile`` / ``ssl_keyfile``   ``tls.cert_file`` / ``tls.key_file`` when both are set (validated: readable, parses, not expired, key matches,
                                     warns when the key file is group/world readable); otherwise a server certificate issued by the built-in CA
                                     (``<data>/pki``) for the host name(s), ``tls.extra_sans``, ``server.public_url`` and ``localhost``.
``ssl_ciphers``                      ECDHE + AEAD suites only for TLS 1.2 (TLS 1.3 suites are always AEAD). SSLv3/TLS 1.0/1.1 are refused by the OpenSSL
                                     defaults of every supported Python; an explicit ``minimum_version`` is enforced through ``ssl_context_factory``.
``ssl_ca_certs``, ``ssl_cert_reqs``  only with ``tls.require_client_cert_for_workers``: the platform CA is the trust anchor for client certificates and
                                     ``CERT_OPTIONAL`` is used (browsers, the CLI and the SDK have no client certificate; the **worker channel**
                                     enforces the certificate itself - see below). A certificate from any other CA, or an expired one, fails the
                                     TLS handshake.
``http`` / ``ws``                    import strings of :mod:`olcf.ops.tlsproto` classes that expose the verified client certificate to the ASGI app
                                     (``scope["extensions"]["tls"]``). **Required for mTLS**: stock uvicorn verifies the certificate but does not pass
                                     it on, so ``olcf.compute.hub`` would reject every worker.

Enforcement split (defence in depth): the TLS layer authenticates *any presented* certificate against the platform CA; the worker hub then requires a
certificate on ``/v1/workers/ws`` (``tls.require_client_cert_for_workers``), binds its CN to the worker id and rejects revoked serials
(``kv pki.revoked``, revoked when a worker is revoked or re-enrolled). Revocation therefore takes effect on the next connection, not mid-session.

Behind a TLS-terminating reverse proxy do **not** enable ``tls.enabled``; set ``server.trusted_proxies`` and have the proxy forward the verified client
certificate (URL-encoded PEM) in ``X-Client-Cert`` and strip any client-supplied header of that name.
"""

from __future__ import annotations

import datetime as dt
import logging
import os
import socket
import ssl
from pathlib import Path
from typing import Any

from cryptography import x509
from cryptography.hazmat.primitives import serialization

from olcf.core.config import Settings
from olcf.core.errors import OlcfError

log = logging.getLogger("olcf.ops.tls")
CIPHERS = "ECDHE+AESGCM:ECDHE+CHACHA20:!aNULL:!eNULL:!MD5:!RC4:!3DES"


class TlsConfigError(OlcfError):
    """The configured TLS certificate or key is unusable (HTTP 500, code ``tls_config_invalid``)."""
    status = 500
    code = "tls_config_invalid"


def validate_cert_and_key(cert_file: Path, key_file: Path) -> x509.Certificate:
    """Fail early (with an actionable message) instead of letting uvicorn die with an OpenSSL error."""
    for p, what in ((cert_file, "tls.cert_file"), (key_file, "tls.key_file")):
        if not p.is_file():
            raise TlsConfigError(f"{what} '{p}' does not exist or is not a file", remediation="fix the path or remove cert_file/key_file to use the built-in CA")
        if not os.access(p, os.R_OK):
            raise TlsConfigError(f"{what} '{p}' is not readable by this process")
    try:
        cert = x509.load_pem_x509_certificate(cert_file.read_bytes())
    except ValueError as exc:
        raise TlsConfigError(f"tls.cert_file '{cert_file}' is not a PEM certificate: {exc}") from exc
    try:
        key = serialization.load_pem_private_key(key_file.read_bytes(), password=None)
    except (ValueError, TypeError) as exc:
        raise TlsConfigError(f"tls.key_file '{key_file}' cannot be loaded (encrypted keys are not supported): {exc}") from exc
    now = dt.datetime.now(dt.UTC)
    if cert.not_valid_after_utc < now:
        raise TlsConfigError(f"the server certificate expired on {cert.not_valid_after_utc:%Y-%m-%d}", remediation="renew it")
    if cert.not_valid_before_utc > now + dt.timedelta(minutes=5):
        raise TlsConfigError(f"the server certificate is not valid before {cert.not_valid_before_utc:%Y-%m-%d %H:%M} UTC (clock skew?)")
    def spki(k: Any) -> bytes:
        return k.public_bytes(serialization.Encoding.DER, serialization.PublicFormat.SubjectPublicKeyInfo)

    if spki(cert.public_key()) != spki(key.public_key()):
        raise TlsConfigError("tls.key_file does not belong to tls.cert_file")
    if os.name == "posix" and key_file.stat().st_mode & 0o077:
        log.warning("TLS key %s is accessible by group/others; chmod 600 it", key_file)
    if (cert.not_valid_after_utc - now) < dt.timedelta(days=14):
        log.warning("the server certificate expires in %d days", (cert.not_valid_after_utc - now).days)
    return cert


def server_sans(settings: Settings) -> list[str]:
    from urllib.parse import urlsplit

    host = socket.gethostname()
    sans = [settings.server.host, host, host.split(".")[0], *settings.tls.extra_sans]
    if settings.server.public_url:
        h = urlsplit(settings.server.public_url).hostname
        if h:
            sans.append(h)
    return [s for s in dict.fromkeys(sans) if s and s not in ("0.0.0.0", "::")]  # noqa: S104


def ssl_context_factory(config: Any, default_factory: Any) -> ssl.SSLContext:
    """uvicorn ``ssl_context_factory``: the stock context plus an enforced TLS >= 1.2 floor."""
    ctx = default_factory()
    ctx.minimum_version = ssl.TLSVersion.TLSv1_2
    return ctx


def build_uvicorn_ssl_kwargs(settings: Settings) -> dict[str, Any]:
    """Keyword arguments for ``uvicorn.Config`` implementing ``settings.tls`` (see module docstring). Empty when TLS is disabled."""
    tls = settings.tls
    if not tls.enabled:
        return {}
    from olcf.security.pki import PKI

    pki = PKI(settings.path("pki"))
    if bool(tls.cert_file) != bool(tls.key_file):
        raise TlsConfigError("tls.cert_file and tls.key_file must be set together", remediation="set both, or neither to use the built-in CA")
    if tls.cert_file and tls.key_file:
        crt, key = Path(tls.cert_file).expanduser(), Path(tls.key_file).expanduser()
        validate_cert_and_key(crt, key)
    else:
        crt, key = pki.ensure_server_cert(server_sans(settings))
    kw: dict[str, Any] = {"ssl_certfile": str(crt), "ssl_keyfile": str(key), "ssl_ciphers": CIPHERS}
    import inspect

    import uvicorn.config as uc

    if "ssl_context_factory" in inspect.signature(uc.Config.__init__).parameters:  # newer uvicorn only; older ones rely on the OpenSSL defaults
        kw["ssl_context_factory"] = ssl_context_factory
    if tls.require_client_cert_for_workers:
        from olcf.ops import tlsproto

        kw["ssl_ca_certs"] = str(pki.ca_cert_path)
        kw["ssl_cert_reqs"] = ssl.CERT_OPTIONAL
        kw["http"] = tlsproto.http_protocol_path()
        ws = tlsproto.ws_protocol_path()
        if ws:
            kw["ws"] = ws
    return kw
