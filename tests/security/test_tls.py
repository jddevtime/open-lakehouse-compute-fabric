# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""TLS and mutual TLS against a real uvicorn: handshake policy, ``build_uvicorn_ssl_kwargs``, the client certificate reaching the worker hub,
and the attacks that must fail (no certificate, foreign CA, expired, revoked, wrong worker, forged ``X-Client-Cert`` header)."""

from __future__ import annotations

import datetime as dt
import json
import socket
import ssl
import urllib.parse
from pathlib import Path

import httpx
import pytest
from websockets.exceptions import ConnectionClosed, InvalidHandshake  # noqa: E402
from websockets.sync.client import connect as _ws_connect  # noqa: E402


def ws_connect(url: str, **kw):  # noqa: ANN003, ANN201
    """``websockets`` sync connect that retries an upgrade the in-process test server never answered (it stalls now and then when this module runs after others in one process)."""
    kw.setdefault("open_timeout", 10)
    last: Exception | None = None
    for _ in range(3):
        try:
            return _ws_connect(url, **kw)
        except TimeoutError as exc:
            last = exc
    raise last  # type: ignore[misc]

from olcf.compute.hub import CLOSE_FORBIDDEN, CLOSE_PROTOCOL, CLOSE_UNAUTH  # noqa: E402
from olcf.ops.tlsconfig import TlsConfigError, build_uvicorn_ssl_kwargs  # noqa: E402
from olcf.security.pki import PKI  # noqa: E402
from tests.security.liveserver import LiveServer  # noqa: E402


# ----------------------------------------------------------------------------- kwargs builder
def test_disabled_tls_gives_no_kwargs(settings):
    assert build_uvicorn_ssl_kwargs(settings) == {}


def test_builtin_ca_server_certificate_and_mtls_kwargs(settings):
    settings.tls.enabled = True
    settings.tls.extra_sans = ["olcf.example.internal", "10.9.8.7"]
    kw = build_uvicorn_ssl_kwargs(settings)
    assert Path(kw["ssl_certfile"]).is_file() and Path(kw["ssl_keyfile"]).is_file() and "ssl_ca_certs" not in kw and "http" not in kw
    from cryptography import x509

    cert = x509.load_pem_x509_certificate(Path(kw["ssl_certfile"]).read_bytes())
    sans = cert.extensions.get_extension_for_class(x509.SubjectAlternativeName).value
    assert "olcf.example.internal" in sans.get_values_for_type(x509.DNSName) and "localhost" in sans.get_values_for_type(x509.DNSName)
    assert "10.9.8.7" in {str(i) for i in sans.get_values_for_type(x509.IPAddress)}
    assert oct(Path(kw["ssl_keyfile"]).stat().st_mode & 0o777) == "0o600"
    settings.tls.require_client_cert_for_workers = True
    kw = build_uvicorn_ssl_kwargs(settings)
    assert kw["ssl_cert_reqs"] == ssl.CERT_OPTIONAL and kw["ssl_ca_certs"].endswith("ca.crt")
    assert kw["http"].startswith("olcf.ops.tlsproto:") and kw["ws"].startswith("olcf.ops.tlsproto:")


def test_custom_certificate_is_validated(settings, tmp_path):
    from cryptography.hazmat.primitives import serialization

    settings.tls.enabled = True
    pki = PKI(tmp_path / "other-pki")
    crt_pem, key_pem = pki.issue_server_cert(["localhost"])
    (tmp_path / "c.crt").write_text(crt_pem)
    (tmp_path / "c.key").write_text(key_pem)
    (tmp_path / "c.key").chmod(0o600)
    settings.tls.cert_file, settings.tls.key_file = str(tmp_path / "c.crt"), str(tmp_path / "c.key")
    assert build_uvicorn_ssl_kwargs(settings)["ssl_certfile"] == str(tmp_path / "c.crt")
    # only one of the two
    settings.tls.key_file = None
    with pytest.raises(TlsConfigError, match="together"):
        build_uvicorn_ssl_kwargs(settings)
    # a key that does not belong to the certificate
    _, other_key = pki.issue_server_cert(["localhost"])
    (tmp_path / "o.key").write_text(other_key)
    settings.tls.key_file = str(tmp_path / "o.key")
    with pytest.raises(TlsConfigError, match="does not belong"):
        build_uvicorn_ssl_kwargs(settings)
    # missing file, garbage, expired
    settings.tls.key_file = str(tmp_path / "missing.key")
    with pytest.raises(TlsConfigError, match="does not exist"):
        build_uvicorn_ssl_kwargs(settings)
    (tmp_path / "junk.crt").write_text("not a certificate")
    settings.tls.cert_file, settings.tls.key_file = str(tmp_path / "junk.crt"), str(tmp_path / "c.key")
    with pytest.raises(TlsConfigError, match="not a PEM"):
        build_uvicorn_ssl_kwargs(settings)
    ec = _expired_cert(pki, "localhost")
    (tmp_path / "e.crt").write_bytes(ec[0])
    (tmp_path / "e.key").write_bytes(ec[1])
    settings.tls.cert_file, settings.tls.key_file = str(tmp_path / "e.crt"), str(tmp_path / "e.key")
    with pytest.raises(TlsConfigError, match="expired"):
        build_uvicorn_ssl_kwargs(settings)
    # encrypted keys are refused with a message instead of a stack trace
    enc = serialization.load_pem_private_key(Path(tmp_path / "c.key").read_bytes(), None).private_bytes(
        serialization.Encoding.PEM, serialization.PrivateFormat.PKCS8, serialization.BestAvailableEncryption(b"pw"))
    (tmp_path / "enc.key").write_bytes(enc)
    settings.tls.cert_file, settings.tls.key_file = str(tmp_path / "c.crt"), str(tmp_path / "enc.key")
    with pytest.raises(TlsConfigError, match="cannot be loaded"):
        build_uvicorn_ssl_kwargs(settings)


def _cert(pki: PKI, cn: str, *, client: bool, not_before: dt.datetime, not_after: dt.datetime) -> tuple[bytes, bytes]:
    from cryptography import x509
    from cryptography.hazmat.primitives import hashes, serialization
    from cryptography.hazmat.primitives.asymmetric import ec
    from cryptography.x509.oid import ExtendedKeyUsageOID, NameOID

    key = ec.generate_private_key(ec.SECP256R1())
    b = (x509.CertificateBuilder().subject_name(x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, cn)])).issuer_name(pki.ca_cert.subject)
         .public_key(key.public_key()).serial_number(x509.random_serial_number()).not_valid_before(not_before).not_valid_after(not_after)
         .add_extension(x509.ExtendedKeyUsage([ExtendedKeyUsageOID.CLIENT_AUTH if client else ExtendedKeyUsageOID.SERVER_AUTH]), critical=False))
    if not client:
        b = b.add_extension(x509.SubjectAlternativeName([x509.DNSName(cn)]), critical=False)
    cert = b.sign(pki.ca_key, hashes.SHA256())
    return (cert.public_bytes(serialization.Encoding.PEM), key.private_bytes(serialization.Encoding.PEM, serialization.PrivateFormat.PKCS8, serialization.NoEncryption()))


def _expired_cert(pki: PKI, cn: str, *, client: bool = False) -> tuple[bytes, bytes]:
    now = dt.datetime.now(dt.UTC)
    return _cert(pki, cn, client=client, not_before=now - dt.timedelta(days=30), not_after=now - dt.timedelta(days=1))


# ----------------------------------------------------------------------------- minimal ASGI app: what does the application see?
async def probe_app(scope, receive, send):  # noqa: ANN001, ANN201
    if scope["type"] == "lifespan":
        while True:
            msg = await receive()
            if msg["type"] == "lifespan.startup":
                await send({"type": "lifespan.startup.complete"})
            else:
                await send({"type": "lifespan.shutdown.complete"})
                return
    info = json.dumps({"tls": (scope.get("extensions") or {}).get("tls"), "header_cert": dict(scope["headers"]).get(b"x-client-cert", b"").decode()[:20]}).encode()
    if scope["type"] == "http":
        await send({"type": "http.response.start", "status": 200, "headers": [(b"content-type", b"application/json")]})
        await send({"type": "http.response.body", "body": info})
    else:
        await receive()
        await send({"type": "websocket.accept"})
        await send({"type": "websocket.send", "text": info.decode()})
        await send({"type": "websocket.close", "code": 1000})


@pytest.fixture()
def mtls_probe(settings):
    settings.tls.enabled = True
    settings.tls.require_client_cert_for_workers = True
    kw = build_uvicorn_ssl_kwargs(settings)
    pki = PKI(settings.path("pki"))
    with LiveServer(probe_app, **kw) as srv:
        yield srv, pki


def client_ctx(pki: PKI, tmp_path: Path, name: str, cert: tuple[bytes, bytes] | None) -> ssl.SSLContext:
    ctx = ssl.create_default_context(cadata=pki.ca_pem)
    if cert:
        c, k = tmp_path / f"{name}.crt", tmp_path / f"{name}.key"
        c.write_bytes(cert[0])
        k.write_bytes(cert[1])
        ctx.load_cert_chain(str(c), str(k))
    return ctx


def worker_cert(pki: PKI, cn: str) -> tuple[bytes, bytes, str]:
    csr, key = PKI.generate_csr(cn)
    crt, serial = pki.sign_worker_csr(csr, cn)
    return crt.encode(), key.encode(), serial


def test_client_certificate_reaches_http_and_websocket_scope(mtls_probe, tmp_path):
    srv, pki = mtls_probe
    crt, key, _ = worker_cert(pki, "wrk_probe")
    ctx = client_ctx(pki, tmp_path, "w", (crt, key))
    with httpx.Client(verify=ctx, base_url=srv.base_url) as c:
        body = c.get("/x").json()
    assert body["tls"]["client_cert_chain"][0].strip() == crt.decode().strip() and body["tls"]["tls_version"] in ("TLSv1.2", "TLSv1.3")
    with ws_connect(f"wss://127.0.0.1:{srv.port}/w", ssl=ctx) as ws:
        assert json.loads(ws.recv())["tls"]["client_cert_chain"][0].strip() == crt.decode().strip()
    # no certificate: the TLS layer lets browsers/CLI in (CERT_OPTIONAL) but the application sees no tls extension
    anon = client_ctx(pki, tmp_path, "anon", None)
    with httpx.Client(verify=anon, base_url=srv.base_url) as c:
        assert c.get("/x").json()["tls"] is None
    with ws_connect(f"wss://127.0.0.1:{srv.port}/w", ssl=anon) as ws:
        assert json.loads(ws.recv())["tls"] is None


def test_handshake_rejects_foreign_ca_expired_and_old_tls(mtls_probe, tmp_path):
    srv, pki = mtls_probe
    other = PKI(tmp_path / "evil-pki")
    csr, key = PKI.generate_csr("wrk_probe")
    forged, _ = other.sign_worker_csr(csr, "wrk_probe")
    expired = _expired_cert(pki, "wrk_probe", client=True)
    for name, cert in (("foreign", (forged.encode(), key.encode())), ("expired", expired)):
        ctx = client_ctx(pki, tmp_path, name, cert)
        with pytest.raises((ssl.SSLError, ConnectionError, OSError, httpx.HTTPError)):
            with httpx.Client(verify=ctx, base_url=srv.base_url) as c:
                c.get("/x")
        # TLS 1.3 reports the rejected client certificate when the client first reads: the websocket must not deliver data either
        with pytest.raises((ssl.SSLError, ConnectionError, OSError, InvalidHandshake, ConnectionClosed, EOFError)):
            with ws_connect(f"wss://127.0.0.1:{srv.port}/w", ssl=ctx) as ws:
                ws.recv(timeout=5)
    # TLS 1.2 negotiates an ECDHE/AEAD suite
    modern = ssl.create_default_context(cadata=pki.ca_pem)
    modern.maximum_version = ssl.TLSVersion.TLSv1_2
    with socket.create_connection(("127.0.0.1", srv.port), timeout=5) as raw, modern.wrap_socket(raw, server_hostname="localhost") as s:
        assert s.version() == "TLSv1.2" and s.cipher()[0].startswith(("ECDHE", "TLS_"))
        assert "AES" in s.cipher()[0] or "CHACHA" in s.cipher()[0]
    # plaintext HTTP on the TLS port is not served
    with pytest.raises(httpx.HTTPError):
        httpx.get(f"http://127.0.0.1:{srv.port}/x", timeout=5)


# ----------------------------------------------------------------------------- the real control plane
@pytest.fixture()
def mtls_cp(settings, tmp_path):
    """The full control plane (core + compute + ops) behind real TLS with mandatory worker client certificates."""
    from olcf.api.app import create_app

    settings.tls.enabled = True
    settings.tls.require_client_cert_for_workers = True
    kw = build_uvicorn_ssl_kwargs(settings)
    app = create_app(settings, module_names=["olcf.api.core.module", "olcf.compute.module", "olcf.ops.module"])
    ctx = app.state.ctx
    pki = ctx.pki
    with LiveServer(app, **kw) as srv:
        cactx = ssl.create_default_context(cadata=pki.ca_pem)
        pw = settings.path("secrets", "bootstrap-admin.txt").read_text().split("password=")[1].strip()
        http = httpx.Client(verify=cactx, base_url=srv.base_url, timeout=20)
        r = http.post("/v1/identity/login", json={"username": "admin", "password": pw})
        assert r.status_code == 200, r.text
        http.headers["Authorization"] = f"Bearer {r.json()['access_token']}"

        def enroll(name: str) -> dict:
            tok = http.post("/v1/workers/enrollment-tokens", json={"name": name}).json()
            assert "token" in tok, tok
            csr, key = PKI.generate_csr(name)
            r = http.post("/v1/workers/enroll", json={"token": tok["token"], "name": name, "csr_pem": csr})
            assert r.status_code == 201, r.text
            body = r.json()
            body["key_pem"] = key
            body["serial"] = None
            return body

        yield type("CP", (), {"srv": srv, "http": http, "pki": pki, "ctx": ctx, "enroll": staticmethod(enroll), "tmp": tmp_path})
        http.close()


def open_channel(cp, worker: dict, *, token: str | None = None, cert: tuple[bytes, bytes] | None | str = "own", headers: dict | None = None):
    crt = (worker["client_cert_pem"].encode(), worker["key_pem"].encode()) if cert == "own" else cert
    ctx = client_ctx(cp.pki, cp.tmp, worker["name"] + str(id(crt)), crt)
    hdr = {"Authorization": f"Bearer {token or worker['worker_token']}", **(headers or {})}
    return ws_connect(f"wss://127.0.0.1:{cp.srv.port}/v1/workers/ws", ssl=ctx, additional_headers=hdr)


def close_info(cp, worker: dict, **kw) -> tuple[int | None, str]:
    """Open the channel and report the close code the server answered with (None: it stayed open past the auth stage)."""
    try:
        with open_channel(cp, worker, **kw) as ws:
            try:
                ws.recv(timeout=2.0)
            except TimeoutError:
                return None, "open"
            except ConnectionClosed as exc:
                return (exc.rcvd.code if exc.rcvd else None), (exc.rcvd.reason if exc.rcvd else "")
    except ConnectionClosed as exc:
        return (exc.rcvd.code if exc.rcvd else None), (exc.rcvd.reason if exc.rcvd else "")
    return None, "closed"


def test_worker_channel_accepts_only_valid_certificates(mtls_cp):
    cp = mtls_cp
    w1, w2 = cp.enroll("tls-worker-1"), cp.enroll("tls-worker-2")
    # 1. the right certificate + token passes authentication (the server then waits for the hello frame)
    code, reason = close_info(cp, w1)
    assert code is None and reason == "open"
    # 2. API over TLS needs no client certificate, plaintext is not served
    assert cp.http.get("/v1/health").status_code == 200
    # 3. no certificate: connection is accepted at TLS level and refused by the hub (require_client_cert_for_workers)
    code, reason = close_info(cp, w1, cert=None)
    assert code == CLOSE_FORBIDDEN and "client certificate is required" in reason
    # 4. certificate of another worker with this worker's token: CN binding
    other = (w2["client_cert_pem"].encode(), w2["key_pem"].encode())
    code, reason = close_info(cp, w1, cert=other)
    assert code == CLOSE_FORBIDDEN and "does not match" in reason
    # 5. wrong / missing token with a valid certificate
    code, _ = close_info(cp, w1, token="olcf_wrk_" + "A" * 40)
    assert code == CLOSE_UNAUTH
    # 6. a header cannot stand in for the certificate (trusted_proxies = 0): forge the certificate of worker 1 in X-Client-Cert
    forged_header = urllib.parse.quote(w1["client_cert_pem"])
    code, reason = close_info(cp, w1, cert=None, headers={"X-Client-Cert": forged_header})
    assert code == CLOSE_FORBIDDEN and "client certificate is required" in reason
    # 7. foreign CA: fails during the TLS handshake, never reaches the application
    evil = PKI(cp.tmp / "evil")
    csr, key = PKI.generate_csr(w1["worker_id"])
    forged, _ = evil.sign_worker_csr(csr, w1["worker_id"])
    with pytest.raises((ssl.SSLError, ConnectionError, OSError, InvalidHandshake, ConnectionClosed, EOFError)):
        with open_channel(cp, w1, cert=(forged.encode(), key.encode())) as ws:
            ws.recv(timeout=3)
    # 8. revoked certificate: CRL (kv pki.revoked) is honoured on the next connection
    from cryptography import x509

    serial = format(x509.load_pem_x509_certificate(w1["client_cert_pem"].encode()).serial_number, "x")
    cp.ctx.service("compute").hub.revoke_serial(serial)
    code, reason = close_info(cp, w1)
    assert code == CLOSE_FORBIDDEN and "revoked" in reason
    # worker 2 is unaffected
    assert close_info(cp, w2) == (None, "open")
    # 9. a good certificate does not bypass the protocol: garbage instead of hello is a protocol error, not access
    with open_channel(cp, w2) as ws:
        ws.send("not json at all")
        with pytest.raises(ConnectionClosed) as ei:
            ws.recv(timeout=5)
        assert ei.value.rcvd.code == CLOSE_PROTOCOL
    # 10. revoking the worker (not just the certificate) also closes the door for a valid certificate
    assert cp.http.post(f"/v1/workers/{w2['worker_id']}/revoke").status_code in (200, 202, 204)
    code, _ = close_info(cp, w2)
    assert code in (CLOSE_FORBIDDEN, CLOSE_UNAUTH)


def test_worker_channel_without_mtls_requirement_still_verifies_presented_certificates(settings, tmp_path):
    """require_client_cert_for_workers=false: certificates are optional, but a presented certificate is still checked by the hub."""
    from olcf.api.app import create_app

    settings.tls.enabled = True
    kw = build_uvicorn_ssl_kwargs(settings)  # no CA requested: certificates are not even asked for
    assert "ssl_ca_certs" not in kw
    app = create_app(settings, module_names=["olcf.api.core.module", "olcf.compute.module"])
    pki = app.state.ctx.pki
    with LiveServer(app, **kw) as srv:
        cactx = ssl.create_default_context(cadata=pki.ca_pem)
        pw = settings.path("secrets", "bootstrap-admin.txt").read_text().split("password=")[1].strip()
        with httpx.Client(verify=cactx, base_url=srv.base_url, timeout=20) as http:
            tok = http.post("/v1/identity/login", json={"username": "admin", "password": pw}).json()["access_token"]
            http.headers["Authorization"] = f"Bearer {tok}"
            et = http.post("/v1/workers/enrollment-tokens", json={"name": "plain"}).json()["token"]
            csr, key = PKI.generate_csr("plain")
            w = http.post("/v1/workers/enroll", json={"token": et, "name": "plain", "csr_pem": csr}).json()
            ctx = ssl.create_default_context(cadata=pki.ca_pem)
            with ws_connect(f"wss://127.0.0.1:{srv.port}/v1/workers/ws", ssl=ctx, additional_headers={"Authorization": f"Bearer {w['worker_token']}"}) as ws:
                with pytest.raises(TimeoutError):
                    ws.recv(timeout=1.5)  # accepted without a certificate: documented trade-off when mTLS is not required
