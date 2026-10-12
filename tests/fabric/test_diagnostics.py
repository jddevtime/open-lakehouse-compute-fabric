# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Connectivity diagnostics with proxy and custom-CA handling, and the shared outbound HTTP settings (``olcf.fabric.http``)."""

from __future__ import annotations

import datetime as dt
import ipaddress
import socket
import ssl
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any

import pytest
from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.x509.oid import NameOID

from olcf.fabric import diagnostics as dg
from olcf.fabric.http import Network, make_client, verify_for


def self_signed(dir_: Path) -> tuple[Path, Path]:
    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "olcf-test-ca")])
    now = dt.datetime.now(dt.UTC)
    cert = (x509.CertificateBuilder().subject_name(name).issuer_name(name).public_key(key.public_key()).serial_number(x509.random_serial_number())
            .not_valid_before(now - dt.timedelta(minutes=5)).not_valid_after(now + dt.timedelta(days=1))
            .add_extension(x509.SubjectAlternativeName([x509.IPAddress(ipaddress.ip_address("127.0.0.1")), x509.DNSName("localhost")]), critical=False)
            .add_extension(x509.BasicConstraints(ca=True, path_length=None), critical=True).sign(key, hashes.SHA256()))
    c, k = dir_ / "ca.pem", dir_ / "key.pem"
    c.write_bytes(cert.public_bytes(serialization.Encoding.PEM))
    k.write_bytes(key.private_bytes(serialization.Encoding.PEM, serialization.PrivateFormat.TraditionalOpenSSL, serialization.NoEncryption()))
    return c, k


@pytest.fixture(scope="module")
def tls_server(tmp_path_factory: pytest.TempPathFactory):
    d = tmp_path_factory.mktemp("tls")
    cert, key = self_signed(d)

    class H(BaseHTTPRequestHandler):
        def log_message(self, *a: Any) -> None:
            pass

        def do_GET(self) -> None:  # noqa: N802
            body = b'{"keys": [{"kid": "k"}]}'
            self.send_response(200)
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

    srv = ThreadingHTTPServer(("127.0.0.1", 0), H)
    ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    ctx.load_cert_chain(str(cert), str(key))
    srv.socket = ctx.wrap_socket(srv.socket, server_side=True)
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    yield f"https://127.0.0.1:{srv.server_address[1]}", cert
    srv.shutdown()
    srv.server_close()


def net(url: str, **kw: Any) -> Network:
    return Network(authority_host=url, fabric_api=url, onelake_dfs=url, trust_env=False, **kw)


def inp(n: Network, **kw: Any) -> dg.DiagnosticsInput:
    return dg.DiagnosticsInput(network=n, broker=None, **kw)


def test_tls_fails_closed_without_the_ca_and_passes_with_it(tls_server: tuple[str, Path]) -> None:
    url, ca = tls_server
    bad = dg._tls(inp(net(url)))
    assert bad["ok"] is False and "certificate verification failed" in bad["detail"] and "ca_bundle" in bad["remediation"]
    good = dg._tls(inp(net(url, ca_bundle=str(ca))))
    assert good["ok"] is True and "TLS ok" in good["detail"]
    full = dg.run_diagnostics(inp(net(url, ca_bundle=str(ca)), allowed_tenants=["t1"], audiences=["api://x"], public_url="https://olcf.example.org", jwks_url=lambda t: f"{url}/{t}/keys"))
    steps = {s["name"]: s for s in full["steps"]}
    assert steps["TLS"]["ok"] and steps["inbound path"]["ok"], steps["inbound path"]
    assert not steps["Entra token"]["ok"] and steps["Fabric API"].get("skipped") and steps["OneLake"].get("skipped")
    assert full["ok"] is False and "Entra token" in full["failed"] and "Fabric API" not in full["failed"]


def test_missing_ca_bundle_is_an_error_never_a_silent_fallback(tmp_path: Path) -> None:
    n = Network(ca_bundle=str(tmp_path / "nope.pem"), trust_env=False)
    with pytest.raises(FileNotFoundError):
        verify_for(n)
    with pytest.raises(FileNotFoundError):
        make_client(n)


def test_network_resolution_order_and_masking() -> None:
    env = {"HTTPS_PROXY": "http://envproxy:3128", "SSL_CERT_FILE": "/etc/ssl/env.pem"}
    n = Network.from_config({}, env)
    assert n.proxy == "http://envproxy:3128" and not n.proxy_explicit and n.ca_bundle == "/etc/ssl/env.pem"
    n = Network.from_config({"proxy": "http://user:pw@cfgproxy:8080", "ca_bundle": "/c.pem"}, env)
    assert n.proxy_explicit and n.ca_bundle == "/c.pem"
    d = n.describe()
    assert "pw" not in str(d) and "***@cfgproxy:8080" in d["proxy"]
    off = Network.from_config({"trust_env": False}, env)
    assert off.proxy is None and off.ca_bundle is None
    assert Network.from_config({"authority_host": "https://login.example/"}, {}).authority_host == "https://login.example"


def test_proxy_step() -> None:
    assert dg._proxy(inp(Network(trust_env=False)))["ok"]
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        s.listen(1)
        port = s.getsockname()[1]
        ok = dg._proxy(inp(Network(proxy=f"http://127.0.0.1:{port}", proxy_explicit=True, trust_env=False)))
        assert ok["ok"] and "accepts connections" in ok["detail"]
    with socket.socket() as s2:
        s2.bind(("127.0.0.1", 0))
        dead = s2.getsockname()[1]
    bad = dg._proxy(inp(Network(proxy=f"http://127.0.0.1:{dead}", proxy_explicit=True, trust_env=False)))
    assert not bad["ok"] and bad["remediation"]


def test_dns_step_names_the_unresolvable_host() -> None:
    n = Network(authority_host="https://no-such-host.invalid", fabric_api="https://127.0.0.1", onelake_dfs="https://127.0.0.1", trust_env=False)
    res = dg._dns(inp(n))
    assert not res["ok"] and "no-such-host.invalid" in res["detail"] and "DNS" in res["remediation"]
    via_proxy = dg._dns(inp(Network(proxy="http://no-such-proxy.invalid:3128", proxy_explicit=True, trust_env=False)))
    assert not via_proxy["ok"] and "proxy host" in via_proxy["detail"]


def test_inbound_step_requires_tenants_audiences_and_https() -> None:
    res = dg._inbound(inp(Network(trust_env=False), public_url="http://olcf.example.org"))
    assert not res["ok"]
    for needle in ("no allowed Entra tenant", "no audience", "not HTTPS"):
        assert needle in res["detail"], res["detail"]


def test_secrets_never_appear_in_step_details() -> None:
    secret = "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"  # noqa: S105
    step = dg._step("x", False, f"failed with aws_secret_access_key = '{secret}'", 0.0)
    assert secret not in step["detail"]
