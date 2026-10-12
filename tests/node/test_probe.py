# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Unit tests for :mod:`olcf.worker.probes` against real local sockets, a real TLS server and real files.

Remote storage probes (abfss/s3/gcs) are tested for URI -> endpoint mapping and against a local TCP endpoint standing in for
the storage host; the real cloud endpoints are not contacted (the sandbox has no route to them), and the probe is by design a
*reachability* check only.
"""

from __future__ import annotations

import datetime as dt
import http.server
import ipaddress
import json
import socket
import ssl
import threading
import time
from pathlib import Path

import pytest
from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import ec
from cryptography.x509.oid import NameOID

from olcf.worker.probes import ProbeSuite


# --------------------------------------------------------------------------------------------- helpers
def _name(cn: str) -> x509.Name:
    return x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, cn)])


def make_ca() -> tuple[ec.EllipticCurvePrivateKey, x509.Certificate]:
    key = ec.generate_private_key(ec.SECP256R1())
    now = dt.datetime.now(dt.UTC)
    cert = (x509.CertificateBuilder().subject_name(_name("probe-test-ca")).issuer_name(_name("probe-test-ca")).public_key(key.public_key())
            .serial_number(x509.random_serial_number()).not_valid_before(now - dt.timedelta(days=1)).not_valid_after(now + dt.timedelta(days=30))
            .add_extension(x509.BasicConstraints(ca=True, path_length=None), critical=True)
            .add_extension(x509.KeyUsage(True, False, False, False, False, True, True, False, False), critical=True)
            .add_extension(x509.SubjectKeyIdentifier.from_public_key(key.public_key()), critical=False).sign(key, hashes.SHA256()))
    return key, cert


def make_server_cert(ca_key, ca_cert, *, days_valid: float = 90.0) -> tuple[bytes, bytes]:
    key = ec.generate_private_key(ec.SECP256R1())
    now = dt.datetime.now(dt.UTC)
    cert = (x509.CertificateBuilder().subject_name(_name("localhost")).issuer_name(ca_cert.subject).public_key(key.public_key())
            .serial_number(x509.random_serial_number()).not_valid_before(now - dt.timedelta(hours=1))
            .not_valid_after(now + dt.timedelta(days=days_valid))
            .add_extension(x509.SubjectAlternativeName([x509.DNSName("localhost"), x509.IPAddress(ipaddress.ip_address("127.0.0.1"))]), critical=False)
            .add_extension(x509.AuthorityKeyIdentifier.from_issuer_public_key(ca_key.public_key()), critical=False)
            .sign(ca_key, hashes.SHA256()))
    return (cert.public_bytes(serialization.Encoding.PEM),
            key.private_bytes(serialization.Encoding.PEM, serialization.PrivateFormat.PKCS8, serialization.NoEncryption()))


class _Handler(http.server.BaseHTTPRequestHandler):
    def do_GET(self):  # noqa: N802
        if self.path == "/json":
            body, code = json.dumps({"status": "ALIVE", "workers": [{"state": "ALIVE"}, {"state": "DEAD"}]}).encode(), 200
        elif self.path == "/dead":
            body, code = json.dumps({"status": "STANDBY", "workers": []}).encode(), 200
        elif self.path == "/missing":
            body, code = b"nope", 404
        else:
            body, code = b"ok", 200
        self.send_response(code)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *a):  # noqa: D401
        pass


class _Server:
    def __init__(self, tls: tuple[bytes, bytes] | None = None, tmp: Path | None = None):
        self.httpd = http.server.ThreadingHTTPServer(("127.0.0.1", 0), _Handler)
        if tls:
            cert, key = tls
            (tmp / "srv.crt").write_bytes(cert)
            (tmp / "srv.key").write_bytes(key)
            ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
            ctx.load_cert_chain(str(tmp / "srv.crt"), str(tmp / "srv.key"))
            self.httpd.socket = ctx.wrap_socket(self.httpd.socket, server_side=True)
        self.port = self.httpd.server_address[1]
        self.thread = threading.Thread(target=self.httpd.serve_forever, daemon=True)
        self.thread.start()

    def close(self):
        self.httpd.shutdown()
        self.httpd.server_close()


@pytest.fixture
def suite(tmp_path):
    return ProbeSuite(state_dir=tmp_path)


@pytest.fixture
def http_server():
    s = _Server()
    yield s
    s.close()


@pytest.fixture
def closed_port():
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


# --------------------------------------------------------------------------------------------- tests
def test_unknown_kind_and_never_raises(suite):
    r = suite.run_one({"kind": "bogus"})
    assert not r["ok"] and "unknown probe kind" in r["detail"]
    r = suite.run_one({"kind": "tcp", "host": "127.0.0.1", "port": "not-an-int"})  # internal ValueError must be contained
    assert r["ok"] is False and "ValueError" in r["detail"]


def test_run_aggregates(suite, http_server):
    out = suite.run([{"kind": "tcp", "host": "127.0.0.1", "port": http_server.port, "name": "a"},
                     {"kind": "dns", "host": "localhost", "name": "b"}])
    assert out["ok"] and [c["name"] for c in out["checks"]] == ["a", "b"] and out["ms"] >= 0
    out = suite.run([{"kind": "tcp", "host": "127.0.0.1", "port": http_server.port}, {"kind": "tcp", "host": "127.0.0.1"}])
    assert not out["ok"] and out["checks"][0]["ok"] and not out["checks"][1]["ok"]


def test_tcp(suite, http_server, closed_port):
    assert suite.run_one({"kind": "tcp", "host": "127.0.0.1", "port": http_server.port})["ok"]
    r = suite.run_one({"kind": "tcp", "host": "127.0.0.1", "port": closed_port, "timeout": 1})
    assert not r["ok"] and "cannot connect" in r["detail"]
    r = suite.run_one({"kind": "tcp", "host": "no-such-host.invalid", "port": 80, "timeout": 2})
    assert not r["ok"]


def test_dns(suite):
    r = suite.run_one({"kind": "dns", "host": "localhost"})
    assert r["ok"] and r["data"]
    assert suite.run_one({"kind": "dns", "host": "localhost", "expect_ip": "127.0.0.1"})["ok"] or "::1" in r["data"]
    r = suite.run_one({"kind": "dns", "host": "localhost", "expect_ip": "203.0.113.9"})
    assert not r["ok"] and "expected" in r["detail"]
    assert not suite.run_one({"kind": "dns", "host": "no-such-host.invalid"})["ok"]
    assert not suite.run_one({"kind": "dns"})["ok"]


def test_http_status_expectations(suite, http_server):
    base = f"http://127.0.0.1:{http_server.port}"
    assert suite.run_one({"kind": "http", "url": base + "/"})["ok"]
    r = suite.run_one({"kind": "http", "url": base + "/missing"})
    assert not r["ok"] and r["data"]["status"] == 404
    assert suite.run_one({"kind": "http", "url": base + "/missing", "expect_status": [404]})["ok"]
    assert not suite.run_one({"kind": "http", "url": "ftp://x"})["ok"]


def test_http_connection_refused(suite, closed_port):
    r = suite.run_one({"kind": "http", "url": f"http://127.0.0.1:{closed_port}/", "timeout": 2})
    assert not r["ok"] and "failed" in r["detail"]


def test_https_pinned_ca_and_expiry(tmp_path):
    ca_key, ca_cert = make_ca()
    ca_pem = ca_cert.public_bytes(serialization.Encoding.PEM).decode()
    srv = _Server(make_server_cert(ca_key, ca_cert, days_valid=90), tmp_path)
    try:
        url = f"https://localhost:{srv.port}/"
        # unknown CA: verification must fail closed
        plain = ProbeSuite(state_dir=tmp_path)
        r = plain.run_one({"kind": "http", "url": url})
        assert not r["ok"] and "certificate verification failed" in r["detail"]
        # pinned agent CA: succeeds and reports TLS info + days left
        pinned = ProbeSuite(state_dir=tmp_path, ca_pem=ca_pem)
        r = pinned.run_one({"kind": "http", "url": url, "use_agent_ca": True})
        assert r["ok"], r
        assert r["data"]["tls"]["protocol"].startswith("TLSv1") and 85 < r["data"]["tls"]["days_left"] <= 90
        assert "TLS" in r["detail"] and "expires" not in r["detail"]
        # explicit ca_pem works too, verify=False skips verification (explicit opt-out)
        assert plain.run_one({"kind": "http", "url": url, "ca_pem": ca_pem})["ok"]
        assert plain.run_one({"kind": "http", "url": url, "verify": False})["ok"]
        # the agent CA is only used when asked for
        assert not pinned.run_one({"kind": "http", "url": url})["ok"]
    finally:
        srv.close()


def test_https_expiring_certificate_is_flagged(tmp_path):
    ca_key, ca_cert = make_ca()
    srv = _Server(make_server_cert(ca_key, ca_cert, days_valid=5), tmp_path)
    try:
        s = ProbeSuite(state_dir=tmp_path, ca_pem=ca_cert.public_bytes(serialization.Encoding.PEM).decode())
        r = s.run_one({"kind": "http", "url": f"https://127.0.0.1:{srv.port}/", "use_agent_ca": True})
        assert r["ok"] and "expires in" in r["detail"] and r["data"]["tls"]["days_left"] < 6
    finally:
        srv.close()


def test_storage_local(suite, tmp_path):
    d = tmp_path / "lake"
    d.mkdir()
    r = suite.run_one({"kind": "storage", "uri": str(d)})
    assert r["ok"] and "readable" in r["detail"]
    r = suite.run_one({"kind": "storage", "uri": d.as_uri(), "write": True})
    assert r["ok"] and "writable" in r["detail"]
    assert list(d.iterdir()) == []  # probe file removed
    r = suite.run_one({"kind": "storage", "uri": str(tmp_path / "missing")})
    assert not r["ok"] and "does not exist" in r["detail"]
    assert not suite.run_one({"kind": "storage"})["ok"]


def test_storage_remote_mapping():
    m = ProbeSuite._storage_target
    assert m("abfss://fs@acct.dfs.core.windows.net/p", None) == ("azure", "acct.dfs.core.windows.net", 443, True)
    assert m("s3a://bucket/k", None) == ("s3", "bucket.s3.amazonaws.com", 443, True)
    assert m("gs://b/o", None)[:2] == ("gcs", "storage.googleapis.com")
    assert m("https://h:8443/x", None) == ("http", "h", 8443, True)
    assert m("s3://b/k", "http://minio:9000") == ("endpoint", "minio", 9000, False)
    assert m("/local/path", None) is None


def test_storage_remote_reachability_via_endpoint(suite, http_server, closed_port):
    uri = "s3a://bucket/key"
    r = suite.run_one({"kind": "storage", "uri": uri, "endpoint": f"http://127.0.0.1:{http_server.port}"})
    assert r["ok"] and "reachability only" in r["detail"] and r["data"]["port"] == http_server.port
    r = suite.run_one({"kind": "storage", "uri": uri, "endpoint": f"http://127.0.0.1:{closed_port}", "timeout": 1})
    assert not r["ok"]
    r = suite.run_one({"kind": "storage", "uri": "abfss://fs@no-such-account.invalid/x", "timeout": 1})
    assert not r["ok"] and "cannot resolve" in r["detail"]


def test_disk(suite, tmp_path):
    r = suite.run_one({"kind": "disk"})
    assert r["ok"] and r["data"]["free_gb"] > 0
    r = suite.run_one({"kind": "disk", "path": str(tmp_path / "not" / "yet" / "there"), "min_free_gb": 0.001})
    assert r["ok"]  # walks up to the nearest existing parent
    r = suite.run_one({"kind": "disk", "min_free_gb": 10**9})
    assert not r["ok"] and "need at least" in r["detail"]
    r = suite.run_one({"kind": "disk", "min_free_gb": 0, "min_free_percent": 101})
    assert not r["ok"]


def test_clock(tmp_path):
    s = ProbeSuite(state_dir=tmp_path, skew=lambda: 0.4)
    assert s.run_one({"kind": "clock"})["ok"]
    r = ProbeSuite(state_dir=tmp_path, skew=lambda: 42.0).run_one({"kind": "clock"})
    assert not r["ok"] and "NTP" in r["detail"] and r["data"]["skew_seconds"] == 42.0
    assert not ProbeSuite(state_dir=tmp_path).run_one({"kind": "clock"})["ok"]  # nothing observed yet -> cannot claim ok
    now = dt.datetime.now(dt.UTC).isoformat()
    assert ProbeSuite(state_dir=tmp_path).run_one({"kind": "clock", "server_time": now})["ok"]
    old = (dt.datetime.now(dt.UTC) - dt.timedelta(minutes=5)).isoformat()
    assert not ProbeSuite(state_dir=tmp_path).run_one({"kind": "clock", "server_time": old})["ok"]


def test_runtime_check(suite, tmp_path):
    assert "no runtime manager" in suite.run_one({"kind": "runtime", "runtime_id": "x"})["detail"]
    from olcf.runtime.manager import ManagerConfig, RuntimeManager

    s = ProbeSuite(state_dir=tmp_path, runtimes=RuntimeManager(ManagerConfig(state_dir=tmp_path / "rt")))
    assert not s.run_one({"kind": "runtime"})["ok"]
    r = s.run_one({"kind": "runtime", "runtime_id": "ghost"})
    assert not r["ok"] and "ghost" in r["detail"]


def test_runtime_check_on_fake_runtime(tmp_path):
    from olcf.runtime.manager import ManagerConfig, RuntimeManager
    from tests.node.fakes import make_fake_runtime

    state = tmp_path / "state"
    make_fake_runtime(state)
    mgr = RuntimeManager(ManagerConfig(state_dir=state))
    s = ProbeSuite(state_dir=state, runtimes=mgr)
    r = s.run_one({"kind": "runtime", "runtime_id": "olcf-rt-fake-1"})
    # the look-alike runtime has neither a signed manifest nor pyspark: the probe must fail closed and say why (with steps)
    assert r["ok"] is False and "failed verification" in r["detail"]
    assert {st["name"]: st["ok"] for st in r["data"]["steps"]}["python"] is False


def test_spark_master_json(suite, http_server):
    base = f"http://127.0.0.1:{http_server.port}"
    r = suite.run_one({"kind": "spark_master", "url": base + "/json"})
    assert r["ok"] and "1 worker" in r["detail"] and r["data"]["status"] == "ALIVE"
    r = suite.run_one({"kind": "spark_master", "url": base + "/dead"})
    assert not r["ok"] and "STANDBY" in r["detail"]
    r = suite.run_one({"kind": "spark_master", "url": base + "/missing"})
    assert not r["ok"]


def test_spark_master_url_tcp(suite, http_server, closed_port):
    assert suite.run_one({"kind": "spark_master", "master_url": f"spark://127.0.0.1:{http_server.port}"})["ok"]
    assert not suite.run_one({"kind": "spark_master", "master_url": f"spark://127.0.0.1:{closed_port}", "timeout": 1})["ok"]
    assert not suite.run_one({"kind": "spark_master", "master_url": "http://x"})["ok"]
    assert not suite.run_one({"kind": "spark_master"})["ok"]


def test_details_are_redacted(suite):
    r = suite.run_one({"kind": "http", "url": "http://127.0.0.1:1/?password=hunter2hunter2", "timeout": 1})
    assert "hunter2hunter2" not in json.dumps(r)


def test_timeout_is_honoured(suite):
    # a listening socket that never answers HTTP: the request must give up at the timeout instead of hanging
    srv = socket.socket()
    srv.bind(("127.0.0.1", 0))
    srv.listen(5)
    try:
        t0 = time.monotonic()
        r = suite.run_one({"kind": "http", "url": f"http://127.0.0.1:{srv.getsockname()[1]}/", "timeout": 1})
        assert not r["ok"] and time.monotonic() - t0 < 6
    finally:
        srv.close()
