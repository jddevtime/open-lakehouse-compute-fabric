# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Diagnostics suite, support bundle (sanitising, opt-ins, leak scanner) and the leak scanner itself."""

from __future__ import annotations

import base64
import gzip
import io
import json
import zipfile

import pytest

from olcf.ops.leakscan import LeakScanner
from tests.ops.conftest import CANARY

LOG_LINE = "2026-10-07 12:00:00,000 ERROR   olcf.test: something failed for {}"


def _bundle(c, **opts):
    return c.post("/v1/admin/support-bundle", json=opts, headers=c.admin)


def _names(c, rec):
    path = c.ctx.service("support").file_path(c.ctx.service("support").get(rec["id"]))
    with zipfile.ZipFile(path) as zf:
        return {n: zf.read(n) for n in zf.namelist()}


# ----------------------------------------------------------------------------- diagnostics
@pytest.fixture()
def listening_control_plane(ops_app):
    """The in-process app has no socket; give the 'control plane' port a real listener so the network checks have something to reach (the suite used to pass only
    when a server happened to run on the default port 8470 of the machine)."""
    import socket

    srv = socket.socket()
    srv.bind(("127.0.0.1", 0))
    srv.listen(8)
    ops_app.ctx.settings.server.host, ops_app.ctx.settings.server.port = "127.0.0.1", srv.getsockname()[1]
    try:
        yield ops_app
    finally:
        srv.close()


def test_diagnostics_suite(listening_control_plane):
    c = listening_control_plane
    r = c.post("/v1/admin/diagnostics", json={}, headers=c.admin)
    assert r.status_code == 200, r.text
    rep = r.json()
    names = {x["name"] for x in rep["checks"]}
    assert {"versions", "read-write", "migrations", "token-roundtrip", "key-permissions", "bootstrap-admin-file", "local-root", "proxy", "database-clock", "system-clock", "effective-settings"} <= names
    assert any(n.startswith("health:") for n in names) and "health:backup" in names and "health:database" in names
    by = {x["name"]: x for x in rep["checks"]}
    assert by["read-write"]["status"] == "ok" and by["token-roundtrip"]["status"] == "ok" and by["local-root"]["status"] == "ok"
    assert by["bootstrap-admin-file"]["status"] == "warn" and "delete the file" in by["bootstrap-admin-file"]["remediation"]
    assert rep["status"] in ("ok", "degraded") and rep["summary"]["fail"] == 0, [x for x in rep["checks"] if x["status"] == "fail"]
    # config is redacted
    cfg = json.dumps(by["effective-settings"]["data"])
    assert c.ctx.settings.path("secrets", "master.key").read_text().strip() not in cfg
    last = c.get("/v1/admin/diagnostics", headers=c.admin).json()
    assert last["id"] == rep["id"]
    assert c.post("/v1/admin/diagnostics", json={"only": ["nope"]}, headers=c.admin).status_code == 400
    sub = c.post("/v1/admin/diagnostics", json={"only": ["time", "certificates"]}, headers=c.admin).json()
    assert {x["category"] for x in sub["checks"]} == {"time", "certificates"}


def test_last_diagnostics_is_a_200_before_the_first_run(ops_app):
    """FIX I: no 404 on a fresh install - ``last_run`` is null; after a run the report comes back with ``last_run`` set."""
    c = ops_app
    first = c.get("/v1/admin/diagnostics", headers=c.admin)
    assert first.status_code == 200
    d = first.json()
    assert d["last_run"] is None and d["checks"] == [] and d["id"] is None and d["summary"] == {"ok": 0, "warn": 0, "fail": 0, "skipped": 0}
    rep = c.post("/v1/admin/diagnostics", json={"only": ["time"]}, headers=c.admin).json()
    last = c.get("/v1/admin/diagnostics", headers=c.admin).json()
    assert last["id"] == rep["id"] and last["last_run"] == rep["started_at"] and last["checks"]
    assert c.get("/v1/admin/diagnostics").status_code == 401


def test_diagnostics_detects_problems(ops_app, tmp_path):
    c = ops_app
    assert c.put("/v1/admin/updates/config", json={"feed_url": "http://updates.nonexistent.invalid/feed", "allow_insecure_http": True}, headers=c.admin).status_code == 200
    # loose permissions on a key file
    k = c.ctx.settings.path("secrets", "jwt-signing.key")
    k.chmod(0o644)
    rep = c.post("/v1/admin/diagnostics", json={"only": ["network", "auth"]}, headers=c.admin).json()
    by = {x["name"]: x for x in rep["checks"]}
    assert by["dns:update-feed"]["status"] == "fail" and "resolve" in by["dns:update-feed"]["detail"] and by["dns:update-feed"]["remediation"]
    assert by["key-permissions"]["status"] == "fail" and "jwt-signing.key" in by["key-permissions"]["detail"]
    assert rep["status"] == "down"
    quick = c.ctx.health.run("diagnostics", force=True)
    assert quick.status.value in ("degraded", "down", "ok")


def test_diagnostics_health_check_quick_and_deep(ops_app):
    c = ops_app
    q = c.ctx.health.run("diagnostics", force=True)
    assert q.status.value == "ok" and "no diagnostics run yet" in q.detail
    d = c.post("/v1/admin/health/checks/diagnostics/test", headers=c.admin).json()
    assert d["steps"] and any(s["name"] == "token-roundtrip" for s in d["steps"])
    q2 = c.ctx.health.run("diagnostics", force=True)
    assert "last run" in q2.detail


def test_diagnostics_permissions(ops_app):
    c = ops_app
    pw = "Correct-Horse-9-battery"
    for role, allowed in (("Viewer", False), ("Contributor", False), ("Operator", False), ("RuntimeAdmin", True), ("SecurityAdmin", False)):
        assert c.post("/v1/identity/users", json={"name": f"d-{role.lower()}", "password": pw, "roles": [{"role": role}]}, headers=c.admin).status_code == 201
        h = {"Authorization": f"Bearer {c.post('/v1/identity/login', json={'username': f'd-{role.lower()}', 'password': pw}).json()['access_token']}"}
        for method, path, body in (("post", "/v1/admin/diagnostics", {"only": ["time"]}), ("get", "/v1/admin/support-bundle", None), ("post", "/v1/admin/support-bundle", {})):
            kw = {"json": body} if body is not None else {}
            code = getattr(c, method)(path, headers=h, **kw).status_code
            assert (code < 300) == allowed, (role, path, code)
    assert c.post("/v1/admin/diagnostics", json={}).status_code == 401


# ----------------------------------------------------------------------------- support bundle
def test_bundle_contents_sanitising_and_redaction(ops_app):
    c = ops_app
    ctx = c.ctx
    ctx.secrets.create("db-pass", "Zx9-very-secret-VALUE")
    logd = ctx.settings.path("logs")
    logd.mkdir(exist_ok=True)
    jwt = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiJ1c3JfMTIzNDU2In0.abcdefghijklmnop"  # gitleaks:allow
    (logd / "olcf.log").write_text("\n".join([
        LOG_LINE.format("user alice@example.com from 10.20.30.40"),
        f"request Authorization: Bearer {jwt}",
        "token olcf_pat_" + "A1b2C3d4" * 6,
        "connect postgresql://svc:hunter2hunter2@db.internal:5432/olcf",
        "password=correct-horse-battery-staple",
    ]) + "\n")
    r = _bundle(c)
    assert r.status_code == 201, r.text
    rec = r.json()
    assert rec["scan"]["ok"] and rec["scan"]["canaries"] >= 3 and rec["sha256"]
    files = _names(c, rec)
    assert {"summary.json", "versions.txt", "diagnostics.json", "health.json", "config/settings.json", "logs/olcf.log", "MANIFEST.txt"} <= set(files)
    assert not [n for n in files if n.startswith(("audit/", "jobs/", "workers/"))]
    log = files["logs/olcf.log"].decode()
    assert "alice@example.com" not in log and "10.20.30.40" not in log and "email-" in log and "ip-" in log
    for leak in (jwt, "olcf_pat_A1b2", "hunter2hunter2", "correct-horse-battery-staple"):
        assert leak not in log
    assert "REDACTED" in log
    blob = b"".join(files.values())
    assert b"Zx9-very-secret-VALUE" not in blob
    master = ctx.settings.path("secrets", "master.key").read_bytes().strip()
    assert master not in blob and base64.b64decode(master) not in blob
    assert ctx.settings.path("secrets", "bootstrap-admin.txt").read_text().split("password=")[1].split()[0].encode() not in blob
    summary = json.loads(files["summary.json"])
    assert summary["database"]["dialect"] == "sqlite" and summary["version"]
    # download is the same file; unsanitized keeps identifiers (but still redacts credentials)
    d = c.get(f"/v1/admin/support-bundle/{rec['id']}/download", headers=c.admin)
    assert d.status_code == 200 and zipfile.ZipFile(io.BytesIO(d.content)).testzip() is None
    raw = _names(c, _bundle(c, sanitize=False, include_diagnostics=False).json())
    assert "10.20.30.40" in raw["logs/olcf.log"].decode() and "jwt" not in raw["logs/olcf.log"].decode().lower().replace("jwt-signing", "")
    assert jwt not in raw["logs/olcf.log"].decode()


def test_bundle_optin_sections(ops_app):
    c = ops_app
    c.ctx.audit.record(None, "x.action", details={"note": "someone@corp.example"})
    inc = _bundle(c, include_audit=True, include_jobs=True).json()
    files = _names(c, inc)
    assert "audit/recent.jsonl" in files
    assert b"someone@corp.example" not in files["audit/recent.jsonl"]
    # audit.read is required for the audit opt-in
    pw = "Correct-Horse-9-battery"
    c.post("/v1/identity/users", json={"name": "rt", "password": pw, "roles": [{"role": "RuntimeAdmin"}]}, headers=c.admin)
    h = {"Authorization": f"Bearer {c.post('/v1/identity/login', json={'username': 'rt', 'password': pw}).json()['access_token']}"}
    assert c.post("/v1/admin/support-bundle", json={"include_audit": True}, headers=h).status_code == 403
    assert c.post("/v1/admin/support-bundle", json={}, headers=h).status_code == 201
    # list/get/delete
    items = c.get("/v1/admin/support-bundle", headers=c.admin).json()["items"]
    assert len(items) >= 2
    assert c.delete(f"/v1/admin/support-bundle/{inc['id']}", headers=c.admin).status_code == 204
    assert c.get(f"/v1/admin/support-bundle/{inc['id']}", headers=c.admin).status_code == 404
    assert c.post("/v1/admin/support-bundle", json={"include_everything": True}, headers=c.admin).status_code == 422


def test_bundle_aborts_when_a_live_secret_leaks_into_logs(ops_app):
    c = ops_app
    ctx = c.ctx
    ctx.secrets.create("storage-key", {"access_key": "AKIAEXAMPLE000000000", "secret": "wJalrXUtnFEMI-K7MDENG-bPxRfiCYEXAMPLEKEY99"})
    ctx.secrets.create("plain", CANARY)
    logd = ctx.settings.path("logs")
    logd.mkdir(exist_ok=True)
    # a component logs the bare value: the pattern redactor cannot know it is a secret
    (logd / "olcf.log").write_text(LOG_LINE.format(f"calling service with {CANARY}") + "\n")
    r = _bundle(c)
    assert r.status_code == 500, r.text
    body = r.text
    assert CANARY not in body and "secret_leak_detected" in body and "plain" not in body.split("findings")[0]
    assert not list(ctx.settings.path("artifacts", "support-bundles").glob("*"))
    assert c.get("/v1/admin/support-bundle", headers=c.admin).json()["items"] == []
    ev = ctx.audit.query(limit=10, action="admin.support_bundle_blocked")
    assert ev and ev[0]["outcome"] == "failure" and CANARY not in json.dumps(ev[0])
    # base64 of the value is a leak too; a different secret leaking in the audit opt-in section
    (logd / "olcf.log").write_text(LOG_LINE.format("clean") + "\n")
    ctx.audit.record(None, "oops", details={"payload": base64.b64encode(CANARY.encode()).decode()})
    r = _bundle(c, include_audit=True)
    assert r.status_code == 500 and "audit/recent.jsonl" in r.text
    # the same secret in structured storage-key form
    (logd / "olcf.log").write_text(LOG_LINE.format("conn secret wJalrXUtnFEMI-K7MDENG-bPxRfiCYEXAMPLEKEY99 ok") + "\n")
    assert _bundle(c).status_code == 500
    # once the leak is gone bundles work again
    (logd / "olcf.log").write_text(LOG_LINE.format("fine") + "\n")
    r = _bundle(c)
    assert r.status_code == 201, r.text


def test_worker_archive_opt_in_is_scanned(ops_app):
    c = ops_app

    def archive(secret_inside: bool) -> bytes:
        buf = io.BytesIO()
        with zipfile.ZipFile(buf, "w") as zf:
            zf.writestr("agent.log", "agent started\n" + (f"conn {CANARY}\n" if secret_inside else ""))
        return buf.getvalue()

    state = {"leak": False}

    class Hub:
        def request(self, wid, type_, payload=None, *, timeout=30.0):  # noqa: ANN001, ANN201
            assert type_ == "collect_diagnostics"
            return {"archive_b64": base64.b64encode(archive(state["leak"])).decode()}

    class Reg:
        def list_workers(self, **_):  # noqa: ANN001, ANN201
            return [{"id": "wrk_1", "name": "node-1", "connected": True}, {"id": "wrk_2", "name": "off", "connected": False}]

    class Compute:
        hub, registry = Hub(), Reg()

    c.ctx.register_service("compute", Compute())
    c.ctx.secrets.create("canary", CANARY)
    ok = _bundle(c, include_worker_diagnostics=True, include_diagnostics=False).json()
    assert "workers/node-1.zip" in _names(c, ok) and "workers/off.zip" not in _names(c, ok)
    state["leak"] = True
    r = _bundle(c, include_worker_diagnostics=True, include_diagnostics=False)
    assert r.status_code == 500 and "workers/node-1.zip!agent.log" in r.text


# ----------------------------------------------------------------------------- scanner unit tests
def test_scanner_finds_across_chunk_boundary_and_in_archives(monkeypatch, tmp_path):
    import olcf.ops.leakscan as L

    monkeypatch.setattr(L, "CHUNK", 64)
    secret = b"s3cr3t-value-0123456789"
    sc = LeakScanner({"sec_1": secret}, patterns=False)
    for pad in range(0, 70):
        data = b"x" * pad + secret + b"y" * 10
        f = sc.scan_bytes("f", data)
        assert len(f) >= 1 and f[0].detail == "value of sec_1" and secret.decode() not in json.dumps([x.to_dict() for x in f])
    assert sc.scan_bytes("f", b"nothing here " * 100) == []
    # nested: zip in gzip, tar.gz, base64 / url-encoded / hex variants
    inner = io.BytesIO()
    with zipfile.ZipFile(inner, "w") as zf:
        zf.writestr("deep/a.txt", b"prefix " + secret)
    p = tmp_path / "x.gz"
    p.write_bytes(gzip.compress(inner.getvalue()))
    res = sc.scan_file(p)
    assert not res.ok and "x.gz!gunzip!deep/a.txt" in res.findings[0].file
    for variant in (base64.b64encode(secret), base64.urlsafe_b64encode(secret), secret.hex().encode()):
        assert sc.scan_bytes("v", b"..." + variant + b"...")
    # patterns
    pat = LeakScanner({})
    for text in (b"eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiIxMjM0NTYifQ.c2lnbmF0dXJl", b"olcf_pat_" + b"A" * 24, b"-----BEGIN PRIVATE KEY-----", b"AKIAABCDEFGHIJKLMNOP",  # gitleaks:allow
                 b"Authorization: Bearer abcdefghijklmnopqrstuvwxyz123456", b"postgresql://u:topsecretpw@h/db", b"password = 'hunter2hunter2'", b'"client_secret": "abcdEFGH12345678"'):  # gitleaks:allow
        assert pat.scan_bytes("p", text), text
    for benign in (b"password: ***REDACTED***", b"postgresql://u:***REDACTED***@h/db", b"password_min_length: 12", b"Authorization: Bearer ***REDACTED***", b'{"secret_id": "sec_01H"}', b"token_ttl: 1800"):
        assert pat.scan_bytes("b", benign) == [], benign


def test_scanner_from_context_collects_live_values(ops_app):
    c = ops_app
    c.ctx.secrets.create("a", "first-secret-value")
    c.ctx.secrets.create("b", {"user": "svc-account", "password": "Passw0rd-123456"})  # gitleaks:allow
    sc = LeakScanner.from_context(c.ctx, {"extra": "an-extra-canary"})
    assert sc.scan_bytes("t", b"x first-secret-value y") and sc.scan_bytes("t", b"Passw0rd-123456") and sc.scan_bytes("t", b"an-extra-canary")
    assert sc.scan_bytes("t", b"svc-account")  # structured secret values are canaries too
    master = base64.b64encode(c.ctx.secrets.master.keys[c.ctx.secrets.master.current_id])
    assert sc.scan_bytes("t", master)
    assert sc.scan_bytes("t", c.ctx.settings.path("secrets", "jwt-signing.key").read_bytes().strip())
    labels = {f.detail for f in sc.scan_bytes("t", b"first-secret-value")}
    assert labels == {"value of secret:a"}
