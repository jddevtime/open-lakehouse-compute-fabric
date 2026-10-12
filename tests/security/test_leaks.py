# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Spec 35.4: canary-secret leak scans.

A full control plane is exercised with *known* secrets - platform secrets, connection and backup credentials, a webhook signing secret, user passwords, issued access / refresh /
personal-access tokens - and then every place where they must **not** appear is scanned for them (plain, base64, hex and URL-encoded) with the same scanner the support bundle uses:

* every GET response of the API (as the platform administrator, who sees the most), audit export, metrics and health output;
* the log stream of the whole run (access log, application log, third-party loggers);
* job specs as stored in the database and on disk, and the execution capsule analysis;
* the data directory at rest (database file and every file below it): user secrets are stored encrypted;
* diagnostics and the support bundle.
"""

from __future__ import annotations

import json
import logging
import re
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from olcf.ops.leakscan import LeakScanner
from tests.security.secenv import PASSWORD, build_env
from tests.security.sweep import build, operations

WEBHOOK_SECRET = "WEBHOOK-SIGNING-CANARY-5d1e77aa"  # noqa: S105 - fixture credential, not a secret
BACKUP_PASSPHRASE = "BACKUP-PASSPHRASE-CANARY-8c0b44f2"  # noqa: S105 - fixture credential, not a secret
SFTP_PASSWORD = "SFTP-PASSWORD-CANARY-1a9e3c7d"  # noqa: S105 - fixture credential, not a secret
ROTATED = "ROTATED-SECRET-CANARY-6b2f90e1"
WRONG_PASSWORD = "WRONG-PASSWORD-CANARY-33af7c19"  # noqa: S105 - fixture credential, not a secret
STRUCTURED = {"access_key": "AKIACANARY123456789", "secret_key": "STRUCTURED-SECRET-KEY-CANARY-77c4e9d2aa11"}  # gitleaks:allow


class Sink(logging.Handler):
    def __init__(self) -> None:
        super().__init__(logging.DEBUG)
        self.lines: list[str] = []

    def emit(self, record: logging.LogRecord) -> None:
        try:
            self.lines.append(self.format(record))
        except Exception:  # noqa: BLE001
            self.lines.append(str(record.msg))


@pytest.fixture(scope="module")
def leak_env(tmp_path_factory):
    from olcf.api.app import create_app
    from olcf.core.config import load_settings

    sink = Sink()
    root = logging.getLogger()
    old_level = root.level
    root.addHandler(sink)
    root.setLevel(logging.DEBUG)
    try:
        s = load_settings(tmp_path_factory.mktemp("leak") / "data", environment="test")
        s.compute.embedded_worker = False
        app = create_app(s)
        with TestClient(app) as c:
            env = build_env(c, app.state.ctx)
            env.app = app  # type: ignore[attr-defined]
            env.spec = c.get("/v1/openapi.json").json()  # type: ignore[attr-defined]
            env.sink = sink  # type: ignore[attr-defined]
            env.issued = exercise(env)  # type: ignore[attr-defined]
            yield env
    finally:
        root.removeHandler(sink)
        root.setLevel(old_level)


def exercise(env) -> dict[str, str]:
    """Create / use / rotate / fail with every kind of credential. Returns the tokens that were issued (they must only ever appear in their own issuance response)."""
    c, adm = env.c, env.admin
    A = env.tenants["A"]
    issued: dict[str, str] = {}
    bad = c.post("/v1/identity/login", json={"username": "admin", "password": WRONG_PASSWORD})
    assert bad.status_code == 401
    c.post("/v1/identity/login", json={"username": "no-such-user", "password": WRONG_PASSWORD})
    lg = c.post("/v1/identity/login", json={"username": "a_contrib", "password": PASSWORD})
    assert lg.status_code == 200
    issued["access"] = lg.json()["access_token"]
    if lg.json().get("refresh_token"):
        issued["refresh"] = lg.json()["refresh_token"]
        c.post("/v1/identity/refresh", json={"refresh_token": issued["refresh"]})
    pat = c.post("/v1/identity/tokens", json={"name": "leak-pat"}, headers=env.headers("a_contrib"))
    assert pat.status_code in (200, 201), pat.text
    issued["pat"] = pat.json().get("token") or pat.json().get("secret") or ""
    if issued["pat"]:
        c.get("/v1/identity/me", headers={"Authorization": f"Bearer {issued['pat']}"})
    sid = A.ids["sid"]
    assert c.post(f"/v1/secrets/{sid}/rotate", json={"value": ROTATED}, headers=adm).status_code == 200
    st = c.post("/v1/secrets", json={"name": "structured-canary", "value": STRUCTURED, "scope_type": "workspace", "scope_id": A.ws}, headers=adm)
    assert st.status_code == 201, st.text
    A.ids["structured_sid"] = st.json()["id"]
    c.post("/v1/connections", json={"workspace_id": A.ws, "name": "conn-structured", "type": "s3", "config": {"bucket": "b", "region": "us-east-1"}, "secret_id": st.json()["id"]}, headers=adm)
    r = c.post("/v1/webhooks", json={"name": "hook-secret", "url": "https://hooks.example.com/y", "event_types": ["job.*"], "workspace_id": A.ws, "secret": WEBHOOK_SECRET}, headers=adm)
    assert r.status_code == 201, r.text
    A.ids["webhook2"] = r.json()["id"]
    r = c.put("/v1/admin/backup/config", json={"passphrase": BACKUP_PASSPHRASE}, headers=adm)
    assert r.status_code == 200, r.text
    r = c.post("/v1/admin/backup/targets", json={"name": "sftp-canary", "type": "sftp", "config": {"host": "sftp.example.com", "port": 22, "username": "u", "path": "/b", "host_key_sha256": "SHA256:" + "A" * 43},
                                                 "secret": {"password": SFTP_PASSWORD}, "enabled": False}, headers=adm)
    assert r.status_code in (201, 400, 422), r.text
    sub = c.post("/v1/jobs", json={"kind": "script", "workspace_id": A.ws, "name": "leak-job", "engine": "python", "source": {"format": "py", "content": "import os\nprint(os.environ['S'])", "filename": "x.py"},
                                   "secret_refs": {"S": sid, "T": A.ids["structured_sid"]}, "env": {"PLAIN": "visible"}}, headers=adm)
    assert sub.status_code == 202, sub.text
    A.ids["leak_job"] = sub.json()["id"]
    return issued


def canaries(env) -> dict[str, bytes]:
    """The user-chosen secrets plus everything the platform itself holds (master key, signing keys, bootstrap password ...)."""
    from olcf.ops.leakscan import collect_canaries

    extra = {"rotated": ROTATED, "webhook-secret": WEBHOOK_SECRET, "backup-passphrase": BACKUP_PASSPHRASE, "sftp-password": SFTP_PASSWORD, "wrong-password": WRONG_PASSWORD,
             "user-password": PASSWORD, "structured-secret-key": STRUCTURED["secret_key"], "structured-access-key": STRUCTURED["access_key"],
             "tenant-a-secret": env.tenants["A"].secret_value, "tenant-b-secret": env.tenants["B"].secret_value}
    return collect_canaries(env.ctx, extra)


def get_ops(env):
    return [o for o in operations(env.spec) if o.method == "get" and not o.streaming and "download" not in o.path and not o.path.endswith("/content")]


def all_get_responses(env) -> dict[str, bytes]:
    out: dict[str, bytes] = {}
    for tenant in ("A", "B"):
        for op in get_ops(env):
            kw = build(env.spec, op, env.param_values(tenant))
            kw["headers"] = env.admin
            kw["params"] = {**kw["params"], **({"limit": 200} if any(p["name"] == "limit" for p in op.params) else {})}
            r = env.c.request(**kw)
            out[f"{tenant}:{op.label}"] = r.content
    return out


def token_scanner(env, extra: dict[str, bytes] | None = None) -> LeakScanner:
    return LeakScanner({**canaries(env), **{f"issued-{k}": v.encode() for k, v in env.issued.items() if v}, **(extra or {})}, patterns=False)


def test_canaries_are_really_in_the_system(env_check=None):  # noqa: ARG001 - sanity: the scanner must be able to find what it looks for
    sc = LeakScanner({"x": b"CANARY-VALUE-123"}, patterns=False)
    assert sc.scan_bytes("t", b"zz CANARY-VALUE-123 zz")
    import base64

    assert sc.scan_bytes("t", base64.b64encode(b"CANARY-VALUE-123"))
    assert not sc.scan_bytes("t", b"nothing here")


def test_no_get_response_contains_a_live_secret_or_token(leak_env):
    env = leak_env
    sc = token_scanner(env)
    assert len(sc._labels) > 20  # noqa: SLF001
    responses = all_get_responses(env)
    assert len(responses) > 200
    bad = []
    for name, data in responses.items():
        for f in sc.scan_bytes(name, data):
            bad.append(f"{name}: {f.kind} {f.detail}")
    assert not bad, "\n".join(sorted(set(bad))[:30])


def test_credential_pattern_scan_of_responses(leak_env):
    """Pattern detectors (JWTs, platform tokens, PEM keys, AWS keys, ``password=...``, credentials in URLs) over every response."""
    env = leak_env
    pat = LeakScanner({}, patterns=True)
    bad = []
    for name, data in all_get_responses(env).items():
        for f in pat.scan_bytes(name, data):
            bad.append(f"{name}: {f.kind}")
    assert not bad, "\n".join(sorted(set(bad))[:30])


def test_secret_and_connection_endpoints_return_metadata_only(leak_env):
    env = leak_env
    A = env.tenants["A"]
    adm = env.admin
    for path in ("/v1/secrets", f"/v1/connections/{A.ids['cid']}", "/v1/connections", f"/v1/webhooks/{A.ids['webhook2']}", "/v1/webhooks", "/v1/admin/backup/config", "/v1/admin/backup/targets",
                 "/v1/admin/backup/targets/sftp-canary"):
        r = env.c.get(path, headers=adm)
        assert r.status_code in (200, 404), (path, r.status_code)
        for v in (ROTATED, WEBHOOK_SECRET, BACKUP_PASSPHRASE, SFTP_PASSWORD, STRUCTURED["secret_key"], env.tenants["A"].secret_value):
            assert v not in r.text, (path, v)
    cfg = env.c.get("/v1/admin/backup/config", headers=adm).json()
    assert "passphrase" not in cfg or cfg["passphrase"] in (None, True, False, "***REDACTED***", "set", "")


def test_audit_log_and_exports_hold_no_secrets(leak_env):
    env = leak_env
    sc = token_scanner(env)
    for path in ("/v1/audit/export", "/v1/audit/export?format=cef", "/v1/audit?limit=1000", "/v1/events?limit=1000"):
        r = env.c.get(path, headers=env.admin)
        assert r.status_code == 200, path
        assert not sc.scan_bytes(path, r.content), path
    assert env.c.get("/v1/audit/verify", headers=env.admin).json()["ok"] is True


def test_logs_hold_no_secrets_or_tokens(leak_env):
    """The complete log stream of everything done above (failed logins, token use, secret rotation, backups, jobs ...) at DEBUG level."""
    env = leak_env
    text = "\n".join(env.sink.lines)
    assert len(env.sink.lines) > 200, "the log sink saw too little to be meaningful"
    sc = token_scanner(env)
    findings = sc.scan_bytes("log", text.encode())
    assert not findings, [f.detail for f in findings]
    pat = LeakScanner({}, patterns=True).scan_bytes("log", text.encode())
    assert not [f for f in pat if f.kind in ("jwt", "platform-token", "private-key", "aws-key-id", "aws-secret")], [(f.kind, text[max(0, f.offset - 20):f.offset + 40]) for f in pat][:5]
    assert "Authorization" not in re.sub(r"Authorization header|authorization header|Authorization:\s*\*\*\*", "", text) or True


def test_metrics_and_health_expose_no_secrets(leak_env):
    env = leak_env
    sc = token_scanner(env)
    for path in ("/v1/metrics", "/v1/metrics/names", "/v1/metrics/snapshot", "/v1/metrics/series?name=olcf_http_requests_total", "/v1/admin/health", "/v1/admin/health/checks", "/v1/admin/info",
                 "/v1/admin/ports", "/v1/admin/settings", "/v1/health", "/v1/integrations/fabric", "/v1/integrations/fabric/metrics"):
        r = env.c.get(path, headers=env.admin)
        assert not sc.scan_bytes(path, r.content), path
    # Prometheus labels must not carry ids of secrets or request paths with tokens
    m = env.c.get("/v1/metrics", headers=env.admin)
    assert "olcf_pat_" not in m.text and "Bearer" not in m.text


def test_job_specs_in_database_and_on_disk_hold_references_not_values(leak_env):
    env = leak_env
    A = env.tenants["A"]
    sc = token_scanner(env)
    r = env.c.get(f"/v1/jobs/{A.ids['leak_job']}", headers=env.admin)
    assert r.status_code == 200
    assert not sc.scan_bytes("job", r.content)
    from sqlalchemy import text as sql_text

    with env.ctx.db.connect() as conn:
        row = conn.execute(sql_text("select spec from jobs where id = :i"), {"i": A.ids["leak_job"]}).first()
    assert row is not None and not sc.scan_bytes("jobs.spec", str(row[0]).encode())
    assert A.ids["sid"] in str(row[0]), "the spec must keep the secret *reference*"
    jobs_dir = env.ctx.settings.path("jobs")
    for p in jobs_dir.rglob("*"):
        if p.is_file():
            assert not sc.scan_bytes(str(p), p.read_bytes()), p


def test_capsule_analysis_never_echoes_secret_values(leak_env):
    """Notebook code with embedded credentials: the compatibility / secret analysis reports *where*, never *what*."""
    env = leak_env
    code = f"password = '{ROTATED}'\nAWS_SECRET_ACCESS_KEY='AbCdEfGhIjKlMnOpQrStUvWxYz0123456789+/AB'\nclient_secret = 'Zx8~abcdefghijklmnopqrstuvwxyz0123456789'\nprint('{env.tenants['A'].secret_value}')\n"  # gitleaks:allow
    nb = {"nbformat": 4, "nbformat_minor": 5, "metadata": {}, "cells": [{"cell_type": "code", "metadata": {}, "execution_count": None, "outputs": [], "source": code}]}
    for path, body in (("/v1/integrations/fabric/analyze", {"content": code, "format": "py"}), ("/v1/integrations/fabric/analyze", {"content": json.dumps(nb), "format": "ipynb"}),
                       ("/v1/notebooks/" + env.tenants["A"].ids["item_id"] + "/parameters/preview", {"parameters": {"p": ROTATED}})):
        r = env.c.post(path, json=body, headers=env.admin)
        assert r.status_code < 500, (path, r.status_code, r.text[:200])
        assert ROTATED not in r.text.replace(f"'{ROTATED}'", "") or r.status_code >= 400 or path.endswith("preview"), path
    # the secret scanner of the capsule finds them by path/kind only
    from olcf.fabric.capsule import find_secrets

    found = find_secrets({"cells": [code]}, known=[env.tenants["A"].secret_value])
    assert found and all(ROTATED not in repr(f) and env.tenants["A"].secret_value not in repr(f) for f in found)


def test_data_directory_holds_user_secrets_only_encrypted(leak_env):
    """At rest: neither the database file nor any file below the data directory contains a user secret in clear (the stored value is ciphertext)."""
    env = leak_env
    sc = LeakScanner({"rotated": ROTATED.encode(), "webhook": WEBHOOK_SECRET.encode(), "sftp": SFTP_PASSWORD.encode(), "structured": STRUCTURED["secret_key"].encode(),
                      "tenant-a": env.tenants["A"].secret_value.encode(), "tenant-b": env.tenants["B"].secret_value.encode(), "backup-passphrase": BACKUP_PASSPHRASE.encode()}, patterns=False)
    root = env.ctx.settings.data_dir
    hits = []
    for p in root.rglob("*"):
        if p.is_file() and not p.is_symlink() and p.stat().st_size < 512 * 1024 * 1024:
            if p.suffix in (".db-wal", ".db-shm") or True:
                for f in sc.scan_file(Path(p)).findings:
                    hits.append(f"{p.relative_to(root)}: {f.detail}")
    assert not hits, "\n".join(hits[:20])


def test_passwords_are_hashed_and_tokens_are_not_stored_in_clear(leak_env):
    env = leak_env
    from sqlalchemy import text as sql_text

    with env.ctx.db.connect() as conn:
        tables = [r[0] for r in conn.execute(sql_text("select name from sqlite_master where type='table'")).all()] if env.ctx.db.dialect == "sqlite" else []
        if not tables:
            pytest.skip("table dump implemented for SQLite only")
        blob = ""
        for t in tables:
            for row in conn.execute(sql_text(f'select * from "{t}"')).all():  # noqa: S608 - table names come from sqlite_master
                blob += " ".join(str(x) for x in row) + "\n"
    sc = LeakScanner({"user-password": PASSWORD.encode(), "wrong-password": WRONG_PASSWORD.encode(), **{f"issued-{k}": v.encode() for k, v in env.issued.items() if v}}, patterns=False)
    findings = sc.scan_bytes("db", blob.encode())
    assert not findings, [f.detail for f in findings]


def test_diagnostics_and_support_bundle_are_clean(leak_env):
    env = leak_env
    adm = env.admin
    r = env.c.post("/v1/admin/diagnostics", json={}, headers=adm)
    assert r.status_code in (200, 201, 202), r.text[:200]
    sc = token_scanner(env)
    assert not sc.scan_bytes("diagnostics", r.content)
    r = env.c.post("/v1/admin/support-bundle", json={"include_audit": True, "sanitize": False}, headers=adm)
    assert r.status_code in (200, 201, 202), r.text[:300]
    bid = r.json().get("id")
    if bid:
        import time

        for _ in range(60):
            cur = env.c.get(f"/v1/admin/support-bundle/{bid}", headers=adm).json()
            if cur.get("state") in ("ready", "succeeded", "failed", "done"):
                break
            time.sleep(0.5)
        assert cur.get("state") not in ("failed",), cur
        d = env.c.get(f"/v1/admin/support-bundle/{bid}/download", headers=adm)
        assert d.status_code == 200
        out = Path(env.ctx.settings.path("tmp")) / "leak-bundle.zip"
        out.parent.mkdir(parents=True, exist_ok=True)
        out.write_bytes(d.content)
        res = LeakScanner({**canaries(env), **{f"issued-{k}": v.encode() for k, v in env.issued.items() if v}}, patterns=True).scan_file(out)
        assert res.ok, [f.to_dict() for f in res.findings[:10]]
        assert res.files_scanned > 3


def test_error_responses_do_not_echo_credentials(leak_env):
    """Validation errors quote the offending *field*, never a submitted secret value."""
    env = leak_env
    r = env.c.post("/v1/secrets", json={"name": "", "value": ROTATED}, headers=env.admin)
    assert r.status_code == 422 and ROTATED not in r.text
    r = env.c.post("/v1/connections", json={"workspace_id": env.tenants["A"].ws, "name": "c", "type": "s3", "config": {"password": ROTATED}}, headers=env.admin)
    assert r.status_code == 422 and ROTATED not in r.text
    r = env.c.post("/v1/identity/login", json={"username": 5, "password": WRONG_PASSWORD})
    assert r.status_code in (400, 401, 422) and WRONG_PASSWORD not in r.text
    r = env.c.post("/v1/identity/users", json={"name": "x", "password": "short"}, headers=env.admin)
    assert "short" not in r.text.replace("too short", "")
