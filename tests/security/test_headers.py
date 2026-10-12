# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Foundation review: response headers, CORS and redaction/audit-export hardening.

Single-valued security headers must never be duplicated (proxies/browsers treat duplicates as ambiguous), error responses carry them too,
cross-origin requests from unlisted origins get no CORS grant, and untrusted correlation ids are not reflected.
"""

from __future__ import annotations

import pytest

SINGLE = ("cache-control", "x-content-type-options", "referrer-policy", "cross-origin-opener-policy", "x-correlation-id",
          "content-security-policy", "x-frame-options", "strict-transport-security")


@pytest.fixture(scope="module")
def env(two_tenants):
    return two_tenants


def _dupes(r) -> list[str]:
    names = [k.decode().lower() for k, _ in r.headers.raw]
    return [n for n in SINGLE if names.count(n) > 1]


def test_no_duplicate_security_headers_on_any_kind_of_response(env):
    a = env.headers("a_admin")
    responses = [
        env.c.get("/v1/health/ready"),
        env.c.get("/v1/workspaces", headers=a),
        env.c.get("/v1/workspaces"),  # 401
        env.c.get("/v1/does-not-exist", headers=a),  # 404
        env.c.post("/v1/workspaces", content=b"{not json", headers={**a, "content-type": "application/json"}),  # 422
        env.c.get("/v1/openapi.json"),
        env.c.get("/"),
    ]
    for r in responses:
        assert not _dupes(r), (r.request.url.path, r.status_code, _dupes(r))
        assert r.headers.get("x-content-type-options") == "nosniff"
        assert r.headers.get("x-correlation-id")


def test_api_responses_are_not_cacheable(env):
    r = env.c.get("/v1/workspaces", headers=env.headers("a_admin"))
    assert r.headers.get("cache-control") == "no-store"


def test_hostile_correlation_ids_are_replaced(env):
    for cid in ("x", "a" * 200, "bad id with spaces", "<script>alert(1)</script>", "a;b;c;d;e;f;g;h"):
        r = env.c.get("/v1/health/ready", headers={"X-Correlation-Id": cid})
        got = r.headers["x-correlation-id"]
        assert got != cid and len(got) <= 64 and got.isalnum()
    ok = env.c.get("/v1/health/ready", headers={"X-Correlation-Id": "trace-1234-abcd"})
    assert ok.headers["x-correlation-id"] == "trace-1234-abcd"


def test_no_cors_grant_by_default(env):
    """No ``server.cors_origins`` configured: neither simple nor preflight cross-origin requests are granted."""
    for origin in ("https://evil.example", "null", "http://localhost:3000"):
        r = env.c.get("/v1/health/ready", headers={"Origin": origin})
        assert "access-control-allow-origin" not in r.headers
        pre = env.c.options("/v1/workspaces", headers={"Origin": origin, "Access-Control-Request-Method": "GET",
                                                            "Access-Control-Request-Headers": "authorization"})
        assert "access-control-allow-origin" not in pre.headers and "access-control-allow-credentials" not in pre.headers


def test_cors_with_allowlist_grants_only_listed_origins(tmp_path):
    from fastapi.testclient import TestClient

    from olcf.api.app import create_app
    from olcf.core.config import load_settings

    s = load_settings(tmp_path / "data", environment="test")
    s.compute.embedded_worker = False
    s.server.cors_origins = ["https://ui.example"]
    with TestClient(create_app(s)) as c:
        good = c.options("/v1/workspaces", headers={"Origin": "https://ui.example", "Access-Control-Request-Method": "GET"})
        assert good.headers.get("access-control-allow-origin") == "https://ui.example"
        assert "access-control-allow-credentials" not in good.headers
        bad = c.options("/v1/workspaces", headers={"Origin": "https://evil.example", "Access-Control-Request-Method": "GET"})
        assert "access-control-allow-origin" not in bad.headers


def test_redaction_covers_every_platform_token_prefix():
    from olcf.core.redaction import redact_text

    for prefix in ("pat", "wrk", "enr", "hook"):
        tok = f"olcf_{prefix}_" + "Ab3_-" * 8
        assert tok not in redact_text(f"calling https://x/hooks/{tok}?x=1 failed")


def test_cef_export_cannot_forge_records():
    from olcf.security.audit import AuditLog

    rec = {"ts": "t", "actor_name": "evil\nCEF:0|forged|x|1|a|a|10|", "action": "a|b", "outcome": "success", "resource_type": "r",
           "resource_id": "i\r\nx", "workspace_id": None, "ip": None}
    line = AuditLog.to_cef(rec)
    assert "\n" not in line and "\r" not in line
    assert line.count("CEF:0|") == 1
