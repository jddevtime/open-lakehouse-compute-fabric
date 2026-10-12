# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Spec 35.4: every ``/v1`` route answers 401 without valid credentials, except an explicit, justified public allow-list."""

from __future__ import annotations

import base64
import json
import time

import jwt

from tests.security.sweep import build, operations

#: operation -> why it is reachable without a bearer token. Anything else that answers non-401 is a finding.
PUBLIC: dict[tuple[str, str], str] = {
    ("POST", "/v1/identity/login"): "credential exchange (rate limited, lockout)",
    ("POST", "/v1/identity/refresh"): "the refresh token is the credential",
    ("POST", "/v1/identity/logout"): "the refresh token (body) or the bearer token identifies the session to end; revokes it, never grants anything",
    ("GET", "/v1/health"): "liveness probe (no detail)",
    ("GET", "/v1/health/ready"): "readiness probe",
    ("GET", "/v1/identity/oidc/providers"): "login page needs the provider list (names only)",
    ("GET", "/v1/identity/oidc/{provider}/login"): "starts the browser SSO flow",
    ("GET", "/v1/identity/oidc/{provider}/callback"): "IdP redirect target (state/PKCE bound)",
    ("POST", "/v1/identity/oidc/exchange"): "one-time login code exchange",
    ("POST", "/v1/identity/ldap/{provider}/login"): "directory credential login",
    ("POST", "/v1/workers/enroll"): "single-use enrollment token in the body is the credential",
    ("POST", "/v1/hooks/pipelines/{trigger_id}"): "webhook trigger authenticated by its own secret/signature",
    ("POST", "/v1/hooks/pipelines/callbacks/{activity_run_id}/{token}"): "callback authenticated by the token in the URL",
}
#: operations outside the /v1 prefix with their own authentication (tested separately below)
SELF_AUTH = {("GET", "/mcp"), ("POST", "/mcp"), ("DELETE", "/mcp")}


def ops_under_test(env):
    return [o for o in operations(env.spec) if o.key not in PUBLIC and o.key not in SELF_AUTH]


def test_public_allow_list_has_no_stale_entries(two_tenants):
    present = {o.key for o in operations(two_tenants.spec)}
    assert set(PUBLIC) <= present, f"allow-list names routes that do not exist: {set(PUBLIC) - present}"


def test_every_route_requires_authentication(two_tenants):
    env = two_tenants
    bad = []
    params = env.param_values("A")
    for o in ops_under_test(env):
        kw = build(env.spec, o, params)
        r = env.c.request(**kw)
        if r.status_code != 401:
            bad.append(f"{o.label} -> {r.status_code}")
    assert not bad, "routes reachable without credentials:\n  " + "\n  ".join(bad)


def _forged_tokens(env) -> dict[str, str]:
    now = int(time.time())
    claims = {"iss": "olcf", "aud": "olcf-api", "sub": env.actor("admin").id, "name": "admin", "kind": "user", "iat": now, "exp": now + 600, "jti": "x", "scp": ["*"]}
    b64 = lambda d: base64.urlsafe_b64encode(json.dumps(d).encode()).rstrip(b"=").decode()  # noqa: E731
    return {
        "garbage": "not-a-token",
        "empty": "",
        "alg_none": f"{b64({'alg': 'none', 'typ': 'JWT'})}.{b64(claims)}.",
        "wrong_key_hs256": jwt.encode(claims, "attacker-chosen-secret-0123456789abcdef", algorithm="HS256"),
        "expired": jwt.encode({**claims, "exp": now - 60}, "attacker-chosen-secret-0123456789abcdef", algorithm="HS256"),
        "platform_prefix_unknown": "olcf_pat_" + "A" * 40,
        "worker_prefix_unknown": "olcf_wrk_" + "A" * 40,
        "sql_injection": "' OR '1'='1",
    }


def test_forged_and_malformed_credentials_are_rejected_on_every_route(two_tenants):
    env = two_tenants
    params = env.param_values("A")
    bad = []
    for name, tok in _forged_tokens(env).items():
        for o in ops_under_test(env):
            kw = build(env.spec, o, params)
            r = env.c.request(headers={"Authorization": f"Bearer {tok}"}, **kw)
            if r.status_code != 401:
                bad.append(f"{name}: {o.label} -> {r.status_code}")
    assert not bad, "\n  ".join(bad[:40])


def test_other_credential_transports_are_not_accepted(two_tenants):
    env = two_tenants
    tok = env.admin["Authorization"].split()[1]
    for label, kw in (("basic", {"headers": {"Authorization": "Basic " + base64.b64encode(b"admin:x").decode()}}),
                      ("token in query", {"params": {"access_token": tok, "token": tok}}),
                      ("token in cookie", {"cookies": {"access_token": tok, "olcf_token": tok, "token": tok}}),
                      ("lowercase scheme garbage", {"headers": {"Authorization": "bearer"}}),
                      ("two tokens", {"headers": {"Authorization": f"Bearer {tok} {tok}"}}),
                      ("x-forwarded identity", {"headers": {"X-Forwarded-User": "admin", "X-Remote-User": "admin", "X-User": "admin", "X-Auth-Request-User": "admin"}})):
        r = env.c.get("/v1/identity/me", **kw)
        assert r.status_code == 401, (label, r.status_code, r.text[:150])


def test_deleted_disabled_and_revoked_identities_lose_access(two_tenants):
    env = two_tenants
    u = env.make_user("short_lived", "Contributor", ("workspace", env.tenants["A"].ws))
    assert env.c.get("/v1/identity/me", headers=u.headers).status_code == 200
    assert env.c.patch(f"/v1/identity/users/{u.id}", json={"is_active": False}, headers=env.admin).status_code == 200
    assert env.c.get("/v1/identity/me", headers=u.headers).status_code == 401  # a still-valid access token of a disabled account is dead at once
    assert env.c.post("/v1/identity/login", json={"username": "short_lived", "password": "Correct-Horse-9-battery"}).status_code in (401, 403)
    assert env.c.patch(f"/v1/identity/users/{u.id}", json={"is_active": True}, headers=env.admin).status_code == 200
    # personal access token: usable, then revoked, then dead
    r = env.c.post("/v1/identity/tokens", json={"name": "t", "scopes": ["*"]}, headers=u.headers)
    assert r.status_code == 201, r.text
    pat = {"Authorization": f"Bearer {r.json()['token']}"}
    assert env.c.get("/v1/identity/me", headers=pat).status_code == 200
    assert env.c.delete(f"/v1/identity/tokens/{r.json()['id']}", headers=u.headers).status_code in (200, 204)
    assert env.c.get("/v1/identity/me", headers=pat).status_code == 401


def test_mcp_endpoint_enforces_its_own_authentication(two_tenants):
    env = two_tenants
    body = {"jsonrpc": "2.0", "id": 1, "method": "tools/list"}
    local = {"Host": "localhost:8470"}
    for hdrs in ({}, {"Authorization": "Bearer nope"}, {"Authorization": "Bearer " + "A" * 40}):
        r = env.c.post("/mcp", json=body, headers={**local, **hdrs})
        assert r.status_code == 401, (hdrs, r.status_code, r.text[:200])
        assert "tools" not in r.text
    for m in ("get", "delete"):
        r = getattr(env.c, m)("/mcp", headers=local)
        assert r.status_code in (401, 405)
    init = {"jsonrpc": "2.0", "id": 1, "method": "initialize", "params": {"protocolVersion": "2025-03-26", "capabilities": {}, "clientInfo": {"name": "sectest", "version": "1"}}}
    r = env.c.post("/mcp", json=init, headers={**local, **env.admin})
    assert r.status_code == 200 and "serverInfo" in r.text, r.text[:200]
    # DNS rebinding and cross-site: a valid token does not help when the Host / Origin is foreign
    r = env.c.post("/mcp", json=init, headers={"Host": "attacker.example", **env.admin})
    assert r.status_code == 403 and "serverInfo" not in r.text
    r = env.c.post("/mcp", json=init, headers={**local, "Origin": "https://attacker.example", **env.admin})
    assert r.status_code == 403
    r = env.c.post("/mcp", json=init, headers={**local, "Origin": "null", **env.admin})
    assert r.status_code == 403


def test_websocket_endpoints_require_credentials(two_tenants):
    env = two_tenants
    from starlette.websockets import WebSocketDisconnect

    sid = "ses_x"
    for path in ("/v1/sessions/" + sid + "/ws", "/v1/workers/ws"):
        for hdrs in ({}, {"Authorization": "Bearer nope"}, {"Authorization": "Bearer olcf_wrk_" + "A" * 40}):
            try:
                with env.c.websocket_connect(path, headers=hdrs) as ws:
                    msg = ws.receive()
                    # accepted-then-closed is fine as long as no data and the close code is an auth/policy one
                    assert msg["type"] == "websocket.close" and msg.get("code") in (1008, 4401, 4403, 4404, 4400), (path, hdrs, msg)
            except WebSocketDisconnect as exc:
                assert exc.code in (1008, 4401, 4403, 4404, 4400, 1000, 1006), (path, exc.code)


def test_openapi_and_docs_exposure_is_documented(two_tenants):
    """The API description is public by design (it contains no data); make sure it does not leak instance details."""
    env = two_tenants
    txt = json.dumps(env.spec)
    for needle in (env.ctx.settings.data_dir.as_posix(), "bootstrap-admin", env.tenants["A"].canary, env.tenants["B"].canary):
        assert needle not in txt
