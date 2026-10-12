# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""FIX regressions D and M: forced password change is enforced by the server, and logout / refresh-token reuse really end a session.

* An account created (or reset) with ``must_change_password`` can sign in, but its session may call ``POST /v1/identity/password`` and nothing else - not even
  ``GET /identity/me`` - until the password was changed; the same token then works normally.
* ``POST /identity/logout`` revokes the refresh token **and** the access tokens of the session (table ``revoked_tokens``, honoured by every node that shares the
  database) and is audited as ``auth.logout``.
* A refresh token that was already rotated and is presented again (outside a small race window) ends the whole login session (token family) and is audited.
"""

from __future__ import annotations

import pytest
from sqlalchemy import select

from olcf.db import schema as S
from olcf.security.tokens import SigningKeys, TokenService
from tests.security.secenv import PASSWORD as SWEEP_PASSWORD
from tests.security.sweep import build, operations
from tests.security.test_unauthenticated import PUBLIC, SELF_AUTH

PW = "correct-horse-battery-1"
NEW_PW = "another-long-passphrase-2"


def _login(client, name: str, password: str = PW):
    r = client.post("/v1/identity/login", json={"username": name, "password": password})
    assert r.status_code == 200, r.text
    return r.json()


def _bearer(tokens: dict) -> dict[str, str]:
    return {"Authorization": f"Bearer {tokens['access_token']}"}


def _create(client, name: str, **extra):
    r = client.post("/v1/identity/users", headers=client.admin, json={"name": name, "password": PW, "roles": [{"role": "Viewer", "scope_type": "org"}], **extra})
    assert r.status_code == 201, r.text
    return r.json()


def _audit_actions(client, action: str) -> list[dict]:
    return client.get("/v1/audit", headers=client.admin, params={"action": action}).json()["items"]


# ===================================================================== D: must_change_password
def test_user_create_accepts_must_change_password_and_the_login_session_is_restricted(client):
    user = _create(client, "fresh", must_change_password=True)
    assert user["must_change_password"] is True
    tokens = _login(client, "fresh")
    assert tokens["must_change_password"] is True
    h = _bearer(tokens)
    # nothing but the change-password endpoint works
    for method, path in (("get", "/v1/identity/me"), ("get", "/v1/workspaces"), ("get", "/v1/identity/users"), ("get", "/v1/admin/health"), ("post", "/v1/workspaces"),
                         ("get", "/v1/events")):
        r = getattr(client, method)(path, headers=h)
        assert r.status_code == 403 and r.json()["error"]["code"] == "password_change_required", (path, r.text)
    # the refresh token does not escape the restriction either
    refreshed = client.post("/v1/identity/refresh", json={"refresh_token": tokens["refresh_token"]}).json()
    assert client.get("/v1/identity/me", headers=_bearer(refreshed)).status_code == 403
    # a wrong current password and an unchanged password are refused
    assert client.post("/v1/identity/password", headers=h, json={"current_password": "nope", "new_password": NEW_PW}).status_code == 403
    r = client.post("/v1/identity/password", headers=h, json={"current_password": PW, "new_password": PW})
    assert r.status_code == 400 and "differ" in r.json()["error"]["message"]
    assert client.get("/v1/identity/me", headers=h).status_code == 403
    # the change works with the restricted token, and the very same token is then a normal session
    assert client.post("/v1/identity/password", headers=h, json={"current_password": PW, "new_password": NEW_PW}).status_code == 204
    me = client.get("/v1/identity/me", headers=h)
    assert me.status_code == 200 and me.json()["principal"]["must_change_password"] is False
    assert client.get("/v1/workspaces", headers=h).status_code == 200
    assert _login(client, "fresh", NEW_PW)["must_change_password"] is False
    assert client.post("/v1/identity/login", json={"username": "fresh", "password": PW}).status_code == 401


def test_default_is_unchanged_and_service_accounts_cannot_be_forced(client):
    assert _create(client, "plain")["must_change_password"] is False
    assert client.get("/v1/identity/me", headers=_bearer(_login(client, "plain"))).status_code == 200
    r = client.post("/v1/identity/users", headers=client.admin, json={"name": "svc", "kind": "service", "must_change_password": True})
    assert r.status_code == 400 and "must_change_password" in r.json()["error"]["message"]


def test_admin_reset_forces_a_change_for_a_session_that_is_already_open(client):
    _create(client, "victim")
    tokens = _login(client, "victim")
    h = _bearer(tokens)
    assert client.get("/v1/identity/me", headers=h).status_code == 200
    uid = client.get("/v1/identity/me", headers=h).json()["principal"]["id"]
    temp = client.post(f"/v1/identity/users/{uid}/reset-password", headers=client.admin).json()["temporary_password"]
    # the already issued token was minted before the flag existed - the server decides from the account's current state
    r = client.get("/v1/workspaces", headers=h)
    assert r.status_code == 403 and r.json()["error"]["code"] == "password_change_required"
    t2 = _login(client, "victim", temp)
    assert t2["must_change_password"] is True
    assert client.post("/v1/identity/password", headers=_bearer(t2), json={"current_password": temp, "new_password": NEW_PW}).status_code == 204


def test_openapi_documents_the_field(client):
    spec = client.get("/v1/openapi.json").json()
    assert "must_change_password" in spec["components"]["schemas"]["UserCreate"]["properties"]


# ===================================================================== M: logout / revocation / reuse
def test_logout_revokes_access_and_refresh_tokens_and_is_audited(client):
    _create(client, "leaver")
    tokens = _login(client, "leaver")
    h = _bearer(tokens)
    assert client.get("/v1/identity/me", headers=h).status_code == 200
    older = _bearer(_login(client, "leaver"))  # another session of the same user is untouched
    r = client.post("/v1/identity/logout", headers=h, json={"refresh_token": tokens["refresh_token"]})
    assert r.status_code == 204
    r = client.get("/v1/identity/me", headers=h)
    assert r.status_code == 401 and "revoked" in r.json()["error"]["message"]
    assert client.post("/v1/identity/refresh", json={"refresh_token": tokens["refresh_token"]}).status_code == 401
    assert client.get("/v1/identity/me", headers=older).status_code == 200
    (ev,) = [e for e in _audit_actions(client, "auth.logout") if e["actor_name"] == "leaver"]
    assert ev["outcome"] == "success" and ev["resource_type"] == "principal"
    # the old body-only call still works (refresh token only) and an anonymous logout with garbage is a quiet no-op
    t2 = _login(client, "leaver")
    assert client.post("/v1/identity/logout", json={"refresh_token": t2["refresh_token"]}).status_code == 204
    assert client.get("/v1/identity/me", headers=_bearer(t2)).status_code == 401  # the whole session (family) ended, not only the refresh token
    assert client.post("/v1/identity/logout", json={"refresh_token": "olcf_ref_garbage_x"}).status_code == 204
    assert client.post("/v1/identity/logout").status_code == 204
    assert len([e for e in _audit_actions(client, "auth.logout") if e["actor_name"] == "leaver"]) == 2


def test_logout_ends_every_access_token_of_the_session(client):
    _create(client, "multi")
    t = _login(client, "multi")
    t_next = client.post("/v1/identity/refresh", json={"refresh_token": t["refresh_token"]}).json()
    assert client.get("/v1/identity/me", headers=_bearer(t)).status_code == 200  # the first access token is still valid after a refresh
    assert client.post("/v1/identity/logout", headers=_bearer(t_next), json={"refresh_token": t_next["refresh_token"]}).status_code == 204
    assert client.get("/v1/identity/me", headers=_bearer(t)).status_code == 401
    assert client.get("/v1/identity/me", headers=_bearer(t_next)).status_code == 401


def test_revocation_is_honoured_by_another_node_sharing_the_database(client):
    _create(client, "twonodes")
    t = _login(client, "twonodes")
    other = TokenService(client.ctx.db, SigningKeys(client.ctx.settings.path("secrets")))  # a second control-plane process: same database, same signing key
    other.REVOCATION_CACHE_SECONDS = 0.0
    assert other.verify_access_token(t["access_token"])["sub"]
    assert client.post("/v1/identity/logout", headers=_bearer(t), json={"refresh_token": t["refresh_token"]}).status_code == 204
    from olcf.core.errors import Unauthenticated

    with pytest.raises(Unauthenticated, match="revoked"):
        other.verify_access_token(t["access_token"])
    with client.ctx.db.connect() as conn:
        rows = conn.execute(select(S.revoked_tokens)).all()
    assert rows and all(r.expires_at for r in rows)  # purgeable: every row carries the expiry of the token it blocks


def test_refresh_token_reuse_revokes_the_whole_family(client, monkeypatch):
    _create(client, "stolen")
    t0 = _login(client, "stolen")
    t1 = client.post("/v1/identity/refresh", json={"refresh_token": t0["refresh_token"]}).json()
    # replay inside the grace window (two tabs refreshing at once): refused, but the session lives on
    assert client.post("/v1/identity/refresh", json={"refresh_token": t0["refresh_token"]}).status_code == 401
    assert client.get("/v1/identity/me", headers=_bearer(t1)).status_code == 200
    assert not _audit_actions(client, "auth.refresh_reuse_detected")
    # replay after the grace window: somebody kept a copy -> the family is dead, including the legitimate descendants
    monkeypatch.setattr(client.ctx.tokens, "REUSE_GRACE_SECONDS", -1.0)
    assert client.post("/v1/identity/refresh", json={"refresh_token": t0["refresh_token"]}).status_code == 401
    assert client.post("/v1/identity/refresh", json={"refresh_token": t1["refresh_token"]}).status_code == 401
    assert client.get("/v1/identity/me", headers=_bearer(t1)).status_code == 401
    assert client.get("/v1/identity/me", headers=_bearer(t0)).status_code == 401
    (ev,) = _audit_actions(client, "auth.refresh_reuse_detected")
    assert ev["outcome"] == "failure" and ev["details"]["refresh_revoked"] == 1
    # other sessions of the user are not affected, and signing in again works
    t_other = _login(client, "stolen")
    assert client.get("/v1/identity/me", headers=_bearer(t_other)).status_code == 200


def test_retention_purges_revocations_after_the_tokens_expired(make_app):
    from fastapi.testclient import TestClient

    app = make_app(["olcf.api.core.module", "olcf.ops.module"])
    with TestClient(app):
        ctx = app.state.ctx
        ctx.tokens.revoke_access("expired-jti", principal_id="usr_x", expires_at=1.0)  # long expired
        ctx.tokens.revoke_access("live-jti", principal_id="usr_x")
        res = ctx.service("retention").run(None, only=["tokens"])
        assert res["steps"]["tokens"]["revoked_access_tokens"]["deleted"] == 1
        with ctx.db.connect() as conn:
            left = {r[0] for r in conn.execute(select(S.revoked_tokens.c.jti)).all()}
        assert left == {"live-jti"}


def test_the_revoked_tokens_migration_is_registered(client):
    from olcf.db.migrate import current_versions

    assert current_versions(client.ctx.db)["core"] >= 2


# ===================================================================== R: the sweeps of the full application include the session features
def _sweep_ops(env):
    """Every operation of the full application's OpenAPI document that needs a bearer token (the same set as the unauthenticated sweep)."""
    return [o for o in operations(env.spec) if o.key not in PUBLIC and o.key not in SELF_AUTH and not o.streaming]


def _admin_user(env, name: str, **extra):
    body = {"name": name, "password": SWEEP_PASSWORD, "roles": [{"role": "PlatformAdmin", "scope_type": "org", "scope_id": ""}], **extra}
    r = env.c.post("/v1/identity/users", json=body, headers=env.admin)
    assert r.status_code == 201, r.text
    login = env.c.post("/v1/identity/login", json={"username": name, "password": SWEEP_PASSWORD})
    assert login.status_code == 200, login.text
    return login.json()


def test_the_sweeps_cover_the_session_and_new_endpoints(two_tenants):
    keys = {o.key for o in operations(two_tenants.spec)}
    assert {("POST", "/v1/identity/logout"), ("POST", "/v1/identity/password"), ("GET", "/v1/workspaces/{ws}/pipelines"), ("GET", "/v1/admin/diagnostics")} <= keys
    assert ("POST", "/v1/identity/logout") in PUBLIC  # takes the refresh token in the body or the bearer token; never needs a valid session
    assert len(_sweep_ops(two_tenants)) > 300


def test_a_session_that_must_change_its_password_is_refused_on_every_route(two_tenants):
    """A PlatformAdmin who must change the password can do nothing but change it: every other operation answers 403 password_change_required."""
    env = two_tenants
    tokens = _admin_user(env, "sweep_forced", must_change_password=True)
    h = _bearer(tokens)
    params = env.param_values("A")
    bad, n = [], 0
    for o in _sweep_ops(env):
        if o.key == ("POST", "/v1/identity/password"):
            continue
        r = env.c.request(**{**build(env.spec, o, params), "headers": h})
        n += 1
        if r.status_code != 403 or r.json().get("error", {}).get("code") != "password_change_required":
            bad.append(f"{o.label} -> {r.status_code} {r.text[:80]}")
    assert n > 300 and not bad, "routes usable before the password was changed:\n  " + "\n  ".join(bad[:30])
    # the one allowed operation reaches its handler (wrong current password -> 403 from the handler, not from the gate)
    r = env.c.post("/v1/identity/password", headers=h, json={"current_password": "wrong-password-xx", "new_password": NEW_PW})
    assert r.status_code == 403 and r.json()["error"]["code"] != "password_change_required"
    assert env.c.post("/v1/identity/password", headers=h, json={"current_password": SWEEP_PASSWORD, "new_password": NEW_PW}).status_code == 204
    assert env.c.get("/v1/identity/me", headers=h).status_code == 200


def test_a_logged_out_session_is_refused_on_every_route(two_tenants):
    env = two_tenants
    tokens = _admin_user(env, "sweep_leaver")
    h = _bearer(tokens)
    assert env.c.get("/v1/identity/me", headers=h).status_code == 200
    assert env.c.post("/v1/identity/logout", headers=h, json={"refresh_token": tokens["refresh_token"]}).status_code == 204
    params = env.param_values("A")
    bad, n = [], 0
    for o in _sweep_ops(env):
        r = env.c.request(**{**build(env.spec, o, params), "headers": h})
        n += 1
        if r.status_code != 401:
            bad.append(f"{o.label} -> {r.status_code}")
    assert n > 300 and not bad, "routes usable after logout:\n  " + "\n  ".join(bad[:30])
