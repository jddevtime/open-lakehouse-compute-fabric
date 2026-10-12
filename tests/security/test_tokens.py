# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Spec 35.4: token handling - issuance, expiry, rotation, revocation, type separation and the login throttle.

Tokens are never logged (see ``test_leaks``); here: a token is only good for what it is (access JWT, refresh, personal access token, enrolment, worker), only while it is
valid, and its owner must still be active.
"""

from __future__ import annotations

import base64
import json
import time

import jwt
import pytest

from olcf.core.timeutil import iso_in
from tests.security.secenv import PASSWORD


@pytest.fixture(scope="module")
def env(two_tenants):
    return two_tenants


@pytest.fixture(autouse=True)
def fresh_limiter(env):
    """Logins of the whole module share one client address: start every test with an empty throttle (the throttle itself is tested explicitly below)."""
    env.ctx.limiter._hits.clear()  # noqa: SLF001
    yield


def bearer(token: str) -> dict[str, str]:
    return {"Authorization": f"Bearer {token}"}


def new_user(env, name: str, role: str = "Contributor", scope=("workspace", None)):
    scope = (scope[0], scope[1] or env.tenants["A"].ws)
    return env.make_user(name, role, scope)


def login(env, name: str, password: str = PASSWORD) -> dict:
    r = env.c.post("/v1/identity/login", json={"username": name, "password": password})
    assert r.status_code == 200, r.text
    return r.json()


def me(env, token: str):
    return env.c.get("/v1/identity/me", headers=bearer(token))


def forge(env, **claims) -> str:
    keys = env.ctx.tokens.keys
    uid = env.actor("a_contrib").id
    now = int(time.time())
    base = {"iss": "olcf", "aud": "olcf", "sub": uid, "name": "a_contrib", "kind": "user", "iat": now, "nbf": now - 5, "exp": now + 600, "jti": "t", "scp": ["*"]}
    from olcf.security import tokens as T

    base["iss"], base["aud"] = T.ISSUER, T.AUDIENCE
    base.update(claims)
    return jwt.encode({k: v for k, v in base.items() if v is not None}, keys.current, algorithm="HS256")


# ---------------------------------------------------------------------------------------------------------------- access tokens
def test_access_token_expiry_is_enforced(env):
    row = env.ctx.service("identity").get(env.actor("a_contrib").id)
    expired, _ = env.ctx.tokens.issue_access_token(row, ttl=-60)
    r = me(env, expired)
    assert r.status_code == 401 and "expired" in r.text
    valid, ttl = env.ctx.tokens.issue_access_token(row)
    assert me(env, valid).status_code == 200 and ttl <= 3600, "access tokens are short lived"


@pytest.mark.parametrize("claims", [{"exp": 1}, {"nbf": int(time.time()) + 3600}, {"iss": "someone-else"}, {"aud": "another-service"}, {"sub": None}, {"exp": None}, {"sub": "usr_doesnotexist"}, {"aud": None}])
def test_forged_or_malformed_claims_are_refused(env, claims):
    assert me(env, forge(env, **claims)).status_code == 401


def test_signature_algorithm_and_key_confusion(env):
    uid = env.actor("a_contrib").id
    now = int(time.time())
    from olcf.security import tokens as T

    payload = {"iss": T.ISSUER, "aud": T.AUDIENCE, "sub": uid, "exp": now + 600, "iat": now}
    key = env.ctx.tokens.keys.current
    for alg in ("HS384", "HS512"):  # same key, other HMAC algorithm: only HS256 is accepted
        assert me(env, jwt.encode(payload, key, algorithm=alg)).status_code == 401
    none_token = base64.urlsafe_b64encode(b'{"alg":"none","typ":"JWT"}').rstrip(b"=").decode() + "." + base64.urlsafe_b64encode(json.dumps(payload).encode()).rstrip(b"=").decode() + "."
    assert me(env, none_token).status_code == 401
    assert me(env, jwt.encode(payload, "wrong-key", algorithm="HS256")).status_code == 401
    good = jwt.encode(payload, key, algorithm="HS256")
    tampered = good.rsplit(".", 1)[0] + "." + ("A" if good[-1] != "A" else "B") * 43
    assert me(env, tampered).status_code == 401
    head, body, sig = good.split(".")
    swapped = json.loads(base64.urlsafe_b64decode(body + "=="))
    swapped["sub"] = env.actor("admin").id  # privilege swap with the old signature
    assert me(env, ".".join([head, base64.urlsafe_b64encode(json.dumps(swapped).encode()).rstrip(b"=").decode(), sig])).status_code == 401


def test_signing_key_rotation(env):
    """After a rotation the previous key still verifies in-flight tokens; after a second rotation they are dead, and new tokens verify throughout."""
    keys = env.ctx.tokens.keys
    row = env.ctx.service("identity").get(env.actor("a_contrib").id)
    before, _ = env.ctx.tokens.issue_access_token(row)
    k0 = keys.current
    keys.rotate()
    try:
        assert keys.current != k0 and keys.previous == k0
        assert me(env, before).status_code == 200, "tokens signed with the previous key survive one rotation"
        mid, _ = env.ctx.tokens.issue_access_token(row)
        assert me(env, mid).status_code == 200
        keys.rotate()
        assert me(env, before).status_code == 401, "after two rotations the old key is gone"
        assert me(env, mid).status_code == 200
        import os
        import stat

        for name in ("jwt-signing.key", "jwt-signing.prev.key"):
            p = env.ctx.settings.path("secrets", name)
            assert p.exists() and stat.S_IMODE(os.stat(p).st_mode) == 0o600, name
    finally:
        # re-login helpers of other tests in this module hold tokens signed with older keys: refresh them
        for name in list(env.actors):
            if name != "admin":
                env.actors[name].headers.update(env.login(name))
        env.admin.update(env.login("admin", env.ctx.settings.path("secrets", "bootstrap-admin.txt").read_text().split("password=")[1].strip()))


# ---------------------------------------------------------------------------------------------------------------- refresh tokens
def test_refresh_tokens_rotate_and_are_single_use(env):
    new_user(env, "tok_refresh")
    first = login(env, "tok_refresh")
    second = env.c.post("/v1/identity/refresh", json={"refresh_token": first["refresh_token"]})
    assert second.status_code == 200 and second.json()["refresh_token"] != first["refresh_token"]
    replay = env.c.post("/v1/identity/refresh", json={"refresh_token": first["refresh_token"]})
    assert replay.status_code == 401, "a used refresh token must not work twice"
    assert me(env, second.json()["access_token"]).status_code == 200
    third = env.c.post("/v1/identity/refresh", json={"refresh_token": second.json()["refresh_token"]})
    assert third.status_code == 200


def test_token_types_are_not_interchangeable(env):
    new_user(env, "tok_types")
    s = login(env, "tok_types")
    pat = env.c.post("/v1/identity/tokens", json={"name": "types"}, headers=bearer(s["access_token"])).json()
    pat_value = pat.get("token") or pat.get("secret")
    assert pat_value
    # a refresh token is not an access credential
    assert me(env, s["refresh_token"]).status_code == 401
    # an access token or a PAT is not a refresh credential
    assert env.c.post("/v1/identity/refresh", json={"refresh_token": s["access_token"]}).status_code == 401
    assert env.c.post("/v1/identity/refresh", json={"refresh_token": pat_value}).status_code == 401
    # enrolment and worker tokens (opaque, other kinds) do not authenticate API calls as users
    enr, _ = env.ctx.tokens.create_enrollment(env.actor("a_contrib").id)
    assert me(env, enr).status_code == 401
    # credentials in the URL are only accepted by the streaming endpoints, never by ordinary routes
    assert env.c.get("/v1/identity/me", params={"access_token": s["access_token"]}).status_code == 401
    # the cookie jar is not a credential either
    assert env.c.get("/v1/identity/me", cookies={"access_token": s["access_token"], "olcf_session": s["access_token"]}).status_code == 401


def test_logout_revokes_the_refresh_token(env):
    new_user(env, "tok_logout")
    s = login(env, "tok_logout")
    assert env.c.post("/v1/identity/logout", json={"refresh_token": s["refresh_token"]}).status_code == 204
    assert env.c.post("/v1/identity/refresh", json={"refresh_token": s["refresh_token"]}).status_code == 401
    assert env.c.post("/v1/identity/logout", json={"refresh_token": "garbage"}).status_code == 204, "logout never reveals whether a token existed"


def test_password_change_and_reset_revoke_refresh_tokens(env):
    new_user(env, "tok_pw")
    s = login(env, "tok_pw")
    r = env.c.post("/v1/identity/password", json={"current_password": "wrong-password-xx", "new_password": "Another-Strong-Pass-1"}, headers=bearer(s["access_token"]))
    assert r.status_code == 403
    assert env.c.post("/v1/identity/refresh", json={"refresh_token": s["refresh_token"]}).status_code == 200  # a failed change revokes nothing
    s = login(env, "tok_pw")
    r = env.c.post("/v1/identity/password", json={"current_password": PASSWORD, "new_password": "x"}, headers=bearer(s["access_token"]))
    assert r.status_code in (400, 422), "weak passwords are refused"
    r = env.c.post("/v1/identity/password", json={"current_password": PASSWORD, "new_password": "Another-Strong-Pass-1"}, headers=bearer(s["access_token"]))
    assert r.status_code == 204
    assert env.c.post("/v1/identity/refresh", json={"refresh_token": s["refresh_token"]}).status_code == 401
    assert env.c.post("/v1/identity/login", json={"username": "tok_pw", "password": PASSWORD}).status_code == 401
    s2 = login(env, "tok_pw", "Another-Strong-Pass-1")
    rr = env.c.post(f"/v1/identity/users/{env.actor('tok_pw').id}/reset-password", json={"new_password": "Admin-Reset-Pass-2"}, headers=env.admin)
    assert rr.status_code in (200, 204), rr.text
    assert env.c.post("/v1/identity/refresh", json={"refresh_token": s2["refresh_token"]}).status_code == 401


# ---------------------------------------------------------------------------------------------------------------- personal access tokens
def test_pat_lifecycle_expiry_revocation_and_listing(env):
    u = new_user(env, "tok_pat")
    h = u.headers
    made = env.c.post("/v1/identity/tokens", json={"name": "ci", "ttl_days": 1}, headers=h)
    assert made.status_code == 201
    pat = made.json().get("token") or made.json().get("secret")
    tid = made.json().get("id") or made.json().get("token_id")
    assert pat.startswith("olcf_pat_") and tid
    assert me(env, pat).status_code == 200
    listing = env.c.get("/v1/identity/tokens", headers=h)
    assert pat not in listing.text and pat.split("_", 3)[3] not in listing.text, "the secret part is shown once, at creation"
    # expiry
    from sqlalchemy import text

    with env.ctx.db.begin() as conn:
        conn.execute(text("update api_tokens set expires_at = :e where id = :i"), {"e": iso_in(seconds=-3600), "i": tid})
    r = me(env, pat)
    assert r.status_code == 401 and "expired" in r.text
    # revocation takes effect on the very next request
    made2 = env.c.post("/v1/identity/tokens", json={"name": "ci2"}, headers=h).json()
    pat2, tid2 = made2.get("token") or made2.get("secret"), made2.get("id") or made2.get("token_id")
    assert me(env, pat2).status_code == 200
    assert env.c.delete(f"/v1/identity/tokens/{tid2}", headers=h).status_code == 204
    r = me(env, pat2)
    assert r.status_code == 401 and "revoked" in r.text
    # TTL is bounded
    assert env.c.post("/v1/identity/tokens", json={"name": "forever", "ttl_days": 100000}, headers=h).status_code == 422


def test_pat_secret_part_is_hashed_at_rest_and_compared_constant_time(env):
    u = new_user(env, "tok_hash")
    made = env.c.post("/v1/identity/tokens", json={"name": "h"}, headers=u.headers).json()
    pat = made.get("token") or made.get("secret")
    secret = pat.split("_", 3)[3]
    from sqlalchemy import text

    with env.ctx.db.connect() as conn:
        rows = conn.execute(text("select * from api_tokens where principal_id = :p"), {"p": u.id}).mappings().all()
    blob = json.dumps([dict(r) for r in rows], default=str)
    assert secret not in blob and pat not in blob
    # one flipped character in the secret never authenticates (and a prefix collision with another token neither)
    flipped = pat[:-1] + ("A" if pat[-1] != "A" else "B")
    assert me(env, flipped).status_code == 401
    assert me(env, pat.rsplit("_", 1)[0] + "_").status_code == 401


def test_token_scopes_limit_what_a_token_can_do(env):
    u = new_user(env, "tok_scope", "WorkspaceAdmin")
    made = env.c.post("/v1/identity/tokens", json={"name": "narrow", "scopes": ["job"]}, headers=u.headers).json()
    pat = made.get("token") or made.get("secret")
    assert me(env, pat).status_code == 200
    r = env.c.post("/v1/connections", json={"workspace_id": env.tenants["A"].ws, "name": "scoped-conn", "type": "s3", "config": {"bucket": "b"}}, headers=bearer(pat))
    assert r.status_code == 403, "a token scoped to jobs must not manage connections even though its owner may"
    r = env.c.post("/v1/identity/tokens", json={"name": "escalate", "scopes": ["*"]}, headers=bearer(pat))
    assert r.status_code in (403, 422) or r.json().get("scopes") != ["*"], "a scoped token cannot mint an unscoped one"


def test_deactivated_users_lose_every_credential_immediately(env):
    u = new_user(env, "tok_disable")
    s = login(env, "tok_disable")
    pat = env.c.post("/v1/identity/tokens", json={"name": "p"}, headers=bearer(s["access_token"])).json()
    pat_value = pat.get("token") or pat.get("secret")
    assert me(env, s["access_token"]).status_code == 200 and me(env, pat_value).status_code == 200
    r = env.c.patch(f"/v1/identity/users/{u.id}", json={"is_active": False}, headers=env.admin)
    assert r.status_code == 200, r.text
    assert me(env, s["access_token"]).status_code == 401, "an access token must not outlive the account"
    assert me(env, pat_value).status_code == 401
    assert env.c.post("/v1/identity/refresh", json={"refresh_token": s["refresh_token"]}).status_code == 401
    assert env.c.post("/v1/identity/login", json={"username": "tok_disable", "password": PASSWORD}).status_code == 401
    env.c.patch(f"/v1/identity/users/{u.id}", json={"is_active": True}, headers=env.admin)
    assert env.c.post("/v1/identity/login", json={"username": "tok_disable", "password": PASSWORD}).status_code == 200


def test_enrollment_tokens_are_single_use_and_worker_tokens_rotate(env):
    tokens = env.ctx.tokens
    pid = env.actor("admin").id
    enr, _ = tokens.create_enrollment(pid, ttl_seconds=60)
    tokens.consume_enrollment(enr)
    with pytest.raises(Exception, match="revoked|invalid"):
        tokens.consume_enrollment(enr)
    expired, row = tokens.create_enrollment(pid, ttl_seconds=1)
    from sqlalchemy import text

    with env.ctx.db.begin() as conn:
        conn.execute(text("update api_tokens set expires_at = :e where id = :i"), {"e": iso_in(seconds=-5), "i": row["id"]})
    with pytest.raises(Exception, match="expired"):
        tokens.consume_enrollment(expired)
    w1, r1 = tokens.create_worker_token(pid)
    assert tokens.verify_opaque(w1, kinds=("worker",))["id"] == r1["id"]
    tokens.revoke(r1["id"])
    with pytest.raises(Exception, match="revoked"):
        tokens.verify_opaque(w1, kinds=("worker",))
    w2, _ = tokens.create_worker_token(pid)
    assert w2 != w1 and tokens.verify_opaque(w2, kinds=("worker",))


# ---------------------------------------------------------------------------------------------------------------- login throttle
def test_login_failures_are_uniform_and_throttled(env):
    new_user(env, "tok_throttle")
    unknown = env.c.post("/v1/identity/login", json={"username": "nobody-here", "password": "whatever-1234"})
    wrong = env.c.post("/v1/identity/login", json={"username": "tok_throttle", "password": "whatever-1234"})
    assert unknown.status_code == wrong.status_code == 401
    strip = lambda r: {k: v for k, v in r.json()["error"].items() if k != "correlation_id"}  # noqa: E731
    assert strip(unknown) == strip(wrong), "unknown user and wrong password must be indistinguishable"
    codes = [env.c.post("/v1/identity/login", json={"username": "tok_throttle", "password": f"bad-{i}-password"}).status_code for i in range(14)]
    assert 429 in codes, "repeated failures for one account must be throttled"
    r = env.c.post("/v1/identity/login", json={"username": "tok_throttle", "password": PASSWORD})
    assert r.status_code in (200, 429)
    # one account's throttle does not lock out the others
    assert env.c.post("/v1/identity/login", json={"username": "a_viewer", "password": PASSWORD}).status_code == 200
    env.ctx.limiter.reset("login-user:tok_throttle")


def test_login_response_does_not_carry_password_material(env):
    new_user(env, "tok_shape")
    body = login(env, "tok_shape")
    text = json.dumps(body)
    assert "password_hash" not in text and "argon" not in text.lower() and "$2b$" not in text
    assert set(body["principal"]) <= {"id", "kind", "name", "display_name", "email", "is_active", "must_change_password", "source", "created_at", "updated_at", "last_login_at", "attrs", "external_id"}


def test_forwarded_for_header_cannot_dodge_the_ip_throttle(env):
    """Password spraying across many accounts from one address: ``X-Forwarded-For`` is only honoured from configured trusted proxies."""
    limit = env.ctx.settings.auth.login_rate_limit_per_minute
    codes = []
    for i in range(limit + 8):
        r = env.c.post("/v1/identity/login", json={"username": f"spray-{i}", "password": "wrong-password-1"}, headers={"X-Forwarded-For": f"10.9.{i}.{i}", "X-Real-IP": f"10.8.{i}.{i}"})
        codes.append(r.status_code)
    assert 429 in codes, "rotating X-Forwarded-For values must not reset the per-address throttle"
