# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Attack tests for identity federation (spec 31 / 35.4): algorithm confusion, wrong audience / issuer / tenant, expiry, replayed state and nonce,
state fixation, open redirects, discovery mix-up and race conditions. Each test forges what a real attacker could send."""

from __future__ import annotations

import base64
import hashlib
import hmac
import json
import threading
import time

import pytest

from olcf.security import federation_oidc as O
from tests.security.fedutil import browser_login, entra_body
from tests.security.mock_idp import OTHER_TENANT, TENANT, Key, b64u


def bearer(tok: str) -> dict[str, str]:
    return {"Authorization": f"Bearer {tok}"}


def me(fed, token: str):
    return fed.get("/v1/identity/me", headers=bearer(token))


GOOD = {"groups": ["g-analysts"]}


# ----------------------------------------------------------------------------- algorithm confusion
def test_alg_none_is_rejected(fed, idp, entra):
    tok = idp.mint(idp.access_claims(**GOOD), alg="none")
    assert tok.endswith(".")
    assert me(fed, tok).status_code == 401
    # also with an empty/garbage signature part
    head, body, _ = tok.split(".")
    assert me(fed, f"{head}.{body}.AAAA").status_code == 401


def test_hs256_signed_with_the_public_key_is_rejected(fed, idp, entra):
    """Classic key-confusion: the attacker HMACs with the (public) RSA key bytes as the secret."""
    claims = idp.access_claims(**GOOD)
    now = int(time.time())
    claims.update({"exp": now + 600, "nbf": now - 1})
    for secret in (idp.key.public_pem, json.dumps(idp.key.jwk()).encode(), idp.key.jwk()["n"].encode()):
        h = b64u(json.dumps({"alg": "HS256", "typ": "JWT", "kid": idp.key.kid}).encode())
        p = b64u(json.dumps(claims).encode())
        sig = b64u(hmac.new(secret, f"{h}.{p}".encode(), hashlib.sha256).digest())
        assert me(fed, f"{h}.{p}.{sig}").status_code == 401
    # sanity: the same claims signed properly by the IdP are accepted
    assert me(fed, idp.mint(idp.access_claims(**GOOD))).status_code == 200


def test_hs384_hs512_and_es256_with_rsa_key_rejected(fed, idp, entra):
    claims = idp.access_claims(**GOOD)
    now = int(time.time())
    claims.update({"exp": now + 600})
    for alg, dig in (("HS384", hashlib.sha384), ("HS512", hashlib.sha512)):
        h = b64u(json.dumps({"alg": alg, "kid": idp.key.kid}).encode())
        p = b64u(json.dumps(claims).encode())
        sig = b64u(hmac.new(idp.key.public_pem, f"{h}.{p}".encode(), dig).digest())
        assert me(fed, f"{h}.{p}.{sig}").status_code == 401
    # an ES256 header on a token whose kid names an RSA key: key type does not fit the algorithm
    h = b64u(json.dumps({"alg": "ES256", "kid": idp.key.kid}).encode())
    p = b64u(json.dumps(claims).encode())
    assert me(fed, f"{h}.{p}.{b64u(b'x' * 64)}").status_code == 401


def test_embedded_key_headers_are_ignored(fed, idp, entra):
    """jku / x5u / jwk headers must never make the server fetch or trust attacker-supplied keys."""
    attacker = Key("attacker")
    tok = idp.mint(idp.access_claims(**GOOD), key=attacker, headers={"jwk": attacker.jwk(), "jku": "http://127.0.0.1:1/evil.json", "kid": "attacker"})
    assert me(fed, tok).status_code == 401
    forged_with_real_kid = idp.mint(idp.access_claims(**GOOD), key=attacker, headers={"kid": idp.key.kid})
    assert me(fed, forged_with_real_kid).status_code == 401


# ----------------------------------------------------------------------------- claims
def test_wrong_audience_rejected(fed, idp, entra):
    for aud in ("api://someone-else", "https://graph.microsoft.com", "", None, ["x", "y"]):
        assert me(fed, idp.mint(idp.access_claims(aud=aud, **GOOD))).status_code == 401, aud
    assert me(fed, idp.mint(idp.access_claims(aud=[idp.client_id, "other"], **GOOD))).status_code == 200


def test_wrong_issuer_rejected_even_with_valid_signature(fed, idp, entra):
    # signed by the right key, but for an issuer that no provider claims
    assert me(fed, idp.mint(idp.access_claims(iss=f"{idp.base}/{TENANT}/v1.0", **GOOD))).status_code == 401
    # generic-looking issuer with the same signature
    assert me(fed, idp.mint(idp.access_claims(iss=idp.realm_issuer, **GOOD))).status_code == 401
    # v1 issuer is only accepted when the provider opts in
    v1 = idp.mint(idp.access_claims(iss=f"https://sts.windows.net/{TENANT}/", **GOOD))
    assert me(fed, v1).status_code == 401


def test_wrong_tenant_rejected(fed, idp, entra):
    # tenant not in the allow-list, issuer consistent with it
    t = idp.mint(idp.access_claims(tid=OTHER_TENANT, **GOOD))
    assert me(fed, t).status_code == 401
    # allowed tid but the issuer embeds another tenant -> issuer no longer selects this provider
    t2 = idp.mint(idp.access_claims(iss=idp.entra_issuer_for(OTHER_TENANT), **GOOD))
    assert me(fed, t2).status_code == 401
    # no tid at all
    c = idp.access_claims(**GOOD)
    c.pop("tid")
    assert me(fed, idp.mint(c)).status_code == 401


def test_expired_future_and_missing_exp_rejected(fed, idp, entra):
    now = int(time.time())
    assert me(fed, idp.mint(idp.access_claims(exp=now - 3600, **GOOD))).status_code == 401
    assert me(fed, idp.mint(idp.access_claims(nbf=now + 3600, **GOOD))).status_code == 401
    c = idp.access_claims(**GOOD)
    tok = idp.mint(c)
    # strip exp by re-signing without it
    import jwt as pyjwt

    no_exp = pyjwt.encode({k: v for k, v in {**c, "iat": now}.items() if k != "exp"}, idp.key.private_pem, algorithm="RS256", headers={"kid": idp.key.kid})
    assert me(fed, no_exp).status_code == 401
    assert me(fed, tok).status_code == 200


def test_tampered_payload_and_garbage_rejected(fed, idp, entra):
    good = idp.mint(idp.access_claims(**GOOD))
    h, p, s = good.split(".")
    claims = json.loads(base64.urlsafe_b64decode(p + "=="))
    claims["groups"] = ["g-admins"]
    forged = f"{h}.{b64u(json.dumps(claims).encode())}.{s}"
    assert me(fed, forged).status_code == 401
    before = (idp.jwks_hits, idp.discovery_hits)
    for junk in ("a.b.c", "....", "x" * 20000, f"{h}.{p}", "eyJhbGciOiJSUzI1NiJ9.e30.", "Bearer", "null"):
        assert me(fed, junk).status_code == 401
    assert (idp.jwks_hits, idp.discovery_hits) == before


def test_privilege_escalation_through_claims_is_not_possible(fed, idp, entra):
    """Role names in claims are data, not authority: only configured mapping rules grant anything."""
    tok = idp.mint(idp.access_claims(groups=["g-analysts", "PlatformAdmin"], roles=["PlatformAdmin", "*"], role="PlatformAdmin", scp="olcf.read"))
    j = me(fed, tok).json()
    assert [b["role"] for b in j["bindings"]] == ["Contributor"]
    assert fed.get("/v1/audit", headers=bearer(tok)).status_code == 403
    assert fed.get("/v1/identity/users", headers=bearer(tok)).status_code == 403


def test_disabled_principal_and_deleted_provider_stop_working_immediately(fed, idp, entra):
    tok = idp.mint(idp.access_claims(**GOOD))
    j = me(fed, tok).json()
    assert me(fed, tok).status_code == 200  # served from the validation cache
    fed.patch(f"/v1/identity/users/{j['principal']['id']}", json={"is_active": False}, headers=fed.admin)
    assert me(fed, tok).status_code == 401
    fed.patch(f"/v1/identity/users/{j['principal']['id']}", json={"is_active": True}, headers=fed.admin)
    assert me(fed, tok).status_code == 200
    fed.delete(f"/v1/identity/providers/{entra['id']}", headers=fed.admin)
    fed.fed.invalidate()
    fed.fed._val_cache.clear()  # noqa: SLF001
    assert me(fed, tok).status_code == 401


# ----------------------------------------------------------------------------- login flow attacks
def test_state_replay_and_unknown_state(fed, idp, entra):
    h = browser_login(fed, idp, "entra")
    assert h.code
    again = fed.get(h.callback_url, follow_redirects=False)  # same state & code replayed
    assert again.status_code == 400
    assert fed.get("/v1/identity/oidc/entra/callback?code=abc&state=forged", follow_redirects=False).status_code == 400
    assert fed.get("/v1/identity/oidc/entra/callback?code=abc", follow_redirects=False).status_code == 400
    # wrong provider in the URL for a valid state
    h2 = browser_login(fed, idp, "entra", stop_before_callback=True)
    other = h2.callback_url.replace("/entra/", "/nonexistent/")
    assert fed.get(other, follow_redirects=False).status_code in (400, 404)


def test_state_fixation_requires_the_same_browser(fed, idp, entra):
    """Login CSRF: attacker starts a flow, tricks the victim into opening the callback URL in *their* browser (no binding cookie)."""
    h = browser_login(fed, idp, "entra", stop_before_callback=True)
    victim = fed  # same client object but with the cookie jar cleared = a different browser
    fed.cookies.clear()
    r = victim.get(h.callback_url, follow_redirects=False)
    assert r.status_code == 400 and "browser" in r.text
    # a cookie belonging to a *different* flow does not help either
    h_other = browser_login(fed, idp, "entra", stop_before_callback=True)
    stolen = {k: v for k, v in fed.cookies.items()}
    fed.cookies.clear()
    h_attacker = browser_login(fed, idp, "entra", stop_before_callback=True)
    fed.cookies.clear()
    for k, v in stolen.items():
        fed.cookies.set(k, v, path="/v1/identity/oidc")
    assert fed.get(h_attacker.callback_url, follow_redirects=False).status_code == 400
    assert h_other.callback_url != h_attacker.callback_url


def test_nonce_mismatch_wrong_audience_and_bad_signature_in_id_token(fed, idp, entra):
    idp.tamper = {"nonce": "replayed-nonce-from-another-login"}
    assert browser_login(fed, idp, "entra").error == "bad_nonce"
    idp.tamper = {"aud": "someone-elses-client"}
    assert browser_login(fed, idp, "entra").error == "bad_audience"
    idp.tamper = {"tid": OTHER_TENANT}
    assert browser_login(fed, idp, "entra").error in ("tenant_not_allowed", "bad_issuer")
    idp.tamper = {"iss": "https://evil.example/"}
    assert browser_login(fed, idp, "entra").error == "bad_issuer"
    idp.tamper = {"exp": int(time.time()) - 3600}
    assert browser_login(fed, idp, "entra").error == "expired"
    idp.tamper = {}
    idp.id_token_key = Key("evil")  # id_token signed by a key that is not in the JWKS
    assert browser_login(fed, idp, "entra").error == "unknown_key"
    idp.id_token_key = Key(idp.key.kid)  # same kid, different key material
    assert browser_login(fed, idp, "entra").error == "bad_signature"
    idp.id_token_key = None
    idp.id_token_alg = "HS256"  # symmetric alg on an id_token: cannot even be produced with the RSA key via PyJWT -> the IdP double fails  # noqa: S105 - fixture credential, not a secret
    idp.id_token_alg = "RS256"  # noqa: S105 - fixture credential, not a secret
    assert browser_login(fed, idp, "entra").code


def test_authorization_code_cannot_be_redeemed_without_pkce_verifier(fed, idp, entra):
    """The IdP double enforces PKCE; this proves OLCF sent a verifier matching its challenge (flow succeeded) and that intercepting a code is useless."""
    import httpx

    h = browser_login(fed, idp, "entra", stop_before_callback=True)
    code = dict(x.split("=", 1) for x in h.callback_url.split("?", 1)[1].split("&"))["code"]
    r = httpx.post(f"{idp.base}/{TENANT}/oauth2/v2.0/token", data={"grant_type": "authorization_code", "code": code, "client_id": idp.client_id,
                                                                  "client_secret": idp.client_secret, "redirect_uri": "http://olcf.test:8470/v1/identity/oidc/entra/callback",
                                                                  "code_verifier": "a" * 64}, trust_env=False)
    assert r.status_code == 400 and r.json()["error"] == "invalid_grant"


def test_discovery_issuer_mixup_is_refused(fed, idp):
    fed.post("/v1/workspaces", json={"name": "analytics"}, headers=fed.admin)
    from tests.security.fedutil import oidc_body

    fed.post("/v1/identity/providers", json=oidc_body(idp), headers=fed.admin)
    idp.disc_issuer_override = "https://other-idp.example/realm"
    res = fed.post("/v1/identity/providers/kc/test", headers=fed.admin).json()
    assert res["ok"] is False and "issuer" in res["steps"][0]["detail"]
    assert fed.get("/v1/identity/oidc/kc/login", follow_redirects=False).status_code == 503


@pytest.mark.parametrize("target", [
    "//evil.example", "///evil.example", "https://evil.example/", "http://evil.example", "/\\evil.example", "\\\\evil.example", "/%2f%2fevil.example",
    "/%5cevil.example", "javascript:alert(1)", "data:text/html,x", "https://olcf.test:8470.evil.example/", "http://olcf.test:8470@evil.example/",
    "https://evil.example\\@olcf.test:8470/", "/ok\r\nSet-Cookie: x=1", "ftp://olcf.test:8470/", " //evil.example", "/ok\x00", "http://olcf.test:8471/",
    "https://olcf.test:8470/", "/" + "a" * 2000,
])
def test_open_redirect_variants_are_rejected(fed, idp, entra, target):
    r = fed.get("/v1/identity/oidc/entra/login", params={"redirect_to": target}, follow_redirects=False)
    assert r.status_code in (400, 422), (target, r.status_code, r.headers.get("location"))


def test_open_redirect_allowed_forms(fed, idp, entra):
    for ok in ("/", "/ui/", "/ui/#/jobs", "/a?b=1&c=2", None):
        h = browser_login(fed, idp, "entra", redirect_to=ok)
        assert h.code and h.location.startswith("/"), (ok, h.location)
    # an absolute origin works only after the admin allow-listed it
    assert fed.get("/v1/identity/oidc/entra/login", params={"redirect_to": "https://ui.example.com/app"}, follow_redirects=False).status_code == 400
    fed.patch(f"/v1/identity/providers/{entra['id']}", json={"config": {"allowed_return_origins": ["https://ui.example.com"]}}, headers=fed.admin)
    h = browser_login(fed, idp, "entra", redirect_to="https://ui.example.com/app")
    assert h.location.startswith("https://ui.example.com/app#") and h.code
    assert fed.get("/v1/identity/oidc/entra/login", params={"redirect_to": "https://ui.example.com.evil.example/"}, follow_redirects=False).status_code == 400


def test_host_header_cannot_change_the_redirect_uri(fed, idp, entra):
    r = fed.get("/v1/identity/oidc/entra/login", headers={"Host": "evil.example", "X-Forwarded-Host": "evil.example", "X-Forwarded-Proto": "https"}, follow_redirects=False)
    assert r.status_code == 302
    loc = r.headers["location"]
    assert "evil.example" not in loc.replace("evil", "") or "redirect_uri=http%3A%2F%2Folcf.test%3A8470" in loc
    assert "redirect_uri=http%3A%2F%2Folcf.test%3A8470%2Fv1%2Fidentity%2Foidc%2Fentra%2Fcallback" in loc


def test_error_from_idp_is_sanitised(fed, idp, entra):
    idp.user = {**idp.user, "__deny": True}
    h = browser_login(fed, idp, "entra")
    assert h.error == "access_denied" and not h.code
    # an attacker-chosen error string is never reflected
    ok = browser_login(fed, idp, "entra", stop_before_callback=True)
    state = dict(x.split("=", 1) for x in ok.callback_url.split("?", 1)[1].split("&"))["state"]
    r = fed.get(f"/v1/identity/oidc/entra/callback?error=%3Cscript%3Ealert(1)%3C/script%3E&state={state}", follow_redirects=False)
    assert r.status_code == 302 and "script" not in r.headers["location"] and "olcf_login_error=idp_error" in r.headers["location"]


def test_state_is_consumed_exactly_once_under_concurrency(fed, idp, entra):
    h = browser_login(fed, idp, "entra", stop_before_callback=True)
    state = dict(x.split("=", 1) for x in h.callback_url.split("?", 1)[1].split("&"))["state"]
    wins: list[bool] = []
    barrier = threading.Barrier(8)

    def go() -> None:
        barrier.wait()
        wins.append(fed.fed._consume_request(state) is not None)  # noqa: SLF001

    ts = [threading.Thread(target=go) for _ in range(8)]
    [t.start() for t in ts]
    [t.join() for t in ts]
    assert wins.count(True) == 1


def test_login_endpoints_are_rate_limited(fed, idp, entra):
    codes = [fed.get("/v1/identity/oidc/entra/login", follow_redirects=False).status_code for _ in range(40)]
    assert 429 in codes and codes[0] == 302
    for _ in range(40):
        fed.post("/v1/identity/oidc/exchange", json={"code": "x" * 43})
    assert fed.post("/v1/identity/oidc/exchange", json={"code": "x" * 43}).status_code == 429


def test_authorization_endpoint_must_not_be_cloud_metadata(fed, idp):
    from tests.security.fedutil import oidc_body

    body = oidc_body(idp, name="meta", config={"discovery_url": "http://169.254.169.254/latest/meta-data/"})
    assert fed.post("/v1/identity/providers", json=body, headers=fed.admin).status_code == 400
    body = oidc_body(idp, name="meta2", config={"jwks_uri": "http://user:pw@127.0.0.1/jwks"})
    assert fed.post("/v1/identity/providers", json=body, headers=fed.admin).status_code == 400


def test_pkce_helpers_match_rfc7636_vector():
    verifier = "dBjftJeZ4CVP-mB92K27uhbUJU1p1r_wW1gFWFOEjXk"
    assert O.pkce_challenge(verifier) == "E9Melhoa2OwvFrEMTJguCHaoeK1t8URWbuGJSstw-cM"


def test_entra_body_helper_is_valid(idp):
    assert entra_body(idp)["tenant"] == idp.tenant
