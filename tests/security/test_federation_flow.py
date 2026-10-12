# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Provider CRUD, the authorization-code + PKCE browser flow, JIT provisioning, role sync and bearer validation against the mock IdP."""

from __future__ import annotations

from olcf.security import schema as SEC
from tests.security.fedutil import browser_login, entra_body, oidc_body
from tests.security.mock_idp import OTHER_TENANT, TENANT


def bearer(tok: str) -> dict[str, str]:
    return {"Authorization": f"Bearer {tok}"}


# ----------------------------------------------------------------------------- provider CRUD
def test_provider_crud_and_secret_by_reference(fed, idp):
    fed.post("/v1/workspaces", json={"name": "analytics"}, headers=fed.admin)
    r = fed.post("/v1/identity/providers", json=entra_body(idp), headers=fed.admin)
    assert r.status_code == 201, r.text
    p = r.json()
    assert p["has_client_secret"] is True and p["issuer"] == f"{idp.base}/{TENANT}/v2.0"
    assert idp.client_secret not in r.text
    assert idp.client_secret not in fed.get("/v1/identity/providers", headers=fed.admin).text
    # the secret is in the secret store by reference (listing shows metadata only)
    secrets = fed.get("/v1/secrets", headers=fed.admin).json()["items"]
    assert any(s["name"] == "idp-entra-client-secret" for s in secrets) and idp.client_secret not in str(secrets)
    # duplicate, bad name, unknown config key (fail closed), HS256 rejected in allowed_algs
    assert fed.post("/v1/identity/providers", json=entra_body(idp), headers=fed.admin).status_code == 409
    assert fed.post("/v1/identity/providers", json=entra_body(idp, name="Bad Name"), headers=fed.admin).status_code == 422
    assert fed.post("/v1/identity/providers", json=entra_body(idp, name="x1", config={"bogus": 1}), headers=fed.admin).status_code == 422
    assert fed.post("/v1/identity/providers", json=entra_body(idp, name="x2", config={"allowed_algs": ["HS256"]}), headers=fed.admin).status_code == 422
    # plain http without the explicit opt-in is refused
    bad = entra_body(idp, name="x3")
    bad["config"].pop("allow_insecure_http")
    assert fed.post("/v1/identity/providers", json=bad, headers=fed.admin).status_code == 400
    # multi-tenant authority without a tenant allow-list would accept nobody -> refused
    assert fed.post("/v1/identity/providers", json=entra_body(idp, name="x4", tenant="organizations"), headers=fed.admin).status_code == 400
    # PATCH with ETag
    etag = fed.get(f"/v1/identity/providers/{p['id']}", headers=fed.admin).headers["etag"]
    assert fed.patch(f"/v1/identity/providers/{p['id']}", json={"display_name": "X"}, headers={**fed.admin, "If-Match": '"nope"'}).status_code == 412
    ok = fed.patch(f"/v1/identity/providers/{p['id']}", json={"display_name": "Contoso", "client_secret": "rotated-secret-123"}, headers={**fed.admin, "If-Match": etag})
    assert ok.status_code == 200 and ok.json()["display_name"] == "Contoso"
    assert "rotated-secret-123" not in ok.text
    assert fed.ctx.secrets.resolve(fed.fed.spec_by_name("entra").client_secret_id) == "rotated-secret-123"
    # public provider list shows buttons only
    pub = fed.get("/v1/identity/oidc/providers").json()
    assert pub["items"][0]["name"] == "entra" and "config" not in pub["items"][0] and "client_id" not in pub["items"][0]
    assert fed.delete(f"/v1/identity/providers/{p['id']}", headers=fed.admin).status_code == 204
    assert fed.get(f"/v1/identity/providers/{p['id']}", headers=fed.admin).status_code == 404


def test_provider_admin_requires_permission_and_privileged_mapping_needs_platform_admin(fed, idp):
    fed.post("/v1/identity/users", json={"name": "sec", "password": "correct horse battery", "roles": [{"role": "SecurityAdmin"}]}, headers=fed.admin)
    fed.post("/v1/identity/users", json={"name": "viewer", "password": "correct horse battery", "roles": [{"role": "Viewer"}]}, headers=fed.admin)
    sec = bearer(fed.post("/v1/identity/login", json={"username": "sec", "password": "correct horse battery"}).json()["access_token"])
    view = bearer(fed.post("/v1/identity/login", json={"username": "viewer", "password": "correct horse battery"}).json()["access_token"])
    assert fed.get("/v1/identity/providers", headers=view).status_code == 403
    assert fed.post("/v1/identity/providers", json=entra_body(idp), headers=view).status_code == 403
    assert fed.get("/v1/identity/providers").status_code == 401
    # a SecurityAdmin may configure providers but must not be able to mint PlatformAdmins through a mapping
    assert fed.post("/v1/identity/providers", json=entra_body(idp), headers=sec).status_code == 403
    safe = entra_body(idp)
    safe["config"]["role_mappings"] = [{"claim": "groups", "value": "g", "role": "Viewer"}]
    safe["config"]["service_principals"] = []
    assert fed.post("/v1/identity/providers", json=safe, headers=sec).status_code == 201


# ----------------------------------------------------------------------------- provider test + health
def test_provider_test_endpoint_and_health(fed, idp, entra):
    h = fed.get("/v1/admin/health", headers=fed.admin).json()
    comp = {c["name"]: c for c in h["components"]}
    assert comp["identity-providers"]["status"] == "unknown"  # configured but never tested
    r = fed.post(f"/v1/identity/providers/{entra['id']}/test", json={"client_credentials": True}, headers=fed.admin)
    assert r.status_code == 200, r.text
    res = r.json()
    assert res["ok"], res
    names = [s["name"] for s in res["steps"]]
    assert names == ["discovery", "jwks", "endpoints", "client_credentials"]
    assert "JWT signature verified" in res["steps"][-1]["detail"]
    deep = fed.post("/v1/admin/health/checks/identity-providers/test", headers=fed.admin).json()
    assert deep["status"] == "ok" and deep["steps"][0]["name"] == "entra"
    # break the IdP: the test reports the failing step with a remediation-friendly detail and health degrades
    idp.stop()
    fed.fed.invalidate()
    res = fed.post(f"/v1/identity/providers/{entra['id']}/test", headers=fed.admin).json()
    assert res["ok"] is False and res["steps"][0]["ok"] is False
    comp = {c["name"]: c for c in fed.get("/v1/admin/health?force=true", headers=fed.admin).json()["components"]}
    assert comp["identity-providers"]["status"] == "degraded"


def test_wrong_client_secret_is_reported_by_client_credentials_check(fed, idp, entra):
    fed.patch(f"/v1/identity/providers/{entra['id']}", json={"client_secret": "WRONG"}, headers=fed.admin)
    res = fed.post(f"/v1/identity/providers/{entra['id']}/test", json={"client_credentials": True}, headers=fed.admin).json()
    assert res["ok"] is False
    assert res["steps"][-1]["name"] == "client_credentials" and "invalid_client" in res["steps"][-1]["detail"]
    assert "WRONG" not in str(res)


# ----------------------------------------------------------------------------- authorization code + PKCE flow
def test_full_login_flow_jit_provisions_and_syncs_roles(fed, idp, entra):
    hops = browser_login(fed, idp, "entra", redirect_to="/ui/#/home")
    assert hops.login.status_code == 302
    # the redirect carries state, nonce, PKCE S256 and the *configured* redirect_uri (never the Host header)
    q = dict(x.split("=", 1) for x in hops.authorize_url.split("?", 1)[1].split("&"))
    assert q["code_challenge_method"] == "S256" and len(q["state"]) >= 40 and len(q["nonce"]) >= 30
    assert "redirect_uri=http%3A%2F%2Folcf.test%3A8470%2Fv1%2Fidentity%2Foidc%2Fentra%2Fcallback" in hops.authorize_url
    sc = hops.login.headers["set-cookie"].lower()
    assert "httponly" in sc and "samesite=lax" in sc
    assert hops.callback.status_code == 302 and hops.code, hops.callback.text
    assert hops.location.startswith("/ui/")
    # nothing sensitive in the query string; the one-time code travels in the fragment only
    assert "olcf_login_code" not in hops.location.split("#")[0]
    ex = fed.post("/v1/identity/oidc/exchange", json={"code": hops.code})
    assert ex.status_code == 200, ex.text
    tok = ex.json()
    assert tok["principal"]["source"] == "oidc:entra" and tok["principal"]["name"] == "entra:alice@example.com"
    me = fed.get("/v1/identity/me", headers=bearer(tok["access_token"])).json()
    assert [(b["role"], b["scope_type"]) for b in me["bindings"]] == [("Contributor", "workspace")]
    # the one-time code cannot be replayed
    assert fed.post("/v1/identity/oidc/exchange", json={"code": hops.code}).status_code == 401
    # refresh works and rotates
    r2 = fed.post("/v1/identity/refresh", json={"refresh_token": tok["refresh_token"]})
    assert r2.status_code == 200
    assert fed.post("/v1/identity/refresh", json={"refresh_token": tok["refresh_token"]}).status_code == 401
    # the contributor can work in 'analytics' only
    ws = fed.get("/v1/workspaces", headers=bearer(tok["access_token"])).json()["items"]
    assert [w["name"] for w in ws] == ["analytics"]
    # no password: the federated account cannot use local login
    assert fed.post("/v1/identity/login", json={"username": "entra:alice@example.com", "password": "anything-at-all-12"}).status_code == 401
    # audit trail
    acts = [a["action"] for a in fed.get("/v1/audit?limit=50", headers=fed.admin).json()["items"]]
    assert "auth.login" in acts and "identity.binding_changed" in acts


def test_second_login_syncs_changed_group_membership(fed, idp, entra):
    h1 = browser_login(fed, idp, "entra")
    tok = fed.post("/v1/identity/oidc/exchange", json={"code": h1.code}).json()
    pid = tok["principal"]["id"]
    idp.user = {**idp.user, "groups": ["g-admins"]}  # promoted in the IdP
    h2 = browser_login(fed, idp, "entra")
    t2 = fed.post("/v1/identity/oidc/exchange", json={"code": h2.code}).json()
    assert t2["principal"]["id"] == pid  # same principal (keyed by oid), not a duplicate
    me = fed.get("/v1/identity/me", headers=bearer(t2["access_token"])).json()
    assert [(b["role"], b["scope_type"]) for b in me["bindings"]] == [("PlatformAdmin", "org")]
    idp.user = {**idp.user, "groups": []}  # removed from every group -> no mapping -> login refused, old bindings stay until next sync
    h3 = browser_login(fed, idp, "entra")
    assert h3.error == "no_role_mapping" and not h3.code


def test_idp_user_named_admin_cannot_take_over_local_admin(fed, idp, entra):
    idp.user = {**idp.user, "preferred_username": "admin", "oid": "oid-evil", "groups": ["g-analysts"]}
    h = browser_login(fed, idp, "entra")
    tok = fed.post("/v1/identity/oidc/exchange", json={"code": h.code}).json()
    assert tok["principal"]["name"] == "entra:admin" and tok["principal"]["id"] != fed.ctx.service("identity").get_by_name("admin")["id"]
    me = fed.get("/v1/identity/me", headers=bearer(tok["access_token"])).json()
    assert all(b["role"] != "PlatformAdmin" for b in me["bindings"])


def test_disabled_account_and_disabled_provider(fed, idp, entra):
    h = browser_login(fed, idp, "entra")
    tok = fed.post("/v1/identity/oidc/exchange", json={"code": h.code}).json()
    pid = tok["principal"]["id"]
    assert fed.patch(f"/v1/identity/users/{pid}", json={"is_active": False}, headers=fed.admin).status_code == 200
    assert browser_login(fed, idp, "entra").error == "account_disabled"
    assert fed.get("/v1/identity/me", headers=bearer(tok["access_token"])).status_code == 401
    fed.patch(f"/v1/identity/users/{pid}", json={"is_active": True}, headers=fed.admin)
    t2 = fed.post("/v1/identity/oidc/exchange", json={"code": browser_login(fed, idp, "entra").code}).json()
    # disabling the provider revokes refresh tokens of its users and hides it from the login page
    r = fed.patch(f"/v1/identity/providers/{entra['id']}", json={"enabled": False}, headers=fed.admin)
    assert r.status_code == 200
    assert fed.post("/v1/identity/refresh", json={"refresh_token": t2["refresh_token"]}).status_code == 401
    assert fed.get("/v1/identity/oidc/providers").json()["items"] == []
    assert fed.get("/v1/identity/oidc/entra/login", follow_redirects=False).status_code == 404


def test_ui_challenge_binds_the_one_time_code(fed, idp, entra):
    from olcf.security.federation_oidc import new_pkce

    verifier, challenge = new_pkce()
    h = browser_login(fed, idp, "entra", ui_challenge=challenge)
    assert h.code
    assert fed.post("/v1/identity/oidc/exchange", json={"code": h.code}).status_code == 401  # interceptor without the verifier
    assert fed.post("/v1/identity/oidc/exchange", json={"code": h.code, "ui_verifier": "x" * 60}).status_code == 401
    ok = fed.post("/v1/identity/oidc/exchange", json={"code": h.code, "ui_verifier": verifier})
    assert ok.status_code == 200  # the failed attempts did not burn the code


def test_one_time_code_expires(fed, idp, entra):
    h = browser_login(fed, idp, "entra")
    from olcf.security import federation_oidc as O

    with fed.ctx.db.begin() as conn:
        conn.execute(SEC.oidc_login_codes.update().where(SEC.oidc_login_codes.c.code_hash == O.sha256_hex(h.code)).values(expires_at="2000-01-01T00:00:00.000000Z"))
    assert fed.post("/v1/identity/oidc/exchange", json={"code": h.code}).status_code == 401


def test_generic_oidc_provider_with_pkce_and_basic_auth(fed, idp):
    fed.post("/v1/workspaces", json={"name": "analytics"}, headers=fed.admin)
    assert fed.post("/v1/identity/providers", json=oidc_body(idp), headers=fed.admin).status_code == 201
    idp.user = {"sub": "kc-user-7", "preferred_username": "bob", "email": "bob@example.com", "groups": ["g-analysts"]}
    h = browser_login(fed, idp, "kc")
    assert h.code, h.callback.text if h.callback else h.login.text
    tok = fed.post("/v1/identity/oidc/exchange", json={"code": h.code}).json()
    assert tok["principal"]["name"] == "kc:bob"
    assert fed.post("/v1/identity/providers/kc/test", headers=fed.admin).json()["ok"] is True


def test_group_overage_resolved_through_graph(fed, idp):
    fed.post("/v1/workspaces", json={"name": "analytics"}, headers=fed.admin)
    body = entra_body(idp, config={"resolve_group_overage": True, "graph_base_url": f"{idp.base}/graph"})
    assert fed.post("/v1/identity/providers", json=body, headers=fed.admin).status_code == 201
    idp.user = {**idp.user, "__overage": True, "groups": []}
    idp.graph_groups = ["g-analysts"]
    h = browser_login(fed, idp, "entra")
    assert h.code
    tok = fed.post("/v1/identity/oidc/exchange", json={"code": h.code}).json()
    me = fed.get("/v1/identity/me", headers=bearer(tok["access_token"])).json()
    assert [b["role"] for b in me["bindings"]] == ["Contributor"]


def test_group_overage_without_resolution_grants_nothing(fed, idp, entra):
    idp.user = {**idp.user, "__overage": True}
    assert browser_login(fed, idp, "entra").error == "no_role_mapping"


def test_preview_mapping_dry_run(fed, idp, entra):
    r = fed.post(f"/v1/identity/providers/{entra['id']}/preview-mapping", json={"claims": {"groups": ["g-analysts", "unmapped"], "roles": ["OLCF.Viewer"]}}, headers=fed.admin)
    assert r.status_code == 200
    got = {(b["role"], b["scope_type"]) for b in r.json()["bindings"]}
    assert got == {("Contributor", "workspace"), ("Viewer", "org")}


# ----------------------------------------------------------------------------- bearer validation
def test_bearer_user_token_authenticates_and_maps(fed, idp, entra):
    tok = idp.mint(idp.access_claims(groups=["g-analysts"]))
    me = fed.get("/v1/identity/me", headers=bearer(tok))
    assert me.status_code == 200, me.text
    j = me.json()
    assert j["principal"]["source"] == "oidc:entra" and [b["role"] for b in j["bindings"]] == ["Contributor"]
    # delegated scope narrowing: olcf.read maps to the 'read' token scope -> no write
    assert j["token_scopes"] == ["read"]
    assert fed.post("/v1/workspaces", json={"name": "nope"}, headers=bearer(tok)).status_code == 403
    # same user via login flow and via bearer is ONE principal (both keyed by oid)
    h = browser_login(fed, idp, "entra")
    t2 = fed.post("/v1/identity/oidc/exchange", json={"code": h.code}).json()
    assert t2["principal"]["id"] == j["principal"]["id"]


def test_bearer_service_principal_maps_to_service_kind(fed, idp, entra):
    tok = idp.mint(idp.app_claims())
    me = fed.get("/v1/identity/me", headers=bearer(tok)).json()
    assert me["principal"]["kind"] == "service" and me["principal"]["name"] == "sp:entra:etl-bot"
    assert [(b["role"], b["scope_type"]) for b in me["bindings"]] == [("ServicePrincipal", "workspace")]
    # an application nobody mapped is refused (fail closed)
    unknown = idp.mint(idp.app_claims(oid="oid-app-2", sub="oid-app-2", roles=["Something.Else"]))
    assert fed.get("/v1/identity/me", headers=bearer(unknown)).status_code == 401
    # app role claims can map too (roles claim rule) even without an explicit sp entry
    viewer_app = idp.mint(idp.app_claims(oid="oid-app-3", sub="oid-app-3", roles=["OLCF.Viewer"]))
    assert fed.get("/v1/identity/me", headers=bearer(viewer_app)).status_code == 200


def test_bearer_scope_present_but_unmapped_is_refused(fed, idp, entra):
    tok = idp.mint(idp.access_claims(groups=["g-analysts"], scp="Other.Scope"))
    assert fed.get("/v1/identity/me", headers=bearer(tok)).status_code == 401


def test_bearer_without_role_mapping_is_refused(fed, idp, entra):
    assert fed.get("/v1/identity/me", headers=bearer(idp.mint(idp.access_claims(groups=["nobody"])))).status_code == 401


def test_key_rotation_is_picked_up_without_restart(fed, idp, entra):
    t1 = idp.mint(idp.access_claims(groups=["g-analysts"]))
    assert fed.get("/v1/identity/me", headers=bearer(t1)).status_code == 200
    before = idp.jwks_hits
    new = idp.rotate()  # IdP starts signing with a new key
    t2 = idp.mint(idp.access_claims(groups=["g-analysts"]), key=new)
    assert fed.get("/v1/identity/me", headers=bearer(t2)).status_code == 200
    assert idp.jwks_hits == before + 1  # exactly one refresh triggered by the unknown kid
    idp.rotate(retire_old=True)  # old keys removed from the JWKS: tokens signed by them stop working after the next refresh
    t3 = idp.mint(idp.access_claims(groups=["g-analysts"], jti="fresh"))
    assert fed.get("/v1/identity/me", headers=bearer(t3)).status_code == 200
    t_old = idp.mint(idp.access_claims(groups=["g-analysts"], jti="old"), key=new)
    assert fed.get("/v1/identity/me", headers=bearer(t_old)).status_code == 401


def test_jwks_outage_uses_stale_keys(fed, idp, entra):
    t = idp.mint(idp.access_claims(groups=["g-analysts"]))
    assert fed.get("/v1/identity/me", headers=bearer(t)).status_code == 200
    idp.fail_jwks = True
    t2 = idp.mint(idp.access_claims(groups=["g-analysts"], jti="2"))
    assert fed.get("/v1/identity/me", headers=bearer(t2)).status_code == 200  # key cached, no refetch needed


def test_unknown_issuer_causes_no_outbound_request(fed, idp, entra):
    before = (idp.jwks_hits, idp.discovery_hits)
    t = idp.mint(idp.access_claims(groups=["g-analysts"], iss="https://evil.example/tenant"))
    assert fed.get("/v1/identity/me", headers=bearer(t)).status_code == 401
    assert (idp.jwks_hits, idp.discovery_hits) == before


def test_bearer_accepts_tokens_for_other_validated_providers_only(fed, idp, entra):
    other = idp.mint(idp.access_claims(tid=OTHER_TENANT, groups=["g-analysts"]), issuer=idp.entra_issuer_for(OTHER_TENANT))
    assert fed.get("/v1/identity/me", headers=bearer(other)).status_code == 401  # tenant not allow-listed


def test_local_login_disable_has_lockout_protection(fed, idp, entra):
    st = fed.get("/v1/identity/local-login", headers=fed.admin).json()
    assert st["enabled"] and st["can_disable"] is False
    r = fed.put("/v1/identity/local-login", json={"enabled": False}, headers=fed.admin)
    assert r.status_code == 409 and r.json()["error"]["details"]["reasons"]
    # a federated administrator signs in -> now it is allowed
    idp.user = {**idp.user, "groups": ["g-admins"], "oid": "oid-boss", "preferred_username": "boss@example.com"}
    fed_admin = fed.post("/v1/identity/oidc/exchange", json={"code": browser_login(fed, idp, "entra").code}).json()
    assert fed.get("/v1/identity/local-login", headers=fed.admin).json()["can_disable"] is True
    assert fed.put("/v1/identity/local-login", json={"enabled": False}, headers=fed.admin).status_code == 200
    assert fed.post("/v1/identity/login", json={"username": "admin", "password": fed.admin_password}).status_code == 403
    # cannot delete the only provider while local login is off
    assert fed.delete(f"/v1/identity/providers/{entra['id']}", headers=fed.admin).status_code == 409
    # the federated admin re-enables local login (break-glass inside the API) and the old password works again
    fa = bearer(fed_admin["access_token"])
    assert fed.put("/v1/identity/local-login", json={"enabled": True}, headers=fa).status_code == 200
    assert fed.post("/v1/identity/login", json={"username": "admin", "password": fed.admin_password}).status_code == 200


def test_break_glass_env_forces_local_login(fed, idp, entra, monkeypatch):
    idp.user = {**idp.user, "groups": ["g-admins"], "oid": "oid-boss", "preferred_username": "boss@example.com"}
    fed.post("/v1/identity/oidc/exchange", json={"code": browser_login(fed, idp, "entra").code})
    assert fed.put("/v1/identity/local-login", json={"enabled": False}, headers=fed.admin).status_code == 200
    monkeypatch.setenv("OLCF_BREAKGLASS_LOCAL_LOGIN", "1")
    assert fed.fed.apply_local_login_flag() is True
    assert fed.post("/v1/identity/login", json={"username": "admin", "password": fed.admin_password}).status_code == 200


def test_login_after_secret_rotation_uses_new_secret(fed, idp, entra):
    fed.patch(f"/v1/identity/providers/{entra['id']}", json={"client_secret": "wrong"}, headers=fed.admin)
    assert browser_login(fed, idp, "entra").error == "token_endpoint_error"
    fed.patch(f"/v1/identity/providers/{entra['id']}", json={"client_secret": idp.client_secret}, headers=fed.admin)
    assert browser_login(fed, idp, "entra").code


def test_no_federation_secret_or_token_in_audit_or_logs(fed, idp, entra, caplog):
    caplog.set_level("DEBUG")
    h = browser_login(fed, idp, "entra")
    tok = fed.post("/v1/identity/oidc/exchange", json={"code": h.code}).json()
    blob = str(fed.get("/v1/audit?limit=500", headers=fed.admin).json()) + caplog.text
    for secret in (idp.client_secret, tok["access_token"], tok["refresh_token"], h.code):
        assert secret not in blob
