# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""LDAP bridge tests against ldap3's in-memory MOCK_SYNC directory (no network). A live AD/OpenLDAP server is NOT exercised (marked live)."""

from __future__ import annotations

import pytest

ldap3 = pytest.importorskip("ldap3")

from ldap3 import MOCK_SYNC, Connection, Server  # noqa: E402

from olcf.core.errors import Unauthenticated  # noqa: E402
from olcf.security.federation_mapping import LdapConfig  # noqa: E402
from olcf.security.ldap_bridge import LdapBridge  # noqa: E402

BASE = "dc=example,dc=com"
SVC_DN, SVC_PW = f"cn=svc,{BASE}", "svc-password"


def make_directory() -> Server:
    server = Server("fake.example.com", get_info="NONE")
    c = Connection(server, user=SVC_DN, password=SVC_PW, client_strategy=MOCK_SYNC)
    c.strategy.add_entry(SVC_DN, {"objectClass": "person", "userPassword": SVC_PW, "sAMAccountName": "svc"})
    for uid, pw, grp in (("alice", "alice-pw", ["cn=analysts,ou=groups," + BASE]), ("bob", "bob-pw", []), ("dup", "dup-pw", [])):
        c.strategy.add_entry(f"cn={uid},ou=users,{BASE}", {"objectClass": "person", "userPassword": pw, "sAMAccountName": uid, "mail": f"{uid}@example.com",
                                                          "displayName": uid.title(), "memberOf": grp, "entryUUID": f"uuid-{uid}"})
    c.strategy.add_entry(f"cn=dup2,ou=users,{BASE}", {"objectClass": "person", "userPassword": "x", "sAMAccountName": "dup", "entryUUID": "uuid-dup2"})
    c.strategy.add_entry(f"cn=analysts,ou=groups,{BASE}", {"objectClass": "group", "cn": "analysts", "member": [f"cn=alice,ou=users,{BASE}"]})
    c.strategy.add_entry(f"cn=admins,ou=groups,{BASE}", {"objectClass": "group", "cn": "admins", "member": [f"cn=bob,ou=users,{BASE}"]})
    return server


@pytest.fixture()
def bridge():
    server = make_directory()

    def factory(user, password):
        return Connection(server, user=user, password=password, client_strategy=MOCK_SYNC)

    cfg = LdapConfig(server_uri="ldaps://fake.example.com", bind_dn=SVC_DN, user_base_dn=f"ou=users,{BASE}", user_filter="(sAMAccountName={username})",
                     group_base_dn=f"ou=groups,{BASE}", group_filter="(member={user_dn})")
    return LdapBridge(cfg, SVC_PW, connection_factory=factory)


def test_authenticate_returns_identity_and_groups(bridge):
    ident = bridge.authenticate("alice", "alice-pw")
    assert ident.username == "alice" and ident.email == "alice@example.com" and ident.external_id == "uuid-alice"
    assert "analysts" in ident.groups and f"cn=analysts,ou=groups,{BASE}" in ident.groups
    # bob has no memberOf but is found through the group search
    assert "admins" in bridge.authenticate("bob", "bob-pw").groups


@pytest.mark.parametrize("user,pw", [("alice", "wrong"), ("alice", ""), ("", "x"), ("nobody", "x"), ("dup", "dup-pw"),
                                     ("alice)(sAMAccountName=*", "alice-pw"), ("*", "alice-pw"), ("alice\x00", "alice-pw"), ("a" * 300, "x")])
def test_bad_credentials_and_filter_injection_fail_closed(bridge, user, pw):
    with pytest.raises(Unauthenticated):
        bridge.authenticate(user, pw)


def test_plain_ldap_is_refused_without_opt_in():
    from olcf.core.errors import BadRequest

    with pytest.raises(BadRequest):
        LdapBridge(LdapConfig(server_uri="ldap://dc.example.com", user_base_dn=BASE), None, connection_factory=lambda u, p: None)
    LdapBridge(LdapConfig(server_uri="ldap://dc.example.com", start_tls=True, user_base_dn=BASE), None, connection_factory=lambda u, p: None)


def test_bridge_test_steps(bridge):
    steps = bridge.test()
    assert [s["name"] for s in steps] == ["bind", "search"] and all(s["ok"] for s in steps)


def test_ldap_login_endpoint_end_to_end(fed, monkeypatch):
    """Provider CRUD + group->role mapping + token issuance through the real API, with the directory replaced by the in-memory server."""
    server = make_directory()
    from olcf.security import ldap_bridge as LB

    real_init = LB.LdapBridge.__init__

    def init(self, cfg, pw, *, connection_factory=None):
        real_init(self, cfg, pw, connection_factory=lambda u, p: Connection(server, user=u, password=p, client_strategy=MOCK_SYNC))

    monkeypatch.setattr(LB.LdapBridge, "__init__", init)
    fed.post("/v1/workspaces", json={"name": "analytics"}, headers=fed.admin)
    body = {"name": "corp-ad", "type": "ldap", "client_secret": SVC_PW, "config": {
        "ldap": {"server_uri": "ldaps://fake.example.com", "bind_dn": SVC_DN, "user_base_dn": f"ou=users,{BASE}", "user_filter": "(sAMAccountName={username})",
                 "group_base_dn": f"ou=groups,{BASE}",
                 "group_rules": [{"claim": "groups", "value": "analysts", "role": "Contributor", "scope_type": "workspace", "workspace": "analytics"},
                                 {"claim": "groups", "pattern": "adm.*", "role": "Viewer"}]}}}
    assert fed.post("/v1/identity/providers", json=body, headers=fed.admin).status_code == 201
    assert fed.post("/v1/identity/providers/corp-ad/test", headers=fed.admin).json()["ok"] is True
    r = fed.post("/v1/identity/ldap/corp-ad/login", json={"username": "alice", "password": "alice-pw"})
    assert r.status_code == 200, r.text
    tok = r.json()
    assert tok["principal"]["source"] == "ldap:corp-ad" and tok["principal"]["name"] == "corp-ad:alice"
    me = fed.get("/v1/identity/me", headers={"Authorization": f"Bearer {tok['access_token']}"}).json()
    assert [(b["role"], b["scope_type"]) for b in me["bindings"]] == [("Contributor", "workspace")]
    assert fed.post("/v1/identity/ldap/corp-ad/login", json={"username": "alice", "password": "nope"}).status_code == 401
    r2 = fed.post("/v1/identity/ldap/corp-ad/login", json={"username": "bob", "password": "bob-pw"})
    assert r2.status_code == 200  # 'admins' matches the pattern rule
    assert fed.post("/v1/identity/ldap/nonexistent/login", json={"username": "a", "password": "b"}).status_code == 404
    assert SVC_PW not in fed.get("/v1/identity/providers", headers=fed.admin).text


@pytest.mark.live
def test_live_directory():
    pytest.skip("needs a real LDAP/AD server (live)")
