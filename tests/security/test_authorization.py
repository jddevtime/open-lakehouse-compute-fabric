# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Spec 35.4: tenant isolation and horizontal / vertical privilege escalation over *every* operation of the full application's OpenAPI document.

Two tenants (workspaces A and B) carry unique canary strings in names, descriptions, definitions, lakehouse files and secrets. Actors with workspace
scoped roles in A attack B's object ids on every route; no response may contain B's canaries, none may succeed, and B's data must be unchanged afterwards.
Organisation-scoped roles (SecurityAdmin, RuntimeAdmin, Operator) are cross-tenant *by design* and tested for separation of duties instead.
"""

from __future__ import annotations

import re

from tests.security.secenv import CANARY_A, CANARY_B, SECRET_A, SECRET_B
from tests.security.sweep import leaked, operations, request_for

SKIP_GENERIC = {("POST", "/v1/identity/login"), ("POST", "/v1/identity/logout"), ("POST", "/v1/identity/refresh"), ("POST", "/v1/compute/spark-smoke")}
#: reads available to every authenticated user: catalogues of platform capabilities, no tenant data
OPEN_TO_AUTHENTICATED = {
    ("GET", "/v1/roles"), ("GET", "/v1/runtimes"), ("GET", "/v1/runtimes/matrix"), ("POST", "/v1/runtimes/resolve"), ("GET", "/v1/providers"), ("GET", "/v1/providers/types"),
    ("GET", "/v1/catalogs"), ("GET", "/v1/sample/status"), ("GET", "/v1/integrations/fabric/submissions"), ("GET", "/v1/identity/me"), ("GET", "/v1/identity/tokens"),
    ("POST", "/v1/identity/tokens"), ("POST", "/v1/identity/password"),
}
#: mutating routes that are safe for read-only principals (stateless parsing / previews / own tokens)
#: ... and ``/v1/governance/review-queue/bulk``: by design it answers 200 with one result per item and authorises every item on its own (a read-only principal gets only
#: ``forbidden`` / ``not_found`` per item and nothing is decided; ``tests/intelligence/governance/test_list_embedding.py`` and ``test_mcp_gov.py`` pin that contract)
SIDE_EFFECT_FREE_POSTS = {("POST", "/v1/catalog/lineage/parse"), ("POST", "/v1/notebooks/{item_id}/parameters/preview"), ("POST", "/v1/runtimes/resolve"),
                          ("POST", "/v1/identity/tokens"), ("POST", "/v1/identity/password"), ("POST", "/v1/governance/review-queue/bulk")}
PLATFORM = re.compile(r"^/v1/(admin|audit|identity/(users|bindings|providers|local-login)|workers|worker-pools|providers|queues|clusters|runtimes|compute|roles|integrations|"
                      r"sample|deployments|catalogs|mcp)(/|$)")
WS_ACTORS = ("a_viewer", "a_contrib", "a_admin", "a_operator", "nobody")


def ops(env):
    return [o for o in operations(env.spec) if o.key not in SKIP_GENERIC and not o.streaming]


def _empty_listing(r) -> bool:
    """A 2xx answer to a foreign ``workspace_id`` filter is acceptable only when it is an empty listing."""
    try:
        body = r.json()
    except ValueError:
        return not r.content
    if isinstance(body, list):
        return not body
    if isinstance(body, dict):
        lists = [v for v in body.values() if isinstance(v, list)]
        if lists:
            return all(not v for v in lists)
        counts = list(body.values())             # a summary of counters (``/governance/review-queue/summary``): all zero is an empty answer, it says nothing about the foreign workspace
        return bool(counts) and all(isinstance(v, int) and not isinstance(v, bool) and v == 0 for v in counts)
    return False


def snapshot_b(env) -> dict:
    """Everything of tenant B that an attacker could alter or delete, read as the platform administrator."""
    t = env.tenants["B"]
    c, h = env.c, env.admin
    snap: dict = {}
    snap["workspace"] = c.get(f"/v1/workspaces/{t.ws}", headers=h).json()
    snap["items"] = sorted((i["id"], i["name"], i.get("etag"), i.get("deleted_at")) for i in c.get(f"/v1/workspaces/{t.ws}/items", headers=h).json()["items"])
    snap["notebook"] = c.get(f"/v1/items/{t.ids['item_id']}", headers=h).json()["definition"]
    snap["connection"] = c.get(f"/v1/connections/{t.ids['cid']}", headers=h).json()
    snap["secret"] = env.ctx.secrets.resolve(t.ids["sid"])
    snap["file"] = c.get(f"/v1/lakehouses/{t.ids['lh_id']}/files/content", params={"path": "Files/secret.txt"}, headers=h).text
    snap["job_state"] = c.get(f"/v1/jobs/{t.ids['job_id']}", headers=h).json()["state"]
    snap["webhooks"] = sorted(w["id"] for w in c.get("/v1/webhooks", params={"workspace_id": t.ws}, headers=h).json().get("items", []))
    return snap


def test_tenant_isolation_on_every_route(two_tenants):
    env = two_tenants
    before = snapshot_b(env)
    violations: list[str] = []
    n = 0
    for attacker, victim in [(a, "B") for a in WS_ACTORS if a in env.actors] + [("b_contrib", "A"), ("b_admin", "A")]:
        needles = (CANARY_B, SECRET_B) if victim == "B" else (CANARY_A, SECRET_A)
        for o in ops(env):
            kw = request_for(env, o, victim, env.headers(attacker))
            r = env.c.request(**kw)
            n += 1
            if r.status_code >= 500:
                violations.append(f"{attacker} {o.label}: server error {r.status_code}")
            if leaked(r.text, *needles):
                violations.append(f"{attacker} {o.label}: LEAK {leaked(r.text, *needles)} ({r.status_code})")
            path_bound = any(p in env.param_values(victim) for p in o.path_params)
            filter_only = not path_bound and any(p.get("in") == "query" and p["name"] in ("workspace_id", "workspace") for p in o.params)
            if 200 <= r.status_code < 300 and o.key not in OPEN_TO_AUTHENTICATED:
                if path_bound:
                    violations.append(f"{attacker} {o.label}: succeeded ({r.status_code}) on {victim}'s object")
                elif filter_only and not _empty_listing(r):
                    violations.append(f"{attacker} {o.label}: workspace filter of another tenant returned data: {r.text[:120]}")
    assert n > 2500
    assert not violations, f"{len(violations)} tenant-isolation violations:\n  " + "\n  ".join(violations[:40])
    assert snapshot_b(env) == before, "tenant B's data changed during the attack run"


def test_listings_never_contain_other_tenants(two_tenants):
    """Cross-tenant visibility through list/search endpoints without any id: what A's members see must not include B (and vice versa)."""
    env = two_tenants
    for who, other in (("a_viewer", CANARY_B), ("a_contrib", CANARY_B), ("a_admin", CANARY_B), ("nobody", CANARY_B), ("nobody", CANARY_A), ("b_contrib", CANARY_A)):
        for o in ops(env):
            if o.method != "get" or o.path_params:
                continue
            r = env.c.request(**request_for(env, o, "A", env.headers(who)))
            assert other not in r.text, f"{who} {o.label} lists {other}"
    r = env.c.get("/v1/search", params={"q": "CANARY"}, headers=env.headers("a_contrib"))
    assert CANARY_B not in r.text
    r = env.c.get("/v1/search", params={"q": "TENANT-B"}, headers=env.headers("a_admin"))
    assert CANARY_B not in r.text


def test_workspace_scoped_roles_cannot_reach_platform_routes(two_tenants):
    env = two_tenants
    bad = []
    for actor in ("a_viewer", "a_contrib", "a_admin", "a_operator", "nobody"):
        for o in ops(env):
            if not PLATFORM.match(o.path) or o.key in OPEN_TO_AUTHENTICATED or o.path.startswith("/v1/mcp") and o.method == "get":
                continue
            r = env.c.request(**request_for(env, o, "A", env.headers(actor)))
            if 200 <= r.status_code < 300:
                bad.append(f"{actor}: {o.label} -> {r.status_code}")
    assert not bad, "workspace-scoped actors reach platform administration:\n  " + "\n  ".join(bad)


def test_read_only_principals_cannot_mutate(two_tenants):
    env = two_tenants
    bad = []
    for actor in ("a_viewer", "nobody"):
        for o in ops(env):
            if o.method == "get" or o.key in SIDE_EFFECT_FREE_POSTS:
                continue
            r = env.c.request(**request_for(env, o, "A", env.headers(actor)))
            if 200 <= r.status_code < 300:
                bad.append(f"{actor}: {o.label} -> {r.status_code}")
    assert not bad, "\n  ".join(bad)


def test_organisation_roles_have_separated_duties(two_tenants):
    """Cross-tenant roles must still be limited to their duty (permission names from security.rbac)."""
    env = two_tenants
    c = env.c
    t = env.tenants["A"]
    sec, rt, op = env.headers("sec_admin"), env.headers("rt_admin"), env.headers("org_operator")
    # SecurityAdmin: identity/audit/secrets yes, platform operations no
    assert c.get("/v1/audit", headers=sec).status_code == 200
    assert c.get("/v1/identity/users", headers=sec).status_code == 200
    for method, path, body in (("get", "/v1/admin/backup", None), ("post", "/v1/admin/backup", {}), ("get", "/v1/admin/updates", None), ("post", "/v1/admin/diagnostics", {}),
                               ("get", "/v1/admin/support-bundle", None), ("put", "/v1/admin/settings/retention.event_days", {"value": 1}), ("get", "/v1/workers", None)):
        r = getattr(c, method)(path, headers=sec, **({"json": body} if body is not None else {}))
        assert r.status_code == 403, (path, r.status_code)
    # RuntimeAdmin: runtimes/compute/diagnostics yes; identity, audit, secrets, backups no
    assert c.post("/v1/admin/diagnostics", json={"only": ["time"]}, headers=rt).status_code == 200
    for method, path in (("get", "/v1/audit"), ("get", "/v1/audit/export"), ("get", "/v1/identity/users"), ("get", "/v1/admin/backup"), ("get", "/v1/identity/bindings"), ("get", "/v1/identity/providers")):
        assert getattr(c, method)(path, headers=rt).status_code == 403, path
    assert c.post("/v1/secrets", json={"name": "rt-made", "value": "v", "workspace_id": t.ws}, headers=rt).status_code == 403
    assert c.delete(f"/v1/workspaces/{t.ws}", headers=rt).status_code == 403
    # Operator (org): may cancel/retry/run, never administer, never write items
    assert c.put(f"/v1/items/{t.ids['item_id']}", json={"definition": {}}, headers=op).status_code == 403
    assert c.delete(f"/v1/items/{t.ids['item_id']}", headers=op).status_code == 403
    assert c.post("/v1/identity/users", json={"name": "op-made", "password": "Correct-Horse-9-battery"}, headers=op).status_code == 403
    assert c.get("/v1/secrets", headers=op).json().get("items", []) == []


# ----------------------------------------------------------------------------- targeted escalation attempts
def test_cannot_mint_tokens_for_other_principals(two_tenants):
    env = two_tenants
    admin_id = env.actor("admin").id
    for who in ("a_contrib", "a_admin", "nobody", "sec_admin"):
        r = env.c.post("/v1/identity/tokens", json={"name": "steal", "principal_id": admin_id}, headers=env.headers(who))
        assert r.status_code in (400, 403, 404, 422), (who, r.status_code, r.text[:200])
        assert "token" not in r.json() if r.headers.get("content-type", "").startswith("application/json") else True
    # tokens are personal: neither listed nor revocable by another user, even in the same workspace
    mine = env.c.post("/v1/identity/tokens", json={"name": "mine"}, headers=env.headers("a_contrib"))
    assert mine.status_code == 201
    tid = mine.json()["id"]
    assert tid not in {t["id"] for t in env.c.get("/v1/identity/tokens", headers=env.headers("a_contrib2")).json()["items"]}
    assert env.c.delete(f"/v1/identity/tokens/{tid}", headers=env.headers("a_contrib2")).status_code in (403, 404)
    assert env.c.get("/v1/identity/me", headers={"Authorization": f"Bearer {mine.json()['token']}"}).status_code == 200  # still alive
    # the secret part is shown once and never again
    listing = env.c.get("/v1/identity/tokens", headers=env.headers("a_contrib")).text
    assert mine.json()["token"] not in listing and "token_hash" not in listing


def test_role_escalation_through_bindings_and_user_management(two_tenants):
    env = two_tenants
    c = env.c
    wa, wb = env.tenants["A"].ws, env.tenants["B"].ws
    me = env.actor("a_admin").id
    attempts = [
        ("self-grant PlatformAdmin (org)", {"principal_id": me, "role": "PlatformAdmin"}),
        ("self-grant PlatformAdmin at workspace scope", {"principal_id": me, "role": "PlatformAdmin", "scope_type": "workspace", "scope_id": wa}),
        ("self-grant SecurityAdmin at workspace scope", {"principal_id": me, "role": "SecurityAdmin", "scope_type": "workspace", "scope_id": wa}),
        ("grant admin in tenant B", {"principal_id": me, "role": "WorkspaceAdmin", "scope_type": "workspace", "scope_id": wb}),
        ("org-scoped WorkspaceAdmin", {"principal_id": me, "role": "WorkspaceAdmin"}),
        ("unknown scope type", {"principal_id": me, "role": "WorkspaceAdmin", "scope_type": "everything", "scope_id": ""}),
    ]
    for label, body in attempts:
        for who in ("a_admin", "a_contrib", "a_viewer"):
            r = c.post("/v1/identity/bindings", json=body, headers=env.headers(who))
            assert r.status_code in (400, 403, 404, 422), (label, who, r.status_code, r.text[:160])
    after = c.get("/v1/identity/me", headers=env.headers("a_admin")).json()
    assert not any(b["role"] in ("PlatformAdmin", "SecurityAdmin") for b in after["bindings"]), after["bindings"]
    assert not any(b.get("scope_id") == wb for b in after["bindings"])
    # creating users with roles / resetting passwords / disabling admins
    admin_id = env.actor("admin").id
    for who in ("a_admin", "a_contrib"):
        h = env.headers(who)
        assert c.post("/v1/identity/users", json={"name": f"evil-{who}", "password": "Correct-Horse-9-battery", "roles": [{"role": "PlatformAdmin", "scope_type": "org", "scope_id": ""}]}, headers=h).status_code == 403
        assert c.post(f"/v1/identity/users/{admin_id}/reset-password", json={"new_password": "Attacker-Pass-123456"}, headers=h).status_code in (403, 404, 422)  # gitleaks:allow
        assert c.patch(f"/v1/identity/users/{admin_id}", json={"is_active": False}, headers=h).status_code == 403
        assert c.get(f"/v1/identity/users/{admin_id}", headers=h).status_code in (403, 404)
        assert c.get("/v1/identity/users", headers=h).status_code == 403
    assert c.get("/v1/identity/me", headers=env.admin).status_code == 200 and env.login("admin", env.ctx.settings.path("secrets", "bootstrap-admin.txt").read_text().split("password=")[1].strip())
    # a workspace admin may manage members of their own workspace with workspace roles
    r = c.post("/v1/identity/bindings", json={"principal_id": env.actor("nobody").id, "role": "Viewer", "scope_type": "workspace", "scope_id": wa}, headers=env.headers("a_admin"))
    assert r.status_code == 201, r.text
    bid = r.json()["id"]
    assert c.delete(f"/v1/identity/bindings/{bid}", headers=env.headers("a_contrib")).status_code == 403
    assert c.delete(f"/v1/identity/bindings/{bid}", headers=env.headers("b_admin")).status_code == 403  # another tenant's admin
    assert c.delete(f"/v1/identity/bindings/{bid}", headers=env.headers("a_admin")).status_code == 204


def test_security_admin_cannot_become_platform_admin(two_tenants):
    """SecurityAdmin manages identities but PlatformAdmin is above it: granting it (to self or a friend) is privilege escalation."""
    env = two_tenants
    c = env.c
    sec = env.actor("sec_admin")
    friend = env.make_user("sec_friend", "")
    for target in (sec.id, friend.id):
        r = c.post("/v1/identity/bindings", json={"principal_id": target, "role": "PlatformAdmin"}, headers=sec.headers)
        assert r.status_code in (403, 422), f"SecurityAdmin granted PlatformAdmin to {target}: {r.status_code} {r.text[:200]}"
    r = c.post("/v1/identity/users", json={"name": "sec-made-admin", "password": "Correct-Horse-9-battery", "roles": [{"role": "PlatformAdmin", "scope_type": "org", "scope_id": ""}]}, headers=sec.headers)
    assert r.status_code in (403, 422), f"SecurityAdmin created a PlatformAdmin: {r.status_code} {r.text[:200]}"
    # nor may it take over the administrator account
    admin_id = env.actor("admin").id
    r = c.post(f"/v1/identity/users/{admin_id}/reset-password", json={"new_password": "Attacker-Pass-123456"}, headers=sec.headers)  # gitleaks:allow
    assert r.status_code in (403, 422), f"SecurityAdmin reset the PlatformAdmin password: {r.status_code}"


def test_token_scopes_restrict_even_an_administrator(two_tenants):
    env = two_tenants
    c = env.c
    r = c.post("/v1/identity/tokens", json={"name": "ro", "scopes": ["read"]}, headers=env.admin)
    assert r.status_code == 201, r.text
    ro = {"Authorization": f"Bearer {r.json()['token']}"}
    assert c.get("/v1/workspaces", headers=ro).status_code == 200
    for method, path, body in (("post", "/v1/workspaces", {"name": "via-ro-token"}), ("delete", f"/v1/items/{env.tenants['A'].ids['pipeline_id']}", None),
                               ("post", "/v1/identity/users", {"name": "ro-made", "password": "Correct-Horse-9-battery"}), ("post", "/v1/admin/backup", {}),
                               ("post", "/v1/secrets", {"name": "ro-secret", "value": "v"}), ("post", "/v1/identity/tokens", {"name": "child", "scopes": ["*"]})):
        resp = getattr(c, method)(path, headers=ro, **({"json": body} if body is not None else {}))
        assert resp.status_code in (401, 403), (path, resp.status_code, resp.text[:160])
    # a token cannot widen its own scope by minting a new one
    r2 = c.post("/v1/identity/tokens", json={"name": "child2", "scopes": ["read", "*"]}, headers=ro)
    assert r2.status_code in (401, 403)
    # the account is untouched
    assert c.get("/v1/items/" + env.tenants["A"].ids["pipeline_id"], headers=env.admin).status_code == 200


def test_security_admin_keeps_its_legitimate_duties(two_tenants):
    """The no-escalation rule must not break identity administration of ordinary accounts and workspace-class roles."""
    env = two_tenants
    c = env.c
    sec = env.headers("sec_admin")
    wa = env.tenants["A"].ws
    r = c.post("/v1/identity/users", json={"name": "sec-made-user", "password": "Correct-Horse-9-battery", "roles": [{"role": "Viewer", "scope_type": "workspace", "scope_id": wa}]}, headers=sec)
    assert r.status_code == 201, r.text
    uid = r.json()["id"]
    assert c.post(f"/v1/identity/users/{uid}/reset-password", headers=sec).status_code == 200
    assert c.patch(f"/v1/identity/users/{uid}", json={"is_active": False}, headers=sec).status_code == 200
    assert c.post("/v1/identity/bindings", json={"principal_id": uid, "role": "Contributor", "scope_type": "workspace", "scope_id": wa}, headers=sec).status_code == 201
    assert c.post("/v1/identity/bindings", json={"principal_id": uid, "role": "Viewer"}, headers=sec).status_code == 201  # org-wide read-only
    assert c.post("/v1/identity/tokens", json={"name": "for-user", "principal_id": uid, "scopes": ["read"]}, headers=sec).status_code == 201
    # peers are fine, other platform roles are not
    peer = env.make_user("sec_peer", "SecurityAdmin")
    assert c.post(f"/v1/identity/users/{peer.id}/reset-password", headers=sec).status_code == 200
    rt = env.actor("rt_admin")
    for body in ({"principal_id": uid, "role": "RuntimeAdmin"}, {"principal_id": uid, "role": "PlatformAdmin"}):
        assert c.post("/v1/identity/bindings", json=body, headers=sec).status_code == 403
    assert c.post(f"/v1/identity/users/{rt.id}/reset-password", headers=sec).status_code == 403
    assert c.post("/v1/identity/tokens", json={"name": "x", "principal_id": rt.id}, headers=sec).status_code == 403
    # removing a platform administrator's role is as privileged as granting it
    with env.ctx.db.connect() as conn:
        from sqlalchemy import select

        from olcf.db import schema as S

        bid = conn.execute(select(S.role_bindings.c.id).where(S.role_bindings.c.principal_id == env.actor("admin").id)).scalar()
    assert c.delete(f"/v1/identity/bindings/{bid}", headers=sec).status_code == 403
    # the administrator can still do everything
    assert c.post(f"/v1/identity/users/{rt.id}/reset-password", headers=env.admin).status_code == 200
