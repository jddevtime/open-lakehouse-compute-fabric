# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Helpers shared by the federation tests (provider bodies, browser-flow simulation)."""

from __future__ import annotations

from urllib.parse import parse_qs, urlsplit

import httpx
from fastapi.testclient import TestClient

from tests.security.mock_idp import TENANT, MockIdp  # noqa: F401


def entra_body(idp: MockIdp, **over):
    body = {
        "name": "entra", "type": "entra", "display_name": "Contoso Entra", "tenant": idp.tenant, "client_id": idp.client_id,
        "client_secret": idp.client_secret,
        "config": {
            "allow_insecure_http": True, "authority_host": idp.base, "jwks_min_refresh_seconds": 0,
            "role_mappings": [
                {"claim": "groups", "value": "g-analysts", "role": "Contributor", "scope_type": "workspace", "workspace": "analytics"},
                {"claim": "groups", "value": "g-admins", "role": "PlatformAdmin"},
                {"claim": "roles", "value": "OLCF.Viewer", "role": "Viewer"},
            ],
            "service_principals": [{"name": "etl-bot", "match": {"oid": "oid-app-1"}, "roles": [{"role": "ServicePrincipal", "scope_type": "workspace", "workspace": "analytics"}]}],
            "scope_map": {"olcf.read": "read", "olcf.write": "write"},
        },
    }
    cfg = over.pop("config", None)
    body.update(over)
    if cfg:
        body["config"] = {**body["config"], **cfg}
    return body


def oidc_body(idp: MockIdp, **over):
    body = {
        "name": "kc", "type": "oidc", "display_name": "Keycloak", "issuer": idp.realm_issuer, "client_id": idp.client_id, "client_secret": idp.client_secret,
        "config": {"allow_insecure_http": True, "jwks_min_refresh_seconds": 0, "allowed_audiences": ["olcf-api"],
                   "role_mappings": [{"claim": "groups", "value": "g-analysts", "role": "Contributor", "scope_type": "workspace", "workspace": "analytics"}]},
    }
    cfg = over.pop("config", None)
    body.update(over)
    if cfg:
        body["config"] = {**body["config"], **cfg}
    return body


def browser_login(client: TestClient, idp: MockIdp, provider: str, *, redirect_to: str | None = None, ui_challenge: str | None = None,
                  tamper_cookie: bool = False, stop_before_callback: bool = False):
    """Simulate a browser: /login -> IdP authorize -> /callback -> read fragment. Returns a namespace of every hop."""
    from types import SimpleNamespace

    params = {}
    if redirect_to is not None:
        params["redirect_to"] = redirect_to
    if ui_challenge:
        params["ui_challenge"] = ui_challenge
    hops = SimpleNamespace(login=None, authorize=None, callback=None, fragment={}, query={}, code=None, error=None, location=None)
    hops.login = client.get(f"/v1/identity/oidc/{provider}/login", params=params, follow_redirects=False)
    if hops.login.status_code != 302:
        return hops
    loc = hops.login.headers["location"]
    hops.authorize_url = loc
    hops.authorize = httpx.get(loc, follow_redirects=False, trust_env=False)
    if hops.authorize.status_code != 302:
        return hops
    cb = urlsplit(hops.authorize.headers["location"])
    hops.callback_url = f"{cb.path}?{cb.query}"
    if stop_before_callback:
        return hops
    if tamper_cookie:
        client.cookies.clear()
    hops.callback = client.get(hops.callback_url, follow_redirects=False)
    if hops.callback.status_code == 302:
        hops.location = hops.callback.headers["location"]
        sp = urlsplit(hops.location)
        hops.fragment = {k: v[0] for k, v in parse_qs(sp.fragment).items()}
        hops.query = {k: v[0] for k, v in parse_qs(sp.query).items()}
        hops.code = hops.fragment.get("olcf_login_code")
        hops.error = hops.fragment.get("olcf_login_error")
    return hops


