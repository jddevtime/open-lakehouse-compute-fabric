# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""A two-tenant control plane for the security suite: the *full* application (every installed module), actors with every role and two
workspaces ("tenant A", "tenant B") each holding resources that carry a unique canary string.

Everything is created through the public API as the platform administrator, so the data looks like real data (hash-chained audit, secrets in the secret
store, lakehouse files on disk ...). ``Env.actor(name)`` returns auth headers; ``Env.ids`` maps OpenAPI path-parameter names to concrete ids per tenant.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Any

from fastapi.testclient import TestClient

PASSWORD = "Correct-Horse-9-battery"  # noqa: S105 - fixture credential, not a secret
CANARY_A = "TENANT-A-CANARY-7f3c91"
CANARY_B = "TENANT-B-CANARY-4be8d2"
SECRET_B = "SECRET-VALUE-B-d41d8cd9"  # noqa: S105 - fixture credential, not a secret
SECRET_A = "SECRET-VALUE-A-98f0e1c2"  # noqa: S105 - fixture credential, not a secret


@dataclass
class Actor:
    name: str
    id: str
    headers: dict[str, str]
    role: str


@dataclass
class Tenant:
    key: str
    canary: str
    secret_value: str
    ws: str = ""
    ws_name: str = ""
    ids: dict[str, str] = field(default_factory=dict)  # resource kind -> id
    created: dict[str, Any] = field(default_factory=dict)


class Env:
    def __init__(self, client: TestClient, ctx: Any) -> None:
        self.c = client
        self.ctx = ctx
        self.actors: dict[str, Actor] = {}
        self.tenants: dict[str, Tenant] = {"A": Tenant("A", CANARY_A, SECRET_A), "B": Tenant("B", CANARY_B, SECRET_B)}
        self.failed_seeds: list[str] = []

    # ------------------------------------------------------------------ actors
    def actor(self, name: str) -> Actor:
        return self.actors[name]

    def headers(self, name: str) -> dict[str, str]:
        return self.actors[name].headers

    def login(self, username: str, password: str = PASSWORD) -> dict[str, str]:
        r = self.c.post("/v1/identity/login", json={"username": username, "password": password})
        assert r.status_code == 200, r.text
        return {"Authorization": f"Bearer {r.json()['access_token']}"}

    def make_user(self, name: str, role: str, scope: tuple[str, str] = ("org", "")) -> Actor:
        body = {"name": name, "password": PASSWORD, "roles": [{"role": role, "scope_type": scope[0], "scope_id": scope[1]}] if role else []}
        r = self.c.post("/v1/identity/users", json=body, headers=self.admin)
        assert r.status_code == 201, r.text
        a = Actor(name, r.json()["id"], self.login(name), role or "none")
        self.actors[name] = a
        return a

    # ------------------------------------------------------------------ seeding
    def post(self, path: str, body: dict[str, Any], label: str, expect: tuple[int, ...] = (200, 201, 202)) -> dict[str, Any] | None:
        r = self.c.post(path, json=body, headers=self.admin)
        if r.status_code not in expect:
            self.failed_seeds.append(f"{label}: {r.status_code} {r.text[:160]}")
            return None
        return r.json() if r.content else {}

    def seed_tenant(self, key: str) -> None:
        t = self.tenants[key]
        t.ws_name = f"tenant-{key.lower()}"
        w = self.post("/v1/workspaces", {"name": t.ws_name, "description": f"workspace of {t.canary}"}, f"workspace {key}")
        assert w, self.failed_seeds
        t.ws = w["id"]
        item = lambda typ, name, definition: self.post(f"/v1/workspaces/{t.ws}/items", {"type": typ, "name": name, "description": f"{t.canary} {typ}", "definition": definition}, f"{typ} {key}")  # noqa: E731
        nb = item("notebook", f"nb-{t.canary}", {"nbformat": {"nbformat": 4, "nbformat_minor": 5, "metadata": {}, "cells": [{"cell_type": "code", "metadata": {}, "execution_count": None, "outputs": [], "source": f"print('{t.canary}')"}]}})
        if nb:
            t.ids["item_id"] = nb["id"]
        pl = item("pipeline", f"pl-{t.canary}", {"activities": [], "description": t.canary})
        if pl:
            t.ids["pipeline_id"] = pl["id"]
        vs = item("variable_set", f"vs-{t.canary}", {"variables": [{"name": "V", "type": "string", "default": t.canary}]})
        if vs:
            t.ids["variable_set_id"] = vs["id"]
        env = item("environment", f"env-{t.canary}", {"description": t.canary})
        if env:
            t.ids["environment_id"] = env["id"]
        sec = self.post("/v1/secrets", {"name": f"secret-{t.canary}", "value": t.secret_value, "scope_type": "workspace", "scope_id": t.ws}, f"secret {key}")
        if sec:
            t.ids["sid"] = sec["id"]
        con = self.post("/v1/connections", {"workspace_id": t.ws, "name": f"conn-{t.canary}", "type": "local", "description": t.canary, "config": {"root": f"/tmp/{t.canary}"},  # noqa: S108 - deliberate fixed path: the test asserts on / needs exactly this location
                                            **({"secret_id": sec["id"]} if sec else {})}, f"connection {key}")
        if con:
            t.ids["cid"] = con["id"]
        lh = self.post(f"/v1/workspaces/{t.ws}/lakehouses", {"name": f"lh_{key.lower()}", "description": t.canary}, f"lakehouse {key}")
        if lh:
            t.ids["lh_id"] = lh["id"]
            r = self.c.put(f"/v1/lakehouses/{lh['id']}/files/content", params={"path": "Files/secret.txt"}, content=t.canary.encode(), headers=self.admin)
            if r.status_code not in (200, 201):
                self.failed_seeds.append(f"lakehouse file {key}: {r.status_code} {r.text[:120]}")
        job = self.post("/v1/jobs", {"kind": "script", "workspace_id": t.ws, "name": f"job-{t.canary}", "engine": "python",
                                     "source": {"format": "py", "content": f"print('{t.canary}')", "filename": "x.py"}, "tags": {"canary": t.canary}}, f"job {key}")
        if job:
            t.ids["job_id"] = job["id"]
        hook = self.post("/v1/webhooks", {"name": f"hook-{t.canary}", "url": "https://hooks.example.com/x", "event_types": ["job.*"], "workspace_id": t.ws}, f"webhook {key}")
        if hook:
            t.ids["webhook_id"] = hook.get("id", "")

    def param_values(self, tenant: str, other: bool = False) -> dict[str, str]:
        """Concrete values for OpenAPI path parameters from one tenant's resources."""
        t = self.tenants[tenant]
        i = t.ids
        vals = {"ws": t.ws, "workspace_id": t.ws, "item_id": i.get("item_id", "x"), "pipeline_id": i.get("pipeline_id", "x"), "sid": i.get("sid", "x"), "cid": i.get("cid", "x"),
                "lh_id": i.get("lh_id", "x"), "job_id": i.get("job_id", "x"), "webhook_id": i.get("webhook_id", "x")}
        if "environment_id" in i:
            vals["environment_id"] = i["environment_id"]
        return vals

    # ------------------------------------------------------------------ build
    def build(self) -> Env:
        pw = self.ctx.settings.path("secrets", "bootstrap-admin.txt").read_text().split("password=")[1].strip()
        self.admin = self.login("admin", pw)
        self.actors["admin"] = Actor("admin", self.c.get("/v1/identity/me", headers=self.admin).json()["principal"]["id"], self.admin, "PlatformAdmin")
        for key in ("A", "B"):
            self.seed_tenant(key)
        wa, wb = self.tenants["A"].ws, self.tenants["B"].ws
        self.make_user("a_admin", "WorkspaceAdmin", ("workspace", wa))
        self.make_user("a_contrib", "Contributor", ("workspace", wa))
        self.make_user("a_contrib2", "Contributor", ("workspace", wa))
        self.make_user("a_viewer", "Viewer", ("workspace", wa))
        self.make_user("a_operator", "Operator", ("workspace", wa))
        self.make_user("b_contrib", "Contributor", ("workspace", wb))
        self.make_user("b_admin", "WorkspaceAdmin", ("workspace", wb))
        self.make_user("sec_admin", "SecurityAdmin")
        self.make_user("rt_admin", "RuntimeAdmin")
        self.make_user("org_operator", "Operator")
        self.make_user("nobody", "")
        return self

    def cleanup_text(self, s: str) -> str:
        return re.sub(r"\s+", " ", s)


def build_env(client: TestClient, ctx: Any) -> Env:
    return Env(client, ctx).build()
