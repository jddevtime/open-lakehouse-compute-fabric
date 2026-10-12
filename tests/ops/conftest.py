# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Fixtures for the ops tests: a populated control plane and fresh target databases."""

from __future__ import annotations

import os

import pytest

PASSPHRASE = "unit-test passphrase 1234"  # noqa: S105 - fixture credential, not a secret
CANARY = "SUPER-SECRET-VALUE-CANARY-9f3a"


@pytest.fixture()
def populated(ctx):
    """The foundation context plus rows in many tables (secrets with ciphertext, audit hash chain, events, kv)."""
    from sqlalchemy import insert

    from olcf.db import schema as S

    for i in range(3):
        ctx.secrets.create(f"canary-{i}", f"{CANARY}-{i}")
    for i in range(25):
        ctx.audit.record(None, "test.action", resource_type="x", resource_id=str(i), details={"i": i})
    for i in range(5):
        ctx.events.publish(f"test.event.{i}", {"n": i})
    with ctx.db.begin() as conn:
        conn.execute(insert(S.kv).values(key="ops.test", value={"a": [1, 2, {"b": None}], "uni": "café ☃"}, updated_at="2026-01-01T00:00:00Z"))
    return ctx


@pytest.fixture()
def target_db(tmp_path):
    """A brand-new, empty SQLite database (no migrations)."""
    from olcf.db.engine import Database

    db = Database(f"sqlite:///{tmp_path / 'restore-target.db'}")
    yield db
    db.dispose()


def pg_url() -> str | None:
    return os.environ.get("OLCF_TEST_POSTGRES_URL")


@pytest.fixture()
def ops_app(settings):
    """Full-ish control plane: foundation + ops (no federation), admin headers, services."""
    from fastapi.testclient import TestClient

    from olcf.api.app import create_app

    app = create_app(settings, module_names=["olcf.api.core.module", "olcf.ops.module"])
    with TestClient(app) as c:
        pw = app.state.ctx.settings.path("secrets", "bootstrap-admin.txt").read_text().split("password=")[1].strip()
        r = c.post("/v1/identity/login", json={"username": "admin", "password": pw})
        assert r.status_code == 200, r.text
        c.admin = {"Authorization": f"Bearer {r.json()['access_token']}"}  # type: ignore[attr-defined]
        c.ctx = app.state.ctx  # type: ignore[attr-defined]
        yield c


def wait_op(client, op_id: str, timeout: float = 60.0) -> dict:
    import time

    end = time.time() + timeout
    while time.time() < end:
        r = client.get(f"/v1/operations/{op_id}", headers=client.admin)
        assert r.status_code == 200, r.text
        if r.json()["state"] in ("succeeded", "failed"):
            return r.json()
        time.sleep(0.1)
    raise AssertionError(f"operation {op_id} did not finish")


@pytest.fixture()
def signer():
    from olcf.ops import manifest as M

    priv, raw, kid = M.generate_keypair()
    return _Signer(priv, raw, kid)


class _Signer:
    """Wraps an Ed25519 key so it can be passed to ``sign_manifest`` as the key and also exposes the public half."""

    def __init__(self, priv, raw, kid):  # noqa: ANN001
        self.priv, self.raw, self.kid = priv, raw, kid

    def public_b64(self) -> str:
        import base64

        return base64.b64encode(self.raw).decode()


@pytest.fixture()
def upd(ops_app, signer):
    """ops_app with the signer trusted, ``ops_app.upd`` the service."""
    c = ops_app
    r = c.post("/v1/admin/updates/trust", json={"public_key": signer.public_b64(), "name": "test-release-key"}, headers=c.admin)
    assert r.status_code == 201, r.text
    c.upd = c.ctx.service("updates")  # type: ignore[attr-defined]
    c.signer = signer  # type: ignore[attr-defined]
    return c


@pytest.fixture()
def ret_app(settings):
    """Control plane with the modules retention touches: core, observability, compute (no embedded worker), ops."""
    from fastapi.testclient import TestClient

    from olcf.api.app import create_app

    app = create_app(settings, module_names=["olcf.api.core.module", "olcf.observability.module", "olcf.compute.module", "olcf.ops.module"])
    with TestClient(app) as c:
        pw = app.state.ctx.settings.path("secrets", "bootstrap-admin.txt").read_text().split("password=")[1].strip()
        r = c.post("/v1/identity/login", json={"username": "admin", "password": pw})
        assert r.status_code == 200, r.text
        c.admin = {"Authorization": f"Bearer {r.json()['access_token']}"}  # type: ignore[attr-defined]
        c.ctx = app.state.ctx  # type: ignore[attr-defined]
        yield c
