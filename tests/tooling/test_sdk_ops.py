# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""SDK wrappers for the operations API (backups, retention, diagnostics / support bundles, updates) against the real in-process application."""

from __future__ import annotations

import base64
import io
import zipfile

import httpx
import pytest

from olcf.sdk import AsyncClient, Client
from olcf.sdk.errors import BadRequestError, PermissionDeniedError, UnavailableError
from tests.tooling.conftest import admin_password

OPS_MODULES = ["olcf.api.core.module", "olcf.ops.module"]


@pytest.fixture()
def ops_app(tmp_path):
    from olcf.api.app import create_app
    from olcf.core.config import load_settings

    s = load_settings(tmp_path / "data", environment="test")
    s.compute.embedded_worker = False
    return create_app(s, module_names=OPS_MODULES)


@pytest.fixture()
def ops(ops_app):
    from fastapi.testclient import TestClient

    with TestClient(ops_app) as http:
        c = Client("http://testserver", http_client=http, max_retries=0)
        c.login("admin", admin_password(ops_app))
        c.http_for_tests = http  # type: ignore[attr-defined]
        yield c


def test_backup_lifecycle(ops, tmp_path):
    b = ops.backups.start(wait=True)
    bid = b["backup_id"]
    assert b["backup"]["state"] == "succeeded"
    assert any(i["id"] == bid for i in ops.backups.list()["items"])
    assert ops.backups.get(bid)["filename"].endswith(".olcfbak")
    prereq = ops.backups.prerequisites(bid)
    assert all(p["ok"] for p in prereq["prerequisites"][:2])
    assert ops.backups.validate(bid, wait=True)["operation"]["result"]["ok"] is True
    assert ops.backups.restore_test(bid, wait=True)["operation"]["state"] == "succeeded"
    data = ops.backups.download(bid)
    assert isinstance(data, bytes) and len(data) > 100
    assert b"admin" not in data or b"password" not in data, "backups are encrypted: no readable credentials"
    imported = ops.backups.import_backup(data, filename="copy.olcfbak")
    assert imported["kind"] == "imported" and imported["sha256"] == ops.backups.get(bid)["sha256"]
    with pytest.raises(BadRequestError, match="RESTORE"):
        ops.backups.restore(bid, confirm="nope", wait=True)
    assert ops.backups.run_retention(dry_run=True)["dry_run"] is True
    assert ops.backups.delete(bid)["removed_from"]


def test_backup_targets_and_config(ops, tmp_path):
    t = ops.backups.create_target("second", "local", config={"path": str(tmp_path / "bk")})
    assert t["has_secret"] is False and t["enabled"] is True
    assert ops.backups.test_target("second")["ok"] is True
    assert ops.backups.update_target("second", enabled=False)["enabled"] is False
    assert [x["name"] for x in ops.backups.list_targets()["items"]] == ["second"]
    assert ops.backups.get_target("second")["type"] == "local"
    assert ops.backups.target_files("second") == {"items": []} or ops.backups.target_files("second") == []
    cfg = ops.backups.set_config(include_keys=False, retention={"keep_last": 3, "keep_days": 10})
    assert cfg["retention"]["keep_last"] == 3 and cfg["passphrase_configured"] is False
    assert ops.backups.config()["retention"]["keep_days"] == 10
    ops.backups.delete_target("second")
    assert ops.backups.list_targets()["items"] == []


def test_backup_passphrase_is_write_only(ops):
    ops.backups.set_config(passphrase="correct horse battery staple")  # noqa: S106 - test value
    cfg = ops.backups.config()
    assert cfg["passphrase_configured"] is True
    assert "correct horse" not in str(cfg)


def test_retention(ops):
    st = ops.retention.status()
    assert st["enabled"] is True and st["settings"]["event_days"] > 0
    run = ops.retention.run(dry_run=True, only=["events", "tokens"])
    assert run["dry_run"] is True
    assert ops.retention.get_run(run["id"])["id"] == run["id"]
    assert any(r["id"] == run["id"] for r in _items(ops.retention.runs(limit=5)))
    assert isinstance(ops.retention.audit(), dict)


def _items(doc):
    return doc["items"] if isinstance(doc, dict) else doc


def test_diagnostics_and_support_bundle_do_not_leak_secrets(ops, ops_app):
    diag = ops.diagnostics.run(only=["platform", "time"])
    assert diag["summary"]["fail"] == 0 and ops.diagnostics.last()["id"] == diag["id"]
    password = admin_password(ops_app)
    bundle = ops.diagnostics.create_bundle(include_logs=True)
    assert bundle["sha256"] and any(b["id"] == bundle["id"] for b in _items(ops.diagnostics.list_bundles()))
    assert ops.diagnostics.get_bundle(bundle["id"])["filename"].endswith(".zip")
    raw = ops.diagnostics.download_bundle(bundle["id"])
    with zipfile.ZipFile(io.BytesIO(raw)) as z:
        blob = b"".join(z.read(n) for n in z.namelist())
    master = ops_app.state.ctx.settings.path("secrets", "master.key")
    assert password.encode() not in blob, "the bootstrap password must never be in a support bundle"
    if master.exists():
        assert master.read_bytes().strip() not in blob
    ops.diagnostics.delete_bundle(bundle["id"])
    assert all(b["id"] != bundle["id"] for b in _items(ops.diagnostics.list_bundles()))


def test_updates(ops):
    st = ops.updates.status()
    assert st["installed"] and st["feed_configured"] is False
    assert ops.updates.set_config(channel="beta", auto_check=False)["channel"] == "beta"
    assert ops.updates.config()["channel"] == "beta"
    with pytest.raises(UnavailableError, match="no update feed"):
        ops.updates.check()
    from cryptography.hazmat.primitives import serialization
    from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey

    pub = Ed25519PrivateKey.generate().public_key().public_bytes(serialization.Encoding.Raw, serialization.PublicFormat.Raw)
    added = ops.updates.trust_add("tooling-test", base64.b64encode(pub).decode())
    assert [k["keyid"] for k in ops.updates.trust_list()["items"]] == [added["keyid"]]
    ops.updates.trust_revoke(added["keyid"])
    assert all(k["revoked"] for k in ops.updates.trust_list()["items"]) or ops.updates.trust_list()["items"] == []
    assert ops.updates.maintenance(True)["maintenance"]["active"] is True
    assert ops.updates.maintenance(False)["maintenance"]["active"] is False


def test_non_admin_is_refused(ops):
    ops.identity.create_user("viewer-ops", kind="user", password="Viewer-Passw0rd-123", roles=[])  # noqa: S106 - test value
    low = Client("http://testserver", http_client=ops.http_for_tests, max_retries=0)  # type: ignore[attr-defined]
    low.login("viewer-ops", "Viewer-Passw0rd-123")
    for call in (low.backups.list, low.retention.status, low.diagnostics.last, low.updates.status):
        with pytest.raises(PermissionDeniedError):
            call()


@pytest.mark.anyio
async def test_async_client_has_the_same_resources(ops_app):
    async with ops_app.router.lifespan_context(ops_app):
        transport = httpx.ASGITransport(app=ops_app)
        async with httpx.AsyncClient(transport=transport, base_url="http://testserver") as http:
            c = AsyncClient("http://testserver", http_client=http, max_retries=0)
            await c.login("admin", admin_password(ops_app))
            assert (await c.retention.status())["enabled"] is True
            assert (await c.updates.status())["installed"]
            made = await c.backups.start(wait=True)
            assert (await c.backups.get(made["backup_id"]))["state"] == "succeeded"
