# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Updates: feed check, offline bundles, preflight with a real migration dry-run, maintenance windows, drain, staged apply, hand-off and rollback."""

from __future__ import annotations

import tarfile
from datetime import UTC, datetime, timedelta

import pytest

from olcf.ops import manifest as M
from tests.ops.feedserver import FeedServer, ForwardProxy
from tests.ops.updateutil import feed_files, make_bundle, make_release

pytestmark = pytest.mark.filterwarnings("ignore::ResourceWarning")


def _cfg(c, **kw):
    r = c.put("/v1/admin/updates/config", json=kw, headers=c.admin)
    assert r.status_code == 200, r.text
    return r.json()


@pytest.fixture()
def feed(upd, tmp_path):
    srv = FeedServer().start()
    _cfg(upd, feed_url=srv.url, allow_insecure_http=True, fetch_previous=False)
    yield srv
    srv.stop()


def _publish(feed, rel, channel="stable"):
    feed.files.clear()
    feed.files.update(feed_files(rel, channel))


# ----------------------------------------------------------------------------- check
def test_check_requires_feed_and_https(upd):
    c = upd
    r = c.post("/v1/admin/updates/check", json={}, headers=c.admin)
    assert r.status_code == 503 and "offline bundle" in r.text
    assert c.put("/v1/admin/updates/config", json={"feed_url": "http://example.invalid/feed"}, headers=c.admin).status_code == 400
    assert c.put("/v1/admin/updates/config", json={"feed_url": "ftp://x/feed"}, headers=c.admin).status_code == 400
    assert c.put("/v1/admin/updates/config", json={"threshold": 0}, headers=c.admin).status_code == 400
    assert c.put("/v1/admin/updates/config", json={"channel": "nightly"}, headers=c.admin).status_code == 400
    assert c.put("/v1/admin/updates/config", json={"bogus": 1}, headers=c.admin).status_code == 422


def test_check_finds_signed_update(upd, feed, tmp_path):
    rel = make_release(tmp_path, upd.signer, "0.2.0", sequence=5)
    _publish(feed, rel)
    r = upd.post("/v1/admin/updates/check", json={}, headers=upd.admin)
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["update_available"] and body["latest"] == "0.2.0" and body["update"]["state"] == "available"
    assert body["update"]["verdict"]["keyids"] == [upd.signer.kid]
    st = upd.get("/v1/admin/updates", headers=upd.admin).json()
    assert st["installed"] == "0.1.0" and st["available"]["version"] == "0.2.0" and st["floor"]["stable"]["sequence"] == 5
    # same check again does not duplicate
    upd.post("/v1/admin/updates/check", json={}, headers=upd.admin)
    assert len([i for i in upd.get("/v1/admin/updates", headers=upd.admin).json()["items"] if i["version"] == "0.2.0"]) == 1


def test_check_no_update_when_current(upd, feed, tmp_path):
    _publish(feed, make_release(tmp_path, upd.signer, "0.1.0", sequence=1))
    body = upd.post("/v1/admin/updates/check", json={}, headers=upd.admin).json()
    assert body["update_available"] is False and body["update"] is None


def test_check_rejects_unsigned_untrusted_expired_replayed_wrongchannel(upd, feed, tmp_path):
    other = M.generate_keypair()
    # untrusted signer
    _publish(feed, make_release(tmp_path, upd.signer, "0.2.0", sign_with=[other[0]]))
    r = upd.post("/v1/admin/updates/check", json={}, headers=upd.admin)
    assert r.status_code == 400 and "trust" in r.text
    # unsigned
    rel = make_release(tmp_path, upd.signer, "0.2.0")
    rel.envelope["signatures"] = []
    _publish(feed, rel)
    assert "unsigned" in upd.post("/v1/admin/updates/check", json={}, headers=upd.admin).text
    # expired
    past = (datetime.now(UTC) - timedelta(days=2)).isoformat()
    _publish(feed, make_release(tmp_path, upd.signer, "0.2.0", expires_at=past))
    assert "expired" in upd.post("/v1/admin/updates/check", json={}, headers=upd.admin).text
    # wrong channel
    _publish(feed, make_release(tmp_path, upd.signer, "0.2.0", channel="beta"))
    assert "channel" in upd.post("/v1/admin/updates/check", json={}, headers=upd.admin).text
    # nothing was recorded for any of those
    assert upd.get("/v1/admin/updates", headers=upd.admin).json()["items"] == []
    # accept 0.3.0 seq 10 ...
    _publish(feed, make_release(tmp_path, upd.signer, "0.3.0", sequence=10))
    assert upd.post("/v1/admin/updates/check", json={}, headers=upd.admin).status_code == 200
    # ... then a validly signed OLDER manifest (replay of a superseded release) is refused
    _publish(feed, make_release(tmp_path, upd.signer, "0.2.0", sequence=4))
    r = upd.post("/v1/admin/updates/check", json={}, headers=upd.admin)
    assert r.status_code == 400 and "rollback refused" in r.text
    # a version older than the installed one is refused too even with a high sequence
    _publish(feed, make_release(tmp_path, upd.signer, "0.0.9", sequence=99))
    assert "rollback refused" in upd.post("/v1/admin/updates/check", json={}, headers=upd.admin).text
    last = upd.get("/v1/admin/updates", headers=upd.admin).json()["last_check"]
    assert last["ok"] is False


def test_check_audit_and_failure_reporting(upd, feed, tmp_path):
    feed.files.clear()
    r = upd.post("/v1/admin/updates/check", json={}, headers=upd.admin)
    assert r.status_code == 503 and "HTTP 404" in r.text
    acts = [a for a in upd.ctx.audit.query(limit=20, action="admin.update_checked")]
    assert acts and acts[0]["outcome"] == "failure"


def test_redirect_to_http_downgrade_refused(upd, tmp_path):
    from tests.ops.feedserver import FeedServer

    srv = FeedServer(redirects={"/stable/manifest.json": "ftp://evil.example/x"}).start()
    try:
        _cfg(upd, feed_url=srv.url, allow_insecure_http=True)
        r = upd.post("/v1/admin/updates/check", json={}, headers=upd.admin)
        assert r.status_code == 503 and "refusing" in r.text
    finally:
        srv.stop()


def test_proxy_is_used(upd, tmp_path):
    rel = make_release(tmp_path, upd.signer, "0.2.0")
    srv = FeedServer(feed_files(rel)).start()
    proxy = ForwardProxy().start()
    try:
        _cfg(upd, feed_url=f"http://localhost:{srv.port}", allow_insecure_http=True, proxy=proxy.url)
        r = upd.post("/v1/admin/updates/check", json={}, headers=upd.admin)
        assert r.status_code == 200, r.text
        assert any("/stable/manifest.json" in s for s in proxy.seen), proxy.seen
    finally:
        srv.stop()
        proxy.stop()


def test_custom_ca_bundle_for_https_feed(upd, tmp_path):
    ctx = upd.ctx
    crt, key = ctx.pki.ensure_server_cert(["localhost", "127.0.0.1"])
    rel = make_release(tmp_path, upd.signer, "0.2.0")
    srv = FeedServer(feed_files(rel), tls=(str(crt), str(key))).start()
    try:
        _cfg(upd, feed_url=f"https://localhost:{srv.port}")
        r = upd.post("/v1/admin/updates/check", json={}, headers=upd.admin)
        assert r.status_code == 503 and ("certificate" in r.text.lower() or "unreach" in r.text.lower()), r.text  # unknown CA -> TLS verification fails closed
        ca = tmp_path / "ca.pem"
        ca.write_text(ctx.pki.ca_pem)
        _cfg(upd, ca_bundle=str(ca))
        r = upd.post("/v1/admin/updates/check", json={}, headers=upd.admin)
        assert r.status_code == 200 and r.json()["update_available"], r.text
    finally:
        srv.stop()


# ----------------------------------------------------------------------------- offline bundle
def test_offline_bundle_import_and_attacks(upd, tmp_path):
    rel = make_release(tmp_path, upd.signer, "0.2.0", sequence=3, channel="offline")
    ok = make_bundle(tmp_path, rel)
    r = upd.post("/v1/admin/updates/import", files={"file": ("bundle.tar.gz", ok.read_bytes())}, headers=upd.admin)
    assert r.status_code == 201, r.text
    assert r.json()["state"] == "staged" and r.json()["source"].startswith("offline-bundle:")
    # tampered artifact
    bad = make_bundle(tmp_path, rel, name="bad.tar.gz", tamper_wheel=True)
    assert upd.post("/v1/admin/updates/import", files={"file": ("b", bad.read_bytes())}, headers=upd.admin).status_code == 400
    # missing artifact
    nourl = make_release(tmp_path, upd.signer, "0.2.0", sequence=3, channel="offline", with_url=False)
    miss = make_bundle(tmp_path, nourl, name="miss.tar.gz", drop_wheel=True)
    assert upd.post("/v1/admin/updates/import", files={"file": ("b", miss.read_bytes())}, headers=upd.admin).status_code == 400
    # unsigned / untrusted
    rel2 = make_release(tmp_path, upd.signer, "0.3.0", sequence=4, channel="offline", sign_with=[M.generate_keypair()[0]])
    assert upd.post("/v1/admin/updates/import", files={"file": ("b", make_bundle(tmp_path, rel2, name="u.tar.gz").read_bytes())}, headers=upd.admin).status_code == 400
    # malicious members
    for member in (tarfile.TarInfo("../../escape.txt"), tarfile.TarInfo("/etc/cron.d/evil")):
        member.size = 3
        evil = make_bundle(tmp_path, rel, name="evil.tar.gz", extra_members=[(member, b"bad")])
        r = upd.post("/v1/admin/updates/import", files={"file": ("b", evil.read_bytes())}, headers=upd.admin)
        assert r.status_code == 400 and "unsafe" in r.text, r.text
    link = tarfile.TarInfo("link")
    link.type = tarfile.SYMTYPE
    link.linkname = "/etc/passwd"
    r = upd.post("/v1/admin/updates/import", files={"file": ("b", make_bundle(tmp_path, rel, name="l.tar.gz", extra_members=[(link, None)]).read_bytes())}, headers=upd.admin)
    assert r.status_code == 400 and "regular" in r.text
    assert not (tmp_path / "escape.txt").exists()
    assert upd.post("/v1/admin/updates/import", files={"file": ("b", b"definitely not a tar")}, headers=upd.admin).status_code == 400
    # replay of an older bundle after a newer one was accepted
    newer = make_release(tmp_path, upd.signer, "0.4.0", sequence=9, channel="offline")
    assert upd.post("/v1/admin/updates/import", files={"file": ("n", make_bundle(tmp_path, newer, name="n.tar.gz").read_bytes())}, headers=upd.admin).status_code == 201
    old = make_release(tmp_path, upd.signer, "0.3.5", sequence=5, channel="offline")
    r = upd.post("/v1/admin/updates/import", files={"file": ("o", make_bundle(tmp_path, old, name="o.tar.gz").read_bytes())}, headers=upd.admin)
    assert r.status_code == 400 and "rollback refused" in r.text
    # nothing leaked into the working directories
    assert not [p for p in upd.ctx.settings.path("updates").glob("bundle-*")]


# ----------------------------------------------------------------------------- trust management
def test_trust_store_permissions_and_revocation(upd, tmp_path):
    c = upd
    other = M.generate_keypair()
    import base64

    pub = base64.b64encode(other[1]).decode()
    # RuntimeAdmin has admin.update but must NOT be able to decide who is trusted to ship code
    pw = "Correct-Horse-9-battery"
    assert c.post("/v1/identity/users", json={"name": "rtadmin", "password": pw, "roles": [{"role": "RuntimeAdmin"}]}, headers=c.admin).status_code == 201
    h = {"Authorization": f"Bearer {c.post('/v1/identity/login', json={'username': 'rtadmin', 'password': pw}).json()['access_token']}"}
    assert c.get("/v1/admin/updates", headers=h).status_code == 200
    assert c.post("/v1/admin/updates/trust", json={"public_key": pub, "name": "evil"}, headers=h).status_code == 403
    assert c.delete(f"/v1/admin/updates/trust/{c.signer.kid}", headers=h).status_code == 403
    assert c.post("/v1/admin/updates/trust", json={"public_key": "AAAA", "name": "junk"}, headers=c.admin).status_code == 400
    # revoke the signer: previously valid manifests are no longer accepted
    rel = make_release(tmp_path, c.signer, "0.2.0")
    srv = FeedServer(feed_files(rel)).start()
    try:
        _cfg(c, feed_url=srv.url, allow_insecure_http=True)
        assert c.post("/v1/admin/updates/check", json={}, headers=c.admin).status_code == 200
        assert c.delete(f"/v1/admin/updates/trust/{c.signer.kid}", headers=c.admin).status_code == 204
        r = c.post("/v1/admin/updates/check", json={}, headers=c.admin)
        assert r.status_code == 400 and "revoked" in r.text
        # and the previously stored record can no longer be applied either
        uid = c.get("/v1/admin/updates", headers=c.admin).json()["items"][0]["id"]
        r = c.post(f"/v1/admin/updates/{uid}/apply", json={"confirm": "APPLY"}, headers=c.admin)
        assert r.status_code == 400
    finally:
        srv.stop()


def test_permissions_on_updates_api(upd):
    c = upd
    pw = "Correct-Horse-9-battery"
    for role in ("Viewer", "Contributor", "Operator", "SecurityAdmin"):
        assert c.post("/v1/identity/users", json={"name": f"u-{role.lower()}", "password": pw, "roles": [{"role": role}]}, headers=c.admin).status_code == 201
        h = {"Authorization": f"Bearer {c.post('/v1/identity/login', json={'username': f'u-{role.lower()}', 'password': pw}).json()['access_token']}"}
        for method, path, body in (("get", "/v1/admin/updates", None), ("put", "/v1/admin/updates/config", {}), ("post", "/v1/admin/updates/check", {}), ("get", "/v1/admin/updates/trust", None),
                                   ("post", "/v1/admin/updates/maintenance", {"on": True}), ("post", "/v1/admin/updates/upd_x/apply", {"confirm": "APPLY"})):
            kw = {"json": body} if body is not None else {}
            assert getattr(c, method)(path, headers=h, **kw).status_code == 403, (role, path)
    assert c.get("/v1/admin/updates").status_code == 401
