# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Off-host backup targets against real servers: S3 (moto HTTP server, path-style custom endpoint), SFTP (paramiko server), lakehouse (local provider)."""

from __future__ import annotations

import sys

import pytest

if "/tmp/pylib" not in sys.path:  # sandbox convenience: optional libs live there  # noqa: S108 - deliberate fixed path: the test asserts on / needs exactly this location
    sys.path.append("/tmp/pylib")  # noqa: S108 - deliberate fixed path: the test asserts on / needs exactly this location

AK, SK = "backup-access-key", "backup-secret-key-0123456789"


def _backup(c, **body):
    r = c.post("/v1/admin/backup?wait=true", json=body, headers=c.admin)
    assert r.status_code == 202, r.text
    return r.json()["backup"]


@pytest.fixture(scope="module")
def s3_url():
    boto3 = pytest.importorskip("boto3")
    server = pytest.importorskip("moto.server")
    srv = server.ThreadedMotoServer(port=0, verbose=False)
    srv.start()
    host, port = srv.get_host_and_port()
    url = f"http://{host}:{port}"
    boto3.client("s3", endpoint_url=url, aws_access_key_id=AK, aws_secret_access_key=SK, region_name="us-east-1").create_bucket(Bucket="olcf-backups")
    yield url
    srv.stop()


def test_s3_target_end_to_end(ops_app, s3_url):
    import boto3

    c = ops_app
    r = c.post("/v1/admin/backup/targets", headers=c.admin, json={
        "name": "minio", "type": "s3", "config": {"bucket": "olcf-backups", "prefix": "prod/olcf", "endpoint_url": s3_url, "path_style": True, "region": "us-east-1"},
        "secret": {"access_key_id": AK, "secret_access_key": SK}})
    assert r.status_code == 201, r.text
    assert SK not in r.text and r.json()["has_secret"] is True
    t = c.post("/v1/admin/backup/targets/minio/test", headers=c.admin).json()
    assert t["ok"], t
    c.put("/v1/admin/backup/config", json={"targets": ["minio"]}, headers=c.admin)
    bk = _backup(c)
    assert bk["targets"][0]["ok"] and bk["targets"][0]["uri"].startswith("s3://olcf-backups/prod/olcf/olcf-backup-")
    s3 = boto3.client("s3", endpoint_url=s3_url, aws_access_key_id=AK, aws_secret_access_key=SK, region_name="us-east-1")
    objs = s3.list_objects_v2(Bucket="olcf-backups", Prefix="prod/olcf/")["Contents"]
    assert [o["Key"].rsplit("/", 1)[-1] for o in objs] == [bk["filename"]]
    # local file is gone, restore-test downloads from the target and passes
    c.ctx.settings.path("backups", bk["filename"]).unlink()
    res = c.post(f"/v1/admin/backup/{bk['id']}/restore-test?wait=true", json={}, headers=c.admin).json()["operation"]
    assert res["state"] == "succeeded" and res["result"]["ok"], res
    # secrets never in the stored target row or audit
    assert SK not in str(c.get("/v1/admin/backup/targets/minio", headers=c.admin).json())
    assert not any(SK in str(a) for a in c.ctx.audit.query(limit=500))
    # a missing bucket is diagnosed with a remediation (moto does not enforce credentials, so that is the failure we can provoke)
    c.patch("/v1/admin/backup/targets/minio", json={"config": {"bucket": "does-not-exist", "endpoint_url": s3_url, "path_style": True, "region": "us-east-1"}}, headers=c.admin)
    bad = c.post("/v1/admin/backup/targets/minio/test", headers=c.admin).json()
    # Do NOT search ``bad["detail"]`` for "NoSuchBucket": that text is clipped to 300 characters and starts with the absolute path of the probe file (``Failed to upload <tmp>/...``),
    # so under a long temporary directory the token is cut off. The remediation is derived from the FULL provider error and is independent of any path.
    first = bad["steps"][0]
    assert not bad["ok"] and first["name"] == "write" and first["ok"] is False
    assert first["code"] == "NoSuchBucket"
    assert first["remediation"] == "create the bucket or fix the bucket name"


def test_sftp_target_end_to_end(ops_app, tmp_path):
    pytest.importorskip("paramiko")
    from tests.ops.sftpserver import SftpTestServer

    root = tmp_path / "sftp-root"
    root.mkdir()
    srv = SftpTestServer(str(root)).start()
    try:
        c = ops_app
        cfg = {"host": "127.0.0.1", "port": srv.port, "username": srv.user, "path": "/olcf", "host_key_sha256": srv.fingerprint}
        # no host key -> refused at configuration time (no trust-on-first-use)
        no_hk = c.post("/v1/admin/backup/targets", headers=c.admin, json={"name": "sftp0", "type": "sftp", "config": {k: v for k, v in cfg.items() if k != "host_key_sha256"}, "secret": {"password": srv.password}})
        assert no_hk.status_code == 201  # shape is checked at use time; the test below proves it is refused
        assert c.post("/v1/admin/backup/targets/sftp0/test", headers=c.admin).json()["ok"] is False
        r = c.post("/v1/admin/backup/targets", headers=c.admin, json={"name": "sftp1", "type": "sftp", "config": cfg, "secret": {"password": srv.password}})
        assert r.status_code == 201, r.text
        t = c.post("/v1/admin/backup/targets/sftp1/test", headers=c.admin).json()
        assert t["ok"], t
        c.put("/v1/admin/backup/config", json={"targets": ["sftp1"]}, headers=c.admin)
        bk = _backup(c)
        assert bk["targets"][0]["ok"], bk["targets"]
        assert (root / "olcf" / bk["filename"]).is_file() and not list((root / "olcf").glob("*.partial"))
        files = c.get("/v1/admin/backup/targets/sftp1/files", headers=c.admin).json()["items"]
        assert [f["name"] for f in files] == [bk["filename"]]
        # a MITM presenting another host key is rejected
        c.patch("/v1/admin/backup/targets/sftp1", json={"config": {**cfg, "host_key_sha256": "SHA256:" + "A" * 43}}, headers=c.admin)
        mitm = c.post("/v1/admin/backup/targets/sftp1/test", headers=c.admin).json()
        assert not mitm["ok"] and "host key" in mitm["detail"]
        # wrong password
        c.patch("/v1/admin/backup/targets/sftp1", json={"config": cfg, "secret": {"password": "nope"}}, headers=c.admin)
        assert not c.post("/v1/admin/backup/targets/sftp1/test", headers=c.admin).json()["ok"]
    finally:
        srv.stop()


def test_lakehouse_target(tmp_path, settings):
    from fastapi.testclient import TestClient

    from olcf.api.app import create_app

    app = create_app(settings, module_names=["olcf.api.core.module", "olcf.storage.module", "olcf.ops.module"])
    with TestClient(app) as c:
        pw = app.state.ctx.settings.path("secrets", "bootstrap-admin.txt").read_text().split("password=")[1].strip()
        c.admin = {"Authorization": f"Bearer {c.post('/v1/identity/login', json={'username': 'admin', 'password': pw}).json()['access_token']}"}
        assert c.post("/v1/workspaces", json={"name": "ops"}, headers=c.admin).status_code == 201
        assert c.post("/v1/workspaces/ops/lakehouses", json={"name": "bk"}, headers=c.admin).status_code == 201
        r = c.post("/v1/admin/backup/targets", headers=c.admin, json={"name": "lake", "type": "lakehouse", "config": {"lakehouse": "ops/bk", "prefix": "Files/olcf-backups"}})
        assert r.status_code == 201, r.text
        t = c.post("/v1/admin/backup/targets/lake/test", headers=c.admin).json()
        assert t["ok"], t
        c.put("/v1/admin/backup/config", json={"targets": ["lake"]}, headers=c.admin)
        bk = c.post("/v1/admin/backup?wait=true", json={}, headers=c.admin).json()["backup"]
        assert bk["targets"][0]["ok"], bk["targets"]
        files = c.get("/v1/admin/backup/targets/lake/files", headers=c.admin).json()["items"]
        assert [f["name"] for f in files] == [bk["filename"]]
        c.post("/v1/admin/backup/targets", headers=c.admin, json={"name": "lake2", "type": "lakehouse", "config": {"lakehouse": "ops/bk", "prefix": "Files/../../x"}})
        esc = c.post("/v1/admin/backup/targets/lake2/test", headers=c.admin).json()
        assert esc["ok"] is False


@pytest.mark.parametrize("name", ["../x.olcfbak", "a/b.olcfbak", "", "x" * 300, ".hidden", "a\x00b", "/etc/passwd"])
def test_target_name_validation(name):
    from olcf.core.errors import BadRequest
    from olcf.ops.backup_targets import safe_name

    with pytest.raises(BadRequest):
        safe_name(name)
