# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
# ruff: noqa: S105, S106
"""ADLS Gen2 and OneLake: config building, real signed requests against an in-process fake DFS server.

The fake verifies SharedKey signatures / SAS / bearer tokens for real, so these tests prove the SDK requests are
constructed and authenticated correctly.  Behaviour against genuine Azure endpoints is covered by ``@pytest.mark.live``
tests that only run when ``OLCF_LIVE_ADLS_*`` / ``OLCF_LIVE_ONELAKE_*`` variables are set.
"""

from __future__ import annotations

import os
import subprocess

import pytest

from olcf.core.errors import BadRequest, Conflict, NotFound, Unsupported
from olcf.storage.providers.adls import AdlsProvider
from olcf.storage.providers.onelake import ONELAKE_HOST, OneLakeProvider

pytest.importorskip("azure.storage.filedatalake")
from .fake_dfs import ACCOUNT, ACCOUNT_KEY, BEARER, SAS, FakeDfs  # noqa: E402

ROOT = "abfss://lake@devacct.dfs.core.windows.net/olcf"


def _cfg(fake: FakeDfs, **kw):
    return {"account_name": ACCOUNT, "filesystem": "lake", "endpoint": fake.endpoint, "auth": "account_key", **kw}


@pytest.fixture()
def dfs_key():
    f = FakeDfs(auth="key").start()
    yield f
    f.stop()


@pytest.fixture()
def dfs_sas():
    f = FakeDfs(auth="sas").start()
    yield f
    f.stop()


@pytest.fixture(scope="module")
def cert(tmp_path_factory):
    d = tmp_path_factory.mktemp("tls")
    subprocess.run(["openssl", "req", "-x509", "-newkey", "rsa:2048", "-nodes", "-keyout", str(d / "k.pem"), "-out", str(d / "c.pem"), "-days", "2",
                    "-subj", "/CN=localhost", "-addext", "subjectAltName=DNS:localhost,IP:127.0.0.1"], check=True, capture_output=True)
    return str(d / "c.pem"), str(d / "k.pem")


# ------------------------------------------------------------------------------------------- ADLS against the fake
def test_adls_filesystem_roundtrip_with_account_key(dfs_key):
    fs = AdlsProvider().filesystem(ROOT, _cfg(dfs_key), {"account_key": ACCOUNT_KEY})
    fs.mkdirs("Files/empty")
    fs.write_bytes("Files/a/b/one.txt", b"hello")
    fs.write_bytes("Files/a/two.txt", b"world!")
    assert fs.stat("Files/a/two.txt").size == 6
    assert fs.stat("Files/a").is_dir and fs.stat("Files/empty").is_dir
    assert {f.name: f.is_dir for f in fs.list("Files")} == {"a": True, "empty": True}
    assert {f.path for f in fs.list("Files/a", recursive=True)} == {"Files/a/b", "Files/a/b/one.txt", "Files/a/two.txt"}
    with fs.open_read("Files/a/two.txt", offset=1, length=3) as r:
        assert r.read() == b"orl"
    assert fs.read_bytes("Files/a/b/one.txt") == b"hello"
    assert fs.physical_uri("Files/a") == "abfss://lake@localhost/olcf/Files/a"
    fs.copy("Files/a", "Files/a2")
    assert fs.read_bytes("Files/a2/b/one.txt") == b"hello"
    fs.move("Files/a2", "Files/a3")
    assert not fs.exists("Files/a2") and fs.exists("Files/a3/two.txt")
    fs.move("Files/a3/two.txt", "Files/a3/three.txt")
    assert fs.read_bytes("Files/a3/three.txt") == b"world!"
    with pytest.raises(Conflict):
        fs.move("Files/a3/three.txt", "Files/a/two.txt")
    with pytest.raises(Conflict):
        fs.delete("Files/a3")
    assert fs.delete("Files/a3", recursive=True) >= 2
    with pytest.raises(NotFound):
        fs.stat("Files/a3")
    assert fs.delete("Files/missing") == 0
    with pytest.raises(NotFound):
        fs.open_read("Files/missing")
    # every request carried a valid SharedKey signature (an invalid one would have produced a 403)
    assert dfs_key.log and all(s < 400 or s in (404, 409, 416) for _, _, s in dfs_key.log), dfs_key.log
    assert all(s != 403 for _, _, s in dfs_key.log)


def test_adls_large_write_is_appended_in_chunks_and_aborts_cleanly(dfs_key, monkeypatch):
    import olcf.storage.providers.adls as mod

    monkeypatch.setattr(mod, "APPEND_CHUNK", 64 * 1024)
    fs = AdlsProvider().filesystem(ROOT, _cfg(dfs_key), {"account_key": ACCOUNT_KEY})
    blob = bytes(range(256)) * 1024  # 256 KiB -> 4 appends
    with fs.open_write("Files/big.bin") as w:
        for i in range(0, len(blob), 10_000):
            w.write(blob[i:i + 10_000])
    assert fs.read_bytes("Files/big.bin") == blob
    assert sum(1 for m, _, s in dfs_key.log if m == "PATCH") >= 5   # >=4 appends + flush
    w = fs.open_write("Files/aborted.bin")
    w.write(b"x" * 100_000)
    w.abort()
    assert not fs.exists("Files/aborted.bin")


def test_adls_sas_authentication_and_rejection(dfs_sas):
    p = AdlsProvider()
    cfg = _cfg(dfs_sas, auth="sas")
    fs = p.filesystem(ROOT, cfg, {"sas_token": "?" + SAS})
    fs.write_bytes("Files/x.txt", b"sas ok")
    assert fs.read_bytes("Files/x.txt") == b"sas ok"
    bad = p.test(ROOT, cfg, {"sas_token": "sv=2022-11-02&sig=wrong"})
    assert bad["ok"] is False
    auth = next(s for s in bad["steps"] if s["name"] == "auth")
    assert not auth["ok"] and "rejected the credentials" in auth["detail"]
    assert [s["name"] for s in bad["steps"] if s["ok"]] == ["resolve DNS", "connect"]
    assert "wrong" not in str(bad)


def test_adls_connection_test_steps(dfs_key):
    p = AdlsProvider()
    ok = p.test(ROOT, _cfg(dfs_key), {"account_key": ACCOUNT_KEY})
    assert ok["ok"], ok
    assert [s["name"] for s in ok["steps"]] == ["resolve DNS", "connect", "auth", "list", "write", "read", "delete"]
    assert ACCOUNT_KEY not in str(ok)
    wrong_key = p.test(ROOT, _cfg(dfs_key), {"account_key": "d3Jvbmc="})
    assert wrong_key["ok"] is False and "HTTP 403" in next(s for s in wrong_key["steps"] if s["name"] == "auth")["detail"]
    no_container = p.test("abfss://nope@devacct.dfs.core.windows.net/x", _cfg(dfs_key, filesystem="nope"), {"account_key": ACCOUNT_KEY})
    assert no_container["ok"] is False and "does not exist" in next(s for s in no_container["steps"] if s["name"] == "auth")["detail"]
    dead = p.test(ROOT, {**_cfg(dfs_key), "endpoint": "http://127.0.0.1:1/devacct"}, {"account_key": ACCOUNT_KEY})
    assert dead["ok"] is False and not dead["steps"][1]["ok"]


def test_adls_path_traversal_is_rejected_before_any_request(dfs_key):
    fs = AdlsProvider().filesystem(ROOT, _cfg(dfs_key), {"account_key": ACCOUNT_KEY})
    before = len(dfs_key.log)
    for bad in ("../x", "Files/../../x", "/abs"):
        with pytest.raises(BadRequest):
            fs.write_bytes(bad, b"x")
    assert len(dfs_key.log) == before


# ------------------------------------------------------------------------------------------- config building (ADLS)
HOST = "devacct.dfs.core.windows.net"
SP = {"auth": "service_principal", "tenant_id": "tid", "client_id": "cid"}


def test_adls_spark_conf_per_auth_mode():
    p = AdlsProvider()
    k = p.spark_conf(ROOT, {"auth": "account_key"}, {"account_key": "KEY"})
    assert k[f"spark.hadoop.fs.azure.account.key.{HOST}"] == "KEY" and k["spark.hadoop.fs.abfss.impl"].endswith("SecureAzureBlobFileSystem")
    s = p.spark_conf(ROOT, {"auth": "sas"}, {"sas_token": "?sv=1&sig=S"})
    assert s[f"spark.hadoop.fs.azure.account.auth.type.{HOST}"] == "SAS" and s[f"spark.hadoop.fs.azure.sas.fixed.token.{HOST}"] == "sv=1&sig=S"
    o = p.spark_conf(ROOT, SP, {"client_secret": "CS"})
    assert o[f"spark.hadoop.fs.azure.account.auth.type.{HOST}"] == "OAuth"
    assert o[f"spark.hadoop.fs.azure.account.oauth.provider.type.{HOST}"].endswith("ClientCredsTokenProvider")
    assert o[f"spark.hadoop.fs.azure.account.oauth2.client.id.{HOST}"] == "cid"
    assert o[f"spark.hadoop.fs.azure.account.oauth2.client.secret.{HOST}"] == "CS"
    assert o[f"spark.hadoop.fs.azure.account.oauth2.client.endpoint.{HOST}"] == "https://login.microsoftonline.com/tid/oauth2/token"
    sov = p.spark_conf(ROOT, {**SP, "authority_host": "https://login.chinacloudapi.cn/"}, {"client_secret": "CS"})
    assert sov[f"spark.hadoop.fs.azure.account.oauth2.client.endpoint.{HOST}"] == "https://login.chinacloudapi.cn/tid/oauth2/token"
    mi = p.spark_conf(ROOT, {"auth": "managed_identity", "client_id": "uami"}, None)
    assert mi[f"spark.hadoop.fs.azure.account.oauth.provider.type.{HOST}"].endswith("MsiTokenProvider")
    assert mi[f"spark.hadoop.fs.azure.account.oauth2.client.id.{HOST}"] == "uami"
    with pytest.raises(Unsupported):
        p.spark_conf(ROOT, {"auth": "default"}, None)
    with pytest.raises(BadRequest):
        p.spark_conf(ROOT, SP, None)
    with pytest.raises(BadRequest):
        p.spark_conf(ROOT, {"auth": "bogus"}, None)


def test_adls_delta_options_env_and_iceberg_props(monkeypatch):
    p = AdlsProvider()
    assert p.delta_storage_options(ROOT, {"auth": "account_key"}, {"account_key": "KEY"}) == {"azure_storage_account_name": "devacct", "azure_storage_account_key": "KEY"}
    assert p.delta_storage_options(ROOT, {"auth": "sas"}, {"sas_token": "?a=b"})["azure_storage_sas_token"] == "a=b"
    sp = p.delta_storage_options(ROOT, SP, {"client_secret": "CS"})
    assert sp["azure_storage_client_id"] == "cid" and sp["azure_storage_client_secret"] == "CS" and sp["azure_storage_tenant_id"] == "tid"
    emu = p.delta_storage_options(ROOT, {"auth": "account_key", "endpoint": "http://127.0.0.1:10000/devacct"}, {"account_key": "K"})
    assert emu["azure_storage_endpoint"].startswith("http://") and emu["allow_http"] == "true"
    monkeypatch.setattr(AdlsProvider, "_acquire_token", lambda self, r, c, s: "JIT-TOKEN")
    assert p.delta_storage_options(ROOT, {"auth": "managed_identity"}, None)["azure_storage_token"] == "JIT-TOKEN"
    assert p.env(ROOT, SP, {"client_secret": "CS"})["AZURE_CLIENT_SECRET"] == "CS"
    ice = p.iceberg_properties(ROOT, {"auth": "account_key"}, {"account_key": "KEY"})
    assert ice == {"adls.account-name": "devacct", "adls.account-key": "KEY"}


def test_adls_validation_and_claims():
    p = AdlsProvider()
    assert p.validate_root(ROOT) == ROOT
    with pytest.raises(BadRequest):
        p.validate_root("abfss://noat.dfs.core.windows.net/x")
    with pytest.raises(BadRequest):
        p.validate_config({"auth": "service_principal"})
    with pytest.raises(BadRequest):
        p.validate_config({"auth": "account_key", "endpoint": "ftp://x"})
    assert p.claims(ROOT) and not p.claims(f"abfss://ws@{ONELAKE_HOST}/x.Lakehouse")
    assert OneLakeProvider().claims(f"abfss://ws@{ONELAKE_HOST}/x.Lakehouse")


# ------------------------------------------------------------------------------------------- OneLake
OL_ROOT = f"abfss://ws1@{ONELAKE_HOST}/Sales.Lakehouse"


def test_onelake_root_building_and_validation():
    p = OneLakeProvider()
    assert p.root_from_config({"workspace": "ws1", "lakehouse": "Sales.Lakehouse"}) == OL_ROOT
    assert p.validate_root(f"https://{ONELAKE_HOST}/ws1/Sales.Lakehouse") == OL_ROOT
    assert p.validate_root(f"https://{ONELAKE_HOST}/ws1/Sales.Lakehouse/Files/x") == OL_ROOT + "/Files/x"
    with pytest.raises(BadRequest):
        p.validate_root(f"abfss://ws1@{ONELAKE_HOST}")
    with pytest.raises(BadRequest):
        p.validate_root("abfss://ws1@acct.dfs.core.windows.net/x")
    with pytest.raises(BadRequest):
        p.root_from_config({"workspace": "ws1"} if False else {"workspace": "", "lakehouse": "x"})
    with pytest.raises(Unsupported):
        p.validate_config({"auth": "account_key"})
    with pytest.raises(Unsupported):
        p.validate_config({"auth": "sas"})
    assert p.physical_uri(OL_ROOT, "Tables/t") == f"abfss://ws1@{ONELAKE_HOST}/Sales.Lakehouse/Tables/t"


def test_onelake_spark_conf_delta_options_and_unsupported_modes():
    p = OneLakeProvider()
    c = p.spark_conf(OL_ROOT, SP, {"client_secret": "CS"})
    assert c[f"spark.hadoop.fs.azure.account.oauth2.client.id.{ONELAKE_HOST}"] == "cid"
    assert c[f"spark.hadoop.fs.azure.account.oauth2.client.endpoint.{ONELAKE_HOST}"] == "https://login.microsoftonline.com/tid/oauth2/token"
    assert c[f"spark.hadoop.fs.azure.account.auth.type.{ONELAKE_HOST}"] == "OAuth"
    d = p.delta_storage_options(OL_ROOT, SP, {"client_secret": "CS"})
    assert d["azure_use_fabric_endpoint"] == "true" and d["azure_storage_account_name"] == "onelake" and d["azure_storage_client_secret"] == "CS"
    assert "azure_storage_endpoint" not in d
    assert p.delta_storage_options(OL_ROOT, {"auth": "user_token"}, {"access_token": "TOK"})["azure_storage_token"] == "TOK"
    regional = p.delta_storage_options(OL_ROOT, {**SP, "endpoint": "https://westus-onelake.dfs.fabric.microsoft.com"}, {"client_secret": "CS"})
    assert regional["azure_storage_endpoint"].startswith("https://westus-onelake")
    with pytest.raises(Unsupported):
        p.spark_conf(OL_ROOT, {"auth": "user_token"}, {"access_token": "TOK"})
    with pytest.raises(Unsupported):
        p.iceberg_properties(OL_ROOT, SP, {"client_secret": "CS"})
    with pytest.raises(BadRequest):
        p.credential(OL_ROOT, {"auth": "user_token"}, None)
    assert p.env(OL_ROOT, SP, {"client_secret": "CS"})["AZURE_STORAGE_ACCOUNT"] == "onelake"


def test_onelake_filesystem_with_bearer_token_over_tls(cert):
    fake = FakeDfs(containers=("ws1",), auth="bearer", certfile=cert[0], keyfile=cert[1]).start()
    try:
        p = OneLakeProvider()
        cfg = {"auth": "user_token", "endpoint": fake.endpoint, "ca_bundle": cert[0]}
        fs = p.filesystem(OL_ROOT, cfg, {"access_token": BEARER})
        fs.mkdirs("Files/raw")
        fs.write_bytes("Files/raw/a.csv", b"a,b\n1,2\n")
        assert fs.read_bytes("Files/raw/a.csv") == b"a,b\n1,2\n"
        assert fs.provider == "onelake" and {f.name for f in fs.list("Files")} == {"raw"}
        # the item folder is the prefix inside the workspace container
        assert "Sales.Lakehouse/Files/raw/a.csv" in fake.files["ws1"]
        ok = p.test(OL_ROOT, cfg, {"access_token": BEARER})
        assert ok["ok"], ok
        assert [s["name"] for s in ok["steps"]] == ["resolve DNS", "connect", "auth", "list", "write", "read", "delete"]
        assert "TCP+TLS" in ok["steps"][1]["detail"] and BEARER not in str(ok)
        bad = p.test(OL_ROOT, cfg, {"access_token": "expired"})
        assert bad["ok"] is False and "HTTP 403" in next(s for s in bad["steps"] if s["name"] == "auth")["detail"]
        # without the CA the TLS handshake itself fails at the connect step
        untrusted = p.test(OL_ROOT, {"auth": "user_token", "endpoint": fake.endpoint}, {"access_token": BEARER})
        assert untrusted["ok"] is False and not untrusted["steps"][1]["ok"]
    finally:
        fake.stop()


# ------------------------------------------------------------------------------------------- live (never in CI)
@pytest.mark.live
@pytest.mark.skipif(not os.environ.get("OLCF_LIVE_ADLS_ROOT"), reason="set OLCF_LIVE_ADLS_ROOT/_ACCOUNT_KEY to run against real ADLS")
def test_live_adls_connection():  # pragma: no cover
    cfg = {"auth": "account_key"}
    res = AdlsProvider().test(os.environ["OLCF_LIVE_ADLS_ROOT"], cfg, {"account_key": os.environ["OLCF_LIVE_ADLS_ACCOUNT_KEY"]})
    assert res["ok"], res


@pytest.mark.live
@pytest.mark.skipif(not os.environ.get("OLCF_LIVE_ONELAKE_ROOT"), reason="set OLCF_LIVE_ONELAKE_ROOT/_TENANT_ID/_CLIENT_ID/_CLIENT_SECRET")
def test_live_onelake_connection():  # pragma: no cover
    cfg = {"auth": "service_principal", "tenant_id": os.environ["OLCF_LIVE_ONELAKE_TENANT_ID"], "client_id": os.environ["OLCF_LIVE_ONELAKE_CLIENT_ID"]}
    res = OneLakeProvider().test(os.environ["OLCF_LIVE_ONELAKE_ROOT"], cfg, {"client_secret": os.environ["OLCF_LIVE_ONELAKE_CLIENT_SECRET"]})
    assert res["ok"], res
