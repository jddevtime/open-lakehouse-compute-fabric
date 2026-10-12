# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
# ruff: noqa: S105, S106  (fixtures use obviously fake credentials)
"""Transport behaviour of the SDK against a scripted ``httpx.MockTransport``: retries, back-off, idempotency keys, ETag headers,
proxy / CA environment handling, path escaping and redaction."""

from __future__ import annotations

import ssl

import httpx
import pytest

from olcf.sdk import Client, OlcfConnectionError, RateLimitError, ServerError
from olcf.sdk import _core as core


def make_client(handler, **kw) -> tuple[Client, list[httpx.Request]]:
    seen: list[httpx.Request] = []

    def wrapped(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        return handler(request, len(seen))

    http = httpx.Client(transport=httpx.MockTransport(wrapped), base_url="http://olcf.test")
    return Client("http://olcf.test", http_client=http, token="olcf_pat_secretsecretsecret", **kw), seen


@pytest.fixture()
def sleeps(monkeypatch):
    rec: list[float] = []
    monkeypatch.setattr("olcf.sdk.client.time.sleep", rec.append)
    return rec


def test_get_retries_503_then_succeeds(sleeps):
    def h(req, n):
        return httpx.Response(503, json={"error": {"code": "unavailable", "message": "starting"}}) if n < 3 else httpx.Response(200, json={"status": "ok"})

    c, seen = make_client(h, max_retries=3)
    assert c.health()["status"] == "ok"
    assert len(seen) == 3 and len(sleeps) == 2 and all(0 < s <= 20 for s in sleeps)


def test_post_without_idempotency_is_not_retried_on_5xx(sleeps):
    c, seen = make_client(lambda r, n: httpx.Response(503, json={"error": {"code": "unavailable", "message": "x"}}), max_retries=3)
    with pytest.raises(ServerError):
        c.call("POST", "/workspaces", json={"name": "a"})
    assert len(seen) == 1 and not sleeps


def test_post_with_idempotency_key_is_retried_with_same_key(sleeps):
    def h(req, n):
        return httpx.Response(502, json={"error": {"code": "bad_gateway", "message": "x"}}) if n < 2 else httpx.Response(202, json={"id": "job_1"})

    c, seen = make_client(h, max_retries=3)
    assert c.jobs.submit(kind="script", workspace_id="wks_1")["id"] == "job_1"      # jobs.submit declares idem=True
    keys = {r.headers["idempotency-key"] for r in seen}
    assert len(seen) == 2 and len(keys) == 1


def test_429_honours_retry_after(sleeps):
    def h(req, n):
        return httpx.Response(429, headers={"Retry-After": "7"}, json={"error": {"code": "rate_limited", "message": "slow down"}}) if n == 1 else httpx.Response(200, json={})

    c, seen = make_client(h, max_retries=2)
    c.call("POST", "/workspaces", json={})                                         # 429 is retried for every method
    assert sleeps == [7.0]
    c2, _ = make_client(lambda r, n: httpx.Response(429, headers={"Retry-After": "2"}, json={"error": {"code": "rate_limited", "message": "x"}}), max_retries=0)
    with pytest.raises(RateLimitError) as ei:
        c2.health()
    assert ei.value.retry_after == 2.0


def test_connect_errors_are_retried_then_wrapped(sleeps):
    def h(req, n):
        raise httpx.ConnectError("boom", request=req)

    c, seen = make_client(h, max_retries=2)
    with pytest.raises(OlcfConnectionError) as ei:
        c.call("POST", "/workspaces", json={})                                      # never reached the server: safe even for POST
    assert len(seen) == 3 and "olcf.test" in str(ei.value)


def test_read_timeout_not_retried_for_plain_post(sleeps):
    def h(req, n):
        raise httpx.ReadTimeout("slow", request=req)

    c, seen = make_client(h, max_retries=3)
    with pytest.raises(OlcfConnectionError):
        c.call("POST", "/workspaces", json={})
    assert len(seen) == 1


def test_headers_etag_and_auth_flags():
    c, seen = make_client(lambda r, n: httpx.Response(200, json={"id": "x"}, headers={"ETag": '"abc123"'}))
    res = c.items.update("itm_1", if_match="abc123", name="n")
    assert res.etag == "abc123"
    req = seen[0]
    assert req.headers["if-match"] == '"abc123"' and req.headers["authorization"].startswith("Bearer ")
    assert req.headers["user-agent"].startswith("olcf-sdk/")
    c.admin.health()
    assert "authorization" not in seen[1].headers                                  # auth=False endpoints send no credentials


def test_path_variables_are_percent_encoded():
    c, seen = make_client(lambda r, n: httpx.Response(200, json={}))
    c.workspaces.get("a b/c?d")
    assert seen[0].url.raw_path.decode().startswith("/v1/workspaces/a%20b%2Fc%3Fd")
    assert core.build_path("/jobs/{id}", {"id": "j"}) == "/v1/jobs/j" and core.build_path("/mcp", {}) == "/mcp"


def test_query_params_drop_none_and_render_booleans():
    c, seen = make_client(lambda r, n: httpx.Response(200, json={"items": [], "next_cursor": None}))
    c.jobs.list(state="running", kind=None, limit=5)
    assert dict(seen[0].url.params) == {"state": "running", "limit": "5"}
    c.workspaces.list(deleted=True)
    assert dict(seen[1].url.params)["deleted"] == "true"


def test_non_json_errors_are_wrapped():
    c, _ = make_client(lambda r, n: httpx.Response(502, text="<html>Bad gateway</html>"), max_retries=0)
    with pytest.raises(ServerError) as ei:
        c.health()
    assert ei.value.status == 502 and "Bad gateway" in ei.value.message


def test_ca_bundle_and_proxy_environment(monkeypatch, tmp_path):
    from cryptography import x509
    from cryptography.hazmat.primitives import hashes, serialization
    from cryptography.hazmat.primitives.asymmetric import ec
    from cryptography.x509.oid import NameOID

    key = ec.generate_private_key(ec.SECP256R1())
    name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "test-ca")])
    import datetime as dt
    cert = (x509.CertificateBuilder().subject_name(name).issuer_name(name).public_key(key.public_key()).serial_number(1)
            .not_valid_before(dt.datetime.now(dt.UTC) - dt.timedelta(days=1)).not_valid_after(dt.datetime.now(dt.UTC) + dt.timedelta(days=1))
            .add_extension(x509.BasicConstraints(ca=True, path_length=None), critical=True).sign(key, hashes.SHA256()))
    pem = tmp_path / "ca.pem"
    pem.write_bytes(cert.public_bytes(serialization.Encoding.PEM))
    monkeypatch.setenv("OLCF_CA_BUNDLE", str(pem))
    assert isinstance(core.resolve_verify(None), ssl.SSLContext)
    for var in core.CA_ENV_VARS:
        monkeypatch.delenv(var, raising=False)
    assert core.resolve_verify(None) is True and core.resolve_verify(False) is False
    # loopback never goes through a proxy; remote hosts honour the environment / explicit proxy
    monkeypatch.setenv("HTTPS_PROXY", "http://proxy.example:3128")
    local = Client("http://127.0.0.1:8470")
    assert local.cfg.httpx_kwargs()["trust_env"] is False
    remote = Client("https://olcf.example.com", proxy="http://px:8080")
    kw = remote.cfg.httpx_kwargs()
    assert kw["trust_env"] is True and kw["proxy"] == "http://px:8080"
    assert core.normalise_base_url("olcf.example.com:8470/v1/") == "http://olcf.example.com:8470"


def test_env_configuration(monkeypatch):
    monkeypatch.setenv("OLCF_URL", "https://olcf.example.com/")
    monkeypatch.setenv("OLCF_TOKEN", "olcf_pat_envtoken_envtoken")
    c = Client.from_env()
    assert c.base_url == "https://olcf.example.com" and c.token == "olcf_pat_envtoken_envtoken"
    assert "envtoken" not in repr(c)


def test_context_store_roundtrip_permissions_and_from_context(tmp_path):
    import os
    import stat

    from olcf.sdk.contexts import ContextStore

    store = ContextStore(tmp_path / "c" / "contexts.json")
    store.set("prod", url="https://olcf.prod:8470", token="olcf_pat_abc_abcdefgh", refresh_token="olcf_ref_x_yyyyyyyy")
    store.set("dev", url="http://127.0.0.1:8470", token="t2", make_current=False)
    if os.name == "posix":
        assert stat.S_IMODE((tmp_path / "c" / "contexts.json").stat().st_mode) == 0o600
        assert stat.S_IMODE((tmp_path / "c").stat().st_mode) == 0o700
    again = ContextStore(tmp_path / "c" / "contexts.json")
    assert again.current == "prod" and again.names() == ["dev", "prod"]
    assert again.get()["token"] == "olcf_pat_abc_abcdefgh" and again.get("dev")["url"].startswith("http://127")
    again.clear_credentials("prod")
    assert "token" not in again.get("prod") and again.get("dev")["token"] == "t2"
    again.delete("prod")
    assert again.current == "dev"


def test_async_client_has_the_same_construction_helpers(monkeypatch, tmp_path):
    from olcf.sdk import AsyncClient
    from olcf.sdk.contexts import ContextStore

    monkeypatch.setenv("OLCF_URL", "https://olcf.example.com/")
    monkeypatch.setenv("OLCF_TOKEN", "olcf_pat_envtoken_envtoken")
    c = AsyncClient.from_env()
    assert c.base_url == "https://olcf.example.com" and "envtoken" not in repr(c) and c.cfg.token == "olcf_pat_envtoken_envtoken"
    monkeypatch.setenv("OLCF_CONFIG_DIR", str(tmp_path / "cfg"))
    monkeypatch.delenv("OLCF_URL")
    monkeypatch.delenv("OLCF_TOKEN")
    ContextStore().set("prod", url="https://olcf.prod:8470", token="olcf_pat_abc_abcdefgh", refresh_token="olcf_ref_x_yyyyyyyy")
    a = AsyncClient.from_context()
    assert a.base_url == "https://olcf.prod:8470" and a.cfg.token == "olcf_pat_abc_abcdefgh" and a.cfg.refresh_token == "olcf_ref_x_yyyyyyyy"
    a.on_tokens("olcf_pat_new_new_new_new", "olcf_ref_new_new_new_new")  # a refreshed pair is written back to the context
    assert ContextStore().get("prod")["token"] == "olcf_pat_new_new_new_new"
    from olcf.sdk.errors import AuthenticationError

    with pytest.raises(AuthenticationError):
        AsyncClient.from_context("nope")
