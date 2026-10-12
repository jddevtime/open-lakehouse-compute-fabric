# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
# ruff: noqa: S106
"""The stdlib-only Fabric SDK: configuration, the four authentication modes, defensive ``getDefinition`` handling, HTTP retry policy, light notebook handling and the
``test_connection`` checklist."""

from __future__ import annotations

import base64
import io
import json
import sys
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any

import jwt as pyjwt
import pytest

import olcf_fabric as olcf
from olcf_fabric import _http, auth, config
from olcf_fabric import fabric as fab
from olcf_fabric.errors import AuthenticationError, ConfigurationError, ConnectionFailed, ServerError
from tests.fabric.fabric_fakes import (
    API_AUDIENCE,
    BROKER_CLIENT_ID,
    BROKER_SECRET,
    TENANT,
    FakeFabricWorld,
    FakeNotebookUtils,
)
from tests.fabric.fabric_support import SUBMIT_CELL, ipynb


@pytest.fixture(autouse=True)
def _clean(monkeypatch: pytest.MonkeyPatch):
    for k in ("OLCF_ENDPOINT", "OLCF_TOKEN", "OLCF_AUTH", "OLCF_TENANT_ID", "OLCF_AUDIENCE", "OLCF_CA_BUNDLE"):
        monkeypatch.delenv(k, raising=False)
    config.reset_settings()
    monkeypatch.delitem(sys.modules, "notebookutils", raising=False)
    yield
    config.reset_settings()


def nbu(monkeypatch: pytest.MonkeyPatch, **kw: Any) -> FakeNotebookUtils:
    ctx = {"currentNotebookName": "n", "currentNotebookId": "id", "currentWorkspaceId": "w"}
    obj = FakeNotebookUtils(context=ctx, **kw)
    monkeypatch.setitem(sys.modules, "notebookutils", obj)  # type: ignore[arg-type]
    return obj


# ------------------------------------------------------------------------------------------------ configuration
def test_version_and_exports() -> None:
    assert olcf.__version__ == "0.1.0"
    for name in ("configure", "run_remote", "extend", "status", "logs", "cancel", "finish", "test_connection", "RemoteRun", "RemoteExecutionError", "CompatibilityError", "SubmissionRejected"):
        assert hasattr(olcf, name), name


def test_configure_validates_and_env_fallbacks(monkeypatch: pytest.MonkeyPatch) -> None:
    with pytest.raises(ConfigurationError):
        olcf.configure(endpoint="https://x.example", auth="telepathy")
    olcf.configure(endpoint="https://x.example/", token="t")
    assert config.current().endpoint == "https://x.example"
    config.reset_settings()
    monkeypatch.setenv("OLCF_ENDPOINT", "https://env.example")
    monkeypatch.setenv("OLCF_TOKEN", "envtok")
    monkeypatch.setenv("OLCF_TENANT_ID", TENANT)
    s = config.current()
    assert (s.endpoint, s.token, s.tenant_id) == ("https://env.example", "envtok", TENANT)
    assert "envtok" not in repr(s)                                                # secrets are not in repr


def test_endpoint_policy() -> None:
    assert _http.check_endpoint("https://ok.example") == "https://ok.example"
    assert _http.check_endpoint("http://127.0.0.1:8080/") == "http://127.0.0.1:8080"
    assert _http.check_endpoint("http://localhost:1") == "http://localhost:1"
    with pytest.raises(ConfigurationError):
        _http.check_endpoint("http://control-plane.corp.example")                 # plain http to a non-loopback host needs an explicit opt-in
    assert _http.check_endpoint("http://control-plane.corp.example", True)
    with pytest.raises(ConfigurationError):
        _http.check_endpoint("ftp://x")
    with pytest.raises(ConfigurationError):
        config.Settings().require()


# ------------------------------------------------------------------------------------------------ authentication
def mk_jwt(exp_in: int = 3600, **claims: Any) -> str:
    return pyjwt.encode({"exp": int(time.time()) + exp_in, **claims}, "k" * 32, algorithm="HS256")


def test_mode_selection_order() -> None:
    olcf.configure(endpoint="https://x.example", token="t", keyvault="https://v/", secret_name="s")
    assert auth.TokenProvider(config.current()).mode() == "token"
    config.reset_settings()
    olcf.configure(endpoint="https://x.example", keyvault="https://v/", secret_name="s", audience="api://a")
    assert auth.TokenProvider(config.current()).mode() == "keyvault"
    config.reset_settings()
    olcf.configure(endpoint="https://x.example", tenant_id="t", client_id="c", audience="api://a", client_secret_name="cs", keyvault="https://v/")
    assert auth.TokenProvider(config.current()).mode() == "client_credentials"
    config.reset_settings()
    olcf.configure(endpoint="https://x.example", audience="api://a")
    assert auth.TokenProvider(config.current()).mode() == "notebookutils"
    config.reset_settings()
    olcf.configure(endpoint="https://x.example")
    with pytest.raises(ConfigurationError):
        auth.TokenProvider(config.current()).mode()


def test_token_mode_and_caching() -> None:
    tok = mk_jwt(tid=TENANT, appid="app")
    olcf.configure(endpoint="https://x.example", token=tok)
    p = auth.TokenProvider(config.current())
    assert p.token() == tok and p.claims()["tid"] == TENANT and p.mode_used == "token"
    short = mk_jwt(exp_in=30)                                                     # inside the refresh margin: never served from the cache, re-resolved every time
    config.reset_settings()
    olcf.configure(endpoint="https://x.example", token=short)
    p2 = auth.TokenProvider(config.current())
    assert p2.token() == short and p2.token(force=True) == short


def test_keyvault_mode(monkeypatch: pytest.MonkeyPatch) -> None:
    obj = nbu(monkeypatch, secrets={("https://kv.vault.azure.net/", "olcf-token"): "  platform-token-123\n"})
    olcf.configure(endpoint="https://x.example", auth="keyvault", keyvault="https://kv.vault.azure.net/", secret_name="olcf-token")
    assert auth.TokenProvider(config.current()).token() == "platform-token-123"
    assert obj
    olcf.configure(secret_name="missing")
    with pytest.raises(AuthenticationError) as ei:
        auth.TokenProvider(config.current()).token()
    assert "get' permission" in (ei.value.remediation or "")


def test_keyvault_mode_outside_fabric() -> None:
    olcf.configure(endpoint="https://x.example", auth="keyvault", keyvault="https://kv/", secret_name="s")
    with pytest.raises(ConfigurationError) as ei:
        auth.TokenProvider(config.current()).token()
    assert "notebookutils is not available" in str(ei.value)


def test_notebookutils_mode(monkeypatch: pytest.MonkeyPatch) -> None:
    tok = mk_jwt(aud="api://x")
    obj = nbu(monkeypatch, tokens={"api://olcf": tok})
    olcf.configure(endpoint="https://x.example", auth="notebookutils", audience="api://olcf")
    assert auth.TokenProvider(config.current()).token() == tok
    assert obj.token_calls == ["api://olcf"]
    olcf.configure(audience="api://unsupported")
    with pytest.raises(AuthenticationError) as ei:
        auth.TokenProvider(config.current()).token()
    assert "client_credentials" in (ei.value.remediation or "")
    config.reset_settings()
    olcf.configure(endpoint="https://x.example", auth="notebookutils")
    with pytest.raises(ConfigurationError):
        auth.TokenProvider(config.current()).token()


def test_client_credentials_mode_against_the_fake_entra(monkeypatch: pytest.MonkeyPatch, world: FakeFabricWorld) -> None:
    nbu(monkeypatch, secrets={("https://kv/", "client-secret"): BROKER_SECRET})
    world.token_requests.clear()
    olcf.configure(endpoint="https://x.example", auth="client_credentials", tenant_id=TENANT, client_id=BROKER_CLIENT_ID, audience=API_AUDIENCE, keyvault="https://kv/",
                   client_secret_name="client-secret", authority=world.base, allow_insecure_http=True)
    p = auth.TokenProvider(config.current())
    tok = p.token()
    assert p.mode_used == "client_credentials" and pyjwt.decode(tok, options={"verify_signature": False})["aud"] == API_AUDIENCE
    assert world.token_requests[-1]["scope"] == API_AUDIENCE + "/.default" and world.token_requests[-1]["grant_type"] == "client_credentials"
    assert p.token() == tok and len(world.token_requests) == 1                    # cached
    olcf.configure(client_secret="wrong")
    p2 = auth.TokenProvider(config.current())
    with pytest.raises(AuthenticationError) as ei:
        p2.token()
    assert "wrong" not in str(ei.value) and ei.value.remediation
    config.reset_settings()
    olcf.configure(endpoint="https://x.example", auth="client_credentials", tenant_id=TENANT)
    with pytest.raises(ConfigurationError):
        auth.TokenProvider(config.current()).token()


# ------------------------------------------------------------------------------------------------ getDefinition spellings
@pytest.mark.parametrize("signature", ["kw-workspace-format", "positional", "format-only", "name-only", "dict-result", "parts-result", "bytes-result"])
def test_get_definition_tolerates_documented_and_plausible_signatures(monkeypatch: pytest.MonkeyPatch, signature: str) -> None:
    text = ipynb([("code", "print(1)")])
    calls: list[tuple] = []

    def make(sig: str):
        def fn(*args: Any, **kwargs: Any) -> Any:
            calls.append((args, kwargs))
            ok = {"kw-workspace-format": "workspaceId" in kwargs and "format" in kwargs, "positional": len(args) == 3, "format-only": set(kwargs) == {"format"} and len(args) == 1,
                  "name-only": len(args) == 1 and not kwargs, "dict-result": True, "parts-result": True, "bytes-result": True}[sig]
            if not ok:
                raise TypeError("unexpected signature")
            if sig == "dict-result":
                return json.loads(text)
            if sig == "parts-result":
                return {"definition": {"parts": [{"path": "notebook-content.ipynb", "payload": base64.b64encode(text.encode()).decode(), "payloadType": "InlineBase64"}]}}
            if sig == "bytes-result":
                return text.encode()
            return text
        return fn

    obj = nbu(monkeypatch)
    obj.notebook.getDefinition = make(signature)
    got = fab.acquire_definition("n", "w", ["ipynb", None])
    assert got is not None and got[0] == "ipynb" and "print(1)" in got[1]
    assert calls


def test_get_definition_unavailable_or_failing_returns_none(monkeypatch: pytest.MonkeyPatch) -> None:
    assert fab.acquire_definition("n", "w", ["ipynb"]) is None                    # no notebookutils at all
    obj = nbu(monkeypatch)
    del obj.notebook.getDefinition
    assert fab.acquire_definition("n", "w", ["ipynb"]) is None

    def boom(*a: Any, **k: Any) -> Any:
        raise PermissionError("no access")

    obj.notebook.getDefinition = boom
    assert fab.acquire_definition("n", "w", ["ipynb", "fabricGitSource", None]) is None


def test_get_definition_prefers_the_first_format_that_works(monkeypatch: pytest.MonkeyPatch) -> None:
    git = "# Fabric notebook source\n\n# CELL ********************\n\nprint(1)\n"
    obj = nbu(monkeypatch)

    def fn(name: str, workspaceId: str = "", format: str | None = None) -> Any:  # noqa: A002, N803
        if format == "ipynb":
            raise RuntimeError("ipynb is not available")
        return git

    obj.notebook.getDefinition = fn
    fmt, text, how = fab.acquire_definition("n", "w", ["ipynb", "fabricGitSource"])
    assert fmt == "fabric_git_source" and text == git and "fabricGitSource" in how


# ------------------------------------------------------------------------------------------------ light notebook handling
GIT = ("# Fabric notebook source\n\n# METADATA ********************\n\n# META {\n# META   \"kernel_info\": {\"name\": \"synapse_pyspark\"}\n# META }\n\n"
       "# PARAMETERS CELL ********************\n\nn = 5\nname: str = \"x\"\nflag = True\n\n# CELL ********************\n\nprint(n)\n\n"
       "# METADATA ********************\n\n# META {\n# META   \"language\": \"python\"\n# META }\n\n"
       "# CELL ********************\n\nimport olcf_fabric as olcf\nolcf.run_remote()\n\n# METADATA ********************\n\n# META {\n# META   \"language\": \"python\"\n# META }\n")


def test_drop_submit_cells_ipynb_and_git() -> None:
    nb = ipynb([("code", "n = 1", ["parameters"]), ("code", "print(n)"), ("code", "import olcf_fabric as olcf\nolcf.extend(parameters={'n': 2})"), ("markdown", "run_remote( is described here")])
    out, removed = fab.drop_submit_cells(nb, "ipynb")
    assert removed == [2] and [c["source"] for c in json.loads(out)["cells"]] == ["n = 1", "print(n)", "run_remote( is described here"]
    assert fab.drop_submit_cells(nb.replace("olcf_fabric", "other"), "ipynb")[1] == []
    out, removed = fab.drop_submit_cells(GIT, "fabric_git_source")
    assert removed == [2] and "run_remote" not in out and "print(n)" in out and out.startswith("# Fabric notebook source")
    assert out.count("# METADATA") == 2                                                   # the removed cell took its own metadata with it
    py = "# %%\nn = 1\n# %%\nimport olcf_fabric as olcf\nolcf.run_remote()\n# %%\nprint(n)\n"
    out, removed = fab.drop_submit_cells(py, "py")
    assert removed == [1] and "run_remote" not in out


def test_declared_parameters() -> None:
    assert fab.declared_parameter_names(GIT, "fabric_git_source") == ["n", "name", "flag"]
    nb = ipynb([("code", "a = 1\nb = a + 1\ndef f(): pass\nc: int = 3", ["parameters"]), ("code", "d = 4")])
    assert fab.declared_parameter_names(nb, "ipynb") == ["a", "b", "c"]
    assert fab.declared_parameter_names(ipynb([("code", "x = 1")]), "ipynb") == []
    assert fab.declared_parameter_names(ipynb([("code", "%pip install x\nn = 1", ["parameters"])]), "ipynb") == ["n"]


def test_secret_refs_and_json_safe_and_context() -> None:
    refs = fab.secret_refs_in("a = notebookutils.credentials.getSecret('https://v.vault.azure.net/', 'k1')\nb = mssparkutils.credentials.getSecret(\"v2\", \"k2\")\n")
    assert refs == [{"vault": "https://v.vault.azure.net/", "name": "k1"}, {"vault": "v2", "name": "k2"}]
    assert fab.json_safe({"a": [1, 2.5, None]}) == (True, {"a": [1, 2.5, None]}) and fab.json_safe(object())[0] is False and fab.json_safe(float("nan"))[0] is False
    raw = {"currentNotebookName": "n", "userName": "u", "accessToken": "SECRET", "api_key": "SECRET", "obj": object(), "nested": {"a": 1}}
    out = fab.raw_context(raw)
    assert "accessToken" not in out and "api_key" not in out and isinstance(out["obj"], str) and out["nested"] == {"a": 1}
    norm = fab.normalise_context({**raw, "currentWorkspaceId": "w"})
    assert norm["currentWorkspaceId"] == "w" and "accessToken" not in norm and norm["defaultLakehouseId"] is None


# ------------------------------------------------------------------------------------------------ HTTP client
class Scripted:
    """A loopback server answering from a script of (status, headers, body) tuples; records the requests."""

    def __init__(self, script: list[tuple[int, dict[str, str], Any]]) -> None:
        self.script, self.requests = list(script), []
        outer = self

        class H(BaseHTTPRequestHandler):
            def log_message(self, *a: Any) -> None:
                pass

            def _go(self) -> None:
                n = int(self.headers.get("Content-Length") or 0)
                body = self.rfile.read(n) if n else b""
                outer.requests.append((self.command, self.path, dict(self.headers), body))
                status, headers, doc = outer.script.pop(0) if len(outer.script) > 1 else outer.script[0]
                data = json.dumps(doc).encode() if doc is not None else b""
                self.send_response(status)
                for k, v in headers.items():
                    self.send_header(k, v)
                self.send_header("Content-Length", str(len(data)))
                self.end_headers()
                self.wfile.write(data)

            do_GET = do_POST = _go

        self.server = ThreadingHTTPServer(("127.0.0.1", 0), H)
        threading.Thread(target=self.server.serve_forever, daemon=True).start()

    @property
    def url(self) -> str:
        return f"http://127.0.0.1:{self.server.server_address[1]}"

    def close(self) -> None:
        self.server.shutdown()
        self.server.server_close()


def test_retries_honour_retry_after_and_stop_at_the_limit() -> None:
    srv = Scripted([(429, {"Retry-After": "2"}, {"error": {"code": "rate_limited", "message": "slow"}}), (503, {}, {"error": {"message": "busy"}}), (200, {}, {"ok": True})])
    sleeps: list[float] = []
    try:
        c = _http.HttpClient(srv.url, sleep=sleeps.append, max_attempts=4)
        assert c.request("GET", "/x", token="tok").json() == {"ok": True}
        assert sleeps[0] == 2.0 and len(sleeps) == 2 and len(srv.requests) == 3
        assert srv.requests[0][2]["Authorization"] == "Bearer tok"
    finally:
        srv.close()
    srv = Scripted([(503, {}, {"error": {"code": "unavailable", "message": "down", "remediation": "wait"}})])
    try:
        c = _http.HttpClient(srv.url, sleep=lambda s: None, max_attempts=3)
        with pytest.raises(ServerError) as ei:
            c.request("GET", "/x")
        assert ei.value.status == 503 and ei.value.code == "unavailable" and ei.value.remediation == "wait" and len(srv.requests) == 3
    finally:
        srv.close()


def test_client_errors_are_not_retried_and_post_bodies_are_json() -> None:
    srv = Scripted([(409, {}, {"error": {"code": "capsule_replay", "message": "used", "details": {"submission_id": "s"}}})])
    try:
        c = _http.HttpClient(srv.url, sleep=lambda s: None)
        with pytest.raises(ServerError) as ei:
            c.request("POST", "/x", body={"a": "é"})
        assert ei.value.status == 409 and ei.value.code == "capsule_replay" and ei.value.details == {"submission_id": "s"} and len(srv.requests) == 1
        assert json.loads(srv.requests[0][3]) == {"a": "é"} and srv.requests[0][2]["Content-Type"] == "application/json"
    finally:
        srv.close()


def test_unreachable_host_is_a_connection_failure_with_remediation() -> None:
    c = _http.HttpClient("http://127.0.0.1:9", sleep=lambda s: None, max_attempts=2, timeout=1)
    with pytest.raises(ConnectionFailed) as ei:
        c.request("GET", "/x")
    assert ei.value.remediation
    c2 = _http.HttpClient("http://does-not-exist.invalid", allow_insecure_http=True, sleep=lambda s: None, max_attempts=1, timeout=2)
    with pytest.raises(ConnectionFailed) as ei2:
        c2.request("GET", "/x")
    assert "resolve" in str(ei2.value) or "reach" in str(ei2.value)


def test_client_refreshes_the_credential_once_after_401() -> None:
    srv = Scripted([(401, {}, {"error": {"message": "expired"}}), (200, {}, {"ok": 1})])
    tokens = iter(["old", "new", "newer"])
    try:
        olcf.configure(endpoint=srv.url, token="seed", allow_insecure_http=True)
        from olcf_fabric.client import get_client

        c = get_client()
        c.auth.token = lambda force=False: next(tokens)  # type: ignore[method-assign]
        assert c.call("GET", "/x") == {"ok": 1}
        assert [r[2]["Authorization"] for r in srv.requests] == ["Bearer old", "Bearer new"]
    finally:
        srv.close()
    srv = Scripted([(401, {}, {"error": {"message": "nope"}})])
    try:
        olcf.configure(endpoint=srv.url, token="seed", allow_insecure_http=True)
        c = get_client()
        with pytest.raises(AuthenticationError):
            c.call("GET", "/x")
        assert len(srv.requests) == 2
    finally:
        srv.close()


# ------------------------------------------------------------------------------------------------ test_connection
def test_test_connection_reports_every_step_against_the_real_control_plane(e2e: Any, session: Any) -> None:
    s = session.make(notebook=ipynb([("code", "print(1)"), ("code", SUBMIT_CELL)]))
    buf = io.StringIO()
    res = s.olcf.test_connection(out=buf)
    names = [c["name"] for c in res["checks"]]
    assert res["ok"] is True, res
    for n in ("DNS", "TLS", "token", "challenge", "handshake", "latency", "server version", "authorisation", "Fabric context"):
        assert n in names
    assert "OLCF connection OK" in buf.getvalue() and res["handshake"]["authorized"] is True
    hs = e2e.st.http.get("/v1/integrations/fabric", headers=e2e.st.admin).json()
    assert hs["readiness"] is not None


def test_test_connection_never_raises_and_explains(e2e: Any, session: Any) -> None:
    session.make(notebook=ipynb([("code", "1")]))
    olcf.configure(endpoint="http://127.0.0.1:9", token="x", allow_insecure_http=True)
    buf = io.StringIO()
    res = olcf.test_connection(out=buf)
    assert res["ok"] is False
    failed = [c for c in res["checks"] if not c["ok"]]
    assert failed and all(c["remediation"] for c in failed)
    config.reset_settings()
    res2 = olcf.test_connection(out=io.StringIO())
    assert res2["ok"] is False and res2["checks"][0]["name"] == "configuration"
    olcf.configure(endpoint=e2e.url, token="olcf_pat_bogus_bogus_bogus_bogus", allow_insecure_http=True)
    res3 = olcf.test_connection(out=io.StringIO())
    assert res3["ok"] is False and any(c["name"] in ("challenge", "handshake") and not c["ok"] for c in res3["checks"])
