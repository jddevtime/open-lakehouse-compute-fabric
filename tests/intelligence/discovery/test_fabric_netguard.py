# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
# ruff: noqa: S105, S106, S104
"""SSRF and credential hygiene of the Microsoft Fabric / Power BI discovery source (``intelligence.discovery.netguard``).

A source's ``config.network`` may override the endpoints the service principal's credential and tokens are sent to. Those overrides are refused at create / update (422 with a
remediation) and again whenever the connector is about to use them (a stored source stays checked after the setting changed), unless the runtime setting
``intelligence.allow_private_endpoints`` allows loopback / private destinations. The tests prove, with a recording fake tenant, that a refused endpoint never receives a request,
and with stub servers that a redirect or a DNS answer that changes between the check and the connection cannot reach an internal address.
"""

from __future__ import annotations

import json
import threading
from collections.abc import Iterator
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any

import pytest

from olcf.core.errors import PolicyDenied
from olcf.fabric.http import Network
from olcf.intelligence import settings as isettings
from olcf.intelligence.discovery import netguard
from olcf.pipelines import ssrf
from tests.fabric.fabric_fakes import (
    BROKER_CLIENT_ID,
    BROKER_SECRET,
    TENANT,
    FakeFabricWorld,
    allow_private_endpoints,
)
from tests.intelligence.discovery.test_fabric_connector import STEPS, Fx, failing_step, populate

PUBLIC = "93.184.216.34"
KEYS = ["authority_host", "fabric_api", "powerbi_api", "onelake_dfs"]


def body(network: dict[str, Any], name: str = "guarded") -> dict[str, Any]:
    return {"name": name, "kind": "fabric", "credentials": {"client_secret": BROKER_SECRET},
            "config": {"tenant_id": TENANT, "metadata_identity": {"tenant_id": TENANT, "client_id": BROKER_CLIENT_ID}, "network": network}}


@pytest.fixture()
def fx(disc: Any) -> Iterator[Fx]:
    """The fake tenant on 127.0.0.1 with the setting at its default (private endpoints NOT allowed); tests that need them call ``allow_private_endpoints``."""
    world = FakeFabricWorld().start()
    populate(world)
    f = Fx(disc, world)
    disc.d.runner.sleep = f.sleeps.append
    yield f
    disc.d.runner.sleep = None
    world.stop()


def refused(disc: Any, network: dict[str, Any], *, name: str = "guarded") -> dict[str, Any]:
    r = disc.api("POST", "/sources", json=body(network, name))
    assert r.status_code == 422, (network, r.status_code, r.text)
    err = r.json()["error"]
    assert err["code"] == "validation_failed" and err["remediation"], err
    assert BROKER_SECRET not in r.text
    return err


# ------------------------------------------------------------------------------------------------ the setting
def test_the_setting_exists_defaults_to_false_and_warns(disc: Any) -> None:
    st = disc.ctx.service("settings")
    assert st.get(isettings.ALLOW_PRIVATE_ENDPOINTS) is False
    reg = st.registry[isettings.ALLOW_PRIVATE_ENDPOINTS]
    desc = reg.description if hasattr(reg, "description") else reg["description"]
    assert "WARNING" in desc and "credential" in desc and "169.254" not in desc
    r = disc.intel.c.request("PUT", f"/v1/admin/settings/{isettings.ALLOW_PRIVATE_ENDPOINTS}", headers=disc.admin, json={"value": "yes"})
    assert r.status_code in (400, 422), r.text                                    # a boolean, nothing else
    assert disc.intel.c.request("PUT", f"/v1/admin/settings/{isettings.ALLOW_PRIVATE_ENDPOINTS}", headers=disc.admin, json={"value": True}).status_code == 200


# ------------------------------------------------------------------------------------------------ create / update: every refused kind of endpoint is a 422
@pytest.mark.parametrize("key", KEYS)
@pytest.mark.parametrize("url", ["https://169.254.169.254/latest/meta-data", "https://[fd00:ec2::254]/", "https://169.254.170.2/v2/credentials"])
def test_the_cloud_metadata_and_link_local_endpoints_are_refused(disc: Any, key: str, url: str) -> None:
    err = refused(disc, {key: url})
    assert f"config.network.{key}" in err["message"]


@pytest.mark.parametrize("url", ["https://localhost", "https://localhost:8443/x", "https://127.0.0.1", "https://127.1.2.3:9", "https://[::1]/", "https://svc.localhost/",
                                 "https://0.0.0.0", "https://2130706433/", "https://0x7f.1/"])
def test_loopback_endpoints_are_refused(disc: Any, url: str) -> None:
    err = refused(disc, {"authority_host": url})
    assert "config.network.authority_host" in err["message"]


@pytest.mark.parametrize("url", ["https://10.0.0.5", "https://10.255.255.255/token", "https://172.16.0.9", "https://172.31.1.1:8443", "https://192.168.1.1", "https://100.64.0.1",
                                 "https://[fc00::1]/", "https://[::ffff:10.0.0.1]/", "https://[::ffff:169.254.169.254]/"])
def test_private_network_endpoints_are_refused(disc: Any, url: str) -> None:
    for key in KEYS:
        refused(disc, {key: url}, name=f"p-{key}")


@pytest.mark.parametrize("url", ["http://login.microsoftonline.com", "http://169.254.169.254/", "http://localhost:1", "ftp://example.com/", "file:///etc/passwd", "//example.com", "example.com"])
def test_a_plain_http_or_other_scheme_is_refused(disc: Any, url: str) -> None:
    err = refused(disc, {"fabric_api": url})
    assert "https" in err["message"] or "URL" in err["message"]


@pytest.mark.parametrize("url", ["https://user:hunter2@api.fabric.microsoft.com", "https://:hunter2@login.microsoftonline.com", "https://app@example.com"])
def test_credentials_in_the_url_are_refused_and_not_echoed(disc: Any, url: str) -> None:
    err = refused(disc, {"authority_host": url})
    assert "hunter2" not in json.dumps(err) and "credentials in the url" in err["message"].lower()


def test_a_proxy_on_a_private_address_or_with_credentials_is_refused(disc: Any) -> None:
    assert "config.network.proxy" in refused(disc, {"proxy": "http://10.1.2.3:3128"})["message"]
    assert "config.network.proxy" in refused(disc, {"proxy": "http://127.0.0.1:3128"}, name="p2")["message"]
    assert "credentials" in refused(disc, {"proxy": "http://user:pw@proxy.example.com:3128"}, name="p3")["message"]


def test_the_guard_resolves_names_and_checks_every_address() -> None:
    def resolver(host: str, port: int) -> list[str]:
        return {"evil.example.com": ["10.0.0.7"], "mixed.example.com": [PUBLIC, "169.254.169.254"], "ok.example.com": [PUBLIC], "gone.example.com": []}[host]

    for host in ("evil.example.com", "mixed.example.com"):
        with pytest.raises(netguard.EndpointNotAllowed) as e:
            netguard.check_overrides(None, {"fabric_api": f"https://{host}"}, resolver=resolver)
        assert host in e.value.message and e.value.remediation
    netguard.check_overrides(None, {"fabric_api": "https://ok.example.com", "powerbi_api": "https://ok.example.com"}, resolver=resolver)


def test_an_update_to_a_refused_endpoint_is_a_422_and_changes_nothing(fx: Fx) -> None:
    disc = fx.disc
    allow_private_endpoints(disc.ctx)
    src = fx.create()                                                                       # accepted while the fake tenant on 127.0.0.1 is allowed
    stored = disc.d.sources.get(src["id"])["config"]["network"]
    disc.ctx.service("settings").set(isettings.ALLOW_PRIVATE_ENDPOINTS, False)
    for net in ({"fabric_api": "https://169.254.169.254"}, {"authority_host": fx.world.base}, {"onelake_dfs": "http://example.com"}):
        r = disc.api("PATCH", f"/sources/{src['id']}", json={"config": {"network": {**stored, **net}}})
        assert r.status_code == 422 and r.json()["error"]["remediation"], (net, r.text)
    assert disc.d.sources.get(src["id"])["config"]["network"] == stored


def test_a_caller_without_the_fabric_manage_permission_cannot_override_endpoints_at_all(disc: Any) -> None:
    from olcf.core.errors import Forbidden

    class Nobody:
        def require(self, permission: str) -> None:
            raise Forbidden("not allowed")

    from olcf.intelligence.discovery import identities
    with pytest.raises(Forbidden):
        identities.validate_network(Nobody(), {"fabric_api": "https://api.fabric.microsoft.com"}, disc.ctx)


# ------------------------------------------------------------------------------------------------ the setting opens loopback / private, never metadata
def test_with_the_setting_on_loopback_and_private_endpoints_and_plain_http_to_them_are_accepted(disc: Any) -> None:
    allow_private_endpoints(disc.ctx)
    for i, net in enumerate(({"fabric_api": "https://127.0.0.1:9"}, {"authority_host": "http://localhost:9"}, {"onelake_dfs": "http://10.1.2.3:8080"},
                             {"powerbi_api": "https://192.168.0.4"}, {"fabric_api": "http://[::1]:9"})):
        r = disc.api("POST", "/sources", json=body(net, f"ok{i}"))
        assert r.status_code == 201, (net, r.text)


@pytest.mark.parametrize("url", ["https://169.254.169.254/", "http://169.254.169.254/latest", "https://[fe80::1]/", "https://0.0.0.0/", "https://224.0.0.1/", "https://[::ffff:169.254.169.254]/"])
def test_with_the_setting_on_the_metadata_and_link_local_endpoints_stay_refused(disc: Any, url: str) -> None:
    allow_private_endpoints(disc.ctx)
    refused(disc, {"fabric_api": url})


def test_with_the_setting_on_http_to_a_public_address_is_still_refused(disc: Any) -> None:
    allow_private_endpoints(disc.ctx)
    err = refused(disc, {"fabric_api": f"http://{PUBLIC}"})
    assert "https" in err["message"]


def test_the_platform_deny_list_still_applies_and_the_pipeline_allow_list_does_not_widen(disc: Any) -> None:
    st = disc.ctx.service("settings")
    ssrf.register_settings(disc.ctx)                                                        # the pipelines module's settings; not installed in this app
    st.set("network.ssrf.allow", ["10.0.0.0/8", "127.0.0.1"])                               # an allow-list made for pipelines and webhooks ...
    refused(disc, {"fabric_api": "https://10.0.0.5"})                                       # ... does not let a credential-bearing source reach those hosts
    allow_private_endpoints(disc.ctx)
    st.set("network.ssrf.deny", ["10.9.0.0/16"])
    refused(disc, {"fabric_api": "https://10.9.1.1"}, name="deny")
    assert disc.api("POST", "/sources", json=body({"fabric_api": "https://10.8.1.1"}, "fine")).status_code == 201


def test_a_public_https_override_is_accepted_without_the_setting(disc: Any) -> None:
    r = disc.api("POST", "/sources", json=body({"authority_host": "https://login.microsoftonline.us", "fabric_api": "https://api.fabric.microsoft.us",
                                                 "powerbi_api": "https://api.powerbigov.us", "onelake_dfs": "https://onelake.dfs.fabric.microsoft.us"}))
    assert r.status_code == 201, r.text


# ------------------------------------------------------------------------------------------------ use time: a stored source is checked again, nothing is sent
def test_a_stored_source_whose_endpoint_is_no_longer_allowed_sends_nothing_and_says_how_to_fix_it(fx: Fx, monkeypatch: pytest.MonkeyPatch) -> None:
    disc = fx.disc
    allow_private_endpoints(disc.ctx)
    fx.create()
    assert fx.test()["ok"] is True                                                          # the fake tenant is reachable while the setting is on
    fx.world.token_requests.clear()
    fx.world.calls.clear()
    disc.ctx.service("settings").set(isettings.ALLOW_PRIVATE_ENDPOINTS, False)               # an administrator turns it off: the stored source is now unacceptable
    out = fx.test()
    assert out["ok"] is False and out["failed_step"] == STEPS[1], out
    step = failing_step(out)
    assert "config.network" in step["detail"] and "no request was sent" in step["detail"] and "allow_private_endpoints" in step["remediation"]
    assert BROKER_SECRET not in json.dumps(out)
    row = fx.scan()
    assert row["status"] == "failed" and "config.network" in row["error"] and "no request was sent" in row["error"], row
    assert BROKER_SECRET not in json.dumps(row, default=str)
    assert fx.world.token_requests == [] and fx.world.calls == []                           # the credential never left, nothing reached the endpoint at all


def test_the_credential_is_not_used_when_the_check_fails(fx: Fx, monkeypatch: pytest.MonkeyPatch) -> None:
    from olcf.fabric import entra

    allow_private_endpoints(fx.disc.ctx)
    fx.create()
    fx.disc.ctx.service("settings").set(isettings.ALLOW_PRIVATE_ENDPOINTS, False)
    used: list[str] = []
    monkeypatch.setattr(entra.TokenBroker, "get_token", lambda self, scope: used.append(scope) or "token")
    fx.test()
    fx.scan()
    assert used == []                                                                       # not even the token broker was asked for a token


def test_a_stored_override_that_bypassed_validation_is_still_refused_at_use(fx: Fx, monkeypatch: pytest.MonkeyPatch) -> None:
    """A row written straight into the database (a restore, an old version, a hostile edit) is stopped at use, even with the setting on: the metadata endpoint is never allowed."""
    from sqlalchemy import update

    from olcf.fabric import entra
    from olcf.intelligence.discovery.schema import discovery_sources

    allow_private_endpoints(fx.disc.ctx)
    src = fx.create()
    with fx.disc.ctx.db.begin() as c:
        cfg = dict(src["config"])
        cfg["network"] = {**cfg["network"], "authority_host": "https://169.254.169.254"}
        c.execute(update(discovery_sources).where(discovery_sources.c.id == src["id"]).values(config=cfg))
    monkeypatch.setattr(entra.TokenBroker, "get_token", lambda self, scope: pytest.fail("a token was requested"))
    out = fx.test()
    assert out["failed_step"] == STEPS[1] and "authority_host" in failing_step(out)["detail"]
    row = fx.scan()
    assert row["status"] == "failed" and "authority_host" in row["error"]


def test_the_integrations_own_network_settings_are_checked_too(disc: Any) -> None:
    """``build_network`` merges the Fabric integration's network settings under the source's: whatever ends up in the resolved ``Network`` is what is checked."""
    net = Network.from_config({"authority_host": "https://login.example.com", "fabric_api": "https://10.0.0.1"}, env={})
    with pytest.raises(netguard.EndpointNotAllowed) as e:
        netguard.check_network(disc.ctx, net, resolver=lambda h, p: [PUBLIC])
    assert "fabric_api" in e.value.message


def test_defaults_and_an_environment_proxy_are_accepted_but_a_configured_private_proxy_is_not(disc: Any) -> None:
    public = lambda host, port: [PUBLIC]  # noqa: E731
    netguard.check_network(disc.ctx, Network.from_config({}, env={}), resolver=public)
    netguard.check_network(disc.ctx, Network.from_config({}, env={"HTTPS_PROXY": "http://10.9.9.9:3128"}), resolver=public)      # the operator's proxy is trusted
    with pytest.raises(netguard.EndpointNotAllowed):
        netguard.check_network(disc.ctx, Network.from_config({"proxy": "http://10.9.9.9:3128"}, env={}), resolver=public)


# ------------------------------------------------------------------------------------------------ connect time: redirects and DNS rebinding
class Stub:
    """A loopback HTTP server: every request is answered with a 302 to ``location`` (or 200 when none) and recorded."""

    def __init__(self, location: str | None = None) -> None:
        self.location, self.seen = location, []
        stub = self

        class H(BaseHTTPRequestHandler):
            def _answer(self) -> None:
                n = int(self.headers.get("Content-Length") or 0)
                stub.seen.append((self.command, self.path, self.rfile.read(n).decode() if n else ""))
                if stub.location:
                    self.send_response(302)
                    self.send_header("Location", stub.location)
                else:
                    self.send_response(200)
                    self.send_header("Content-Type", "application/json")
                body = b"{}"
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

            do_GET = do_POST = _answer                                                      # noqa: N815

            def log_message(self, *a: Any) -> None:
                pass

        self.server = ThreadingHTTPServer(("127.0.0.1", 0), H)
        self.base = f"http://127.0.0.1:{self.server.server_address[1]}"
        threading.Thread(target=self.server.serve_forever, daemon=True).start()

    def close(self) -> None:
        self.server.shutdown()
        self.server.server_close()


@pytest.fixture()
def stub() -> Iterator[Stub]:
    s = Stub()
    yield s
    s.close()


def test_a_redirect_to_the_metadata_endpoint_is_not_followed_and_would_not_connect_if_it_were(disc: Any, stub: Stub) -> None:
    allow_private_endpoints(disc.ctx)
    stub.location = "http://169.254.169.254/latest/meta-data/iam/security-credentials/"
    net = Network.from_config({"authority_host": stub.base, "fabric_api": stub.base, "powerbi_api": stub.base, "onelake_dfs": stub.base, "trust_env": False}, env={})
    make = netguard.client_factory(disc.ctx, net)
    with make() as client:
        assert client.follow_redirects is False
        r = client.post(f"{stub.base}/tenant/oauth2/v2.0/token", data={"client_secret": BROKER_SECRET})
    assert r.status_code == 302 and len(stub.seen) == 1                                     # the 302 is the answer; the Location is never requested
    # defence in depth: even a client that follows redirects cannot connect to the target (the pinned transport refuses it before a socket is opened)
    policy = netguard.policy_for(disc.ctx)
    with ssrf.guarded_client(policy, follow_redirects=True) as following, pytest.raises(PolicyDenied):
        following.get(f"{stub.base}/x")
    assert len(stub.seen) == 2                                                              # the stub was asked once more; the metadata address was not


def test_a_redirect_to_a_private_address_from_the_token_endpoint_fails_the_step_without_a_second_request(fx: Fx) -> None:
    allow_private_endpoints(fx.disc.ctx)
    stub = Stub("http://10.0.0.5/steal")
    try:
        fx.create(network={"authority_host": stub.base, "fabric_api": stub.base, "powerbi_api": stub.base, "onelake_dfs": stub.base, "trust_env": False})
        out = fx.test()
        assert out["ok"] is False and out["failed_step"] == STEPS[1], out
        assert [m for m, _p, _b in stub.seen] == ["POST"]                                    # one request to the configured endpoint, the redirect target is never contacted
    finally:
        stub.close()


def test_a_dns_answer_that_changes_after_the_check_cannot_reach_loopback(disc: Any, stub: Stub) -> None:
    """The names resolve to a public address when the endpoint is checked and to 127.0.0.1 when the connection is made (DNS rebinding): the connection is refused."""
    allow_private_endpoints(disc.ctx)
    disc.ctx.service("settings").set(isettings.ALLOW_PRIVATE_ENDPOINTS, False)
    answers: dict[str, int] = {}

    def rebinding(host: str, port: int) -> list[str]:
        answers[host] = answers.get(host, 0) + 1
        return [PUBLIC] if answers[host] == 1 else ["127.0.0.1"]

    port = stub.server.server_address[1]
    net = Network.from_config({k: f"https://{k.replace('_', '-')}.example.com:{port}" for k in KEYS} | {"trust_env": False}, env={})
    make = netguard.client_factory(disc.ctx, net, resolver=rebinding)                       # the check resolved every name once: all public
    assert set(answers.values()) == {1}
    with make() as client, pytest.raises(PolicyDenied):
        client.get(f"https://authority-host.example.com:{port}/x")                          # the connection resolves again: loopback, refused
    assert stub.seen == []


def test_the_connection_goes_to_the_address_that_was_checked(disc: Any, stub: Stub) -> None:
    """With the setting on, a name that resolves to loopback is reached exactly at the resolved address (the pin), over http for a test double."""
    allow_private_endpoints(disc.ctx)
    port = stub.server.server_address[1]
    net = Network.from_config({"authority_host": f"http://fabric.test:{port}", "fabric_api": f"http://fabric.test:{port}", "powerbi_api": f"http://fabric.test:{port}",
                               "onelake_dfs": f"http://fabric.test:{port}", "trust_env": False}, env={})
    make = netguard.client_factory(disc.ctx, net, resolver=lambda h, p: ["127.0.0.1"])
    with make() as client:
        assert client.get(f"http://fabric.test:{port}/ping").status_code == 200
    assert [p for _m, p, _b in stub.seen] == ["/ping"]
