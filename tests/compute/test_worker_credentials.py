# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Worker credential lifecycle (CONTRACTS 4.2 "Credential renewal"): machine-certificate renewal over the authenticated channel, certificate expiry tracking,
worker-token max age and rotation, the ``worker-certificates`` health check, the operator command (``POST /v1/workers/{id}/renew``, ``olcf worker renew``).

Everything runs against a live control plane with the fake agent (``renew_cert`` frames exactly as the real agent sends them); the real agent is covered in
``test_real_agent.py``.
"""

from __future__ import annotations

import asyncio
import datetime as dt
import json
from collections.abc import Iterator
from typing import Any

import pytest
from cryptography import x509
from cryptography.x509.oid import NameOID
from websockets.asyncio.client import connect
from websockets.exceptions import ConnectionClosed, InvalidStatus

from olcf.core.timeutil import from_iso, to_iso
from tests.compute.fake_agent import wait_until

DAY = 86400.0


def _days(iso: str | None) -> float:
    assert iso, "expected a timestamp"
    return (from_iso(iso).timestamp() - dt.datetime.now(dt.UTC).timestamp()) / DAY


def _put_setting(st: Any, key: str, value: Any) -> None:
    r = st.http.put(f"/v1/admin/settings/{key}", headers=st.admin, json={"value": value})
    assert r.status_code == 200, r.text


@pytest.fixture()
def policy(fleet: Any) -> Iterator[Any]:
    """Change the credential settings for one test; the defaults come back afterwards."""
    st = fleet
    keys = {"compute.worker_cert_renew_before_days": 30, "compute.worker_cert_lifetime_days": 90, "compute.worker_cert_auto_renew": True,
            "compute.worker_token_max_age_days": 0, "compute.worker_token_grace_seconds": 86400}

    def setter(**kw: Any) -> None:
        for k, v in kw.items():
            _put_setting(st, f"compute.worker_{k}", v)

    try:
        yield setter
    finally:
        for k, v in keys.items():
            _put_setting(st, k, v)


def _row(st: Any, worker_id: str) -> dict[str, Any]:
    return st.compute.registry.get_worker(worker_id)


def _age_cert(st: Any, worker_id: str, days_left: float) -> None:
    """Pretend the worker's certificate has ``days_left`` days to live (the controller's record is what the policy looks at)."""
    st.compute.registry.update_worker(worker_id, cert_expires_at=to_iso(dt.datetime.now(dt.UTC) + dt.timedelta(days=days_left)))


def _cert_of(pem: str) -> x509.Certificate:
    return x509.load_pem_x509_certificate(pem.encode())


def _audit(st: Any, action: str, worker_id: str) -> list[dict[str, Any]]:
    r = st.http.get("/v1/audit", headers=st.admin, params={"action": action, "resource_id": worker_id, "limit": 50})
    assert r.status_code == 200, r.text
    return r.json()["items"]


def _ws_result(st: Any, token: str) -> tuple[str, int | None]:
    async def go() -> tuple[str, int | None]:
        url = st.settings.base_url.replace("http", "ws", 1) + "/v1/workers/ws"
        try:
            async with connect(url, additional_headers={"Authorization": f"Bearer {token}"}, open_timeout=5) as ws:
                try:
                    await asyncio.wait_for(ws.recv(), 1.0)
                except TimeoutError:
                    return "open", None
                except ConnectionClosed as exc:
                    return "closed", exc.rcvd.code if exc.rcvd else None
                return "open", None
        except InvalidStatus as exc:
            return "http", exc.response.status_code
        except ConnectionClosed as exc:
            return "closed", exc.rcvd.code if exc.rcvd else None

    return asyncio.run(go())


def _rejected(st: Any, token: str) -> bool:
    kind, code = _ws_result(st, token)
    return (kind, code) in (("closed", 4401), ("closed", 4403), ("http", 401), ("http", 403))


# ------------------------------------------------------------------------------------------------------------------ expiry is tracked
def test_enrolment_records_certificate_expiry_in_worker_and_topology(fleet):
    st = fleet
    a = st.agent("cred-expiry")
    w = st.worker(a.worker_id)
    assert 89 < _days(w["cert_expires_at"]) < 91
    assert w["token_expires_at"] is None  # default behaviour: worker tokens do not expire
    assert "cert_serial" not in w and "token_id" not in json.dumps(w)  # no internal identifiers in the API
    topo = st.http.get("/v1/compute/topology", headers=st.admin).json()
    node = next(n for n in topo["nodes"] if n["id"] == a.worker_id)
    assert node["cert_expires_at"] == w["cert_expires_at"]
    # what the controller issued is what the agent holds
    assert to_iso(_cert_of(a.cert_pem).not_valid_after_utc.replace(microsecond=0))[:16] == w["cert_expires_at"][:16]


def test_worker_without_certificate_has_no_expiry_and_is_not_flagged(fleet):
    st = fleet
    a = st.agent("cred-nocsr", csr=False)
    w = st.worker(a.worker_id)
    assert w["cert_expires_at"] is None
    h = st.ctx.health.run("worker-certificates", force=True)
    assert h.status.value in ("ok", "degraded", "down")  # never an error for a worker that has no certificate
    assert "cred-nocsr" not in h.detail


def test_hello_ack_tells_the_agent_when_to_renew(fleet, policy):
    st = fleet
    a = st.agent("cred-hello")
    block = a.hello_acks[-1]["credentials"]
    assert 59 < _days(block["renew_after"]) < 61  # 90 day certificate, last 30 days
    assert 89 < _days(block["cert_expires_at"]) < 91 and block["token_expires_at"] is None and block["auto_renew"] is True
    policy(cert_auto_renew=False)
    a.drop()
    a.reconnect()
    off = a.hello_acks[-1]["credentials"]
    assert off["renew_after"] is None and off["auto_renew"] is False  # an operator who renews by hand tells agents not to do it themselves


def test_agent_reported_certificate_is_used_for_workers_enrolled_before_tracking(fleet):
    """A worker row without a recorded expiry (enrolled by an older controller) learns it from the hello of an agent that holds the certificate the controller issued."""
    st = fleet
    a = st.agent("cred-legacy")
    st.compute.registry.update_worker(a.worker_id, cert_expires_at=None)
    assert st.worker(a.worker_id)["cert_expires_at"] is None
    a.drop()
    a.reconnect()
    w = wait_until(lambda: (x := st.worker(a.worker_id))["cert_expires_at"] and x, 10, 0.1, "the hello to restore the expiry")
    assert 89 < _days(w["cert_expires_at"]) < 91


# ------------------------------------------------------------------------------------------------------------------ certificate renewal
def test_renewal_outside_the_window_is_refused_with_the_next_date(fleet):
    st = fleet
    a = st.agent("cred-notdue")
    before = _row(st, a.worker_id)["cert_serial"]
    res = a.renew()
    assert res["ok"] is False and res["error"]["code"] == "renewal_not_due"
    assert 59 < _days(res["error"]["details"]["renew_after"]) < 61
    assert _row(st, a.worker_id)["cert_serial"] == before
    assert not _audit(st, "worker.certificate_renewed", a.worker_id)


def test_renewal_in_the_window_signs_a_new_certificate(fleet):
    st = fleet
    a = st.agent("cred-renew")
    old = _row(st, a.worker_id)["cert_serial"]
    old_cert_pem = a.cert_pem
    _age_cert(st, a.worker_id, 10)
    res = a.renew()
    assert res["ok"], res
    d = res["data"]
    assert d["renewed"] == ["certificate"] and "worker_token" not in d and d["ca_pem"] and d["serial"] != old
    cert = _cert_of(d["cert_pem"])
    ok, who = st.ctx.pki.verify_client_cert(d["cert_pem"])
    assert ok and who == a.worker_id  # the controller decides the identity: CN is the worker id, whatever the CSR asked for
    assert cert.subject.get_attributes_for_oid(NameOID.COMMON_NAME)[0].value == a.worker_id
    assert 89 < _days(d["cert_expires_at"]) < 91 and d["cert_expires_at"] == st.worker(a.worker_id)["cert_expires_at"]
    row = _row(st, a.worker_id)
    assert row["cert_serial"] == d["serial"] and a.cert_pem != old_cert_pem
    # audit (actor: the worker) + event
    rec = _audit(st, "worker.certificate_renewed", a.worker_id)
    assert rec and rec[0]["details"]["serial"] == d["serial"] and rec[0]["details"]["previous_serial"] == old
    evs = st.http.get("/v1/events", headers=st.admin, params={"type": "worker.certificate_renewed", "limit": 20}).json()["items"]
    assert any(e["payload"].get("worker_id") == a.worker_id for e in evs)
    assert "BEGIN" not in json.dumps(rec) and "cert_pem" not in json.dumps(rec)  # no certificate or key material in the audit trail


def test_old_certificate_is_revoked_once_the_new_one_is_in_use(fleet):
    st = fleet
    hub = st.compute.hub
    a = st.agent("cred-swap", heartbeat=0.5)
    old = _row(st, a.worker_id)["cert_serial"]
    _age_cert(st, a.worker_id, 5)
    res = a.renew()
    assert res["ok"]
    new = res["data"]["serial"]
    assert old not in hub.revoked_serials(), "the old certificate must keep working until the agent has switched (a lost response must not lock the node out)"
    a.drop()
    a.reconnect()  # the agent's next connection: it holds the new certificate now
    wait_until(lambda: old in hub.revoked_serials(), 10, 0.1, "the old certificate to be revoked after the first connection with the new one")
    assert new not in hub.revoked_serials()
    assert st.worker(a.worker_id)["state"] in ("online", "degraded")


def test_unfinished_rotation_is_closed_when_the_grace_expires(fleet, policy):
    st = fleet
    a = st.agent("cred-grace")
    old = _row(st, a.worker_id)["cert_serial"]
    policy(token_grace_seconds=1)
    _age_cert(st, a.worker_id, 5)
    assert a.renew()["ok"]
    wait_until(lambda: (st.compute.credentials.sweep(), old in st.compute.hub.revoked_serials())[1], 15, 0.5, "the sweep to revoke the superseded certificate after the grace period")


def test_window_and_lifetime_are_settings(fleet, policy):
    st = fleet
    a = st.agent("cred-policy")
    _age_cert(st, a.worker_id, 10)
    policy(cert_renew_before_days=5)
    refused = a.renew()
    assert refused["ok"] is False and refused["error"]["code"] == "renewal_not_due"
    policy(cert_renew_before_days=15, cert_lifetime_days=30)
    res = a.renew()
    assert res["ok"], res
    assert 29 < _days(res["data"]["cert_expires_at"]) < 31


def test_invalid_csr_is_rejected_and_nothing_changes(fleet):
    st = fleet
    a = st.agent("cred-badcsr")
    _age_cert(st, a.worker_id, 5)
    before = _row(st, a.worker_id)["cert_serial"]
    res = a.renew(csr_pem="-----BEGIN CERTIFICATE REQUEST-----\nnot a csr\n-----END CERTIFICATE REQUEST-----\n")
    assert res["ok"] is False and res["error"]["code"] == "bad_csr"
    assert _row(st, a.worker_id)["cert_serial"] == before


def test_renewals_cannot_be_hammered(fleet):
    st = fleet
    a = st.agent("cred-rate")
    _age_cert(st, a.worker_id, 5)
    assert a.renew()["ok"]
    _age_cert(st, a.worker_id, 5)
    again = a.renew()
    assert again["ok"] is False and again["error"]["code"] == "renewal_too_soon"


def test_auto_renew_off_refuses_scheduled_renewals_but_not_the_operator(fleet, policy):
    st = fleet
    a = st.agent("cred-manual")
    _age_cert(st, a.worker_id, 5)
    policy(cert_auto_renew=False)
    res = a.renew(reason="scheduled")
    assert res["ok"] is False and res["error"]["code"] == "auto_renew_disabled"
    r = st.http.post(f"/v1/workers/{a.worker_id}/renew", headers=st.admin, json={})
    assert r.status_code == 200, r.text
    assert "certificate" in r.json()["renewed"]


def test_revoking_a_worker_revokes_every_credential_including_the_superseded_ones(fleet, policy):
    st = fleet
    a = st.agent("cred-revoke")
    policy(token_max_age_days=60)  # the token predates the policy: due at the next exchange
    old_token, old_serial = a.token, _row(st, a.worker_id)["cert_serial"]
    _age_cert(st, a.worker_id, 5)
    res = a.renew()
    assert res["ok"] and set(res["data"]["renewed"]) == {"certificate", "token"}
    new_token, new_serial = a.token, res["data"]["serial"]
    assert st.http.post(f"/v1/workers/{a.worker_id}/revoke", headers=st.admin, json={}).status_code == 200
    revoked = st.compute.hub.revoked_serials()
    assert {old_serial, new_serial} <= revoked
    assert _rejected(st, old_token) and _rejected(st, new_token)
    from olcf.core.errors import OlcfError

    with pytest.raises(OlcfError):  # and a revoked worker can no longer get anything signed
        st.compute.credentials.renew(a.worker_id, None, {"csr_pem": "x"}, ip=None)


# ------------------------------------------------------------------------------------------------------------------ token expiry and rotation
def test_default_keeps_worker_tokens_unlimited_and_unrotated(fleet):
    st = fleet
    a = st.agent("cred-default-token")
    _age_cert(st, a.worker_id, 5)
    res = a.renew()
    assert res["ok"] and res["data"]["renewed"] == ["certificate"] and "worker_token" not in res["data"]
    with st.ctx.db.connect() as conn:
        from sqlalchemy import select

        from olcf.db import schema as S

        exp = conn.execute(select(S.api_tokens.c.expires_at).where(S.api_tokens.c.id == _row(st, a.worker_id)["token_id"])).scalar_one()
    assert exp is None


def test_token_max_age_rotates_the_token_in_the_renewal_exchange(fleet, policy):
    st = fleet
    a = st.agent("cred-token")
    old_token = a.token
    block = a.hello_acks[-1]["credentials"]
    assert _days(block["renew_after"]) > 30  # nothing due while the policy is the default
    policy(token_max_age_days=30)
    a.drop()
    a.reconnect()
    assert _days(a.hello_acks[-1]["credentials"]["renew_after"]) < 0.01, "a token without expiry is due for rotation as soon as a max age is set"
    res = a.renew()
    assert res["ok"], res
    d = res["data"]
    assert d["renewed"] == ["token"]  # the certificate is not due
    assert d["worker_token"].startswith("olcf_wrk_") and d["worker_token"] != old_token
    assert 29 < _days(d["token_expires_at"]) < 31 and st.worker(a.worker_id)["token_expires_at"] == d["token_expires_at"]
    assert d["token_grace_seconds"] == 86400
    assert "worker.token_rotated" in {x["action"] for x in _audit(st, "worker.token_rotated", a.worker_id)}
    # both tokens work until the new one has been used: a response lost in transit must not lock the node out
    assert _ws_result(st, old_token)[0] == "open"
    a.drop()
    a.reconnect()  # authenticates with the new token
    wait_until(lambda: _rejected(st, old_token), 10, 0.2, "the superseded token to be revoked after the first connection with the new one")
    assert _ws_result(st, a.token)[0] in ("open", "closed")  # the new token is accepted (a second connection replaces the first one)
    assert a.connections >= 2


def test_new_workers_get_expiring_tokens_when_a_max_age_is_set(fleet, policy):
    st = fleet
    policy(token_max_age_days=7)
    a = st.agent("cred-expiring")
    w = st.worker(a.worker_id)
    assert 6 < _days(w["token_expires_at"]) < 8
    assert a.hello_acks[-1]["credentials"]["token_expires_at"] == w["token_expires_at"]


def test_expired_token_is_rejected_by_the_channel(fleet):
    st = fleet
    a = st.agent("cred-expired-token", start=False)
    from sqlalchemy import update

    from olcf.db import schema as S

    with st.ctx.db.begin() as conn:
        conn.execute(update(S.api_tokens).where(S.api_tokens.c.id == _row(st, a.worker_id)["token_id"]).values(expires_at=to_iso(dt.datetime.now(dt.UTC) - dt.timedelta(minutes=1))))
    assert _rejected(st, a.token or "")


def test_rotation_when_the_agent_never_received_the_new_token(fleet, policy):
    """The response is lost: the agent still holds the superseded token (valid during the grace period) and asks again; the undelivered token is revoked."""
    st = fleet
    a = st.agent("cred-lost")
    policy(token_max_age_days=30)
    first = a.renew(apply=False)  # the agent "crashed" before saving the result
    assert first["ok"] and first["data"]["worker_token"]
    lost = first["data"]["worker_token"]
    a.drop()
    a.reconnect()  # still authenticates with the old token
    assert st.worker(a.worker_id)["state"] in ("online", "degraded")
    second = a.renew()
    assert second["ok"], second
    assert _rejected(st, lost), "the token that was never delivered must not stay valid"


# ------------------------------------------------------------------------------------------------------------------ operator command
def test_operator_renew_runs_the_exchange_through_the_agent(fleet):
    st = fleet
    a = st.agent("cred-operator")
    old = _row(st, a.worker_id)["cert_serial"]
    r = st.http.post(f"/v1/workers/{a.worker_id}/renew", headers=st.admin, json={})
    assert r.status_code == 200, r.text
    d = r.json()
    assert d["renewed"] == ["certificate", "token"] and d["worker"]["id"] == a.worker_id
    assert _row(st, a.worker_id)["cert_serial"] != old and a.commands_of("renew_credentials")
    assert 89 < _days(d["worker"]["cert_expires_at"]) < 91
    assert _audit(st, "worker.credentials_renewal_requested", a.worker_id) and _audit(st, "worker.certificate_renewed", a.worker_id)
    # selective
    only_cert = st.http.post(f"/v1/workers/{a.worker_id}/renew", headers=st.admin, json={"token": False})
    assert only_cert.status_code == 200 and only_cert.json()["renewed"] == ["certificate"]
    assert st.http.post(f"/v1/workers/{a.worker_id}/renew", headers=st.admin, json={"token": False, "certificate": False}).status_code == 400


def test_operator_renew_rbac_and_errors(fleet):
    st = fleet
    a = st.agent("cred-rbac")
    ws = st.workspace()
    assert st.http.post(f"/v1/workers/{a.worker_id}/renew", headers=st.user("Viewer", ws), json={}).status_code == 403
    assert st.http.post(f"/v1/workers/{a.worker_id}/renew", json={}).status_code == 401
    assert st.http.post("/v1/workers/wrk_nope/renew", headers=st.admin, json={}).status_code == 404
    a.fail_commands["renew_credentials"] = "disk is read-only"
    bad = st.http.post(f"/v1/workers/{a.worker_id}/renew", headers=st.admin, json={})
    assert bad.status_code >= 400 and "read-only" in bad.text
    a.stop()
    wait_until(lambda: st.worker(a.worker_id)["connected"] is False, 20, 0.2, "the worker to disconnect")
    off = st.http.post(f"/v1/workers/{a.worker_id}/renew", headers=st.admin, json={})
    assert off.status_code == 409 and "renew" in json.dumps(off.json()).lower()


# ------------------------------------------------------------------------------------------------------------------ health
def test_certificate_health_check_thresholds(fleet):
    st = fleet
    a = st.agent("cred-health-a")
    b = st.agent("cred-health-b")
    names = {a.worker_id: "cred-health-a", b.worker_id: "cred-health-b"}

    def check() -> Any:
        h = st.ctx.health.run("worker-certificates", force=True)
        return h

    _age_cert(st, a.worker_id, 80)
    _age_cert(st, b.worker_id, 80)
    assert check().status.value == "ok"
    _age_cert(st, a.worker_id, 20)  # warn below 30 days
    h = check()
    assert h.status.value == "degraded" and "cred-health-a" in h.detail and "cred-health-b" not in h.detail
    _age_cert(st, b.worker_id, 3)  # fail below 7 days
    h = check()
    assert h.status.value == "down" and "cred-health-b" in h.detail
    _age_cert(st, b.worker_id, -2)  # expired
    h = check()
    assert h.status.value == "down" and "expired" in h.detail.lower()
    # the topology and the health endpoint carry it
    topo = st.http.get("/v1/compute/topology", headers=st.admin).json()
    assert any(c["name"] == "worker-certificates" for c in topo["control_plane"]["components"])
    # revoked nodes do not count
    st.http.post(f"/v1/workers/{b.worker_id}/revoke", headers=st.admin, json={})
    assert check().status.value == "degraded"
    st.http.post(f"/v1/workers/{a.worker_id}/revoke", headers=st.admin, json={})
    assert names and check().status.value == "ok"


# ------------------------------------------------------------------------------------------------------------------ CLI and SDK
def _cli(st: Any, *args: str) -> Any:
    from click.testing import CliRunner

    from olcf.cli.main import cli

    env = {"OLCF_URL": st.url, "OLCF_TOKEN": st.admin["Authorization"].split()[1], "NO_COLOR": "1", "NO_PROXY": "127.0.0.1,localhost", "OLCF_CONFIG_DIR": str(st.root / "cli-config")}
    return CliRunner().invoke(cli, list(args), env=env, catch_exceptions=False)


def test_cli_worker_renew_and_sdk(fleet):
    st = fleet
    a = st.agent("cred-cli")
    old = _row(st, a.worker_id)["cert_serial"]
    res = _cli(st, "--json", "worker", "renew", "cred-cli")
    assert res.exit_code == 0, res.output
    doc = json.loads(res.stdout)
    assert doc["renewed"] == ["certificate", "token"] and doc["worker"]["cert_expires_at"]
    assert _row(st, a.worker_id)["cert_serial"] != old
    human = _cli(st, "worker", "renew", "cred-cli", "--no-token").output
    assert "certificate" in human and "expires" in human.lower()
    shown = _cli(st, "worker", "get", "cred-cli").output
    assert "cert_expires_at" in shown and "cert_days_left" in shown  # the expiry is part of `worker get`
    # the SDK
    from olcf.sdk import Client

    c = Client(st.url, token=st.admin["Authorization"].split()[1], max_retries=0)
    out = c.compute.renew_worker(a.worker_id, token=False)
    assert out["renewed"] == ["certificate"]
    # the CLI explains a node that is not connected
    a.stop()
    wait_until(lambda: st.worker(a.worker_id)["connected"] is False, 20, 0.2, "the worker to disconnect")
    off = _cli(st, "worker", "renew", "cred-cli")
    assert off.exit_code != 0 and "not connected" in off.output
