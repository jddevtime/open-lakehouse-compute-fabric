# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Review: the development runtime-signing key (its private half is in the source tree) is not trusted in production unless an operator opts in.

Covers the policy matrix, the store the control plane builds from its settings, the actionable rejection, the ``runtime-trust`` health check, the control-plane manifest
validation, the ``olcf.runtime trust`` sub-command and that a fresh ``olcf init`` / quickstart configuration keeps working."""

from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from olcf.core.config import load_settings
from olcf.core.errors import ValidationFailed
from olcf.core.health import Status
from olcf.runtime.errors import SignatureInvalid
from olcf.runtime.manager import bundled_manifest_paths
from olcf.runtime.signing import (
    TrustStore,
    dev_key_ids,
    generate_keypair,
    key_id_for,
    load_private_key,
    load_public_key,
    resolve_dev_key_policy,
    sign_manifest_dict,
    verify_manifest,
)
from olcf.security import runtime_trust

DEV_KEY_ID = "8ea01f731883a91d"


@pytest.fixture(autouse=True)
def _clean_env(monkeypatch: pytest.MonkeyPatch) -> None:
    for k in ("OLCF_RUNTIME_ALLOW_DEV_KEY", "OLCF_RUNTIME_TRUST_DIR", "OLCF_ENVIRONMENT"):
        monkeypatch.delenv(k, raising=False)


def _dev_signed() -> dict:
    return json.loads(next(p for p in bundled_manifest_paths() if p.stem == "olcf-rt-2.0.0").read_text())


def _settings(tmp_path: Path, **kw):
    return load_settings(tmp_path / "data", **kw)


# ------------------------------------------------------------------------------------------------------------------------ policy
@pytest.mark.parametrize(("explicit", "env_var", "environment", "expected"), [
    (None, None, "production", False),
    (None, None, None, False),  # nothing configured at all = production
    (None, None, "development", True),
    (None, None, "test", True),
    (True, None, "production", True),  # an explicit opt-in is honoured (and reported by the health check)
    (False, None, "development", False),
    (None, "0", "development", False),
    (None, "1", "production", True),
    (False, "1", "development", False),  # the setting beats the legacy variable
    (True, "0", "production", True),
])
def test_policy_matrix(monkeypatch: pytest.MonkeyPatch, explicit, env_var, environment, expected) -> None:
    if env_var is not None:
        monkeypatch.setenv("OLCF_RUNTIME_ALLOW_DEV_KEY", env_var)
    assert resolve_dev_key_policy(explicit, environment) is expected


def test_the_environment_variable_names_the_environment_when_no_setting_object_exists(monkeypatch: pytest.MonkeyPatch) -> None:
    assert not TrustStore.default().trusts_dev_key()  # a bare worker with no configuration: production
    monkeypatch.setenv("OLCF_ENVIRONMENT", "development")
    assert TrustStore.default().trusts_dev_key()
    monkeypatch.setenv("OLCF_ENVIRONMENT", "production")
    assert not TrustStore.default().trusts_dev_key()


def test_the_bundled_dev_key_is_flagged_by_id() -> None:
    assert dev_key_ids() == [DEV_KEY_ID] and TrustStore.builtin().trusts_dev_key()


# ------------------------------------------------------------------------------------------------------------------------ production rejects it
def test_production_rejects_a_dev_signed_manifest_with_an_actionable_message(tmp_path: Path) -> None:
    store = TrustStore.for_settings(_settings(tmp_path, environment="production"))
    assert not store.trusts_dev_key()
    with pytest.raises(SignatureInvalid) as ei:
        verify_manifest(_dev_signed(), store)
    msg = f"{ei.value.message} {ei.value.remediation}"
    assert "development key" in msg and DEV_KEY_ID in msg
    assert "keygen" in msg and "OLCF_RUNTIME_TRUST_DIR" in msg and "allow_development_key" in msg  # what to do, for production and for a dev machine


def test_a_forged_manifest_signed_with_the_published_dev_private_key_is_rejected_in_production(tmp_path: Path) -> None:
    from olcf.runtime.signing import KEYS_DIR

    priv = next(p for p in KEYS_DIR.iterdir() if p.is_file() and b"PRIVATE KEY" in p.read_bytes())  # the published development private key
    forged = sign_manifest_dict({**_dev_signed(), "id": "attacker-rt"}, load_private_key(priv))
    assert verify_manifest(forged, TrustStore.builtin()).dev_key is True  # what a naive store would accept
    with pytest.raises(SignatureInvalid):
        verify_manifest(forged, TrustStore.for_settings(_settings(tmp_path, environment="production")))


def test_development_and_test_environments_trust_it(tmp_path: Path) -> None:
    for env in ("development", "test"):
        store = TrustStore.for_settings(_settings(tmp_path, environment=env))
        assert verify_manifest(_dev_signed(), store).dev_key is True


def test_explicit_opt_in_in_production_and_explicit_opt_out_in_development(tmp_path: Path) -> None:
    on = _settings(tmp_path, environment="production", runtime={"trust": {"allow_development_key": True}})
    assert on.development_key_allowed and verify_manifest(_dev_signed(), TrustStore.for_settings(on)).dev_key is True
    off = _settings(tmp_path, environment="development", runtime={"trust": {"allow_development_key": False}})
    assert not off.development_key_allowed
    with pytest.raises(SignatureInvalid):
        verify_manifest(_dev_signed(), TrustStore.for_settings(off))


def test_an_operator_trust_dir_replaces_the_built_in_store_and_signs_manifests(tmp_path: Path) -> None:
    priv, pub = generate_keypair()
    d = tmp_path / "trust"
    d.mkdir()
    (d / "org.pub").write_bytes(pub)
    for env in ("production", "development"):
        s = _settings(tmp_path, environment=env, runtime={"trust": {"dir": str(d)}})
        store = TrustStore.for_settings(s)
        assert store.key_ids() == [key_id_for(load_public_key(pub))] and not store.trusts_dev_key()
        with pytest.raises(SignatureInvalid):
            verify_manifest(_dev_signed(), store)
        mine = sign_manifest_dict(_dev_signed(), load_private_key(priv))
        assert verify_manifest(mine, store).signer == "org"


# ------------------------------------------------------------------------------------------------------------------------ control plane
def _catalog(settings):
    from olcf.compute.runtimes import RuntimeCatalog

    return SimpleNamespace(ctx=SimpleNamespace(settings=settings), validate_manifest=lambda data, **kw: RuntimeCatalog.validate_manifest(SimpleNamespace(ctx=SimpleNamespace(settings=settings)), data, **kw))


def test_the_control_plane_refuses_a_dev_signed_manifest_in_production_and_accepts_it_in_development(tmp_path: Path) -> None:
    prod = _catalog(_settings(tmp_path, environment="production"))
    with pytest.raises(ValidationFailed) as ei:
        prod.validate_manifest(_dev_signed())
    assert "development key" in ei.value.message and ei.value.remediation and "keygen" in ei.value.remediation
    dev = _catalog(_settings(tmp_path, environment="development"))
    assert dev.validate_manifest(_dev_signed())["id"] == "olcf-rt-2.0.0"


# ------------------------------------------------------------------------------------------------------------------------ health
def test_health_check_states(tmp_path: Path) -> None:
    prod = runtime_trust.check(_settings(tmp_path, environment="production"))
    assert prod.name == "runtime-trust" and prod.status is Status.DEGRADED and "no runtime signing key" in prod.detail  # nothing trusted at all
    opted_in = runtime_trust.check(_settings(tmp_path, environment="production", runtime={"trust": {"allow_development_key": True}}))
    assert opted_in.status is Status.DOWN and "development" in opted_in.detail and "production" in opted_in.detail
    dev = runtime_trust.check(_settings(tmp_path, environment="development"))
    assert dev.status is Status.DEGRADED and dev.labels["dev_key_trusted"] is True
    priv, pub = generate_keypair()
    (tmp_path / "t").mkdir()
    (tmp_path / "t" / "mine.pub").write_bytes(pub)
    ok = runtime_trust.check(_settings(tmp_path, environment="production", runtime={"trust": {"dir": str(tmp_path / "t")}}))
    assert ok.status is Status.OK and ok.labels["dev_key_trusted"] is False
    del priv


def test_dev_key_notice_points_at_the_endpoint_that_really_carries_the_check(tmp_path: Path) -> None:
    from olcf.compute.runtimes import RuntimeCatalog

    notice = RuntimeCatalog.dev_key_notice(_catalog(_settings(tmp_path, environment="production")), ["olcf-rt-2.0.0"])
    assert "GET /v1/admin/health" in notice and "GET /v1/health)" not in notice and "olcf health" in notice
    # the check exists only in the authenticated report; the unauthenticated probe is just {"status": "ok"}
    from fastapi.testclient import TestClient

    from olcf.api.app import create_app

    s = _settings(tmp_path, environment="test")
    s.compute.embedded_worker = False
    with TestClient(create_app(s)) as client:
        assert "runtime-trust" not in client.get("/v1/health").text


def test_remediation_follows_the_labels_of_the_health_result(tmp_path: Path) -> None:
    down = runtime_trust.check(_settings(tmp_path, environment="production", runtime={"trust": {"allow_development_key": True}}))
    assert runtime_trust.remediation_for(down.labels) == runtime_trust.DEV_KEY_FIX and "keygen" in runtime_trust.DEV_KEY_FIX
    none = runtime_trust.check(_settings(tmp_path, environment="production"))
    assert runtime_trust.remediation_for(none.labels) == runtime_trust.NO_KEY_FIX
    priv, pub = generate_keypair()
    (tmp_path / "t").mkdir()
    (tmp_path / "t" / "mine.pub").write_bytes(pub)
    ok = runtime_trust.check(_settings(tmp_path, environment="production", runtime={"trust": {"dir": str(tmp_path / "t")}}))
    assert runtime_trust.remediation_for(ok.labels) is None
    del priv


def test_health_check_is_registered_on_the_control_plane(tmp_path: Path) -> None:
    from fastapi.testclient import TestClient

    from olcf.api.app import create_app

    s = _settings(tmp_path, environment="test")
    s.compute.embedded_worker = False
    app = create_app(s)
    with TestClient(app):
        res = app.state.ctx.health.run("runtime-trust", force=True)
    assert res.status is Status.DEGRADED and res.category == "security" and res.labels["dev_key_trusted"] is True


# ------------------------------------------------------------------------------------------------------------------------ CLI
def test_trust_subcommand_add_list_remove(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    from olcf.runtime.__main__ import main

    priv, pub = generate_keypair()
    pubfile = tmp_path / "org.pub"
    pubfile.write_bytes(pub)
    d = tmp_path / "trust"
    kid = key_id_for(load_public_key(pub))
    assert main(["--json", "--trust-dir", str(d), "trust", "add", str(pubfile)]) == 0
    assert (d / "org.pub").is_file()
    capsys.readouterr()
    assert main(["--json", "--trust-dir", str(d), "trust", "list"]) == 0
    listed = json.loads(capsys.readouterr().out)
    assert listed["key_ids"] == [kid] and listed["keys"][0]["development"] is False
    assert main(["--json", "--trust-dir", str(d), "trust", "remove", kid]) == 0
    assert not list(d.glob("*.pub"))
    del priv


def test_trust_add_refuses_a_private_key_and_needs_a_directory(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    from olcf.runtime.__main__ import main

    priv, _pub = generate_keypair()
    keyfile = tmp_path / "x.key"
    keyfile.write_bytes(priv)
    with pytest.raises(ValueError):  # the PEM loader rejects a private key where a public one is expected
        main(["--trust-dir", str(tmp_path / "t"), "trust", "add", str(keyfile)])
    assert main(["trust", "add", str(keyfile)]) == 1  # no trust dir configured: an actionable error, nothing written
    assert "trust directory" in capsys.readouterr().err
    assert not (tmp_path / "t").exists()


def test_trust_list_without_a_directory_flags_the_dev_key_in_development(monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]) -> None:
    from olcf.runtime.__main__ import main

    assert main(["--json", "trust", "list"]) == 0
    assert json.loads(capsys.readouterr().out)["keys"] == []  # production by default: nothing trusted
    monkeypatch.setenv("OLCF_ENVIRONMENT", "development")
    assert main(["--json", "trust", "list"]) == 0
    keys = json.loads(capsys.readouterr().out)["keys"]
    assert [k["key_id"] for k in keys] == [DEV_KEY_ID] and keys[0]["development"] is True


# ------------------------------------------------------------------------------------------------------------------------ quickstart / init
def test_a_fresh_init_configuration_is_a_development_environment_that_trusts_the_dev_key(tmp_path: Path) -> None:
    from olcf.cli.commands.lifecycle import write_config

    dd = tmp_path / "data"
    dd.mkdir()
    write_config(dd, host="127.0.0.1", port=8470, public_url=None, tls=False, database_url=None, force=False)
    s = load_settings(dd)
    assert s.environment == "development" and s.development_key_allowed
    assert verify_manifest(_dev_signed(), TrustStore.for_settings(s)).dev_key is True
    assert runtime_trust.check(s).status is Status.DEGRADED  # visible, not an error


def test_the_embedded_worker_gets_the_same_policy_as_the_control_plane(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    from olcf.compute import embedded

    captured: dict = {}

    class Stop(Exception):
        pass

    def fake_popen(cmd, **kw):
        captured.update(kw["env"])
        raise Stop

    for env, expect in (("production", "0"), ("development", "1")):
        s = _settings(tmp_path / env, environment=env)
        s.data_dir.mkdir(parents=True, exist_ok=True)
        agent = embedded.EmbeddedWorker.__new__(embedded.EmbeddedWorker)
        agent.ctx, agent.state_dir = SimpleNamespace(settings=s), tmp_path / env / "worker"
        agent.log_path = agent.state_dir / "log" / "w.log"
        monkeypatch.setattr(embedded.subprocess, "Popen", fake_popen)
        monkeypatch.setattr(agent, "_command", lambda: ["x"], raising=False)
        monkeypatch.setattr(agent, "_identity_valid", lambda: True, raising=False)
        with pytest.raises(Stop):
            agent._spawn()  # noqa: SLF001
        assert captured["OLCF_RUNTIME_ALLOW_DEV_KEY"] == expect


# ------------------------------------------------------------------------------------------------------------------------ readiness (C05)
def _probe(tmp_path: Path, **kw) -> tuple[int, dict, int, dict, bool]:
    """Boot the app with ``kw`` and return (ready status, ready body, live status, live body, whether runtime-trust is registered as critical)."""
    from fastapi.testclient import TestClient

    from olcf.api.app import create_app

    s = _settings(tmp_path, **kw)
    s.compute.embedded_worker = False
    app = create_app(s)
    with TestClient(app) as client:
        ready, live = client.get("/v1/health/ready"), client.get("/v1/health")
        critical = {c["name"]: c["critical"] for c in app.state.ctx.health.describe()}["runtime-trust"]
        return ready.status_code, ready.json(), live.status_code, live.json(), critical


def test_production_that_trusts_the_development_key_fails_readiness_but_not_liveness(tmp_path: Path) -> None:
    ready_code, ready_body, live_code, live_body, critical = _probe(tmp_path, environment="production", runtime={"trust": {"allow_development_key": True}})
    assert critical is True
    assert ready_code == 503 and ready_body == {"status": "unavailable"}
    assert live_code == 200 and live_body["status"] == "ok"


def test_the_readiness_and_liveness_bodies_do_not_leak_trust_details(tmp_path: Path) -> None:
    ready_code, ready_body, _, live_body, _ = _probe(tmp_path, environment="production", runtime={"trust": {"allow_development_key": True}})
    text = json.dumps([ready_body, live_body]).lower()
    assert ready_code == 503
    for needle in (DEV_KEY_ID, "development", "runtime-trust", "signing", "key", "keygen", "production"):
        assert needle not in text, needle


@pytest.mark.parametrize("kw", [
    {"environment": "development"},
    {"environment": "test"},
    {"environment": "development", "runtime": {"trust": {"allow_development_key": True}}},
])
def test_development_and_test_environments_stay_ready_with_the_development_key_trusted(tmp_path: Path, kw: dict) -> None:
    ready_code, ready_body, live_code, *_ = _probe(tmp_path, **kw)
    assert ready_code == 200 and ready_body["status"] == "ready" and live_code == 200


def test_production_with_operator_anchors_is_ready(tmp_path: Path) -> None:
    priv, pub = generate_keypair()
    (tmp_path / "t").mkdir()
    (tmp_path / "t" / "mine.pub").write_bytes(pub)
    del priv
    ready_code, ready_body, live_code, *_ = _probe(tmp_path, environment="production", runtime={"trust": {"dir": str(tmp_path / "t")}})
    assert ready_code == 200 and ready_body["status"] == "ready" and live_code == 200


def test_production_with_no_trust_anchor_at_all_is_degraded_but_still_ready(tmp_path: Path) -> None:
    """The out-of-the-box production configuration (nothing trusted) must keep serving: `degraded` is advisory, only `down` fails readiness."""
    ready_code, _, live_code, *_ = _probe(tmp_path, environment="production")
    assert ready_code == 200 and live_code == 200
