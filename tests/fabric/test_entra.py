# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Inbound Entra token validation (local RSA key + local JWKS server) and outbound broker token acquisition."""

from __future__ import annotations

import time
import uuid

import jwt
import pytest

from olcf.fabric.entra import EntraTokenError, EntraValidator, TokenBroker, summarise_token
from olcf.fabric.errors import TokenInvalid
from olcf.fabric.http import Network
from tests.fabric.fabric_fakes import (
    API_AUDIENCE,
    BROKER_CLIENT_ID,
    BROKER_SECRET,
    OTHER_TENANT,
    TENANT,
    FakeFabricWorld,
    new_rsa,
)


@pytest.fixture()
def validator(world: FakeFabricWorld) -> EntraValidator:
    return EntraValidator(allowed_tenants=[TENANT], audiences=[API_AUDIENCE], network=Network(**{k: v for k, v in world.network_config().items() if k != "trust_env"}, trust_env=False))


def test_valid_v2_token(world: FakeFabricWorld, validator: EntraValidator) -> None:
    oid = str(uuid.uuid4())
    ident = validator.validate(world.mint(oid=oid, appid="app-1", roles=["Task.Execute"], upn="a@contoso.example"))
    assert ident.tenant_id == TENANT and ident.object_id == oid and ident.app_id == "app-1" and ident.roles == ("Task.Execute",)
    assert validator.cached_tenants() == {TENANT: 1}


def test_v1_issuer_and_api_prefixed_audience(world: FakeFabricWorld, validator: EntraValidator) -> None:
    assert validator.validate(world.mint(ver="1.0")).tenant_id == TENANT
    app_id = str(uuid.uuid4())
    net = Network(**{k: v for k, v in world.network_config().items() if k != "trust_env"}, trust_env=False)
    guid_validator = EntraValidator(allowed_tenants=[TENANT], audiences=[f"api://{app_id}"], network=net)
    assert guid_validator.validate(world.mint(aud=app_id)).tenant_id == TENANT       # v2 tokens carry the bare application id; api://<id> is equivalent
    assert guid_validator.validate(world.mint(aud=f"api://{app_id}")).tenant_id == TENANT


@pytest.mark.parametrize("kwargs,needle", [
    ({"tid": OTHER_TENANT}, "tenant"),
    ({"aud": "api://not-us"}, "audience"),
    ({"exp_in": -600}, "expired"),
    ({"nbf_in": 3600}, "invalid"),
    ({"iss": "https://evil.example/" + TENANT + "/v2.0"}, "issuer"),
])
def test_rejected_tokens(world: FakeFabricWorld, validator: EntraValidator, kwargs: dict, needle: str) -> None:
    with pytest.raises(TokenInvalid) as ei:
        validator.validate(world.mint(**kwargs))
    assert needle in str(ei.value).lower()


def test_alg_none_hs256_and_garbage_are_rejected(world: FakeFabricWorld, validator: EntraValidator) -> None:
    with pytest.raises(TokenInvalid):
        validator.validate(world.mint(alg="none"))
    forged = jwt.encode({"iss": f"{world.base}/{TENANT}/v2.0", "aud": API_AUDIENCE, "tid": TENANT, "exp": int(time.time()) + 600}, "x" * 40, algorithm="HS256", headers={"kid": world.kid})
    with pytest.raises(TokenInvalid) as ei:
        validator.validate(forged)
    assert "RS256" in str(ei.value)
    with pytest.raises(TokenInvalid):
        validator.validate("not.a.jwt")


def test_token_signed_with_an_unknown_key_is_rejected(world: FakeFabricWorld, validator: EntraValidator) -> None:
    with pytest.raises(TokenInvalid) as ei:
        validator.validate(world.mint(key=new_rsa()))        # right kid, wrong key
    assert "invalid" in str(ei.value).lower()
    with pytest.raises(TokenInvalid) as ei2:
        validator.validate(world.mint(kid="kid-missing"))
    assert "unknown key" in str(ei2.value)


def test_no_configuration_means_no_access(world: FakeFabricWorld) -> None:
    net = Network(**{k: v for k, v in world.network_config().items() if k != "trust_env"}, trust_env=False)
    with pytest.raises(TokenInvalid):
        EntraValidator(allowed_tenants=[], audiences=[API_AUDIENCE], network=net).validate(world.mint())
    with pytest.raises(TokenInvalid):
        EntraValidator(allowed_tenants=[TENANT], audiences=[], network=net).validate(world.mint())


def test_key_rotation_refreshes_the_jwks_with_rate_limit(world: FakeFabricWorld) -> None:
    clock = [1000.0]
    net = Network(**{k: v for k, v in world.network_config().items() if k != "trust_env"}, trust_env=False)
    v = EntraValidator(allowed_tenants=[TENANT], audiences=[API_AUDIENCE], network=net, now=lambda: clock[0], jwks_min_refresh_seconds=20)
    v.validate(world.mint())
    fetches = lambda: sum(1 for m, p in world.calls if p.endswith("/discovery/v2.0/keys"))  # noqa: E731
    base = fetches()
    world.rotate_key(keep_old=True)
    clock[0] += 5
    with pytest.raises(TokenInvalid):                           # new kid, but a refresh happened less than 20 s ago: no new fetch
        v.validate(world.mint())
    assert fetches() == base
    clock[0] += 30
    assert v.validate(world.mint()).tenant_id == TENANT          # rate limit elapsed: refresh finds the new key
    assert fetches() == base + 1
    assert v.validate(world.mint(key=world.old_keys[0][0], kid=world.old_keys[0][1])).tenant_id == TENANT     # old key still published


def test_jwks_outage_keeps_the_cached_keys() -> None:
    world = FakeFabricWorld().start()
    try:
        _outage(world)
    finally:
        world.stop()


def _outage(world: FakeFabricWorld) -> None:
    clock = [1000.0]
    net = Network(**{k: v for k, v in world.network_config().items() if k != "trust_env"}, trust_env=False)
    v = EntraValidator(allowed_tenants=[TENANT], audiences=[API_AUDIENCE], network=net, now=lambda: clock[0], jwks_ttl_seconds=60, jwks_min_refresh_seconds=1)
    tok = world.mint()
    v.validate(tok)
    world.hide_current_key = True
    clock[0] += 120                                              # cache stale -> refresh returns an empty key set -> previous keys are kept
    try:
        assert v.validate(world.mint()).tenant_id == TENANT
    finally:
        world.hide_current_key = False


# ------------------------------------------------------------------------------------------------ broker
def test_broker_acquires_and_caches_tokens(world: FakeFabricWorld) -> None:
    clock = [5000.0]
    net = Network(**{k: v for k, v in world.network_config().items() if k != "trust_env"}, trust_env=False)
    b = TokenBroker(tenant_id=TENANT, client_id=BROKER_CLIENT_ID, secret={"client_secret": BROKER_SECRET}, network=net, now=lambda: clock[0])
    world.token_requests.clear()
    t1 = b.get_token("https://api.fabric.microsoft.com/.default")
    assert b.get_token("https://api.fabric.microsoft.com/.default") == t1 and len(world.token_requests) == 1
    claims = b.decode_unverified(t1)
    assert claims["aud"] == "https://api.fabric.microsoft.com" and claims["appid"] == BROKER_CLIENT_ID
    assert BROKER_SECRET not in summarise_token(claims)
    clock[0] += 4000                                             # past expiry minus the safety margin
    b.get_token("https://api.fabric.microsoft.com/.default")
    assert len(world.token_requests) == 2
    b.get_token("https://storage.azure.com/.default")
    assert world.token_requests[-1]["scope"] == "https://storage.azure.com/.default"


def test_broker_errors_carry_the_aadsts_code_and_never_the_secret(world: FakeFabricWorld) -> None:
    net = Network(**{k: v for k, v in world.network_config().items() if k != "trust_env"}, trust_env=False)
    b = TokenBroker(tenant_id=TENANT, client_id=BROKER_CLIENT_ID, secret="wrong-secret-value", network=net)  # noqa: S106
    with pytest.raises(EntraTokenError) as ei:
        b.get_token("https://api.fabric.microsoft.com/.default")
    assert ei.value.code == "AADSTS7000215" and ei.value.remediation and "wrong-secret-value" not in str(ei.value)
    with pytest.raises(EntraTokenError):
        TokenBroker(tenant_id="", client_id="x", secret="s")  # noqa: S106
    with pytest.raises(EntraTokenError) as ei2:
        TokenBroker(tenant_id=TENANT, client_id="c", secret={}, network=net).get_token("s")
    assert "client_secret" in str(ei2.value)


def test_broker_unreachable_authority_is_reported() -> None:
    net = Network(authority_host="http://127.0.0.1:9", trust_env=False, timeout_seconds=1.0)
    with pytest.raises(EntraTokenError) as ei:
        TokenBroker(tenant_id=TENANT, client_id="c", secret="s", network=net).get_token("x/.default")  # noqa: S106
    assert "reach" in str(ei.value) and ei.value.remediation
