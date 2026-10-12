# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Runtime manifests: schema, pins with real hashes, canonical JSON, Ed25519 signatures and the configurable trust store."""

from __future__ import annotations

import copy
import json
import re
from pathlib import Path

import pytest

from olcf.runtime import manifest as M
from olcf.runtime.errors import ManifestInvalid, PlatformUnsupported, SignatureInvalid
from olcf.runtime.manager import ManagerConfig, RuntimeManager, bundled_manifest_paths
from olcf.runtime.signing import (
    TrustedKey,
    TrustStore,
    generate_keypair,
    key_id_for,
    load_private_key,
    load_public_key,
    sign_manifest_dict,
    verify_manifest,
)

BUNDLED = {p.stem: p for p in bundled_manifest_paths()}


def load(name: str) -> dict:
    return json.loads(BUNDLED[name].read_text())


def test_three_runtime_families_are_shipped() -> None:
    assert set(BUNDLED) == {"olcf-rt-2.0.0", "olcf-rt-1.3.0", "olcf-rt-native-1.0.0"}
    fams = {n: M.RuntimeManifest.load(p).family for n, p in BUNDLED.items()}
    assert fams == {"olcf-rt-2.0.0": "fabric-2.0-compatible", "olcf-rt-1.3.0": "fabric-1.3-compatible", "olcf-rt-native-1.0.0": "olcf-native"}


def test_fabric_20_versions_match_the_contract() -> None:
    m = M.RuntimeManifest.load(BUNDLED["olcf-rt-2.0.0"])
    assert m.spark_version == "4.1.1" and m.delta_version == "4.2.0"
    assert m.python_series == "3.13" and m.java_major == 21 and m.scala_binary == "2.13"
    pins = {p.name: p.version for p in m.packages}
    assert pins["pyspark"] == "4.1.1" and pins["delta-spark"] == "4.2.0"


def test_fabric_13_versions() -> None:
    m = M.RuntimeManifest.load(BUNDLED["olcf-rt-1.3.0"])
    assert (m.spark_version, m.delta_version, m.python_series, m.java_major) == ("3.5.5", "3.2.0", "3.11", 11)


@pytest.mark.parametrize("name", sorted(BUNDLED))
def test_bundled_manifests_are_signed_and_verify_with_the_dev_key(name: str) -> None:
    sig = verify_manifest(M.RuntimeManifest.load(BUNDLED[name]), TrustStore.builtin())
    assert sig.dev_key is True


@pytest.mark.parametrize("name", sorted(BUNDLED))
def test_every_pin_has_real_sha256_hashes_for_all_target_platforms(name: str) -> None:
    doc = load(name)
    sha = re.compile(r"^[0-9a-f]{64}$")
    assert doc["packages"], "no pins"
    for p in doc["packages"]:
        assert re.fullmatch(r"[A-Za-z0-9_.-]+", p["name"]) and re.fullmatch(r"[0-9][A-Za-z0-9.!+_-]*", p["version"]), p
        hashes = p["hashes"]
        assert hashes and all(sha.match(h) for h in hashes), f"{p['name']} has malformed hashes"
        assert len(set(hashes)) == len(hashes)
    # requirements text is what pip --require-hashes consumes: every line pinned with ==, every line hashed
    text = M.RuntimeManifest.from_dict(doc).requirements_text()
    assert "--hash=sha256:" in text and "==" in text
    assert not re.search(r"^[A-Za-z0-9_.-]+\s*(>=|<=|~=|>|<)", text, re.M), "unpinned requirement"


def test_pip_pins_cover_platform_specific_wheels() -> None:
    """numpy/pandas ship per-platform wheels: the locked hash set must contain several wheels, not only the sdist."""
    doc = load("olcf-rt-2.0.0")
    pins = {p["name"]: p for p in doc["packages"]}
    assert len(pins["numpy"]["hashes"]) >= 4
    assert len(pins["pyarrow"]["hashes"]) >= 4


def test_unsigned_manifest_is_rejected() -> None:
    doc = load("olcf-rt-2.0.0")
    doc.pop("signature")
    with pytest.raises(SignatureInvalid, match="not signed"):
        verify_manifest(doc, TrustStore.builtin())


def test_tampering_after_signing_is_detected() -> None:
    doc = load("olcf-rt-2.0.0")
    doc["packages"][0]["hashes"][0] = "0" * 64
    with pytest.raises(SignatureInvalid, match="does not match"):
        verify_manifest(doc, TrustStore.builtin())


def test_untrusted_key_is_rejected_and_message_lists_trusted_ids() -> None:
    priv, pub = generate_keypair()
    doc = sign_manifest_dict({k: v for k, v in load("olcf-rt-2.0.0").items() if k != "signature"}, load_private_key(priv))
    with pytest.raises(SignatureInvalid) as ei:
        verify_manifest(doc, TrustStore.builtin())
    assert "untrusted key" in ei.value.message and ei.value.details["trusted_key_ids"]
    # the same manifest verifies once the key is trusted
    store = TrustStore([TrustedKey(key_id_for(load_public_key(pub)), load_public_key(pub), "mine")])
    assert verify_manifest(doc, store).signer == "mine"


def test_trust_dir_is_exclusive_and_configurable(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    priv, pub = generate_keypair()
    (tmp_path / "org.pub").write_bytes(pub)
    monkeypatch.setenv("OLCF_RUNTIME_TRUST_DIR", str(tmp_path))
    store = TrustStore.default()
    assert store.key_ids() == [key_id_for(load_public_key(pub))]  # the dev key is NOT trusted when a trust dir is configured
    with pytest.raises(SignatureInvalid):
        verify_manifest(load("olcf-rt-2.0.0"), store)
    monkeypatch.delenv("OLCF_RUNTIME_TRUST_DIR")
    monkeypatch.setenv("OLCF_RUNTIME_ALLOW_DEV_KEY", "0")
    assert len(TrustStore.default()) == 0  # production hardening: no dev key at all


def test_manager_refuses_untrusted_manifest(tmp_path: Path) -> None:
    mgr = RuntimeManager(ManagerConfig(state_dir=tmp_path, trust=TrustStore([])))
    with pytest.raises(SignatureInvalid):
        mgr.load_manifest(BUNDLED["olcf-rt-2.0.0"])


def test_canonical_json_is_stable_and_excludes_signature() -> None:
    a = M.RuntimeManifest.from_dict(load("olcf-rt-2.0.0"))
    doc = load("olcf-rt-2.0.0")
    doc["signature"]["signed_at"] = "2030-01-01T00:00:00Z"  # signature metadata never changes the signed bytes
    b = M.RuntimeManifest.from_dict(doc)
    assert a.canonical_bytes() == b.canonical_bytes()
    assert M.canonical_json({"b": 1, "a": [2, {"d": 1, "c": 2}]}) == b'{"a":[2,{"c":2,"d":1}],"b":1}'


def test_schema_rejects_bad_manifests() -> None:
    base = load("olcf-rt-2.0.0")
    bad = copy.deepcopy(base)
    bad["family"] = "fabric-9.9-compatible"
    with pytest.raises(ManifestInvalid):
        M.validate_manifest_dict(bad)
    bad = copy.deepcopy(base)
    bad["packages"][0]["hashes"] = []
    with pytest.raises(ManifestInvalid):
        M.validate_manifest_dict(bad)
    bad = copy.deepcopy(base)
    bad["packages"][0]["version"] = ">=1"
    with pytest.raises(ManifestInvalid):
        M.validate_manifest_dict(bad)
    bad = copy.deepcopy(base)
    del bad["spark"]
    with pytest.raises(ManifestInvalid):
        M.validate_manifest_dict(bad)


def test_platform_constraints_fail_closed() -> None:
    m = M.RuntimeManifest.load(BUNDLED["olcf-rt-2.0.0"])
    m.require_platform("linux", "x86_64")
    m.require_platform("macos", "aarch64")
    with pytest.raises(PlatformUnsupported):
        m.require_platform("solaris", "sparc")
    assert M.normalise_os("Darwin") == "macos" and M.normalise_arch("arm64") == "aarch64" and M.normalise_arch("AMD64") == "x86_64"


def test_jars_are_declared_not_hashed_in_the_manifest() -> None:
    m = M.RuntimeManifest.from_dict(load("olcf-rt-2.0.0"))
    coords = [j.coordinate for j in m.jars]
    assert any("delta-spark_2.13" in c and "4.2.0" in c for c in coords)
    assert all(not re.search(r"(latest|release|\[|\()", c, re.I) for c in coords), "version ranges / LATEST are never allowed"


def test_dev_key_readme_warns() -> None:
    from olcf.runtime.signing import KEYS_DIR

    assert "not" in (KEYS_DIR / "README.md").read_text().lower() and "production" in (KEYS_DIR / "README.md").read_text().lower()
