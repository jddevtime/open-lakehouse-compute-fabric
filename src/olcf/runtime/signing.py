# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Ed25519 signing and verification of runtime manifests (spec section 31 "Supply chain").

* The signature covers the canonical JSON of the manifest *without* its ``signature`` member.
* A :class:`TrustStore` maps ``key_id`` (first 16 hex chars of sha256(raw public key)) to a public key.
* Trust is configurable: ``OLCF_RUNTIME_TRUST_DIR`` (a directory of ``*.pub`` PEM files) *replaces* the
  built-in store, or pass a ``TrustStore`` explicitly. The built-in store contains only the **development**
  public key shipped in ``olcf/runtime/keys/``. The matching private key is shipped too (so that developers
  and the test-suite can sign manifests), which means **anyone can forge a manifest accepted by a store that
  trusts it**. Production deployments must point ``OLCF_RUNTIME_TRUST_DIR`` (or the agent's
  ``--runtime-trust-dir``) at their own public key(s) and sign manifests with ``python -m olcf.runtime sign``.
* The development key is therefore **not trusted in production** unless an operator opts in. Resolution order
  (:func:`resolve_dev_key_policy`): the setting ``runtime.trust.allow_development_key`` > ``OLCF_RUNTIME_ALLOW_DEV_KEY``
  (``0``/``1``) > the environment (``OLCF_ENVIRONMENT``; **unset means production**): trusted only when it is not ``production``.
"""

from __future__ import annotations

import base64
import hashlib
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from cryptography.exceptions import InvalidSignature
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey, Ed25519PublicKey

from olcf.core.timeutil import now_iso
from olcf.runtime.errors import SignatureInvalid
from olcf.runtime.manifest import RuntimeManifest, canonical_json

KEYS_DIR = Path(__file__).with_name("keys")
DEV_KEY_ID_FILE = "olcf-dev.pub"
TRUST_DIR_ENV = "OLCF_RUNTIME_TRUST_DIR"
ALLOW_DEV_ENV = "OLCF_RUNTIME_ALLOW_DEV_KEY"
ENVIRONMENT_ENV = "OLCF_ENVIRONMENT"
_FALSE = ("0", "false", "no", "off")
_TRUE = ("1", "true", "yes", "on")


def resolve_dev_key_policy(explicit: bool | None = None, environment: str | None = None) -> bool:
    """Whether the development signing key is trusted.

    ``explicit`` (the ``runtime.trust.allow_development_key`` setting) wins, then ``OLCF_RUNTIME_ALLOW_DEV_KEY``, then the environment name
    (``environment``, else ``$OLCF_ENVIRONMENT``; unset = ``production``): the key is trusted only outside production.
    """
    if explicit is not None:
        return bool(explicit)
    raw = os.environ.get(ALLOW_DEV_ENV, "").strip().lower()
    if raw in _FALSE:
        return False
    if raw in _TRUE:
        return True
    env = (environment if environment is not None else os.environ.get(ENVIRONMENT_ENV, "")).strip().lower() or "production"
    return env != "production"


def dev_key_advice(config_path: str | Path | None = None) -> str:
    """What an operator does about bundled runtime manifests that are refused because they are signed with the development key (one text, used everywhere)."""
    where = str(config_path) if config_path else "config.toml in the data directory"
    return (f'Development / single machine: add  environment = "development"  to {where} (or set OLCF_ENVIRONMENT=development) and restart; '
            f"`olcf init` and `olcf quickstart` write it for new installations, an older config.toml has none and is therefore treated as production. "
            f"Production: keep it, create your own key (`python -m olcf.runtime keygen`), re-sign the manifests (`python -m olcf.runtime sign --key <private.pem> <manifest.json>`) "
            f"and put the public key in a directory named by runtime.trust.dir / ${TRUST_DIR_ENV}.")


def is_dev_key_refusal(exc: BaseException | None) -> bool:
    """True when ``exc`` (or its cause) is "signed with the OLCF development key, which is not trusted here"."""
    while exc is not None:
        if isinstance(exc, SignatureInvalid) and (getattr(exc, "details", None) or {}).get("development_key_id"):
            return True
        exc = exc.__cause__
    return False


def key_id_for(public_key: Ed25519PublicKey) -> str:
    raw = public_key.public_bytes(serialization.Encoding.Raw, serialization.PublicFormat.Raw)
    return hashlib.sha256(raw).hexdigest()[:16]


def generate_keypair() -> tuple[bytes, bytes]:
    """Return ``(private_pem, public_pem)`` for a fresh Ed25519 key pair."""
    key = Ed25519PrivateKey.generate()
    priv = key.private_bytes(serialization.Encoding.PEM, serialization.PrivateFormat.PKCS8, serialization.NoEncryption())
    pub = key.public_key().public_bytes(serialization.Encoding.PEM, serialization.PublicFormat.SubjectPublicKeyInfo)
    return priv, pub


def load_private_key(pem: bytes | str | Path) -> Ed25519PrivateKey:
    data = Path(pem).read_bytes() if isinstance(pem, Path) else (pem.encode() if isinstance(pem, str) else pem)
    key = serialization.load_pem_private_key(data, password=None)
    if not isinstance(key, Ed25519PrivateKey):
        raise SignatureInvalid("signing key is not an Ed25519 private key")
    return key


def load_public_key(pem: bytes | str) -> Ed25519PublicKey:
    data = pem.encode() if isinstance(pem, str) else pem
    key = serialization.load_pem_public_key(data)
    if not isinstance(key, Ed25519PublicKey):
        raise SignatureInvalid("trusted key is not an Ed25519 public key")
    return key


@dataclass(frozen=True)
class TrustedKey:
    """A public key trusted to sign runtime manifests: key id, key, name and whether it is the development key."""
    key_id: str
    public_key: Ed25519PublicKey
    name: str
    dev: bool = False


class TrustStore:
    """The set of public keys whose signatures are accepted."""

    def __init__(self, keys: list[TrustedKey] | None = None) -> None:
        self._keys: dict[str, TrustedKey] = {k.key_id: k for k in (keys or [])}

    # ------------------------------------------------------------------ factories
    @classmethod
    def from_dir(cls, directory: str | Path, *, dev_names: tuple[str, ...] = ()) -> TrustStore:
        """Load every ``*.pub`` PEM file in ``directory``."""
        d = Path(directory)
        keys: list[TrustedKey] = []
        if d.is_dir():
            for p in sorted(d.glob("*.pub")):
                pub = load_public_key(p.read_bytes())
                keys.append(TrustedKey(key_id_for(pub), pub, p.stem, dev=p.name in dev_names))
        return cls(keys)

    @classmethod
    def builtin(cls) -> TrustStore:
        """Only the development key shipped with the package (see module docstring for the risk)."""
        return cls.from_dir(KEYS_DIR, dev_names=(DEV_KEY_ID_FILE,))

    @classmethod
    def default(cls, *, allow_dev_key: bool | None = None, trust_dir: str | Path | None = None) -> TrustStore:
        """``$OLCF_RUNTIME_TRUST_DIR`` (or ``trust_dir``) if set (exclusive), else the built-in store, which holds the development key only when
        :func:`resolve_dev_key_policy` (or ``allow_dev_key``) allows it, i.e. **not in production** unless explicitly enabled.
        """
        env = os.environ.get(TRUST_DIR_ENV) or (str(trust_dir) if trust_dir else "")
        if env:
            return cls.from_dir(env)
        store = cls.builtin()
        if not resolve_dev_key_policy(allow_dev_key):
            return cls([k for k in store._keys.values() if not k.dev])
        return store

    @classmethod
    def for_settings(cls, settings: Any) -> TrustStore:
        """The store a process configured by ``settings`` uses (``runtime.trust.*`` and ``environment``)."""
        t = settings.runtime.trust
        return cls.default(allow_dev_key=resolve_dev_key_policy(t.allow_development_key, settings.environment), trust_dir=t.dir)

    def trusts_dev_key(self) -> bool:
        return any(k.dev for k in self._keys.values())

    def dev_key_ids(self) -> list[str]:
        return sorted(k.key_id for k in self._keys.values() if k.dev)

    # ------------------------------------------------------------------ API
    def add(self, key: TrustedKey) -> None:
        self._keys[key.key_id] = key

    def get(self, key_id: str) -> TrustedKey | None:
        return self._keys.get(key_id)

    def key_ids(self) -> list[str]:
        return sorted(self._keys)

    def remove(self, key_id: str) -> bool:
        return self._keys.pop(key_id, None) is not None

    def __len__(self) -> int:
        return len(self._keys)


def dev_key_ids() -> list[str]:
    """Key ids of the development key(s) bundled with the package (never trusted in production)."""
    return TrustStore.builtin().dev_key_ids()


def sign_manifest_dict(manifest: dict[str, Any], private_key: Ed25519PrivateKey, *, key_id: str | None = None) -> dict[str, Any]:
    """Return a copy of ``manifest`` carrying a fresh ``signature`` member."""
    body = {k: v for k, v in manifest.items() if k != "signature"}
    sig = private_key.sign(canonical_json(body))
    kid = key_id or key_id_for(private_key.public_key())
    body["signature"] = {"alg": "ed25519", "key_id": kid, "value": base64.b64encode(sig).decode("ascii"), "signed_at": now_iso()}
    return body


@dataclass(frozen=True)
class VerifiedSignature:
    """A runtime manifest signature that verified: the key id, the signer's name and whether the key is the development key."""
    key_id: str
    signer: str
    dev_key: bool


def verify_manifest(manifest: RuntimeManifest | dict[str, Any], trust: TrustStore | None = None) -> VerifiedSignature:
    """Verify the manifest signature against ``trust`` (default: :meth:`TrustStore.default`).

    Raises :class:`SignatureInvalid` (fail closed) when the manifest is unsigned, signed by an unknown key
    or has been modified after signing.
    """
    m = manifest if isinstance(manifest, RuntimeManifest) else RuntimeManifest.from_dict(manifest, validate=False)
    trust = trust if trust is not None else TrustStore.default()
    sig = m.signature
    if not sig:
        raise SignatureInvalid(f"runtime manifest {m.id} is not signed",
                               remediation="Sign it with `python -m olcf.runtime sign --key <private.pem> <manifest.json>`.")
    key = trust.get(str(sig.get("key_id", "")))
    if key is None and str(sig.get("key_id", "")) in dev_key_ids():
        raise SignatureInvalid(
            f"runtime manifest {m.id} is signed with the OLCF development key ({sig.get('key_id')}), which is not trusted here: its private half is public, so anyone could have forged it",
            details={"trusted_key_ids": trust.key_ids(), "development_key_id": sig.get("key_id")},
            remediation=f"{dev_key_advice()} (For a development machine you can also set runtime.trust.allow_development_key = true.)",
        )
    if key is None:
        raise SignatureInvalid(
            f"runtime manifest {m.id} is signed by untrusted key {sig.get('key_id')!r}",
            details={"trusted_key_ids": trust.key_ids()},
            remediation=f"Add the signer's public key (*.pub) to the trust directory (${TRUST_DIR_ENV}) if you trust it.",
        )
    try:
        key.public_key.verify(base64.b64decode(sig["value"], validate=True), m.canonical_bytes())
    except (InvalidSignature, ValueError) as exc:
        raise SignatureInvalid(f"signature of runtime manifest {m.id} does not match its content (tampered or re-edited after signing)",
                               remediation="Re-obtain the manifest from a trusted source or re-sign it.") from exc
    return VerifiedSignature(key.key_id, key.name, key.dev)
