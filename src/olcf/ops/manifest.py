# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Signed update manifests (Ed25519) and the trust store they are verified against.

Envelope (``manifest.json`` on a feed or inside an offline bundle)::

    {"payload": "<base64 of the canonical manifest JSON>",
     "signatures": [{"keyid": "<16 hex>", "alg": "ed25519", "sig": "<base64>"}, ...]}

The signature covers ``DOMAIN || payload bytes`` (domain separation, so a signature made for another purpose can never validate a manifest).
``keyid`` is the first 16 hex characters of SHA-256 over the raw public key and is only a *hint*: the key is looked up in the trust store,
never taken from the envelope. A manifest is accepted only if **at least ``threshold`` distinct trusted, non-revoked keys** produced a valid
signature, it has not expired, belongs to the expected channel, and (checked by the caller against persisted state) is not older than what
this installation already accepted (anti-rollback ``sequence``).

There is no code path that skips verification: every consumer (``UpdateService``, the offline-bundle import, the ``upgrade`` hand-off CLI)
calls :func:`verify_envelope`. Release engineering uses ``python -m olcf.ops.manifest keygen|sign|verify``.

Manifest payload::

    {"schema": 1, "product": "olcf", "channel": "stable", "version": "0.2.0", "sequence": 12, "released_at": "...", "expires_at": "...",
     "min_version": "0.1.0", "notes": "...",
     "artifacts": [{"kind": "wheel", "name": "olcf-0.2.0-py3-none-any.whl", "sha256": "...", "size": 123, "url": "https://..."}, ...],
     "compat": {"python": ">=3.11", "databases": ["sqlite", "postgresql"], "workers": {"min_version": "0.1.0", "required": false}},
     "migrations": {"core": 1, "ops": 2}}
"""

from __future__ import annotations

import argparse
import base64
import hashlib
import json
import re
import sys
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, Literal

from cryptography.exceptions import InvalidSignature
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey, Ed25519PublicKey
from packaging.version import InvalidVersion, Version
from pydantic import BaseModel, ConfigDict, Field, ValidationError, field_validator

from olcf.core.errors import BadRequest

DOMAIN = b"OLCF-UPDATE-MANIFEST-V1\n"
MAX_ENVELOPE_BYTES = 1024 * 1024
CHANNELS = ("stable", "beta", "lts", "offline")
ARTIFACT_NAME = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._+-]{0,199}$")


class ManifestError(BadRequest):
    """The manifest is malformed, unsigned, signed by an untrusted key, expired, for another channel or otherwise unacceptable."""

    code = "manifest_rejected"


def keyid_of(public_raw: bytes) -> str:
    return hashlib.sha256(public_raw).hexdigest()[:16]


class Artifact(BaseModel):
    """One file listed in an update manifest: kind, name, SHA-256 and size."""
    model_config = ConfigDict(extra="forbid")
    kind: Literal["wheel", "runtime-bundle", "agent", "sdist", "other"]
    name: str
    sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    size: int = Field(ge=0, le=64 * 1024**3)
    url: str | None = Field(None, max_length=2048)
    runtime_id: str | None = Field(None, max_length=128)

    @field_validator("name")
    @classmethod
    def _name(cls, v: str) -> str:
        if not ARTIFACT_NAME.match(v) or ".." in v:
            raise ValueError("artifact name must be a plain file name")
        return v


class Manifest(BaseModel):
    """The update manifest: schema, product, channel, version and the artifacts of a release."""
    model_config = ConfigDict(extra="forbid")
    schema_: int = Field(1, alias="schema")
    product: Literal["olcf"] = "olcf"
    channel: Literal["stable", "beta", "lts", "offline"]
    version: str
    sequence: int = Field(ge=0)
    released_at: str
    expires_at: str | None = None
    min_version: str | None = None
    notes: str = Field("", max_length=20000)
    artifacts: list[Artifact] = Field(default_factory=list, max_length=64)
    compat: dict[str, Any] = Field(default_factory=dict)
    migrations: dict[str, int] = Field(default_factory=dict)

    @field_validator("version", "min_version")
    @classmethod
    def _version(cls, v: str | None) -> str | None:
        if v is not None:
            Version(v)
        return v

    @property
    def parsed_version(self) -> Version:
        return Version(self.version)

    def artifact(self, kind: str) -> Artifact | None:
        return next((a for a in self.artifacts if a.kind == kind), None)


@dataclass
class TrustedKey:
    """A public key trusted to sign update manifests, with its key id, name and revoked flag."""
    keyid: str
    public_raw: bytes
    name: str = ""
    revoked: bool = False


@dataclass
class Verified:
    """A manifest whose signature verified: the parsed manifest, the signed payload, the key ids that signed and the envelope."""
    manifest: Manifest
    payload: dict[str, Any]
    keyids: list[str] = field(default_factory=list)
    envelope: dict[str, Any] = field(default_factory=dict)


# ----------------------------------------------------------------------------- signing (release tooling and tests)
def generate_keypair() -> tuple[Ed25519PrivateKey, bytes, str]:
    priv = Ed25519PrivateKey.generate()
    raw = priv.public_key().public_bytes(serialization.Encoding.Raw, serialization.PublicFormat.Raw)
    return priv, raw, keyid_of(raw)


def private_to_b64(priv: Ed25519PrivateKey) -> str:
    return base64.b64encode(priv.private_bytes(serialization.Encoding.Raw, serialization.PrivateFormat.Raw, serialization.NoEncryption())).decode()


def private_from_b64(s: str) -> Ed25519PrivateKey:
    return Ed25519PrivateKey.from_private_bytes(base64.b64decode(s))


def canonical(payload: dict[str, Any]) -> bytes:
    return json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()


def sign_manifest(payload: dict[str, Any], *private_keys: Ed25519PrivateKey) -> dict[str, Any]:
    """Create a signed envelope. ``payload`` is validated against :class:`Manifest` first, so a release can never publish a malformed manifest."""
    Manifest.model_validate(payload)
    raw = canonical(payload)
    sigs = []
    for k in private_keys:
        pub = k.public_key().public_bytes(serialization.Encoding.Raw, serialization.PublicFormat.Raw)
        sigs.append({"keyid": keyid_of(pub), "alg": "ed25519", "sig": base64.b64encode(k.sign(DOMAIN + raw)).decode()})
    return {"payload": base64.b64encode(raw).decode(), "signatures": sigs}


# ----------------------------------------------------------------------------- verification
def parse_envelope(data: bytes | str | dict[str, Any]) -> dict[str, Any]:
    if isinstance(data, dict):
        env = data
    else:
        raw = data.encode() if isinstance(data, str) else data
        if len(raw) > MAX_ENVELOPE_BYTES:
            raise ManifestError("manifest envelope is too large")
        try:
            env = json.loads(raw)
        except ValueError as exc:
            raise ManifestError("manifest is not valid JSON") from exc
    if not isinstance(env, dict) or not isinstance(env.get("payload"), str) or not isinstance(env.get("signatures"), list):
        raise ManifestError("manifest envelope must contain 'payload' and 'signatures'")
    return env


def verify_envelope(data: bytes | str | dict[str, Any], trusted: list[TrustedKey], *, threshold: int = 1, expect_channel: str | None = None,
                    now: datetime | None = None, skew_seconds: int = 300) -> Verified:
    """Verify signature, trust, schema, channel and expiry. Raises :class:`ManifestError` with a precise reason; never returns unverified data."""
    env = parse_envelope(data)
    if not env["signatures"]:
        raise ManifestError("the manifest is unsigned")
    try:
        raw = base64.b64decode(env["payload"], validate=True)
    except ValueError as exc:
        raise ManifestError("manifest payload is not valid base64") from exc
    if len(raw) > MAX_ENVELOPE_BYTES:
        raise ManifestError("manifest payload is too large")
    by_id = {k.keyid: k for k in trusted}
    good: list[str] = []
    saw_untrusted = saw_revoked = saw_bad = False
    for s in env["signatures"][:16]:
        if not isinstance(s, dict) or s.get("alg") != "ed25519":
            saw_bad = True
            continue
        key = by_id.get(str(s.get("keyid")))
        if key is None:
            saw_untrusted = True
            continue
        if key.revoked:
            saw_revoked = True
            continue
        try:
            Ed25519PublicKey.from_public_bytes(key.public_raw).verify(base64.b64decode(str(s.get("sig", "")), validate=True), DOMAIN + raw)
        except (InvalidSignature, ValueError):
            saw_bad = True
            continue
        if key.keyid not in good:
            good.append(key.keyid)
    if len(good) < max(1, threshold):
        why = ("signed only with revoked key(s)" if saw_revoked and not good else "signature verification failed" if saw_bad else
               "signed by a key that is not in the trust store" if saw_untrusted else "no valid signature")
        raise ManifestError(f"the manifest cannot be trusted: {why} ({len(good)} of {max(1, threshold)} required valid signature(s))",
                            remediation="add the publisher's public key to the update trust store (PlatformAdmin) or obtain a correctly signed manifest")
    try:
        payload = json.loads(raw)
        manifest = Manifest.model_validate(payload)
    except (ValueError, ValidationError, InvalidVersion) as exc:
        raise ManifestError(f"the manifest payload is invalid: {str(exc)[:300]}") from exc
    if expect_channel and manifest.channel != expect_channel:
        raise ManifestError(f"the manifest is for channel '{manifest.channel}', expected '{expect_channel}'")
    now = now or datetime.now(UTC)
    if manifest.expires_at:
        try:
            exp = datetime.fromisoformat(manifest.expires_at.replace("Z", "+00:00"))
        except ValueError as exc:
            raise ManifestError("expires_at is not a valid timestamp") from exc
        if exp.tzinfo is None:
            exp = exp.replace(tzinfo=UTC)
        if exp + timedelta(seconds=skew_seconds) < now:
            raise ManifestError(f"the manifest expired at {manifest.expires_at}", remediation="fetch a current manifest; an expired manifest may be a replay")
    return Verified(manifest=manifest, payload=payload, keyids=good, envelope=env)


def check_not_rollback(manifest: Manifest, *, installed: str, floor: dict[str, Any] | None) -> None:
    """Anti-rollback: a validly signed but *older* manifest must not be accepted (replay of a superseded release).

    ``floor`` is the highest ``{sequence, version}`` this installation ever accepted for the channel.
    """
    if floor:
        if manifest.sequence < int(floor.get("sequence", 0)):
            raise ManifestError(f"rollback refused: manifest sequence {manifest.sequence} is older than the accepted sequence {floor['sequence']}",
                                remediation="this looks like a replay of a superseded release")
        if floor.get("version") and manifest.parsed_version < Version(str(floor["version"])):
            raise ManifestError(f"rollback refused: version {manifest.version} is older than the previously accepted {floor['version']}")


# ----------------------------------------------------------------------------- CLI for release engineering
def _cli(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="python -m olcf.ops.manifest", description="Create keys, sign and verify OLCF update manifests")
    sub = ap.add_subparsers(dest="cmd", required=True)
    sub.add_parser("keygen", help="print a new Ed25519 key pair (keep the private key offline!)")
    s = sub.add_parser("sign", help="sign a manifest payload JSON file")
    s.add_argument("payload")
    s.add_argument("--key-env", default="OLCF_UPDATE_SIGNING_KEY", help="environment variable with the base64 private key (never a command-line argument)")
    s.add_argument("-o", "--output", required=True)
    v = sub.add_parser("verify", help="verify an envelope against public keys")
    v.add_argument("envelope")
    v.add_argument("--public-key", action="append", required=True, help="base64 raw Ed25519 public key (repeatable)")
    v.add_argument("--channel")
    ns = ap.parse_args(argv)
    import os

    try:
        if ns.cmd == "keygen":
            priv, raw, kid = generate_keypair()
            print(json.dumps({"keyid": kid, "public_key": base64.b64encode(raw).decode(), "private_key": private_to_b64(priv)}, indent=2))
            return 0
        if ns.cmd == "sign":
            key = os.environ.get(ns.key_env)
            if not key:
                print(f"error: set {ns.key_env}", file=sys.stderr)
                return 2
            env = sign_manifest(json.loads(Path(ns.payload).read_text()), private_from_b64(key))
            Path(ns.output).write_text(json.dumps(env, indent=2))
            return 0
        keys = [TrustedKey(keyid_of(base64.b64decode(k)), base64.b64decode(k)) for k in ns.public_key]
        ver = verify_envelope(Path(ns.envelope).read_bytes(), keys, expect_channel=ns.channel)
        print(json.dumps({"ok": True, "version": ver.manifest.version, "channel": ver.manifest.channel, "signed_by": ver.keyids}))
        return 0
    except ManifestError as exc:
        print(f"REJECTED: {exc.message}", file=sys.stderr)
        return 2


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(_cli())
