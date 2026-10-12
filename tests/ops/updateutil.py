# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Helpers to create signed releases (wheel + manifest) and offline bundles for the update tests."""

from __future__ import annotations

import base64
import hashlib
import io
import json
import tarfile
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from olcf.ops import manifest as M
from tests.ops.wheelutil import build_olcf_wheel


@dataclass
class Release:
    version: str
    wheel: Path
    payload: dict[str, Any]
    envelope: dict[str, Any]
    extra: dict[str, bytes] = field(default_factory=dict)

    @property
    def manifest_bytes(self) -> bytes:
        return json.dumps(self.envelope).encode()


def sha(p: Path) -> str:
    return hashlib.sha256(p.read_bytes()).hexdigest()


def make_release(tmp: Path, signer, version: str, *, sequence: int = 1, channel: str = "stable", migration: str | None = None, name_version: str | None = None,
                 expires_at: str | None = None, migrations: dict[str, int] | None = None, compat: dict[str, Any] | None = None, extra_artifacts: list[tuple[str, str, bytes, str | None]] | None = None,
                 min_version: str | None = None, sign_with: list | None = None, with_url: bool = True, code_version: str | None = None) -> Release:
    d = tmp / f"rel-{version}-{sequence}"
    wheel = build_olcf_wheel(d, version, migration=migration, name_version=name_version, code_version=code_version)
    arts = [{"kind": "wheel", "name": wheel.name, "sha256": sha(wheel), "size": wheel.stat().st_size, **({"url": wheel.name} if with_url else {})}]
    extras: dict[str, bytes] = {}
    for kind, name, data, runtime_id in extra_artifacts or []:
        (d / name).write_bytes(data)
        extras[name] = data
        arts.append({"kind": kind, "name": name, "sha256": hashlib.sha256(data).hexdigest(), "size": len(data), **({"url": name} if with_url else {}), **({"runtime_id": runtime_id} if runtime_id else {})})
    payload: dict[str, Any] = {"schema": 1, "product": "olcf", "channel": channel, "version": version, "sequence": sequence, "released_at": "2026-02-01T00:00:00Z",
                               "artifacts": arts, "compat": compat or {}, "migrations": migrations or {}, "notes": f"release {version}"}
    if expires_at:
        payload["expires_at"] = expires_at
    if min_version:
        payload["min_version"] = min_version
    env = M.sign_manifest(payload, *(sign_with or [signer.priv]))
    return Release(version, wheel, payload, env, extras)


def feed_files(rel: Release, channel: str = "stable") -> dict[str, bytes]:
    files = {f"/{channel}/manifest.json": rel.manifest_bytes, f"/{channel}/{rel.wheel.name}": rel.wheel.read_bytes()}
    for n, b in rel.extra.items():
        files[f"/{channel}/{n}"] = b
    return files


def make_bundle(tmp: Path, rel: Release, *, name: str = "bundle.tar.gz", extra_members: list[tuple[tarfile.TarInfo, bytes | None]] | None = None, tamper_wheel: bool = False,
                drop_wheel: bool = False, manifest_bytes: bytes | None = None) -> Path:
    path = tmp / name
    with tarfile.open(path, "w:gz") as tf:
        def add(n: str, data: bytes) -> None:
            ti = tarfile.TarInfo(n)
            ti.size = len(data)
            tf.addfile(ti, io.BytesIO(data))

        add("manifest.json", manifest_bytes if manifest_bytes is not None else rel.manifest_bytes)
        if not drop_wheel:
            data = rel.wheel.read_bytes()
            if tamper_wheel:
                data = data[:-10] + b"X" * 10
            add(rel.wheel.name, data)
        for n, b in rel.extra.items():
            add(n, b)
        for ti, data in extra_members or []:
            tf.addfile(ti, io.BytesIO(data) if data is not None else None)
    return path


def b64pub(raw: bytes) -> str:
    return base64.b64encode(raw).decode()
