# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Runtime manifest model (CONTRACTS section 4.5, spec section 12).

A *runtime manifest* is a signed JSON document that fully describes one immutable Spark runtime:
the Spark / Delta / Iceberg / Java / Scala / Python versions, the hash-pinned Python packages, the Maven
coordinates of the extra JARs (their hashes are recorded in ``lock.json`` at first resolution), default
and approved Spark configuration and the OS / CPU constraints.

The model is intentionally a thin typed view over the JSON document: the JSON is the source of truth
(the signature covers its canonical form), unknown keys are preserved, and ``to_dict()`` returns exactly
what was loaded. ``fingerprint`` binds manifest and lock (``sha256(manifest + lock)``) so every job can
record the exact environment it ran on.
"""

from __future__ import annotations

import copy
import hashlib
import json
import platform
import re
import sys
from dataclasses import dataclass, field
from functools import lru_cache
from pathlib import Path
from typing import Any

from olcf.runtime.errors import ManifestInvalid, PlatformUnsupported

MANIFEST_SCHEMA_ID = "olcf.runtime.manifest/1"
LOCK_SCHEMA_ID = "olcf.runtime.lock/1"

_SCHEMA_PATH = Path(__file__).with_name("schema") / "manifest.schema.json"
_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")


def canonical_json(obj: Any) -> bytes:
    """Deterministic JSON encoding used for signing and fingerprints (sorted keys, no whitespace)."""
    return json.dumps(obj, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode("utf-8")


def sha256_hex(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


@lru_cache(maxsize=1)
def manifest_schema() -> dict[str, Any]:
    """The JSON schema (draft 2020-12) every manifest must satisfy."""
    return json.loads(_SCHEMA_PATH.read_text(encoding="utf-8"))


def normalise_os(name: str | None = None) -> str:
    """Map ``sys.platform`` style names to ``linux`` / ``macos`` / ``windows``."""
    raw = (name or sys.platform).lower()
    if raw.startswith("linux"):
        return "linux"
    if raw.startswith("darwin") or raw == "macos":
        return "macos"
    if raw.startswith(("win", "cygwin", "msys")):
        return "windows"
    return raw


def normalise_arch(name: str | None = None) -> str:
    """Map ``platform.machine()`` values to ``x86_64`` / ``aarch64``."""
    raw = (name or platform.machine()).lower()
    if raw in ("x86_64", "amd64", "x64"):
        return "x86_64"
    if raw in ("aarch64", "arm64", "armv8l", "armv8b"):
        return "aarch64"
    return raw


@dataclass(frozen=True)
class PackagePin:
    """One pip requirement pinned to an exact version with sha256 hashes for every acceptable artifact."""

    name: str
    version: str
    hashes: tuple[str, ...]
    marker: str | None = None

    def requirement_lines(self) -> str:
        """Render as a ``requirements.txt`` entry usable with ``pip install --require-hashes``."""
        head = f"{self.name}=={self.version}"
        if self.marker:
            head += f" ; {self.marker}"
        parts = [head] + [f"    --hash=sha256:{h}" for h in self.hashes]
        return " \\\n".join(parts)


@dataclass(frozen=True)
class JarSpec:
    """A Maven coordinate to resolve (with transitive compile/runtime deps) into the runtime's ``jars/``."""

    coordinate: str
    #: Optional groups can be switched on/off at install time (``iceberg``, ``cloud``...).
    group: str = "core"
    optional: bool = False
    exclusions: tuple[str, ...] = ()

    def resolved_coordinate(self, variables: dict[str, str]) -> str:
        out = self.coordinate
        for key, value in variables.items():
            out = out.replace("${" + key + "}", value)
        if "${" in out:
            raise ManifestInvalid(f"unresolved placeholder in jar coordinate {self.coordinate!r}",
                                  remediation="Supported placeholders: ${hadoop.version}, ${spark.version}, ${scala.binary}, ${delta.version}.")
        return out


@dataclass
class RuntimeManifest:
    """Typed view of a manifest document."""

    data: dict[str, Any]
    packages: list[PackagePin] = field(default_factory=list)
    build_packages: list[PackagePin] = field(default_factory=list)
    jars: list[JarSpec] = field(default_factory=list)

    # ------------------------------------------------------------------ construction
    @classmethod
    def from_dict(cls, data: dict[str, Any], *, validate: bool = True) -> RuntimeManifest:
        if validate:
            validate_manifest_dict(data)
        d = copy.deepcopy(data)
        return cls(
            data=d,
            packages=[_pin(p) for p in d.get("packages", [])],
            build_packages=[_pin(p) for p in d.get("build_packages", [])],
            jars=[_jar(j) for j in d.get("jars", [])],
        )

    @classmethod
    def load(cls, path: str | Path, *, validate: bool = True) -> RuntimeManifest:
        try:
            raw = json.loads(Path(path).read_text(encoding="utf-8"))
        except (OSError, ValueError) as exc:
            raise ManifestInvalid(f"cannot read manifest {path}: {exc}") from exc
        return cls.from_dict(raw, validate=validate)

    def to_dict(self) -> dict[str, Any]:
        return copy.deepcopy(self.data)

    # ------------------------------------------------------------------ simple accessors
    @property
    def id(self) -> str:
        return str(self.data["id"])

    @property
    def version(self) -> str:
        return str(self.data["version"])

    @property
    def family(self) -> str:
        return str(self.data["family"])

    @property
    def title(self) -> str:
        return str(self.data.get("title") or self.id)

    @property
    def spark_version(self) -> str:
        return str(self.data["spark"]["version"])

    @property
    def scala_binary(self) -> str:
        return str(self.data["scala"])

    @property
    def python_series(self) -> str:
        """``"3.13"`` style major.minor required for the venv."""
        return str(self.data["python"]["version"])

    @property
    def java_major(self) -> int:
        return int(self.data["java"]["major"])

    @property
    def java_supported(self) -> list[int]:
        return [int(v) for v in self.data["java"].get("supported", [self.java_major])]

    @property
    def delta_version(self) -> str | None:
        d = self.data.get("delta")
        return str(d["version"]) if d else None

    @property
    def iceberg_version(self) -> str | None:
        d = self.data.get("iceberg")
        return str(d["version"]) if d else None

    @property
    def spark_conf_defaults(self) -> dict[str, str]:
        return {str(k): str(v) for k, v in (self.data.get("spark_conf_defaults") or {}).items()}

    @property
    def approved_spark_conf(self) -> list[str]:
        return [str(v) for v in (self.data.get("approved_spark_conf") or [])]

    @property
    def provided_jars(self) -> list[str]:
        return [str(v) for v in (self.data.get("provided_jars") or [])]

    @property
    def verify_mode(self) -> str:
        """``spark`` (full local Spark smoke test), ``import`` (python imports only) or ``none``."""
        return str((self.data.get("verify") or {}).get("smoke", "spark"))

    @property
    def signature(self) -> dict[str, Any] | None:
        sig = self.data.get("signature")
        return dict(sig) if isinstance(sig, dict) else None

    def unsigned_dict(self) -> dict[str, Any]:
        """The manifest without its ``signature`` member: this is what gets signed."""
        d = self.to_dict()
        d.pop("signature", None)
        return d

    def canonical_bytes(self) -> bytes:
        return canonical_json(self.unsigned_dict())

    @property
    def manifest_sha256(self) -> str:
        return sha256_hex(self.canonical_bytes())

    # ------------------------------------------------------------------ platform checks
    def supports_platform(self, os_name: str | None = None, arch: str | None = None) -> bool:
        os_n, arch_n = normalise_os(os_name), normalise_arch(arch)
        oses = [normalise_os(o) for o in self.data.get("os", ["linux", "macos", "windows"])]
        arches = [normalise_arch(a) for a in self.data.get("arch", ["x86_64", "aarch64"])]
        return os_n in oses and arch_n in arches

    def require_platform(self, os_name: str | None = None, arch: str | None = None) -> None:
        if not self.supports_platform(os_name, arch):
            raise PlatformUnsupported(
                f"runtime {self.id} does not support {normalise_os(os_name)}/{normalise_arch(arch)}",
                details={"os": self.data.get("os"), "arch": self.data.get("arch")},
                remediation="Choose a runtime whose manifest lists this OS/architecture, or build a new manifest with scripts/lock_runtime.py.",
            )

    def jar_variables(self, hadoop_version: str | None) -> dict[str, str]:
        """Values for ``${...}`` placeholders inside jar coordinates."""
        out = {"spark.version": self.spark_version, "scala.binary": self.scala_binary}
        if self.delta_version:
            out["delta.version"] = self.delta_version
        if hadoop_version:
            out["hadoop.version"] = hadoop_version
        return out

    def requirements_text(self, *, build: bool = False) -> str:
        """The hash-pinned requirements file passed to ``pip install --require-hashes -r``."""
        pins = self.build_packages if build else self.packages
        return "\n".join(p.requirement_lines() for p in pins) + "\n"


def _pin(p: dict[str, Any]) -> PackagePin:
    return PackagePin(name=p["name"], version=p["version"], hashes=tuple(p["hashes"]), marker=p.get("marker"))


def _jar(j: dict[str, Any] | str) -> JarSpec:
    if isinstance(j, str):
        return JarSpec(coordinate=j)
    return JarSpec(
        coordinate=j["coordinate"],
        group=j.get("group", "core"),
        optional=bool(j.get("optional", False)),
        exclusions=tuple(j.get("exclusions", [])),
    )


def validate_manifest_dict(data: dict[str, Any]) -> None:
    """Validate against the JSON schema plus semantic rules the schema cannot express."""
    import jsonschema

    validator = jsonschema.Draft202012Validator(manifest_schema())
    errors = sorted(validator.iter_errors(data), key=lambda e: list(e.absolute_path))
    if errors:
        first = errors[0]
        loc = "/".join(str(p) for p in first.absolute_path) or "<root>"
        raise ManifestInvalid(
            f"manifest invalid at {loc}: {first.message}",
            details={"errors": [{"path": "/".join(str(p) for p in e.absolute_path), "message": e.message} for e in errors[:20]]},
            remediation="Fix the manifest (see olcf/runtime/schema/manifest.schema.json) and re-sign it.",
        )
    seen: set[str] = set()
    for sect in ("build_packages", "packages"):
        for p in data.get(sect, []):
            key = (sect, p["name"].lower().replace("_", "-"), p.get("marker"))
            if key in seen:
                raise ManifestInvalid(f"duplicate pin for {p['name']} ({p.get('marker') or 'no marker'}) in {sect}")
            seen.add(key)  # type: ignore[arg-type]
            for h in p["hashes"]:
                if not _SHA256_RE.match(h):
                    raise ManifestInvalid(f"package {p['name']}: hash {h!r} is not a lowercase sha256 hex digest")
