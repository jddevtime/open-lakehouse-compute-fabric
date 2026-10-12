# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""The execution capsule (spec section 10): server-side models, canonical JSON, and the secret scanner.

A capsule is everything the control plane needs to re-create a Fabric notebook run on customer-controlled Spark: the *normalised source*, the Fabric
*context*, a runtime request, the declared *parameters*, data references, policy and security claims. It is **not** an interpreter dump: only values the author
declared as parameters travel; credentials never do (secrets are references; :func:`find_secrets` is the guard that enforces it).

The JSON Schema in ``schemas/capsule-v1.json`` is generated from these models (:func:`json_schema`); a test keeps the file in sync, and the stdlib-only SDK builds
capsules that validate against the same schema.

Canonical form (what is hashed and signed): ``json.dumps(obj, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False)`` encoded as UTF-8.
"""

from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator
from pydantic.alias_generators import to_camel

from olcf.core.errors import BadRequest
from olcf.version import CAPSULE_SCHEMA_VERSION

SourceFormat = Literal["ipynb", "fabric_git_source", "py"]
DEFAULT_FAMILY = "fabric-2.0-compatible"
SUBMISSION_ID = re.compile(r"^sub_[0-9A-Za-z]{26}$")
MAC_DOMAIN = b"olcf.capsule.v1\x00"


class _M(BaseModel):
    model_config = ConfigDict(alias_generator=to_camel, populate_by_name=True, extra="forbid")


class OriginalDocument(_M):
    """A notebook document exactly as Fabric returned it (kept for audit; the *normalised* source is what runs)."""

    format: SourceFormat
    content: str
    sha256: str = Field(pattern=r"^[0-9a-f]{64}$")


class CapsuleSource(_M):
    """``mode='inline'``: ``content`` is the notebook. ``mode='pull'``: only identifiers travel and the control plane fetches the definition with the broker identity."""

    mode: Literal["inline", "pull"] = "inline"
    notebook_id: str | None = None
    notebook_name: str | None = None
    workspace_id: str | None = None
    workspace_name: str | None = None
    format: SourceFormat = "ipynb"
    content: str | None = None
    sha256: str | None = Field(default=None, pattern=r"^[0-9a-f]{64}$")
    original: OriginalDocument | None = None
    #: indexes (in the original notebook) of cells the SDK removed - the submit cell itself
    excluded_cells: list[int] = Field(default_factory=list)
    #: notebooks referenced by ``%run`` (display name -> document), inlined by the analyzer
    bundled: dict[str, OriginalDocument] = Field(default_factory=dict)


class CapsuleContext(_M):
    """``normalized``: the Fabric keys of ``notebookutils.runtime.context`` (currentNotebookName ...). ``raw``: the dict as the session returned it."""

    normalized: dict[str, Any] = Field(default_factory=dict)
    raw: dict[str, Any] = Field(default_factory=dict)


class RuntimeRequestModel(_M):
    """The runtime section of a capsule: runtime family, version constraints, an optional dependency lock and the Spark settings the author asks for."""
    family: str = DEFAULT_FAMILY
    constraints: dict[str, Any] = Field(default_factory=dict)
    dependency_lock: dict[str, Any] | None = None
    #: Spark settings the author asks for; only keys on the runtime's approved list are applied
    spark_conf: dict[str, str] = Field(default_factory=dict)
    environment_id: str | None = None


class DataReferences(_M):
    """The data references of a capsule: those the SDK resolved itself and logical references the control plane resolves."""
    #: canonical references the SDK could resolve itself (``abfss://`` OneLake URLs, ``olcf://`` URIs)
    resolved: list[dict[str, Any]] = Field(default_factory=list)
    #: logical references (Fabric ids/names, ``Files/...``, ``Tables/...``, catalog tables) resolved by the control plane
    logical: list[dict[str, Any]] = Field(default_factory=list)


class CapsulePolicy(_M):
    """The scheduling policy a capsule asks for: queue, priority, resources, timeout and retries."""
    queue: str | None = None
    priority: int | None = Field(default=None, ge=0, le=100)
    resources: dict[str, Any] | None = None
    timeout_seconds: int | None = Field(default=None, ge=1)
    retries: int = Field(default=0, ge=0, le=10)
    retention_days: int | None = Field(default=None, ge=0)
    placement: dict[str, Any] = Field(default_factory=dict)
    #: a *request*; the administrator's integration policy decides (it can only narrow)
    allow_pip_install: bool = False


class SecretRef(_M):
    """A Key Vault secret the notebook reads with ``notebookutils.credentials.getSecret(vault, name)``. A reference, never a value."""

    vault: str = Field(min_length=1, max_length=300)
    name: str = Field(min_length=1, max_length=200)


class CapsuleSecurity(_M):
    """Identity *claims* (what the caller says it is; the authenticated token is the authority) and replay-protection material."""

    nonce: str = Field(min_length=16, max_length=128)
    key_id: str = Field(min_length=1, max_length=64)
    identity: dict[str, Any] = Field(default_factory=dict)
    correlation_id: str | None = Field(default=None, max_length=200)
    secret_refs: list[SecretRef] = Field(default_factory=list)


class Capsule(_M):
    """The signed execution capsule that the Fabric SDK sends to the control plane: versions, submission id and idempotency key, source, runtime, data references, policy and the security block (nonce, key id and identity claims)."""
    schema_version: Literal["1.0"] = Field(default=CAPSULE_SCHEMA_VERSION)  # type: ignore[assignment]
    sdk_version: str = Field(min_length=1, max_length=40)
    adapter_version: str = Field(min_length=1, max_length=80)
    submission_id: str = Field(pattern=SUBMISSION_ID.pattern)
    idempotency_key: str = Field(min_length=1, max_length=200)
    created_at: str
    source: CapsuleSource
    context: CapsuleContext = Field(default_factory=CapsuleContext)
    runtime: RuntimeRequestModel = Field(default_factory=RuntimeRequestModel)
    parameters: dict[str, Any] = Field(default_factory=dict)
    data_references: DataReferences = Field(default_factory=DataReferences)
    policy: CapsulePolicy = Field(default_factory=CapsulePolicy)
    security: CapsuleSecurity
    #: advisory report from the SDK's pre-flight ``/validate`` call; the control plane always re-analyses
    compatibility: dict[str, Any] | None = None

    @field_validator("created_at")
    @classmethod
    def _iso(cls, v: str) -> str:
        parse_ts(v)
        return v


class Signature(_M):
    """Detached signature over the canonical capsule: ``mac = HMAC-SHA256(key, MAC_DOMAIN + canonical_bytes)`` (hex)."""

    alg: Literal["HMAC-SHA256"] = "HMAC-SHA256"
    key_id: str
    digest: str = Field(pattern=r"^sha256:[0-9a-f]{64}$")
    mac: str = Field(pattern=r"^[0-9a-f]{64}$")


class SignedCapsule(_M):
    """The request body of ``POST /v1/integrations/fabric/submit``."""

    capsule: Capsule
    signature: Signature


# ===================================================================================== canonical JSON
def canonical_json(obj: Any) -> bytes:
    """The bytes that are hashed and signed. ``allow_nan=False`` rejects NaN/Infinity (not valid JSON, not portable)."""
    try:
        return json.dumps(obj, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False).encode("utf-8")
    except (TypeError, ValueError) as exc:
        raise BadRequest(f"the capsule is not canonical-JSON serialisable: {exc}") from exc


def capsule_digest(obj: Any) -> str:
    return "sha256:" + hashlib.sha256(canonical_json(obj)).hexdigest()


def parse_ts(value: str) -> datetime:
    try:
        dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except (ValueError, AttributeError) as exc:
        raise ValueError(f"'{value}' is not an ISO 8601 timestamp") from exc
    if dt.tzinfo is None:
        raise ValueError(f"'{value}' has no UTC offset (use e.g. 2026-01-01T00:00:00Z)")
    return dt


def json_schema() -> dict[str, Any]:
    """JSON Schema (draft 2020-12) of the signed submit body; written to ``schemas/capsule-v1.json``."""
    doc = SignedCapsule.model_json_schema(by_alias=True)
    doc["$schema"] = "https://json-schema.org/draft/2020-12/schema"
    doc["$id"] = "https://olcf.invalid/schemas/capsule-v1.json"
    doc["title"] = "OLCF execution capsule v1 (signed submit body)"
    doc["description"] = (f"Capsule schema version {CAPSULE_SCHEMA_VERSION}. Generated from olcf.fabric.capsule; the stdlib-only SDK builds documents that validate against it. "
                          "Compatible with Fabric Runtime 2.0; not a Microsoft schema.")
    return doc


# ===================================================================================== secret scanner
@dataclass(frozen=True)
class SecretFinding:
    """Where credential-like content was found. ``excerpt`` is never the value (kind + length only)."""

    path: str
    kind: str
    severity: str  # high | medium

    def to_dict(self) -> dict[str, str]:
        return {"path": self.path, "kind": self.kind, "severity": self.severity}


_PATTERNS: list[tuple[str, str, re.Pattern[str]]] = [
    ("jwt", "high", re.compile(r"\beyJ[A-Za-z0-9_-]{8,}\.eyJ[A-Za-z0-9_-]{8,}\.[A-Za-z0-9_-]{8,}")),
    ("private-key", "high", re.compile(r"-----BEGIN (?:RSA |EC |DSA |OPENSSH |ENCRYPTED )?PRIVATE KEY-----")),
    ("entra-client-secret", "high", re.compile(r"(?<![A-Za-z0-9])[A-Za-z0-9_.~-]{3}\dQ~[A-Za-z0-9_.~-]{30,}")),
    ("olcf-token", "high", re.compile(r"\bolcf_(?:pat|wrk|enr|job|svc|usr)_[A-Za-z0-9_-]{8,}")),
    ("azure-storage-key", "high", re.compile(r"(?i)\b(?:AccountKey|SharedAccessKey)\s*=\s*[A-Za-z0-9+/]{40,}={0,2}")),
    ("sas-token", "high", re.compile(r"(?i)[?&]sig=[A-Za-z0-9%+/]{30,}")),
    ("bearer-token", "high", re.compile(r"(?i)\bauthorization\s*[:=]\s*['\"]?bearer\s+[A-Za-z0-9._~+/-]{20,}")),
    ("aws-secret", "high", re.compile(r"(?i)\baws_secret_access_key\s*[:=]\s*['\"]?[A-Za-z0-9/+]{40}")),
    ("client-secret-assignment", "medium", re.compile(r"(?i)\b(?:client_secret|clientsecret|client-secret)\b\s*[:=]\s*['\"][^'\"\s]{8,}['\"]")),
    ("password-assignment", "medium", re.compile(r"(?i)\b(?:password|passwd|pwd)\b\s*[:=]\s*['\"][^'\"\s]{6,}['\"]")),
]


def _walk(obj: Any, path: str) -> Iterable[tuple[str, str]]:
    if isinstance(obj, str):
        yield path, obj
    elif isinstance(obj, dict):
        for k, v in obj.items():
            yield from _walk(v, f"{path}.{k}" if path else str(k))
    elif isinstance(obj, (list, tuple)):
        for i, v in enumerate(obj):
            yield from _walk(v, f"{path}[{i}]")


def find_secrets(obj: Any, *, known: Iterable[str] = (), min_known_length: int = 6) -> list[SecretFinding]:
    """Scan a JSON-like value (or a plain string) for credential-like content.

    ``known`` are exact secret values to look for (for example the broker service principal's client secret read from the secret store); they are matched as
    substrings, so a secret embedded in a larger string is still found. Findings carry the JSON path and the kind, never the matched text.
    """
    known_values = [k for k in known if isinstance(k, str) and len(k) >= min_known_length]
    findings: list[SecretFinding] = []
    root = obj if not isinstance(obj, str) else obj
    for path, text in _walk(root, ""):
        for kind, sev, rx in _PATTERNS:
            if rx.search(text):
                findings.append(SecretFinding(path or "$", kind, sev))
        for kv in known_values:
            if kv in text:
                findings.append(SecretFinding(path or "$", "known-secret", "high"))
    seen: set[tuple[str, str]] = set()
    out = []
    for f in findings:
        if (f.path, f.kind) not in seen:
            seen.add((f.path, f.kind))
            out.append(f)
    return out


def scan_text(text: str, *, known: Iterable[str] = ()) -> list[SecretFinding]:
    """Convenience for logs and support bundles (plain text)."""
    return find_secrets(text, known=known)


# ===================================================================================== context normalisation
#: Fabric ``notebookutils.runtime.context`` keys the platform understands; other keys are kept in ``raw`` only.
CONTEXT_KEYS = (
    "currentNotebookName", "currentNotebookId", "currentWorkspaceName", "currentWorkspaceId", "defaultLakehouseName", "defaultLakehouseId",
    "defaultLakehouseWorkspaceName", "defaultLakehouseWorkspaceId", "currentRunId", "parentRunId", "rootRunId", "isForPipeline", "isReferenceRun",
    "referenceTreePath", "userName", "userId", "productType", "activityId",
)


def normalise_context(raw: dict[str, Any]) -> dict[str, Any]:
    """Keep only the known Fabric keys (scalar values); everything else stays in ``raw``."""
    out: dict[str, Any] = {}
    for k in CONTEXT_KEYS:
        v = raw.get(k)
        if isinstance(v, (str, int, bool)) or v is None:
            out[k] = v
    return out
