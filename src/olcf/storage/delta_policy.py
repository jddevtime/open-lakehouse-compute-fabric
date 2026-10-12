# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Delta Lake interoperability policy (spec section 16, CONTRACTS section 3).

Why: a Delta table is shared between engines (OLCF Spark, delta-rs, Microsoft Fabric).  An engine that silently
*upgrades* a table's protocol (``minReaderVersion``/``minWriterVersion`` or reader/writer *table features*) can make
the table unreadable or unwritable for the others.  This module is the single, versioned, pure-Python authority that
answers three questions before OLCF touches a table:

* ``read``    - may engine X read a table with this protocol?
* ``write``   - may engine X write it (without changing the protocol)?
* ``upgrade`` - would moving from protocol A to B (or enabling table properties that imply features) break interop?

The decision is evaluated against a *profile*.  The default profile ``fabric-2.0`` allows the features supported by
the Delta Lake release shipped with the "Fabric Runtime 2.0 Compatible" family (Delta 4.x) and blocks anything beyond
it; ``fabric-1.3`` models the legacy family (Delta 3.2).  The feature tables below are *our* statement of OSS Delta
feature support per release - they are not a Microsoft guarantee; the policy is versioned so operators can see which
rules produced a decision (``POLICY_VERSION``).  Unknown features are always blocked (fail closed).
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

POLICY_VERSION = "olcf-delta-interop/2026.1"
DEFAULT_PROFILE = "fabric-2.0"

# Legacy (pre-table-features) protocol versions imply these features (Delta PROTOCOL.md "Table Features" appendix).
LEGACY_WRITER_FEATURES: dict[int, tuple[str, ...]] = {
    2: ("appendOnly", "invariants"),
    3: ("checkConstraints",),
    4: ("changeDataFeed", "generatedColumns"),
    5: ("columnMapping",),
    6: ("identityColumns",),
}
LEGACY_READER_FEATURES: dict[int, tuple[str, ...]] = {2: ("columnMapping",)}

# feature -> (kind, needed legacy (reader, writer) or None when table-features-only)
#   kind: "writer" (writer-only feature) | "reader_writer"
FEATURES: dict[str, dict[str, Any]] = {
    "appendOnly": {"kind": "writer"},
    "invariants": {"kind": "writer"},
    "checkConstraints": {"kind": "writer"},
    "changeDataFeed": {"kind": "writer"},
    "generatedColumns": {"kind": "writer"},
    "identityColumns": {"kind": "writer"},
    "columnMapping": {"kind": "reader_writer"},
    "deletionVectors": {"kind": "reader_writer"},
    "timestampNtz": {"kind": "reader_writer"},
    "v2Checkpoint": {"kind": "reader_writer"},
    "vacuumProtocolCheck": {"kind": "reader_writer"},
    "typeWidening": {"kind": "reader_writer"},
    "typeWidening-preview": {"kind": "reader_writer"},
    "variantType": {"kind": "reader_writer"},
    "variantType-preview": {"kind": "reader_writer"},
    "domainMetadata": {"kind": "writer"},
    "rowTracking": {"kind": "writer"},
    "icebergCompatV1": {"kind": "writer"},
    "icebergCompatV2": {"kind": "writer"},
    "clustering": {"kind": "writer"},
    "inCommitTimestamp": {"kind": "writer"},
    "allowColumnDefaults": {"kind": "writer"},
}

#: Table properties that cause a (compatible) engine to add a feature when set.
PROPERTY_FEATURES: dict[str, tuple[str, str]] = {
    "delta.appendOnly": ("true", "appendOnly"),
    "delta.enableChangeDataFeed": ("true", "changeDataFeed"),
    "delta.enableDeletionVectors": ("true", "deletionVectors"),
    "delta.enableRowTracking": ("true", "rowTracking"),
    "delta.enableTypeWidening": ("true", "typeWidening"),
    "delta.enableIcebergCompatV1": ("true", "icebergCompatV1"),
    "delta.enableIcebergCompatV2": ("true", "icebergCompatV2"),
    "delta.enableInCommitTimestamps": ("true", "inCommitTimestamp"),
    "delta.checkpointPolicy": ("v2", "v2Checkpoint"),
}

#: Profiles: highest legacy versions and the features the runtime family can read / write.
PROFILES: dict[str, dict[str, Any]] = {
    "fabric-2.0": {
        "label": "Fabric Runtime 2.0 Compatible (Delta 4.x)",
        "max_reader": 3, "max_writer": 7,
        "read": {"appendOnly", "invariants", "checkConstraints", "changeDataFeed", "generatedColumns", "identityColumns", "columnMapping",
                 "deletionVectors", "timestampNtz", "v2Checkpoint", "vacuumProtocolCheck", "typeWidening", "typeWidening-preview",
                 "variantType", "variantType-preview", "domainMetadata", "rowTracking", "icebergCompatV1", "icebergCompatV2", "clustering",
                 "inCommitTimestamp", "allowColumnDefaults"},
        "write": {"appendOnly", "invariants", "checkConstraints", "changeDataFeed", "generatedColumns", "identityColumns", "columnMapping",
                  "deletionVectors", "timestampNtz", "v2Checkpoint", "vacuumProtocolCheck", "typeWidening", "typeWidening-preview",
                  "variantType", "variantType-preview", "domainMetadata", "rowTracking", "icebergCompatV1", "icebergCompatV2", "clustering",
                  "inCommitTimestamp", "allowColumnDefaults"},
    },
    "fabric-1.3": {
        "label": "Fabric Runtime 1.3 Compatible (Delta 3.2, legacy)",
        "max_reader": 3, "max_writer": 7,
        "read": {"appendOnly", "invariants", "checkConstraints", "changeDataFeed", "generatedColumns", "identityColumns", "columnMapping",
                 "deletionVectors", "timestampNtz", "v2Checkpoint", "domainMetadata", "rowTracking", "icebergCompatV1", "icebergCompatV2",
                 "clustering", "typeWidening-preview"},
        "write": {"appendOnly", "invariants", "checkConstraints", "changeDataFeed", "generatedColumns", "identityColumns", "columnMapping",
                  "deletionVectors", "timestampNtz", "v2Checkpoint", "domainMetadata", "rowTracking", "icebergCompatV1", "icebergCompatV2",
                  "clustering", "typeWidening-preview"},
    },
}

#: What the embedded delta-rs engine (used for OLCF-managed maintenance and the DuckDB/Python paths) can *write*.
#: Conservative: features it cannot honour must be handled by Spark.
DELTA_RS_WRITE: set[str] = {"appendOnly", "invariants", "checkConstraints", "changeDataFeed", "generatedColumns", "timestampNtz",
                            "domainMetadata", "deletionVectors", "columnMapping", "v2Checkpoint", "vacuumProtocolCheck"}
DELTA_RS_READ: set[str] = {"appendOnly", "invariants", "checkConstraints", "changeDataFeed", "generatedColumns", "timestampNtz",
                           "domainMetadata", "deletionVectors", "columnMapping", "v2Checkpoint", "vacuumProtocolCheck", "rowTracking",
                           "icebergCompatV1", "icebergCompatV2", "clustering", "identityColumns", "inCommitTimestamp", "allowColumnDefaults"}


@dataclass(frozen=True)
class Protocol:
    """A Delta protocol action in normalised form."""

    min_reader: int = 1
    min_writer: int = 2
    reader_features: frozenset[str] = frozenset()
    writer_features: frozenset[str] = frozenset()

    @classmethod
    def from_any(cls, p: Any) -> Protocol:
        """Build from a dict (``minReaderVersion`` or ``min_reader_version`` spellings) or a delta-rs ``ProtocolVersions``."""
        if isinstance(p, Protocol):
            return p
        get = (lambda *keys: next((p[k] for k in keys if k in p and p[k] is not None), None)) if isinstance(p, dict) else (
            lambda *keys: next((getattr(p, k) for k in keys if getattr(p, k, None) is not None), None))
        return cls(
            min_reader=int(get("minReaderVersion", "min_reader_version") or 1),
            min_writer=int(get("minWriterVersion", "min_writer_version") or 2),
            reader_features=frozenset(get("readerFeatures", "reader_features") or ()),
            writer_features=frozenset(get("writerFeatures", "writer_features") or ()),
        )

    def effective_reader_features(self) -> set[str]:
        """Features a reader must understand (explicit + implied by legacy versions)."""
        out = set(self.reader_features)
        if self.min_reader < 3:
            for v, feats in LEGACY_READER_FEATURES.items():
                if self.min_reader >= v:
                    out.update(feats)
        return out

    def effective_writer_features(self) -> set[str]:
        out = set(self.writer_features)
        if self.min_writer < 7:
            for v, feats in LEGACY_WRITER_FEATURES.items():
                if self.min_writer >= v:
                    out.update(feats)
        return out

    def to_dict(self) -> dict[str, Any]:
        return {"min_reader_version": self.min_reader, "min_writer_version": self.min_writer,
                "reader_features": sorted(self.reader_features), "writer_features": sorted(self.writer_features)}


@dataclass
class Finding:
    """One problem the Delta policy found: a code, message, the feature involved and a severity of ``blocked`` or ``warn``."""
    code: str  # unknown_feature | unsupported_feature | version_too_high | engine_cannot_write | breaks_readers | ...
    message: str
    feature: str | None = None
    severity: str = "blocked"  # blocked | warn

    def to_dict(self) -> dict[str, Any]:
        return {"code": self.code, "message": self.message, "feature": self.feature, "severity": self.severity}


@dataclass
class PolicyDecision:
    """The Delta policy's verdict for an operation on a table with a given profile and engine, with its findings."""
    operation: str
    profile: str
    engine: str
    findings: list[Finding] = field(default_factory=list)
    policy_version: str = POLICY_VERSION

    @property
    def allowed(self) -> bool:
        return not any(f.severity == "blocked" for f in self.findings)

    @property
    def level(self) -> str:
        if not self.allowed:
            return "blocked"
        return "warn" if self.findings else "ok"

    def reasons(self) -> list[str]:
        return [f.message for f in self.findings]

    def to_dict(self) -> dict[str, Any]:
        return {"operation": self.operation, "profile": self.profile, "engine": self.engine, "allowed": self.allowed, "level": self.level,
                "policy_version": self.policy_version, "findings": [f.to_dict() for f in self.findings]}


def _profile(name: str | None) -> dict[str, Any]:
    p = PROFILES.get(name or DEFAULT_PROFILE)
    if p is None:
        raise ValueError(f"unknown Delta interop profile '{name}' (known: {sorted(PROFILES)})")
    return p


def features_from_properties(properties: dict[str, str] | None) -> set[str]:
    """Table features that setting ``properties`` would add (``delta.enableX = true`` ...)."""
    out: set[str] = set()
    for key, value in (properties or {}).items():
        spec = PROPERTY_FEATURES.get(key)
        if spec and str(value).lower() == spec[0]:
            out.add(spec[1])
        if key == "delta.columnMapping.mode" and str(value).lower() in ("name", "id"):
            out.add("columnMapping")
    return out


def evaluate(protocol: Any, operation: str = "read", *, engine: str = "spark", profile: str | None = None,
             requested: Any = None, properties: dict[str, str] | None = None) -> PolicyDecision:
    """Evaluate ``operation`` (``read`` | ``write`` | ``upgrade``) on a table with ``protocol``.

    ``engine`` is ``spark`` (OLCF Spark runtime) or ``delta-rs`` (embedded engine).  For ``upgrade`` pass the
    ``requested`` protocol and/or the table ``properties`` that would be set.
    """
    if operation not in ("read", "write", "upgrade"):
        raise ValueError("operation must be read, write or upgrade")
    prof = _profile(profile)
    pname = profile or DEFAULT_PROFILE
    cur = Protocol.from_any(protocol)
    d = PolicyDecision(operation=operation, profile=pname, engine=engine)

    def check_version(p: Protocol, role: str) -> None:
        if p.min_reader > prof["max_reader"]:
            d.findings.append(Finding("version_too_high", f"{role}: minReaderVersion {p.min_reader} exceeds what {prof['label']} supports ({prof['max_reader']})"))
        if p.min_writer > prof["max_writer"]:
            d.findings.append(Finding("version_too_high", f"{role}: minWriterVersion {p.min_writer} exceeds what {prof['label']} supports ({prof['max_writer']})"))

    def check_features(feats: set[str], supported: set[str], role: str, *, engine_set: set[str] | None = None, what: str = "") -> None:
        for f in sorted(feats):
            if f not in FEATURES:
                d.findings.append(Finding("unknown_feature", f"{role}: unknown table feature '{f}' - OLCF cannot guarantee interoperability", f))
            elif f not in supported:
                d.findings.append(Finding("unsupported_feature", f"{role}: feature '{f}' is not supported by {prof['label']}", f))
            elif engine_set is not None and f not in engine_set:
                d.findings.append(Finding("engine_cannot_write", f"{role}: the embedded delta-rs engine cannot honour feature '{f}'{what}; run this operation with Spark", f))

    if operation == "read":
        check_version(cur, "read")
        check_features(cur.effective_reader_features(), prof["read"], "read")
        if engine == "delta-rs":
            for f in sorted(cur.effective_reader_features()):
                if f in FEATURES and f in prof["read"] and f not in DELTA_RS_READ:
                    d.findings.append(Finding("engine_cannot_read", f"the embedded delta-rs engine cannot read feature '{f}'; use the Spark engine", f))
    elif operation == "write":
        check_version(cur, "write")
        check_features(cur.effective_reader_features(), prof["read"], "write(reader side)")
        check_features(cur.effective_writer_features(), prof["write"], "write", engine_set=DELTA_RS_WRITE if engine == "delta-rs" else None)
    else:  # upgrade
        new = Protocol.from_any(requested) if requested is not None else cur
        added = features_from_properties(properties)
        merged = Protocol(
            max(new.min_reader, cur.min_reader), max(new.min_writer, cur.min_writer),
            frozenset(new.reader_features | cur.reader_features | {a for a in added if FEATURES.get(a, {}).get("kind") == "reader_writer"}),
            frozenset(new.writer_features | cur.writer_features | added),
        )
        check_version(merged, "upgrade")
        check_features(merged.effective_reader_features(), prof["read"], "upgrade")
        check_features(merged.effective_writer_features(), prof["write"], "upgrade")
        # downgrade-compat: would the upgraded table stop being readable on the *other* profiles?
        for other, oprof in PROFILES.items():
            if other == pname:
                continue
            bad = sorted(f for f in merged.effective_reader_features() if f in FEATURES and f not in oprof["read"])
            if bad:
                d.findings.append(Finding("breaks_readers", f"upgrade would make the table unreadable on {oprof['label']}: needs {', '.join(bad)}",
                                          bad[0], severity="warn"))
        if merged.min_reader > cur.min_reader and cur.min_reader < 3:
            d.findings.append(Finding("raises_reader_version", f"upgrade raises minReaderVersion {cur.min_reader} -> {merged.min_reader}; every reader of this table must support it",
                                      severity="warn"))
    return d


def compatibility_matrix(protocol: Any) -> dict[str, dict[str, Any]]:
    """Per profile: can it read / write a table with ``protocol`` (used by the table detail page)."""
    out: dict[str, dict[str, Any]] = {}
    for name, prof in PROFILES.items():
        r, w = evaluate(protocol, "read", profile=name), evaluate(protocol, "write", profile=name)
        out[name] = {"label": prof["label"], "read": r.allowed, "write": w.allowed, "reasons": r.reasons() + [x for x in w.reasons() if x not in r.reasons()]}
    return out


def protocol_for_schema(arrow_schema: Any, properties: dict[str, str] | None = None) -> Protocol:
    """Protocol a *new* table created with ``arrow_schema``/``properties`` will need (delta-rs behaviour)."""
    import pyarrow as pa

    reader: set[str] = set()
    writer: set[str] = set()
    for f in arrow_schema:
        t = f.type
        if pa.types.is_timestamp(t) and t.tz is None:
            reader.add("timestampNtz")
            writer.add("timestampNtz")
    feats = features_from_properties(properties)
    for f in feats:
        writer.add(f)
        if FEATURES.get(f, {}).get("kind") == "reader_writer":
            reader.add(f)
    if reader or writer:
        return Protocol(3 if reader else 1, 7, frozenset(reader), frozenset(writer))
    return Protocol(1, 2)
