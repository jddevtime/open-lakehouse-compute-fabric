# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Unit tests for the Delta interoperability policy: allowed / blocked protocol and feature combinations."""

from __future__ import annotations

import pyarrow as pa
import pytest

from olcf.storage import delta_policy as dp


def proto(r=1, w=2, rf=(), wf=()):
    return dp.Protocol(r, w, frozenset(rf), frozenset(wf))


def test_baseline_protocol_is_fully_allowed():
    for op in ("read", "write"):
        for engine in ("spark", "delta-rs"):
            d = dp.evaluate(proto(), op, engine=engine)
            assert d.allowed and d.level == "ok", d.to_dict()


def test_legacy_versions_imply_features():
    p = proto(2, 5)
    assert {"columnMapping"} <= p.effective_reader_features()
    assert {"appendOnly", "invariants", "checkConstraints", "changeDataFeed", "generatedColumns", "columnMapping"} <= p.effective_writer_features()
    assert dp.evaluate(p, "read").allowed and dp.evaluate(p, "write").allowed


def test_supported_table_features_allowed_for_spark():
    p = proto(3, 7, rf=["deletionVectors", "timestampNtz", "columnMapping"], wf=["deletionVectors", "timestampNtz", "columnMapping", "rowTracking", "domainMetadata"])
    assert dp.evaluate(p, "read", engine="spark").allowed
    assert dp.evaluate(p, "write", engine="spark").allowed


def test_delta_rs_cannot_write_features_it_does_not_honour():
    p = proto(1, 7, wf=["rowTracking", "identityColumns"])
    d = dp.evaluate(p, "write", engine="delta-rs")
    assert not d.allowed
    codes = {(f.code, f.feature) for f in d.findings}
    assert ("engine_cannot_write", "rowTracking") in codes and ("engine_cannot_write", "identityColumns") in codes
    assert dp.evaluate(p, "write", engine="spark").allowed  # Spark can


def test_unknown_features_fail_closed():
    p = proto(3, 7, rf=["quantumColumns"], wf=["quantumColumns"])
    for op in ("read", "write"):
        d = dp.evaluate(p, op)
        assert not d.allowed and d.findings[0].code == "unknown_feature"


def test_version_beyond_profile_is_blocked():
    d = dp.evaluate(proto(4, 9), "read")
    assert not d.allowed and any(f.code == "version_too_high" for f in d.findings)


def test_profiles_differ():
    p = proto(3, 7, rf=["variantType"], wf=["variantType"])
    assert dp.evaluate(p, "read", profile="fabric-2.0").allowed
    d13 = dp.evaluate(p, "read", profile="fabric-1.3")
    assert not d13.allowed and d13.findings[0].code == "unsupported_feature"
    m = dp.compatibility_matrix(p)
    assert m["fabric-2.0"]["read"] is True and m["fabric-1.3"]["read"] is False
    with pytest.raises(ValueError):
        dp.evaluate(p, "read", profile="nope")


def test_upgrade_blocks_features_beyond_profile_and_warns_on_other_profiles():
    cur = proto()
    ok = dp.evaluate(cur, "upgrade", properties={"delta.enableChangeDataFeed": "true"})
    assert ok.allowed and ok.level == "ok"
    d = dp.evaluate(cur, "upgrade", requested=proto(3, 7, rf=["variantType"], wf=["variantType"]), profile="fabric-1.3")
    assert not d.allowed and d.findings[0].code == "unsupported_feature"
    # enabling variant on the 2.0 profile is allowed but breaks readers on the legacy 1.3 family -> warning
    w = dp.evaluate(cur, "upgrade", requested=proto(3, 7, rf=["variantType"], wf=["variantType"]))
    assert w.allowed and w.level == "warn" and any(f.code == "breaks_readers" for f in w.findings)
    # raising the reader version warns that every reader must follow
    r = dp.evaluate(proto(1, 2), "upgrade", requested=proto(3, 7, rf=["deletionVectors"], wf=["deletionVectors"]))
    assert any(f.code == "raises_reader_version" for f in r.findings)
    # unknown requested feature is blocked
    assert not dp.evaluate(cur, "upgrade", requested=proto(3, 7, rf=["mystery"], wf=["mystery"])).allowed


def test_features_from_properties():
    f = dp.features_from_properties({"delta.enableDeletionVectors": "true", "delta.enableChangeDataFeed": "false", "delta.columnMapping.mode": "name",
                                     "delta.checkpointPolicy": "v2", "other": "x"})
    assert f == {"deletionVectors", "columnMapping", "v2Checkpoint"}


def test_protocol_for_schema_detects_timestamp_ntz():
    naive = pa.schema([("t", pa.timestamp("us"))])
    aware = pa.schema([("t", pa.timestamp("us", tz="UTC"))])
    assert dp.protocol_for_schema(naive).writer_features == frozenset({"timestampNtz"})
    p = dp.protocol_for_schema(aware)
    assert (p.min_reader, p.min_writer) == (1, 2)
    p2 = dp.protocol_for_schema(aware, {"delta.enableDeletionVectors": "true"})
    assert "deletionVectors" in p2.reader_features and p2.min_reader == 3


def test_protocol_from_delta_rs_object_and_dict():
    from deltalake import DeltaTable  # noqa: F401 - ensures the library is importable

    p = dp.Protocol.from_any({"minReaderVersion": 3, "minWriterVersion": 7, "readerFeatures": ["columnMapping"], "writerFeatures": ["columnMapping"]})
    assert p.min_reader == 3 and "columnMapping" in p.reader_features
    q = dp.Protocol.from_any({"min_reader_version": 1, "min_writer_version": 2, "reader_features": None, "writer_features": None})
    assert q == proto()
    assert p.to_dict()["reader_features"] == ["columnMapping"]
