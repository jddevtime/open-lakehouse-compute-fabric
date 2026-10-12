# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""The capsule contract: schema file in sync with the models, SDK documents validate against it, canonical form and MAC are byte-identical on both sides, scanner patterns."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

import olcf_fabric._canonical as sdk_canon
from olcf.fabric import capsule as server
from olcf.fabric import signing
from olcf_fabric import capsule as sdk_cap
from tests.fabric.fabric_support import ROOT, ipynb

jsonschema = pytest.importorskip("jsonschema")
SCHEMA_PATH = ROOT / "schemas" / "capsule-v1.json"


def sample_signed(**over: Any) -> dict[str, Any]:
    text = ipynb([("code", "n = 1", ["parameters"]), ("code", "print(n)")])
    parts = sdk_cap.request_parts(
        source=sdk_cap.source_block(mode="inline", fmt="ipynb", content=text, notebook_id="nb1", notebook_name="etl", workspace_id="ws1", workspace_name="Sales",
                                    original=("ipynb", text), excluded=[2]),
        context={"normalized": {"currentNotebookName": "etl", "isForPipeline": False}, "raw": {"currentNotebookName": "etl"}},
        runtime={"family": "fabric-2.0-compatible", "constraints": {"engine": "spark"}, "sparkConf": {"spark.sql.shuffle.partitions": "4"}},
        parameters={"n": 1, "name": "x", "nested": {"a": [1, 2, {"b": None}]}},
        data_references={"resolved": [], "logical": [{"kind": "lakehouse", "role": "default", "name": "bronze"}]},
        policy={"queue": "default", "priority": 5, "timeoutSeconds": 600}, identity={"tenantId": "t", "userId": "u"},
        secret_refs=[{"vault": "https://v.vault.azure.net/", "name": "k"}])
    ch = {"nonce": "n" * 43, "submissionId": "sub_" + "0" * 26, "key": "a2V5" * 8, "keyId": "k-abc", "serverTime": "2026-10-07T10:00:00.000Z", "adapterVersion": "0.1.0"}
    signed = sdk_cap.build_signed(parts, ch, idempotency_key="idem-1", correlation_id="corr-1", elapsed_since_challenge=1.5)
    signed.update(over)
    return signed


def test_schema_file_is_in_sync_with_the_models() -> None:
    on_disk = json.loads(SCHEMA_PATH.read_text())
    assert on_disk == json.loads(json.dumps(server.json_schema())), "regenerate: python -c 'import json;from olcf.fabric.capsule import json_schema;print(json.dumps(json_schema(),indent=2,sort_keys=True))' > schemas/capsule-v1.json"
    jsonschema.Draft202012Validator.check_schema(on_disk)
    assert on_disk["$schema"].endswith("2020-12/schema")


def test_sdk_documents_validate_against_the_schema_and_the_models() -> None:
    schema = json.loads(SCHEMA_PATH.read_text())
    signed = sample_signed()
    jsonschema.Draft202012Validator(schema).validate(signed)
    model = server.SignedCapsule.model_validate(signed)
    assert model.capsule.submission_id.startswith("sub_") and model.capsule.parameters["n"] == 1
    assert signed["capsule"]["createdAt"] == "2026-10-07T10:00:01.500Z"        # server time + elapsed, not the Fabric session's clock


def test_schema_rejects_unknown_and_malformed_fields() -> None:
    validator = jsonschema.Draft202012Validator(json.loads(SCHEMA_PATH.read_text()))
    bad = sample_signed()
    bad["capsule"]["surprise"] = 1
    assert list(validator.iter_errors(bad))
    bad2 = sample_signed()
    del bad2["capsule"]["security"]["nonce"]
    assert list(validator.iter_errors(bad2))
    with pytest.raises(Exception):  # noqa: B017, PT011 - pydantic ValidationError
        server.SignedCapsule.model_validate(bad)


def test_canonical_json_and_mac_are_identical_in_the_sdk_and_the_server() -> None:
    doc = {"b": [1, 2.5, None, True], "a": {"z": "é中", "y": "line\nbreak"}, "c": 10**20}
    assert sdk_canon.canonical_json(doc) == server.canonical_json(doc)
    assert sdk_canon.canonical_json(doc) == b'{"a":{"y":"line\\nbreak","z":"\xc3\xa9\xe4\xb8\xad"},"b":[1,2.5,null,true],"c":100000000000000000000}'
    assert sdk_canon.MAC_DOMAIN == server.MAC_DOMAIN
    key_b64 = "c2VjcmV0LWtleS1mb3ItdGVzdHM"  # gitleaks:allow
    sig = sdk_canon.sign_capsule(doc, key_b64, "k-1")
    assert sig["digest"] == server.capsule_digest(doc) and sig["alg"] == "HMAC-SHA256"
    assert sig["mac"] == signing.sign(sdk_canon.b64url_decode(key_b64), server.canonical_json(doc))
    from olcf.core.errors import BadRequest

    with pytest.raises(BadRequest):
        server.canonical_json({"x": float("nan")})
    with pytest.raises(ValueError):
        sdk_canon.canonical_json({"x": float("inf")})


def test_timestamps() -> None:
    assert sdk_cap.iso(0) == "1970-01-01T00:00:00.000Z"
    assert sdk_cap.parse_iso("2026-10-07T10:00:00.250Z").microsecond == 250000
    assert server.parse_ts("2026-10-07T10:00:00.250Z").tzinfo is not None
    with pytest.raises(ValueError):
        server.parse_ts("yesterday")


def test_source_block_hashes_and_pull_mode_carries_no_content() -> None:
    inline = sdk_cap.source_block(mode="inline", fmt="ipynb", content="abc", notebook_id="n", notebook_name=None, workspace_id="w", workspace_name=None, original=("ipynb", "abc"), excluded=[])
    assert inline["sha256"] == sdk_canon.sha256_hex("abc") and inline["original"]["sha256"] == inline["sha256"] and "notebookName" not in inline and "notebook_name" not in inline
    pull = sdk_cap.source_block(mode="pull", fmt="ipynb", content="must not travel", notebook_id="n", notebook_name="x", workspace_id="w", workspace_name=None, original=None, excluded=[])
    assert "content" not in pull and pull["mode"] == "pull"


# ------------------------------------------------------------------------------------------------ scanner
SECRETS = {
    "jwt": "tok = 'eyJhbGciOiJSUzI1NiJ9.eyJzdWIiOiIxMjM0NTY3ODkwIn0.c2lnbmF0dXJlLXNpZ25hdHVyZQ'",  # gitleaks:allow
    "private-key": "-----BEGIN RSA PRIVATE KEY-----\nMIIB",
    "entra-client-secret": "secret = 'abc8Q~" + "x" * 34 + "'",
    "olcf-token": "headers = {'t': 'olcf_pat_abcdefgh12345678_zzzzzzzzzzzzzzzzzzzz'}",
    "azure-storage-key": "cs = 'DefaultEndpointsProtocol=https;AccountName=a;AccountKey=" + "A" * 44 + "=='",
    "sas-token": "url = 'https://a.blob.core.windows.net/c/f?sv=2021&sig=" + "B" * 40 + "'",
    "bearer-token": "h = 'Authorization: Bearer " + "c" * 40 + "'",
    "aws-secret": "aws_secret_access_key = '" + "d" * 40 + "'",
}


@pytest.mark.parametrize("kind", sorted(SECRETS))
def test_high_severity_patterns(kind: str) -> None:
    found = server.find_secrets({"cells": [SECRETS[kind]]})
    assert [f.kind for f in found if f.severity == "high"] == [kind]
    assert found[0].path == "cells[0]"
    assert SECRETS[kind][10:30] not in json.dumps([f.to_dict() for f in found])          # findings never echo the value


def test_medium_patterns_and_known_values() -> None:
    med = server.find_secrets("password = 'hunter2hunter2'\nclient_secret = 'abcdefghij'")
    assert {f.kind for f in med} == {"password-assignment", "client-secret-assignment"} and all(f.severity == "medium" for f in med)
    known = server.find_secrets({"deep": {"x": ["prefix-Zx9~broker-client-secret-do-not-leak-suffix"]}}, known=["Zx9~broker-client-secret-do-not-leak", "abc"])
    assert [(f.path, f.kind) for f in known] == [("deep.x[0]", "known-secret")]
    assert server.find_secrets("short", known=["abc"]) == []                                 # known values shorter than 6 characters are ignored
    assert server.scan_text("nothing to see here") == []


def test_benign_code_is_not_flagged() -> None:
    code = "df = spark.read.format('delta').load('Tables/orders')\nsecret = notebookutils.credentials.getSecret('https://v.vault.azure.net/', 'k')\npassword_hint = None\n"
    assert server.find_secrets(code) == []
    assert server.find_secrets({"n": 1, "ok": True, "items": [None, 2.5]}) == []


def test_normalise_context_keeps_only_known_keys() -> None:
    out = server.normalise_context({"currentNotebookName": "x", "isForPipeline": 1, "mysteryKey": "dropped", "defaultLakehouseId": None})
    assert out["currentNotebookName"] == "x" and "mysteryKey" not in out
    assert Path(SCHEMA_PATH).exists()
