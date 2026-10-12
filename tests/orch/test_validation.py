# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Pipeline definition validation, the JSON Schema, the plugin registry and the Fabric import."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import jsonschema
import pytest
import yaml

from olcf.core.errors import ValidationFailed
from olcf.pipelines import model as M
from olcf.pipelines.activities import build_registry
from olcf.pipelines.activities.base import Activity, ActivityRegistry, obj
from olcf.pipelines.fabric_import import FabricImportError, import_fabric_pipeline, support_document

REG = build_registry(plugins=False)
REPO = Path(__file__).resolve().parents[2]


def W(name: str, *deps: Any, **kw: Any) -> dict[str, Any]:
    a: dict[str, Any] = {"name": name, "type": "wait", "typeProperties": {"waitTimeInSeconds": 1}, **kw}
    if deps:
        a["dependsOn"] = list(deps)
    return a


def check(defn: dict[str, Any]) -> M.Validation:
    return M.validate_definition({"schema": M.SCHEMA_ID, **defn}, REG)


def codes(res: M.Validation) -> set[str]:
    return {i.code for i in res.errors}


def issue(res: M.Validation, code: str) -> Any:
    hit = [i for i in res.errors if i.code == code]
    assert hit, f"no {code} among {[(i.code, i.path) for i in res.errors]}"
    return hit[0]


def test_valid_definition_and_normal_form() -> None:
    res = check({"parameters": {"day": {"type": "string", "default": "d"}}, "variables": {"v": {"type": "string"}},
                 "activities": [W("a"), W("b", "a"), {"name": "c", "type": "set_variable", "dependsOn": [{"activity": "b", "conditions": ["succeeded", "failed"]}],
                                                      "typeProperties": {"variable": "v", "value": "@pipeline().parameters.day"}}]})
    assert res.ok and not res.errors
    edges = {(e["from"], e["to"]): e["conditions"] for e in res.graph["edges"]}
    assert edges[("a", "b")] == ["succeeded"]           # a bare name means "succeeded"
    assert edges[("b", "c")] == ["succeeded", "failed"]
    assert [n["id"] for n in res.graph["nodes"]] == ["a", "b", "c"]


@pytest.mark.parametrize(("defn", "code", "needle"), [
    ({"activities": [W("a"), W("a")]}, "duplicate_name", "duplicate activity name 'a'"),
    ({"activities": [W("a", "zz")]}, "unknown_dependency", "unknown activity 'zz'"),
    ({"activities": [W("a", "a")]}, "self_dependency", "cannot depend on itself"),
    ({"activities": [W("a", "b"), W("b", "c"), W("c", "a")]}, "cycle", "a -> b -> c -> a"),
    ({"activities": [W("a"), W("b", {"activity": "a", "conditions": ["bogus"]})]}, "bad_condition", "allowed: succeeded, failed, skipped, completed"),
    ({"activities": [{"name": "a", "type": "nope"}]}, "unknown_activity_type", "unknown activity type 'nope'"),
    ({"activities": [{"name": "a", "type": "wait", "typeProperties": {"waitTimeInSeconds": "@bad("}}]}, "expression_syntax", "invalid expression"),
    ({"activities": [{"name": "a", "type": "wait", "typeProperties": {"waitTimeInSeconds": "@foo(1)"}}]}, "expression_syntax", "unknown function 'foo'"),
    ({"activities": [{"name": "a", "type": "wait", "typeProperties": {"waitTimeInSeconds": "@pipeline().parameters.q"}}]}, "unknown_parameter", "undeclared parameter 'q'"),
    ({"activities": [{"name": "a", "type": "wait", "typeProperties": {"waitTimeInSeconds": "@variables('q')"}}]}, "unknown_variable", "undeclared variable 'q'"),
    ({"activities": [W("a"), {"name": "b", "type": "wait", "typeProperties": {"waitTimeInSeconds": "@activity('a').output.x"}}]}, "bad_activity_reference", "not upstream of 'b'"),
    ({"activities": [{"name": "b", "type": "wait", "typeProperties": {"waitTimeInSeconds": "@item()"}}]}, "item_outside_foreach", "inside a for_each"),
    ({"parameters": {"n": {"type": "int", "default": "x"}}, "activities": [W("a")]}, "type", "parameter 'n' must be of type int"),
    ({"activities": [{"name": "bad name!", "type": "wait", "typeProperties": {"waitTimeInSeconds": 1}}]}, "pattern", "does not match"),
    ({"activities": [{"name": "a", "type": "wait", "typeProperties": {}}]}, "required", "missing required property 'waitTimeInSeconds'"),
    ({"activities": [{"name": "a", "type": "wait", "typeProperties": {"seconds": 1, "waitTimeInSeconds": 1}}]}, "unknown_property", "unknown property 'seconds'"),
    ({"activities": [W("a")], "extra": 1}, "unknown_property", "unknown property 'extra'"),
    ({"activities": [W("a", policy={"retries": -1})]}, "minimum", "less than the minimum"),
    ({"activities": [{"name": "s", "type": "set_variable", "typeProperties": {"variable": "w", "value": "1"}}], "variables": {"v": {"type": "string"}}}, "unknown_variable", "variable 'w' is not declared"),
    ({"activities": [{"name": "c", "type": "child_pipeline", "typeProperties": {}}]}, "required", "'pipeline'"),
    ({"activities": [{"name": "h", "type": "http", "typeProperties": {"url": "ftp://x", "method": "GET"}}]}, "bad_url", "absolute http(s) URL"),
])
def test_validation_messages_are_precise(defn: dict[str, Any], code: str, needle: str) -> None:
    res = check(defn)
    assert not res.ok
    i = issue(res, code)
    assert needle in i.message, i.message
    assert i.path, "every issue carries a path"


def test_paths_point_into_nested_scopes() -> None:
    res = check({"activities": [{"name": "f", "type": "for_each", "typeProperties": {"items": "@range(0, 2)", "activities": [
        W("ok"), {"name": "bad", "type": "wait", "dependsOn": ["ghost"], "typeProperties": {"waitTimeInSeconds": 1}}]}}]})
    assert issue(res, "unknown_dependency").path == "activities[0].typeProperties.activities[1].dependsOn[0]"


def test_all_problems_are_reported_at_once() -> None:
    res = check({"activities": [W("a", "zz"), W("a"), {"name": "c", "type": "nope"}]})
    assert {"unknown_dependency", "duplicate_name", "unknown_activity_type"} <= codes(res)


def test_parallel_for_each_variable_race_is_rejected_but_sequential_is_fine() -> None:
    def pipe(**fe: Any) -> M.Validation:
        return check({"variables": {"v": {"type": "string"}}, "activities": [{"name": "f", "type": "for_each", "typeProperties": {
            "items": "@range(0, 3)", **fe, "activities": [{"name": "s", "type": "set_variable", "typeProperties": {"variable": "v", "value": "@string(item())"}}]}}]})
    assert "unsafe_parallel_variable" in codes(pipe(batchCount=4))
    assert pipe(isSequential=True).ok
    assert pipe(batchCount=1).ok


def test_nested_scopes_see_only_their_upstream_activities() -> None:
    res = check({"activities": [{"name": "f", "type": "for_each", "typeProperties": {"items": "@range(0, 2)", "activities": [
        W("g"), {"name": "h", "type": "wait", "dependsOn": ["g"], "typeProperties": {"waitTimeInSeconds": "@add(item(), activity('g').output.waitedSeconds)"}}]}}]})
    assert res.ok, [(i.path, i.message) for i in res.errors]


def test_parse_json_and_yaml_and_errors() -> None:
    assert M.parse_definition_text('{"activities": []}')["activities"] == []
    assert M.parse_definition_text("schema: olcf.pipeline/1\nactivities: []\n", "yaml")["schema"] == "olcf.pipeline/1"
    for bad in ("{bad json", "[1,2]", ""):
        with pytest.raises(ValidationFailed):
            M.parse_definition_text(bad)
    with pytest.raises(ValidationFailed):
        M.parse_definition_text("a: [1\n", "yaml")
    res = check({"activities": [W("a", "z")]})
    text = M.dump_definition(res.definition, "yaml") if res.definition else M.dump_definition({"activities": [W("a")]}, "yaml")
    assert yaml.safe_load(text)["activities"]


def test_limits() -> None:
    too_many = [W(f"a{i}") for i in range(M.MAX_ACTIVITIES + 1)]
    res = check({"activities": too_many})
    assert not res.ok and any("too many" in i.message.lower() or "at most" in i.message.lower() for i in res.errors)


# ------------------------------------------------------------------------------------------------------------------------------ JSON schema
def test_shipped_schema_matches_the_code() -> None:
    from olcf.pipelines.gen_schemas import documents, render

    for name, doc in documents().items():
        path = REPO / "schemas" / name
        assert path.is_file(), f"{path} is missing: python -m olcf.pipelines.gen_schemas"
        assert path.read_text(encoding="utf-8") == render(doc), f"{name} is stale: python -m olcf.pipelines.gen_schemas"


def test_schema_is_valid_draft_2020_12_and_accepts_valid_definitions() -> None:
    schema = json.loads((REPO / "schemas" / "pipeline-v1.json").read_text())
    jsonschema.Draft202012Validator.check_schema(schema)
    v = jsonschema.Draft202012Validator(schema)
    good = {"schema": M.SCHEMA_ID, "parameters": {"day": {"type": "string"}}, "activities": [
        W("a"), {"name": "f", "type": "for_each", "dependsOn": [{"activity": "a", "conditions": ["completed"]}],
                 "typeProperties": {"items": "@range(0, 3)", "batchCount": 2, "activities": [W("w")]}}]}
    assert not list(v.iter_errors(good))
    assert M.validate_definition(good, REG).ok
    for bad in ({"schema": M.SCHEMA_ID, "activities": [{"name": "a", "type": "nope"}]}, {"schema": M.SCHEMA_ID, "activities": [{"name": "a"}]}, {"schema": "other", "activities": []},
                {"schema": M.SCHEMA_ID, "activities": [{"name": "a", "type": "wait", "typeProperties": {}}]}):
        assert list(v.iter_errors(bad)), bad


def test_schema_documents_every_builtin_activity() -> None:
    schema = M.build_schema(REG)
    text = json.dumps(schema)
    for t in ("notebook", "spark_job", "sql", "copy", "script", "http", "child_pipeline", "wait", "set_variable", "if", "for_each", "switch", "fail", "webhook_wait", "until"):
        assert f'"{t}"' in text, t
        assert REG.get(t) is not None


# ------------------------------------------------------------------------------------------------------------------------------ plugins
class Echo(Activity):
    type = "echo_test"
    mode = "blocking"
    properties_schema = obj({"value": {}})

    def execute(self, actx: Any) -> dict[str, Any]:
        return {"value": actx.props.get("value")}


def test_plugin_registration_and_rejection() -> None:
    reg = build_registry(plugins=False)
    reg.register(Echo, source="test")
    assert reg.get("echo_test") is not None
    res = M.validate_definition({"schema": M.SCHEMA_ID, "activities": [{"name": "e", "type": "echo_test", "typeProperties": {"value": 1}}]}, reg)
    assert res.ok
    assert "echo_test" in json.dumps(M.build_schema(reg))

    class Shadow(Echo):
        type = "wait"

    reg.register(Shadow, source="entry-point:bad")
    assert any("shadow" in r["reason"] for r in reg.rejected)

    class Old(Echo):
        type = "old_api"
        api_version = "99.0"

    reg.register(Old, source="entry-point:old")
    assert reg.get("old_api") is None and any(r["type"] == "old_api" for r in reg.rejected)


def test_entry_point_group_name() -> None:
    from olcf.pipelines.activities import ENTRY_POINT_GROUP

    assert ENTRY_POINT_GROUP == "olcf.pipeline_activities"
    assert isinstance(ActivityRegistry().all(), list)


# ------------------------------------------------------------------------------------------------------------------------------ fabric import
FABRIC = {
    "name": "pl_sales", "properties": {
        "parameters": {"day": {"type": "String", "defaultValue": "2026-01-01"}},
        "variables": {"status": {"type": "String"}},
        "activities": [
            {"name": "Wait1", "type": "Wait", "typeProperties": {"waitTimeInSeconds": 3}},
            {"name": "SetStatus", "type": "SetVariable", "dependsOn": [{"activity": "Wait1", "dependencyConditions": ["Succeeded"]}],
             "typeProperties": {"variableName": "status", "value": {"value": "@concat('d-', pipeline().parameters.day)", "type": "Expression"}}},
            {"name": "Run NB", "type": "TridentNotebook", "dependsOn": [{"activity": "SetStatus", "dependencyConditions": ["Succeeded"]}],
             "typeProperties": {"notebookId": "nb-guid-1", "workspaceId": "ws-guid", "parameters": {"day": {"value": {"value": "@pipeline().parameters.day", "type": "Expression"}, "type": "string"}}}},
            {"name": "Mail", "type": "Office365Outlook", "dependsOn": [{"activity": "Run NB", "dependencyConditions": ["Failed"]}], "typeProperties": {}},
            {"name": "Check", "type": "IfCondition", "dependsOn": [{"activity": "Run NB", "dependencyConditions": ["Completed"]}],
             "typeProperties": {"expression": {"value": "@equals(variables('status'), 'x')", "type": "Expression"},
                                "ifTrueActivities": [{"name": "Hello", "type": "Wait", "typeProperties": {"waitTimeInSeconds": 1}}], "ifFalseActivities": []}},
        ]}}


def test_fabric_import_maps_supported_activities_and_reports_the_rest() -> None:
    out = import_fabric_pipeline(FABRIC, REG, item_map={"nb-guid-1": "load_sales"})
    rep, d = out["report"], out["definition"]
    assert d["schema"] == M.SCHEMA_ID
    by = {a["name"]: a for a in d["activities"]}
    assert by["Wait1"]["type"] == "wait" and by["SetStatus"]["type"] == "set_variable"
    assert by["Run NB"]["type"] == "notebook" and by["Run NB"]["typeProperties"]["notebook"] == "load_sales"
    assert by["Check"]["type"] == "if"
    assert by["SetStatus"]["dependsOn"] == [{"activity": "Wait1", "conditions": ["succeeded"]}]
    assert [u["name"] for u in rep["unsupported"]] == ["Mail"]
    assert "Office365Outlook" in json.dumps(rep["unsupported"])
    assert rep["unsupported"][0].get("reason") and rep["unsupported"][0].get("remediation")
    assert out["validation"]["valid"], out["validation"]
    assert rep["imported"] >= 4
    # skip drops the activity, error refuses the whole import
    skipped = import_fabric_pipeline(FABRIC, REG, item_map={"nb-guid-1": "load_sales"}, on_unsupported="skip")
    assert "Mail" not in {a["name"] for a in skipped["definition"]["activities"]}
    with pytest.raises(FabricImportError):
        import_fabric_pipeline(FABRIC, REG, on_unsupported="error")


def test_fabric_import_reports_unresolved_references() -> None:
    out = import_fabric_pipeline(FABRIC, REG)
    assert out["report"].get("unresolved") or any("nb-guid-1" in json.dumps(w) for w in out["report"].get("warnings", []))


def test_fabric_support_document_lists_every_mapping() -> None:
    doc = support_document()
    assert doc["target"] == M.SCHEMA_ID and doc["on_unsupported"] == ["placeholder", "skip", "error"]
    assert "Wait" in json.dumps(doc["supported"]) and "IfCondition" in json.dumps(doc["supported"])
