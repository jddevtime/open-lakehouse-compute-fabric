# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""The generated OpenAPI document of the full application is clean and typed.

* every operation id is unique and building the application / its document raises no warning (a route registered with several methods under one name
  produces "Duplicate Operation ID" warnings and breaks generated clients);
* no operation takes an untyped JSON request body: every body is a named schema with properties (``ALLOWED_UNTYPED`` lists the deliberate exceptions - there are none);
* the schemas that SDK / UI authors rely on (identity provider, compute provider, notebook session request) carry their fields.
"""

from __future__ import annotations

import collections
import warnings
from typing import Any

import pytest

#: ``"METHOD /path"`` of operations whose JSON body is deliberately free-form. Keep this empty: model the body instead (``olcf.core.models.OpenBody`` keeps unknown keys).
ALLOWED_UNTYPED: set[str] = set()


@pytest.fixture(scope="module")
def spec(tmp_path_factory) -> dict[str, Any]:
    from olcf.api.app import create_app
    from olcf.core.config import load_settings

    settings = load_settings(tmp_path_factory.mktemp("openapi"), environment="test")
    settings.compute.embedded_worker = False
    with warnings.catch_warnings():
        warnings.simplefilter("error")  # FastAPI reports duplicate operation ids (and other document problems) as warnings
        app = create_app(settings)
        return app.openapi()


def _ops(spec: dict[str, Any]):
    for path, item in sorted(spec["paths"].items()):
        for method, op in item.items():
            if isinstance(op, dict) and "operationId" in op:
                yield method.upper(), path, op


def _untyped(spec: dict[str, Any], js: dict[str, Any] | None) -> bool:
    """True when a JSON schema says nothing about its content: no schema, a bare object / ``additionalProperties`` map, or a model without properties."""
    if not js:
        return True
    schemas = spec["components"]["schemas"]
    if "$ref" in js:
        t = schemas[js["$ref"].rsplit("/", 1)[-1]]
        return not (t.get("properties") or t.get("anyOf") or t.get("allOf") or t.get("oneOf") or t.get("enum"))
    for key in ("anyOf", "oneOf", "allOf"):
        if key in js:
            return all(_untyped(spec, x) for x in js[key] if x.get("type") != "null")
    if js.get("type") == "array":
        return _untyped(spec, js.get("items") or {})
    if js.get("type") == "object" and not js.get("properties"):
        extra = js.get("additionalProperties")
        return not (isinstance(extra, dict) and extra.get("type"))  # a map with typed values (labels: str -> str) says what it holds
    return False


def test_openapi_has_unique_operation_ids_and_no_warnings(spec):
    ops = [op["operationId"] for _, _, op in _ops(spec)]
    assert len(ops) > 100
    assert [k for k, v in collections.Counter(ops).items() if v > 1] == []


def test_no_operation_has_an_untyped_json_body(spec):
    offenders = []
    for method, path, op in _ops(spec):
        content = (op.get("requestBody") or {}).get("content") or {}
        if "application/json" in content and _untyped(spec, content["application/json"].get("schema")) and f"{method} {path}" not in ALLOWED_UNTYPED:
            offenders.append(f"{method} {path}")
    assert offenders == [], "model these request bodies (pydantic) instead of dict[str, Any]: " + ", ".join(offenders)


def test_the_allow_list_has_no_stale_entries(spec):
    live = {f"{m} {p}" for m, p, _ in _ops(spec)}
    assert ALLOWED_UNTYPED <= live


def test_trigger_schedule_webhook_alert_bodies_document_their_fields(spec):
    schemas = spec["components"]["schemas"]

    def body(method: str, path: str) -> dict[str, Any]:
        js = spec["paths"][path][method]["requestBody"]["content"]["application/json"]["schema"]
        js = next((x for x in js.get("anyOf", [js]) if x.get("type") != "null"), js)
        return schemas[js["$ref"].rsplit("/", 1)[-1]]["properties"]

    assert {"workspace_id", "name", "type", "target", "parameters", "config", "enabled"} <= set(body("post", "/v1/triggers"))
    assert {"minutes", "until"} <= set(body("post", "/v1/triggers/{trigger_id}/pause"))
    assert {"workspace_id", "name", "definition"} <= set(body("post", "/v1/schedules"))
    assert {"cron", "timezone", "count", "after"} <= set(body("post", "/v1/schedules/preview"))
    assert {"name", "url", "event_types", "secret", "headers", "enabled"} <= set(body("post", "/v1/webhooks"))
    assert {"name", "type", "params", "severity", "channels"} <= set(body("post", "/v1/alerts/rules"))
    assert {"name", "type", "config", "secret"} <= set(body("post", "/v1/alerts/channels"))
    assert {"parameters", "queue_policy"} <= set(body("post", "/v1/pipelines/{pipeline_id}/runs"))
    assert {"value"} <= set(body("put", "/v1/admin/settings/{key}"))
    assert {"description", "settings", "quota", "tags"} <= set(body("patch", "/v1/workspaces/{ws}"))


def test_notebook_session_request_and_provider_models_carry_their_fields(spec):
    schemas = spec["components"]["schemas"]
    assert {"engine", "runtime", "resources", "default_lakehouse", "placement", "name", "idle_timeout_seconds", "eager_spark"} <= set(schemas["SessionRequest"]["properties"])
    js = spec["paths"]["/v1/notebooks/{item_id}/session"]["post"]["requestBody"]["content"]["application/json"]["schema"]
    assert any(x.get("$ref", "").endswith("/SessionRequest") for x in js["anyOf"])
    assert {"config", "has_client_secret", "last_test", "issuer", "client_id", "display_name"} <= set(schemas["IdentityProviderOut"]["properties"])
    assert {"config", "secret_id", "last_discovery", "capabilities", "live_verified", "builtin"} <= set(schemas["ProviderOut"]["properties"])
    ok = spec["paths"]["/v1/identity/providers/{provider_id}"]["get"]["responses"]["200"]["content"]["application/json"]["schema"]
    assert ok["$ref"].endswith("/IdentityProviderOut")
