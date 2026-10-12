# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Walk the application's OpenAPI document: every operation, with generated (schema-valid) bodies, concrete path parameters and query parameters."""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from typing import Any

METHODS = ("get", "post", "put", "patch", "delete")


@dataclass
class Op:
    method: str
    path: str
    params: list[dict[str, Any]]
    body_schema: dict[str, Any] | None
    body_type: str | None
    summary: str
    streaming: bool = False

    @property
    def key(self) -> tuple[str, str]:
        return self.method.upper(), self.path

    @property
    def label(self) -> str:
        return f"{self.method.upper()} {self.path}"

    @property
    def path_params(self) -> list[str]:
        return re.findall(r"\{([^}]+)\}", self.path)

    @property
    def mutating(self) -> bool:
        return self.method != "get"


def resolve(spec: dict[str, Any], schema: dict[str, Any] | None, depth: int = 0) -> dict[str, Any]:
    if not isinstance(schema, dict):
        return {}
    if "$ref" in schema:
        node: Any = spec
        for part in schema["$ref"].lstrip("#/").split("/"):
            node = node[part]
        return resolve(spec, node, depth + 1) if depth < 12 else {}
    return schema


def sample(spec: dict[str, Any], schema: dict[str, Any] | None, depth: int = 0) -> Any:
    """A minimal schema-valid value (required properties only; first enum value; sensible strings)."""
    s = resolve(spec, schema)
    if depth > 6 or not s:
        return None
    for key in ("anyOf", "oneOf"):
        if key in s:
            opts = [resolve(spec, o) for o in s[key]]
            opts = [o for o in opts if o.get("type") != "null"] or opts
            return sample(spec, opts[0], depth + 1)
    if "allOf" in s:
        merged: dict[str, Any] = {}
        for o in s["allOf"]:
            v = sample(spec, o, depth + 1)
            if isinstance(v, dict):
                merged.update(v)
        return merged
    if "const" in s:
        return s["const"]
    if "enum" in s:
        return s["enum"][0]
    if "default" in s and s["default"] is not None:
        return s["default"]
    t = s.get("type")
    if t == "string":
        fmt = s.get("format")
        if fmt == "date-time":
            return "2026-01-01T00:00:00Z"
        if fmt == "uri":
            return "https://example.com/x"
        if fmt == "email":
            return "x@example.com"
        n = max(int(s.get("minLength", 0)), 1)
        return "x" * n
    if t in ("integer", "number"):
        return s.get("minimum", 1) if s.get("minimum", 1) >= 0 else 1
    if t == "boolean":
        return False
    if t == "array":
        n = int(s.get("minItems", 0))
        return [sample(spec, s.get("items"), depth + 1) for _ in range(n)]
    if t == "object" or "properties" in s:
        out: dict[str, Any] = {}
        for name in s.get("required", []):
            out[name] = sample(spec, (s.get("properties") or {}).get(name), depth + 1)
        return out
    return None


def operations(spec: dict[str, Any]) -> list[Op]:
    ops: list[Op] = []
    for path, item in spec["paths"].items():
        shared = item.get("parameters", [])
        for m in METHODS:
            if m not in item:
                continue
            o = item[m]
            params = [resolve(spec, p) for p in [*shared, *o.get("parameters", [])]]
            body = (o.get("requestBody") or {}).get("content") or {}
            btype = next(iter(body), None)
            resp = json.dumps(o.get("responses", {}))
            ops.append(Op(m, path, params, resolve(spec, body[btype].get("schema")) if btype else None, btype, o.get("summary", ""),
                          streaming="text/event-stream" in resp or path.endswith("/stream")))
    return ops


def build(spec: dict[str, Any], op: Op, path_values: dict[str, str]) -> dict[str, Any]:
    """httpx request kwargs for ``op`` (method/url/params/json/content)."""
    url = op.path
    for name in op.path_params:
        url = url.replace("{" + name + "}", path_values.get(name, "x"))
    query: dict[str, Any] = {}
    for p in op.params:
        if p.get("in") == "query" and p.get("required"):
            v = sample(spec, p.get("schema"))
            query[p["name"]] = v if v is not None else "x"
    kw: dict[str, Any] = {"method": op.method.upper(), "url": url, "params": query}
    if op.body_type and "json" in op.body_type:
        kw["json"] = sample(spec, op.body_schema) if op.body_schema else {}
    elif op.body_type and "multipart" in op.body_type:
        kw["files"] = {"file": ("x.txt", b"x", "text/plain")}
    elif op.body_type and "octet-stream" in op.body_type:
        kw["content"] = b"x"
    return kw


#: request-body / query keys that carry a resource id of the tenant under attack (key -> Env.tenants[..].ids key, or "ws")
ID_KEYS = {"workspace_id": "ws", "ws": "ws", "item_id": "item_id", "pipeline_id": "pipeline_id", "secret_id": "sid", "connection_id": "cid", "lakehouse_id": "lh_id",
           "job_id": "job_id", "environment_id": "environment_id"}


def retarget(value: Any, tenant: Any) -> Any:
    """Replace resource-id-looking fields of a generated body by the ids of ``tenant`` (so an attacker aims at somebody else's objects)."""
    if isinstance(value, dict):
        out = {}
        for k, v in value.items():
            if k in ID_KEYS:
                real = tenant.ws if ID_KEYS[k] == "ws" else tenant.ids.get(ID_KEYS[k])
                out[k] = real if real and isinstance(v, str) else retarget(v, tenant)
            else:
                out[k] = retarget(v, tenant)
        return out
    if isinstance(value, list):
        return [retarget(v, tenant) for v in value]
    return value


def request_for(env: Any, op: Op, tenant_key: str, headers: dict[str, str] | None) -> dict[str, Any]:
    """Request kwargs aimed at tenant ``tenant_key``'s objects (path params, ids in the body, workspace_id filter in the query)."""
    t = env.tenants[tenant_key]
    kw = build(env.spec, op, env.param_values(tenant_key))
    if "json" in kw:
        kw["json"] = retarget(kw["json"], t)
    declared = {p["name"] for p in op.params if p.get("in") == "query"}
    for name in ("workspace_id", "workspace"):
        if name in declared:
            kw["params"][name] = t.ws
    if headers:
        kw["headers"] = headers
    return kw


def leaked(text: str, *needles: str) -> list[str]:
    return [n for n in needles if n and n in text]
