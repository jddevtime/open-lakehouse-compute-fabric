# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Pipeline definition model: parsing (JSON / YAML), typed parameters and variables, and static validation of the activity DAG.

A pipeline is stored as the ``definition`` of an item of type ``pipeline`` - a plain JSON document that is friendly to Git diffs
(``GET /v1/pipelines/{id}/definition?format=yaml`` renders the same document as YAML)::

    {
      "schema": "olcf.pipeline/1",
      "description": "nightly load",
      "parameters": {"day": {"type": "string", "default": "2026-01-01"}, "limit": {"type": "int"}},
      "variables":  {"status": {"type": "string", "default": "new"}},
      "settings":   {"concurrency": 1, "queuePolicy": "queue", "timeoutSeconds": 7200, "failFast": false},
      "activities": [
        {"name": "Load", "type": "notebook", "typeProperties": {"notebook": "load_sales", "parameters": {"day": "@pipeline().parameters.day"}},
         "policy": {"retries": 2, "retryBackoffSeconds": 30, "timeoutSeconds": 3600}},
        {"name": "Check", "type": "if", "dependsOn": [{"activity": "Load", "conditions": ["succeeded"]}],
         "typeProperties": {"expression": "@greater(activity('Load').output.exitValue, 0)", "ifTrueActivities": [...], "ifFalseActivities": []}}
      ],
      "outputs": {"rows": "@activity('Load').output.exitValue"}
    }

Validation (:func:`validate_definition`) reports *every* problem with a precise ``path`` (``activities[2].typeProperties.items``) and message:
JSON-schema structure and types, unknown activity types, duplicate names, unknown / self / cyclic dependencies (the cycle is printed), invalid
dependency conditions, expression syntax, unknown functions and arity, references to unknown or non-upstream activities, undeclared
variables and parameters, ``item()`` outside ``for_each``, mistyped parameter / variable defaults and unsafe parallel ``set_variable``.
The machine-readable schema is ``schemas/pipeline-v1.json`` (generated from the activity registry by :func:`build_schema`).
"""

from __future__ import annotations

import copy
import hashlib
import json
import re
import threading
import weakref
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml
from jsonschema import Draft202012Validator
from jsonschema.exceptions import ValidationError as JsonSchemaError

from olcf.core.errors import ValidationFailed
from olcf.pipelines.activities.base import Activity, ActivityRegistry, Container, ValidationIssue
from olcf.pipelines.expressions import (
    ExpressionError,
    analyze_value,
    is_expression_value,
)

SCHEMA_ID = "olcf.pipeline/1"
MAX_DEFINITION_BYTES = 5 * 1024 * 1024
MAX_ACTIVITIES = 500
MAX_DEPTH = 6
NAME_RE = re.compile(r"^[A-Za-z0-9_][A-Za-z0-9_ .\-]{0,127}$")
PARAM_TYPES = ("string", "int", "float", "bool", "array", "object", "secret_ref")
VAR_TYPES = ("string", "int", "float", "bool", "array", "object")
CONDITIONS = ("succeeded", "failed", "skipped", "completed")
QUEUE_POLICIES = ("queue", "skip", "cancel_previous")
ENVELOPE_KEYS = {"name", "type", "description", "dependsOn", "policy", "typeProperties", "userProperties"}
POLICY_KEYS = {"retries", "retryBackoffSeconds", "backoffMultiplier", "maxBackoffSeconds", "timeoutSeconds", "cancelGraceSeconds", "idempotent", "reusable", "secureInput", "secureOutput"}


# ---------------------------------------------------------------------------------------------------------------------------------------- typed values
def coerce_typed(value: Any, typ: str, name: str, *, what: str = "parameter") -> Any:
    """Coerce ``value`` to the declared type or raise ``ValueError`` with a precise message."""
    def bad(extra: str = "") -> ValueError:
        shown = json.dumps(value, default=str)
        return ValueError(f"{what} '{name}' must be of type {typ} but got {type(value).__name__} {shown[:60]}{extra}")

    if typ == "string":
        if isinstance(value, str):
            return value
        if isinstance(value, (int, float)) and not isinstance(value, bool):
            return str(value)
        if isinstance(value, bool):
            return "true" if value else "false"
        raise bad()
    if typ == "int":
        if isinstance(value, bool):
            raise bad()
        if isinstance(value, int):
            return value
        if isinstance(value, float) and value.is_integer():
            return int(value)
        if isinstance(value, str) and re.fullmatch(r"[+-]?\d+", value.strip()):
            return int(value.strip())
        raise bad()
    if typ == "float":
        if isinstance(value, bool):
            raise bad()
        if isinstance(value, (int, float)):
            return float(value)
        if isinstance(value, str):
            try:
                return float(value.strip())
            except ValueError:
                pass
        raise bad()
    if typ == "bool":
        if isinstance(value, bool):
            return value
        if isinstance(value, str) and value.strip().lower() in ("true", "false"):
            return value.strip().lower() == "true"
        raise bad()
    if typ in ("array", "object"):
        want = list if typ == "array" else dict
        if isinstance(value, want):
            return value
        if isinstance(value, str):
            try:
                parsed = json.loads(value)
            except ValueError:
                raise bad(" (a string was given but it is not valid JSON)") from None
            if isinstance(parsed, want):
                return parsed
        raise bad()
    if typ == "secret_ref":
        if isinstance(value, str) and value.strip():
            return value.strip()
        raise bad(" (expected a secret id or name)")
    raise ValueError(f"unknown type '{typ}' for {what} '{name}'")


def resolve_parameters(defn: dict[str, Any], supplied: dict[str, Any] | None) -> dict[str, Any]:
    """Merge supplied parameter values with declared defaults and type-check them. Raises :class:`ValidationFailed` listing every problem."""
    declared = defn.get("parameters") or {}
    supplied = dict(supplied or {})
    errors: list[str] = []
    out: dict[str, Any] = {}
    for k in supplied:
        if k not in declared:
            errors.append(f"unknown parameter '{k}' (declared: {', '.join(sorted(declared)) or 'none'})")
    for name, spec in declared.items():
        if name in supplied:
            try:
                out[name] = coerce_typed(supplied[name], spec["type"], name)
            except ValueError as exc:
                errors.append(str(exc))
        elif "default" in spec:
            out[name] = copy.deepcopy(spec["default"])
        else:
            errors.append(f"parameter '{name}' is required (type {spec['type']}) and has no default")
    if errors:
        raise ValidationFailed("invalid pipeline parameters", details={"errors": errors})
    return out


def initial_variables(defn: dict[str, Any]) -> dict[str, Any]:
    out: dict[str, Any] = {}
    for name, spec in (defn.get("variables") or {}).items():
        out[name] = copy.deepcopy(spec["default"]) if "default" in spec else {"string": "", "int": 0, "float": 0.0, "bool": False, "array": [], "object": {}}[spec["type"]]
    return out


# ---------------------------------------------------------------------------------------------------------------------------------------- parse / hash
def canonical_json(obj: Any) -> str:
    return json.dumps(obj, sort_keys=True, separators=(",", ":"), default=str)


def sha256_of(obj: Any) -> str:
    return hashlib.sha256(canonical_json(obj).encode()).hexdigest()


def parse_definition_text(text: str | bytes, fmt: str | None = None) -> dict[str, Any]:
    """Parse a pipeline document given as JSON or YAML text (``fmt`` = ``json`` | ``yaml`` | ``None`` for auto-detect)."""
    raw = text.decode("utf-8") if isinstance(text, bytes) else text
    if len(raw.encode("utf-8")) > MAX_DEFINITION_BYTES:
        raise ValidationFailed(f"pipeline definition is larger than {MAX_DEFINITION_BYTES // 1024 // 1024} MiB")
    try:
        if fmt == "json" or (fmt is None and raw.lstrip().startswith("{")):
            doc = json.loads(raw)
        else:
            doc = yaml.safe_load(raw)
    except (ValueError, yaml.YAMLError) as exc:
        raise ValidationFailed(f"cannot parse pipeline definition: {exc}", remediation="the document must be valid JSON or YAML") from None
    if not isinstance(doc, dict):
        raise ValidationFailed("a pipeline definition must be an object at the top level")
    return doc


def dump_definition(defn: dict[str, Any], fmt: str = "json") -> str:
    if fmt == "yaml":
        return yaml.safe_dump(defn, sort_keys=False, allow_unicode=True, default_flow_style=False, width=120)
    return json.dumps(defn, indent=2, ensure_ascii=False) + "\n"


# ---------------------------------------------------------------------------------------------------------------------------------------- JSON schema
def _envelope_schema() -> dict[str, Any]:
    return {
        "type": "object", "required": ["name", "type"], "additionalProperties": False,
        "properties": {
            "name": {"type": "string", "pattern": NAME_RE.pattern, "description": "Unique within its scope; letters, digits, space, '_', '.', '-'."},
            "type": {"type": "string"},
            "description": {"type": "string", "maxLength": 1024},
            "dependsOn": {"type": "array", "items": {"$ref": "#/$defs/dependency"}},
            "policy": {"$ref": "#/$defs/policy"},
            "typeProperties": {"type": "object"},
            "userProperties": {"type": "object"},
        },
    }


def build_schema(registry: ActivityRegistry, *, include_types: bool = True) -> dict[str, Any]:
    """The JSON Schema (draft 2020-12) of a pipeline definition, generated from the activity registry."""
    defs: dict[str, Any] = {
        "expression": {"type": "string", "pattern": r"^@(?!@)|@\{", "description": "An expression: '@func(...)' or text with '@{...}' interpolations."},
        "dependency": {
            "oneOf": [
                {"type": "string"},
                {"type": "object", "required": ["activity"], "additionalProperties": False, "properties": {
                    "activity": {"type": "string"},
                    "conditions": {"type": "array", "items": {"enum": list(CONDITIONS)}, "minItems": 1, "uniqueItems": True},
                }},
            ],
        },
        "policy": {"type": "object", "additionalProperties": False, "properties": {
            "retries": {"type": "integer", "minimum": 0, "maximum": 10},
            "retryBackoffSeconds": {"type": "number", "minimum": 0, "maximum": 86400},
            "backoffMultiplier": {"type": "number", "minimum": 1, "maximum": 10},
            "maxBackoffSeconds": {"type": "number", "minimum": 0, "maximum": 86400},
            "timeoutSeconds": {"type": "integer", "minimum": 1, "maximum": 30 * 86400},
            "cancelGraceSeconds": {"type": "number", "minimum": 0, "maximum": 3600, "description": "After a timeout the attempt is cancelled and the engine waits this long (default 30) for it to stop before it concludes the attempt; "
                                   "an attempt that has not stopped by then ends as timeout_uncertain and is retried only when idempotent."},
            "idempotent": {"type": "boolean", "description": "Safe to execute again: after a control-plane crash, and after a timeout whose outcome is unknown (the attempt may have completed its work)."},
            "reusable": {"type": "boolean", "description": "A successful result may be reused when re-running from failure."},
            "secureInput": {"type": "boolean"}, "secureOutput": {"type": "boolean"},
        }},
        "parameter": {"type": "object", "required": ["type"], "additionalProperties": False, "properties": {
            "type": {"enum": list(PARAM_TYPES)}, "default": {}, "description": {"type": "string"},
        }},
        "variable": {"type": "object", "required": ["type"], "additionalProperties": False, "properties": {
            "type": {"enum": list(VAR_TYPES)}, "default": {}, "description": {"type": "string"},
        }},
        "activity_envelope": _envelope_schema(),
    }
    activity_ref: dict[str, Any] = {"$ref": "#/$defs/activity_envelope"}
    if include_types:
        all_of = []
        for a in registry.all():
            all_of.append({"if": {"properties": {"type": {"const": a.type}}, "required": ["type"]},
                           "then": {"properties": {"typeProperties": a.properties_schema}, **({"required": ["typeProperties"]} if a.properties_schema.get("required") else {})}})
        defs["activity_types"] = {"enum": registry.types()}
        defs["activity"] = {"allOf": [{"$ref": "#/$defs/activity_envelope"}, {"properties": {"type": {"$ref": "#/$defs/activity_types"}}}, *all_of]}
        activity_ref = {"$ref": "#/$defs/activity"}
    return {
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "$id": "https://olcf.dev/schemas/pipeline-v1.json",
        "title": "Open Lakehouse Compute Fabric pipeline definition (olcf.pipeline/1)",
        "description": "Author: Justin Dodrill. Container activities (if, switch, for_each, until) nest activity lists inside typeProperties; those nested "
                       "definitions are validated recursively with the same rules (plus the semantic checks documented in olcf.pipelines.model).",
        "type": "object", "required": ["activities"], "additionalProperties": False,
        "properties": {
            "schema": {"const": SCHEMA_ID}, "$schema": {"type": "string"},
            "description": {"type": "string", "maxLength": 4096},
            "parameters": {"type": "object", "propertyNames": {"pattern": r"^[A-Za-z_][A-Za-z0-9_]{0,63}$"}, "additionalProperties": {"$ref": "#/$defs/parameter"}},
            "variables": {"type": "object", "propertyNames": {"pattern": r"^[A-Za-z_][A-Za-z0-9_]{0,63}$"}, "additionalProperties": {"$ref": "#/$defs/variable"}},
            "settings": {"type": "object", "additionalProperties": False, "properties": {
                "concurrency": {"type": "integer", "minimum": 0, "maximum": 1000, "description": "Maximum simultaneous runs of this pipeline (0 = unlimited)."},
                "queuePolicy": {"enum": list(QUEUE_POLICIES), "description": "What to do with a run that exceeds `concurrency`."},
                "timeoutSeconds": {"type": "integer", "minimum": 1, "maximum": 90 * 86400},
                "failFast": {"type": "boolean", "description": "Cancel the rest of the run as soon as an activity fails without a failure handler."},
                "maxParallelActivities": {"type": "integer", "minimum": 1, "maximum": 64},
            }},
            "outputs": {"type": "object", "description": "Named run outputs; values may be expressions evaluated when the run ends."},
            "annotations": {"type": "array", "items": {"type": "string"}},
            "activities": {"type": "array", "maxItems": MAX_ACTIVITIES, "items": activity_ref},
        },
        "$defs": defs,
    }


# The caches are keyed by the object itself (weakly), never by ``id(obj)``: an id is unique only among *live* objects, so after a registry or an activity instance was
# collected a new one - of another activity type - could get the same id and be validated with the old one's schema.
_SCHEMA_CACHE: weakref.WeakKeyDictionary[ActivityRegistry, tuple[Draft202012Validator, Draft202012Validator]] = weakref.WeakKeyDictionary()
_CACHE_LOCK = threading.Lock()


def _validators(registry: ActivityRegistry) -> tuple[Draft202012Validator, Draft202012Validator]:
    with _CACHE_LOCK:
        hit = _SCHEMA_CACHE.get(registry)
    if hit is None:
        schema = build_schema(registry, include_types=False)
        top = Draft202012Validator(schema)
        env = Draft202012Validator({"$ref": "#/$defs/activity_envelope", "$defs": schema["$defs"]})
        hit = (top, env)
        with _CACHE_LOCK:
            hit = _SCHEMA_CACHE.setdefault(registry, hit)
    return hit


_PROPS_CACHE: weakref.WeakKeyDictionary[Activity, Draft202012Validator] = weakref.WeakKeyDictionary()
_EXPR_DEF = {"expression": {"type": "string", "pattern": r"^@(?!@)|@\{"}}


def _props_validator(impl: Activity) -> Draft202012Validator:
    """Validator for one activity type's ``typeProperties`` (the shared ``$defs`` are attached so ``X()`` expression alternatives resolve)."""
    with _CACHE_LOCK:
        v = _PROPS_CACHE.get(impl)
    if v is None:
        v = Draft202012Validator({**impl.properties_schema, "$defs": _EXPR_DEF})
        with _CACHE_LOCK:
            v = _PROPS_CACHE.setdefault(impl, v)
    return v


def schema_file_candidates() -> list[Path]:
    here = Path(__file__).resolve()
    return [here.parents[3] / "schemas" / "pipeline-v1.json", here.parents[1] / "schemas" / "pipeline-v1.json"]


def load_shipped_schema() -> dict[str, Any] | None:
    for p in schema_file_candidates():
        if p.is_file():
            return json.loads(p.read_text(encoding="utf-8"))
    return None


def _jpath(path: Any, base: str) -> str:
    out = base
    for part in path:
        out += f"[{part}]" if isinstance(part, int) else (f".{part}" if out else str(part))
    return out


def _describe(v: Any) -> str:
    t = {str: "string", bool: "boolean", int: "integer", float: "number", list: "array", dict: "object", type(None): "null"}.get(type(v), type(v).__name__)
    s = json.dumps(v, default=str)
    return f"{t} {s[:40]}{'…' if len(s) > 40 else ''}"


def _flatten_errors(err: JsonSchemaError, base: str) -> list[ValidationIssue]:
    """Translate a jsonschema error (and its sub-errors) into precise, human-readable issues."""
    path = _jpath(err.absolute_path, base)
    v = err.validator
    if v == "required":
        missing = re.findall(r"'([^']+)' is a required property", err.message)
        return [ValidationIssue(_jpath(list(err.absolute_path) + [m], base), f"missing required property '{m}'", "required") for m in missing] or [ValidationIssue(path, err.message, "required")]
    if v == "additionalProperties":
        extra = re.findall(r"'([^']+)'", err.message)
        allowed = sorted((err.schema.get("properties") or {}).keys()) if isinstance(err.schema, dict) else []
        return [ValidationIssue(_jpath(list(err.absolute_path) + [e], base), f"unknown property '{e}'" + (f" (allowed: {', '.join(allowed)})" if allowed else ""), "unknown_property") for e in extra] or [ValidationIssue(path, err.message, "unknown_property")]
    if v == "type":
        return [ValidationIssue(path, f"must be of type {err.validator_value if isinstance(err.validator_value, str) else ' or '.join(err.validator_value)} but got {_describe(err.instance)}", "type")]
    if v in ("anyOf", "oneOf"):
        branches = [c for c in (err.context or [])]
        is_expr = any(isinstance(b.schema, dict) and b.schema.get("pattern", "").startswith("^@") for b in branches)
        if is_expr:
            types = sorted({str(b.validator_value) for b in branches if b.validator == "type"})
            if not types:
                other = [b for b in branches if not (isinstance(b.schema, dict) and b.schema.get("pattern", "").startswith("^@"))]
                if other:
                    inner = _flatten_errors(other[0], base)
                    for it in inner:
                        it.message += " (or an expression string starting with '@')"
                    return inner
            return [ValidationIssue(path, f"must be {' or '.join(types) or 'a valid value'} or an expression string starting with '@', but got {_describe(err.instance)}", "type")]
        deepest = max(branches, key=lambda b: len(list(b.absolute_path)), default=None)
        return _flatten_errors(deepest, base) if deepest is not None else [ValidationIssue(path, err.message, "invalid")]
    if v == "enum":
        return [ValidationIssue(path, f"must be one of {', '.join(map(str, err.validator_value))} but got {_describe(err.instance)}", "enum")]
    if v == "const":
        return [ValidationIssue(path, f"must be {err.validator_value!r} but got {_describe(err.instance)}", "const")]
    if v in ("minimum", "maximum", "minLength", "maxLength", "minItems", "maxItems", "pattern", "uniqueItems", "propertyNames"):
        return [ValidationIssue(path, err.message.replace(json.dumps(err.instance, default=str), _describe(err.instance)) if len(json.dumps(err.instance, default=str)) > 60 else err.message, v)]
    return [ValidationIssue(path, err.message, "invalid")]


def _schema_issues(validator: Draft202012Validator, instance: Any, base: str) -> list[ValidationIssue]:
    out: list[ValidationIssue] = []
    for err in sorted(validator.iter_errors(instance), key=lambda e: [str(p) for p in e.absolute_path]):
        out.extend(_flatten_errors(err, base))
    return out


# ---------------------------------------------------------------------------------------------------------------------------------------- validation
@dataclass
class ValidationResult:
    """Result of validating a pipeline definition: the issues found, the normalised definition and the dependency graph."""
    issues: list[ValidationIssue] = field(default_factory=list)
    definition: dict[str, Any] = field(default_factory=dict)
    graph: dict[str, Any] = field(default_factory=dict)

    @property
    def errors(self) -> list[ValidationIssue]:
        return [i for i in self.issues if i.severity == "error"]

    @property
    def warnings(self) -> list[ValidationIssue]:
        return [i for i in self.issues if i.severity == "warning"]

    @property
    def ok(self) -> bool:
        return not self.errors

    def to_dict(self) -> dict[str, Any]:
        return {"valid": self.ok, "errors": [i.to_dict() for i in self.errors], "warnings": [i.to_dict() for i in self.warnings], "definition": self.definition, "graph": self.graph}

    def raise_if_invalid(self) -> None:
        if not self.ok:
            first = self.errors[0]
            raise ValidationFailed(f"invalid pipeline definition: {first.path}: {first.message}" + (f" (and {len(self.errors) - 1} more problem(s))" if len(self.errors) > 1 else ""),
                                   details={"errors": [i.to_dict() for i in self.errors], "warnings": [i.to_dict() for i in self.warnings]},
                                   remediation="POST the definition to /v1/pipelines/validate for the full list")


def normalize_dependencies(raw: Any) -> list[dict[str, Any]]:
    out = []
    for d in raw or []:
        if isinstance(d, str):
            out.append({"activity": d, "conditions": ["succeeded"]})
        elif isinstance(d, dict):
            conds = d.get("conditions") or d.get("dependencyConditions") or ["succeeded"]
            out.append({"activity": d.get("activity"), "conditions": [str(c).lower() for c in conds]})
    return out


def find_cycle(names: list[str], deps: dict[str, list[str]]) -> list[str] | None:
    """Return one dependency cycle ``[A, B, C, A]`` (activity -> its dependency -> ...) or ``None``."""
    state: dict[str, int] = {}
    stack: list[str] = []

    def dfs(n: str) -> list[str] | None:
        state[n] = 1
        stack.append(n)
        for m in deps.get(n, []):
            if m not in deps:
                continue
            if state.get(m) == 1:
                return stack[stack.index(m):] + [m]
            if state.get(m) is None:
                c = dfs(m)
                if c:
                    return c
        stack.pop()
        state[n] = 2
        return None

    for n in names:
        if state.get(n) is None:
            c = dfs(n)
            if c:
                return c
    return None


def closure(name: str, deps: dict[str, list[str]]) -> set[str]:
    seen: set[str] = set()
    todo = list(deps.get(name, []))
    while todo:
        n = todo.pop()
        if n in seen:
            continue
        seen.add(n)
        todo.extend(deps.get(n, []))
    return seen


class _Validator:
    def __init__(self, registry: ActivityRegistry, defn: dict[str, Any]) -> None:
        self.registry, self.defn = registry, defn
        self.issues: list[ValidationIssue] = []
        self.count = 0
        self.top_v, self.env_v = _validators(registry)
        self.params = defn.get("parameters") if isinstance(defn.get("parameters"), dict) else {}
        self.vars = defn.get("variables") if isinstance(defn.get("variables"), dict) else {}
        self.graph_nodes: list[dict[str, Any]] = []
        self.graph_edges: list[dict[str, Any]] = []

    def err(self, path: str, message: str, code: str = "invalid") -> None:
        self.issues.append(ValidationIssue(path, message, code))

    def warn(self, path: str, message: str, code: str = "warning") -> None:
        self.issues.append(ValidationIssue(path, message, code, "warning"))

    # -- top level
    def run(self) -> None:
        d = self.defn
        # structure of everything except the activity bodies (they are validated one by one so each gets its own precise path)
        top = {k: v for k, v in d.items() if k != "activities"}
        top["activities"] = []
        self.issues += [i for i in _schema_issues(self.top_v, top, "") if not i.path.startswith("activities")]
        if "activities" not in d:
            self.err("activities", "missing required property 'activities'", "required")
            return
        if not isinstance(d["activities"], list):
            self.err("activities", f"must be of type array but got {_describe(d['activities'])}", "type")
            return
        for name, spec in self.params.items():
            if isinstance(spec, dict) and "default" in spec and spec.get("type") in PARAM_TYPES:
                if is_expression_value(spec["default"]):
                    self.err(f"parameters.{name}.default", "parameter defaults must be literal values, not expressions", "expression_not_allowed")
                else:
                    try:
                        coerce_typed(spec["default"], spec["type"], name)
                    except ValueError as exc:
                        self.err(f"parameters.{name}.default", str(exc), "type")
        for name, spec in self.vars.items():
            if isinstance(spec, dict) and "default" in spec and spec.get("type") in VAR_TYPES:
                try:
                    coerce_typed(spec["default"], spec["type"], name, what="variable")
                except ValueError as exc:
                    self.err(f"variables.{name}.default", str(exc), "type")
        self.scope(d["activities"], "activities", outer=set(), in_foreach=False, parallel=False, depth=1, parent_id=None)
        if isinstance(d.get("outputs"), dict):
            root_names = {a.get("name") for a in d["activities"] if isinstance(a, dict)}
            for k, v in d["outputs"].items():
                self.check_expressions(v, f"outputs.{k}", root_names, "outputs", in_foreach=False)

    # -- one scope
    def scope(self, acts: list[Any], base: str, *, outer: set[str], in_foreach: bool, parallel: bool, depth: int, parent_id: str | None) -> None:
        if depth > MAX_DEPTH:
            self.err(base, f"activities are nested more than {MAX_DEPTH} levels deep", "too_deep")
            return
        names: list[str] = []
        by_name: dict[str, dict[str, Any]] = {}
        index_of: dict[str, int] = {}
        for i, a in enumerate(acts):
            p = f"{base}[{i}]"
            if not isinstance(a, dict):
                self.err(p, f"must be of type object but got {_describe(a)}", "type")
                continue
            self.count += 1
            if self.count > MAX_ACTIVITIES:
                if self.count == MAX_ACTIVITIES + 1:
                    self.err(p, f"a pipeline may contain at most {MAX_ACTIVITIES} activities (including nested ones)", "too_many")
                continue
            self.issues += [i2 for i2 in _schema_issues(self.env_v, {k: v for k, v in a.items()}, p)]
            nm = a.get("name")
            if not isinstance(nm, str):
                continue
            if nm in by_name:
                self.err(f"{p}.name", f"duplicate activity name '{nm}' in this scope (first used at {base}[{index_of[nm]}])", "duplicate_name")
                continue
            names.append(nm)
            by_name[nm] = a
            index_of[nm] = i
        # dependencies ----------------------------------------------------------------------------------------------------
        deps: dict[str, list[str]] = {}
        for i, a in enumerate(acts):
            nm = a.get("name") if isinstance(a, dict) else None
            if not isinstance(nm, str) or by_name.get(nm) is not a:
                continue
            p = f"{base}[{i}]"
            lst = normalize_dependencies(a.get("dependsOn"))
            deps[nm] = []
            seen: set[str] = set()
            for j, dep in enumerate(lst):
                dp = f"{p}.dependsOn[{j}]"
                target = dep["activity"]
                if not isinstance(target, str):
                    self.err(dp, "dependency needs an 'activity' name", "bad_dependency")
                    continue
                if target == nm:
                    self.err(dp, f"activity '{nm}' cannot depend on itself", "self_dependency")
                    continue
                if target not in by_name:
                    near = [n for n in by_name if n.lower() == target.lower()]
                    self.err(dp, f"unknown activity '{target}' in dependsOn" + (f" (did you mean '{near[0]}'?)" if near else f" (activities in this scope: {', '.join(by_name) or 'none'})") +
                             ("; dependencies must be siblings in the same scope" if target in outer else ""), "unknown_dependency")
                    continue
                if target in seen:
                    self.err(dp, f"duplicate dependency on '{target}'", "duplicate_dependency")
                    continue
                seen.add(target)
                bad = [c for c in dep["conditions"] if c not in CONDITIONS]
                if bad:
                    self.err(dp, f"invalid dependency condition(s) {bad}; allowed: {', '.join(CONDITIONS)}", "bad_condition")
                    continue
                deps[nm].append(target)
        cyc = find_cycle(names, deps)
        if cyc:
            first = index_of.get(cyc[0], 0)
            self.err(f"{base}[{first}].dependsOn", "dependency cycle: " + " -> ".join(cyc) + " (each activity depends on the next; break the cycle)", "cycle")
        # per activity ----------------------------------------------------------------------------------------------------
        for i, a in enumerate(acts):
            nm = a.get("name") if isinstance(a, dict) else None
            if not isinstance(nm, str) or by_name.get(nm) is not a:
                continue
            p = f"{base}[{i}]"
            node_id = (parent_id + "/" if parent_id else "") + nm
            self.graph_nodes.append({"id": node_id, "name": nm, "type": a.get("type"), "parent": parent_id, "scope": base})
            for dep in normalize_dependencies(a.get("dependsOn")):
                if dep["activity"] in by_name and dep["activity"] != nm:
                    self.graph_edges.append({"from": (parent_id + "/" if parent_id else "") + dep["activity"], "to": node_id, "conditions": dep["conditions"]})
            visible = (closure(nm, deps) if not cyc else set(deps.get(nm, []))) | outer
            self.activity(a, p, nm, visible, set(by_name), in_foreach=in_foreach, parallel=parallel, depth=depth, node_id=node_id, deps=closure(nm, deps) if not cyc else set(deps.get(nm, [])), outer=outer)

    def activity(self, a: dict[str, Any], p: str, nm: str, visible: set[str], siblings: set[str], *, in_foreach: bool, parallel: bool, depth: int, node_id: str,
                 deps: set[str], outer: set[str]) -> None:
        typ = a.get("type")
        impl = self.registry.get(typ) if isinstance(typ, str) else None
        if impl is None:
            if isinstance(typ, str):
                self.err(f"{p}.type", f"unknown activity type '{typ}' (available: {', '.join(self.registry.types())})", "unknown_activity_type")
            return
        props = a.get("typeProperties") if isinstance(a.get("typeProperties"), dict) else {}
        if a.get("typeProperties") is None and impl.properties_schema.get("required"):
            self.err(f"{p}.typeProperties", f"activity type '{typ}' needs typeProperties with: {', '.join(impl.properties_schema['required'])}", "required")
        self.issues += _schema_issues(_props_validator(impl), props, f"{p}.typeProperties")
        if isinstance(a.get("policy"), dict):
            pol = a["policy"]
            if pol.get("maxBackoffSeconds") is not None and pol.get("retryBackoffSeconds") is not None and pol["maxBackoffSeconds"] < pol["retryBackoffSeconds"]:
                self.err(f"{p}.policy.maxBackoffSeconds", "maxBackoffSeconds must be >= retryBackoffSeconds", "range")
        try:
            self.issues += [ValidationIssue(i.path, i.message, i.code, i.severity) for i in impl.validate(props, f"{p}.typeProperties")]
        except Exception as exc:  # noqa: BLE001 - a plugin's validate() must never break validation of the rest
            self.err(f"{p}.typeProperties", f"validation of activity type '{typ}' crashed: {exc}", "plugin_error")
        # expressions
        container = impl if isinstance(impl, Container) else None
        lazy_visible = set(visible)
        inner_names: set[str] = set()
        if container is not None:
            for _key, inner in container.scopes(props):
                inner_names |= {x.get("name") for x in inner if isinstance(x, dict) and isinstance(x.get("name"), str)}
            lazy_visible |= inner_names
        for k, val in props.items():
            if container is not None and k in container.scope_keys:
                if k == "cases":  # switch: values are expressions, nested activities are validated as scopes
                    for ci, case in enumerate(val if isinstance(val, list) else []):
                        if isinstance(case, dict):
                            self.check_expressions(case.get("value"), f"{p}.typeProperties.cases[{ci}].value", visible, nm, in_foreach=in_foreach)
                continue
            vis = lazy_visible if (container is not None and k in container.lazy_keys) else visible
            self.check_expressions(val, f"{p}.typeProperties.{k}", vis, nm, in_foreach=in_foreach)
        # set_variable specifics
        if typ == "set_variable":
            vname = props.get("variable")
            if isinstance(vname, str) and not is_expression_value(vname):
                if vname not in self.vars:
                    self.err(f"{p}.typeProperties.variable", f"variable '{vname}' is not declared (declared: {', '.join(self.vars) or 'none'})", "unknown_variable")
                else:
                    spec = self.vars[vname]
                    val = props.get("value")
                    if val is not None and not is_expression_value(val) and isinstance(spec, dict) and spec.get("type") in VAR_TYPES:
                        try:
                            coerce_typed(val, spec["type"], vname, what="variable")
                        except ValueError as exc:
                            self.err(f"{p}.typeProperties.value", str(exc), "type")
                if parallel:
                    self.err(f"{p}.typeProperties.variable", f"set_variable '{nm}' runs inside a parallel for_each; concurrent writers of pipeline variable '{vname}' would race "
                             "(set isSequential=true or batchCount=1)", "unsafe_parallel_variable")
        # containers: recurse
        if container is not None:
            for key, inner in container.scopes(props):
                if not isinstance(inner, list):
                    continue
                fe = in_foreach or typ == "for_each"
                par = parallel or (typ == "for_each" and not props.get("isSequential") and (props.get("batchCount") is None or (isinstance(props.get("batchCount"), int) and props["batchCount"] > 1)))
                self.scope(inner, f"{p}.typeProperties.{key}", outer=visible | siblings & deps, in_foreach=fe, parallel=par, depth=depth + 1, parent_id=node_id)

    def check_expressions(self, value: Any, path: str, visible: set[str], me: str, *, in_foreach: bool) -> None:
        def walk(v: Any, pth: str) -> None:
            if isinstance(v, str) and (v.startswith("@") or "@{" in v):
                try:
                    refs = analyze_value(v)
                except ExpressionError as exc:
                    self.err(pth, f"invalid expression: {exc.render()}", "expression_syntax")
                    return
                for act in sorted(refs.activities):
                    if act not in visible:
                        self.err(pth, f"expression references activity '{act}' which is {'not upstream of' if act in self._all_names() else 'unknown to'} '{me}'" +
                                 (f"; add a dependsOn entry for '{act}'" if act in self._all_names() else f" (known activities: {', '.join(sorted(self._all_names())) or 'none'})"), "bad_activity_reference")
                for var in sorted(refs.variables):
                    if var not in self.vars:
                        self.err(pth, f"expression references undeclared variable '{var}' (declared: {', '.join(self.vars) or 'none'})", "unknown_variable")
                for prm in sorted(refs.parameters):
                    if prm not in self.params:
                        self.err(pth, f"expression references undeclared parameter '{prm}' (declared: {', '.join(self.params) or 'none'})", "unknown_parameter")
                if refs.uses_item and not in_foreach:
                    self.err(pth, "item() / itemIndex() can only be used inside a for_each activity", "item_outside_foreach")
            elif isinstance(v, dict):
                if is_expression_value(v):
                    walk("@" + v["value"] if not v["value"].startswith("@") else v["value"], pth)
                else:
                    for k, x in v.items():
                        walk(x, f"{pth}.{k}")
            elif isinstance(v, list):
                for i, x in enumerate(v):
                    walk(x, f"{pth}[{i}]")

        walk(value, path)

    def _all_names(self) -> set[str]:
        if not hasattr(self, "_names_cache"):
            names: set[str] = set()

            def collect(acts: Any) -> None:
                for a in acts if isinstance(acts, list) else []:
                    if isinstance(a, dict) and isinstance(a.get("name"), str):
                        names.add(a["name"])
                        impl = self.registry.get(a.get("type")) if isinstance(a.get("type"), str) else None
                        if isinstance(impl, Container) and isinstance(a.get("typeProperties"), dict):
                            for _k, inner in impl.scopes(a["typeProperties"]):
                                collect(inner)

            collect(self.defn.get("activities"))
            self._names_cache = names
        return self._names_cache


def normalize_definition(defn: dict[str, Any], registry: ActivityRegistry) -> dict[str, Any]:
    """Fill structural defaults (schema id, empty maps, normalized dependsOn) without changing semantics. Does not validate."""
    d = copy.deepcopy(defn)
    d.setdefault("schema", SCHEMA_ID)
    d.setdefault("parameters", {})
    d.setdefault("variables", {})
    d.setdefault("settings", {})

    def norm(acts: Any) -> None:
        for a in acts if isinstance(acts, list) else []:
            if not isinstance(a, dict):
                continue
            a["dependsOn"] = normalize_dependencies(a.get("dependsOn"))
            a.setdefault("typeProperties", {})
            a.setdefault("policy", {})
            impl = registry.get(a.get("type")) if isinstance(a.get("type"), str) else None
            if isinstance(impl, Container) and isinstance(a["typeProperties"], dict):
                for _k, inner in impl.scopes(a["typeProperties"]):
                    norm(inner)

    norm(d.get("activities"))
    return d


def validate_definition(defn: Any, registry: ActivityRegistry) -> ValidationResult:
    """Validate a definition (already parsed). Never raises for user errors - inspect ``result.errors``."""
    if not isinstance(defn, dict):
        return ValidationResult([ValidationIssue("", "a pipeline definition must be an object at the top level", "type")], {}, {})
    if len(canonical_json(defn)) > MAX_DEFINITION_BYTES:
        return ValidationResult([ValidationIssue("", f"definition is larger than {MAX_DEFINITION_BYTES // 1024 // 1024} MiB", "too_large")], {}, {})
    norm = normalize_definition(defn, registry)
    v = _Validator(registry, norm)
    try:
        v.run()
    except RecursionError:
        v.err("", "definition is nested too deeply", "too_deep")
    return ValidationResult(v.issues, norm, {"nodes": v.graph_nodes, "edges": v.graph_edges})


def activity_defs_index(defn: dict[str, Any], registry: ActivityRegistry) -> dict[tuple[str, ...], dict[str, Any]]:
    """Map ``(container name, ..., activity name)`` paths to activity definitions (used for hashing and display)."""
    out: dict[tuple[str, ...], dict[str, Any]] = {}

    def walk(acts: list[dict[str, Any]], prefix: tuple[str, ...]) -> None:
        for a in acts:
            key = (*prefix, a["name"])
            out[key] = a
            impl = registry.get(a["type"])
            if isinstance(impl, Container):
                for _k, inner in impl.scopes(a.get("typeProperties") or {}):
                    walk(inner, key)

    walk(defn.get("activities") or [], ())
    return out


def activity_hash(a: dict[str, Any]) -> str:
    return sha256_of(a)


def retry_policy(a: dict[str, Any], impl: Activity) -> dict[str, Any]:
    pol = a.get("policy") or {}
    return {
        "retries": int(pol.get("retries", 0)), "backoff": float(pol.get("retryBackoffSeconds", 5)), "multiplier": float(pol.get("backoffMultiplier", 2)),
        "max_backoff": float(pol.get("maxBackoffSeconds", 3600)), "timeout": pol.get("timeoutSeconds", impl.default_timeout_seconds),
        "timeout_explicit": "timeoutSeconds" in pol, "cancel_grace": float(pol["cancelGraceSeconds"]) if pol.get("cancelGraceSeconds") is not None else None,
        "idempotent": bool(pol.get("idempotent", False)), "reusable": bool(pol.get("reusable", True)) and impl.reusable,
    }
