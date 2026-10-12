# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Helpers shared by the two pipeline analyzers (:mod:`olcf.intelligence.lineage.pipeline_olcf` and :mod:`olcf.intelligence.lineage.pipeline_fabric`).

* :func:`resolve_template` turns a pipeline expression that is only a parameter reference (``@pipeline().parameters.day``) or a string with ``@{pipeline().parameters.x}``
  interpolations into text using the parameters' default values; everything else it cannot evaluate becomes an unknown-marker so that the SQL analyzer reports a dynamic
  reference instead of guessing. Values that came from a parameter default are returned as *taint* so that the edges derived from them are only ``inferred``.
* :func:`activity_ref` builds the reference of an activity asset.
* :func:`item_ref` builds the reference of the item an activity executes (a notebook, a Spark job, a child pipeline).
* :class:`Walker` collects the activities of a pipeline (including nested containers) with their JSON paths and the ``dependsOn`` structure.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Any

from olcf.intelligence.lineage.base import LineageContext
from olcf.intelligence.model import AssetRef

_UNKNOWN = "__olcf_unknown_"
_PARAM = re.compile(r"pipeline\(\)\.parameters\.(\w+)", re.IGNORECASE)
_PARAM_ONLY = re.compile(r"^@\{?\s*pipeline\(\)\.parameters\.(\w+)\s*\}?$", re.IGNORECASE)
_INTERP = re.compile(r"@\{(.*?)\}", re.DOTALL)


def as_dict(x: Any) -> dict[str, Any]:
    """``x`` when it is a dict, else an empty dict (definition JSON is untrusted)."""
    return x if isinstance(x, dict) else {}


@dataclass
class Template:
    """Result of :func:`resolve_template`: the ``text`` (unknown parts replaced by unknown-markers), whether ``all_known``, and the parameter default values used (``taint``)."""

    text: str
    all_known: bool
    taint: tuple[str, ...] = ()


def default_of(params: dict[str, Any], name: str) -> str | None:
    """Default value of pipeline parameter ``name`` when it is a non-secret string / number."""
    for k, spec in params.items():
        if k.lower() != name.lower():
            continue
        v = spec.get("defaultValue", spec.get("default")) if isinstance(spec, dict) else spec
        if isinstance(spec, dict) and str(spec.get("type", "")).lower() in ("secret_ref", "securestring"):
            return None
        if isinstance(v, (str, int, float)) and not isinstance(v, bool):
            return str(v)
    return None


def resolve_template(value: Any, params: dict[str, Any], counter: list[int] | None = None) -> Template:
    """Resolve ``value`` (a pipeline property) to text using parameter defaults. Non-strings give an unknown template."""
    cnt = counter if counter is not None else [0]

    def unknown() -> str:
        cnt[0] += 1
        return f"{_UNKNOWN}{cnt[0]}__"

    if isinstance(value, dict) and "value" in value and str(value.get("type", "Expression")).lower() == "expression":
        value = value["value"]
    if not isinstance(value, str):
        return Template(unknown(), False)
    if not value.startswith("@") and "@{" not in value:
        return Template(value, True)
    m = _PARAM_ONLY.match(value.strip())
    if m:
        d = default_of(params, m.group(1))
        return Template(d, True, (d,)) if d is not None else Template(unknown(), False)
    if value.startswith("@") and not value.startswith("@{"):
        return Template(unknown(), False)
    taint: list[str] = []
    ok = True

    def repl(match: re.Match[str]) -> str:
        nonlocal ok
        inner = match.group(1).strip()
        pm = _PARAM_ONLY.match("@" + inner)
        if pm:
            d = default_of(params, pm.group(1))
            if d is not None:
                taint.append(d)
                return d
        ok = False
        return unknown()

    out = _INTERP.sub(repl, value)
    return Template(out, ok, tuple(taint))


def activity_ref(ctx: LineageContext, pipeline_key: str, pipeline_name: str, chain: list[str]) -> AssetRef:
    """The reference of an activity asset: named ``<pipeline>/<outer>/<inner>`` and aliased by ``source`` so that re-analysis finds the same asset."""
    path = "/".join(chain)
    return AssetRef.make("activity", aliases={"source": f"{ctx.source_id or ctx.source}:{pipeline_key}/{path}"}, name=f"{pipeline_name}/{path}", scope=ctx.scope())


def item_ref(kind: str, value: str, ctx: LineageContext, known: dict[str, str] | None = None) -> AssetRef:
    """Reference to an item named or identified by ``value`` (OLCF item ids start with ``itm_``)."""
    if value.startswith("itm_"):
        return AssetRef.make(kind, aliases={"olcf_item": value}, scope=ctx.scope())
    if known and value in known:
        return AssetRef.make(kind, aliases={"olcf_item": known[value]}, name=value, scope=ctx.scope())
    return AssetRef.make(kind, name=value, scope=ctx.scope())


@dataclass
class Node:
    """An activity found in a pipeline definition: its JSON ``path``, the ``chain`` of activity names from the pipeline down to it, and the raw dict."""

    name: str
    path: str
    chain: list[str]
    raw: dict[str, Any]
    parent: Node | None = None
    children: list[Node] = field(default_factory=list)


MAX_ACTIVITIES = 5000
MAX_NESTING = 12


def walk(activities: Any, base_path: str, parent: Node | None, out: list[Node], gaps: list[tuple[str, str, str]], counter: list[int], depth: int = 0) -> list[Node]:
    """Collect the activities of one list (and, recursively, of containers) into ``out``; ``gaps`` receives ``(kind, subject, detail)`` for malformed input. Returns this list's nodes."""
    nodes: list[Node] = []
    if not isinstance(activities, list):
        if activities is not None:
            gaps.append(("parse_error", base_path, "the activity list is not an array"))
        return nodes
    for i, a in enumerate(activities):
        path = f"{base_path}[{i}]"
        if not isinstance(a, dict):
            gaps.append(("parse_error", path, "the activity is not an object"))
            continue
        counter[0] += 1
        if counter[0] > MAX_ACTIVITIES:
            gaps.append(("unsupported_construct", "pipeline", f"more than {MAX_ACTIVITIES} activities: the rest was not analysed"))
            return nodes
        name = str(a.get("name") or f"activity_{counter[0]}")
        chain = [*(parent.chain if parent else []), name]
        node = Node(name, path, chain, a, parent)
        out.append(node)
        nodes.append(node)
        if parent is not None:
            parent.children.append(node)
        if depth + 1 >= MAX_NESTING:
            if _nested_lists(a):
                gaps.append(("unsupported_construct", path, f"activities nested deeper than {MAX_NESTING} levels were not analysed"))
            continue
        for key, lst in _nested_lists(a):
            walk(lst, f"{path}.{key}", node, out, gaps, counter, depth + 1)
    return nodes


def _nested_lists(a: dict[str, Any]) -> list[tuple[str, Any]]:
    """The nested activity lists of a container activity as ``(json path suffix, list)``: ``typeProperties.ifTrueActivities`` ..., Fabric and OLCF spellings."""
    tp = as_dict(a.get("typeProperties"))
    out: list[tuple[str, Any]] = []
    for key in ("activities", "ifTrueActivities", "ifFalseActivities", "defaultActivities"):
        if isinstance(tp.get(key), list):
            out.append((f"typeProperties.{key}", tp[key]))
    cases = tp.get("cases")
    if isinstance(cases, list):
        for ci, case in enumerate(cases):
            if isinstance(case, dict) and isinstance(case.get("activities"), list):
                out.append((f"typeProperties.cases[{ci}].activities", case["activities"]))
    return out


def dependencies(node: Node) -> list[tuple[str, list[str]]]:
    """``(upstream activity name, conditions)`` of an activity (``dependsOn`` entries may be plain names or objects with ``dependencyConditions`` / ``conditions``)."""
    out: list[tuple[str, list[str]]] = []
    deps = node.raw.get("dependsOn")
    if not isinstance(deps, list):
        return out
    for d in deps:
        if isinstance(d, str):
            out.append((d, ["succeeded"]))
        elif isinstance(d, dict) and isinstance(d.get("activity"), str):
            conds = d.get("conditions", d.get("dependencyConditions"))
            out.append((d["activity"], [str(c).lower() for c in conds] if isinstance(conds, list) else ["succeeded"]))
    return out


def emit_structure(b: Any, ctx: LineageContext, analyzer: str, pipeline: AssetRef, pipeline_key: str, pipeline_name: str, nodes: list[Node],
                   type_of: Any) -> dict[str, AssetRef]:
    """Add the activity assets, the ``CONTAINS`` edges (pipeline or container -> activity) and the ``DEPENDS_ON`` edges (activity -> upstream activity).

    ``type_of(node) -> str`` gives the activity type to store in the asset attributes. Returns ``{json path: activity ref}``. A dependency on a name that does not exist
    among the siblings is an ``unresolved_reference`` gap.
    """
    from olcf.intelligence.model import AssetFact

    refs: dict[str, AssetRef] = {}
    seen: dict[tuple[str, ...], str] = {}
    for n in nodes:
        ref = activity_ref(ctx, pipeline_key, pipeline_name, n.chain)
        refs[n.path] = ref
        chain_key = tuple(c.lower() for c in n.chain)
        if chain_key in seen:
            b.gap("parse_error", n.name, f"two activities are named '{n.name}' ({seen[chain_key]} and {n.path}); they cannot be told apart, so the edges of both are attributed to one activity",
                  "Give every activity of a list a unique name.", ctx.loc(path=n.path, activity=n.name), analyzer)
        seen.setdefault(chain_key, n.path)
        b.asset(AssetFact(ref=ref, name=n.name, attrs={"activity_type": type_of(n), "path": n.path, "pipeline": pipeline_name}, workspace_id=ctx.workspace_id))
    for n in nodes:
        parent_ref = refs[n.parent.path] if n.parent is not None else pipeline
        loc = ctx.loc(path=n.path, activity=n.name)
        b.edge(parent_ref, refs[n.path], "CONTAINS", confidence="derived", kind="definition", location=loc, snippet=n.name, analyzer=analyzer)
        siblings = {s.name.lower(): s for s in (n.parent.children if n.parent is not None else [x for x in nodes if x.parent is None])}
        for dep_name, conds in dependencies(n):
            up = siblings.get(dep_name.lower())
            if up is None or up is n:
                b.gap("unresolved_reference", dep_name, f"activity '{n.name}' depends on '{dep_name}', which is not an activity of the same list", "Fix the dependsOn entry.",
                      ctx.loc(path=n.path + ".dependsOn", activity=n.name), analyzer)
                continue
            b.edge(refs[n.path], refs[up.path], "DEPENDS_ON", confidence="derived", kind="definition", location=ctx.loc(path=n.path + ".dependsOn", activity=n.name),
                   snippet=f"{n.name} after {dep_name}", attrs={"conditions": conds}, analyzer=analyzer)
    return refs
