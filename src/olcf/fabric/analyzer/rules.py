# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Loader and matcher for the declarative compatibility rule file(s) in ``rules/``.

The rule file is data, not code: classification (``native-portable`` ... ``unsupported``), messages and remediation can be reviewed and
versioned without reading the detectors. :class:`RuleSet` validates the document on load (fail closed on a malformed rule file), indexes
the matchers and answers the questions the detectors ask:

* :meth:`RuleSet.rule` - a rule by id;
* :meth:`RuleSet.match_call` - dotted ``notebookutils`` chains (exact / glob; container prefixes are recognised so ``fs = notebookutils.fs``
  is not flagged, while a chain that matches nothing is *unknown* and therefore blocked);
* :meth:`RuleSet.match_import`, :meth:`match_conf`, :meth:`match_magic`, :meth:`match_api`.
"""

from __future__ import annotations

import fnmatch
import functools
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import jsonschema
import yaml

from olcf.version import COMPAT_RULESET_VERSION

RULES_DIR = Path(__file__).resolve().parent / "rules"
DEFAULT_RULE_FILE = "fabric-2.0.yaml"
CLASSES = ("native-portable", "compatibility-shim", "deterministic-rewrite", "warning", "fabric-only-fallback", "unsupported")
SEVERITY = {"native-portable": "info", "compatibility-shim": "info", "deterministic-rewrite": "info", "fabric-only-fallback": "info",
            "warning": "warning", "unsupported": "error"}

_SCHEMA = {
    "type": "object",
    "required": ["ruleset_version", "classes", "rules"],
    "properties": {
        "ruleset_version": {"type": "string"},
        "runtime_families": {"type": "array", "items": {"type": "string"}},
        "classes": {"type": "array", "items": {"enum": list(CLASSES)}},
        "rules": {"type": "array", "items": {
            "type": "object", "required": ["id", "title", "class", "message"], "additionalProperties": False,
            "properties": {
                "id": {"type": "string", "pattern": r"^FAB-[A-Z0-9-]+$"}, "title": {"type": "string"}, "class": {"enum": list(CLASSES)},
                "message": {"type": "string"}, "remediation": {"type": "string"}, "shim": {"type": "string"},
                "call": {"type": "array", "items": {"type": "string"}}, "import": {"type": "array", "items": {"type": "string"}},
                "conf": {"type": "array", "items": {"type": "string"}}, "api": {"type": "array", "items": {"type": "string"}},
                "magic": {"type": "object", "required": ["kind", "names"], "properties": {"kind": {"enum": ["cell", "line", "any"]}, "names": {"type": "array", "items": {"type": "string"}}}},
            }}},
    },
}


@dataclass(frozen=True)
class Rule:
    """One analyzer rule: id, title, classification, message and optional remediation text."""
    id: str
    title: str
    cls: str
    message: str
    remediation: str | None = None
    shim: str | None = None

    @property
    def severity(self) -> str:
        return SEVERITY[self.cls]

    def to_dict(self) -> dict[str, Any]:
        return {"id": self.id, "title": self.title, "class": self.cls, "severity": self.severity, "message": self.message, "remediation": self.remediation, "shim": self.shim}


@dataclass
class RuleSet:
    """The loaded set of analyzer rules: version, rule families, the rules by id and the call and import patterns that trigger them."""
    version: str
    families: list[str]
    rules: dict[str, Rule]
    calls: list[tuple[str, str]] = field(default_factory=list)  # (glob, rule id), declaration order
    imports: list[tuple[str, str]] = field(default_factory=list)
    confs: list[tuple[str, str]] = field(default_factory=list)
    magics: list[tuple[str, str, str]] = field(default_factory=list)  # (kind, name, rule id)
    apis: dict[str, str] = field(default_factory=dict)
    raw: dict[str, Any] = field(default_factory=dict)

    def rule(self, rule_id: str) -> Rule:
        return self.rules[rule_id]

    # ------------------------------------------------------------------ matchers
    def match_call(self, dotted: str) -> tuple[str, Rule | None]:
        """Classify a ``notebookutils`` chain: ``("rule", Rule)``, ``("container", None)`` or ``("unknown", Rule)`` (fail closed)."""
        for pat, rid in self.calls:
            if fnmatch.fnmatchcase(dotted, pat):
                return "rule", self.rules[rid]
        # a strict prefix of a known API (notebookutils.fs, notebookutils.notebook) is just a namespace
        prefix = dotted + "."
        if any(pat.startswith(prefix) for pat, _ in self.calls):
            return "container", None
        # a longer chain below a matched API (notebookutils.runtime.context.get) is covered by that API's rule
        parts = dotted.split(".")
        for n in range(len(parts) - 1, 1, -1):
            head = ".".join(parts[:n])
            for pat, rid in self.calls:
                if fnmatch.fnmatchcase(head, pat) and self.rules[rid].cls != "unsupported":
                    return "rule", self.rules[rid]
        return "unknown", self.rules["FAB-NBU-UNKNOWN"]

    def match_import(self, module: str) -> Rule | None:
        for pat, rid in self.imports:
            if fnmatch.fnmatchcase(module, pat):
                return self.rules[rid]
        return None

    def match_conf(self, key: str) -> Rule | None:
        for pat, rid in self.confs:
            if fnmatch.fnmatchcase(key, pat):
                return self.rules[rid]
        return None

    def match_magic(self, kind: str, name: str) -> Rule | None:
        for k, n, rid in self.magics:
            if n == name and k in (kind, "any"):
                return self.rules[rid]
        return None

    def match_api(self, name: str) -> Rule | None:
        rid = self.apis.get(name)
        return self.rules[rid] if rid else None

    def describe(self) -> dict[str, Any]:
        return {"ruleset_version": self.version, "runtime_families": self.families, "classes": list(CLASSES),
                "rules": [dict(r.to_dict(), matchers=self._matchers_of(r.id)) for r in self.rules.values()]}

    def _matchers_of(self, rid: str) -> dict[str, Any]:
        out: dict[str, Any] = {}
        for key, items in (("call", [p for p, r in self.calls if r == rid]), ("import", [p for p, r in self.imports if r == rid]),
                           ("conf", [p for p, r in self.confs if r == rid]), ("magic", [f"{k}:{n}" for k, n, r in self.magics if r == rid]),
                           ("api", [a for a, r in self.apis.items() if r == rid])):
            if items:
                out[key] = items
        return out


def parse_rules(doc: dict[str, Any]) -> RuleSet:
    jsonschema.validate(doc, _SCHEMA)
    rs = RuleSet(version=str(doc["ruleset_version"]), families=list(doc.get("runtime_families") or []), rules={}, raw=doc)
    for r in doc["rules"]:
        if r["id"] in rs.rules:
            raise ValueError(f"duplicate rule id {r['id']}")
        rs.rules[r["id"]] = Rule(r["id"], r["title"], r["class"], r["message"], r.get("remediation"), r.get("shim"))
        for pat in r.get("call", []):
            rs.calls.append((pat, r["id"]))
        for pat in r.get("import", []):
            rs.imports.append((pat, r["id"]))
        for pat in r.get("conf", []):
            rs.confs.append((pat, r["id"]))
        for name in (r.get("magic") or {}).get("names", []):
            rs.magics.append(((r["magic"]["kind"]), name, r["id"]))
        for name in r.get("api", []):
            rs.apis[name] = r["id"]
    for needed in ("FAB-NBU-UNKNOWN", "FAB-IMP-THIRDPARTY", "FAB-MAGIC-UNKNOWN", "FAB-PATH-REL", "FAB-CONF-OSS"):
        if needed not in rs.rules:
            raise ValueError(f"rule file lacks the mandatory rule {needed}")
    # longest, most specific call patterns first so an exact entry wins over a glob
    rs.calls.sort(key=lambda t: (t[0].count("*"), -len(t[0])))
    return rs


@functools.lru_cache(maxsize=4)
def load_ruleset(name: str = DEFAULT_RULE_FILE) -> RuleSet:
    path = (RULES_DIR / name).resolve()
    if RULES_DIR.resolve() not in path.parents:
        raise ValueError("rule file must live in the rules directory")
    rs = parse_rules(yaml.safe_load(path.read_text(encoding="utf-8")))
    if name == DEFAULT_RULE_FILE and rs.version != COMPAT_RULESET_VERSION:
        raise ValueError(f"rule file version {rs.version} does not match olcf.version.COMPAT_RULESET_VERSION {COMPAT_RULESET_VERSION}")
    return rs
