#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Generate ``docs/reference/api.md`` from the live OpenAPI document of the control plane.

The script builds the real application (``olcf.api.app.create_app``) against a throw-away data
directory, asks FastAPI for the OpenAPI document that is served at ``/v1/openapi.json`` and renders
it as Markdown grouped by tag.  It additionally scans the source of every route handler for
``principal.require("<permission>")`` / ``principal.can("<permission>")`` calls so the reference can
list the permissions a call checks.  That scan is *static*: permissions checked by helper functions
that the handler calls are not visible to it, and the page says so.

Usage::

    PYTHONPATH=src python scripts/gen_api_reference.py            # writes docs/reference/api.md
    PYTHONPATH=src python scripts/gen_api_reference.py --check    # exit 1 when the file is stale
    PYTHONPATH=src python scripts/gen_api_reference.py --json openapi.json   # also dump the raw document
"""

from __future__ import annotations

import argparse
import inspect
import json
import logging
import re
import sys
import tempfile
from collections import defaultdict
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parent.parent
OUT = ROOT / "docs" / "reference" / "api.md"
_PERM_RE = re.compile(r"""\.(?:require|can)\(\s*["']([a-z_]+(?:\.[a-z_:]+)+)["']""")
_HTTP_METHODS = ("get", "post", "put", "patch", "delete", "head", "options")


def iter_routes(routes: Any) -> Any:
    """Yield ``(methods, path, endpoint)`` for every route, flattening lazily included routers.

    Recent FastAPI releases keep ``include_router`` results as ``_IncludedRouter`` objects instead of
    copying the routes, so the walker understands both layouts without relying on a private import.
    """
    for r in routes:
        if hasattr(r, "effective_candidates"):
            yield from iter_routes(list(r.effective_candidates()) + list(getattr(r, "effective_low_priority_routes", lambda: [])()))
        elif hasattr(r, "original_route"):  # effective route context
            orig = r.original_route
            methods = getattr(orig, "methods", None)
            if methods and (r.endpoint or getattr(orig, "endpoint", None)):
                yield methods, r.path, r.endpoint or orig.endpoint
        elif getattr(r, "methods", None) and getattr(r, "endpoint", None):
            yield r.methods, r.path, r.endpoint


def build_spec() -> tuple[dict[str, Any], dict[tuple[str, str], list[str]]]:
    """Return the OpenAPI document and a ``{(METHOD, path): [permissions]}`` map from a static source scan."""
    sys.path.insert(0, str(ROOT / "src"))
    logging.disable(logging.CRITICAL)
    from olcf.api.app import create_app
    from olcf.core.config import Settings

    with tempfile.TemporaryDirectory(prefix="olcf-apidoc-") as tmp:
        app = create_app(Settings(data_dir=Path(tmp)))
        spec = app.openapi()
        perms: dict[tuple[str, str], list[str]] = {}
        for methods, path, endpoint in iter_routes(app.routes):
            try:
                src = inspect.getsource(endpoint)
            except (OSError, TypeError):
                continue
            found = sorted(set(_PERM_RE.findall(src)))
            for m in methods:
                perms[(m.upper(), path)] = found
        app.state.ctx.db.dispose()
    return spec, perms


def deref(spec: dict[str, Any], node: Any) -> Any:
    """Follow a local ``$ref``."""
    while isinstance(node, dict) and "$ref" in node:
        cur: Any = spec
        for part in node["$ref"].lstrip("#/").split("/"):
            cur = cur[part]
        node = cur
    return node


def type_of(spec: dict[str, Any], schema: Any, depth: int = 0) -> str:
    """Compact human type of a JSON-schema node."""
    if not isinstance(schema, dict):
        return "any"
    if "$ref" in schema:
        return f"`{schema['$ref'].rsplit('/', 1)[-1]}`"
    if "anyOf" in schema or "oneOf" in schema:
        parts = [type_of(spec, s, depth + 1) for s in schema.get("anyOf") or schema.get("oneOf") or []]
        parts = [p for p in parts if p != "null"]
        return " \\| ".join(dict.fromkeys(parts)) or "any"
    if "allOf" in schema:
        return " + ".join(type_of(spec, s, depth + 1) for s in schema["allOf"])
    if "enum" in schema:
        vals = ", ".join(f"`{v}`" for v in schema["enum"][:8])
        return f"enum ({vals}{', ...' if len(schema['enum']) > 8 else ''})"
    t = schema.get("type", "any")
    if t == "array":
        return f"array of {type_of(spec, schema.get('items', {}), depth + 1)}"
    if t == "object" and schema.get("additionalProperties") not in (None, False, True) and depth < 3:
        return f"map of {type_of(spec, schema['additionalProperties'], depth + 1)}"
    fmt = schema.get("format")
    return f"{t} ({fmt})" if fmt else str(t)


def clean(text: str | None) -> str:
    if not text:
        return ""
    text = re.sub(r"\s*\n\s*", " ", text.strip())
    return text.replace("|", "\\|")


def body_fields(spec: dict[str, Any], schema: Any) -> list[tuple[str, str, bool, str]]:
    schema = deref(spec, schema)
    if not isinstance(schema, dict):
        return []
    required = set(schema.get("required", []))
    rows = []
    for name, prop in (schema.get("properties") or {}).items():
        rows.append((name, type_of(spec, prop), name in required, clean(deref(spec, prop).get("description") if isinstance(deref(spec, prop), dict) else "")))
    return rows


def render(spec: dict[str, Any], perms: dict[tuple[str, str], list[str]]) -> str:
    info = spec.get("info", {})
    by_tag: dict[str, list[tuple[str, str, dict[str, Any]]]] = defaultdict(list)
    for path, item in spec.get("paths", {}).items():
        for method in _HTTP_METHODS:
            op = item.get(method)
            if op:
                for tag in op.get("tags") or ["untagged"]:
                    by_tag[tag].append((path, method.upper(), op))
    total = sum(len(v) for v in by_tag.values())

    out: list[str] = []
    w = out.append
    w("# REST API reference\n")
    w("> Generated by `scripts/gen_api_reference.py` from the live OpenAPI document (`GET /v1/openapi.json`). Do not edit by hand; regenerate instead.\n")
    w(f"**{info.get('title', 'OLCF API')}**, version `{info.get('version', '?')}`, {len(spec.get('paths', {}))} paths, {total} operations, "
      f"{len(by_tag)} tag groups. Author: Justin Dodrill. Licence: Apache-2.0.\n")
    w("## How to read this page\n")
    w("* Base path `/v1`. Send `Authorization: Bearer <token>` (a login access token or a personal access token, see `olcf login` and `olcf token create` in the [CLI guide](../guides/cli.md)). "
      "Exceptions are listed under **hooks** (the credential is in the request) and the health probes.")
    w("* Errors use one envelope: `{\"error\": {\"code\", \"message\", \"details\", \"remediation\", \"correlation_id\"}}`; see [Error codes](errors.md).")
    w("* Lists return `{\"items\": [...], \"next_cursor\": ...}`; mutating long-running calls return `202` with a job or operation id and accept an `Idempotency-Key` header; "
      "updates use `ETag` / `If-Match`.")
    w("* **Permissions checked** is the result of a *static* scan of the handler source for `require(...)`/`can(...)` calls. A handler may delegate further checks to a service "
      "it calls (for example per-workspace checks on the stored object), so the list is a lower bound; an operation without a list performs its checks in the service layer (or needs only an authenticated caller). The role to permission mapping is in [Permissions and roles](permissions.md).")
    w("* The MCP endpoint (`/mcp`) is mounted at the application root and is described in the [MCP guide](../guides/mcp.md).")
    w("* The interactive explorer of a running server is `/v1/docs`.\n")
    w("## Index of tag groups\n")
    w("| Tag | Operations |")
    w("|---|---|")
    for tag in sorted(by_tag):
        w(f"| [{tag}](#{re.sub(r'[^a-z0-9-]+', '-', tag.lower())}) | {len(by_tag[tag])} |")
    w("")

    for tag in sorted(by_tag):
        w(f"## {tag}\n")
        for path, method, op in sorted(by_tag[tag], key=lambda x: (x[0], _HTTP_METHODS.index(x[1].lower()))):
            w(f"### `{method} {path}`\n")
            summary = op.get("summary", "")
            desc = (op.get("description") or "").strip()
            if desc:
                w(clean(desc).replace("\\|", "|") + "\n")
            elif summary:
                w(f"{summary}.\n")
            if op.get("deprecated"):
                w("**Deprecated.**\n")
            found = perms.get((method, path))
            if found:
                w("Permissions checked: " + ", ".join(f"`{p}`" for p in found) + "\n")
            params = [deref(spec, p) for p in op.get("parameters", [])]
            if params:
                w("| Parameter | In | Type | Required | Description |")
                w("|---|---|---|---|---|")
                for p in params:
                    w(f"| `{p['name']}` | {p['in']} | {type_of(spec, p.get('schema', {}))} | {'yes' if p.get('required') else 'no'} | {clean(p.get('description'))} |")
                w("")
            rb = op.get("requestBody")
            if rb:
                for ctype, media in rb.get("content", {}).items():
                    schema = media.get("schema", {})
                    w(f"Request body (`{ctype}`): {type_of(spec, schema)}{' (required)' if rb.get('required') else ''}\n")
                    rows = body_fields(spec, schema)
                    if rows:
                        w("| Field | Type | Required | Description |")
                        w("|---|---|---|---|")
                        for name, typ, req, d in rows:
                            w(f"| `{name}` | {typ} | {'yes' if req else 'no'} | {d} |")
                        w("")
            resp_bits = []
            for code, resp in op.get("responses", {}).items():
                if code == "422":
                    continue
                content = (resp.get("content") or {}).values()
                schema = next(iter(content), {}).get("schema") if content else None
                resp_bits.append(f"`{code}`" + (f" {type_of(spec, schema)}" if schema else "") + (f" ({clean(resp.get('description'))})" if resp.get("description") and resp.get("description") != "Successful Response" else ""))
            if resp_bits:
                w("Responses: " + "; ".join(resp_bits) + ". Errors use the standard envelope (`422` validation, `401`, `403`, `404`, `409`, `412`, `429` as applicable).\n")
    w("## Schemas\n")
    schemas = spec.get("components", {}).get("schemas", {})
    w(f"{len(schemas)} request/response models are defined. Field-level documentation is available from the live `GET /v1/openapi.json`; the models referenced above can be looked up there by name.\n")
    return "\n".join(out).rstrip() + "\n"


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--out", type=Path, default=OUT)
    ap.add_argument("--check", action="store_true", help="exit 1 if the output file is not up to date")
    ap.add_argument("--json", type=Path, help="also write the raw OpenAPI document here")
    args = ap.parse_args()
    spec, perms = build_spec()
    if args.json:
        args.json.write_text(json.dumps(spec, indent=2, sort_keys=True), encoding="utf-8")
    text = render(spec, perms)
    if args.check:
        current = args.out.read_text(encoding="utf-8") if args.out.exists() else ""
        if current != text:
            print(f"{args.out} is out of date; run scripts/gen_api_reference.py", file=sys.stderr)
            return 1
        return 0
    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(text, encoding="utf-8")
    print(f"wrote {args.out} ({len(text.splitlines())} lines)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
