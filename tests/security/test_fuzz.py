# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Spec 35.4: API fuzzing. Every non-streaming operation of the full application receives seeded, mutated requests (type confusion, nasty strings, huge numbers, deep
nesting, missing/extra keys, malformed bodies, hostile path and query values) as three different principals.

Invariants: no 5xx (an unhandled exception), every error is the JSON envelope, no stack trace or server path in any body, no tenant-B canary and no live secret in any
response, and no request takes unreasonably long. ``OLCF_FUZZ_SEED`` / ``OLCF_FUZZ_ROUNDS`` reproduce and deepen a run; a failure prints the operation, the seed and the mutation.
"""

from __future__ import annotations

import json
import os
import random
import re
import time
from urllib.parse import quote

from tests.security.fuzzgen import mutate_json, nasty, raw_body
from tests.security.secenv import CANARY_B, SECRET_A, SECRET_B
from tests.security.sweep import build, operations, request_for

SEED = int(os.environ.get("OLCF_FUZZ_SEED", "20260101"))
ROUNDS = int(os.environ.get("OLCF_FUZZ_ROUNDS", "4"))
#: not fuzzed: they would end the principal's own session / change its credentials, or stream forever
SKIP = {("POST", "/v1/identity/logout"), ("POST", "/v1/identity/password"), ("POST", "/v1/compute/spark-smoke")}
TRACE = re.compile(r"Traceback \(most recent call last\)|File \"/|\.py\", line \d+|sqlalchemy\.exc|psycopg\.errors|sqlite3\.\w*Error|/home/claude|site-packages")
SLOW_SECONDS = 20.0


def fuzz_ops(env):
    ops = [o for o in operations(env.spec) if o.key not in SKIP and not o.streaming]
    return sorted(ops, key=lambda o: (o.method == "delete", o.path, o.method))  # destructive calls last so that earlier operations keep their targets


def mutate_request(env, op, rng: random.Random, headers: dict[str, str]) -> tuple[dict, str]:
    kw = request_for(env, op, "A", headers)
    kind = rng.randrange(7)
    note = ""
    if kind <= 2 and "json" in kw:
        kw["json"] = mutate_json(kw["json"], rng)
        note = "json-mutation"
    elif kind == 3:
        body, ctype = raw_body(rng)
        kw.pop("json", None)
        kw.pop("files", None)
        kw["content"] = body
        kw["headers"] = {**headers, "Content-Type": ctype}
        note = f"raw-body {ctype}"
    elif kind == 4:
        qs = {p["name"] for p in op.params if p.get("in") == "query"}
        for name in qs or {"q"}:
            val = nasty(rng) if rng.random() < 0.7 else "x"
            kw["params"][name] = val[:4000] if isinstance(val, str) else json.dumps(val)[:4000]
        note = "query"
    elif kind == 5 and op.path_params:
        url = op.path
        for name in op.path_params:
            val = nasty(rng)
            url = url.replace("{" + name + "}", quote(str(val) if not isinstance(val, (dict, list)) else json.dumps(val), safe="")[:300] or "x")
        kw["url"] = url
        note = "path"
    elif kind == 6 and "files" in kw:
        kw["files"] = {"file": (str(nasty(rng))[:200], str(nasty(rng)).encode()[:100000], "application/octet-stream")}
        note = "multipart"
    else:
        note = "plain"
    return kw, note


def check(response, elapsed: float, label: str, problems: list[str], platform: bool = False) -> None:
    text = response.text
    if response.status_code >= 500 and not (response.status_code == 503 and '"code":"unavailable"' in text):  # a clean "not available right now" is an answer, not a crash
        problems.append(f"{label}: HTTP {response.status_code} {text[:160]}")
    if elapsed > SLOW_SECONDS:
        problems.append(f"{label}: took {elapsed:.1f}s")
    if TRACE.search(text):
        problems.append(f"{label}: stack trace / server path in the body: {TRACE.search(text).group(0)!r}")
    if SECRET_B in text or SECRET_A in text or (CANARY_B in text and not platform):  # the platform administrator legitimately sees tenant B's names
        problems.append(f"{label}: canary secret in the response")
    if 400 <= response.status_code < 500 and response.headers.get("content-type", "").startswith("application/json"):
        try:
            body = response.json()
        except ValueError:
            problems.append(f"{label}: invalid JSON in an error response")
            return
        if not isinstance(body, dict) or "error" not in body and "detail" not in body:
            problems.append(f"{label}: error response without the error envelope: {text[:120]}")


def test_fuzz_every_operation(lenient_tenants):
    env = lenient_tenants
    rng = random.Random(SEED)
    problems: list[str] = []
    actors = ["admin", "a_admin", "a_contrib"]
    sent = 0
    for op in fuzz_ops(env):
        for i in range(ROUNDS):
            who = actors[(i + len(op.path)) % len(actors)]
            kw, note = mutate_request(env, op, rng, env.headers(who))
            label = f"{op.label} as {who} [{note}] seed={SEED} i={i}"
            t0 = time.perf_counter()
            try:
                r = env.c.request(**kw)
            except Exception as exc:  # noqa: BLE001 - a transport-level crash is a finding as well
                problems.append(f"{label}: client/transport error {type(exc).__name__}: {str(exc)[:120]}")
                continue
            sent += 1
            check(r, time.perf_counter() - t0, label, problems, platform=who == "admin")
    assert sent > 1000, sent
    assert not problems, f"{len(problems)} problem(s) in {sent} requests:\n" + "\n".join(problems[:60])


def test_fuzz_unauthenticated_and_garbage_credentials(lenient_tenants):
    """Mutated requests with no / forged / truncated credentials: never a 5xx, never anything but 401 on protected routes."""
    env = lenient_tenants
    rng = random.Random(SEED + 1)
    problems: list[str] = []
    bad_headers = [{}, {"Authorization": "Bearer"}, {"Authorization": "Bearer "}, {"Authorization": "Basic Zm9vOmJhcg=="}, {"Authorization": "Bearer " + "A" * 5000},
                   {"Authorization": "Bearer a.b.c"}, {"Authorization": "Bearer olcf_pat_" + "x" * 40}, {"Authorization": "\x00"}, {"Authorization": "Bearer eyJhbGciOiJub25lIn0.e30."},
                   {"X-API-Key": "x"}, {"Cookie": "olcf_session=" + "x" * 64}]
    public = {"/v1/health", "/v1/health/live", "/v1/health/ready", "/v1/openapi.json", "/v1/identity/login", "/v1/identity/refresh", "/v1/identity/local-login"}
    for op in fuzz_ops(env):
        if op.path in public or op.path.startswith(("/v1/identity/providers", "/v1/auth/", "/v1/hooks/", "/v1/oidc", "/v1/saml", "/v1/ui", "/v1/identity/oidc", "/v1/identity/saml", "/mcp")):
            continue
        h = rng.choice(bad_headers)
        kw = build(env.spec, op, env.param_values("A"))
        kw["headers"] = {k: v for k, v in h.items() if all(ord(c) < 256 for c in v)}
        r = env.c.request(**kw)
        if r.status_code >= 500:
            problems.append(f"{op.label} {h}: HTTP {r.status_code}")
        elif r.status_code not in (401, 404, 405) and not (r.status_code == 403 and "forbidden" in r.text):
            problems.append(f"{op.label} {h}: unexpected HTTP {r.status_code} {r.text[:100]}")
    assert not problems, "\n".join(problems[:40])


def test_fuzz_mcp_endpoint(lenient_tenants):
    """The MCP endpoint (its own bearer check, Host/Origin guard and JSON-RPC parser) with hostile JSON-RPC."""
    env = lenient_tenants
    rng = random.Random(SEED + 2)
    h = {**env.headers("a_contrib"), "Host": "localhost:8470", "Content-Type": "application/json"}
    base = {"jsonrpc": "2.0", "id": 1, "method": "initialize", "params": {"protocolVersion": "2025-03-26", "capabilities": {}, "clientInfo": {"name": "t", "version": "1"}}}
    first = env.c.post("/mcp", json=base, headers=h)
    assert first.status_code < 500, first.text[:200]
    problems: list[str] = []
    methods = ["tools/list", "tools/call", "resources/list", "resources/read", "prompts/list", "ping", "initialize", "notifications/initialized", "x" * 300, "", None, 5]
    for _ in range(300):
        doc = {"jsonrpc": rng.choice(["2.0", "1.0", None, 2]), "id": rng.choice([1, "a", None, 2**70, [], {}]), "method": rng.choice(methods),
               "params": rng.choice([{}, [], None, "x", {"name": nasty(rng), "arguments": nasty(rng)}, {"uri": nasty(rng)}, nasty(rng)])}
        r = env.c.post("/mcp", content=json.dumps(doc).encode(), headers=h) if rng.random() < 0.85 else env.c.post("/mcp", content=raw_body(rng)[0], headers=h)
        if r.status_code >= 500 or TRACE.search(r.text) or CANARY_B in r.text:
            problems.append(f"{doc!r:.200}: HTTP {r.status_code} {r.text[:150]}")
    for host in ("evil.example.com", "localhost.evil.com", "127.0.0.1.evil.com", ""):
        r = env.c.post("/mcp", json=base, headers={**h, "Host": host})
        if r.status_code == 200:
            problems.append(f"DNS-rebinding guard: Host {host!r} accepted")
    r = env.c.post("/mcp", json=base, headers={**h, "Origin": "https://evil.example.com"})
    if r.status_code == 200:
        problems.append("Origin https://evil.example.com accepted")
    assert not problems, "\n".join(problems[:30])
