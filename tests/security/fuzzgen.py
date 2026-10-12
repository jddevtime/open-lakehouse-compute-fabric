# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Dependency-free request mutator for the API fuzz test (a seeded ``random.Random``: failures are reproducible from the printed seed and mutation index)."""

from __future__ import annotations

import copy
import random
from typing import Any

NASTY_STRINGS = [
    "", " ", "\x00", "a\x00b", "\n", "\r\n\r\nX-Injected: 1", "../../../../etc/passwd", "..\\..\\windows", "/etc/passwd", "file:///etc/passwd", "'; DROP TABLE users;--", "' OR '1'='1",
    '" OR ""="', "1; SELECT pg_sleep(5)", "${jndi:ldap://x/a}", "{{7*7}}", "%s%s%s%n", "%00", "%2e%2e%2f", "‮", "﻿", "\U0001f4a9" * 50, "A" * 70000, "ä" * 1000,
    "<script>alert(1)</script>", "javascript:alert(1)", "-1", "NaN", "Infinity", "null", "true", "{}", "[]", "*", "%", "_", "\\", "'", '"', "`id`", "$(id)", "; id", "| id",
    "olcf://x/y/Files/z", "s3://bucket/key", "http://169.254.169.254/latest/meta-data/", "http://[::1]:1/", "https://user:pass@example.com/",
]
NASTY_NUMBERS: list[Any] = [0, -1, 1, 2**31, 2**63, -(2**63), 2**100, 1.5, 1e308, -1e308, 3.4e38, True, False]


def nasty(rng: random.Random) -> Any:
    kind = rng.randrange(8)
    if kind == 0:
        return None
    if kind in (1, 2, 3):
        return rng.choice(NASTY_STRINGS)
    if kind == 4:
        return rng.choice(NASTY_NUMBERS)
    if kind == 5:
        return []
    if kind == 6:
        return {}
    return deep(rng.randrange(5, 120), rng.random() < 0.5)


def deep(depth: int, as_list: bool) -> Any:
    v: Any = "x"
    for _ in range(depth):
        v = [v] if as_list else {"a": v}
    return v


def paths(value: Any, prefix: tuple[Any, ...] = ()) -> list[tuple[Any, ...]]:
    out: list[tuple[Any, ...]] = [prefix]
    if isinstance(value, dict):
        for k, v in value.items():
            out += paths(v, (*prefix, k))
    elif isinstance(value, list):
        for i, v in enumerate(value):
            out += paths(v, (*prefix, i))
    return out


def get_at(value: Any, path: tuple[Any, ...]) -> Any:
    for p in path:
        value = value[p]
    return value


def set_at(value: Any, path: tuple[Any, ...], new: Any) -> Any:
    if not path:
        return new
    parent = get_at(value, path[:-1])
    parent[path[-1]] = new
    return value


def delete_at(value: Any, path: tuple[Any, ...]) -> Any:
    if path:
        parent = get_at(value, path[:-1])
        if isinstance(parent, dict):
            parent.pop(path[-1], None)
        elif isinstance(parent, list) and path[-1] < len(parent):
            parent.pop(path[-1])
    return value


def mutate_json(body: Any, rng: random.Random) -> Any:
    """One structural mutation of a JSON document: replace a value, drop a key, add an unknown key, or swap the type of the whole document."""
    body = copy.deepcopy(body)
    op = rng.randrange(6)
    ps = paths(body)
    if op == 0 or not isinstance(body, (dict, list)):
        return nasty(rng) if op == 5 or not isinstance(body, (dict, list)) else set_at(body, rng.choice(ps), nasty(rng))
    if op in (1, 2):
        return set_at(body, rng.choice(ps), nasty(rng))
    if op == 3:
        return delete_at(body, rng.choice(ps))
    if op == 4:
        tgt = rng.choice([p for p in ps if isinstance(get_at(body, p), dict)] or [()])
        get_at(body, tgt)[rng.choice(NASTY_STRINGS[:30]) or "k"] = nasty(rng)
        return body
    return rng.choice([[], {}, "", 0, None, [body], {"a": body}])


def raw_body(rng: random.Random) -> tuple[bytes, str]:
    """A malformed request body plus content type."""
    choices: list[tuple[bytes, str]] = [
        (b"{", "application/json"), (b"[1,2", "application/json"), (b"\xff\xfe\x00{", "application/json"), (b"{'a': 1}", "application/json"), (b'{"a": NaN}', "application/json"),
        (b'{"a": 1}' * 3, "application/json"), (b"\x00" * 100, "application/json"), (b"", "application/json"), (b"a=1&b=2", "application/x-www-form-urlencoded"),
        (b"--x\r\nContent-Disposition: form-data; name=\"file\"\r\n\r\nx\r\n--x--", "multipart/form-data; boundary=x"), (b"--x\r\n", "multipart/form-data; boundary=x"),
        (b"x", "multipart/form-data"), (b'{"a":' + b"[" * 5000, "application/json"), (b"<xml/>", "application/xml"), (b"x", "text/plain"), (b'"' + b"A" * 200000 + b'"', "application/json"),
    ]
    return rng.choice(choices)
