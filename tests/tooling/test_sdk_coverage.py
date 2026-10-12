# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Every operation of the application's OpenAPI document must be bound by an SDK method (or be listed in EXCLUDED with a reason), and
every SDK method must point at a real operation - the SDK cannot silently fall behind the API."""

from __future__ import annotations

import re
from pathlib import Path

import pytest

SDK = Path(__file__).resolve().parents[2] / "src" / "olcf" / "sdk"
CALL = re.compile(r'self\._call\(\s*"(GET|POST|PUT|PATCH|DELETE)",\s*"([^"]+)"')

#: operations deliberately not wrapped, with the reason
EXCLUDED = {
    ("GET", "/events/stream"): "Server-Sent Events stream; use admin.events() polling or any SSE client",
    ("GET", "/jobs/{}/logs/stream"): "SSE; jobs.logs(follow=True) polls the paged log endpoint instead",
    ("GET", "/spark-ui/{}/{}"): "reverse proxy for the Spark web UI (open in a browser)",
    ("HEAD", "/spark-ui/{}/{}"): "reverse proxy for the Spark web UI",
    ("GET", "/spark-applications/{}/ui/{}"): "reverse proxy for the Spark web UI",
    ("HEAD", "/spark-applications/{}/ui/{}"): "reverse proxy for the Spark web UI",
    ("GET", "/integrations/fabric/"): "trailing-slash alias of /integrations/fabric",
    ("PUT", "/integrations/fabric/"): "trailing-slash alias of /integrations/fabric",
    ("GET", "/identity/oidc/{}/login"): "browser redirect to the identity provider (use the UI or open the URL)",
    ("GET", "/identity/oidc/{}/callback"): "browser redirect target of the identity provider",
    ("GET", "/environments/jars/{}"): "worker-agent endpoint (content-addressed jar cache download); not part of the client API",
}


def norm(path: str) -> str:
    p = re.sub(r"\{[^}]+\}", "{}", path)
    return p[3:] if p.startswith("/v1") else p


def sdk_ops() -> set[tuple[str, str]]:
    ops: set[tuple[str, str]] = set()
    for f in SDK.glob("_resources*.py"):
        src = f.read_text()
        for m in CALL.finditer(src):
            ops.add((m.group(1), norm(m.group(2))))
    return ops


def api_ops(app) -> set[tuple[str, str]]:
    spec = app.openapi()
    ops = set()
    for path, methods in spec["paths"].items():
        for method in methods:
            ops.add((method.upper(), norm(path)))
    return ops


@pytest.fixture()
def full_app(tmp_path):
    from olcf.api.app import create_app
    from olcf.core.config import load_settings

    s = load_settings(tmp_path / "data", environment="test")
    s.compute.embedded_worker = False
    return create_app(s)


def test_every_api_operation_is_wrapped(full_app):
    missing = sorted(op for op in api_ops(full_app) - sdk_ops() if op not in EXCLUDED)
    assert not missing, "operations without an SDK method (add one to olcf/sdk/_resources*.py or EXCLUDED):\n" + "\n".join(f"{m} {p}" for m, p in missing)


def test_every_sdk_method_targets_a_real_operation(full_app):
    api = api_ops(full_app)
    phantom = sorted(op for op in sdk_ops() - api if op not in {("GET", "/mcp"), ("POST", "/mcp"), ("DELETE", "/mcp")})
    assert not phantom, "SDK methods pointing at routes that do not exist:\n" + "\n".join(f"{m} {p}" for m, p in phantom)
