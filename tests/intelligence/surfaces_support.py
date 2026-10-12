# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
# ruff: noqa: S105, S106
"""A populated Intelligence application for the SDK, CLI and MCP surface tests.

``intel_stack(path, extra_modules=...)`` builds the real application (core + storage + catalog + sql + intelligence, plus ``extra_modules`` such as ``olcf.mcp.module``),
logs the administrator in, fills the graph with the two-workspace world of ``tests/intelligence/conftest.py`` (alpha: raw -> etl -> clean -> agg -> mart -> sales_model ->
sales_report with a column edge raw.amount -> clean.amount; beta: a notebook reading alpha's clean and a private table; an organisation-level source system) and returns a
:class:`Stack` with helpers to get an SDK client for any of the world's callers.
"""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path
from typing import Any

from fastapi.testclient import TestClient

from olcf.api.app import create_app
from olcf.core.config import load_settings
from olcf.sdk import Client
from tests.intelligence.conftest import INTEL_MODULES, IntelClient, World, build_world


class Stack:
    """The running application, the TestClient, the populated world and SDK clients bound to the same in-process transport."""

    def __init__(self, app: Any, http: TestClient, intel: IntelClient, world: World, admin_token: str) -> None:
        self.app, self.http, self.intel, self.world, self.admin_token = app, http, intel, world, admin_token
        self.ids = world.ids

    @property
    def ctx(self) -> Any:
        return self.app.state.ctx

    def token(self, who: str = "admin") -> str:
        """The bearer token of one of the world's callers (``alpha``, ``beta``, ``org``, ``none``, ``curator``, ``curator_org``, ``agent``, ``wsadmin``) or ``admin``."""
        if who == "admin":
            return self.admin_token
        return self.world.h[who]["Authorization"].split(" ", 1)[1]

    def sdk(self, who: str = "admin") -> Client:
        """An SDK client for ``who`` over the in-process transport (no retries)."""
        return Client("http://testserver", http_client=self.http, token=self.token(who), max_retries=0)

    def headers(self, who: str = "admin") -> dict[str, str]:
        return {"Authorization": f"Bearer {self.token(who)}"}


@contextmanager
def intel_stack(path: Path, extra_modules: tuple[str, ...] = ()) -> Iterator[Stack]:
    """Build the application under ``path`` and yield a populated :class:`Stack`."""
    s = load_settings(path / "data", environment="test")
    s.compute.embedded_worker = False
    app = create_app(s, module_names=[*INTEL_MODULES, *extra_modules])
    with TestClient(app) as c:
        pw = app.state.ctx.settings.path("secrets", "bootstrap-admin.txt").read_text().split("password=")[1].strip()
        r = c.post("/v1/identity/login", json={"username": "admin", "password": pw})
        assert r.status_code == 200, r.text
        token = r.json()["access_token"]
        c.admin = {"Authorization": f"Bearer {token}"}  # type: ignore[attr-defined]
        c.ctx = app.state.ctx  # type: ignore[attr-defined]
        intel = IntelClient(c)
        yield Stack(app, c, intel, build_world(intel), token)


class McpClient:
    """A tiny MCP client over the in-process TestClient (Streamable HTTP, JSON replies): ``init()``, ``result(method, params)`` and ``call(tool, **args)``.

    ``call`` returns the decoded tool result; with ``error=True`` it expects a tool-level failure (``isError``) and returns its ``error`` object.
    """

    HOST = {"Host": "localhost"}

    def __init__(self, http: TestClient, headers: dict[str, str], version: str = "2025-06-18") -> None:
        self.http, self.auth, self.version, self.sid, self.n = http, headers, version, None, 0

    def headers(self, session: bool = True) -> dict[str, str]:
        h = {**self.auth, **self.HOST, "Content-Type": "application/json", "Accept": "application/json, text/event-stream"}
        if session and self.sid:
            h["Mcp-Session-Id"] = self.sid
        return h

    def rpc(self, method: str, params: dict[str, Any] | None = None, *, session: bool = True) -> Any:
        import json

        self.n += 1
        body: dict[str, Any] = {"jsonrpc": "2.0", "id": self.n, "method": method}
        if params is not None:
            body["params"] = params
        return self.http.post("/mcp", content=json.dumps(body), headers=self.headers(session))

    def init(self) -> dict[str, Any]:
        import json

        r = self.rpc("initialize", {"protocolVersion": self.version, "capabilities": {}, "clientInfo": {"name": "pytest-agent", "version": "1"}}, session=False)
        assert r.status_code == 200, r.text
        self.sid = r.headers["Mcp-Session-Id"]
        n = self.http.post("/mcp", content=json.dumps({"jsonrpc": "2.0", "method": "notifications/initialized"}), headers=self.headers())
        assert n.status_code == 202
        return r.json()["result"]

    def result(self, method: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
        r = self.rpc(method, params)
        assert r.status_code == 200, r.text
        body = r.json()
        assert "error" not in body, body
        return body["result"]

    def error(self, method: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
        """The JSON-RPC ``error`` object of a request that must fail at the protocol level (unknown tool, bad params)."""
        r = self.rpc(method, params)
        assert r.status_code == 200, r.text
        body = r.json()
        assert "error" in body, body
        return body["error"]

    def call(self, name: str, *, error: bool = False, **args: Any) -> dict[str, Any]:
        import json

        res = self.result("tools/call", {"name": name, "arguments": args})
        data = json.loads(res["content"][0]["text"])
        assert res["isError"] is error, data
        assert res["structuredContent"] == data
        return data["error"] if error else data
