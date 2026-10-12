# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Fake *external systems* for the provider tests: an Apache Livy server (also serving the Fabric Livy path shape and an Entra token endpoint) and a Kubernetes API.

The Kubernetes fake is in :mod:`tests.compute.fake_kube`. Both implement the real wire/contract shapes the providers speak, nothing more. They live in the tests only; the product code never imports them.
"""

from __future__ import annotations

import json
import re
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any

from tests.compute.fake_kube import (
    FakeKube,  # noqa: F401 - re-exported: the Kubernetes fake lives in its own module
)


# ===================================================================================== Livy
class FakeLivy:
    """Livy REST server: sessions, statements, batches, logs, DELETE; optional Fabric prefix and Entra token endpoint.

    Behaviour knobs (all mutable at any time): ``startup_polls`` GETs before a new session is ``idle``; ``statement_polls`` GETs before a
    statement is ``available``; ``outcome`` (``ok`` | ``error`` | ``hang``); ``output`` text; ``fail_requests`` makes the next N requests answer 503 (only those with HTTP method ``fail_method`` when set).
    """

    def __init__(self, *, require_bearer: str | None = None, token: str = "tok-1") -> None:  # noqa: S107
        self.sessions: dict[int, dict[str, Any]] = {}
        self.batches: dict[int, dict[str, Any]] = {}
        self.requests: list[tuple[str, str, dict[str, str], Any]] = []
        self.startup_polls, self.statement_polls, self.outcome, self.output = 1, 1, "ok", "42"
        self.log_lines: list[str] = ["livy: session booting", "driver: hello"]
        self.fail_requests = 0
        self.fail_method: str | None = None
        self.require_bearer = require_bearer
        self.token, self.token_requests = token, []
        self._next = 0
        self._lock = threading.Lock()
        outer = self

        class H(BaseHTTPRequestHandler):
            protocol_version = "HTTP/1.1"

            def log_message(self, *a: Any) -> None:  # silence
                pass

            def _body(self) -> Any:
                n = int(self.headers.get("Content-Length") or 0)
                raw = self.rfile.read(n) if n else b""
                ctype = self.headers.get("Content-Type", "")
                if "json" in ctype and raw:
                    return json.loads(raw)
                return raw.decode() if raw else None

            def _send(self, code: int, doc: Any = None, headers: dict[str, str] | None = None) -> None:
                data = b"" if doc is None else json.dumps(doc).encode()
                self.send_response(code)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(data)))
                for k, v in (headers or {}).items():
                    self.send_header(k, v)
                self.end_headers()
                self.wfile.write(data)

            def do_GET(self) -> None:
                outer._handle(self, "GET")

            def do_POST(self) -> None:
                outer._handle(self, "POST")

            def do_DELETE(self) -> None:
                outer._handle(self, "DELETE")

        self._server = ThreadingHTTPServer(("127.0.0.1", 0), H)
        self._server.daemon_threads = True
        self.port = self._server.server_address[1]
        self._thread = threading.Thread(target=self._server.serve_forever, name="fake-livy", daemon=True)
        self._thread.start()

    # --------------------------------------------------------------------------
    @property
    def url(self) -> str:
        return f"http://127.0.0.1:{self.port}"

    def close(self) -> None:
        self._server.shutdown()
        self._server.server_close()
        self._thread.join(timeout=5)

    def calls(self, method: str, pattern: str) -> list[tuple[str, str, dict[str, str], Any]]:
        return [r for r in self.requests if r[0] == method and re.search(pattern, r[1])]

    def add_foreign_session(self, name: str, state: str = "busy") -> int:
        """A session created by someone else / a previous controller (for reconcile)."""
        with self._lock:
            sid = self._next = self._next + 1
            self.sessions[sid] = {"id": sid, "name": name, "state": state, "appId": f"application_{sid}", "polls": 99, "statements": {}}
            return sid

    # --------------------------------------------------------------------------
    def _handle(self, h: BaseHTTPRequestHandler, method: str) -> None:
        body = h._body()  # type: ignore[attr-defined]
        send = h._send  # type: ignore[attr-defined]
        full = h.path
        path, _, query = full.partition("?")
        params = dict(p.split("=", 1) for p in query.split("&") if "=" in p)
        hdrs = {k: v for k, v in h.headers.items()}
        with self._lock:
            self.requests.append((method, path, hdrs, body))
            if path == "/token":
                self.token_requests.append(body)
                return send(200, {"access_token": self.token, "expires_in": 3600, "token_type": "Bearer"})
            if self.require_bearer and hdrs.get("Authorization") != f"Bearer {self.require_bearer}":
                return send(401, {"msg": "unauthorized"})
            if self.fail_requests > 0 and (self.fail_method is None or self.fail_method == method):
                self.fail_requests -= 1
                return send(503, {"msg": "busy"}, {"Retry-After": "0"})
            m = re.search(r"(?:/livyapi/versions/[^/]+)?(/(?:sessions|batches).*)$", path)
            if not m:
                return send(404, {"msg": "no such route"})
            route = m.group(1).strip("/").split("/")
            kind = route[0]
            store = self.sessions if kind == "sessions" else self.batches
            if len(route) == 1:
                if method == "GET":
                    return send(200, {"from": 0, "total": len(store), "sessions": list(store.values())})
                if method == "POST":
                    sid = self._next = self._next + 1
                    rec = {"id": sid, "name": (body or {}).get("name"), "state": "starting", "appId": f"application_{sid}_0001", "polls": 0, "statements": {}, "request": body,
                           "appInfo": {"sparkUiUrl": f"http://spark-ui.invalid/{sid}"}}
                    store[sid] = rec
                    return send(201, {k: v for k, v in rec.items() if k not in ("polls", "statements")})
            sid = int(route[1])
            rec = store.get(sid)
            if rec is None:
                return send(404, {"msg": f"{kind[:-1]} {sid} not found"})
            if len(route) == 2:
                if method == "DELETE":
                    rec["state"] = "killed"
                    del store[sid]
                    self.deleted = getattr(self, "deleted", []) + [(kind, sid)]
                    return send(200, {"msg": "deleted"})
                rec["polls"] += 1
                if rec["state"] == "starting" and rec["polls"] > self.startup_polls:
                    rec["state"] = "idle" if kind == "sessions" else "running"
                if kind == "batches" and rec["state"] == "running" and rec["polls"] > self.startup_polls + self.statement_polls:
                    rec["state"] = {"ok": "success", "error": "dead"}.get(self.outcome, "running")
                return send(200, {k: v for k, v in rec.items() if k not in ("polls", "statements", "request")})
            tail = route[2]
            if tail == "log":
                lines = self.log_lines + [x for x in rec.get("extra_log", [])]
                frm, size = int(params.get("from", 0)), int(params.get("size", 100))
                return send(200, {"id": sid, "from": frm, "total": len(lines), "log": lines[frm:frm + size]})
            if tail == "statements":
                if len(route) == 3 and method == "GET":
                    listing = [{"id": x["id"], "state": x["state"], "output": x["output"], "code": x["code"]} for x in rec["statements"].values()]
                    return send(200, {"total_statements": len(listing), "statements": listing})
                if len(route) == 3 and method == "POST":
                    st_id = len(rec["statements"])
                    rec["statements"][st_id] = {"id": st_id, "code": body.get("code"), "state": "running", "polls": 0, "output": None, "kind": body.get("kind")}
                    rec["state"] = "busy"
                    return send(201, {"id": st_id, "state": "waiting", "output": None})
                st = rec["statements"].get(int(route[3]))
                if st is None:
                    return send(404, {"msg": "statement not found"})
                st["polls"] += 1
                if st["state"] == "running" and st["polls"] > self.statement_polls and self.outcome != "hang":
                    st["state"] = "available"
                    if self.outcome == "ok":
                        st["output"] = {"status": "ok", "execution_count": 1, "data": {"text/plain": self.output}}
                    else:
                        st["output"] = {"status": "error", "ename": "ValueError", "evalue": "boom", "traceback": ["Traceback...", "ValueError: boom"]}
                return send(200, {"id": st["id"], "state": st["state"], "output": st["output"], "progress": 0.5 if st["state"] == "running" else 1.0})
        return send(404, {"msg": "unhandled"})
