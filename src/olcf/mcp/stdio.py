# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Stdio launcher for MCP clients that only speak stdio (Claude Desktop, many IDEs).

It bridges newline-delimited JSON-RPC on stdin/stdout to the OLCF Streamable HTTP endpoint::

    python -m olcf.mcp.stdio --url https://olcf.example.com/mcp          # token from $OLCF_MCP_TOKEN

The bearer token comes from ``--token-file``, ``$OLCF_MCP_TOKEN`` or (least preferred, visible in the process list) ``--token``.  Only
protocol messages are written to stdout; diagnostics go to stderr and never contain the token.  A session that expired on the server is
re-established transparently once.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import threading
from concurrent.futures import ThreadPoolExecutor
from typing import Any, TextIO

import httpx

from olcf.core.redaction import redact_text

SESSION_HEADER = "Mcp-Session-Id"
VERSION_HEADER = "MCP-Protocol-Version"


class Bridge:
    """Bridge between an MCP client that speaks over standard input and output and the server's HTTP MCP endpoint, forwarding each message with the access token.
    """
    def __init__(self, url: str, token: str, *, verify: bool | str = True, timeout: float = 300.0, out: TextIO | None = None, err: TextIO | None = None, verbose: bool = False) -> None:
        self.url, self.token, self.verbose = url, token, verbose
        self.out, self.err = out or sys.stdout, err or sys.stderr
        self.client = httpx.Client(verify=verify, timeout=httpx.Timeout(timeout, connect=15.0), follow_redirects=False)
        self.session_id: str | None = None
        self.version: str | None = None
        self.init_request: dict[str, Any] | None = None
        self._out_lock = threading.Lock()
        self._session_lock = threading.Lock()

    # ------------------------------------------------------------------ io
    def emit(self, msg: Any) -> None:
        line = json.dumps(msg, separators=(",", ":"), ensure_ascii=False)
        with self._out_lock:
            self.out.write(line + "\n")
            self.out.flush()

    def log(self, text: str) -> None:
        self.err.write(f"olcf-mcp: {redact_text(text)}\n")
        self.err.flush()

    def _headers(self) -> dict[str, str]:
        h = {"Authorization": f"Bearer {self.token}", "Content-Type": "application/json", "Accept": "application/json, text/event-stream"}
        if self.session_id:
            h[SESSION_HEADER] = self.session_id
        if self.version:
            h[VERSION_HEADER] = self.version
        return h

    # ------------------------------------------------------------------ forwarding
    def forward(self, msg: Any) -> None:
        """Send one client message upstream and relay whatever comes back."""
        is_request = isinstance(msg, dict) and "method" in msg and "id" in msg
        req_id = msg.get("id") if isinstance(msg, dict) else None
        try:
            for attempt in (1, 2):
                with self.client.stream("POST", self.url, content=json.dumps(msg), headers=self._headers()) as resp:
                    if resp.status_code == 404 and attempt == 1 and self.session_id and not (isinstance(msg, dict) and msg.get("method") == "initialize"):
                        resp.read()
                        if self._reinitialize():
                            continue
                    self._relay(resp, msg, is_request, req_id)
                    return
        except httpx.HTTPError as exc:
            self._error(is_request, req_id, -32000, f"cannot reach the OLCF MCP endpoint: {type(exc).__name__}: {str(exc)[:200]}")

    def _relay(self, resp: httpx.Response, msg: Any, is_request: bool, req_id: Any) -> None:
        if isinstance(msg, dict) and msg.get("method") == "initialize" and resp.status_code == 200:
            sid = resp.headers.get(SESSION_HEADER)
            with self._session_lock:
                self.session_id = sid
                self.init_request = msg
        if resp.status_code in (202, 204):
            return
        ctype = resp.headers.get("content-type", "")
        if resp.status_code == 200 and ctype.startswith("text/event-stream"):
            data: list[str] = []
            for line in resp.iter_lines():
                if line.startswith("data:"):
                    data.append(line[5:].lstrip())
                elif line == "" and data:
                    self._deliver(json.loads("\n".join(data)))
                    data = []
            if data:
                self._deliver(json.loads("\n".join(data)))
            return
        raw = resp.read()
        if ctype.startswith("application/json") and raw:
            try:
                body = json.loads(raw)
            except (ValueError, RecursionError):
                body = None
            if body is not None:
                if resp.status_code >= 400:
                    err = body.get("error") if isinstance(body, dict) else None
                    msg_text = (err or {}).get("message") or "request failed"
                    self._error(is_request, req_id, (err or {}).get("code", -32000), f"HTTP {resp.status_code}: {msg_text}")   # re-addressed to the request id
                else:
                    self._deliver(body)
                return
        self._error(is_request, req_id, -32000, f"HTTP {resp.status_code} from the OLCF MCP endpoint: {raw[:200].decode('utf-8', 'replace')}")

    def _deliver(self, body: Any) -> None:
        if isinstance(body, dict) and "result" in body and isinstance(body["result"], dict) and "protocolVersion" in body["result"] and "serverInfo" in body["result"]:
            self.version = body["result"]["protocolVersion"]
        self.emit(body)

    def _error(self, is_request: bool, req_id: Any, code: int, message: str) -> None:
        self.log(message)
        if is_request:
            self.emit({"jsonrpc": "2.0", "id": req_id, "error": {"code": code, "message": message}})

    def _reinitialize(self) -> bool:
        """The server forgot our session (expired / restarted): open a new one with the original initialize parameters."""
        with self._session_lock:
            init = self.init_request
            if init is None:
                return False
            self.session_id, self.version = None, None
            resp = self.client.post(self.url, content=json.dumps({**init, "id": "olcf-reinit"}), headers=self._headers())
            if resp.status_code != 200 or not resp.headers.get(SESSION_HEADER):
                return False
            self.session_id = resp.headers[SESSION_HEADER]
            try:
                self.version = resp.json()["result"]["protocolVersion"]
            except (ValueError, KeyError):
                return False
            self.client.post(self.url, content=json.dumps({"jsonrpc": "2.0", "method": "notifications/initialized"}), headers=self._headers())
            self.log("session was re-established")
            return True

    def close(self) -> None:
        if self.session_id:
            try:
                self.client.delete(self.url, headers=self._headers())
            except httpx.HTTPError:
                pass
        self.client.close()

    # ------------------------------------------------------------------ main loop
    def run(self, stdin: TextIO | None = None, workers: int = 8) -> int:
        stdin = stdin or sys.stdin
        pool = ThreadPoolExecutor(max_workers=workers, thread_name_prefix="olcf-mcp-stdio")
        try:
            for line in stdin:
                line = line.strip()
                if not line:
                    continue
                try:
                    msg = json.loads(line)
                except (ValueError, RecursionError):
                    self.emit({"jsonrpc": "2.0", "id": None, "error": {"code": -32700, "message": "parse error"}})
                    continue
                if isinstance(msg, dict) and msg.get("method") == "initialize":
                    self.forward(msg)           # the handshake is strictly ordered; everything after it may run concurrently
                else:
                    pool.submit(self.forward, msg)
        finally:
            pool.shutdown(wait=True)
            self.close()
        return 0


def _token(args: argparse.Namespace) -> str | None:
    if args.token_file:
        with open(args.token_file, encoding="utf-8") as fh:
            return fh.read().strip()
    return os.environ.get("OLCF_MCP_TOKEN") or args.token


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(prog="python -m olcf.mcp.stdio", description="Bridge MCP stdio to the OLCF Streamable HTTP endpoint.")
    p.add_argument("--url", default=os.environ.get("OLCF_MCP_URL"), help="MCP endpoint, e.g. https://olcf.example.com/mcp (or $OLCF_MCP_URL)")
    p.add_argument("--token", help="platform token (prefer $OLCF_MCP_TOKEN or --token-file: arguments are visible to other users)")
    p.add_argument("--token-file", help="file containing the platform token")
    p.add_argument("--ca-file", help="PEM bundle to trust for https (private CA)")
    p.add_argument("--insecure", action="store_true", help="do not verify the server certificate (never use outside a lab)")
    p.add_argument("--timeout", type=float, default=300.0, help="seconds to wait for one response (default 300)")
    p.add_argument("--verbose", action="store_true")
    args = p.parse_args(argv)
    token = _token(args)
    if not args.url:
        p.error("--url (or OLCF_MCP_URL) is required")
    if not token:
        p.error("a token is required: set OLCF_MCP_TOKEN, or use --token-file / --token")
    if not args.url.startswith(("http://", "https://")):
        p.error("--url must start with http:// or https://")
    verify: bool | str = False if args.insecure else (args.ca_file or True)
    bridge = Bridge(args.url, token, verify=verify, timeout=args.timeout, verbose=args.verbose)
    try:
        return bridge.run()
    except KeyboardInterrupt:
        return 0


if __name__ == "__main__":
    sys.exit(main())
