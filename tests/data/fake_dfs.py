# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""A small in-memory ADLS Gen2 / OneLake (DFS + Blob REST) server for testing request construction.

It implements exactly the operations the OLCF Azure providers use: container properties, path properties, list
(``resource=filesystem``), create file/directory, append/flush, ranged download, rename, delete.  Requests are
authenticated for real: SharedKey signatures are recomputed and compared, SAS tokens are compared, and bearer tokens
must match.  It speaks plain HTTP, or HTTPS when given a certificate (the Azure SDK refuses bearer tokens over HTTP).

Paths use the Azurite style ``/<account>/<container>/<path>``: the SDK treats ``localhost``/IP endpoints that way.
"""

from __future__ import annotations

import base64
import hashlib
import hmac
import json
import ssl
import threading
from email.utils import formatdate
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, unquote, urlparse

ACCOUNT = "devacct"
ACCOUNT_KEY = base64.b64encode(b"fake-account-key-0123456789abcdef").decode()
SAS = "sv=2022-11-02&ss=bf&srt=sco&sp=rwdlacx&sig=fakesignature"
BEARER = "fake-bearer-token"


class FakeDfs:
    def __init__(self, containers: tuple[str, ...] = ("lake",), auth: str = "key", certfile: str | None = None, keyfile: str | None = None) -> None:
        self.auth = auth
        self.files: dict[str, dict[str, bytes]] = {c: {} for c in containers}
        self.dirs: dict[str, set[str]] = {c: set() for c in containers}
        self.pending: dict[tuple[str, str], bytearray] = {}
        self.log: list[tuple[str, str, int]] = []   # (method, path, status)
        self.lock = threading.Lock()
        outer = self

        class H(BaseHTTPRequestHandler):
            protocol_version = "HTTP/1.1"

            def log_message(self, *a: object) -> None:  # silence
                return

            def do_HEAD(self) -> None: outer.handle(self)
            def do_GET(self) -> None: outer.handle(self)
            def do_PUT(self) -> None: outer.handle(self)
            def do_PATCH(self) -> None: outer.handle(self)
            def do_DELETE(self) -> None: outer.handle(self)

        self.httpd = ThreadingHTTPServer(("127.0.0.1", 0), H)
        self.httpd.daemon_threads = True
        self.tls = bool(certfile)
        if certfile:
            ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
            ctx.load_cert_chain(certfile, keyfile)
            self.httpd.socket = ctx.wrap_socket(self.httpd.socket, server_side=True)
        self.port = self.httpd.server_address[1]
        self.thread = threading.Thread(target=self.httpd.serve_forever, daemon=True)

    @property
    def endpoint(self) -> str:
        return f"{'https' if self.tls else 'http'}://localhost:{self.port}/{ACCOUNT}"

    def start(self) -> FakeDfs:
        self.thread.start()
        return self

    def stop(self) -> None:
        self.httpd.shutdown()
        self.httpd.server_close()
        self.thread.join(5)

    # ---------------------------------------------------------------- auth
    def _authorised(self, h: BaseHTTPRequestHandler, u, q: dict[str, list[str]]) -> bool:
        a = h.headers.get("Authorization", "")
        if self.auth == "sas":
            return q.get("sig", [""])[0] == parse_qs(SAS)["sig"][0] and q.get("sv", [""])[0] == parse_qs(SAS)["sv"][0]
        if self.auth == "bearer":
            return a == f"Bearer {BEARER}"
        if not a.startswith(f"SharedKey {ACCOUNT}:"):
            return False
        hv = lambda n: h.headers.get(n, "")  # noqa: E731
        cl = hv("Content-Length")
        cl = "" if cl == "0" else cl
        ms = sorted((k.lower(), " ".join(v.split())) for k, v in h.headers.items() if k.lower().startswith("x-ms-"))
        canon_h = "".join(f"{k}:{v}\n" for k, v in ms)
        canon_q = "".join(f"\n{k.lower()}:{','.join(sorted(v))}" for k, v in sorted(q.items(), key=lambda kv: kv[0].lower()))
        sts = "\n".join([h.command, hv("Content-Encoding"), hv("Content-Language"), cl, hv("Content-MD5"), hv("Content-Type"),
                         "" if any(k == "x-ms-date" for k, _ in ms) else hv("Date"), hv("If-Modified-Since"), hv("If-Match"),
                         hv("If-None-Match"), hv("If-Unmodified-Since"), hv("Range"), canon_h + f"/{ACCOUNT}{u.path}{canon_q}"])
        sig = base64.b64encode(hmac.new(base64.b64decode(ACCOUNT_KEY), sts.encode(), hashlib.sha256).digest()).decode()
        return hmac.compare_digest(a.split(":", 1)[1], sig)

    # ---------------------------------------------------------------- plumbing
    @staticmethod
    def _send(h: BaseHTTPRequestHandler, status: int, body: bytes = b"", headers: dict[str, str] | None = None, head: bool = False) -> int:
        h.send_response(status)
        hs = {"x-ms-version": "2022-11-02", "Date": formatdate(usegmt=True), **(headers or {})}
        hs.setdefault("Content-Length", str(len(body)))
        for k, v in hs.items():
            h.send_header(k, v)
        h.end_headers()
        if body and not head:
            h.wfile.write(body)
        return status

    def _err(self, h: BaseHTTPRequestHandler, status: int, code: str, msg: str = "") -> int:
        body = json.dumps({"error": {"code": code, "message": msg or code}}).encode()
        return self._send(h, status, body, {"x-ms-error-code": code, "Content-Type": "application/json"}, head=h.command == "HEAD")

    def handle(self, h: BaseHTTPRequestHandler) -> None:
        n = int(h.headers.get("Content-Length") or 0)
        body = h.rfile.read(n) if n else b""
        u = urlparse(h.path)
        q = parse_qs(u.query, keep_blank_values=True)
        status = 500
        try:
            if not self._authorised(h, u, q):
                status = self._err(h, 403, "AuthenticationFailed", "Server failed to authenticate the request")
            else:
                with self.lock:
                    status = self.dispatch(h, u, q, body)
        except Exception as exc:  # noqa: BLE001
            status = self._err(h, 500, "InternalError", repr(exc))
        self.log.append((h.command, u.path, status))

    # ---------------------------------------------------------------- operations
    def dispatch(self, h: BaseHTTPRequestHandler, u, q: dict[str, list[str]], body: bytes) -> int:
        segs = [unquote(s) for s in u.path.split("/") if s]
        if segs and segs[0] == ACCOUNT:
            segs = segs[1:]
        if not segs:
            return self._err(h, 400, "InvalidUri")
        cont, path = segs[0], "/".join(segs[1:])
        if cont not in self.files:
            return self._err(h, 404, "FilesystemNotFound", f"container {cont}")
        files, dirs = self.files[cont], self.dirs[cont]
        m = h.command

        def is_dir(p: str) -> bool:
            return p in dirs or any(f.startswith(p + "/") for f in files) or any(d.startswith(p + "/") for d in dirs)

        def mkparents(p: str) -> None:
            parts = p.split("/")[:-1]
            for i in range(1, len(parts) + 1):
                dirs.add("/".join(parts[:i]))

        if not path:  # container level
            if m in ("HEAD", "GET") and q.get("resource") == ["filesystem"]:
                d = (q.get("directory") or [""])[0].strip("/")
                rec = (q.get("recursive") or ["true"])[0] == "true"
                if d and not is_dir(d):
                    return self._err(h, 404, "PathNotFound")
                out = []
                names = {*files, *dirs}
                for p in sorted(names):
                    if d and not p.startswith(d + "/"):
                        continue
                    if not rec and "/" in (p[len(d) + 1:] if d else p):
                        continue
                    if p in dirs or p not in files:
                        out.append({"name": p, "isDirectory": "true", "lastModified": formatdate(usegmt=True), "eTag": '"0x1"'})
                    else:
                        out.append({"name": p, "contentLength": str(len(files[p])), "lastModified": formatdate(usegmt=True), "eTag": '"0x2"'})
                return self._send(h, 200, json.dumps({"paths": out}).encode(), {"Content-Type": "application/json"}, head=m == "HEAD")
            if m in ("HEAD", "GET"):
                return self._send(h, 200, b"", {"ETag": '"0x1"', "Last-Modified": formatdate(usegmt=True)}, head=True)
            return self._err(h, 400, "UnsupportedOperation")

        if m == "PUT":
            src = h.headers.get("x-ms-rename-source")
            if src:
                sp = unquote(src).strip("/")
                sp = sp.split("/", 1)[1] if "/" in sp else ""
                if sp in files:
                    files[path] = files.pop(sp)
                    mkparents(path)
                elif is_dir(sp):
                    for f in [f for f in files if f.startswith(sp + "/")]:
                        files[path + f[len(sp):]] = files.pop(f)
                    for d in [d for d in dirs if d == sp or d.startswith(sp + "/")]:
                        dirs.discard(d)
                        dirs.add(path + d[len(sp):])
                    dirs.add(path)
                    mkparents(path)
                else:
                    return self._err(h, 404, "SourcePathNotFound")
                return self._send(h, 201)
            res = (q.get("resource") or [""])[0]
            if res == "directory":
                dirs.add(path)
                mkparents(path)
            elif res == "file":
                files[path] = b""
                self.pending[(cont, path)] = bytearray()
                mkparents(path)
            else:
                return self._err(h, 400, "InvalidQueryParameterValue")
            return self._send(h, 201)
        if m == "PATCH":
            action = (q.get("action") or [""])[0]
            key = (cont, path)
            if key not in self.pending:
                return self._err(h, 404, "PathNotFound")
            if action == "append":
                pos = int((q.get("position") or ["0"])[0])
                if pos != len(self.pending[key]):
                    return self._err(h, 400, "InvalidFlushPosition", f"position {pos} != {len(self.pending[key])}")
                self.pending[key].extend(body)
                return self._send(h, 202)
            if action == "flush":
                pos = int((q.get("position") or ["0"])[0])
                if pos != len(self.pending[key]):
                    return self._err(h, 400, "InvalidFlushPosition")
                files[path] = bytes(self.pending[key])
                return self._send(h, 200)
            return self._err(h, 400, "UnsupportedOperation")
        if m == "DELETE":
            rec = (q.get("recursive") or ["false"])[0] == "true"
            if path in files:
                del files[path]
                self.pending.pop((cont, path), None)
                return self._send(h, 200)
            if is_dir(path):
                kids = [f for f in files if f.startswith(path + "/")] + [d for d in dirs if d.startswith(path + "/")]
                if kids and not rec:
                    return self._err(h, 409, "DirectoryNotEmpty")
                for f in [f for f in files if f.startswith(path + "/")]:
                    del files[f]
                for d in [d for d in dirs if d == path or d.startswith(path + "/")]:
                    dirs.discard(d)
                return self._send(h, 200)
            return self._err(h, 404, "PathNotFound")
        if m in ("HEAD", "GET"):
            if path in files:
                data = files[path]
                hdr = {"ETag": '"0x2"', "Last-Modified": formatdate(usegmt=True), "x-ms-blob-type": "BlockBlob", "x-ms-resource-type": "file",
                       "Accept-Ranges": "bytes", "Content-Type": "application/octet-stream"}
                rng = h.headers.get("Range") or h.headers.get("x-ms-range")
                if m == "GET" and rng and rng.startswith("bytes="):
                    a, _, b = rng[6:].partition("-")
                    start = int(a)
                    if start >= len(data) and len(data) > 0 or len(data) == 0:
                        return self._err(h, 416, "InvalidRange")
                    end = min(int(b), len(data) - 1) if b else len(data) - 1
                    part = data[start:end + 1]
                    return self._send(h, 206, part, {**hdr, "Content-Range": f"bytes {start}-{end}/{len(data)}"})
                if m == "HEAD":
                    hdr["Content-Length"] = str(len(data))
                    return self._send(h, 200, b"", hdr, head=True)
                return self._send(h, 200, data, hdr)
            if is_dir(path):
                hdr = {"ETag": '"0x1"', "Last-Modified": formatdate(usegmt=True), "x-ms-meta-hdi_isfolder": "true", "x-ms-resource-type": "directory",
                       "x-ms-blob-type": "BlockBlob", "Content-Length": "0"}
                return self._send(h, 200, b"", hdr, head=True)
            return self._err(h, 404, "BlobNotFound" if m != "HEAD" else "PathNotFound")
        return self._err(h, 400, "UnsupportedOperation")
