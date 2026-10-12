# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Helpers of the Git tests: local bare repositories, a 'second developer' clone and a smart-HTTP server (``git http-backend``) that requires Basic auth."""

from __future__ import annotations

import base64
import http.server
import os
import subprocess
import threading
from pathlib import Path

GIT_ENV = {"GIT_CONFIG_NOSYSTEM": "1", "GIT_TERMINAL_PROMPT": "0", "HOME": "/nonexistent-home", "GIT_AUTHOR_NAME": "Dev", "GIT_AUTHOR_EMAIL": "dev@example.invalid",
           "GIT_COMMITTER_NAME": "Dev", "GIT_COMMITTER_EMAIL": "dev@example.invalid"}


def git(*args: str, cwd: Path | str | None = None, check: bool = True) -> str:
    env = {"PATH": os.environ["PATH"], **{k: v for k in ("SYSTEMROOT", "TEMP", "TMP") if (v := os.environ.get(k))}, **GIT_ENV}
    r = subprocess.run(["git", *args], cwd=cwd, env=env, capture_output=True, text=True, timeout=60, check=False)  # noqa: S603, S607
    if check and r.returncode:
        raise AssertionError(f"git {' '.join(args)} failed: {r.stderr}")
    return r.stdout


def bare_repo(path: Path, *, branch: str = "main") -> Path:
    git("init", "--bare", "--initial-branch", branch, str(path))
    git("config", "http.receivepack", "true", cwd=path)
    return path


class Dev:
    """A second developer working in a clone of the bare repository."""

    def __init__(self, remote: Path | str, work: Path) -> None:
        self.work = work
        git("clone", "-q", str(remote), str(work))
        git("config", "user.name", "Dev", cwd=work)
        git("config", "user.email", "dev@example.invalid", cwd=work)

    def pull(self, branch: str = "main") -> None:
        git("pull", "-q", "--ff-only", "origin", branch, cwd=self.work)

    def write(self, rel: str, data: str | bytes) -> None:
        p = self.work / rel
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_bytes(data.encode() if isinstance(data, str) else data)

    def read(self, rel: str) -> str:
        return (self.work / rel).read_text()

    def remove(self, rel: str) -> None:
        git("rm", "-q", "-r", rel, cwd=self.work)

    def files(self) -> list[str]:
        return sorted(x for x in git("ls-files", cwd=self.work).splitlines() if x)

    def push(self, message: str = "dev change", branch: str = "main") -> None:
        git("add", "-A", cwd=self.work)
        git("commit", "-q", "-m", message, cwd=self.work)
        git("push", "-q", "origin", f"HEAD:refs/heads/{branch}", cwd=self.work)


class SmartHttp:
    """``git http-backend`` behind http.server on 127.0.0.1 with HTTP Basic auth; records every request line + whether auth was right."""

    def __init__(self, root: Path, user: str, password: str) -> None:
        self.root, self.user, self.password = root, user, password
        self.requests: list[tuple[str, bool]] = []
        outer = self

        class H(http.server.BaseHTTPRequestHandler):
            def log_message(self, *a: object) -> None:  # noqa: D102
                return

            def _serve(self) -> None:
                ok = self.headers.get("Authorization") == "Basic " + base64.b64encode(f"{outer.user}:{outer.password}".encode()).decode()
                outer.requests.append((self.path, ok))
                if not ok:
                    self.send_response(401)
                    self.send_header("WWW-Authenticate", 'Basic realm="git"')
                    self.send_header("Content-Length", "0")
                    self.end_headers()
                    return
                path, _, query = self.path.partition("?")
                n = int(self.headers.get("Content-Length") or 0)
                body = self.rfile.read(n) if n else b""
                env = {"PATH": os.environ["PATH"], "GIT_PROJECT_ROOT": str(outer.root), "GIT_HTTP_EXPORT_ALL": "1", "REQUEST_METHOD": self.command, "PATH_INFO": path,
                       "QUERY_STRING": query, "CONTENT_TYPE": self.headers.get("Content-Type", ""), "CONTENT_LENGTH": str(len(body)), "REMOTE_USER": outer.user,
                       "REMOTE_ADDR": "127.0.0.1", "GIT_CONFIG_NOSYSTEM": "1", "HOME": "/nonexistent-home"}
                if self.headers.get("Content-Encoding"):
                    env["HTTP_CONTENT_ENCODING"] = self.headers["Content-Encoding"]
                if self.headers.get("Git-Protocol"):
                    env["GIT_PROTOCOL"] = self.headers["Git-Protocol"]
                r = subprocess.run(["git", "http-backend"], input=body, env=env, capture_output=True, timeout=60, check=False)  # noqa: S603, S607
                head, _, payload = r.stdout.partition(b"\r\n\r\n")
                status, headers = 200, []
                for line in head.decode("latin1").split("\r\n"):
                    k, _, v = line.partition(":")
                    if k.lower() == "status":
                        status = int(v.strip().split()[0])
                    elif k:
                        headers.append((k, v.strip()))
                self.send_response(status)
                for k, v in headers:
                    self.send_header(k, v)
                self.send_header("Content-Length", str(len(payload)))
                self.end_headers()
                self.wfile.write(payload)

            do_GET = do_POST = _serve  # noqa: N815

        self.server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), H)
        self.port = self.server.server_address[1]
        self._t = threading.Thread(target=self.server.serve_forever, daemon=True)

    def __enter__(self) -> SmartHttp:
        self._t.start()
        return self

    def __exit__(self, *a: object) -> None:
        self.server.shutdown()
        self.server.server_close()

    def url(self, name: str) -> str:
        return f"http://127.0.0.1:{self.port}/{name}"
