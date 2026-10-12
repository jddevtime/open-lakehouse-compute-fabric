# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Test doubles for the NODE package: a local Maven repository, a fake Adoptium API, hand-built wheels and a fake runtime directory.

Everything here is real I/O against real local servers/files (no mocks of our own code): the production resolver, installer and
agent talk to these exactly as they would talk to Maven Central, api.adoptium.net or a PyPI mirror.
"""

from __future__ import annotations

import hashlib
import http.server
import io
import json
import os
import socketserver
import stat
import sys
import tarfile
import textwrap
import threading
import zipfile
from contextlib import contextmanager
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[2]


# ------------------------------------------------------------------------------------------------ http helpers
class _QuietHandler(http.server.SimpleHTTPRequestHandler):
    def log_message(self, format: str, *args: Any) -> None:  # noqa: A002
        pass


class _Server(socketserver.ThreadingMixIn, http.server.HTTPServer):
    daemon_threads = True
    allow_reuse_address = True


@contextmanager
def serve_directory(root: Path):
    """Serve ``root`` over http on 127.0.0.1 (yields the base URL, no trailing slash)."""
    handler = type("H", (_QuietHandler,), {"directory": str(root)})

    def factory(*a: Any, **kw: Any) -> _QuietHandler:
        return handler(*a, directory=str(root), **kw)

    srv = _Server(("127.0.0.1", 0), factory)
    t = threading.Thread(target=srv.serve_forever, daemon=True)
    t.start()
    try:
        yield f"http://127.0.0.1:{srv.server_address[1]}"
    finally:
        srv.shutdown()
        srv.server_close()


# ------------------------------------------------------------------------------------------------ maven
class FakeMavenRepo:
    """Writes a Maven-layout repository (poms, jars, ``.sha1``/``.sha256``) under ``root``."""

    def __init__(self, root: Path) -> None:
        self.root = Path(root)
        self.root.mkdir(parents=True, exist_ok=True)

    def _dir(self, g: str, a: str, v: str) -> Path:
        d = self.root / g.replace(".", "/") / a / v
        d.mkdir(parents=True, exist_ok=True)
        return d

    def _write(self, path: Path, data: bytes, *, checksums: bool = True) -> None:
        path.write_bytes(data)
        if checksums:
            Path(str(path) + ".sha1").write_text(hashlib.sha1(data).hexdigest() + "\n")  # noqa: S324 - Maven checksum format
            Path(str(path) + ".sha256").write_text(hashlib.sha256(data).hexdigest() + "\n")

    def add(self, g: str, a: str, v: str, *, deps: list[tuple] | None = None, parent: tuple[str, str, str] | None = None, props: dict[str, str] | None = None,
            managed: list[tuple[str, str, str]] | None = None, packaging: str = "jar", jar: bytes | None = None, checksums: bool = True) -> bytes:
        """``deps`` items: ``(g, a, v|None, scope|None, optional, [(excl_g, excl_a), ...])`` (trailing members optional)."""
        d = self._dir(g, a, v)
        lines = ['<?xml version="1.0"?>', '<project xmlns="http://maven.apache.org/POM/4.0.0">', "  <modelVersion>4.0.0</modelVersion>"]
        if parent:
            lines.append(f"  <parent><groupId>{parent[0]}</groupId><artifactId>{parent[1]}</artifactId><version>{parent[2]}</version></parent>")
        lines += [f"  <groupId>{g}</groupId>", f"  <artifactId>{a}</artifactId>", f"  <version>{v}</version>", f"  <packaging>{packaging}</packaging>"]
        if props:
            lines.append("  <properties>" + "".join(f"<{k}>{val}</{k}>" for k, val in props.items()) + "</properties>")
        if managed:
            lines.append("  <dependencyManagement><dependencies>" + "".join(
                f"<dependency><groupId>{mg}</groupId><artifactId>{ma}</artifactId><version>{mv}</version></dependency>" for mg, ma, mv in managed) + "</dependencies></dependencyManagement>")
        if deps:
            lines.append("  <dependencies>")
            for dep in deps:
                dg, da, dv = dep[0], dep[1], dep[2]
                scope = dep[3] if len(dep) > 3 else None
                optional = dep[4] if len(dep) > 4 else False
                excl = dep[5] if len(dep) > 5 else []
                s = f"    <dependency><groupId>{dg}</groupId><artifactId>{da}</artifactId>"
                if dv:
                    s += f"<version>{dv}</version>"
                if scope:
                    s += f"<scope>{scope}</scope>"
                if optional:
                    s += "<optional>true</optional>"
                if excl:
                    s += "<exclusions>" + "".join(f"<exclusion><groupId>{eg}</groupId><artifactId>{ea}</artifactId></exclusion>" for eg, ea in excl) + "</exclusions>"
                lines.append(s + "</dependency>")
            lines.append("  </dependencies>")
        lines.append("</project>")
        self._write(d / f"{a}-{v}.pom", "\n".join(lines).encode(), checksums=checksums)
        data = jar if jar is not None else _jar_bytes(f"{g}:{a}:{v}")
        if packaging == "jar":
            self._write(d / f"{a}-{v}.jar", data, checksums=checksums)
        return data

    def jar_path(self, g: str, a: str, v: str) -> Path:
        return self._dir(g, a, v) / f"{a}-{v}.jar"

    def corrupt_jar(self, g: str, a: str, v: str) -> None:
        """Change the jar's bytes but keep the published checksum files (a tampered mirror)."""
        p = self.jar_path(g, a, v)
        p.write_bytes(p.read_bytes() + b"TAMPERED")

    def drop_checksums(self, g: str, a: str, v: str) -> None:
        for ext in (".sha1", ".sha256"):
            Path(str(self.jar_path(g, a, v)) + ext).unlink(missing_ok=True)


def _jar_bytes(label: str) -> bytes:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        zf.writestr("META-INF/MANIFEST.MF", f"Manifest-Version: 1.0\nImplementation-Title: {label}\n")
        zf.writestr("data.txt", label)
    return buf.getvalue()


# ------------------------------------------------------------------------------------------------ adoptium
class FakeAdoptium:
    """Serves ``/v3/assets/latest/<major>/hotspot`` and a JDK archive whose ``bin/java`` is a tiny script.

    ``tamper=True`` serves an archive whose sha256 differs from the one the API announces (the installer must refuse it).
    """

    def __init__(self, root: Path, *, major: int = 21, tamper: bool = False) -> None:
        self.root, self.major, self.tamper = Path(root), major, tamper
        self.root.mkdir(parents=True, exist_ok=True)
        self.release = f"jdk-{major}.0.8+9"
        self.archive_name = f"OpenJDK{major}U-jdk_x64_linux_hotspot_{major}.0.8_9.tar.gz"
        self._build_archive()

    def _build_archive(self) -> None:
        script = textwrap.dedent(f"""\
            #!/bin/sh
            HOME_DIR="$(cd "$(dirname "$0")/.." && pwd)"
            if [ "$1" = "-XshowSettings:properties" ]; then
              echo "Property settings:" >&2
              echo "    java.home = $HOME_DIR" >&2
              echo "    java.specification.version = {self.major}" >&2
              echo "    java.version = {self.major}.0.8" >&2
              echo "    java.vendor = Eclipse Adoptium" >&2
            fi
            exit 0
            """).encode()
        buf = io.BytesIO()
        with tarfile.open(fileobj=buf, mode="w:gz") as tf:
            ti = tarfile.TarInfo(f"{self.release}/bin/java")
            ti.size, ti.mode = len(script), 0o755
            tf.addfile(ti, io.BytesIO(script))
            rel = b"IMPLEMENTOR=Eclipse Adoptium\n"
            ti2 = tarfile.TarInfo(f"{self.release}/release")
            ti2.size, ti2.mode = len(rel), 0o644
            tf.addfile(ti2, io.BytesIO(rel))
        self.data = buf.getvalue()
        self.sha256 = hashlib.sha256(self.data).hexdigest()
        (self.root / "files").mkdir(exist_ok=True)
        (self.root / "files" / self.archive_name).write_bytes(self.data + (b"EVIL" if self.tamper else b""))

    @contextmanager
    def serve(self):
        outer = self

        class H(_QuietHandler):
            def do_GET(self) -> None:  # noqa: N802
                if self.path.startswith("/v3/assets/latest/") or self.path.startswith("/v3/assets/release_name/"):
                    doc = [{"release_name": outer.release, "binary": {"package": {"name": outer.archive_name, "checksum": outer.sha256,
                                                                                   "link": f"http://127.0.0.1:{self.server.server_address[1]}/files/{outer.archive_name}"}}}]
                    body = json.dumps(doc).encode()
                    self.send_response(200)
                    self.send_header("Content-Type", "application/json")
                    self.send_header("Content-Length", str(len(body)))
                    self.end_headers()
                    self.wfile.write(body)
                    return
                super().do_GET()

        def factory(*a: Any, **kw: Any) -> H:
            return H(*a, directory=str(outer.root), **kw)

        srv = _Server(("127.0.0.1", 0), factory)
        threading.Thread(target=srv.serve_forever, daemon=True).start()
        try:
            yield f"http://127.0.0.1:{srv.server_address[1]}"
        finally:
            srv.shutdown()
            srv.server_close()


# ------------------------------------------------------------------------------------------------ wheels
def build_wheel(dest_dir: Path, name: str, version: str, files: dict[str, str] | None = None, *, requires: list[str] | None = None) -> Path:
    """Hand-build a pure-python wheel (zip + METADATA + WHEEL + RECORD) so tests can pin and install it offline with ``--require-hashes``."""
    dest_dir.mkdir(parents=True, exist_ok=True)
    norm = name.replace("-", "_")
    dist = f"{norm}-{version}.dist-info"
    files = files if files is not None else {f"{norm}/__init__.py": f"__version__ = '{version}'\n"}
    meta = f"Metadata-Version: 2.1\nName: {name}\nVersion: {version}\n" + "".join(f"Requires-Dist: {r}\n" for r in (requires or []))
    wheel = "Wheel-Version: 1.0\nGenerator: olcf-tests\nRoot-Is-Purelib: true\nTag: py3-none-any\n"
    members = {**files, f"{dist}/METADATA": meta, f"{dist}/WHEEL": wheel}
    record = "".join(f"{n},sha256={_b64(hashlib.sha256(c.encode()).digest())},{len(c.encode())}\n" for n, c in members.items()) + f"{dist}/RECORD,,\n"
    path = dest_dir / f"{norm}-{version}-py3-none-any.whl"
    with zipfile.ZipFile(path, "w", zipfile.ZIP_DEFLATED) as zf:
        for n, c in members.items():
            zf.writestr(zipfile.ZipInfo(n, (2020, 1, 1, 0, 0, 0)), c)
        zf.writestr(zipfile.ZipInfo(f"{dist}/RECORD", (2020, 1, 1, 0, 0, 0)), record)
    return path


def _b64(raw: bytes) -> str:
    import base64

    return base64.urlsafe_b64encode(raw).rstrip(b"=").decode()


# ------------------------------------------------------------------------------------------------ fake runtime directory
def make_fake_runtime(state_dir: Path, runtime_id: str = "olcf-rt-fake-1", *, family: str = "olcf-native", fake_spark: bool = False) -> Path:
    """A runtime directory that ``RuntimeManager.light_check``/``handle`` accept and whose ``venv/bin/python`` runs *this* interpreter
    with the repository's ``src`` on its path. Lets agent/job tests run real runner processes without installing pyspark.

    The venv look-alike works because Python locates ``pyvenv.cfg`` next to the *invoked* executable (not its resolved target); a
    ``.pth`` file in its (empty) site-packages adds the repo sources and the host's site-packages.

    ``fake_spark=True`` also installs :data:`FAKE_SPARK_CLASS` as ``<pyspark>/bin/spark-class`` so the daemon manager can be tested
    end to end (real processes, real sockets, real health polling) without a JVM; see that constant for its behaviour switches.
    """
    root = Path(state_dir) / "runtimes" / runtime_id
    venv = root / "venv"
    exe_dir = venv / ("Scripts" if os.name == "nt" else "bin")
    exe_dir.mkdir(parents=True, exist_ok=True)
    py = exe_dir / ("python.exe" if os.name == "nt" else "python")
    if not py.exists():
        py.symlink_to(sys.executable)
    host_cfg = Path(sys.prefix) / "pyvenv.cfg"
    (venv / "pyvenv.cfg").write_text(host_cfg.read_text() if host_cfg.exists() else f"home = {Path(sys.executable).parent}\ninclude-system-site-packages = false\n")
    ver = f"python{sys.version_info.major}.{sys.version_info.minor}"
    site = venv / "lib" / ver / "site-packages"
    site.mkdir(parents=True, exist_ok=True)
    import site as _site

    extra = [str(REPO_ROOT / "src"), *[p for p in _site.getsitepackages() if Path(p).is_dir()]]
    (site / "olcf-dev.pth").write_text("\n".join(extra) + "\n")
    base = json.loads((REPO_ROOT / "src/olcf/runtime/manifests/olcf-rt-native-1.0.0.json").read_text())
    base.update({"id": runtime_id, "family": family})
    (root / "manifest.json").write_text(json.dumps(base))
    lock = {"schema": "olcf.runtime.lock/1", "spark": {"version": "4.1.1", "pyspark_dir": f"venv/lib/{ver}/site-packages/pyspark"}, "jars": [], "java": {}, "packages": []}
    (root / "lock.json").write_text(json.dumps(lock))
    (root / "VERIFIED").write_text(json.dumps({"verified_at": "2026-01-01T00:00:00Z"}))
    (root / "jars").mkdir(exist_ok=True)
    try:
        os.chmod(py, py.stat().st_mode | stat.S_IXUSR)
    except OSError:
        pass
    if fake_spark and os.name != "nt":
        bindir = site / "pyspark" / "bin"
        bindir.mkdir(parents=True, exist_ok=True)
        launcher = bindir / "spark-class"
        launcher.write_text(f"#!{sys.executable}\n" + FAKE_SPARK_CLASS)
        launcher.chmod(0o755)
    return root


#: A stand-in for ``$SPARK_HOME/bin/spark-class``: parses the same arguments the agent passes to the real launcher, records the
#: invocation (argv + a few environment variables) to ``$SPARK_LOG_DIR/fake-invocation.json``, listens on the RPC and web UI
#: ports and answers ``GET /json/`` like the standalone Master / Worker web UIs. Behaviour switches live in the daemon directory
#: (``$SPARK_PID_DIR/fake-mode``): ``crash`` exits(1) immediately, ``slow:<s>`` delays listening, ``ignore-term`` ignores SIGTERM,
#: ``dead`` listens but reports ``status: DEAD`` (master) - the *agent's* environment never reaches this process.
FAKE_SPARK_CLASS = r'''
import json, os, signal, socket, sys, threading, time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

argv = sys.argv[1:]
cls = argv[0]
opts, positional, i = {}, [], 1
while i < len(argv):
    if argv[i].startswith("--"):
        opts[argv[i][2:]] = argv[i + 1]
        i += 2
    else:
        positional.append(argv[i])
        i += 1
log_dir = os.environ["SPARK_LOG_DIR"]
mode = ""
try:
    mode = open(os.path.join(os.environ["SPARK_PID_DIR"], "fake-mode")).read().strip()
except OSError:
    pass
count_file = os.path.join(os.environ["SPARK_PID_DIR"], "fake-launches")
try:
    n = int(open(count_file).read()) + 1
except (OSError, ValueError):
    n = 1
open(count_file, "w").write(str(n))
props = ""
try:
    props = open(opts.get("properties-file", "")).read()
except OSError:
    pass
json.dump({"argv": argv, "launch": n, "props": props,
           "env": {k: os.environ.get(k) for k in ("SPARK_CONF_DIR", "SPARK_DAEMON_MEMORY", "SPARK_LOCAL_IP", "SPARK_PUBLIC_DNS", "JAVA_HOME", "SPARK_HOME",
                                                  "SPARK_NO_DAEMONIZE", "AWS_SECRET_ACCESS_KEY", "OLCF_WORKER_TOKEN", "HOME")}},
          open(os.path.join(log_dir, "fake-invocation.json"), "w"))
print("fake spark-class", cls, flush=True)
if mode == "crash":
    print("fake crash", file=sys.stderr, flush=True)
    sys.exit(1)
if mode.startswith("slow:"):
    time.sleep(float(mode.split(":")[1]))
if mode == "ignore-term":
    signal.signal(signal.SIGTERM, signal.SIG_IGN)
is_master = cls.endswith(".Master")
host = opts.get("host", "127.0.0.1")
if is_master:
    rpc = socket.socket(); rpc.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    rpc.bind((host, int(opts["port"]))); rpc.listen(16)
    doc = {"status": "DEAD" if mode == "dead" else "ALIVE", "url": f"spark://{host}:{opts['port']}", "workers": [], "cores": 0, "coresused": 0}
else:
    doc = {"id": f"worker-20260101000000-{host}-41234", "masterurl": positional[-1], "cores": int(opts["cores"]), "coresused": 0, "memory": opts["memory"]}

class H(BaseHTTPRequestHandler):
    def do_GET(self):
        body = json.dumps(doc).encode()
        if self.path == "/json":   # Spark 4 redirects the slash-less path
            self.send_response(302); self.send_header("Location", "/json/"); self.send_header("Content-Length", "0"); self.end_headers()
            return
        self.send_response(200 if self.path.rstrip("/") == "/json" else 404)
        self.send_header("Content-Length", str(len(body) if self.path.rstrip("/") == "/json" else 0)); self.end_headers()
        if self.path.rstrip("/") == "/json":
            self.wfile.write(body)
    def log_message(self, *a):
        pass

import socketserver


class Srv(ThreadingHTTPServer):
    def server_bind(self):
        # HTTPServer.server_bind asks the resolver for the host's name (socket.getfqdn); on macOS CI hosts that can take longer than the daemon's ready timeout
        socketserver.TCPServer.server_bind(self)
        self.server_name, self.server_port = host, self.server_address[1]


Srv((host, int(opts["webui-port"])), H).serve_forever()
'''
