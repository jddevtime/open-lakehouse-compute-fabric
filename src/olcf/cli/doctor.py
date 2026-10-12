# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Environment checks behind ``olcf doctor`` and the first-run summary.

Each check returns a :class:`Check` with a status (``ok`` / ``warn`` / ``fail`` / ``info``), a one-line detail and - for anything not ``ok`` - an
actionable ``fix``.  Checks never execute a macOS installer stub (see :mod:`olcf.cli.hostsafe`) and never need developer tools.
"""

from __future__ import annotations

import os
import platform
import shutil
import socket
import sqlite3
import sys
from collections.abc import Callable
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

from olcf.cli import hostsafe

OK, WARN, FAIL, INFO = "ok", "warn", "fail", "info"
MIN_PYTHON = (3, 11)
MIN_FREE_GB_FAIL = 3.0
MIN_FREE_GB_WARN = 10.0


@dataclass
class Check:
    """The result of one ``olcf doctor`` check: a name, a status, the detail found and, when something is wrong, how to fix it."""
    name: str
    status: str
    detail: str
    fix: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {k: v for k, v in asdict(self).items() if v is not None}


def check_python() -> Check:
    v = sys.version_info
    if (v.major, v.minor) < MIN_PYTHON:
        return Check("python", FAIL, f"Python {platform.python_version()} at {sys.executable}", "OLCF needs Python >= 3.11; reinstall with the installer (it brings its own Python via uv)")
    if hostsafe.is_apple_stub(sys.executable):
        return Check("python", FAIL, f"{sys.executable} is the macOS installer stub", "reinstall OLCF with install.sh (uses uv's standalone Python), or use the Docker route")
    return Check("python", OK, f"{platform.python_implementation()} {platform.python_version()} ({sys.executable})")


def check_platform() -> Check:
    extra = ""
    if hostsafe.is_macos():
        extra = "; no Xcode Command Line Tools needed" if not hostsafe.developer_tools_installed() else "; developer tools present (not required)"
    return Check("platform", INFO, f"{platform.system()} {platform.release()} {platform.machine()}{extra}")


def check_uv() -> Check:
    uv = hostsafe.safe_which("uv") or str(Path.home() / ".local" / "bin" / "uv")
    if Path(uv).exists():
        return Check("uv", OK, uv)
    return Check("uv", WARN, "uv not found (only needed to install Python/runtimes)", "curl -LsSf https://astral.sh/uv/install.sh | sh   (or re-run the OLCF installer)")


def check_jdk(supported: tuple[int, ...] = (17, 21), state_dirs: list[Path] | None = None) -> Check:
    jdks = hostsafe.find_jdks(state_dirs)
    good = [j for j in jdks if j.major in supported]
    if good:
        j = good[0]
        return Check("jdk", OK, f"Java {j.version} ({j.vendor or 'unknown vendor'}) at {j.home} [{j.source}]")
    have = ", ".join(f"{j.major}" for j in jdks) or "none"
    return Check("jdk", WARN, f"no Java {'/'.join(map(str, supported))} found (found: {have}); Spark jobs need one, SQL/notebooks on DuckDB do not",
                 "olcf runtime install --with-jdk   (downloads a SHA-256 verified Eclipse Temurin 21 into the OLCF data directory)")


def port_in_use(host: str, port: int) -> bool:
    for fam, addr in ((socket.AF_INET, "127.0.0.1" if host in ("0.0.0.0", "localhost", "") else host), (socket.AF_INET6, "::1")):  # noqa: S104
        try:
            with socket.socket(fam, socket.SOCK_STREAM) as s:
                s.settimeout(0.5)
                if s.connect_ex((addr, port)) == 0:
                    return True
        except OSError:
            continue
    return False


def port_owner(port: int) -> str | None:
    try:
        import psutil

        for c in psutil.net_connections(kind="tcp"):
            if c.laddr and c.laddr.port == port and c.status == psutil.CONN_LISTEN and c.pid:
                p = psutil.Process(c.pid)
                return f"{p.name()} (pid {c.pid})"
    except Exception:  # noqa: BLE001 - needs privileges on some platforms
        return None
    return None


def check_ports(host: str, port: int, server_running: bool) -> list[Check]:
    out = []
    if port_in_use(host, port):
        if server_running:
            out.append(Check(f"port {port}", OK, "in use by the running OLCF server"))
        else:
            owner = port_owner(port)
            out.append(Check(f"port {port}", FAIL, f"already in use{' by ' + owner if owner else ''}", f"stop that program, or start OLCF elsewhere: olcf serve --port {port + 1}"))
    else:
        out.append(Check(f"port {port}", OK, "free"))
    busy = [p for p in (7077, 18080) if port_in_use("127.0.0.1", p)]
    if busy:
        out.append(Check("spark ports", WARN, f"port(s) {', '.join(map(str, busy))} already in use (Spark master/UI defaults; OLCF picks the next free port)", None))
    return out


def check_disk(path: Path) -> Check:
    probe = path
    while not probe.exists() and probe != probe.parent:
        probe = probe.parent
    free = shutil.disk_usage(probe).free / 1024**3
    if free < MIN_FREE_GB_FAIL:
        return Check("disk", FAIL, f"{free:.1f} GiB free at {probe}", "free up space (a Spark runtime needs ~3 GiB) or use --data-dir on a larger volume")
    if free < MIN_FREE_GB_WARN:
        return Check("disk", WARN, f"{free:.1f} GiB free at {probe}", "keep >10 GiB free for runtimes, job logs and data")
    return Check("disk", OK, f"{free:.0f} GiB free at {probe}")


def check_data_dir(path: Path) -> list[Check]:
    out: list[Check] = []
    if not path.exists():
        return [Check("data dir", INFO, f"{path} does not exist yet", "olcf init   (or olcf quickstart)")]
    if not os.access(path, os.W_OK):
        return [Check("data dir", FAIL, f"{path} is not writable", "chown/chmod the directory or choose --data-dir")]
    out.append(Check("data dir", OK, str(path)))
    sec = path / "secrets"
    if sec.exists() and os.name == "posix":
        mode = sec.stat().st_mode & 0o777
        if mode & 0o077:
            out.append(Check("secrets permissions", WARN, f"{sec} is mode {mode:o}", f"chmod 700 {sec}"))
    return out


def check_database(settings: Any) -> Check:
    url = settings.database_url
    redacted = url if url.startswith("sqlite") else url.split("@")[-1]
    if url.startswith("sqlite"):
        db = Path(url.replace("sqlite:///", "", 1))
        if not db.exists():
            return Check("database", INFO, f"SQLite {db} (not created yet)", "olcf init")
        try:
            con = sqlite3.connect(f"file:{db}?mode=ro", uri=True, timeout=5)
            n = con.execute("select count(*) from schema_migrations").fetchone()[0]
            con.close()
            return Check("database", OK, f"SQLite {db} ({n} migrations applied)")
        except sqlite3.Error as exc:
            return Check("database", FAIL, f"SQLite {db}: {exc}", "olcf db status; restore from a backup if the file is damaged")
    try:
        from sqlalchemy import create_engine, text

        eng = create_engine(url.replace("postgresql://", "postgresql+psycopg://", 1), connect_args={"connect_timeout": 5})
        with eng.connect() as c:
            c.execute(text("select 1"))
        eng.dispose()
        return Check("database", OK, f"PostgreSQL {redacted}")
    except Exception as exc:  # noqa: BLE001
        return Check("database", FAIL, f"PostgreSQL {redacted}: {type(exc).__name__}: {str(exc)[:150]}", "check db.url, network and credentials; the psycopg driver is in `pip install olcf[postgres]`")


def check_runtime(state_dirs: list[Path]) -> Check:
    try:
        from olcf.runtime.manager import ManagerConfig, RuntimeManager

        for d in state_dirs:
            if (d / "runtimes").is_dir():
                mgr = RuntimeManager(ManagerConfig(state_dir=d))
                ready = mgr.ready_ids()
                if ready:
                    return Check("runtime", OK, f"{', '.join(ready)} verified in {d}")
        return Check("runtime", WARN, "no Spark runtime installed (SQL on DuckDB and the UI work; Spark jobs need a runtime)", "olcf runtime install --with-jdk")
    except Exception as exc:  # noqa: BLE001
        return Check("runtime", WARN, f"could not inspect runtimes: {exc}", None)


def check_network_env() -> Check:
    bits = [f"{k}={'set' if os.environ.get(k) else 'unset'}" for k in ("HTTPS_PROXY", "HTTP_PROXY", "NO_PROXY")]
    ca = next((k for k in ("OLCF_CA_BUNDLE", "SSL_CERT_FILE", "REQUESTS_CA_BUNDLE") if os.environ.get(k)), None)
    return Check("network", INFO, ", ".join(bits) + (f", CA bundle from {ca}" if ca else ""))


def check_container_route() -> Check:
    d = hostsafe.safe_which("docker")
    return Check("docker", INFO, f"{d} (the all-in-one container route is available: `docker compose --profile allinone up`)" if d else "not installed (optional alternative route)")


def check_tls(settings: Any) -> Check | None:
    if not settings.tls.enabled:
        return None
    crt = Path(settings.tls.cert_file) if settings.tls.cert_file else settings.path("pki", "server.crt")
    if not crt.exists():
        return Check("tls", INFO, "TLS enabled; the certificate is issued from the built-in CA on first start")
    try:
        import datetime as dt

        from cryptography import x509

        cert = x509.load_pem_x509_certificate(crt.read_bytes())
        left = (cert.not_valid_after_utc - dt.datetime.now(dt.UTC)).days
        if left < 0:
            return Check("tls", FAIL, f"server certificate expired {-left} days ago", "delete pki/server.crt to re-issue (built-in CA) or replace your certificate")
        if left < 30:
            return Check("tls", WARN, f"server certificate expires in {left} days", "built-in CA certificates are renewed automatically on start; for your own certificate renew it")
        return Check("tls", OK, f"server certificate valid for {left} more days")
    except Exception as exc:  # noqa: BLE001
        return Check("tls", WARN, f"cannot read {crt}: {exc}")


def run_local_checks(settings: Any, *, server_running: bool, port: int | None = None, host: str | None = None, state_dirs: list[Path] | None = None) -> list[Check]:
    """All checks that need no running server."""
    checks: list[Check] = [check_platform(), check_python(), check_uv(), check_jdk(state_dirs=state_dirs)]
    checks += check_data_dir(settings.data_dir)
    checks.append(check_disk(settings.data_dir))
    checks += check_ports(host or settings.server.host, port or settings.server.port, server_running)
    checks.append(check_database(settings))
    checks.append(check_runtime(state_dirs or []))
    tls = check_tls(settings)
    if tls:
        checks.append(tls)
    checks += [check_network_env(), check_container_route()]
    return checks


CheckFn = Callable[[], Check]
