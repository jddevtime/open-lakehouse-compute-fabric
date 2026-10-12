# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Install-and-run commands: ``init``, ``serve``, ``stop``, ``quickstart``, ``status``, ``doctor``, ``version``.

These work on the *local installation* (data directory, pid file, runtime manager) and use the SDK only to talk to the server they started.
Nothing here executes ``java``, ``python3`` or ``git`` from ``/usr/bin`` on macOS; see :mod:`olcf.cli.hostsafe`.
"""

from __future__ import annotations

import contextlib
import json
import os
import platform
import signal
import socket
import subprocess
import sys
import threading
import time
import webbrowser
from pathlib import Path
from typing import Any

import click

from olcf.cli import doctor as checks
from olcf.cli import hostsafe
from olcf.cli.common import pass_state
from olcf.cli.state import EXIT_FAIL, EXIT_PREREQ, EXIT_SERVER, State
from olcf.sdk import Client, OlcfSdkError
from olcf.sdk.contexts import ContextStore
from olcf.version import __version__

PID_NAME = "olcf.pid"
CLI_CONTEXT = "local"
CONFIG_TEMPLATE = """# Open Lakehouse Compute Fabric configuration (author: Justin Dodrill) - written by `olcf init`.
#
# Precedence: environment (OLCF_<SECTION>__<KEY>) > this file > defaults, merged key by key: OLCF_SERVER__PORT=9000 changes only
# server.port, the other [server] keys below still apply.

# "development" (written by `olcf init` / `olcf quickstart`) trusts the bundled DEVELOPMENT runtime-signing key, so the shipped Spark runtime installs on a fresh machine.
# That key's private half is public: before this installation serves real users set  environment = "production"  (the development key is then refused), create your
# own key (`python -m olcf.runtime keygen`), re-sign the runtime manifests and set  [runtime.trust] dir = "/etc/olcf/trust"  (the `runtime-trust` health check reports it).
environment = "development"

[server]
host = "{host}"          # "127.0.0.1" = this machine only. Use "0.0.0.0" only behind TLS and a firewall.
port = {port}
{public_url}
[tls]
enabled = {tls}          # true: HTTPS with a certificate from the built-in CA (pki/ca.crt) or your own cert_file/key_file
# cert_file = "/path/server.crt"
# key_file = "/path/server.key"
# extra_sans = ["olcf.example.com"]

[db]
{db_line}

[compute]
embedded_worker = true   # run a worker inside this machine; add more with `olcf worker enroll-token`

[fabric]
enabled = true
# allowed_tenants = ["<entra-tenant-guid>"]
# audiences = ["api://<olcf-app-client-id>"]
"""


# --------------------------------------------------------------------------------------------- pid file helpers
def pid_path(data_dir: Path) -> Path:
    return data_dir / PID_NAME


def read_pidfile(data_dir: Path) -> dict[str, Any] | None:
    """The record of a running server started by ``olcf serve`` (``None`` if absent, stale or the pid was reused)."""
    import psutil

    p = pid_path(data_dir)
    try:
        info = json.loads(p.read_text())
        proc = psutil.Process(int(info["pid"]))
        if abs(proc.create_time() - float(info.get("create_time", proc.create_time()))) > 5 or not proc.is_running() or proc.status() == psutil.STATUS_ZOMBIE:
            raise psutil.NoSuchProcess(proc.pid)
        return info
    except (OSError, ValueError, KeyError, psutil.Error):
        with contextlib.suppress(OSError):
            p.unlink()
        return None


def write_pidfile(data_dir: Path, **info: Any) -> None:
    import psutil

    me = psutil.Process()
    doc = {"pid": me.pid, "create_time": me.create_time(), "started_at": time.time(), "version": __version__, **info}
    data_dir.mkdir(parents=True, exist_ok=True)
    tmp = pid_path(data_dir).with_suffix(".tmp")
    tmp.write_text(json.dumps(doc, indent=2))
    os.replace(tmp, pid_path(data_dir))


def remove_pidfile(data_dir: Path) -> None:
    try:
        if json.loads(pid_path(data_dir).read_text()).get("pid") == os.getpid():
            pid_path(data_dir).unlink()
    except (OSError, ValueError):
        pass


def wait_healthy(url: str, *, timeout: float, proc: subprocess.Popen[bytes] | None = None, ca_bundle: str | None = None) -> bool:
    """Poll ``/v1/health`` until it answers 200 (or the child process died / the timeout passed)."""
    deadline = time.monotonic() + timeout
    c = Client(url, timeout=3.0, max_retries=0, ca_bundle=ca_bundle)
    try:
        while time.monotonic() < deadline:
            if proc is not None and proc.poll() is not None:
                return False
            try:
                c.health()
                return True
            except OlcfSdkError:
                time.sleep(0.4)
    finally:
        c.close()
    return False


def tail_file(path: Path, lines: int = 15) -> str:
    try:
        data = path.read_bytes()[-8192:].decode("utf-8", "replace")
    except OSError:
        return ""
    return "\n".join(data.splitlines()[-lines:])


# --------------------------------------------------------------------------------------------- init
def _toml_str(v: str) -> str:
    return json.dumps(v)  # JSON string syntax is valid TOML basic-string syntax for the characters we emit


def write_config(data_dir: Path, *, host: str, port: int, public_url: str | None, tls: bool, database_url: str | None, force: bool) -> tuple[Path, bool]:
    cfg = data_dir / "config.toml"
    if cfg.exists() and not force:
        return cfg, False
    cfg.write_text(CONFIG_TEMPLATE.format(
        host=host, port=port, tls="true" if tls else "false",
        public_url=f"public_url = {_toml_str(public_url)}\n" if public_url else '# public_url = "https://olcf.example.com"\n',
        db_line=f"url = {_toml_str(database_url)}" if database_url else '# url = "postgresql+psycopg://olcf:password@localhost/olcf"   # default: SQLite in the data directory'),
        encoding="utf-8")
    with contextlib.suppress(OSError):
        os.chmod(cfg, 0o600)
    return cfg, True


def initialise(state: State, *, host: str | None, port: int | None, public_url: str | None, tls: bool | None, database_url: str | None,
               admin_password: str | None, force_config: bool, cli_context: bool, rotate_cli_token: bool) -> dict[str, Any]:
    """Create/migrate the local installation.  Idempotent; returns what was done (and one-time credentials)."""
    from olcf.api.app import create_app
    from olcf.api.core.identity import bootstrap_admin
    from olcf.core.config import load_settings
    from olcf.db import schema as S  # noqa: N812

    dd = state.data_path()
    dd.mkdir(parents=True, exist_ok=True)
    existing = load_settings(dd, log_level="WARNING")
    cfg, wrote = write_config(
        dd, host=host or existing.server.host, port=port or existing.server.port, public_url=public_url or existing.server.public_url,
        tls=existing.tls.enabled if tls is None else tls, database_url=database_url or existing.db.url, force=force_config)
    settings = load_settings(dd, log_level="WARNING")
    result: dict[str, Any] = {"data_dir": str(dd), "config": str(cfg), "config_written": wrote, "database": settings.database_url.split("@")[-1],
                              "url": settings.base_url, "admin_created": False, "version": __version__}
    if not wrote and (note := runtime_trust_notice(settings)):
        result["runtime_trust_note"] = note
    if admin_password is not None and len(admin_password) < settings.auth.password_min_length:
        raise click.UsageError(f"the admin password must have at least {settings.auth.password_min_length} characters")
    app = create_app(settings, module_names=None)
    ctx = app.state.ctx
    try:
        created = bootstrap_admin(ctx, password=admin_password)
        if created:
            row, pw = created
            if admin_password is not None or os.environ.get("OLCF_BOOTSTRAP_ADMIN_PASSWORD"):
                # the operator chose this password and already has it: it is never echoed (a container prints this output into its log) and never written to disk
                result.update(admin_created=True, admin_user=row["name"], admin_password_supplied=True)
            else:
                result.update(admin_created=True, admin_user=row["name"], admin_password=pw)
                secret = settings.path("secrets", "bootstrap-admin.txt")
                fd = os.open(secret, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
                with os.fdopen(fd, "w") as fh:
                    fh.write(f"username={row['name']}\npassword={pw}\n")
                result["credentials_file"] = str(secret)
        if cli_context:
            store = ContextStore()
            current = store.get(CLI_CONTEXT)
            token_ok = False
            if current and current.get("token") and current.get("url") == settings.base_url:
                try:  # a token that this database does not know (another data directory, deleted, expired) is as good as none
                    ctx.tokens.verify_opaque(str(current["token"]), touch=False)
                    token_ok = True
                except Exception:  # noqa: BLE001
                    token_ok = False
            if rotate_cli_token or bool(created) or not current or not token_ok:
                with ctx.db.connect() as conn:
                    from sqlalchemy import select

                    admin_id = conn.execute(select(S.principals.c.id).where(S.principals.c.name == settings.auth.bootstrap_admin,
                                                                           S.principals.c.kind == "user")).scalar()
                if admin_id:
                    plaintext, _rec = ctx.tokens.create_pat(admin_id, "cli-local", ttl_days=365)
                    store.set(CLI_CONTEXT, make_current=store.current in (None, CLI_CONTEXT), url=settings.base_url, token=plaintext,
                              ca_bundle=str(settings.path("pki", "ca.crt")) if settings.tls.enabled else None)
                    result["cli_context"] = CLI_CONTEXT
                else:
                    result["cli_context_note"] = f"the administrator '{settings.auth.bootstrap_admin}' does not exist here; run `olcf login` against the server"
            else:
                result["cli_context"] = CLI_CONTEXT
    finally:
        ctx.db.dispose()
    return result


@click.command("init", help="Create the data directory, configuration and database; create the first administrator (idempotent).")
@click.option("--host", default=None, help="Bind address written to config.toml (default 127.0.0.1).")
@click.option("--port", type=int, default=None, help="Port written to config.toml (default 8470).")
@click.option("--public-url", default=None, help="External URL when behind a proxy / load balancer.")
@click.option("--tls/--no-tls", default=None, help="Serve HTTPS with a certificate from the built-in CA.")
@click.option("--database-url", default=None, help="PostgreSQL URL (default: SQLite in the data directory).")
@click.option("--admin-password-stdin", is_flag=True, help="Read the first administrator's password from stdin (default: generate one and show it once).")
@click.option("--force-config", is_flag=True, help="Overwrite an existing config.toml.")
@click.option("--no-cli-context", is_flag=True, help="Do not store a CLI login for the local server.")
@click.option("--rotate-cli-token", is_flag=True, help="Issue a new long-lived CLI token for the 'local' context.")
@pass_state
def init_cmd(state: State, host: str | None, port: int | None, public_url: str | None, tls: bool | None, database_url: str | None, admin_password_stdin: bool,
             force_config: bool, no_cli_context: bool, rotate_cli_token: bool) -> None:
    pw = sys.stdin.readline().rstrip("\n") if admin_password_stdin else os.environ.get("OLCF_BOOTSTRAP_ADMIN_PASSWORD") or None
    state.out.step(f"initialising {state.data_path()}")
    res = initialise(state, host=host, port=port, public_url=public_url, tls=tls, database_url=database_url, admin_password=pw, force_config=force_config,
                     cli_context=not no_cli_context, rotate_cli_token=rotate_cli_token)

    def human(r: dict[str, Any]) -> None:
        o = state.out
        o.success(f"data directory ready: {r['data_dir']}")
        o.kv([("config", r["config"] + ("" if r["config_written"] else " (kept)")), ("database", r["database"]), ("url", r["url"])])
        if r["admin_created"] and r.get("admin_password_supplied"):
            o.info("")
            o.info(f"First administrator '{r['admin_user']}' created with the password you supplied (it is not printed or stored).")
        elif r["admin_created"]:
            o.info("")
            o.info(o.style("First administrator (shown once - save it now):", bold=True))
            o.kv([("username", r["admin_user"]), ("password", r["admin_password"])], indent=2)
            o.info(f"  also saved to {r['credentials_file']} (mode 0600) - delete it once you stored the password")
        if r.get("cli_context"):
            o.info(f"\nThe CLI is logged in to this server as context '{r['cli_context']}' (token stored in {ContextStore().path}).")
        if r.get("runtime_trust_note"):
            o.warn(r["runtime_trust_note"])
        o.info("\nNext: olcf serve        (or: olcf serve --daemon)")

    state.out.emit(res, human, reveal=("admin_password",))


# --------------------------------------------------------------------------------------------- serve
def tls_kwargs(settings: Any) -> dict[str, Any]:
    """uvicorn TLS keyword arguments from the settings (``olcf.ops.tlsconfig`` when present, otherwise the built-in PKI)."""
    if not settings.tls.enabled:
        return {}
    try:
        from olcf.ops.tlsconfig import build_uvicorn_ssl_kwargs  # type: ignore[import-not-found]

        return dict(build_uvicorn_ssl_kwargs(settings))
    except ImportError:
        pass
    import ssl

    from olcf.security.pki import PKI

    pki = PKI(settings.path("pki"))
    if settings.tls.cert_file and settings.tls.key_file:
        crt, key = Path(settings.tls.cert_file), Path(settings.tls.key_file)
    else:
        host = socket.gethostname()
        sans = [settings.server.host, host, host.split(".")[0], *settings.tls.extra_sans]
        crt, key = pki.ensure_server_cert([s for s in sans if s and s not in ("0.0.0.0", "::")])  # noqa: S104
    kw: dict[str, Any] = {"ssl_certfile": str(crt), "ssl_keyfile": str(key)}
    if settings.tls.require_client_cert_for_workers:
        kw.update(ssl_ca_certs=str(pki.ca_cert_path), ssl_cert_reqs=ssl.CERT_OPTIONAL)
    return kw


def serve_overrides(host: str | None, port: int | None, public_url: str | None, tls: bool | None, workers: int, embedded: bool | None) -> dict[str, Any]:
    ov: dict[str, Any] = {}
    srv = {k: v for k, v in (("host", host), ("port", port), ("public_url", public_url)) if v is not None}
    if srv:
        ov["server"] = srv
    if tls is not None:
        ov["tls"] = {"enabled": tls}
    comp: dict[str, Any] = {}
    if embedded is not None:
        comp["embedded_worker"] = embedded
    elif workers > 1:
        comp["embedded_worker"] = False
    if comp:
        ov["compute"] = comp
    return ov


def _open_browser_when_up(url: str, ca_bundle: str | None) -> None:
    def run() -> None:
        if wait_healthy(url, timeout=90, ca_bundle=ca_bundle):
            with contextlib.suppress(Exception):
                webbrowser.open(str(url))

    threading.Thread(target=run, daemon=True, name="open-browser").start()


def spawn_daemon(state: State, argv_tail: list[str], log_path: Path) -> subprocess.Popen[bytes]:
    """Start ``olcf serve`` detached from this terminal, logging to ``log_path``."""
    log_path.parent.mkdir(parents=True, exist_ok=True)
    cmd = [sys.executable, "-m", "olcf.cli.main", "--data-dir", str(state.data_path()), "-q", "serve", *argv_tail]
    env = dict(os.environ, OLCF_DATA_DIR=str(state.data_path()), PYTHONUNBUFFERED="1")
    kw: dict[str, Any] = {}
    if os.name == "nt":
        kw["creationflags"] = subprocess.DETACHED_PROCESS | subprocess.CREATE_NEW_PROCESS_GROUP | subprocess.CREATE_NO_WINDOW  # type: ignore[attr-defined]
    else:
        kw["start_new_session"] = True
    logf = open(log_path, "ab")  # noqa: SIM115 - handed to the child
    try:
        return subprocess.Popen(cmd, stdin=subprocess.DEVNULL, stdout=logf, stderr=logf, env=env, close_fds=True, **kw)
    finally:
        logf.close()


@click.command("serve", help="Run the OLCF server (API, web UI, scheduler and - by default - an embedded worker).")
@click.option("--host", default=None, help="Bind address (default from config: 127.0.0.1).")
@click.option("--port", type=int, default=None, help="Port (default from config: 8470).")
@click.option("--public-url", default=None, help="External URL, when behind a reverse proxy.")
@click.option("--tls/--no-tls", default=None, help="Serve HTTPS (built-in CA or configured certificate).")
@click.option("--workers", type=click.IntRange(1, 64), default=1, show_default=True, help="API processes. >1 needs PostgreSQL; disables the embedded worker.")
@click.option("--embedded-worker/--no-embedded-worker", default=None, help="Run a Spark worker inside this node (default: from config, on).")
@click.option("--daemon", "-d", is_flag=True, help="Detach and keep running in the background (pid file in the data directory).")
@click.option("--wait", type=float, default=60.0, show_default=True, help="With --daemon: seconds to wait until the server answers.")
@click.option("--open", "open_browser", is_flag=True, help="Open the web UI in your browser once the server is up.")
@click.option("--log-level", type=click.Choice(["critical", "error", "warning", "info", "debug"]), default=None)
@pass_state
def serve_cmd(state: State, host: str | None, port: int | None, public_url: str | None, tls: bool | None, workers: int, embedded_worker: bool | None,
              daemon: bool, wait: float, open_browser: bool, log_level: str | None) -> None:
    import uvicorn

    from olcf.cli.factory import OVERRIDES_ENV

    overrides = serve_overrides(host, port, public_url, tls, workers, embedded_worker)
    if log_level:
        overrides["log_level"] = log_level.upper()
    settings = state.settings(**overrides)
    dd = settings.data_dir
    running = read_pidfile(dd)
    if running:
        state.out.success(f"OLCF is already running (pid {running['pid']}) at {running.get('url', settings.base_url)}")
        if open_browser:
            webbrowser.open(running.get("url", settings.base_url))
        return
    bind_host, bind_port = settings.server.host, settings.server.port
    if checks.port_in_use(bind_host, bind_port):
        owner = checks.port_owner(bind_port)
        raise click.ClickException(f"port {bind_port} is already in use{' by ' + owner if owner else ''}; choose another with --port (or stop that program)")
    if note := runtime_trust_notice(settings):
        state.out.warn(note)
    if workers > 1 and settings.database_url.startswith("sqlite"):
        state.out.warn("--workers > 1 with SQLite: concurrent writers contend on one file; use PostgreSQL (db.url) for real multi-process serving")
    ca = str(settings.path("pki", "ca.crt")) if settings.tls.enabled else None

    if daemon:
        tail = [f"--{k}={v}" for k, v in (("host", host), ("port", port), ("public-url", public_url), ("workers", workers if workers != 1 else None),
                                         ("log-level", log_level)) if v is not None]
        if tls is not None:
            tail.append("--tls" if tls else "--no-tls")
        if embedded_worker is not None:
            tail.append("--embedded-worker" if embedded_worker else "--no-embedded-worker")
        log_path = dd / "logs" / "server.out"
        proc = spawn_daemon(state, tail, log_path)
        state.out.step(f"starting OLCF in the background (pid {proc.pid}); log: {log_path}")
        if not wait_healthy(settings.base_url.replace("://localhost", "://127.0.0.1"), timeout=wait, proc=proc, ca_bundle=ca):
            if proc.poll() is not None:
                raise click.ClickException(f"the server exited with status {proc.returncode} during start-up:\n{tail_file(log_path)}")
            raise click.ClickException(f"the server did not answer within {wait:.0f}s (still starting?). See {log_path}")
        state.out.success(f"OLCF is running at {settings.base_url}   (stop with: olcf stop)")
        if open_browser:
            webbrowser.open(settings.base_url)
        return

    os.environ["OLCF_DATA_DIR"] = str(dd)
    os.environ[OVERRIDES_ENV] = json.dumps(overrides)
    ssl_kw = tls_kwargs(settings)
    level = (log_level or settings.log_level).lower()
    cfg_kw: dict[str, Any] = {"app": "olcf.cli.factory:create", "factory": True, "host": bind_host, "port": bind_port, "log_level": level,
                              "timeout_graceful_shutdown": 20, "server_header": False, "proxy_headers": settings.server.trusted_proxies > 0,
                              "forwarded_allow_ips": "*" if settings.server.trusted_proxies > 0 else "127.0.0.1", "access_log": False, **ssl_kw}
    if open_browser:
        _open_browser_when_up(settings.base_url.replace("://localhost", "://127.0.0.1"), ca)
    state.out.step(f"OLCF {__version__} listening on {settings.base_url}  (data: {dd})")
    write_pidfile(dd, url=settings.base_url, host=bind_host, port=bind_port, workers=workers)
    try:
        if workers > 1:
            uvicorn.run(workers=workers, **cfg_kw)
        else:
            server = uvicorn.Server(uvicorn.Config(**cfg_kw))
            server.run()
            if not server.started:
                raise click.ClickException("the server could not start; see the messages above")
    finally:
        remove_pidfile(dd)


# --------------------------------------------------------------------------------------------- stop
def stop_running(data_dir: Path, *, grace: float, force: bool) -> bool:
    """Gracefully stop the server recorded in the pid file; returns False when none was running."""
    import psutil

    info = read_pidfile(data_dir)
    if not info:
        return False
    proc = psutil.Process(int(info["pid"]))
    try:
        if os.name == "nt":
            os.kill(proc.pid, signal.CTRL_BREAK_EVENT)  # type: ignore[attr-defined]
        else:
            proc.send_signal(signal.SIGTERM)
        proc.wait(timeout=grace)
    except psutil.TimeoutExpired:
        if not force:
            raise click.ClickException(f"the server is still shutting down after {grace:.0f}s; wait, or use --force") from None
        for child in proc.children(recursive=True):
            with contextlib.suppress(psutil.Error):
                child.kill()
        proc.kill()
    except psutil.NoSuchProcess:
        pass
    with contextlib.suppress(OSError):
        pid_path(data_dir).unlink()
    return True


@click.command("stop", help="Stop the server started by `olcf serve --daemon` (graceful; waits for running requests).")
@click.option("--timeout", "grace", type=float, default=30.0, show_default=True, help="Seconds to wait before giving up (or forcing with --force).")
@click.option("--force", is_flag=True, help="Kill the process if it does not stop in time.")
@pass_state
def stop_cmd(state: State, grace: float, force: bool) -> None:
    dd = state.data_path()
    info = read_pidfile(dd)
    if not info:
        state.out.info("OLCF is not running (no live pid file in " + str(dd) + ").")
        return
    state.out.step(f"stopping OLCF (pid {info['pid']})")
    stop_running(dd, grace=grace, force=force)
    state.out.success("stopped")


def runtime_trust_row(report: dict[str, Any] | None) -> dict[str, Any] | None:
    """The ``runtime-trust`` component of a ``GET /v1/admin/health`` report as ``{status, detail, fix}`` when it is NOT ``ok`` (advisory: it never changes readiness), else ``None``."""
    from olcf.security.runtime_trust import remediation_for

    for comp in (report or {}).get("components") or []:
        if comp.get("name") == "runtime-trust" and str(comp.get("status")) in ("down", "degraded"):
            return {"status": str(comp["status"]), "detail": str(comp.get("detail") or ""), "fix": remediation_for(comp.get("labels") or {})}
    return None


# --------------------------------------------------------------------------------------------- status
@click.command("status", help="Show whether the server is up and a summary of workers, runtimes and jobs.")
@pass_state
def status_cmd(state: State) -> None:
    conn = state.resolve_connection()
    dd = state.data_path()
    info = read_pidfile(dd)
    doc: dict[str, Any] = {"url": conn["url"], "context": conn.get("context"), "pid": info["pid"] if info else None, "reachable": False}
    c = state.client
    try:
        doc["health"] = c.health()
        doc["reachable"] = True
    except OlcfSdkError as exc:
        doc["error"] = str(exc)
    if doc["reachable"]:
        for key, fn in (("info", c.admin.info), ("topology", c.compute.topology), ("jobs", c.jobs.stats)):
            try:
                doc[key] = fn()
            except OlcfSdkError as exc:
                doc[key] = {"unavailable": str(exc)}
        with contextlib.suppress(OlcfSdkError):  # needs admin.health: without it the line is simply absent (olcf doctor says the same after a login)
            if row := runtime_trust_row(c.admin.health_report()):
                doc["runtime_trust"] = row

    def human(d: dict[str, Any]) -> None:
        o = state.out
        if not d["reachable"]:
            o.error(f"no OLCF server answers at {d['url']}", "start one with `olcf serve --daemon` (or `olcf quickstart`); use --url/--context for a remote server")
            return
        h = d.get("health") or {}
        o.kv([("server", f"{d['url']}  {o.state(str(h.get('status', 'ok')))}"), ("version", h.get("version") or (d.get("info") or {}).get("version")),
              ("pid", d["pid"] or "(not started by this CLI)"), ("context", d.get("context") or "(flags/env)")])
        topo = d.get("topology") or {}
        if "unavailable" not in topo:
            workers = topo.get("nodes") or []
            states: dict[str, int] = {}
            for w in workers:
                states[w.get("state", "?")] = states.get(w.get("state", "?"), 0) + 1
            o.kv([("workers", ", ".join(f"{n} {s}" for s, n in sorted(states.items())) or "none (the embedded worker may still be starting)")])
            rts = sorted({r.get("id", "?") for w in workers for r in (w.get("runtimes") or []) if r.get("state") == "ready"})
            o.kv([("runtimes", ", ".join(rts) or "none ready (olcf runtime install)")])
        elif "authentication" in topo["unavailable"].lower() or "401" in topo["unavailable"]:
            o.info("  (log in with `olcf login` to see workers and jobs)")
        jobs = d.get("jobs") or {}
        if jobs and "unavailable" not in jobs:
            o.kv([("jobs", ", ".join(f"{k}={v}" for k, v in (jobs.get("counts") or {}).items()) or "none queued or running")])
        if rt := d.get("runtime_trust"):
            o.warn(f"runtime-trust is {rt['status']}: {rt['detail']}")

    state.out.emit(doc, human)
    if not doc["reachable"]:
        raise SystemExit(EXIT_SERVER)


# --------------------------------------------------------------------------------------------- doctor
def embedded_state_dir(dd: Path) -> Path:
    from olcf.compute.embedded import embedded_name

    return dd / "workers" / embedded_name()


def runtime_trust_notice(settings: Any) -> str | None:
    """A one-paragraph note for an EXISTING installation whose ``config.toml`` sets no ``environment`` (created before the runtime-trust change): it is production,
    so the bundled, development-signed Spark runtime manifests are refused.  ``None`` when nothing needs saying (explicit environment, own trust anchor, no config)."""
    import tomllib

    cfg = Path(settings.data_dir) / "config.toml"
    try:
        doc = tomllib.loads(cfg.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return None
    t = settings.runtime.trust
    if ("environment" in doc or os.environ.get("OLCF_ENVIRONMENT") or t.allow_development_key is not None or t.dir or os.environ.get("OLCF_RUNTIME_TRUST_DIR")
            or os.environ.get("OLCF_RUNTIME_ALLOW_DEV_KEY")):
        return None
    from olcf.runtime.signing import dev_key_advice

    return (f"{cfg} has no `environment` key, so this installation is treated as production and does NOT trust the development key that signs the bundled Spark runtime "
            f"manifests: Spark runtimes are unavailable (Python notebooks still run on the built-in host-Python runtime). {dev_key_advice(cfg)}")


def local_trust_store(state: State) -> Any:
    """The runtime trust anchor the SERVER of this data directory uses (``config.toml`` ``environment`` / ``[runtime.trust]``, then ``OLCF_*``).

    Every local runtime command (``runtime list|install|verify``, ``quickstart``) must use it, never ``TrustStore.default()``: that one only sees the
    process environment (unset = production) and would refuse the bundled manifests on an installation that ``olcf init`` configured as development.
    """
    from olcf.runtime.signing import TrustStore

    return TrustStore.for_settings(state.settings(log_level="WARNING"))


@click.command("doctor", help="Check this machine and the installation; every problem comes with the command that fixes it.")
@click.option("--server/--no-server", "check_server", default=True, help="Also ask the running server for its health checks.")
@pass_state
def doctor_cmd(state: State, check_server: bool) -> None:
    settings = state.settings()
    dd = settings.data_dir
    running = read_pidfile(dd)
    items = checks.run_local_checks(settings, server_running=bool(running), state_dirs=[embedded_state_dir(dd)])
    if check_server:
        c = state.client
        try:
            h = c.health()
            items.append(checks.Check("server", checks.OK, f"{c.base_url} answers (version {h.get('version', '?')})"))
            try:
                rep = c.admin.health_report()
                trust = runtime_trust_row(rep)
                for chk in (rep.get("components") or []):
                    status = {"ok": checks.OK, "degraded": checks.WARN, "unknown": checks.INFO}.get(str(chk.get("status")), checks.FAIL)
                    fix = chk.get("remediation")
                    if trust and chk.get("name") == "runtime-trust":
                        status, fix = checks.WARN, trust["fix"]  # advisory by design (readiness is unchanged): a warning line with the fix, never a failed doctor run
                    items.append(checks.Check(f"server: {chk.get('name')}", status, str(chk.get("detail") or chk.get("message") or ""), fix))
            except OlcfSdkError as exc:
                items.append(checks.Check("server checks", checks.INFO, f"detailed checks need a login ({exc})", "olcf login"))
        except OlcfSdkError:
            items.append(checks.Check("server", checks.INFO, f"nothing answers at {state.client.base_url}", "olcf serve --daemon"))
    failed = [i for i in items if i.status == checks.FAIL]
    prereq = {"python", "disk", "data dir"} | {i.name for i in items if i.name.startswith("port ")}

    def human(rows: list[dict[str, Any]]) -> None:
        o = state.out
        mark = {checks.OK: ("OK  ", "green"), checks.WARN: ("WARN", "yellow"), checks.FAIL: ("FAIL", "red"), checks.INFO: ("info", "grey")}
        for r in rows:
            tag, fg = mark[r["status"]]
            print(f"{o.style(tag, fg=fg, bold=True)}  {r['name']:<22} {r['detail']}", file=o.out)
            if r.get("fix") and r["status"] != checks.OK:
                print(f"      {o.style('fix:', fg='cyan')} {r['fix']}", file=o.out)
        n_warn = sum(1 for r in rows if r["status"] == checks.WARN)
        print(f"\n{len(failed)} problem(s), {n_warn} warning(s)", file=o.out)

    state.out.emit([i.to_dict() for i in items], human)
    if failed:
        raise SystemExit(EXIT_PREREQ if any(i.name in prereq for i in failed) else EXIT_FAIL)


# --------------------------------------------------------------------------------------------- version
def find_install_info() -> dict[str, Any] | None:
    """``install.json`` written by the installer, located relative to the running interpreter (``<prefix>/versions/<v>/venv``)."""
    env = os.environ.get("OLCF_HOME")
    cands = [Path(env) / "install.json"] if env else []
    p = Path(sys.prefix).resolve()
    cands += [parent / "install.json" for parent in list(p.parents)[:4]]
    for c in cands:
        try:
            return json.loads(c.read_text())
        except (OSError, ValueError):
            continue
    return None


@click.command("version", help="Show versions of OLCF, Python and the installation.")
@pass_state
def version_cmd(state: State) -> None:
    info = find_install_info()
    doc: dict[str, Any] = {"olcf": __version__, "python": platform.python_version(), "platform": f"{platform.system()} {platform.release()} {platform.machine()}",
                           "executable": sys.executable, "install": info}
    with contextlib.suppress(Exception):
        from importlib.metadata import version as v

        doc["dependencies"] = {n: v(n) for n in ("fastapi", "uvicorn", "httpx", "sqlalchemy", "click", "pydantic")}
    if state.verbose:
        pass

    def human(d: dict[str, Any]) -> None:
        state.out.kv([("olcf", d["olcf"]), ("python", f"{d['python']}  ({d['executable']})"), ("platform", d["platform"])]
                     + ([("installed", f"{d['install'].get('installed_at', '?')} via {d['install'].get('method', '?')} into {d['install'].get('prefix', '?')}")] if d["install"] else []))

    state.out.emit(doc, human)


# --------------------------------------------------------------------------------------------- quickstart
@click.command("quickstart", help="Everything in one go: initialise, start the server, load the sample project, then (optionally) install a Spark runtime and open the UI.")
@click.option("--port", type=int, default=None, help="Port for the server (default 8470).")
@click.option("--runtime", "runtime_id", default=None, help="Runtime manifest id to install (default: the newest bundled one).")
@click.option("--no-runtime", is_flag=True, help="Skip the Spark runtime (SQL on DuckDB, lakehouses, the UI and Python notebooks work without it).")
@click.option("--with-jdk/--no-jdk", default=True, show_default=True, help="Download a verified Temurin JDK when no suitable Java exists.")
@click.option("--no-sample", is_flag=True, help="Do not install the sample workspace.")
@click.option("--open/--no-open", "open_browser", default=True, show_default=True, help="Open the web UI when ready.")
@click.option("--wait", type=float, default=90.0, show_default=True, help="Seconds to wait for the server to come up.")
@pass_state
def quickstart_cmd(state: State, port: int | None, runtime_id: str | None, no_runtime: bool, with_jdk: bool, no_sample: bool, open_browser: bool, wait: float) -> None:
    """Order matters: the server and the sample come first and are reported as a *working* state (Python notebooks run on the host-Python runtime, which needs no
    download); the Spark runtime - hundreds of MB from Maven Central, the step most likely to fail - is a non-fatal follow-up with its own retry command."""
    o = state.out
    settings = state.settings()
    dd = settings.data_dir
    o.step("checking this machine")
    local = checks.run_local_checks(settings, server_running=bool(read_pidfile(dd)), port=port, state_dirs=[embedded_state_dir(dd)])
    hard = [c for c in local if c.status == checks.FAIL]
    for c in hard:
        o.error(f"{c.name}: {c.detail}", c.fix)
    if hard:
        raise SystemExit(EXIT_PREREQ)

    o.step("initialising")
    res = initialise(state, host=None, port=port, public_url=None, tls=None, database_url=None, admin_password=os.environ.get("OLCF_BOOTSTRAP_ADMIN_PASSWORD") or None,
                     force_config=False, cli_context=True, rotate_cli_token=False)
    if res.get("cli_context"):
        # talk to the server we are about to start, not to whatever context happened to be "current" from an earlier install
        state.context = str(res["cli_context"])
        state._client = None
    o.step("starting the server")
    settings = state.settings(**({"server": {"port": port}} if port else {}))
    running = read_pidfile(dd)
    url = str(running.get("url") if running else settings.base_url)
    if not running:
        tail = [f"--port={port}"] if port else []
        log_path = dd / "logs" / "server.out"
        proc = spawn_daemon(state, tail, log_path)
        ca = str(settings.path("pki", "ca.crt")) if settings.tls.enabled else None
        if not wait_healthy(url.replace("://localhost", "://127.0.0.1"), timeout=wait, proc=proc, ca_bundle=ca):
            raise click.ClickException(f"the server did not start:\n{tail_file(log_path)}")
    _point_cli_context_at(state, url, res)
    _ensure_cli_login(state, res, url)

    sample: dict[str, Any] = {}
    sample_note = "skipped (--no-sample)"
    if not no_sample:
        o.step("installing the sample workspace")
        try:
            sample = state.client.sample.install()
            sample_note = f"{sample.get('workspace', 'installed')} ({sample.get('action', 'installed')})"
        except OlcfSdkError as exc:
            sample_note = f"not installed ({exc})"
            o.warn(f"the sample workspace could not be installed: {exc}")
    python_ready = _wait_host_python(state)
    summary: dict[str, Any] = {**res, "url": url, "sample": sample_note, "runtime": "skipped (--no-runtime)" if no_runtime else "pending", "python_notebooks_ready": python_ready,
                               "next": _next_steps(state, url, sample)}

    if not o.json_mode:
        _print_working_state(state, summary, sample, no_runtime)

    retry = ""
    if not no_runtime:
        rid = runtime_id or default_runtime_id(state)
        retry = f"olcf runtime install {rid}" + (" --with-jdk" if with_jdk else "")
        o.step(f"installing the Spark runtime {rid} (optional: only Spark notebooks, Spark SQL and Spark jobs need it)")
        try:
            summary["runtime"] = install_runtime_local(state, rid, with_jdk=with_jdk, force=False)
            o.success(f"Spark runtime ready: {summary['runtime']}")
        except Exception as exc:  # noqa: BLE001 - the platform is already useful without Spark; report, give the retry command, go on
            summary["runtime"] = f"NOT installed: {getattr(exc, 'message', None) or exc}"
            summary["runtime_retry"] = retry
            o.warn(f"the Spark runtime could not be installed: {getattr(exc, 'message', None) or exc}")
            if hint := getattr(exc, "remediation", None):
                o.info(f"  hint: {hint}")
            o.info("  Everything else is running. Python notebooks, SQL, dashboards and the AI-agent endpoint do not need Spark.")
            o.info(f"  Retry later with:  {retry}")
            o.info("  The Spark runtime downloads PySpark and JARs from PyPI and Maven Central (repo1.maven.org): check your network or proxy, point OLCF_MAVEN_REPOSITORIES at a")
            o.info("  mirror, or install offline with `olcf runtime install --bundle FILE`.")
    if o.json_mode:
        o.emit(summary, None, reveal=("admin_password",))
    if open_browser:
        with contextlib.suppress(Exception):
            webbrowser.open(str(summary["next"].get("dashboard_url") or url))


def _point_cli_context_at(state: State, url: str, res: dict[str, Any]) -> None:
    """The stored ``local`` context must name the URL the server really listens on (``--port`` can differ from an older config.toml)."""
    name = res.get("cli_context")
    if not name:
        return
    store = ContextStore()
    cur = store.get(str(name))
    if cur and cur.get("url") != url:
        store.set(str(name), make_current=False, url=url)
        state._client = None


def _ensure_cli_login(state: State, res: dict[str, Any], url: str) -> None:
    """A stale token in the ``local`` context (left over from another data directory at the same URL) is replaced by a fresh one."""
    name = res.get("cli_context")
    if not name:
        return
    try:
        state.client.me()
        return
    except OlcfSdkError as exc:
        if getattr(exc, "status", None) not in (401, 403):
            return
    state.out.detail("the stored CLI login is not valid for this installation; issuing a new one")
    initialise(state, host=None, port=None, public_url=None, tls=None, database_url=None, admin_password=None, force_config=False, cli_context=True, rotate_cli_token=True)
    _point_cli_context_at(state, url, res)
    state._client = None


def _wait_host_python(state: State, timeout: float = 45.0) -> bool:
    """Wait until a worker offers the built-in host-Python runtime (the embedded worker connects a moment after the server starts)."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            rows = state.client.runtimes.list()
            ready = any(r.get("id") == "olcf-host-python" and r.get("state") == "ready" for r in (rows if isinstance(rows, list) else rows.get("items", [])))
            workers = state.client.compute.list_workers()
            # after a restart the database still says "online" for a moment: wait for a live connection, not just for a stored state
            live = any(w.get("connected") and w.get("state") in ("online", "degraded") for w in (workers if isinstance(workers, list) else workers.get("items", [])))
            if ready and live:
                return True
        except OlcfSdkError:
            pass
        time.sleep(1.0)
    return False


def _next_steps(state: State, url: str, sample: dict[str, Any]) -> dict[str, Any]:
    """Commands and links for the end of quickstart: the dashboard (direct hash link), a notebook run, the MCP configuration."""
    out: dict[str, Any] = {"ui_url": url, "mcp_config": "olcf mcp config --client claude-code"}
    ws = sample.get("workspace")
    if ws:
        out["notebook_run"] = f'olcf notebook run -w "{ws}" 02_Retail_Explore_Python --wait'
        rid = (sample.get("item_ids") or {}).get("Retail Sales Overview")
        if rid:
            out["dashboard_url"] = f"{url.rstrip('/')}/#/dashboards/{rid}"
    return out


def _print_working_state(state: State, r: dict[str, Any], sample: dict[str, Any], no_runtime: bool) -> None:
    o = state.out
    nxt = r["next"]
    o.info("")
    o.success(f"OLCF is running at {r['url']}")
    o.kv([("sample", r["sample"]), ("python", "notebooks run on the built-in host-Python runtime (no Spark needed)" if r["python_notebooks_ready"]
                                          else "host-Python runtime not reported yet - check `olcf runtime list --remote`"), ("data", r["data_dir"])])
    if r.get("admin_created") and r.get("admin_password_supplied"):
        o.info(f"\nSign in to the web UI as '{r['admin_user']}' with the password you supplied.")
    elif r.get("admin_created"):
        o.info(o.style("\nSign in to the web UI with (shown once - save it now):", bold=True))
        o.kv([("username", r["admin_user"]), ("password", r["admin_password"])], indent=2)
    else:
        o.info(f"\nSign in with your administrator account (credentials: {r['data_dir']}/secrets/bootstrap-admin.txt if you never changed them).")
    o.info(o.style("\nNext steps:", bold=True))
    if nxt.get("dashboard_url"):
        o.info(f"  dashboard:   {nxt['dashboard_url']}")
    else:
        o.info(f"  web UI:      {r['url']}")
    if nxt.get("notebook_run"):
        o.info(f"  notebook:    {nxt['notebook_run']}")
    o.info(f"  AI agents:   {nxt['mcp_config']}      (prints a ready-to-paste MCP client configuration)")
    if sample.get("workspace"):
        o.info("  SQL:         " + f'olcf sql run -w "{sample["workspace"]}" ' + "'select count(*) from retail_lakehouse.fact_sales'")  # noqa: S608 - a help text, not a query
    o.info("  also:        olcf status      olcf stop")
    if no_runtime:
        o.info("\nNo Spark runtime was requested. Add one any time with: olcf runtime install   (needed only for Spark notebooks, Spark SQL and Spark jobs)")


def default_runtime_id(state: State) -> str:
    """The newest bundled Spark runtime manifest id."""
    from olcf.runtime.manager import ManagerConfig, RuntimeManager

    mgr = RuntimeManager(ManagerConfig(state_dir=embedded_state_dir(state.data_path()), host_python=False, trust=local_trust_store(state)))
    spark = [m for m in mgr.available_manifests() if "native" not in m.family]
    if not spark:
        if mgr.skipped_dev_signed:  # they ship, but this installation does not trust their signing key: say how to fix THAT, not "none ship"
            from olcf.runtime.signing import dev_key_advice

            raise click.ClickException(f"the bundled runtime manifests ({', '.join(mgr.skipped_dev_signed)}) are signed with the OLCF development key, which this installation does "
                                       f"not trust. {dev_key_advice(state.data_path() / 'config.toml')}")
        raise click.ClickException("no runtime manifest ships with this build")
    return max(spark, key=lambda m: tuple(int(x) if x.isdigit() else 0 for x in m.version.split("."))).id


def install_runtime_local(state: State, runtime_id: str | None, *, with_jdk: bool, force: bool, activate: bool = True, bundle: str | None = None,
                          index_url: str | None = None, find_links: tuple[str, ...] = (), uv_python: bool = True) -> str:
    """Install a runtime on this machine (the embedded worker's state directory).  Used by ``quickstart`` and ``runtime install``."""
    from olcf.runtime.manager import ManagerConfig, RuntimeManager

    dd = state.data_path()
    mgr = RuntimeManager(ManagerConfig(state_dir=embedded_state_dir(dd), with_jdk=with_jdk and not hostsafe.find_jdks([embedded_state_dir(dd)]),
                                       allow_uv_python_install=uv_python, pip_index_url=index_url, pip_find_links=list(find_links),
                                       trust=local_trust_store(state)))  # the same trust anchor as the server (runtime.trust.*)
    if runtime_id is None:
        runtime_id = default_runtime_id(state)
    if not bundle:
        mgr.load_manifest(runtime_id)  # unknown / built-in ids fail here with the list of valid ones, before anything is announced
    state.out.step(f"installing runtime {runtime_id} (several minutes on first run; downloads Python, PySpark and JARs, then runs a smoke test)")
    if bundle:
        res = mgr.import_bundle(bundle, progress=state.out.progress, force=force, activate=activate)
    else:
        res = mgr.install(runtime_id, force=force, activate=activate, progress=state.out.progress)
    return f"{res.get('runtime_id', runtime_id)} {res.get('state', 'ready')}"


def register(group: click.Group) -> None:
    for cmd in (init_cmd, serve_cmd, stop_cmd, status_cmd, doctor_cmd, version_cmd, quickstart_cmd):
        group.add_command(cmd)
