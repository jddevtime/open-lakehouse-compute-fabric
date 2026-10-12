# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Diagnostics suite (service ``diagnostics``) and the ``diagnostics`` health check.

``run`` executes a fixed list of independent checks - each one actively *does* something and reports ``{name, category, status ok|warn|fail|skipped,
detail, remediation, ms, data}`` - so a support engineer (or the UI's "Run diagnostics" button) gets one document that answers "what is wrong
with this installation?":

platform        versions, Python, OS, CPU/memory/disk of the control-plane host
database        round-trip write/read/delete, migration state vs. code, dialect/server version, size
health          the deep variant of every registered health check (each bounded by a timeout)
auth            token issue/verify round-trip, signing-key presence, local-login/lock-out state, leftover bootstrap-admin file, key-file permissions
storage         write/read/delete in the default local lakehouse root plus the stepwise test of every registered lakehouse
compute         workers, queues, running jobs; optional real distributed Spark smoke test (``spark=True``; needs compute.manage)
network         DNS + TCP reachability of the public URL, identity providers, update feed and backup targets; proxy environment (credentials stripped);
                optional per-node probe matrix from every worker (``worker_probes=True``)
time            database-server vs. control-plane clock, worker clock skew, optional network time (HTTP ``Date``) comparison
certificates    validity of the CA, server and TLS certificates
logs            error counts and most frequent error messages of the last day (redacted)
config          redacted effective settings

Nothing here mutates state beyond throw-away probe objects, and every value passes through the redaction filter before it is returned or stored.
"""

from __future__ import annotations

import concurrent.futures as cf
import logging
import os
import platform
import re
import shutil
import socket
import sys
import time
from collections import Counter
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit

import httpx
from sqlalchemy import func, select, text, update

from olcf.core.context import AppContext
from olcf.core.health import ComponentHealth, HealthCheck, Status
from olcf.core.ids import new_id
from olcf.core.redaction import redact_obj, redact_text
from olcf.core.timeutil import now_iso
from olcf.db import schema as S
from olcf.db.util import fetch_all
from olcf.security.rbac import Principal
from olcf.version import __version__

log = logging.getLogger("olcf.ops.diagnostics")
LAST_KEY = "ops.diagnostics.last"
CHECK_TIMEOUT = 60.0
ALL_CHECKS = ("platform", "database", "health", "auth", "storage", "compute", "network", "time", "certificates", "logs", "config")


def _res(name: str, category: str, status: str, detail: str, *, remediation: str | None = None, ms: float = 0.0, data: dict[str, Any] | None = None) -> dict[str, Any]:
    out = {"name": name, "category": category, "status": status, "detail": redact_text(detail), "ms": round(ms, 1)}
    if remediation:
        out["remediation"] = remediation
    if data:
        out["data"] = redact_obj(data)
    return out


class DiagnosticsService:
    """Runs the diagnostic checks (platform, database, health, authentication, storage, compute and more) and returns their results."""
    def __init__(self, ctx: AppContext) -> None:
        self.ctx = ctx

    # ------------------------------------------------------------------ orchestration
    def run(self, principal: Principal | None, *, only: list[str] | None = None, spark: bool = False, worker_probes: bool = False, network_time: bool = False,
            deep_health: bool = True) -> dict[str, Any]:
        names = [n for n in ALL_CHECKS if not only or n in only]
        unknown = set(only or []) - set(ALL_CHECKS)
        if unknown:
            from olcf.core.errors import BadRequest

            raise BadRequest(f"unknown diagnostic check(s): {sorted(unknown)}", details={"available": list(ALL_CHECKS)})
        opts = {"spark": spark, "worker_probes": worker_probes, "network_time": network_time, "deep_health": deep_health}
        t0 = time.perf_counter()
        results: list[dict[str, Any]] = []
        for n in names:
            fn = getattr(self, f"_check_{n}")
            t1 = time.perf_counter()
            try:
                with cf.ThreadPoolExecutor(max_workers=1) as pool:
                    fut = pool.submit(fn, principal, opts)
                    out = fut.result(timeout=CHECK_TIMEOUT if not spark else 900)
            except cf.TimeoutError:
                out = [_res(n, n, "fail", f"the '{n}' checks did not finish within {CHECK_TIMEOUT:.0f} s", remediation="look at the component's own health check / logs")]
            except Exception as exc:  # noqa: BLE001 - a broken check must not break the report
                out = [_res(n, n, "fail", f"{type(exc).__name__}: {exc}"[:300])]
            for o in out:
                o["ms"] = o["ms"] or round((time.perf_counter() - t1) * 1000 / max(len(out), 1), 1)
            results.extend(out)
        counts = Counter(r["status"] for r in results)
        status = "down" if counts["fail"] else "degraded" if counts["warn"] else "ok"
        report = {"id": new_id("evt").replace("evt_", "dgn_"), "started_at": now_iso(), "duration_ms": round((time.perf_counter() - t0) * 1000, 1), "status": status,
                  "summary": {"ok": counts["ok"], "warn": counts["warn"], "fail": counts["fail"], "skipped": counts["skipped"]}, "version": __version__, "node": self.ctx.node_id,
                  "options": opts, "checks": results}
        self._store(report)
        self.ctx.audit.record(principal, "admin.diagnostics_run", resource_type="diagnostics", resource_id=report["id"], outcome="success" if status != "down" else "failure",
                              details={"summary": report["summary"], "only": only})
        return report

    def _store(self, report: dict[str, Any]) -> None:
        slim = {k: report[k] for k in ("id", "started_at", "status", "summary", "duration_ms", "version")}
        slim["failed"] = [{"name": c["name"], "detail": c["detail"][:200]} for c in report["checks"] if c["status"] == "fail"][:20]
        slim["warned"] = [{"name": c["name"], "detail": c["detail"][:200]} for c in report["checks"] if c["status"] == "warn"][:20]
        with self.ctx.db.begin() as conn:
            n = conn.execute(update(S.kv).where(S.kv.c.key == LAST_KEY).values(value={"summary": slim, "report": report}, updated_at=now_iso())).rowcount
            if not n:
                conn.execute(S.kv.insert().values(key=LAST_KEY, value={"summary": slim, "report": report}, updated_at=now_iso()))

    def last(self) -> dict[str, Any] | None:
        with self.ctx.db.connect() as conn:
            row = conn.execute(select(S.kv.c.value).where(S.kv.c.key == LAST_KEY)).scalar()
        return row["report"] if row else None

    def last_summary(self) -> dict[str, Any] | None:
        with self.ctx.db.connect() as conn:
            row = conn.execute(select(S.kv.c.value).where(S.kv.c.key == LAST_KEY)).scalar()
        return row["summary"] if row else None

    # ------------------------------------------------------------------ individual checks
    def _check_platform(self, principal: Principal | None, opts: dict[str, Any]) -> list[dict[str, Any]]:
        import importlib.metadata as md

        libs = {}
        for lib in ("fastapi", "uvicorn", "starlette", "pydantic", "sqlalchemy", "cryptography", "httpx", "pyjwt", "pyspark", "delta-spark", "deltalake", "pyarrow", "psycopg", "boto3"):
            try:
                libs[lib] = md.version(lib)
            except md.PackageNotFoundError:
                pass
        data: dict[str, Any] = {"olcf": __version__, "python": sys.version.split()[0], "implementation": platform.python_implementation(), "os": platform.platform(),
                                "machine": platform.machine(), "libraries": libs, "node": self.ctx.node_id, "uptime_seconds": round(self.ctx.uptime, 1), "environment": self.ctx.settings.environment}
        out = [_res("versions", "platform", "ok", f"OLCF {__version__}, Python {data['python']} on {data['os']}", data=data)]
        try:
            import psutil

            vm = psutil.virtual_memory()
            du = shutil.disk_usage(self.ctx.settings.data_dir)
            free_pct = du.free * 100 / du.total
            st = "fail" if free_pct < 3 or du.free < 512 * 1024**2 else "warn" if free_pct < 10 else "ok"
            out.append(_res("resources", "platform", st, f"{psutil.cpu_count()} CPUs, {vm.available // 1024**2} MiB of {vm.total // 1024**2} MiB memory available; data dir {du.free // 1024**3} GiB free ({free_pct:.0f}%)",
                            remediation="free disk space or move the data directory" if st != "ok" else None,
                            data={"cpus": psutil.cpu_count(), "mem_total": vm.total, "mem_available": vm.available, "disk_free": du.free, "disk_total": du.total, "load": os.getloadavg() if hasattr(os, "getloadavg") else None}))
        except ImportError:
            du = shutil.disk_usage(self.ctx.settings.data_dir)
            out.append(_res("resources", "platform", "ok" if du.free > 512 * 1024**2 else "fail", f"data dir {du.free // 1024**2} MiB free (psutil not installed: no CPU/memory figures)"))
        return out

    def _check_database(self, principal: Principal | None, opts: dict[str, Any]) -> list[dict[str, Any]]:
        from olcf.db import migrate as dbm

        db = self.ctx.db
        out = []
        t0 = time.perf_counter()
        probe = f"diag.probe.{os.getpid()}.{int(time.time() * 1000)}"
        with db.begin() as conn:
            conn.execute(S.kv.insert().values(key=probe, value={"t": now_iso()}, updated_at=now_iso()))
        with db.begin() as conn:
            got = conn.execute(select(S.kv.c.key).where(S.kv.c.key == probe)).first()
            conn.execute(S.kv.delete().where(S.kv.c.key == probe))
        out.append(_res("read-write", "database", "ok" if got else "fail", f"{db.dialect}: write, read-back and delete round-trip {'succeeded' if got else 'FAILED'}", ms=(time.perf_counter() - t0) * 1000))
        dbm.discover()
        cur = dbm.current_versions(db)
        known = {s: max(v) for s, v in dbm._REGISTRY.items()}  # noqa: SLF001
        pending = {s: (cur.get(s, 0), v) for s, v in known.items() if cur.get(s, 0) < v}
        ahead = {s: (v, known.get(s)) for s, v in cur.items() if v > known.get(s, 0)}
        st = "fail" if ahead else "warn" if pending else "ok"
        out.append(_res("migrations", "database", st, "schema is current" if st == "ok" else f"pending={pending} ahead_of_code={ahead}",
                        remediation=("the database was migrated by a newer release: run that release, or restore a backup" if ahead else "restart the platform to apply pending migrations" if pending else None),
                        data={"applied": cur}))
        extra: dict[str, Any] = {"dialect": db.dialect}
        try:
            with db.connect() as conn:
                if db.is_postgres:
                    extra["server_version"] = conn.execute(text("SHOW server_version")).scalar()
                    extra["size_bytes"] = int(conn.execute(text("SELECT pg_database_size(current_database())")).scalar() or 0)
                    extra["connections"] = int(conn.execute(text("SELECT count(*) FROM pg_stat_activity WHERE datname = current_database()")).scalar() or 0)
                else:
                    extra["server_version"] = conn.exec_driver_sql("select sqlite_version()").scalar()
                    p = Path(db.engine.url.database or "")
                    extra["size_bytes"] = p.stat().st_size if p.is_file() else 0
                    extra["journal_mode"] = conn.exec_driver_sql("PRAGMA journal_mode").scalar()
            rows = {t: int(self._count(t)) for t in ("principals", "workspaces", "items", "secrets", "audit_log", "events")}
            extra["rows"] = rows
            out.append(_res("size", "database", "ok", f"{db.dialect} {extra['server_version']}, {extra['size_bytes'] // 1024} KiB", data=extra))
        except Exception as exc:  # noqa: BLE001
            out.append(_res("size", "database", "warn", f"{type(exc).__name__}: {exc}"[:200]))
        if db.is_sqlite and self.ctx.settings.environment == "production":
            out.append(_res("ha-readiness", "database", "warn", "SQLite is used: only a single control-plane node is possible", remediation="use PostgreSQL for HA (see the load-balancer pattern in the operations guide)"))
        return out

    def _count(self, table: str) -> int:
        with self.ctx.db.connect() as conn:
            return conn.execute(select(func.count()).select_from(S.metadata.tables[table])).scalar_one()

    def _check_health(self, principal: Principal | None, opts: dict[str, Any]) -> list[dict[str, Any]]:
        h = self.ctx.health
        out = []
        names = [n for n in h.names() if n != "diagnostics"]
        with cf.ThreadPoolExecutor(max_workers=4) as pool:
            futs = {n: pool.submit(h.run, n, deep=bool(opts["deep_health"]), force=True) for n in names}
            for n, f in futs.items():
                try:
                    r = f.result(timeout=CHECK_TIMEOUT)
                    st = {"ok": "ok", "degraded": "warn", "down": "fail", "unknown": "skipped"}[r.status.value]
                    out.append(_res(f"health:{n}", "health", st, r.detail or r.status.value, ms=r.latency_ms or 0.0, data={"steps": r.steps, "labels": r.labels} if (r.steps or r.labels) else None))
                except cf.TimeoutError:
                    out.append(_res(f"health:{n}", "health", "fail", f"deep check timed out after {CHECK_TIMEOUT:.0f} s"))
                except Exception as exc:  # noqa: BLE001
                    out.append(_res(f"health:{n}", "health", "fail", f"{type(exc).__name__}: {exc}"[:200]))
        return out

    def _check_auth(self, principal: Principal | None, opts: dict[str, Any]) -> list[dict[str, Any]]:
        out = []
        ctx = self.ctx
        try:
            tok, _ = ctx.tokens.issue_access_token({"id": "usr_diagnostics", "name": "diagnostics", "kind": "user"}, scopes=["read"], ttl=60)
            claims = ctx.tokens.verify_access_token(tok)
            out.append(_res("token-roundtrip", "auth", "ok" if claims.get("sub") == "usr_diagnostics" else "fail", "issued and verified a 60 s access token with the current signing key"))
        except Exception as exc:  # noqa: BLE001
            out.append(_res("token-roundtrip", "auth", "fail", f"{type(exc).__name__}: {getattr(exc, 'message', exc)}"[:200], remediation="check secrets/jwt-signing.key and OLCF_JWT_SIGNING_KEY"))
        sdir = ctx.settings.path("secrets")
        if os.name == "posix":
            loose = [p.name for p in list(sdir.glob("*")) + list(ctx.settings.path("pki").glob("*.key")) if p.is_file() and p.stat().st_mode & 0o077]
            out.append(_res("key-permissions", "auth", "fail" if loose else "ok", f"group/world-accessible key files: {loose}" if loose else "key files are private (0600)", remediation="chmod 600 on the listed files" if loose else None))
        boot = sdir / "bootstrap-admin.txt"
        out.append(_res("bootstrap-admin-file", "auth", "warn" if boot.exists() else "ok",
                        "secrets/bootstrap-admin.txt still contains the initial administrator password" if boot.exists() else "no bootstrap credential file present",
                        remediation="save the password in a vault, change it, and delete the file" if boot.exists() else None))
        fed = ctx.maybe_service("federation")
        if fed is not None:
            try:
                ll = fed.local_login_enabled()
                provs = fed.providers()
                out.append(_res("login-methods", "auth", "ok", f"local login {'enabled' if ll else 'disabled'}; {len(provs)} identity provider(s) configured",
                                data={"local_login": ll, "providers": [p.name for p in provs]}))
                if not ll:
                    admins = self._federated_admins()
                    out.append(_res("lockout-guard", "auth", "ok" if admins else "fail", f"{admins} enabled non-local PlatformAdmin account(s) can still sign in" if admins else
                                    "local login is disabled and no federated PlatformAdmin exists: nobody can administer the platform", remediation=None if admins else "re-enable local login (`olcf` CLI or settings file)"))
            except Exception as exc:  # noqa: BLE001
                out.append(_res("login-methods", "auth", "warn", f"{type(exc).__name__}: {exc}"[:200]))
        pt = ctx.settings.tls
        host = ctx.settings.server.host
        if not pt.enabled and host not in ("127.0.0.1", "localhost", "::1") and not ctx.settings.server.public_url:
            out.append(_res("transport", "auth", "warn", f"the control plane listens on {host} without TLS", remediation="enable tls.enabled or terminate TLS at a reverse proxy and set server.public_url to the https URL"))
        elif not pt.enabled and ctx.settings.server.public_url and ctx.settings.server.public_url.startswith("http://") and "localhost" not in ctx.settings.server.public_url and "127.0.0.1" not in ctx.settings.server.public_url:
            out.append(_res("transport", "auth", "warn", "server.public_url is plain http for a non-loopback host", remediation="use https"))
        else:
            out.append(_res("transport", "auth", "ok", "TLS enabled" if pt.enabled else "loopback / TLS terminated upstream"))
        return out

    def _federated_admins(self) -> int:
        with self.ctx.db.connect() as conn:
            return int(conn.execute(text("SELECT COUNT(DISTINCT p.id) FROM principals p JOIN role_bindings b ON b.principal_id = p.id WHERE b.role = 'PlatformAdmin' AND p.is_active = :a AND p.source NOT IN ('local', 'bootstrap', 'system')"),
                                    {"a": True}).scalar() or 0)

    def _check_storage(self, principal: Principal | None, opts: dict[str, Any]) -> list[dict[str, Any]]:
        out = []
        root = self.ctx.settings.local_storage_root
        t0 = time.perf_counter()
        try:
            root.mkdir(parents=True, exist_ok=True)
            p = root / f".diag-{os.getpid()}-{int(time.time() * 1000)}"
            payload = os.urandom(4096)
            p.write_bytes(payload)
            ok = p.read_bytes() == payload
            p.unlink()
            out.append(_res("local-root", "storage", "ok" if ok else "fail", f"write/read/delete in {root}: {'ok' if ok else 'content mismatch'}", ms=(time.perf_counter() - t0) * 1000))
        except OSError as exc:
            out.append(_res("local-root", "storage", "fail", f"{type(exc).__name__}: {exc}"[:200], remediation="check permissions and free space of the local storage root"))
        storage = self.ctx.maybe_service("storage")
        if storage is None:
            out.append(_res("lakehouses", "storage", "skipped", "storage module not loaded"))
            return out
        with self.ctx.db.connect() as conn:
            lhs = fetch_all(conn, select(S.items.c.id, S.items.c.name).where(S.items.c.type == "lakehouse", S.items.c.deleted_at.is_(None)).limit(25))
        if not lhs:
            out.append(_res("lakehouses", "storage", "ok", "no lakehouses defined yet"))
        for lh in lhs:
            try:
                res = storage.test(lh["id"])
                out.append(_res(f"lakehouse:{lh['name']}", "storage", "ok" if res.get("ok") else "fail", res.get("detail") or ("all steps passed" if res.get("ok") else "failed"),
                                data={"steps": res.get("steps")}))
            except Exception as exc:  # noqa: BLE001
                out.append(_res(f"lakehouse:{lh['name']}", "storage", "fail", f"{type(exc).__name__}: {getattr(exc, 'message', exc)}"[:200]))
        return out

    def _check_compute(self, principal: Principal | None, opts: dict[str, Any]) -> list[dict[str, Any]]:
        compute = self.ctx.maybe_service("compute")
        if compute is None:
            return [_res("compute", "compute", "skipped", "compute module not loaded")]
        out = []
        workers = compute.registry.list_workers(include_revoked=False)
        online = [w for w in workers if w.get("connected")]
        counts = compute.jobs.counts()
        st = "ok" if online else "warn"
        out.append(_res("workers", "compute", st, f"{len(online)} of {len(workers)} worker(s) connected; {counts['running']} running / {counts['queued']} queued job(s)",
                        remediation=None if online else "start a worker agent or enable compute.embedded_worker",
                        data={"workers": [{"name": w["name"], "state": w["state"], "scheduling": w["scheduling"], "connected": w["connected"], "agent_version": w.get("agent_version")} for w in workers]}))
        if opts["worker_probes"]:
            try:
                res = compute.topology_service.test_all()
                out.append(_res("worker-probes", "compute", "ok" if res.get("ok") else "fail", f"{sum(1 for n in res['nodes'].values() if n['ok'])} of {len(res['nodes'])} node(s) passed the probe suite",
                                data={"matrix": res.get("matrix")}))
            except Exception as exc:  # noqa: BLE001
                out.append(_res("worker-probes", "compute", "fail", f"{type(exc).__name__}: {getattr(exc, 'message', exc)}"[:200]))
        if opts["spark"]:
            if principal is None or not principal.can("compute.manage"):
                out.append(_res("spark-smoke", "compute", "skipped", "running the Spark smoke test needs the compute.manage permission"))
            else:
                from olcf.compute.topology import SmokeRequest

                try:
                    res = compute.topology_service.spark_smoke(principal, SmokeRequest())
                    out.append(_res("spark-smoke", "compute", "ok" if res.get("ok") else "fail", f"distributed Spark job {res.get('state')} (runtime {res.get('runtime_id')}, provider {res.get('provider')})",
                                    data={k: res.get(k) for k in ("job_id", "state", "nodes", "distributed", "duration_ms") if k in res}))
                except Exception as exc:  # noqa: BLE001
                    out.append(_res("spark-smoke", "compute", "fail", f"{type(exc).__name__}: {getattr(exc, 'message', exc)}"[:300], remediation="see the job's logs; verify a runtime is installed on a worker"))
        else:
            out.append(_res("spark-smoke", "compute", "skipped", "not requested (pass spark=true to run a real distributed smoke job)"))
        return out

    def _targets(self) -> list[tuple[str, str]]:
        """(label, url) pairs worth probing."""
        out: list[tuple[str, str]] = []
        s = self.ctx.settings
        out.append(("control-plane", s.base_url))
        try:
            from olcf.security import schema as SEC

            with self.ctx.db.connect() as conn:
                for r in fetch_all(conn, select(SEC.identity_providers.c.name, SEC.identity_providers.c.issuer, SEC.identity_providers.c.enabled)):
                    if r["enabled"] and r.get("issuer"):
                        out.append((f"idp:{r['name']}", r["issuer"]))
        except Exception:  # noqa: BLE001, S110
            pass
        svc = self.ctx.maybe_service("updates")
        if svc is not None and svc.get_config().get("feed_url"):
            out.append(("update-feed", svc.get_config()["feed_url"]))
        try:
            from olcf.ops import schema as O

            with self.ctx.db.connect() as conn:
                for r in fetch_all(conn, select(O.backup_targets.c.name, O.backup_targets.c.type, O.backup_targets.c.config).where(O.backup_targets.c.enabled.is_(True))):
                    ep = (r["config"] or {}).get("endpoint_url") or (f"sftp://{r['config'].get('host')}:{r['config'].get('port', 22)}" if r["type"] == "sftp" else None)
                    if ep:
                        out.append((f"backup-target:{r['name']}", ep))
        except Exception:  # noqa: BLE001, S110
            pass
        return out

    def _check_network(self, principal: Principal | None, opts: dict[str, Any]) -> list[dict[str, Any]]:
        out = []
        env = {k: re.sub(r"//[^/@]*@", "//***@", v) for k, v in os.environ.items() if k.lower() in ("http_proxy", "https_proxy", "no_proxy", "all_proxy")}
        ca = self.ctx.settings.tls.ca_bundle
        out.append(_res("proxy", "network", "ok", ("proxy environment: " + ", ".join(f"{k}={v}" for k, v in sorted(env.items()))) if env else "no proxy environment variables", data={"env": env, "ca_bundle": ca}))
        if ca and not Path(ca).is_file():
            out.append(_res("ca-bundle", "network", "fail", f"tls.ca_bundle {ca} does not exist", remediation="fix the path or remove the setting"))
        for label, url in self._targets():
            u = urlsplit(url)
            host = u.hostname
            if not host:
                continue
            port = u.port or {"https": 443, "http": 80, "sftp": 22}.get(u.scheme, 443)
            t0 = time.perf_counter()
            try:
                addrs = sorted({a[4][0] for a in socket.getaddrinfo(host, port, type=socket.SOCK_STREAM)})
                dns = (True, ", ".join(addrs[:4]))
            except OSError as exc:
                dns = (False, str(exc))
            out.append(_res(f"dns:{label}", "network", "ok" if dns[0] else "fail", f"{host} -> {dns[1]}" if dns[0] else f"cannot resolve {host}: {dns[1]}",
                            remediation=None if dns[0] else "check /etc/resolv.conf, split-horizon DNS and the configured URL", ms=(time.perf_counter() - t0) * 1000))
            if dns[0]:
                t0 = time.perf_counter()
                via_proxy = bool(env.get("https_proxy") or env.get("HTTPS_PROXY")) and u.scheme == "https" and not self._no_proxy(host)
                try:
                    with socket.create_connection((host, port), timeout=3):
                        out.append(_res(f"tcp:{label}", "network", "ok", f"{host}:{port} accepts connections", ms=(time.perf_counter() - t0) * 1000))
                except OSError as exc:
                    out.append(_res(f"tcp:{label}", "network", "warn" if via_proxy else "fail", f"{host}:{port} not reachable directly: {exc}" + (" (an HTTPS proxy is configured; direct access may be blocked by design)" if via_proxy else ""),
                                    remediation=None if via_proxy else "check firewall rules, security groups and that the service is running"))
        try:
            port = self.ctx.settings.server.port
            with socket.socket() as s:
                s.settimeout(1)
                listening = s.connect_ex(("127.0.0.1", port)) == 0
            out.append(_res("control-plane-port", "network", "ok" if listening else "warn", f"port {port} {'is listening locally' if listening else 'is not listening on 127.0.0.1 (embedded test server or different bind address)'}"))
        except OSError:
            pass
        return out

    @staticmethod
    def _no_proxy(host: str) -> bool:
        np = os.environ.get("no_proxy") or os.environ.get("NO_PROXY") or ""
        return any(host == p.strip() or host.endswith("." + p.strip().lstrip(".")) for p in np.split(",") if p.strip())

    def _check_time(self, principal: Principal | None, opts: dict[str, Any]) -> list[dict[str, Any]]:
        out = []
        if self.ctx.db.is_postgres:
            before = time.time()
            with self.ctx.db.connect() as conn:
                dbnow = conn.execute(text("SELECT extract(epoch from clock_timestamp())")).scalar()
            skew = float(dbnow) - (before + time.time()) / 2
            out.append(_res("database-clock", "time", "ok" if abs(skew) < 2 else "warn" if abs(skew) < 10 else "fail", f"database server clock differs from this node by {skew:+.2f} s",
                            remediation="synchronise time (chrony / systemd-timesyncd) on all control-plane nodes and the database host; leader leases and token expiry depend on it" if abs(skew) >= 2 else None))
        else:
            out.append(_res("database-clock", "time", "ok", "SQLite runs in this process: no separate database clock"))
        compute = self.ctx.maybe_service("compute")
        if compute is not None:
            skews = {w["name"]: (w.get("notes") or {}).get("clock_skew_seconds") for w in compute.registry.list_workers(include_revoked=False) if w.get("connected")}
            bad = {k: v for k, v in skews.items() if isinstance(v, (int, float)) and abs(v) > 5}
            out.append(_res("worker-clocks", "time", "warn" if bad else "ok", f"workers with skew > 5 s: {bad}" if bad else f"{len(skews)} connected worker(s), all within 5 s",
                            remediation="fix time synchronisation on the listed nodes (mTLS, token expiry and Spark all depend on it)" if bad else None, data={"skews": skews}))
        if opts["network_time"]:
            tried = []
            for label, url in self._targets():
                if not url.startswith("https://"):
                    continue
                try:
                    t0 = time.time()
                    r = httpx.head(url, timeout=5.0, follow_redirects=False)
                    d = r.headers.get("date")
                    if d:
                        from email.utils import parsedate_to_datetime

                        skew = parsedate_to_datetime(d).timestamp() - (t0 + time.time()) / 2
                        out.append(_res(f"network-time:{label}", "time", "ok" if abs(skew) < 5 else "warn", f"clock differs from {urlsplit(url).hostname} (HTTP Date) by {skew:+.1f} s"))
                        tried.append(label)
                        break
                except Exception:  # noqa: BLE001, S112
                    continue
            if not tried:
                out.append(_res("network-time", "time", "skipped", "no HTTPS endpoint with a Date header was reachable"))
        out.append(_res("system-clock", "time", "ok", f"UTC now {datetime.now(UTC).isoformat(timespec='seconds')}"))
        return out

    def _check_certificates(self, principal: Principal | None, opts: dict[str, Any]) -> list[dict[str, Any]]:
        from cryptography import x509

        out = []
        paths = {"ca": self.ctx.settings.path("pki", "ca.crt"), "server": self.ctx.settings.path("pki", "server.crt")}
        if self.ctx.settings.tls.cert_file:
            paths["tls.cert_file"] = Path(self.ctx.settings.tls.cert_file)
        for label, p in paths.items():
            if not p.is_file():
                out.append(_res(f"cert:{label}", "certificates", "ok" if label != "ca" else "warn", f"{label} certificate not present ({p.name})"))
                continue
            try:
                cert = x509.load_pem_x509_certificate(p.read_bytes())
                days = (cert.not_valid_after_utc - datetime.now(UTC)).days
                st = "fail" if days < 0 else "warn" if days < 30 else "ok"
                out.append(_res(f"cert:{label}", "certificates", st, f"{cert.subject.rfc4514_string()} valid until {cert.not_valid_after_utc.date()} ({days} days)",
                                remediation="renew / re-issue the certificate" if st != "ok" else None, data={"serial": format(cert.serial_number, "x"), "days_left": days}))
            except Exception as exc:  # noqa: BLE001
                out.append(_res(f"cert:{label}", "certificates", "fail", f"cannot parse {p.name}: {type(exc).__name__}"))
        return out

    def _check_logs(self, principal: Principal | None, opts: dict[str, Any]) -> list[dict[str, Any]]:
        log_dir = self.ctx.settings.path("logs")
        files = sorted(log_dir.glob("*.log"))
        if not files:
            return [_res("errors", "logs", "ok", "no log files in the data directory (logging to stderr)")]
        errs: Counter[str] = Counter()
        total = 0
        from olcf.ops.leakscan import LeakScanner

        scrub = LeakScanner.from_context(self.ctx)
        for f in files[-3:]:
            tail = self._tail(f, 1024 * 1024)
            for line in tail.splitlines():
                if " ERROR " in line or " CRITICAL " in line:
                    total += 1
                    msg = re.sub(r"\b[0-9a-f]{16,}\b|\b\d+\b", "#", line.split(": ", 1)[-1])[:140]
                    errs[scrub.scrub_text(redact_text(msg))] += 1
        st = "warn" if total > 20 else "ok"
        return [_res("errors", "logs", st, f"{total} ERROR/CRITICAL line(s) in the recent log tail", data={"top": errs.most_common(10)})]

    @staticmethod
    def _tail(path: Path, limit: int) -> str:
        with open(path, "rb") as fh:
            fh.seek(0, 2)
            size = fh.tell()
            fh.seek(max(0, size - limit))
            return fh.read().decode("utf-8", "replace")

    def _check_config(self, principal: Principal | None, opts: dict[str, Any]) -> list[dict[str, Any]]:
        import json

        cfg = redact_obj(json.loads(self.ctx.settings.model_dump_json()))
        cfg["database_url"] = re.sub(r"//([^/:@]+):[^@]*@", r"//\1:***REDACTED***@", self.ctx.settings.database_url)
        return [_res("effective-settings", "config", "ok", "effective settings (secrets redacted)", data=cfg)]


# ===================================================================== health check
def install_diagnostics(ctx: AppContext) -> DiagnosticsService:
    svc = DiagnosticsService(ctx)
    ctx.register_service("diagnostics", svc)

    def quick() -> ComponentHealth:
        last = svc.last_summary()
        if not last:
            return ComponentHealth("diagnostics", Status.OK, "no diagnostics run yet", category="operations")
        age_h = (datetime.now(UTC) - datetime.fromisoformat(last["started_at"].replace("Z", "+00:00"))).total_seconds() / 3600
        st = {"ok": Status.OK, "degraded": Status.DEGRADED, "down": Status.DEGRADED}[last["status"]]
        detail = f"last run {age_h:.1f} h ago: {last['summary']['ok']} ok, {last['summary']['warn']} warning(s), {last['summary']['fail']} failure(s)"
        if last["failed"]:
            detail += "; failing: " + ", ".join(f["name"] for f in last["failed"][:5])
        return ComponentHealth("diagnostics", st, detail, category="operations", labels={"last": last["summary"], "at": last["started_at"]})

    def deep() -> ComponentHealth:
        rep = svc.run(None, only=["database", "auth", "storage", "time", "certificates"], deep_health=False)
        st = {"ok": Status.OK, "degraded": Status.DEGRADED, "down": Status.DOWN}[rep["status"]]
        bad = [c for c in rep["checks"] if c["status"] in ("fail", "warn")]
        return ComponentHealth("diagnostics", st, f"{rep['summary']['ok']} ok, {rep['summary']['warn']} warning(s), {rep['summary']['fail']} failure(s)" + (": " + bad[0]["detail"][:150] if bad else ""),
                               category="operations", steps=[{"name": c["name"], "ok": c["status"] in ("ok", "skipped"), "detail": c["detail"][:200]} for c in rep["checks"]], mode="deep")

    ctx.health.register(HealthCheck("diagnostics", "operations", "Diagnostics suite (database, auth, storage, time, certificates)", quick, deep, ttl=30))
    return svc
