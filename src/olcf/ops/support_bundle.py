# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Support bundle (service ``support``): one zip a customer can hand to support without handing over secrets.

Content is an **allow-list** (nothing is copied from the data directory wholesale):

    summary.json            versions, platform, uptime, module list, database dialect/migrations/row counts, leader, settings (redacted)
    diagnostics.json        a fresh diagnostics report (or the last one if younger than 5 minutes)
    health.json             health snapshot (quick checks)
    versions.txt            installed Python distributions
    logs/<file>.log         bounded tails of the platform logs, redacted
    config/settings.json    effective settings, redacted
    audit/recent.jsonl      OPT-IN  recent audit records
    jobs/recent.json        OPT-IN  recent failed/finished jobs (state, error, runtime; never the code or parameters)
    workers/<name>.zip      OPT-IN  each connected worker's own redacted diagnostics archive

Sanitized by default: IP addresses, e-mail addresses and the host name are replaced by per-bundle pseudonyms (same value -> same pseudonym
inside one bundle, so support can still correlate lines). ``sanitize=false`` keeps them.

Never included: secrets, key material, tokens, the bootstrap credential file, backups, job source, connection secrets - and as a safety net the
finished zip is scanned (:mod:`olcf.ops.leakscan`) for every live secret value in the secret store, the master/signing/CA keys and for
credential patterns; **a finding aborts bundle creation** (nothing is kept) and is audited.
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
import re
import socket
import time
import zipfile
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from sqlalchemy import func, select

from olcf.core import hostname
from olcf.core.context import AppContext
from olcf.core.errors import NotFound
from olcf.core.ids import new_id
from olcf.core.redaction import redact_obj, redact_text
from olcf.core.timeutil import now_iso
from olcf.db import schema as S
from olcf.db.util import fetch_all, fetch_one, insert_row
from olcf.ops import schema as O
from olcf.ops.leakscan import LeakDetected, LeakScanner
from olcf.security.rbac import Principal
from olcf.version import __version__

log = logging.getLogger("olcf.ops.support")
LOG_TAIL = 1024 * 1024
_IPV4 = re.compile(r"\b(?!127\.0\.0\.1\b|0\.0\.0\.0\b)(?:(?:25[0-5]|2[0-4]\d|1?\d?\d)\.){3}(?:25[0-5]|2[0-4]\d|1?\d?\d)\b")
_IPV6 = re.compile(r"\b(?:[0-9a-f]{1,4}:){3,7}[0-9a-f]{1,4}\b", re.I)
_EMAIL = re.compile(r"\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}\b")


class Sanitizer:
    """Per-bundle pseudonymisation of network identifiers and e-mail addresses."""

    def __init__(self, enabled: bool, hostnames: list[str]) -> None:
        self.enabled = enabled
        self.salt = os.urandom(8).hex()
        self.hosts = [h for h in hostnames if h and h not in ("localhost",)]

    def _p(self, prefix: str, v: str) -> str:
        return f"{prefix}-{hashlib.sha256((self.salt + v.lower()).encode()).hexdigest()[:8]}"

    def text(self, s: str) -> str:
        s = redact_text(s)
        if not self.enabled:
            return s
        s = _EMAIL.sub(lambda m: self._p("email", m.group(0)) + "@redacted.invalid", s)
        s = _IPV4.sub(lambda m: self._p("ip", m.group(0)), s)
        s = _IPV6.sub(lambda m: self._p("ip6", m.group(0)), s)
        for h in self.hosts:
            s = re.sub(re.escape(h), self._p("host", h), s, flags=re.I)
        return s

    def obj(self, o: Any) -> Any:
        o = redact_obj(o)
        if not self.enabled:
            return o
        return json.loads(self.text(json.dumps(o, default=str)))


class SupportBundleService:
    """Builds support bundles: archives of diagnostics, logs and configuration with secrets removed and, optionally, addresses and host names pseudonymised.
    """
    def __init__(self, ctx: AppContext) -> None:
        self.ctx = ctx
        self.dir = ctx.settings.path("artifacts", "support-bundles")

    # ------------------------------------------------------------------ records
    def list(self, limit: int = 50) -> list[dict[str, Any]]:
        with self.ctx.db.connect() as conn:
            return [self.public(r) for r in fetch_all(conn, select(O.support_bundles).order_by(O.support_bundles.c.created_at.desc()).limit(limit))]

    def get(self, bid: str) -> dict[str, Any]:
        with self.ctx.db.connect() as conn:
            row = fetch_one(conn, select(O.support_bundles).where(O.support_bundles.c.id == bid))
        if not row:
            raise NotFound(f"support bundle '{bid}' not found")
        return row

    @staticmethod
    def public(r: dict[str, Any]) -> dict[str, Any]:
        return {k: r.get(k) for k in ("id", "filename", "size_bytes", "sha256", "options", "scan", "created_by", "created_at")}

    def file_path(self, rec: dict[str, Any]) -> Path:
        p = (self.dir / Path(rec["filename"]).name).resolve()
        if self.dir.resolve() not in p.parents or not p.is_file():
            raise NotFound("the support bundle file no longer exists")
        return p

    def delete(self, principal: Principal, bid: str) -> None:
        rec = self.get(bid)
        (self.dir / Path(rec["filename"]).name).unlink(missing_ok=True)
        from sqlalchemy import delete

        with self.ctx.db.begin() as conn:
            conn.execute(delete(O.support_bundles).where(O.support_bundles.c.id == bid))
        self.ctx.audit.record(principal, "admin.support_bundle_deleted", resource_type="support_bundle", resource_id=bid)

    # ------------------------------------------------------------------ create
    def create(self, principal: Principal, options: dict[str, Any] | None = None, *, extra_canaries: dict[str, str] | None = None) -> dict[str, Any]:
        o = {"sanitize": True, "include_logs": True, "include_config": True, "include_diagnostics": True, "include_health": True, "include_audit": False, "audit_limit": 500,
             "include_jobs": False, "job_limit": 100, "include_worker_diagnostics": False, "log_tail_kb": 1024, **(options or {})}
        bid = new_id("sbd")
        self.dir.mkdir(parents=True, exist_ok=True, mode=0o700)
        stamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
        filename = f"olcf-support-{stamp}-{bid.split('_', 1)[1][-6:].lower()}.zip"
        path = self.dir / filename
        san = Sanitizer(bool(o["sanitize"]), [socket.gethostname(), hostname.fqdn()])
        entries: dict[str, bytes] = {}

        def put(name: str, data: Any) -> None:
            entries[name] = (data if isinstance(data, bytes) else (data if isinstance(data, str) else json.dumps(data, indent=2, default=str, sort_keys=True)).encode())

        try:
            put("summary.json", san.obj(self._summary()))
            put("versions.txt", san.text(self._versions()))
            if o["include_config"]:
                cfg = json.loads(self.ctx.settings.model_dump_json())
                cfg["database_url"] = re.sub(r"//([^/:@]+):[^@]*@", r"//\1:***REDACTED***@", self.ctx.settings.database_url)
                put("config/settings.json", san.obj(cfg))
            if o["include_health"]:
                put("health.json", san.obj(self.ctx.health.snapshot()))
            if o["include_diagnostics"]:
                svc = self.ctx.service("diagnostics")
                last = svc.last()
                if last and (time.time() - datetime.fromisoformat(last["started_at"].replace("Z", "+00:00")).timestamp()) < 300:
                    rep = last
                else:
                    rep = svc.run(principal, deep_health=False)
                put("diagnostics.json", san.obj(rep))
            if o["include_logs"]:
                for f in sorted(self.ctx.settings.path("logs").glob("*.log"))[-5:]:
                    put(f"logs/{f.name}", san.text(self._tail(f, int(o["log_tail_kb"]) * 1024)))
            if o["include_audit"]:
                rows = self.ctx.audit.query(limit=int(o["audit_limit"]))
                if san.enabled:
                    for r in rows:
                        r["actor_name"] = san._p("user", r["actor_name"]) if r.get("actor_name") else None  # noqa: SLF001
                put("audit/recent.jsonl", "\n".join(json.dumps(san.obj(r), default=str, sort_keys=True) for r in rows) + "\n")
            if o["include_jobs"]:
                put("jobs/recent.json", san.obj(self._jobs(int(o["job_limit"]))))
            if o["include_worker_diagnostics"]:
                for name, blob in self._worker_archives().items():
                    put(f"workers/{re.sub(r'[^A-Za-z0-9._-]', '_', name)}.zip", blob)
            put("MANIFEST.txt", self._manifest(o, entries))
            self._write_zip(path, entries)
            scanner = LeakScanner.from_context(self.ctx, extra_canaries)
            scan = scanner.scan_file(path, filename)
        except BaseException:
            path.unlink(missing_ok=True)
            raise
        if not scan.ok:
            path.unlink(missing_ok=True)
            kinds = sorted({f"{f.kind}@{f.file}" for f in scan.findings})[:20]
            self.ctx.audit.record(principal, "admin.support_bundle_blocked", resource_type="support_bundle", outcome="failure",
                                  details={"findings": [f.to_dict() for f in scan.findings[:20]], "options": {k: v for k, v in o.items() if isinstance(v, (bool, int))}})
            raise LeakDetected(f"support bundle creation aborted: {len(scan.findings)} potential secret(s) were detected in the generated archive (nothing was kept)",
                               details={"findings": kinds}, remediation="report this to the OLCF maintainers - a component is writing secrets where it must not; do not share raw logs")
        from olcf.ops.crypto import sha256_file

        os.chmod(path, 0o600)
        rec = {"id": bid, "filename": filename, "size_bytes": path.stat().st_size, "sha256": sha256_file(path), "options": o, "scan": scan.to_dict(), "created_by": principal.id, "created_at": now_iso()}
        with self.ctx.db.begin() as conn:
            insert_row(conn, O.support_bundles, rec)
        self.ctx.audit.record(principal, "admin.support_bundle_created", resource_type="support_bundle", resource_id=bid,
                              details={"size": rec["size_bytes"], "files": len(entries), "sanitized": bool(o["sanitize"]), "audit": o["include_audit"], "jobs": o["include_jobs"], "workers": o["include_worker_diagnostics"]})
        return self.public(rec)

    # ------------------------------------------------------------------ parts
    @staticmethod
    def _write_zip(path: Path, entries: dict[str, bytes]) -> None:
        tmp = path.with_suffix(".tmp")
        with zipfile.ZipFile(tmp, "w", zipfile.ZIP_DEFLATED, compresslevel=6) as zf:
            for name, data in sorted(entries.items()):
                zi = zipfile.ZipInfo(name, date_time=time.gmtime()[:6])
                zi.compress_type = zipfile.ZIP_DEFLATED
                zi.external_attr = 0o600 << 16
                zf.writestr(zi, data)
        os.replace(tmp, path)

    @staticmethod
    def _tail(path: Path, limit: int) -> str:
        with open(path, "rb") as fh:
            fh.seek(0, 2)
            size = fh.tell()
            fh.seek(max(0, size - limit))
            data = fh.read().decode("utf-8", "replace")
        return ("[... truncated ...]\n" if size > limit else "") + data

    def _manifest(self, options: dict[str, Any], entries: dict[str, bytes]) -> str:
        lines = [f"OLCF support bundle, created {now_iso()} by version {__version__}", f"sanitized: {options['sanitize']}", "", "files:"]
        lines += [f"  {n}  {len(b)} bytes  sha256={hashlib.sha256(b).hexdigest()[:16]}" for n, b in sorted(entries.items())]
        lines += ["", "opt-in sections included: " + (", ".join(k for k in ("include_audit", "include_jobs", "include_worker_diagnostics") if options[k]) or "none"),
                  "This archive was scanned for secret values and credential patterns before it was released."]
        return "\n".join(lines) + "\n"

    def _summary(self) -> dict[str, Any]:
        import platform
        import sys

        from olcf.db import migrate as dbm

        counts = {}
        for t in ("principals", "workspaces", "items", "connections", "secrets", "audit_log", "events", "backups", "operations"):
            try:
                with self.ctx.db.connect() as conn:
                    counts[t] = conn.execute(select(func.count()).select_from(S.metadata.tables[t])).scalar_one()
            except Exception:  # noqa: BLE001, S112
                continue
        holder = self.ctx.leader.current_holder()
        return {"product": "OLCF", "version": __version__, "python": sys.version, "platform": platform.platform(), "node": self.ctx.node_id, "uptime_seconds": round(self.ctx.uptime, 1),
                "environment": self.ctx.settings.environment, "modules": [m.name for m in self.ctx.modules], "services": sorted(self.ctx.services), "database": {"dialect": self.ctx.db.dialect,
                "migrations": dbm.current_versions(self.ctx.db), "rows": counts}, "leader": {"holder": (holder or {}).get("holder"), "term": (holder or {}).get("term"), "this_node": self.ctx.leader.is_leader()},
                "created_at": now_iso()}

    @staticmethod
    def _versions() -> str:
        import importlib.metadata as md

        return "\n".join(sorted(f"{d.metadata['Name']}=={d.version}" for d in md.distributions() if d.metadata["Name"])) + "\n"

    def _jobs(self, limit: int) -> list[dict[str, Any]]:
        try:
            from olcf.compute import schema as C
        except ImportError:
            return []
        with self.ctx.db.connect() as conn:
            rows = fetch_all(conn, select(C.jobs.c.id, C.jobs.c.kind, C.jobs.c.state, C.jobs.c.state_reason, C.jobs.c.runtime_id, C.jobs.c.provider, C.jobs.c.queue, C.jobs.c.attempt,
                                          C.jobs.c.created_at).order_by(C.jobs.c.created_at.desc()).limit(limit))
        return rows

    def _worker_archives(self) -> dict[str, bytes]:
        import base64

        compute = self.ctx.maybe_service("compute")
        out: dict[str, bytes] = {}
        if compute is None:
            return out
        for w in compute.registry.list_workers(include_revoked=False):
            if not w.get("connected"):
                continue
            try:
                res = compute.hub.request(w["id"], "collect_diagnostics", {"inline": True}, timeout=60.0)
                if res.get("archive_b64"):
                    out[w["name"]] = base64.b64decode(res["archive_b64"])
            except Exception as exc:  # noqa: BLE001
                log.warning("worker %s diagnostics unavailable: %s", w["name"], exc)
        return out


def install_support(ctx: AppContext) -> SupportBundleService:
    svc = SupportBundleService(ctx)
    ctx.register_service("support", svc)
    return svc
