# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Platform updates (service name ``updates``): signed manifests, preflight, maintenance windows, drain, staged apply, hand-off and rollback.

How an update flows
-------------------
1. **Obtain** a manifest: ``check`` (HTTPS feed per channel, proxy/CA aware) or ``import_bundle`` (offline tarball containing the signed
   manifest and its artifacts, for air-gapped sites). Either way the manifest is verified (:mod:`olcf.ops.manifest`): Ed25519 signature by
   a trusted key, channel, expiry, anti-rollback sequence. Nothing unsigned is ever stored as an update, and ``apply`` re-verifies the stored
   envelope - there is no flag, setting or endpoint that applies an unverified update.
2. **Preflight**: artifact hashes, compatibility matrix (Python, database dialect, installed -> target version, worker agent versions), disk
   space, active jobs, and a **real migration dry-run**: the current database is dumped, restored into a throw-away SQLite file and the *new*
   release's migrations are executed against it in a subprocess that imports the new wheel.
3. **Apply** (inside a maintenance window): automatic ``pre-update`` backup, drain compute (workers draining, queues paused, wait for running
   jobs), import runtime bundles through the runtimes service, write the hand-off marker ``<data_dir>/updates/handoff.json``.
4. **Hand-off**: replacing the code of the running process is not something a Python service can do safely to itself. ``python -m
   olcf.ops.upgrade apply`` (also reachable as ``olcf upgrade``) re-verifies the signature and wheel hash, installs the wheel into the environment
   and optionally runs a configured restart command; the new version runs the schema migrations on start, and ``reconcile`` marks the update
   *applied* (resuming compute) when it sees the target version running. Container/Kubernetes installs replace the image instead, and the
   same marker/reconcile logic applies. Honest limits: pip-installed single-host upgrades need a process restart that this service does not
   perform unless ``restart_command`` is configured.
5. **Rollback**: before the hand-off the update is simply cancelled. After it, ``rollback`` restores the automatic pre-update backup (the new
   release's migrations are not reversible in general) and reinstalls the previous version via ``python -m olcf.ops.upgrade rollback``.
"""

from __future__ import annotations

import hashlib
import io
import json
import logging
import os
import shutil
import subprocess
import sys
import tarfile
import tempfile
import threading
import time
import zipfile
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import httpx
from packaging.specifiers import InvalidSpecifier, SpecifierSet
from packaging.version import Version
from sqlalchemy import select, update

from olcf.core.context import AppContext
from olcf.core.errors import BadRequest, Conflict, NotFound, Unavailable
from olcf.core.ids import new_id
from olcf.core.timeutil import now_iso
from olcf.db import schema as S
from olcf.db.engine import Database
from olcf.db.util import fetch_all, fetch_one, insert_row
from olcf.ops import backup_core as core
from olcf.ops import manifest as M
from olcf.ops import schema as O
from olcf.ops.common import run_operation
from olcf.security.rbac import Principal
from olcf.version import __version__

log = logging.getLogger("olcf.ops.updates")

CONFIG_KEY, TRUST_KEY, STATE_KEY, MAINT_KEY = "ops.updates.config", "ops.updates.trust", "ops.updates.state", "ops.maintenance"
DEFAULT_CONFIG: dict[str, Any] = {
    "channel": "stable", "feed_url": None, "proxy": None, "ca_bundle": None, "allow_insecure_http": False, "threshold": 1,
    "maintenance_windows": [], "drain_timeout_seconds": 600, "auto_check": False, "check_interval_hours": 24, "restart_command": None,
    "fetch_previous": True, "dry_run_max_rows": 5_000_000,
}
MAX_ARTIFACT = 8 * 1024**3
MAX_BUNDLE_FILES = 512
MAX_BUNDLE_BYTES = 32 * 1024**3
SYSTEM = Principal(id="usr_system", name="system", kind="service")


def installed_version() -> str:
    """Overridable in tests via monkeypatching ``olcf.ops.updates.installed_version``."""
    return __version__


class UpdateService:
    """Checks for updates, verifies and stages them, and prepares the upgrade hand-off."""
    def __init__(self, ctx: AppContext) -> None:
        self.ctx = ctx
        self.root = ctx.settings.path("updates")
        self._lock = threading.Lock()

    # ------------------------------------------------------------------ kv helpers
    def _kv_get(self, key: str, default: Any) -> Any:
        with self.ctx.db.connect() as conn:
            row = fetch_one(conn, select(S.kv).where(S.kv.c.key == key))
        return row["value"] if row else default

    def _kv_set(self, key: str, value: Any) -> None:
        with self.ctx.db.begin() as conn:
            n = conn.execute(update(S.kv).where(S.kv.c.key == key).values(value=value, updated_at=now_iso())).rowcount
            if not n:
                conn.execute(S.kv.insert().values(key=key, value=value, updated_at=now_iso()))

    # ------------------------------------------------------------------ config
    def get_config(self) -> dict[str, Any]:
        cfg = dict(DEFAULT_CONFIG)
        cfg.update(self._kv_get(CONFIG_KEY, {}))
        return cfg

    def set_config(self, principal: Principal, patch: dict[str, Any]) -> dict[str, Any]:
        from croniter import croniter

        cfg = self.get_config()
        for k, v in patch.items():
            if k not in DEFAULT_CONFIG:
                raise BadRequest(f"unknown update setting '{k}'", details={"known": sorted(DEFAULT_CONFIG)})
            cfg[k] = v
        if cfg["channel"] not in M.CHANNELS:
            raise BadRequest(f"channel must be one of {list(M.CHANNELS)}")
        if cfg["feed_url"] is not None:
            u = httpx.URL(str(cfg["feed_url"]))
            if u.scheme not in ("http", "https") or not u.host:
                raise BadRequest("feed_url must be an http(s) URL")
            if u.scheme == "http" and not cfg["allow_insecure_http"]:
                raise BadRequest("feed_url must be https (set allow_insecure_http only for isolated test feeds)")
        if not isinstance(cfg["threshold"], int) or not 1 <= cfg["threshold"] <= 5:
            raise BadRequest("threshold must be an integer between 1 and 5")
        for w in cfg["maintenance_windows"]:
            if not isinstance(w, dict) or not croniter.is_valid(str(w.get("cron", ""))) or not isinstance(w.get("minutes"), int) or not 1 <= w["minutes"] <= 1440:
                raise BadRequest("each maintenance window needs a valid 5-field 'cron' start and 'minutes' (1-1440)")
        self._kv_set(CONFIG_KEY, cfg)
        self.ctx.audit.record(principal, "admin.update_config_changed", resource_type="update_config", details={"keys": sorted(patch)})
        return cfg

    # ------------------------------------------------------------------ trust store
    def _file_anchors(self) -> list[M.TrustedKey]:
        out = []
        p = self.ctx.settings.path("trust", "update-keys.json")
        if p.is_file():
            try:
                for k in json.loads(p.read_text()):
                    raw = __import__("base64").b64decode(k["public_key"])
                    out.append(M.TrustedKey(M.keyid_of(raw), raw, k.get("name", "file-anchor")))
            except (ValueError, KeyError, TypeError):
                log.warning("ignoring malformed trust anchor file %s", p)
        env = os.environ.get("OLCF_UPDATE_TRUST_KEYS")
        if env:
            for part in env.split(","):
                try:
                    raw = __import__("base64").b64decode(part.strip())
                    out.append(M.TrustedKey(M.keyid_of(raw), raw, "env-anchor"))
                except ValueError:
                    log.warning("ignoring malformed key in OLCF_UPDATE_TRUST_KEYS")
        return out

    def trusted_keys(self) -> list[M.TrustedKey]:
        import base64

        out = {k.keyid: k for k in self._file_anchors()}
        for r in self._kv_get(TRUST_KEY, []):
            raw = base64.b64decode(r["public_key"])
            out[r["keyid"]] = M.TrustedKey(r["keyid"], raw, r.get("name", ""), bool(r.get("revoked_at")))
        return list(out.values())

    def trust_list(self) -> list[dict[str, Any]]:
        import base64

        anchors = {k.keyid for k in self._file_anchors()}
        rows = {r["keyid"]: r for r in self._kv_get(TRUST_KEY, [])}
        out = [{"keyid": k.keyid, "public_key": base64.b64encode(k.public_raw).decode(), "name": k.name, "revoked": k.revoked, "source": "file" if k.keyid in anchors and k.keyid not in rows else "database",
                "added_at": rows.get(k.keyid, {}).get("added_at"), "added_by": rows.get(k.keyid, {}).get("added_by")} for k in self.trusted_keys()]
        return sorted(out, key=lambda x: x["keyid"])

    def trust_add(self, principal: Principal, public_key_b64: str, name: str) -> dict[str, Any]:
        import base64

        try:
            raw = base64.b64decode(public_key_b64, validate=True)
            M.Ed25519PublicKey.from_public_bytes(raw)
        except Exception as exc:  # noqa: BLE001
            raise BadRequest("public_key must be a base64 raw Ed25519 public key (32 bytes)") from exc
        kid = M.keyid_of(raw)
        rows = [r for r in self._kv_get(TRUST_KEY, []) if r["keyid"] != kid]
        rows.append({"keyid": kid, "public_key": base64.b64encode(raw).decode(), "name": name[:100], "added_by": principal.id, "added_at": now_iso(), "revoked_at": None})
        self._kv_set(TRUST_KEY, rows)
        self.ctx.audit.record(principal, "admin.update_trust_added", resource_type="update_key", resource_id=kid, details={"name": name[:100]})
        return {"keyid": kid, "name": name}

    def trust_revoke(self, principal: Principal, keyid: str) -> None:
        rows = self._kv_get(TRUST_KEY, [])
        hit = [r for r in rows if r["keyid"] == keyid]
        if not hit:
            raise NotFound(f"trust key '{keyid}' not found (file/env anchors are managed outside the database)")
        hit[0]["revoked_at"] = now_iso()
        self._kv_set(TRUST_KEY, rows)
        self.ctx.audit.record(principal, "admin.update_trust_revoked", resource_type="update_key", resource_id=keyid)

    # ------------------------------------------------------------------ records
    def _verify(self, data: Any, *, expect_channel: str | None = None) -> M.Verified:
        return M.verify_envelope(data, self.trusted_keys(), threshold=self.get_config()["threshold"], expect_channel=expect_channel)

    @staticmethod
    def public(r: dict[str, Any]) -> dict[str, Any]:
        payload = (r.get("manifest") or {}).get("payload", {})
        return {"id": r["id"], "version": r["version"], "channel": r["channel"], "state": r["state"], "source": r["source"], "notes": payload.get("notes", ""),
                "released_at": payload.get("released_at"), "expires_at": payload.get("expires_at"), "sequence": payload.get("sequence"),
                "artifacts": [{k: a.get(k) for k in ("kind", "name", "size", "sha256")} for a in payload.get("artifacts", [])],
                "verdict": r.get("verdict"), "preflight": r.get("preflight"), "backup_id": r.get("backup_id"), "result": r.get("result"),
                "staged": bool(r.get("staged_dir")), "created_at": r["created_at"], "updated_at": r["updated_at"]}

    def list(self, limit: int = 50) -> list[dict[str, Any]]:
        with self.ctx.db.connect() as conn:
            rows = fetch_all(conn, select(O.update_records).order_by(O.update_records.c.created_at.desc()).limit(limit))
        return [self.public(r) for r in rows]

    def get(self, uid: str) -> dict[str, Any]:
        with self.ctx.db.connect() as conn:
            row = fetch_one(conn, select(O.update_records).where(O.update_records.c.id == uid))
        if not row:
            raise NotFound(f"update '{uid}' not found")
        return row

    def _set(self, uid: str, **vals: Any) -> None:
        vals["updated_at"] = now_iso()
        with self.ctx.db.begin() as conn:
            conn.execute(update(O.update_records).where(O.update_records.c.id == uid).values(**vals))

    def _accept(self, principal: Principal | None, ver: M.Verified, source: str, staged_dir: str | None = None) -> dict[str, Any]:
        """Persist a verified manifest as an update record (anti-rollback checked and floor advanced)."""
        m = ver.manifest
        floor_all = self._kv_get(STATE_KEY, {}).get("floor", {})
        M.check_not_rollback(m, installed=installed_version(), floor=floor_all.get(m.channel))
        if m.parsed_version < Version(installed_version()):
            raise M.ManifestError(f"rollback refused: version {m.version} is older than the installed {installed_version()}",
                                  remediation="downgrades are only possible through the rollback procedure (restores the automatic pre-update backup)")
        with self.ctx.db.begin() as conn:
            existing = fetch_one(conn, select(O.update_records).where(O.update_records.c.version == m.version, O.update_records.c.channel == m.channel))
        verdict = {"keyids": ver.keyids, "threshold": self.get_config()["threshold"], "verified_at": now_iso(), "sequence": m.sequence}
        now = now_iso()
        if existing and existing["state"] in ("available", "staged", "failed", "rolled_back"):
            vals = {"manifest": {"envelope": ver.envelope, "payload": ver.payload}, "verdict": verdict, "source": source, "state": existing["state"] if existing["state"] != "failed" else "available"}
            if staged_dir:
                vals["staged_dir"] = staged_dir
            self._set(existing["id"], **vals)
            uid = existing["id"]
        elif existing:
            uid = existing["id"]
        else:
            uid = new_id("upd")
            rec = {"id": uid, "version": m.version, "channel": m.channel, "state": "staged" if staged_dir else "available", "manifest": {"envelope": ver.envelope, "payload": ver.payload},
                   "verdict": verdict, "source": source, "staged_dir": staged_dir, "created_by": principal.id if principal else None, "created_at": now, "updated_at": now}
            with self.ctx.db.begin() as conn:
                insert_row(conn, O.update_records, rec)
        st = self._kv_get(STATE_KEY, {})
        fl = st.setdefault("floor", {}).setdefault(m.channel, {"sequence": 0, "version": m.version})
        if m.sequence >= int(fl.get("sequence", 0)):
            st["floor"][m.channel] = {"sequence": m.sequence, "version": max(Version(fl.get("version", m.version)), m.parsed_version).public}
        self._kv_set(STATE_KEY, st)
        return self.get(uid)

    # ------------------------------------------------------------------ check (feed)
    def _client(self, cfg: dict[str, Any]) -> httpx.Client:
        """Explicit ``proxy`` wins; otherwise the standard HTTP(S)_PROXY / NO_PROXY environment applies. ``ca_bundle`` (or ``tls.ca_bundle``) for private CAs."""
        verify: Any = True
        cafile = cfg.get("ca_bundle") or self.ctx.settings.tls.ca_bundle
        if cafile:
            import ssl

            verify = ssl.create_default_context()  # system roots PLUS the private CA bundle
            verify.load_verify_locations(cafile=str(cafile))
        return httpx.Client(proxy=cfg.get("proxy") or None, verify=verify, timeout=httpx.Timeout(20.0, read=120.0), follow_redirects=False, trust_env=True,
                            headers={"User-Agent": f"olcf/{installed_version()} update-check"})

    def _get(self, client: httpx.Client, url: str, cfg: dict[str, Any], *, stream_to: Path | None = None, limit: int = M.MAX_ENVELOPE_BYTES) -> bytes | tuple[str, int]:
        for _ in range(4):
            u = httpx.URL(url)
            if u.scheme != "https" and not (u.scheme == "http" and cfg["allow_insecure_http"]):
                raise Unavailable(f"refusing to fetch {u.scheme} URL (https required)")
            with client.stream("GET", url) as resp:
                if resp.status_code in (301, 302, 303, 307, 308):
                    url = str(u.join(resp.headers.get("location", "")))
                    continue
                if resp.status_code != 200:
                    raise Unavailable(f"update server answered HTTP {resp.status_code} for {u.host}{u.path}")
                h, n = hashlib.sha256(), 0
                if stream_to is None:
                    buf = io.BytesIO()
                    for chunk in resp.iter_bytes():
                        n += len(chunk)
                        if n > limit:
                            raise Unavailable("response too large")
                        buf.write(chunk)
                    return buf.getvalue()
                with open(stream_to, "wb") as fh:
                    for chunk in resp.iter_bytes(1024 * 1024):
                        n += len(chunk)
                        if n > limit:
                            raise Unavailable("artifact larger than declared")
                        h.update(chunk)
                        fh.write(chunk)
                return h.hexdigest(), n
        raise Unavailable("too many redirects")

    def check(self, principal: Principal | None, channel: str | None = None) -> dict[str, Any]:
        cfg = self.get_config()
        channel = channel or cfg["channel"]
        if channel not in M.CHANNELS:
            raise BadRequest(f"unknown channel '{channel}'")
        feed = cfg.get("feed_url")
        if not feed:
            raise Unavailable("no update feed is configured", remediation="set feed_url in the update settings, or import an offline bundle (air-gapped sites)")
        result: dict[str, Any] = {"channel": channel, "installed": installed_version(), "checked_at": now_iso()}
        try:
            with self._client(cfg) as client:
                raw = self._get(client, f"{str(feed).rstrip('/')}/{channel}/manifest.json", cfg)
            ver = self._verify(raw, expect_channel=channel)
            rec = self._accept(principal, ver, f"{str(feed).rstrip('/')}/{channel}/manifest.json")
            newer = ver.manifest.parsed_version > Version(installed_version())
            result.update(update_available=newer, update=self.public(rec) if newer else None, latest=ver.manifest.version)
            if newer and ver.manifest.min_version and Version(installed_version()) < Version(ver.manifest.min_version):
                result["note"] = f"direct upgrade not supported from {installed_version()}: upgrade to {ver.manifest.min_version} (or later) first"
            self._kv_set(STATE_KEY, {**self._kv_get(STATE_KEY, {}), "last_check": {"at": now_iso(), "ok": True, "latest": ver.manifest.version}})
            self.ctx.audit.record(principal, "admin.update_checked", resource_type="update", details={"channel": channel, "latest": ver.manifest.version, "available": newer})
        except (M.ManifestError, Unavailable, httpx.HTTPError) as exc:
            msg = getattr(exc, "message", str(exc))
            self._kv_set(STATE_KEY, {**self._kv_get(STATE_KEY, {}), "last_check": {"at": now_iso(), "ok": False, "error": msg[:300]}})
            self.ctx.audit.record(principal, "admin.update_checked", resource_type="update", outcome="failure", details={"channel": channel, "error": msg[:300]})
            if isinstance(exc, httpx.HTTPError):
                raise Unavailable(f"could not reach the update server: {type(exc).__name__}: {exc}"[:300], remediation="check proxy / CA bundle settings; use an offline bundle if the host has no internet access") from exc
            raise
        return result

    # ------------------------------------------------------------------ offline bundle
    @staticmethod
    def _open_tar(path: Path) -> tarfile.TarFile:
        with open(path, "rb") as fh:
            magic = fh.read(4)
        if magic == b"\x28\xb5\x2f\xfd":
            import zstandard

            fh2 = open(path, "rb")  # noqa: SIM115 - closed with the TarFile
            return tarfile.open(fileobj=zstandard.ZstdDecompressor().stream_reader(fh2), mode="r|")
        return tarfile.open(path, mode="r:*")

    def import_bundle(self, principal: Principal, stream: Any, filename: str) -> dict[str, Any]:
        """Import an offline bundle: tarball with ``manifest.json`` (signed envelope) and the artifacts it lists. Verified before anything is trusted."""
        self.root.mkdir(parents=True, exist_ok=True)
        work = Path(tempfile.mkdtemp(prefix="bundle-", dir=self.root))
        try:
            upload = work / "upload.bin"
            size = 0
            with open(upload, "wb") as out:
                while chunk := stream.read(1024 * 1024):
                    size += len(chunk)
                    if size > MAX_BUNDLE_BYTES:
                        raise BadRequest("bundle too large")
                    out.write(chunk)
            extract = work / "x"
            extract.mkdir()
            self._safe_extract(upload, extract)
            upload.unlink()
            mf = extract / "manifest.json"
            if not mf.is_file():
                raise BadRequest("the bundle has no manifest.json at its top level")
            ver = self._verify(mf.read_bytes())  # signature BEFORE looking at anything else it claims
            self._verify_artifacts(ver.manifest, extract)
            final = self.root / f"{ver.manifest.version}-{ver.manifest.channel}"
            rec = self._accept(principal, ver, f"offline-bundle:{Path(filename or 'bundle').name[:80]}", staged_dir=str(final))
            if final.exists():
                shutil.rmtree(final)
            shutil.move(str(extract), str(final))
            self._set(rec["id"], state="staged" if rec["state"] == "available" else rec["state"], staged_dir=str(final))
            self.ctx.audit.record(principal, "admin.update_bundle_imported", resource_type="update", resource_id=rec["id"], details={"version": ver.manifest.version, "channel": ver.manifest.channel, "keys": ver.keyids})
            return self.public(self.get(rec["id"]))
        finally:
            shutil.rmtree(work, ignore_errors=True)

    @staticmethod
    def _safe_extract(archive: Path, dest: Path) -> None:
        dest_r = dest.resolve()
        total = files = 0
        tf = None
        try:
            tf = UpdateService._open_tar(archive)
            for member in tf:
                name = member.name
                parts = Path(name).parts
                if name.startswith(("/", "\\")) or ".." in parts or (parts and ":" in parts[0]):
                    raise BadRequest(f"unsafe path in bundle: {name!r}")
                if member.isdir():
                    continue
                if not member.isreg():
                    raise BadRequest(f"bundle member {name!r} is not a regular file (links and devices are not allowed)")
                files += 1
                total += member.size
                if files > MAX_BUNDLE_FILES or total > MAX_BUNDLE_BYTES:
                    raise BadRequest("bundle has too many or too large members")
                target = (dest / name).resolve()
                if dest_r not in target.parents:
                    raise BadRequest(f"unsafe path in bundle: {name!r}")
                target.parent.mkdir(parents=True, exist_ok=True)
                src = tf.extractfile(member)
                assert src is not None
                fd = os.open(target, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
                with os.fdopen(fd, "wb") as out:
                    shutil.copyfileobj(src, out, 1024 * 1024)
        except tarfile.TarError as exc:
            raise BadRequest(f"not a readable bundle archive: {exc}") from exc
        finally:
            if tf is not None:
                tf.close()

    @staticmethod
    def _verify_artifacts(m: M.Manifest, directory: Path) -> None:
        for a in m.artifacts:
            p = directory / a.name
            if not p.is_file():
                if a.url:
                    continue  # downloadable at stage time
                raise BadRequest(f"artifact {a.name} listed in the manifest is missing from the bundle")
            from olcf.ops.crypto import sha256_file

            if p.stat().st_size != a.size or sha256_file(p) != a.sha256:
                raise M.ManifestError(f"artifact {a.name} does not match the signed hash/size")

    # ------------------------------------------------------------------ stage
    def stage(self, uid: str, progress: Any = None) -> dict[str, Any]:
        rec = self.get(uid)
        ver = self._verify(rec["manifest"]["envelope"])  # always from the stored signed envelope
        m = ver.manifest
        cfg = self.get_config()
        d = Path(rec["staged_dir"]) if rec.get("staged_dir") else self.root / f"{m.version}-{m.channel}"
        d.mkdir(parents=True, exist_ok=True)
        client = None
        try:
            for i, a in enumerate(m.artifacts):
                p = d / a.name
                from olcf.ops.crypto import sha256_file

                if p.is_file() and p.stat().st_size == a.size and sha256_file(p) == a.sha256:
                    continue
                if not a.url:
                    raise BadRequest(f"artifact {a.name} is not staged and has no download URL (import the offline bundle)")
                client = client or self._client(cfg)
                url = a.url if "://" in a.url else f"{str(cfg['feed_url']).rstrip('/')}/{m.channel}/{a.url.lstrip('/')}"
                if progress:
                    progress(10 + int(40 * i / max(len(m.artifacts), 1)), f"downloading {a.name}")
                tmp = p.with_name(p.name + ".part")
                digest, n = self._get(client, url, cfg, stream_to=tmp, limit=min(a.size + 1, MAX_ARTIFACT))  # type: ignore[misc]
                if n != a.size or digest != a.sha256:
                    tmp.unlink(missing_ok=True)
                    raise M.ManifestError(f"downloaded artifact {a.name} does not match the signed hash/size")
                os.replace(tmp, p)
        finally:
            if client:
                client.close()
        (d / "manifest.json").write_text(json.dumps(rec["manifest"]["envelope"]))
        self._set(uid, staged_dir=str(d), state="staged" if rec["state"] in ("available", "staged") else rec["state"])
        return self.get(uid)

    # ------------------------------------------------------------------ preflight
    def in_window(self, now: datetime | None = None) -> tuple[bool, str]:
        from croniter import croniter

        wins = self.get_config()["maintenance_windows"]
        if not wins:
            return True, "no maintenance window configured (updates may be applied at any time)"
        now = now or datetime.now(UTC)
        for w in wins:
            start = croniter(w["cron"], now - timedelta(minutes=w["minutes"])).get_next(datetime)
            if start.tzinfo is None:
                start = start.replace(tzinfo=UTC)
            if start <= now < start + timedelta(minutes=w["minutes"]):
                return True, f"inside the maintenance window that started {start.isoformat()} ({w['minutes']} min)"
        return False, "outside every configured maintenance window"

    def preflight(self, uid: str, progress: Any = None) -> dict[str, Any]:
        rec = self.stage(uid, progress)  # re-checks the hashes of what is on disk and downloads what is missing
        ver = self._verify(rec["manifest"]["envelope"])
        m = ver.manifest
        d = Path(rec["staged_dir"])
        checks: list[dict[str, Any]] = []

        def add(name: str, status: str, detail: str, **extra: Any) -> None:
            checks.append({"name": name, "status": status, "detail": detail, **extra})

        add("signature", "ok", f"signed by {', '.join(ver.keyids)} (threshold {self.get_config()['threshold']}), channel {m.channel}, sequence {m.sequence}")
        inst = Version(installed_version())
        if m.parsed_version <= inst:
            add("version", "fail", f"target {m.version} is not newer than the installed {inst}")
        elif m.min_version and inst < Version(m.min_version):
            add("version", "fail", f"direct upgrade from {inst} is not supported; upgrade to {m.min_version} or later first")
        else:
            add("version", "ok", f"{inst} -> {m.version}")
        spec = m.compat.get("python")
        try:
            py_ok = (not spec) or Version(".".join(map(str, sys.version_info[:3]))) in SpecifierSet(spec)
        except InvalidSpecifier:
            py_ok = False
        add("python", "ok" if py_ok else "fail", f"running Python {sys.version_info.major}.{sys.version_info.minor}; release requires '{spec or 'any'}'")
        dbs = m.compat.get("databases")
        add("database", "ok" if (not dbs or self.ctx.db.dialect in dbs) else "fail", f"{self.ctx.db.dialect}; release supports {dbs or 'any'}")
        add("artifacts", "ok", f"{len(m.artifacts)} artifact(s) staged with verified SHA-256")
        wheel = m.artifact("wheel")
        # disk
        need = sum(a.size for a in m.artifacts) * 3 + self._db_size() * 2 + 128 * 1024**2
        free = shutil.disk_usage(self.ctx.settings.data_dir).free
        add("disk", "ok" if free >= need else "fail", f"{free // 1024**2} MiB free in the data directory, about {need // 1024**2} MiB needed (staging + pre-update backup + dry-run copy)")
        # workers + jobs
        self._workers_check(m, add)
        # migrations
        if wheel is None:
            add("migration-dry-run", "ok", "this release ships no core wheel (runtime/agent artifacts only)")
        else:
            try:
                add("migration-dry-run", *self._migration_dry_run(d / wheel.name, m, progress))
            except Exception as exc:  # noqa: BLE001
                add("migration-dry-run", "fail", f"{type(exc).__name__}: {getattr(exc, 'message', exc)}"[:500])
        inw, why = self.in_window()
        add("maintenance-window", "ok" if inw else "warn", why)
        prev = "ok" if self.get_config()["fetch_previous"] else "warn"
        add("rollback", prev, "the automatic pre-update backup is taken at apply time; code rollback reinstalls the previous version" if prev == "ok"
            else "fetch_previous is disabled: keep the current release wheel available for `olcf upgrade --rollback`")
        ok = not any(c["status"] == "fail" for c in checks)
        out = {"ok": ok, "checks": checks, "at": now_iso(), "target": m.version, "installed": str(inst)}
        self._set(uid, preflight=out)
        return out

    def _db_size(self) -> int:
        try:
            if self.ctx.db.is_sqlite:
                p = Path(self.ctx.db.engine.url.database or "")
                return p.stat().st_size if p.is_file() else 0
            with self.ctx.db.connect() as conn:
                return int(conn.exec_driver_sql("SELECT pg_database_size(current_database())").scalar() or 0)
        except Exception:  # noqa: BLE001
            return 0

    def _workers_check(self, m: M.Manifest, add: Any) -> None:
        compute = self.ctx.maybe_service("compute")
        if compute is None:
            add("workers", "ok", "compute module not loaded")
            return
        wmin = (m.compat.get("workers") or {}).get("min_version")
        required = bool((m.compat.get("workers") or {}).get("required"))
        workers = compute.registry.list_workers(include_revoked=False)
        old = [w["name"] for w in workers if wmin and w.get("agent_version") and Version(str(w["agent_version"])) < Version(wmin)] if wmin else []
        add("workers", ("fail" if required else "warn") if old else "ok",
            f"{len(workers)} worker(s); " + (f"{len(old)} below the agent version {wmin}: {', '.join(old[:10])} (agents upgrade separately, see the agent update procedure)" if old else "all compatible"),
            versions={w["name"]: w.get("agent_version") for w in workers})
        counts = compute.jobs.counts()
        add("active-jobs", "warn" if counts["running"] else "ok", f"{counts['running']} running and {counts['queued']} queued job(s); apply drains compute and waits for running jobs")

    def _migration_dry_run(self, wheel: Path, m: M.Manifest, progress: Any) -> tuple[str, str]:
        """Dump the live DB, restore into scratch SQLite at today's schema, then run the NEW wheel's migrations on it in a subprocess."""
        from olcf.ops.crypto import KeySpec

        tmp = self.ctx.settings.path("tmp")
        tmp.mkdir(parents=True, exist_ok=True)
        with tempfile.TemporaryDirectory(dir=tmp) as td:
            tdp = Path(td)
            rows = sum(core.table_counts(self.ctx.db).values())
            with_data = rows <= int(self.get_config()["dry_run_max_rows"])
            spec = KeySpec(None, self.ctx.secrets.master)
            if progress:
                progress(55, "migration dry-run: copying the database")
            if with_data:
                core.write_backup(self.ctx.db, tdp / "snap.olcfbak", spec, meta={"kind": "dry-run"})
                scratch = Database(f"sqlite:///{tdp / 'dry.db'}")
                core.restore_backup(tdp / "snap.olcfbak", spec, scratch, run_migrations=False)
            else:
                scratch = Database(f"sqlite:///{tdp / 'dry.db'}")
                core.migrate_streams(scratch, core.dbm.current_versions(self.ctx.db))  # noqa: SLF001 - schema only
            scratch.dispose()
            ex = tdp / "wheel"
            with zipfile.ZipFile(wheel) as zf:
                for n in zf.namelist():
                    if n.startswith("/") or ".." in Path(n).parts:
                        raise BadRequest(f"unsafe path in wheel: {n!r}")
                zf.extractall(ex)  # noqa: S202 - a zip, not a tar; every member name was checked above (no absolute or '..' path)
            if progress:
                progress(70, "migration dry-run: running the new release's migrations")
            script = ("import sys, json, time\nsys.path.insert(0, sys.argv[1])\nimport olcf\nfrom olcf.db.engine import Database\nt=time.time()\n"
                      "db = Database('sqlite:///' + sys.argv[2])\nv = db.migrate()\nprint(json.dumps({'version': olcf.__version__, 'file': olcf.__file__, 'migrations': v, 'seconds': round(time.time()-t, 2)}))\n")
            env = {k: v for k, v in os.environ.items() if k not in ("PYTHONPATH",)}
            env["OLCF_DATA_DIR"] = str(tdp / "dd")
            proc = subprocess.run([sys.executable, "-c", script, str(ex), str(tdp / "dry.db")], capture_output=True, text=True, timeout=1800, env=env, cwd=td, check=False)  # noqa: S603
            if proc.returncode != 0:
                tail = (proc.stderr or proc.stdout).strip().splitlines()[-3:]
                raise BadRequest("the new release's migrations FAILED on a copy of your data: " + " | ".join(tail)[:400])
            res = json.loads(proc.stdout.strip().splitlines()[-1])
            if not str(Path(res["file"]).resolve()).startswith(str(ex.resolve())):
                raise BadRequest("dry-run imported the installed package instead of the staged wheel; cannot vouch for the migration")
            if res["version"] != m.version:
                raise M.ManifestError(f"the wheel contains version {res['version']} but the signed manifest says {m.version}")
            cur = core.dbm.current_versions(self.ctx.db)
            lower = [s for s, v in cur.items() if res["migrations"].get(s, 0) < v]
            if lower:
                return "fail", f"the new release would downgrade the schema of {lower}"
            mismatch = {s: (v, res["migrations"].get(s)) for s, v in m.migrations.items() if res["migrations"].get(s) != v}
            if mismatch:
                return "fail", f"migration versions differ from the signed manifest: {mismatch}"
            return "ok", (f"migrations applied to a copy of {rows} rows in {res['seconds']} s ({'with your data' if with_data else 'schema only: database above dry_run_max_rows'}); "
                          f"{cur} -> {res['migrations']}; SQLite dry-run (PostgreSQL-specific DDL is covered by the release test suite)")

    # ------------------------------------------------------------------ drain / maintenance
    def maintenance_state(self) -> dict[str, Any]:
        return self._kv_get(MAINT_KEY, {"active": False})

    def drain(self, principal: Principal, timeout: int | None = None, progress: Any = None) -> dict[str, Any]:
        compute = self.ctx.maybe_service("compute")
        st = {"active": True, "since": now_iso(), "workers": {}, "queues": {}}
        if compute is None:
            self._kv_set(MAINT_KEY, st)
            return {"drained": True, "note": "compute module not loaded"}
        for w in compute.registry.list_workers(include_revoked=False):
            if w["scheduling"] == "active":
                st["workers"][w["id"]] = w["scheduling"]
                try:
                    compute.admin.set_scheduling(principal, w["id"], "draining", "platform update")
                except Exception as exc:  # noqa: BLE001
                    log.warning("could not drain worker %s: %s", w["name"], exc)
        for q in compute.registry.list_queues():
            if not q.get("paused"):
                st["queues"][q["name"]] = False
                compute.registry.update_queue(q["name"], {"paused": True}, fields_set={"paused"})
        self._kv_set(MAINT_KEY, st)
        deadline = time.time() + (timeout if timeout is not None else int(self.get_config()["drain_timeout_seconds"]))
        while True:
            running = compute.jobs.counts()["running"]
            if running == 0:
                return {"drained": True, "running": 0}
            if time.time() > deadline:
                self.undrain(principal)
                raise Conflict(f"{running} job(s) still running after the drain timeout; compute was resumed", remediation="wait for the jobs, cancel them, or retry with a longer drain_timeout_seconds")
            if progress:
                progress(60, f"waiting for {running} running job(s)")
            time.sleep(1.0)

    def undrain(self, principal: Principal | None = None) -> dict[str, Any]:
        st = self.maintenance_state()
        compute = self.ctx.maybe_service("compute")
        if compute is not None and st.get("active"):
            for wid in st.get("workers", {}):
                try:
                    compute.admin.set_scheduling(principal or SYSTEM, wid, "active", "platform update finished")
                except Exception as exc:  # noqa: BLE001
                    log.warning("could not resume worker %s: %s", wid, exc)
            for name in st.get("queues", {}):
                try:
                    compute.registry.update_queue(name, {"paused": False}, fields_set={"paused"})
                except Exception as exc:  # noqa: BLE001
                    log.warning("could not unpause queue %s: %s", name, exc)
        self._kv_set(MAINT_KEY, {"active": False, "ended": now_iso()})
        return {"resumed": True}

    # ------------------------------------------------------------------ apply
    @property
    def marker_path(self) -> Path:
        return self.root / "handoff.json"

    def start_apply(self, principal: Principal, uid: str, *, confirm: str, override_window: bool = False, execute: bool = False, wait: bool = False) -> dict[str, Any]:
        if confirm != "APPLY":
            raise BadRequest("applying an update takes the platform through a maintenance procedure; confirm by sending confirm='APPLY'")
        rec = self.get(uid)
        if rec["state"] in ("applying", "handoff"):
            raise Conflict(f"update is already {rec['state']}")
        if rec["state"] == "applied":
            raise Conflict("update already applied")
        self._verify(rec["manifest"]["envelope"])  # fail fast, synchronously, on anything untrusted
        inw, why = self.in_window()
        if not inw and not override_window:
            raise Conflict(f"refusing to apply: {why}", remediation="wait for the window or pass override_window=true (audited)")
        if not inw:
            self.ctx.audit.record(principal, "admin.update_window_override", resource_type="update", resource_id=uid, details={"reason": why})
        return run_operation(self.ctx, "update.apply", principal, lambda p: self._apply(principal, uid, execute, p), wait=wait, message=f"applying update {rec['version']}")

    def _apply(self, principal: Principal, uid: str, execute: bool, progress: Any) -> dict[str, Any]:
        rec = self.get(uid)
        ver = self._verify(rec["manifest"]["envelope"])
        m = ver.manifest
        self._set(uid, state="applying")
        backup_id = None
        drained = False
        try:
            progress(5, "preflight")
            pre = self.preflight(uid, progress)
            if not pre["ok"]:
                raise Conflict("preflight failed: " + "; ".join(f"{c['name']}: {c['detail']}" for c in pre["checks"] if c["status"] == "fail")[:500])
            backups = self.ctx.maybe_service("backup")
            if backups is None:
                raise Unavailable("the backup service is not available; refusing to update without a pre-update backup")
            progress(30, "taking the pre-update backup")
            bres = backups.start_backup(principal, kind="pre-update", wait=True)
            brec = backups.get(bres["backup_id"])
            if brec["state"] != "succeeded":
                raise Unavailable("the pre-update backup failed; refusing to update: " + str((brec.get("error") or {}).get("message", ""))[:200])
            backup_id = brec["id"]
            self._set(uid, backup_id=backup_id)
            progress(45, "draining compute")
            self.drain(principal, progress=progress)
            drained = True
            d = Path(self.get(uid)["staged_dir"])
            imported = self._import_runtime_bundles(principal, m, d)
            wheel = m.artifact("wheel")
            result: dict[str, Any] = {"backup_id": backup_id, "runtime_bundles": imported, "from": installed_version(), "to": m.version}
            if wheel is None:
                self.undrain(principal)
                drained = False
                self._set(uid, state="applied", result={**result, "handoff": False})
                self.ctx.audit.record(principal, "admin.update_applied", resource_type="update", resource_id=uid, details={"version": m.version, "core": False})
                return {**result, "handoff": False, "note": "no core wheel in this release; runtime artifacts imported"}
            progress(85, "writing the hand-off marker")
            marker = {"state": "handoff", "update_id": uid, "version": m.version, "channel": m.channel, "from_version": installed_version(), "wheel": str(d / wheel.name),
                      "wheel_sha256": wheel.sha256, "wheel_size": wheel.size, "manifest": str(d / "manifest.json"), "backup_id": backup_id,
                      "backup_file": brec["filename"], "created_at": now_iso(), "created_by": principal.id, "trusted_keys": [{"keyid": k.keyid, "public_key": __import__("base64").b64encode(k.public_raw).decode(), "revoked": k.revoked} for k in self.trusted_keys()],
                      "threshold": self.get_config()["threshold"], "restart_command": self.get_config()["restart_command"], "db_dialect": self.ctx.db.dialect, "data_dir": str(self.ctx.settings.data_dir)}
            self.root.mkdir(parents=True, exist_ok=True)
            tmp = self.marker_path.with_suffix(".tmp")
            tmp.write_text(json.dumps(marker, indent=2))
            os.replace(tmp, self.marker_path)
            self._set(uid, state="handoff", result={**result, "handoff": True})
            self.ctx.audit.record(principal, "admin.update_handoff", resource_type="update", resource_id=uid, details={"version": m.version, "backup_id": backup_id})
            self.ctx.events.publish("update.handoff", {"id": uid, "version": m.version})
            out = {**result, "handoff": True, "marker": str(self.marker_path),
                   "next": "run `olcf upgrade` (python -m olcf.ops.upgrade apply) on the control-plane host, then restart the service; compute stays drained until the new version is running"}
            if execute:
                progress(92, "running the upgrade hand-off")
                proc = subprocess.Popen([sys.executable, "-m", "olcf.ops.upgrade", "apply", "--data-dir", str(self.ctx.settings.data_dir)], start_new_session=True,  # noqa: S603
                                        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, env={**os.environ, "OLCF_DATA_DIR": str(self.ctx.settings.data_dir)})
                out["upgrade_pid"] = proc.pid
            return out
        except Exception as exc:  # noqa: BLE001
            if drained:
                try:
                    self.undrain(principal)
                except Exception:  # noqa: BLE001
                    log.exception("could not resume compute after a failed update")
            self._set(uid, state="failed", result={"error": getattr(exc, "message", str(exc))[:500], "backup_id": backup_id})
            self.ctx.audit.record(principal, "admin.update_applied", resource_type="update", resource_id=uid, outcome="failure", details={"error": getattr(exc, "message", str(exc))[:300]})
            raise

    def _import_runtime_bundles(self, principal: Principal, m: M.Manifest, d: Path) -> list[dict[str, Any]]:
        out = []
        runtimes = self.ctx.maybe_service("runtimes")
        bundles = [a for a in m.artifacts if a.kind == "runtime-bundle"]
        if bundles and runtimes is None:
            raise Unavailable("the manifest ships runtime bundles but the compute module (runtimes service) is not loaded")
        for a in bundles:
            with open(d / a.name, "rb") as fh:
                b = runtimes.save_bundle(principal, a.name, fh, a.runtime_id)
            out.append({"bundle_id": b["id"], "name": a.name, "sha256": b["sha256"]})
        return out

    # ------------------------------------------------------------------ reconcile after restart / cancel / rollback
    def reconcile(self) -> dict[str, Any]:
        """Called on start and from the leader tick: finish an update whose target version is now running."""
        if not self.marker_path.is_file():
            return {}
        try:
            marker = json.loads(self.marker_path.read_text())
        except ValueError:
            log.error("hand-off marker is corrupt")
            return {"error": "corrupt marker"}
        if marker.get("state") != "handoff":
            return {}
        cur, target = Version(installed_version()), Version(marker["version"])
        if cur < target:
            return {"pending": marker["version"]}
        uid = marker["update_id"]
        with self._lock:
            hist = self.root / "history"
            hist.mkdir(parents=True, exist_ok=True)
            os.replace(self.marker_path, hist / f"handoff-{marker['version']}-{int(time.time())}.json")
        try:
            rec = self.get(uid)
        except NotFound:
            rec = None
        state = "applied" if cur == target else "superseded"
        if rec is not None:
            self._set(uid, state=state, result={**(rec.get("result") or {}), "applied_at": now_iso(), "running_version": str(cur), "migrations": core.dbm.current_versions(self.ctx.db)})
        self.undrain(None)
        self.ctx.audit.record(None, "admin.update_applied", resource_type="update", resource_id=uid, details={"version": marker["version"], "running": str(cur), "state": state})
        self.ctx.events.publish("update.applied", {"id": uid, "version": marker["version"]})
        return {"applied": marker["version"], "state": state}

    def cancel(self, principal: Principal, uid: str) -> dict[str, Any]:
        rec = self.get(uid)
        if rec["state"] not in ("applying", "handoff", "failed", "staged", "available"):
            raise Conflict(f"cannot cancel an update that is {rec['state']}; use rollback")
        if rec["state"] == "handoff" and Version(installed_version()) >= Version(rec["version"]):
            raise Conflict("the new version is already running; use rollback")
        self.marker_path.unlink(missing_ok=True)
        self.undrain(principal)
        self._set(uid, state="rolled_back" if rec["state"] in ("handoff", "applying") else rec["state"], result={**(rec.get("result") or {}), "cancelled_at": now_iso()})
        self.ctx.audit.record(principal, "admin.update_cancelled", resource_type="update", resource_id=uid)
        return self.public(self.get(uid))

    def rollback(self, principal: Principal, uid: str, *, execute: bool = False) -> dict[str, Any]:
        rec = self.get(uid)
        if rec["state"] in ("handoff", "applying", "failed") and Version(installed_version()) < Version(rec["version"]):
            return self.cancel(principal, uid)
        if rec["state"] != "applied":
            raise Conflict(f"update is {rec['state']}; nothing to roll back")
        if not rec.get("backup_id"):
            raise Conflict("this update has no pre-update backup; manual restore required")
        brec = self.ctx.service("backup").get(rec["backup_id"])
        prev = (rec.get("result") or {}).get("from")
        marker = {"state": "rollback", "update_id": uid, "from_version": rec["version"], "to_version": prev, "backup_id": brec["id"], "backup_file": brec["filename"],
                  "created_at": now_iso(), "created_by": principal.id, "data_dir": str(self.ctx.settings.data_dir)}
        (self.root).mkdir(parents=True, exist_ok=True)
        (self.root / "rollback.json").write_text(json.dumps(marker, indent=2))
        self._set(uid, state="rollback_pending")
        self.ctx.audit.record(principal, "admin.update_rollback_requested", resource_type="update", resource_id=uid, details={"to": prev, "backup": brec["id"]})
        out = {"marker": str(self.root / "rollback.json"), "to_version": prev,
               "next": "stop the platform, run `python -m olcf.ops.upgrade rollback` (restores the pre-update backup and reinstalls the previous version), then start it again. "
                       "Changes made since the update are lost."}
        if execute:
            raise BadRequest("rollback replaces the database under a running process and is therefore never executed in-process; stop the platform and run the command above")
        return out

    # ------------------------------------------------------------------ status / tick
    def status(self) -> dict[str, Any]:
        st = self._kv_get(STATE_KEY, {})
        cfg = self.get_config()
        newest = None
        for r in self.list(20):
            if r["state"] in ("available", "staged") and Version(r["version"]) > Version(installed_version()):
                newest = newest if newest and Version(newest["version"]) >= Version(r["version"]) else r
        marker = json.loads(self.marker_path.read_text()) if self.marker_path.is_file() else None
        return {"installed": installed_version(), "channel": cfg["channel"], "feed_configured": bool(cfg["feed_url"]), "last_check": st.get("last_check"),
                "floor": st.get("floor", {}), "available": newest, "handoff": ({k: marker[k] for k in ("version", "update_id", "created_at", "backup_id")} if marker else None),
                "maintenance": self.maintenance_state(), "in_window": self.in_window()[0], "trusted_keys": len(self.trusted_keys())}

    def tick(self, now: datetime | None = None) -> dict[str, Any]:
        out: dict[str, Any] = {}
        rec = self.reconcile()
        if rec:
            out["reconcile"] = rec
        cfg = self.get_config()
        if cfg["auto_check"] and cfg["feed_url"]:
            last = (self._kv_get(STATE_KEY, {}).get("last_check") or {}).get("at")
            due = True
            if last:
                due = datetime.fromisoformat(last.replace("Z", "+00:00")) + timedelta(hours=cfg["check_interval_hours"]) <= (now or datetime.now(UTC))
            if due:
                try:
                    out["check"] = self.check(None)["latest"]
                except Exception as exc:  # noqa: BLE001
                    out["check_error"] = getattr(exc, "message", str(exc))[:200]
        return out


def install_updates(ctx: AppContext) -> UpdateService:
    svc = UpdateService(ctx)
    ctx.register_service("updates", svc)
    try:
        svc.reconcile()
    except Exception:  # noqa: BLE001
        log.exception("update reconcile failed at start")
    return svc
