# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Hand-off for core upgrades and rollbacks: ``python -m olcf.ops.upgrade apply|rollback|status`` (TOOLING's ``olcf upgrade`` calls :func:`main`).

Why a separate step: a Python service cannot safely replace its own code and database schema while it is running. The control plane
therefore *prepares* an update (signature verified, hashes checked, migrations dry-run on a copy, automatic backup, compute drained) and writes
``<data_dir>/updates/handoff.json``. This module consumes that marker:

``apply``     re-verifies the signed manifest (Ed25519, trust snapshot + file/env anchors, expiry, channel), checks that the staged wheel is exactly the
              signed artifact (size, SHA-256, wheel metadata version) and only then installs it with ``pip install`` into the current environment (or
              ``--target``). It then runs the configured ``restart_command`` (argv list, never a shell) if any. The *new* version runs the schema migrations
              when it starts and marks the update applied; compute is resumed at that point.
``rollback``  restores the pre-update backup taken by ``apply`` and reinstalls the previous version (``--previous-wheel`` or the same index). Stop the
              platform first. Everything written after the update is lost - the new release's migrations are not reversible in general.

Nothing here accepts an unsigned or hash-mismatching artifact; there is deliberately no ``--force`` / ``--skip-verify``.
"""

from __future__ import annotations

import argparse
import base64
import json
import os
import subprocess
import sys
import zipfile
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from olcf.ops import manifest as M
from olcf.ops.crypto import sha256_file


class UpgradeError(Exception):
    """The upgrade hand-off cannot proceed; the message says what is missing or wrong."""
    pass


def _updates_dir(data_dir: Path) -> Path:
    return data_dir / "updates"


def load_marker(data_dir: Path, name: str = "handoff.json") -> dict[str, Any]:
    p = _updates_dir(data_dir) / name
    if not p.is_file():
        raise UpgradeError(f"no {name} in {p.parent}: nothing prepared (use POST /v1/admin/updates/<id>/apply first)")
    try:
        return json.loads(p.read_text())
    except ValueError as exc:
        raise UpgradeError(f"{p} is corrupt") from exc


def trusted_keys(marker: dict[str, Any], data_dir: Path) -> list[M.TrustedKey]:
    keys: dict[str, M.TrustedKey] = {}
    for k in marker.get("trusted_keys", []):
        raw = base64.b64decode(k["public_key"])
        keys[k["keyid"]] = M.TrustedKey(k["keyid"], raw, "snapshot", bool(k.get("revoked")))
    anchors = data_dir / "trust" / "update-keys.json"
    extra: list[bytes] = []
    if anchors.is_file():
        try:
            extra += [base64.b64decode(k["public_key"]) for k in json.loads(anchors.read_text())]
        except (ValueError, KeyError, TypeError):
            pass
    for part in filter(None, (os.environ.get("OLCF_UPDATE_TRUST_KEYS") or "").split(",")):
        try:
            extra.append(base64.b64decode(part.strip()))
        except ValueError:
            pass
    for raw in extra:
        kid = M.keyid_of(raw)
        keys.setdefault(kid, M.TrustedKey(kid, raw, "anchor"))
    return list(keys.values())


def wheel_version(wheel: Path) -> str:
    with zipfile.ZipFile(wheel) as zf:
        meta = next((n for n in zf.namelist() if n.endswith(".dist-info/METADATA")), None)
        if meta is None:
            raise UpgradeError("the wheel has no METADATA")
        for line in zf.read(meta).decode("utf-8", "replace").splitlines():
            if line.startswith("Version:"):
                return line.split(":", 1)[1].strip()
    raise UpgradeError("the wheel METADATA has no Version")


def verify_marker(marker: dict[str, Any], data_dir: Path) -> tuple[M.Verified, Path]:
    """Everything that must hold before a staged wheel is installed."""
    mf = Path(marker["manifest"])
    if not mf.is_file():
        raise UpgradeError("the staged manifest is missing")
    try:
        ver = M.verify_envelope(mf.read_bytes(), trusted_keys(marker, data_dir), threshold=int(marker.get("threshold", 1)), expect_channel=marker.get("channel"))
    except M.ManifestError as exc:
        raise UpgradeError(f"signature check failed: {exc.message}") from exc
    m = ver.manifest
    if m.version != marker["version"]:
        raise UpgradeError(f"marker version {marker['version']} differs from the signed manifest ({m.version})")
    art = m.artifact("wheel")
    if art is None:
        raise UpgradeError("the signed manifest lists no wheel")
    wheel = Path(marker["wheel"])
    if wheel.name != art.name or wheel.parent != mf.parent:
        raise UpgradeError("the marker points at a wheel that is not the signed artifact")
    if not wheel.is_file() or wheel.stat().st_size != art.size or sha256_file(wheel) != art.sha256:
        raise UpgradeError("the staged wheel does not match the signed size/SHA-256")
    if wheel_version(wheel) != m.version:
        raise UpgradeError(f"the wheel contains version {wheel_version(wheel)}, the signed manifest says {m.version}")
    return ver, wheel


def _pip(args: list[str], log: Path) -> int:
    cmd = [sys.executable, "-m", "pip", "install", "--no-input", "--disable-pip-version-check", *args]
    with open(log, "ab") as fh:
        fh.write((f"\n$ {' '.join(cmd)}\n").encode())
        return subprocess.run(cmd, stdout=fh, stderr=subprocess.STDOUT, check=False, timeout=3600).returncode  # noqa: S603


def apply(data_dir: Path, *, target: str | None = None, extra_pip: list[str] | None = None, run_restart: bool = True) -> dict[str, Any]:
    marker = load_marker(data_dir)
    if marker.get("state") != "handoff":
        raise UpgradeError(f"marker state is '{marker.get('state')}', expected 'handoff'")
    _, wheel = verify_marker(marker, data_dir)
    log = _updates_dir(data_dir) / "upgrade.log"
    args = [*(["--target", target] if target else []), *(extra_pip or []), str(wheel)]
    rc = _pip(args, log)
    result: dict[str, Any] = {"ok": rc == 0, "pip_returncode": rc, "version": marker["version"], "at": datetime.now(UTC).isoformat(), "log": str(log)}
    if rc == 0:
        marker["installed_at"] = result["at"]
        (_updates_dir(data_dir) / "handoff.json").write_text(json.dumps(marker, indent=2))
        cmd = marker.get("restart_command")
        if run_restart and cmd:
            if not isinstance(cmd, list) or not all(isinstance(c, str) for c in cmd):
                raise UpgradeError("restart_command must be an argv list")
            result["restart_returncode"] = subprocess.run(cmd, check=False, timeout=600).returncode  # noqa: S603
        else:
            result["next"] = "restart the OLCF service; the new version applies the schema migrations on start and resumes compute"
    (_updates_dir(data_dir) / "handoff-result.json").write_text(json.dumps(result, indent=2))
    return result


def rollback(data_dir: Path, *, db_url: str | None = None, previous_wheel: Path | None = None, target: str | None = None, skip_code: bool = False,
             extra_pip: list[str] | None = None) -> dict[str, Any]:
    """Restore the pre-update backup (fresh schema) and reinstall the previous version. The platform must be stopped."""
    marker = load_marker(data_dir, "rollback.json")
    if marker.get("state") != "rollback":
        raise UpgradeError("rollback marker has an unexpected state")
    backup = (data_dir / "backups" / Path(marker["backup_file"]).name).resolve()
    if (data_dir / "backups").resolve() not in backup.parents or not backup.is_file():
        raise UpgradeError(f"the pre-update backup {marker['backup_file']} is not in {data_dir / 'backups'} (copy it back from your backup target)")
    log = _updates_dir(data_dir) / "upgrade.log"
    result: dict[str, Any] = {"ok": False, "backup": backup.name, "to_version": marker.get("to_version")}
    if not skip_code:
        spec = [str(previous_wheel)] if previous_wheel else [f"olcf=={marker['to_version']}"] if marker.get("to_version") else []
        if not spec:
            raise UpgradeError("the previous version is unknown; pass --previous-wheel")
        rc = _pip([*(["--target", target] if target else []), *(extra_pip or []), *spec], log)
        result["pip_returncode"] = rc
        if rc != 0:
            raise UpgradeError(f"could not reinstall the previous version (see {log}); the database was NOT touched")
    env = {**os.environ, "OLCF_DATA_DIR": str(data_dir)}
    if target:
        env["PYTHONPATH"] = target + os.pathsep + env.get("PYTHONPATH", "")
    cmd = [sys.executable, "-m", "olcf.ops.backup", "restore", str(backup), "--fresh-schema", "--data-dir", str(data_dir)]
    if db_url:
        cmd += ["--db-url", db_url]
    else:
        from olcf.core.config import load_settings

        cmd += ["--db-url", load_settings(data_dir).database_url]
    proc = subprocess.run(cmd, capture_output=True, text=True, env=env, timeout=7200, check=False)  # noqa: S603
    result["restore_returncode"] = proc.returncode
    if proc.returncode != 0:
        raise UpgradeError("restoring the pre-update backup failed: " + (proc.stderr or proc.stdout).strip()[-400:])
    (_updates_dir(data_dir) / "rollback.json").rename(_updates_dir(data_dir) / f"rollback-done-{int(datetime.now(UTC).timestamp())}.json")
    result["ok"] = True
    result["next"] = "start the OLCF service"
    return result


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="olcf upgrade", description="Install a prepared (signed, verified) update, or roll one back")
    ap.add_argument("action", choices=["apply", "rollback", "status"])
    ap.add_argument("--data-dir", default=os.environ.get("OLCF_DATA_DIR") or str(Path.home() / ".olcf"))
    ap.add_argument("--target", help="pip --target directory (containers / test installs)")
    ap.add_argument("--no-restart", action="store_true", help="do not run the configured restart command")
    ap.add_argument("--previous-wheel", help="wheel of the previous release (rollback)")
    ap.add_argument("--skip-code", action="store_true", help="rollback: only restore the database")
    ap.add_argument("--db-url", help="rollback: database URL (default from the data directory settings)")
    ap.add_argument("--pip-arg", action="append", default=[], help="extra argument for pip install (e.g. --no-index, --find-links=DIR)")
    ns = ap.parse_args(argv)
    data_dir = Path(ns.data_dir).expanduser().resolve()
    try:
        if ns.action == "status":
            out: dict[str, Any] = {}
            for name in ("handoff.json", "rollback.json", "handoff-result.json"):
                p = _updates_dir(data_dir) / name
                if p.is_file():
                    d = json.loads(p.read_text())
                    out[name] = {k: d.get(k) for k in ("state", "version", "from_version", "to_version", "created_at", "installed_at", "ok")}
            print(json.dumps(out, indent=2))
            return 0
        if ns.action == "apply":
            res = apply(data_dir, target=ns.target, extra_pip=ns.pip_arg, run_restart=not ns.no_restart)
        else:
            res = rollback(data_dir, db_url=ns.db_url, previous_wheel=Path(ns.previous_wheel) if ns.previous_wheel else None, target=ns.target, skip_code=ns.skip_code, extra_pip=ns.pip_arg)
        print(json.dumps(res, indent=2))
        return 0 if res.get("ok") else 1
    except UpgradeError as exc:
        print(f"REFUSED: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
