# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""The Runtime Manager: installs, verifies, repairs, retires and rolls back side-by-side Spark runtimes.

Layout (CONTRACTS section 4.5)::

    <state>/runtimes/
        state.json                      # active runtime per family + history (for rollback)
        .lock                           # cross-process lock
        .stage/  .trash/                # transactional work areas
        _jdk/<major>/                   # JDKs downloaded with --with-jdk (checksum verified)
        <runtime-id>/
            manifest.json               # the signed manifest that was installed
            lock.json                   # exact versions + jar hashes + fingerprint inputs
            VERIFIED                    # written only after the smoke test passed
            venv/                       # python venv: pinned wheels, pyspark, olcf (--no-deps)
            jars/                       # resolved Maven jars (also linked into pyspark/jars)

Install is **transactional**: everything is built in ``.stage/<id>-<rand>`` and only moved into place with an atomic
``rename`` after verification succeeded; a failed install leaves no trace and an existing install untouched.
Nothing "latest" is ever downloaded: pip artifacts are hash pinned (``--require-hashes``), Maven artifacts are
checked against their published checksum and recorded in ``lock.json`` and re-verified on every runtime start.
"""

from __future__ import annotations

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
import uuid
from collections.abc import Callable
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from olcf.core.errors import Unsupported
from olcf.core.hostruntime import HOST_RUNTIME_ID
from olcf.core.timeutil import now_iso
from olcf.runtime import hostenv
from olcf.runtime.errors import (
    BundleInvalid,
    InstallFailed,
    ManifestInvalid,
    PrerequisiteMissing,
    RuntimeNotFound,
    VerifyFailed,
)
from olcf.runtime.locks import FileLock, build_olcf_wheel
from olcf.runtime.manifest import (
    LOCK_SCHEMA_ID,
    RuntimeManifest,
    canonical_json,
    normalise_arch,
    normalise_os,
    sha256_hex,
)
from olcf.runtime.maven import MavenResolver, jar_artifact_index, sha256_file, verify_locked_jars
from olcf.runtime.signing import TrustStore, dev_key_advice, is_dev_key_refusal, verify_manifest
from olcf.version import __version__

log = logging.getLogger("olcf.runtime.manager")

ProgressFn = Callable[[str, str], None]  # (step, detail)
BUNDLED_MANIFESTS = Path(__file__).with_name("manifests")
BUNDLE_SCHEMA = "olcf.runtime.bundle/1"


@dataclass
class ManagerConfig:
    """Everything configurable about a node's runtime manager."""

    state_dir: Path
    trust: TrustStore | None = None
    #: Maven repositories (URLs or dicts, see :class:`olcf.runtime.maven.Repository`); default Maven Central / $OLCF_MAVEN_REPOSITORIES.
    maven_repositories: list[Any] | None = None
    maven_cache: Path | None = None
    maven_offline: bool = False
    pip_index_url: str | None = None
    pip_extra_index_urls: list[str] = field(default_factory=list)
    pip_find_links: list[str] = field(default_factory=list)
    pip_no_index: bool = False
    pip_extra_args: list[str] = field(default_factory=list)
    #: Administrators can forbid the public Internet (spec 21): then an index/find-links/bundle must be configured.
    deny_public_install: bool = False
    allow_uv_python_install: bool = False
    with_jdk: bool = False
    adoptium_api: str = hostenv.ADOPTIUM_API
    python_candidates: list[str] = field(default_factory=list)
    extra_manifest_dirs: list[Path] = field(default_factory=list)
    smoke_timeout: float = 900.0
    pip_timeout: float = 3600.0
    #: Prebuilt olcf wheel to install into runtimes (default: build one from the running package).
    olcf_wheel: Path | None = None
    link_jars: str = "symlink"  # symlink | copy
    #: Command prefix for the smoke-test subprocess (e.g. ``["flock", "/tmp/olcf-spark.lock"]`` on shared CI machines).
    smoke_command_prefix: list[str] = field(default_factory=list)
    #: Offer the built-in ``olcf-host-python`` pseudo-runtime (the worker's own interpreter, no Spark) next to the installed Spark runtimes.
    host_python: bool = True

    @property
    def runtimes_dir(self) -> Path:
        return Path(self.state_dir) / "runtimes"


@dataclass
class RuntimeHandle:
    """Resolved view of an installed runtime, as used by the agent to launch processes."""

    id: str
    root: Path
    manifest: dict[str, Any]
    lock: dict[str, Any]

    @property
    def python(self) -> Path:
        return hostenv.venv_python(self.root / "venv")

    @property
    def has_spark(self) -> bool:
        """False for pure-Python runtimes (no pyspark in the package set)."""
        return bool((self.lock.get("spark") or {}).get("pyspark_dir"))

    @property
    def spark_home(self) -> Path:
        """``SPARK_HOME`` (the pip-installed ``pyspark`` directory). Only valid when :attr:`has_spark`."""
        d = (self.lock.get("spark") or {}).get("pyspark_dir")
        if not d:
            raise RuntimeNotFound(f"runtime {self.id} contains no Spark (pyspark is not part of its package set)",
                                  remediation="Use a runtime of the fabric-*/olcf-native family for Spark workloads.")
        return self.root / d

    @property
    def jars_dir(self) -> Path:
        return self.root / "jars"

    @property
    def fingerprint(self) -> str:
        return fingerprint_of(self.manifest, self.lock)

    @property
    def family(self) -> str:
        return str(self.manifest["family"])

    def java_home(self, extra_homes: list[Path] | None = None) -> Path:
        """Locked JAVA_HOME if it still works, else the best currently installed supported JDK."""
        m = RuntimeManifest.from_dict(self.manifest, validate=False)
        locked = self.lock.get("java", {}).get("home")
        if locked and Path(locked).is_dir():
            exe = Path(locked) / "bin" / ("java.exe" if os.name == "nt" else "java")
            info = hostenv.probe_java(exe) if exe.exists() else None
            if info and info.major in m.java_supported:
                return Path(info.home)
        return Path(hostenv.find_java(m.java_supported, m.java_major, extra_homes=extra_homes).home)

    def env(self, extra_java_homes: list[Path] | None = None) -> dict[str, str]:
        """Environment variables every Spark-related process of this runtime needs."""
        jh = self.java_home(extra_java_homes)
        venv_bin = str(self.python.parent)
        path = os.pathsep.join([str(jh / "bin"), venv_bin, os.environ.get("PATH", "")])
        env = {
            "JAVA_HOME": str(jh), "PYSPARK_PYTHON": str(self.python),
            "PYSPARK_DRIVER_PYTHON": str(self.python), "OLCF_RUNTIME_DIR": str(self.root), "OLCF_RUNTIME_ID": self.id,
            "PATH": path, "PYTHONNOUSERSITE": "1", "PYTHONDONTWRITEBYTECODE": "1",
        }
        if self.has_spark:
            env["SPARK_HOME"] = str(self.spark_home)
        return env

    def module_argv(self, module: str, *args: str) -> list[str]:
        """Command line that runs ``python -m <module>`` in this runtime's isolated interpreter (``-I``: no user site, no ``PYTHONPATH``, no cwd on the path)."""
        return [str(self.python), "-I", "-m", module, *args]

    def summary(self, state: str = "ready", verified: bool = True) -> dict[str, Any]:
        return {"id": self.id, "state": state, "verified": verified, "family": self.family, "version": self.manifest.get("version"),
                "fingerprint": self.fingerprint, "spark": self.lock.get("spark", {}).get("version")}


def fingerprint_of(manifest: dict[str, Any], lock: dict[str, Any]) -> str:
    """``sha256(manifest + lock)`` over canonical JSON, excluding volatile members (signature, timestamps, olcf wheel)."""
    m = {k: v for k, v in manifest.items() if k != "signature"}
    lk = {k: v for k, v in lock.items() if k not in ("meta", "olcf")}
    return "sha256:" + sha256_hex(canonical_json(m) + b"\n" + canonical_json(lk))


def bundled_manifest_paths() -> list[Path]:
    return sorted(BUNDLED_MANIFESTS.glob("*.json"))


# ======================================================================================================= manager
class RuntimeManager:
    """See module docstring. All mutating methods are serialised by an inter-process lock."""

    def __init__(self, config: ManagerConfig) -> None:
        self.cfg = config
        self.dir = config.runtimes_dir
        self.dir.mkdir(parents=True, exist_ok=True)
        self._lock_path = self.dir / ".lock"
        self._busy: dict[str, str] = {}  # runtime id -> operation in progress (for heartbeat "installing")
        self._busy_lock = threading.Lock()
        self.skipped_dev_signed: list[str] = []  # ids of bundled manifests the last available_manifests() refused because they carry the development key
        self.trust = config.trust if config.trust is not None else TrustStore.default()

    # ------------------------------------------------------------------ paths & state
    def path(self, runtime_id: str) -> Path:
        if not runtime_id or "/" in runtime_id or "\\" in runtime_id or runtime_id.startswith("."):
            raise RuntimeNotFound(f"invalid runtime id {runtime_id!r}")
        return self.dir / runtime_id

    def _state_file(self) -> Path:
        return self.dir / "state.json"

    def _load_state(self) -> dict[str, Any]:
        try:
            st = json.loads(self._state_file().read_text())
            st.setdefault("active", {}), st.setdefault("history", {}), st.setdefault("retired", {})
            return st
        except (OSError, ValueError):
            return {"schema": 1, "active": {}, "history": {}, "retired": {}}

    def _save_state(self, st: dict[str, Any]) -> None:
        tmp = self._state_file().with_suffix(".tmp")
        tmp.write_text(json.dumps(st, indent=2, sort_keys=True))
        os.replace(tmp, self._state_file())

    def _set_busy(self, runtime_id: str, op: str | None) -> None:
        with self._busy_lock:
            if op:
                self._busy[runtime_id] = op
            else:
                self._busy.pop(runtime_id, None)

    # ------------------------------------------------------------------ manifests
    def manifest_paths(self) -> list[Path]:
        out = list(bundled_manifest_paths())
        for d in self.cfg.extra_manifest_dirs:
            out.extend(sorted(Path(d).glob("*.json")))
        return out

    def available_manifests(self) -> list[RuntimeManifest]:
        """Manifests that ship with OLCF or sit in configured extra directories (valid + trusted only)."""
        out: list[RuntimeManifest] = []
        dev_signed: list[str] = []
        for p in self.manifest_paths():
            try:
                m = RuntimeManifest.load(p)
                verify_manifest(m, self.trust)
                out.append(m)
            except Exception as exc:  # noqa: BLE001 - a bad manifest must not hide the good ones
                if is_dev_key_refusal(exc):
                    dev_signed.append(p.stem)
                    continue
                log.warning("ignoring manifest %s: %s", p, exc)
        if dev_signed:  # one warning with the way out, not one bare line per manifest
            log.warning("skipped %d bundled runtime manifest(s) (%s): they are signed with the OLCF development key, which this process does not trust. %s",
                        len(dev_signed), ", ".join(dev_signed), dev_key_advice())
        self.skipped_dev_signed = dev_signed
        return out

    def load_manifest(self, ref: str | Path | dict[str, Any] | RuntimeManifest) -> RuntimeManifest:
        """Resolve ``ref`` (id, file path, dict, manifest) to a *validated and signature-verified* manifest."""
        if isinstance(ref, RuntimeManifest):
            m = ref
        elif isinstance(ref, dict):
            m = RuntimeManifest.from_dict(ref)
        elif isinstance(ref, Path) or (isinstance(ref, str) and (os.sep in ref or ref.endswith(".json"))):
            m = RuntimeManifest.load(ref)
        else:
            if isinstance(ref, str):
                self._refuse_host(ref, "install")
            avail = self.available_manifests()
            found = [x for x in avail if x.id == ref]
            if not found:
                ids = ", ".join(sorted(x.id for x in avail)) or "none ship with this build"
                raise RuntimeNotFound(f"no runtime named {ref!r}", details={"valid_ids": sorted(x.id for x in avail)},
                                      remediation=f"valid runtime ids: {ids}. List them with `olcf runtime list`.")
            m = found[0]
        verify_manifest(m, self.trust)
        return m

    # ------------------------------------------------------------------ introspection
    def _is_host(self, runtime_id: str) -> bool:
        return runtime_id == HOST_RUNTIME_ID

    def _refuse_host(self, runtime_id: str, action: str) -> None:
        """The host-Python runtime is the worker's own interpreter: there is nothing to install, repair, retire or export."""
        if self._is_host(runtime_id):
            raise Unsupported(f"cannot {action} '{HOST_RUNTIME_ID}': it is the worker's own Python interpreter and is built into every worker",
                              remediation="to run Spark work install a Spark runtime, e.g. `olcf runtime install olcf-rt-2.0.0`")

    def host_handle(self) -> RuntimeHandle:
        """Handle of the built-in host-Python runtime; :class:`RuntimeNotFound` when this node does not offer it or its interpreter lacks required modules."""
        from olcf.runtime import hostpython

        if not self.cfg.host_python:
            raise RuntimeNotFound(f"runtime {HOST_RUNTIME_ID} is disabled on this node (host_python = false)", remediation="Enable `host_python` in agent.toml or install a Spark runtime.")
        info = hostpython.describe()
        if info["missing"]:
            raise VerifyFailed(f"the host Python of this node lacks {', '.join(info['missing'])}", remediation=f"pip install {' '.join(info['missing'])} into the worker's environment")
        return hostpython.make_handle(self.dir)

    def handle(self, runtime_id: str, *, require_verified: bool = True) -> RuntimeHandle:
        if self._is_host(runtime_id):
            return self.host_handle()
        root = self.path(runtime_id)
        try:
            manifest = json.loads((root / "manifest.json").read_text())
            lock = json.loads((root / "lock.json").read_text())
        except (OSError, ValueError) as exc:
            raise RuntimeNotFound(f"runtime {runtime_id} is not installed on this node", remediation="Install it with the install_runtime command or `python -m olcf.runtime install`.") from exc
        if require_verified and not (root / "VERIFIED").is_file():
            raise VerifyFailed(f"runtime {runtime_id} is installed but not verified", remediation="Run verify or repair for this runtime.")
        return RuntimeHandle(runtime_id, root, manifest, lock)

    def list(self) -> list[dict[str, Any]]:
        """State of every runtime known on this node (heartbeat ``runtimes`` field)."""
        st = self._load_state()
        out: list[dict[str, Any]] = []
        seen: set[str] = set()
        with self._busy_lock:
            busy = dict(self._busy)
        for child in sorted(self.dir.iterdir()) if self.dir.is_dir() else []:
            if child.name.startswith(".") or child.name.startswith("_") or not child.is_dir():
                continue
            rid = child.name
            seen.add(rid)
            try:
                h = self.handle(rid, require_verified=False)
            except RuntimeNotFound:
                out.append({"id": rid, "state": "installing" if rid in busy else "failed", "verified": False})
                continue
            verified = (child / "VERIFIED").is_file()
            retired = rid in st["retired"]
            state = "retired" if retired else ("installing" if rid in busy else ("ready" if verified else "failed"))
            item = h.summary(state, verified)
            item["active"] = st["active"].get(h.family) == rid
            out.append(item)
        for rid, op in busy.items():
            if rid not in seen:
                out.append({"id": rid, "state": "installing", "verified": False, "operation": op})
        if self.cfg.host_python:
            from olcf.runtime import hostpython

            out.append(hostpython.summary())
        return out

    def ready_ids(self) -> list[str]:
        return [r["id"] for r in self.list() if r["state"] == "ready"]

    def active_for_family(self, family: str) -> str | None:
        return self._load_state()["active"].get(family)

    # ------------------------------------------------------------------ verification
    def verify(self, runtime_id: str, *, deep: bool = False, progress: ProgressFn | None = None) -> dict[str, Any]:
        """Check an installed runtime. Light: markers, manifest/lock consistency, jar hashes, venv python runs.
        Deep: additionally re-run the smoke test. Raises :class:`VerifyFailed` on problems (and removes ``VERIFIED``)."""
        if self._is_host(runtime_id):
            h = self.host_handle()  # raises with the missing modules when the interpreter cannot run notebooks
            return {"runtime_id": runtime_id, "ok": True, "steps": [{"name": "interpreter", "ok": True, "detail": h.summary()["detail"], "ms": 0}], "fingerprint": h.fingerprint}
        steps: list[dict[str, Any]] = []
        root = self.path(runtime_id)

        def step(name: str, ok: bool, detail: str = "", t0: float | None = None) -> None:
            steps.append({"name": name, "ok": ok, "detail": detail, "ms": round((time.monotonic() - t0) * 1000, 1) if t0 else 0})
            if progress:
                progress(name, detail)

        h = self.handle(runtime_id, require_verified=False)
        problems: list[str] = []
        t0 = time.monotonic()
        if not (root / "VERIFIED").is_file():
            problems.append("VERIFIED marker is missing")
        m = RuntimeManifest.from_dict(h.manifest, validate=False)
        if m.manifest_sha256 != h.lock.get("manifest_sha256"):
            problems.append("manifest.json does not match lock.json (manifest was modified after install)")
        try:
            verify_manifest(m, self.trust)
        except Exception as exc:  # noqa: BLE001
            problems.append(f"signature: {exc}")
        step("manifest", not problems, "; ".join(problems) or f"signature ok, fingerprint {h.fingerprint[:19]}...", t0)
        t0 = time.monotonic()
        jar_problems = verify_locked_jars(root / "jars", h.lock.get("jars", []))
        linked = [e for e in h.lock.get("jars", []) if e.get("linked")]
        jar_problems += verify_locked_jars(h.spark_home / "jars", linked) if linked else []
        step("jars", not jar_problems, "; ".join(jar_problems[:5]) or f"{len(h.lock.get('jars', []))} jar(s) match their locked sha256", t0)
        problems += jar_problems
        t0 = time.monotonic()
        probe = "import sys; print(sys.version.split()[0])" if m.verify_mode != "spark" else "import pyspark; print(pyspark.__version__)"
        try:
            res = subprocess.run([str(h.python), "-I", "-c", probe], capture_output=True, text=True, timeout=120, check=False)  # noqa: S603
            detail = res.stdout.strip() or res.stderr.strip()[-300:]
            py_ok = res.returncode == 0 and (m.verify_mode != "spark" or res.stdout.strip() == m.spark_version)
        except (OSError, subprocess.SubprocessError) as exc:
            py_ok, detail = False, str(exc)
        step("python", py_ok, detail, t0)
        if not py_ok:
            problems.append(f"python/pyspark check failed: {detail}")
        if deep and not problems:
            try:
                smoke = self._smoke(h.root, h, m)
                step("smoke", True, f"{len(smoke['steps'])} smoke steps passed", None)
                steps.extend({**s, "name": f"smoke:{s['name']}"} for s in smoke["steps"])
            except VerifyFailed as exc:
                problems.append(str(exc))
                step("smoke", False, str(exc), None)
        if problems:
            (root / "VERIFIED").unlink(missing_ok=True)
            raise VerifyFailed(f"runtime {runtime_id} failed verification: {'; '.join(problems[:3])}", details={"steps": steps},
                               remediation="Run `repair` for this runtime (re-resolves and re-verifies pinned artifacts transactionally).")
        return {"runtime_id": runtime_id, "ok": True, "steps": steps, "fingerprint": h.fingerprint}

    def light_check(self, runtime_id: str) -> RuntimeHandle:
        """Fast gate used before *every* start of a runtime (job, daemon, kernel): marker + jar hashes only."""
        if self._is_host(runtime_id):
            return self.host_handle()
        h = self.handle(runtime_id)
        if str(h.root.name) in self._load_state()["retired"]:
            raise VerifyFailed(f"runtime {runtime_id} is retired", remediation="Install or roll back to a non-retired runtime.")
        problems = verify_locked_jars(h.root / "jars", h.lock.get("jars", []))
        linked = [e for e in h.lock.get("jars", []) if e.get("linked")]
        if linked:
            problems += verify_locked_jars(h.spark_home / "jars", linked)
        if problems:
            (h.root / "VERIFIED").unlink(missing_ok=True)
            raise VerifyFailed(f"runtime {runtime_id} jar verification failed: {'; '.join(problems[:3])}",
                               remediation="Jars were modified or are missing: run repair for this runtime.")
        return h

    # ------------------------------------------------------------------ install / repair
    def install(self, manifest_ref: str | Path | dict[str, Any] | RuntimeManifest, *, force: bool = False, activate: bool = True,
                bundle_dir: Path | None = None, jar_groups: list[str] | None = None, progress: ProgressFn | None = None,
                _mode: str = "install") -> dict[str, Any]:
        """Install a runtime (no-op if already installed and verified unless ``force``). Returns a result dict."""
        m = self.load_manifest(manifest_ref)
        self._refuse_host(m.id, "install")
        m.require_platform()
        rid = m.id
        progress = progress or (lambda step, detail: None)
        with self._exclusive(rid, _mode):
            final = self.path(rid)
            st0 = self._load_state()
            if final.exists() and (final / "VERIFIED").is_file() and not force and rid not in st0["retired"]:
                try:
                    self.light_check(rid)
                    progress("already-installed", rid)
                    if activate:
                        self._activate(rid, m.family)
                    return {"runtime_id": rid, "state": "ready", "changed": False, "fingerprint": self.handle(rid).fingerprint}
                except VerifyFailed:
                    log.warning("runtime %s present but failing verification; reinstalling", rid)
            previous_lock: dict[str, Any] | None = None
            if final.exists():
                try:
                    previous_lock = json.loads((final / "lock.json").read_text())
                except (OSError, ValueError):
                    previous_lock = None
            stage = self.dir / ".stage" / f"{rid}-{uuid.uuid4().hex[:8]}"
            stage.mkdir(parents=True)
            try:
                lock = self._build(m, stage, bundle_dir=bundle_dir, previous_lock=previous_lock, jar_groups=jar_groups, progress=progress)
                (stage / "manifest.json").write_text(json.dumps(m.to_dict(), indent=2, sort_keys=True))
                (stage / "lock.json").write_text(json.dumps(lock, indent=2, sort_keys=True))
                h = RuntimeHandle(rid, stage, m.to_dict(), lock)
                progress("verify", f"smoke test ({m.verify_mode})")
                smoke = self._smoke(stage, h, m)
                shutil.rmtree(stage / "_wheel", ignore_errors=True)
                _relocate_venv(stage, final)
                (stage / "VERIFIED").write_text(json.dumps({"verified_at": now_iso(), "fingerprint": fingerprint_of(m.to_dict(), lock), "smoke": smoke}, indent=2))
                progress("commit", "atomic rename into place")
                self._swap(stage, final)
            except BaseException:
                shutil.rmtree(stage, ignore_errors=True)
                raise
            st = self._load_state()
            st["retired"].pop(rid, None)  # an explicit (re)install un-retires the runtime (the new files were just verified)
            self._save_state(st)
            try:
                self.light_check(rid)
            except VerifyFailed as exc:
                raise InstallFailed(f"runtime {rid} failed post-install verification: {exc.message}") from exc
            if activate:
                self._activate(rid, m.family)
            progress("done", rid)
            return {"runtime_id": rid, "state": "ready", "changed": True, "fingerprint": self.handle(rid).fingerprint, "smoke": smoke}

    def repair(self, runtime_id: str, *, progress: ProgressFn | None = None, bundle_dir: Path | None = None) -> dict[str, Any]:
        """Verify; if healthy do nothing, otherwise reinstall transactionally from the *installed* manifest and its lock.

        The jar hashes recorded in ``lock.json`` are enforced during re-resolution, so a repair can never silently
        pick up different bytes than the original install."""
        self._refuse_host(runtime_id, "repair")
        try:
            self.verify(runtime_id)
            return {"runtime_id": runtime_id, "state": "ready", "changed": False, "detail": "already healthy"}
        except VerifyFailed:
            pass
        root = self.path(runtime_id)
        try:
            manifest = json.loads((root / "manifest.json").read_text())
        except (OSError, ValueError) as exc:
            raise RuntimeNotFound(f"cannot repair {runtime_id}: manifest.json is unreadable",
                                  remediation="Reinstall it from its manifest.") from exc
        res = self.install(manifest, force=True, bundle_dir=bundle_dir, progress=progress, _mode="repair")
        res["repaired"] = True
        return res

    def sync_olcf_package(self, runtime_id: str) -> dict[str, Any]:
        """Reinstall the ``olcf`` slice inside a runtime if it differs from the running agent's package (after an agent upgrade).

        The olcf wheel is excluded from the runtime fingerprint, so this never changes the fingerprint of a verified runtime."""
        with self._exclusive(runtime_id, "sync-olcf"):
            h = self.handle(runtime_id, require_verified=False)
            with tempfile.TemporaryDirectory(prefix="olcf-wheel-") as td:
                wheel = self.cfg.olcf_wheel or build_olcf_wheel(td)
                digest = sha256_file(wheel)
                if h.lock.get("olcf", {}).get("wheel_sha256") == digest:
                    return {"runtime_id": runtime_id, "changed": False}
                self._pip([str(h.python), "-m", "pip", "install", "--no-deps", "--no-index", "--force-reinstall", str(wheel)])
            lock = dict(h.lock)
            lock["olcf"] = {"version": __version__, "wheel_sha256": digest}
            tmp = h.root / "lock.json.tmp"
            tmp.write_text(json.dumps(lock, indent=2, sort_keys=True))
            os.replace(tmp, h.root / "lock.json")
            return {"runtime_id": runtime_id, "changed": True, "wheel_sha256": digest}

    # ------------------------------------------------------------------ retire / rollback
    def retire(self, runtime_id: str, *, purge: bool = False) -> dict[str, Any]:
        """Take a runtime out of service (the scheduler stops placing work on it). ``purge`` also deletes the files."""
        self._refuse_host(runtime_id, "retire")
        with self._exclusive(runtime_id, "retire"):
            root = self.path(runtime_id)
            if not root.exists():
                raise RuntimeNotFound(f"runtime {runtime_id} is not installed")
            st = self._load_state()
            st["retired"][runtime_id] = now_iso()
            fam = _family_of(root)
            if fam and st["active"].get(fam) == runtime_id:
                st["active"].pop(fam)
                for cand in reversed(st["history"].get(fam, [])):
                    if cand != runtime_id and cand not in st["retired"] and (self.path(cand) / "VERIFIED").is_file():
                        st["active"][fam] = cand
                        break
            self._save_state(st)
            if purge:
                trash = self.dir / ".trash" / f"{runtime_id}-{int(time.time())}"
                trash.parent.mkdir(exist_ok=True)
                os.replace(root, trash)
                shutil.rmtree(trash, ignore_errors=True)
                st = self._load_state()
                for ids in st["history"].values():
                    if runtime_id in ids:
                        ids.remove(runtime_id)
                self._save_state(st)
            return {"runtime_id": runtime_id, "state": "retired", "purged": purge, "active": self._load_state()["active"]}

    def rollback(self, family_or_id: str) -> dict[str, Any]:
        """Make the previously active runtime of a family active again (new versions install side by side, so the
        previous bundle is still on disk). ``family_or_id`` may be a family name or the runtime id being rolled back."""
        self._refuse_host(family_or_id, "roll back")
        with self._exclusive(family_or_id, "rollback"):
            st = self._load_state()
            fam = family_or_id if family_or_id in st["history"] else None
            if fam is None:
                fam = _family_of(self.path(family_or_id)) if self.path(family_or_id).exists() else None
            if not fam or fam not in st["history"]:
                raise RuntimeNotFound(f"no install history for {family_or_id!r}")
            current = st["active"].get(fam)
            hist = [h for h in st["history"][fam]]
            candidates = [h for h in reversed(hist) if h != current and h not in st["retired"] and (self.path(h) / "VERIFIED").is_file()]
            # prefer the entry installed immediately before the current one
            if current in hist:
                before = [h for h in reversed(hist[: hist.index(current)]) if h not in st["retired"] and (self.path(h) / "VERIFIED").is_file()]
                candidates = before or candidates
            if not candidates:
                raise RuntimeNotFound(f"no earlier verified runtime to roll back to in family {fam}",
                                      remediation="Install an older manifest side by side first.")
            target = candidates[0]
            st["active"][fam] = target
            self._save_state(st)
            return {"family": fam, "previous": current, "active": target}

    # ------------------------------------------------------------------ offline bundles
    def export_bundle(self, runtime_id: str, out_path: str | Path, *, progress: ProgressFn | None = None) -> dict[str, Any]:
        """Write ``out_path`` (tar.gz): manifest + signature + hash-pinned wheels/sdists for *this platform* + olcf wheel +
        a Maven-layout repository holding every jar (and POM) of the lock. Needs network (or a warm cache/mirror)."""
        progress = progress or (lambda s, d: None)
        self._refuse_host(runtime_id, "export")
        h = self.handle(runtime_id)
        m = RuntimeManifest.from_dict(h.manifest, validate=False)
        verify_manifest(m, self.trust)
        work = Path(tempfile.mkdtemp(prefix="olcf-bundle-", dir=self.dir / ".stage" if (self.dir / ".stage").exists() else None))
        try:
            files_root = work / "b"
            (files_root / "wheels").mkdir(parents=True)
            (files_root / "build-wheels").mkdir()
            progress("pip-download", "downloading hash-pinned packages")
            for pins, sub in ((m.build_packages, "build-wheels"), (m.packages, "wheels")):
                if not pins:
                    continue
                req = files_root / f"{sub}.requirements.txt"
                req.write_text(m.requirements_text(build=(sub == "build-wheels")))
                self._pip([str(h.python), "-m", "pip", "download", "--disable-pip-version-check", "--no-input", "--require-hashes", "--no-deps",
                           "-d", str(files_root / sub), "-r", str(req)] + self._pip_index_args(None))
            progress("maven", "exporting Maven repository")
            maven_dir = files_root / "maven"
            maven_entries = list(h.lock.get("jars", []))
            if maven_entries:
                resolver = self._resolver(h.spark_home / "jars", m, offline=self.cfg.maven_offline)
                roots = h.lock.get("jar_roots", [])
                coords = resolver.resolve(roots)
                resolver.fetch_jars(coords, work / "jars-check", locked={e["coordinate"]: e["sha256"] for e in maven_entries})
                resolver.export_repository(maven_dir)
            wheel = self.cfg.olcf_wheel or build_olcf_wheel(files_root / "olcf")
            if wheel.parent != files_root / "olcf":
                (files_root / "olcf").mkdir(exist_ok=True)
                shutil.copyfile(wheel, files_root / "olcf" / wheel.name)
            (files_root / "manifest.json").write_text(json.dumps(m.to_dict(), indent=2, sort_keys=True))
            (files_root / "signature.json").write_text(json.dumps(m.signature, indent=2))
            (files_root / "lock.json").write_text(json.dumps(h.lock, indent=2, sort_keys=True))
            index: dict[str, str] = {}
            for p in sorted(files_root.rglob("*")):
                if p.is_file():
                    index[p.relative_to(files_root).as_posix()] = sha256_file(p)
            bundle_json = {"schema": BUNDLE_SCHEMA, "runtime_id": runtime_id, "manifest_sha256": m.manifest_sha256, "created_at": now_iso(),
                           "platform": {"os": normalise_os(), "arch": normalise_arch(), "python": m.python_series}, "files": index,
                           "created_by": f"olcf {__version__}"}
            (files_root / "bundle.json").write_text(json.dumps(bundle_json, indent=2, sort_keys=True))
            out = Path(out_path)
            out.parent.mkdir(parents=True, exist_ok=True)
            with tarfile.open(out, "w:gz") as tf:
                for p in sorted(files_root.rglob("*")):
                    tf.add(p, arcname=p.relative_to(files_root).as_posix(), recursive=False)
            return {"path": str(out), "sha256": sha256_file(out), "size": out.stat().st_size, "files": len(index), "runtime_id": runtime_id}
        finally:
            shutil.rmtree(work, ignore_errors=True)

    def extract_bundle(self, bundle_path: str | Path, dest: Path) -> dict[str, Any]:
        """Safely extract + verify (every file hash, platform, manifest signature) an offline bundle into ``dest``."""
        bp = Path(bundle_path)
        if not bp.is_file():
            raise BundleInvalid(f"bundle {bp} not found")
        dest.mkdir(parents=True, exist_ok=True)
        root = dest.resolve()
        try:
            with tarfile.open(bp, "r:*") as tf:
                members = tf.getmembers()
                for mem in members:
                    target = (dest / mem.name).resolve()
                    if not (str(target) + os.sep).startswith(str(root) + os.sep) or mem.issym() or mem.islnk() or mem.isdev():
                        raise BundleInvalid(f"bundle contains an unsafe entry: {mem.name}")
                try:
                    tf.extractall(dest, filter="data")
                except TypeError:  # pragma: no cover - Python 3.11
                    tf.extractall(dest)  # noqa: S202 - members validated above
        except (tarfile.TarError, OSError) as exc:
            raise BundleInvalid(f"cannot read bundle {bp}: {exc}") from exc
        try:
            meta = json.loads((dest / "bundle.json").read_text())
        except (OSError, ValueError) as exc:
            raise BundleInvalid("bundle.json missing or unreadable") from exc
        if meta.get("schema") != BUNDLE_SCHEMA:
            raise BundleInvalid(f"unsupported bundle schema {meta.get('schema')!r}")
        for rel, digest in meta["files"].items():
            p = (dest / rel).resolve()
            if not str(p).startswith(str(root) + os.sep) or not p.is_file():
                raise BundleInvalid(f"bundle file {rel} is missing")
            if sha256_file(p) != digest:
                raise BundleInvalid(f"bundle file {rel} does not match bundle.json (corrupted or tampered)")
        plat = meta.get("platform", {})
        if plat.get("os") != normalise_os() or plat.get("arch") != normalise_arch():
            raise BundleInvalid(f"bundle was built for {plat.get('os')}/{plat.get('arch')}, this node is {normalise_os()}/{normalise_arch()}",
                                remediation="Export a bundle on a machine with the same OS/CPU architecture.")
        m = RuntimeManifest.load(dest / "manifest.json")
        verify_manifest(m, self.trust)
        if m.manifest_sha256 != meta["manifest_sha256"]:
            raise BundleInvalid("bundle manifest does not match bundle.json")
        meta["manifest"] = m.to_dict()
        return meta

    def import_bundle(self, bundle_path: str | Path, *, progress: ProgressFn | None = None, force: bool = False, activate: bool = True) -> dict[str, Any]:
        """Install a runtime from an offline bundle: no network is used (``--no-index --find-links``, file:// Maven repo)."""
        work = Path(tempfile.mkdtemp(prefix="olcf-import-", dir=self._ensure(self.dir / ".stage")))
        try:
            meta = self.extract_bundle(bundle_path, work)
            res = self.install(meta["manifest"], bundle_dir=work, force=force, activate=activate, progress=progress)
            res["source"] = "bundle"
            return res
        finally:
            shutil.rmtree(work, ignore_errors=True)

    # ------------------------------------------------------------------ internals
    @staticmethod
    def _ensure(p: Path) -> Path:
        p.mkdir(parents=True, exist_ok=True)
        return p

    def _exclusive(self, runtime_id: str, op: str):
        mgr = self

        class _Ctx:
            def __enter__(self_inner):  # noqa: N805
                mgr._set_busy(runtime_id, op)
                self_inner.lock = FileLock(mgr._lock_path)
                self_inner.lock.__enter__()
                return self_inner

            def __exit__(self_inner, *exc):  # noqa: N805
                try:
                    self_inner.lock.__exit__(*exc)
                finally:
                    mgr._set_busy(runtime_id, None)

        return _Ctx()

    def _activate(self, rid: str, family: str) -> None:
        st = self._load_state()
        hist = st["history"].setdefault(family, [])
        if rid in hist:
            hist.remove(rid)
        hist.append(rid)
        st["active"][family] = rid
        self._save_state(st)

    def _swap(self, stage: Path, final: Path) -> None:
        trash: Path | None = None
        if final.exists():
            trash = self.dir / ".trash" / f"{final.name}-{int(time.time())}-{uuid.uuid4().hex[:6]}"
            trash.parent.mkdir(exist_ok=True)
            os.replace(final, trash)
        try:
            os.replace(stage, final)
        except OSError:
            if trash is not None:
                os.replace(trash, final)
            raise
        if trash is not None:
            shutil.rmtree(trash, ignore_errors=True)

    def _pip_index_args(self, bundle_dir: Path | None) -> list[str]:
        c = self.cfg
        if bundle_dir is not None:
            return ["--no-index", "--find-links", str(bundle_dir / "wheels"), "--find-links", str(bundle_dir / "build-wheels")]
        args: list[str] = []
        if c.deny_public_install and not (c.pip_index_url or c.pip_find_links or c.pip_no_index):
            raise PrerequisiteMissing("public package installation is denied by policy and no mirror/wheelhouse is configured",
                                      remediation="Configure pip_index_url (your mirror) or pip_find_links (a wheelhouse), or import an offline bundle.")
        if c.pip_no_index:
            args.append("--no-index")
        elif c.pip_index_url:
            args += ["--index-url", c.pip_index_url]
        for u in c.pip_extra_index_urls:
            args += ["--extra-index-url", u]
        for f in c.pip_find_links:
            args += ["--find-links", f]
        return args + list(c.pip_extra_args)

    def _pip(self, cmd: list[str], *, cwd: Path | None = None) -> None:
        env = {k: v for k, v in os.environ.items() if k not in ("PYTHONPATH", "PYTHONHOME", "VIRTUAL_ENV")}
        env["PIP_DISABLE_PIP_VERSION_CHECK"] = "1"
        try:
            res = subprocess.run(cmd, capture_output=True, text=True, timeout=self.cfg.pip_timeout, check=False, cwd=cwd, env=env)  # noqa: S603
        except subprocess.TimeoutExpired as exc:
            raise InstallFailed(f"pip timed out after {self.cfg.pip_timeout}s") from exc
        if res.returncode != 0:
            tail = (res.stderr or res.stdout)[-1500:]
            hint = None
            if "HASHES" in tail or "THESE PACKAGES DO NOT MATCH" in tail:
                hint = "A downloaded package does not match the hash pinned in the signed manifest: do NOT override; check your index/mirror."
            raise InstallFailed(f"pip failed: {tail}", remediation=hint or "Check the pip output above, network/proxy access to the package index, or use an offline bundle.")

    def _resolver(self, spark_jars: Path, m: RuntimeManifest, *, offline: bool, bundle_dir: Path | None = None) -> MavenResolver:
        repos: list[Any] | None = self.cfg.maven_repositories
        if bundle_dir is not None and (bundle_dir / "maven").is_dir():
            repos = [(bundle_dir / "maven").resolve().as_uri()]
            offline = True
        cache = self.cfg.maven_cache or (Path(self.cfg.state_dir) / "cache" / "maven")
        return MavenResolver(repos, cache_dir=cache, offline=offline, provided_artifacts=jar_artifact_index(spark_jars),
                             provided_patterns=m.provided_jars, overrides=(m.data.get("jar_overrides") or {}))

    def _jdk(self, m: RuntimeManifest, progress: ProgressFn) -> hostenv.JavaInfo:
        extra = [self.dir / "_jdk" / str(v) for v in m.java_supported]
        try:
            return hostenv.find_java(m.java_supported, m.java_major, extra_homes=extra)
        except PrerequisiteMissing:
            if not self.cfg.with_jdk:
                raise
        progress("jdk", f"downloading Temurin {m.java_major} from the Adoptium API (sha256 verified)")
        return hostenv.install_temurin(m.java_major, self.dir / "_jdk", release_name=(m.data["java"].get("temurin_release")), api_base=self.cfg.adoptium_api)

    def _build(self, m: RuntimeManifest, stage: Path, *, bundle_dir: Path | None, previous_lock: dict[str, Any] | None,
               jar_groups: list[str] | None, progress: ProgressFn) -> dict[str, Any]:
        """Create venv, install pinned packages, resolve jars; return the lock dict."""
        progress("prerequisites", "locating Python and Java")
        py = hostenv.find_python(m.python_series, extra=self.cfg.python_candidates, allow_uv_install=self.cfg.allow_uv_python_install)
        java = self._jdk(m, progress)
        if java.major not in m.java_supported:
            raise PrerequisiteMissing(f"Java {java.major} is not supported by runtime {m.id} (supported: {m.java_supported})")
        venv = stage / "venv"
        progress("venv", f"creating venv with {py.executable}")
        self._create_venv(py, venv)
        vpy = hostenv.venv_python(venv)
        index_args = self._pip_index_args(bundle_dir)
        if m.build_packages:
            progress("pip-build-deps", f"{len(m.build_packages)} build requirement(s)")
            req = stage / "build.requirements.txt"
            req.write_text(m.requirements_text(build=True))
            self._pip([str(vpy), "-m", "pip", "install", "--require-hashes", "--no-deps", "-r", str(req)] + index_args)
        progress("pip", f"installing {len(m.packages)} hash-pinned packages")
        req = stage / "requirements.lock.txt"
        req.write_text(m.requirements_text())
        self._pip([str(vpy), "-m", "pip", "install", "--require-hashes", "--no-deps", "--no-build-isolation", "-r", str(req)] + index_args)
        progress("olcf", "installing the olcf runtime library (--no-deps)")
        wheel_dir = stage / "_wheel"
        if bundle_dir is not None and any((bundle_dir / "olcf").glob("*.whl")):
            wheel = next((bundle_dir / "olcf").glob("*.whl"))
        else:
            wheel = self.cfg.olcf_wheel or build_olcf_wheel(wheel_dir)
        self._pip([str(vpy), "-m", "pip", "install", "--no-deps", "--no-index", "--force-reinstall", str(wheel)])
        # pyspark location + hadoop version (drives ${hadoop.version} in jar coordinates)
        info = self._introspect(vpy, venv, stage)
        hadoop = info.get("hadoop_version")
        spark_dir = stage / info["pyspark_dir"] if info.get("pyspark_dir") else None
        jar_entries: list[dict[str, Any]] = []
        roots: list[str] = []
        if m.jars:
            if spark_dir is None:
                raise InstallFailed("manifest lists jars but pyspark is not part of its packages")
            variables = m.jar_variables(hadoop)
            enabled = set(jar_groups or []) | {"core"}
            roots = [j.resolved_coordinate(variables) for j in m.jars if (not j.optional) or j.group in enabled]
            progress("maven", f"resolving {len(roots)} root coordinate(s)")
            resolver = self._resolver(spark_dir / "jars", m, offline=self.cfg.maven_offline, bundle_dir=bundle_dir)
            locked = None
            src = previous_lock or (json.loads((bundle_dir / "lock.json").read_text()) if bundle_dir and (bundle_dir / "lock.json").is_file() else None)
            if src and src.get("jar_roots") == roots:
                locked = {e["coordinate"]: e["sha256"] for e in src.get("jars", [])}
            jar_entries = resolver.resolve_and_fetch(roots, stage / "jars", locked=locked)
            progress("jars", f"linking {len(jar_entries)} jar(s) into the Spark distribution")
            self._link_jars(stage / "jars", spark_dir / "jars", jar_entries)
        else:
            (stage / "jars").mkdir(exist_ok=True)
        packages = self._installed_packages(vpy)
        lock: dict[str, Any] = {
            "schema": LOCK_SCHEMA_ID, "runtime_id": m.id, "manifest_sha256": m.manifest_sha256,
            "python": {"version": py.version, "series": py.series, "venv": "venv"},
            "java": {"major": java.major, "version": java.version, "home": java.home, "vendor": java.vendor},
            "spark": {"version": info.get("spark_version") or m.spark_version, "hadoop_version": hadoop, "pyspark_dir": info.get("pyspark_dir")},
            "platform": {"os": normalise_os(), "arch": normalise_arch()},
            "packages": packages, "jars": jar_entries, "jar_roots": roots,
            "olcf": {"version": __version__, "wheel_sha256": sha256_file(wheel)},
            "meta": {"created_at": now_iso(), "installer": f"olcf-runtime-manager {__version__}", "source": "bundle" if bundle_dir else "online"},
        }
        if m.data["java"].get("temurin_release") or (self.dir / "_jdk" / str(java.major) / "OLCF-JDK.json").exists():
            try:
                lock["java"]["temurin"] = json.loads((self.dir / "_jdk" / str(java.major) / "OLCF-JDK.json").read_text())
            except (OSError, ValueError):
                pass
        return lock

    def _create_venv(self, py: hostenv.PythonInfo, venv: Path) -> None:
        env = {k: v for k, v in os.environ.items() if k not in ("PYTHONPATH", "PYTHONHOME", "VIRTUAL_ENV")}
        if py.has_ensurepip and py.has_venv:
            cmd = [py.executable, "-I", "-m", "venv", "--clear", str(venv)]
        else:
            uv = shutil.which("uv")
            if not uv:
                raise PrerequisiteMissing(f"python at {py.executable} cannot create venvs", remediation="Install python3-venv or uv.")
            cmd = [uv, "venv", "--seed", "--python", py.executable, str(venv)]
        res = subprocess.run(cmd, capture_output=True, text=True, timeout=300, check=False, env=env)  # noqa: S603
        if res.returncode != 0:
            raise InstallFailed(f"venv creation failed: {res.stderr.strip()[-500:]}", remediation="Check the Python installation (venv/ensurepip).")

    def _introspect(self, vpy: Path, venv: Path, stage: Path) -> dict[str, Any]:
        code = (
            "import json, os, sys\n"
            "out = {}\n"
            "try:\n"
            "    import pyspark, glob\n"
            "    out['spark_version'] = pyspark.__version__\n"
            "    d = os.path.dirname(pyspark.__file__)\n"
            "    out['pyspark_dir_abs'] = d\n"
            "    hs = glob.glob(os.path.join(d, 'jars', 'hadoop-client-api-*.jar'))\n"
            "    if hs:\n"
            "        b = os.path.basename(hs[0]); out['hadoop_version'] = b[len('hadoop-client-api-'):-len('.jar')]\n"
            "except ImportError:\n    pass\n"
            "print(json.dumps(out))\n"
        )
        res = subprocess.run([str(vpy), "-I", "-c", code], capture_output=True, text=True, timeout=120, check=False)  # noqa: S603
        if res.returncode != 0:
            raise InstallFailed(f"cannot introspect the new venv: {res.stderr[-400:]}")
        out = json.loads(res.stdout.strip().splitlines()[-1])
        if out.get("pyspark_dir_abs"):
            out["pyspark_dir"] = Path(out["pyspark_dir_abs"]).resolve().relative_to(stage.resolve()).as_posix()
        return out

    def _installed_packages(self, vpy: Path) -> list[dict[str, str]]:
        code = ("import json, importlib.metadata as m\n"
                "print(json.dumps(sorted([{'name': d.metadata['Name'], 'version': d.version} for d in m.distributions()], key=lambda x: x['name'].lower())))\n")
        res = subprocess.run([str(vpy), "-I", "-c", code], capture_output=True, text=True, timeout=120, check=False)  # noqa: S603
        return json.loads(res.stdout.strip().splitlines()[-1]) if res.returncode == 0 else []

    def _link_jars(self, jars_dir: Path, spark_jars: Path, entries: list[dict[str, Any]]) -> None:
        """Make resolved jars visible to the driver *and* executors without per-app ``spark.jars`` shipping.

        Executors (started by Spark Worker daemons on other nodes, possibly from a different state dir) load
        ``$SPARK_HOME/jars/*`` by default, so the jars are linked there with *relative* symlinks (still valid after the
        stage directory is renamed); copies are used where symlinks are unavailable (Windows without privilege)."""
        spark_jars.mkdir(parents=True, exist_ok=True)
        for e in entries:
            src, dst = jars_dir / e["file"], spark_jars / e["file"]
            if dst.exists() or dst.is_symlink():
                if dst.is_symlink() or sha256_file(dst) != e["sha256"]:
                    dst.unlink()
                else:
                    e["linked"] = "existing"
                    continue
            try:
                if self.cfg.link_jars == "copy":
                    raise OSError("copy requested")
                os.symlink(os.path.relpath(src, dst.parent), dst)
                e["linked"] = "symlink"
            except OSError:
                shutil.copyfile(src, dst)
                e["linked"] = "copy"

    def _smoke(self, root: Path, h: RuntimeHandle, m: RuntimeManifest) -> dict[str, Any]:
        mode = m.verify_mode
        if mode == "none":
            return {"mode": "none", "steps": []}
        vpy = hostenv.venv_python(root / "venv")
        env = {k: v for k, v in os.environ.items() if k not in ("PYTHONPATH", "PYTHONHOME", "VIRTUAL_ENV", "SPARK_HOME", "JAVA_TOOL_OPTIONS")}
        env.update(RuntimeHandle(h.id, root, h.manifest, h.lock).env(extra_java_homes=[self.dir / "_jdk" / str(v) for v in m.java_supported]))
        env["OLCF_RUNTIME_DIR"] = str(root)
        with tempfile.TemporaryDirectory(prefix="olcf-smoke-") as td:
            cmd = [*self.cfg.smoke_command_prefix, str(vpy), "-I", "-m", "olcf.runtime.smoke", "--mode", mode, "--workdir", td, "--expect-spark", m.spark_version]
            imports = (m.data.get("verify") or {}).get("imports") or []
            if imports:
                cmd += ["--imports", ",".join(imports)]
            if any("delta-spark" in e["coordinate"] for e in h.lock.get("jars", [])):
                cmd += ["--delta"]
            try:
                res = subprocess.run(cmd, capture_output=True, text=True, timeout=self.cfg.smoke_timeout, check=False, env=env, cwd=td)  # noqa: S603
            except subprocess.TimeoutExpired as exc:
                raise VerifyFailed(f"smoke test of {m.id} timed out after {self.cfg.smoke_timeout}s") from exc
        try:
            out = json.loads(res.stdout.strip().splitlines()[-1])
        except (ValueError, IndexError):
            out = {"ok": False, "steps": [], "error": (res.stderr or res.stdout)[-800:]}
        if res.returncode != 0 or not out.get("ok"):
            bad = [s for s in out.get("steps", []) if not s.get("ok")]
            detail = bad[0]["detail"] if bad else out.get("error", "")
            raise VerifyFailed(f"smoke test of runtime {m.id} failed: {str(detail)[-600:]}", details={"steps": out.get("steps", [])},
                               remediation="Check Java/Python compatibility and the jar set; see the step list for the failing check.")
        return out


def _family_of(root: Path) -> str | None:
    try:
        return str(json.loads((root / "manifest.json").read_text())["family"])
    except (OSError, ValueError, KeyError):
        return None


def _relocate_venv(stage: Path, final: Path) -> None:
    """Rewrite absolute stage paths in venv scripts so they still work after the atomic rename to ``final``."""
    venv = stage / "venv"
    old, new = str(stage), str(final)
    for sub in ("bin", "Scripts"):
        d = venv / sub
        if not d.is_dir():
            continue
        for f in d.iterdir():
            if f.is_symlink() or not f.is_file() or f.stat().st_size > 2_000_000:
                continue
            try:
                data = f.read_bytes()
            except OSError:
                continue
            if old.encode() in data and b"\0" not in data[:4096]:
                f.write_bytes(data.replace(old.encode(), new.encode()))
    cfg = venv / "pyvenv.cfg"
    if cfg.is_file():
        txt = cfg.read_text()
        if old in txt:
            cfg.write_text(txt.replace(old, new))


__all__ = ["BUNDLED_MANIFESTS", "ManagerConfig", "RuntimeHandle", "RuntimeManager", "bundled_manifest_paths", "fingerprint_of"]

# keep linters honest about re-exported names used by other modules
_ = (ManifestInvalid, sys)
