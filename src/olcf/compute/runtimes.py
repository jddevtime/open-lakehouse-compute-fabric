# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Runtime catalog (CONTRACTS 4.5): manifests, per-node install status, deterministic resolution and install operations.

* **Catalog** - manifests shipped in ``olcf/runtime/manifests/*.json`` are loaded into the ``runtimes`` table on start (an absent or
  broken directory is tolerated and logged); administrators can register more through ``POST /v1/runtimes`` (signature verified
  with the trust store unless ``compute.allow_unsigned_runtimes`` is on). Per-node install state comes from agent heartbeats
  (``runtime_installs``, maintained by the hub).
* **Resolution** - :meth:`RuntimeCatalog.resolve` turns a :class:`RuntimeRequest` into exactly one runtime. It is a pure function of
  (request, catalog): family / exact id / version specifiers, highest version wins, ties break on id. The approved Spark-conf
  allow-list of the chosen manifest is applied to user + environment conf.
* **Operations** - ``install | verify | repair | retire | rollback`` fan an ``install_runtime`` command out to the selected agents
  as a tracked operation (``/v1/operations/{id}``); offline bundles are uploaded here and fetched by agents from the download URL.
* **Matrix** - the compatibility matrix lists per family the engine versions and the Delta protocol (reader/writer versions and table
  features) the family can read and write. The feature table is static knowledge from the Delta release notes: it is advisory
  and is enforced where tables are actually written (``storage.delta_policy``).
"""

from __future__ import annotations

import fnmatch
import hashlib
import json
import logging
import re
import threading
from concurrent.futures import ThreadPoolExecutor
from dataclasses import asdict, dataclass, field, is_dataclass
from importlib import resources
from pathlib import Path
from typing import Annotated, Any

from fastapi import APIRouter, File, Query, Request, UploadFile
from fastapi.responses import FileResponse
from packaging.specifiers import InvalidSpecifier, SpecifierSet
from packaging.version import InvalidVersion, Version
from pydantic import BaseModel, Field
from sqlalchemy import select, update

from olcf.api.deps import Authed, Ctx, Idempotency, IdempotencyKey
from olcf.compute import schema as C
from olcf.compute.errors import WorkerOffline
from olcf.compute.registry import ComputeRegistry
from olcf.core.context import AppContext
from olcf.core.errors import (
    BadRequest,
    Conflict,
    NotFound,
    OlcfError,
    PolicyDenied,
    Unsupported,
    ValidationFailed,
)
from olcf.core.hostruntime import HOST_FAMILY, HOST_RUNTIME_ID, NO_SPARK_REMEDIATION
from olcf.core.ids import new_id
from olcf.core.models import RuntimeRequest
from olcf.core.timeutil import now_iso
from olcf.db.util import fetch_all, fetch_one, insert_row
from olcf.security.rbac import Principal

log = logging.getLogger("olcf.compute.runtimes")

DEFAULT_FAMILY = "fabric-2.0-compatible"
MODES = ("install", "verify", "repair", "retire", "rollback")
MAX_BUNDLE_BYTES = 8 * 1024 ** 3

#: Spark settings the platform owns: a job (or environment) that tries to set them is rejected, never silently overridden.
OLCF_OWNED_CONF = (
    "spark.master", "spark.app.name", "spark.driver.host", "spark.driver.bindAddress", "spark.driver.port", "spark.blockManager.port",
    "spark.port.maxRetries", "spark.submit.*", "spark.authenticate*", "spark.network.crypto.*", "spark.ui.proxyBase", "spark.ui.reverseProxy*",
    "spark.kubernetes.*", "spark.jars", "spark.jars.*", "spark.driver.extraClassPath", "spark.executor.extraClassPath",
    "spark.driver.extraJavaOptions", "spark.executor.extraJavaOptions", "spark.driver.extraLibraryPath", "spark.executor.extraLibraryPath",
)

# --------------------------------------------------------------------------- Delta compatibility knowledge (static, advisory)
_DELTA_COMMON = ("appendOnly", "invariants", "checkConstraints", "generatedColumns", "changeDataFeed", "columnMapping", "identityColumns",
                 "deletionVectors", "timestampNtz", "v2Checkpoint", "rowTracking", "domainMetadata", "clustering", "icebergCompatV1", "allowColumnDefaults")
_DELTA_4_ONLY = ("typeWidening", "variantType", "icebergCompatV2", "inCommitTimestamp", "vacuumProtocolCheck", "checkpointProtection", "coordinatedCommits")
DELTA_TABLE: dict[str, dict[str, Any]] = {
    "3": {"min_reader": 3, "min_writer": 7, "features": _DELTA_COMMON + ("typeWidening-preview",)},
    "4": {"min_reader": 3, "min_writer": 7, "features": _DELTA_COMMON + _DELTA_4_ONLY},
}
#: Delta features that make a table unreadable by older readers / by Fabric Runtime 1.x (the interop-relevant ones).
DELTA_READER_FEATURES = {"deletionVectors", "columnMapping", "timestampNtz", "v2Checkpoint", "typeWidening", "typeWidening-preview", "variantType", "vacuumProtocolCheck"}


# =========================================================================== resolution types
@dataclass
class RuntimeSelection:
    """Result of :meth:`RuntimeCatalog.resolve` (everything the job-spec generator needs about the runtime)."""

    runtime_id: str
    family: str
    version: str
    fingerprint: str | None
    manifest: dict[str, Any]
    spark_conf: dict[str, str] = field(default_factory=dict)  # approved, effective user/environment conf
    rejected_conf: list[dict[str, str]] = field(default_factory=list)  # dropped keys with the reason
    environment: dict[str, Any] | None = None
    dependency_lock: dict[str, Any] | None = None
    ready_nodes: list[str] = field(default_factory=list)
    notes: list[str] = field(default_factory=list)
    #: Set when the request was served by the host-Python fallback: ``{"requested": <runtime that was not installed>, "using": "olcf-host-python", "reason": ...}``.
    fallback: dict[str, Any] | None = None

    def to_dict(self) -> dict[str, Any]:
        d = asdict(self)
        d.pop("manifest", None)
        d["spark_version"] = (self.manifest.get("spark") or {}).get("version")
        return d


def _ver(value: Any) -> str | None:
    if isinstance(value, dict):
        value = value.get("version") or value.get("major")
    return None if value in (None, "") else str(value)


def _specifier(text: str) -> SpecifierSet:
    """``'4.1'`` means ``==4.1.*``; ``'4.1.1'`` means ``==4.1.1``; anything with an operator is a normal PEP 440 specifier."""
    t = text.strip()
    if not t:
        raise BadRequest("empty version specifier")
    if t[0].isdigit():
        t = f"=={t}" if t.count(".") >= 2 or t.endswith(".*") else f"=={t}.*"
    try:
        return SpecifierSet(t)
    except InvalidSpecifier as exc:
        raise BadRequest(f"invalid version specifier '{text}'", remediation="use PEP 440 syntax such as '>=4.1,<4.2'") from exc


def _matches(spec_text: str | None, value: str | None) -> bool:
    if not spec_text:
        return True
    if value is None:
        return False
    try:
        return _specifier(spec_text).contains(Version(value), prereleases=True)
    except InvalidVersion:
        return False


def _as_dict(obj: Any) -> dict[str, Any]:
    if obj is None:
        return {}
    if isinstance(obj, dict):
        return dict(obj)
    if hasattr(obj, "model_dump"):
        return obj.model_dump()
    if is_dataclass(obj) and not isinstance(obj, type):
        return asdict(obj)
    return dict(getattr(obj, "__dict__", {}))


def conf_allowed(key: str, patterns: list[str]) -> bool:
    return any(fnmatch.fnmatchcase(key, p) for p in patterns)


def filter_spark_conf(conf: dict[str, Any], approved: list[str]) -> tuple[dict[str, str], list[dict[str, str]]]:
    """Split ``conf`` into (accepted, rejected). Platform-owned keys raise :class:`PolicyDenied`; unapproved keys are dropped and reported."""
    accepted: dict[str, str] = {}
    rejected: list[dict[str, str]] = []
    for k in sorted(conf):
        v = conf[k]
        if not isinstance(k, str) or not k.startswith("spark."):
            rejected.append({"key": str(k), "reason": "not a spark.* key"})
        elif conf_allowed(k, list(OLCF_OWNED_CONF)):
            raise PolicyDenied(f"Spark setting '{k}' is managed by the platform and cannot be overridden",
                               remediation="remove it from the job, environment or notebook configuration")
        elif not conf_allowed(k, approved):
            rejected.append({"key": k, "reason": "not on the runtime's approved Spark configuration list"})
        elif isinstance(v, (dict, list)):
            raise BadRequest(f"Spark setting '{k}' must be a scalar")
        else:
            accepted[k] = ("true" if v is True else "false" if v is False else str(v))
    return accepted, rejected


# =========================================================================== catalog
class RuntimeCatalog:
    """Registry of runtime manifests + resolution + install operations. Registered as the ``runtimes`` service."""

    def __init__(self, ctx: AppContext, registry: ComputeRegistry, hub: Any) -> None:
        self.ctx, self.registry, self.hub = ctx, registry, hub
        self._ops = ThreadPoolExecutor(max_workers=4, thread_name_prefix="olcf-runtime-op")
        self._bundle_dir = ctx.settings.path("artifacts", "runtime-bundles")
        self._inflight: set[tuple[str, str]] = set()
        self._lock = threading.Lock()

    # ------------------------------------------------------------------ manifests
    def load_builtin_manifests(self) -> int:
        """Load ``olcf/runtime/manifests/*.json`` into the catalog. Missing/invalid files are logged and skipped (never fatal)."""
        count = 0
        try:
            root = resources.files("olcf.runtime") / "manifests"
            files = sorted(p for p in root.iterdir() if p.name.endswith(".json"))
        except (ModuleNotFoundError, FileNotFoundError, OSError):
            log.warning("no built-in runtime manifests found (olcf/runtime/manifests); register runtimes through POST /v1/runtimes")
            return 0
        dev_signed: list[str] = []
        for f in files:
            try:
                data = json.loads(f.read_text(encoding="utf-8"))
                self._upsert(data, source="manifest")
                count += 1
            except (OlcfError, ValueError, OSError) as exc:
                from olcf.runtime.signing import is_dev_key_refusal

                if is_dev_key_refusal(exc):
                    dev_signed.append(f.name.removesuffix(".json"))
                    continue
                log.warning("ignoring runtime manifest %s: %s", f.name, getattr(exc, "message", exc))
        if dev_signed:
            log.warning("%s", self.dev_key_notice(dev_signed))
        return count

    def dev_key_notice(self, ids: list[str]) -> str:
        """The one-paragraph warning for bundled manifests refused because of the development key: why, and exactly what the operator does."""
        from olcf.runtime.signing import dev_key_advice

        s = self.ctx.settings
        cfg = s.data_dir / "config.toml"
        try:
            import tomllib

            explicit = "environment" in tomllib.loads(cfg.read_text(encoding="utf-8"))
        except (OSError, ValueError):
            explicit = False
        why = (f"environment = \"{s.environment}\" is set in {cfg}" if explicit else
               f"{cfg} has no `environment` key, so this installation is treated as production (installations created before the runtime-trust change are)")
        return (f"Spark runtimes are NOT available: the bundled runtime manifest(s) {', '.join(ids)} are signed with the OLCF development key, whose private half is public, "
                f"and it is not trusted here ({why}). Notebooks and jobs keep running on the built-in host-Python runtime. {dev_key_advice(cfg)} "
                f"The `runtime-trust` health check (GET /v1/admin/health, or `olcf health`) shows the current state.")

    def ensure_host_runtime(self) -> dict[str, Any]:
        """Register the built-in ``olcf-host-python`` entry (always: it is code, not a downloaded artifact, so there is no signature to verify)."""
        from olcf.runtime.hostpython import catalog_manifest

        data = catalog_manifest()
        now = now_iso()
        row = {"family": HOST_FAMILY, "version": str(data["version"]), "spark": None, "delta": None, "iceberg": None, "java": None, "scala": None, "python": None,
               "manifest": data, "fingerprint": self._fingerprint(data), "source": "builtin", "updated_at": now}
        with self.ctx.db.begin() as conn:
            cur = fetch_one(conn, select(C.runtimes).where(C.runtimes.c.id == HOST_RUNTIME_ID))
            if cur is None:
                insert_row(conn, C.runtimes, {"id": HOST_RUNTIME_ID, "state": "available", "created_at": now, **row})
            else:
                conn.execute(update(C.runtimes).where(C.runtimes.c.id == HOST_RUNTIME_ID).values(**{**row, "state": "available"}))  # builtin: cannot stay retired
            return fetch_one(conn, select(C.runtimes).where(C.runtimes.c.id == HOST_RUNTIME_ID)) or {}

    @staticmethod
    def _fingerprint(manifest: dict[str, Any]) -> str:
        return "sha256:" + hashlib.sha256(json.dumps(manifest, sort_keys=True, separators=(",", ":")).encode()).hexdigest()

    def validate_manifest(self, data: dict[str, Any], *, allow_unsigned: bool = False) -> dict[str, Any]:
        """Structural validation (the NODE manifest model when importable) and signature check against the trust store."""
        if not isinstance(data, dict) or not data.get("id") or not data.get("family"):
            raise ValidationFailed("a runtime manifest needs at least 'id' and 'family'")
        if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._-]{0,127}", str(data["id"])):
            raise ValidationFailed("runtime id may only contain letters, digits, '.', '_' and '-'")
        try:
            from olcf.runtime.manifest import RuntimeManifest
            from olcf.runtime.signing import verify_manifest
        except ImportError:
            if not allow_unsigned:
                raise Unsupported("runtime signature verification is unavailable on this control plane", remediation="install the OLCF runtime package") from None
            return data
        try:
            RuntimeManifest.from_dict(data)
        except OlcfError:
            raise
        except Exception as exc:  # noqa: BLE001
            raise ValidationFailed(f"invalid runtime manifest: {exc}") from exc
        if data.get("signature"):
            try:
                from olcf.runtime.signing import TrustStore

                verify_manifest(data, TrustStore.for_settings(self.ctx.settings))  # the development key is trusted only outside production (runtime.trust.*)
            except Exception as exc:  # noqa: BLE001
                raise ValidationFailed(f"runtime manifest signature is invalid or untrusted: {getattr(exc, 'message', None) or exc}",
                                       remediation=getattr(exc, "remediation", None)) from exc
        elif not allow_unsigned:
            raise PolicyDenied("runtime manifests must be signed", remediation="sign the manifest, or enable the platform setting compute.allow_unsigned_runtimes")
        return data

    def _upsert(self, data: dict[str, Any], *, source: str, allow_unsigned: bool = False) -> dict[str, Any]:
        data = self.validate_manifest(data, allow_unsigned=allow_unsigned)
        now = now_iso()
        row = {
            "family": str(data["family"]), "version": str(data.get("version") or "0"), "spark": _ver(data.get("spark")), "delta": _ver(data.get("delta")),
            "iceberg": _ver(data.get("iceberg")), "java": _ver(data.get("java")), "scala": _ver(data.get("scala")), "python": _ver(data.get("python")),
            "manifest": data, "fingerprint": self._fingerprint(data), "source": source, "updated_at": now,
        }
        with self.ctx.db.begin() as conn:
            cur = fetch_one(conn, select(C.runtimes).where(C.runtimes.c.id == data["id"]))
            if cur is None:
                insert_row(conn, C.runtimes, {"id": data["id"], "state": "available", "created_at": now, **row})
            else:
                if cur["source"] == "api" and source == "manifest":
                    return cur  # an administrator's registration beats the bundled file
                conn.execute(update(C.runtimes).where(C.runtimes.c.id == data["id"]).values(**row))  # keeps state (a retired runtime stays retired)
            return fetch_one(conn, select(C.runtimes).where(C.runtimes.c.id == data["id"])) or {}

    def register(self, principal: Principal, manifest: dict[str, Any]) -> dict[str, Any]:
        principal.require("runtime.manage")
        allow = bool(self.registry.setting("compute.allow_unsigned_runtimes", False))
        row = self._upsert(manifest, source="api", allow_unsigned=allow)
        self.ctx.audit.record(principal, "runtime.registered", resource_type="runtime", resource_id=row["id"], details={"family": row["family"], "version": row["version"]})
        return self.public(row)

    # ------------------------------------------------------------------ reads
    def rows(self, *, include_retired: bool = True) -> list[dict[str, Any]]:
        stmt = select(C.runtimes).order_by(C.runtimes.c.family, C.runtimes.c.id)
        if not include_retired:
            stmt = stmt.where(C.runtimes.c.state == "available")
        with self.ctx.db.connect() as conn:
            return fetch_all(conn, stmt)

    def get_row(self, runtime_id: str) -> dict[str, Any]:
        with self.ctx.db.connect() as conn:
            row = fetch_one(conn, select(C.runtimes).where(C.runtimes.c.id == runtime_id))
        if not row:
            raise NotFound(f"runtime '{runtime_id}' not found", details={"available": [r["id"] for r in self.rows()]})
        return row

    def installs(self, runtime_id: str | None = None) -> list[dict[str, Any]]:
        stmt = select(C.runtime_installs)
        if runtime_id:
            stmt = stmt.where(C.runtime_installs.c.runtime_id == runtime_id)
        with self.ctx.db.connect() as conn:
            return fetch_all(conn, stmt)

    def public(self, row: dict[str, Any], installs: list[dict[str, Any]] | None = None, workers: dict[str, dict[str, Any]] | None = None, *, full: bool = False) -> dict[str, Any]:
        m = row["manifest"] or {}
        out: dict[str, Any] = {
            "id": row["id"], "family": row["family"], "version": row["version"], "title": m.get("title"), "description": m.get("description"),
            "spark": row["spark"], "delta": row["delta"], "iceberg": row["iceberg"], "java": row["java"], "scala": row["scala"], "python": row["python"],
            "state": row["state"], "source": row["source"], "fingerprint": row["fingerprint"], "signed": bool(m.get("signature")),
            "os": m.get("os"), "arch": m.get("arch"), "created_at": row["created_at"], "updated_at": row["updated_at"],
            "no_spark": bool(m.get("no_spark")), "engines": list(m.get("engines") or ["spark", "python"]), "builtin": row["source"] == "builtin",
        }
        if installs is not None:
            workers = workers or {}
            nodes = [{"worker_id": i["worker_id"], "worker": (workers.get(i["worker_id"]) or {}).get("name"), "state": i["state"], "verified": bool(i["verified"]),
                      "fingerprint": i["fingerprint"], "detail": i["detail"], "updated_at": i["updated_at"]} for i in installs if i["runtime_id"] == row["id"]]
            if row["id"] == HOST_RUNTIME_ID:  # the worker's own interpreter: show which Python it is on every node
                for n in nodes:
                    mine = next((r for r in ((workers.get(n["worker_id"]) or {}).get("runtimes") or []) if isinstance(r, dict) and r.get("id") == HOST_RUNTIME_ID), {})
                    n["python"], n["packages"] = mine.get("python"), mine.get("packages") or {}
            out["nodes"] = sorted(nodes, key=lambda n: n["worker"] or "")
            out["ready_nodes"] = sum(1 for n in nodes if n["state"] == "ready")
            if row["id"] == HOST_RUNTIME_ID:
                out["python"] = ", ".join(sorted({n["python"] for n in nodes if n.get("python") and n["state"] == "ready"})) or None
                # the catalog entry is code, not a download: it is "ready" as soon as one worker offers it
                out["state"] = "ready" if out["ready_nodes"] else row["state"]
        if full:
            out["manifest"] = {k: v for k, v in m.items() if k != "signature"}
            out["approved_spark_conf"] = list(m.get("approved_spark_conf") or [])
            out["spark_conf_defaults"] = dict(m.get("spark_conf_defaults") or {})
        return out

    def list_public(self, *, include_retired: bool = True) -> list[dict[str, Any]]:
        installs = self.installs()
        with self.ctx.db.connect() as conn:
            workers = {w["id"]: w for w in fetch_all(conn, select(C.workers.c.id, C.workers.c.name, C.workers.c.runtimes))}
        return [self.public(r, installs, workers) for r in self.rows(include_retired=include_retired)]

    def node_fingerprint(self, runtime_id: str, worker_id: str) -> str | None:
        """The fingerprint ``worker_id`` reported for ``runtime_id`` (for the host-Python runtime it describes that node's interpreter and libraries)."""
        t = C.runtime_installs.c
        with self.ctx.db.connect() as conn:
            row = fetch_one(conn, select(t.fingerprint).where(t.runtime_id == runtime_id, t.worker_id == worker_id))
        return (row or {}).get("fingerprint")

    def explain_missing(self, runtime_id: str) -> tuple[str, str]:
        """``(message, remediation)`` for a job that waits because ``runtime_id`` is not ready on any *eligible* node: not installed anywhere, install running / failed,
        installed only on workers that are offline or cordoned, or (for the host runtime) offered by no online worker."""
        with self.ctx.db.connect() as conn:
            workers = {w["id"]: w for w in fetch_all(conn, select(C.workers.c.id, C.workers.c.name, C.workers.c.state, C.workers.c.scheduling))}
        installs = self.installs(runtime_id)
        ready = [i for i in installs if i["state"] == "ready"]
        name = lambda i: (workers.get(i["worker_id"]) or {}).get("name") or i["worker_id"]  # noqa: E731
        if runtime_id == HOST_RUNTIME_ID:
            return (f"runtime '{runtime_id}' (the workers' own Python) is not offered by any online worker",
                    "bring a worker online (`olcf worker list`; the server embeds one) or check that the worker's Python has ipykernel, jupyter_client and nbformat")
        if ready:
            off = ", ".join(sorted({name(i) for i in ready}))
            return (f"runtime '{runtime_id}' is installed only on worker(s) that cannot take work right now: {off} (offline, cordoned or draining)",
                    "bring that worker online / uncordon it, or install the runtime on another worker: olcf runtime install " + runtime_id)
        installing = [i for i in installs if i["state"] == "installing"]
        if installing:
            return (f"runtime '{runtime_id}' is being installed on {', '.join(sorted({name(i) for i in installing}))}; the job starts when it is ready",
                    "follow the install with: olcf runtime operations")
        failed = [i for i in installs if i["state"] == "failed"]
        if failed:
            det = str(failed[0].get("detail") or "").strip().splitlines()
            return (f"the install of runtime '{runtime_id}' failed on {name(failed[0])}" + (f": {det[0][:200]}" if det else ""),
                    f"retry with: olcf runtime install {runtime_id}   (Spark runtimes download from Maven Central: check network/proxy access), "
                    "or run Python-only notebooks on the built-in host Python runtime")
        return (f"runtime '{runtime_id}' is not installed on any worker",
                f"install it: olcf runtime install {runtime_id}   (downloads Spark from Maven Central; needs network access), "
                "or use a Python-engine notebook, which runs on the built-in host Python runtime")

    def ready_nodes(self, runtime_id: str) -> list[str]:
        return sorted(i["worker_id"] for i in self.installs(runtime_id) if i["state"] == "ready")

    # ------------------------------------------------------------------ resolution
    def online_ready_nodes(self, runtime_id: str) -> list[str]:
        """Like :meth:`ready_nodes`, but only workers that are online right now (not offline, revoked or never connected)."""
        ready = self.ready_nodes(runtime_id)
        if not ready:
            return []
        with self.ctx.db.connect() as conn:
            live = {r["id"] for r in fetch_all(conn, select(C.workers.c.id).where(C.workers.c.id.in_(ready), C.workers.c.state.in_(("online", "degraded")), C.workers.c.revoked_at.is_(None)))}
        return [w for w in ready if w in live]

    def resolve(self, req: RuntimeRequest, *, workspace_id: str | None = None, engine: str | None = None, online_only: bool = False) -> RuntimeSelection:
        """Deterministically pick the runtime for ``req`` (see module docstring). Raises ``Unsupported`` when nothing matches.

        ``engine`` (``"spark"`` | ``"python"`` | ``None`` = unknown) enables the host-Python fallback: a Python-engine request for which no installed runtime fits runs on
        ``olcf-host-python`` (:meth:`_host_fallback`); a Spark request never does, and naming the host runtime for ``engine=spark`` is refused.
        ``online_only`` makes "installed on a worker" mean "ready on a worker that is online now" (re-binding of queued jobs; submission keeps the lenient rule).
        """
        env_spec: dict[str, Any] | None = None
        eff = req.model_copy(deep=True)
        conf: dict[str, Any] = {}
        lock = eff.dependency_lock
        notes: list[str] = []
        if eff.environment_id:
            svc = self.ctx.maybe_service("environments")
            if svc is None:
                raise Unsupported("the request references an environment but the environments service is not available",
                                  remediation="install/enable the notebooks module or remove environment_id")
            env_spec = _as_dict(svc.resolve(eff.environment_id, workspace_id=workspace_id))
            rt = _as_dict(env_spec.get("runtime"))
            for fld in ("family", "runtime_id", "spark", "python", "java", "scala"):
                if getattr(eff, fld) in (None, "") and rt.get(fld):
                    setattr(eff, fld, rt[fld])
            conf.update(_as_dict(env_spec.get("spark_conf")))
            lock = lock or env_spec.get("dependency_lock") or env_spec.get("lock")
        elif lock:
            # an explicit lock without an environment item: validate its structure, integrity (lock_hash) and the index policy exactly like a published one, and
            # derive the node's layer block from it - never hand an unchecked lock to a node
            svc = self.ctx.maybe_service("environments")
            if svc is None:
                raise Unsupported("the request carries a dependency_lock but the environments service is not available",
                                  remediation="install/enable the notebooks module or remove dependency_lock")
            env_spec = _as_dict(svc.resolve(lock=lock, workspace_id=workspace_id))
            lock = env_spec.get("dependency_lock") or lock
            conf.update(_as_dict(env_spec.get("spark_conf")))
        conf.update(eff.spark_conf)  # explicit request conf wins over the environment's

        rows = self.rows()
        chosen: dict[str, Any] | None = None
        fallback: dict[str, Any] | None = None
        host_row = next((r for r in rows if r["id"] == HOST_RUNTIME_ID and r["state"] == "available"), None)
        python_engine = engine == "python"
        if eff.runtime_id:
            chosen = next((r for r in rows if r["id"] == eff.runtime_id), None)
            if chosen is None:
                raise Unsupported(f"runtime '{eff.runtime_id}' is not registered", details={"available": [r["id"] for r in rows]},
                                  remediation="list runtimes with GET /v1/runtimes")
            if chosen["state"] != "available":
                raise Unsupported(f"runtime '{eff.runtime_id}' is retired", remediation="choose another runtime or ask a RuntimeAdmin to restore it")
            if chosen["id"] != HOST_RUNTIME_ID:
                for fld, val in (("spark", chosen["spark"]), ("python", chosen["python"]), ("java", chosen["java"]), ("scala", chosen["scala"])):
                    if getattr(eff, fld) and not _matches(getattr(eff, fld), val):
                        raise Unsupported(f"runtime '{chosen['id']}' has {fld} {val}, which does not satisfy '{getattr(eff, fld)}'")
        else:
            family = eff.family
            constrained = any(getattr(eff, f) for f in ("spark", "python", "java", "scala"))
            if family is None and not constrained:
                family = str(self.registry.setting("compute.default_family", DEFAULT_FAMILY) or DEFAULT_FAMILY)
            # the host-Python runtime is never a candidate of the ordinary selection (it has no Spark/Java/Scala version): it is chosen only by name, by family
            # ``host-python``, or as the fallback below
            cands = [r for r in rows if r["state"] == "available" and (family is None or r["family"] == family) and (r["family"] != HOST_FAMILY or family == HOST_FAMILY)]
            for fld in ("spark", "python", "java", "scala"):
                spec = getattr(eff, fld)
                if spec and family != HOST_FAMILY:
                    cands = [r for r in cands if _matches(spec, r[fld])]
            if family == HOST_FAMILY:
                cands = [r for r in cands if not (eff.spark or eff.java or eff.scala)]
            if not cands:
                want = {k: v for k, v in (("family", family), ("spark", eff.spark), ("python", eff.python), ("java", eff.java), ("scala", eff.scala)) if v}
                fb = self._host_fallback(eff, host_row, python_engine, want_text=f"no registered runtime satisfies {want}")
                if fb is None:
                    raise Unsupported(f"no registered runtime satisfies {want}", details={"available": [{"id": r["id"], "family": r["family"], "spark": r["spark"], "python": r["python"]}
                                                                                              for r in rows if r["state"] == "available" and r["family"] != HOST_FAMILY]},
                                      remediation="register a matching runtime (POST /v1/runtimes) or relax the request")
                chosen, fallback = host_row, fb
            else:
                cands.sort(key=lambda r: (_sortable(r["version"]), r["id"]), reverse=True)
                chosen = cands[0]
                if python_engine and chosen["id"] != HOST_RUNTIME_ID and not (self.online_ready_nodes(chosen["id"]) if online_only else self.ready_nodes(chosen["id"])):
                    fb = self._host_fallback(eff, host_row, True, want_text=f"runtime '{chosen['id']}' is not installed on any worker", requested=chosen["id"])
                    if fb is not None:
                        chosen, fallback = host_row, fb
        if chosen["id"] == HOST_RUNTIME_ID and engine == "spark":
            raise Unsupported(f"runtime '{HOST_RUNTIME_ID}' has no Spark: it is the worker's own Python interpreter and cannot run engine=spark work",
                              remediation=NO_SPARK_REMEDIATION)
        if chosen["id"] == HOST_RUNTIME_ID:
            if conf:
                notes.append("Spark settings are ignored: the host Python runtime has no Spark")
            conf = {}
            if eff.python and not self._host_python_ok(eff.python):
                raise Unsupported(f"no online worker offers a host Python satisfying '{eff.python}'", remediation="relax the python version or install a runtime with that Python")
        manifest = chosen["manifest"] or {}
        accepted, rejected = filter_spark_conf(conf, list(manifest.get("approved_spark_conf") or []))
        if rejected:
            notes.append(f"{len(rejected)} Spark setting(s) were dropped: " + ", ".join(r["key"] for r in rejected[:8]))
        if fallback:
            notes.append(fallback["reason"])
        return RuntimeSelection(
            runtime_id=chosen["id"], family=chosen["family"], version=chosen["version"], fingerprint=chosen["fingerprint"], manifest=manifest,
            spark_conf=accepted, rejected_conf=rejected, environment=env_spec, dependency_lock=lock, ready_nodes=self.ready_nodes(chosen["id"]), notes=notes, fallback=fallback)

    # ------------------------------------------------------------------ host-Python fallback
    def host_python_versions(self) -> list[str]:
        """Python versions of the host-Python runtime as reported by the workers that currently offer it (state ``ready``)."""
        with self.ctx.db.connect() as conn:
            rows = fetch_all(conn, select(C.workers.c.runtimes).where(C.workers.c.state.in_(("online", "degraded", "offline"))))
        out: list[str] = []
        for r in rows:
            for rt in (r.get("runtimes") or []):
                if isinstance(rt, dict) and rt.get("id") == HOST_RUNTIME_ID and rt.get("state") == "ready" and rt.get("python"):
                    out.append(str(rt["python"]))
        return out

    def _host_python_ok(self, python_spec: str) -> bool:
        """A ``python`` constraint is satisfied when some worker's host interpreter matches (nothing known yet: refuse, we cannot promise it)."""
        return any(_matches(python_spec, v) for v in self.host_python_versions())

    def _host_fallback(self, eff: RuntimeRequest, host_row: dict[str, Any] | None, python_engine: bool, *, want_text: str, requested: str | None = None) -> dict[str, Any] | None:
        """``{"requested", "using", "reason"}`` when a Python-engine request may run on the workers' own Python, else ``None``.

        Allowed when: the engine is ``python``; the administrator did not switch ``compute.host_python_fallback`` off; the request names no exact runtime and asks for no Spark /
        Java / Scala version; some worker currently offers the host runtime; and a ``python`` constraint, if any, is met by some worker's interpreter.
        """
        if not python_engine or host_row is None or eff.runtime_id or eff.spark or eff.java or eff.scala:
            return None
        if not bool(self.registry.setting("compute.host_python_fallback", True)):
            return None
        if not self.host_python_versions():
            return None  # no worker offers it (yet): keep the normal choice - the job waits with an explicit reason instead of being bound to a runtime nobody has
        if eff.python and not self._host_python_ok(eff.python):
            return None
        return {"requested": requested, "using": HOST_RUNTIME_ID,
                "reason": f"{want_text}; running on the worker's own Python (runtime {HOST_RUNTIME_ID}, no Spark). Install a Spark runtime to use Spark in this notebook"}

    # ------------------------------------------------------------------ compatibility matrix
    def matrix(self) -> dict[str, Any]:
        rows = [r for r in self.rows() if r["family"] != HOST_FAMILY]  # the Python-only host runtime has no Spark/Delta/Java versions to compare
        families: dict[str, dict[str, Any]] = {}
        for r in rows:
            f = families.setdefault(r["family"], {"family": r["family"], "runtimes": [], "spark": set(), "delta": set(), "iceberg": set(), "java": set(), "scala": set(), "python": set()})
            f["runtimes"].append({"id": r["id"], "version": r["version"], "state": r["state"], "ready_nodes": len(self.ready_nodes(r["id"]))})
            for k in ("spark", "delta", "iceberg", "java", "scala", "python"):
                if r[k]:
                    f[k].add(r[k])
        out_f = []
        for name, f in sorted(families.items()):
            delta_major = next((str(v).split(".")[0] for v in sorted(f["delta"], reverse=True)), None)
            tbl = DELTA_TABLE.get(delta_major or "", None)
            entry = {k: (sorted(v) if isinstance(v, set) else v) for k, v in f.items()}
            entry["delta_protocol"] = ({"min_reader": tbl["min_reader"], "min_writer": tbl["min_writer"], "features": list(tbl["features"])} if tbl else None)
            entry["fabric_interop"] = name.startswith("fabric-")
            out_f.append(entry)
        feats = sorted({x for f in out_f for x in ((f["delta_protocol"] or {}).get("features") or [])})
        features = [{"feature": ft, "affects_readers": ft in DELTA_READER_FEATURES, "families": {f["family"]: ft in ((f["delta_protocol"] or {}).get("features") or []) for f in out_f}} for ft in feats]
        return {"generated_at": now_iso(), "families": out_f, "delta_features": features,
                "note": "Delta feature support is static knowledge from release notes (advisory); table writes are checked by the storage interop policy."}

    # ------------------------------------------------------------------ bundles
    def save_bundle(self, principal: Principal, filename: str, stream: Any, runtime_id: str | None) -> dict[str, Any]:
        principal.require("runtime.manage")
        safe = re.sub(r"[^A-Za-z0-9._-]", "_", Path(filename or "bundle.tar.zst").name)[:128] or "bundle"
        bid = new_id("bun")
        self._bundle_dir.mkdir(parents=True, exist_ok=True)
        dest = (self._bundle_dir / f"{bid}-{safe}").resolve()
        if self._bundle_dir.resolve() not in dest.parents:
            raise BadRequest("invalid bundle file name")
        h, size = hashlib.sha256(), 0
        try:
            with dest.open("wb") as fh:
                while True:
                    chunk = stream.read(1024 * 1024)
                    if not chunk:
                        break
                    size += len(chunk)
                    if size > MAX_BUNDLE_BYTES:
                        raise BadRequest("bundle exceeds the maximum size")
                    h.update(chunk)
                    fh.write(chunk)
        except Exception:
            dest.unlink(missing_ok=True)
            raise
        if runtime_id:
            self.get_row(runtime_id)
        rec = {"id": bid, "runtime_id": runtime_id, "filename": safe, "sha256": h.hexdigest(), "size": size, "path": dest.name, "uploaded_by": principal.id, "created_at": now_iso()}
        with self.ctx.db.begin() as conn:
            insert_row(conn, C.runtime_bundles, rec)
        self.ctx.audit.record(principal, "runtime.bundle_uploaded", resource_type="runtime_bundle", resource_id=bid, details={"filename": safe, "size": size, "sha256": rec["sha256"]})
        return self.public_bundle(rec)

    @staticmethod
    def public_bundle(r: dict[str, Any]) -> dict[str, Any]:
        return {k: r[k] for k in ("id", "runtime_id", "filename", "sha256", "size", "uploaded_by", "created_at")}

    def get_bundle(self, bundle_id: str) -> dict[str, Any]:
        with self.ctx.db.connect() as conn:
            row = fetch_one(conn, select(C.runtime_bundles).where(C.runtime_bundles.c.id == bundle_id))
        if not row:
            raise NotFound(f"bundle '{bundle_id}' not found")
        return row

    def bundle_file(self, row: dict[str, Any]) -> Path:
        p = (self._bundle_dir / row["path"]).resolve()
        if self._bundle_dir.resolve() not in p.parents or not p.is_file():
            raise NotFound("bundle file is missing on the control plane")
        return p

    # ------------------------------------------------------------------ operations
    def _targets(self, runtime_id: str, mode: str, worker_ids: list[str] | None, pool: str | None) -> list[dict[str, Any]]:
        workers = [w for w in self.registry.list_workers(include_revoked=False) if w["state"] in ("online", "degraded")]
        if worker_ids:
            wanted = set(worker_ids)
            picked = [w for w in workers if w["id"] in wanted or w["name"] in wanted]
            missing = wanted - {w["id"] for w in picked} - {w["name"] for w in picked}
            if missing:
                raise WorkerOffline(f"worker(s) not connected: {sorted(missing)}")
            workers = picked
        if pool:
            workers = [w for w in workers if w["pool"] == pool]
        if mode != "install":  # the other modes only make sense where the runtime is present
            have = {i["worker_id"] for i in self.installs(runtime_id)}
            workers = [w for w in workers if w["id"] in have]
        return workers

    def start_operation(self, principal: Principal, runtime_id: str, mode: str, *, worker_ids: list[str] | None = None, pool: str | None = None,
                        bundle_id: str | None = None, force: bool = False, base_url: str = "") -> dict[str, Any]:
        principal.require("runtime.manage", runtime_id=runtime_id)
        if mode not in MODES:
            raise BadRequest(f"mode must be one of {MODES}")
        row = self.get_row(runtime_id)
        if runtime_id == HOST_RUNTIME_ID:
            raise Conflict(f"'{HOST_RUNTIME_ID}' is the workers' own Python interpreter and is built in: nothing to {mode}",
                           remediation="to run Spark work install a Spark runtime, e.g. POST /v1/runtimes/olcf-rt-2.0.0/install; switch the fallback off with the setting compute.host_python_fallback")
        if mode in ("install", "repair") and row["state"] == "retired":
            raise Conflict(f"runtime '{runtime_id}' is retired; roll it back first")
        targets = self._targets(runtime_id, mode, worker_ids, pool)
        if not targets:
            raise Conflict(f"no connected worker to {mode} '{runtime_id}' on", remediation="check GET /v1/workers; for verify/repair/retire/rollback the runtime must already be installed")
        if mode == "retire" and not force:
            busy = self._runtime_in_use(runtime_id)
            if busy:
                raise Conflict(f"runtime '{runtime_id}' is used by {busy} active job(s)/session(s)", remediation="wait for them to finish or pass force=true")
        bundle = None
        if bundle_id:
            b = self.get_bundle(bundle_id)
            bundle = {"bundle_id": b["id"], "url": f"{base_url.rstrip('/')}/v1/runtimes/bundles/{b['id']}/download", "sha256": b["sha256"], "size": b["size"], "filename": b["filename"]}
        ops = self.ctx.service("operations")
        oid = ops.create(f"runtime.{mode}", principal.id, f"{mode} {runtime_id} on {len(targets)} node(s)")
        payload = {"runtime_manifest": row["manifest"], "mode": mode}
        if bundle:
            payload["offline_bundle"] = bundle
        self.ctx.audit.record(principal, f"runtime.{mode}", resource_type="runtime", resource_id=runtime_id,
                              details={"workers": [w["name"] for w in targets], "operation_id": oid, "offline": bool(bundle), "force": force})
        self._ops.submit(self._run_operation, oid, runtime_id, mode, payload, targets)
        return {"operation_id": oid, "runtime_id": runtime_id, "mode": mode, "targets": [{"id": w["id"], "name": w["name"]} for w in targets]}

    def _runtime_in_use(self, runtime_id: str) -> int:
        with self.ctx.db.connect() as conn:
            jobs = conn.execute(select(C.jobs.c.id).where(C.jobs.c.runtime_id == runtime_id, C.jobs.c.state.in_(("queued", "provisioning", "running", "cancel_requested")))).all()
            sess = conn.execute(select(C.sessions.c.id).where(C.sessions.c.runtime_id == runtime_id, C.sessions.c.state.in_(("starting", "idle", "busy")))).all()
        return len(jobs) + len(sess)

    def _set_install(self, runtime_id: str, worker_id: str, state: str, *, verified: bool | None = None, fingerprint: str | None = None, detail: str | None = None) -> None:
        t = C.runtime_installs.c
        now = now_iso()
        with self.ctx.db.begin() as conn:
            cur = fetch_one(conn, select(C.runtime_installs).where(t.runtime_id == runtime_id, t.worker_id == worker_id))
            vals = {"state": state, "updated_at": now, "detail": detail}
            if verified is not None:
                vals["verified"] = verified
            if fingerprint:
                vals["fingerprint"] = fingerprint
            if cur is None:
                insert_row(conn, C.runtime_installs, {"runtime_id": runtime_id, "worker_id": worker_id, "verified": bool(verified), "fingerprint": fingerprint, **vals})
            else:
                conn.execute(update(C.runtime_installs).where(t.runtime_id == runtime_id, t.worker_id == worker_id).values(**vals))

    def _run_operation(self, oid: str, runtime_id: str, mode: str, payload: dict[str, Any], targets: list[dict[str, Any]]) -> None:
        ops = self.ctx.service("operations")
        timeout = float(self.registry.setting("compute.runtime_install_timeout_seconds", 3600) or 3600)
        results: list[dict[str, Any]] = []
        done = 0

        def one(w: dict[str, Any]) -> dict[str, Any]:
            key = (runtime_id, w["id"])
            with self._lock:
                if key in self._inflight:
                    return {"worker_id": w["id"], "name": w["name"], "ok": False, "error": "another runtime operation is already running on this node"}
                self._inflight.add(key)
            try:
                if mode in ("install", "repair"):
                    self._set_install(runtime_id, w["id"], "installing", detail=f"{mode} started")
                data = self.hub.request(w["id"], "install_runtime", payload, timeout=timeout)
                data = data if isinstance(data, dict) else {}
                state = str(data.get("state") or ("retired" if mode == "retire" else "ready"))
                self._set_install(runtime_id, w["id"], state, verified=bool(data.get("verified", state == "ready" and mode != "retire")),
                                  fingerprint=data.get("fingerprint"), detail=data.get("detail"))
                return {"worker_id": w["id"], "name": w["name"], "ok": True, "state": state, "data": data}
            except OlcfError as exc:
                if mode in ("install", "repair"):
                    self._set_install(runtime_id, w["id"], "failed", verified=False, detail=exc.message[:500])
                return {"worker_id": w["id"], "name": w["name"], "ok": False, "error": exc.message, "details": exc.details}
            except Exception as exc:  # noqa: BLE001
                log.exception("runtime %s on %s failed", mode, w["name"])
                return {"worker_id": w["id"], "name": w["name"], "ok": False, "error": f"{type(exc).__name__}: {exc}"}
            finally:
                with self._lock:
                    self._inflight.discard(key)

        with ThreadPoolExecutor(max_workers=min(4, len(targets)), thread_name_prefix="olcf-rt-node") as pool:
            futures = [pool.submit(one, w) for w in targets]
            for f in futures:
                results.append(f.result())
                done += 1
                ops.update(oid, progress=int(100 * done / len(futures)), message=f"{mode} {runtime_id}: {done}/{len(futures)} node(s) finished")
        ok = [r for r in results if r["ok"]]
        if mode == "retire" and ok:
            with self.ctx.db.begin() as conn:
                conn.execute(update(C.runtimes).where(C.runtimes.c.id == runtime_id).values(state="retired", updated_at=now_iso()))
        if mode == "rollback" and ok:
            with self.ctx.db.begin() as conn:
                conn.execute(update(C.runtimes).where(C.runtimes.c.id == runtime_id).values(state="available", updated_at=now_iso()))
        summary = {"runtime_id": runtime_id, "mode": mode, "ok": len(ok), "failed": len(results) - len(ok), "nodes": results}
        self.ctx.events.publish(f"runtime.{mode}", {k: summary[k] for k in ("runtime_id", "mode", "ok", "failed")}, subject=runtime_id)
        if len(ok) == len(results):
            ops.update(oid, state="succeeded", progress=100, message=f"{mode} {runtime_id} finished on {len(ok)} node(s)", result=summary)
        else:
            ops.update(oid, state="failed", progress=100, message=f"{mode} {runtime_id} failed on {len(results) - len(ok)} of {len(results)} node(s)", result=summary,
                       error={"code": "runtime_operation_failed", "message": "; ".join(f"{r['name']}: {r.get('error')}" for r in results if not r["ok"])[:1000]})

    def request_install_if_missing(self, runtime_id: str) -> str | None:
        """Auto-install hook used by the scheduler when ``compute.auto_install_runtimes`` is on (system principal). Returns an operation id."""
        from olcf.security.rbac import SYSTEM_PRINCIPAL

        if self.installs(runtime_id) and any(i["state"] in ("ready", "installing") for i in self.installs(runtime_id)):
            return None
        try:
            return self.start_operation(SYSTEM_PRINCIPAL, runtime_id, "install", base_url=self.ctx.settings.base_url)["operation_id"]
        except OlcfError as exc:
            log.warning("auto-install of %s not started: %s", runtime_id, exc.message)
            return None

    def close(self) -> None:
        self._ops.shutdown(wait=False, cancel_futures=True)


def _sortable(v: str) -> tuple[int, Any]:
    try:
        return (1, Version(v))
    except InvalidVersion:
        return (0, v)


# =========================================================================== API
router = APIRouter(tags=["runtimes"])


class RegisterBody(BaseModel):
    """Request body that registers a runtime manifest with the control plane."""
    manifest: dict[str, Any]


class OperationBody(BaseModel):
    """Request body of a runtime operation on nodes (install, repair and the like): which workers or pool, which bundle, and whether to force."""
    worker_ids: list[str] | None = None
    pool: str | None = None
    bundle_id: str | None = None
    force: bool = False


class ResolveBody(BaseModel):
    """Request body of the runtime resolution preview: a runtime request whose resolved runtime id is returned."""
    runtime: dict[str, Any] = Field(default_factory=dict)


def _catalog(ctx: Any) -> RuntimeCatalog:
    return ctx.service("runtimes")


@router.get("")
def list_runtimes(principal: Authed, ctx: Ctx, include_retired: bool = Query(True)) -> dict[str, Any]:
    principal.require("runtime.read")
    items = _catalog(ctx).list_public(include_retired=include_retired)
    return {"items": items, "next_cursor": None}


@router.get("/matrix")
def matrix(principal: Authed, ctx: Ctx) -> dict[str, Any]:
    principal.require("runtime.read")
    return _catalog(ctx).matrix()


@router.post("", status_code=201)
def register(body: RegisterBody, principal: Authed, ctx: Ctx) -> dict[str, Any]:
    return _catalog(ctx).register(principal, body.manifest)


@router.post("/resolve")
def resolve_preview(body: ResolveBody, principal: Authed, ctx: Ctx) -> dict[str, Any]:
    """What a job with this runtime request would run on (no side effects)."""
    principal.require("runtime.read")
    try:
        req = RuntimeRequest.model_validate(body.runtime)
    except Exception as exc:  # noqa: BLE001
        raise ValidationFailed(f"invalid runtime request: {exc}") from exc
    return _catalog(ctx).resolve(req).to_dict()


@router.get("/bundles")
def list_bundles(principal: Authed, ctx: Ctx) -> dict[str, Any]:
    principal.require("runtime.manage")
    with ctx.db.connect() as conn:
        rows = fetch_all(conn, select(C.runtime_bundles).order_by(C.runtime_bundles.c.created_at.desc()))
    return {"items": [RuntimeCatalog.public_bundle(r) for r in rows], "next_cursor": None}


@router.post("/bundles", status_code=201)
def upload_bundle(principal: Authed, ctx: Ctx, file: Annotated[UploadFile, File()], runtime_id: str | None = Query(None)) -> dict[str, Any]:
    return _catalog(ctx).save_bundle(principal, file.filename or "bundle", file.file, runtime_id)


@router.get("/bundles/{bundle_id}/download")
def download_bundle(bundle_id: str, principal: Authed, ctx: Ctx) -> FileResponse:
    # agents fetch bundles with their worker token; administrators with runtime.manage
    if principal.kind != "worker":
        principal.require("runtime.manage")
    cat = _catalog(ctx)
    row = cat.get_bundle(bundle_id)
    return FileResponse(cat.bundle_file(row), media_type="application/octet-stream", filename=row["filename"], headers={"X-Content-SHA256": row["sha256"]})


@router.delete("/bundles/{bundle_id}", status_code=204)
def delete_bundle(bundle_id: str, principal: Authed, ctx: Ctx) -> None:
    principal.require("runtime.manage")
    cat = _catalog(ctx)
    row = cat.get_bundle(bundle_id)
    with ctx.db.begin() as conn:
        conn.execute(C.runtime_bundles.delete().where(C.runtime_bundles.c.id == bundle_id))
    try:
        cat.bundle_file(row).unlink(missing_ok=True)
    except NotFound:
        pass
    ctx.audit.record(principal, "runtime.bundle_deleted", resource_type="runtime_bundle", resource_id=bundle_id)


@router.get("/{runtime_id}")
def get_runtime(runtime_id: str, principal: Authed, ctx: Ctx) -> dict[str, Any]:
    principal.require("runtime.read")
    cat = _catalog(ctx)
    row = cat.get_row(runtime_id)
    with ctx.db.connect() as conn:
        workers = {w["id"]: w for w in fetch_all(conn, select(C.workers.c.id, C.workers.c.name, C.workers.c.runtimes))}
    return cat.public(row, cat.installs(runtime_id), workers, full=True)


def _op(mode: str):  # noqa: ANN202
    def handler(runtime_id: str, request: Request, principal: Authed, ctx: Ctx, body: OperationBody | None = None, idem: IdempotencyKey = None) -> dict[str, Any]:
        body = body or OperationBody()
        base = ctx.settings.server.public_url or f"{request.url.scheme}://{request.headers.get('host', 'localhost')}"
        with Idempotency(ctx, principal, f"POST /v1/runtimes/{runtime_id}/{mode}", idem, body.model_dump()) as ik:
            if ik.replay is not None:
                return ik.replay
            res = _catalog(ctx).start_operation(principal, runtime_id, mode, worker_ids=body.worker_ids, pool=body.pool, bundle_id=body.bundle_id, force=body.force, base_url=base)
            ik.save(202, res)
            return res
    handler.__name__ = f"runtime_{mode}"
    return handler


for _mode in MODES:
    router.add_api_route(f"/{{runtime_id}}/{_mode}", _op(_mode), methods=["POST"], status_code=202, name=f"runtime_{_mode}")


__all__ = ["DELTA_TABLE", "MODES", "OLCF_OWNED_CONF", "RuntimeCatalog", "RuntimeSelection", "conf_allowed", "filter_spark_conf", "router"]
