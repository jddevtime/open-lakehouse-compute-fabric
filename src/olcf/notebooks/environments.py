# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""``EnvironmentService`` (``ctx.service("environments")``): environment items, immutable publications and the spec handed to the job planner.

Lifecycle
---------
1. The *definition* of an ``environment`` item (:mod:`olcf.notebooks.envspec`) declares the base runtime request, Python requirements, JARs,
   variables and approved Spark conf.  Editing it never changes what running jobs get.
2. ``publish`` resolves the requirements with pip (:mod:`olcf.notebooks.piplock`) and the JARs with the runtime manager's Maven resolver, applies the
   index / native-package policy, and stores an **immutable, content-addressed lock** (``environment_locks``; key = ``lock_hash``).  The item receives an
   append-only ``publications`` entry ``{version, lock_hash, layer_key, ...}``.  Publishing identical inputs again returns the existing publication.
3. ``resolve(item_id | lock)`` returns the ``EnvironmentSpec`` dict CTRL's ``RuntimeCatalog.resolve`` merges into a job: ``runtime`` (request fields),
   ``spark_conf``, ``variables``, ``dependency_lock`` (the full lock) and ``layer`` (the instructions of :mod:`olcf.notebooks.layer`: hash-pinned requirements
   keyed by the dependency hash, which a node installs once with ``pip install --require-hashes --no-deps --target``).

Fail-closed rules: an environment that declares Python / JAR dependencies and has no publication is refused (``Unsupported``) instead of silently running
without them; imported locks are re-validated and re-checked against the index policy; policy can only be tightened by KV settings, never loosened
beyond ``settings.security``.
"""

from __future__ import annotations

import os
import re
import shutil
import tempfile
import threading
from pathlib import Path
from typing import Any

from packaging.utils import canonicalize_name
from sqlalchemy import select

from olcf.core.context import AppContext
from olcf.core.errors import BadRequest, NotFound, PolicyDenied, Unavailable, Unsupported, ValidationFailed
from olcf.core.models import ItemUpdate, RuntimeRequest
from olcf.core.redaction import redact_obj, redact_text
from olcf.core.timeutil import now_iso
from olcf.db.util import fetch_all, fetch_one
from olcf.notebooks import envspec, layer, piplock
from olcf.notebooks import schema as NS
from olcf.security.rbac import Principal

_REF = re.compile(r"^(?P<ref>[^@]+)(?:@(?P<ver>\d{1,6}))?$")


class EnvironmentService:
    """See the module docstring."""

    def __init__(self, ctx: AppContext) -> None:
        self.ctx = ctx
        self._locks: dict[str, threading.Lock] = {}
        self._guard = threading.Lock()
        self._register_settings()

    # ------------------------------------------------------------------ admin settings / policy
    def _register_settings(self) -> None:
        kv = self.ctx.maybe_service("settings")
        if kv is None:
            return
        reg = kv.register

        def _bool(v: Any) -> Any:
            if v is None or isinstance(v, bool):
                return v
            raise BadRequest("must be true, false or null")

        def _strlist(v: Any) -> list[str]:
            if not isinstance(v, list) or not all(isinstance(x, str) and len(x) < 500 for x in v):
                raise BadRequest("must be a list of strings")
            return v

        def _pip(v: Any) -> str:
            if v not in envspec.PIP_POLICIES:
                raise BadRequest("must be block, declared or allow")
            return v

        reg("environments.allow_public_package_install", description="Set to false to additionally deny the public Internet for package installs (can only tighten settings.security)", default=None, validator=_bool)
        reg("environments.allowed_package_indexes", description="Allowed package index URL prefixes (used when settings.security.allowed_package_indexes is empty)", default=[], validator=_strlist)
        reg("environments.wheelhouses", description="Approved local wheelhouse directories usable as python.index.find_links", default=[], validator=_strlist)
        reg("environments.maven_repositories", description="Maven repositories used to pin environment JARs (default: Maven Central / OLCF_MAVEN_REPOSITORIES)", default=[], validator=_strlist)
        reg("environments.allow_sdist", description="Allow environments to opt in to source distributions (native compilation on the node)", default=False, validator=lambda v: bool(v))
        reg("environments.pip_policy_default", description="Default notebook-time %pip policy: block | declared | allow", default="declared", validator=_pip)
        reg("environments.pip_policy_max", description="Most permissive %pip policy any notebook/environment may select", default="allow", validator=_pip)

    def _kv(self, key: str, default: Any = None) -> Any:
        kv = self.ctx.maybe_service("settings")
        if kv is None:
            return default
        v = kv.get(key)
        return default if v is None else v

    def wheelhouse_dir(self) -> Path:
        d = self.ctx.settings.path("wheelhouse")
        d.mkdir(parents=True, exist_ok=True)
        return d

    def policy(self) -> piplock.IndexPolicy:
        """Effective package-source policy: ``settings.security`` is the ceiling, KV settings can only tighten it (or fill an empty allow list)."""
        sec = self.ctx.settings.security
        kv_public = self._kv("environments.allow_public_package_install", None)
        allow_public = bool(sec.allow_public_package_install) and (kv_public is not False)
        allowed = list(sec.allowed_package_indexes) or list(self._kv("environments.allowed_package_indexes", []) or [])
        wheelhouses = [str(self.wheelhouse_dir()), *[w for w in (self._kv("environments.wheelhouses", []) or [])]]
        return piplock.IndexPolicy(allow_public=allow_public, allowed_indexes=allowed, wheelhouses=wheelhouses, allow_sdist=bool(self._kv("environments.allow_sdist", False)))

    # ------------------------------------------------------------------ lock store
    def put_lock(self, lock: dict[str, Any], *, workspace_id: str | None, user_id: str | None, spec_hash: str | None = None) -> str:
        h = lock.get("lock_hash") or envspec.compute_lock_hash(lock)
        if h != envspec.compute_lock_hash(lock):
            raise ValidationFailed("lock_hash does not match the lock content")
        body = {**lock, "lock_hash": h}
        with self.ctx.db.begin() as conn:
            if conn.execute(select(NS.environment_locks.c.lock_hash).where(NS.environment_locks.c.lock_hash == h)).first() is None:
                conn.execute(NS.environment_locks.insert().values(lock_hash=h, lock=body, layer_key=envspec.lock_layer_key(body), spec_hash=spec_hash, workspace_id=workspace_id,
                                                                  created_at=now_iso(), created_by=user_id))
        return h

    def get_lock(self, lock_hash: str) -> dict[str, Any]:
        with self.ctx.db.connect() as conn:
            row = fetch_one(conn, select(NS.environment_locks).where(NS.environment_locks.c.lock_hash == lock_hash))
        if not row:
            raise NotFound(f"environment lock {lock_hash[:24]} not found", remediation="publish the environment again")
        lock = row["lock"]
        if envspec.compute_lock_hash(lock) != lock_hash:  # tamper evidence for the content-addressed store
            raise Unavailable("the stored lock does not match its hash", remediation="restore the database from a backup")
        return lock

    def has_lock(self, lock_hash: str) -> bool:
        with self.ctx.db.connect() as conn:
            return conn.execute(select(NS.environment_locks.c.lock_hash).where(NS.environment_locks.c.lock_hash == lock_hash)).first() is not None

    # ------------------------------------------------------------------ item hook: validate
    def validate(self, defn: Any) -> dict[str, Any]:
        """``validate`` hook of the ``environment`` item type."""
        d = envspec.normalise_definition(defn)
        for pb in d.get("publications") or []:
            if not self.has_lock(pb["lock_hash"]):
                raise ValidationFailed(f"publication v{pb['version']} refers to an unknown lock", remediation="publications are created by POST /v1/environments/{id}/publish; "
                                                                                                         "when importing, include the lock files of the export")
        return d

    # ------------------------------------------------------------------ item access
    def item(self, principal: Principal, item_id: str, perm: str) -> dict[str, Any]:
        row = self.ctx.service("items").get(item_id)
        if row["type"] != "environment":
            raise NotFound(f"environment {item_id} not found")
        principal.require(perm, workspace_id=row["workspace_id"], item_id=row["id"])
        return row

    def _item_lock(self, item_id: str) -> threading.Lock:
        with self._guard:
            return self._locks.setdefault(item_id, threading.Lock())

    # ------------------------------------------------------------------ runtime context
    def runtime_context(self, runtime: dict[str, Any], workspace_id: str | None) -> dict[str, Any]:
        """Resolve the base runtime (when the runtimes service exists): python series, the packages and jars it already provides."""
        out: dict[str, Any] = {"series": envspec.host_series(), "provided": {}, "provided_jars": [], "runtime_id": runtime.get("runtime_id"), "warnings": [], "manifest": None}
        svc = self.ctx.maybe_service("runtimes")
        if svc is None:
            out["warnings"].append("no runtime catalog is available; resolved for the control plane's Python and without runtime constraints")
            return out
        try:
            sel = svc.resolve(RuntimeRequest.model_validate({k: v for k, v in runtime.items() if k in ("family", "runtime_id", "spark", "python", "java", "scala")}), workspace_id=workspace_id)
        except Exception as exc:  # noqa: BLE001 - an unresolvable runtime is a validation problem for the author
            raise ValidationFailed(f"the environment's base runtime cannot be resolved: {getattr(exc, 'message', str(exc))}", remediation="fix environment.runtime or register a matching runtime") from exc
        m = sel.manifest or {}
        ver = str((m.get("python") or {}).get("version") or "")
        mm = re.match(r"^(3\.\d{1,2})", ver)
        out.update(series=mm.group(1) if mm else envspec.host_series(), runtime_id=sel.runtime_id, family=sel.family, manifest=m,
                   provided={canonicalize_name(p["name"]): p["version"] for p in (m.get("packages") or []) if not p.get("marker") or "linux" in p["marker"] or "platform" not in p["marker"]},
                   provided_jars=list(m.get("provided_jars") or []))
        if not mm:
            out["warnings"].append(f"runtime {sel.runtime_id} does not declare a Python version; used the control plane's {out['series']}")
        return out

    # ------------------------------------------------------------------ connections (private package indexes)
    def _index_credentials(self, principal: Principal | None, workspace_id: str, index_def: dict[str, Any]) -> tuple[piplock.IndexSpec, str | None]:
        spec = piplock.IndexSpec.from_definition(index_def)
        ref = index_def.get("connection")
        if not ref:
            return spec, None
        conns = self.ctx.service("connections")
        con = conns.resolve(ref, workspace_id)
        if con.get("workspace_id") not in (None, workspace_id):
            raise NotFound(f"connection '{ref}' not found")
        if principal is not None:
            principal.require("connection.use", workspace_id=workspace_id, connection_id=con["id"])
        cfg = con.get("config") or {}
        url = cfg.get("url") or cfg.get("index_url")
        if url and not spec.index_url:
            spec.index_url = envspec.check_index_url(url, "connection url")
        user = cfg.get("username") or cfg.get("user")
        secret = self.ctx.secrets.resolve(con["secret_id"]) if con.get("secret_id") else None
        password = secret.get("password") or secret.get("token") if isinstance(secret, dict) else secret
        if not spec.index_url:
            raise ValidationFailed(f"connection '{ref}' has no index url", remediation="set config.url on the connection or python.index.index_url")
        _, netrc = piplock.apply_credentials(spec.index_url, user, password if isinstance(password, str) else None)
        return spec, netrc

    # ------------------------------------------------------------------ publish
    def publish(self, principal: Principal, row: dict[str, Any], *, comment: str = "", dry_run: bool = False, force: bool = False, verify: bool = False) -> dict[str, Any]:
        """Resolve and pin the environment; returns ``{publication, lock, created, warnings}``.  ``dry_run`` resolves but stores nothing."""
        with self._item_lock(row["id"]):
            row = self.ctx.service("items").get(row["id"])  # the freshest definition + etag under the lock
            defn = envspec.normalise_definition(row["definition"])
            ws = row["workspace_id"]
            py = defn.get("python") or {}
            reqs: list[str] = list(py.get("requirements") or [])
            jars = defn.get("jars") or {}
            if not reqs and not jars.get("coordinates") and not jars.get("files") and not py.get("index"):
                pass  # an environment with only runtime / variables / spark_conf can still be published (empty dependency lock)
            platforms = list(py.get("platforms") or [envspec.host_platform()])
            allow_sdist = bool(py.get("allow_sdist"))
            pol = self.policy()
            if allow_sdist and not pol.allow_sdist:
                raise PolicyDenied("this deployment does not allow source distributions (native compilation) in environments",
                                   remediation="publish binary wheels, or ask an administrator to enable environments.allow_sdist")
            spec, netrc = self._index_credentials(principal, ws, py.get("index") or {})
            if reqs:
                piplock.check_index_policy(spec, pol)
            rt = self.runtime_context(defn.get("runtime") or {}, ws)
            warnings = list(rt["warnings"])
            override = set(py.get("override_runtime_packages") or [])
            packages: list[dict[str, Any]] = []
            if reqs:
                provided = {n: v for n, v in rt["provided"].items() if n not in override}
                try:
                    res = piplock.resolve(reqs, index=spec, series=rt["series"], platforms=platforms, allow_sdist=allow_sdist, provided=provided, netrc=netrc, timeout=900.0)
                except piplock.PipMissing as exc:
                    raise ValidationFailed(f"cannot publish the environment: {exc}", remediation=exc.remediation) from exc
                except piplock.PipError as exc:
                    raise ValidationFailed(f"cannot resolve the environment: {redact_text(str(exc))}") from exc
                for p in res.packages:
                    rv = rt["provided"].get(p["name"])
                    if rv and not p.get("provided_by_runtime"):
                        if rv == p["version"]:
                            p = {**p, "provided_by_runtime": True, "hashes": [], "artifacts": []}
                        elif p["name"] in override:
                            p = {**p, "overrides_runtime": rv}
                        else:
                            raise ValidationFailed(f"'{p['name']}' resolves to {p['version']} but the runtime {rt['runtime_id']} provides {rv}",
                                                   remediation=f"relax the requirement to match the runtime or list '{p['name']}' in python.override_runtime_packages "
                                                               "(the layer then shadows the runtime's copy on the driver only)")
                    packages.append(p)
            jar_block = self._resolve_jars(principal, ws, jars, rt)
            lock: dict[str, Any] = {
                "schema": envspec.LOCK_SCHEMA,
                "runtime": {k: v for k, v in {"family": rt.get("family"), "runtime_id": rt.get("runtime_id")}.items() if v},
                "python": {"series": rt["series"], "abi": envspec.abi_tag(rt["series"]), "platforms": platforms, "allow_sdist": allow_sdist, "requirements": reqs, "packages": packages},
                "index": spec.lock_block() if reqs else {},
                "jars": jar_block,
                "variables": defn.get("variables") or {},
                "spark_conf": defn.get("spark_conf") or {},
                "pip_policy": defn.get("pip_policy"),
                "spec_hash": envspec.spec_hash(defn),
            }
            lock["lock_hash"] = envspec.compute_lock_hash(lock)
            envspec.validate_lock(lock)
            lock["meta"] = {"resolved_at": now_iso(), "resolved_by": principal.name, "pip": _pip_version(), "host": envspec.host_platform(), "notes": warnings}
            pubs = list(defn.get("publications") or [])
            if pubs and pubs[-1]["lock_hash"] == lock["lock_hash"] and not force:
                return {"publication": pubs[-1], "lock": self.public_lock(lock), "created": False, "warnings": warnings, "dry_run": dry_run}
            number = len(pubs) + 1
            publication = {"version": number, "lock_hash": lock["lock_hash"], "layer_key": envspec.lock_layer_key(lock), "spec_hash": lock["spec_hash"],
                           "published_at": now_iso(), "published_by": principal.name, "comment": comment[:500]}
            if dry_run:
                return {"publication": publication, "lock": self.public_lock(lock), "created": False, "warnings": warnings, "dry_run": True}
            if verify:
                self.verify_lock(lock)
            self.put_lock(lock, workspace_id=ws, user_id=principal.id, spec_hash=lock["spec_hash"])
            new_def = {**defn, "publications": [*pubs, publication]}
            self.ctx.service("items").update(principal, row["id"], ItemUpdate(definition=new_def, comment=f"published v{number}"), row["etag"])
            self.ctx.audit.record(principal, "environment.published", resource_type="environment", resource_id=row["id"], workspace_id=ws,
                                  details={"version": number, "lock_hash": lock["lock_hash"], "packages": len(packages), "jars": len(jar_block.get("resolved", []))})
            self.ctx.events.publish("environment.published", {"name": row["name"], "version": number, "lock_hash": lock["lock_hash"]}, workspace_id=ws, subject=row["id"])
            return {"publication": publication, "lock": self.public_lock(lock), "created": True, "warnings": warnings, "dry_run": False}

    @staticmethod
    def public_lock(lock: dict[str, Any]) -> dict[str, Any]:
        return redact_obj(lock)

    def _resolve_jars(self, principal: Principal | None, ws: str, jars: dict[str, Any], rt: dict[str, Any]) -> dict[str, Any]:
        out: dict[str, Any] = {"coordinates": list(jars.get("coordinates") or []), "resolved": [], "files": []}
        if out["coordinates"]:
            from olcf.runtime.errors import DependencyResolutionError
            from olcf.runtime.maven import MavenResolver

            repos = list(self._kv("environments.maven_repositories", []) or []) or None
            cache = self.ctx.settings.path("tmp", "maven-cache")
            work = Path(tempfile.mkdtemp(prefix="olcf-envjars-", dir=str(self.ctx.settings.path("tmp"))))
            try:
                resolver = MavenResolver(repos, cache_dir=cache, provided_patterns=rt.get("provided_jars") or [])
                try:
                    entries = resolver.resolve_and_fetch(out["coordinates"], work)
                except DependencyResolutionError as exc:
                    raise ValidationFailed(f"cannot resolve the environment's JARs: {redact_text(str(exc))}", remediation="check the Maven coordinates and repositories") from exc
                except Exception as exc:  # noqa: BLE001 - checksum errors etc. from the resolver carry their own message
                    raise ValidationFailed(f"cannot resolve the environment's JARs: {redact_text(str(exc))}") from exc
                out["resolved"] = [{"coordinate": e["coordinate"], "sha256": e["sha256"], "size": e["size"]} for e in entries]
            finally:
                shutil.rmtree(work, ignore_errors=True)
        for f in jars.get("files") or []:
            out["files"].append(self._pin_jar_file(principal, ws, f))
        return out

    def _pin_jar_file(self, principal: Principal | None, ws: str, f: dict[str, Any]) -> dict[str, Any]:
        """Read a JAR from a lakehouse (``Files/...``, ``abfss://``, ``olcf://``), pin its sha256 and keep a content-addressed copy."""
        import hashlib

        st = self.ctx.maybe_service("storage")
        if st is None:
            raise Unavailable("the storage service is required for jars.files")
        loc = st.resolve(f["uri"], None, workspace_id=ws)
        lh = st.get_lakehouse(loc.lakehouse_id)
        if principal is not None:
            principal.require("lakehouse.read", workspace_id=lh.workspace_id, item_id=lh.id)
        data = bytes(st.read_bytes(lh, f"{loc.area}/{loc.relative_path}".strip("/")))
        if len(data) > 512 * 1024 * 1024:
            raise ValidationFailed("a JAR file is larger than 512 MiB")
        if data[:2] != b"PK":
            raise ValidationFailed(f"{f['uri']} is not a JAR (zip) file")
        sha = hashlib.sha256(data).hexdigest()
        if f.get("sha256") and f["sha256"] != sha:
            raise ValidationFailed(f"{f['uri']} does not match the sha256 in the environment definition", details={"expected": f["sha256"], "actual": sha})
        d = self.ctx.settings.path("artifacts", "env-jars")
        d.mkdir(parents=True, exist_ok=True)
        dest = d / f"{sha}.jar"
        if not dest.exists():
            tmp = dest.with_suffix(".tmp")
            tmp.write_bytes(data)
            os.replace(tmp, dest)
        return {"uri": f["uri"], "sha256": sha, "size": len(data)}

    def env_jar_path(self, sha256: str) -> Path | None:
        """The content-addressed copy of an uploaded environment jar (``jars.files``), or ``None``."""
        p = self.ctx.settings.path("artifacts", "env-jars") / f"{sha256}.jar"
        return p if p.is_file() else None

    # ------------------------------------------------------------------ verify (build the layer on this host)
    def verify_lock(self, lock: dict[str, Any], *, cache_dir: Path | None = None) -> dict[str, Any]:
        """Install the lock's layer on the control-plane host exactly as a node would (``--require-hashes --no-deps``); proves it is installable."""
        block = envspec.layer_block(lock)
        cache = cache_dir or self.ctx.settings.path("tmp", "layers")
        env = None
        try:
            d = layer.ensure_layer(block, cache, timeout=900.0, env=env)
        except layer.LayerError as exc:
            raise ValidationFailed(f"the lock cannot be installed: {redact_text(str(exc))}") from exc
        names = sorted(p.name for p in Path(d).iterdir() if p.name.endswith(".dist-info"))
        return {"key": block["key"], "path": str(d), "dist_info": names, "packages": len(block["packages"])}

    # ------------------------------------------------------------------ resolve (consumed by CTRL)
    def _split_ref(self, ref: str) -> tuple[str, int | None]:
        m = _REF.match(ref or "")
        if not m:
            raise BadRequest(f"invalid environment reference '{str(ref)[:80]}'")
        return m.group("ref"), int(m.group("ver")) if m.group("ver") else None

    def resolve(self, ref: str | None = None, *, lock: dict[str, Any] | None = None, version: int | None = None, workspace_id: str | None = None) -> dict[str, Any]:
        """``EnvironmentSpec``: ``resolve("itm_...[@version]")`` or ``resolve(lock={...})``.

        ``workspace_id`` (when given) pins the lookup to that workspace; CTRL's runtime catalog calls this with an item id only, so the callers that know the
        workspace (notebook / job-definition runs) verify it before the id ever reaches the controller.
        """
        if lock is not None:
            envspec.validate_lock(lock)
            pol = self.policy()
            envspec.check_policy_for_lock(lock, allow_public=pol.allow_public, allowed=pol.allowed_indexes)
            self.put_lock(lock, workspace_id=workspace_id, user_id=None)
            return self._spec_from_lock(lock, item=None, version=None)
        if not ref:
            raise BadRequest("an environment id or a lock is required")
        base, ref_ver = self._split_ref(ref)
        items = self.ctx.service("items")
        row = items.get(base) if base.startswith("itm_") else (items.resolve(workspace_id, base, "environment") if workspace_id else None)
        if row is None:
            raise NotFound(f"environment '{base}' not found", remediation="reference environments by id, or by name together with a workspace")
        if row["type"] != "environment":
            raise NotFound(f"item {row['id']} is not an environment")
        if workspace_id and row["workspace_id"] != workspace_id:
            raise NotFound(f"environment {row['id']} not found in this workspace")
        defn = envspec.normalise_definition(row["definition"])
        want = version or ref_ver
        pubs = defn.get("publications") or []
        py = defn.get("python") or {}
        jars = defn.get("jars") or {}
        needs_lock = bool(py.get("requirements") or jars.get("coordinates") or jars.get("files"))
        if want is not None:
            pub = next((p for p in pubs if p["version"] == want), None)
            if pub is None:
                raise NotFound(f"environment '{row['name']}' has no published version {want}", details={"published": [p["version"] for p in pubs]})
        elif pubs:
            pub = pubs[-1]
        else:
            pub = None
        if pub is None and needs_lock:
            raise Unsupported(f"environment '{row['name']}' declares dependencies but has not been published",
                              remediation=f"publish it first: POST /v1/environments/{row['id']}/publish", details={"item_id": row["id"]})
        if pub is not None:
            lk = self.get_lock(pub["lock_hash"])
            pol = self.policy()
            envspec.check_policy_for_lock(lk, allow_public=pol.allow_public, allowed=pol.allowed_indexes)
            return self._spec_from_lock(lk, item=row, version=pub["version"], definition=defn)
        spec = {"item_id": row["id"], "name": row["name"], "workspace_id": row["workspace_id"], "version": None, "published": False, "lock_hash": None,
                "runtime": defn.get("runtime") or {}, "spark_conf": defn.get("spark_conf") or {}, "variables": defn.get("variables") or {}, "requirements": [],
                "pip_policy": defn.get("pip_policy"), "dependency_lock": None, "layer": None}
        return spec

    def _spec_from_lock(self, lock: dict[str, Any], *, item: dict[str, Any] | None, version: int | None, definition: dict[str, Any] | None = None) -> dict[str, Any]:
        defn = definition or {}
        return {
            "item_id": item["id"] if item else None, "name": item["name"] if item else None, "workspace_id": item["workspace_id"] if item else None,
            "version": version, "published": True, "lock_hash": lock["lock_hash"],
            "runtime": {**(defn.get("runtime") or {}), **({} if (defn.get("runtime") or {}) else lock.get("runtime") or {})},
            "spark_conf": dict(lock.get("spark_conf") or {}), "variables": dict(lock.get("variables") or {}),
            "requirements": list((lock.get("python") or {}).get("requirements") or []), "pip_policy": lock.get("pip_policy"),
            "dependency_lock": {k: v for k, v in lock.items() if k != "meta"}, "layer": envspec.layer_block(lock),
        }

    # ------------------------------------------------------------------ listings / diff
    def publications(self, row: dict[str, Any]) -> list[dict[str, Any]]:
        return list(reversed(envspec.normalise_definition(row["definition"]).get("publications") or []))

    def publication(self, row: dict[str, Any], version: int) -> dict[str, Any]:
        pub = next((p for p in envspec.normalise_definition(row["definition"]).get("publications") or [] if p["version"] == version), None)
        if pub is None:
            raise NotFound(f"environment '{row['name']}' has no published version {version}")
        return pub

    def diff(self, row: dict[str, Any], a: int, b: int) -> dict[str, Any]:
        la, lb = self.get_lock(self.publication(row, a)["lock_hash"]), self.get_lock(self.publication(row, b)["lock_hash"])
        pa = {p["name"]: p["version"] for p in la["python"]["packages"]}
        pb = {p["name"]: p["version"] for p in lb["python"]["packages"]}
        ja = {j["coordinate"] for j in (la.get("jars") or {}).get("resolved", [])}
        jb = {j["coordinate"] for j in (lb.get("jars") or {}).get("resolved", [])}
        return {"a": a, "b": b, "packages": {"added": sorted(set(pb) - set(pa)), "removed": sorted(set(pa) - set(pb)),
                                              "changed": [{"name": n, "from": pa[n], "to": pb[n]} for n in sorted(set(pa) & set(pb)) if pa[n] != pb[n]]},
                "jars": {"added": sorted(jb - ja), "removed": sorted(ja - jb)},
                "spark_conf_changed": la.get("spark_conf") != lb.get("spark_conf"), "variables_changed": la.get("variables") != lb.get("variables"),
                "identical": la["lock_hash"] == lb["lock_hash"]}

    # ------------------------------------------------------------------ %pip -> requirements
    def requirements_from_notebook(self, nb: dict[str, Any]) -> dict[str, Any]:
        """Convert a notebook's ``%pip install`` lines into environment requirements (plain names only; URLs / paths are reported, never accepted)."""
        nbs = self.ctx.service("notebooks")
        found, rejected = [], []
        for c in nbs.pip_commands(nb):
            if c.get("kind") != "install":
                continue
            for r in c["requirements"]:
                try:
                    found.append(str(envspec.parse_requirement(r)))
                except ValidationFailed as exc:
                    rejected.append({"requirement": r[:80], "reason": exc.message, "cell_id": c["cell_id"], "line": c["line"]})
            for d in c.get("direct") or []:
                rejected.append({"requirement": d[:80], "reason": "direct URLs and paths are not allowed in environments", "cell_id": c["cell_id"], "line": c["line"]})
        return {"requirements": list(dict.fromkeys(found)), "rejected": rejected}

    # ------------------------------------------------------------------ exports
    def export_files(self, row: dict[str, Any]) -> dict[str, bytes]:
        """Files written next to ``item.json`` in a workspace export / git: one lock file per publication, named by version."""
        files: dict[str, bytes] = {}
        for p in envspec.normalise_definition(row["definition"]).get("publications") or []:
            lk = self.get_lock(p["lock_hash"])
            files[f"locks/v{p['version']}.lock.json"] = envspec.to_json({k: v for k, v in lk.items() if k != "meta"}).encode()
        return files

    def import_definition(self, meta: dict[str, Any], files: dict[str, bytes], *, workspace_id: str | None = None) -> dict[str, Any]:
        """Re-create an exported environment: the locks are re-validated, re-checked against policy and stored before the item exists."""
        import json

        defn = dict(meta.get("definition") or {})
        pubs = list(defn.get("publications") or [])
        for p in pubs:
            raw = files.get(f"locks/v{p['version']}.lock.json")
            if raw is None:
                raise ValidationFailed(f"the export has no lock file for publication v{p['version']}")
            try:
                lk = json.loads(raw)
            except ValueError as exc:
                raise ValidationFailed(f"lock file of v{p['version']} is not valid JSON") from exc
            envspec.validate_lock(lk)
            if lk.get("lock_hash") != p["lock_hash"]:
                raise ValidationFailed(f"lock file of v{p['version']} does not match its publication hash")
            pol = self.policy()
            envspec.check_policy_for_lock(lk, allow_public=pol.allow_public, allowed=pol.allowed_indexes)
            self.put_lock(lk, workspace_id=workspace_id, user_id=None)
        return defn

    def health(self) -> dict[str, Any]:
        with self.ctx.db.connect() as conn:
            n = len(fetch_all(conn, select(NS.environment_locks.c.lock_hash).limit(1000)))
        try:
            import subprocess
            import sys

            out = subprocess.run([sys.executable, "-m", "pip", "--version"], capture_output=True, text=True, timeout=30, check=False)  # noqa: S603
            ok = out.returncode == 0
        except Exception:  # noqa: BLE001
            ok = False
        return {"pip_available": ok, "locks": n, "policy": self.policy().to_dict()}


def _pip_version() -> str:
    try:
        import pip

        return pip.__version__
    except Exception:  # noqa: BLE001
        return "unknown"


__all__ = ["EnvironmentService"]
