# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Per-job dependency layers on the node (the consumer side of :mod:`olcf.notebooks.layer`).

A job (or interactive session) whose runtime request names an environment carries ``spec.runtime.environment.layer``: hash-pinned requirements and
the pinned jars of that environment.  Before the runner / kernel starts, :class:`LayerManager`

* builds the layer **once per node and dependency hash** below ``<state>/layers/<key>`` (:func:`olcf.notebooks.layer.ensure_layer`: ``pip install
  --require-hashes --no-deps --target``, flock-serialised, atomically renamed into place - a layer that is already there needs no network at all);
* honours the node's own index policy: ``pip_index_url`` / ``pip_find_links`` (a node-local mirror or wheelhouse) override the index the lock was
  resolved against, ``layer_allowed_indexes`` (when set) refuses any other index, ``layer_offline`` never contacts an index (cache + wheelhouse only);
* fetches the pinned jars (``layer.jars``) into a content-addressed jar cache - Maven artifacts from the node's Maven repositories, uploaded files from
  the control plane - and verifies every sha256 (fail closed, never run with an unverified jar);
* returns the instructions the in-runtime code needs: the layer directory (``sys.path`` / ``PYTHONPATH`` in front of the runtime's site-packages for the
  driver and the notebook kernel) and the jar URIs (merged into ``spark.jars`` by :func:`olcf.runtime_lib.spark.build_spark_conf`), plus the
  dependency fingerprint that ends up in ``result.json`` and the job record.
"""

from __future__ import annotations

import hashlib
import logging
import os
from collections.abc import Callable
from pathlib import Path
from typing import Any

from olcf.notebooks.layer import LayerError, ensure_layer
from olcf.worker.config import AgentConfig
from olcf.worker.protocol import CommandError

log = logging.getLogger("olcf.worker.layers")

#: ``fetch(sha256, dest)`` downloads a control-plane-held environment jar (uploaded ``jars.files``) to ``dest``.
FileFetcher = Callable[[str, Path], None]


def layer_of(spec: dict[str, Any]) -> dict[str, Any] | None:
    """The layer instructions of a job spec: ``runtime.environment.layer``, derived from a bare ``runtime.dependency_lock`` when only that is present."""
    rt = spec.get("runtime") or {}
    env = rt.get("environment") if isinstance(rt.get("environment"), dict) else None
    if env and isinstance(env.get("layer"), dict):
        return env["layer"]
    lock = rt.get("dependency_lock")
    if isinstance(lock, dict) and lock:
        try:
            from olcf.notebooks import envspec  # noqa: PLC0415 - agent installs carry the full package

            return envspec.layer_block(lock)
        except Exception as exc:  # noqa: BLE001
            raise CommandError("layer_invalid", f"the job's dependency_lock cannot be turned into a layer: {exc}",
                               remediation="Publish the environment again (POST /v1/environments/{id}/publish).") from exc
    return None


def _empty(layer: dict[str, Any]) -> bool:
    jars = layer.get("jars") or {}
    return not (layer.get("packages") or jars.get("resolved") or jars.get("files"))


class LayerManager:
    """Builds and caches the dependency layer of a job (pinned Python packages and jars) on top of a runtime, and records it in the job spec."""
    def __init__(self, cfg: AgentConfig, fetch_file: FileFetcher | None = None) -> None:
        self.cfg = cfg
        self.fetch_file = fetch_file

    # ------------------------------------------------------------------ paths
    @property
    def root(self) -> Path:
        return self.cfg.path("layers")

    @property
    def jar_dir(self) -> Path:
        return self.root / "_jars"

    @property
    def wheelhouse(self) -> Path:
        return self.root / "wheelhouse"

    # ------------------------------------------------------------------ policy
    def _pip_block(self, layer: dict[str, Any]) -> dict[str, Any]:
        pip = dict(layer.get("pip") or {})
        cfg = self.cfg
        offline = bool(getattr(cfg, "layer_offline", False))
        allowed = [str(u).rstrip("/") for u in (getattr(cfg, "layer_allowed_indexes", None) or [])]
        if cfg.pip_index_url:  # the node's own mirror replaces the index named in the lock (hashes still pin every artifact)
            pip["index_url"] = cfg.pip_index_url
            pip["no_index"] = False
        if cfg.pip_find_links:
            pip["find_links"] = list(dict.fromkeys([*(pip.get("find_links") or []), *cfg.pip_find_links]))
        if self.wheelhouse.is_dir():
            pip["find_links"] = list(dict.fromkeys([*(pip.get("find_links") or []), str(self.wheelhouse)]))
        if offline:
            pip["no_index"] = True
        if allowed and not pip.get("no_index"):
            used = [u for u in [pip.get("index_url"), *(pip.get("extra_index_urls") or [])] if u]
            bad = [u for u in used if not any(str(u).rstrip("/").startswith(a) for a in allowed)]
            if bad:
                raise CommandError("index_not_allowed", f"this node only allows package indexes {allowed}; the environment uses {bad}",
                                   remediation="Use an allowed index/mirror in the environment, or ask the node administrator to extend layer_allowed_indexes.")
        return pip

    # ------------------------------------------------------------------ jars
    def _jar_path(self, sha: str) -> Path:
        return self.jar_dir / f"{sha}.jar"

    def _maven_jar(self, entry: dict[str, Any]) -> Path:
        sha = str(entry["sha256"]).lower()
        dest = self._jar_path(sha)
        if dest.is_file():
            return dest
        if getattr(self.cfg, "layer_offline", False) and not self.cfg.maven_repositories:
            raise CommandError("jar_unavailable", f"jar {entry.get('coordinate')} is not in this node's jar cache and the node is offline",
                               remediation="Pre-seed <state>/layers/_jars/<sha256>.jar or configure maven_repositories (a mirror).")
        from olcf.runtime.errors import DependencyResolutionError  # noqa: PLC0415
        from olcf.runtime.maven import (  # noqa: PLC0415
            Coordinate,
            Repository,
            Transport,
            default_repositories,
        )

        repos = [Repository.coerce(r) for r in self.cfg.maven_repositories] or default_repositories()
        coord = Coordinate.parse(str(entry["coordinate"]))
        self.jar_dir.mkdir(parents=True, exist_ok=True)
        tmp = dest.with_suffix(f".{os.getpid()}.tmp")
        last: Exception | None = None
        for repo in repos:
            try:
                got, _sha1, _size = Transport(offline=bool(getattr(self.cfg, "layer_offline", False))).download(repo, coord.relpath(), tmp)
            except DependencyResolutionError as exc:
                last = exc
                continue
            except Exception as exc:  # noqa: BLE001 - _NotFound of the transport: try the next repository
                last = exc
                continue
            if got != sha:
                tmp.unlink(missing_ok=True)
                raise CommandError("jar_corrupt", f"jar {entry['coordinate']} from {repo.base} does not match the sha256 pinned in the environment lock",
                                   remediation="Never override: the artifact was changed upstream or tampered with. Re-publish the environment to re-pin it.")
            os.replace(tmp, dest)
            return dest
        tmp.unlink(missing_ok=True)
        raise CommandError("jar_unavailable", f"cannot fetch jar {entry.get('coordinate')}: {last}",
                           remediation="Check network access / the Maven mirror (maven_repositories) of this node, or pre-seed the node's jar cache.")

    def _file_jar(self, entry: dict[str, Any]) -> Path:
        sha = str(entry["sha256"]).lower()
        dest = self._jar_path(sha)
        if dest.is_file():
            return dest
        if self.fetch_file is None:
            raise CommandError("jar_unavailable", f"jar {entry.get('uri')} is held by the control plane but this agent cannot download it",
                               remediation="Run the agent connected to the control plane or pre-seed <state>/layers/_jars/<sha256>.jar.")
        self.jar_dir.mkdir(parents=True, exist_ok=True)
        tmp = dest.with_suffix(f".{os.getpid()}.tmp")
        try:
            self.fetch_file(sha, tmp)
            h = hashlib.sha256()
            with open(tmp, "rb") as fh:
                while chunk := fh.read(1 << 20):
                    h.update(chunk)
            if h.hexdigest() != sha:
                raise CommandError("jar_corrupt", f"jar {entry.get('uri')} does not match its pinned sha256", remediation="Re-publish the environment.")
            os.replace(tmp, dest)
        finally:
            tmp.unlink(missing_ok=True)
        return dest

    # ------------------------------------------------------------------ main entry
    def prepare(self, spec: dict[str, Any], python: str | os.PathLike[str]) -> dict[str, Any] | None:
        """Build / reuse the layer for ``spec`` and return its description, or ``None`` when the job has no environment layer.

        Result keys: ``key`` (layer hash), ``path`` (directory or ``None`` without Python packages), ``jars`` (file URIs), ``lock_hash``, ``environment``
        (item id, name, version) - everything a result.json fingerprint needs.
        """
        layer = layer_of(spec)
        env = (spec.get("runtime") or {}).get("environment") or {}
        if layer is None or _empty(layer):
            if env:
                return {"key": None, "path": None, "jars": [], "lock_hash": env.get("lock_hash"), "environment": _env_ref(env)}
            return None
        path: Path | None = None
        if layer.get("packages"):
            eff = dict(layer)
            eff["pip"] = self._pip_block(layer)
            self.root.mkdir(parents=True, exist_ok=True)
            try:
                path = ensure_layer(eff, self.root, python=str(python), timeout=float(getattr(self.cfg, "layer_timeout_seconds", 1800.0)))
            except LayerError as exc:
                raise CommandError("layer_failed", str(exc).splitlines()[0][:1500] if str(exc) else "dependency layer could not be built",
                                   remediation="Check the pinned packages, the package index / wheelhouse reachable from this node (pip_index_url, pip_find_links, "
                                               "layer_offline) and the logs; an artifact whose hash differs from the lock is always refused.") from exc
        jars = layer.get("jars") or {}
        uris = [self._maven_jar(e).resolve().as_uri() for e in jars.get("resolved") or []]
        uris += [self._file_jar(e).resolve().as_uri() for e in jars.get("files") or []]
        info = {"key": layer.get("key"), "path": str(path) if path else None, "jars": uris, "lock_hash": env.get("lock_hash"), "environment": _env_ref(env)}
        log.info("dependency layer %s ready (%d package(s), %d jar(s))", layer.get("key"), len(layer.get("packages") or []), len(uris))
        return info

    def apply(self, spec: dict[str, Any], python: str | os.PathLike[str]) -> dict[str, Any] | None:
        """:meth:`prepare` and record the result in ``spec`` (``runtime.layer``; jars appended to ``spark.jars``) for the runner / kernel."""
        info = self.prepare(spec, python)
        if info is None:
            return None
        spec.setdefault("runtime", {})["layer"] = info
        sp = spec.get("spark")
        if info["jars"] and isinstance(sp, dict):
            sp["jars"] = list(dict.fromkeys([*(sp.get("jars") or []), *info["jars"]]))
        return info


def _env_ref(env: dict[str, Any]) -> dict[str, Any] | None:
    if not env:
        return None
    return {k: env.get(k) for k in ("item_id", "name", "version", "lock_hash") if env.get(k) is not None}
