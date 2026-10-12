# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""A small, strict Maven artifact resolver (stdlib only).

Why not ``spark.jars.packages`` / Ivy at job time?  Spec section 21 and 26 require that runtime installs are
pinned, reproducible, offline-capable and never fetch "latest". This module therefore resolves a list of
coordinates *once, at install time* into plain jar files whose sha256 is written to ``lock.json`` and
re-verified every time the runtime starts.

What is supported (enough for ``delta-spark``, ``hadoop-azure``, ``hadoop-aws`` + the AWS SDK bundle, Iceberg):

* repositories: any number of ``https://`` / ``http://`` / ``file://`` Maven-layout repositories tried in order
  (Maven Central by default; corporate mirrors / proxies / an offline bundle repo are just more entries);
  optional HTTP basic auth per repository; proxies and custom CAs come from the standard environment variables.
* POM processing: parent chains, property interpolation (``${...}``, ``project.*``), ``dependencyManagement``
  including ``import``-scope BOMs, inherited ``<dependencies>``, exclusions (``g:a``, ``g:*``, ``*:*``), ``optional``,
  scopes (only ``compile`` and ``runtime`` end up on the class path), ``type=pom`` dependencies (traversed, no jar).
* mediation: nearest-wins breadth first, ties broken by declaration order (Maven's default).
* "provided" filtering: anything already shipped by the Spark distribution (matched by artifactId against the
  jars in ``pyspark/jars`` and by ``group:artifact`` glob patterns from the manifest) is neither downloaded nor traversed.
* integrity: every POM and jar must match its published ``.sha256`` or ``.sha1`` (fail closed when neither
  exists or the digests disagree); when a previous lock is supplied, the sha256 must equal the locked one.

Not supported (rejected loudly, never guessed): version ranges other than ``[x]``, ``LATEST``/``RELEASE``,
SNAPSHOT metadata resolution, ``system`` scope, profiles/activation.
"""

from __future__ import annotations

import base64
import fnmatch
import hashlib
import logging
import os
import re
import shutil
import tempfile
import time
import urllib.error
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET  # noqa: S405 - POMs are checksum-verified before parsing
from collections import deque
from collections.abc import Callable, Iterable
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from olcf.runtime.errors import ChecksumMismatch, DependencyResolutionError

log = logging.getLogger("olcf.runtime.maven")

MAVEN_CENTRAL = "https://repo1.maven.org/maven2"
REPOS_ENV = "OLCF_MAVEN_REPOSITORIES"
_USER_AGENT = "olcf-runtime-manager/0.1 (+https://olcf.dev)"
_PROP_RE = re.compile(r"\$\{([^}]+)\}")
_MAX_POM_BYTES = 8 * 1024 * 1024


# --------------------------------------------------------------------------------------------- coordinates
@dataclass(frozen=True, order=True)
class Coordinate:
    """``group:artifact[:packaging[:classifier]]:version``."""

    group: str
    artifact: str
    version: str
    packaging: str = "jar"
    classifier: str = ""

    @classmethod
    def parse(cls, text: str) -> Coordinate:
        parts = text.strip().split(":")
        if len(parts) == 3:
            g, a, v = parts
            return cls(g, a, v)
        if len(parts) == 4:
            g, a, p, v = parts
            return cls(g, a, v, packaging=p)
        if len(parts) == 5:
            g, a, p, c, v = parts
            return cls(g, a, v, packaging=p, classifier=c)
        raise DependencyResolutionError(f"invalid Maven coordinate {text!r}",
                                        remediation="Use group:artifact:version (optionally group:artifact:packaging[:classifier]:version).")

    @property
    def ga(self) -> str:
        return f"{self.group}:{self.artifact}"

    @property
    def key(self) -> tuple[str, str, str, str]:
        return (self.group, self.artifact, self.packaging, self.classifier)

    def relpath(self, ext: str | None = None) -> str:
        e = ext or ("jar" if self.packaging in ("jar", "bundle", "test-jar", "maven-plugin", "ejb") else self.packaging)
        cls_part = f"-{self.classifier}" if self.classifier else ""
        return f"{self.group.replace('.', '/')}/{self.artifact}/{self.version}/{self.artifact}-{self.version}{cls_part}.{e}"

    def pom_relpath(self) -> str:
        return f"{self.group.replace('.', '/')}/{self.artifact}/{self.version}/{self.artifact}-{self.version}.pom"

    def __str__(self) -> str:
        mid = ""
        if self.packaging != "jar" or self.classifier:
            mid = f":{self.packaging}" + (f":{self.classifier}" if self.classifier else "")
        return f"{self.group}:{self.artifact}{mid}:{self.version}"


# --------------------------------------------------------------------------------------------- repositories
@dataclass(frozen=True)
class Repository:
    """One Maven-layout repository."""

    url: str
    name: str = ""
    username: str | None = None
    password: str | None = None

    @property
    def base(self) -> str:
        return self.url.rstrip("/")

    @classmethod
    def coerce(cls, value: str | dict[str, Any] | Repository) -> Repository:
        if isinstance(value, Repository):
            return value
        if isinstance(value, str):
            return cls(url=value)
        # credentials may be given inline or as names of environment variables (preferred: never write secrets in config)
        user = value.get("username") or (os.environ.get(value["username_env"]) if value.get("username_env") else None)
        pw = value.get("password") or (os.environ.get(value["password_env"]) if value.get("password_env") else None)
        return cls(url=value["url"], name=value.get("name", ""), username=user, password=pw)


def default_repositories() -> list[Repository]:
    """``$OLCF_MAVEN_REPOSITORIES`` (comma separated URLs) or Maven Central."""
    env = os.environ.get(REPOS_ENV)
    if env:
        return [Repository(u.strip()) for u in env.split(",") if u.strip()]
    return [Repository(MAVEN_CENTRAL, "central")]


# --------------------------------------------------------------------------------------------- transport
class _NotFound(Exception):
    pass


class Transport:
    """Fetches files from repositories (http/https/file) with retries; ``offline`` refuses network access."""

    def __init__(self, *, timeout: float = 60.0, retries: int = 3, offline: bool = False) -> None:
        self.timeout, self.retries, self.offline = timeout, retries, offline

    def _open(self, repo: Repository, rel: str):
        url = f"{repo.base}/{urllib.parse.quote(rel)}"
        if url.startswith("file://"):
            path = Path(urllib.request.url2pathname(urllib.parse.urlparse(url).path))
            if not path.is_file():
                raise _NotFound(url)
            return open(path, "rb")  # noqa: SIM115 - closed by caller
        if self.offline:
            raise _NotFound(f"{url} (offline mode: network repositories are not consulted)")
        req = urllib.request.Request(url, headers={"User-Agent": _USER_AGENT})  # noqa: S310
        if repo.username is not None:
            tok = base64.b64encode(f"{repo.username}:{repo.password or ''}".encode()).decode()
            req.add_header("Authorization", f"Basic {tok}")
        last: Exception | None = None
        for attempt in range(self.retries):
            try:
                return urllib.request.urlopen(req, timeout=self.timeout)  # noqa: S310 - scheme restricted by Repository URLs
            except urllib.error.HTTPError as exc:
                if exc.code == 404:
                    raise _NotFound(url) from exc
                if exc.code in (401, 403):
                    raise DependencyResolutionError(
                        f"repository {repo.base} refused access ({exc.code}) for {rel}",
                        remediation="Check the repository credentials / proxy policy, or use a mirror via OLCF_MAVEN_REPOSITORIES.") from exc
                last = exc
            except (urllib.error.URLError, TimeoutError, ConnectionError) as exc:
                last = exc
            time.sleep(0.4 * (2**attempt))
        raise DependencyResolutionError(
            f"cannot reach Maven repository {repo.base}: {last}",
            remediation="Check network/proxy settings, point OLCF_MAVEN_REPOSITORIES at a reachable mirror, or install from an offline bundle.")

    def get_bytes(self, repo: Repository, rel: str, *, limit: int = _MAX_POM_BYTES) -> bytes:
        with self._open(repo, rel) as fh:
            data = fh.read(limit + 1)
        if len(data) > limit:
            raise DependencyResolutionError(f"{rel} from {repo.base} is unexpectedly large (> {limit} bytes)")
        return data

    def download(self, repo: Repository, rel: str, dest: Path) -> tuple[str, str, int]:
        """Stream ``rel`` into ``dest``; return (sha256, sha1, size)."""
        h256, h1, size = hashlib.sha256(), hashlib.sha1(usedforsecurity=False), 0  # noqa: S324 - Maven publishes SHA-1
        with self._open(repo, rel) as fh, open(dest, "wb") as out:
            while chunk := fh.read(1024 * 1024):
                out.write(chunk)
                h256.update(chunk)
                h1.update(chunk)
                size += len(chunk)
        return h256.hexdigest(), h1.hexdigest(), size


def _parse_checksum_file(data: bytes) -> str | None:
    text = data.decode("ascii", "ignore").strip().split()
    return text[0].lower() if text and re.fullmatch(r"[0-9a-fA-F]{32,128}", text[0]) else None


# --------------------------------------------------------------------------------------------- POM model
@dataclass
class _Dep:
    group: str
    artifact: str
    version: str | None
    scope: str | None
    type: str
    classifier: str
    optional: bool
    exclusions: list[tuple[str, str]]

    @property
    def key(self) -> tuple[str, str, str, str]:
        return (self.group, self.artifact, self.type, self.classifier)


def _strip_ns(root: ET.Element) -> ET.Element:
    for el in root.iter():
        if isinstance(el.tag, str) and "}" in el.tag:
            el.tag = el.tag.split("}", 1)[1]
    return root


def _text(el: ET.Element | None, tag: str) -> str | None:
    if el is None:
        return None
    child = el.find(tag)
    return child.text.strip() if child is not None and child.text and child.text.strip() else None


@dataclass
class Pom:
    """A parsed POM with inheritance applied (properties, dependencyManagement, dependencies)."""

    coord: Coordinate
    props: dict[str, str]
    depmgmt: dict[tuple[str, str, str, str], _Dep]
    deps: list[_Dep]
    packaging: str = "jar"
    #: BOMs imported by this POM (resolved lazily by the resolver, which needs the transport)
    imports: list[_Dep] = field(default_factory=list)

    def interp(self, value: str | None) -> str | None:
        if value is None:
            return None
        out = value
        for _ in range(12):
            new = _PROP_RE.sub(lambda m: self.props.get(m.group(1), m.group(0)), out)
            if new == out:
                break
            out = new
        return out


class MavenResolver:
    """Resolve coordinates into verified jar files. See the module docstring for scope and guarantees."""

    def __init__(
        self,
        repositories: Iterable[str | dict[str, Any] | Repository] | None = None,
        *,
        cache_dir: str | Path | None = None,
        offline: bool = False,
        provided_artifacts: Iterable[str] = (),
        provided_patterns: Iterable[str] = (),
        overrides: dict[str, str] | None = None,
        timeout: float = 60.0,
    ) -> None:
        repos = list(repositories) if repositories else default_repositories()
        self.repos = [Repository.coerce(r) for r in repos]
        self.transport = Transport(timeout=timeout, offline=offline)
        self.cache = Path(cache_dir) if cache_dir else Path(tempfile.mkdtemp(prefix="olcf-maven-"))
        self.cache.mkdir(parents=True, exist_ok=True)
        self.provided_artifacts = set(provided_artifacts)
        self.provided_patterns = list(provided_patterns)
        self.overrides = dict(overrides or {})
        self._poms: dict[Coordinate, Pom] = {}
        self.used_paths: set[str] = set()  # relpaths read during this resolution (for offline bundle export)
        self.repo_of: dict[str, str] = {}

    # ------------------------------------------------------------------ verified fetch + cache
    def _cached(self, rel: str) -> Path:
        # defence in depth against hostile relpaths (coordinates come from signed manifests, POMs are less trusted)
        p = (self.cache / rel).resolve()
        if not str(p).startswith(str(self.cache.resolve()) + os.sep):
            raise DependencyResolutionError(f"refusing path outside the cache: {rel}")
        return p

    def _expected(self, repo: Repository, rel: str) -> tuple[str | None, str | None]:
        sha256 = sha1 = None
        for suffix, setter in ((".sha256", "sha256"), (".sha1", "sha1")):
            try:
                val = _parse_checksum_file(self.transport.get_bytes(repo, rel + suffix, limit=4096))
            except _NotFound:
                continue
            if setter == "sha256":
                sha256 = val
            else:
                sha1 = val
        return sha256, sha1

    def _fetch_verified(self, rel: str, *, locked_sha256: str | None = None) -> tuple[Path, str, str, int, str]:
        """Return (path, sha256, sha1, size, repo_url) for ``rel``, downloading + verifying if not cached."""
        path = self._cached(rel)
        meta = Path(str(path) + ".olcf-meta")
        if path.is_file() and meta.is_file():
            try:
                sha256_c, sha1_c, repo_url = meta.read_text().split()
                h256, h1 = _hash_file(path)
                if h256 == sha256_c and h1 == sha1_c:
                    if locked_sha256 and h256 != locked_sha256:
                        raise ChecksumMismatch(f"cached {rel} sha256 {h256} differs from the locked value {locked_sha256}",
                                               remediation="The runtime lock and the artifact disagree; do not install. Re-resolve only if you intentionally upgrade.")
                    self.used_paths.add(rel)
                    return path, h256, h1, path.stat().st_size, repo_url
            except ValueError:
                pass
            path.unlink(missing_ok=True)
            meta.unlink(missing_ok=True)
        errors: list[str] = []
        for repo in self.repos:
            tmp = path.with_suffix(path.suffix + f".tmp{os.getpid()}")
            path.parent.mkdir(parents=True, exist_ok=True)
            try:
                got256, got1, size = self.transport.download(repo, rel, tmp)
            except _NotFound as exc:
                errors.append(str(exc))
                tmp.unlink(missing_ok=True)
                continue
            pub256, pub1 = self._expected(repo, rel)
            if not pub256 and not pub1:
                tmp.unlink(missing_ok=True)
                raise ChecksumMismatch(f"{repo.base} publishes no .sha256/.sha1 for {rel}; refusing unverifiable artifact",
                                       remediation="Use a repository that serves checksum files (Maven Central, Nexus and Artifactory do).")
            if (pub256 and pub256 != got256) or (pub1 and pub1 != got1):
                tmp.unlink(missing_ok=True)
                raise ChecksumMismatch(
                    f"checksum mismatch for {rel} from {repo.base} (downloaded sha256={got256}, published sha256={pub256}, sha1={got1}/{pub1})",
                    details={"artifact": rel, "repository": repo.base},
                    remediation="The artifact or its checksum was corrupted or tampered with. Nothing was installed. Check your mirror/proxy.")
            if locked_sha256 and got256 != locked_sha256:
                tmp.unlink(missing_ok=True)
                raise ChecksumMismatch(f"{rel} sha256 {got256} differs from the locked value {locked_sha256}",
                                       remediation="The upstream artifact changed after the runtime was locked; investigate before re-locking.")
            os.replace(tmp, path)
            meta.write_text(f"{got256} {got1} {repo.base}\n")
            self.used_paths.add(rel)
            return path, got256, got1, size, repo.base
        raise DependencyResolutionError(f"{rel} not found in any repository", details={"tried": errors},
                                        remediation="Check the coordinate/version, the repository list, or the offline bundle contents.")

    # ------------------------------------------------------------------ POM loading
    def load_pom(self, coord: Coordinate, _depth: int = 0) -> Pom:
        if coord in self._poms:
            return self._poms[coord]
        if _depth > 20:
            raise DependencyResolutionError(f"POM parent chain too deep at {coord}")
        pom_coord = Coordinate(coord.group, coord.artifact, coord.version, "pom")
        path, *_ = self._fetch_verified(pom_coord.pom_relpath())
        try:
            root = _strip_ns(ET.parse(path).getroot())  # noqa: S314
        except ET.ParseError as exc:
            raise DependencyResolutionError(f"invalid POM {coord}: {exc}") from exc
        parent_el = root.find("parent")
        parent: Pom | None = None
        if parent_el is not None:
            pc = Coordinate(_text(parent_el, "groupId") or "", _text(parent_el, "artifactId") or "", _text(parent_el, "version") or "")
            parent = self.load_pom(pc, _depth + 1)
        group = _text(root, "groupId") or (_text(parent_el, "groupId") if parent_el is not None else None) or coord.group
        version = _text(root, "version") or (_text(parent_el, "version") if parent_el is not None else None) or coord.version
        artifact = _text(root, "artifactId") or coord.artifact
        packaging = _text(root, "packaging") or "jar"
        props: dict[str, str] = dict(parent.props) if parent else {}
        props_el = root.find("properties")
        if props_el is not None:
            for child in props_el:
                if isinstance(child.tag, str):
                    props[child.tag] = (child.text or "").strip()
        builtin = {
            "project.groupId": group, "pom.groupId": group, "groupId": group,
            "project.artifactId": artifact, "pom.artifactId": artifact, "artifactId": artifact,
            "project.version": version, "pom.version": version, "version": version,
            "project.packaging": packaging,
        }
        if parent_el is not None:
            builtin.update({
                "project.parent.groupId": _text(parent_el, "groupId") or "", "project.parent.artifactId": _text(parent_el, "artifactId") or "",
                "project.parent.version": _text(parent_el, "version") or "", "parent.version": _text(parent_el, "version") or "",
                "parent.groupId": _text(parent_el, "groupId") or "",
            })
        props.update(builtin)
        pom = Pom(coord=Coordinate(group, artifact, version), props=props, depmgmt=dict(parent.depmgmt) if parent else {},
                  deps=list(parent.deps) if parent else [], packaging=packaging)
        # dependencyManagement (own entries override inherited; BOM imports only fill gaps)
        own_mgmt: dict[tuple[str, str, str, str], _Dep] = {}
        for d in root.findall("dependencyManagement/dependencies/dependency"):
            dep = _parse_dep(d)
            if dep.scope == "import" and dep.type == "pom":
                pom.imports.append(dep)
            else:
                own_mgmt[dep.key] = dep
        pom.depmgmt.update(own_mgmt)
        for imp in pom.imports:
            ver = pom.interp(imp.version) or ""
            g, a = pom.interp(imp.group) or "", pom.interp(imp.artifact) or ""
            if "${" in ver + g + a:
                raise DependencyResolutionError(f"cannot resolve BOM import {g}:{a}:{ver} in {coord}")
            bom = self.load_pom(Coordinate(g, a, ver), _depth + 1)
            for k, bd in bom.depmgmt.items():
                if k not in pom.depmgmt:
                    # values must be interpolated in the BOM's own context before crossing over
                    pom.depmgmt[k] = _Dep(bd.group, bd.artifact, bom.interp(bd.version), bom.interp(bd.scope), bd.type, bd.classifier, bd.optional, bd.exclusions)
        # dependencies: own override inherited ones with the same key
        merged = {d.key: d for d in pom.deps}
        for d in root.findall("dependencies/dependency"):
            dep = _parse_dep(d)
            merged[dep.key] = dep
        pom.deps = list(merged.values())
        self._poms[coord] = pom
        return pom

    def effective_dependencies(self, coord: Coordinate) -> list[_Dep]:
        """Direct dependencies of ``coord`` with versions/scopes completed from dependencyManagement."""
        pom = self.load_pom(coord)
        out: list[_Dep] = []
        for d in pom.deps:
            g, a = pom.interp(d.group) or "", pom.interp(d.artifact) or ""
            typ, cls_ = pom.interp(d.type) or "jar", pom.interp(d.classifier) or ""
            mg = pom.depmgmt.get((d.group, d.artifact, d.type, d.classifier)) or pom.depmgmt.get((g, a, typ, cls_))
            version = pom.interp(d.version) or (pom.interp(mg.version) if mg else None)
            scope = pom.interp(d.scope) or (pom.interp(mg.scope) if mg and mg.scope else None) or "compile"
            excl = list(d.exclusions) + (list(mg.exclusions) if mg else [])
            out.append(_Dep(g, a, version, scope, typ, cls_, d.optional, excl))
        return out

    # ------------------------------------------------------------------ provided filtering
    def is_provided(self, c: Coordinate) -> bool:
        if c.artifact in self.provided_artifacts:
            return True
        return any(fnmatch.fnmatch(c.ga, pat) for pat in self.provided_patterns)

    # ------------------------------------------------------------------ resolution
    def resolve(self, roots: Iterable[str | Coordinate]) -> list[Coordinate]:
        """Resolve ``roots`` plus transitive compile/runtime dependencies (nearest wins). Roots are never filtered out."""
        root_coords = [Coordinate.parse(r) if isinstance(r, str) else r for r in roots]
        selected: dict[tuple[str, str, str, str], tuple[Coordinate, int]] = {}
        order: list[tuple[str, str, str, str]] = []
        # queue items: (coordinate, depth, exclusions, via)
        queue: deque[tuple[Coordinate, int, tuple[tuple[str, str], ...], str]] = deque((c, 0, (), "root") for c in root_coords)
        while queue:
            coord, depth, excl, via = queue.popleft()
            ov = self.overrides.get(coord.ga)
            if ov:
                coord = Coordinate(coord.group, coord.artifact, ov, coord.packaging, coord.classifier)
            _check_version(coord)
            if coord.key in selected:
                continue
            if depth > 0 and self.is_provided(coord):
                log.debug("skip %s (provided by the Spark distribution), required by %s", coord, via)
                continue
            selected[coord.key] = (coord, depth)
            order.append(coord.key)
            for dep in self.effective_dependencies(coord):
                if dep.scope in ("provided", "test", "system", "import") or dep.optional:
                    continue
                if _excluded(dep.group, dep.artifact, excl):
                    continue
                if not dep.version:
                    raise DependencyResolutionError(f"dependency {dep.group}:{dep.artifact} of {coord} has no version (not managed by any parent/BOM)")
                child = Coordinate(dep.group, dep.artifact, dep.version, dep.type, dep.classifier)
                queue.append((child, depth + 1, excl + tuple(dep.exclusions), str(coord)))
        return [selected[k][0] for k in order]

    # ------------------------------------------------------------------ fetch + lock
    def fetch_jars(self, coords: Iterable[Coordinate], dest_dir: str | Path, *, locked: dict[str, str] | None = None) -> list[dict[str, Any]]:
        """Download + verify the jar of every coordinate into ``dest_dir``; return lock entries (sorted by coordinate).

        ``locked`` maps ``str(coordinate)`` to the expected sha256 (from a previous ``lock.json``).
        Coordinates with ``type=pom`` have no jar and are skipped.
        """
        dest = Path(dest_dir)
        dest.mkdir(parents=True, exist_ok=True)
        out: list[dict[str, Any]] = []
        for c in sorted(set(coords)):
            if c.packaging == "pom":
                continue
            rel = c.relpath()
            path, sha256, sha1, size, repo = self._fetch_verified(rel, locked_sha256=(locked or {}).get(str(c)))
            target = dest / Path(rel).name
            shutil.copyfile(path, target)
            out.append({"coordinate": str(c), "file": target.name, "sha256": sha256, "sha1": sha1, "size": size, "repository": repo})
        return out

    def resolve_and_fetch(self, roots: Iterable[str | Coordinate], dest_dir: str | Path, *, locked: dict[str, str] | None = None) -> list[dict[str, Any]]:
        return self.fetch_jars(self.resolve(roots), dest_dir, locked=locked)

    def export_repository(self, dest: str | Path) -> int:
        """Copy every verified file this resolver used into ``dest`` as a Maven-layout repo (+ checksum files). Returns file count."""
        d = Path(dest)
        n = 0
        for rel in sorted(self.used_paths):
            src = self._cached(rel)
            if not src.is_file():
                continue
            target = d / rel
            target.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(src, target)
            h256, h1 = _hash_file(src)
            Path(str(target) + ".sha256").write_text(h256 + "\n")
            Path(str(target) + ".sha1").write_text(h1 + "\n")
            n += 1
        return n


def _hash_file(path: Path) -> tuple[str, str]:
    h256, h1 = hashlib.sha256(), hashlib.sha1(usedforsecurity=False)  # noqa: S324
    with open(path, "rb") as fh:
        while chunk := fh.read(1024 * 1024):
            h256.update(chunk)
            h1.update(chunk)
    return h256.hexdigest(), h1.hexdigest()


def sha256_file(path: str | Path) -> str:
    """sha256 of a file (streamed)."""
    h = hashlib.sha256()
    with open(path, "rb") as fh:
        while chunk := fh.read(1024 * 1024):
            h.update(chunk)
    return h.hexdigest()


def _check_version(c: Coordinate) -> None:
    v = c.version
    if v in ("LATEST", "RELEASE") or v.endswith("-SNAPSHOT"):
        raise DependencyResolutionError(f"{c}: LATEST/RELEASE/SNAPSHOT versions are not allowed (runtimes are pinned)",
                                        remediation="Pin an exact released version.")
    if v.startswith(("[", "(")):
        if re.fullmatch(r"\[[^,\]\[()]+\]", v):
            return
        raise DependencyResolutionError(f"{c}: version ranges are not supported", remediation="Pin an exact version (jar_overrides in the manifest can force one).")
    if "${" in v:
        raise DependencyResolutionError(f"{c}: unresolved property in version")


def _excluded(group: str, artifact: str, exclusions: tuple[tuple[str, str], ...]) -> bool:
    for g, a in exclusions:
        if (g in ("*", group)) and (a in ("*", artifact)):
            return True
    return False


def _parse_dep(el: ET.Element) -> _Dep:
    excl: list[tuple[str, str]] = []
    for e in el.findall("exclusions/exclusion"):
        excl.append((_text(e, "groupId") or "*", _text(e, "artifactId") or "*"))
    return _Dep(
        group=_text(el, "groupId") or "",
        artifact=_text(el, "artifactId") or "",
        version=_text(el, "version"),
        scope=_text(el, "scope"),
        type=_text(el, "type") or "jar",
        classifier=_text(el, "classifier") or "",
        optional=(_text(el, "optional") or "false").lower() == "true",
        exclusions=excl,
    )


# --------------------------------------------------------------------------------------------- helpers for the manager
def jar_artifact_index(jars_dir: str | Path) -> set[str]:
    """artifactIds of the jars shipped in a Spark distribution (``pyspark/jars``), derived from file names."""
    out: set[str] = set()
    d = Path(jars_dir)
    if not d.is_dir():
        return out
    for f in d.glob("*.jar"):
        m = re.match(r"^(?P<a>.+?)-(?P<v>\d[\w.\-+]*)\.jar$", f.name)
        out.add(m.group("a") if m else f.stem)
    return out


def verify_locked_jars(jars_dir: str | Path, entries: Iterable[dict[str, Any]]) -> list[str]:
    """Re-hash every locked jar in ``jars_dir``; return human-readable problems (empty list = all good)."""
    problems: list[str] = []
    d = Path(jars_dir)
    for e in entries:
        p = d / e["file"]
        if not p.is_file():
            problems.append(f"missing jar {e['file']}")
            continue
        got = sha256_file(p)
        if got != e["sha256"]:
            problems.append(f"jar {e['file']} sha256 {got[:12]}... differs from lock {e['sha256'][:12]}...")
    return problems


ResolverFactory = Callable[..., MavenResolver]
