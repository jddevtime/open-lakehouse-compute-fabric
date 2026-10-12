# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Environment item definitions and locks: validation, canonical hashing and platform / ABI compatibility rules (spec section 21).

Definition (``environment`` item)::

    {"runtime": {"family"|"runtime_id"|"spark"|"python"|"java"|"scala"},
     "python":  {"requirements": ["pandas>=2,<3"], "index": {"index_url", "extra_index_urls", "find_links", "no_index"},
                 "platforms": ["linux-x86_64"], "allow_sdist": false, "override_runtime_packages": []},
     "jars":    {"coordinates": ["org.apache.hadoop:hadoop-azure:3.4.0"], "files": [{"uri": "...", "sha256": "..."}]},
     "variables": {"NAME": "value"}, "spark_conf": {"spark.sql.shuffle.partitions": "8"},
     "pip_policy": "block|declared|allow",
     "publications": [{"version", "lock_hash", "layer_key", "published_at", "published_by", "comment", "spec_hash"}]}   # platform managed

A *lock* (``olcf.envlock/1``) is the immutable result of publishing: exact versions with sha256 for every acceptable artifact (per target platform),
the pinned jars, the index it was resolved against (never credentials), the environment variables and approved Spark conf.  Its ``lock_hash`` is the
sha256 of the canonical JSON of everything except ``meta`` - identical inputs resolve to an identical hash.
"""

from __future__ import annotations

import hashlib
import json
import platform as _platform
import re
import sys
from typing import Any
from urllib.parse import urlsplit

from packaging.requirements import InvalidRequirement, Requirement
from packaging.utils import InvalidWheelFilename, canonicalize_name, parse_wheel_filename

from olcf.core.errors import PolicyDenied, ValidationFailed
from olcf.notebooks.layer import canonical_json, layer_key, requirements_text

LOCK_SCHEMA = "olcf.envlock/1"
PIP_POLICIES = ("block", "declared", "allow")
SDIST_SUFFIXES = (".tar.gz", ".zip", ".tar.bz2", ".tgz", ".tar.xz")
_NAME_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]{0,127}$")
_SECRETISH = re.compile(r"(?i)(password|passwd|secret|token|api[_-]?key|private[_-]?key|credential)")
_COORD_RE = re.compile(r"^[A-Za-z0-9_.\-]+:[A-Za-z0-9_.\-]+(:[A-Za-z0-9_.\-]+){1,3}$")
_BAD_VERSION = re.compile(r"(?i)(^latest$|^release$|snapshot|^\[|^\(|,|\*|\+$)")

# ----------------------------------------------------------------------------- platforms
TARGET_PLATFORMS: dict[str, re.Pattern[str]] = {
    "linux-x86_64": re.compile(r"^(any|manylinux[\w.]*_x86_64|linux_x86_64)$"),
    "linux-aarch64": re.compile(r"^(any|manylinux[\w.]*_aarch64|linux_aarch64)$"),
    "macos-arm64": re.compile(r"^(any|macosx_\d+_\d+_(arm64|universal2))$"),
    "macos-x86_64": re.compile(r"^(any|macosx_\d+_\d+_(x86_64|universal2|intel|fat64|fat32))$"),
    "windows-x86_64": re.compile(r"^(any|win_amd64)$"),
    "windows-arm64": re.compile(r"^(any|win_arm64)$"),
}


def host_platform() -> str:
    sysname = {"linux": "linux", "darwin": "macos", "win32": "windows"}.get(sys.platform, sys.platform)
    m = _platform.machine().lower()
    arch = {"x86_64": "x86_64", "amd64": "x86_64", "aarch64": "aarch64", "arm64": "arm64" if sysname == "macos" else "aarch64"}.get(m, m)
    if sysname == "macos" and arch == "aarch64":
        arch = "arm64"
    return f"{sysname}-{arch}"


def host_series() -> str:
    return f"{sys.version_info.major}.{sys.version_info.minor}"


def pip_platform_args(plat: str) -> list[str]:
    """``--platform`` arguments (pip needs the explicit compatible tag list when resolving for a foreign platform)."""
    if plat not in TARGET_PLATFORMS:
        raise ValidationFailed(f"unknown platform '{plat}'", details={"known": sorted(TARGET_PLATFORMS)})
    # Oldest first: pip prefers the earlier tags, so a package that ships several manylinux wheels resolves to the most portable one
    # (a lock must install on every node of the fleet, not only on the newest glibc).
    tags: list[str] = []
    if plat.startswith("linux-"):
        arch = plat.split("-", 1)[1]
        tags = [f"manylinux1_{arch}", f"manylinux2010_{arch}", f"manylinux2014_{arch}"] + [f"manylinux_2_{n}_{arch}" for n in range(17, 41)] + [f"linux_{arch}"]
    elif plat.startswith("macos-"):
        arch = plat.split("-", 1)[1]
        majors = ([(10, n) for n in range(9, 17)] if arch == "x86_64" else []) + [(m, 0) for m in range(11, 16)]
        tags = [f"macosx_{a}_{b}_{arch}" for a, b in majors] + [f"macosx_{a}_{b}_universal2" for a, b in majors]
    elif plat == "windows-x86_64":
        tags = ["win_amd64"]
    elif plat == "windows-arm64":
        tags = ["win_arm64"]
    out: list[str] = []
    for t in tags:
        out += ["--platform", t]
    return out


def wheel_compatible(filename: str, series: str, plat: str) -> bool:
    """Is the wheel installable on ``plat`` with CPython ``series`` (``3.13``)?  Pure-python (``py3-none-any``), abi3 and exact ``cpXY`` wheels pass."""
    try:
        _, _, _, tags = parse_wheel_filename(filename)
    except InvalidWheelFilename:
        return False
    minor = int(series.split(".")[1])
    rx = TARGET_PLATFORMS[plat]
    for t in tags:
        interp, abi, pl = t.interpreter, t.abi, t.platform
        py_ok = interp in (f"cp3{minor}", "py3", f"py3{minor}", "cp3") or (abi == "abi3" and re.fullmatch(r"cp3(\d+)", interp) is not None and int(interp[3:]) <= minor)
        abi_ok = abi in ("none", "abi3", f"cp3{minor}")
        if py_ok and abi_ok and rx.match(pl):
            return True
    return False


def is_sdist(filename: str) -> bool:
    return filename.lower().endswith(SDIST_SUFFIXES)


def abi_tag(series: str) -> str:
    return "cp" + series.replace(".", "")


# ----------------------------------------------------------------------------- requirements
def parse_requirement(text: str) -> Requirement:
    """A strict PEP 508 *named* requirement.  Direct URLs, VCS references, local paths and pip options are rejected (supply-chain and argument injection)."""
    t = (text or "").strip()
    if not t or len(t) > 400:
        raise ValidationFailed("empty or over-long requirement")
    if t.startswith("-") or "\n" in t or "\r" in t or "\x00" in t:
        raise ValidationFailed(f"requirement {t[:60]!r} looks like a pip option or contains control characters",
                               remediation="list plain requirements such as 'pandas>=2,<3'; indexes belong in python.index")
    try:
        req = Requirement(t)
    except InvalidRequirement as exc:
        raise ValidationFailed(f"invalid requirement {t[:60]!r}: {exc}") from exc
    if req.url:
        raise ValidationFailed(f"requirement {t[:60]!r} points to a URL; only package names from an approved index are allowed",
                               remediation="publish the package to your index / wheelhouse and require it by name")
    return req


def normalise_requirements(items: Any) -> list[str]:
    if items is None:
        return []
    if not isinstance(items, list):
        raise ValidationFailed("python.requirements must be a list of strings")
    seen, out = set(), []
    for it in items:
        if not isinstance(it, str):
            raise ValidationFailed("python.requirements must be a list of strings")
        r = parse_requirement(it)
        text = str(r)
        if text not in seen:
            seen.add(text)
            out.append(text)
    return out


def check_index_url(url: str, what: str) -> str:
    if not isinstance(url, str) or not url or len(url) > 500 or re.search(r"[\s\x00-\x1f]", url) or url.startswith("-"):
        raise ValidationFailed(f"{what} is not a valid URL")
    u = urlsplit(url)
    if u.scheme not in ("http", "https", "file"):
        raise ValidationFailed(f"{what} must use http, https or file")
    if u.username or u.password:
        raise ValidationFailed(f"{what} must not contain credentials", remediation="reference a connection (python.index.connection) so the secret stays in the secret store")
    return url


def check_jar_coordinate(c: Any) -> str:
    if not isinstance(c, str) or not _COORD_RE.match(c.strip()):
        raise ValidationFailed(f"invalid Maven coordinate {str(c)[:60]!r}", remediation="use group:artifact:version")
    version = c.strip().split(":")[-1]
    if _BAD_VERSION.search(version):
        raise ValidationFailed(f"Maven coordinate {c!r} is not pinned to an exact version", remediation="LATEST, RELEASE, SNAPSHOT and version ranges are not allowed")
    return c.strip()


# ----------------------------------------------------------------------------- definition
_ALLOWED_KEYS = {"runtime", "python", "jars", "variables", "spark_conf", "pip_policy", "publications", "description"}
_RUNTIME_KEYS = {"family", "runtime_id", "spark", "python", "java", "scala"}


def spec_hash(defn: dict[str, Any]) -> str:
    core = {k: v for k, v in defn.items() if k != "publications"}
    return "sha256:" + hashlib.sha256(canonical_json(core).encode()).hexdigest()


def normalise_definition(defn: Any) -> dict[str, Any]:
    """Structural validation of an ``environment`` definition (no I/O).  Idempotent."""
    if defn is None:
        defn = {}
    if not isinstance(defn, dict):
        raise ValidationFailed("an environment definition must be an object")
    extra = [k for k in defn if k not in _ALLOWED_KEYS and not str(k).startswith("x-")]
    if extra:
        raise ValidationFailed(f"unknown environment keys {sorted(extra)}", remediation=f"allowed: {sorted(_ALLOWED_KEYS)}")
    out: dict[str, Any] = {}
    rt = defn.get("runtime")
    if rt is not None:
        if not isinstance(rt, dict) or set(rt) - _RUNTIME_KEYS:
            raise ValidationFailed(f"environment.runtime may only contain {sorted(_RUNTIME_KEYS)}")
        for k, v in rt.items():
            if v is not None and not isinstance(v, str):
                raise ValidationFailed(f"environment.runtime.{k} must be a string")
        out["runtime"] = {k: v for k, v in rt.items() if v}
    py = defn.get("python")
    if py is not None:
        if not isinstance(py, dict):
            raise ValidationFailed("environment.python must be an object")
        bad = set(py) - {"requirements", "index", "platforms", "allow_sdist", "override_runtime_packages"}
        if bad:
            raise ValidationFailed(f"unknown environment.python keys {sorted(bad)}")
        p: dict[str, Any] = {}
        if "requirements" in py:
            p["requirements"] = normalise_requirements(py["requirements"])
        if py.get("index") is not None:
            ix = py["index"]
            if not isinstance(ix, dict) or set(ix) - {"index_url", "extra_index_urls", "find_links", "no_index", "connection"}:
                raise ValidationFailed("environment.python.index may contain index_url, extra_index_urls, find_links, no_index, connection")
            nix: dict[str, Any] = {}
            if ix.get("index_url"):
                nix["index_url"] = check_index_url(ix["index_url"], "python.index.index_url")
            if ix.get("extra_index_urls"):
                if not isinstance(ix["extra_index_urls"], list):
                    raise ValidationFailed("python.index.extra_index_urls must be a list")
                nix["extra_index_urls"] = [check_index_url(u, "python.index.extra_index_urls[]") for u in ix["extra_index_urls"]]
            if ix.get("find_links"):
                if not isinstance(ix["find_links"], list):
                    raise ValidationFailed("python.index.find_links must be a list")
                nix["find_links"] = [str(u) for u in ix["find_links"]]
                for u in nix["find_links"]:
                    if re.search(r"[\s\x00-\x1f]", u) or u.startswith("-") or len(u) > 500:
                        raise ValidationFailed("python.index.find_links contains an invalid entry")
            if ix.get("no_index"):
                nix["no_index"] = True
            if ix.get("connection"):
                if not isinstance(ix["connection"], str):
                    raise ValidationFailed("python.index.connection must be a connection name or id")
                nix["connection"] = ix["connection"]
            p["index"] = nix
        if "platforms" in py:
            pl = py["platforms"]
            if not isinstance(pl, list) or not pl or not all(isinstance(x, str) and x in TARGET_PLATFORMS for x in pl):
                raise ValidationFailed("python.platforms must be a non-empty list of known platforms", details={"known": sorted(TARGET_PLATFORMS)})
            p["platforms"] = list(dict.fromkeys(pl))
        if "allow_sdist" in py:
            p["allow_sdist"] = bool(py["allow_sdist"])
        if py.get("override_runtime_packages"):
            ov = py["override_runtime_packages"]
            if not isinstance(ov, list) or not all(isinstance(x, str) for x in ov):
                raise ValidationFailed("python.override_runtime_packages must be a list of package names")
            p["override_runtime_packages"] = sorted({canonicalize_name(x) for x in ov})
        out["python"] = p
    jars = defn.get("jars")
    if jars is not None:
        if not isinstance(jars, dict) or set(jars) - {"coordinates", "files"}:
            raise ValidationFailed("environment.jars may contain coordinates and files")
        j: dict[str, Any] = {}
        if jars.get("coordinates"):
            if not isinstance(jars["coordinates"], list):
                raise ValidationFailed("jars.coordinates must be a list")
            j["coordinates"] = list(dict.fromkeys(check_jar_coordinate(c) for c in jars["coordinates"]))
        if jars.get("files"):
            if not isinstance(jars["files"], list):
                raise ValidationFailed("jars.files must be a list")
            files = []
            for f in jars["files"]:
                if not isinstance(f, dict) or not isinstance(f.get("uri"), str) or set(f) - {"uri", "sha256"}:
                    raise ValidationFailed("each jars.files entry needs a 'uri' (and optionally 'sha256')")
                if f.get("sha256") is not None and not re.fullmatch(r"[0-9a-f]{64}", str(f["sha256"])):
                    raise ValidationFailed("jars.files[].sha256 must be 64 lowercase hex characters")
                files.append({k: f[k] for k in ("uri", "sha256") if f.get(k)})
            j["files"] = files
        out["jars"] = j
    if defn.get("variables"):
        v = defn["variables"]
        if not isinstance(v, dict):
            raise ValidationFailed("environment.variables must be an object")
        for k, val in v.items():
            if not _NAME_RE.match(str(k)) or str(k).upper().startswith("OLCF_"):
                raise ValidationFailed(f"invalid variable name {k!r}", remediation="letters, digits and underscores; the OLCF_ prefix is reserved")
            if not isinstance(val, str) or len(val) > 4096:
                raise ValidationFailed(f"environment variable {k} must be a string of at most 4096 characters")
            if _SECRETISH.search(str(k)) and val:
                raise ValidationFailed(f"environment variable {k} looks like a secret; environment variables are stored in clear text",
                                       remediation="store the value as a secret and reference it from the job (secret_refs) or a variable set")
        out["variables"] = dict(sorted(v.items()))
    if defn.get("spark_conf"):
        sc = defn["spark_conf"]
        if not isinstance(sc, dict):
            raise ValidationFailed("environment.spark_conf must be an object")
        norm = {}
        for k, val in sc.items():
            if not isinstance(k, str) or not k.startswith("spark."):
                raise ValidationFailed(f"spark_conf key {k!r} must start with 'spark.'")
            if isinstance(val, (dict, list)):
                raise ValidationFailed(f"spark_conf {k} must be a scalar")
            norm[k] = "true" if val is True else "false" if val is False else str(val)
        out["spark_conf"] = dict(sorted(norm.items()))
    pp = defn.get("pip_policy")
    if pp is not None:
        if pp not in PIP_POLICIES:
            raise ValidationFailed(f"pip_policy must be one of {PIP_POLICIES}")
        out["pip_policy"] = pp
    if isinstance(defn.get("description"), str):
        out["description"] = defn["description"]
    pubs = defn.get("publications")
    if pubs:
        if not isinstance(pubs, list):
            raise ValidationFailed("environment.publications must be a list")
        norm_pubs = []
        for i, pb in enumerate(pubs, start=1):
            if not isinstance(pb, dict) or pb.get("version") != i or not isinstance(pb.get("lock_hash"), str) or not pb["lock_hash"].startswith("sha256:"):
                raise ValidationFailed("environment.publications must be consecutive versions 1..n, each with a lock_hash", details={"index": i})
            norm_pubs.append(pb)
        out["publications"] = norm_pubs
    for k, val in defn.items():
        if str(k).startswith("x-"):
            out[k] = val
    return out


# ----------------------------------------------------------------------------- locks
def lock_core(lock: dict[str, Any]) -> dict[str, Any]:
    return {k: v for k, v in lock.items() if k not in ("meta", "lock_hash")}


def compute_lock_hash(lock: dict[str, Any]) -> str:
    return "sha256:" + hashlib.sha256(canonical_json(lock_core(lock)).encode()).hexdigest()


def layer_packages(lock: dict[str, Any]) -> list[dict[str, Any]]:
    """The packages the job layer must install (everything the runtime does not already provide at the same version)."""
    return [p for p in (lock.get("python") or {}).get("packages", []) if not p.get("provided_by_runtime")]


def lock_layer_key(lock: dict[str, Any]) -> str:
    py = lock.get("python") or {}
    jars = [j["coordinate"] + "@" + j["sha256"] for j in (lock.get("jars") or {}).get("resolved", [])] + \
           [f["uri"] + "@" + f["sha256"] for f in (lock.get("jars") or {}).get("files", [])]
    return layer_key(layer_packages(lock), py.get("abi") or abi_tag(py.get("series") or host_series()), jars)


def validate_lock(lock: Any) -> None:
    """Structural and integrity validation of a lock (used for imported locks; published ones are valid by construction)."""
    if not isinstance(lock, dict) or lock.get("schema") != LOCK_SCHEMA:
        raise ValidationFailed(f"not an environment lock (schema must be {LOCK_SCHEMA})")
    py = lock.get("python")
    if not isinstance(py, dict) or not isinstance(py.get("packages"), list) or not re.fullmatch(r"3\.\d{1,2}", str(py.get("series") or "")):
        raise ValidationFailed("lock.python needs a series (3.x) and a package list")
    plats = py.get("platforms")
    if not isinstance(plats, list) or not plats or not all(x in TARGET_PLATFORMS for x in plats):
        raise ValidationFailed("lock.python.platforms must list known platforms")
    seen = set()
    for p in py["packages"]:
        if not isinstance(p, dict) or not all(k in p for k in ("name", "version")) or ("hashes" not in p and not p.get("provided_by_runtime")):
            raise ValidationFailed("each locked package needs name, version and hashes")
        key = canonicalize_name(str(p["name"]))
        if key in seen:
            raise ValidationFailed(f"package {p['name']} is locked twice")
        seen.add(key)
        if key != p["name"]:
            raise ValidationFailed(f"locked package name {p['name']!r} must be canonical ({key!r})")
        if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._+!-]*", str(p["version"])):
            raise ValidationFailed(f"locked package {p['name']} has an invalid version")
        hs = p["hashes"]
        if p.get("provided_by_runtime"):
            continue  # shipped by the runtime itself: pinned by the runtime fingerprint, never installed into the layer
        if not isinstance(hs, list) or not hs or not all(isinstance(h, str) and re.fullmatch(r"[0-9a-f]{64}", h) for h in hs):
            raise ValidationFailed(f"locked package {p['name']} needs sha256 hashes (64 hex characters)")
        for a in p.get("artifacts") or []:
            fn = str(a.get("filename", ""))
            if is_sdist(fn):
                if not py.get("allow_sdist"):
                    raise ValidationFailed(f"locked package {p['name']} is a source distribution but the lock does not allow sdists (native compilation)")
            else:
                for pl in a.get("platforms") or plats:
                    if pl not in TARGET_PLATFORMS or not wheel_compatible(fn, py["series"], pl):
                        raise ValidationFailed(f"locked artifact {fn} is not installable on {pl} with Python {py['series']}")
    claimed = lock.get("lock_hash")
    if claimed is not None and claimed != compute_lock_hash(lock):
        raise ValidationFailed("lock_hash does not match the lock content (the lock was modified)")


def check_policy_for_lock(lock: dict[str, Any], *, allow_public: bool, allowed: list[str]) -> None:
    """Re-apply the index policy to an imported lock (no network access)."""
    ix = lock.get("index") or {}
    urls = [u for u in [ix.get("index_url"), *(ix.get("extra_index_urls") or [])] if u]
    if not urls and not ix.get("no_index") and not ix.get("find_links") and (lock.get("python") or {}).get("packages") and not allow_public and not allowed:
        raise PolicyDenied("this lock was resolved against the public package index, which this deployment denies")
    for u in urls:
        if allowed and not any(url_within(u, a) for a in allowed):
            raise PolicyDenied(f"the lock's package index {u} is not on the allowed list", remediation="ask an administrator to allow it or re-resolve the environment")


def url_within(url: str, prefix: str) -> bool:
    a, b = urlsplit(url), urlsplit(prefix)
    if (a.scheme, a.netloc.lower()) != (b.scheme, b.netloc.lower()):
        return False
    pa, pb = a.path.rstrip("/"), b.path.rstrip("/")
    return pa == pb or pa.startswith(pb + "/")


def layer_block(lock: dict[str, Any]) -> dict[str, Any]:
    """The instructions handed to the node (see :mod:`olcf.notebooks.layer`)."""
    py = lock.get("python") or {}
    ix = lock.get("index") or {}
    pk = layer_packages(lock)
    jars = lock.get("jars") or {}
    return {
        "key": lock_layer_key(lock),
        "python": {"series": py.get("series"), "abi": py.get("abi")},
        "platforms": list(py.get("platforms") or []),
        "packages": [{"name": p["name"], "version": p["version"], "hashes": list(p["hashes"]), **({"marker": p["marker"]} if p.get("marker") else {})} for p in pk],
        "requirements": requirements_text(pk),
        "pip": {"require_hashes": True, "no_deps": True, "allow_sdist": bool(py.get("allow_sdist")), "no_index": bool(ix.get("no_index")), "index_url": ix.get("index_url"),
                "extra_index_urls": list(ix.get("extra_index_urls") or []), "find_links": list(ix.get("find_links") or [])},
        "jars": {"resolved": list(jars.get("resolved") or []), "files": list(jars.get("files") or [])},
    }


def to_json(obj: Any) -> str:
    return json.dumps(obj, indent=2, sort_keys=True, ensure_ascii=False) + "\n"
