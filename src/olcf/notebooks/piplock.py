# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Hash-pinned dependency resolution with the real ``pip`` (the "pip-compile" step of an environment publication).

Why pip itself?  The lock must contain exactly what pip would install, so the resolution is delegated to pip's resolver
(``pip install --dry-run --report``) and the artifacts are fetched with ``pip download`` so that every pin carries the sha256 of the
files that were really obtained.  Nothing here shells out: arguments are passed as a list, requirement strings are validated first
(:func:`olcf.notebooks.envspec.parse_requirement`), credentials for private indexes only ever live in a throw-away ``HOME`` (mode 0600
``.netrc``) and every message that leaves this module passes :func:`olcf.core.redaction.redact_text`.

Index policy (:func:`check_index_policy`) is enforced *before* pip runs:

* ``allow_public`` False  -> the default public index is refused; only allow-listed index URLs and approved local wheelhouses may be used;
* ``allowed`` (non-empty) -> every index URL (default one included) must be below one of the allowed prefixes;
* local ``find_links`` directories must be inside an approved wheelhouse root (no arbitrary server directories).

Limitations (also in CONTRACTS): environment markers are evaluated on the resolving host, not on the target platform; source distributions are
only accepted for the host platform / ABI (pip cannot resolve them for a foreign target) and only when the environment says ``allow_sdist``.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import subprocess
import sys
import tempfile
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any
from urllib.parse import quote, urlsplit

from packaging.utils import canonicalize_name

from olcf.core.errors import PolicyDenied, ValidationFailed
from olcf.core.redaction import redact_text
from olcf.notebooks import envspec

DEFAULT_PUBLIC_INDEX = "https://pypi.org/simple"
_ENV_KEEP = ("PATH", "LANG", "LC_ALL", "SSL_CERT_FILE", "SSL_CERT_DIR", "REQUESTS_CA_BUNDLE", "CURL_CA_BUNDLE", "HTTP_PROXY", "HTTPS_PROXY", "NO_PROXY",
             "http_proxy", "https_proxy", "no_proxy", "TMPDIR")


@dataclass
class IndexSpec:
    """The package sources of one resolution (credentials are *not* part of the spec)."""

    index_url: str | None = None
    extra_index_urls: list[str] = field(default_factory=list)
    find_links: list[str] = field(default_factory=list)
    no_index: bool = False

    @classmethod
    def from_definition(cls, d: dict[str, Any] | None) -> IndexSpec:
        d = d or {}
        return cls(index_url=d.get("index_url"), extra_index_urls=list(d.get("extra_index_urls") or []), find_links=list(d.get("find_links") or []), no_index=bool(d.get("no_index")))

    def uses_public_default(self) -> bool:
        return not self.no_index and not self.index_url

    def lock_block(self) -> dict[str, Any]:
        """What the lock records (never credentials)."""
        out: dict[str, Any] = {}
        if self.index_url:
            out["index_url"] = self.index_url
        if self.extra_index_urls:
            out["extra_index_urls"] = list(self.extra_index_urls)
        if self.find_links:
            out["find_links"] = list(self.find_links)
        if self.no_index:
            out["no_index"] = True
        return out

    def args(self) -> list[str]:
        a: list[str] = []
        if self.no_index:
            a.append("--no-index")
        elif self.index_url:
            a += ["--index-url", self.index_url]
        for u in self.extra_index_urls:
            a += ["--extra-index-url", u]
        for f in self.find_links:
            a += ["--find-links", f]
        return a


@dataclass
class IndexPolicy:
    """Admin policy for package sources (settings.security + live overrides, see ``EnvironmentService.policy``)."""

    allow_public: bool = True
    allowed_indexes: list[str] = field(default_factory=list)
    wheelhouses: list[str] = field(default_factory=list)
    allow_sdist: bool = True

    def to_dict(self) -> dict[str, Any]:
        return {"allow_public_package_install": self.allow_public, "allowed_package_indexes": list(self.allowed_indexes), "wheelhouses": list(self.wheelhouses),
                "allow_sdist": self.allow_sdist}


def _is_local(path: str) -> bool:
    return not re.match(r"^[A-Za-z][A-Za-z0-9+.-]*://", path) or path.startswith("file://")


def _local_path(path: str) -> Path:
    if path.startswith("file://"):
        path = urlsplit(path).path
    return Path(path)


def check_index_policy(spec: IndexSpec, policy: IndexPolicy) -> None:
    """Raise :class:`PolicyDenied` unless every package source of ``spec`` is acceptable under ``policy``."""
    urls = ([spec.index_url] if spec.index_url else []) + list(spec.extra_index_urls)
    if spec.uses_public_default():
        if not policy.allow_public:
            raise PolicyDenied("this deployment does not allow installing packages from the public Internet",
                               remediation="point python.index at an approved internal index or an approved wheelhouse (python.index.no_index + find_links), "
                                           "or ask an administrator to enable settings.security.allow_public_package_install")
        if policy.allowed_indexes and not any(envspec.url_within(DEFAULT_PUBLIC_INDEX, a) for a in policy.allowed_indexes):
            raise PolicyDenied("the public package index is not on the allowed index list", details={"allowed": policy.allowed_indexes},
                               remediation="use one of the allowed indexes in python.index.index_url")
    for u in urls:
        if re.match(r"^https?://(pypi\.org|files\.pythonhosted\.org)(/|$)", u) and not policy.allow_public:
            raise PolicyDenied("this deployment does not allow installing packages from the public Internet", details={"index": u})
        allowed = any(envspec.url_within(u, a) for a in policy.allowed_indexes)
        if policy.allowed_indexes and not allowed:
            raise PolicyDenied(f"package index {u} is not on the allowed index list", details={"allowed": policy.allowed_indexes},
                               remediation="ask an administrator to allow the index (settings.security.allowed_package_indexes)")
        if not policy.allowed_indexes and not policy.allow_public:
            raise PolicyDenied(f"package index {u} is not approved", remediation="ask an administrator to add it to settings.security.allowed_package_indexes")
    for f in spec.find_links:
        if _is_local(f):
            p = _local_path(f)
            if not p.is_absolute() or ".." in p.parts:
                raise PolicyDenied("find_links must be an absolute wheelhouse path", details={"find_links": f})
            rp = p.resolve()
            if not any(rp == Path(w).resolve() or Path(w).resolve() in rp.parents for w in policy.wheelhouses):
                raise PolicyDenied(f"{f} is not inside an approved wheelhouse", details={"wheelhouses": policy.wheelhouses},
                                   remediation="ask an administrator to register the wheelhouse directory (environments.wheelhouses)")
        else:
            if policy.allowed_indexes and not any(envspec.url_within(f, a) for a in policy.allowed_indexes):
                raise PolicyDenied(f"find_links location {f} is not on the allowed index list", details={"allowed": policy.allowed_indexes})
            if not policy.allowed_indexes and not policy.allow_public:
                raise PolicyDenied(f"find_links location {f} is not approved")


def apply_credentials(url: str, user: str | None, password: str | None) -> tuple[str, str | None]:
    """Return ``(host, netrc_line)`` for index credentials (used to build the throw-away ``.netrc``)."""
    host = urlsplit(url).hostname or ""
    if not user and not password:
        return host, None
    for v in (user or "", password or "", host):
        if re.search(r"[\s\x00-\x1f]", v):
            raise ValidationFailed("index credentials contain unsupported characters")
    return host, f"machine {host} login {user or '__token__'} password {password or ''}\n"


class PipError(Exception):
    """pip failed; ``args[0]`` is a redacted, actionable message."""

    remediation: str | None = None

    def __init__(self, message: str, *, output: str = "") -> None:
        super().__init__(message)
        self.output = output


#: what to do when the interpreter has no pip (``uv venv`` makes environments without it unless ``--seed`` is given)
MISSING_PIP_REMEDIATION = ("install pip into that Python environment: `python -m ensurepip --upgrade` (or `uv pip install pip`), or create the environment with `uv venv --seed`; "
                           "the `olcf[dev]` extra and the installers include pip")
_NO_PIP = re.compile(r"No module named '?pip\b")


class PipMissing(PipError):
    """The interpreter that was asked to run pip has no pip. Environment publication and the Fabric SDK build need it (see ``remediation``)."""

    def __init__(self, python: str, *, output: str = "") -> None:
        super().__init__(f"pip is not installed for the Python interpreter that resolves environments ({python})", output=output)
        self.remediation = MISSING_PIP_REMEDIATION


def _env(home: Path, extra: dict[str, str] | None = None) -> dict[str, str]:
    env = {k: os.environ[k] for k in _ENV_KEEP if k in os.environ}
    env.update({"HOME": str(home), "PIP_CONFIG_FILE": os.devnull, "PIP_NO_INPUT": "1", "PIP_DISABLE_PIP_VERSION_CHECK": "1", "PIP_NO_COLOR": "1",
                "PYTHONNOUSERSITE": "1", "PIP_CACHE_DIR": str(home / "cache")})
    env.update(extra or {})
    return env


def run_pip(args: list[str], *, home: Path, timeout: float, python: str | None = None, extra_env: dict[str, str] | None = None) -> subprocess.CompletedProcess[str]:
    exe = python or sys.executable
    cmd = [exe, "-m", "pip", *args]
    try:
        res = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout, check=False, env=_env(home, extra_env), cwd=str(home))  # noqa: S603 - argv list, no shell
    except subprocess.TimeoutExpired as exc:
        raise PipError(f"pip timed out after {timeout:.0f}s") from exc
    except OSError as exc:
        raise PipError(f"cannot run pip: {exc}") from exc
    if res.returncode != 0 and _NO_PIP.search(res.stderr or ""):
        raise PipMissing(exe, output=_tail(res))  # not a problem with the requirements: say what is missing instead of "pip could not resolve"
    return res


def _tail(res: subprocess.CompletedProcess[str], n: int = 1800) -> str:
    return redact_text(((res.stderr or "") + (res.stdout or ""))[-n:])


@dataclass
class Resolution:
    """Result of resolving requirements into a lock: the pinned packages and warnings."""
    packages: list[dict[str, Any]]
    warnings: list[str] = field(default_factory=list)


def platform_args(series: str, plat: str) -> list[str]:
    major_minor = series.replace(".", "")
    return ["--python-version", series, "--implementation", "cp", "--abi", f"cp{major_minor}", *envspec.pip_platform_args(plat)]


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def _explain_failure(output: str, spec: IndexSpec, allow_sdist: bool) -> str:
    low = output.lower()
    if "resolutionimpossible" in low or "conflicting dependencies" in low or "cannot install" in low and "conflict" in low:
        return "the requirements cannot be satisfied together (conflicting dependencies)"
    if "no matching distribution" in low or "could not find a version" in low:
        m = re.search(r"No matching distribution found for ([^\s]+)", output)
        what = f" for '{m.group(1)}'" if m else ""
        return f"no installable distribution was found{what} on the configured package sources" + ("" if allow_sdist else " (source distributions that need compilation are not allowed)")
    if "connection" in low or "temporary failure" in low or "name or service not known" in low or "max retries" in low:
        return "the package index could not be reached"
    if "401" in low or "403" in low or "authenticat" in low:
        return "the package index rejected the credentials"
    return "pip could not resolve the requirements"


def build_stub_wheel(dest: Path, name: str, version: str) -> None:
    """A metadata-only wheel (no files, no dependencies) standing in for a package the runtime already provides, so pip can resolve against it offline."""
    import base64
    import zipfile

    norm = name.replace("-", "_")
    dist = f"{norm}-{version}.dist-info"
    files = {f"{dist}/METADATA": f"Metadata-Version: 2.1\nName: {name}\nVersion: {version}\n",
             f"{dist}/WHEEL": "Wheel-Version: 1.0\nGenerator: olcf-stub\nRoot-Is-Purelib: true\nTag: py3-none-any\n"}
    rec = [f"{k},sha256={base64.urlsafe_b64encode(hashlib.sha256(v.encode()).digest()).rstrip(b'=').decode()},{len(v.encode())}" for k, v in files.items()]
    files[f"{dist}/RECORD"] = "\n".join([*rec, f"{dist}/RECORD,,"]) + "\n"
    with zipfile.ZipFile(dest / f"{norm}-{version}-py3-none-any.whl", "w") as z:
        for k, v in files.items():
            z.writestr(k, v)


def resolve(requirements: list[str], *, index: IndexSpec, series: str, platforms: list[str], allow_sdist: bool = False, provided: dict[str, str] | None = None,
            netrc: str | None = None, python: str | None = None, timeout: float = 600.0, workdir: Path | None = None) -> Resolution:
    """Resolve ``requirements`` to exact, hash-pinned packages for ``series`` / ``platforms``.  Raises :class:`ValidationFailed` with an actionable message.

    ``provided`` maps the packages the runtime already ships to their exact versions.  The dependency graph is first resolved *against* them
    (metadata-only stand-in wheels, so nothing is downloaded for them and an offline index does not need them); only when that is impossible is it
    resolved without them, and the caller decides whether the differences are acceptable.  Every pin is then downloaded for *every* platform so that
    availability is proven and the lock holds the real sha256 of each artifact.
    """
    host_ok = series == envspec.host_series() and platforms == [envspec.host_platform()]
    if allow_sdist and not host_ok:
        raise ValidationFailed("source distributions are only supported when the environment targets the resolving host's platform and Python", remediation="drop python.allow_sdist or set python.platforms to the host platform")
    own = workdir is None
    tmp = Path(tempfile.mkdtemp(prefix="olcf-piplock-")) if own else Path(workdir)
    try:
        home = tmp / "home"
        home.mkdir(parents=True, exist_ok=True)
        os.chmod(home, 0o700)
        if netrc:
            nrc = home / ".netrc"
            fd = os.open(nrc, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
            with os.fdopen(fd, "w") as fh:
                fh.write(netrc)
        reqs = tmp / "requirements.in"
        reqs.write_text("\n".join(requirements) + "\n")
        base = ["--no-input", "--disable-pip-version-check", *index.args()]
        # ---- 1. resolve the dependency graph (with the runtime's pins as constraints first, then without) -----------------------
        attempts: list[tuple[str, dict[str, str] | None]] = []
        if provided:
            attempts.append(("against the runtime's packages", provided))
        attempts.append(("without the runtime's packages", None))
        report: dict[str, Any] | None = None
        used_stubs = False
        last_out = ""
        for n, (_label, cons) in enumerate(attempts):
            rpt = tmp / f"report-{n}.json"
            args = ["install", "--dry-run", "--ignore-installed", "--quiet", "--report", str(rpt), "--target", str(tmp / "target"), "-r", str(reqs), *base]
            if cons:
                stubs = tmp / "stubs"
                stubs.mkdir(exist_ok=True)
                for pn, pv in cons.items():
                    build_stub_wheel(stubs, pn, pv)
                cf = tmp / "constraints.txt"
                cf.write_text("".join(f"{n}=={v}\n" for n, v in sorted(cons.items())))
                args += ["-c", str(cf), "--find-links", str(stubs)]
            if not allow_sdist:
                args.append("--only-binary=:all:")
            if not host_ok or not allow_sdist:
                args += platform_args(series, platforms[0])
            res = run_pip(args, home=home, timeout=timeout, python=python)
            last_out = _tail(res)
            if res.returncode == 0 and rpt.is_file():
                report = json.loads(rpt.read_text())
                used_stubs = bool(cons)
                break
        if report is None:
            msg = _explain_failure(last_out, index, allow_sdist)
            if not allow_sdist and "no installable distribution" in msg:
                msg += _probe_sdist(requirements, tmp, home, base, timeout, python)
            raise ValidationFailed(f"cannot resolve the environment: {msg}", details={"pip": last_out[-900:]},
                                   remediation="check the requirement names / versions and the configured package index or wheelhouse")
        pins: list[dict[str, Any]] = []
        for it in report.get("install", []):
            md = it.get("metadata") or {}
            name = canonicalize_name(md.get("name", ""))
            version = str(md.get("version", ""))
            if not name or not version:
                raise ValidationFailed("pip returned an incomplete resolution report")
            pins.append({"name": name, "version": version, "requested": bool(it.get("requested")), "url": (it.get("download_info") or {}).get("url", "")})
        pins.sort(key=lambda p: p["name"])
        prov = {canonicalize_name(k): v for k, v in (provided or {}).items()}
        for p in pins:
            p["provided"] = used_stubs and prov.get(p["name"]) == p["version"]
        # ---- 2. download every pin for every platform; hash what was really obtained ---------------------------------------------
        artifacts: dict[str, dict[str, dict[str, Any]]] = {p["name"]: {} for p in pins}
        for plat in platforms:
            dest = tmp / f"dl-{plat}"
            dest.mkdir(exist_ok=True)
            for p in pins:
                if p["provided"]:
                    continue
                sub = dest / p["name"]
                sub.mkdir(exist_ok=True)
                args = ["download", "--no-deps", "--dest", str(sub), f"{p['name']}=={p['version']}", *base]
                if not allow_sdist:
                    args.append("--only-binary=:all:")
                if not host_ok or not allow_sdist:
                    args += platform_args(series, plat)
                res = run_pip(args, home=home, timeout=timeout, python=python)
                files = sorted(f for f in sub.iterdir() if f.is_file())
                if res.returncode != 0 or not files:
                    raise ValidationFailed(f"{p['name']}=={p['version']} has no installable artifact for {plat} / Python {series}",
                                           details={"pip": _tail(res, 700)}, remediation="the package may only ship wheels for other platforms or require compilation; "
                                                                                           "publish a compatible wheel to your index/wheelhouse")
                for f in files:
                    sdist = envspec.is_sdist(f.name)
                    if sdist and not allow_sdist:
                        raise ValidationFailed(f"{p['name']}=={p['version']} is only available as a source distribution (it needs compilation)",
                                               remediation="publish a binary wheel for the target platform, or set python.allow_sdist (host platform only) and accept building on the node")
                    if not sdist and not envspec.wheel_compatible(f.name, series, plat):
                        raise ValidationFailed(f"{f.name} is not compatible with {plat} / Python {series}", remediation="use a wheel built for the target platform and ABI")
                    digest = sha256_file(f)
                    a = artifacts[p["name"]].setdefault(f.name, {"filename": f.name, "sha256": digest, "size": f.stat().st_size, "platforms": []})
                    if a["sha256"] != digest:  # same file name, different bytes between two downloads: refuse
                        raise ValidationFailed(f"{f.name} changed between downloads", remediation="the package source is not serving stable files")
                    if plat not in a["platforms"]:
                        a["platforms"].append(plat)
        out: list[dict[str, Any]] = []
        for p in pins:
            if p["provided"]:
                out.append({"name": p["name"], "version": p["version"], "requested": p["requested"], "provided_by_runtime": True, "hashes": [], "artifacts": []})
                continue
            arts = sorted(artifacts[p["name"]].values(), key=lambda a: a["filename"])
            out.append({"name": p["name"], "version": p["version"], "requested": p["requested"], "hashes": sorted({a["sha256"] for a in arts}),
                        "artifacts": [{k: v for k, v in a.items()} for a in arts]})
        return Resolution(packages=out)
    finally:
        if own:
            import shutil

            shutil.rmtree(tmp, ignore_errors=True)


def _probe_sdist(requirements: list[str], tmp: Path, home: Path, base: list[str], timeout: float, python: str | None) -> str:
    """When binary-only resolution failed, find out whether an sdist exists (so the message says "needs compilation" rather than "not found")."""
    for r in requirements:
        d = tmp / "probe"
        d.mkdir(exist_ok=True)
        res = run_pip(["download", "--no-deps", "--no-build-isolation", "--dest", str(d), r, *base], home=home, timeout=min(timeout, 120), python=python)
        if res.returncode == 0 and any(envspec.is_sdist(f.name) for f in d.iterdir()):
            return f". '{r}' is only available as a source distribution, which needs compilation on the node"
        for f in d.iterdir():
            f.unlink(missing_ok=True)
    return ""


def quote_userinfo(s: str) -> str:
    return quote(s, safe="")
