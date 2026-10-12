# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Builds tiny, real, valid wheels / sdists / Maven repositories for the ITEMS tests (no Internet, no compilation)."""

from __future__ import annotations

import base64
import hashlib
import io
import tarfile
import zipfile
from collections.abc import Iterable
from pathlib import Path


def build_wheel(dest: Path, name: str, version: str, requires: Iterable[str] = (), tag: str = "py3-none-any", files: dict[str, str] | None = None) -> Path:
    """Write ``name-version-tag.whl`` below ``dest`` (a valid wheel: METADATA, WHEEL, RECORD) and return its path."""
    dest.mkdir(parents=True, exist_ok=True)
    norm = name.replace("-", "_").lower()
    dist = f"{norm}-{version}.dist-info"
    body = dict(files or {f"{norm}/__init__.py": f"VERSION = {version!r}\n"})
    meta = f"Metadata-Version: 2.1\nName: {name}\nVersion: {version}\n" + "".join(f"Requires-Dist: {r}\n" for r in requires)
    pure = tag.endswith("none-any")
    wheel = f"Wheel-Version: 1.0\nGenerator: olcf-tests\nRoot-Is-Purelib: {'true' if pure else 'false'}\nTag: {tag}\n"
    allf = {**body, f"{dist}/METADATA": meta, f"{dist}/WHEEL": wheel}
    rec = []
    for k, v in allf.items():
        b = v.encode()
        h = base64.urlsafe_b64encode(hashlib.sha256(b).digest()).rstrip(b"=").decode()
        rec.append(f"{k},sha256={h},{len(b)}")
    rec.append(f"{dist}/RECORD,,")
    allf[f"{dist}/RECORD"] = "\n".join(rec) + "\n"
    path = dest / f"{norm}-{version}-{tag}.whl"
    with zipfile.ZipFile(path, "w", zipfile.ZIP_DEFLATED) as z:
        for k, v in allf.items():
            z.writestr(k, v)
    return path


_BACKEND = '''"""A minimal in-tree PEP 517 backend: no build requirements, so an sdist can be built without any index."""
import base64, hashlib, os, zipfile

NAME, VERSION = {name!r}, {version!r}


def get_requires_for_build_wheel(config_settings=None):
    return []


def prepare_metadata_for_build_wheel(metadata_directory, config_settings=None):
    d = os.path.join(metadata_directory, NAME.replace("-", "_") + "-" + VERSION + ".dist-info")
    os.makedirs(d, exist_ok=True)
    with open(os.path.join(d, "METADATA"), "w") as fh:
        fh.write("Metadata-Version: 2.1\\nName: %s\\nVersion: %s\\n" % (NAME, VERSION))
    return os.path.basename(d)


def build_wheel(wheel_directory, config_settings=None, metadata_directory=None):
    norm = NAME.replace("-", "_")
    dist = norm + "-" + VERSION + ".dist-info"
    files = {{norm + "/__init__.py": "VERSION = %r\\n" % VERSION,
             dist + "/METADATA": "Metadata-Version: 2.1\\nName: %s\\nVersion: %s\\n" % (NAME, VERSION),
             dist + "/WHEEL": "Wheel-Version: 1.0\\nGenerator: backend\\nRoot-Is-Purelib: true\\nTag: py3-none-any\\n"}}
    rec = []
    for k, v in files.items():
        b = v.encode()
        rec.append("%s,sha256=%s,%d" % (k, base64.urlsafe_b64encode(hashlib.sha256(b).digest()).rstrip(b"=").decode(), len(b)))
    rec.append(dist + "/RECORD,,")
    files[dist + "/RECORD"] = "\\n".join(rec) + "\\n"
    name = "%s-%s-py3-none-any.whl" % (norm, VERSION)
    with zipfile.ZipFile(os.path.join(wheel_directory, name), "w") as z:
        for k, v in files.items():
            z.writestr(k, v)
    return name
'''


def build_sdist(dest: Path, name: str, version: str) -> Path:
    """An sdist that needs a build step (built by an in-tree PEP 517 backend, so no index is needed to build it)."""
    dest.mkdir(parents=True, exist_ok=True)
    norm = name.replace("-", "_").lower()
    top = f"{norm}-{version}"
    files = {
        f"{top}/PKG-INFO": f"Metadata-Version: 2.1\nName: {name}\nVersion: {version}\n",
        f"{top}/pyproject.toml": '[build-system]\nrequires = []\nbuild-backend = "_backend"\nbackend-path = ["."]\n\n'
                                  f'[project]\nname = "{name}"\nversion = "{version}"\n',
        f"{top}/_backend.py": _BACKEND.format(name=name, version=version),
    }
    path = dest / f"{top}.tar.gz"
    with tarfile.open(path, "w:gz") as t:
        for k, v in files.items():
            b = v.encode()
            ti = tarfile.TarInfo(k)
            ti.size = len(b)
            ti.mtime = 0
            t.addfile(ti, io.BytesIO(b))
    return path


def build_maven_repo(root: Path, coords: dict[str, list[str]]) -> dict[str, str]:
    """Create a ``file://`` Maven repository.  ``coords`` maps ``group:artifact:version`` to its dependency coordinates; returns coordinate -> jar sha256."""
    out: dict[str, str] = {}
    for coord, deps in coords.items():
        g, a, v = coord.split(":")
        d = root / g.replace(".", "/") / a / v
        d.mkdir(parents=True, exist_ok=True)
        depxml = "".join(
            f"<dependency><groupId>{x.split(':')[0]}</groupId><artifactId>{x.split(':')[1]}</artifactId><version>{x.split(':')[2]}</version></dependency>" for x in deps)
        pom = f'<project xmlns="http://maven.apache.org/POM/4.0.0"><modelVersion>4.0.0</modelVersion><groupId>{g}</groupId><artifactId>{a}</artifactId><version>{v}</version>' \
              f"<dependencies>{depxml}</dependencies></project>"
        (d / f"{a}-{v}.pom").write_text(pom)
        jar = d / f"{a}-{v}.jar"
        with zipfile.ZipFile(jar, "w") as z:
            z.writestr("META-INF/MANIFEST.MF", "Manifest-Version: 1.0\n")
            z.writestr(f"{a}.txt", f"{coord}\n")
        for f in (d / f"{a}-{v}.pom", jar):
            (Path(str(f) + ".sha256")).write_text(hashlib.sha256(f.read_bytes()).hexdigest() + "\n")
        out[coord] = hashlib.sha256(jar.read_bytes()).hexdigest()
    return out
