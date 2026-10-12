#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Generate ``THIRD_PARTY_NOTICES.md`` from the packages of a Python environment plus the vendored web assets and the components OLCF downloads.

Standard library only, so it runs in a bare release environment.

    python scripts/licenses.py                              # inspect the current interpreter, write THIRD_PARTY_NOTICES.md
    python scripts/licenses.py --python /path/to/venv/bin/python --out dist/THIRD_PARTY_NOTICES.md
    python scripts/licenses.py --check                      # exit 1 on unknown or strong-copyleft licenses (CI gate)
    python scripts/licenses.py --stdout --no-texts          # just the summary table

The environment to inspect should contain ``olcf`` and its runtime dependencies *only* (a fresh virtualenv with ``pip install olcf[extras]``); that is
what ``scripts/build_release.sh`` and the release workflow do.  Every installed distribution except the packaging tools is listed.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SKIP = {"olcf", "pip", "setuptools", "wheel", "uv", "pkg-resources", "pkg_resources"}
LICENSE_FILE = re.compile(r"(^|/)(licen[cs]e|copying|notice|authors)[^/]*$", re.I)
MAX_TEXT = 60_000

#: Normalised license names that need a human decision before a release (GPL/AGPL are incompatible with Apache-2.0 distribution of a combined work).
STRONG_COPYLEFT = re.compile(r"\b(A?GPL|GNU (Affero )?General Public)", re.I)
WEAK_COPYLEFT = re.compile(r"\b(LGPL|MPL|EPL|CDDL|Lesser General Public|Mozilla Public)", re.I)
#: Packages whose metadata is known to be incomplete: curated, with the evidence.
CURATED = {
    "typing-extensions": "PSF-2.0",
    "certifi": "MPL-2.0",
}
#: ``License:`` values that are placeholders, not identifications (older setuptools writes UNKNOWN when none was given): treated as if the field was absent, so the
#: ``License ::`` classifiers (or the curated table) still get their say.
PLACEHOLDER_LICENSES = frozenset({"UNKNOWN", "NONE", "NOASSERTION"})
CLASSIFIER_MAP = {
    "License :: OSI Approved :: MIT License": "MIT",
    "License :: OSI Approved :: BSD License": "BSD",
    "License :: OSI Approved :: Apache Software License": "Apache-2.0",
    "License :: OSI Approved :: ISC License (ISCL)": "ISC",
    "License :: OSI Approved :: Python Software Foundation License": "PSF-2.0",
    "License :: OSI Approved :: Mozilla Public License 2.0 (MPL 2.0)": "MPL-2.0",
    "License :: OSI Approved :: GNU Lesser General Public License v3 (LGPLv3)": "LGPL-3.0",
    "License :: OSI Approved :: GNU Lesser General Public License v2 or later (LGPLv2+)": "LGPL-2.1+",
    "License :: OSI Approved :: GNU General Public License v3 (GPLv3)": "GPL-3.0",
    "License :: OSI Approved :: The Unlicense (Unlicense)": "Unlicense",
    "License :: OSI Approved :: BSD-3-Clause": "BSD-3-Clause",
    "License :: OSI Approved :: Apache-2.0": "Apache-2.0",
    "License :: CC0 1.0 Universal (CC0 1.0) Public Domain Dedication": "CC0-1.0",
}


def usable_license_field(value: str) -> str:
    """The legacy ``License`` metadata field when it names a license: empty for placeholders (UNKNOWN / NONE / NOASSERTION), multi-line texts and long prose."""
    value = (value or "").strip()
    if not value or value.upper() in PLACEHOLDER_LICENSES or len(value) > 100 or "\n" in value:
        return ""
    return value


def resolve_license(key: str, expression: str, legacy: str, classifiers: list[str]) -> str:
    """Pick the license of one distribution: SPDX ``License-Expression``, then a real ``License`` value, then ``License ::`` classifiers, then the curated table.

    Returns "" when nothing identifies it (the caller reports UNKNOWN, which ``--check`` rejects): a placeholder never counts as an identification.
    """
    expr = usable_license_field(expression) or usable_license_field(legacy)
    classifiers = [c for c in classifiers if c.startswith("License ::")]
    if not expr and classifiers:
        expr = " OR ".join(sorted({CLASSIFIER_MAP.get(c, c.split("::")[-1].strip()) for c in classifiers}))
    return expr or CURATED.get(key, "")


def collect_here() -> list[dict[str, object]]:
    """Describe every installed distribution of *this* interpreter (runs in the inspected environment)."""
    from importlib import metadata

    out: dict[str, dict[str, object]] = {}
    for dist in metadata.distributions():
        meta = dist.metadata
        name = (meta["Name"] or "").strip()
        if not name or name.lower().replace("_", "-") in SKIP:
            continue
        key = name.lower().replace("_", "-")
        expr = resolve_license(key, meta.get("License-Expression") or "", meta.get("License") or "", list(meta.get_all("Classifier") or []))
        texts: list[tuple[str, str]] = []
        for f in dist.files or []:
            if LICENSE_FILE.search(str(f).replace("\\", "/")):
                try:
                    body = dist.read_text(str(f)) if hasattr(dist, "read_text") else None
                    body = body or (Path(str(dist.locate_file(f))).read_text(encoding="utf-8", errors="replace"))
                except (OSError, ValueError):
                    continue
                if body and body.strip():
                    texts.append((Path(str(f)).name, body[:MAX_TEXT]))
        urls = [u.split(",", 1)[-1].strip() for u in (meta.get_all("Project-URL") or [])]
        home = (meta.get("Home-page") or "").strip() or (urls[0] if urls else "")
        out[key] = {"name": name, "version": dist.version, "license": expr or "UNKNOWN", "homepage": home, "texts": texts}
    return [out[k] for k in sorted(out)]


def collect(python: str | None) -> list[dict[str, object]]:
    if not python:
        return collect_here()
    res = subprocess.run([python, "-I", str(Path(__file__).resolve()), "--emit-json"], capture_output=True, text=True, check=False)  # noqa: S603
    if res.returncode != 0:
        raise SystemExit(f"could not inspect {python}: {res.stderr.strip()[-500:]}")
    return json.loads(res.stdout)  # type: ignore[no-any-return]


# --------------------------------------------------------------------------------------------- vendored web assets
def vendored_assets() -> list[dict[str, object]]:
    """Libraries copied into ``src/olcf/web/static/vendor`` (listed in VENDOR.md once the UI package delivers it, otherwise discovered from the files)."""
    base = ROOT / "src" / "olcf" / "web" / "static" / "vendor"
    if not base.is_dir():
        return []
    items: list[dict[str, object]] = []
    manifest = base / "VENDOR.md"
    listed: dict[str, str] = {}
    if manifest.is_file():
        for line in manifest.read_text(encoding="utf-8").splitlines():
            cells = [c.strip().strip("`") for c in line.strip().strip("|").split("|")]
            if len(cells) >= 3 and cells[0] and not set(cells[0]) <= set("-: ") and cells[0].lower() not in {"library", "name", "package"}:
                listed[cells[0].lower()] = " | ".join(cells[1:])
    for sub in sorted(p for p in base.iterdir() if p.is_dir()):
        texts = []
        for f in sorted(sub.rglob("*")):
            if f.is_file() and LICENSE_FILE.search(f.name) and f.stat().st_size < MAX_TEXT:
                texts.append((f.relative_to(base).as_posix(), f.read_text(encoding="utf-8", errors="replace")))
        items.append({"name": sub.name, "detail": listed.get(sub.name.lower(), ""), "texts": texts})
    return items


def downloaded_components() -> list[tuple[str, str, str]]:
    """Software that OLCF downloads at install time (never redistributed inside the wheel): name, license, source."""
    rows = [("Eclipse Temurin JDK 17/21", "GPL-2.0 with Classpath Exception", "https://adoptium.net (verified SHA-256)"),
            ("Apache Spark / PySpark", "Apache-2.0", "PyPI (hash pinned in the runtime manifest)")]
    seen: set[str] = set()
    for mf in sorted((ROOT / "src" / "olcf" / "runtime" / "manifests").glob("*.json")):
        try:
            data = json.loads(mf.read_text(encoding="utf-8"))
        except (OSError, ValueError):
            continue
        for jar in data.get("jars", []):
            coord = str(jar.get("coordinate", ""))
            ga = ":".join(coord.split(":")[:2])
            if ga and ga not in seen:
                seen.add(ga)
                lic = "Apache-2.0" if re.match(r"(io\.delta|org\.apache)", ga) else "see the project"
                rows.append((ga, lic, "Maven Central (checksum recorded in the runtime lock)"))
    return rows


# --------------------------------------------------------------------------------------------- rendering
def render(pkgs: list[dict[str, object]], *, texts: bool) -> str:
    lines = ["# Third-party notices", "",
             "Open Lakehouse Compute Fabric (OLCF) is copyright (c) 2026 Justin Dodrill and licensed under the Apache License 2.0.",
             "It includes or depends on the following third-party software. This file is generated by `scripts/licenses.py`; do not edit it by hand.", "",
             "## Python packages", "", "| Package | Version | License |", "|---|---|---|"]
    for p in pkgs:
        lines.append(f"| {p['name']} | {p['version']} | {str(p['license']).replace('|', '/')} |")
    seen: dict[str, str] = {}
    if texts:
        lines += ["", "### License texts", ""]
        for p in pkgs:
            lines.append(f"#### {p['name']} {p['version']} - {p['license']}")
            if p.get("homepage"):
                lines.append(f"Home page: {p['homepage']}")
            lines.append("")
            blocks = p.get("texts") or []
            if not blocks:
                lines += ["(the package ships no license file; see the home page)", ""]
            for fname, body in blocks:  # type: ignore[misc]
                digest = hashlib.sha256(re.sub(r"\s+", " ", body).encode()).hexdigest()
                if digest in seen:
                    lines += [f"`{fname}`: identical to the text under {seen[digest]}.", ""]
                    continue
                seen[digest] = str(p["name"])
                lines += [f"`{fname}`", "", "```text", body.rstrip(), "```", ""]
    vend = vendored_assets()
    if vend:
        lines += ["## Vendored web assets (served by the OLCF web UI)", ""]
        for v in vend:
            lines.append(f"### {v['name']}" + (f" - {v['detail']}" if v["detail"] else ""))
            for fname, body in v["texts"]:  # type: ignore[misc]
                lines += ["", f"`{fname}`", "", "```text", body.rstrip(), "```"]
            if not v["texts"]:
                lines.append("(no license file found next to the assets; see static/vendor/VENDOR.md)")
            lines.append("")
    lines += ["## Components downloaded at install time", "",
              "Not part of the OLCF distribution; fetched on demand by `olcf runtime install` with checksum verification.", "",
              "| Component | License | Source |", "|---|---|---|"]
    lines += [f"| {n} | {lic} | {src} |" for n, lic, src in downloaded_components()]
    return "\n".join(lines) + "\n"


def problems(pkgs: list[dict[str, object]]) -> tuple[list[str], list[str]]:
    errors, notes = [], []
    for p in pkgs:
        lic = str(p["license"])
        if lic == "UNKNOWN":
            errors.append(f"{p['name']} {p['version']}: no license metadata")
        elif STRONG_COPYLEFT.search(lic) and not re.search(r"\b(LGPL|Lesser)\b", lic, re.I):
            errors.append(f"{p['name']} {p['version']}: strong copyleft license '{lic}'")
        elif WEAK_COPYLEFT.search(lic):
            notes.append(f"{p['name']} {p['version']}: '{lic}' (weak copyleft; fine for use as a library, keep the notice)")
    return errors, notes


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("--python", help="interpreter of the environment to inspect (default: this one)")
    ap.add_argument("--out", default=str(ROOT / "THIRD_PARTY_NOTICES.md"))
    ap.add_argument("--stdout", action="store_true", help="print instead of writing a file")
    ap.add_argument("--no-texts", action="store_true", help="only the summary table")
    ap.add_argument("--check", action="store_true", help="exit 1 on unknown or strong-copyleft licenses")
    ap.add_argument("--emit-json", action="store_true", help=argparse.SUPPRESS)
    a = ap.parse_args(argv)
    if a.emit_json:
        json.dump(collect_here(), sys.stdout)
        return 0
    pkgs = collect(a.python)
    errors, notes = problems(pkgs)
    text = render(pkgs, texts=not a.no_texts)
    if a.stdout:
        sys.stdout.write(text)
    else:
        Path(a.out).write_text(text, encoding="utf-8")
        print(f"wrote {a.out}: {len(pkgs)} packages, {len(vendored_assets())} vendored asset groups", file=sys.stderr)
    for n in notes:
        print(f"note: {n}", file=sys.stderr)
    for e in errors:
        print(f"{'ERROR' if a.check else 'warning'}: {e}", file=sys.stderr)
    return 1 if (errors and a.check) else 0


if __name__ == "__main__":
    raise SystemExit(main())
