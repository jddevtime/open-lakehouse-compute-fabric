#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Release identity preflight: refuse to release while the public files still carry unresolved owner / repository / contact placeholders.

    python scripts/check_release_config.py                 # exit 0 when every identity file is resolved, 1 (listing the files) otherwise
    python scripts/check_release_config.py --strict        # also fail on the secondary templates (verification examples, image names, example.invalid)
    python scripts/check_release_config.py --root DIR      # check another tree (the tests do)

Non-mutating: it only reads.  ``scripts/build_release.sh`` runs it as a warning for development builds and as an error with ``--require-identity``,
which ``.github/workflows/release.yml`` passes.  The files named in ``IDENTITY_FILES`` are always checked; every other text file of the tree is scanned for the
same placeholders, so a copy of the owner left in a changelog link or a setup guide is found too.  Besides placeholders it checks that the security and conduct
documents name a contact address and that CODEOWNERS names at least one valid owner.  Standard library only.
"""

from __future__ import annotations

import argparse
import os
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]

#: The public identity surface: always read (when present).
IDENTITY_FILES = ("README.md", "CONTRIBUTING.md", "CITATION.cff", "SECURITY.md", "CODE_OF_CONDUCT.md", ".github/CODEOWNERS", ".github/ISSUE_TEMPLATE/config.yml")
#: Documents that must name a private contact address.
CONTACT_FILES = ("SECURITY.md", "CODE_OF_CONDUCT.md")
#: Unresolved identity: the owner / repository / contact slots of the templates.  Any occurrence fails the release.
IDENTITY = re.compile(r"<github-owner>|<repository>|<(?:security|conduct)[ -]contact[^>]*>|to be set by maintainer|<your[-_ ]?(?:org|github)[^>]*>", re.I)
#: Secondary templates: verification examples (``jddevtime/open-lakehouse-compute-fabric``), image-name examples, deployment defaults.  Reported as notes, failures with ``--strict``.
SECONDARY = re.compile(r"<OWNER>|<REPO>|<PUBLISHED-HOST>|ghcr\.io/<owner>|ghcr\.io/your-org|https?://example\.invalid")
EMAIL = re.compile(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9-]+(?:\.[A-Za-z0-9-]+)*\.[A-Za-z]{2,}")
CODEOWNER = re.compile(r"^@[A-Za-z0-9](?:[A-Za-z0-9-]*[A-Za-z0-9])?(?:/[A-Za-z0-9._-]+)?$")
SKIP_DIRS = {".git", ".venv", "node_modules", "__pycache__", ".mypy_cache", ".ruff_cache", ".pytest_cache", ".hypothesis", "dist", "build", "tests"}
SKIP_FILES = {"THIRD_PARTY_NOTICES.md", "check_release_config.py"}
SKIP_PREFIXES = ("src/olcf/web/static/vendor/",)
MAX_BYTES = 2_000_000


def read_text(path: Path) -> str | None:
    try:
        if path.stat().st_size > MAX_BYTES:
            return None
        return path.read_text(encoding="utf-8")
    except (OSError, UnicodeDecodeError):
        return None


def candidate_files(root: Path) -> list[Path]:
    """Identity files first, then every other readable text file of the tree (without caches, vendored assets, tests)."""
    found = {root / name for name in IDENTITY_FILES if (root / name).is_file()}
    for base, dirs, names in os.walk(root):
        dirs[:] = sorted(d for d in dirs if d not in SKIP_DIRS)            # prune: never descend into virtual environments, caches or tests
        for name in sorted(names):
            path = Path(base) / name
            if name in SKIP_FILES or path.relative_to(root).as_posix().startswith(SKIP_PREFIXES) or not path.is_file():
                continue
            found.add(path)
    return sorted(found)


def check_codeowners(path: Path, text: str) -> list[str]:
    owners = 0
    problems = []
    for n, line in enumerate(text.splitlines(), 1):
        line = line.split("#", 1)[0].strip()
        if not line:
            continue
        for token in line.split()[1:]:
            owners += 1
            if not CODEOWNER.match(token) and not EMAIL.fullmatch(token):
                problems.append(f"{path.name}:{n}: '{token}' is not a valid CODEOWNERS owner (@user, @org/team or an e-mail address)")
    if not owners:
        problems.append(f"{path.name}: names no owner")
    return problems


def scan(root: Path) -> tuple[dict[str, list[str]], dict[str, list[str]]]:
    """(failures, notes): file (relative, posix) -> messages."""
    failures: dict[str, list[str]] = {}
    notes: dict[str, list[str]] = {}
    for path in candidate_files(root):
        text = read_text(path)
        rel = path.relative_to(root).as_posix()
        if text is None:
            continue
        for n, line in enumerate(text.splitlines(), 1):
            m = IDENTITY.search(line)
            if m:
                failures.setdefault(rel, []).append(f"line {n}: unresolved placeholder {m.group(0)!r}")
            m = SECONDARY.search(line)
            if m:
                notes.setdefault(rel, []).append(f"line {n}: template value {m.group(0)!r}")
        if rel in CONTACT_FILES and not EMAIL.search(text):
            failures.setdefault(rel, []).append("no contact e-mail address")
        if rel == ".github/CODEOWNERS":
            for msg in check_codeowners(path, text):
                failures.setdefault(rel, []).append(msg)
    for rel in CONTACT_FILES:
        if (root / rel).is_file() and read_text(root / rel) is None:
            failures.setdefault(rel, []).append("could not be read")
    return failures, notes


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("--root", type=Path, default=ROOT, help="tree to check (default: this repository)")
    ap.add_argument("--strict", action="store_true", help="also fail on secondary templates (jddevtime/open-lakehouse-compute-fabric examples, ghcr.io/jddevtime, example.invalid)")
    a = ap.parse_args(argv)
    failures, notes = scan(a.root.resolve())
    if a.strict:
        for rel, msgs in notes.items():
            failures.setdefault(rel, []).extend(msgs)
        notes = {}
    if failures:
        print(f"release identity metadata is incomplete: {len(failures)} file(s) with unresolved placeholders", file=sys.stderr)
        for rel in sorted(failures):
            print(f"  {rel}", file=sys.stderr)
            for msg in failures[rel][:5]:
                print(f"      {msg}", file=sys.stderr)
            if len(failures[rel]) > 5:
                print(f"      ... and {len(failures[rel]) - 5} more", file=sys.stderr)
        print("Set the real GitHub owner and repository (and CODEOWNERS reviewers) in those files; see docs/development/release-readiness.md (R01).", file=sys.stderr)
        return 1
    if notes:
        print(f"note: {len(notes)} file(s) still contain generic templates (not blocking): {', '.join(sorted(notes))}", file=sys.stderr)
    print("release identity metadata: ok", file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
