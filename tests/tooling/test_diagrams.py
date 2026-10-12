# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""The README graphics are generated: they must match the generator and be well-formed XML.

Why: a hand-edited or stale picture misleads, and a duplicated attribute makes a browser silently truncate an SVG, which XML parsing catches.
"""

from __future__ import annotations

import subprocess
import sys
import xml.etree.ElementTree as ET
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
DIAGRAMS = ROOT / "docs" / "images" / "diagrams"


def test_committed_diagrams_match_the_generator() -> None:
    r = subprocess.run([sys.executable, str(ROOT / "scripts" / "gen_diagrams.py"), "--check"], capture_output=True, text=True, timeout=120)
    assert r.returncode == 0, r.stdout + r.stderr


def test_every_diagram_is_well_formed_and_described() -> None:
    files = sorted(DIAGRAMS.glob("*.svg"))
    assert len(files) >= 7
    for f in files:
        root = ET.parse(f).getroot()  # noqa: S314 - files of this repository
        ns = "{http://www.w3.org/2000/svg}"
        assert root.tag == ns + "svg", f
        assert root.find(ns + "title") is not None and root.find(ns + "desc") is not None, f"{f.name} needs a title and a description for screen readers"


def test_readme_images_exist() -> None:
    import re
    text = (ROOT / "README.md").read_text(encoding="utf-8")
    refs = set(re.findall(r'(?:src="|\]\()(docs/images/[^")\s]+)', text))
    refs = {r for r in refs if not r.endswith("/")}
    assert refs
    missing = [r for r in refs if not (ROOT / r).is_file()]
    assert not missing, missing
