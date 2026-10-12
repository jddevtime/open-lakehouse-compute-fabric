# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Run the browser API client's token-storage unit test (``remember_auth.test.mjs``) under Node (review finding F14).

The UI sources are plain ES modules served as ``.js``; Node only treats ``.js`` as ESM inside a ``"type": "module"`` package, so the three modules the client needs are
staged into a temporary package (no change to what ships) and the test is started with ``node --test``.
"""

from __future__ import annotations

import os
import shutil
import subprocess
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
STATIC_JS = ROOT / "src" / "olcf" / "web" / "static" / "js"
NEEDED = ("api/client.js", "core/util.js", "core/store.js")



def test_remember_me_survives_reload_and_token_refresh(tmp_path: Path) -> None:
    if shutil.which("node") is None:
        if os.environ.get("OLCF_REQUIRE_NODE"):  # CI installs Node: a missing interpreter there must not turn the test into a silent skip
            pytest.fail("node is required (OLCF_REQUIRE_NODE is set) but is not installed")
        pytest.skip("node is not installed")
    pkg = tmp_path / "pkg"
    for rel in NEEDED:
        dst = pkg / "js" / rel
        dst.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(STATIC_JS / rel, dst)
    (pkg / "package.json").write_text('{"type": "module"}')
    shutil.copy2(Path(__file__).with_name("remember_auth.test.mjs"), pkg / "remember_auth.test.mjs")
    # the reporter is named explicitly: Node's default depends on its version and on whether stdout is a terminal, and only TAP prints the "# pass N" summary read below
    r = subprocess.run(["node", "--test", "--test-reporter=tap", "remember_auth.test.mjs"], cwd=pkg, capture_output=True, text=True, timeout=120, check=False)
    assert r.returncode == 0, r.stdout + r.stderr
    assert "# pass 6" in r.stdout, r.stdout  # all six scenarios ran (a silently skipped file would also exit 0)
