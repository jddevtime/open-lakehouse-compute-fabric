# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Run the unit tests of the Intelligence UI's pure modules (``intel_ui.test.mjs``) under Node: the lineage layout (no overlaps with hundreds of nodes, deterministic), the graph helpers,
the formatters and link builders, the SVG export and the Markdown renderer. Also static checks that the routes, navigation, translations, stylesheet and documentation agree.

Like ``test_remember_auth.py`` the plain ES modules are staged into a temporary ``"type": "module"`` package (no change to what ships) and started with ``node --test``.
"""

from __future__ import annotations

import os
import re
import shutil
import subprocess
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
STATIC = ROOT / "src" / "olcf" / "web" / "static"
JS = STATIC / "js"
NEEDED = ("intel/layout.js", "intel/graphops.js", "intel/graphdraw.js", "intel/model.js", "core/h.js", "notebook/markdown.js")
VENDOR = ("vendor/preact/preact.module.js", "vendor/preact/hooks.module.js", "vendor/htm/htm.module.js")


def _node() -> None:
    if shutil.which("node") is None:
        if os.environ.get("OLCF_REQUIRE_NODE"):  # CI installs Node: a missing interpreter there must not turn the test into a silent skip
            pytest.fail("node is required (OLCF_REQUIRE_NODE is set) but is not installed")
        pytest.skip("node is not installed")


def test_intelligence_ui_modules_pass_their_node_tests(tmp_path: Path) -> None:
    _node()
    pkg = tmp_path / "pkg"
    for rel in NEEDED:
        dst = pkg / "js" / rel
        dst.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(JS / rel, dst)
    for rel in VENDOR:
        dst = pkg / rel
        dst.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(STATIC / rel, dst)
    (pkg / "package.json").write_text('{"type": "module"}')
    shutil.copy2(Path(__file__).with_name("intel_ui.test.mjs"), pkg / "intel_ui.test.mjs")
    r = subprocess.run(["node", "--test", "--test-reporter=tap", "intel_ui.test.mjs"], cwd=pkg, capture_output=True, text=True, timeout=300, check=False)
    assert r.returncode == 0, r.stdout + r.stderr
    assert "# pass 22" in r.stdout, r.stdout  # every scenario ran (a silently skipped file would also exit 0)
    assert "# fail 0" in r.stdout, r.stdout


# ------------------------------------------------------------------------------------------------------------------------------ static checks
def _routes() -> str:
    return (JS / "routes.js").read_text()


def test_intelligence_routes_nav_and_translations_agree() -> None:
    routes = _routes()
    for path in ("/intelligence", "/intelligence/catalog", "/intelligence/assets/:id", "/intelligence/lineage", "/intelligence/lineage/:id", "/intelligence/impact/:id", "/intelligence/discovery",
                 "/intelligence/governance", "/intelligence/governance/:section"):
        assert f"'{path}'" in routes, f"route {path} is not registered"
    i18n = (JS / "core" / "i18n.js").read_text()
    keys = re.findall(r"'((?:nav|group)\.intel\w*)'", routes)
    assert sorted(keys) == sorted(["group.intelligence", "nav.intel", "nav.intelCatalog", "nav.intelLineage", "nav.intelDiscovery", "nav.intelGovernance"]), keys
    for key in keys:
        assert re.search(rf"['\"]?{re.escape(key)}['\"]?\s*:", i18n), f"no translation for {key}"
    # every page module named by a route exists and exports the component the route asks for
    mods = set(re.findall(r"p\('(intel[a-z_]*\.js)'\)", routes))
    assert {"intelligence.js", "intel_asset.js", "intel_lineage.js", "intel_discovery.js", "intel_governance.js"} <= mods
    for mod in mods:
        assert (JS / "pages" / mod).exists(), mod
    for comp in ("IntelOverviewPage", "IntelCatalogPage", "AssetPage", "ImpactPage", "LineagePage", "DiscoveryPage", "GovernancePage"):
        assert comp in routes, f"{comp} is not routed"
        assert any(re.search(rf"export (function|const) {comp}\b", p.read_text()) for p in (JS / "pages").glob("intel*.js")), f"{comp} is not exported by a page module"
    index = (STATIC / "index.html").read_text()
    assert "css/intelligence.css" in index
    assert (STATIC / "css" / "intelligence.css").exists()


def test_intelligence_files_have_headers_and_no_placeholders() -> None:
    files = [*(JS / "intel").glob("*.js"), *(JS / "pages").glob("intel*.js"), STATIC / "css" / "intelligence.css"]
    assert len(files) >= 9
    for f in files:
        text = f.read_text()
        assert "SPDX-License-Identifier: Apache-2.0" in text[:400], f"{f.name}: missing SPDX header"
        assert "Justin Dodrill" in text[:400], f"{f.name}: missing author line"
        assert not re.search(r"\b(TODO|FIXME|XXX)\b", text), f"{f.name}: leftover marker"
        assert "console.log(" not in text, f"{f.name}: debug output"


def test_intelligence_pages_never_store_values_in_browser_storage() -> None:
    """Sample rows are read on demand and live only in component state: nothing the sample route returns may be written to localStorage / sessionStorage / IndexedDB."""
    for f in (JS / "pages").glob("intel*.js"):
        text = f.read_text()
        assert "localStorage" not in text and "sessionStorage" not in text and "indexedDB" not in text, f.name


def test_command_palette_and_rail_share_the_intelligence_gate() -> None:
    """Without the Intelligence module the rail group and the palette's page entries disappear together (both use ``hasRoute('/v1/intelligence/summary')``)."""
    shell = (JS / "shell.js").read_text()
    assert "hasRoute('/v1/intelligence/summary', 'get')" in shell and shell.count("hasRoute('/v1/intelligence/summary'") == 1
    assert "export function useHasIntelligence" in shell
    palette = shell[shell.index("export function Palette"):]
    assert "useHasIntelligence()" in palette and "g !== 'group.intelligence'" in palette.split("const lookup")[0]
    assert "useHasIntelligence()" in shell[shell.index("export function Rail"):shell.index("export function Palette")]


def test_confidence_wording_matches_what_the_code_does() -> None:
    """``verified`` is observed at run time by the platform; definitions (pipeline dependsOn, a report's model) are ``derived``."""
    model = (JS / "intel" / "model.js").read_text()
    help_ = model[model.index("CONF_HELP"):model.index("confLabel")]
    assert "authoritative" not in help_ and "run time" in help_
    overview = (JS / "pages" / "intelligence.js").read_text()
    assert "Verified means the platform observed it" in overview
