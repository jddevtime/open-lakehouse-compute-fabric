# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""The release identity preflight (``scripts/check_release_config.py``): passes on a resolved tree, fails on owner/repository/contact placeholders."""

from __future__ import annotations

import importlib.util
import shutil
import subprocess
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
SCRIPT = ROOT / "scripts" / "check_release_config.py"
_spec = importlib.util.spec_from_file_location("check_release_config_under_test", SCRIPT)
assert _spec and _spec.loader
cfg = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(cfg)

OWNER, REPO = "acme-labs", "open-lakehouse"
RESOLVED = {
    "README.md": f"# OLCF\n\ngit clone https://github.com/{OWNER}/{REPO}.git && cd {REPO}\n",
    "CONTRIBUTING.md": f"Clone https://github.com/{OWNER}/{REPO}.git\n",
    "CITATION.cff": f'repository-code: "https://github.com/{OWNER}/{REPO}"\n',
    "SECURITY.md": "Report privately to security@acme.example\n",
    "CODE_OF_CONDUCT.md": "Report to conduct@acme.example\n",
    ".github/CODEOWNERS": f"# owners\n*  @{OWNER}\n/deploy/  @{OWNER}/platform-team\n",
    ".github/ISSUE_TEMPLATE/config.yml": f"contact_links:\n  - url: https://github.com/{OWNER}/{REPO}/security/policy\n",
    "CHANGELOG.md": f"[Unreleased]: https://github.com/{OWNER}/{REPO}/compare/v0.1.0...HEAD\n",
}


def write_tree(root: Path, files: dict[str, str]) -> Path:
    for name, body in files.items():
        path = root / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(body, encoding="utf-8")
    return root


def run(root: Path, *extra: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run([sys.executable, str(SCRIPT), "--root", str(root), *extra], capture_output=True, text=True, check=False)  # noqa: S603


def test_resolved_tree_passes(tmp_path: Path) -> None:
    res = run(write_tree(tmp_path, RESOLVED))
    assert res.returncode == 0, res.stderr
    assert "ok" in res.stderr


@pytest.mark.parametrize(("file", "bad"), [
    ("README.md", "git clone https://github.com/<github-owner>/<repository>.git\n"),
    ("CONTRIBUTING.md", "https://github.com/<github-owner>/<repository>\n"),
    ("CITATION.cff", 'repository-code: "https://github.com/<github-owner>/<repository>"\n'),
    ("SECURITY.md", "> `<security contact email - to be set by maintainer>`\n"),
    ("CODE_OF_CONDUCT.md", "reported at <conduct contact email - to be set by maintainer>\n"),
    (".github/CODEOWNERS", "*  @<github-owner>\n"),
    (".github/ISSUE_TEMPLATE/config.yml", "url: https://github.com/<github-owner>/<repository>/security/policy\n"),
    ("CHANGELOG.md", "[Unreleased]: https://github.com/<github-owner>/<repository>/compare/v0.1.0...HEAD\n"),
    ("docs/development/setup.md", "git clone https://github.com/<github-owner>/<repository>.git\n"),
])
def test_placeholder_fails_and_names_the_file(tmp_path: Path, file: str, bad: str) -> None:
    files = dict(RESOLVED)
    files[file] = bad
    res = run(write_tree(tmp_path, files))
    assert res.returncode == 1
    assert file in res.stderr and "incomplete" in res.stderr
    others = [f for f in RESOLVED if f != file]
    assert not any(f"  {o}\n" in res.stderr for o in others)             # only the offending file is listed


def test_all_unresolved_files_are_listed_together(tmp_path: Path) -> None:
    files = {k: v.replace(OWNER, "<github-owner>").replace(REPO, "<repository>") for k, v in RESOLVED.items()}
    res = run(write_tree(tmp_path, files))
    assert res.returncode == 1
    for name in ("README.md", "CONTRIBUTING.md", "CITATION.cff", ".github/CODEOWNERS", ".github/ISSUE_TEMPLATE/config.yml"):
        assert f"  {name}\n" in res.stderr


def test_a_contact_document_without_an_address_fails(tmp_path: Path) -> None:
    files = dict(RESOLVED)
    files["SECURITY.md"] = "Report privately to the maintainer.\n"
    res = run(write_tree(tmp_path, files))
    assert res.returncode == 1 and "no contact e-mail address" in res.stderr


@pytest.mark.parametrize("owner_line", ["*  @bad owner!", "*  not-an-owner", "*"])
def test_invalid_codeowners_fail(tmp_path: Path, owner_line: str) -> None:
    files = dict(RESOLVED)
    files[".github/CODEOWNERS"] = owner_line + "\n"
    assert run(write_tree(tmp_path, files)).returncode == 1


def test_secondary_templates_are_notes_unless_strict(tmp_path: Path) -> None:
    files = dict(RESOLVED)
    files["docs/verify.md"] = "cosign ... https://github.com/<OWNER>/<REPO>/ and ghcr.io/<owner>/olcf\n"
    root = write_tree(tmp_path, files)
    res = run(root)
    assert res.returncode == 0 and "not blocking" in res.stderr
    assert run(root, "--strict").returncode == 1


def test_it_only_reads(tmp_path: Path) -> None:
    root = write_tree(tmp_path, {**RESOLVED, "README.md": "<github-owner>\n"})
    before = {p: p.read_bytes() for p in root.rglob("*") if p.is_file()}
    run(root)
    assert {p: p.read_bytes() for p in root.rglob("*") if p.is_file()} == before


def test_tests_and_caches_are_not_scanned(tmp_path: Path) -> None:
    files = {**RESOLVED, "tests/fixture.md": "<github-owner>\n", ".venv/lib/x.md": "<github-owner>\n", "node_modules/a/b.md": "<repository>\n"}
    assert run(write_tree(tmp_path, files)).returncode == 0


def test_repository_contacts_are_set() -> None:
    """The conduct and security contacts are configured (C10); owner/repository placeholders are the remaining blocker (R01) until the real names are applied."""
    failures, _ = cfg.scan(ROOT)
    assert "SECURITY.md" not in failures and "CODE_OF_CONDUCT.md" not in failures
    assert "jdsql@hotmail.com" in (ROOT / "SECURITY.md").read_text(encoding="utf-8")
    assert "jdsql@hotmail.com" in (ROOT / "CODE_OF_CONDUCT.md").read_text(encoding="utf-8")
    assert "private vulnerability reporting" in (ROOT / "SECURITY.md").read_text(encoding="utf-8")


def test_build_script_runs_the_preflight_as_warning_unless_required(tmp_path: Path) -> None:
    script = (ROOT / "scripts" / "build_release.sh").read_text(encoding="utf-8")
    assert "check_release_config.py" in script and "--require-identity" in script and "warning: release identity" in script
    workflow = (ROOT / ".github" / "workflows" / "release.yml").read_text(encoding="utf-8")
    assert "--require-identity" in workflow
    assert shutil.which("bash") is None or subprocess.run(["bash", "-n", str(ROOT / "scripts" / "build_release.sh")], check=False).returncode == 0  # noqa: S603
