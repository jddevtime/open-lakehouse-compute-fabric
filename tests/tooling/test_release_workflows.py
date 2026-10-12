# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Structure of the release automation (``.github/workflows``) and of the secret-scanning configuration.

GitHub Actions cannot run here, so these tests pin the properties the review asked for; only a repository dry run proves the behaviour
(``docs/development/release-readiness.md``, R02).
"""

from __future__ import annotations

import tomllib
from pathlib import Path
from typing import Any

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[2]
WF = ROOT / ".github" / "workflows"


def load(name: str) -> dict[str, Any]:
    doc = yaml.safe_load((WF / name).read_text(encoding="utf-8"))
    doc["_on"] = doc.pop(True, doc.get("on"))               # PyYAML reads the key `on` as the boolean True
    return doc  # type: ignore[no-any-return]


def steps_text(job: dict[str, Any]) -> str:
    return "\n".join(str(s.get("run", "")) + str(s.get("with", "")) for s in job.get("steps", []))


@pytest.mark.parametrize("name", ["ci.yml", "container.yml", "security.yml"])
def test_gate_workflows_are_reusable_and_keep_their_triggers(name: str) -> None:
    doc = load(name)
    assert "workflow_call" in doc["_on"] and "workflow_dispatch" in doc["_on"]
    assert "pull_request" in doc["_on"] or "push" in doc["_on"]
    group = doc["concurrency"]["group"]
    assert "github.workflow" in group, "a caller must not share a concurrency group with the callee"
    assert "github.workflow ==" in str(doc["concurrency"]["cancel-in-progress"]), "only the stand-alone run cancels superseded runs"


def test_release_build_needs_all_three_gates() -> None:
    rel = load("release.yml")
    jobs = rel["jobs"]
    called = {"verify": "ci.yml", "containers": "container.yml", "security": "security.yml"}
    for job, wf in called.items():
        assert jobs[job]["uses"] == f"./.github/workflows/{wf}"
    assert set(jobs["build"]["needs"]) == set(called)
    assert jobs["security"]["permissions"]["security-events"] == "write"      # CodeQL in the called workflow
    group = rel["concurrency"]["group"]
    assert "github.workflow" in group and not group.startswith(("ci-", "container-", "security-"))


def test_release_script_runs_under_bash_with_the_identity_gate() -> None:
    for name in ("release.yml", "ci.yml"):
        text = (WF / name).read_text(encoding="utf-8")
        assert "sh scripts/build_release.sh" not in text.replace("bash scripts/build_release.sh", "")
        assert "bash scripts/build_release.sh --out dist" in text
    assert "--require-identity" in (WF / "release.yml").read_text(encoding="utf-8")


def test_pip_audit_does_not_resolve_dependencies() -> None:
    text = (WF / "security.yml").read_text(encoding="utf-8")
    assert "pip-audit --strict --disable-pip --no-deps" in text


def test_releases_are_drafts_and_zero_versions_are_prereleases() -> None:
    jobs = load("release.yml")["jobs"]
    release = next(s for s in jobs["release"]["steps"] if "action-gh-release" in str(s.get("uses")))["with"]
    assert release["draft"] is True
    assert release["prerelease"] == "${{ startsWith(github.ref_name, 'v0.') || contains(github.ref_name, '-') }}"
    notes = steps_text(jobs["release"])
    assert "releases/download/@TAG@/olcf-@VERSION@-py3-none-any.whl" in notes and "olcf_fabric-@VERSION@" in notes
    assert "Beta for evaluation on trusted infrastructure" in notes and "no OS sandbox" in notes
    assert "pypi" not in jobs, "PyPI publication lives in pypi.yml, after a human publishes the draft"


def test_latest_tag_only_from_the_explicit_stable_rule() -> None:
    images = load("release.yml")["jobs"]["images"]
    meta = next(s for s in images["steps"] if s.get("id") == "meta")["with"]
    assert meta["flavor"].strip() == "latest=false"
    latest = [ln for ln in meta["tags"].splitlines() if "value=latest" in ln]
    assert len(latest) == 1 and "!startsWith(github.ref_name, 'v0.')" in latest[0] and "!contains(github.ref_name, '-')" in latest[0]
    assert images["environment"] == "release"
    sign = next(s for s in images["steps"] if "sign the image" in str(s.get("name")))
    assert "tr '[:upper:]' '[:lower:]'" in sign["run"]


def test_signing_and_images_require_a_tag_and_attestation_subjects_are_separate_patterns() -> None:
    jobs = load("release.yml")["jobs"]
    for job in ("sign", "images", "release"):
        assert "startsWith(github.ref, 'refs/tags/v')" in jobs[job]["if"]
    attest = next(s for s in jobs["sign"]["steps"] if "attest-build-provenance" in str(s.get("uses")))["with"]["subject-path"]
    assert "," not in attest and attest.splitlines() == ["dist/*.whl", "dist/*.tar.gz"]


def test_pypi_workflow_publishes_only_reviewed_assets_for_both_packages() -> None:
    doc = load("pypi.yml")
    assert doc["_on"] == {"release": {"types": ["published"]}}
    job = doc["jobs"]["publish"]
    assert job["if"] == "vars.PUBLISH_PYPI == 'true'"
    assert job["environment"]["name"] == "pypi" and job["permissions"]["id-token"] == "write"
    text = (WF / "pypi.yml").read_text(encoding="utf-8")
    assert "gh release download" in text and "sha256sum -c" in text
    uploads = [s["with"]["packages-dir"] for s in job["steps"] if "gh-action-pypi-publish" in str(s.get("uses"))]
    assert uploads == ["pypi/olcf", "pypi/olcf-fabric"], "one trusted-publisher upload per project"
    assert "olcf_fabric-${version}-py3-none-any.whl" in text and "olcf-${version}.tar.gz" in text


def test_gitleaks_config_extends_defaults_with_a_narrow_allowlist() -> None:
    cfg = tomllib.loads((ROOT / ".gitleaks.toml").read_text(encoding="utf-8"))
    assert cfg["extend"] == {"useDefault": True}
    allow = cfg["allowlist"]
    assert set(allow["paths"]) == {r"(^|/)__pycache__/", r"\.pyc$"}, "no blanket exclusion of tests or source"
    assert len(allow["regexes"]) == 1 and allow["regexes"][0].startswith("MC4CAQAw")
    assert "rules" not in cfg and "gitleaks" in (WF / "security.yml").read_text(encoding="utf-8")
    assert "--config .gitleaks.toml" in (WF / "security.yml").read_text(encoding="utf-8")


def test_gitignore_covers_local_keys_env_files_and_tool_caches() -> None:
    lines = (ROOT / ".gitignore").read_text(encoding="utf-8").split()
    for need in ("*.pem", "*.key", ".env", ".env.*", "!.env.example", ".DS_Store", ".hypothesis/", "!src/olcf/runtime/keys/olcf-dev.key"):
        assert need in lines, need
