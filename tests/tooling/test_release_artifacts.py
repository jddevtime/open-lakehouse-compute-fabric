# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Release gate regressions: what a source archive may carry versus an installed wheel, the Fabric SDK distribution, and license-metadata precedence.

The archives here are small synthetic ones: the gate (``scripts/verify_dist.py``) is exercised on exactly the member names that matter, not on a full build
(``scripts/build_release.sh`` does that).
"""

from __future__ import annotations

import importlib.util
import io
import tarfile
import zipfile
from pathlib import Path
from types import ModuleType

import pytest

ROOT = Path(__file__).resolve().parents[2]


def load(name: str) -> ModuleType:
    spec = importlib.util.spec_from_file_location(f"{name}_under_test", ROOT / "scripts" / f"{name}.py")
    assert spec and spec.loader
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


verify_dist = load("verify_dist")
licenses = load("licenses")
VERSION = verify_dist.project_version()


# --------------------------------------------------------------------------------------------------------------- helpers
def make_tar(path: Path, members: dict[str, str]) -> Path:
    with tarfile.open(path, "w:gz") as tf:
        for name, body in members.items():
            data = body.encode()
            info = tarfile.TarInfo(name)
            info.size = len(data)
            tf.addfile(info, io.BytesIO(data))
    return path


def server_sdist(tmp_path: Path, extra: dict[str, str] | None = None) -> Path:
    top = f"olcf-{VERSION}/"
    members = {top + n: "x" for n in ("pyproject.toml", "LICENSE", "NOTICE", "README.md", "PKG-INFO", "src/olcf/cli/main.py", "schemas/capsule-v1.json")}
    members.update({top + n: b for n, b in (extra or {}).items()})
    return make_tar(tmp_path / f"olcf-{VERSION}.tar.gz", members)


def sdk_sdist(tmp_path: Path, extra: dict[str, str] | None = None, drop: tuple[str, ...] = ()) -> Path:
    top = f"olcf_fabric-{VERSION}/"
    members = {top + n: "x" for n in verify_dist.REQUIRED_SDK_SDIST}
    members = {k: v for k, v in members.items() if k[len(top):] not in drop}
    members.update({top + n: b for n, b in (extra or {}).items()})
    return make_tar(tmp_path / f"olcf_fabric-{VERSION}.tar.gz", members)


def sdk_wheel(tmp_path: Path, *, metadata_extra: str = "", drop: tuple[str, ...] = (), source: dict[str, str] | None = None, version: str = VERSION) -> Path:
    di = f"olcf_fabric-{version}.dist-info"
    header = "# SPDX-License-Identifier: Apache-2.0\n# Copyright (c) 2026 Justin Dodrill\n"
    members = {m: header + "x = 1\n" for m in verify_dist.REQUIRED_SDK_WHEEL}
    members.update(source or {})
    members.update({
        f"{di}/METADATA": ("Metadata-Version: 2.4\nName: olcf-fabric\nVersion: " + version + "\nLicense: Apache-2.0\nRequires-Python: >=3.10\n"
                           "License-File: LICENSE\nLicense-File: NOTICE\n" + metadata_extra + "\nreadme body\n"),
        f"{di}/WHEEL": "Wheel-Version: 1.0\nRoot-Is-Purelib: true\nTag: py3-none-any\n",
        f"{di}/RECORD": "", f"{di}/top_level.txt": "olcf_fabric\n",
        f"{di}/licenses/LICENSE": "license", f"{di}/licenses/NOTICE": "notice",
    })
    path = tmp_path / f"olcf_fabric-{version}-py3-none-any.whl"
    with zipfile.ZipFile(path, "w") as zf:
        for name, body in members.items():
            if name not in drop:
                zf.writestr(name, body)
    return path


# --------------------------------------------------------------------------------------- C06: source archive vs wheel rules
def test_source_archive_may_carry_tests_ci_files_and_the_public_env_example(tmp_path: Path) -> None:
    sdist = server_sdist(tmp_path, {
        "tests/unit/test_x.py": "pass", "tests/tooling/golden/unit.service": "[Unit]", ".github/workflows/ci.yml": "name: ci", ".github/CODEOWNERS": "* @x",
        "deploy/.env.example": "OLCF_MASTER_KEY=", "src/olcf/runtime/keys/olcf-dev.key": "public development key",
    })
    assert verify_dist.check_sdist(sdist, VERSION) == []


def test_the_same_members_are_refused_in_an_installed_wheel() -> None:
    names = ["olcf/cli/main.py", "tests/unit/test_x.py", ".github/workflows/ci.yml", "olcf/tests/test_y.py", "deploy/.env.example"]
    errs = verify_dist.check_names(names, (), "wheel")                       # source=False is the wheel rule
    assert {e.split(": ")[-1] for e in errs} == {"tests/unit/test_x.py", ".github/workflows/ci.yml", "olcf/tests/test_y.py", "deploy/.env.example"}
    assert verify_dist.check_names(names, (), "sdist", source=True) == []


@pytest.mark.parametrize("member", [
    "deploy/.env", "deploy/.env.local", "deploy/docker/.env.example", ".env", "src/olcf/id_rsa", "src/olcf/server.pem", "certs/worker.key", "tests/fixtures/prod.key",
    "olcf-data/olcf.db", "tests/tmp/x.sqlite3", "src/olcf/__pycache__/main.cpython-313.pyc", "tests/__pycache__/t.cpython-313-pytest-9.pyc", ".hypothesis/constants/ab12",
    ".pytest_cache/README.md", ".venv/bin/python", ".git/config", ".DS_Store", "deploy/.DS_Store",
])
def test_secret_and_cache_files_are_refused_even_in_a_source_archive(tmp_path: Path, member: str) -> None:
    errs = verify_dist.check_sdist(server_sdist(tmp_path, {member: "x"}), VERSION)
    assert any(member in e and "must not ship" in e for e in errs), errs


def test_only_the_exact_public_env_example_is_allowed(tmp_path: Path) -> None:
    ok = verify_dist.check_sdist(server_sdist(tmp_path, {"deploy/.env.example": "x"}), VERSION)
    assert ok == []
    bad = verify_dist.check_sdist(server_sdist(tmp_path, {"deploy/.env.example": "x", "deploy/.env.example.bak": "x", "other/.env.example": "x"}), VERSION)
    assert len(bad) == 2 and all("must not ship" in e for e in bad)


def test_the_public_development_key_is_the_only_key_allowed(tmp_path: Path) -> None:
    errs = verify_dist.check_sdist(server_sdist(tmp_path, {"src/olcf/runtime/keys/olcf-prod.key": "x"}), VERSION)
    assert any("olcf-prod.key" in e for e in errs)


def test_source_archive_must_still_have_license_readme_and_sources(tmp_path: Path) -> None:
    top = f"olcf-{VERSION}/"
    path = make_tar(tmp_path / f"olcf-{VERSION}.tar.gz", {top + "pyproject.toml": "x", top + "PKG-INFO": "x"})
    errs = verify_dist.check_sdist(path, VERSION)
    for need in ("LICENSE", "NOTICE", "README.md", "src/olcf/cli/main.py", "schemas/"):
        assert any(f"missing {need}" in e for e in errs), (need, errs)


# ------------------------------------------------------------------------------------------------------- C06: Fabric SDK
def test_sdk_and_server_versions_agree() -> None:
    assert verify_dist.sdk_version() == verify_dist.project_version() == verify_dist.module_version()


def test_sdk_wheel_gate_accepts_a_complete_wheel(tmp_path: Path) -> None:
    assert verify_dist.check_sdk_wheel(sdk_wheel(tmp_path), VERSION) == []


def test_sdk_wheel_gate_demands_the_license_and_notice(tmp_path: Path) -> None:
    di = f"olcf_fabric-{VERSION}.dist-info"
    errs = verify_dist.check_sdk_wheel(sdk_wheel(tmp_path, drop=(f"{di}/licenses/NOTICE", f"{di}/licenses/LICENSE")), VERSION)
    assert any("licenses/NOTICE" in e for e in errs) and any("licenses/LICENSE" in e for e in errs)


def test_sdk_wheel_gate_demands_required_modules(tmp_path: Path) -> None:
    errs = verify_dist.check_sdk_wheel(sdk_wheel(tmp_path, drop=("olcf_fabric/client.py", "olcf_fabric/__init__.py")), VERSION)
    assert any("missing olcf_fabric/client.py" in e for e in errs) and any("missing olcf_fabric/__init__.py" in e for e in errs)


def test_sdk_wheel_gate_rejects_dependencies_server_code_and_wrong_versions(tmp_path: Path) -> None:
    assert any("declares dependencies" in e for e in verify_dist.check_sdk_wheel(sdk_wheel(tmp_path, metadata_extra="Requires-Dist: httpx\n"), VERSION))
    header = "# SPDX-License-Identifier: Apache-2.0\n"
    assert any("imports the server package" in e for e in verify_dist.check_sdk_wheel(sdk_wheel(tmp_path, source={"olcf_fabric/api.py": header + "from olcf.core import x\n"}), VERSION))
    assert any("unexpected top-level" in e for e in verify_dist.check_sdk_wheel(sdk_wheel(tmp_path, source={"olcf/cli/main.py": header}), VERSION))
    assert any("SPDX" in e for e in verify_dist.check_sdk_wheel(sdk_wheel(tmp_path, source={"olcf_fabric/api.py": "x = 1\n"}), VERSION))
    wrong = sdk_wheel(tmp_path, version="9.9.9")
    assert any("expected olcf_fabric-" in e for e in verify_dist.check_sdk_wheel(wrong, VERSION))


def test_sdk_sdist_gate(tmp_path: Path) -> None:
    assert verify_dist.check_sdk_sdist(sdk_sdist(tmp_path), VERSION) == []
    assert any("missing NOTICE" in e for e in verify_dist.check_sdk_sdist(sdk_sdist(tmp_path, drop=("NOTICE",)), VERSION))
    assert any("must not ship" in e for e in verify_dist.check_sdk_sdist(sdk_sdist(tmp_path, {"olcf_fabric/__pycache__/api.pyc": "x"}), VERSION))


def test_gate_requires_the_sdk_when_asked(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    sdk_sdist(tmp_path)
    assert verify_dist.main(["verify_dist.py", str(tmp_path)]) == 0
    assert verify_dist.main(["verify_dist.py", str(tmp_path), "--require-sdk"]) == 1
    assert "required but not both found" in capsys.readouterr().err


def test_sdk_ships_the_apache_license_and_notice() -> None:
    sdk = ROOT / "sdk" / "fabric"
    assert (sdk / "LICENSE").read_text(encoding="utf-8") == (ROOT / "LICENSE").read_text(encoding="utf-8")
    notice = (sdk / "NOTICE").read_text(encoding="utf-8")
    assert "Copyright 2026 Justin Dodrill" in notice and "Apache License, Version 2.0" in notice
    pyproject = (sdk / "pyproject.toml").read_text(encoding="utf-8")
    assert 'license-files = ["LICENSE", "NOTICE"]' in pyproject and 'license = { text = "Apache-2.0" }' in pyproject


def test_build_script_builds_both_distributions_and_never_deletes_the_output_directory() -> None:
    script = (ROOT / "scripts" / "build_release.sh").read_text(encoding="utf-8")
    assert 'uv build --out-dir "$OUT" sdk/fabric' in script and "--require-sdk" in script
    assert "rm -rf \"$OUT\"" not in script and "is not empty" in script
    assert "--require-identity" in script


def test_build_script_refuses_a_non_empty_output_directory(tmp_path: Path) -> None:
    import shutil
    import subprocess

    if shutil.which("uv") is None or shutil.which("bash") is None:
        pytest.skip("needs uv and bash")
    (tmp_path / "keep.txt").write_text("precious")
    res = subprocess.run(["bash", str(ROOT / "scripts" / "build_release.sh"), "--out", str(tmp_path)], capture_output=True, text=True, timeout=120, check=False)  # noqa: S603
    assert res.returncode == 1 and "is not empty" in res.stderr, res.stderr
    assert (tmp_path / "keep.txt").read_text() == "precious"


# ------------------------------------------------------------------------------------------- C07: license metadata fallback
ISC = "License :: OSI Approved :: ISC License (ISCL)"


@pytest.mark.parametrize("placeholder", ["UNKNOWN", "unknown", "NONE", "None", "NOASSERTION", "  UNKNOWN  "])
def test_placeholder_license_field_falls_through_to_the_classifier(placeholder: str) -> None:
    assert licenses.resolve_license("ptyprocess", "", placeholder, [ISC, "Programming Language :: Python :: 3"]) == "ISC"
    assert licenses.resolve_license("x", placeholder, "", [ISC]) == "ISC"                    # same for License-Expression


def test_a_real_license_field_still_wins_over_classifiers() -> None:
    assert licenses.resolve_license("pkg", "", "MIT", [ISC]) == "MIT"
    assert licenses.resolve_license("pkg", "Apache-2.0", "UNKNOWN", [ISC]) == "Apache-2.0"


def test_unidentified_licenses_still_fail_the_check() -> None:
    assert licenses.resolve_license("mystery", "", "UNKNOWN", []) == ""
    assert licenses.resolve_license("mystery", "", "NOASSERTION", ["Programming Language :: Python"]) == ""
    pkgs = [{"name": "mystery", "version": "1", "license": "UNKNOWN"}]                         # what collect_here() records for an empty result
    errors, _ = licenses.problems(pkgs)
    assert errors and "no license metadata" in errors[0]


def test_curated_table_is_the_last_resort() -> None:
    assert licenses.resolve_license("certifi", "", "UNKNOWN", []) == "MPL-2.0"


def test_collect_reads_a_ptyprocess_shaped_distribution(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    di = tmp_path / "ptyprocess_fixture-0.7.0.dist-info"
    di.mkdir()
    (di / "METADATA").write_text(f"Metadata-Version: 2.1\nName: ptyprocess-fixture\nVersion: 0.7.0\nLicense: UNKNOWN\nClassifier: {ISC}\n", encoding="utf-8")
    monkeypatch.syspath_prepend(str(tmp_path))
    found = {p["name"]: p for p in licenses.collect_here()}
    assert found["ptyprocess-fixture"]["license"] == "ISC"
