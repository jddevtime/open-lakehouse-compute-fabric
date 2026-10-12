# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
# ruff: noqa: S104, S314, S603, S607
"""Service definition generators: golden files, structural validity and quoting edge cases (the files are what ``olcf service install`` and ``deploy/`` ship)."""

from __future__ import annotations

import plistlib
import shutil
import subprocess
import xml.etree.ElementTree as ET
from pathlib import Path

import pytest

from olcf.cli import service as svc

GOLDEN = Path(__file__).parent / "golden"
DEPLOY = Path(__file__).resolve().parents[2] / "deploy"


def _spec(**kw):
    base = dict(command=["/opt/olcf/current/bin/olcf", "--data-dir", "/home/u/.olcf", "serve", "--port", "8470"], data_dir=Path("/home/u/.olcf"),
                log_dir=Path("/home/u/.olcf/logs"), env={"OLCF_DATA_DIR": "/home/u/.olcf"})
    base.update(kw)
    return svc.ServiceSpec(**base)


def test_launchd_golden_and_valid_plist():
    out = svc.render_launchd(_spec())
    assert out == (GOLDEN / "launchd.plist").read_bytes()
    doc = plistlib.loads(out)
    assert doc["Label"] == "org.olcf.server" and doc["RunAtLoad"] is True
    assert doc["ProgramArguments"][0].endswith("/olcf")
    assert doc["KeepAlive"] == {"SuccessfulExit": False}                  # restart after a crash, not after a clean `olcf stop`
    assert "/usr/bin" in doc["EnvironmentVariables"]["PATH"]


def test_systemd_user_unit_golden():
    assert svc.render_systemd(_spec()) == (GOLDEN / "systemd-user.service").read_text()


def test_systemd_system_unit_golden_and_hardening():
    spec = _spec(command=["/opt/olcf/current/bin/olcf", "--data-dir", "/var/lib/olcf", "serve", "--host", "0.0.0.0", "--port", "8470"], data_dir=Path("/var/lib/olcf"),
                 log_dir=Path("/var/log/olcf"), env={}, system=True, user="olcf", group="olcf")
    text = svc.render_systemd(spec)
    assert text == (GOLDEN / "systemd-system.service").read_text()
    for line in ("User=olcf", "Group=olcf", "ProtectSystem=strict", "NoNewPrivileges=true", "ReadWritePaths=/var/lib/olcf", "WantedBy=multi-user.target"):
        assert line in text.splitlines()


def test_windows_task_golden_and_wellformed():
    spec = _spec(command=["C:\\Users\\Justin Dodrill\\AppData\\Local\\olcf\\current\\Scripts\\olcf.exe", "--data-dir", "C:\\Users\\Justin Dodrill\\.olcf", "serve", "--port", "8470"],
                 data_dir=Path("C:/Users/Justin Dodrill/.olcf"), log_dir=Path("C:/logs"))
    xml = svc.render_windows_task(spec, user="DESKTOP\\justin")
    assert xml == (GOLDEN / "windows-task.xml").read_text()
    root = ET.fromstring(xml.split("?>", 1)[1])
    ns = {"t": "http://schemas.microsoft.com/windows/2004/02/mit/task"}
    assert root.find("t:Actions/t:Exec/t:Command", ns).text.endswith("olcf.exe")
    args = root.find("t:Actions/t:Exec/t:Arguments", ns).text
    assert '"C:\\Users\\Justin Dodrill\\.olcf"' in args               # a path with a space is quoted
    assert root.find("t:Principals/t:Principal/t:RunLevel", ns).text == "LeastPrivilege"      # never needs administrator rights


def test_systemd_quoting_and_specifiers():
    text = svc.render_systemd(_spec(command=["/opt/my olcf/bin/olcf", "--data-dir", "/data/50%/x", "serve"], env={"OLCF_X": 'a "quoted" value'}))
    assert 'ExecStart="/opt/my olcf/bin/olcf" --data-dir "/data/50%%/x" serve' in text
    assert 'Environment="OLCF_X=a \\"quoted\\" value"' in text


def test_no_secrets_in_generated_files():
    for rendered in (svc.render_systemd(_spec()), svc.render_launchd(_spec()).decode(), svc.render_windows_task(_spec())):
        assert "olcf_pat_" not in rendered and "password" not in rendered.lower()


def test_serve_command_builder():
    cmd = svc.serve_command(["/x/olcf"], Path("/d"), host="0.0.0.0", port=9000, extra=["--no-embedded-worker"])
    assert cmd == ["/x/olcf", "--data-dir", "/d", "serve", "--host", "0.0.0.0", "--port", "9000", "--no-embedded-worker"]


def test_locations():
    home = Path("/h")
    assert svc.launchd_path(home) == Path("/h/Library/LaunchAgents/org.olcf.server.plist")
    assert svc.systemd_path(home, False) == Path("/h/.config/systemd/user/olcf.service")
    assert svc.systemd_path(home, True) == Path("/etc/systemd/system/olcf.service")


@pytest.mark.skipif(shutil.which("systemd-analyze") is None, reason="systemd-analyze not installed")
def test_systemd_analyze_accepts_user_unit(tmp_path):
    unit = tmp_path / "olcf.service"
    unit.write_text(svc.render_systemd(_spec(command=["/bin/true"])))
    res = subprocess.run(["systemd-analyze", "verify", str(unit)], capture_output=True, text=True, check=False)  # noqa: S603, S607
    assert "Unknown" not in res.stderr


# --- hand-written service files under deploy/ (present once the deploy package exists)
@pytest.mark.skipif(not (DEPLOY / "systemd").exists(), reason="deploy/systemd not generated yet")
def test_deploy_service_files_parse():
    for unit in (DEPLOY / "systemd").glob("*.service"):
        text = unit.read_text()
        assert "[Service]" in text and "ExecStart=" in text and "NoNewPrivileges=true" in text, unit
    for plist in (DEPLOY / "launchd").glob("*.plist"):
        assert plistlib.loads(plist.read_bytes())["Label"]
    for xml in (DEPLOY / "windows").glob("*.xml"):
        ET.fromstring(xml.read_text(encoding="utf-8").split("?>", 1)[-1])
