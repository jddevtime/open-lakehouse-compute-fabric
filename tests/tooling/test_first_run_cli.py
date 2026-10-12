# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
# ruff: noqa: S105, S106
"""First-run behaviour of the CLI (FIRSTRUN): errors are one clear line with a hint instead of a Python traceback, ``quickstart`` starts the server and the sample first,
survives a stale ``current`` context and a stale token, and can be run twice; a queued job tells the user why it waits."""

from __future__ import annotations

import json
import subprocess
import sys

import click
import pytest

from tests.tooling.conftest import ROOT, cli_env, free_port

# --------------------------------------------------------------------------- clean errors


def _cli(env, *args, timeout=120):
    return subprocess.run([sys.executable, "-m", "olcf.cli.main", *args], env=env, capture_output=True, text=True, timeout=timeout, check=False)


@pytest.fixture()
def env(tmp_path):
    """A bare data directory has no config.toml and is therefore "production" (the development runtime-signing key is not trusted): these tests state
    ``OLCF_ENVIRONMENT=development`` explicitly. The production behaviour and the ``olcf init`` / ``quickstart`` path are covered further down."""
    return {**cli_env(tmp_path, tmp_path / "data"), "OLCF_ENVIRONMENT": "development"}


@pytest.mark.parametrize("args", [
    ("runtime", "install", "nope"),
    ("runtime", "verify", "nope"),
    ("runtime", "repair", "nope"),
    ("runtime", "retire", "nope"),
    ("runtime", "rollback", "nope"),
    ("runtime", "export-bundle", "nope", "-o", "x.tgz"),
    ("runtime", "import-bundle", "BUNDLE"),
    ("runtime", "install", "--bundle", "BUNDLE"),
    ("runtime", "install", "olcf-host-python"),
    ("runtime", "retire", "olcf-host-python"),
    ("worker", "list"),
    ("status",),
])
def test_bad_input_is_a_clear_error_not_a_traceback(env, tmp_path, args):
    bundle = tmp_path / "garbage.tgz"
    bundle.write_text("not a bundle")
    args = tuple(str(bundle) if a == "BUNDLE" else a for a in args)
    res = _cli({**env, "OLCF_URL": "http://127.0.0.1:1"}, *args)
    assert res.returncode != 0
    assert "Traceback" not in res.stderr + res.stdout, res.stderr
    assert "error:" in res.stderr, res.stderr


def test_unknown_runtime_lists_the_valid_ids(env):
    res = _cli(env, "runtime", "install", "olcf-rt-9.9.9")
    assert res.returncode == 1
    assert "error: no runtime named 'olcf-rt-9.9.9'" in res.stderr and "hint:" in res.stderr
    assert "olcf-rt-2.0.0" in res.stderr and "olcf-rt-1.3.0" in res.stderr
    assert "installing runtime" not in res.stderr, "nothing may be announced before the id is checked"


def test_runtime_list_shows_host_python_as_ready_without_spark(env):
    res = _cli(env, "--json", "runtime", "list")
    assert res.returncode == 0, res.stderr
    rows = {r["id"]: r for r in json.loads(res.stdout)["runtimes"]}
    assert rows["olcf-host-python"]["state"] == "ready" and rows["olcf-host-python"]["family"] == "host-python" and rows["olcf-host-python"]["spark"] == "no Spark"
    assert rows["olcf-rt-2.0.0"]["state"] == "not installed"
    human = _cli(env, "runtime", "list")
    assert "olcf-host-python" in human.stdout and "no Spark" in human.stdout


def test_olcf_errors_with_a_remediation_render_as_error_and_hint(monkeypatch):
    from click.testing import CliRunner

    from olcf.cli.main import cli
    from olcf.core.errors import Unsupported

    @click.command("boom-test")
    @click.option("--kind", default="olcf")
    def boom(kind):
        if kind == "olcf":
            raise Unsupported("the thing cannot be done", remediation="do the other thing")
        raise OSError("disk exploded")

    cli.add_command(boom)
    try:
        r = CliRunner().invoke(cli, ["boom-test"])
        assert r.exit_code == 1 and "error: the thing cannot be done" in r.output and "hint:  do the other thing" in r.output and "Traceback" not in r.output
        r = CliRunner().invoke(cli, ["boom-test", "--kind", "os"])
        assert r.exit_code == 1 and "OSError: disk exploded" in r.output and "-v" in r.output
        with pytest.raises(Unsupported):  # -v shows the traceback
            CliRunner().invoke(cli, ["-v", "boom-test"], catch_exceptions=False)
    finally:
        cli.commands.pop("boom-test", None)


# --------------------------------------------------------------------------- queued jobs explain themselves


def test_job_status_and_timeline_show_why_a_job_waits(live_server, tmp_path):
    """The shared server has no worker at all: the job stays queued and says so (code, sentence and what to do) in ``job status``, ``job get`` and the timeline."""
    import time

    live_server.json("workspace", "create", "qr-ws")
    src = tmp_path / "waits.py"
    src.write_text("print(1)\n")
    sub = live_server.json("job", "submit", "-w", "qr-ws", "--kind", "script", "--engine", "python", "--name", "waits", "--file", str(src))
    jid = sub["id"]
    deadline = time.monotonic() + 20
    j = {}
    while time.monotonic() < deadline:
        j = live_server.json("job", "get", jid)
        if j.get("queue_reason"):
            break
        time.sleep(0.3)
    assert j["state"] == "queued" and j["queue_reason"]["code"] == "no_node", j
    res = live_server.invoke("job", "status", jid)
    assert res.exit_code == 1 and "waiting: [no_node]" in res.output and "fix:" in res.output
    tl = live_server.invoke("job", "timeline", jid)
    assert "queue_reason" in tl.output and "no_node" in tl.output
    live_server.invoke("job", "cancel", jid)


# --------------------------------------------------------------------------- quickstart


def _quickstart(env, port, *extra, timeout=300):
    return _cli(env, "--data-dir", env["OLCF_DATA_DIR"], "quickstart", "--no-runtime", "--no-open", "--port", str(port), *extra, timeout=timeout)


def _stop_quietly(env):
    _cli(env, "--data-dir", env["OLCF_DATA_DIR"], "stop", "--force", timeout=60)


def test_quickstart_is_idempotent_and_repairs_a_stale_context_and_token(tmp_path):
    port = free_port()
    env = cli_env(tmp_path, tmp_path / "data")
    cfg = tmp_path / "cli-config"
    cfg.mkdir(parents=True)
    # a previous installation: the "current" context points to a server that is gone, and a stale 'local' login for the URL this quickstart will use
    (cfg / "contexts.json").write_text(json.dumps({"current": "prod", "contexts": {
        "prod": {"url": "http://127.0.0.1:1", "token": "olcf_pat_dead_beef"},
        "local": {"url": f"http://127.0.0.1:{port}", "token": "olcf_pat_abcd_stale0000000000000000000000000000"}}}))
    try:
        first = _quickstart(env, port)
        assert first.returncode == 0, first.stdout + first.stderr
        out = first.stdout
        assert "OLCF is running at" in out and "Next steps:" in out and "Sample Retail" in out
        assert f"http://127.0.0.1:{port}/#/dashboards/itm_" in out, "a direct link to the sample dashboard"
        assert 'olcf notebook run -w "Sample Retail" 02_Retail_Explore_Python' in out and "olcf mcp config" in out
        assert "password" in out.lower(), "the first run shows the administrator password once"
        # the CLI works against the new server without any flags: the stale token and the dead 'current' context did not get in the way
        who = _cli(env, "-c", "local", "--json", "workspace", "list")
        assert who.returncode == 0, who.stderr
        assert "Sample Retail" in who.stdout
        # the runtime follow-up is optional: --no-runtime says how to add it later
        assert "olcf runtime install" in out
        # second run: nothing is created twice, no new password, the server keeps running
        second = _quickstart(env, port)
        assert second.returncode == 0, second.stdout + second.stderr
        assert "OLCF is running at" in second.stdout and "(unchanged)" in second.stdout
        assert "Sign in to the web UI with (shown once" not in second.stdout
    finally:
        _stop_quietly(env)


def test_quickstart_reports_a_failed_runtime_install_as_a_warning_and_keeps_going(tmp_path):
    """Spark runtime install fails (unreachable Maven mirror / no network): the platform is already up, the failure is a warning with the retry command."""
    port = free_port()
    env = cli_env(tmp_path, tmp_path / "data")
    env.update({"OLCF_MAVEN_REPOSITORIES": "http://127.0.0.1:1/maven", "PIP_INDEX_URL": "http://127.0.0.1:1/simple", "PIP_RETRIES": "0", "PIP_DEFAULT_TIMEOUT": "2",
                "HTTPS_PROXY": "", "HTTP_PROXY": "", "https_proxy": "", "http_proxy": ""})
    try:
        res = _cli(env, "--data-dir", env["OLCF_DATA_DIR"], "quickstart", "--no-open", "--no-sample", "--no-jdk", "--port", str(port), timeout=600)
        assert res.returncode == 0, res.stdout + res.stderr
        out = res.stdout + res.stderr
        assert "OLCF is running at" in res.stdout
        assert out.index("OLCF is running at") < out.index("could not be installed"), "the server is reported before the runtime install is attempted"
        assert "Retry later with:  olcf runtime install olcf-rt-2.0.0" in out
        assert "Maven" in out and "Traceback" not in out
    finally:
        _stop_quietly(env)


def test_default_context_is_not_hijacked_by_quickstart(tmp_path):
    """An existing, working 'current' context stays current; quickstart only talks to the local server through its own 'local' context."""
    port = free_port()
    env = cli_env(tmp_path, tmp_path / "data")
    cfg = tmp_path / "cli-config"
    cfg.mkdir(parents=True)
    (cfg / "contexts.json").write_text(json.dumps({"current": "work", "contexts": {"work": {"url": "https://olcf.example.com", "token": "olcf_pat_x_y"}}}))
    try:
        assert _quickstart(env, port, "--no-sample").returncode == 0
        doc = json.loads((cfg / "contexts.json").read_text())
        assert doc["current"] == "work" and "local" in doc["contexts"] and doc["contexts"]["local"]["url"] == f"http://127.0.0.1:{port}"
    finally:
        _stop_quietly(env)


def test_repo_root_marker():
    assert (ROOT / "pyproject.toml").is_file()
