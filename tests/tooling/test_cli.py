# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
# ruff: noqa: S105, S106
"""The ``olcf`` command line against a real server process (``olcf init`` + ``olcf serve``, no embedded worker, no Spark)."""

from __future__ import annotations

import csv
import io
import json
import os
import stat
import sys
import zipfile
from pathlib import Path

import pytest

from tests.tooling.conftest import ROOT

EXAMPLES = ROOT / "examples"


@pytest.fixture(scope="module")
def ws(live_server) -> str:
    live_server.json("workspace", "create", "cli-ws")
    return "cli-ws"


# --------------------------------------------------------------------------- basics without a server
def test_version_and_help(live_server):
    res = live_server.invoke("version")
    assert res.exit_code == 0 and "olcf" in res.output.lower()
    j = live_server.json("version")
    assert j["olcf"] and j["python"]
    top = live_server.invoke("--help")
    assert top.exit_code == 0 and "Exit codes:" in top.output
    for cmd in ("init", "serve", "quickstart", "workspace", "job", "sql", "worker", "runtime", "fabric", "mcp", "backup", "db", "service", "completion", "upgrade"):
        assert cmd in top.output, cmd


def test_every_command_has_help(live_server):
    from olcf.cli.main import cli

    def walk(cmd, path):
        yield path
        for name, sub in getattr(cmd, "commands", {}).items():
            yield from walk(sub, [*path, name])

    seen = 0
    for path in walk(cli, []):
        res = live_server.invoke(*path, "--help")
        assert res.exit_code == 0, path
        seen += 1
    assert seen > 150


def test_unknown_command_is_usage_error(live_server):
    res = live_server.run("no-such-command")
    assert res.returncode == 2


def test_not_authenticated_exit_code(tmp_path, live_server):
    res = live_server.run("--url", live_server.url, "--token", "olcf_pat_bogus", "workspace", "list", env={"OLCF_CONFIG_DIR": str(tmp_path / "x")})
    assert res.returncode == 3, res.stderr
    assert "olcf_pat_bogus" not in res.stdout + res.stderr


def test_unreachable_server_exit_code(tmp_path, live_server):
    res = live_server.run("--url", "http://127.0.0.1:9", "--token", "olcf_pat_x", "--timeout", "2", "workspace", "list", env={"OLCF_CONFIG_DIR": str(tmp_path / "y")})
    assert res.returncode == 8


# --------------------------------------------------------------------------- status / doctor / health
def test_status_health_doctor(live_server):
    st = live_server.json("status")
    assert st["reachable"] is True and st["url"] == live_server.url and "jobs" in st
    h = live_server.json("health")
    assert h
    d = live_server.invoke("--json", "doctor", "--no-server")
    assert d.exit_code in (0, 1)
    checks = json.loads(d.stdout)
    assert isinstance(checks, (list, dict)) and checks


def test_context_list_show_and_token_permissions(live_server):
    ctxs = live_server.json("context", "list")
    names = [c["name"] for c in ctxs]
    assert "local" in names
    shown = live_server.json("context", "show")
    assert shown["url"].startswith("http://127.0.0.1")
    assert "olcf_pat_" not in json.dumps(shown)
    cfg = Path(live_server.env["OLCF_CONFIG_DIR"])
    for f in cfg.rglob("*"):
        if f.is_file() and f.name in {"credentials.json", "tokens.json", "credentials"}:
            assert stat.S_IMODE(f.stat().st_mode) == 0o600, f


def test_login_logout_and_context_management(live_server):
    r2 = live_server.invoke("--url", live_server.url, "login", "--name", "pw", "-u", "admin", "--password-stdin", input=live_server.admin_password + "\n")
    assert r2.exit_code == 0, r2.output
    assert live_server.admin_password not in r2.output
    who = live_server.json("-c", "pw", "whoami")
    assert "admin" in json.dumps(who)
    live_server.json("context", "use", "pw")
    assert live_server.json("context", "show")["context"] == "pw"
    live_server.invoke("logout", "-c", "pw")  # tolerate either spelling
    bad = live_server.invoke("--url", live_server.url, "login", "--name", "bad", "-u", "admin", "--password-stdin", input="wrong-password\n")
    assert bad.exit_code == 3
    live_server.json("context", "use", "local")
    live_server.json("context", "delete", "pw")


def test_env_token_overrides_context(live_server, tmp_path):
    tok = live_server.json("token", "create", "envtok", "--scope", "read", "--days", "1")
    secret = tok.get("token") or tok.get("secret")
    assert secret and secret.startswith("olcf_pat_")
    res = live_server.run("--json", "--url", live_server.url, "whoami", env={"OLCF_TOKEN": secret, "OLCF_CONFIG_DIR": str(tmp_path / "empty")})
    assert res.returncode == 0, res.stderr
    assert secret not in res.stdout + res.stderr
    # a read-only token cannot create things
    denied = live_server.run("--url", live_server.url, "workspace", "create", "nope", env={"OLCF_TOKEN": secret, "OLCF_CONFIG_DIR": str(tmp_path / "empty")})
    assert denied.returncode == 4


# --------------------------------------------------------------------------- workspaces, items, notebooks
def test_workspace_crud_and_roundtrip(live_server, ws, tmp_path):
    lst = live_server.json("workspace", "list")
    assert any(w["name"] == ws for w in lst)
    got = live_server.json("workspace", "get", ws)
    assert got["name"] == ws
    live_server.json("workspace", "create", "tmp-ws", "--description", "scratch")
    live_server.json("workspace", "delete", "tmp-ws", "--yes")
    assert all(w["name"] != "tmp-ws" for w in live_server.json("workspace", "list"))
    assert any(w["name"] == "tmp-ws" for w in live_server.json("workspace", "list", "--deleted"))
    live_server.json("workspace", "restore", "tmp-ws")
    dup = live_server.invoke("--json", "workspace", "create", ws)
    assert dup.exit_code == 6
    missing = live_server.invoke("--json", "workspace", "get", "does-not-exist")
    assert missing.exit_code == 5
    out = tmp_path / "ws.zip"
    assert live_server.invoke("workspace", "export", ws, "-o", str(out)).exit_code == 0
    assert zipfile.is_zipfile(out)
    imp = live_server.invoke("--json", "workspace", "import", str(out))
    assert imp.exit_code in (0, 6), imp.output


def test_notebook_import_export_and_params(live_server, ws, tmp_path):
    nb = tmp_path / "hello.py"
    nb.write_text("# %% tags=[\"parameters\"]\nname = 'x'\n# %%\nprint('hi', name)\n")
    imp = live_server.invoke("--json", "notebook", "import", str(nb), "-w", ws)
    assert imp.exit_code == 0, imp.output
    names = [n["name"] for n in live_server.json("notebook", "list", "-w", ws)]
    assert "hello" in names
    out = tmp_path / "back.ipynb"
    res = live_server.invoke("notebook", "export", "hello", "-w", ws, "-f", "ipynb", "-o", str(out))
    assert res.exit_code == 0, res.output
    assert json.loads(out.read_text())["nbformat"] == 4
    stdout_py = live_server.invoke("notebook", "export", "hello", "-w", ws, "-f", "py")
    assert stdout_py.exit_code == 0 and "print" in stdout_py.output
    params = live_server.json("notebook", "parameters", "hello", "-w", ws)
    assert "name" in json.dumps(params)
    again = live_server.invoke("--json", "notebook", "import", str(nb), "-w", ws, "--on-conflict", "fail")
    assert again.exit_code in (1, 6, 7)


def test_item_create_list_get(live_server, ws, tmp_path):
    d = tmp_path / "def.json"
    d.write_text(json.dumps({"schema": "olcf.pipeline/1", "activities": [{"name": "W", "type": "wait", "typeProperties": {"waitTimeInSeconds": 0}}]}))
    created = live_server.json("item", "create", "item-pipe", "--type", "pipeline", "--definition", str(d), "-w", ws)
    assert created["type"] == "pipeline"
    items = live_server.json("item", "list", "--type", "pipeline", "-w", ws)
    assert any(i["name"] == "item-pipe" for i in items)


# --------------------------------------------------------------------------- lakehouse, sql
def test_lakehouse_sql_and_formats(live_server, ws, tmp_path):
    lh = live_server.json("lakehouse", "create", "lh1", "-w", ws)
    assert lh["name"] == "lh1"
    assert any(x["name"] == "lh1" for x in live_server.json("lakehouse", "list", "-w", ws))
    src = tmp_path / "data.csv"
    src.write_text("id,name\n1,ada\n2,grace\n")
    up = live_server.invoke("lakehouse", "files", "--help")
    assert up.exit_code == 0
    j = live_server.json("sql", "run", "select 1 as a, 'x' as b", "-w", ws)
    rows = j["rows"] if isinstance(j, dict) else j
    assert rows
    table = live_server.invoke("sql", "run", "select 1 as a, 'x' as b union all select 2, 'y'", "-w", ws)
    assert table.exit_code == 0 and "a" in table.output.splitlines()[0]
    c = live_server.invoke("-o", "csv", "sql", "run", "select 1 as a, 'x' as b union all select 2, 'y'", "-w", ws)
    parsed = list(csv.reader(io.StringIO(c.output)))
    assert parsed[0] == ["a", "b"] and len(parsed) == 3
    f = tmp_path / "q.sql"
    f.write_text("select 40 + 2 as answer")
    from_file = live_server.json("sql", "run", "-f", str(f), "-w", ws)
    assert "42" in json.dumps(from_file)
    bad = live_server.invoke("--json", "sql", "run", "select * from nope_table", "-w", ws)
    assert bad.exit_code in (7, 5)
    assert live_server.json("sql", "history", "--limit", "5")


def test_sql_engines_and_search_tables_show_their_columns(live_server, ws, tmp_path):
    engines = live_server.invoke("sql", "engines")
    assert engines.exit_code == 0 and "duckdb" in engines.output, engines.output  # the ENGINE column reads the engine id
    nb = tmp_path / "findable_notebook.py"
    nb.write_text("# %%\nprint('hi')\n")
    assert live_server.invoke("--json", "notebook", "import", str(nb), "-w", ws).exit_code == 0
    hits = live_server.json("search", "findable_notebook")
    (hit,) = hits["items"] if isinstance(hits, dict) else hits
    shown = live_server.invoke("search", "findable_notebook").output
    for cell in (hit["type"], hit["name"], hit["id"], hit["workspace_id"]):  # TYPE / WORKSPACE are filled from the API's keys, not "-"
        assert cell[:20] in shown, (cell, shown)


def test_sample_install_and_catalog(live_server):
    res = live_server.invoke("--json", "sample", "install")
    assert res.exit_code == 0, res.output
    st = live_server.json("sample", "status")
    assert "installed" in json.dumps(st).lower()
    assert live_server.invoke("--json", "search", "sample").exit_code == 0
    assert live_server.invoke("--json", "catalog", "tables").exit_code == 0


# --------------------------------------------------------------------------- secrets, connections, tokens, users, roles
def test_secret_value_never_printed(live_server, ws):
    value = "s3cr3t-value-123"
    res = live_server.invoke("--json", "secret", "set", "api-key", "--value-stdin", "-w", ws, input=value + "\n")
    assert res.exit_code == 0, res.output
    assert value not in res.output
    listing = live_server.invoke("--json", "secret", "list")
    assert listing.exit_code == 0 and value not in listing.output and "api-key" in listing.output


def test_secret_value_not_accepted_as_argument(live_server):
    res = live_server.invoke("secret", "set", "x", "--value", "oops")
    assert res.exit_code == 2


def test_connection_secret_from_stdin(live_server, ws):
    res = live_server.invoke("--json", "connection", "create", "pg1", "--type", "postgres", "--config", "host=db.example", "--config", "database=app", "--secret-stdin", "-w", ws,
                             input='{"username":"u","password":"hunter2hunter2"}')
    assert res.exit_code == 0, res.output
    assert "hunter2hunter2" not in res.output
    got = live_server.invoke("--json", "connection", "list", "-w", ws)
    assert "hunter2hunter2" not in got.output and "pg1" in got.output


def test_token_lifecycle(live_server):
    t = live_server.json("token", "create", "ci", "--scope", "read", "--scope", "mcp", "--days", "7")
    assert t["token"].startswith("olcf_pat_") if "token" in t else t["secret"].startswith("olcf_pat_")
    listing = live_server.invoke("--json", "token", "list")
    assert "olcf_pat_" not in listing.output and "ci" in listing.output
    tid = next(x["id"] for x in json.loads(listing.stdout) if x["name"] == "ci")
    assert live_server.invoke("--json", "token", "revoke", tid).exit_code == 0


def test_user_and_role_management(live_server, ws):
    created = live_server.json("user", "create", "alice", "--role", f"Viewer@workspace:{ws}")
    pw = created.get("generated_password")
    assert pw and len(pw) >= 12
    assert any(u["name"] == "alice" or u.get("username") == "alice" for u in live_server.json("user", "list"))
    roles = live_server.json("role", "list")
    assert any("admin" in json.dumps(r) for r in roles)
    assert live_server.json("role", "bindings")
    # alice can log in and is not allowed to create workspaces
    res = live_server.invoke("--url", live_server.url, "login", "--name", "alice", "-u", "alice", "--password-stdin", input=pw + "\n")
    assert res.exit_code == 0, res.output
    assert pw not in res.output
    denied = live_server.invoke("--json", "-c", "alice", "workspace", "create", "alice-ws")
    assert denied.exit_code == 4
    live_server.json("context", "delete", "alice")


def test_user_create_must_change_password_forces_the_change_before_anything_else(live_server):
    """FIX D: ``user create --must-change-password`` -> sign-in warns and the session can only run ``olcf password``."""
    created = live_server.json("user", "create", "forced-bob", "--must-change-password")
    assert created["must_change_password"] is True
    pw = created["generated_password"]
    res = live_server.invoke("--url", live_server.url, "login", "--name", "forced-bob", "-u", "forced-bob", "--password-stdin", input=pw + "\n")
    assert res.exit_code == 0, res.output
    assert "olcf password" in res.output and pw not in res.output
    blocked = live_server.invoke("--json", "-c", "forced-bob", "whoami")
    assert blocked.exit_code == 4 and "password" in blocked.output.lower()
    new = "a-brand-new-passphrase-9"
    changed = live_server.invoke("-c", "forced-bob", "password", "--password-stdin", input=f"{pw}\n{new}\n")
    assert changed.exit_code == 0, changed.output
    assert live_server.invoke("--json", "-c", "forced-bob", "whoami").exit_code == 0
    live_server.json("context", "delete", "forced-bob")


# --------------------------------------------------------------------------- jobs and pipelines
def test_job_submit_status_cancel_and_wait_timeout(live_server, ws):
    job = live_server.json("job", "submit", "--sql", "select 1", "-w", ws, "--name", "queued-job", "--idempotency-key", "k-1")
    jid = job["id"]
    same = live_server.json("job", "submit", "--sql", "select 1", "-w", ws, "--name", "queued-job", "--idempotency-key", "k-1")
    assert same["id"] == jid
    st = live_server.invoke("--json", "job", "status", jid)
    assert st.exit_code == 1                                           # documented: 1 = still running / queued
    assert json.loads(st.stdout)["state"] in {"queued", "pending", "submitted"}
    res = live_server.invoke("--json", "job", "wait", jid, "--timeout", "1")
    assert res.exit_code == 9
    assert live_server.invoke("--json", "job", "cancel", jid).exit_code == 0
    final = live_server.invoke("--json", "job", "wait", jid, "--timeout", "30")
    assert final.exit_code == 10
    logs = live_server.invoke("job", "logs", jid)
    assert logs.exit_code == 0
    assert any(j["id"] == jid for j in live_server.json("job", "list", "-w", ws))


def test_pipeline_validate_create_run_wait(live_server, ws):
    example = str(EXAMPLES / "pipeline_example.json")
    v = live_server.invoke("--json", "pipeline", "validate", example)
    assert v.exit_code == 0, v.output
    live_server.json("pipeline", "create", "hello", "--file", example, "-w", ws)
    run = live_server.invoke("--json", "pipeline", "run", "hello", "-w", ws, "--wait", "--timeout", "60", "-p", "who=olcf")
    assert run.exit_code == 0, run.output
    body = json.loads(run.stdout)
    assert body["state"] == "succeeded" and body["outputs"] == {"greeting": "hello olcf"}
    assert live_server.json("pipeline", "runs", "hello", "-w", ws)


def test_pipeline_validate_rejects_bad_definition(live_server, tmp_path):
    bad = tmp_path / "bad.json"
    bad.write_text(json.dumps({"schema": "olcf.pipeline/1", "activities": [{"name": "X", "type": "nonsense", "typeProperties": {}}]}))
    res = live_server.invoke("--json", "pipeline", "validate", str(bad))
    assert res.exit_code in (1, 7)


def test_failing_pipeline_exit_code(live_server, ws, tmp_path):
    f = tmp_path / "fail.json"
    f.write_text(json.dumps({"schema": "olcf.pipeline/1", "activities": [{"name": "F", "type": "fail", "typeProperties": {"message": "boom"}}]}))
    live_server.json("pipeline", "create", "failing", "--file", str(f), "-w", ws)
    res = live_server.invoke("--json", "pipeline", "run", "failing", "-w", ws, "--wait", "--timeout", "60")
    assert res.exit_code == 10


# --------------------------------------------------------------------------- compute / workers / runtime / fabric / mcp
def test_worker_and_compute_commands(live_server):
    tok = live_server.json("worker", "enroll-token")
    assert "token" in tok and tok["token"] not in live_server.invoke("worker", "list").output
    assert live_server.json("worker", "list") == []
    topo = live_server.json("compute", "topology")
    assert topo is not None


def test_runtime_list_local_without_server(tmp_path, live_server):
    # A bare data dir has no config.toml, so it is "production": the bundled manifests (signed with the development key) need the explicit opt-in.
    env = {"OLCF_URL": "http://127.0.0.1:9", "OLCF_CONFIG_DIR": str(tmp_path / "bare-cfg"), "OLCF_ENVIRONMENT": "development"}
    res = live_server.run("--json", "--data-dir", str(tmp_path / "bare"), "runtime", "list", env=env)
    assert res.returncode == 0, res.stderr
    ids = json.dumps(json.loads(res.stdout))
    assert "olcf-rt-2.0.0" in ids


def test_fabric_commands(live_server, tmp_path):
    st = live_server.invoke("--json", "fabric", "status")
    assert st.exit_code == 0, st.output
    snippet = live_server.invoke("fabric", "snippet")
    assert snippet.exit_code == 0 and "olcf" in snippet.output.lower()
    nb = tmp_path / "n.py"
    nb.write_text("import notebookutils\nnotebookutils.mssparkutils.fs.ls('abfss://x@y.dfs.core.windows.net/')\n")
    an = live_server.invoke("--json", "fabric", "analyze", str(nb))
    assert an.exit_code in (0, 1), an.output
    assert json.loads(an.stdout)


@pytest.mark.parametrize("client", ["claude-desktop", "claude-code", "generic"])
def test_mcp_config_creates_scoped_token(live_server, client):
    res = live_server.invoke("--json", "mcp", "config", "--client", client)
    assert res.exit_code == 0, res.output
    text = res.output
    assert "olcf_pat_" in text and "/mcp" in text
    toks = live_server.json("token", "list")
    mine = [t for t in toks if t["name"] == f"mcp-{client}"]
    assert mine and "mcp" in json.dumps(mine[0])
    assert "*" not in (mine[0].get("scopes") or [])


def test_mcp_info(live_server):
    assert live_server.invoke("--json", "mcp", "info").exit_code == 0


# --------------------------------------------------------------------------- admin
def test_backup_create_list_verify(live_server, tmp_path):
    out = tmp_path / "b.olcfbak"
    res = live_server.invoke("--json", "backup", "create", "-o", str(out), "--passphrase-stdin", "--include-keys", input="correct horse battery\n")
    assert res.exit_code == 0, res.output
    assert out.exists() and b"correct horse" not in out.read_bytes()
    v = live_server.invoke("--json", "backup", "verify", str(out), "--passphrase-stdin", input="correct horse battery\n")
    assert v.exit_code == 0, v.output
    wrong = live_server.invoke("--json", "backup", "verify", str(out), "--passphrase-stdin", input="nope nope nope\n")
    assert wrong.exit_code != 0


def test_db_status_and_audit(live_server):
    st = live_server.json("db", "status")
    assert st
    a = live_server.invoke("--json", "audit", "verify")
    assert a.exit_code == 0, a.output


def test_diagnostics_bundle_has_no_secrets(live_server, tmp_path):
    out = tmp_path / "support.zip"
    res = live_server.invoke("--json", "diagnostics", "-o", str(out))
    assert res.exit_code == 0, res.output
    needles = [live_server.admin_password.encode(), b"olcf_pat_"]
    with zipfile.ZipFile(out) as z:
        for n in z.namelist():
            blob = z.read(n)
            assert not n.endswith((".key", ".pem")), n
            for needle in needles:
                assert needle not in blob, (n, needle[:8])


def test_service_print_each_platform(live_server):
    for plat, marker in (("macos", "<plist"), ("linux", "[Service]"), ("windows", "<Task")):
        res = live_server.invoke("service", "print", "--platform", plat)
        assert res.exit_code == 0 and marker in res.output, plat


@pytest.mark.parametrize("shell", ["bash", "zsh", "fish", "powershell"])
def test_completion_scripts(live_server, shell):
    res = live_server.invoke("completion", shell)
    assert res.exit_code == 0 and "olcf" in res.output


def test_bash_completion_script_is_valid_syntax(live_server, tmp_path, shellcheck):
    import shutil
    import subprocess

    script = tmp_path / "olcf.bash"
    script.write_text(live_server.invoke("completion", "bash").output)
    bash = shutil.which("bash")
    assert bash
    assert subprocess.run([bash, "-n", str(script)], check=False, capture_output=True).returncode == 0  # noqa: S603


def test_dynamic_completion_lists_subcommands(live_server):
    res = live_server.run(env={"_OLCF_COMPLETE": "bash_complete", "COMP_WORDS": "olcf work", "COMP_CWORD": "1"})
    assert res.returncode == 0 and "worker" in res.stdout and "workspace" in res.stdout


# --------------------------------------------------------------------------- process-level behaviour
def test_output_has_no_ansi_when_piped(live_server):
    res = live_server.run("workspace", "list")
    assert "\x1b[" not in res.stdout


def test_entry_point_module_matches_pyproject():
    text = (ROOT / "pyproject.toml").read_text()
    assert 'olcf = "olcf.cli.main:main"' in text
    assert callable(__import__("olcf.cli.main", fromlist=["main"]).main)


def test_main_returns_exit_code_instead_of_exiting(tmp_path):
    from olcf.cli.main import main

    old = dict(os.environ)
    os.environ["OLCF_CONFIG_DIR"] = str(tmp_path)
    try:
        assert main(["--version"]) == 0
        assert main(["no-such"]) == 2
    finally:
        os.environ.clear()
        os.environ.update(old)
    assert sys.version_info >= (3, 11)
