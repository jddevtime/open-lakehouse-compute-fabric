# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""The runtime-trust change from the user's side (SEC): ``olcf init`` / ``quickstart`` installations keep working (their config.toml says ``environment = "development"``),
and an OLDER installation whose config.toml has no ``environment`` (= production) is told, in every place it meets the problem, exactly what to do.

Everything runs as real subprocesses with NO ``OLCF_ENVIRONMENT`` in the environment: the only thing that can make the bundled manifests trusted is the data directory's
own config, which is what a user has.
"""

from __future__ import annotations

import contextlib
import json
import re
import subprocess
import sys
import time

import httpx
import pytest

from tests.tooling.conftest import cli_env, free_port


def _cli(env, *args, timeout=180):
    return subprocess.run([sys.executable, "-m", "olcf.cli.main", *args], env=env, capture_output=True, text=True, timeout=timeout, check=False)


def _manifest_ids(env, data) -> tuple[set[str], subprocess.CompletedProcess[str]]:
    res = _cli(env, "--json", "--data-dir", str(data), "runtime", "list")
    assert res.returncode == 0, res.stderr
    return {r["id"] for r in json.loads(res.stdout)["runtimes"]}, res


@pytest.fixture()
def fresh(tmp_path):
    """A data directory created by ``olcf init`` (what a new user has) and the process environment of a user: no OLCF_ENVIRONMENT."""
    data = tmp_path / "data"
    env = cli_env(tmp_path, data)
    assert "OLCF_ENVIRONMENT" not in env
    res = _cli(env, "-q", "--data-dir", str(data), "init", "--port", str(free_port()))
    assert res.returncode == 0, res.stderr
    return env, data


@pytest.fixture()
def legacy(fresh):
    """The same installation as it looked before the runtime-trust change: config.toml without ``environment``."""
    env, data = fresh
    cfg = data / "config.toml"
    kept = [line for line in cfg.read_text().splitlines() if not re.match(r"\s*environment\s*=", line)]
    cfg.write_text("\n".join(kept) + "\n")
    assert "environment" not in "\n".join(line for line in kept if not line.lstrip().startswith("#"))
    return env, data


def test_init_writes_development_and_the_bundled_runtimes_are_installable(fresh):
    env, data = fresh
    assert re.search(r'^environment = "development"$', (data / "config.toml").read_text(), re.M)
    ids, res = _manifest_ids(env, data)
    assert {"olcf-rt-2.0.0", "olcf-rt-1.3.0", "olcf-host-python"} <= ids
    assert "development key" not in res.stderr and "not trusted" not in res.stderr, res.stderr
    rows = {r["id"]: r for r in json.loads(res.stdout)["runtimes"]}
    assert rows["olcf-rt-2.0.0"]["state"] == "not installed"  # installable, not hidden
    # the same through `runtime install` (does not download anything here: an unknown id is checked against the trusted catalogue first)
    bad = _cli(env, "--data-dir", str(data), "runtime", "install", "olcf-rt-9.9.9")
    assert bad.returncode == 1 and "olcf-rt-2.0.0" in bad.stderr


def test_an_old_data_directory_loses_the_bundled_runtimes_with_one_actionable_warning(legacy):
    env, data = legacy
    ids, res = _manifest_ids(env, data)
    assert ids == {"olcf-host-python"}, ids  # notebooks still have a Python runtime
    warning = res.stderr
    assert warning.count("skipped 3 bundled runtime manifest(s)") == 1, warning  # one paragraph, not one bare line per manifest
    assert "olcf-rt-2.0.0" in warning and "development key" in warning
    # exactly what to do, for a development machine and for production
    assert 'environment = "development"' in warning and "OLCF_ENVIRONMENT=development" in warning
    assert "keygen" in warning and "runtime.trust.dir" in warning and "OLCF_RUNTIME_TRUST_DIR" in warning
    # `runtime install` without an id used to say "no runtime manifest ships with this build"
    inst = _cli(env, "--data-dir", str(data), "runtime", "install")
    assert inst.returncode != 0 and "Traceback" not in inst.stderr
    assert "no runtime manifest ships" not in inst.stderr and "olcf-rt-2.0.0" in inst.stderr
    assert 'environment = "development"' in inst.stderr and str(data / "config.toml") in inst.stderr
    # the fix the message gives works
    ids_dev, _ = _manifest_ids({**env, "OLCF_ENVIRONMENT": "development"}, data)
    assert "olcf-rt-2.0.0" in ids_dev


def test_init_on_an_old_data_directory_says_what_to_change(legacy):
    env, data = legacy
    res = _cli(env, "--data-dir", str(data), "init", "--port", str(free_port()))
    assert res.returncode == 0, res.stderr
    out = res.stdout + res.stderr
    assert "has no `environment` key" in out and str(data / "config.toml") in out, out
    assert 'environment = "development"' in out and "OLCF_ENVIRONMENT=development" in out and "keygen" in out
    js = _cli(env, "--json", "--data-dir", str(data), "init", "--port", str(free_port()))
    assert js.returncode == 0, js.stderr
    assert "environment" in json.loads(js.stdout)["runtime_trust_note"]
    assert "Traceback" not in out


def test_init_on_a_current_data_directory_or_with_an_explicit_choice_is_quiet(fresh, tmp_path):
    env, data = fresh
    res = _cli(env, "--data-dir", str(data), "init", "--port", str(free_port()))
    assert res.returncode == 0 and "environment" not in (res.stdout + res.stderr).split("Next:")[0].replace('environment = "development"', ""), res.stdout + res.stderr
    # an operator who chose production on purpose and brought a trust directory is not nagged
    cfg = data / "config.toml"
    cfg.write_text(cfg.read_text().replace('environment = "development"', 'environment = "production"'))
    quiet = _cli({**env, "OLCF_RUNTIME_TRUST_DIR": str(tmp_path / "trust")}, "--data-dir", str(data), "init", "--port", str(free_port()))
    assert quiet.returncode == 0 and "has no `environment` key" not in quiet.stdout + quiet.stderr


def test_serve_on_an_old_data_directory_explains_and_stays_healthy(legacy, tmp_path):
    env, data = legacy
    port = free_port()
    logp = tmp_path / "serve.log"
    with open(logp, "wb") as log:
        proc = subprocess.Popen([sys.executable, "-m", "olcf.cli.main", "--data-dir", str(data), "serve", "--port", str(port), "--no-embedded-worker"], env=env, stdout=log,
                                stderr=subprocess.STDOUT)
        try:
            url = f"http://127.0.0.1:{port}"
            deadline = time.monotonic() + 90
            health = None
            while time.monotonic() < deadline:
                assert proc.poll() is None, logp.read_text()[-3000:]
                with contextlib.suppress(httpx.HTTPError):
                    r = httpx.get(url + "/v1/health", timeout=2, trust_env=False)
                    if r.status_code == 200:
                        health = r.json()
                        break
                time.sleep(0.3)
            assert health is not None, logp.read_text()[-3000:]
        finally:
            proc.terminate()
            try:
                proc.wait(timeout=30)
            except subprocess.TimeoutExpired:
                proc.kill()
                proc.wait(timeout=10)
    text = logp.read_text()
    assert "Spark runtimes are NOT available" in text and str(data / "config.toml") in text, text[-3000:]
    assert 'environment = "development"' in text and "OLCF_ENVIRONMENT=development" in text and "keygen" in text
    assert "has no `environment` key" in text  # the CLI's own notice, before the server starts
    assert text.count("Spark runtimes are NOT available") == 1


def _configured_port(data) -> int:
    import tomllib

    return int(tomllib.loads((data / "config.toml").read_text())["server"]["port"])


def _serve(env, data, tmp_path, port, name="serve.log"):
    logp = tmp_path / name
    log = open(logp, "wb")  # noqa: SIM115 - closed by the caller's finally
    proc = subprocess.Popen([sys.executable, "-m", "olcf.cli.main", "--data-dir", str(data), "serve", "--port", str(port), "--no-embedded-worker"], env=env, stdout=log,
                            stderr=subprocess.STDOUT)
    deadline = time.monotonic() + 90
    while time.monotonic() < deadline:
        assert proc.poll() is None, logp.read_text()[-3000:]
        with contextlib.suppress(httpx.HTTPError):
            if httpx.get(f"http://127.0.0.1:{port}/v1/health", timeout=2, trust_env=False).status_code == 200:
                return proc, log
        time.sleep(0.3)
    raise AssertionError(logp.read_text()[-3000:])


def test_runtime_trust_down_fails_readiness_and_is_visible_in_status_and_doctor(legacy, tmp_path):
    """Production + the development key trusted by an explicit opt-in = `runtime-trust` is `down`.  Operators look at `olcf status` and `olcf doctor`, not at the raw health
    API, so both must show it as a warning with the fix.  `runtime-trust` is critical: readiness is 503 (a fixed body, no detail) while liveness stays 200; `olcf doctor` still does not fail on it."""
    env, data = legacy
    port = _configured_port(data)  # the CLI context written by `olcf init` points at this port
    env = {**env, "OLCF_RUNTIME_ALLOW_DEV_KEY": "1"}
    proc, log = _serve(env, data, tmp_path, port)
    try:
        ready = httpx.get(f"http://127.0.0.1:{port}/v1/health/ready", timeout=5, trust_env=False)
        assert ready.status_code == 503 and ready.json() == {"status": "unavailable"}
        assert httpx.get(f"http://127.0.0.1:{port}/v1/health", timeout=5, trust_env=False).status_code == 200
        st = _cli(env, "--json", "--data-dir", str(data), "status")
        assert st.returncode == 0, st.stderr
        doc = json.loads(st.stdout)
        assert doc["runtime_trust"]["status"] == "down" and "keygen" in doc["runtime_trust"]["fix"] and "production" in doc["runtime_trust"]["detail"]
        human = _cli(env, "--data-dir", str(data), "status")
        assert "runtime-trust is down" in human.stderr, human.stderr
        dr = _cli(env, "--data-dir", str(data), "doctor")
        line = next(ln for ln in dr.stdout.splitlines() if "server: runtime-trust" in ln)
        assert line.startswith("WARN"), dr.stdout
        assert "fix:" in dr.stdout.split(line, 1)[1].splitlines()[1] and "OLCF_RUNTIME_TRUST_DIR" in dr.stdout
        assert "FAIL  server: runtime-trust" not in dr.stdout
    finally:
        proc.terminate()
        try:
            proc.wait(timeout=30)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait(timeout=10)
        log.close()


def test_status_has_no_runtime_trust_line_when_the_trust_anchor_is_healthy(fresh, tmp_path):
    """A development installation reports `degraded` (the dev key is trusted: shown so it is not forgotten) - and an operator key directory makes it `ok`, i.e. silent."""
    env, data = fresh
    trust = tmp_path / "trust"
    trust.mkdir()
    keygen = subprocess.run([sys.executable, "-m", "olcf.runtime", "keygen", "--out", str(tmp_path / "ops")], env=env, capture_output=True, text=True, check=False)
    assert keygen.returncode == 0, keygen.stderr
    (trust / "ops.pub").write_bytes((tmp_path / "ops" / "olcf-signing.pub").read_bytes())
    env = {**env, "OLCF_RUNTIME_TRUST_DIR": str(trust)}
    port = _configured_port(data)
    proc, log = _serve(env, data, tmp_path, port, "serve-ok.log")
    try:
        st = _cli(env, "--json", "--data-dir", str(data), "status")
        assert st.returncode == 0, st.stderr
        assert "runtime_trust" not in json.loads(st.stdout)
    finally:
        proc.terminate()
        try:
            proc.wait(timeout=30)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait(timeout=10)
        log.close()
