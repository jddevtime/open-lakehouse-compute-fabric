# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Clean-environment restore drill (CONTRACTS addendum L5): jobs that were queued or running at backup time and have no live worker attempt become ``failed`` with the
category ``restore_reconcile`` - in the restore-test report, in ``olcf restore`` and in the online restore - and nothing else about them is guessed."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest
from click.testing import CliRunner
from sqlalchemy import select

from olcf.cli.main import cli
from olcf.compute import schema as C
from olcf.core.timeutil import now_iso
from olcf.db.util import fetch_all, insert_row
from olcf.ops import backup_core as core
from olcf.ops.crypto import KeySpec
from olcf.ops.restore_reconcile import reconcile_restored_jobs

PASSPHRASE = "correct-horse-battery-1"  # noqa: S105 - test fixture passphrase, not a credential
WS = "wks_restore"


def _job(db: Any, jid: str, state: str, *, worker: str | None = None, provider: str | None = "standalone", attempt: int = 1, token: str | None = None, handle: dict | None = None) -> None:
    now = now_iso()
    with db.begin() as conn:
        insert_row(conn, C.jobs, {"id": jid, "kind": "script", "engine": "python", "state": state, "workspace_id": WS, "name": f"job-{jid}", "queue": "default", "attempt": attempt,
                                  "driver_worker_id": worker, "provider": provider, "provider_handle": handle, "created_at": now, "updated_at": now, "queued_at": now,
                                  "started_at": now if state in ("running", "cancel_requested") else None, "wait_reason": "no_node: nobody" if state == "queued" else None,
                                  "finished_at": now if state in ("succeeded", "failed", "cancelled") else None})
        if worker:
            insert_row(conn, C.job_attempts, {"id": f"att_{jid}", "job_id": jid, "attempt": attempt, "worker_id": worker, "provider": provider, "token_id": token,
                                              "state": "running" if state in ("running", "cancel_requested") else ("provisioning" if state == "provisioning" else "succeeded"), "created_at": now})


def _states(db: Any) -> dict[str, tuple[str, str | None]]:
    with db.connect() as conn:
        return {r["id"]: (r["state"], r["failure_category"]) for r in fetch_all(conn, select(C.jobs))}


def _seed(db: Any) -> None:
    for jid, state, worker in (("job_acc", "accepted", None), ("job_val", "validating", None), ("job_que", "queued", None), ("job_pro", "provisioning", "wrk_a"),
                               ("job_run", "running", "wrk_a"), ("job_can", "cancel_requested", "wrk_b"), ("job_ok", "succeeded", "wrk_a"), ("job_bad", "failed", None)):
        _job(db, jid, state, worker=worker)
    _job(db, "job_k8s", "running", provider="kubernetes", handle={"sent": True, "native": {"pod": "p"}})


# ------------------------------------------------------------------------------------------------------------------------- the reconciliation itself
def test_unfinished_jobs_without_a_live_attempt_are_failed_with_the_category(settings, tmp_path):
    from olcf.db.engine import Database

    db = Database(f"sqlite:///{tmp_path / 'r.db'}")
    try:
        db.migrate()
        _seed(db)
        dry = reconcile_restored_jobs(db, apply=False)
        assert dry["applied"] is False and dry["failed"] == 6 and _states(db)["job_run"][0] == "running", "a dry run changes nothing"
        rep = reconcile_restored_jobs(db, backup_id="bak_1")
        assert rep["applied"] is True and rep["category"] == "restore_reconcile" and rep["considered"] == 7
        assert rep["failed"] == 6 and rep["by_state"] == {"accepted": 1, "validating": 1, "queued": 1, "provisioning": 1, "running": 1, "cancel_requested": 1}
        assert [j["id"] for j in rep["jobs"]][:3] == ["job_acc", "job_val", "job_que"] and rep["jobs"][4]["state_at_backup"] == "running" and rep["jobs"][4]["worker_id"] == "wrk_a"
        assert [j["id"] for j in rep["left_to_provider"]] == ["job_k8s"] and rep["kept"] == 1
        assert "marked failed" in rep["message"] and "olcf job retry" in rep["message"]
        st = _states(db)
        for jid in ("job_acc", "job_val", "job_que", "job_pro", "job_run", "job_can"):
            assert st[jid] == ("failed", "restore_reconcile"), (jid, st[jid])
        assert st["job_ok"] == ("succeeded", None) and st["job_bad"] == ("failed", None), "finished jobs are never touched"
        assert st["job_k8s"][0] == "running", "a job run by a remote provider stays with the provider's own monitor"
        with db.connect() as conn:
            run = next(r for r in fetch_all(conn, select(C.jobs)) if r["id"] == "job_run")
            tl = fetch_all(conn, select(C.job_events).where(C.job_events.c.job_id == "job_run").order_by(C.job_events.c.seq))
            att = fetch_all(conn, select(C.job_attempts).where(C.job_attempts.c.job_id == "job_run"))
        assert run["failure"]["category"] == "restore_reconcile" and "running when the backup was taken" in run["failure"]["message"] and run["failure"]["remediation"]
        assert run["finished_at"] and run["wait_reason"] is None and "bak_1" in run["state_reason"]
        assert [e["to_state"] for e in tl if e["kind"] == "state"] == ["failed"] and any(e["event"] == "restore_reconcile" and e["data"]["backup_id"] == "bak_1" for e in tl)
        assert att[0]["state"] == "failed" and att[0]["finished_at"]
        again = reconcile_restored_jobs(db)  # idempotent
        assert again["failed"] == 0 and again["considered"] == 1
    finally:
        db.dispose()


def test_a_job_on_a_connected_worker_with_a_live_attempt_is_left_alone(tmp_path):
    from olcf.db.engine import Database

    db = Database(f"sqlite:///{tmp_path / 'r2.db'}")
    try:
        db.migrate()
        _seed(db)
        rep = reconcile_restored_jobs(db, live_workers={"wrk_a"})
        st = _states(db)
        assert st["job_run"][0] == "running" and st["job_pro"][0] == "provisioning", "live attempts on wrk_a are theirs to report"
        assert st["job_can"] == ("failed", "restore_reconcile"), "wrk_b is not connected"
        assert st["job_que"] == ("failed", "restore_reconcile")
        assert rep["failed"] == 4 and rep["kept"] == 3 and rep["live_workers"] == 1
    finally:
        db.dispose()


def test_the_job_scoped_credential_of_a_failed_attempt_is_revoked(tmp_path):
    from olcf.db import schema as S
    from olcf.db.engine import Database

    db = Database(f"sqlite:///{tmp_path / 'r3.db'}")
    try:
        db.migrate()
        cols = {c.name for c in S.api_tokens.columns}
        now = now_iso()
        row = {"id": "tok_job", "principal_id": "usr_1", "kind": "derived", "name": "job job_run", "created_at": now}
        row = {k: v for k, v in row.items() if k in cols}
        for c in S.api_tokens.columns:
            if not c.nullable and c.name not in row and c.default is None and c.server_default is None:
                row[c.name] = "x" * 8 if "hash" not in c.name else "h" * 64
        with db.begin() as conn:
            insert_row(conn, S.api_tokens, row)
        _job(db, "job_run", "running", worker="wrk_a", token="tok_job")  # noqa: S106 - an id
        reconcile_restored_jobs(db)
        with db.connect() as conn:
            assert conn.execute(select(S.api_tokens.c.revoked_at).where(S.api_tokens.c.id == "tok_job")).scalar() is not None
    finally:
        db.dispose()


def test_a_database_without_compute_tables_reports_nothing(tmp_path):
    from olcf.db.engine import Database

    db = Database(f"sqlite:///{tmp_path / 'empty.db'}")
    try:
        rep = reconcile_restored_jobs(db)
        assert rep["failed"] == 0 and rep["considered"] == 0
    finally:
        db.dispose()


# ------------------------------------------------------------------------------------------------------------------------- restore_backup + restore-test + CLI
@pytest.fixture()
def compute_app(settings):
    from fastapi.testclient import TestClient

    from olcf.api.app import create_app

    app = create_app(settings, module_names=["olcf.api.core.module", "olcf.compute.module", "olcf.ops.module"])
    with TestClient(app) as c:
        pw = app.state.ctx.settings.path("secrets", "bootstrap-admin.txt").read_text().split("password=")[1].strip()
        r = c.post("/v1/identity/login", json={"username": "admin", "password": pw})
        assert r.status_code == 200, r.text
        c.admin = {"Authorization": f"Bearer {r.json()['access_token']}"}  # type: ignore[attr-defined]
        c.ctx = app.state.ctx  # type: ignore[attr-defined]
        yield c


def test_restore_test_reports_what_happened_to_the_unfinished_jobs(compute_app):
    c = compute_app
    _seed(c.ctx.db)
    bk = c.post("/v1/admin/backup?wait=true", json={}, headers=c.admin).json()["backup"]
    r = c.post(f"/v1/admin/backup/{bk['id']}/restore-test?wait=true", json={}, headers=c.admin)
    assert r.status_code == 200, r.text
    res = r.json()["operation"]["result"]
    assert res["ok"], [x for x in res["checks"] if not x["ok"]]
    check = next(x for x in res["checks"] if x["name"] == "job-reconcile")
    assert check["ok"] and "6 job(s)" in check["detail"] and "restore_reconcile" in check["detail"]
    jobs = res["jobs"]
    assert jobs["failed"] == 6 and jobs["category"] == "restore_reconcile" and jobs["by_state"]["running"] == 1 and jobs["by_state"]["queued"] == 1
    assert {j["id"] for j in jobs["jobs"]} == {"job_acc", "job_val", "job_que", "job_pro", "job_run", "job_can"}
    assert [j["id"] for j in jobs["left_to_provider"]] == ["job_k8s"]
    # the live database is untouched: the drill ran on a throw-away copy
    assert _states(c.ctx.db)["job_run"] == ("running", None)
    assert c.get(f"/v1/admin/backup/{bk['id']}", headers=c.admin).json()["last_restore_test"]["jobs"]["failed"] == 6
    audit = [a for a in c.ctx.audit.query(limit=50) if a["action"] == "admin.backup_restore_tested"]
    assert audit and audit[0]["details"]["jobs_reconciled"] == 6


def test_a_backup_without_unfinished_jobs_says_so(compute_app):
    c = compute_app
    _job(c.ctx.db, "job_done", "succeeded", worker="wrk_a")
    bk = c.post("/v1/admin/backup?wait=true", json={}, headers=c.admin).json()["backup"]
    res = c.post(f"/v1/admin/backup/{bk['id']}/restore-test?wait=true", json={}, headers=c.admin).json()["operation"]["result"]
    assert res["ok"] and res["jobs"]["failed"] == 0 and "no jobs" in res["jobs"]["message"]


def test_online_restore_keeps_attempts_on_connected_workers(compute_app, monkeypatch):
    c = compute_app
    _seed(c.ctx.db)
    bk = c.post("/v1/admin/backup?wait=true", json={}, headers=c.admin).json()["backup"]
    svc = c.ctx.service("backup")
    monkeypatch.setattr(type(svc), "_connected_workers", lambda self: {"wrk_a"})
    r = c.post(f"/v1/admin/backup/{bk['id']}/restore?wait=true", json={"confirm": "RESTORE"}, headers=c.admin)
    assert r.status_code == 202, r.text
    op = c.get(f"/v1/operations/{r.json()['operation_id']}", headers=c.admin).json()
    assert op["state"] == "succeeded", op
    assert op["result"]["jobs"]["failed"] == 4 and op["result"]["jobs"]["kept"] == 3
    st = _states(c.ctx.db)
    assert st["job_run"][0] == "running" and st["job_que"] == ("failed", "restore_reconcile") and st["job_can"] == ("failed", "restore_reconcile")


def _cli(*args: str) -> Any:
    res = CliRunner().invoke(cli, list(args), env={"OLCF_BACKUP_PASSPHRASE": PASSPHRASE}, catch_exceptions=False)
    assert res.exit_code == 0, res.output
    return res


def test_cli_restore_into_a_clean_installation_fails_unfinished_jobs_and_says_so(tmp_path: Path):
    from olcf.core.config import load_settings
    from olcf.db.engine import Database

    src, dst = tmp_path / "src", tmp_path / "dst"
    _cli("--data-dir", str(src), "init")
    db = Database(load_settings(src).database_url)
    try:
        _seed(db)
    finally:
        db.dispose()
    out = tmp_path / "b.olcfbak"
    _cli("--data-dir", str(src), "backup", "create", "-o", str(out))
    _cli("--data-dir", str(dst), "init")
    human = _cli("--data-dir", str(dst), "restore", str(out), "--yes")
    assert "marked failed with category restore_reconcile" in human.output and "job_run" in human.output and "was running" in human.output, human.output
    dbd = Database(load_settings(dst).database_url)
    try:
        st = _states(dbd)
        assert st["job_run"] == ("failed", "restore_reconcile") and st["job_que"] == ("failed", "restore_reconcile") and st["job_ok"][0] == "succeeded"
    finally:
        dbd.dispose()
    # the JSON report carries the same facts for scripts
    dst2 = tmp_path / "dst2"
    _cli("--data-dir", str(dst2), "init")
    rep = json.loads(_cli("--data-dir", str(dst2), "--json", "restore", str(out), "--yes").stdout)
    assert rep["jobs"]["failed"] == 6 and rep["jobs"]["category"] == "restore_reconcile" and rep["jobs"]["by_state"]["running"] == 1


def test_core_restore_does_not_reconcile_unless_asked(tmp_path, compute_app):
    """An update rollback restores into the same fleet: its agents keep their processes, so ``restore_backup`` only reconciles when the caller opts in."""
    from olcf.db.engine import Database

    c = compute_app
    _seed(c.ctx.db)
    bk = c.post("/v1/admin/backup?wait=true", json={}, headers=c.admin).json()["backup"]
    svc = c.ctx.service("backup")
    with __import__("tempfile").TemporaryDirectory() as td:
        path = svc.materialise(bk["id"], Path(td))
        spec, _ = svc.keyspec(None)
        db = Database(f"sqlite:///{tmp_path / 'plain.db'}")
        try:
            rep = core.restore_backup(path, spec if isinstance(spec, KeySpec) else spec, db)
            assert rep.jobs is None and "jobs" not in rep.to_dict()
            assert _states(db)["job_run"][0] == "running"
        finally:
            db.dispose()
