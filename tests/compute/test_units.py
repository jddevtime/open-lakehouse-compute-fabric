# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Pure unit tests (no server, no database): failure rules, wire protocol, log store, health model, Spark UI rewriting, session helpers, provider registry, schema."""

from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Any

import pytest

from olcf.compute import logstore
from olcf.compute import protocol as P
from olcf.compute.failures import (
    MAX_BACKOFF_SECONDS,
    RETRYABLE_CATEGORIES,
    backoff_for,
    classify_failure,
    retry_decision,
)
from olcf.compute.logstore import LogStore
from olcf.compute.providers import BUILTIN_TYPES, ENTRY_POINT_GROUP, ProviderManager
from olcf.compute.providers.base import ComputeProvider, Discovery, PrepareRequest, ProviderEnv
from olcf.compute.providers.local import LocalProvider
from olcf.compute.registry import assess_health, summarize_metrics
from olcf.compute.sessions import cell_of, make_msg_id, to_output
from olcf.compute.spark_ui import already_prefixed, rewrite_html, rewrite_location
from olcf.core.errors import BadRequest, FailureCategory
from olcf.core.timeutil import iso_in, now_iso


# ============================================================================================= failures
@pytest.mark.parametrize("message,category", [
    ("java.lang.OutOfMemoryError: Java heap space", "resource"),
    ("Container killed by YARN for exceeding memory limits", "resource"),
    ("ModuleNotFoundError: No module named 'pandas'", "dependency"),
    ("java.lang.ClassNotFoundException: com.acme.Udf", "dependency"),
    ("AnalysisException: Table or view not found: sales", "catalog"),
    ("org.apache.hadoop.fs.FileNotFoundException: abfss://c@a/x", "storage"),
    ("AccessDenied: not allowed", "authz"),
    ("401 Unauthorized", "authn"),
    ("ExecutorLostFailure (executor 3 exited)", "spark"),
    ("operation timed out after 30s", "timeout"),
    ("CompatibilityBlocked: unsupported in OLCF", "compatibility"),
    ("KeyboardInterrupt", "cancellation"),
])
def test_failure_rules(message: str, category: str) -> None:
    c = classify_failure({"category": "internal", "message": message})
    assert c.category == category and c.source == "rule"
    # the rules also look at the log tail, not only the message
    assert classify_failure(None, log_tail="...\n" + message).category == category


def test_reported_category_wins_and_exit_codes_and_default() -> None:
    assert classify_failure({"category": "user-code", "message": "OutOfMemoryError in my code"}).category == "user-code"  # runner knows best
    assert classify_failure({"category": "bogus", "message": "x"}).category == "user-code"  # unknown category falls through to the default
    assert classify_failure({"message": "boom"}, exit_code=137).category == "resource"
    assert classify_failure({"message": "boom"}, exit_code=143).category == "cancellation"
    d = classify_failure(None, exit_code=3)
    assert d.category == "user-code" and "code 3" in d.message and d.source == "default"
    assert classify_failure({"category": "internal", "message": "weird"}).category == "internal"
    f = classify_failure({"category": "internal", "message": "java heap space"}).as_failure({"message": "orig", "traceback": "tb"})
    assert f["category"] == "resource" and f["message"] == "orig" and f["traceback"] == "tb" and f["remediation"] and f["classified_by"] == "rule"
    assert all(FailureCategory(c) for c in (x.value for x in RETRYABLE_CATEGORIES))


def test_retry_rule_matrix() -> None:
    base = {"attempt": 1, "max_attempts": 3, "category": "provider", "idempotent": False, "reached_running": False, "infrastructure_loss": False, "confirmed_gone": False}

    def d(**kw: Any):  # noqa: ANN202
        return retry_decision(**{**base, **kw})

    assert d().retry and d().backoff_seconds == 30
    assert not d(attempt=3).retry and "no attempts left" in d(attempt=3).reason
    for cat in ("user-code", "authz", "authn", "compatibility", "timeout", "cancellation"):
        assert not d(category=cat).retry, cat
    for cat in ("provider", "resource", "spark", "storage", "catalog", "internal"):
        assert d(category=cat).retry, cat
    assert d(category="not-a-category").retry  # unknown -> internal
    # a job that had started running is only replayed when declared idempotent
    assert not d(reached_running=True).retry and d(reached_running=True, idempotent=True).retry
    # a lost agent must have confirmed the old process is gone
    lost = {"reached_running": True, "idempotent": True, "infrastructure_loss": True}
    assert not d(**lost).retry and "confirmed" in d(**lost).reason
    assert d(**lost, confirmed_gone=True).retry
    assert d(infrastructure_loss=True).retry  # never reached running: nothing to duplicate


def test_backoff_doubles_and_is_capped() -> None:
    assert [backoff_for(a, 10) for a in (1, 2, 3, 4)] == [10, 20, 40, 80]
    assert backoff_for(30, 10) == MAX_BACKOFF_SECONDS and backoff_for(1, 0) == 0 and backoff_for(0, 5) == 5


# ============================================================================================= protocol
def test_frame_round_trip_and_commands() -> None:
    f = P.command("run_job", {"spec": {"a": 1}})
    assert f.id and f.type == "run_job"
    back = P.decode(f.encode())
    assert back.type == "run_job" and back.id == f.id and back.payload == {"spec": {"a": 1}} and back.v == 1
    with pytest.raises(ValueError):
        P.command("rm_rf")
    r = P.result_frame("abc", True, {"x": 1})
    assert json.loads(r.encode()) | {"ts": 0} == {"v": 1, "type": "result", "ref": "abc", "ts": 0, "payload": {"ok": True, "data": {"x": 1}}}
    e = P.error_frame("bad", "nope", "r1")
    assert e.payload == {"code": "bad", "message": "nope"} and e.ref == "r1"


@pytest.mark.parametrize("raw,code,fatal", [
    ("not json", "bad_json", False),
    ("[1,2]", "bad_frame", False),
    ('{"v": 1}', "bad_frame", False),
    ('{"v": 1, "type": ""}', "bad_frame", False),
    ('{"v": 2, "type": "hello"}', "unsupported_version", True),
    ('{"v": 1, "type": "hello", "payload": [1]}', "bad_frame", False),
    ('{"v": 1, "type": "hello", "id": 7}', "bad_frame", False),
    ('{"v": 1, "type": "hello", "ref": "' + "x" * 200 + '"}', "bad_frame", False),
])
def test_decode_rejects_malformed_frames(raw: str, code: str, fatal: bool) -> None:
    with pytest.raises(P.ProtocolError) as ei:
        P.decode(raw)
    assert ei.value.code == code and ei.value.fatal is fatal


def test_decode_size_limit_and_tolerance() -> None:
    with pytest.raises(P.ProtocolError) as ei:
        P.decode("x" * (P.MAX_AGENT_FRAME_BYTES + 1))
    assert ei.value.code == "frame_too_large" and ei.value.fatal
    f = P.decode('{"type": "heartbeat", "payload": null, "future_field": 1}')  # missing v / null payload / unknown keys are tolerated
    assert f.payload == {} and f.v == 1


def test_payload_validation_is_strict_on_structure_tolerant_on_additions() -> None:
    hello = P.parse_payload(P.Frame("hello", {"worker_id": "wrk_1", "inventory": {"cores": 4}, "active_jobs": [{"job_id": "job_1", "state": "succeeded"}], "future": "kept"}, id="i"))
    assert hello.active_jobs[0].state == "succeeded" and hello.model_extra == {"future": "kept"} and hello.protocol == P.WORKER_PROTOCOL_VERSION
    with pytest.raises(P.ProtocolError) as ei:
        P.parse_payload(P.Frame("hello", {"active_jobs": [{"state": "running"}]}, id="i"))
    assert ei.value.code == "bad_payload" and ei.value.fatal and "job_id" in ei.value.message  # a bad hello closes the connection
    with pytest.raises(P.ProtocolError) as ei:
        P.parse_payload(P.Frame("job_state", {"job_id": "j", "state": "exploded"}, id="i"))
    assert not ei.value.fatal and ei.value.ref == "i"
    with pytest.raises(P.ProtocolError):
        P.parse_payload(P.Frame("job_log", {"job_id": "j", "seq": -1, "data": "x"}))
    assert P.parse_payload(P.Frame("pong", {"anything": 1})) == {"anything": 1}
    hb = P.parse_payload(P.Frame("heartbeat", {"metrics": {"cpu_percent": 12.5}}))
    assert hb.metrics.cpu_percent == 12.5 and hb.runtimes is None  # None = "unchanged since the last heartbeat"


def test_inventory_normalisation_accepts_agent_spellings() -> None:
    n = P.normalize_inventory({"cpu_count": "8", "memory_gb": 16, "ip": "10.0.0.5", "platform": "linux", "machine": "x86_64", "disk_total_gb": 100})
    assert (n["cores"], n["memory_mb"], n["disk_gb"], n["address"], n["addresses"], n["os"], n["arch"], n["gpus"]) == (8, 16384, 100.0, "10.0.0.5", ["10.0.0.5"], "linux", "x86_64", [])
    e = P.normalize_inventory(None)
    assert e["cores"] == 0 and e["memory_mb"] == 0 and e["addresses"] == []
    assert P.normalize_inventory({"cores": 2, "addresses": ["a", "b"]})["address"] == "a"


# ============================================================================================= log store
JOB = "job_01HZZ9ABCDEFGHJKMNPQRSTVWX"


def test_logstore_appends_dedupes_and_redacts(tmp_path: Path) -> None:
    ls = LogStore(tmp_path)
    assert ls.append(JOB, "stdout", 0, "hello\n") == 6
    assert ls.append(JOB, "stdout", 0, "hello\n") == 0  # duplicate seq (redelivery after reconnect)
    assert ls.append(JOB, "stdout", 1, "password=hunter2xyz Bearer abcdefghijklmnop\n") > 0
    ls.append(JOB, "stderr", 0, "warn\n")
    out = ls.read(JOB).data
    assert "hunter2xyz" not in out and "abcdefghijklmnop" not in out and out.startswith("hello\n")
    assert ls.read(JOB, stream="stderr").data == "warn\n" and ls.read(JOB, stream="stdout").data.count("\n") == 2
    ls.close(JOB)
    # sequence numbers survive a restart of the store (seq.json)
    ls2 = LogStore(tmp_path)
    assert ls2.append(JOB, "stdout", 1, "again\n") == 0 and ls2.append(JOB, "stdout", 2, "next\n") == 5
    ls2.mark(JOB, "--- attempt 2 ---")
    assert "--- attempt 2 ---" in ls2.read(JOB).data and ls2.exists(JOB)
    with pytest.raises(BadRequest):
        ls2.append(JOB, "combined", 5, "x")


def test_logstore_pages_never_split_a_character(tmp_path: Path) -> None:
    ls = LogStore(tmp_path)
    text = "héllo wörld ✓ 日本語\n" * 50
    ls.append(JOB, "stdout", 0, text)
    got, off = "", 0
    for _ in range(500):
        pg = ls.read(JOB, off, 7)
        assert "�" not in pg.data
        got += pg.data
        if pg.eof:
            break
        assert pg.next_offset > off
        off = pg.next_offset
    assert got == text
    # a client offset that lands inside a multi-byte character is realigned, not an error
    mid = text.encode().index("é".encode()) + 1
    assert ls.read(JOB, mid, 20).data.startswith("llo")
    tail = ls.read(JOB, 0, 30, tail=True)
    assert tail.next_offset == tail.size and tail.eof and tail.data.endswith("日本語\n")
    with pytest.raises(BadRequest):
        ls.read(JOB, 10**9)
    assert ls.read("job_00000000000000000000000000", 0).eof


def test_logstore_rejects_traversal_and_bad_ids(tmp_path: Path) -> None:
    ls = LogStore(tmp_path / "logs")
    for bad in ("../x", "job_../../etc", "", "x" * 100, "job_01/../../..", "/etc/passwd"):
        with pytest.raises(BadRequest):
            ls.job_dir(bad)
    with pytest.raises(BadRequest):
        ls.read(JOB, stream="../../etc/passwd")


def test_logstore_size_cap_retention_and_streaming(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(logstore, "MAX_JOB_LOG_BYTES", 100)
    ls = LogStore(tmp_path)
    assert ls.append(JOB, "stdout", 0, "x" * 150) == 150
    assert ls.append(JOB, "stdout", 1, "dropped") == 0
    assert "log size limit reached" in ls.read(JOB, 0, 10_000).data
    assert b"".join(ls.iter_bytes(JOB, chunk=16)) == ls.read(JOB, 0, 10_000).data.encode()
    other = "job_01HZZ9ABCDEFGHJKMNPQRSTVW2"
    ls.append(other, "stdout", 0, "keep")
    ls.close_all()
    import os

    old = time.time() - 10 * 86400
    for f in (tmp_path / JOB).rglob("*"):
        os.utime(f, (old, old))
    os.utime(tmp_path / JOB, (old, old))
    assert ls.purge_older_than(time.time() - 86400, protect={other}) == 1
    assert not ls.exists(JOB) and ls.exists(other)
    ls.delete(other)
    assert not ls.exists(other)


# ============================================================================================= health model
def worker_row(**kw: Any) -> dict[str, Any]:
    return {"state": "online", "last_seen_at": now_iso(), "components": [], "metrics": {}, **kw}


def test_assess_health_rules() -> None:
    now = time.time()
    h = lambda row, **k: assess_health(row, now, heartbeat_s=k.get("hb", 5), offline_after_s=k.get("off", 30))  # noqa: E731
    assert h(worker_row()) == ("online", [])
    assert h({"state": "pending", "last_seen_at": None})[0] == "pending"
    assert h(worker_row(revoked_at=now_iso()))[0] == "revoked" and h(worker_row(state="revoked"))[0] == "revoked"
    assert h(worker_row(last_seen_at=iso_in(-31)))[0] == "offline"
    st, why = h(worker_row(last_seen_at=iso_in(-12)))
    assert st == "degraded" and "late heartbeat" in why[0]
    assert h(worker_row(last_seen_at=iso_in(-12)), hb=30)[0] == "online"  # lateness is relative to the heartbeat interval
    st, why = h(worker_row(components=[{"name": "spark-worker", "status": "down"}, {"name": "ok", "status": "ok"}]))
    assert st == "degraded" and why == ["component spark-worker: down"]
    st, why = h(worker_row(metrics={"disk_total_gb": 100, "disk_free_gb": 2, "mem_total_mb": 1000, "mem_available_mb": 10}))
    assert st == "degraded" and set(why) == {"disk almost full", "memory almost exhausted"}
    assert h(worker_row(metrics={"disk_total_gb": 0, "disk_free_gb": 0}))[0] == "online"  # no division by zero
    assert h(worker_row(metrics={"disk_total_gb": "x", "disk_free_gb": 1}))[0] == "online"  # junk metrics never crash the liveness loop


def test_metric_summary_always_has_every_key() -> None:
    empty = summarize_metrics(None)
    assert set(empty) == {"cpu_percent", "mem_percent", "disk_percent", "load1", "net_rx_bps", "net_tx_bps", "mem_total_mb", "mem_available_mb"} and empty["mem_percent"] is None
    s = summarize_metrics({"cpu_percent": 10, "mem_total_mb": 1000, "mem_available_mb": 250, "disk_total_gb": 200, "disk_free_gb": 50})
    assert s["mem_percent"] == 75.0 and s["disk_percent"] == 75.0 and s["cpu_percent"] == 10


# ============================================================================================= Spark UI proxy rewriting
PROXY = "/v1/spark-applications/app_1/ui"


def test_redirects_are_mapped_onto_the_proxy_and_never_leave_it() -> None:
    up = "http://10.0.0.5:4040"
    assert rewrite_location("http://10.0.0.5:4040/jobs/?x=1#a", up, PROXY) == f"{PROXY}/jobs/?x=1#a"
    assert rewrite_location("/stages", up, PROXY) == f"{PROXY}/stages"
    assert rewrite_location("http://evil.example/steal", up, PROXY) == PROXY + "/"  # foreign hosts are never followed or leaked
    assert rewrite_location("https://10.0.0.5:4040/x", up, PROXY) == PROXY + "/"  # different scheme = different origin
    assert rewrite_location("jobs/1", up, PROXY) == "jobs/1"
    assert rewrite_location("http://h:1/base/x", "http://h:1/base", PROXY) == f"{PROXY}/x"


def test_html_rewriting() -> None:
    html = ('<a href="/jobs">j</a> <script src=\'/static/x.js\'></script> <link href="//cdn.example/x.css"> <a href="rel">r</a> '
            '<div style="background:url(/img/a.png)"></div><script>setUIRoot("")</script>')
    out = rewrite_html(html, PROXY)
    assert f'href="{PROXY}/jobs"' in out and f"src='{PROXY}/static/x.js'" in out
    assert 'href="//cdn.example/x.css"' in out and 'href="rel"' in out  # protocol-relative and relative links untouched
    assert f"url({PROXY}/img/a.png)" in out and f'setUIRoot("{PROXY}")' in out
    assert already_prefixed(out, PROXY) and not already_prefixed("<a href='/jobs'>", PROXY)


# ============================================================================================= session helpers
def test_message_ids_carry_the_cell() -> None:
    m = make_msg_id("cell-3")
    assert m.startswith("cell-3~") and len(m.split("~")[1]) == 12 and cell_of(m) == "cell-3"
    assert make_msg_id("cell-3") != m
    assert cell_of(None) is None and cell_of("nocell") is None
    for bad in ("", "has space", "a/b", "x" * 65, "a~b"):
        with pytest.raises(BadRequest):
            make_msg_id(bad)


def test_iopub_to_nbformat_outputs() -> None:
    assert to_output("stream", {"name": "stderr", "text": "x"}) == {"output_type": "stream", "name": "stderr", "text": "x"}
    assert to_output("stream", {})["name"] == "stdout"
    assert to_output("execute_result", {"data": {"text/plain": "1"}, "execution_count": 4})["execution_count"] == 4
    d = to_output("update_display_data", {"data": {"text/html": "<b>"}, "transient": {"display_id": "d1"}})
    assert d["output_type"] == "display_data" and d["update"] is True and d["transient"] == {"display_id": "d1"}
    assert to_output("error", {"ename": "ValueError", "evalue": "x", "traceback": ["t"]}) == {"output_type": "error", "ename": "ValueError", "evalue": "x", "traceback": ["t"]}
    assert to_output("status", {"execution_state": "busy"}) is None and to_output("comm_open", {}) is None


# ============================================================================================= providers
class _Plug(ComputeProvider):
    type = "plug"

    def discover(self): return Discovery(self.name, self.type, True)  # noqa: ANN201, E704
    def submit(self, req): raise NotImplementedError  # noqa: ANN001, ANN201, E704
    def status(self, handle): raise NotImplementedError  # noqa: ANN001, ANN201, E704
    def logs(self, handle, offset=0, limit=65536): raise NotImplementedError  # noqa: ANN001, ANN201, E704
    def cancel(self, handle, *, force=False, grace_seconds=15): raise NotImplementedError  # noqa: ANN001, ANN201, E704
    def ui(self, handle): raise NotImplementedError  # noqa: ANN001, ANN201, E704
    def reconcile(self): return []  # noqa: ANN201, E704


class _Plug2(_Plug):
    pass


def manager() -> ProviderManager:
    return ProviderManager(ProviderEnv(None, None, None, None), load_plugins=False)  # type: ignore[arg-type]


def test_provider_plugin_registration(monkeypatch: pytest.MonkeyPatch) -> None:
    m = manager()
    assert {t["type"] for t in m.types()} == {c.type for c in BUILTIN_TYPES} == {"standalone", "local", "external", "kubernetes", "fabric"}
    m.register_type(_Plug)
    m.register_type(_Plug)  # idempotent for the same class
    with pytest.raises(BadRequest, match="already registered"):
        m.register_type(_Plug2)  # a different class may not hijack a type
    with pytest.raises(BadRequest):
        m.register_type(type("NoType", (ComputeProvider,), {}))  # type: ignore[arg-type]
    with pytest.raises(BadRequest):
        m.register_type(int)  # type: ignore[arg-type]
    assert m.instantiate("plug", "p1", {"a": 1}, "sec_1").config == {"a": 1, "secret_id": "sec_1"}
    with pytest.raises(BadRequest, match="unknown provider type"):
        m.instantiate("nope", "x", {})
    assert [t for t in m.types() if t["type"] == "plug"][0]["builtin"] is False

    class EP:
        def __init__(self, name: str, obj: Any) -> None:
            self.name, self._obj = name, obj

        def load(self) -> Any:
            if isinstance(self._obj, Exception):
                raise self._obj
            return self._obj

    calls: list[str] = []

    def fake_entry_points(*, group: str) -> list[EP]:
        calls.append(group)
        return [EP("good", _Plug), EP("broken", ImportError("missing dep")), EP("notaprovider", int), EP("clash", type("Clash", (_Plug,), {"type": "local"}))]

    import olcf.compute.providers as pkg

    monkeypatch.setattr(pkg.metadata, "entry_points", fake_entry_points)
    m2 = manager()
    loaded = m2.load_entry_points()
    assert calls[-1] == ENTRY_POINT_GROUP == "olcf.compute_providers"
    assert loaded == ["good"]  # broken / invalid / clashing plug-ins are skipped and never stop the control plane
    assert "plug" in {t["type"] for t in m2.types()} and m2.types()[0]["type"] in {"external", "fabric", "kubernetes", "local", "plug", "standalone"}


def test_local_provider_prepare_builds_local_master() -> None:
    p = LocalProvider("local", {}, ProviderEnv(None, None, None, None))  # type: ignore[arg-type]
    req = PrepareRequest(job={"id": "job_1"}, placement={"driver_cores": 3, "driver_memory_mb": 2048, "driver_port": 41000, "block_manager_port": 41001},
                         runtime={"manifest": {"spark_conf_defaults": {"spark.ui.enabled": "true"}}}, spark_conf={"spark.sql.shuffle.partitions": "4"}, app_name="app")
    sp = p.prepare(req)
    assert sp["master"] == "local[3]" and sp["driver_host"] == "127.0.0.1"
    c = sp["conf"]
    assert c["spark.driver.bindAddress"] == "127.0.0.1" and c["spark.driver.port"] == "41000" and c["spark.blockManager.port"] == "41001"
    assert c["spark.driver.memory"] == "2048m" and c["spark.driver.cores"] == "3" and c["spark.app.name"] == "app"
    assert c["spark.ui.enabled"] == "true" and c["spark.sql.shuffle.partitions"] == "4"
    assert p.execution == "agent" and p.type == "local"


# ============================================================================================= schema
def test_schema_compiles_on_postgresql_and_has_the_required_indexes() -> None:
    """PostgreSQL is not available in every environment: compile the DDL with the PostgreSQL dialect so type / index mistakes still fail the build."""
    from sqlalchemy.dialects import postgresql
    from sqlalchemy.schema import CreateIndex, CreateTable

    from olcf.compute import schema as C

    dialect = postgresql.dialect()
    ddl = []
    for t in C.COMPUTE_TABLES:
        ddl.append(str(CreateTable(t).compile(dialect=dialect)))
        ddl += [str(CreateIndex(ix).compile(dialect=dialect)) for ix in t.indexes]
    text = "\n".join(ddl)
    assert "CREATE TABLE jobs" in text and "CREATE INDEX ix_jobs_kind_id ON jobs (kind, id)" in text
    assert "BIGSERIAL" in text.upper() or "GENERATED" in text.upper()  # session_events.seq / job_events.seq auto-increment on PostgreSQL
    assert len(C.COMPUTE_TABLES) == 15 and {t.name for t in C.COMPUTE_TABLES} >= {"workers", "jobs", "sessions", "session_events", "worker_commands", "providers"}
    names = {ix.name for ix in C.jobs.indexes}
    # list endpoints are keyset-paginated; 100k+ retained jobs must never fall back to a scan
    assert {"ix_jobs_kind_id", "ix_jobs_queue_id", "ix_jobs_submitter_id"} <= names
    assert any(ix.name and "state" in ix.name for ix in C.jobs.indexes) and any(ix.name and "ws" in ix.name for ix in C.jobs.indexes)
    # every table has a primary key
    for t in C.COMPUTE_TABLES:
        assert list(t.primary_key.columns), t.name


def test_compute_migration_stream_is_registered() -> None:
    from olcf.db import migrate

    migrate.discover()
    assert 1 in migrate._REGISTRY["compute"] and migrate._REGISTRY["compute"][1].description
