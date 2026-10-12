# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Runner ``dataset_*`` events become platform ``dataset.*`` events: validation of untrusted payloads, budget, drop counters, job and session forwarding.

Pure tests of :mod:`olcf.compute.lineage_events` come first; the rest runs the real control plane (``fleet``) with fake agents that report runner events.
"""

from __future__ import annotations

import json
from typing import Any

import pytest

from olcf.compute import lineage_events as le
from olcf.core import dataset_events as dsev
from tests.compute.fake_agent import JobPlan

ROW = {"id": "job_1", "workspace_id": "ws_1", "item_id": "itm_nb", "name": "nightly", "kind": "notebook", "engine": "spark", "parent_run_id": "run_9"}
LH = "itm_lakehouse_aaa"
# Built at run time: a synthetic credential-shaped value that never appears as one literal in the source (the secret-scan gate reads source text, and this file is not excluded from it).
FAKE_PASSWORD = "abcdefgh" + "12345"


def sanitize(event: str, data: Any, **kw: Any):
    return le.sanitize_runner_event(event, data, row=kw.pop("row", ROW), allowed_lakehouses=kw.pop("allowed", {LH}), default_lakehouse=kw.pop("default", LH))


# --------------------------------------------------------------------------------------------------- pure sanitizer
def test_read_event_is_built_from_the_job_row_not_from_the_runner():
    etype, p = sanitize("dataset_read", {"datasets": [{"kind": "table", "lakehouse_id": LH, "name": "sales", "format": "delta"}], "engine": "evil", "workspace_id": "ws_other",
                                         "process": {"kind": "x"}, "at": "2026-01-02T03:04:05Z", "mode": "overwrite", "extra": 1})
    assert etype == "dataset.read"
    assert p == {"workspace_id": "ws_1", "process": {"kind": "notebook", "item_id": "itm_nb", "job_id": "job_1", "query_id": None, "name": "nightly", "run_id": "run_9"},
                 "datasets": [{"kind": "table", "lakehouse_id": LH, "name": "sales", "path": None, "uri": None, "format": "delta"}],
                 "engine": "spark", "mode": None, "statement_hash": None, "at": "2026-01-02T03:04:05Z"}


def test_written_event_keeps_valid_modes_only():
    ok = sanitize("dataset_written", {"datasets": [{"kind": "table", "name": "t"}], "mode": "merge"})[1]
    assert ok["mode"] == "merge"
    assert sanitize("dataset_written", {"datasets": [{"kind": "table", "name": "t"}], "mode": "drop-everything"})[1]["mode"] is None


def test_statement_event_clips_redacts_and_defaults():
    etype, p = sanitize("dataset_statement", {"sql": "SELECT 'password=" + FAKE_PASSWORD + "' " + "x" * 30000, "dialect": "oracle", "default_lakehouse_id": "itm_not_mine"})
    assert etype == "dataset.statement"
    assert len(p["sql"]) == dsev.MAX_STATEMENT_CHARS and FAKE_PASSWORD not in p["sql"]
    assert p["dialect"] == "spark" and p["default_lakehouse_id"] == LH       # unknown lakehouse ids fall back to the job's default


def test_lakehouse_ids_the_job_does_not_have_are_dropped_and_uris_lose_their_query_string():
    _, p = sanitize("dataset_read", {"datasets": [{"kind": "table", "lakehouse_id": "itm_other_ws", "name": "t"},
                                                  {"kind": "file", "uri": "abfss://c@a.dfs.core.windows.net/x/y.parquet?sv=2024&sig=SECRET#frag", "format": "parquet"}]})
    assert p["datasets"][0]["lakehouse_id"] is None
    assert p["datasets"][1]["uri"] == "abfss://c@a.dfs.core.windows.net/x/y.parquet" and "SECRET" not in json.dumps(p)


def test_control_characters_and_long_values_are_cleaned():
    _, p = sanitize("dataset_read", {"datasets": [{"kind": "table", "name": "a\x00b\x1b[31m" + "n" * 2000, "format": "delta; DROP"}]})
    d = p["datasets"][0]
    assert "\x00" not in d["name"] and "\x1b" not in d["name"] and len(d["name"]) == le.MAX_NAME_CHARS
    assert d["format"] is None


def test_at_must_look_like_a_timestamp():
    _, p = sanitize("dataset_read", {"datasets": [{"kind": "table", "name": "t"}], "at": "yesterday-ish"})
    assert p["at"] != "yesterday-ish" and p["at"][:2] == "20"


def test_session_rows_use_the_session_as_run():
    _, p = sanitize("dataset_read", {"datasets": [{"kind": "table", "name": "t"}]}, row={"kind": "session", "id": "ses_1", "workspace_id": "ws_1", "item_id": "itm_nb", "name": "nb", "engine": "python"})
    assert p["process"] == {"kind": "notebook", "item_id": "itm_nb", "job_id": None, "query_id": None, "name": "nb", "run_id": "ses_1"} and p["engine"] == "python"


FAB_TENANT, FAB_WS, FAB_NB, FAB_LH = ("11111111-1111-4111-8111-111111111111", "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa", "dddddddd-dddd-4ddd-8ddd-dddddddddddd", "cccccccc-cccc-4ccc-8ccc-cccccccccccc")
FAB_ROW = {"id": "job_f", "workspace_id": "ws_1", "item_id": None, "name": "nb_load (Fabric)", "kind": "capsule", "engine": "python", "parent_run_id": None, "origin_kind": "fabric",
           "origin": {"fabric": {"tenant_id": FAB_TENANT, "workspace_id": FAB_WS, "notebook_id": FAB_NB, "notebook_name": "nb_load", "run_id": "run-0001", "default_lakehouse": "claimed"}}}
FAB_LHS = {LH: {"workspace_id": FAB_WS, "lakehouse_id": FAB_LH}}


def test_a_fabric_origin_adds_the_fabric_block_to_the_process_and_the_lakehouse_ids_to_the_datasets():
    etype, p = le.sanitize_runner_event("dataset_written", {"datasets": [{"kind": "table", "lakehouse_id": LH, "name": "silver"}, {"kind": "table", "lakehouse_id": "itm_other", "name": "x"}]},
                                        row=FAB_ROW, allowed_lakehouses={LH, "itm_other"}, default_lakehouse=LH, fabric_lakehouses=FAB_LHS)
    assert etype == "dataset.written"
    assert p["process"]["fabric"] == {"tenant_id": FAB_TENANT, "workspace_id": FAB_WS, "item_id": FAB_NB, "item_name": "nb_load", "item_type": "Notebook", "run_id": "run-0001"}
    assert p["process"]["kind"] == "notebook" and p["process"]["job_id"] == "job_f" and p["process"]["item_id"] is None                           # existing keys unchanged
    assert [d.get("fabric") for d in p["datasets"]] == [FAB_LHS[LH], None]                       # only a lakehouse recorded as a Fabric lakehouse gets the block


def test_what_the_runner_claims_about_fabric_is_never_used():
    _, p = le.sanitize_runner_event("dataset_read", {"datasets": [{"kind": "table", "lakehouse_id": LH, "name": "t", "fabric": {"workspace_id": "evil", "lakehouse_id": "evil"}}],
                                                     "process": {"fabric": {"item_id": "evil"}}, "fabric": {"tenant_id": "evil"}},
                                    row=ROW, allowed_lakehouses={LH}, fabric_lakehouses=FAB_LHS)
    assert "fabric" not in p["process"] and p["datasets"][0]["fabric"] == FAB_LHS[LH] and "evil" not in json.dumps(p)       # not a Fabric job; the dataset block is the lakehouse's, not the event's
    _, q = le.sanitize_runner_event("dataset_read", {"datasets": [{"kind": "table", "lakehouse_id": LH, "name": "t", "fabric": {"workspace_id": "evil", "lakehouse_id": "evil"}}]},
                                    row=FAB_ROW, allowed_lakehouses={LH}, fabric_lakehouses=FAB_LHS)
    assert q["datasets"][0]["fabric"] == FAB_LHS[LH] and "evil" not in json.dumps(q)


def test_a_session_and_a_job_without_fabric_ids_get_no_fabric_block():
    _, p = le.sanitize_runner_event("dataset_read", {"datasets": [{"kind": "table", "name": "t"}]}, row={**FAB_ROW, "kind": "session"}, allowed_lakehouses=set())
    assert "fabric" not in p["process"]
    _, q = le.sanitize_runner_event("dataset_read", {"datasets": [{"kind": "table", "name": "t"}]}, row={**FAB_ROW, "origin": {"fabric": {"run_id": "r"}}}, allowed_lakehouses=set())
    assert "fabric" not in q["process"]


def test_the_statement_event_names_the_fabric_default_lakehouse():
    _, p = le.sanitize_runner_event("dataset_statement", {"sql": "select * from silver"}, row=FAB_ROW, allowed_lakehouses={LH}, default_lakehouse=LH, fabric_lakehouses=FAB_LHS)
    assert p["default_lakehouse_fabric"] == FAB_LHS[LH] and p["process"]["fabric"]["item_id"] == FAB_NB
    _, q = le.sanitize_runner_event("dataset_statement", {"sql": "select 1"}, row=ROW, allowed_lakehouses={LH}, default_lakehouse=LH, fabric_lakehouses={})
    assert "default_lakehouse_fabric" not in q


def test_fabric_lakehouses_of_reads_the_job_spec_descriptors_and_ignores_names():
    spec = {"default_lakehouse": {"id": LH, "fabric": {"workspace_id": FAB_WS.upper(), "lakehouse_id": FAB_LH}},
            "lakehouses": [{"id": "itm_b", "fabric": {"workspace_id": "Sales", "lakehouse_id": "lh_sales"}}, {"id": "itm_c"}, "junk", {"fabric": {"workspace_id": FAB_WS, "lakehouse_id": FAB_LH}}]}
    assert le.fabric_lakehouses_of(spec) == {LH: {"workspace_id": FAB_WS, "lakehouse_id": FAB_LH}}
    assert le.fabric_lakehouses_of(None) == {} and le.fabric_lakehouses_of({"lakehouses": "x"}) == {}


@pytest.mark.parametrize(("event", "data", "reason"), [
    ("dataset_nope", {}, "unknown_event"),
    ("dataset_read", [], "not_an_object"),
    ("dataset_read", "x", "not_an_object"),
    ("dataset_read", None, "not_an_object"),
    ("dataset_read", {}, "no_datasets"),
    ("dataset_read", {"datasets": []}, "no_datasets"),
    ("dataset_read", {"datasets": "t"}, "no_datasets"),
    ("dataset_read", {"datasets": ["t"]}, "dataset_not_object"),
    ("dataset_read", {"datasets": [{"kind": "view", "name": "v"}]}, "dataset_kind"),
    ("dataset_read", {"datasets": [{"kind": "table"}]}, "table_without_name"),
    ("dataset_read", {"datasets": [{"kind": "table", "name": "   "}]}, "table_without_name"),
    ("dataset_read", {"datasets": [{"kind": "table", "name": 5}]}, "table_without_name"),
    ("dataset_read", {"datasets": [{"kind": "file"}]}, "file_without_path"),
    ("dataset_statement", {}, "statement_without_sql"),
    ("dataset_statement", {"sql": "   "}, "statement_without_sql"),
    ("dataset_statement", {"sql": {"a": 1}}, "statement_without_sql"),
])
def test_malformed_events_raise_with_a_reason(event, data, reason):
    with pytest.raises(le.MalformedDatasetEvent) as ei:
        sanitize(event, data)
    assert ei.value.reason == reason


def test_too_many_datasets_are_cut_and_oversized_payloads_rejected():
    _, p = sanitize("dataset_read", {"datasets": [{"kind": "table", "name": f"t{i}"} for i in range(500)]})
    assert len(p["datasets"]) == le.MAX_DATASETS
    huge = {"datasets": [{"kind": "file", "path": "p" * le.MAX_PATH_CHARS, "uri": "u" * le.MAX_PATH_CHARS, "name": "n" * le.MAX_NAME_CHARS} for _ in range(le.MAX_DATASETS)]}
    with pytest.raises(le.MalformedDatasetEvent) as ei:
        sanitize("dataset_read", huge)
    assert ei.value.reason == "too_large"


def test_compact_for_job_log_drops_the_sql_text():
    _, p = sanitize("dataset_statement", {"sql": "SELECT 1 FROM t"})
    c = le.compact_for_job_log("dataset.statement", p)
    assert c == {"sql_chars": 15, "statement_hash": dsev.statement_hash("SELECT 1 FROM t"), "dialect": "spark"} and "SELECT" not in json.dumps(c)
    _, r = sanitize("dataset_read", {"datasets": [{"kind": "table", "name": "t"}]})
    assert le.compact_for_job_log("dataset.read", r) == {"datasets": r["datasets"], "mode": None}


def test_budget_counts_per_key_and_forgets_old_keys():
    b = le.EventBudget(limit=3, max_keys=2)
    assert [b.take("a") for _ in range(5)] == [True, True, True, False, False]
    assert b.take("b") and b.take("c")              # "a" is now the oldest key and is forgotten
    assert b.take("a")                              # ... so it starts from scratch


def test_stats_snapshot_is_a_copy():
    s = le.EventStats()
    s.forward()
    s.drop("x")
    s.drop("x")
    snap = s.snapshot()
    snap["dropped"]["x"] = 99
    assert s.snapshot() == {"forwarded": 1, "dropped": {"x": 2}}


# --------------------------------------------------------------------------------------------------- control plane
class Capture:
    def __init__(self, ctx: Any) -> None:
        self.events: list[dict[str, Any]] = []
        self.unsub = ctx.events.subscribe(lambda e: self.events.append(e) if e["type"].startswith("dataset.") else None)

    def of(self, t: str) -> list[dict[str, Any]]:
        return [e for e in self.events if e["type"] == t]


@pytest.fixture()
def cap(fleet):
    c = Capture(fleet.ctx)
    yield c
    c.unsub()


def run_job(st, events: list[tuple], **submit: Any) -> dict[str, Any]:
    a = st.agent()
    a.job_plan = lambda spec: JobPlan(events=events, duration=0.3)
    st.wait_worker(a)
    ws = st.workspace()
    done = st.wait_job(st.submit(ws, **submit)["id"])
    assert done["state"] == "succeeded", done
    return done


def test_runner_events_become_platform_events_in_order(fleet, cap):
    st = fleet
    done = run_job(st, [("dataset_statement", {"sql": "SELECT * FROM lh.sales", "dialect": "spark"}, 1),
                        ("dataset_read", {"datasets": [{"kind": "table", "name": "sales"}, {"kind": "file", "path": "Files/raw/a.csv", "format": "csv"}]}, 2),
                        ("dataset_written", {"datasets": [{"kind": "table", "name": "gold"}], "mode": "overwrite"}, 3)])
    assert [e["type"] for e in cap.events] == ["dataset.statement", "dataset.read", "dataset.written"]
    read = cap.of("dataset.read")[0]
    assert read["workspace_id"] == done["workspace_id"] and read["subject"] == done["id"]
    assert read["payload"]["process"] == {"kind": "job", "item_id": None, "job_id": done["id"], "query_id": None, "name": done["name"], "run_id": None}
    assert read["payload"]["engine"] == "python" and [d["name"] or d["path"] for d in read["payload"]["datasets"]] == ["sales", "Files/raw/a.csv"]
    assert cap.of("dataset.written")[0]["payload"]["mode"] == "overwrite"
    # the SQL text never reaches the persisted event table or the job's own timeline
    assert not [e for e in st.ctx.events.list(type_prefix="dataset.", limit=100) if e["type"] == "dataset.statement"]
    timeline = json.dumps(st.job(done["id"])["timeline"])
    assert "SELECT * FROM lh.sales" not in timeline and "sql_chars" in timeline
    assert st.compute.jobs.dataset_event_stats.snapshot()["forwarded"] >= 3


def test_hostile_runner_lines_are_dropped_counted_and_never_fail_the_job(fleet, cap):
    st = fleet
    jobs = st.compute.jobs
    before = jobs.dataset_event_stats.snapshot()
    done = run_job(st, [("dataset_read", {"datasets": [{"kind": "table"}]}, 2), ("dataset_statement", {"sql": 5}, 3),
                        ("dataset_written", {"datasets": [{"kind": "view", "name": "v"}]}, 4), ("dataset_read", {"datasets": [{"kind": "table", "name": "ok"}]}, 5)])
    assert done["state"] == "succeeded"
    assert [e["type"] for e in cap.events] == ["dataset.read"]
    after = jobs.dataset_event_stats.snapshot()
    assert after["forwarded"] - before["forwarded"] == 1
    for reason, n in (("table_without_name", 1), ("statement_without_sql", 1), ("dataset_kind", 1)):
        assert after["dropped"].get(reason, 0) - before["dropped"].get(reason, 0) == n


def test_event_budget_limits_a_chatty_job(fleet, cap):
    st = fleet
    evs = [("dataset_read", {"datasets": [{"kind": "table", "name": f"t{i}"}]}, i + 1) for i in range(le.MAX_EVENTS_PER_JOB + 25)]
    before = st.compute.jobs.dataset_event_stats.snapshot()["dropped"].get("over_budget", 0)
    run_job(st, evs)
    assert len(cap.of("dataset.read")) == le.MAX_EVENTS_PER_JOB
    assert st.compute.jobs.dataset_event_stats.snapshot()["dropped"].get("over_budget", 0) - before == 25


def test_a_publisher_that_raises_keeps_the_job_succeeded(fleet, monkeypatch, caplog):
    st = fleet
    real = st.ctx.events.publish
    dsev.reset_emission_state()

    def publish(type_, payload=None, **kw):
        if type_.startswith("dataset."):
            raise RuntimeError("store down")
        return real(type_, payload, **kw)

    monkeypatch.setattr(st.ctx.events, "publish", publish)
    done = run_job(st, [("dataset_read", {"datasets": [{"kind": "table", "name": "t"}]}, 1), ("dataset_written", {"datasets": [{"kind": "table", "name": "u"}], "mode": "append"}, 2)])
    assert done["state"] == "succeeded" and done["failure"] is None
    assert dsev.emission_failures().get("dataset.read") == 1 and dsev.emission_failures().get("dataset.written") == 1


def test_item_id_is_part_of_job_state_changed_and_the_process(fleet, cap):
    st = fleet
    seen: list[dict[str, Any]] = []
    unsub = st.ctx.events.subscribe(lambda e: seen.append(e) if e["type"] == "job.state_changed" else None)
    try:
        run_job(st, [("dataset_read", {"datasets": [{"kind": "table", "name": "t"}]}, 1)])
    finally:
        unsub()
    assert seen and all("item_id" in e["payload"] for e in seen)


def test_direct_forwarding_with_attached_lakehouses_and_the_default(fleet, cap):
    jobs = fleet.compute.jobs
    row = {"id": "job_direct", "workspace_id": "ws_x", "item_id": None, "name": "d", "kind": "script", "engine": "spark", "attempt": 1,
           "spec": {"lakehouse_ids": [LH, "itm_second"], "default_lakehouse": {"id": LH}}}
    assert jobs.forward_dataset_event(row, "dataset_read", {"datasets": [{"kind": "table", "lakehouse_id": "itm_second", "name": "a"}, {"kind": "table", "lakehouse_id": "itm_evil", "name": "b"}]})
    (e,) = cap.events
    assert [d["lakehouse_id"] for d in e["payload"]["datasets"]] == ["itm_second", None]
    assert jobs.forward_dataset_event(row, "dataset_statement", {"sql": "select 1"})
    assert cap.of("dataset.statement")[0]["payload"]["default_lakehouse_id"] == LH
    assert not jobs.forward_dataset_event(row, "dataset_read", {})
    assert not jobs.forward_dataset_event({}, "dataset_read", {"datasets": [{"kind": "table", "name": "t"}]})       # a row without an id is dropped (internal_error), not raised
    assert jobs.dataset_event_stats.snapshot()["dropped"].get("internal_error", 0) >= 1


def test_a_new_attempt_gets_a_fresh_budget(fleet, cap):
    jobs = fleet.compute.jobs
    base = {"id": "job_budget", "workspace_id": "ws_x", "item_id": None, "name": "b", "kind": "script", "engine": "python", "spec": {}}
    data = {"datasets": [{"kind": "table", "name": "t"}]}
    sent = [jobs.forward_dataset_event({**base, "attempt": 1}, "dataset_read", data) for _ in range(le.MAX_EVENTS_PER_JOB + 3)]
    assert sent.count(True) == le.MAX_EVENTS_PER_JOB
    assert jobs.forward_dataset_event({**base, "attempt": 2}, "dataset_read", data)


def test_session_forwarding_skips_hidden_sql_sessions(fleet, cap):
    sessions = fleet.compute.sessions
    base = {"id": "ses_1", "workspace_id": "ws_x", "notebook_id": "itm_nb", "engine": "python", "spec": {"name": "nb"}, "state": "idle"}
    content = {"event": "dataset_read", "datasets": [{"kind": "table", "name": "t"}], "at": "2026-01-01T00:00:00Z"}
    sessions.handle_kernel_event({**base, "hidden": True}, "status", "event", dict(content), None)
    sessions.handle_kernel_event({**base, "kind": "sql"}, "status", "event", dict(content), None)
    assert cap.events == []
    sessions.handle_kernel_event({**base, "hidden": False, "kind": "notebook"}, "status", "event", dict(content), None)
    (e,) = cap.events
    assert e["payload"]["process"]["run_id"] == "ses_1" and e["payload"]["process"]["item_id"] == "itm_nb" and e["payload"]["process"]["job_id"] is None
    assert e["subject"] == "ses_1" and e["workspace_id"] == "ws_x"


def test_forwarding_a_fabric_job_carries_the_ids_from_its_origin_and_its_attached_lakehouse(fleet, cap):
    jobs = fleet.compute.jobs
    row = {**FAB_ROW, "attempt": 1, "spec": {"lakehouse_ids": [LH], "default_lakehouse": {"id": LH, "fabric": {"workspace_id": FAB_WS, "lakehouse_id": FAB_LH}}}}
    assert jobs.forward_dataset_event(row, "dataset_read", {"datasets": [{"kind": "table", "name": "bronze"}]})
    (e,) = cap.events
    assert e["payload"]["process"]["fabric"]["item_id"] == FAB_NB and e["payload"]["process"]["fabric"]["tenant_id"] == FAB_TENANT

