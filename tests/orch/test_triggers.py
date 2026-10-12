# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
# ruff: noqa: S105, S106
"""Triggers: cron schedules with a fake clock (catch-up, missed runs, time zones, exactly-once), webhook triggers (signature, replay, rate limit), storage arrival and dependencies."""

from __future__ import annotations

import json
import threading
from datetime import UTC, datetime, timedelta
from typing import Any

import pytest
from sqlalchemy import update

from olcf.core.errors import (
    BadRequest,
    Conflict,
    NotFound,
    TooManyRequests,
    Unauthenticated,
    ValidationFailed,
)
from olcf.core.models import ItemUpdate
from olcf.pipelines import triggers as T
from olcf.pipelines.schema import triggers as TRG
from olcf.pipelines.triggers import TriggerService
from olcf.pipelines.webhooks import sign
from tests.orch.orch_support import FakeClock, Inproc

START = "2026-03-01T00:00:30+00:00"


def P(name: str, **props: Any) -> dict[str, Any]:
    return {"name": name, "type": "t_probe", "typeProperties": props}


@pytest.fixture()
def clk(ip_idle: Inproc) -> FakeClock:
    c = FakeClock(START)
    ip_idle.set_clock(c)
    return c


def schedule(ip: Inproc, cron: str = "*/5 * * * *", target: str = "target", name: str | None = None, **over: Any) -> dict[str, Any]:
    d = {"cron": cron, "timezone": "UTC", "target": {"type": "pipeline", "ref": target, "parameters": {"at": "@trigger().scheduledTime"}}, **over}
    return ip.item("schedule", name or f"sch-{len(ip.triggers.list(ip.ws_id))}", d)


def trigger_of(ip: Inproc, item: dict[str, Any]) -> dict[str, Any]:
    rows = [r for r in ip.triggers.list(ip.ws_id, type_="schedule") if r["item_id"] == item["id"]]
    assert len(rows) == 1
    return rows[0]


def runs(ip: Inproc, pipeline: dict[str, Any]) -> list[dict[str, Any]]:
    rows = ip.svc.list_runs(ip.admin, pipeline_id=pipeline["id"], limit=500)["items"]
    return sorted(rows, key=lambda r: r["created_at"])


def fires(ip: Inproc, trg: dict[str, Any]) -> list[dict[str, Any]]:
    return sorted(ip.triggers.runs(trg["id"], 1000), key=lambda r: r["fire_time"])


@pytest.fixture()
def target(ip_idle: Inproc) -> dict[str, Any]:
    return ip_idle.pipeline("target", [P("A")], parameters={"at": {"type": "string", "default": ""}})


# ------------------------------------------------------------------------------------------------------------------------------ cron helpers
@pytest.mark.parametrize(("expr", "needle"), [
    ("", "required"), ("* * *", "5 fields"), ("61 * * * *", "not a valid"), ("0 0 31 2 *", "never fires"), ("* * * * * *", "minimum interval"), ("not a cron", "5 fields"),
])
def test_cron_validation(expr: str, needle: str) -> None:
    with pytest.raises(ValidationFailed) as ei:
        T.validate_cron(expr)
    assert needle in str(ei.value)


def test_cron_validation_accepts_common_expressions() -> None:
    for e in ("* * * * *", "*/15 * * * *", "0 2 * * 1-5", "0 0 1 * *", "30 6 * * MON", "0 0 * * * 0"):
        T.validate_cron(e, 1)


def test_fire_times_and_next_are_timezone_and_dst_aware() -> None:
    berlin = T.parse_tz("Europe/Berlin")
    # DST starts 2026-03-29 in Europe/Berlin: 09:00 local is 08:00Z before and 07:00Z after
    nxt = T.next_fire_times("0 9 * * *", berlin, datetime(2026, 3, 27, 12, 0, tzinfo=UTC), 4)
    assert [t.strftime("%m-%d %H:%M") for t in nxt] == ["03-28 08:00", "03-29 07:00", "03-30 07:00", "03-31 07:00"]
    assert all(t.tzinfo is not None and t.utcoffset() == timedelta(0) for t in nxt)
    ft = T.fire_times("*/10 * * * *", T.parse_tz("UTC"), datetime(2026, 1, 1, 0, 0, tzinfo=UTC), datetime(2026, 1, 1, 1, 0, tzinfo=UTC))
    assert len(ft) == 6 and ft[0].minute == 10 and ft[-1].hour == 1          # (after, upto]
    with pytest.raises(ValidationFailed):
        T.parse_tz("Mars/Olympus")


def test_json_path() -> None:
    body = {"a": {"b": [{"c": 5}, {"c": 6}]}, "k y": 1}
    assert T.jpath(body, "$.a.b[1].c") == 6 and T.jpath(body, "$['k y']") == 1 and T.jpath(body, "$.a.x") is None and T.jpath(body, "$.a.b[9]") is None
    with pytest.raises(BadRequest):
        T.jpath(body, "a.b")


# ------------------------------------------------------------------------------------------------------------------------------ schedule items
def test_schedule_item_creates_a_trigger_with_next_fire_times(ip_idle: Inproc, clk: FakeClock, target: dict[str, Any]) -> None:
    ip = ip_idle
    item = schedule(ip, "*/5 * * * *")
    trg = trigger_of(ip, item)
    assert trg["type"] == "schedule" and trg["enabled"] and trg["target_id"] == target["id"] and trg["config"]["cron"] == "*/5 * * * *"
    nxt = ip.triggers.preview(trg, 3, T.from_iso(trg["state"]["cursor"]))
    assert [n["utc"][11:19] for n in nxt] == ["00:05:00", "00:10:00", "00:15:00"]
    assert nxt[0]["timezone"] == "UTC"


def test_invalid_schedule_definitions_are_rejected_on_save(ip_idle: Inproc, clk: FakeClock, target: dict[str, Any]) -> None:
    ip = ip_idle
    bad: list[dict[str, Any]] = [
        {"cron": "bad", "target": {"type": "pipeline", "ref": "target"}},
        {"cron": "* * * * *", "timezone": "Nowhere/Land", "target": {"type": "pipeline", "ref": "target"}},
        {"cron": "* * * * *", "target": {"type": "pipeline"}},
        {"cron": "* * * * *", "target": {"type": "teapot", "ref": "x"}},
        {"cron": "*/5 * * * *", "target": {"type": "pipeline", "ref": "target"}, "catchup": "yolo"},
        {"cron": "*/5 * * * *", "target": {"type": "pipeline", "ref": "target"}, "surprise": 1},
        {"cron": "*/5 * * * *", "target": {"type": "pipeline", "ref": "target", "parameters": {"x": "@bad("}}},
        {"cron": "*/5 * * * *", "target": {"type": "pipeline", "ref": "target"}, "start_at": "2026-02-01T00:00:00Z", "end_at": "2026-01-01T00:00:00Z"},
    ]
    for i, d in enumerate(bad):
        with pytest.raises(ValidationFailed):
            ip.item("schedule", f"bad{i}", d)


def test_updating_the_schedule_item_updates_the_trigger_and_deleting_removes_it(ip_idle: Inproc, clk: FakeClock, target: dict[str, Any]) -> None:
    ip = ip_idle
    item = schedule(ip, "*/5 * * * *")
    d = dict(item["definition"], cron="0 * * * *")
    ip.ctx.service("items").update(ip.admin, item["id"], ItemUpdate(definition=d), None)
    assert ip.triggers.reconcile_schedules() >= 1
    assert trigger_of(ip, item)["config"]["cron"] == "0 * * * *"
    with pytest.raises(ValidationFailed):
        ip.ctx.service("items").update(ip.admin, item["id"], ItemUpdate(definition=dict(d, cron="nope")), None)
    ip.ctx.service("items").delete(ip.admin, item["id"])
    ip.triggers.reconcile_schedules()
    assert not [r for r in ip.triggers.list(ip.ws_id, type_="schedule") if r["item_id"] == item["id"]]


# ------------------------------------------------------------------------------------------------------------------------------ firing, exactly once
def test_schedule_fires_at_the_cron_time_once(ip_idle: Inproc, clk: FakeClock, target: dict[str, Any]) -> None:
    ip = ip_idle
    item = schedule(ip, "*/5 * * * *")
    trg = trigger_of(ip, item)
    assert ip.triggers.tick_schedules() == 0                                # 00:00:30: nothing due
    clk.set("2026-03-01T00:05:02+00:00")
    assert ip.triggers.tick_schedules() == 1
    assert ip.triggers.tick_schedules() == 0                                # ticking again must not fire again
    rs = runs(ip, target)
    assert len(rs) == 1 and rs[0]["trigger_type"] == "schedule" and rs[0]["trigger"]["scheduledTime"].startswith("2026-03-01T00:05:00")
    assert rs[0]["parameters"]["at"].startswith("2026-03-01T00:05:00")        # @trigger().scheduledTime is the fire time, not the tick time
    f = fires(ip, trg)
    assert [x["status"] for x in f] == ["started"] and f[0]["run_id"] == rs[0]["id"] and f[0]["fire_key"].startswith("2026-03-01T00:05:00")
    clk.set("2026-03-01T00:10:00+00:00")                                    # exactly on the boundary counts
    assert ip.triggers.tick_schedules() == 1
    assert len(runs(ip, target)) == 2


def test_concurrent_replicas_fire_each_slot_exactly_once(ip_idle: Inproc, clk: FakeClock, target: dict[str, Any]) -> None:
    ip = ip_idle
    item = schedule(ip, "* * * * *")
    trg = trigger_of(ip, item)
    replicas = [TriggerService(ip.ctx, ip.engine, clock=clk) for _ in range(4)]
    for minute in range(1, 6):
        clk.set(f"2026-03-01T00:0{minute}:01+00:00")
        barrier = threading.Barrier(len(replicas))
        results: list[int] = []

        def go(svc: TriggerService, barrier: threading.Barrier = barrier, results: list[int] = results) -> None:
            barrier.wait()
            results.append(svc.tick_schedules())

        ts = [threading.Thread(target=go, args=(r,)) for r in replicas]
        [t.start() for t in ts]
        [t.join() for t in ts]
        assert sum(results) == 1, (minute, results)
    assert len(runs(ip, target)) == 5
    keys = [r["fire_key"] for r in fires(ip, trg)]
    assert len(keys) == len(set(keys)) == 5


def test_a_stale_unfinished_claim_is_repaired_without_a_second_run(ip_idle: Inproc, clk: FakeClock, target: dict[str, Any]) -> None:
    ip = ip_idle
    trg = trigger_of(ip, schedule(ip, "*/5 * * * *"))
    key, ft = "2026-03-01T00:05:00.000000Z", datetime(2026, 3, 1, 0, 5, tzinfo=UTC)
    first = ip.triggers._claim(trg, key, ft)                                  # replica A claims and then "crashes" before starting the run
    assert first and ip.triggers._claim(trg, key, ft) is None                  # B cannot take it over yet
    assert ip.triggers.fire(trg, fire_key=key, fire_time=ft)["status"] == "duplicate"
    clk.advance(120)
    res = ip.triggers.fire(trg, fire_key=key, fire_time=ft)                    # the stale claim is taken over and the run started
    assert res["status"] == "started" and res["trigger_run_id"] == first
    assert ip.triggers.fire(trg, fire_key=key, fire_time=ft)["status"] == "duplicate"
    assert len(runs(ip, target)) == 1


# ------------------------------------------------------------------------------------------------------------------------------ catch-up
def _downtime(ip: Inproc, clk: FakeClock, item_cron: str, hours: float, **over: Any) -> dict[str, Any]:
    item = schedule(ip, item_cron, **over)
    clk.advance(hours=hours)
    return trigger_of(ip, item)


def test_catchup_skip_records_missed_runs_and_does_not_replay(ip_idle: Inproc, clk: FakeClock, target: dict[str, Any]) -> None:
    ip = ip_idle
    clk.set("2026-03-01T00:00:00+00:00")
    trg = _downtime(ip, clk, "*/10 * * * *", 2, catchup="skip", misfire_grace_seconds=300)       # 12 slots missed, the latest (02:00) is exactly due
    clk.advance(400)                                                                         # 02:06:40 - the newest slot is 400 s late (> grace)
    assert ip.triggers.tick_schedules() == 0
    assert runs(ip, target) == []
    st = [x["status"] for x in fires(ip, trg)]
    assert st.count("skipped") == 12 and "started" not in st
    assert all("catchup=skip" in (x["reason"] or "") or "missed" in (x["reason"] or "") for x in fires(ip, trg))
    clk.set("2026-03-01T02:10:00+00:00")                                                     # business as usual afterwards
    assert ip.triggers.tick_schedules() == 1


def test_catchup_skip_fires_the_latest_slot_when_within_the_grace_period(ip_idle: Inproc, clk: FakeClock, target: dict[str, Any]) -> None:
    ip = ip_idle
    clk.set("2026-03-01T00:00:00+00:00")
    trg = _downtime(ip, clk, "*/10 * * * *", 2, catchup="skip", misfire_grace_seconds=300)
    clk.advance(60)                                                                          # 02:01:00 - newest slot (02:00) is 60 s late
    assert ip.triggers.tick_schedules() == 1
    rs = runs(ip, target)
    assert len(rs) == 1 and rs[0]["trigger"]["scheduledTime"].startswith("2026-03-01T02:00:00")
    st = [x["status"] for x in fires(ip, trg)]
    assert st.count("started") == 1 and st.count("skipped") == 11


def test_catchup_run_once_collapses_the_backlog(ip_idle: Inproc, clk: FakeClock, target: dict[str, Any]) -> None:
    ip = ip_idle
    clk.set("2026-03-01T00:00:00+00:00")
    trg = _downtime(ip, clk, "*/10 * * * *", 3, catchup="run_once")
    clk.advance(400)
    assert ip.triggers.tick_schedules() == 1
    rs = runs(ip, target)
    assert len(rs) == 1 and rs[0]["trigger"]["scheduledTime"].startswith("2026-03-01T03:00:00")
    st = [x["status"] for x in fires(ip, trg)]
    assert st.count("started") == 1 and st.count("skipped") == 17
    assert ip.triggers.tick_schedules() == 0


def test_catchup_run_all_replays_every_missed_slot_in_order(ip_idle: Inproc, clk: FakeClock, target: dict[str, Any]) -> None:
    ip = ip_idle
    clk.set("2026-03-01T00:00:00+00:00")
    trg = _downtime(ip, clk, "*/10 * * * *", 2, catchup="run_all")
    assert ip.triggers.tick_schedules() == 12
    assert ip.triggers.tick_schedules() == 0
    rs = runs(ip, target)
    times = [r["trigger"]["scheduledTime"][11:16] for r in rs]
    assert len(rs) == 12
    assert sorted(times)[0] == "00:10" and sorted(times)[-1] == "02:00" and len(set(times)) == 12
    assert all(x["status"] == "started" for x in fires(ip, trg))


def test_catchup_run_all_is_batched_by_max_catchup_runs(ip_idle: Inproc, clk: FakeClock, target: dict[str, Any]) -> None:
    ip = ip_idle
    clk.set("2026-03-01T00:00:00+00:00")
    _downtime(ip, clk, "* * * * *", 1, catchup="run_all", max_catchup_runs=25)
    assert [ip.triggers.tick_schedules() for _ in range(4)] == [25, 25, 10, 0]
    assert len(runs(ip, target)) == 60


def test_pause_does_not_replay_the_paused_period(ip_idle: Inproc, clk: FakeClock, target: dict[str, Any]) -> None:
    ip = ip_idle
    clk.set("2026-03-01T00:00:00+00:00")
    item = schedule(ip, "*/10 * * * *", catchup="run_all")
    trg = trigger_of(ip, item)
    ip.triggers.pause(trg["id"], clk() + timedelta(hours=1))
    clk.advance(minutes=45)
    assert ip.triggers.tick_schedules() == 0 and runs(ip, target) == []
    clk.advance(minutes=20)                                                                  # pause over at 01:05 (now 01:05)
    assert ip.triggers.tick_schedules() == 0                                                 # cursor jumps to now: nothing replayed even with run_all
    assert ip.triggers.get(trg["id"])["paused_until"] is None
    clk.advance(minutes=10)
    assert ip.triggers.tick_schedules() == 1
    assert ip.triggers.pause(trg["id"], None)["paused_until"] is None


def test_disable_and_enable_do_not_replay(ip_idle: Inproc, clk: FakeClock, target: dict[str, Any]) -> None:
    ip = ip_idle
    clk.set("2026-03-01T00:00:00+00:00")
    item = schedule(ip, "*/10 * * * *", catchup="run_all")
    trg = trigger_of(ip, item)
    off = ip.triggers.set_enabled(ip.admin, trg["id"], False)
    assert off["enabled"] is False
    assert ip.ctx.service("items").get(item["id"])["definition"]["enabled"] is False        # the item is the source of truth
    clk.advance(hours=2)
    assert ip.triggers.tick_schedules() == 0
    on = ip.triggers.set_enabled(ip.admin, trg["id"], True)
    assert on["enabled"] is True
    assert ip.triggers.tick_schedules() == 0                                                 # the disabled period is not replayed
    clk.advance(minutes=10)
    assert ip.triggers.tick_schedules() == 1 and len(runs(ip, target)) == 1


def test_start_and_end_windows(ip_idle: Inproc, clk: FakeClock, target: dict[str, Any]) -> None:
    ip = ip_idle
    clk.set("2026-03-01T00:00:00+00:00")
    schedule(ip, "*/10 * * * *", start_at="2026-03-01T01:00:00+00:00", end_at="2026-03-01T01:30:00+00:00", catchup="run_all")
    clk.set("2026-03-01T00:50:00+00:00")
    assert ip.triggers.tick_schedules() == 0                                                 # before start_at
    clk.set("2026-03-01T01:35:00+00:00")
    n = ip.triggers.tick_schedules()
    times = sorted(r["trigger"]["scheduledTime"][11:16] for r in runs(ip, target))
    assert n == len(times) and times[0] >= "01:00" and times[-1] <= "01:30"
    clk.set("2026-03-01T03:00:00+00:00")
    assert ip.triggers.tick_schedules() == 0                                                 # after end_at nothing fires


def test_backfill_from_replays_history_once(ip_idle: Inproc, clk: FakeClock, target: dict[str, Any]) -> None:
    ip = ip_idle
    clk.set("2026-03-01T06:00:00+00:00")
    schedule(ip, "0 * * * *", catchup="run_all", backfill_from="2026-03-01T00:00:00+00:00")
    assert ip.triggers.tick_schedules() == 6
    assert ip.triggers.tick_schedules() == 0


def test_run_now_runs_as_the_caller_and_leaves_the_schedule_alone(ip_idle: Inproc, clk: FakeClock, target: dict[str, Any]) -> None:
    ip = ip_idle
    item = schedule(ip, "0 3 * * *")
    trg = trigger_of(ip, item)
    cursor = trg["state"]["cursor"]
    res = ip.triggers.run_now(ip.admin, trg["id"])
    assert res["status"] == "started"
    run = ip.get(res["run_id"])
    assert run["principal_id"] == ip.admin.id and run["trigger"]["manual"] is True and run["trigger_type"] == "schedule"
    assert ip.triggers.get(trg["id"])["state"]["cursor"] == cursor
    res2 = ip.triggers.run_now(ip.admin, trg["id"])
    assert res2["run_id"] != res["run_id"]                                                   # each manual run is distinct
    clk.set("2026-03-01T03:00:05+00:00")
    assert ip.triggers.tick_schedules() == 1                                                 # and the scheduled one still fires


def test_schedule_trigger_failing_target_is_recorded_not_raised(ip_idle: Inproc, clk: FakeClock) -> None:
    ip = ip_idle
    ip.pipeline("tmp-target", [P("A")])
    item = schedule(ip, "*/5 * * * *", target="tmp-target")
    trg = trigger_of(ip, item)
    with ip.ctx.db.begin() as conn:
        conn.execute(update(TRG).where(TRG.c.id == trg["id"]).values(owner_id="usr_does_not_exist"))
    clk.set("2026-03-01T00:05:01+00:00")
    assert ip.triggers.tick_schedules() == 0
    f = fires(ip, trg)
    assert [x["status"] for x in f] == ["failed"] and "owner" in f[0]["reason"]
    ev = [e["type"] for e in ip.ctx.events.list(after_seq=0, limit=200, type_prefix="pipeline.trigger.")]
    assert "pipeline.trigger.failed" in ev


# ------------------------------------------------------------------------------------------------------------------------------ webhook triggers
@pytest.fixture()
def hook(ip_idle: Inproc, clk: FakeClock) -> dict[str, Any]:
    ip = ip_idle
    tgt = ip.pipeline("hook-target", [P("A", value="@pipeline().parameters.n")], parameters={"n": {"type": "int", "default": 0}, "who": {"type": "string", "default": ""}})
    row, shown = ip.triggers.create(ip.admin, ip.ws_id, {"type": "webhook", "name": "gh", "target": {"type": "pipeline", "ref": "hook-target"},
                                                         "config": {"payload_mapping": {"n": "$.data.count", "who": "@concat('by ', triggerBody().sender)"}, "rate_limit_per_minute": 100}})
    return {"row": row, "secret": shown["secret"], "url": shown["url"], "target": tgt}


def call(ip: Inproc, hook: dict[str, Any], body: Any, *, ts: float | None = None, secret: str | None = None, extra: dict[str, str] | None = None, raw: bytes | None = None) -> dict[str, Any]:
    payload = raw if raw is not None else json.dumps(body).encode()
    t = str(int(ts if ts is not None else ip.triggers.now().timestamp()))
    headers = {"X-OLCF-Timestamp": t, "X-OLCF-Signature": sign(secret or hook["secret"], t, payload), "Content-Type": "application/json", **(extra or {})}
    return ip.triggers.handle_webhook(hook["row"]["id"], headers, payload)


def test_webhook_trigger_accepts_a_signed_call_and_maps_the_payload(ip_idle: Inproc, hook: dict[str, Any]) -> None:
    ip = ip_idle
    assert hook["url"].endswith(f"/v1/hooks/pipelines/{hook['row']['id']}") and hook["secret"] and "secret" not in ip.triggers.public(hook["row"])
    res = call(ip, hook, {"data": {"count": 7}, "sender": "alice"})
    assert res["status"] == "accepted" and res["run_id"]
    run = ip.get(res["run_id"])
    assert run["parameters"] == {"n": 7, "who": "by alice"} and run["trigger_type"] == "webhook" and run["trigger"]["body"]["sender"] == "alice"
    assert run["principal_id"] == ip.admin.id
    f = ip.triggers.runs(hook["row"]["id"])
    assert len(f) == 1 and f[0]["status"] == "started" and f[0]["run_id"] == res["run_id"]
    assert hook["secret"] not in json.dumps(f, default=str) + json.dumps(run, default=str)


def test_webhook_rejects_bad_signatures_stale_timestamps_and_tampering(ip_idle: Inproc, hook: dict[str, Any]) -> None:
    ip = ip_idle
    now = ip.triggers.now().timestamp()
    with pytest.raises(Unauthenticated):
        call(ip, hook, {"x": 1}, secret="a-different-secret-value-0000000000")
    with pytest.raises(Unauthenticated) as ei:
        call(ip, hook, {"x": 1}, ts=now - 3600)
    assert "tolerance" in str(ei.value)
    with pytest.raises(Unauthenticated):
        call(ip, hook, {"x": 1}, ts=now + 3600)
    with pytest.raises(Unauthenticated):
        ip.triggers.handle_webhook(hook["row"]["id"], {}, b"{}")                           # no headers at all
    body = json.dumps({"data": {"count": 1}}).encode()
    t = str(int(now))
    with pytest.raises(Unauthenticated):
        ip.triggers.handle_webhook(hook["row"]["id"], {"X-OLCF-Timestamp": t, "X-OLCF-Signature": sign(hook["secret"], t, body)}, body + b" ")        # body changed after signing
    with pytest.raises(Unauthenticated):
        ip.triggers.handle_webhook(hook["row"]["id"], {"X-OLCF-Timestamp": str(int(now) + 1), "X-OLCF-Signature": sign(hook["secret"], t, body)}, body)  # timestamp swapped
    with pytest.raises(Unauthenticated):
        ip.triggers.handle_webhook(hook["row"]["id"], {"X-OLCF-Timestamp": "yesterday", "X-OLCF-Signature": "sha256=00"}, body)
    assert runs(ip, hook["target"]) == []


def test_webhook_replay_protection(ip_idle: Inproc, hook: dict[str, Any], clk: FakeClock) -> None:
    ip = ip_idle
    body = {"data": {"count": 2}, "sender": "bob"}
    ts = clk().timestamp()
    assert call(ip, hook, body, ts=ts)["status"] == "accepted"
    with pytest.raises(Conflict) as ei:
        call(ip, hook, body, ts=ts)
    assert "replay" in str(ei.value)
    with pytest.raises(Conflict):                                                            # an unsigned header cannot be used to dodge replay detection
        call(ip, hook, body, ts=ts, extra={"X-OLCF-Delivery": "a-brand-new-delivery-id"})
    clk.advance(5)
    assert call(ip, hook, body, ts=clk().timestamp())["status"] == "accepted"                # a genuinely new signature (new timestamp) is a new delivery
    assert len(runs(ip, hook["target"])) == 2
    assert [x["status"] for x in ip.triggers.runs(hook["row"]["id"])] == ["started", "started"]


def test_webhook_replay_outside_the_tolerance_window_is_rejected(ip_idle: Inproc, hook: dict[str, Any], clk: FakeClock) -> None:
    ip = ip_idle
    ts = clk().timestamp()
    call(ip, hook, {"data": {"count": 1}, "sender": "x"}, ts=ts)
    clk.advance(301)
    with pytest.raises(Unauthenticated):
        call(ip, hook, {"data": {"count": 1}, "sender": "x"}, ts=ts)


def test_webhook_payload_and_size_validation(ip_idle: Inproc, hook: dict[str, Any]) -> None:
    ip = ip_idle
    with pytest.raises(BadRequest):
        call(ip, hook, None, raw=b"this is not json")
    with pytest.raises(BadRequest):
        call(ip, hook, None, raw=b"x" * (T.MAX_BODY_BYTES + 1))
    with pytest.raises(BadRequest) as ei:                                                    # parameter coercion failure surfaces as a clear 400
        call(ip, hook, {"data": {"count": "many"}, "sender": "z"})
    assert "could not start" in str(ei.value) or "n" in str(ei.value)


def test_webhook_rate_limit(ip_idle: Inproc, clk: FakeClock) -> None:
    ip = ip_idle
    ip.pipeline("rl-target", [P("A")])
    row, shown = ip.triggers.create(ip.admin, ip.ws_id, {"type": "webhook", "name": "rl", "target": {"type": "pipeline", "ref": "rl-target"}, "config": {"rate_limit_per_minute": 3}})
    h = {"row": row, "secret": shown["secret"]}
    codes = []
    for i in range(5):
        try:
            call(ip, h, {"i": i})
            codes.append(202)
        except TooManyRequests:
            codes.append(429)
    assert codes == [202, 202, 202, 429, 429]
    with pytest.raises(Unauthenticated):                                                     # bad signatures never consume the authenticated budget
        call(ip, h, {"i": 99}, secret="wrong-secret-wrong-secret-wrong")


def test_webhook_disabled_unknown_and_wrong_type_are_404(ip_idle: Inproc, hook: dict[str, Any], clk: FakeClock) -> None:
    ip = ip_idle
    with pytest.raises(NotFound):
        ip.triggers.handle_webhook("trg_nope", {}, b"{}")
    ip.triggers.set_enabled(ip.admin, hook["row"]["id"], False)
    with pytest.raises(NotFound):
        call(ip, hook, {"data": {"count": 1}, "sender": "x"})
    item = schedule(ip, "0 3 * * *", target="hook-target")
    with pytest.raises(NotFound):
        ip.triggers.handle_webhook(trigger_of(ip, item)["id"], {}, b"{}")


def test_webhook_secret_rotation(ip_idle: Inproc, hook: dict[str, Any], clk: FakeClock) -> None:
    ip = ip_idle
    new = ip.triggers.rotate_secret(hook["row"]["id"])["secret"]
    assert new != hook["secret"]
    with pytest.raises(Unauthenticated):
        call(ip, hook, {"data": {"count": 1}, "sender": "x"})
    assert call(ip, {**hook, "secret": new}, {"data": {"count": 1}, "sender": "x"})["status"] == "accepted"


def test_bearer_webhook_trigger_and_passthrough(ip_idle: Inproc, clk: FakeClock) -> None:
    ip = ip_idle
    ip.pipeline("b-target", [P("A", value="@pipeline().parameters.n")], parameters={"n": {"type": "int", "default": 0}})
    row, shown = ip.triggers.create(ip.admin, ip.ws_id, {"type": "webhook", "name": "bearer", "target": {"type": "pipeline", "ref": "b-target"}, "config": {"auth": "bearer", "passthrough": True}})
    tok = shown["token"]
    assert "secret" not in shown and tok.startswith("olcf_hook_") and "token_hash" not in ip.triggers.public(row)
    body = json.dumps({"n": 4}).encode()
    with pytest.raises(Unauthenticated):
        ip.triggers.handle_webhook(row["id"], {"Authorization": "Bearer nope"}, body)
    with pytest.raises(Unauthenticated):
        ip.triggers.handle_webhook(row["id"], {}, body)
    res = ip.triggers.handle_webhook(row["id"], {"Authorization": f"Bearer {tok}", "X-OLCF-Delivery": "d-1"}, body)
    assert ip.get(res["run_id"])["parameters"] == {"n": 4}
    with pytest.raises(Conflict):                                                            # the sender's delivery id makes retries idempotent
        ip.triggers.handle_webhook(row["id"], {"Authorization": f"Bearer {tok}", "X-OLCF-Delivery": "d-1"}, body)
    new = ip.triggers.rotate_secret(row["id"])["token"]
    with pytest.raises(Unauthenticated):
        ip.triggers.handle_webhook(row["id"], {"Authorization": f"Bearer {tok}", "X-OLCF-Delivery": "d-2"}, body)
    assert ip.triggers.handle_webhook(row["id"], {"Authorization": f"Bearer {new}", "X-OLCF-Delivery": "d-2"}, body)["status"] == "accepted"


def test_trigger_config_validation(ip_idle: Inproc, clk: FakeClock) -> None:
    ip = ip_idle
    ip.pipeline("v-target", [P("A")])
    base = {"type": "webhook", "name": "v", "target": {"type": "pipeline", "ref": "v-target"}}
    for bad in ({"config": {"auth": "magic"}}, {"config": {"rate_limit_per_minute": 0}}, {"config": {"nope": 1}}, {"config": {"payload_mapping": {"x": "@bad("}}}, {"config": {"tolerance_seconds": 99999}},
                {"target": {"type": "pipeline", "ref": "does-not-exist"}}, {"type": "carrier-pigeon"}, {"name": ""}, {"parameters": {"x": "@bad("}}):
        with pytest.raises((ValidationFailed, NotFound)):
            ip.triggers.create(ip.admin, ip.ws_id, {**base, **bad})
    # updating and deleting
    row, _ = ip.triggers.create(ip.admin, ip.ws_id, base)
    upd = ip.triggers.update(row["id"], {"name": "renamed", "config": {"rate_limit_per_minute": 5}}, ip.admin)
    assert upd["name"] == "renamed" and upd["config"]["rate_limit_per_minute"] == 5 and upd["etag"] != row["etag"]
    ip.triggers.delete(row["id"])
    with pytest.raises(NotFound):
        ip.triggers.get(row["id"])


# ------------------------------------------------------------------------------------------------------------------------------ dependency triggers
def test_dependency_trigger_follows_the_upstream_pipeline(ip: Inproc) -> None:
    up = ip.pipeline("up", [P("A", value="u")])
    bad = ip.pipeline("up-bad", [{"name": "A", "type": "t_probe", "typeProperties": {"fail": True}}])
    ip.pipeline("down", [P("B", value="@pipeline().parameters.src")], parameters={"src": {"type": "string", "default": ""}})
    ok_row, _ = ip.triggers.create(ip.admin, ip.ws_id, {"type": "dependency", "name": "after-up", "target": {"type": "pipeline", "ref": "down"},
                                                       "parameters": {"src": "@trigger().body.upstreamRunId"}, "config": {"upstream": {"pipeline": "up"}}})
    fail_row, _ = ip.triggers.create(ip.admin, ip.ws_id, {"type": "dependency", "name": "after-bad", "target": {"type": "pipeline", "ref": "down"},
                                                         "config": {"upstream": {"pipeline": "up-bad"}, "on": ["failed"]}})
    r1 = ip.wait(ip.run(up)["id"])
    r2 = ip.wait(ip.run(bad)["id"])
    assert r1["state"] == "succeeded" and r2["state"] == "failed"
    assert ip.triggers.tick_dependencies() == 2
    assert ip.triggers.tick_dependencies() == 0                                              # cursor advanced: nothing fires twice
    fired = {x["trigger_id"]: x for tid in (ok_row["id"], fail_row["id"]) for x in ip.triggers.runs(tid)}
    assert set(fired) == {ok_row["id"], fail_row["id"]} and all(x["status"] == "started" for x in fired.values())
    run = ip.get(fired[ok_row["id"]]["run_id"])
    assert run["parameters"]["src"] == r1["id"] and run["trigger_type"] == "dependency"
    # an upstream run that finished before the trigger existed does not fire it
    late_row, _ = ip.triggers.create(ip.admin, ip.ws_id, {"type": "dependency", "name": "late", "target": {"type": "pipeline", "ref": "down"}, "config": {"upstream": {"pipeline": "up"}}})
    assert ip.triggers.tick_dependencies() == 0 and ip.triggers.runs(late_row["id"]) == []
    ip.wait(ip.run(up)["id"])
    assert ip.triggers.tick_dependencies() == 2                                              # now both "up" triggers fire for the new run


# ------------------------------------------------------------------------------------------------------------------------------ storage arrival
def test_storage_arrival_debounce_batches_and_dedupes(ip_idle: Inproc, clk: FakeClock) -> None:
    ip = ip_idle
    lh = ip.lakehouse("arrivals")
    ip.pipeline("ingest", [P("A")], parameters={"files": {"type": "array", "default": []}})
    row, _ = ip.triggers.create(ip.admin, ip.ws_id, {"type": "storage_event", "name": "csv-arrival", "target": {"type": "pipeline", "ref": "ingest"},
                                                    "parameters": {"files": "@trigger().files"},
                                                    "config": {"lakehouse": "arrivals", "path": "Files/in", "pattern": "*.csv", "poll_seconds": 5, "debounce_seconds": 20}})
    assert row["config"]["lakehouse"] == lh.id
    ip.storage.write_bytes(lh, "Files/in/existing.csv", b"a\n")
    assert ip.triggers.tick_storage() == 0                                                   # first poll records the baseline (existing files do not fire)
    clk.advance(10)
    ip.storage.write_bytes(lh, "Files/in/a.csv", b"1\n")
    ip.storage.write_bytes(lh, "Files/in/ignore.txt", b"nope")
    assert ip.triggers.tick_storage() == 0                                                   # change seen, debounce running
    clk.advance(10)
    ip.storage.write_bytes(lh, "Files/in/b.csv", b"2\n")                                      # another file arrives: the quiet period restarts
    assert ip.triggers.tick_storage() == 0
    clk.advance(15)
    assert ip.triggers.tick_storage() == 0                                                   # still within 20 s of the last change
    clk.advance(10)
    assert ip.triggers.tick_storage() == 1                                                   # quiet for 25 s: one batched run
    [f] = ip.triggers.runs(row["id"])
    run = ip.get(f["run_id"])
    assert sorted(run["trigger"]["files"]) == ["Files/in/a.csv", "Files/in/b.csv"] and sorted(run["parameters"]["files"]) == ["Files/in/a.csv", "Files/in/b.csv"]
    clk.advance(60)
    assert ip.triggers.tick_storage() == 0                                                   # known files do not fire again
    clk.advance(10)
    ip.storage.write_bytes(lh, "Files/in/a.csv", b"1\nchanged\n")                             # a modified file does
    ip.triggers.tick_storage()
    clk.advance(30)
    assert ip.triggers.tick_storage() == 1
    st = ip.triggers.get(row["id"])["state"]
    assert st["pending_count"] == 0 and st["known_count"] == 3


def test_storage_arrival_per_file_mode_and_include_existing(ip_idle: Inproc, clk: FakeClock) -> None:
    ip = ip_idle
    lh = ip.lakehouse("arrivals2")
    ip.pipeline("ingest2", [P("A")])
    for n in ("x", "y"):
        ip.storage.write_bytes(lh, f"Files/in/{n}.json", b"{}")
    row, _ = ip.triggers.create(ip.admin, ip.ws_id, {"type": "storage_event", "name": "per-file", "target": {"type": "pipeline", "ref": "ingest2"},
                                                    "config": {"lakehouse": "arrivals2", "path": "Files/in", "mode": "per_file", "debounce_seconds": 0, "include_existing": True, "poll_seconds": 5}})
    assert ip.triggers.tick_storage() == 2                                                   # one run per file, existing files included
    assert sorted(len(ip.get(f["run_id"])["trigger"]["files"]) for f in ip.triggers.runs(row["id"])) == [1, 1]
    clk.advance(10)
    assert ip.triggers.tick_storage() == 0


def test_storage_trigger_requires_lakehouse_read_access(ip_idle: Inproc, clk: FakeClock) -> None:
    ip = ip_idle
    ip.pipeline("ingest3", [P("A")])
    with pytest.raises(Exception) as ei:
        ip.triggers.create(ip.admin, ip.ws_id, {"type": "storage_event", "name": "nolh", "target": {"type": "pipeline", "ref": "ingest3"}, "config": {"lakehouse": "missing", "path": "Files"}})
    assert "lakehouse" in str(ei.value).lower() or "not found" in str(ei.value).lower()
