# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Observability: metrics registry, Prometheus exposition round trip, series store (write / query / compaction / retention), API middleware, OTLP payloads and export, log correlation."""

from __future__ import annotations

import json
import logging
import math
import threading
from datetime import UTC, datetime, timedelta
from typing import Any

import pytest

from olcf.observability import correlation
from olcf.observability.metrics import MetricsRegistry, estimate_quantile, parse_prometheus
from olcf.observability.middleware import install_middleware, route_label, uninstall_middleware
from olcf.observability.otel import (
    OtlpExporter,
    Tracer,
    build_metrics_payload,
    build_traces_payload,
    parse_headers,
)
from tests.orch.orch_support import FakeClock, Inproc

T0 = datetime(2026, 5, 1, 12, 0, 7, tzinfo=UTC)


def value_of(parsed: dict[str, Any], name: str, **labels: str) -> float | None:
    for lab, v in parsed.get(name, []):
        if all(lab.get(k) == x for k, x in labels.items()):
            return v
    return None


# ------------------------------------------------------------------------------------------------------------------------------ registry + exposition
def test_counter_gauge_histogram_semantics() -> None:
    r = MetricsRegistry()
    c = r.counter("jobs_total", "Jobs")
    c.inc(1, state="ok")
    c.inc(2.5, state="ok")
    c.inc(-5, state="ok")                                                                    # counters never go down
    c.inc(float("nan"), state="ok")
    assert c.value(state="ok") == 3.5 and c.value(state="none") == 0.0
    g = r.gauge("queue_depth", "Depth")
    g.set(5, queue="a")
    g.inc(2, queue="a")
    g.dec(1, queue="a")
    assert g.value(queue="a") == 6
    g.replace([({"queue": "b"}, 1.0)])
    assert g.label_sets() == [{"queue": "b"}]
    h = r.histogram("job_duration_seconds", "Duration")
    for v in (0.001, 0.2, 0.2, 7, 5000):
        h.observe(v, kind="x")
    assert h.count(kind="x") == 5 and h.total(kind="x") == pytest.approx(5007.401)
    assert 0.1 <= (h.quantile(0.5, kind="x") or 0) <= 0.25 and h.quantile(0.5, kind="nope") is None
    assert r.counter("jobs_total") is c                                                      # get-or-create returns the same object
    assert r.get("jobs_total") is c and "jobs_total" in r.names()


def test_estimate_quantile_edges() -> None:
    assert estimate_quantile((1.0, 2.0), [0, 0, 0], 0.5) is None
    assert estimate_quantile((1.0, 2.0), [10, 0, 0], 0.5) == pytest.approx(0.5)
    assert estimate_quantile((1.0, 2.0), [0, 0, 5], 0.99) == 2.0                             # in the +Inf bucket: reports the last finite bound


def test_record_sample_dispatches_by_kind() -> None:
    r = MetricsRegistry()
    r.gauge("g1")
    r.counter("c1")
    r.record_sample("g1", 4, {"a": "b"})
    r.record_sample("g1", 9, {"a": "b"})
    r.record_sample("c1", 4)
    r.record_sample("c1", 6)
    r.record_sample("brand_new_seconds", 0.2, {"x": "y"})                                    # unknown names become histograms
    r.record_sample("nothing", None)                                                         # type: ignore[arg-type]
    assert r.get("g1").value(a="b") == 9 and r.get("c1").value() == 10 and r.get("brand_new_seconds").count(x="y") == 1 and r.get("nothing") is None


def test_label_and_name_sanitising_and_cardinality_cap() -> None:
    r = MetricsRegistry()
    c = r.counter("weird name-1", "h")
    c.inc(1, **{"bad-label": "v", "9x": "w"})
    text = r.render_prometheus()
    assert "weird_name_1" in text and 'bad_label="v"' in text and '_9x="w"' in text
    parse_prometheus(text)
    capped = r.counter("capped_total", "h")
    capped.max_series = 5
    for i in range(20):
        capped.inc(1, user=f"u{i}")
    assert len(capped.label_sets()) == 5
    assert value_of(parse_prometheus(r.render_prometheus()), "olcf_metrics_dropped_series_total", metric="capped_total") == 15


def test_prometheus_round_trip_with_awkward_label_values() -> None:
    r = MetricsRegistry()
    nasty = 'quote" back\\slash new\nline {brace} = comma, é'
    r.counter("req_total", "Requests with a\nnewline in help").inc(3, route=nasty, status="200")
    r.gauge("temp", "T").set(-1.5e-7, where="a")
    r.gauge("inf_gauge", "I").set(math.inf)
    h = r.histogram("lat_seconds", "L", buckets=[0.1, 1.0])
    for v in (0.05, 0.5, 0.5, 3):
        h.observe(v, route="r")
    text = r.render_prometheus()
    assert text.endswith("\n") and text.count("# TYPE req_total counter") == 1 and "# TYPE lat_seconds histogram" in text
    p = parse_prometheus(text)
    assert value_of(p, "req_total", route=nasty, status="200") == 3
    assert value_of(p, "temp", where="a") == pytest.approx(-1.5e-7) and value_of(p, "inf_gauge") == math.inf
    assert value_of(p, "lat_seconds_bucket", route="r", le="0.1") == 1 and value_of(p, "lat_seconds_bucket", route="r", le="1") == 3
    assert value_of(p, "lat_seconds_bucket", route="r", le="+Inf") == 4 and value_of(p, "lat_seconds_count", route="r") == 4 and value_of(p, "lat_seconds_sum", route="r") == pytest.approx(4.05)
    assert value_of(p, "olcf_process_start_time_seconds") == pytest.approx(r.process_start)
    buckets = [v for lab, v in p["lat_seconds_bucket"]]
    assert buckets == sorted(buckets)                                                        # cumulative buckets are monotonic


def test_parse_prometheus_rejects_garbage() -> None:
    with pytest.raises(ValueError):
        parse_prometheus("this is not a metric line")
    assert parse_prometheus("# HELP x y\n\nx 1\nx{a=\"b\"} 2 1700000000\n")["x"] == [({}, 1.0), ({"a": "b"}, 2.0)]


def test_registry_is_thread_safe() -> None:
    r = MetricsRegistry()
    c = r.counter("hits_total", "h")
    h = r.histogram("w_seconds", "h")

    def work() -> None:
        for i in range(2000):
            c.inc(1, t="x")
            h.observe(0.001 * (i % 7), t="x")
            if i % 500 == 0:
                r.render_prometheus()

    threads = [threading.Thread(target=work) for _ in range(6)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert c.value(t="x") == 12000 and h.count(t="x") == 12000


def test_collectors_run_before_exposition_and_failures_are_contained() -> None:
    r = MetricsRegistry()
    g = r.gauge("fresh", "h")
    calls = []
    r.add_collector("a", lambda: (calls.append(1), g.set(len(calls)))[0])
    r.add_collector("boom", lambda: 1 / 0)
    assert value_of(parse_prometheus(r.render_prometheus()), "fresh") == 1
    assert value_of(parse_prometheus(r.render_prometheus()), "fresh") == 2
    r.add_collector("a", lambda: g.set(100))                                                 # same name replaces
    assert value_of(parse_prometheus(r.render_prometheus()), "fresh") == 100
    g.set(5)
    assert value_of(parse_prometheus(r.render_prometheus(collect=False)), "fresh") == 5          # collect=False reads the current values without refreshing


def test_snapshot_shape() -> None:
    r = MetricsRegistry()
    r.counter("a_total").inc(2, k="v")
    r.histogram("b_seconds").observe(0.3)
    snap = {m["name"]: m for m in r.snapshot()}
    assert snap["a_total"]["series"] == [{"labels": {"k": "v"}, "value": 2.0}] and snap["b_seconds"]["series"][0]["count"] == 1 and snap["b_seconds"]["buckets"]
    json.dumps(snap)


# ------------------------------------------------------------------------------------------------------------------------------ series store
@pytest.fixture()
def obs(ip_idle: Inproc) -> Inproc:
    ip_idle.clock = FakeClock(T0.isoformat())                                                # type: ignore[attr-defined]
    ip_idle.set_clock(ip_idle.clock)                                                         # type: ignore[attr-defined]
    return ip_idle


def test_series_sample_and_query(obs: Inproc) -> None:
    store, reg, clk = obs.obs.series, obs.metrics, obs.clock                                 # type: ignore[attr-defined]
    c = reg.counter("t_events_total", "h")
    g = reg.gauge("t_depth", "h", scope="global")
    h = reg.histogram("t_work_seconds", "h")
    for i in range(6):
        c.inc(2, kind="a")
        g.set(10 + i, queue="q1")
        h.observe(0.1 * (i + 1), op="x")
        assert store.sample(leader=True) > 0
        clk.advance(15)
    res = store.query("t_depth", since=T0 - timedelta(minutes=1), until=clk())
    [s] = res["series"]
    assert s["labels"] == {"queue": "q1"} and [p["value"] for p in s["points"]] == [10, 11, 12, 13, 14, 15] and res["step"] == 15
    assert store.query("t_depth", labels={"queue": "other"}, since=T0 - timedelta(minutes=1), until=clk())["series"] == []
    res = store.query("t_events_total", labels={"kind": "a"}, since=T0 - timedelta(minutes=1), until=clk())
    assert res["cumulative"] is True and res["series"][0]["labels"]["node"] == str(obs.ctx.node_id) and res["series"][0]["points"][-1]["value"] == 12
    coarse = store.query("t_depth", since=T0 - timedelta(minutes=1), until=clk(), step=60)
    assert coarse["step"] == 60 and sum(p["n"] for p in coarse["series"][0]["points"]) == 6 and coarse["series"][0]["points"][0]["min"] <= coarse["series"][0]["points"][0]["max"]
    hist = store.query("t_work_seconds", since=T0 - timedelta(minutes=1), until=clk())["series"][0]["points"]
    assert hist[0]["value"] == pytest.approx(0.1) and store.query("t_work_seconds_p95", since=T0 - timedelta(minutes=1), until=clk())["series"]
    assert "t_depth" in store.names("t_") and store.names("zzz") == [] and store.stats()["rows"] > 0
    assert store.query("t_depth", since=clk(), until=clk() - timedelta(hours=1))["series"] == []


def test_global_series_are_written_by_the_leader_only(obs: Inproc) -> None:
    store, reg = obs.obs.series, obs.metrics
    reg.gauge("t_only_leader", "h", scope="global").set(1)
    reg.gauge("t_every_node", "h").set(1)
    store.sample(leader=False)
    names = set(store.names("t_"))
    assert "t_every_node" in names and "t_only_leader" not in names
    obs.clock.advance(15)                                                                    # type: ignore[attr-defined]
    store.sample(leader=True)
    assert "t_only_leader" in set(store.names("t_"))
    [s] = store.query("t_only_leader", since=T0 - timedelta(minutes=1), until=obs.clock() + timedelta(minutes=1))["series"]    # type: ignore[attr-defined]
    assert "node" not in s["labels"]                                                         # global series are not per node


def test_compaction_and_retention(obs: Inproc) -> None:
    store, reg, clk = obs.obs.series, obs.metrics, obs.clock                                 # type: ignore[attr-defined]
    g = reg.gauge("t_comp", "h", scope="global")
    cnt = reg.counter("t_comp_total", "h", scope="global")
    for i in range(40):                                                                      # 10 minutes of raw samples
        g.set(float(i))
        cnt.inc(1)
        store.sample(leader=True)
        clk.advance(15)
    before = store.stats()["rows"]
    clk.advance(hours=2)
    done = store.compact()
    assert done["to_300"] > 0 and store.stats()["rows"] < before
    [s] = store.query("t_comp", since=T0 - timedelta(minutes=1), until=T0 + timedelta(hours=1), step=300)["series"]
    assert 1 <= len(s["points"]) <= 3 and sum(p["n"] for p in s["points"]) == 40
    assert min(p["min"] for p in s["points"]) == 0 and max(p["max"] for p in s["points"]) == 39           # extremes survive compaction
    [cs] = store.query("t_comp_total", since=T0 - timedelta(minutes=1), until=T0 + timedelta(hours=1), step=300)["series"]
    assert cs["points"][-1]["value"] == 40                                                   # cumulative series keep the last value, they are not averaged
    clk.advance(hours=30)
    assert store.compact()["to_3600"] > 0
    assert store.purge(days=1) > 0 and store.stats()["rows"] == 0
    assert store.purge() == 0


def test_sampler_pass_persists_collectors_and_is_leader_aware(obs: Inproc) -> None:
    rows = obs.obs.sample_once()
    assert rows > 0 and obs.obs.last_error is None
    names = set(obs.obs.series.names("olcf_"))
    assert {"olcf_uptime_seconds", "olcf_is_leader", "olcf_db_ping_milliseconds"} <= names
    obs.ctx.leader.release()
    obs.clock.advance(15)                                                                    # type: ignore[attr-defined]
    follower_rows = obs.obs.sample_once()
    assert 0 < follower_rows < rows                                                          # global series are skipped on a follower
    obs.ctx.leader.acquire()


def test_collectors_expose_pipeline_trigger_and_webhook_state(obs: Inproc, httpd: Any) -> None:
    obs.hooks.fanout()                                                                       # seeds the event cursor
    obs.hooks.create(obs.admin, {"name": "m", "url": httpd.url + "/x", "event_types": ["*"]})
    obs.ctx.events.publish("job.state_changed", {"to": "failed"}, workspace_id=obs.ws_id)
    obs.hooks.fanout()
    obs.obs.collectors.collect(force=True)
    p = parse_prometheus(obs.metrics.render_prometheus())
    assert value_of(p, "olcf_webhook_deliveries", state="pending") is not None and "olcf_webhook_oldest_due_lag_seconds" in p
    assert value_of(p, "olcf_build_info") == 1 and value_of(p, "olcf_is_leader") == 1 and value_of(p, "olcf_alerts_firing", severity="critical") == 0


# ------------------------------------------------------------------------------------------------------------------------------ API middleware
def test_middleware_records_requests_by_route_template() -> None:
    from fastapi import FastAPI, HTTPException
    from fastapi.testclient import TestClient

    app = FastAPI()
    reg = MetricsRegistry()
    tracer = Tracer(sample_rate=1.0)
    tracer.enabled = True

    @app.get("/v1/things/{thing_id}")
    def thing(thing_id: str) -> dict[str, str]:
        if thing_id == "bad":
            raise HTTPException(404, "no")
        if thing_id == "boom":
            raise RuntimeError("x")
        return {"id": thing_id, "cid": correlation.get("correlation_id") or ""}

    assert install_middleware(app, reg, tracer) and not install_middleware(app, reg, tracer)    # idempotent
    with TestClient(app, raise_server_exceptions=False) as client:
        assert client.get("/v1/things/a1").status_code == 200
        r = client.get("/v1/things/b2", headers={"X-Correlation-ID": "my-correlation-id-1"})
        assert r.json()["cid"] == "my-correlation-id-1"                                      # a well-formed id is propagated into the handler
        assert client.get("/v1/things/bad").status_code == 404
        assert client.get("/v1/things/boom").status_code == 500
        assert client.get("/v1/nope/123456789012345678").status_code == 404
        assert client.get("/v1/things/z", headers={"X-Correlation-ID": "bad id with spaces"}).json()["cid"] not in ("", "bad id with spaces")
    p = parse_prometheus(reg.render_prometheus())
    assert value_of(p, "olcf_api_requests_total", route="/v1/things/{thing_id}", status="200") == 3
    assert value_of(p, "olcf_api_requests_total", route="/v1/things/{thing_id}", status="404") == 1
    assert value_of(p, "olcf_api_requests_total", route="/v1/things/{thing_id}", status="500") == 1
    assert value_of(p, "olcf_api_errors_total", route="/v1/things/{thing_id}", **{"class": "4xx"}) == 1 and value_of(p, "olcf_api_errors_total", route="/v1/things/{thing_id}", **{"class": "5xx"}) == 1
    assert any(lab["route"].startswith("unmatched:/v1/nope/{id}") for lab, _ in p["olcf_api_requests_total"])      # ids never explode label cardinality
    assert value_of(p, "olcf_api_inflight_requests") == 0 and value_of(p, "olcf_api_request_duration_seconds_count", route="/v1/things/{thing_id}") == 5
    spans = tracer.drain()
    assert len(spans) >= 5 and all(len(s["traceId"]) == 32 for s in spans) and any(s["status"]["code"] == 2 for s in spans)
    assert uninstall_middleware(app) and not uninstall_middleware(app)


def test_middleware_honours_incoming_traceparent() -> None:
    from fastapi import FastAPI
    from fastapi.testclient import TestClient

    app = FastAPI()
    reg, tracer = MetricsRegistry(), Tracer(sample_rate=0.0)
    tracer.enabled = True

    @app.get("/v1/x")
    def x() -> dict[str, str | None]:
        return {"trace": correlation.get("trace_id")}

    install_middleware(app, reg, tracer)
    with TestClient(app) as c:
        tid, sid = "4bf92f3577b34da6a3ce929d0e0e4736", "00f067aa0ba902b7"
        assert c.get("/v1/x", headers={"traceparent": f"00-{tid}-{sid}-01"}).json()["trace"] == tid          # parent sampled: joined
        assert c.get("/v1/x", headers={"traceparent": f"00-{tid}-{sid}-00"}).json()["trace"] == tid
        c.get("/v1/x")                                                                       # no parent and sample_rate 0: no span
    spans = tracer.drain()
    assert len(spans) == 1 and spans[0]["traceId"] == tid and spans[0]["parentSpanId"] == sid


def test_route_label_fallbacks() -> None:
    assert route_label({"path": "/v1/jobs/job_0123456789abcdef/logs"}) == "unmatched:/v1/jobs/{id}/logs"
    assert route_label({"path": "/ui/app.js"}) == "ui" and route_label({"path": ""}) == "unmatched"


# ------------------------------------------------------------------------------------------------------------------------------ correlation + OTLP
def test_correlation_binding_and_logging(caplog: pytest.LogCaptureFixture) -> None:
    assert correlation.current() == {}
    with correlation.bind(run_id="run_1", activity_id=None, workspace_id="ws_9") as f:
        assert f == {"run_id": "run_1", "workspace_id": "ws_9"}
        with correlation.bind(correlation_id="abc12345", trace_id="a" * 32, span_id="b" * 16):
            h = correlation.outgoing_headers({"X-Other": "1"})
            assert h["X-Correlation-ID"] == "abc12345" and h["X-Other"] == "1"
            assert correlation.parse_traceparent(h["traceparent"]) == ("a" * 32, "b" * 16, True)
            lg = logging.getLogger("olcf.test.corr")
            flt = correlation.CorrelationFilter()
            rec = lg.makeRecord("olcf.test.corr", logging.INFO, __file__, 1, "hello %s", ("x",), None)
            assert flt.filter(rec) and "hello x [" in rec.msg and "run=run_1" in rec.msg and rec.olcf["workspace_id"] == "ws_9"          # type: ignore[attr-defined]
            flt.filter(rec)
            assert rec.msg.count("run=run_1") == 1                                           # not appended twice
        assert "traceparent" not in correlation.outgoing_headers()
    assert correlation.current() == {} and correlation.outgoing_headers() == {}
    assert correlation.suffix({}) == ""


def test_traceparent_parsing() -> None:
    assert correlation.parse_traceparent(None) is None and correlation.parse_traceparent("garbage") is None
    assert correlation.parse_traceparent("00-" + "0" * 32 + "-" + "1" * 16 + "-01") is None            # all-zero trace id is invalid
    assert correlation.parse_traceparent("00-" + "1" * 32 + "-" + "0" * 16 + "-01") is None
    assert correlation.parse_traceparent("00-" + "A" * 32 + "-" + "B" * 16 + "-00") == ("a" * 32, "b" * 16, False)
    assert correlation.make_traceparent("a" * 32, "b" * 16, False).endswith("-00")


def test_tracer_span_nesting_and_bounds() -> None:
    t = Tracer(sample_rate=1.0, max_spans=3)
    t.add(name="ignored", trace_id="1" * 32, span_id="2" * 16, parent_id=None, start=1, end=2)    # disabled: nothing is kept
    assert t.pending() == 0
    t.enabled = True
    with t.span("outer", attributes={"k": 1}) as outer:
        with t.span("inner"):
            pass
        with pytest.raises(ValueError), t.span("failing"):
            raise ValueError("bad")
    spans = t.drain()
    names = {s["name"]: s for s in spans}
    assert set(names) == {"outer", "inner", "failing"} and names["inner"]["parentSpanId"] == outer["span_id"] and names["inner"]["traceId"] == names["outer"]["traceId"]
    assert names["failing"]["status"]["code"] == 2 and "ValueError" in names["failing"]["status"]["message"]
    for i in range(5):
        t.add(name=f"s{i}", trace_id="1" * 32, span_id=f"{i:016x}", parent_id=None, start=1, end=1)
    assert t.pending() == 3 and t.dropped == 2
    got = t.drain(2)
    t.requeue(got)
    assert t.pending() == 3
    assert t.should_sample(True) and not t.should_sample(False)


def test_otlp_payloads_have_the_wire_shape() -> None:
    r = MetricsRegistry()
    r.counter("c_total", "C").inc(3, a="b")
    r.gauge("g", "G").set(2.5)
    r.gauge("glob", "G", scope="global").set(1)
    r.histogram("h_seconds", "H", buckets=[0.1, 1]).observe(0.5)
    res = {"service.name": "olcf", "service.instance.id": "n1"}
    body = build_metrics_payload(r, resource=res, leader=False, now=1_700_000_000.0)
    [rm] = body["resourceMetrics"]
    assert {"key": "service.name", "value": {"stringValue": "olcf"}} in rm["resource"]["attributes"]
    ms = {m["name"]: m for m in rm["scopeMetrics"][0]["metrics"]}
    assert ms["c_total"]["sum"]["isMonotonic"] is True and ms["c_total"]["sum"]["aggregationTemporality"] == 2 and ms["c_total"]["sum"]["dataPoints"][0]["asDouble"] == 3
    assert ms["c_total"]["sum"]["dataPoints"][0]["attributes"] == [{"key": "a", "value": {"stringValue": "b"}}] and ms["c_total"]["sum"]["dataPoints"][0]["timeUnixNano"] == str(1_700_000_000 * 10**9)
    hp = ms["h_seconds"]["histogram"]["dataPoints"][0]
    assert hp["count"] == "1" and hp["explicitBounds"] == [0.1, 1.0] and hp["bucketCounts"] == ["0", "1", "0"] and len(hp["bucketCounts"]) == len(hp["explicitBounds"]) + 1
    assert "glob" not in ms                                                                   # global metrics only from the leader
    assert "glob" in {m["name"] for m in build_metrics_payload(r, resource=res, leader=True)["resourceMetrics"][0]["scopeMetrics"][0]["metrics"]}
    json.dumps(body)
    tr = build_traces_payload([{"traceId": "1" * 32, "spanId": "2" * 16, "name": "x"}], resource=res)
    assert tr["resourceSpans"][0]["scopeSpans"][0]["spans"][0]["name"] == "x"


def test_otlp_header_parsing() -> None:
    assert parse_headers("Authorization=Bearer abc, X-Team=data") == {"Authorization": "Bearer abc", "X-Team": "data"}
    assert parse_headers("") == {} and parse_headers(None) == {}


def test_otlp_export_end_to_end_with_a_real_http_collector(obs: Inproc, httpd: Any) -> None:
    settings = obs.ctx.service("settings")
    exporter: OtlpExporter = obs.ctx.service("otlp")
    assert exporter.enabled is False and exporter.export_once()["enabled"] is False
    httpd.on("/v1/metrics", 200)
    httpd.on("/v1/traces", 200)
    settings.set("observability.otlp.endpoint", httpd.url)
    sec = obs.ctx.secrets.create("otlp-headers", "X-Scope-OrgID=olcf-test", scope_type="org", scope_id="", created_by=obs.admin.id, meta={})
    settings.set("observability.otlp.headers_secret", sec["id"])
    assert exporter.enabled and exporter.endpoint == httpd.url
    obs.ctx.service("tracer").enabled = True
    obs.ctx.service("tracer").add(name="unit", trace_id="1" * 32, span_id="2" * 16, parent_id=None, start=1.0, end=2.0)
    obs.metrics.counter("t_otlp_total", "h").inc(7)
    res = exporter.export_once()
    assert res["metrics"] == {"ok": True} and res["traces"] == {"ok": True, "spans": 1}
    [m] = httpd.to("/v1/metrics")
    assert m.headers["content-type"].startswith("application/json") and m.headers["x-scope-orgid"] == "olcf-test"
    names = {x["name"] for x in m.json()["resourceMetrics"][0]["scopeMetrics"][0]["metrics"]}
    assert "t_otlp_total" in names
    assert httpd.to("/v1/traces")[0].json()["resourceSpans"][0]["scopeSpans"][0]["spans"][0]["name"] == "unit"
    httpd.on("/v1/traces", 503)
    obs.ctx.service("tracer").add(name="again", trace_id="1" * 32, span_id="3" * 16, parent_id=None, start=1.0, end=2.0)
    bad = exporter.export_once()
    assert bad["traces"]["ok"] is False and "503" in bad["traces"]["error"] and obs.ctx.service("tracer").pending() == 1       # spans are kept for the next attempt
    assert value_of(parse_prometheus(obs.metrics.render_prometheus()), "olcf_otlp_export_failures_total", signal="traces") == 1
    deep = obs.obs._deep()
    assert deep.status.value.lower() == "down" and "OTLP export" in [s["name"] for s in deep.steps if not s["ok"]]       # the deep check surfaces a failing collector
    settings.set("observability.otlp.endpoint", "")


def test_otlp_endpoint_goes_through_the_ssrf_guard(obs: Inproc, httpd: Any) -> None:
    settings = obs.ctx.service("settings")
    settings.set("network.ssrf.allow", [])
    settings.set("observability.otlp.endpoint", httpd.url)
    res = obs.ctx.service("otlp").export_once()
    assert res.get("error") and httpd.hits == []
    settings.set("observability.otlp.endpoint", "")
    settings.set("network.ssrf.allow", ["127.0.0.0/8"])


def test_otlp_settings_validation(obs: Inproc) -> None:
    settings = obs.ctx.service("settings")
    for key, bad in (("observability.otlp.endpoint", "ftp://x"), ("observability.otlp.endpoint", "not a url"), ("observability.otlp.timeout_seconds", 0)):
        with pytest.raises(Exception):  # noqa: B017, PT011
            settings.set(key, bad)


# ------------------------------------------------------------------------------------------------------------------------------ health
def test_metrics_health_check(obs: Inproc) -> None:
    quick = obs.obs._quick()
    assert quick.status.value.lower() == "down"                                              # the sampler thread is not running in Inproc
    obs.obs.start()
    try:
        assert obs.obs._quick().status.value.lower() in ("ok", "degraded")
        deep = obs.obs._deep()
        assert deep.status.value.lower() == "ok" and [s["name"] for s in deep.steps] == ["registry round trip", "series store write", "OTLP export"] and all(s["ok"] for s in deep.steps)
        assert "skipped" in deep.steps[2]["detail"]
    finally:
        obs.obs.stop()
