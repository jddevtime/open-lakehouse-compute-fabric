# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
# ruff: noqa: S105, S106
"""Data-plane activities against real backends: ``http`` (local HTTP server, SSRF guard, redaction), ``sql`` (embedded DuckDB through the SQL service) and ``copy`` (local lakehouse
storage: binary, csv/jsonl/parquet conversion, Delta table sink, resume, verification, lineage, authorisation). No Spark and no JVM."""

from __future__ import annotations

import io
import json
from typing import Any

import pytest

from olcf.core.errors import ValidationFailed
from tests.orch.orch_support import Inproc


def A(name: str, type_: str, *deps: Any, policy: dict[str, Any] | None = None, **props: Any) -> dict[str, Any]:
    a: dict[str, Any] = {"name": name, "type": type_, "typeProperties": props}
    if deps:
        a["dependsOn"] = list(deps)
    if policy:
        a["policy"] = policy
    return a


# ------------------------------------------------------------------------------------------------------------------------------ http
def test_http_get_json_post_body_headers_and_query(ip: Inproc, httpd: Any) -> None:
    httpd.on("/api/items", (200, json.dumps({"items": [1, 2, 3], "next": None}), {"Content-Type": "application/json", "X-Rate-Limit": "5"}))
    httpd.on("/api/create", (201, json.dumps({"id": "abc"}), {"Content-Type": "application/json"}))
    run, acts = ip.execute([
        A("G", "http", url=httpd.url + "/api/items", queryParams={"page": 2, "q": "a b"}, headers={"X-Trace": "t-1"}),
        A("P", "http", "G", url=httpd.url + "/api/create", method="POST", body={"n": "@length(activity('G').output.body.items)"}, expectStatus=[201]),
    ], outputs={"count": "@activity('G').output.body.items[2]", "id": "@activity('P').output.body.id"})
    assert run["state"] == "succeeded", run
    g, p = acts["G"][0]["output"], acts["P"][0]["output"]
    assert g["statusCode"] == 200 and g["body"]["items"] == [1, 2, 3] and g["headers"]["x-rate-limit"] == "5" and g["elapsedMs"] >= 0
    assert run["outputs"] == {"count": 3, "id": "abc"}
    get_hit, post_hit = httpd.to("/api/items")[0], httpd.to("/api/create")[0]
    assert get_hit.method == "GET" and get_hit.path == "/api/items?page=2&q=a+b" and get_hit.headers["x-trace"] == "t-1" and get_hit.headers["user-agent"].startswith("olcf-pipelines")
    assert post_hit.method == "POST" and post_hit.json() == {"n": 3} and post_hit.headers["content-type"].startswith("application/json")
    assert p["statusCode"] == 201


def test_http_unexpected_status_fails_and_retries_only_server_errors(ip: Inproc, httpd: Any) -> None:
    httpd.on("/nf", 404)
    httpd.on("/flaky", 503, 503, (200, "fine", {"Content-Type": "text/plain"}))
    httpd.on("/ok-404", 404)
    run, acts = ip.execute([A("NF", "http", url=httpd.url + "/nf", policy={"retries": 3, "retryBackoffSeconds": 0}),
                            A("FL", "http", url=httpd.url + "/flaky", policy={"retries": 3, "retryBackoffSeconds": 0}),
                            A("OK", "http", url=httpd.url + "/ok-404", expectStatus=[200, 404])])
    assert run["state"] == "failed"
    assert acts["NF"][0]["state"] == "failed" and "404" in acts["NF"][0]["error"]["message"] and len(httpd.to("/nf")) == 1                  # client errors are not retried
    assert acts["FL"][0]["state"] == "succeeded" and len(httpd.to("/flaky")) == 3 and acts["FL"][0]["output"]["body"] == "fine"            # 5xx are
    assert acts["OK"][0]["state"] == "succeeded"


def test_http_ssrf_guard_blocks_private_targets_and_redirects(ip: Inproc, httpd: Any) -> None:
    httpd.on("/hop", (302, "", {"Location": "http://169.254.169.254/latest/meta-data/"}))
    httpd.on("/hop2", (302, "", {"Location": httpd.url + "/final"}))
    httpd.on("/final", (200, "arrived", {"Content-Type": "text/plain"}))
    run, acts = ip.execute([A("R", "http", url=httpd.url + "/hop", expectStatus=[302]), A("F", "http", url=httpd.url + "/hop2", followRedirects=True),
                            A("N", "http", url=httpd.url + "/hop2", expectStatus=[302]), A("B", "http", url="http://169.254.169.254/latest/meta-data/", policy={"retries": 3, "retryBackoffSeconds": 0})])
    assert acts["R"][0]["state"] == "succeeded" and acts["R"][0]["output"]["statusCode"] == 302          # redirects are not followed by default
    assert acts["F"][0]["output"]["body"] == "arrived" and acts["N"][0]["output"]["statusCode"] == 302
    assert acts["B"][0]["state"] == "failed" and acts["B"][0]["error"]["category"] == "ssrf" and acts["B"][0]["attempt"] == 1                # never retried
    ip.ctx.service("settings").set("network.ssrf.allow", [])
    run, acts = ip.execute([A("L", "http", url=httpd.url + "/final")])
    assert run["state"] == "failed" and acts["L"][0]["error"]["category"] == "ssrf" and len(httpd.to("/final")) == 1
    ip.ctx.service("settings").set("network.ssrf.allow", ["127.0.0.0/8"])
    ip.ctx.service("settings").set("network.ssrf.deny", [])
    httpd.on("/hop3", (302, "", {"Location": "http://localhost:1/"}))


def test_http_follow_redirect_into_a_blocked_range_is_refused(ip: Inproc, httpd: Any) -> None:
    ip.ctx.service("settings").set("network.ssrf.deny", ["10.0.0.0/8", "169.254.0.0/16"])
    httpd.on("/evil", (302, "", {"Location": "http://10.1.2.3/admin"}))
    run, acts = ip.execute([A("E", "http", url=httpd.url + "/evil", followRedirects=True)])
    assert run["state"] == "failed" and acts["E"][0]["error"]["category"] == "ssrf"
    ip.ctx.service("settings").set("network.ssrf.deny", [])


def test_http_response_size_cap_and_timeout(ip: Inproc, httpd: Any) -> None:
    httpd.on("/big", (200, "x" * 100_000, {"Content-Type": "text/plain"}))
    httpd.on("/slow", ("sleep", 3, 200))
    run, acts = ip.execute([A("Big", "http", url=httpd.url + "/big", maxResponseBytes=1000), A("Slow", "http", url=httpd.url + "/slow", timeoutSeconds=1, policy={"retries": 0})])
    big = acts["Big"][0]["output"]
    assert len(big["body"]) == 1000 and big["bodyTruncated"] is True
    assert acts["Slow"][0]["state"] == "failed" and acts["Slow"][0]["error"]["category"] == "timeout"


def test_http_auth_uses_a_secret_and_never_leaks_it(ip: Inproc, httpd: Any) -> None:
    sec = ip.ctx.secrets.create("api-token", {"token": "tok_live_0123456789abcdef"}, scope_type="workspace", scope_id=ip.ws_id, created_by=ip.admin.id, meta={})  # gitleaks:allow
    basic = ip.ctx.secrets.create("api-basic", {"username": "svc", "password": "p4ssw0rd-0123456789"}, scope_type="workspace", scope_id=ip.ws_id, created_by=ip.admin.id, meta={})  # gitleaks:allow
    echo = lambda v: (200, json.dumps({"echo": "ok", "leak": v}), {"Content-Type": "application/json"})  # noqa: E731 - a server that reflects the credential it received
    httpd.on("/secure-b", echo("tok_live_0123456789abcdef"))
    httpd.on("/secure-c", echo("p4ssw0rd-0123456789"))
    httpd.on("/secure-h", echo("tok_live_0123456789abcdef"))
    httpd.on("/secure", 200)
    run, acts = ip.execute([A("B", "http", url=httpd.url + "/secure-b", auth={"type": "bearer", "secret": sec["id"]}),
                            A("C", "http", "B", url=httpd.url + "/secure-c", auth={"type": "basic", "secret": basic["id"]}),
                            A("H", "http", "C", url=httpd.url + "/secure-h", auth={"type": "header", "name": "X-Api-Key", "secret": sec["id"]})])
    assert run["state"] == "succeeded", run
    h1, h2, h3 = httpd.to("/secure-b")[0], httpd.to("/secure-c")[0], httpd.to("/secure-h")[0]
    assert h1.headers["authorization"] == "Bearer tok_live_0123456789abcdef"
    import base64

    assert h2.headers["authorization"] == "Basic " + base64.b64encode(b"svc:p4ssw0rd-0123456789").decode() and h3.headers["x-api-key"] == "tok_live_0123456789abcdef"
    assert acts["B"][0]["output"]["body"]["leak"] == "***" and acts["C"][0]["output"]["body"]["leak"] == "***" and acts["H"][0]["output"]["body"]["echo"] == "ok"
    dump = json.dumps({"run": run, "acts": acts}, default=str)
    assert "tok_live_0123456789abcdef" not in dump and "p4ssw0rd-0123456789" not in dump                  # not in inputs, outputs or errors (the echoed token in the body is redacted too)
    run, acts = ip.execute([A("M", "http", url=httpd.url + "/secure", auth={"type": "bearer", "secret": "sec_does_not_exist"})])
    assert run["state"] == "failed" and acts["M"][0]["error"]["category"] in ("not_found", "configuration") and acts["M"][0]["attempt"] == 1
    other_ws = ip.workspace()["id"]
    foreign = ip.ctx.secrets.create("foreign", {"token": "zzz-foreign-secret-0123456789"}, scope_type="workspace", scope_id=other_ws, created_by=ip.admin.id, meta={})  # gitleaks:allow
    run, acts = ip.execute([A("X", "http", url=httpd.url + "/secure", auth={"type": "bearer", "secret": foreign["id"]})])
    assert run["state"] == "failed" and "another workspace" in acts["X"][0]["error"]["message"]           # a pipeline cannot use another workspace's secret


def test_http_definition_validation(ip: Inproc) -> None:
    for props in ({"url": "ftp://x/y"}, {"url": "http://user:pw@example.com/"}, {"url": "http://example.com", "headers": {"Host": "evil"}}, {"url": "http://example.com", "auth": {"type": "bearer"}},
                  {"url": "http://example.com", "auth": {"type": "header", "secret": "s"}}, {"url": "http://example.com", "auth": {"type": "magic", "secret": "s"}}, {"url": "http://example.com", "method": "TRACE"}):
        with pytest.raises(ValidationFailed):
            ip.pipeline("bad-" + str(abs(hash(json.dumps(props)))), [A("H", "http", **props)])


def test_http_carries_the_correlation_header(ip: Inproc, httpd: Any) -> None:
    from olcf.observability import correlation

    with correlation.bind(correlation_id="corr-1234567890", trace_id="a" * 32, span_id="b" * 16):
        assert correlation.outgoing_headers()["X-Correlation-ID"] == "corr-1234567890"
    run, _ = ip.execute([A("H", "http", url=httpd.url + "/corr")])
    assert run["state"] == "succeeded"
    hit = httpd.to("/corr")[0]
    assert hit.headers["user-agent"].startswith("olcf-pipelines")


# ------------------------------------------------------------------------------------------------------------------------------ sql
def test_sql_scalar_rows_and_bound_parameters(ip: Inproc) -> None:
    lh = ip.lakehouse("sqllh")
    run, acts = ip.execute([
        A("S", "sql", sql="SELECT 41 + 1 AS answer", lakehouse=lh.name),
        A("R", "sql", "S", sql="SELECT * FROM (VALUES (1, 'a'), (2, 'b'), (3, 'c')) t(id, name) WHERE id >= :min ORDER BY id", parameters={"min": 2}, lakehouse=lh.id),
        A("T", "sql", "R", sql="SELECT :txt AS t", parameters={"txt": "x'; DROP TABLE users; --"}, lakehouse=lh.name),
    ], outputs={"answer": "@activity('S').output.scalar", "n": "@activity('R').output.rowCount", "first": "@activity('R').output.firstRow.name", "t": "@activity('T').output.scalar"})
    assert run["state"] == "succeeded", (run, acts)
    assert run["outputs"] == {"answer": 42, "n": 2, "first": "b", "t": "x'; DROP TABLE users; --"}         # parameters are bound, never concatenated
    r = acts["R"][0]["output"]
    assert [c["name"] for c in r["columns"]] == ["id", "name"] and r["rows"] == [{"id": 2, "name": "b"}, {"id": 3, "name": "c"}] and r["truncated"] is False and r["engine"] == "duckdb"


def test_sql_row_limit_error_and_expression_parameters(ip: Inproc) -> None:
    lh = ip.lakehouse("sqllh2")
    run, acts = ip.execute([A("L", "sql", sql="SELECT i FROM range(0, 50) t(i)", maxRows=10, lakehouse=lh.name),
                            A("E", "sql", "L", sql="SELECT * FROM table_that_does_not_exist", lakehouse=lh.name, policy={"retries": 2, "retryBackoffSeconds": 0}),
                            A("P", "sql", "L", sql="SELECT :n * 2 AS d", parameters={"n": "@activity('L').output.rowCount"}, lakehouse=lh.name)])
    assert acts["L"][0]["output"]["rowCount"] >= 10 and len(acts["L"][0]["output"]["rows"]) == 10 and acts["L"][0]["output"]["truncated"] is True
    assert acts["E"][0]["state"] == "failed" and acts["E"][0]["attempt"] == 1 and "table_that_does_not_exist" in acts["E"][0]["error"]["message"]       # a user error is not retried
    assert acts["P"][0]["output"]["scalar"] == acts["L"][0]["output"]["rowCount"] * 2
    assert run["state"] == "failed"
    run, acts = ip.execute([A("N", "sql", sql="SELECT 1", lakehouse="no-such-lakehouse")])
    assert run["state"] == "failed" and acts["N"][0]["error"]["category"] == "configuration"


def test_sql_requires_the_runs_principal_to_be_allowed(ip: Inproc) -> None:
    from olcf.pipelines.activities.sql import is_read_only

    assert is_read_only("SELECT 1") and is_read_only("with a as (select 1) select * from a") and not is_read_only("DROP TABLE x") and not is_read_only("select 1; select 2")
    assert not is_read_only("INSERT INTO t VALUES (1)") and not is_read_only("select 1 -- ; \n; drop table x") and is_read_only("select 'drop table x'")


# ------------------------------------------------------------------------------------------------------------------------------ copy
def put(ip: Inproc, lh: Any, path: str, data: bytes | str) -> None:
    ip.storage.write_bytes(lh, path, data if isinstance(data, bytes) else data.encode())


def read(ip: Inproc, lh: Any, path: str) -> bytes:
    return b"".join(ip.storage.read_bytes(lh, path)) if not isinstance(ip.storage.read_bytes(lh, path), bytes) else ip.storage.read_bytes(lh, path)


CSV = "id,name,score\n1,ann,9.5\n2,bob,7.25\n3,cy,8\n"


def test_copy_binary_files_with_pattern_flatten_and_verification(ip: Inproc) -> None:
    src, dst = ip.lakehouse("src"), ip.lakehouse("dst")
    put(ip, src, "Files/in/a.csv", CSV)
    put(ip, src, "Files/in/sub/b.csv", CSV + "4,di,6\n")
    put(ip, src, "Files/in/sub/c.txt", "ignored")
    run, acts = ip.execute([A("C", "copy", source={"lakehouse": src.name, "path": "Files/in", "pattern": "*.csv"}, sink={"lakehouse": dst.name, "path": "Files/out"}, options={"verify": "checksum"})])
    assert run["state"] == "succeeded", run
    out = acts["C"][0]["output"]
    assert out["filesCopied"] == 2 and out["filesSkipped"] == 0 and out["bytesWritten"] == len(CSV) * 2 + 7
    assert {f["sink"] for f in out["files"]} == {"Files/out/a.csv", "Files/out/sub/b.csv"}
    assert ip.storage.stat(dst, "Files/out/sub/b.csv").size == len(CSV) + 7
    assert not [f for f in ip.storage.list_files(dst, "Files/out", True) if ".olcf-tmp" in f.path and not f.is_dir]            # no temporary parts are left behind
    run, acts = ip.execute([A("F", "copy", source={"lakehouse": src.name, "path": "Files/in", "pattern": "*.csv"}, sink={"lakehouse": dst.name, "path": "Files/flat", "flatten": True})])
    assert {f["sink"] for f in acts["F"][0]["output"]["files"]} == {"Files/flat/a.csv", "Files/flat/b.csv"}
    run, acts = ip.execute([A("S", "copy", source={"lakehouse": src.name, "path": "Files/in/a.csv"}, sink={"lakehouse": dst.name, "path": "Files/single/renamed.csv"})])
    assert run["state"] == "succeeded" and ip.storage.stat(dst, "Files/single/renamed.csv").size == len(CSV)


def test_copy_modes_and_missing_source(ip: Inproc) -> None:
    src, dst = ip.lakehouse("src2"), ip.lakehouse("dst2")
    put(ip, src, "Files/in/a.csv", CSV)
    put(ip, dst, "Files/out/a.csv", "old")
    def cp(mode: str, **opt: Any) -> Any:
        return ip.execute([A("C", "copy", source={"lakehouse": src.name, "path": "Files/in"}, sink={"lakehouse": dst.name, "path": "Files/out", "mode": mode}, options=opt)])
    run, acts = cp("skip_existing")
    assert run["state"] == "succeeded" and acts["C"][0]["output"]["filesSkipped"] == 1 and ip.storage.stat(dst, "Files/out/a.csv").size == 3
    run, acts = cp("fail_if_exists")
    assert run["state"] == "failed" and "already exists" in acts["C"][0]["error"]["message"]
    run, acts = cp("overwrite")
    assert run["state"] == "succeeded" and ip.storage.stat(dst, "Files/out/a.csv").size == len(CSV)
    run, acts = ip.execute([A("M", "copy", source={"lakehouse": src.name, "path": "Files/nope"}, sink={"lakehouse": dst.name, "path": "Files/x"}, policy={"retries": 2, "retryBackoffSeconds": 0})])
    assert run["state"] == "failed" and acts["M"][0]["error"]["category"] == "not_found" and acts["M"][0]["attempt"] == 1
    run, acts = cp("overwrite", failOnEmpty=True) if False else ip.execute([A("E", "copy", source={"lakehouse": src.name, "path": "Files/in", "pattern": "*.zzz"}, sink={"lakehouse": dst.name, "path": "Files/x"}, options={"failOnEmpty": True})])
    assert run["state"] == "failed"
    run, acts = ip.execute([A("E2", "copy", source={"lakehouse": src.name, "path": "Files/in", "pattern": "*.zzz"}, sink={"lakehouse": dst.name, "path": "Files/x"})])
    assert run["state"] == "succeeded" and acts["E2"][0]["output"]["filesCopied"] == 0
    run, acts = ip.execute([A("Lim", "copy", source={"lakehouse": src.name, "path": "Files/in"}, sink={"lakehouse": dst.name, "path": "Files/x"}, options={"maxBytes": 5})])
    assert run["state"] == "failed" and acts["Lim"][0]["error"]["category"] == "limit"


def test_copy_converts_csv_to_parquet_and_jsonl_with_row_verification(ip: Inproc) -> None:
    import pyarrow.parquet as pq

    src, dst = ip.lakehouse("src3"), ip.lakehouse("dst3")
    put(ip, src, "Files/in/people.csv", CSV)
    run, acts = ip.execute([A("P", "copy", source={"lakehouse": src.name, "path": "Files/in", "format": "csv"}, sink={"lakehouse": dst.name, "path": "Files/pq", "format": "parquet"}),
                            A("J", "copy", "P", source={"lakehouse": dst.name, "path": "Files/pq", "format": "parquet"}, sink={"lakehouse": dst.name, "path": "Files/js", "format": "jsonl"})])
    assert run["state"] == "succeeded", (run, acts)
    assert acts["P"][0]["output"]["rowsCopied"] == 3 and [f["sink"] for f in acts["P"][0]["output"]["files"]] == ["Files/pq/people.parquet"]
    table = pq.read_table(io.BytesIO(read(ip, dst, "Files/pq/people.parquet")))
    assert table.num_rows == 3 and table.column_names == ["id", "name", "score"] and table.column("name").to_pylist() == ["ann", "bob", "cy"]
    lines = read(ip, dst, "Files/js/people.jsonl").decode().strip().splitlines()
    assert [json.loads(x)["name"] for x in lines] == ["ann", "bob", "cy"]
    run, acts = ip.execute([A("Bin", "copy", source={"lakehouse": src.name, "path": "Files/in", "format": "binary"}, sink={"lakehouse": dst.name, "path": "Files/x", "format": "parquet"})])
    assert run["state"] == "failed" and acts["Bin"][0]["error"]["category"] == "configuration"


def test_copy_to_delta_table_then_query_it_with_sql_and_append_idempotently(ip: Inproc) -> None:
    src, dst = ip.lakehouse("src4"), ip.lakehouse("dst4")
    put(ip, src, "Files/in/people.csv", CSV)
    copy = A("C", "copy", source={"lakehouse": src.name, "path": "Files/in/people.csv", "format": "csv"}, sink={"lakehouse": dst.name, "table": "people", "mode": "overwrite"})
    run, acts = ip.execute([copy, A("Q", "sql", "C", sql="SELECT count(*) AS n, sum(score) AS s FROM people", lakehouse=dst.name)])
    assert run["state"] == "succeeded", (run, acts)
    assert acts["C"][0]["output"]["rowsCopied"] == 3 and acts["Q"][0]["output"]["firstRow"] == {"n": 3, "s": pytest.approx(24.75)}
    app = A("C", "copy", source={"lakehouse": src.name, "path": "Files/in/people.csv", "format": "csv"}, sink={"lakehouse": dst.name, "table": "people", "mode": "append"})
    run, acts = ip.execute([app, A("Q", "sql", "C", sql="SELECT count(*) AS n FROM people", lakehouse=dst.name)])
    assert acts["Q"][0]["output"]["scalar"] == 6
    run, acts = ip.execute([A("E", "copy", source={"lakehouse": src.name, "path": "Files/in/people.csv", "format": "csv"}, sink={"lakehouse": dst.name, "table": "people", "mode": "error"})])
    assert run["state"] == "failed"
    # table -> table
    run, acts = ip.execute([A("T", "copy", source={"lakehouse": dst.name, "table": "people"}, sink={"lakehouse": dst.name, "table": "people_copy", "mode": "overwrite"}),
                            A("Q", "sql", "T", sql="SELECT count(*) FROM people_copy", lakehouse=dst.name)])
    assert run["state"] == "succeeded" and acts["Q"][0]["output"]["scalar"] == 6


def test_copy_manifest_skips_finished_files_when_the_activity_runs_again(ip: Inproc) -> None:
    src, dst = ip.lakehouse("src5"), ip.lakehouse("dst5")
    for i in range(4):
        put(ip, src, f"Files/in/f{i}.bin", bytes([i]) * 1000)
    p = ip.pipeline("copy-resume", [A("C", "copy", source={"lakehouse": src.name, "path": "Files/in"}, sink={"lakehouse": dst.name, "path": "Files/out", "mode": "fail_if_exists"}, options={"parallelism": 1}),
                                    A("Fail", "fail", "C", message="after copy")])
    first = ip.wait(ip.run(p)["id"])
    assert first["state"] == "failed" and ip.acts(first["id"])["C"][0]["output"]["filesCopied"] == 4
    # forcing C to execute again in the same run group: fail_if_exists would refuse the existing sink files, but the manifest knows they were produced by this copy
    again = ip.wait(ip.svc.resume(ip.admin, first["id"], rerun=["C"])["id"])
    c = ip.acts(again["id"])["C"][0]
    assert c["state"] == "succeeded" and c["output"]["filesResumed"] == 4 and c["output"]["filesCopied"] == 0
    # the same files copied by an unrelated pipeline are a conflict
    p2 = ip.pipeline("copy-other", [A("C", "copy", source={"lakehouse": src.name, "path": "Files/in"}, sink={"lakehouse": dst.name, "path": "Files/out", "mode": "fail_if_exists"})])
    r2 = ip.wait(ip.run(p2)["id"])
    assert r2["state"] == "failed" and "already exists" in ip.acts(r2["id"])["C"][0]["error"]["message"]
    # a changed source file is copied again
    put(ip, src, "Files/in/f0.bin", b"changed-content")
    third = ip.wait(ip.svc.resume(ip.admin, first["id"], rerun=["C"])["id"])
    out = ip.acts(third["id"])["C"][0]
    assert out["state"] == "failed" or out["output"]["filesResumed"] == 3


def test_copy_records_lineage_and_enforces_authorisation(ip: Inproc) -> None:
    src, dst = ip.lakehouse("src6"), ip.lakehouse("dst6")
    put(ip, src, "Files/in/a.csv", CSV)
    run, acts = ip.execute([A("C", "copy", source={"lakehouse": src.name, "path": "Files/in"}, sink={"lakehouse": dst.name, "path": "Files/out"})])
    assert run["state"] == "succeeded"
    graph = ip.ctx.service("catalog").lineage(ip.admin, {"kind": "file", "lakehouse_id": dst.id, "path": "Files/out"}, direction="up")
    nodes = graph["nodes"] if isinstance(graph["nodes"], list) else list(graph["nodes"].values())
    assert any(n.get("lakehouse_id") == src.id and n.get("path") == "Files/in" for n in nodes), graph
    assert any((e.get("via") or {}).get("kind") == "pipeline" for e in graph["edges"])
    ident = ip.ctx.service("identity")
    row = ident.create_principal("copy-viewer", kind="user", password="correct horse battery staple")
    ident.bind(row["id"], "Viewer", scope_type="workspace", scope_id=ip.ws_id)
    viewer = ip.ctx.authz.principal_from_row(row)
    p = ip.pipeline("copy-auth", [A("C", "copy", source={"lakehouse": src.name, "path": "Files/in"}, sink={"lakehouse": dst.name, "path": "Files/out2"})])
    from olcf.core.errors import Forbidden

    with pytest.raises(Forbidden):                                                             # a viewer cannot start runs at all
        ip.svc.run(viewer, ip.ws_id, p["id"], {})
    run, acts = ip.execute([A("X", "copy", source={"lakehouse": "no-such", "path": "Files/in"}, sink={"lakehouse": dst.name, "path": "Files/x"})])
    assert run["state"] == "failed" and acts["X"][0]["attempt"] == 1


def test_copy_definition_validation(ip: Inproc) -> None:
    for i, (source, sink) in enumerate([({"lakehouse": "a"}, {"lakehouse": "b", "path": "x"}), ({"lakehouse": "a", "path": "p", "table": "t"}, {"lakehouse": "b", "path": "x"}),
                                         ({"lakehouse": "a", "path": "p"}, {"lakehouse": "b"}), ({"lakehouse": "a", "path": "p"}, {"lakehouse": "b", "table": "t", "format": "csv"}),
                                         ({"lakehouse": "a", "path": "p"}, {"lakehouse": "b", "table": "t", "mode": "skip_existing"}), ({"lakehouse": "a", "path": "p"}, {"lakehouse": "b", "path": "x", "mode": "append"}),
                                         ({"lakehouse": "a", "path": "p", "format": "delta"}, {"lakehouse": "b", "path": "x"})]):
        with pytest.raises(ValidationFailed):
            ip.pipeline(f"badcopy{i}", [A("C", "copy", source=source, sink=sink)])
