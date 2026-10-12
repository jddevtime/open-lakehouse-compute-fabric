# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Property-based fuzzing (hypothesis) of the highest-risk inputs: credentials, item definitions, SQL, file paths, job specs and list filters.

Skipped when ``hypothesis`` is not installed (it is a test-only dependency: ``pip install hypothesis``). Property: whatever is sent, the answer is never a 5xx, never a stack
trace and never contains a canary from the other tenant.
"""

from __future__ import annotations

import json

import pytest

hyp = pytest.importorskip("hypothesis")
from hypothesis import HealthCheck, given, settings  # noqa: E402
from hypothesis import strategies as st  # noqa: E402

from tests.security.secenv import CANARY_B, SECRET_A, SECRET_B  # noqa: E402

SETTINGS = settings(max_examples=60, deadline=None, suppress_health_check=[HealthCheck.function_scoped_fixture, HealthCheck.too_slow], database=None)
TEXT = st.text(alphabet=st.characters(blacklist_categories=("Cs",)), max_size=300)
NASTY = st.sampled_from(["../../etc/passwd", "'; DROP TABLE x;--", "\x00", "${jndi:x}", "{{7*7}}", "%s%n", "A" * 5000, "file:///etc/passwd", "‮", "\n\r"])
SCALAR = st.one_of(st.none(), st.booleans(), st.integers(min_value=-(2**70), max_value=2**70), st.floats(allow_nan=False, allow_infinity=False), TEXT, NASTY)
JSON = st.recursive(SCALAR, lambda inner: st.one_of(st.lists(inner, max_size=5), st.dictionaries(TEXT.filter(lambda k: len(k) < 40), inner, max_size=5)), max_leaves=25)


def ok(resp, platform: bool = False) -> None:
    text = resp.text
    assert resp.status_code < 500 or (resp.status_code == 503 and '"unavailable"' in text), (resp.status_code, text[:200])
    assert "Traceback" not in text and "site-packages" not in text
    assert (platform or CANARY_B not in text) and SECRET_B not in text and SECRET_A not in text


@pytest.fixture(scope="module")
def env(lenient_tenants):
    return lenient_tenants


def send(env, method: str, url: str, who: str = "a_contrib", **kw):
    try:
        body = json.dumps(kw.pop("json"), allow_nan=False).encode() if "json" in kw else None
    except ValueError:
        return None
    headers = {**env.headers(who), **({"Content-Type": "application/json"} if body is not None else {})}
    r = env.c.request(method, url, content=body, headers=headers, **kw)
    ok(r, platform=who == "admin")
    return r


@SETTINGS
@given(doc=JSON)
def test_login_accepts_anything_without_crashing(env, doc):
    r = env.c.post("/v1/identity/login", content=json.dumps(doc, allow_nan=False).encode() if True else b"", headers={"Content-Type": "application/json"})
    ok(r)
    assert r.status_code != 200


@SETTINGS
@given(token=TEXT.filter(lambda t: t.isascii() and "\n" not in t and "\r" not in t))
def test_bearer_token_parser(env, token):
    r = env.c.get("/v1/identity/me", headers={"Authorization": f"Bearer {token}"})
    ok(r)
    assert r.status_code == 401


@SETTINGS
@given(name=TEXT, definition=JSON, typ=st.sampled_from(["notebook", "pipeline", "variable_set", "environment", "lakehouse", "report", "schedule", "sql_query", "spark_job_definition"]))
def test_item_definitions(env, name, definition, typ):
    send(env, "POST", f"/v1/workspaces/{env.tenants['A'].ws}/items", who="a_admin", json={"type": typ, "name": name, "definition": definition})


@SETTINGS
@given(sql=st.one_of(TEXT, st.sampled_from(["select 1", "select * from lh_a.t", "with a as (select 1) select * from a"]).map(lambda s: s + " " + "x" * 3), NASTY))
def test_sql_endpoint(env, sql):
    send(env, "POST", "/v1/sql/execute", json={"sql": sql, "lakehouse": "lh_a", "workspace_id": env.tenants["A"].ws})


@SETTINGS
@given(path=st.one_of(TEXT, NASTY, st.lists(st.sampled_from(["..", ".", "Files", "Tables", "a", "%2e%2e", "\\", ""]), min_size=1, max_size=8).map("/".join)))
def test_file_paths(env, path):
    lh = env.tenants["A"].ids["lh_id"]
    r = send(env, "GET", f"/v1/lakehouses/{lh}/files/content", params={"path": path})
    if r is not None:
        assert "root:x:0:0" not in r.text
    send(env, "GET", f"/v1/lakehouses/{lh}/files", params={"path": path})


@SETTINGS
@given(spec=JSON)
def test_job_submission(env, spec):
    base = {"kind": "script", "workspace_id": env.tenants["A"].ws, "engine": "python", "source": {"format": "py", "content": "print(1)", "filename": "x.py"}}
    send(env, "POST", "/v1/jobs", who="a_admin", json={**base, **(spec if isinstance(spec, dict) else {"parameters": spec})})


@SETTINGS
@given(params=st.dictionaries(st.sampled_from(["limit", "cursor", "state", "kind", "text", "since", "until", "workspace_id", "order", "type", "q", "offset"]), st.one_of(TEXT, NASTY), max_size=6))
def test_list_filters(env, params):
    for path in ("/v1/jobs", "/v1/audit", "/v1/workspaces", "/v1/secrets", "/v1/connections", "/v1/events", "/v1/catalog/search", "/v1/pipeline-runs"):
        send(env, "GET", path, who="a_admin" if path != "/v1/audit" else "admin", params=params)
