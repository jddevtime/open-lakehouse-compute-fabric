# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""The Spark UI reverse proxies are authenticated, authorised, read-only and confined to the cluster's UI."""

from __future__ import annotations

from typing import Any

import pytest

from tests.compute.fake_agent import JobPlan


@pytest.fixture()
def cluster(fleet: Any) -> Any:
    st = fleet
    ws = st.workspace()
    a = st.agent()
    a.job_plan = lambda spec: JobPlan(spark_app=True, duration=0.3)
    st.wait_worker(a)
    job = st.submit(ws, engine="spark", resources={"driver_cores": 1, "driver_memory_mb": 1024, "executor_cores": 1, "executor_memory_mb": 1024, "min_executors": 1},
                    policy={"placement": {"provider": "standalone"}})
    done = st.wait_job(job["id"], timeout=60)
    assert done["state"] == "succeeded" and done["cluster_id"], done
    return st, ws, done["cluster_id"], done


def test_cluster_ui_requires_authentication_and_rewrites_links(cluster: Any) -> None:
    st, _ws, cid, _job = cluster
    base = f"/v1/spark-ui/{cid}"
    assert st.http.get(f"{base}/proxy-test").status_code == 401
    assert st.http.get(f"{base}/proxy-test", headers={"Authorization": "Bearer olcf_pat_bogusbogusbogus"}).status_code == 401
    r = st.http.get(f"{base}/proxy-test", headers=st.admin)
    assert r.status_code == 200 and r.headers["content-type"].startswith("text/html")
    assert f'href="{base}/jobs/"' in r.text  # root-relative links stay inside the proxy
    assert "no-store" in r.headers["Cache-Control"] and "nosniff" in r.headers["X-Content-Type-Options"] and "frame-ancestors" in r.headers["Content-Security-Policy"]
    assert st.http.get(f"{base}/json/", headers=st.admin).json()["status"] == "ALIVE"  # JSON passes through untouched
    assert st.http.get(f"{base}/does-not-exist", headers=st.admin).status_code == 404
    # the bare cluster URL redirects to the trailing-slash form instead of leaking the upstream address
    r = st.http.get(base, headers=st.admin, follow_redirects=False)
    assert r.status_code == 307 and r.headers["location"].endswith(base + "/")


def test_query_token_becomes_a_scoped_cookie_and_is_not_forwarded(cluster: Any) -> None:
    st, _ws, cid, _job = cluster
    base = f"/v1/spark-ui/{cluster_id_safe(cid)}"
    tok = st.admin["Authorization"].split(" ", 1)[1]
    with __import__("httpx").Client(base_url=st.url) as anon:  # a browser: no Authorization header, only the link and its cookie
        r = anon.get(f"{base}/proxy-test", params={"access_token": tok})
        assert r.status_code == 200
        sc = r.headers["set-cookie"]
        assert "olcf_ui=" in sc and "HttpOnly" in sc and f"Path={base}" in sc and "SameSite=strict" in sc
        assert anon.get(f"{base}/proxy-test").status_code == 200  # the cookie carries the session for sub-resources
        assert anon.get("/v1/spark-ui/clu_other/proxy-test").status_code == 401  # ... but only below the cluster path
        assert anon.get("/v1/workers").status_code == 401  # and never for the rest of the API


def cluster_id_safe(cid: str) -> str:
    assert cid.startswith("clu_")
    return cid


def test_proxy_is_read_only_and_path_confined(cluster: Any) -> None:
    st, _ws, cid, _job = cluster
    base = f"/v1/spark-ui/{cid}"
    assert st.http.post(f"{base}/proxy-test", headers=st.admin).status_code == 405
    assert st.http.delete(f"{base}/proxy-test", headers=st.admin).status_code == 405
    for evil in ("%2e%2e/%2e%2e/etc/passwd", "a%5c..%5cb", "..%2fsecret"):
        r = st.http.get(f"{base}/{evil}", headers=st.admin)
        assert r.status_code in (400, 404), (evil, r.status_code)
    assert st.http.get("/v1/spark-ui/clu_unknown/x", headers=st.admin).status_code == 404


def test_proxy_authorisation(cluster: Any) -> None:
    st, ws, cid, job = cluster
    viewer = st.user("Viewer")  # no compute.read: infrastructure is operator territory
    assert st.http.get(f"/v1/spark-ui/{cid}/proxy-test", headers=viewer).status_code == 403
    operator = st.user("Operator")
    assert st.http.get(f"/v1/spark-ui/{cid}/proxy-test", headers=operator).status_code == 200
    # application UIs follow job.read on the application's workspace
    apps = st.http.get("/v1/spark-applications", headers=st.admin).json()["items"]
    app = next(a for a in apps if a["job_id"] == job["id"])
    stranger = st.user("Contributor", st.workspace())
    r = st.http.get(f"/v1/spark-applications/{app['id']}/ui/proxy-test", headers=stranger)
    assert r.status_code in (403, 404)
    assert st.http.get(f"/v1/spark-applications/{app['id']}/ui/proxy-test").status_code == 401
    assert st.http.get("/v1/spark-applications/app_missing/ui/x", headers=st.admin).status_code == 404
