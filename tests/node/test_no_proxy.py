# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Cluster-internal HTTP (Spark daemon health, master probes, the standalone provider's master polling) never goes through ``HTTP(S)_PROXY``.

A site proxy cannot reach the nodes' own addresses; honouring it made a healthy cluster look dead ("started but no worker registered") on machines whose
``NO_PROXY`` does not list the node address."""

from __future__ import annotations

import http.server
import json
import threading
from pathlib import Path

import pytest

from olcf.worker.daemons import http_json
from olcf.worker.probes import ProbeSuite


@pytest.fixture()
def master_ui():
    body = json.dumps({"status": "ALIVE", "workers": [{"state": "ALIVE"}], "url": "spark://x:1"}).encode()

    class H(http.server.BaseHTTPRequestHandler):
        def do_GET(self):  # noqa: N802
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, *a):  # noqa: ANN002
            pass

    srv = http.server.ThreadingHTTPServer(("127.0.0.1", 0), H)
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    yield f"http://127.0.0.1:{srv.server_address[1]}"
    srv.shutdown()


@pytest.fixture()
def dead_proxy(monkeypatch):
    for k in ("http_proxy", "HTTP_PROXY", "https_proxy", "HTTPS_PROXY", "all_proxy", "ALL_PROXY"):
        monkeypatch.setenv(k, "http://127.0.0.1:9")          # nothing listens there: any request that honours the proxy fails
    for k in ("no_proxy", "NO_PROXY"):
        monkeypatch.setenv(k, "")


def test_daemon_health_poll_ignores_proxy_environment(master_ui, dead_proxy):
    assert http_json(master_ui + "/json/")["status"] == "ALIVE"


def test_spark_master_probe_ignores_proxy_environment(master_ui, dead_proxy, tmp_path: Path):
    res = ProbeSuite(state_dir=tmp_path).run([{"kind": "spark_master", "name": "m", "url": master_ui + "/json/"}])
    chk = res["checks"][0]
    assert chk["ok"] is True and "ALIVE" in chk["detail"], chk


def test_standalone_provider_master_client_ignores_proxy_environment(master_ui, dead_proxy):
    from olcf.compute.providers.standalone import StandaloneProvider

    prov = StandaloneProvider("standalone", {}, None)  # type: ignore[arg-type] - the provider environment is not used by the HTTP client
    r = prov._http.get(master_ui + "/json/")           # noqa: SLF001
    assert r.status_code == 200 and r.json()["status"] == "ALIVE"
