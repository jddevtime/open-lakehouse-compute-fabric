# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Embedded worker (single-node mode): the control plane enrols itself and supervises a *real* ``olcf.worker.main`` agent child.

This is the one place the controller is exercised against the production agent instead of the fake one. It needs no Spark and no runtime.
"""

from __future__ import annotations

import importlib.util
import os
import signal
import time
from collections.abc import Callable
from typing import Any

import psutil
import pytest

from tests.compute.fake_agent import wait_for_agent, wait_until

if importlib.util.find_spec("olcf.worker.main") is None:  # pragma: no cover - the NODE package is part of the repository
    pytest.skip("the worker agent package is not importable", allow_module_level=True)


def online(st: Any) -> dict[str, Any]:
    return wait_for_agent(lambda: next((w for w in st.http.get("/v1/workers", headers=st.admin).json()["items"] if w["state"] == "online"), None),
                          st.compute.embedded.log_path, 60, 0.25, "the embedded worker to come online")


def test_embedded_worker_enrols_connects_and_serves_probes(make_stack: Callable[..., Any]) -> None:
    st = make_stack(compute__embedded_worker=True)
    w = online(st)
    emb = st.compute.embedded
    assert emb.enabled and emb.pid and w["labels"].get("olcf.io/embedded") == "true" and w["name"] == emb.name
    assert w["inventory"]["cores"] >= 1 and w["inventory"]["memory_mb"] > 0 and w["inventory"]["protocol"] == "1.0"
    # the enrollment token was handed over through the environment, never argv
    cmdline = " ".join(psutil.Process(emb.pid).cmdline())
    assert "enr_" not in cmdline and "--token" not in cmdline
    # the real agent answers ping / probe commands
    r = st.http.post(f"/v1/workers/{w['id']}/test", headers=st.admin)
    assert r.status_code == 200 and r.json()["ok"], r.text
    names = {s["name"] for s in r.json()["steps"]}
    assert {"agent-link", "disk", "clock"} <= names
    # drain / uncordon go through the real agent as well
    assert st.http.post(f"/v1/workers/{w['id']}/drain", headers=st.admin, json={}).status_code == 200
    wait_until(lambda: st.worker(w["id"])["scheduling"] == "draining", 10, 0.2, "draining")
    assert st.http.post(f"/v1/workers/{w['id']}/uncordon", headers=st.admin).status_code == 200
    wait_until(lambda: st.worker(w["id"])["scheduling"] == "active", 10, 0.2, "active again")
    topo = st.http.get("/v1/compute/topology", headers=st.admin).json()
    assert any(n.get("name") == w["name"] for n in topo["nodes"])


def test_embedded_worker_is_restarted_after_it_dies_and_stopped_with_the_server(make_stack: Callable[..., Any]) -> None:
    st = make_stack(compute__embedded_worker=True)
    online(st)
    emb = st.compute.embedded
    first = emb.pid
    assert first
    os.kill(first, signal.SIGKILL)
    wait_until(lambda: emb.pid and emb.pid != first and emb.restarts >= 1, 30, 0.25, "the supervisor to restart the agent")
    w = wait_until(lambda: next((x for x in st.http.get("/v1/workers", headers=st.admin).json()["items"] if x["state"] == "online"), None), 60, 0.25, "the worker to reconnect")
    assert w["name"] == emb.name
    pid = emb.pid
    st.stop()  # also stops the supervisor and the child
    deadline = time.time() + 20
    while time.time() < deadline and psutil.pid_exists(pid) and psutil.Process(pid).status() != psutil.STATUS_ZOMBIE:
        time.sleep(0.2)
    assert not psutil.pid_exists(pid) or psutil.Process(pid).status() == psutil.STATUS_ZOMBIE, "the embedded agent must not outlive a clean shutdown"


def test_embedded_worker_disabled_by_default(stack: Any) -> None:
    assert stack.compute.embedded.enabled is False and stack.compute.embedded.pid is None
    assert "disabled" in stack.compute.embedded.note
