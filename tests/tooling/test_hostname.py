# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""``olcf.core.hostname.fqdn``: a resolver that never answers must not stall the worker agent's start-up (seen on macOS hosts whose name ends in ``.local``)."""

from __future__ import annotations

import socket
import threading
import time

import pytest

from olcf.core import hostname


@pytest.fixture(autouse=True)
def _fresh(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(hostname, "_cached", None)


def test_a_resolver_that_never_answers_falls_back_to_the_hostname(monkeypatch: pytest.MonkeyPatch) -> None:
    release = threading.Event()
    calls: list[int] = []

    def stuck() -> str:
        calls.append(1)
        release.wait(30)
        return "never.example"

    monkeypatch.setattr(socket, "getfqdn", stuck)
    monkeypatch.setattr(socket, "gethostname", lambda: "box")
    t0 = time.monotonic()
    assert hostname.fqdn(timeout=0.2) == "box"
    assert time.monotonic() - t0 < 5
    assert hostname.fqdn(timeout=0.2) == "box" and len(calls) == 1  # remembered: the slow resolver is asked once
    release.set()


def test_a_normal_answer_is_returned(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(socket, "getfqdn", lambda: "host.example.org")
    assert hostname.fqdn() == "host.example.org"
