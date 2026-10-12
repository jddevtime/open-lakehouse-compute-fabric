# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Small helpers shared by the orchestration services: platform settings, the ``kv`` cursor store, URL masking and a leader-loop thread."""

from __future__ import annotations

import logging
import threading
from collections.abc import Callable
from typing import Any
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

from sqlalchemy import select, update
from sqlalchemy.exc import IntegrityError

from olcf.core.timeutil import now_iso
from olcf.db import schema as S
from olcf.db.util import fetch_one


def setting(ctx: Any, key: str, default: Any) -> Any:
    """Value of a registered platform setting, or ``default`` when the key is unknown / unset / the settings service is absent."""
    try:
        st = ctx.maybe_service("settings")
        if st is not None and key in st.registry:
            v = st.get(key)
            return default if v is None else v
    except Exception:  # noqa: BLE001
        pass
    return default


def kv_get(ctx: Any, key: str, default: Any = None) -> Any:
    with ctx.db.connect() as conn:
        row = fetch_one(conn, select(S.kv).where(S.kv.c.key == key))
    return row["value"] if row else default


def kv_set(ctx: Any, key: str, value: Any) -> None:
    with ctx.db.begin() as conn:
        n = conn.execute(update(S.kv).where(S.kv.c.key == key).values(value=value, updated_at=now_iso())).rowcount
        if not n:
            conn.execute(S.kv.insert().values(key=key, value=value, updated_at=now_iso()))


def kv_set_if_absent(ctx: Any, key: str, value: Any) -> bool:
    """Atomically create ``key`` (True) unless another replica did first (False)."""
    try:
        with ctx.db.begin() as conn:
            conn.execute(S.kv.insert().values(key=key, value=value, updated_at=now_iso()))
        return True
    except IntegrityError:
        return False


_SENSITIVE_QUERY = ("token", "key", "secret", "sig", "signature", "password", "pwd", "auth", "code", "apikey", "api_key")


def mask_url(url: str | None) -> str | None:
    """Hide query-string values that look like credentials (webhook URLs frequently carry tokens)."""
    if not url:
        return url
    try:
        parts = urlsplit(url)
        if not parts.query:
            return url
        q = [(k, "***" if any(s in k.lower() for s in _SENSITIVE_QUERY) else v) for k, v in parse_qsl(parts.query, keep_blank_values=True)]
        return urlunsplit((parts.scheme, parts.netloc, parts.path, urlencode(q, safe="*"), parts.fragment))
    except ValueError:
        return "<invalid url>"


class LeaderLoop:
    """A daemon thread that calls ``fn`` every ``interval`` seconds while this replica is the leader. ``tick_now`` runs one pass synchronously (tests)."""

    def __init__(self, ctx: Any, name: str, fn: Callable[[], None], interval: float, log: logging.Logger) -> None:
        self.ctx, self.name, self.fn, self.interval, self.log = ctx, name, fn, interval, log
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None

    def start(self) -> None:
        if self._thread and self._thread.is_alive():
            return
        self._stop.clear()
        self._thread = threading.Thread(target=self._run, name=f"olcf-{self.name}", daemon=True)
        self._thread.start()

    def stop(self, timeout: float = 10.0) -> None:
        self._stop.set()
        if self._thread:
            self._thread.join(timeout=timeout)
            self._thread = None

    def tick_now(self) -> None:
        self.fn()

    def _run(self) -> None:
        while not self._stop.is_set():
            try:
                if self.ctx.leader.is_leader():
                    self.fn()
            except Exception:  # noqa: BLE001
                self.log.exception("%s loop failed", self.name)
            self._stop.wait(self.interval)
