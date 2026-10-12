# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""'Keep me signed in on this device' in a real browser (review finding F14): the choice survives a reload, a token refresh and a browser restart.

The client used to forget the choice on reload (the flag lived only in memory), so the first token refresh afterwards deleted the persistent copy of the refresh token and
the user was signed out when the browser closed.  The Node unit test ``tests/web/remember_auth.test.mjs`` covers the storage logic; this drives the real page.
The module brings its own throw-away stack (no Spark runtime needed), like ``test_14_first_run``.
"""

from __future__ import annotations

import json
from collections.abc import Iterator
from typing import Any

import pytest

from tests.e2e.firstrun import FirstRunStack


@pytest.fixture(scope="module")
def stack(tmp_path_factory: pytest.TempPathFactory) -> Iterator[FirstRunStack]:  # overrides the session-wide stack fixture for this module
    st = FirstRunStack(tmp_path_factory.mktemp("e2e-remember")).start()
    try:
        yield st
    finally:
        st.stop()


def _sign_in(pg: Any, stack: FirstRunStack, *, remember: bool) -> None:
    pg.goto("/")
    pg.wait_for_selector("form")
    pg.get_by_label("Username").fill(stack.username)
    pg.locator("input[type=password]").fill(stack.password)
    if remember:
        pg.get_by_label("Keep me signed in on this device").check()
    pg.get_by_role("button", name="Sign in").click()
    pg.wait_for_selector(".page-head h1")


def _stored(pg: Any, where: str) -> dict[str, Any] | None:
    raw = pg.evaluate(f"{where}.getItem('olcf.auth')")
    return json.loads(raw) if raw else None


def _force_refresh(pg: Any) -> None:
    """Make the access token unusable (as after a long idle tab) and reload: the client has to refresh with the stored refresh token."""
    pg.evaluate("() => { const b = JSON.parse(sessionStorage.getItem('olcf.auth')); b.access = 'expired-access-token'; sessionStorage.setItem('olcf.auth', JSON.stringify(b)); }")
    pg.reload()
    pg.wait_for_selector(".page-head h1")


def test_remembered_session_survives_reload_refresh_and_browser_restart(browser: Any, stack: FirstRunStack) -> None:
    ctx = browser.new_context(base_url=stack.url)
    ctx.set_default_timeout(20_000)
    try:
        pg = ctx.new_page()
        _sign_in(pg, stack, remember=True)
        first = _stored(pg, "localStorage")
        assert first and first["refresh"], "ticking the box stores the refresh token in localStorage"
        assert _stored(pg, "sessionStorage")["remember"] is True

        _force_refresh(pg)  # reload + 401 + refresh: the refresh token rotates
        second = _stored(pg, "localStorage")
        assert second and second["refresh"] and second["refresh"] != first["refresh"], "the persistent copy follows the rotated refresh token instead of being deleted"
        assert _stored(pg, "sessionStorage")["remember"] is True

        # a browser restart keeps localStorage and cookies but drops sessionStorage
        state = ctx.storage_state()
    finally:
        ctx.close()
    assert any(o["origin"] == stack.url and any(i["name"] == "olcf.auth" for i in o["localStorage"]) for o in state["origins"])
    ctx2 = browser.new_context(base_url=stack.url, storage_state=state)
    ctx2.set_default_timeout(20_000)
    try:
        pg2 = ctx2.new_page()
        pg2.goto("/")
        pg2.wait_for_selector(".page-head h1")  # signed in again with no form
        assert _stored(pg2, "localStorage")["refresh"] != second["refresh"], "and that sign-in rotated the token once more"
    finally:
        ctx2.close()


def test_not_remembered_session_never_reaches_local_storage(browser: Any, stack: FirstRunStack) -> None:
    ctx = browser.new_context(base_url=stack.url)
    ctx.set_default_timeout(20_000)
    try:
        pg = ctx.new_page()
        _sign_in(pg, stack, remember=False)
        assert _stored(pg, "localStorage") is None
        assert _stored(pg, "sessionStorage")["remember"] is False
        _force_refresh(pg)
        assert _stored(pg, "localStorage") is None
        state = ctx.storage_state()
    finally:
        ctx.close()
    ctx2 = browser.new_context(base_url=stack.url, storage_state=state)
    try:
        pg2 = ctx2.new_page()
        pg2.goto("/")
        pg2.wait_for_selector("form")  # closing the browser signed this user out
    finally:
        ctx2.close()
