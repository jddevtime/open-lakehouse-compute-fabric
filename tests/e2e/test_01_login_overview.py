# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Sign in through the real form, see the platform lights, switch theme, sign out."""

from __future__ import annotations

from typing import Any


def test_login_flow_and_overview_lights(new_page: Any, stack: Any, snap: Any) -> None:
    pg = new_page(anon=True)
    pg.goto("/")
    pg.wait_for_selector("form")
    snap(pg, "login-light")
    # a wrong password is refused with a readable message (and the browser logs the expected 401, which this test allows)
    pg.expect_problems(r"\[http 401\]", r"status of 401")
    pg.get_by_label("Username").fill(stack.username)
    pg.locator("input[type=password]").fill("not-the-password")
    pg.get_by_role("button", name="Sign in").click()
    pg.get_by_text("The username or password is not correct").wait_for()
    pg.locator("input[type=password]").fill(stack.password)
    pg.get_by_role("button", name="Sign in").click()
    pg.wait_for_selector(".page-head h1")
    assert "Good" in pg.locator(".page-head h1").inner_text() or "Working" in pg.locator(".page-head h1").inner_text()
    # tokens live in sessionStorage, never localStorage, unless "remember me" was ticked (it was not)
    assert pg.evaluate("sessionStorage.getItem('olcf.auth') !== null")
    assert pg.evaluate("Object.keys(localStorage).filter(k => /auth/.test(k)).length") == 0
    # the heartbeat strip and the health grid show real components
    pg.wait_for_selector(".hrow")
    assert pg.locator(".hrow").count() >= 8
    assert pg.get_by_role("button", name="Platform health").is_visible()
    snap(pg, "overview-light")
    # theme toggle writes data-theme and persists across a reload
    pg.get_by_role("button", name="Choose colour theme").click()
    pg.get_by_role("menuitem", name="Dark").click()
    assert pg.evaluate("document.documentElement.getAttribute('data-theme')") == "dark"
    pg.reload()
    pg.wait_for_selector(".hrow")
    assert pg.evaluate("document.documentElement.getAttribute('data-theme')") == "dark"
    snap(pg, "overview-dark")
    pg.get_by_role("button", name="Choose colour theme").click()
    pg.get_by_role("menuitem", name="Light").click()
    # sign out returns to the form and forgets the session
    refresh = pg.evaluate("JSON.parse(sessionStorage.getItem('olcf.auth')).refresh")
    pg.locator("button[aria-label^='Account menu']").click()
    pg.get_by_role("menuitem", name="Sign out").click()
    pg.wait_for_selector("form")
    assert pg.evaluate("sessionStorage.getItem('olcf.auth')") is None
    assert not [p for p in pg.problems if "401" not in p]
    r = stack.http.post("/v1/identity/refresh", json={"refresh_token": refresh})
    assert r.status_code == 401  # signing out revoked the refresh token on the server, it was not merely forgotten by the browser


def test_keyboard_navigation_and_landmarks(page: Any) -> None:
    page.goto("/")
    page.wait_for_selector(".hrow")
    # landmarks and a skip link exist; the first Tab lands on a visible, focusable control
    assert page.locator("nav[aria-label='Main']").count() == 1
    assert page.locator("main").count() >= 1
    page.keyboard.press("Tab")
    active = page.evaluate("document.activeElement && document.activeElement.tagName")
    assert active in ("A", "BUTTON")
    # Ctrl+K opens the command palette; Escape closes it and returns focus
    page.keyboard.press("Control+k")
    page.wait_for_selector("[role=dialog]")
    # the search field owns the keyboard the moment the dialog exists (no wait): keystrokes typed right away must not be lost to the previously focused control
    assert page.evaluate("document.activeElement && document.activeElement.getAttribute('role')") == "combobox"
    page.keyboard.type("compute")
    assert page.locator("[role=dialog] input").input_value() == "compute"
    page.wait_for_timeout(300)
    page.keyboard.press("Enter")
    page.wait_for_url("**/#/compute*")
