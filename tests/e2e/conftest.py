# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Browser end-to-end fixtures: a REAL OLCF stack (uvicorn, all modules, 3 production worker agents, the cached Spark runtime) driven by headless Chromium.

Run the suite:   ``PYTHONPATH=src pytest tests/e2e -m "not spark"``  (add ``-m spark`` for the JVM tests, serialised through ``/tmp/olcf-spark.lock``).
Fast iteration:  ``OLCF_E2E_URL=http://127.0.0.1:8470 OLCF_E2E_DATA=/tmp/olcf-ui-dev pytest tests/e2e`` reuses a stack started with ``python -m tests.e2e.stack``.

Every test page records console errors/warnings, uncaught exceptions, failed requests and HTTP >= 400 responses; ``page.problems`` is asserted empty at the end of each test
(a test that provokes an expected failure calls ``page.expect_problems(...)``).
"""

from __future__ import annotations

import contextlib
import json
import os
import re
import sys
import time
from collections.abc import Callable, Iterator
from pathlib import Path
from typing import Any

import httpx
import pytest

HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[1]
for p in (str(ROOT), str(ROOT / "src"), str(ROOT / "sdk" / "fabric")):
    if p not in sys.path:
        sys.path.insert(0, p)

from tests.e2e.stack import E2EStack  # noqa: E402

#: Explicit browser override. Unset (the default) means Playwright's own resolution: the Chromium it installed (``playwright install chromium``) in ``PLAYWRIGHT_BROWSERS_PATH`` or its platform cache.
CHROMIUM = os.environ.get("OLCF_E2E_CHROMIUM") or None
#: ``OLCF_E2E_REQUIRE_BROWSER=1`` (set in CI): a missing browser is a test FAILURE, not a skip, so a green run always means the UI was exercised.
REQUIRE_BROWSER = os.environ.get("OLCF_E2E_REQUIRE_BROWSER", "").strip().lower() in ("1", "true", "yes")
#: Failure artifacts (screenshot + Playwright trace of every page of a failed test) go here; CI uploads ``**/test-results``.
ARTIFACTS = Path(os.environ["OLCF_E2E_ARTIFACTS"]) if os.environ.get("OLCF_E2E_ARTIFACTS") else ROOT / "test-results"
TRACE = os.environ.get("OLCF_E2E_TRACE", "").strip().lower() in ("1", "true", "yes")
SHOTS = Path(os.environ["OLCF_E2E_SHOTS"]) if os.environ.get("OLCF_E2E_SHOTS") else ROOT / "docs" / "images" / "ui"  # OLCF_E2E_SHOTS: write elsewhere (the docs images are curated)


def pytest_configure(config: pytest.Config) -> None:
    config.addinivalue_line("markers", "spark: requires a local Spark runtime (serialised, slow)")
    config.addinivalue_line("markers", "e2e: browser based end-to-end tests")


def pytest_collection_modifyitems(items: list[pytest.Item]) -> None:
    for it in items:
        if str(HERE) in str(it.fspath):
            it.add_marker(pytest.mark.e2e)


@pytest.hookimpl(hookwrapper=True)
def pytest_runtest_makereport(item: pytest.Item, call: pytest.CallInfo[None]) -> Iterator[None]:
    """Remember each phase's report on the item so the browser fixtures can tell whether the test failed."""
    outcome = yield
    rep = outcome.get_result()
    setattr(item, "rep_" + rep.when, rep)


# ----------------------------------------------------------------------------------------------------------------------------------------------------- stack
@pytest.fixture(scope="session")
def stack(tmp_path_factory: pytest.TempPathFactory) -> Iterator[E2EStack]:
    """The control plane with three real agents, or an already running dev stack (``OLCF_E2E_URL``)."""
    ext = os.environ.get("OLCF_E2E_URL")
    if ext:
        data = Path(os.environ.get("OLCF_E2E_DATA", "/tmp/olcf-ui-dev"))  # noqa: S108
        st = E2EStack(data, port=int(ext.rsplit(":", 1)[1]))
        st.http = httpx.Client(base_url=st.url, timeout=60)
        st.password = (data / "data" / "secrets" / "bootstrap-admin.txt").read_text().split("password=")[1].strip()
        r = st.http.post("/v1/identity/login", json={"username": st.username, "password": st.password})
        assert r.status_code == 200, r.text
        st.admin = {"Authorization": f"Bearer {r.json()['access_token']}"}
        st.wait_nodes_online(3, 60)
        yield st
        st.http.close()
        return
    st = E2EStack(tmp_path_factory.mktemp("e2e-stack"))
    try:  # startup is inside the cleanup too: a control plane that half-starts must not leak processes
        st.start()
        st.register_runtime()
        for i in range(3):
            st.add_agent(f"node-{i + 1}", labels=f"site=lab,rack=r{i % 3 + 1}")
        st.wait_nodes_online(3, 180)
        yield st
    finally:
        st.stop()


@pytest.fixture(scope="session")
def has_runtime(stack: E2EStack) -> bool:
    return stack.has_runtime_cache()


@pytest.fixture(scope="session")
def api(stack: E2EStack) -> Callable[..., Any]:
    return stack.api


@pytest.fixture(scope="module")
def spark_exclusive() -> Iterator[None]:
    """Serialise anything that starts a JVM across the whole sandbox."""
    from tests.spark_locking import spark_lock_held

    with spark_lock_held():
        yield


# ----------------------------------------------------------------------------------------------------------------------------------------------------- browser
@pytest.fixture(scope="session")
def browser() -> Iterator[Any]:
    try:
        from playwright.sync_api import sync_playwright
    except ImportError as exc:
        _browser_unavailable(f"the playwright package is not installed ({exc})")
    with sync_playwright() as pw:
        kw: dict[str, Any] = {"args": ["--no-sandbox", "--disable-dev-shm-usage"]}
        if CHROMIUM:
            if not Path(CHROMIUM).exists():
                _browser_unavailable(f"OLCF_E2E_CHROMIUM points to {CHROMIUM}, which does not exist")
            kw["executable_path"] = CHROMIUM
        try:
            b = pw.chromium.launch(**kw)
        except Exception as exc:  # noqa: BLE001 - Playwright raises its own Error type for a missing or broken browser
            _browser_unavailable("Chromium could not be launched with Playwright's default resolution (run `playwright install chromium`, or set OLCF_E2E_CHROMIUM):\n" + str(exc)[:800])
        yield b
        b.close()


def _browser_unavailable(reason: str) -> Any:
    """No usable browser: fail when ``OLCF_E2E_REQUIRE_BROWSER`` demands one (CI), otherwise skip (a laptop without Playwright browsers)."""
    if REQUIRE_BROWSER:
        pytest.fail(f"OLCF_E2E_REQUIRE_BROWSER=1 and no browser is available: {reason}", pytrace=False)
    pytest.skip(reason)


class Problems(list):
    """Collected console/network problems of one page."""

    def __init__(self) -> None:
        super().__init__()
        self.allowed: list[re.Pattern[str]] = []

    def add(self, text: str) -> None:
        if not any(a.search(text) for a in self.allowed):
            self.append(text)


def _install_listeners(page: Any) -> None:
    probs = Problems()
    page.problems = probs

    def expect_problems(*patterns: str) -> None:
        probs.allowed.extend(re.compile(p) for p in patterns)
        probs[:] = [x for x in probs if not any(a.search(x) for a in probs.allowed)]

    page.expect_problems = expect_problems
    page.on("console", lambda m: probs.add(f"[console.{m.type}] {m.text}") if m.type in ("error", "warning") else None)
    page.on("pageerror", lambda e: probs.add(f"[pageerror] {e}"))
    # ERR_ABORTED is what Chromium reports for a request cancelled by a navigation, a closed stream or an AbortController: not a failure of the product
    page.on("requestfailed", lambda r: probs.add(f"[requestfailed] {r.method} {r.url} {r.failure}") if "ERR_ABORTED" not in str(r.failure) else None)
    page.on("response", lambda r: probs.add(f"[http {r.status}] {r.request.method} {r.url}") if r.status >= 400 else None)


_TOKEN_CACHE: dict[str, Any] = {}


def _auth_script(stack: E2EStack, fresh: bool = False) -> str:
    """Seed ``sessionStorage`` with a signed-in session. The login endpoint is rate limited per address, so one login is shared for ten minutes; ``fresh`` takes a private one
    (for tests that sign out, which revokes the session)."""
    cached = _TOKEN_CACHE.get(stack.url)
    if fresh or not cached or time.time() - cached[0] > 600:
        r = stack.http.post("/v1/identity/login", json={"username": stack.username, "password": stack.password})
        assert r.status_code == 200, f"login failed: {r.status_code} {r.text}"
        t = r.json()
        cached = (time.time(), {"access": t["access_token"], "refresh": t["refresh_token"]})
        if not fresh:
            _TOKEN_CACHE[stack.url] = cached
    blob = json.dumps({**cached[1], "exp": int(time.time() * 1000) + 1_200_000})
    return "try{sessionStorage.setItem('olcf.auth'," + json.dumps(blob) + ")}catch(e){}"


def _safe_name(nodeid: str) -> str:
    return re.sub(r"[^A-Za-z0-9._-]+", "_", nodeid)[-150:]


def _collect_failure_artifacts(request: pytest.FixtureRequest, contexts: list[Any]) -> None:
    """Screenshot every open page and keep the Playwright trace of a failed test (``test-results/<test>/``); the trace of a passing test is dropped."""
    call = getattr(request.node, "rep_call", None)
    failed = (call is not None and call.failed) or any(getattr(pg, "problems", None) for c in contexts for pg in c.pages)
    out = ARTIFACTS / _safe_name(request.node.nodeid)
    for i, c in enumerate(contexts):
        if failed:
            out.mkdir(parents=True, exist_ok=True)
            for j, pg in enumerate(c.pages):
                with contextlib.suppress(Exception):
                    pg.screenshot(path=str(out / f"page-{i}-{j}.png"), full_page=True)
        if TRACE:
            with contextlib.suppress(Exception):
                c.tracing.stop(path=str(out / f"trace-{i}.zip") if failed else None)


@pytest.fixture()
def new_page(request: pytest.FixtureRequest, browser: Any, stack: E2EStack) -> Iterator[Callable[..., Any]]:
    """Factory of authenticated pages (``anon=True`` for the login screen, ``dark=True`` for the dark theme)."""
    made: list[Any] = []

    def make(*, anon: bool = False, dark: bool = False, width: int = 1440, height: int = 900, reduced_motion: bool = False, fresh: bool = False) -> Any:
        ctx = browser.new_context(viewport={"width": width, "height": height}, color_scheme="dark" if dark else "light", base_url=stack.url,
                                  reduced_motion="reduce" if reduced_motion else "no-preference", accept_downloads=True)
        ctx.set_default_timeout(20_000)
        if TRACE:
            ctx.tracing.start(screenshots=True, snapshots=True)
        if not anon:
            ctx.add_init_script(_auth_script(stack, fresh))
        pg = ctx.new_page()
        _install_listeners(pg)
        made.append(ctx)
        return pg

    yield make
    _collect_failure_artifacts(request, made)
    for c in made:
        c.close()


@pytest.fixture()
def page(new_page: Callable[..., Any]) -> Iterator[Any]:
    """An authenticated page; the test fails when the page produced console errors, warnings, failed requests or HTTP >= 400."""
    pg = new_page()
    yield pg
    assert not pg.problems, "unexpected browser problems:\n" + "\n".join(pg.problems)


def open_route(page: Any, route: str, ready: str | None = None) -> None:
    """Navigate with hash routing and wait for the page head (or ``ready`` selector)."""
    page.goto("/#" + route if not route.startswith("#") else "/" + route)
    page.wait_for_selector(ready or ".page-head h1", state="visible")


@pytest.fixture()
def goto() -> Callable[..., None]:
    return open_route


def _shot(page: Any, name: str, *, full: bool = False) -> Path:
    """Screenshot into ``docs/images/ui`` (palette-quantised PNG)."""
    SHOTS.mkdir(parents=True, exist_ok=True)
    out = SHOTS / f"{name}.png"
    page.wait_for_timeout(350)
    page.screenshot(path=str(out), full_page=full)
    try:
        from PIL import Image

        im = Image.open(out).convert("RGB").quantize(colors=160, method=Image.Quantize.MEDIANCUT, dither=Image.Dither.NONE)
        im.save(out, optimize=True)
    except Exception:  # noqa: BLE001, S110 - Pillow is optional; an unquantised PNG is still correct
        pass
    return out


@pytest.fixture()
def snap() -> Callable[..., Path]:
    return _shot
