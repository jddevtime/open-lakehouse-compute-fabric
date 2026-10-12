# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Alerts and webhooks: real built-in rules and alerts from the fleet, evaluation on demand, a notification channel, and a signed webhook delivered to a real receiver."""

from __future__ import annotations

import hashlib
import hmac
import re
import threading
import uuid
from http.server import BaseHTTPRequestHandler, HTTPServer

import pytest
from playwright.sync_api import expect


@pytest.fixture()
def receiver(api):
    """A loopback HTTP receiver; private destinations must be allow-listed by an administrator, which is what the platform setting is for."""
    got: list[tuple[dict[str, str], bytes]] = []

    class H(BaseHTTPRequestHandler):
        def do_POST(self) -> None:  # noqa: N802
            body = self.rfile.read(int(self.headers.get("content-length", 0)))
            got.append(({k.lower(): v for k, v in self.headers.items()}, body))
            self.send_response(200)
            self.end_headers()
            self.wfile.write(b"ok")

        def log_message(self, *a: object) -> None:
            pass

    srv = HTTPServer(("127.0.0.1", 0), H)
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    old = api("GET", "/v1/admin/settings")["settings"]["network.ssrf.allow"]["value"]
    api("PUT", "/v1/admin/settings/network.ssrf.allow", json={"value": [*old, "127.0.0.1"]})
    yield srv, got
    api("PUT", "/v1/admin/settings/network.ssrf.allow", json={"value": old})
    srv.shutdown()


def test_alerts_rules_and_evaluate(page, goto, api, snap):
    goto(page, "/alerts")
    alerts = page.get_by_test_id("alerts")
    expect(alerts).to_be_visible()
    page.get_by_role("button", name="Evaluate rules now").click()
    # whatever real rule fires on this machine (a nearly full disk, a token or certificate about to expire, ...) must be listed in the console
    firing = api("GET", "/v1/alerts", params={"state": "firing"})["items"]
    if firing:
        expect(alerts).to_contain_text(firing[0]["summary"][:40], timeout=30_000)
    snap(page, "alerts-light")
    page.get_by_role("tab", name="Rules").click()
    rules = page.get_by_test_id("rules")
    expect(rules).to_contain_text("Failed jobs")
    assert len(api("GET", "/v1/alerts/rules")["items"]) >= 5


def test_webhook_is_delivered_and_signed(page, goto, api, receiver):
    srv, got = receiver
    name = "e2e-hook-" + uuid.uuid4().hex[:5]
    goto(page, "/alerts?tab=webhooks")
    page.get_by_role("button", name="New webhook").first.click()
    dlg = page.get_by_role("dialog")
    dlg.get_by_label(re.compile(r"^Name")).fill(name)
    dlg.get_by_label(re.compile(r"^URL")).fill(f"http://127.0.0.1:{srv.server_port}/hook")
    dlg.get_by_label("Other patterns").fill("job.*")
    dlg.get_by_role("button", name="Create webhook").click()
    secret_dlg = page.get_by_role("dialog")
    expect(secret_dlg).to_contain_text("Copy the secret now")
    secret = secret_dlg.locator("pre, code").first.inner_text().strip()
    secret_dlg.get_by_role("button", name="I have saved it").click()
    expect(page.get_by_test_id("webhooks")).to_contain_text(name)
    # send a test event from the row menu
    page.get_by_role("button", name=f"Actions for {name}").click()
    page.get_by_role("menuitem", name="Send a test event").click()
    expect(page.get_by_text(re.compile(r"Test delivered \(200\)"))).to_be_visible(timeout=30_000)
    assert got, "the receiver got no request"
    headers, body = got[0]
    ts = headers.get("x-olcf-timestamp", "")
    sig = headers.get("x-olcf-signature", "")
    want = hmac.new(secret.encode(), f"{ts}.".encode() + body, hashlib.sha256).hexdigest()
    assert sig.split("=")[-1] == want, (sig, want)
    # the delivery is in the history with the response code
    page.get_by_role("button", name=f"Actions for {name}").click()
    page.get_by_role("menuitem", name="Deliveries").click()
    drawer = page.get_by_role("dialog")
    expect(drawer).to_contain_text("Succeeded")
    expect(drawer).to_contain_text("200")
    page.keyboard.press("Escape")


def test_log_channel(page, goto, api):
    name = "e2e-chan-" + uuid.uuid4().hex[:5]
    goto(page, "/alerts?tab=channels")
    page.get_by_role("button", name=re.compile("New channel")).first.click()
    dlg = page.get_by_role("dialog")
    dlg.get_by_label("Name").fill(name)
    dlg.get_by_label("Type").select_option(label="Server log only")
    dlg.get_by_role("button", name=re.compile("Create")).click()
    expect(page.get_by_test_id("channels")).to_contain_text(name)
