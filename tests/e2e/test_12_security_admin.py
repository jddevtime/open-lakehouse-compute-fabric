# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Security and Administration: accounts, roles, tokens, identity providers, the audit trail and its chain check, settings, backup, diagnostics, retention, About."""

from __future__ import annotations

import json
import re
import uuid

import httpx
from playwright.sync_api import expect


def _uniq(prefix: str) -> str:
    return f"{prefix}-{uuid.uuid4().hex[:5]}"


def test_new_account_signs_in_with_least_privilege(page, new_page, goto, api, stack):
    name = _uniq("e2e-user")
    goto(page, "/security?tab=users")
    page.get_by_test_id("new-user").click()
    d = page.get_by_role("dialog")
    d.get_by_test_id("user-name").fill(name)
    d.get_by_test_id("user-password").fill("Initial-pass-123")
    expect(d.get_by_test_id("user-must-change")).to_be_checked()
    d.get_by_label("First role").select_option(label="Viewer")
    d.get_by_test_id("user-save").click()
    expect(page.get_by_test_id("users")).to_contain_text(name)
    u = next(x for x in api("GET", "/v1/identity/users", params={"q": name})["items"] if x["name"] == name)
    assert u["kind"] == "user" and u["is_active"] is True and u["must_change_password"] is True
    # the person signs in on a fresh browser profile: a Viewer sees no administration
    anon = new_page(anon=True)
    anon.goto("/")
    anon.get_by_label("Username").fill(name)
    anon.locator("input[type=password]").fill("Initial-pass-123")
    anon.get_by_role("button", name=re.compile("Sign in")).click()
    # accounts created in the UI require a new password at first sign-in (the box is ticked by default): the session can do nothing else until it was changed
    expect(anon.get_by_role("heading", name="Choose a new password")).to_be_visible()
    anon.get_by_label(re.compile(r"^New password")).fill("Second-pass-456-xyz")
    anon.get_by_label(re.compile(r"^Repeat new password")).fill("Second-pass-456-xyz")
    anon.get_by_role("button", name=re.compile("Save and continue")).click()
    expect(anon.get_by_role("navigation").get_by_text("Lakehouse").first).to_be_visible()
    expect(anon.get_by_role("navigation").get_by_text("Administration")).to_have_count(0)
    # the administrator grants another role from the access drawer
    page.get_by_role("button", name=re.compile(f"Actions for {re.escape(name)}")).click()
    page.get_by_role("menuitem", name=re.compile("Access|roles", re.I)).first.click()
    drawer = page.get_by_role("dialog")
    expect(drawer).to_contain_text(name)
    drawer.get_by_label("Role").select_option(label="Contributor")
    drawer.get_by_test_id("grant-role").click()
    expect(drawer).to_contain_text("Contributor")
    roles = [b["role"] for b in api("GET", "/v1/identity/bindings", params={"principal_id": u["id"]})["items"]]
    assert sorted(roles) == ["Contributor", "Viewer"]
    page.keyboard.press("Escape")
    # disabling the account locks the person out
    page.get_by_role("button", name=re.compile(f"Actions for {re.escape(name)}")).click()
    page.get_by_role("menuitem", name=re.compile("Disable")).click()
    if page.get_by_role("dialog").count():
        page.get_by_role("dialog").get_by_role("button", name=re.compile("Disable")).click()
    for _ in range(40):
        if not next(x for x in api("GET", "/v1/identity/users", params={"q": name})["items"] if x["name"] == name)["is_active"]:
            break
        page.wait_for_timeout(250)
    r = httpx.post(f"{stack.url}/v1/identity/login", json={"username": name, "password": "Second-pass-456-xyz"})  # gitleaks:allow
    assert r.status_code in (401, 403)
    anon.context.close()


def test_token_is_shown_once_and_works(page, goto, api, stack):
    name = _uniq("e2e-token")
    goto(page, "/security?tab=tokens")
    page.get_by_test_id("new-token").click()
    d = page.get_by_role("dialog")
    d.get_by_test_id("token-name").fill(name)
    d.get_by_test_id("token-save").click()
    shown = page.get_by_role("dialog")
    token = shown.locator("pre, code").first.inner_text().strip()
    assert token.startswith("olcf_pat_")
    shown.get_by_role("button", name="I have saved it").click()
    expect(page.get_by_role("dialog")).to_have_count(0)
    expect(page.get_by_test_id("tokens")).to_contain_text(name)
    assert token not in page.content()
    me = httpx.get(f"{stack.url}/v1/identity/me", headers={"Authorization": f"Bearer {token}"})
    assert me.status_code == 200 and me.json()["principal"]["name"] == "admin"
    rec = next(t for t in api("GET", "/v1/identity/tokens")["items"] if t["name"] == name)
    assert token not in json.dumps(rec) and rec["prefix"] and token.startswith(rec["prefix"])


def test_identity_provider_role_mapping_preview(page, goto, api):
    name = _uniq("e2e-idp")
    goto(page, "/security?tab=providers")
    page.get_by_test_id("new-idp").click()
    d = page.get_by_role("dialog")
    d.get_by_test_id("idp-name").fill(name)
    d.get_by_test_id("idp-issuer").fill("https://idp.example.invalid/realms/main")
    d.get_by_label(re.compile(r"^Client ID")).fill("olcf-e2e")
    d.get_by_test_id("add-mapping").click()
    d.get_by_label("Claim").fill("groups")
    d.get_by_label("Value", exact=True).fill("data-eng")
    d.get_by_label("Role", exact=True).select_option(label="Contributor")
    d.get_by_test_id("idp-save").click()
    prov = page.get_by_test_id("providers")
    expect(prov).to_contain_text(name)
    stored = next(p for p in api("GET", "/v1/identity/providers")["items"] if p["name"] == name)
    assert stored["config"]["role_mappings"][0]["role"] == "Contributor"
    assert '"client_secret"' not in json.dumps(stored) and stored["has_client_secret"] is False
    page.get_by_role("button", name=re.compile(f"Actions for {re.escape(name)}")).click()
    page.get_by_role("menuitem", name=re.compile("Preview")).click()
    pd = page.get_by_role("dialog")
    pd.get_by_test_id("preview-run").click()
    res = pd.get_by_test_id("preview-result")
    expect(res).to_contain_text("Contributor")
    expect(res).to_contain_text("would be allowed")
    page.keyboard.press("Escape")


def test_audit_filter_verify_and_export(page, goto, api, snap):
    goto(page, "/security?tab=audit")
    audit = page.get_by_test_id("audit")
    expect(audit.locator("table tbody tr").first).to_be_visible()
    page.get_by_test_id("audit-action").fill("auth.login")
    page.wait_for_timeout(700)
    rows = audit.locator("table tbody tr")
    assert rows.count() >= 1
    for t in rows.all_inner_texts()[:5]:
        assert "login" in t
    page.get_by_test_id("audit-verify").click()
    verdict = page.get_by_test_id("audit-verdict")
    expect(verdict).to_contain_text("The audit log is intact", timeout=60_000)
    assert api("GET", "/v1/audit/verify")["ok"] is True
    with page.expect_download() as dl:
        page.get_by_role("button", name=re.compile("Export")).first.click()
        if page.get_by_role("menuitem", name=re.compile("JSON", re.I)).count():
            page.get_by_role("menuitem", name=re.compile("JSON", re.I)).first.click()
    path = dl.value.path()
    first = open(path, encoding="utf-8").readline()
    assert json.loads(first)["action"]
    snap(page, "security-audit-light")


def test_settings_edit_and_reset(page, goto, api):
    goto(page, "/admin?tab=settings")
    page.get_by_test_id("settings-search").fill("alerts.history_days")
    row = page.locator(".set-row", has_text="alerts.history_days")
    expect(row).to_be_visible()
    old = api("GET", "/v1/admin/settings")["settings"]["alerts.history_days"]["value"]
    row.locator("input").first.fill(str(old + 7))
    row.get_by_role("button", name=re.compile("Save")).click()
    for _ in range(40):
        if api("GET", "/v1/admin/settings")["settings"]["alerts.history_days"]["value"] == old + 7:
            break
        page.wait_for_timeout(250)
    assert api("GET", "/v1/admin/settings")["settings"]["alerts.history_days"]["value"] == old + 7
    api("PUT", "/v1/admin/settings/alerts.history_days", json={"value": old})


def test_backup_diagnostics_retention(page, goto, api, snap):
    goto(page, "/admin?tab=backup")
    page.get_by_test_id("backup-now").click()
    page.get_by_test_id("backup-start").click()
    op = page.get_by_role("dialog")
    expect(op).to_contain_text("Finished", timeout=180_000)
    op.get_by_role("button", name="Close", exact=True).click()
    expect(page.get_by_role("dialog")).to_have_count(0)
    items = api("GET", "/v1/admin/backup")["items"]
    assert items, "no backup was created"
    # diagnostics: run a real check set and see the report
    page.get_by_role("tab", name="Diagnostics").click()
    page.get_by_test_id("run-diagnostics").click()
    expect(page.get_by_test_id("diag-report")).to_be_visible(timeout=180_000)
    rep = api("GET", "/v1/admin/diagnostics")
    assert rep["summary"]["ok"] > 0
    # retention preview only reports, it deletes nothing
    page.get_by_role("tab", name="Retention").click()
    page.get_by_test_id("retention-dry").click()
    expect(page.get_by_test_id("retention")).to_contain_text(re.compile("candidate|Would|dry", re.I), timeout=60_000)
    snap(page, "admin-light")


def test_about_page(page, goto, stack):
    goto(page, "/about")
    expect(page.get_by_test_id("about-author")).to_contain_text("Justin Dodrill")
    info = stack.api("GET", "/v1/admin/info")
    expect(page.get_by_test_id("about-version")).to_contain_text(str(info.get("version", "")))
    expect(page.get_by_text("Apache License").first).to_be_visible(timeout=15_000)
    expect(page.get_by_text("Preact").first).to_be_visible()
