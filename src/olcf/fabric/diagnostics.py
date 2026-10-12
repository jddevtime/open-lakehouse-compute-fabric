# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Stepwise connectivity diagnostics for the Fabric integration (``POST /v1/integrations/fabric/test``).

Steps (each ``{name, ok, detail, ms, remediation}``; a step that cannot run because an earlier one failed is reported with ``skipped: true``):

1. **DNS**     - the Entra authority, Fabric API and OneLake hosts resolve (with a proxy configured, name resolution is the proxy's job; the proxy host is resolved instead);
2. **TLS**     - an HTTPS request to each host completes the handshake with the configured trust store (custom CA honoured);
3. **proxy**   - the proxy (explicit or from the environment) accepts connections, or the path is direct;
4. **Entra token** - client-credentials token for the Fabric API audience with the broker identity (the claims ``tid`` / ``appid`` are shown, never the token);
5. **Fabric API**   - ``GET /v1/workspaces`` is authorised, and the mapped Fabric workspaces are visible to the broker;
6. **OneLake**      - the workspace's DFS filesystem can be listed at the OneLake endpoint with a storage-audience token;
7. **inbound path** - the other direction: Entra tenant/audience allow-lists are set and each tenant's JWKS is reachable, the advertised URL is usable from Fabric, and the last
   handshake from a Fabric notebook (the only real proof that Fabric reaches this control plane) is recent.

Every call uses :func:`olcf.fabric.http.make_client`, so the diagnostics exercise exactly the proxy/CA configuration production traffic uses.
"""

from __future__ import annotations

import socket
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any
from urllib.parse import urlsplit

import httpx

from olcf.core.redaction import redact_text
from olcf.fabric.entra import EntraTokenError, TokenBroker
from olcf.fabric.http import Network, make_client
from olcf.fabric.rest import FABRIC_SCOPE, FabricApiError, FabricRestClient

STORAGE_SCOPE = "https://storage.azure.com/.default"
ONELAKE_API_VERSION = "2023-11-03"


@dataclass
class DiagnosticsInput:
    """Inputs to the Fabric connectivity diagnostics: the network settings, the token broker (or why there is none), mapped Fabric workspace ids and allowed tenants.
    """
    network: Network
    broker: TokenBroker | None
    broker_problem: str | None = None  # why there is no broker (not configured / secret unreadable)
    fabric_workspace_ids: list[str] = field(default_factory=list)  # mapped Fabric workspaces
    allowed_tenants: list[str] = field(default_factory=list)
    audiences: list[str] = field(default_factory=list)
    public_url: str = ""
    last_handshake: dict[str, Any] | None = None
    handshake_max_age_seconds: int = 24 * 3600
    jwks_url: Callable[[str], str] | None = None
    client_factory: Callable[[], httpx.Client] | None = None


def _step(name: str, ok: bool, detail: str, started: float, remediation: str | None = None, *, skipped: bool = False, **extra: Any) -> dict[str, Any]:
    s: dict[str, Any] = {"name": name, "ok": ok, "detail": redact_text(detail), "ms": round((time.perf_counter() - started) * 1000, 1), "remediation": remediation}
    if skipped:
        s["skipped"] = True
    s.update(extra)
    return s


def _hosts(net: Network) -> list[tuple[str, str]]:
    return [("Entra ID", net.authority_host), ("Fabric API", net.fabric_api), ("OneLake", net.onelake_dfs)]


def _client(inp: DiagnosticsInput, timeout: float | None = None) -> httpx.Client:
    return inp.client_factory() if inp.client_factory else make_client(inp.network, timeout=timeout)


def run_diagnostics(inp: DiagnosticsInput) -> dict[str, Any]:
    steps: list[dict[str, Any]] = []
    steps.append(_dns(inp))
    steps.append(_tls(inp))
    steps.append(_proxy(inp))
    token_step, fabric_token = _entra_token(inp)
    steps.append(token_step)
    api_step, visible = _fabric_api(inp, fabric_token)
    steps.append(api_step)
    steps.append(_onelake(inp, visible))
    steps.append(_inbound(inp))
    failed = [s["name"] for s in steps if not s["ok"] and not s.get("skipped")]
    return {"ok": not failed and all(not s.get("skipped") for s in steps), "steps": steps, "failed": failed, "network": inp.network.describe(),
            "checked_at": datetime.now(UTC).isoformat(),
            "detail": "all steps passed" if not failed else "failed at: " + ", ".join(failed)}


# ------------------------------------------------------------------------------------------------- steps
def _dns(inp: DiagnosticsInput) -> dict[str, Any]:
    t0 = time.perf_counter()
    if inp.network.proxy:
        host = urlsplit(inp.network.proxy).hostname or ""
        try:
            socket.getaddrinfo(host, None)
        except OSError as exc:
            return _step("DNS", False, f"the proxy host '{host}' does not resolve: {exc}", t0, "Fix the proxy host name in the integration's network settings (or HTTPS_PROXY).")
        return _step("DNS", True, f"a proxy is configured; target names are resolved by the proxy (proxy host {host} resolves)", t0)
    bad, good = [], []
    for label, url in _hosts(inp.network):
        host = urlsplit(url).hostname or ""
        try:
            addrs = {a[4][0] for a in socket.getaddrinfo(host, None)}
            good.append(f"{label} {host} -> {sorted(addrs)[0]}")
        except OSError as exc:
            bad.append(f"{label} {host}: {exc}")
    if bad:
        return _step("DNS", False, "; ".join(bad), t0,
                     "The control plane cannot resolve Microsoft endpoints. Check the DNS server of the host, split-horizon rules, and egress policy (or configure an HTTPS proxy).")
    return _step("DNS", True, "; ".join(good), t0)


def _tls(inp: DiagnosticsInput) -> dict[str, Any]:
    t0 = time.perf_counter()
    ok_hosts, bad = [], []
    for label, url in _hosts(inp.network):
        scheme = urlsplit(url).scheme
        try:
            with _client(inp, 10.0) as c:
                r = c.get(url + "/")
            ok_hosts.append(f"{label} ({'TLS ok' if scheme == 'https' else 'plain http'}, HTTP {r.status_code})")
        except httpx.HTTPError as exc:
            name = type(exc).__name__
            cert = "certificate" in str(exc).lower() or "ssl" in str(exc).lower()
            bad.append(f"{label}: {name}{' (certificate verification failed)' if cert else ''}")
        except (OSError, FileNotFoundError) as exc:
            bad.append(f"{label}: {exc}")
    if bad:
        return _step("TLS", False, "; ".join(bad), t0,
                     "If your network inspects TLS, put the proxy's root CA chain in a PEM file and set network.ca_bundle (or SSL_CERT_FILE). "
                     "Otherwise check that outbound HTTPS to these hosts is allowed.")
    return _step("TLS", True, "; ".join(ok_hosts), t0)


def _proxy(inp: DiagnosticsInput) -> dict[str, Any]:
    t0 = time.perf_counter()
    if not inp.network.proxy:
        return _step("proxy", True, "no proxy configured: connections are direct" + (" (environment proxy settings are honoured)" if inp.network.trust_env else ""), t0)
    u = urlsplit(inp.network.proxy)
    host, port = u.hostname or "", u.port or (443 if u.scheme == "https" else 8080)
    try:
        with socket.create_connection((host, port), timeout=5):
            pass
    except OSError as exc:
        return _step("proxy", False, f"cannot connect to the proxy {host}:{port}: {exc}", t0, "Check the proxy address/port and that this host may use it.")
    return _step("proxy", True, f"proxy {host}:{port} accepts connections", t0)


def _entra_token(inp: DiagnosticsInput) -> tuple[dict[str, Any], str | None]:
    t0 = time.perf_counter()
    if inp.broker is None:
        return (_step("Entra token", False, inp.broker_problem or "no broker connection is configured", t0,
                      "Create an 'onelake' connection whose secret holds {client_secret} (or certificate) of a service principal and select it as the broker connection "
                      "(PUT /v1/integrations/fabric)."), None)
    try:
        tok = inp.broker.get_token(FABRIC_SCOPE)
    except EntraTokenError as exc:
        return _step("Entra token", False, f"{exc}" + (f" ({exc.code})" if exc.code else ""), t0, exc.remediation), None
    except Exception as exc:  # noqa: BLE001 - diagnostics must report, not raise
        return _step("Entra token", False, f"unexpected error: {type(exc).__name__}", t0), None
    claims = inp.broker.decode_unverified(tok)
    return (_step("Entra token", True, f"token acquired for the Fabric API (tid={claims.get('tid')}, appid={claims.get('appid') or claims.get('azp')}, "
                                       f"expires in {int(float(claims.get('exp', 0)) - time.time())} s)", t0), tok)


def _fabric_api(inp: DiagnosticsInput, token: str | None) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    t0 = time.perf_counter()
    if token is None or inp.broker is None:
        return _step("Fabric API", False, "skipped: no Entra token", t0, "Fix the previous step first.", skipped=True), []
    client = FabricRestClient(inp.broker, inp.network, client_factory=(lambda: _client(inp)), max_attempts=2)
    try:
        ws = client.list_workspaces()
    except FabricApiError as exc:
        return _step("Fabric API", False, str(exc), t0, exc.remediation), []
    visible = {w.get("id") for w in ws}
    missing = [w for w in inp.fabric_workspace_ids if w not in visible]
    if missing:
        return (_step("Fabric API", False, f"authorised ({len(ws)} workspace(s) visible) but mapped workspace(s) {missing} are not visible to the broker", t0,
                      "Add the broker service principal to those Fabric workspaces (Contributor or higher)."), ws)
    return _step("Fabric API", True, f"GET /v1/workspaces authorised: {len(ws)} workspace(s) visible; {len(inp.fabric_workspace_ids)} mapped workspace(s) found", t0), ws


def _onelake(inp: DiagnosticsInput, visible: list[dict[str, Any]]) -> dict[str, Any]:
    t0 = time.perf_counter()
    if inp.broker is None or not visible and not inp.fabric_workspace_ids:
        why = "skipped: no Entra token" if inp.broker is None else "skipped: no workspace to list (none visible, none mapped)"
        return _step("OneLake", False, why, t0, "Fix the previous steps / map a Fabric workspace.", skipped=True)
    ws_id = (inp.fabric_workspace_ids or [visible[0]["id"]])[0]
    try:
        tok = inp.broker.get_token(STORAGE_SCOPE)
    except EntraTokenError as exc:
        return _step("OneLake", False, f"no storage token: {exc}", t0, exc.remediation)
    url = f"{inp.network.onelake_dfs}/{ws_id}"
    try:
        with _client(inp) as c:
            r = c.get(url, params={"resource": "filesystem", "recursive": "false", "maxResults": "1"},
                      headers={"Authorization": f"Bearer {tok}", "x-ms-version": ONELAKE_API_VERSION})
    except httpx.HTTPError as exc:
        return _step("OneLake", False, f"cannot reach OneLake: {type(exc).__name__}", t0, "Check DNS/proxy/CA (see the earlier steps).")
    if r.status_code == 200:
        return _step("OneLake", True, f"listed filesystem of workspace {ws_id} at {inp.network.onelake_dfs}", t0)
    hint = {401: "The storage token was rejected: confirm the tenant and that the service principal is valid.",
            403: ("The service principal cannot read this workspace in OneLake. Enable 'Users can access data stored in OneLake with apps external to Fabric' in the Fabric admin "
                  "portal and add the service principal to the workspace (Viewer or higher)."),
            404: "OneLake did not find the workspace id. Use the workspace GUID, not its name."}.get(r.status_code, "Check the OneLake endpoint (regional endpoints differ).")
    return _step("OneLake", False, f"OneLake answered HTTP {r.status_code} for workspace {ws_id}", t0, hint)


def _inbound(inp: DiagnosticsInput) -> dict[str, Any]:
    t0 = time.perf_counter()
    problems: list[str] = []
    notes: list[str] = []
    if not inp.allowed_tenants:
        problems.append("no allowed Entra tenant is configured")
    if not inp.audiences:
        problems.append("no audience (API application id URI) is configured")
    for tid in inp.allowed_tenants:
        url = inp.jwks_url(tid) if inp.jwks_url else f"{inp.network.authority_host}/{tid}/discovery/v2.0/keys"
        try:
            with _client(inp, 10.0) as c:
                r = c.get(url)
            n = len((r.json() or {}).get("keys", [])) if r.status_code == 200 else 0
            if r.status_code != 200 or not n:
                problems.append(f"JWKS of tenant {tid} unavailable (HTTP {r.status_code})")
            else:
                notes.append(f"tenant {tid}: {n} signing key(s)")
        except (httpx.HTTPError, ValueError) as exc:
            problems.append(f"JWKS of tenant {tid} unreachable: {type(exc).__name__}")
    u = urlsplit(inp.public_url or "")
    if not inp.public_url or u.hostname in (None, "localhost", "127.0.0.1", "::1"):
        notes.append(f"advertised URL '{inp.public_url or '-'}' is local: Fabric cannot reach it until server.public_url points at a name resolvable from the Fabric tenant")
    elif u.scheme != "https":
        problems.append(f"advertised URL {inp.public_url} is not HTTPS (Fabric notebooks should only send tokens over TLS)")
    hs = inp.last_handshake
    if hs:
        age = (datetime.now(UTC) - datetime.fromisoformat(hs["received_at"])).total_seconds()
        notes.append(f"last handshake from a Fabric notebook {int(age)} s ago (workspace {hs.get('fabric_workspace_id')}, SDK {hs.get('sdk_version')})")
        if age > inp.handshake_max_age_seconds:
            notes.append("that handshake is old; run olcf.test_connection() in a Fabric notebook to refresh the proof")
    else:
        notes.append("no handshake from a Fabric notebook has been recorded yet; run olcf.test_connection() in a Fabric notebook")
    if problems:
        return _step("inbound path", False, "; ".join(problems + notes), t0,
                     "Configure allowed tenants and audience (PUT /v1/integrations/fabric), make the Entra JWKS endpoint reachable, and publish the control plane over HTTPS.")
    return _step("inbound path", True, "; ".join(notes), t0)
