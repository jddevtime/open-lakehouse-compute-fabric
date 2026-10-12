# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Public functions: ``run_remote`` / ``extend``, ``status``, ``logs``, ``cancel`` and ``test_connection``."""

from __future__ import annotations

import socket
import ssl
import sys
import time
import urllib.parse
import uuid
from dataclasses import dataclass, field
from typing import Any

from . import __version__
from . import capsule as cap
from . import fabric as fab
from .client import Client, get_client
from .config import current
from .errors import (
    CompatibilityError,
    ConfigurationError,
    OlcfFabricError,
    RemoteExecutionError,
    ServerError,
    SubmissionRejected,
)

TERMINAL = ("succeeded", "failed", "cancelled", "timed_out", "rejected")
DEFAULT_FORMATS: list[str | None] = ["fabricGitSource", "ipynb", None]


@dataclass
class RemoteRun:
    """A submitted remote run. ``wait()`` blocks for the result; ``status()``, ``logs()`` and ``cancel()`` work at any time."""

    submission_id: str
    job_id: str | None
    state: str
    data: dict[str, Any] = field(default_factory=dict, repr=False)
    compatibility: dict[str, Any] | None = field(default=None, repr=False)
    _log_offset: int = field(default=0, repr=False)

    @property
    def exit_value(self) -> Any:
        return ((self.data.get("job") or {}).get("exit_value"))

    @property
    def done(self) -> bool:
        return self.state in TERMINAL

    def refresh(self) -> RemoteRun:
        self.data = get_client().submission(self.submission_id)
        self.state = self.data["state"]
        self.job_id = self.data.get("job_id")
        return self

    def status(self) -> dict[str, Any]:
        self.refresh()
        job = self.data.get("job") or {}
        return {"submission_id": self.submission_id, "job_id": self.job_id, "state": self.state, "exit_value": job.get("exit_value"), "failure": job.get("failure"),
                "started_at": job.get("started_at"), "finished_at": job.get("finished_at")}

    def logs(self, offset: int | None = None, limit: int = 65536) -> str:
        """Log text from ``offset`` (default: continue where the last call stopped)."""
        start = self._log_offset if offset is None else offset
        page = get_client().logs(self.submission_id, start, limit)
        if offset is None:
            self._log_offset = page.get("next_offset", start)
        return page.get("data", "")

    def cancel(self) -> dict[str, Any]:
        out = get_client().cancel(self.submission_id)
        self.state = out.get("state", self.state)
        return out

    def wait(self, *, stream_logs: bool = True, poll_interval: float = 2.0, timeout: float | None = None, out: Any = None) -> RemoteRun:
        """Poll until the run is terminal, printing new log text as it arrives."""
        out = out or sys.stdout
        deadline = None if timeout is None else time.monotonic() + timeout
        interval = max(0.2, poll_interval)
        tail = ""
        try:
            while True:
                self.refresh()
                if stream_logs:
                    tail = self._drain(out, tail)
                if self.done:
                    break
                if deadline is not None and time.monotonic() > deadline:
                    raise OlcfFabricError(f"timed out after {timeout} s waiting for {self.submission_id} (state: {self.state})",
                                          remediation="the remote run keeps going; use olcf.status()/olcf.logs()/olcf.cancel() with the submission id")
                time.sleep(interval)
                interval = min(interval * 1.25, 5.0)
            if stream_logs:
                self._drain(out, tail, final=True)
                self._print_output(out)
        except KeyboardInterrupt:
            try:
                self.cancel()
            finally:
                raise
        self._tail = tail  # type: ignore[attr-defined]
        return self

    def _print_output(self, out: Any) -> None:
        """Print what the remote notebook printed (the executed notebook's cell outputs); never fails the run."""
        try:
            doc = get_client().output(self.submission_id)
        except OlcfFabricError:
            return
        if not doc.get("available"):
            return
        for cell in doc.get("cells", []):
            out.write(f"--- remote cell [{cell.get('index')}] ---\n")
            for part in cell.get("outputs", []):
                text = part.get("text", "")
                out.write(text if text.endswith("\n") else text + "\n")
        if doc.get("truncated"):
            out.write("[... remote output truncated ...]\n")
        out.flush()

    def _drain(self, out: Any, tail: str, final: bool = False) -> str:
        for _ in range(200):
            try:
                page = get_client().logs(self.submission_id, self._log_offset)
            except ServerError:
                break
            data = page.get("data", "")
            if data:
                out.write(data)
                out.flush()
                tail = (tail + data)[-8192:]
            moved = page.get("next_offset", self._log_offset) != self._log_offset
            self._log_offset = page.get("next_offset", self._log_offset)
            if not moved or (not final and not data):
                break
        return tail


# ================================================================================================= helpers
def _caller_globals(depth: int) -> dict[str, Any]:
    try:
        return sys._getframe(depth).f_globals  # noqa: SLF001 - reads the declared parameters from the notebook's namespace
    except ValueError:
        return {}


def _identity(client: Client, ctxn: dict[str, Any]) -> dict[str, Any]:
    claims = client.auth.claims()
    ident = {"tenantId": claims.get("tid") or client.settings.tenant_id, "userId": ctxn.get("userId"), "userName": ctxn.get("userName"), "appId": claims.get("appid") or claims.get("azp")}
    return {k: v for k, v in ident.items() if v}


def _acquire(client: Client, ctxn: dict[str, Any], source: str | None, source_format: str | None, notebook: str | None, workspace_id: str | None, mode: str
             ) -> tuple[str, str | None, tuple[str, str] | None, str | None]:
    """``(mode, text, original, how)``: inline with text, or pull."""
    if source is not None:
        fmt = source_format or fab.detect_format(source)
        return "inline", source, (fmt, source), "provided"
    if mode == "pull":
        return "pull", None, None, "pull requested"
    name = notebook or ctxn.get("currentNotebookName")
    if name:
        hints = client.settings.extra.get("definition_formats")
        found = fab.acquire_definition(str(name), workspace_id or ctxn.get("currentWorkspaceId"), list(hints) if hints else DEFAULT_FORMATS)
        if found:
            fmt, text, how = found
            return "inline", text, (fmt, text), how
    if mode == "inline":
        raise ConfigurationError("could not read the notebook definition with notebookutils.notebook.getDefinition",
                                 remediation="pass source='...' explicitly, or let the SDK use pull mode (mode='auto'), which needs the control plane to be configured with a broker identity")
    return "pull", None, None, "getDefinition unavailable"


def _declared_parameters(text: str | None, fmt: str, caller: dict[str, Any], explicit: dict[str, Any] | None) -> dict[str, Any]:
    params: dict[str, Any] = {}
    if text:
        for name in fab.declared_parameter_names(text, fmt):
            if name in caller:
                ok, value = fab.json_safe(caller[name])
                if ok:
                    params[name] = value
    for k, v in (explicit or {}).items():
        ok, value = fab.json_safe(v)
        if not ok:
            raise ConfigurationError(f"parameter '{k}' is not JSON-serialisable", remediation="pass strings, numbers, booleans, lists and dicts of those")
        params[str(k)] = value
    return params


def _raise_rejected(exc: ServerError) -> None:
    if exc.code == "compatibility_blocked":
        report = (exc.details or {}).get("report") or {}
        raise CompatibilityError(exc.message, status=exc.status, code=exc.code, remediation=exc.remediation, details=exc.details, report=report) from None
    if 400 <= exc.status < 500 and exc.status not in (401, 408, 429):
        raise SubmissionRejected(exc.message, status=exc.status, code=exc.code, remediation=exc.remediation, details=exc.details) from None
    raise exc


# ================================================================================================= run_remote
def run_remote(*, parameters: dict[str, Any] | None = None, wait: bool = True, exit: bool | None = None, stream_logs: bool = True, poll_interval: float = 2.0,  # noqa: A002
               timeout: float | None = None, notebook: str | None = None, workspace_id: str | None = None, source: str | None = None, source_format: str | None = None,
               mode: str = "auto", runtime_family: str | None = None, spark_conf: dict[str, str] | None = None, dependency_lock: dict[str, Any] | None = None,
               engine: str | None = None, queue: str | None = None, priority: int | None = None, timeout_seconds: int | None = None, retries: int | None = None,
               resources: dict[str, Any] | None = None, placement: dict[str, Any] | None = None, idempotency_key: str | None = None, validate: bool = True,
               correlation_id: str | None = None, _depth: int = 2) -> RemoteRun | Any:
    """Run the *whole current notebook* on the on-prem compute fabric and return its result.

    The cell that calls this function is excluded from the remote run. With ``wait=True`` (default) the call streams the remote log into the cell output, then

    * on success hands the notebook's exit value to ``notebookutils.notebook.exit(...)`` (so a pipeline sees it) - pass ``exit=False`` to get it returned instead;
    * on any other end state raises :class:`RemoteExecutionError` (so a pipeline activity fails exactly as a local failure would).

    With ``wait=False`` a :class:`RemoteRun` handle is returned immediately (``olcf.status(run)``, ``olcf.logs(run)``, ``olcf.cancel(run)``).

    ``parameters`` are *explicit* values merged over the notebook's declared parameter-cell variables (names assigned in the cell tagged ``parameters``, read from this
    session). Nothing else from the interpreter is sent. Rejections before execution raise :class:`CompatibilityError` (with the analyzer report) or :class:`SubmissionRejected`.
    """
    client = get_client()
    s = client.settings
    raw = fab.runtime_context()
    ctxn = fab.normalise_context(raw)
    ws_id = workspace_id or ctxn.get("currentWorkspaceId")
    nb_mode, text, original, how = _acquire(client, ctxn, source, source_format, notebook, ws_id, mode)
    fmt = original[0] if original else "ipynb"
    excluded: list[int] = []
    if text is not None:
        text, excluded = fab.drop_submit_cells(text, fmt)
    caller = _caller_globals(_depth)
    params = _declared_parameters(text, fmt, caller, parameters)
    nb_name = notebook or ctxn.get("currentNotebookName")
    src = cap.source_block(mode=nb_mode, fmt=fmt, content=text, notebook_id=ctxn.get("currentNotebookId"), notebook_name=nb_name, workspace_id=ws_id,
                           workspace_name=ctxn.get("currentWorkspaceName"), original=original, excluded=excluded)
    logical: list[dict[str, Any]] = []
    if ctxn.get("defaultLakehouseId") or ctxn.get("defaultLakehouseName"):
        logical.append({"kind": "lakehouse", "role": "default", "fabricLakehouseId": ctxn.get("defaultLakehouseId"), "name": ctxn.get("defaultLakehouseName"),
                        "fabricWorkspaceId": ctxn.get("defaultLakehouseWorkspaceId") or ws_id, "workspaceName": ctxn.get("defaultLakehouseWorkspaceName")})
    constraints = {"engine": engine} if engine else {}
    policy = {k: v for k, v in {"queue": queue, "priority": priority, "timeoutSeconds": timeout_seconds, "retries": retries, "resources": resources, "placement": placement}.items() if v is not None}
    parts = cap.request_parts(source=src, context={"normalized": ctxn, "raw": fab.raw_context(raw)},
                              runtime={"family": runtime_family or s.runtime_family, "constraints": constraints, "sparkConf": dict(spark_conf or {}),
                                       **({"dependencyLock": dependency_lock} if dependency_lock else {})},
                              parameters=params, data_references={"resolved": [], "logical": logical}, policy=policy, identity=_identity(client, ctxn),
                              secret_refs=fab.secret_refs_in(text or ""))
    report: dict[str, Any] | None = None
    if validate:
        try:
            v = client.validate(parts)
        except ServerError as exc:
            _raise_rejected(exc)
            raise
        report = v.get("compatibility")
        if not v.get("ok"):
            blockers = (report or {}).get("blockers") or []
            if v.get("blocked"):
                rules = ", ".join(sorted({b.get("rule", "?") for b in blockers})[:8])
                raise CompatibilityError(f"the notebook uses constructs that the on-prem runtime cannot run ({rules})", remediation="see the report: replace or remove the blocking constructs",
                                         details={"issues": v.get("issues"), "blockers": blockers}, report=report)
            issue = (v.get("issues") or [{}])[0]
            raise SubmissionRejected(issue.get("reason") or "the control plane would reject this notebook", status=422, code=issue.get("code"), remediation=issue.get("remediation"),
                                     details={"issues": v.get("issues")})
        parts["compatibility"] = {"analyzerVersion": (report or {}).get("analyzer_version"), "counts": (report or {}).get("counts"), "blocked": False}
    t0 = time.monotonic()
    ch = client.challenge()
    signed = cap.build_signed(parts, ch, idempotency_key=idempotency_key, correlation_id=correlation_id or str(uuid.uuid4()), elapsed_since_challenge=time.monotonic() - t0)
    try:
        acc = client.submit(signed)
    except ServerError as exc:
        _raise_rejected(exc)
        raise
    run = RemoteRun(submission_id=acc["id"], job_id=acc.get("job_id"), state=acc["state"], data=acc, compatibility=report)
    if not wait:
        return run
    run.wait(stream_logs=stream_logs, poll_interval=poll_interval, timeout=timeout)
    return _finish(run, exit)


def extend(**kwargs: Any) -> RemoteRun | Any:
    """Alias of :func:`run_remote` ("extend this notebook onto the on-prem fabric")."""
    kwargs["_depth"] = 3
    return run_remote(**kwargs)


def finish(run: RemoteRun | str, *, exit: bool | None = None) -> Any:  # noqa: A002
    """Apply the success/failure semantics of ``run_remote(wait=True)`` to a run started with ``wait=False`` (after it finished)."""
    r = run if isinstance(run, RemoteRun) else RemoteRun(str(run), None, "unknown")
    r.refresh()
    return _finish(r, exit)


def _finish(run: RemoteRun, exit_: bool | None) -> Any:
    job = run.data.get("job") or {}
    if run.state != "succeeded":
        failure = job.get("failure") or ({"category": run.data.get("failure_category"), "message": run.data.get("error")} if run.data.get("error") else {})
        msg = f"the remote run {run.state}" + (f" ({failure.get('category')}): {failure.get('message')}" if failure.get("message") else "")
        if failure.get("cell") is not None:
            msg += f" [cell {failure.get('cell')}" + (f", line {failure.get('line')}]" if failure.get("line") else "]")
        raise RemoteExecutionError(msg, state=run.state, job_id=run.job_id, submission_id=run.submission_id, failure=failure, logs=getattr(run, "_tail", ""),
                                   remediation=failure.get("remediation"))
    value = job.get("exit_value")
    in_fabric = fab.notebookutils_module() is not None
    if exit_ is None:
        exit_ = in_fabric
    if exit_:
        fab.exit_notebook("" if value is None else value)
    return value


def status(run: RemoteRun | str) -> dict[str, Any]:
    """Current state of a remote run (a :class:`RemoteRun` or a submission id)."""
    r = run if isinstance(run, RemoteRun) else RemoteRun(str(run), None, "unknown")
    return r.status()


def logs(run: RemoteRun | str, offset: int | None = None, limit: int = 65536) -> str:
    r = run if isinstance(run, RemoteRun) else RemoteRun(str(run), None, "unknown")
    return r.logs(offset, limit)


def cancel(run: RemoteRun | str) -> dict[str, Any]:
    r = run if isinstance(run, RemoteRun) else RemoteRun(str(run), None, "unknown")
    return r.cancel()


# ================================================================================================= test_connection
def test_connection(*, verbose: bool = True, out: Any = None) -> dict[str, Any]:
    """Check the whole path from this Fabric session to the control plane and report it to the control plane (``POST /handshake``).

    Checklist: DNS, TLS, token, challenge, handshake, authorisation, server version, latency, Fabric context seen. Returns ``{"ok": bool, "checks": [...]}``; nothing raises.
    """
    out = out or sys.stdout
    checks: list[dict[str, Any]] = []

    def add(name: str, ok: bool, detail: str, ms: float | None = None, remediation: str | None = None) -> None:
        checks.append({"name": name, "ok": ok, "detail": detail, "ms": None if ms is None else round(ms, 1), "remediation": None if ok else remediation})

    s = current()
    if not s.endpoint:
        add("configuration", False, "no endpoint configured", None, "olcf.configure(endpoint='https://...', ...)")
        return _report(checks, None, verbose, out)
    u = urllib.parse.urlsplit(s.endpoint)
    host = u.hostname or ""
    port = u.port or (443 if u.scheme == "https" else 80)
    t = time.perf_counter()
    try:
        addrs = {a[4][0] for a in socket.getaddrinfo(host, port, type=socket.SOCK_STREAM)}
        add("DNS", True, f"{host} -> {', '.join(sorted(addrs))}", (time.perf_counter() - t) * 1000)
    except OSError as exc:
        add("DNS", False, f"{host} does not resolve: {exc}", (time.perf_counter() - t) * 1000, "check the endpoint and that Fabric can resolve it (private DNS / VPN / public record)")
    if u.scheme == "https":
        t = time.perf_counter()
        try:
            from ._http import ssl_context

            with socket.create_connection((host, port), timeout=min(s.timeout, 10)) as raw_sock, ssl_context(s.ca_bundle).wrap_socket(raw_sock, server_hostname=host) as tls:
                add("TLS", True, f"{tls.version()} certificate verified", (time.perf_counter() - t) * 1000)
        except ssl.SSLCertVerificationError as exc:
            add("TLS", False, f"certificate not trusted: {exc.verify_message}", (time.perf_counter() - t) * 1000, "pass your CA bundle: olcf.configure(ca_bundle='/path/ca.pem')")
        except OSError as exc:
            add("TLS", False, f"cannot connect: {exc}", (time.perf_counter() - t) * 1000, "check firewall / private endpoint / proxy between Fabric and the control plane")
    else:
        add("TLS", True, "plain http to a loopback address (development only)")
    client: Client | None = None
    try:
        client = get_client()
        t = time.perf_counter()
        client.auth.token()
        add("token", True, f"obtained via '{client.auth.mode_used}'", (time.perf_counter() - t) * 1000)
    except OlcfFabricError as exc:
        add("token", False, exc.message, None, exc.remediation)
    ch = None
    if client is not None and all(c["ok"] for c in checks if c["name"] == "token"):
        try:
            t = time.perf_counter()
            ch = client.challenge()
            add("challenge", True, f"nonce + signing key issued (key id {ch.get('keyId')})", (time.perf_counter() - t) * 1000)
        except OlcfFabricError as exc:
            add("challenge", False, exc.message, None, exc.remediation or "the credential may not hold integration.fabric.submit")
    hs: dict[str, Any] | None = None
    raw = fab.runtime_context()
    ctxn = fab.normalise_context(raw)
    seen = sorted(k for k, v in ctxn.items() if v not in (None, ""))
    if client is not None:
        try:
            t = time.perf_counter()
            hs = client.handshake({"sdkVersion": __version__, "checks": checks, "context": {"normalized": ctxn}, "identity": _identity(client, ctxn)})
            latency = (time.perf_counter() - t) * 1000
            add("handshake", True, f"recorded as {hs.get('handshakeId')}", latency)
            add("latency", True, f"{latency:.0f} ms round trip", latency)
            add("server version", True, f"{hs.get('serverVersion')} ({hs.get('adapterVersion')})")
            add("authorisation", bool(hs.get("authorized")), "authorised for the mapped workspace" if hs.get("authorized") else "; ".join(hs.get("problems") or ["not authorised"]),
                None, "ask your OLCF administrator to map this Fabric workspace and allow this identity (PUT /v1/integrations/fabric)")
        except OlcfFabricError as exc:
            add("handshake", False, exc.message, None, exc.remediation)
    inside = fab.notebookutils_module() is not None
    add("Fabric context", bool(ctxn.get("currentWorkspaceId") and ctxn.get("currentNotebookId")),
        f"{len(seen)} context keys seen ({', '.join(seen[:6])}{'...' if len(seen) > 6 else ''})" if seen else ("notebookutils present but the context is empty" if inside else "not inside a Fabric session (no notebookutils)"),
        None, "run this from a Fabric notebook; outside Fabric only the connectivity checks are meaningful")
    return _report(checks, hs, verbose, out)


def _report(checks: list[dict[str, Any]], hs: dict[str, Any] | None, verbose: bool, out: Any) -> dict[str, Any]:
    ok = all(c["ok"] for c in checks)
    if verbose:
        for c in checks:
            out.write(f"  [{'ok' if c['ok'] else 'FAIL'}] {c['name']}: {c['detail']}" + (f" ({c['ms']} ms)" if c.get("ms") is not None else "") + "\n")
            if not c["ok"] and c.get("remediation"):
                out.write(f"         -> {c['remediation']}\n")
        out.write("OLCF connection OK\n" if ok else "OLCF connection has problems (see above)\n")
    return {"ok": ok, "checks": checks, "handshake": hs}


test_connection.__test__ = False  # type: ignore[attr-defined]  # not a pytest test

__all__ = ["RemoteRun", "cancel", "extend", "finish", "logs", "run_remote", "status", "test_connection"]
