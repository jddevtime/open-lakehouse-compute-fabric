# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Apache Livy REST client and a *remote execution* provider base built on it.

Livy is used by two provider types:

* ``fabric`` - the Microsoft Fabric Livy API of a lakehouse (``.../livyapi/versions/2023-12-01``), authenticated with an Entra token,
* ``external`` registered with a ``livy_url`` and no ``master_url`` - any Livy server (self-managed Spark, HDInsight, EMR ...).

Protocol used (Livy REST): ``POST /sessions`` (pyspark session), ``GET /sessions/{id}``, ``POST /sessions/{id}/statements``,
``GET /sessions/{id}/statements/{sid}``, ``GET /sessions/{id}/log``, ``DELETE /sessions/{id}``, and ``POST/GET/DELETE /batches``
for jar/python files that are already addressable by the cluster (``artifact_uri``).

Execution is **driven by ``status()``** (called by the controller's monitor loop): create the session, wait until it is ``idle``,
submit *one* statement containing the whole job (notebook cells are flattened), then poll it. That keeps remote jobs resumable after a
controller restart: everything needed is in the persisted provider handle (``session_id``, ``statement_id``).

**Never replay a create (review finding F04).** Livy has no idempotency key, so the client retries only requests that cannot have changed anything remotely: reads
and deletes, failures proven to happen before the request was transmitted (connection refused / DNS / TLS handshake), and ``429`` *responses* (a throttling answer is proof
that the request was rejected). A ``POST`` whose answer was lost after transmission, or that was answered ``500``/``502``/``503``/``504`` (a gateway can return ``503``
after the upstream committed the create), raises
:class:`~olcf.compute.errors.SubmissionUncertain`. The provider then reconciles against the remote state instead of trying again:

* sessions and batches carry the OLCF attempt as their Livy ``name`` (:func:`livy_name`) and are looked up by it;
* the statement is submitted in two phases (the intent is persisted with the handle first); a session belongs to exactly one attempt and receives exactly one
  statement, so the statement list of the session shows without ambiguity whether the POST was accepted;
* what cannot be learned stays an explicit, operator-visible "uncertain" status (``AppStatus.code``) - never a blind second POST. Only after the remote has been
  verified free of the object for ``submission_quiesce_seconds`` is the attempt failed as *not created* (safe to retry); an outcome that stays unknown for
  ``submission_reconcile_seconds`` fails the attempt as *unresolved* and is never retried automatically.
"""

from __future__ import annotations

import json
import logging
import re
import time
from typing import Any

import httpx

from olcf.compute.errors import ProviderError, SubmissionUncertain
from olcf.compute.providers.base import (
    AppHandle,
    AppStatus,
    ComputeProvider,
    Discovery,
    LogsPage,
    SubmitRequest,
    UiInfo,
)
from olcf.core.errors import Unsupported, ValidationFailed
from olcf.notebooks.formats import normalize_notebook
from olcf.notebooks.params import INJECTED_TAG, inject_parameters, py_literal, render_parameter_cell
from olcf.runtime_lib.magics import split_statements

log = logging.getLogger("olcf.compute.livy")

_SESSION_STATE = {
    "not_started": "pending", "starting": "pending", "idle": "pending", "busy": "running", "running": "running", "recovering": "running",
    "shutting_down": "running", "error": "failed", "dead": "failed", "killed": "cancelled", "success": "succeeded",
}
_BATCH_STATE = {
    "not_started": "pending", "starting": "pending", "recovering": "running", "running": "running", "busy": "running", "idle": "running",
    "shutting_down": "running", "error": "failed", "dead": "failed", "killed": "cancelled", "success": "succeeded",
}
#: Livy settings the platform (or the Livy server) owns. They are never forwarded from user configuration, whatever the provider.
_FORBIDDEN_CONF = ("spark.driver.host", "spark.driver.bindAddress", "spark.driver.port", "spark.blockManager", "spark.master")
#: Failures that happen *before* a request is transmitted: nothing can have reached the server, so even a POST may be sent again.
_PRE_TRANSMISSION = (httpx.ConnectError, httpx.ConnectTimeout, httpx.PoolTimeout, httpx.UnsupportedProtocol)
_NAME_RE = re.compile(r"^olcf-(job_[A-Za-z0-9]+?)(?:-a(\d+))?$")
DEFAULT_QUIESCE_SECONDS = 60.0
DEFAULT_UNRESOLVED_SECONDS = 900.0
OUTPUT_LIMIT = 64 * 1024


def livy_name(job_id: str, attempt: int) -> str:
    """The Livy ``name`` of a session or batch created for one OLCF attempt - the client-generated tag that makes an ambiguous create reconcilable."""
    return f"olcf-{job_id}-a{int(attempt)}"


def parse_livy_name(name: str) -> tuple[str, int | None] | None:
    """Inverse of :func:`livy_name` (also accepts the attempt-less names of earlier versions)."""
    m = _NAME_RE.match(name or "")
    return (m.group(1), int(m.group(2)) if m.group(2) else None) if m else None


def livy_conf(spark: dict[str, Any] | None) -> dict[str, str]:
    """User Spark configuration for a Livy session or batch: scalar values as strings, without driver / network / master settings (Livy's server owns those)."""
    out: dict[str, str] = {}
    for k, v in ((spark or {}).get("conf") or {}).items():
        if isinstance(v, (dict, list)) or any(str(k).startswith(p) for p in _FORBIDDEN_CONF):
            continue
        out[str(k)] = "true" if v is True else "false" if v is False else str(v)
    return out


class LivyClient:
    """Minimal Livy REST client (httpx) with an *operation-specific* retry policy (see the module docstring).

    ``GET``/``DELETE`` are retried on any transport failure and on ``429/502/503/504``. A ``POST`` is retried only on failures proven to precede transmission and on
    ``429`` responses; any other ambiguous outcome (including any ``5xx`` answer such as ``500`` or ``503``) raises :class:`SubmissionUncertain` after exactly one transmission.
    """

    def __init__(self, base_url: str, headers_fn=None, *, verify: bool | str = True, timeout: float = 30.0, sleep=time.sleep, max_attempts: int = 3) -> None:  # noqa: ANN001
        self.base = base_url.rstrip("/")
        self._headers_fn = headers_fn or (lambda: {})
        self._sleep, self._max_attempts = sleep, max(1, int(max_attempts))
        self._http = httpx.Client(timeout=httpx.Timeout(timeout, connect=10.0), verify=verify, follow_redirects=False)

    def close(self) -> None:
        self._http.close()

    def _backoff(self, attempt: int, response: httpx.Response | None = None) -> None:
        wait = 0.2 * (attempt + 1)
        if response is not None and response.headers.get("Retry-After", "").isdigit():
            wait = min(30.0, float(response.headers["Retry-After"]))
        self._sleep(wait)

    def _call(self, method: str, path: str, *, json_body: Any = None, params: dict[str, Any] | None = None, ok: tuple[int, ...] = (200, 201, 202)) -> Any:
        creates = method == "POST"  # every POST of this API creates remote work and is not idempotent
        last: str = ""
        for attempt in range(self._max_attempts):
            more = attempt < self._max_attempts - 1
            headers = {"Accept": "application/json", "X-Requested-By": "olcf", **self._headers_fn()}  # a credential failure here happens before any transmission
            try:
                r = self._http.request(method, self.base + path, json=json_body, params=params, headers=headers)
            except httpx.HTTPError as exc:
                last = f"{type(exc).__name__}: {exc}"
                if creates and not isinstance(exc, _PRE_TRANSMISSION):
                    raise SubmissionUncertain(f"Livy {method} {path}: the connection failed after the request may have been sent ({last}); not repeating it",
                                              remediation="OLCF reconciles with the Livy server by session/batch name; check the server if the job stays in 'submission uncertain'") from exc
                if more:
                    self._backoff(attempt)
                    continue
                break
            code = r.status_code
            if code == 429 and more:  # a 429 is a known rejection (the request was throttled, not processed), so even a create may be repeated
                self._backoff(attempt, r)
                continue
            if code in (502, 503, 504) and not creates and more:  # a gateway can answer 503 after the upstream committed: only reads and deletes may be repeated
                self._backoff(attempt, r)
                continue
            if code == 404 and method in ("GET", "DELETE"):
                return None
            if code not in ok:
                if creates and code >= 500:
                    raise SubmissionUncertain(f"Livy {method} {path}: HTTP {code} {r.text[:200]}; the server may have created the object before failing",
                                              details={"status": code})
                raise ProviderError(f"Livy {method} {path} failed: HTTP {code} {r.text[:300]}", details={"status": code})
            if not r.content:
                return {}
            try:
                return r.json()
            except ValueError:
                return {"raw": r.text}
        raise ProviderError(f"Livy {method} {path} unreachable: {last}")

    # sessions
    def create_session(self, body: dict[str, Any]) -> dict[str, Any]:
        return self._call("POST", "/sessions", json_body=body)

    def get_session(self, sid: int | str) -> dict[str, Any] | None:
        return self._call("GET", f"/sessions/{sid}")

    def _list(self, path: str, key_options: tuple[str, ...]) -> list[dict[str, Any]]:
        """All entries of a Livy collection (paged; bounded)."""
        out: list[dict[str, Any]] = []
        for _ in range(40):
            d = self._call("GET", path, params={"from": len(out), "size": 500}) or {}
            items = next((d[k] for k in key_options if d.get(k) is not None), [])
            out += list(items)
            if not items or len(out) >= int(d.get("total", len(out))):
                break
        return out

    def list_sessions(self) -> list[dict[str, Any]]:
        return self._list("/sessions", ("sessions",))

    def post_statement(self, sid: int | str, code: str, kind: str = "pyspark") -> dict[str, Any]:
        return self._call("POST", f"/sessions/{sid}/statements", json_body={"code": code, "kind": kind})

    def list_statements(self, sid: int | str) -> list[dict[str, Any]] | None:
        """The statements of a session (``None`` when the server has no such route / session)."""
        d = self._call("GET", f"/sessions/{sid}/statements")
        return None if d is None else list(d.get("statements") or [])

    def get_statement(self, sid: int | str, stid: int | str) -> dict[str, Any] | None:
        return self._call("GET", f"/sessions/{sid}/statements/{stid}")

    def session_log(self, sid: int | str, start: int = 0, size: int = 200) -> dict[str, Any]:
        return self._call("GET", f"/sessions/{sid}/log", params={"from": start, "size": size}) or {"log": [], "total": 0, "from": start}

    def delete_session(self, sid: int | str) -> None:
        self._call("DELETE", f"/sessions/{sid}", ok=(200, 202, 204))

    # batches
    def create_batch(self, body: dict[str, Any]) -> dict[str, Any]:
        return self._call("POST", "/batches", json_body=body)

    def get_batch(self, bid: int | str) -> dict[str, Any] | None:
        return self._call("GET", f"/batches/{bid}")

    def list_batches(self) -> list[dict[str, Any]]:
        return self._list("/batches", ("sessions", "batches"))

    def batch_log(self, bid: int | str, start: int = 0, size: int = 200) -> dict[str, Any]:
        return self._call("GET", f"/batches/{bid}/log", params={"from": start, "size": size}) or {"log": [], "total": 0, "from": start}

    def delete_batch(self, bid: int | str) -> None:
        self._call("DELETE", f"/batches/{bid}", ok=(200, 202, 204))


def _notebook(text: str) -> dict[str, Any]:
    """The notebook of a job source as a validated, normalised nbformat document (every cell has an id and a string source)."""
    try:
        raw = json.loads(text)
    except ValueError as exc:
        raise Unsupported("the notebook source is not valid JSON") from exc
    if not isinstance(raw, dict):
        raise Unsupported("the notebook source is not a notebook document")
    raw.setdefault("nbformat", 4)
    raw.setdefault("nbformat_minor", 5)
    raw.setdefault("metadata", {})
    try:
        return normalize_notebook(raw)
    except ValidationFailed as exc:
        raise Unsupported(f"the notebook source is not a valid nbformat document: {exc.message}") from exc


def flatten_source(spec: dict[str, Any], source: bytes | None) -> str:
    """Turn the job source into the Python text of a single PySpark statement (fail closed on unsupported constructs).

    Parameters use the platform's shared injection (:mod:`olcf.notebooks.params`): a notebook gets the ``injected-parameters`` cell *after* its tagged ``parameters``
    cell (so submitted values override the defaults, exactly as on a worker); scripts and SQL get the same assignments plus ``PARAMETERS``, as the runner provides.
    """
    fmt = (spec.get("source") or {}).get("format")
    text = (source or b"").decode("utf-8", "replace")
    params = dict(spec.get("parameters") or {})
    if fmt in ("py", "script"):
        return _script_header(params) + text
    if fmt == "sql":
        try:
            stmts = split_statements(text, strict=True)
        except ValueError as exc:
            raise Unsupported(f"the SQL source cannot be split into statements: {exc}", remediation="close the quote or comment, or submit one statement per job") from exc
        lines = []
        for s in stmts:
            args = ", args=PARAMETERS" if params and re.search(r":[A-Za-z_]\w*", s) else ""
            lines.append(f"spark.sql({s!r}{args}).show(truncate=False)")
        return _script_header(params) + "\n".join(lines)
    if fmt in ("ipynb", "fabric_git_source"):
        cells = inject_parameters(_notebook(text), params or None)["cells"]
        parts, index = [], 0
        for cell in cells:
            injected = INJECTED_TAG in ((cell.get("metadata") or {}).get("tags") or [])
            label = "injected parameters" if injected else f"cell {index}"
            if not injected:
                index += 1
            if cell.get("cell_type") != "code":
                continue
            src = str(cell.get("source") or "")
            first = src.lstrip().split("\n", 1)[0].strip()
            if first.startswith("%%sql"):
                body = src.lstrip().split("\n", 1)[1] if "\n" in src.lstrip() else ""
                src = f"spark.sql({body.strip()!r}).show(truncate=False)"
            elif first.startswith("%%pyspark"):
                src = src.lstrip().split("\n", 1)[1] if "\n" in src.lstrip() else ""
            elif first.startswith("%"):
                raise Unsupported(f"{label} uses the magic '{first.split()[0]}' which cannot be executed through Livy",
                                  remediation="run this notebook on a worker-backed provider (standalone/local) instead")
            parts.append(f"# --- {label} ---\n{src}")
        return "\n\n".join(parts)
    raise Unsupported(f"source format '{fmt}' cannot be executed through a Livy session")


def _script_header(params: dict[str, Any]) -> str:
    if not params:
        return ""
    return "# --- OLCF parameters ---\n" + render_parameter_cell(params) + f"\nPARAMETERS = {py_literal(params)}\n\n"


def _text(out: dict[str, Any]) -> str:
    return str((out.get("data") or {}).get("text/plain", ""))


class LivyRemoteProvider(ComputeProvider):
    """Remote execution through a Livy server. Subclasses provide ``_client()`` (URL + auth) and metadata."""

    uses_runtimes = False  # the Livy server brings its own Spark/Python
    prepares_spark = False  # Livy builds the Spark session itself
    result_contract = "provider"  # the Livy statement / batch state and output are the result

    @property
    def execution(self) -> str:
        return "remote"

    def _client(self) -> LivyClient:
        raise NotImplementedError

    @property
    def _quiesce(self) -> float:
        return float(self.config.get("submission_quiesce_seconds", DEFAULT_QUIESCE_SECONDS))

    @property
    def _unresolved_after(self) -> float:
        return float(self.config.get("submission_reconcile_seconds", DEFAULT_UNRESOLVED_SECONDS))

    def _session_conf(self, req: SubmitRequest) -> dict[str, Any]:
        spark = req.spec.get("spark") or {}
        body: dict[str, Any] = {"name": livy_name(req.job["id"], int(req.job["attempt"])), "kind": "pyspark", "conf": livy_conf(spark)}
        res = (req.job.get("resources_request") or {})
        if res.get("driver_memory_mb"):
            body["driverMemory"] = f"{int(res['driver_memory_mb'])}m"
        if res.get("driver_cores"):
            body["driverCores"] = int(res["driver_cores"])
        if res.get("executor_memory_mb"):
            body["executorMemory"] = f"{int(res['executor_memory_mb'])}m"
        if res.get("executor_cores"):
            body["executorCores"] = int(res["executor_cores"])
        if res.get("max_executors") or res.get("min_executors"):
            body["numExecutors"] = int(res.get("max_executors") or res.get("min_executors") or 1)
        return body

    def _batch_body(self, req: SubmitRequest, name: str) -> dict[str, Any]:
        """Livy batch request from the *normalised* source (``source.artifact_uri`` / ``main_class`` / ``args``), never from the notebook parameters (F11)."""
        src = req.spec.get("source") or {}
        args = src.get("args") or []
        if not isinstance(args, (list, tuple)):
            raise Unsupported("source.args must be a list of strings", remediation="define the arguments as a list in the Spark job definition")
        body: dict[str, Any] = {"file": src["artifact_uri"], "name": name, "conf": livy_conf(req.spec.get("spark")), "args": [str(a) for a in args]}
        main_class = src.get("main_class")
        if main_class is not None and not isinstance(main_class, str):
            raise Unsupported("source.main_class must be a string")
        if src.get("format") == "jar" and not main_class:
            raise Unsupported("a jar job needs source.main_class (the entry point Livy passes as className)",
                              remediation="set main_class on the Spark job definition (or the job's source) and submit again")
        if main_class:
            body["className"] = main_class
        return body

    # ---- provider interface ------------------------------------------------------------
    def submission_intent(self, req: SubmitRequest) -> dict[str, Any] | None:
        src = req.spec.get("source") or {}
        name = livy_name(req.job["id"], int(req.job["attempt"]))
        if src.get("artifact_uri") and src.get("format") in ("jar", "py", "scala"):
            return {"kind": "batch", "batch_name": name}
        return {"kind": "session", "session_name": name, "code": flatten_source(req.spec, req.source_bytes), "log_offset": 0}

    def submit(self, req: SubmitRequest) -> AppHandle:
        client = self._client()
        src = req.spec.get("source") or {}
        name = livy_name(req.job["id"], int(req.job["attempt"]))
        job_id, attempt = req.job["id"], int(req.job["attempt"])
        if src.get("artifact_uri") and src.get("format") in ("jar", "py", "scala"):
            body = self._batch_body(req, name)
            try:
                d = client.create_batch(body)
            except SubmissionUncertain as exc:
                return self._after_lost_create(client, job_id, attempt, "batch", name, exc, {})
            return AppHandle(self.name, job_id, attempt, application_id=d.get("appId"), native={"kind": "batch", "batch_id": d["id"], "batch_name": name})
        code = flatten_source(req.spec, req.source_bytes)
        try:
            d = client.create_session(self._session_conf(req))
        except SubmissionUncertain as exc:
            return self._after_lost_create(client, job_id, attempt, "session", name, exc, {"code": code, "log_offset": 0})
        return AppHandle(self.name, job_id, attempt, native={"kind": "session", "session_id": d["id"], "session_name": name, "code": code, "log_offset": 0})

    def _find_by_name(self, client: LivyClient, kind: str, name: str) -> dict[str, Any] | None:
        items = client.list_sessions() if kind == "session" else client.list_batches()
        hits = sorted((i for i in items if i.get("name") == name), key=lambda i: int(i.get("id", 0)))
        if len(hits) > 1:
            log.warning("Livy holds %d %ss named %s; using the oldest (id %s)", len(hits), kind, name, hits[0].get("id"))
        return hits[0] if hits else None

    def _after_lost_create(self, client: LivyClient, job_id: str, attempt: int, kind: str, name: str, exc: SubmissionUncertain, extra: dict[str, Any]) -> AppHandle:
        """A create request may have been accepted. Adopt the object if the server shows it; otherwise return an explicitly *uncertain* handle (never a retry)."""
        try:
            found = self._find_by_name(client, kind, name)
        except ProviderError:
            found = None
        base = {"kind": kind, f"{kind}_name": name, **extra}
        if found is not None:
            log.warning("the response to the %s create for %s was lost; adopted Livy %s %s by name", kind, job_id, kind, found.get("id"))
            return AppHandle(self.name, job_id, attempt, application_id=found.get("appId"), native={**base, f"{kind}_id": found["id"], "reconciled": True})
        return AppHandle(self.name, job_id, attempt, native={**base, "submission": "uncertain", "uncertain_since": time.time(), "uncertain_reason": exc.message[:300]})

    def _reconcile_submission(self, client: LivyClient, handle: AppHandle, n: dict[str, Any]) -> AppStatus:
        """Resolve a create whose outcome was unknown by looking for the object under the attempt's name."""
        kind, name, now = n["kind"], n[f"{n['kind']}_name"], time.time()
        since = float(n.get("uncertain_since") or now)
        try:
            found = self._find_by_name(client, kind, name)
        except ProviderError as exc:
            n = {k: v for k, v in n.items() if k != "absent_since"}
            if now - since > self._unresolved_after:
                return AppStatus("failed", None, f"could not learn whether Livy created {kind} '{name}' (Livy unreachable for {int(now - since)}s): {exc.message}", native=n,
                                 code="submission_unresolved", confirmed_gone=False, reached_running=True)
            return AppStatus("pending", None, f"waiting to learn whether Livy accepted the {kind} request ({exc.message[:120]})", native=n, code="submission_uncertain")
        if found is not None:
            n2 = {k: v for k, v in n.items() if k not in ("submission", "uncertain_since", "absent_since", "uncertain_reason")}
            n2.update({f"{kind}_id": found["id"], "reconciled": True})
            return AppStatus("pending", found.get("appId"), f"adopted the Livy {kind} created by the request whose answer was lost", native=n2, code="submission_adopted")
        absent = float(n.get("absent_since") or now)
        n2 = {**n, "absent_since": absent}
        if now - absent >= self._quiesce:
            return AppStatus("failed", None, f"Livy never created {kind} '{name}' (absent from the server for {int(now - absent)}s); the request was lost before it was accepted",
                             native=n2, code="submission_not_created", confirmed_gone=True, reached_running=False)
        return AppStatus("pending", None, f"the Livy {kind} request got no answer; confirming that '{name}' does not exist before deciding", native=n2, code="submission_uncertain")

    def _submit_statement(self, client: LivyClient, s: dict[str, Any], n: dict[str, Any]) -> AppStatus:
        """Two-phase, replay-free statement submission (see the module docstring)."""
        sid, now, app = n["session_id"], time.time(), s.get("appId")
        phase = n.get("statement_state")
        if phase is None:  # phase 1: persist the intent before anything is sent
            return AppStatus("pending", app, "preparing to submit the statement", native={**n, "statement_state": "submitting", "statement_intent_at": now})
        try:
            existing = client.list_statements(sid)
        except ProviderError as exc:
            if phase == "uncertain" and now - float(n.get("uncertain_since") or now) > self._unresolved_after:
                return AppStatus("failed", app, f"could not learn whether Livy accepted the statement: {exc.message}", native=n, code="submission_unresolved",
                                 confirmed_gone=False, reached_running=True)
            raise  # reading failed: nothing was sent, the next poll looks again
        if existing:  # this session belongs to one attempt and gets one statement: it is ours
            stm = min(existing, key=lambda x: int(x.get("id", 0)))
            n2 = {k: v for k, v in n.items() if k not in ("code", "statement_state", "statement_intent_at", "uncertain_since", "absent_since", "uncertain_reason")}
            n2["statement_id"] = stm["id"]
            return AppStatus("running", app, "statement found on the Livy session" if phase == "uncertain" else "statement submitted", native=n2,
                             code="submission_adopted" if phase == "uncertain" else None)
        if existing is None and phase == "uncertain":
            return AppStatus("failed", app, "the Livy server cannot list statements, so the lost statement request cannot be reconciled", native=n,
                             code="submission_unresolved", confirmed_gone=False, reached_running=True)
        if phase == "uncertain":
            absent = float(n.get("absent_since") or now)
            n2 = {**n, "absent_since": absent}
            if now - absent < self._quiesce:
                return AppStatus("pending", app, "the statement request got no answer; confirming the session holds no statement before deciding", native=n2, code="submission_uncertain")
            if int(n.get("statement_replays", 0)) >= 1:
                return AppStatus("failed", app, "the statement request was lost twice", native=n2, code="submission_unresolved", confirmed_gone=False, reached_running=True)
            # the session was verified free of the statement for the whole quiesce window: the request did not arrive, sending it once more cannot duplicate it
            n3 = {k: v for k, v in n2.items() if k not in ("absent_since", "uncertain_since", "uncertain_reason")}
            return AppStatus("pending", app, "statement request verified absent; submitting it again once", native={**n3, "statement_state": "submitting", "statement_replays": 1},
                             code="submission_uncertain")
        try:
            stm = client.post_statement(sid, n["code"])
        except SubmissionUncertain as exc:
            return AppStatus("pending", app, f"statement request outcome unknown: {exc.message[:200]}", code="submission_uncertain",
                             native={**n, "statement_state": "uncertain", "uncertain_since": now, "uncertain_reason": exc.message[:300]})
        except ProviderError as exc:
            status = int((exc.details or {}).get("status") or 0)
            if 400 <= status < 500 and status != 429:  # a definite rejection: nothing to wait for
                return AppStatus("failed", app, f"Livy rejected the statement: {exc.message}", native=n, confirmed_gone=True, reached_running=False)
            raise
        n2 = {k: v for k, v in n.items() if k not in ("code", "statement_state", "statement_intent_at")}  # the code lives on the Livy server from now on (it may contain parameter values)
        n2["statement_id"] = stm["id"]
        return AppStatus("running", app, "statement submitted", native=n2)

    def status(self, handle: AppHandle) -> AppStatus:
        client = self._client()
        n = dict(handle.native)
        if n.get("submission") == "uncertain":
            return self._reconcile_submission(client, handle, n)
        if n.get("kind") == "batch":
            b = client.get_batch(n["batch_id"])
            if b is None:
                return AppStatus("failed", handle.application_id, "batch no longer exists on the Livy server", native=n)
            app_info = b.get("appInfo") or {}
            state = _BATCH_STATE.get(str(b.get("state")).lower(), "unknown")
            app_id = b.get("appId") or handle.application_id
            res = {"state": "succeeded", "exit_value": None, "failure": None, "spark": {"application_id": app_id, "ui_url": app_info.get("sparkUiUrl"), "app_name": n.get("batch_name")},
                   "artifacts": [], "metrics": {}} if state == "succeeded" else None
            return AppStatus(state, app_id, str(b.get("state")), native={**n, "ui": app_info.get("sparkUiUrl")}, result=res, exit_code=0 if state == "succeeded" else None)
        s = client.get_session(n["session_id"])
        if s is None:
            return AppStatus("failed", None, "session no longer exists on the Livy server", native=n)
        st = str(s.get("state", "")).lower()
        if st in ("dead", "error", "killed"):
            return AppStatus("cancelled" if st == "killed" else "failed", s.get("appId"), f"Livy session is {st}", native=n)
        if "statement_id" not in n:
            if st != "idle":
                return AppStatus("pending", s.get("appId"), f"waiting for the Livy session ({st})", native=n)
            return self._submit_statement(client, s, n)
        stm = client.get_statement(n["session_id"], n["statement_id"])
        if stm is None:
            return AppStatus("failed", s.get("appId"), "statement vanished", native=n)
        state = str(stm.get("state"))
        if state in ("waiting", "running", "cancelling"):
            return AppStatus("running", s.get("appId"), f"statement {state}", progress=stm.get("progress"), native=n)
        out = stm.get("output") or {}
        if state == "cancelled":
            return AppStatus("cancelled", s.get("appId"), "statement cancelled", native=n)
        text = _text(out)
        app_id = s.get("appId")
        spark = {"application_id": app_id, "ui_url": None, "app_name": n.get("session_name")}
        if state == "available" and out.get("status") == "ok":
            self._safe_delete_session(client, n["session_id"])
            res = {"state": "succeeded", "exit_value": text[:OUTPUT_LIMIT] if text else None, "failure": None, "spark": spark, "artifacts": [], "metrics": {}}
            return AppStatus("succeeded", app_id, "statement finished", native={**n, "output": text[:100000]}, exit_code=0, result=res)
        detail = f"{out.get('ename', 'error')}: {out.get('evalue', state)}"
        self._safe_delete_session(client, n["session_id"])
        tb = "\n".join(out.get("traceback") or [])[:20000]
        res = {"state": "failed", "exit_value": None, "failure": {"message": detail, "traceback": tb}, "spark": spark, "artifacts": [], "metrics": {}}
        return AppStatus("failed", app_id, detail, native={**n, "traceback": tb, "output": text[:20000]}, exit_code=1, result=res)

    @staticmethod
    def _safe_delete_session(client: LivyClient, sid: Any) -> None:
        try:
            client.delete_session(sid)
        except ProviderError as exc:
            log.warning("could not delete Livy session %s: %s", sid, exc.message)

    def logs(self, handle: AppHandle, offset: int = 0, limit: int = 200, *, cursor: Any = None) -> LogsPage:
        """``offset`` is a *line* number for Livy providers (Livy pages logs by line)."""
        client = self._client()
        n = handle.native
        if n.get("submission") == "uncertain":
            return LogsPage("", offset, True, source="livy")
        if n.get("kind") == "batch":
            d = client.batch_log(n["batch_id"], offset, max(1, min(limit, 1000)))
        else:
            d = client.session_log(n["session_id"], offset, max(1, min(limit, 1000)))
        lines = d.get("log") or []
        total = int(d.get("total", offset + len(lines)))
        nxt = offset + len(lines)
        text = "\n".join(lines) + ("\n" if lines else "")
        extra = n.get("output")
        return LogsPage(text, nxt, nxt >= total, source="livy" if not extra else "livy+output")

    def cancel(self, handle: AppHandle, *, force: bool = False, grace_seconds: int = 15) -> None:
        client = self._client()
        n = handle.native
        if n.get("submission") == "uncertain":  # the create may have been accepted: find it by name so it does not run on unattended
            try:
                found = self._find_by_name(client, n["kind"], n[f"{n['kind']}_name"])
            except ProviderError as exc:
                log.warning("cancel of %s: could not look for the uncertain Livy %s: %s", handle.job_id, n["kind"], exc.message)
                return
            if found is not None:
                (client.delete_batch if n["kind"] == "batch" else client.delete_session)(found["id"])
            return
        if n.get("kind") == "batch":
            client.delete_batch(n["batch_id"])
        elif n.get("session_id") is not None:
            client.delete_session(n["session_id"])

    def ui(self, handle: AppHandle) -> UiInfo:
        ui = handle.native.get("ui")
        return UiInfo(None, False, "the Spark UI of remote Livy applications is not reachable through the OLCF proxy" + (f"; provider UI: {ui}" if ui else ""))

    def reconcile(self) -> list[AppStatus]:
        client = self._client()
        out = []
        for s in client.list_sessions():
            parsed = parse_livy_name(str(s.get("name", "")))
            if parsed:
                out.append(AppStatus(_SESSION_STATE.get(str(s.get("state")).lower(), "unknown"), s.get("appId"), f"Livy session {s.get('id')}", job_id=parsed[0],
                                     native={"kind": "session", "session_id": s.get("id"), "session_name": s.get("name"), "attempt": parsed[1]}))
        for b in client.list_batches():
            parsed = parse_livy_name(str(b.get("name", "")))
            if parsed:
                out.append(AppStatus(_BATCH_STATE.get(str(b.get("state")).lower(), "unknown"), b.get("appId"), f"Livy batch {b.get('id')}", job_id=parsed[0],
                                     native={"kind": "batch", "batch_id": b.get("id"), "batch_name": b.get("name"), "attempt": parsed[1]}))
        return out

    def discover(self) -> Discovery:
        try:
            n_s, n_b = len(self._client().list_sessions()), len(self._client().list_batches())
            return Discovery(self.name, self.type, True, f"Livy reachable: {n_s} sessions, {n_b} batches", list(self.capabilities), "remote", live_verified=self.live_verified)
        except ProviderError as exc:
            return Discovery(self.name, self.type, False, exc.message, list(self.capabilities), "remote", live_verified=self.live_verified)
