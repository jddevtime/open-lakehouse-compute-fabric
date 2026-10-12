# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""``http`` activity: call an HTTP(S) endpoint through the SSRF-guarded client.

* The destination is checked when the activity is validated *and* at connect time (every resolved address, every redirect hop, see
  :mod:`olcf.pipelines.ssrf`); private / loopback / link-local ranges are refused unless an administrator allow-listed them.
* Credentials never appear in the pipeline definition: ``auth`` references a secret id (needs ``secret.use`` in the workspace) that is resolved just in
  time and only used to build the request header.
* Response size and time are capped; response headers and bodies are redacted before they are stored or shown.
* **Retry safety.** A request that was never sent (DNS, connect failure or connect timeout, pool timeout, cancelled while connecting) can always be repeated. A request that
  *was* sent but not answered (read/write timeout, connection reset, the pipeline timeout aborting the call, a ``502``/``504`` from a gateway) may have been accepted: for
  the non-idempotent methods ``POST``, ``PUT``, ``PATCH`` and ``DELETE`` that outcome is ``timeout_uncertain`` / ``network_uncertain`` and the attempt is retried **only** when
  the activity is idempotent - ``policy.idempotent``, or an ``Idempotency-Key`` header (or ``idempotencyKey``) is sent, which makes repeating the request safe for the server.
  ``GET`` / ``HEAD`` / ``OPTIONS`` are always repeatable.
"""

from __future__ import annotations

import base64
import json
import time
from typing import Any
from urllib.parse import urlsplit

import httpx

from olcf.core.errors import Forbidden, NotFound, OlcfError
from olcf.core.redaction import redact_obj, redact_text
from olcf.observability import correlation
from olcf.pipelines import ssrf
from olcf.pipelines.activities.base import (
    Activity,
    ActivityCancelled,
    ActivityContext,
    ActivityError,
    ValidationIssue,
    obj,
    s_bool,
    s_int,
    s_obj,
    s_str,
)

METHODS = ("GET", "POST", "PUT", "PATCH", "DELETE", "HEAD", "OPTIONS")
#: methods that are safe to repeat whatever the outcome of the first request was
SAFE_METHODS = ("GET", "HEAD", "OPTIONS")
IDEMPOTENCY_HEADER = "idempotency-key"
#: transport errors that prove no request byte was sent (the connection was never established)
PRE_SEND_ERRORS = (httpx.ConnectTimeout, httpx.ConnectError, httpx.PoolTimeout, httpx.UnsupportedProtocol, httpx.ProxyError)
#: statuses that mean "not processed, try again" even for a non-idempotent request. A 5xx does not: a gateway can answer 502/503/504 after the upstream committed the request
NOT_PROCESSED_STATUSES = (408, 425, 429)
FORBIDDEN_HEADERS = {"host", "content-length", "transfer-encoding", "connection", "upgrade", "te", "trailer", "proxy-authorization", "proxy-connection"}
DEFAULT_MAX_BYTES = 1024 * 1024
HARD_MAX_BYTES = 16 * 1024 * 1024
MAX_REQUEST_BYTES = 4 * 1024 * 1024


def resolve_secret(actx: ActivityContext, secret_id: str) -> Any:
    """Resolve a workspace-visible secret for an activity of this run (authorised as the run's principal)."""
    actx.principal.require("secret.use", workspace_id=actx.workspace_id)
    sec = actx.ctx.secrets
    try:
        meta = sec.get_meta(secret_id)
    except NotFound:
        raise ActivityError(f"secret '{secret_id}' not found", category="configuration", retryable=False) from None
    if meta.get("scope_type") not in ("org", "workspace", None, ""):
        raise Forbidden(f"secret '{secret_id}' cannot be used by pipelines")
    if meta.get("scope_type") == "workspace" and meta.get("scope_id") != actx.workspace_id:
        raise Forbidden(f"secret '{secret_id}' belongs to another workspace")
    return sec.resolve(secret_id)


def _secret_text(value: Any, *keys: str) -> str:
    if isinstance(value, dict):
        for k in keys:
            if isinstance(value.get(k), str):
                return value[k]
        raise ActivityError(f"the referenced secret has none of the fields {', '.join(keys)}", category="configuration", retryable=False)
    return str(value)


class HttpActivity(Activity):
    """Send one HTTP request and expose status, (redacted) headers and the parsed body to downstream expressions."""

    type = "http"
    title = "HTTP request"
    description = "Call an HTTP(S) endpoint (SSRF-guarded). Output: statusCode, headers, body (JSON parsed when the response is JSON), elapsedMs."
    mode = "blocking"
    default_timeout_seconds = 900
    properties_schema = obj({
        "url": s_str(minLength=1), "method": {"enum": list(METHODS)}, "headers": s_obj(), "queryParams": s_obj(), "body": {}, "contentType": s_str(),
        "auth": s_obj(), "expectStatus": {"anyOf": [{"type": "array", "items": {"type": "integer", "minimum": 100, "maximum": 599}}, {"type": "string"}]},
        "timeoutSeconds": s_int(1, 600), "followRedirects": s_bool(), "maxResponseBytes": s_int(1, HARD_MAX_BYTES), "verifyTls": s_bool(),
        "idempotencyKey": {"anyOf": [{"type": "boolean"}, {"type": "string", "minLength": 1, "maxLength": 255}, {"$ref": "#/$defs/expression"}],
                           "description": "Send an Idempotency-Key header so the request may be repeated safely: true generates a key that stays the same for every attempt of this activity run, a string uses that value."},
    }, ["url"])
    sensitive_props = ("headers", "auth", "body")

    def validate(self, props: dict[str, Any], path: str) -> list[ValidationIssue]:
        out: list[ValidationIssue] = []
        url = props.get("url")
        if isinstance(url, str) and not url.startswith("@") and "@{" not in url:
            try:
                parts = urlsplit(url)
                if parts.scheme.lower() not in ("http", "https") or not parts.hostname:
                    out.append(ValidationIssue(f"{path}.url", f"url must be an absolute http(s) URL but got '{url[:60]}'", "bad_url"))
                elif parts.username or parts.password:
                    out.append(ValidationIssue(f"{path}.url", "credentials in the url are not allowed; use auth with a secret reference", "credentials_in_url"))
            except ValueError as exc:
                out.append(ValidationIssue(f"{path}.url", f"invalid url: {exc}", "bad_url"))
        hdrs = props.get("headers")
        if isinstance(hdrs, dict):
            for k in hdrs:
                if str(k).lower() in FORBIDDEN_HEADERS:
                    out.append(ValidationIssue(f"{path}.headers.{k}", f"header '{k}' is managed by the platform and cannot be set", "forbidden_header"))
        auth = props.get("auth")
        if isinstance(auth, dict) and not auth.get("type", "").startswith("@"):
            if auth.get("type") not in ("none", "bearer", "basic", "header"):
                out.append(ValidationIssue(f"{path}.auth.type", "auth.type must be one of none, bearer, basic, header", "enum"))
            elif auth.get("type") != "none" and not auth.get("secret"):
                out.append(ValidationIssue(f"{path}.auth.secret", f"auth type '{auth.get('type')}' needs 'secret' (a secret id); secrets are never written into definitions", "required"))
            if auth.get("type") == "header" and not auth.get("name"):
                out.append(ValidationIssue(f"{path}.auth.name", "auth type 'header' needs the header 'name'", "required"))
        return out

    def idempotent_for(self, props: dict[str, Any]) -> bool:
        """Repeatable: a safe method, or a request that carries an Idempotency-Key (then the server de-duplicates the repeat)."""
        if str(props.get("method") or "GET").upper() in SAFE_METHODS:
            return True
        return self._has_idempotency_key(props)

    @staticmethod
    def _has_idempotency_key(props: dict[str, Any]) -> bool:
        if any(str(k).lower() == IDEMPOTENCY_HEADER and v not in (None, "") for k, v in (props.get("headers") or {}).items()):
            return True
        key = props.get("idempotencyKey")
        return bool(key) and key is not False

    # -- request construction
    def _headers(self, actx: ActivityContext, used: list[str]) -> dict[str, str]:
        """Request headers; every resolved credential value is appended to ``used`` so it can be scrubbed from whatever the server echoes back."""
        p = actx.props
        headers: dict[str, str] = {}
        for k, v in (p.get("headers") or {}).items():
            if str(k).lower() in FORBIDDEN_HEADERS:
                raise ActivityError(f"header '{k}' cannot be set", category="configuration", retryable=False)
            headers[str(k)] = v if isinstance(v, str) else json.dumps(v)
        key = p.get("idempotencyKey")
        if key and not any(str(k).lower() == IDEMPOTENCY_HEADER for k in headers):
            # stable across the attempts of one activity run: a retry repeats the very same key, which is what lets the server drop the duplicate
            headers["Idempotency-Key"] = key if isinstance(key, str) else f"olcf-{actx.activity_run_id}"
        auth = p.get("auth") or {}
        typ = auth.get("type") or "none"
        if typ != "none":
            try:
                secret = resolve_secret(actx, str(auth.get("secret")))
            except OlcfError as exc:
                raise ActivityError(exc.message, category=exc.code, retryable=False) from None
            if typ == "bearer":
                token = _secret_text(secret, "token", "access_token")
                headers["Authorization"] = "Bearer " + token
                used.append(token)
            elif typ == "basic":
                if isinstance(secret, dict):
                    user, pw = str(secret.get("username", auth.get("username", ""))), str(secret.get("password", ""))
                else:
                    user, pw = str(auth.get("username", "")), str(secret)
                headers["Authorization"] = "Basic " + base64.b64encode(f"{user}:{pw}".encode()).decode()
                used += [pw, headers["Authorization"][6:]]
            elif typ == "header":
                value = _secret_text(secret, "value", "token", "key")
                headers[str(auth["name"])] = value
                used.append(value)
        return headers

    def execute(self, actx: ActivityContext) -> dict[str, Any]:
        p = actx.props
        method = str(p.get("method") or "GET").upper()
        if method not in METHODS:
            raise ActivityError(f"unsupported HTTP method '{method}'", category="configuration", retryable=False)
        url = str(p.get("url") or "")
        policy = ssrf.policy_from_ctx(actx.ctx)
        try:
            ssrf.validate_url(url, policy)
        except OlcfError as exc:
            raise ActivityError(exc.message, category="ssrf" if exc.status == 403 else "configuration", retryable=False,
                                details={"remediation": exc.remediation} if exc.remediation else {}) from None
        credentials: list[str] = []
        headers = self._headers(actx, credentials)
        body = p.get("body")
        content: bytes | None = None
        if body is not None and method not in ("GET", "HEAD"):
            if isinstance(body, (dict, list)):
                content = json.dumps(body).encode()
                headers.setdefault("Content-Type", str(p.get("contentType") or "application/json"))
            else:
                content = (body if isinstance(body, str) else json.dumps(body)).encode()
                headers.setdefault("Content-Type", str(p.get("contentType") or "text/plain; charset=utf-8"))
            if len(content) > MAX_REQUEST_BYTES:
                raise ActivityError(f"request body is larger than {MAX_REQUEST_BYTES // 1024 // 1024} MiB", category="limit", retryable=False)
        headers.setdefault("User-Agent", "olcf-pipelines/1")
        for k, v in correlation.outgoing_headers().items():
            headers.setdefault(k, v)
        params = {str(k): (v if isinstance(v, str) else json.dumps(v)) for k, v in (p.get("queryParams") or {}).items()} or None
        timeout = float(p.get("timeoutSeconds") or 30)
        max_bytes = min(int(p.get("maxResponseBytes") or DEFAULT_MAX_BYTES), HARD_MAX_BYTES)
        verify = False if p.get("verifyTls") is False else True
        expect = p.get("expectStatus")
        tracker = ssrf.ConnectionTracker()
        client = ssrf.guarded_client(policy, timeout=timeout, follow_redirects=bool(p.get("followRedirects", False)), verify=verify, tracker=tracker)
        disarm = actx.on_cancel(tracker.abort)  # a timeout / cancel must not wait for the server: shut the sockets down so the blocked read returns now
        safe = self.idempotent_for(p) or bool((actx.definition.get("policy") or {}).get("idempotent"))
        t0 = time.perf_counter()
        try:
            with client, client.stream(method, url, headers=headers, params=params, content=content) as resp:
                buf = bytearray()
                truncated = False
                for chunk in resp.iter_bytes():
                    actx.check_cancelled()
                    room = max_bytes - len(buf)
                    if len(chunk) > room:
                        buf += chunk[:room]
                        truncated = True
                        break
                    buf += chunk
                status, reason, rheaders, ctype = resp.status_code, resp.reason_phrase, dict(resp.headers), resp.headers.get("content-type", "")
        except OlcfError as exc:  # SSRF guard (redirect hop or rebinding)
            raise ActivityError(exc.message, category="ssrf", retryable=False) from None
        except (httpx.HTTPError, ActivityCancelled) as exc:
            raise self._transport_failure(exc, method, url, timeout, tracker, safe) from None
        finally:
            disarm()
        elapsed = int((time.perf_counter() - t0) * 1000)
        text = buf.decode("utf-8", errors="replace")
        parsed: Any = text
        if "json" in ctype.lower() and not truncated:
            try:
                parsed = json.loads(text)
            except ValueError:
                parsed = text
        out = {"statusCode": status, "reasonPhrase": reason, "headers": scrub(redact_obj({k.lower(): v for k, v in rheaders.items()}), credentials), "body": scrub(parsed, credentials),
               "bodyTruncated": truncated, "elapsedMs": elapsed, "url": _safe_url(url)}
        if not _status_ok(status, expect):
            # a 5xx answer does not prove the request was not executed (the server may fail after committing, a gateway after forwarding): repeat it only when that is safe
            retry = status in (408, 425, 429) or status >= 500
            if not safe and status not in NOT_PROCESSED_STATUSES:
                retry = False
            raise ActivityError(f"{method} {_safe_url(url)} answered HTTP {status} {reason}", category="http", retryable=retry,
                                details={"statusCode": status, "body": scrub(redact_text(text[:500]), credentials),
                                         **({"retrySuppressed": "the request is not idempotent and the status does not prove it was not processed; add an Idempotency-Key or set policy.idempotent"}
                                            if (status >= 500 and not retry) else {})})
        return out

    @staticmethod
    def _transport_failure(exc: BaseException, method: str, url: str, timeout: float, tracker: ssrf.ConnectionTracker, safe: bool) -> ActivityError | ActivityCancelled:
        """Classify a failed or aborted call: never sent (always repeatable) versus sent-but-unanswered (repeatable only when ``safe``)."""
        target = f"{method} {_safe_url(url)}"
        sent = tracker.connected and not isinstance(exc, PRE_SEND_ERRORS)
        if tracker.aborted or isinstance(exc, ActivityCancelled):  # the pipeline timed out or was cancelled (we tore the connection down, or stopped reading the answer)
            if not sent:
                return ActivityCancelled(f"{target} was cancelled before it was sent")
            return ActivityCancelled(f"{target} was cancelled after it was sent; the server may still have processed it", uncertain=not safe,
                                     details={"method": method, "requestSent": True})
        if not sent:
            kind = "timeout" if isinstance(exc, httpx.TimeoutException) else "network"
            msg = f"{target} timed out after {timeout:g}s while connecting" if kind == "timeout" else f"{target} could not connect: {type(exc).__name__}: {redact_text(str(exc))}"
            return ActivityError(msg, category=kind, retryable=True, details={"requestSent": False})
        if isinstance(exc, httpx.TimeoutException):
            msg, cat = f"{target} timed out after {timeout:g}s waiting for the server", "timeout" if safe else "timeout_uncertain"
        else:
            msg, cat = f"{target} failed after the request was sent: {type(exc).__name__}: {redact_text(str(exc))}", "network" if safe else "network_uncertain"
        details: dict[str, Any] = {"requestSent": True, "method": method}
        if not safe:
            details["outcome"] = "unknown"
            msg += "; the server may have processed it, so it is not repeated automatically (send an Idempotency-Key or set policy.idempotent to allow retries)"
        return ActivityError(msg, category=cat, retryable=safe, details=details)


def scrub(value: Any, secrets: list[str]) -> Any:
    """Replace every occurrence of a credential we sent in ``value`` (a server may echo it back in a body or header) with ``***``."""
    needles = sorted({x for x in secrets if isinstance(x, str) and len(x) >= 6}, key=len, reverse=True)
    if not needles:
        return value
    if isinstance(value, str):
        for n in needles:
            value = value.replace(n, "***")
        return value
    if isinstance(value, dict):
        return {k: scrub(v, needles) for k, v in value.items()}
    if isinstance(value, list):
        return [scrub(v, needles) for v in value]
    return value


def _safe_url(url: str) -> str:
    try:
        parts = urlsplit(url)
        return f"{parts.scheme}://{parts.hostname}{(':' + str(parts.port)) if parts.port else ''}{parts.path}"
    except ValueError:
        return "<invalid url>"


def _status_ok(status: int, expect: Any) -> bool:
    if expect is None or expect == "":
        return 200 <= status < 300
    if isinstance(expect, str):
        e = expect.strip().lower()
        if e.endswith("xx") and e[:1].isdigit():
            return status // 100 == int(e[0])
        return str(status) == e
    if isinstance(expect, list):
        return status in [int(x) for x in expect]
    return 200 <= status < 300


HTTP_ACTIVITIES: list[type[Activity]] = [HttpActivity]
