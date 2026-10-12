# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""FastAPI dependencies: context, authentication, ETags, idempotency, pagination."""

from __future__ import annotations

import hashlib
import json
from typing import Annotated, Any

from fastapi import Depends, Header, Query, Request
from sqlalchemy import select, update

from olcf.core.context import AppContext
from olcf.core.errors import Conflict, PasswordChangeRequired, PreconditionFailed, Unauthenticated
from olcf.core.timeutil import iso_in, now_iso
from olcf.db import schema as S
from olcf.db.util import fetch_one
from olcf.security.rbac import PASSWORD_CHANGE_SCOPE, Principal, apply_credential


def get_ctx(request: Request) -> AppContext:
    return request.app.state.ctx


Ctx = Annotated[AppContext, Depends(get_ctx)]


def _client_ip(request: Request, ctx: AppContext) -> str | None:
    if ctx.settings.server.trusted_proxies > 0:
        xff = request.headers.get("x-forwarded-for")
        if xff:
            parts = [p.strip() for p in xff.split(",")]
            idx = max(0, len(parts) - ctx.settings.server.trusted_proxies)
            return parts[idx]
    return request.client.host if request.client else None


def bearer_token(request: Request, *, allow_query: bool = False) -> str | None:
    auth = request.headers.get("authorization", "")
    if auth.lower().startswith("bearer "):
        return auth[7:].strip()
    if allow_query:
        return request.query_params.get("access_token")
    return None


def _bind_derived_credential(ctx: AppContext, principal: Principal, row: dict[str, Any], derived: dict[str, Any]) -> None:
    """Turn a job / kernel callback token into what it is allowed to be (review F01).

    The token's own scope (``job``) is only the *minimum* it may do. Its effective permissions are that scope intersected with the ceiling persisted when the work was
    submitted (so a restricted PAT yields a restricted job) and with the owner's role bindings as they are *now*, and it answers for exactly one workspace. It is refused
    when the user credential it was derived from was revoked, when its job moved on to another attempt or finished, or when its kernel session ended - so a revocation
    of the parent reaches work that is already running. Anything unclear fails closed.
    """
    apply_credential(principal, derived)
    if ctx.tokens.derived_origin_revoked(principal.origin_token_id, principal.origin_family):
        raise Unauthenticated("the credential this token was derived from was revoked", remediation="submit the job again with a valid credential")
    meta = row.get("meta") or {}
    from olcf.compute import schema as C

    with ctx.db.connect() as conn:
        if meta.get("job_id"):
            job = fetch_one(conn, select(C.jobs.c.workspace_id, C.jobs.c.state, C.jobs.c.attempt).where(C.jobs.c.id == meta["job_id"]))
            live = job is not None and job["state"] not in ("succeeded", "failed", "cancelled", "timed_out") and job["attempt"] == meta.get("attempt")
            where = job["workspace_id"] if job else None
        elif meta.get("session_id"):
            sess = fetch_one(conn, select(C.sessions.c.workspace_id, C.sessions.c.state).where(C.sessions.c.id == meta["session_id"]))
            live = sess is not None and sess["state"] != "dead"
            where = sess["workspace_id"] if sess else None
        else:
            live, where = False, None
    if not live or where != derived.get("workspace_id"):
        raise Unauthenticated("this job credential is no longer valid", remediation="a job credential lives only as long as its attempt (or kernel session)")


def authenticate(ctx: AppContext, token: str, *, ip: str | None = None, allow_password_change: bool = False) -> Principal:
    """Resolve a bearer token (local JWT, platform token or federated JWT) into a Principal.

    A local login session of an account that must choose a new password (``principals.must_change_password``) is refused with ``PasswordChangeRequired`` (403) for every
    caller - REST routes, streams, the Fabric adapter, the Spark UI proxy, MCP - unless ``allow_password_change`` (only ``POST /v1/identity/password`` passes it).
    """
    principal: Principal | None = None
    if token.startswith("olcf_"):
        row = ctx.tokens.verify_opaque(token, kinds=("pat", "worker"))
        with ctx.db.connect() as conn:
            prow = fetch_one(conn, select(S.principals).where(S.principals.c.id == row["principal_id"]))
        if not prow or not prow["is_active"]:
            raise Unauthenticated("principal disabled")
        principal = ctx.authz.principal_from_row(prow, token_scopes=list(row["scopes"] or ["*"]), token_id=row["id"])
        derived = (row.get("meta") or {}).get("derived")
        if derived is not None:
            _bind_derived_credential(ctx, principal, row, derived)
    elif ctx.tokens.looks_like_local_jwt(token):
        claims = ctx.tokens.verify_access_token(token)
        with ctx.db.connect() as conn:
            prow = fetch_one(conn, select(S.principals).where(S.principals.c.id == claims["sub"]))
        if not prow or not prow["is_active"]:
            raise Unauthenticated("principal disabled")
        principal = ctx.authz.principal_from_row(prow, token_scopes=list(claims.get("scp") or ["*"]), token_id=claims.get("jti"),
                                                 claims={"fam": claims["fam"]} if claims.get("fam") else None)
        if prow.get("must_change_password"):
            # decided from the account's CURRENT state, not from a claim: tokens issued while the flag was set (and tokens issued before an administrator
            # set it) can do nothing but change the password; the same token works normally the moment the password was changed
            if not allow_password_change:
                raise PasswordChangeRequired("you must choose a new password before you can use the platform",
                                             remediation="POST /v1/identity/password with {current_password, new_password}")
            principal.token_scopes = [PASSWORD_CHANGE_SCOPE]  # belt and braces: even if a route forgot the gate, an unknown scope grants nothing
            principal.claims = {**principal.claims, "must_change_password": True}
    else:
        fed = ctx.maybe_service("federation")
        if fed is not None:
            principal = fed.authenticate_bearer(token)
    if principal is None:
        raise Unauthenticated("invalid credentials")
    principal.ip = ip
    return principal


class StreamGuard:
    """Keeps a long-lived stream (SSE) honest: its credential is checked once when the stream opens, so without this a disabled account, a revoked or expired token or a
    withdrawn role would keep receiving data for as long as the connection lives.

    ``await guard.current()`` returns the freshly resolved principal (bindings included) or ``None`` when the stream must end; it re-authenticates at most every
    ``interval`` seconds and costs nothing in between. SECOPS addition (see tests/security/test_streams.py).
    """

    def __init__(self, ctx: AppContext, request: Request | None, principal: Principal, *, interval: float = 10.0, token: str | None = None) -> None:
        import time

        self._ctx, self._interval = ctx, interval
        self._token = token if request is None else bearer_token(request, allow_query=True)
        self._principal, self._at, self._clock = principal, time.monotonic(), time.monotonic

    async def current(self) -> Principal | None:
        import asyncio

        now = self._clock()
        if now - self._at < self._interval:
            return self._principal
        self._at = now
        if not self._token:
            return None
        try:
            fresh = await asyncio.to_thread(authenticate, self._ctx, self._token, ip=self._principal.ip)
        except (Unauthenticated, PasswordChangeRequired):
            return None
        fresh.correlation_id = self._principal.correlation_id
        self._principal = fresh
        return fresh


def _is_password_change(request: Request) -> bool:
    """``POST /v1/identity/password``: the one call a session that must change its password may make (``/identity/logout`` takes no credentials)."""
    return request.method == "POST" and request.url.path.rstrip("/").endswith("/v1/identity/password")


def get_principal(request: Request, ctx: Ctx) -> Principal:
    token = bearer_token(request)
    if not token:
        raise Unauthenticated("authentication required", remediation="send 'Authorization: Bearer <token>'")
    principal = authenticate(ctx, token, ip=_client_ip(request, ctx), allow_password_change=_is_password_change(request))
    principal.correlation_id = getattr(request.state, "correlation_id", None)
    return principal


def get_principal_allow_query(request: Request, ctx: Ctx) -> Principal:
    """For SSE/WebSocket endpoints where browsers cannot set headers: also accepts ``?access_token=``."""
    token = bearer_token(request, allow_query=True)
    if not token:
        raise Unauthenticated("authentication required")
    principal = authenticate(ctx, token, ip=_client_ip(request, ctx), allow_password_change=_is_password_change(request))
    principal.correlation_id = getattr(request.state, "correlation_id", None)
    return principal


Authed = Annotated[Principal, Depends(get_principal)]
AuthedStream = Annotated[Principal, Depends(get_principal_allow_query)]


def client_ip(request: Request, ctx: Ctx) -> str | None:
    return _client_ip(request, ctx)


# ----------------------------------------------------------------------------- ETag / If-Match
def etag_header(etag: str) -> str:
    return f'"{etag}"'


def check_if_match(if_match: str | None, current_etag: str, *, required: bool = False) -> None:
    if if_match is None:
        if required:
            raise PreconditionFailed("If-Match header required")
        return
    candidates = {c.strip().strip('"') for c in if_match.split(",")}
    if "*" in candidates or current_etag in candidates:
        return
    raise PreconditionFailed("resource was modified by someone else", details={"current_etag": current_etag})


IfMatch = Annotated[str | None, Header(alias="If-Match")]


# ----------------------------------------------------------------------------- idempotency
IdempotencyKey = Annotated[str | None, Header(alias="Idempotency-Key", max_length=191)]


class Idempotency:
    """Replay-safe mutating operations (spec 24).

    Usage::

        def create(..., idem: IdempotencyKey = None):
            with Idempotency(ctx, principal, "POST /v1/jobs", idem, body) as ik:
                if ik.replay is not None:
                    return ik.replay
                result = do_work()
                ik.save(201, result)
                return result
    """

    def __init__(self, ctx: AppContext, principal: Principal, endpoint: str, key: str | None, body: Any) -> None:
        self.ctx, self.principal, self.endpoint, self.key = ctx, principal, endpoint, key
        self.hash = hashlib.sha256(json.dumps(body, sort_keys=True, default=str).encode()).hexdigest()
        self.replay: Any = None
        self.status_code: int | None = None

    def __enter__(self) -> Idempotency:
        if not self.key:
            return self
        t = S.idempotency_keys.c
        with self.ctx.db.connect() as conn:
            row = fetch_one(conn, select(S.idempotency_keys).where(t.key == self.key, t.principal_id == self.principal.id, t.endpoint == self.endpoint))
        if row:
            if row["request_hash"] != self.hash:
                raise Conflict("Idempotency-Key was already used with a different request body")
            if row["response"] is not None:
                self.replay = row["response"]
                self.status_code = row["status_code"]
            else:
                raise Conflict("a request with this Idempotency-Key is still in progress")
            return self
        try:
            with self.ctx.db.begin() as conn:
                conn.execute(S.idempotency_keys.insert().values(
                    key=self.key, principal_id=self.principal.id, endpoint=self.endpoint, request_hash=self.hash,
                    created_at=now_iso(), expires_at=iso_in(86400)))
        except Exception as exc:  # noqa: BLE001 - concurrent duplicate
            raise Conflict("a request with this Idempotency-Key is still in progress") from exc
        return self

    def save(self, status_code: int, response: Any) -> None:
        if not self.key:
            return
        t = S.idempotency_keys.c
        with self.ctx.db.begin() as conn:
            conn.execute(update(S.idempotency_keys).where(t.key == self.key, t.principal_id == self.principal.id, t.endpoint == self.endpoint)
                         .values(status_code=status_code, response=json.loads(json.dumps(response, default=str))))

    def __exit__(self, exc_type, exc, tb) -> None:
        if exc_type is not None and self.key and self.replay is None:
            # failed before save(): release the key so the client can retry
            t = S.idempotency_keys.c
            try:
                with self.ctx.db.begin() as conn:
                    conn.execute(S.idempotency_keys.delete().where(t.key == self.key, t.principal_id == self.principal.id,
                                                                    t.endpoint == self.endpoint, t.response.is_(None)))
            except Exception:  # noqa: BLE001
                pass


# ----------------------------------------------------------------------------- pagination
class PageParams:
    """The ``limit`` and ``cursor`` query parameters shared by every paginated list endpoint."""
    def __init__(self, limit: int = Query(50, ge=1, le=500), cursor: str | None = Query(None)) -> None:
        self.limit = limit
        self.cursor = cursor


Pagination = Annotated[PageParams, Depends()]
