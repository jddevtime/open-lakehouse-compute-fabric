# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Exception handlers that render the canonical error envelope."""

from __future__ import annotations

import logging

from fastapi import FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from pydantic import ValidationError as PydanticValidationError
from starlette.exceptions import HTTPException as StarletteHTTPException

from olcf.core.errors import OlcfError
from olcf.core.redaction import redact_obj, redact_text

log = logging.getLogger("olcf.api")


def _cid(request: Request) -> str | None:
    return getattr(request.state, "correlation_id", None)


def _envelope(code: str, message: str, request: Request, details=None, remediation=None) -> dict:
    body: dict = {"code": code, "message": message, "correlation_id": _cid(request)}
    if details:
        body["details"] = redact_obj(details)
    if remediation:
        body["remediation"] = remediation
    return {"error": body}


def install_error_handlers(app: FastAPI) -> None:
    @app.exception_handler(OlcfError)
    async def _olcf(request: Request, exc: OlcfError) -> JSONResponse:
        headers = {}
        if exc.status == 401:
            headers["WWW-Authenticate"] = "Bearer"
        if exc.status == 429 and exc.details.get("retry_after_seconds"):
            headers["Retry-After"] = str(exc.details["retry_after_seconds"])
        if exc.status >= 500:
            log.error("olcf error %s: %s", exc.code, exc.message)
        return JSONResponse(_envelope(exc.code, exc.message, request, exc.details, exc.remediation), status_code=exc.status, headers=headers)

    @app.exception_handler(RequestValidationError)
    async def _validation(request: Request, exc: RequestValidationError) -> JSONResponse:
        errs = [{"loc": [str(x) for x in e.get("loc", [])], "msg": e.get("msg"), "type": e.get("type")} for e in exc.errors()]
        return JSONResponse(_envelope("validation_failed", "request validation failed", request, {"errors": errs}), status_code=422)

    @app.exception_handler(PydanticValidationError)
    async def _model_validation(request: Request, exc: PydanticValidationError) -> JSONResponse:
        """A handler that validates a free-form body itself (``Model.model_validate(body)``) must answer 422, not 500; the submitted values are never echoed."""
        errs = [{"loc": [str(x) for x in e.get("loc", [])], "msg": e.get("msg"), "type": e.get("type")} for e in exc.errors(include_input=False, include_url=False)]
        return JSONResponse(_envelope("validation_failed", "request validation failed", request, {"errors": errs}), status_code=422)

    @app.exception_handler(StarletteHTTPException)
    async def _http(request: Request, exc: StarletteHTTPException) -> JSONResponse:
        code = {404: "not_found", 405: "method_not_allowed", 401: "unauthenticated", 403: "forbidden"}.get(exc.status_code, "http_error")
        return JSONResponse(_envelope(code, str(exc.detail), request), status_code=exc.status_code, headers=getattr(exc, "headers", None))

    @app.exception_handler(Exception)
    async def _unhandled(request: Request, exc: Exception) -> JSONResponse:
        log.exception("unhandled error on %s %s", request.method, request.url.path)
        return JSONResponse(_envelope("internal", redact_text(f"internal error ({type(exc).__name__})"), request), status_code=500)
