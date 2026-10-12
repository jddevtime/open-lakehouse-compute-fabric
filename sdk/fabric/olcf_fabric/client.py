# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""The control-plane client: authenticated calls to ``/v1/integrations/fabric`` with one token refresh on ``401`` and the retry policy of :mod:`olcf_fabric._http`."""

from __future__ import annotations

from typing import Any

from ._http import HttpClient
from .auth import TokenProvider
from .config import Settings, current
from .errors import AuthenticationError, ServerError

BASE = "/v1/integrations/fabric"


class Client:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings.require()
        self.http = HttpClient(settings.endpoint, timeout=settings.timeout, ca_bundle=settings.ca_bundle, allow_insecure_http=settings.allow_insecure_http,
                               user_agent="olcf-fabric-sdk")
        self.auth = TokenProvider(settings)

    def call(self, method: str, path: str, body: Any = None, *, retry: bool = True, timeout: float | None = None) -> Any:
        refreshed = False
        while True:
            token = self.auth.token(force=refreshed)
            try:
                return self.http.request(method, BASE + path, token=token, body=body, retry=retry, timeout=timeout).json()
            except ServerError as exc:
                if exc.status == 401 and not refreshed:
                    refreshed = True
                    continue
                if exc.status == 401:
                    raise AuthenticationError(f"the control plane rejected the credential: {exc.message}", remediation=exc.remediation or
                                              "check that the token is valid for this control plane (platform token, or an Entra token for its API audience) and has not expired") from None
                raise

    # ------------------------------------------------------------------ endpoints
    def challenge(self) -> dict[str, Any]:
        return self.call("POST", "/challenge", {})

    def validate(self, body: dict[str, Any]) -> dict[str, Any]:
        return self.call("POST", "/validate", body)

    def submit(self, signed: dict[str, Any]) -> dict[str, Any]:
        return self.call("POST", "/submit", signed)

    def handshake(self, body: dict[str, Any]) -> dict[str, Any]:
        return self.call("POST", "/handshake", body)

    def submission(self, submission_id: str) -> dict[str, Any]:
        return self.call("GET", f"/submissions/{submission_id}")

    def logs(self, submission_id: str, offset: int = 0, limit: int = 65536) -> dict[str, Any]:
        return self.call("GET", f"/submissions/{submission_id}/logs?offset={int(offset)}&limit={int(limit)}")

    def output(self, submission_id: str) -> dict[str, Any]:
        return self.call("GET", f"/submissions/{submission_id}/output")

    def cancel(self, submission_id: str) -> dict[str, Any]:
        return self.call("POST", f"/submissions/{submission_id}/cancel", {})


_CLIENT: Client | None = None


def get_client() -> Client:
    global _CLIENT  # noqa: PLW0603
    if _CLIENT is None:
        _CLIENT = Client(current())
    return _CLIENT


def reset() -> None:
    global _CLIENT  # noqa: PLW0603
    _CLIENT = None
