# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Pull mode: fetch a notebook definition from the Fabric REST API with the broker identity.

Implements the documented item-definition flow (Fabric "Items - Get Notebook Definition"):

1. ``POST {api}/v1/workspaces/{workspaceId}/notebooks/{notebookId}/getDefinition?format=ipynb|fabricGitSource``
2. ``200`` -> the definition is in the body. ``202`` -> long-running operation: ``Location`` (operation URL) and ``Retry-After`` headers; poll
   ``GET <Location>`` until ``status`` is ``Succeeded`` (or ``Failed``), then ``GET <Location>/result`` (or the ``Location`` the final poll returned).
3. ``definition.parts[]`` carries ``{path, payload, payloadType: "InlineBase64"}``; ``notebook-content.ipynb|py|sql|scala|r`` is the notebook and ``.platform`` the
   item metadata (``metadata.displayName`` ...). Payloads are base64-decoded here.

Robustness: ``429`` honours ``Retry-After`` (capped), transient ``5xx`` are retried with backoff, one ``401`` triggers a token refresh, every error is translated into
:class:`FabricApiError` with the Fabric ``errorCode`` / ``requestId`` and a remediation. The flow is exercised against a local Fabric test server in the tests (see
``docs/guides/fabric-assumptions.md``).
"""

from __future__ import annotations

import base64
import json
import time
import zlib
from collections.abc import Callable, Iterable, Iterator
from dataclasses import dataclass, field
from typing import Any
from urllib.parse import quote, urlsplit

import httpx

from olcf.fabric.entra import EntraTokenError, TokenBroker
from olcf.fabric.http import Network, make_client

FABRIC_SCOPE = "https://api.fabric.microsoft.com/.default"
#: Power BI REST API audience (admin / metadata scanner calls of OLCF Intelligence)
POWERBI_SCOPE = "https://analysis.windows.net/powerbi/api/.default"
#: Azure Storage audience (OneLake DFS endpoint)
STORAGE_SCOPE = "https://storage.azure.com/.default"
ONELAKE_API_VERSION = "2023-11-03"
FORMATS = {"ipynb": "ipynb", "fabric_git_source": "fabricGitSource", "fabricGitSource": "fabricGitSource"}
MAX_RETRY_AFTER = 60.0
#: Largest JSON body (decoded bytes) a call accepts before it gives up; large enough for a 100-workspace metadata-scanner result, small enough to protect the control plane's memory.
MAX_JSON_BYTES = 256 * 1024 * 1024
#: Largest directory listing page accepted.
MAX_LISTING_BYTES = 32 * 1024 * 1024
#: An error body is only read for a message; never more than this.
MAX_ERROR_BODY_BYTES = 64 * 1024


class FabricApiError(Exception):
    """An error from the Fabric REST API (or the path to it)."""

    def __init__(self, message: str, *, status: int | None = None, error_code: str | None = None, request_id: str | None = None, remediation: str | None = None,
                 retry_after: float | None = None) -> None:
        super().__init__(message)
        self.status, self.error_code, self.request_id, self.remediation = status, error_code, request_id, remediation
        #: seconds the service asked the caller to wait (``Retry-After`` of a 429/503), when it sent one
        self.retry_after = retry_after


class IncompleteInventory(FabricApiError):
    """A paged listing could not be read to its end: the page limit was reached while the service still offered a continuation token (``reason`` ``max_pages``), or the service handed
    out the same token twice (``repeated_token``). What was received is in :attr:`partial` - useful, but **not** the whole inventory, so a caller must never treat it as complete
    (in particular it must not conclude that anything missing has disappeared)."""

    def __init__(self, message: str, *, reason: str, pages: int, label: str = "", partial: list[Any] | None = None) -> None:
        super().__init__(message, remediation="the listing was cut off; nothing missing from it may be treated as removed. Investigate the service, or raise the page limit if the inventory is genuinely this large")
        self.reason, self.pages, self.label = reason, pages, label
        #: the items received before the listing stopped
        self.partial: list[Any] = partial if partial is not None else []


@dataclass
class NotebookDefinition:
    """Decoded ``getDefinition`` result."""

    format: str  # canonical OLCF format: ipynb | fabric_git_source
    content: str
    content_path: str
    platform: dict[str, Any] | None = None
    parts: list[str] = field(default_factory=list)

    @property
    def display_name(self) -> str | None:
        return ((self.platform or {}).get("metadata") or {}).get("displayName")


@dataclass
class ItemDefinition:
    """Decoded ``getDefinition`` result of any Fabric item: ``parts`` maps part path to its text, ``platform`` is the parsed ``.platform`` part (item metadata) when present."""

    parts: dict[str, str]
    platform: dict[str, Any] | None = None
    binary_parts: int = 0


_HINTS = {
    401: "The token was rejected. Check that the broker service principal's token audience is the Fabric API and that its secret/certificate is valid.",
    403: ("The broker service principal may not call this Fabric API. In the Fabric admin portal enable 'Service principals can use Fabric APIs' for a security group that contains "
          "it, and add it to the workspace with at least Contributor (read/write on the notebook is required by getDefinition)."),
    404: "The workspace or notebook was not found (or the broker service principal cannot see it). Check the ids and the workspace membership of the service principal.",
}


def _path_of(url: str) -> str:
    """The path of a URL for messages (never the query string, which can carry continuation tokens)."""
    return urlsplit(url).path or url


def _error_from(resp: httpx.Response) -> FabricApiError:
    try:
        doc = resp.json()
    except ValueError:
        doc = {}
    code = doc.get("errorCode") or (doc.get("error") or {}).get("code") if isinstance(doc, dict) else None
    msg = (doc.get("message") if isinstance(doc, dict) else None) or resp.text[:200] or f"HTTP {resp.status_code}"
    try:
        retry_after = float(resp.headers["Retry-After"]) if resp.headers.get("Retry-After") is not None else None
    except ValueError:
        retry_after = None
    return FabricApiError(f"Fabric API returned {resp.status_code}: {str(msg)[:300]}", status=resp.status_code, error_code=code,
                          request_id=resp.headers.get("requestid") or resp.headers.get("x-ms-request-id") or (doc.get("requestId") if isinstance(doc, dict) else None),
                          remediation=_HINTS.get(resp.status_code), retry_after=retry_after)


class FabricRestClient:
    """Minimal Fabric REST client for the calls the adapter needs.

    Every response body is read as a bounded stream (:meth:`_request`): it stops at the limit (plus at most one chunk), rejects an excessive ``Content-Length`` before reading but does not
    trust it, bounds decompressed size, and always closes the connection. Every paged listing raises :class:`IncompleteInventory` instead of silently returning a prefix."""

    #: default page limits (class attributes so a test or a deployment can lower them)
    DEFAULT_MAX_PAGES = 2000
    DEFAULT_MAX_ONELAKE_PAGES = 200

    def __init__(self, broker: TokenBroker, network: Network | None = None, *, sleep: Callable[[float], None] = time.sleep, monotonic: Callable[[], float] = time.monotonic,
                 client_factory: Callable[[], httpx.Client] | None = None, max_attempts: int = 5) -> None:
        self.broker = broker
        self.network = network or broker.network
        self._sleep, self._monotonic = sleep, monotonic
        self._client_factory = client_factory or (lambda: make_client(self.network))
        self.max_attempts = max_attempts
        #: ``None`` (default, unchanged behaviour): a 429 is retried after ``min(Retry-After, 60 s)``. A number: a 429 whose ``Retry-After`` is longer than this many seconds
        #: is *not* slept on but raised at once (``FabricApiError.retry_after`` carries the wait) so a scanner can decide whether to wait out an hourly quota.
        self.give_up_retry_after: float | None = None
        #: called before every page and every body chunk; a scan sets it to its cancellation check (which raises), so a long listing or download stops promptly
        self.cancel_check: Callable[[], None] | None = None
        #: decoded size limit of any JSON body (see :data:`MAX_JSON_BYTES`)
        self.max_json_bytes = MAX_JSON_BYTES

    # ------------------------------------------------------------------ plumbing
    def _token(self, refresh: bool = False, scope: str = FABRIC_SCOPE) -> str:
        if refresh:
            self.broker._cache.pop(scope, None)  # noqa: SLF001 - forget the cached token after a 401
        try:
            return self.broker.get_token(scope)
        except EntraTokenError as exc:
            audience = "the Fabric API" if scope == FABRIC_SCOPE else scope.removesuffix("/.default")
            raise FabricApiError(f"cannot acquire a token for {audience}: {exc}", remediation=exc.remediation) from exc

    def _check(self) -> None:
        if self.cancel_check is not None:
            self.cancel_check()

    def _request(self, method: str, url: str, *, json_body: Any = None, accept_statuses: tuple[int, ...] = (200,), scope: str = FABRIC_SCOPE,
                 headers: dict[str, str] | None = None, max_body: int | None = None) -> httpx.Response:
        """One HTTP exchange with retries; the body is read as a bounded stream of at most ``max_body`` decoded bytes (default :attr:`max_json_bytes`). Raises :class:`FabricApiError`."""
        limit = self.max_json_bytes if max_body is None else max_body
        refreshed = False
        attempt = 0
        delay = 1.0
        while True:
            attempt += 1
            self._check()
            try:
                with self._client_factory() as client, client.stream(method, url, json=json_body, headers={"Authorization": f"Bearer {self._token(scope=scope)}", "Accept": "application/json",
                                                                                                           "Accept-Encoding": "identity", **(headers or {})}) as streamed:
                    resp = self._read_bounded(streamed, accept_statuses, limit, url)
            except httpx.HTTPError as exc:
                if attempt < self.max_attempts:
                    self._sleep(min(delay, 10.0))
                    delay *= 2
                    continue
                raise FabricApiError(f"cannot reach the Fabric API: {type(exc).__name__}", remediation="check DNS, proxy and CA settings (POST /v1/integrations/fabric/test)") from None
            if resp.status_code in accept_statuses:
                return resp
            if resp.status_code == 401 and not refreshed:
                refreshed = True
                self._token(refresh=True, scope=scope)
                continue
            if resp.status_code == 429 and self.give_up_retry_after is not None and (_err := _error_from(resp)).retry_after is not None and _err.retry_after > self.give_up_retry_after:
                raise _err
            if resp.status_code == 429 and attempt < self.max_attempts:
                self._sleep(self._retry_after(resp, default=delay))
                delay *= 2
                continue
            if resp.status_code >= 500 and attempt < self.max_attempts:
                self._sleep(min(delay, 10.0))
                delay *= 2
                continue
            raise _error_from(resp)

    def _read_bounded(self, streamed: httpx.Response, accept: tuple[int, ...], limit: int, url: str) -> httpx.Response:
        """Read the body of ``streamed`` without ever holding more than ``limit`` decoded bytes (plus one chunk).

        * a declared ``Content-Length`` above the limit is refused before any body byte is read - but the header is **not** trusted: the bytes actually received are counted too;
        * compression is decoded here with a hard output bound (``gzip`` / ``deflate``; the request asks for ``identity``, so a compressed answer is unusual), other encodings are refused;
        * an error status only has its first :data:`MAX_ERROR_BODY_BYTES` read (it is used for a message);
        * the caller's ``with`` closes the stream, also when this raises.
        """
        ok = streamed.status_code in accept
        cap = limit if ok else MAX_ERROR_BODY_BYTES
        declared = streamed.headers.get("Content-Length")
        if ok and declared is not None:
            try:
                if int(declared) > cap:
                    raise FabricApiError(f"the response body is larger than {cap} bytes (it declares {int(declared)}); refusing to read it ({_path_of(url)})")
            except ValueError:
                pass
        encoding = (streamed.headers.get("Content-Encoding") or "identity").strip().lower()
        decoder: Any = None
        if encoding in ("gzip", "x-gzip"):
            decoder = zlib.decompressobj(16 + zlib.MAX_WBITS)
        elif encoding == "deflate":
            decoder = zlib.decompressobj()
        elif encoding not in ("identity", ""):
            if ok:
                raise FabricApiError(f"the response uses the unsupported content encoding '{encoding}'; refusing to read it ({_path_of(url)})")
            decoder = None
        body = bytearray()
        # (a response a test transport built from bytes is already read - its content is the one chunk)
        for chunk in (iter([streamed.content]) if streamed.is_stream_consumed else streamed.iter_raw()):
            self._check()
            if decoder is None:
                body += chunk
            else:
                data = chunk
                while data and len(body) <= cap:
                    body += decoder.decompress(data, cap - len(body) + 1)          # hard bound on the decompressed output of this step
                    data = decoder.unconsumed_tail
            if len(body) > cap:
                if ok:
                    raise FabricApiError(f"the response body is larger than {cap} bytes{' once decompressed' if decoder is not None else ''}; refusing to read more ({_path_of(url)})")
                del body[cap:]
                break
        headers = [(k, v) for k, v in streamed.headers.multi_items() if k.lower() not in ("content-encoding", "content-length", "transfer-encoding")]
        return httpx.Response(streamed.status_code, headers=headers, content=bytes(body), request=streamed.request)

    @staticmethod
    def _retry_after(resp: httpx.Response, default: float = 2.0) -> float:
        raw = resp.headers.get("Retry-After")
        try:
            return max(0.0, min(float(raw), MAX_RETRY_AFTER)) if raw is not None else min(default, MAX_RETRY_AFTER)
        except ValueError:
            return min(default, MAX_RETRY_AFTER)

    def _same_host(self, url: str) -> str:
        """Operation URLs come from a response header: only follow them on the Fabric API host (never send the token elsewhere)."""
        base = urlsplit(self.network.fabric_api)
        loc = urlsplit(url)
        if not loc.scheme:
            return self.network.fabric_api + url
        if (loc.scheme, loc.netloc) != (base.scheme, base.netloc):
            raise FabricApiError(f"the operation URL points to another host ({loc.netloc}); refusing to send the Fabric token there")
        return url

    # ------------------------------------------------------------------ calls
    def list_workspaces(self) -> list[dict[str, Any]]:
        """Workspaces visible to the identity (diagnostics). Raises :class:`IncompleteInventory` (with ``partial``) instead of returning a silent prefix when 20 pages are not enough."""
        return self._collect("workspaces", self._walk("workspaces", f"{self.network.fabric_api}/v1/workspaces", lambda t: f"{self.network.fabric_api}/v1/workspaces?continuationToken={quote(t)}",
                                                      lambda d: d.get("value", []), 20))

    def get_definition(self, workspace_id: str, notebook_id: str, fmt: str = "ipynb", *, timeout_seconds: float = 120.0) -> NotebookDefinition:
        """Fetch and decode a notebook definition (``fmt``: ``ipynb`` or ``fabric_git_source``)."""
        api_fmt = FORMATS.get(fmt)
        if api_fmt is None:
            raise FabricApiError(f"format '{fmt}' is not supported (use ipynb or fabric_git_source)")
        url = f"{self.network.fabric_api}/v1/workspaces/{quote(workspace_id)}/notebooks/{quote(notebook_id)}/getDefinition?format={api_fmt}"
        resp = self._request("POST", url, accept_statuses=(200, 202))
        if resp.status_code == 202:
            doc = self._await_operation(resp, timeout_seconds)
        else:
            doc = resp.json()
        return self.decode_definition(doc, "ipynb" if api_fmt == "ipynb" else "fabric_git_source")

    def _await_operation(self, first: httpx.Response, timeout_seconds: float) -> dict[str, Any]:
        loc = first.headers.get("Location")
        if not loc:
            raise FabricApiError("Fabric accepted the request (202) but returned no operation Location header")
        op_url = self._same_host(loc)
        deadline = self._monotonic() + timeout_seconds
        wait = self._retry_after(first, default=2.0)
        while True:
            self._sleep(wait)
            state_resp = self._request("GET", op_url)
            state = state_resp.json() if state_resp.content else {}
            status = str(state.get("status", "")).lower()
            if status == "succeeded":
                result_url = self._same_host(state_resp.headers["Location"]) if state_resp.headers.get("Location") else op_url.rstrip("/") + "/result"
                return self._request("GET", result_url).json()
            if status == "failed":
                err = state.get("error") or {}
                raise FabricApiError(f"the getDefinition operation failed: {err.get('message') or err.get('errorCode') or 'no detail'}", error_code=err.get("errorCode"),
                                     remediation="Check the notebook in Fabric (it may be mid-edit or corrupt) and retry.")
            if self._monotonic() >= deadline:
                raise FabricApiError(f"the getDefinition operation did not finish within {int(timeout_seconds)} s (last status: {status or 'unknown'})",
                                     remediation="Retry; large notebooks can take longer. Increase the pull timeout in the integration settings if this repeats.")
            wait = self._retry_after(state_resp, default=2.0)

    @staticmethod
    def decode_definition(doc: dict[str, Any], fmt: str = "ipynb") -> NotebookDefinition:
        definition = doc.get("definition") if isinstance(doc, dict) else None
        parts = (definition or {}).get("parts") if isinstance(definition, dict) else None
        if not isinstance(parts, list) or not parts:
            raise FabricApiError("the getDefinition response has no definition parts")
        decoded: dict[str, str] = {}
        for p in parts:
            path = str(p.get("path", ""))
            if p.get("payloadType", "InlineBase64") != "InlineBase64":
                raise FabricApiError(f"part '{path}' has payloadType '{p.get('payloadType')}', only InlineBase64 is supported")
            try:
                decoded[path] = base64.b64decode(str(p.get("payload", "")), validate=False).decode("utf-8")
            except (ValueError, UnicodeDecodeError) as exc:
                raise FabricApiError(f"part '{path}' is not valid base64/UTF-8: {exc}") from exc
        content_path = next((p for p in decoded if p.startswith("notebook-content.")), None)
        if content_path is None:
            raise FabricApiError("the definition has no notebook-content part", remediation=f"parts returned: {sorted(decoded)}")
        platform = None
        if ".platform" in decoded:
            try:
                platform = json.loads(decoded[".platform"])
            except ValueError:
                platform = None
        out_fmt = "ipynb" if content_path.endswith(".ipynb") else "fabric_git_source"  # the part's extension is the truth, not the format that was asked for
        return NotebookDefinition(format=out_fmt, content=decoded[content_path], content_path=content_path, platform=platform, parts=sorted(decoded))

    # ================================================================== OLCF Intelligence discovery additions
    # Everything below is additive (used by ``olcf.intelligence.discovery.fabric``). Like the rest of this client it is exercised in the tests against the local Fabric test server in
    # ``tests/fabric/fabric_fakes.py``; the documented behaviour it relies on is named in each docstring (Microsoft Learn page titles).
    # ---- paging
    def _walk(self, label: str, first_url: str, next_url: Callable[[str], str], extract: Callable[[dict[str, Any]], Iterable[Any]], max_pages: int) -> Iterator[Any]:
        """Yield the items of a ``continuationToken``-paged listing. Ends normally only when the service stopped sending a token (an exactly-full last page and an empty page that still
        carries a token are both handled); raises :class:`IncompleteInventory` when ``max_pages`` pages were read and a token is still outstanding, or when a token repeats."""
        url, seen = first_url, set()
        for page in range(1, max_pages + 1):
            self._check()
            doc = self._request("GET", url).json()
            if not isinstance(doc, dict):
                raise FabricApiError(f"the {label} listing answered with something other than a JSON object")
            yield from extract(doc)
            tok = doc.get("continuationToken")
            if not tok:
                return
            if tok in seen:
                raise IncompleteInventory(f"the {label} listing repeated a continuation token after {page} page(s); the inventory is incomplete", reason="repeated_token", pages=page, label=label)
            seen.add(tok)
            url = next_url(str(tok))
        raise IncompleteInventory(f"the {label} listing still had a continuation token after {max_pages} page(s); the inventory is incomplete", reason="max_pages", pages=max_pages, label=label)

    @staticmethod
    def _collect(label: str, items: Iterator[Any]) -> list[Any]:
        """Drain a listing into a list; an :class:`IncompleteInventory` carries what was received in ``partial``."""
        out: list[Any] = []
        try:
            for x in items:
                out.append(x)
        except IncompleteInventory as exc:
            exc.partial = out
            raise
        return out

    def iter_workspaces(self, *, max_pages: int | None = None) -> Iterator[dict[str, Any]]:
        """Yield every workspace the identity can see, following ``continuationToken`` (Learn: *Workspaces - List Workspaces*).

        Unlike :meth:`list_workspaces` this does not stop after 20 pages, so a large tenant is listed completely. ``max_pages`` (default :attr:`DEFAULT_MAX_PAGES`) is a runaway guard:
        when it is reached with a token still outstanding - or a token repeats - the generator raises :class:`IncompleteInventory` *after* yielding everything it received.
        """
        yield from self._walk("workspaces", f"{self.network.fabric_api}/v1/workspaces", lambda t: f"{self.network.fabric_api}/v1/workspaces?continuationToken={quote(t)}",
                              lambda d: d.get("value", []), max_pages or self.DEFAULT_MAX_PAGES)

    def list_items(self, workspace_id: str, *, item_type: str | None = None, max_pages: int | None = None) -> list[dict[str, Any]]:
        """All items of a workspace, optionally one ``type`` (Learn: *Items - List Items*, ``GET /v1/workspaces/{workspaceId}/items?type=..&continuationToken=..``).

        Each element is ``{id, displayName, description, type, workspaceId, folderId?}``. The continuation token is followed until the service stops sending one; an incomplete
        listing raises :class:`IncompleteInventory` (``partial`` holds the items received).
        """
        base = f"{self.network.fabric_api}/v1/workspaces/{quote(workspace_id)}/items"
        params: list[str] = [f"type={quote(item_type)}"] if item_type else []
        return self._collect("items", self._walk("items", base + ("?" + "&".join(params) if params else ""), lambda t: base + "?" + "&".join([*params, f"continuationToken={quote(t)}"]),
                                                 lambda d: d.get("value", []), max_pages or self.DEFAULT_MAX_PAGES))

    def list_lakehouse_tables(self, workspace_id: str, lakehouse_id: str, *, max_pages: int | None = None) -> list[dict[str, Any]]:
        """Tables of a lakehouse (Learn: *Tables - List Tables*, ``GET /v1/workspaces/{ws}/lakehouses/{lh}/tables``).

        Elements are ``{name, location, format, type, path}`` (the documented list key is ``data``; ``value`` is accepted too). The documented limitations are the caller's to
        handle: the API is not available for lakehouses with schemas enabled and needs the lakehouse to be readable by the identity (HTTP 400/404/403/501 surface as
        :class:`FabricApiError`). An incomplete listing raises :class:`IncompleteInventory` (``partial`` holds the tables received).
        """
        base = f"{self.network.fabric_api}/v1/workspaces/{quote(workspace_id)}/lakehouses/{quote(lakehouse_id)}/tables"
        return self._collect("lakehouse tables", self._walk("lakehouse tables", base, lambda t: f"{base}?continuationToken={quote(t)}", lambda d: d.get("data", d.get("value", [])),
                                                            max_pages or self.DEFAULT_MAX_PAGES))

    def get_item_definition(self, workspace_id: str, item_id: str, *, fmt: str | None = None, timeout_seconds: float = 120.0) -> ItemDefinition:
        """Fetch the definition parts of any item (Learn: *Items - Get Item Definition*, ``POST /v1/workspaces/{ws}/items/{id}/getDefinition[?format=..]``).

        Follows the documented long-running-operation protocol (``202`` + ``Location`` + ``Retry-After``; poll the operation, then ``GET <operation>/result``) with the same
        helper as :meth:`get_definition`. Text parts are base64-decoded; parts that are not UTF-8 (images in a report's ``StaticResources``) are counted in
        :attr:`ItemDefinition.binary_parts` and not returned.
        """
        url = f"{self.network.fabric_api}/v1/workspaces/{quote(workspace_id)}/items/{quote(item_id)}/getDefinition" + (f"?format={quote(fmt)}" if fmt else "")
        resp = self._request("POST", url, accept_statuses=(200, 202))
        doc = self._await_operation(resp, timeout_seconds) if resp.status_code == 202 else resp.json()
        return self.decode_item_definition(doc)

    @staticmethod
    def decode_item_definition(doc: dict[str, Any]) -> ItemDefinition:
        """Decode a ``getDefinition`` result for any item type into text parts (see :meth:`get_item_definition`)."""
        definition = doc.get("definition") if isinstance(doc, dict) else None
        parts = (definition or {}).get("parts") if isinstance(definition, dict) else None
        if not isinstance(parts, list) or not parts:
            raise FabricApiError("the getDefinition response has no definition parts")
        decoded: dict[str, str] = {}
        binary = 0
        for p in parts:
            path = str(p.get("path", ""))
            if p.get("payloadType", "InlineBase64") != "InlineBase64":
                raise FabricApiError(f"part '{path}' has payloadType '{p.get('payloadType')}', only InlineBase64 is supported")
            try:
                raw = base64.b64decode(str(p.get("payload", "")), validate=False)
            except ValueError as exc:
                raise FabricApiError(f"part '{path}' is not valid base64: {exc}") from exc
            try:
                decoded[path] = raw.decode("utf-8-sig")
            except UnicodeDecodeError:
                binary += 1
        platform = None
        if ".platform" in decoded:
            try:
                platform = json.loads(decoded[".platform"])
            except ValueError:
                platform = None
        return ItemDefinition(parts=decoded, platform=platform, binary_parts=binary)

    # ------------------------------------------------------------------ Power BI admin (metadata scanner)
    def _pbi(self, path: str) -> str:
        return f"{self.network.powerbi_api}/v1.0/myorg/admin/{path}"

    def powerbi_modified_workspaces(self, modified_since: str | None = None, *, exclude_personal: bool = True) -> list[str]:
        """Workspace ids modified since a timestamp (Learn: *Admin - Get Modified Workspaces*, ``GET /v1.0/myorg/admin/workspaces/modified``).

        ``modified_since`` is an ISO-8601 UTC timestamp within the last 30 days (older values are rejected by the service). The documented response is an array of
        ``{"id": "<guid>"}``; the quota is documented as 30 requests per hour.
        """
        q = []
        if modified_since:
            q.append(f"modifiedSince={quote(modified_since)}")
        q.append(f"excludePersonalWorkspaces={'True' if exclude_personal else 'False'}")
        doc = self._request("GET", self._pbi("workspaces/modified?" + "&".join(q)), scope=POWERBI_SCOPE).json()
        rows = doc if isinstance(doc, list) else doc.get("value", []) if isinstance(doc, dict) else []
        return [str(r["id"]) if isinstance(r, dict) else str(r) for r in rows if (isinstance(r, dict) and r.get("id")) or isinstance(r, str)]

    def powerbi_scan_workspaces(self, workspace_ids: list[str], *, lineage: bool = True, datasource_details: bool = True, dataset_schema: bool = True,
                                dataset_expressions: bool = True, artifact_users: bool = True) -> dict[str, Any]:
        """Start a metadata scan (Learn: *Admin - WorkspaceInfo PostWorkspaceInfo*, ``POST /v1.0/myorg/admin/workspaces/getInfo?lineage=True&datasourceDetails=True&...``).

        The body is ``{"workspaces": [<=100 ids]}``; the documented answer is ``202`` with ``{"id": <scanId>, "createdDateTime", "status": "NotStarted"}``. The documented limits
        are 500 requests per hour and 16 concurrent scans - enforcing them is the caller's job (the discovery connector has settings for both).
        """
        if not 1 <= len(workspace_ids) <= 100:
            raise FabricApiError("a metadata scan takes between 1 and 100 workspaces per request")

        def flag(v: bool) -> str:
            return "True" if v else "False"

        q = f"lineage={flag(lineage)}&datasourceDetails={flag(datasource_details)}&datasetSchema={flag(dataset_schema)}&datasetExpressions={flag(dataset_expressions)}&getArtifactUsers={flag(artifact_users)}"
        resp = self._request("POST", self._pbi("workspaces/getInfo?" + q), json_body={"workspaces": list(workspace_ids)}, accept_statuses=(200, 202), scope=POWERBI_SCOPE)
        doc = resp.json()
        if not isinstance(doc, dict) or not doc.get("id"):
            raise FabricApiError("the getInfo response has no scan id")
        return doc

    def powerbi_scan_status(self, scan_id: str) -> dict[str, Any]:
        """State of a scan (Learn: *Admin - WorkspaceInfo GetScanStatus*): ``{"id", "createdDateTime", "status": "NotStarted"|"Running"|"Succeeded"}``."""
        return dict(self._request("GET", self._pbi(f"workspaces/scanStatus/{quote(scan_id)}"), scope=POWERBI_SCOPE).json())

    def powerbi_scan_result(self, scan_id: str) -> dict[str, Any]:
        """The result of a succeeded scan (Learn: *Admin - WorkspaceInfo GetScanResult*): ``{"workspaces": [...], "datasourceInstances": [...], ...}``."""
        return dict(self._request("GET", self._pbi(f"workspaces/scanResult/{quote(scan_id)}"), scope=POWERBI_SCOPE).json())

    # ------------------------------------------------------------------ OneLake (ADLS Gen2 style DFS endpoint)
    def onelake_list(self, workspace_id: str, directory: str, *, recursive: bool = False, max_pages: int | None = None) -> list[dict[str, Any]]:
        """List a OneLake directory (Learn: *OneLake access with Azure Data Lake Storage APIs*; ADLS Gen2 *Path - List*:
        ``GET {dfs}/{workspace}?resource=filesystem&directory=..&recursive=..``, continuation in the ``x-ms-continuation`` header).

        ``directory`` is relative to the workspace, for example ``<lakehouse id>/Tables``. Entries are ``{name, isDirectory, contentLength, lastModified}`` with
        ``name`` relative to the workspace. Uses a storage-audience token of the same identity. A listing that is still continuing after ``max_pages`` pages (default
        :attr:`DEFAULT_MAX_ONELAKE_PAGES`) or that repeats a continuation raises :class:`IncompleteInventory` (``partial`` holds the entries received).
        """
        base = f"{self.network.onelake_dfs}/{quote(workspace_id)}"
        limit = max_pages or self.DEFAULT_MAX_ONELAKE_PAGES
        out: list[dict[str, Any]] = []
        cont, seen = "", set()
        for page in range(1, limit + 1):
            url = f"{base}?resource=filesystem&recursive={'true' if recursive else 'false'}&directory={quote(directory.strip('/'))}" + (f"&continuation={quote(cont)}" if cont else "")
            resp = self._request("GET", url, scope=STORAGE_SCOPE, headers={"x-ms-version": ONELAKE_API_VERSION}, max_body=MAX_LISTING_BYTES)
            doc = resp.json() if resp.content else {}
            out.extend(doc.get("paths", []) if isinstance(doc, dict) else [])
            cont = resp.headers.get("x-ms-continuation", "")
            if not cont:
                return out
            if cont in seen:
                raise IncompleteInventory(f"the OneLake listing of {directory} repeated a continuation after {page} page(s); the inventory is incomplete", reason="repeated_token", pages=page,
                                          label="onelake", partial=out)
            seen.add(cont)
        raise IncompleteInventory(f"the OneLake listing of {directory} still continued after {limit} page(s); the inventory is incomplete", reason="max_pages", pages=limit, label="onelake", partial=out)

    def onelake_read(self, workspace_id: str, path: str, *, max_bytes: int = 4_000_000) -> bytes:
        """Read a small OneLake file (``GET {dfs}/{workspace}/{path}``), refusing anything longer than ``max_bytes`` (Delta log entries, not data files).

        The limit is enforced while streaming: an excessive ``Content-Length`` is refused before the body is read, the bytes actually received are counted whatever the header says,
        reading stops at ``max_bytes`` plus at most one chunk, a compressed body is bounded after decompression and the connection is closed on every path.
        """
        url = f"{self.network.onelake_dfs}/{quote(workspace_id)}/{quote(path.strip('/'), safe='/')}"
        return self._request("GET", url, scope=STORAGE_SCOPE, headers={"x-ms-version": ONELAKE_API_VERSION}, max_body=max_bytes).content
