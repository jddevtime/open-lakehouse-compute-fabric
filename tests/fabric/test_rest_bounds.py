# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Review findings F08 and F14 at the REST-client level, with scripted transports (no network, no sockets):

* F08 - every paging method says so when it did not read the whole inventory (page limit exhausted, repeated token), and copes with exactly-full final pages and empty pages that
  still carry a token;
* F14 - every body is read as a bounded stream: it stops at the cap plus at most one chunk, an excessive ``Content-Length`` is refused unread but never trusted, compressed bodies are
  bounded after decompression, the stream is closed on every path and cancellation stops the read.

Tests use scripted transports.
"""

from __future__ import annotations

import gzip
import json
import zlib
from collections.abc import Callable, Iterator
from typing import Any
from urllib.parse import parse_qs, urlsplit

import httpx
import pytest

from olcf.fabric.http import Network
from olcf.fabric.rest import MAX_ERROR_BODY_BYTES, FabricApiError, FabricRestClient, IncompleteInventory

WS = "00000000-0000-4000-8000-000000000001"
LH = "00000000-0000-4000-8000-000000000002"


class StubBroker:
    def __init__(self, net: Network) -> None:
        self.network, self._cache = net, {}

    def get_token(self, scope: str) -> str:
        return "token"


class Stream(httpx.SyncByteStream):
    """A response body that counts what was pulled from it and whether it was closed."""

    def __init__(self, chunks: Iterator[bytes] | list[bytes]) -> None:
        self._chunks = iter(chunks)
        self.pulled = 0
        self.bytes = 0
        self.closed = False

    def __iter__(self) -> Iterator[bytes]:
        for c in self._chunks:
            self.pulled += 1
            self.bytes += len(c)
            yield c

    def close(self) -> None:
        self.closed = True


def make(handler: Callable[[httpx.Request], httpx.Response], **kw: Any) -> FabricRestClient:
    net = Network.from_config({})
    return FabricRestClient(StubBroker(net), net, sleep=lambda s: None, client_factory=lambda: httpx.Client(transport=httpx.MockTransport(handler)), max_attempts=1, **kw)  # type: ignore[arg-type]


def json_resp(doc: Any, **headers: str) -> httpx.Response:
    return httpx.Response(200, content=json.dumps(doc).encode(), headers={"Content-Type": "application/json", **headers})


# ------------------------------------------------------------------------------------------------ F08: paging
def pager(pages: list[dict[str, Any]], key: str = "value", onelake: bool = False) -> Callable[[httpx.Request], httpx.Response]:
    """Serves ``pages`` in order, whatever the token asked for (so a script can repeat a token or never end)."""
    state = {"i": 0}

    def handler(req: httpx.Request) -> httpx.Response:
        page = pages[min(state["i"], len(pages) - 1)]
        state["i"] += 1
        items, tok = page.get("items", []), page.get("token")
        if onelake:
            return json_resp({"paths": items}, **({"x-ms-continuation": tok} if tok else {}))
        return json_resp({key: items, **({"continuationToken": tok} if tok else {})})

    return handler


CALLS: dict[str, Callable[[FabricRestClient, int], Any]] = {
    "iter_workspaces": lambda c, n: list(c.iter_workspaces(max_pages=n)),
    "list_workspaces": lambda c, n: c.list_workspaces(),
    "list_items": lambda c, n: c.list_items(WS, max_pages=n),
    "list_lakehouse_tables": lambda c, n: c.list_lakehouse_tables(WS, LH, max_pages=n),
    "onelake_list": lambda c, n: c.onelake_list(WS, f"{LH}/Tables", max_pages=n),
}
KEYS = {"iter_workspaces": "value", "list_workspaces": "value", "list_items": "value", "list_lakehouse_tables": "data", "onelake_list": "paths"}


def pages_of(name: str, n: int, *, last_token: bool, prefix: str = "t") -> Callable[[httpx.Request], httpx.Response]:
    pages = [{"items": [{"name": f"{name}{i}", "id": str(i)}], "token": f"{prefix}{i}" if (i < n - 1 or last_token) else None} for i in range(n)]
    return pager(pages, key=KEYS[name], onelake=name == "onelake_list")


@pytest.mark.parametrize("name", list(CALLS))
def test_exhausting_the_page_limit_with_a_token_outstanding_raises_incomplete_inventory(name):
    n = 20 if name == "list_workspaces" else 3
    c = make(pages_of(name, n, last_token=True))
    with pytest.raises(IncompleteInventory) as exc:
        CALLS[name](c, n)
    assert exc.value.reason == "max_pages" and exc.value.pages == n and isinstance(exc.value, FabricApiError)
    if name != "iter_workspaces":
        assert len(exc.value.partial) == n                       # what arrived is not thrown away


@pytest.mark.parametrize("name", list(CALLS))
def test_an_exactly_full_final_page_is_complete(name):
    n = 20 if name == "list_workspaces" else 3
    c = make(pages_of(name, n, last_token=False))
    assert len(CALLS[name](c, n)) == n


@pytest.mark.parametrize("name", list(CALLS))
def test_a_repeated_continuation_token_is_incomplete_not_a_loop(name):
    c = make(pager([{"items": [{"name": "a"}], "token": "same"}], key=KEYS[name], onelake=name == "onelake_list"))
    with pytest.raises(IncompleteInventory) as exc:
        CALLS[name](c, 50)
    assert exc.value.reason == "repeated_token" and exc.value.pages <= 3


@pytest.mark.parametrize("name", list(CALLS))
def test_an_empty_page_that_still_carries_a_token_is_followed(name):
    pages = [{"items": [], "token": "a"}, {"items": [{"name": "x"}], "token": "b"}, {"items": [], "token": None}]
    c = make(pager(pages, key=KEYS[name], onelake=name == "onelake_list"))
    assert len(CALLS[name](c, 10)) == 1


@pytest.mark.parametrize("name", list(CALLS))
def test_cancellation_stops_between_pages(name):
    requests: list[str] = []
    inner = pages_of(name, 50, last_token=True)

    def handler(req: httpx.Request) -> httpx.Response:
        requests.append(str(req.url))
        return inner(req)

    c = make(handler)

    class Cancelled(Exception):
        pass

    def check() -> None:
        if len(requests) >= 2:
            raise Cancelled

    c.cancel_check = check
    with pytest.raises(Cancelled):
        CALLS[name](c, 50)
    assert len(requests) == 2


def test_a_listing_that_is_not_a_json_object_is_an_error_not_an_empty_inventory():
    c = make(lambda req: json_resp(["unexpected"]))
    with pytest.raises(FabricApiError, match="JSON object"):
        c.list_items(WS)


# ------------------------------------------------------------------------------------------------ F14: bounded bodies
CHUNK = 1024


def big_body(chunks: int = 8) -> Stream:
    return Stream([b"x" * CHUNK for _ in range(chunks)])


def test_a_chunked_body_without_content_length_stops_at_the_cap_plus_one_chunk():
    stream = big_body(2000)
    c = make(lambda req: httpx.Response(200, stream=stream))
    with pytest.raises(FabricApiError, match="larger than 1024 bytes"):
        c.onelake_read(WS, f"{LH}/f.json", max_bytes=CHUNK)
    assert stream.bytes <= CHUNK + CHUNK and stream.pulled <= 2           # never pulled the rest
    assert stream.closed


def test_an_excessive_declared_content_length_is_refused_without_reading_the_body():
    stream = big_body(10)
    c = make(lambda req: httpx.Response(200, stream=stream, headers={"Content-Length": "9999999999"}))
    with pytest.raises(FabricApiError, match="larger than 1024 bytes"):
        c.onelake_read(WS, f"{LH}/f.json", max_bytes=CHUNK)
    assert stream.pulled == 0 and stream.closed


def test_a_false_content_length_is_not_trusted():
    stream = big_body(2000)
    c = make(lambda req: httpx.Response(200, stream=stream, headers={"Content-Length": "10"}))
    with pytest.raises(FabricApiError, match="larger than 1024 bytes"):
        c.onelake_read(WS, f"{LH}/f.json", max_bytes=CHUNK)
    assert stream.bytes <= 2 * CHUNK and stream.closed


def test_a_gzip_bomb_is_bounded_after_decompression():
    bomb = gzip.compress(b"\0" * (200 * 1024 * 1024), compresslevel=9)            # ~200 KiB on the wire, 200 MiB decoded
    assert len(bomb) < 1024 * 1024
    stream = Stream([bomb[i:i + 4096] for i in range(0, len(bomb), 4096)])
    c = make(lambda req: httpx.Response(200, stream=stream, headers={"Content-Encoding": "gzip"}))
    with pytest.raises(FabricApiError, match="once decompressed"):
        c.onelake_read(WS, f"{LH}/f.json", max_bytes=64 * 1024)
    assert stream.closed


def test_a_compressed_valid_file_is_decoded_and_a_small_plain_file_is_read_whole():
    doc = json.dumps({"metaData": {"schemaString": "x" * 500}}).encode()
    c = make(lambda req: httpx.Response(200, stream=Stream([gzip.compress(doc)]), headers={"Content-Encoding": "gzip"}))
    assert c.onelake_read(WS, f"{LH}/f.json", max_bytes=10_000) == doc
    c2 = make(lambda req: httpx.Response(200, stream=Stream([doc[:100], doc[100:]])))
    assert c2.onelake_read(WS, f"{LH}/f.json", max_bytes=10_000) == doc
    c3 = make(lambda req: httpx.Response(200, stream=Stream([zlib.compress(doc)]), headers={"Content-Encoding": "deflate"}))
    assert c3.onelake_read(WS, f"{LH}/f.json", max_bytes=10_000) == doc


def test_a_body_of_exactly_the_cap_is_accepted():
    c = make(lambda req: httpx.Response(200, stream=Stream([b"y" * CHUNK])))
    assert len(c.onelake_read(WS, f"{LH}/f.json", max_bytes=CHUNK)) == CHUNK


def test_an_unsupported_content_encoding_is_refused():
    c = make(lambda req: httpx.Response(200, stream=Stream([b"abc"]), headers={"Content-Encoding": "br"}))
    with pytest.raises(FabricApiError, match="content encoding"):
        c.onelake_read(WS, f"{LH}/f.json")


def test_the_stream_is_closed_when_reading_is_cancelled():
    stream = big_body(100)
    c = make(lambda req: httpx.Response(200, stream=stream))
    seen = {"n": 0}

    class Cancelled(Exception):
        pass

    def check() -> None:
        seen["n"] += 1
        if seen["n"] > 3:                       # the request is admitted, then the third chunk is where the scan was cancelled
            raise Cancelled

    c.cancel_check = check
    with pytest.raises(Cancelled):
        c.onelake_read(WS, f"{LH}/f.json", max_bytes=10 * CHUNK * 100)
    assert stream.closed and stream.pulled < 100


def test_an_error_body_is_only_read_for_its_message():
    stream = Stream([b"e" * 4096 for _ in range(1000)])
    c = make(lambda req: httpx.Response(500, stream=stream))
    with pytest.raises(FabricApiError) as exc:
        c.onelake_read(WS, f"{LH}/f.json")
    assert exc.value.status == 500 and stream.bytes <= MAX_ERROR_BODY_BYTES + 4096 and stream.closed


def test_json_calls_have_a_decoded_size_limit_too_definitions_and_scanner_results():
    huge = Stream([b'{"workspaces": ["' + b"a" * CHUNK] + [b"a" * CHUNK for _ in range(5000)])
    c = make(lambda req: httpx.Response(200, stream=huge))
    c.max_json_bytes = 10 * CHUNK
    with pytest.raises(FabricApiError, match="larger than"):
        c.powerbi_scan_result("scan1")
    assert huge.bytes <= 12 * CHUNK and huge.closed
    big_def = Stream([b'{"definition": {"parts": ["' + b"b" * CHUNK] + [b"b" * CHUNK for _ in range(5000)])
    c2 = make(lambda req: httpx.Response(200, stream=big_def))
    c2.max_json_bytes = 10 * CHUNK
    with pytest.raises(FabricApiError, match="larger than"):
        c2.get_item_definition(WS, LH)
    assert big_def.bytes <= 12 * CHUNK


def test_retries_still_work_through_the_streaming_path():
    calls = {"n": 0}

    def handler(req: httpx.Request) -> httpx.Response:
        calls["n"] += 1
        if calls["n"] == 1:
            return httpx.Response(503, content=b"busy")
        return json_resp({"value": [{"id": "1"}]})

    net = Network.from_config({})
    c = FabricRestClient(StubBroker(net), net, sleep=lambda s: None, client_factory=lambda: httpx.Client(transport=httpx.MockTransport(handler)), max_attempts=3)  # type: ignore[arg-type]
    assert c.list_items(WS) == [{"id": "1"}] and calls["n"] == 2


def test_query_strings_with_tokens_are_not_part_of_size_messages():
    c = make(lambda req: httpx.Response(200, stream=big_body(20)))
    with pytest.raises(FabricApiError) as exc:
        c.onelake_read(WS, f"{LH}/f.json", max_bytes=10)
    assert "continuation" not in str(exc.value) and parse_qs(urlsplit("https://x/?a=1").query) == {"a": ["1"]}
