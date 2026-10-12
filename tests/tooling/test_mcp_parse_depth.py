# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""A JSON-RPC body nested thousands of levels deep is a parse error (HTTP 400), never an internal error: Python 3.11 raises ``RecursionError`` from the JSON decoder."""

from __future__ import annotations

import pytest

from olcf.mcp import protocol


@pytest.mark.parametrize("body", [b'{"a":' + b"[" * 5000, b"[" * 200000, b'{"a":' * 5000])
def test_deeply_nested_json_is_a_parse_error(body: bytes) -> None:
    with pytest.raises(protocol.RpcError) as ei:
        protocol.parse_body(body)
    assert ei.value.code == protocol.PARSE_ERROR
