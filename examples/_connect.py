# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Shared by the MCP examples: open an authenticated MCP session against an OLCF server with the official ``mcp`` package.

Works with both generations of the package (``pip install mcp``): the 2.x ``Client`` / ``streamable_http_client`` API and the 1.x
``ClientSession`` / ``streamablehttp_client`` API.  The bearer token is read from ``OLCF_MCP_TOKEN`` (a personal access token created with
``olcf token create agent --scope mcp``) and is never printed.
"""

from __future__ import annotations

import contextlib
import json
import os
import sys
from collections.abc import AsyncIterator
from typing import Any

DEFAULT_URL = "http://127.0.0.1:8470/mcp"


def endpoint(url: str | None = None) -> str:
    """The MCP endpoint: ``--url``, else ``$OLCF_MCP_URL``, else ``$OLCF_URL`` + ``/mcp``, else the local default."""
    value = url or os.environ.get("OLCF_MCP_URL")
    if not value:
        base = os.environ.get("OLCF_URL")
        value = base.rstrip("/") + "/mcp" if base else DEFAULT_URL
    return value


def token_from_env() -> str:
    token = os.environ.get("OLCF_MCP_TOKEN", "").strip()
    if not token:
        sys.exit("OLCF_MCP_TOKEN is not set. Create one with:  olcf token create agent --scope mcp   (the secret is shown once)")
    return token


@contextlib.asynccontextmanager
async def open_session(url: str, token: str, *, ca_bundle: str | None = None) -> AsyncIterator[Any]:
    """Yield an initialised MCP client session (``list_tools`` / ``call_tool`` / ``list_resources`` ...)."""
    headers = {"Authorization": f"Bearer {token}"}
    try:  # mcp 2.x
        from mcp.client.client import Client
        from mcp.client.streamable_http import streamable_http_client
        from mcp.shared._httpx_utils import create_mcp_http_client
    except ImportError:
        pass
    else:
        kwargs: dict[str, Any] = {"headers": headers}
        if ca_bundle:
            import ssl

            kwargs["verify"] = ssl.create_default_context(cafile=ca_bundle)
        async with create_mcp_http_client(**kwargs) as http:
            async with Client(streamable_http_client(url, http_client=http)) as client:
                yield client
        return
    try:  # mcp 1.x
        from mcp import ClientSession
        from mcp.client.streamable_http import streamablehttp_client
    except ImportError:
        sys.exit("the 'mcp' package is required:  pip install mcp")
    async with streamablehttp_client(url, headers=headers) as streams:
        async with ClientSession(streams[0], streams[1]) as session:
            await session.initialize()
            yield session


def result_json(result: Any) -> Any:
    """The JSON document a tool returned (OLCF tools answer with one JSON text block); raises ``RuntimeError`` for an error result."""
    text = "".join(getattr(block, "text", "") for block in result.content)
    if getattr(result, "is_error", False) or getattr(result, "isError", False):
        raise RuntimeError(text or "tool call failed")
    return json.loads(text) if text else None
