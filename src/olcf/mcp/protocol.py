# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""JSON-RPC 2.0 / MCP wire primitives: versions, error codes, message classification and tool-argument validation."""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import Any

from jsonschema import Draft202012Validator

SUPPORTED_VERSIONS = ("2025-06-18", "2025-03-26")
LATEST_VERSION = SUPPORTED_VERSIONS[0]
SESSION_HEADER = "Mcp-Session-Id"
VERSION_HEADER = "MCP-Protocol-Version"

PARSE_ERROR = -32700
INVALID_REQUEST = -32600
METHOD_NOT_FOUND = -32601
INVALID_PARAMS = -32602
INTERNAL_ERROR = -32603
RESOURCE_NOT_FOUND = -32002        # MCP: resource not found
SERVER_ERROR = -32000              # implementation-defined (unauthorised, session problems, ...)


class RpcError(Exception):
    """A JSON-RPC level error (as opposed to a tool that ran and failed, which is reported with ``isError``)."""

    def __init__(self, code: int, message: str, data: Any = None, *, http_status: int | None = None) -> None:
        super().__init__(message)
        self.code, self.message, self.data, self.http_status = code, message, data, http_status

    def body(self, req_id: Any = None) -> dict[str, Any]:
        err: dict[str, Any] = {"code": self.code, "message": self.message}
        if self.data is not None:
            err["data"] = self.data
        return {"jsonrpc": "2.0", "id": req_id, "error": err}


def ok(req_id: Any, result: Any) -> dict[str, Any]:
    return {"jsonrpc": "2.0", "id": req_id, "result": result}


def notification(method: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
    msg: dict[str, Any] = {"jsonrpc": "2.0", "method": method}
    if params is not None:
        msg["params"] = params
    return msg


def parse_body(raw: bytes) -> Any:
    try:
        return json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, ValueError) as exc:
        raise RpcError(PARSE_ERROR, f"parse error: {str(exc)[:200]}") from None
    except RecursionError:  # thousands of nested brackets: Python 3.11 recurses in the C decoder and raises this instead of ValueError
        raise RpcError(PARSE_ERROR, "parse error: the JSON is nested too deeply") from None


def classify(msg: Any) -> str:
    """``request`` | ``notification`` | ``response``; raises ``RpcError(INVALID_REQUEST)`` for anything else."""
    if not isinstance(msg, dict) or msg.get("jsonrpc") != "2.0":
        raise RpcError(INVALID_REQUEST, "not a JSON-RPC 2.0 message")
    if "method" in msg:
        if not isinstance(msg["method"], str) or not msg["method"]:
            raise RpcError(INVALID_REQUEST, "method must be a non-empty string")
        params = msg.get("params")
        if params is not None and not isinstance(params, (dict, list)):
            raise RpcError(INVALID_REQUEST, "params must be an object")
        if "id" not in msg:
            return "notification"
        if isinstance(msg["id"], bool) or not isinstance(msg["id"], (str, int)):
            raise RpcError(INVALID_REQUEST, "id must be a string or an integer")
        return "request"
    if "id" in msg and ("result" in msg or "error" in msg):
        return "response"
    raise RpcError(INVALID_REQUEST, "message has neither method nor result/error")


def negotiate(requested: Any) -> str:
    """The client's version when we speak it, else our latest (the client then decides whether to continue)."""
    return requested if isinstance(requested, str) and requested in SUPPORTED_VERSIONS else LATEST_VERSION


def validate_arguments(schema: dict[str, Any], args: Any) -> list[str]:
    """Human-readable problems of ``args`` against a tool's JSON Schema (empty when valid)."""
    out = []
    for e in sorted(Draft202012Validator(schema).iter_errors(args), key=lambda e: list(e.absolute_path)):
        path = ".".join(str(p) for p in e.absolute_path)
        if e.validator == "additionalProperties":
            extra = sorted(set(e.instance) - set(e.schema.get("properties", {}))) if isinstance(e.instance, dict) else []
            out.append(f"unknown argument(s): {', '.join(extra)}; allowed: {', '.join(sorted(e.schema.get('properties', {})))}")
        elif e.validator == "required":
            out.append(e.message.replace("is a required property", "is required"))
        else:
            out.append(f"{path or 'arguments'}: {e.message[:200]}")
    return out


@dataclass
class Tool:
    """One MCP tool: name, schema and the handler ``fn(env, args) -> dict``."""

    name: str
    title: str
    description: str
    schema: dict[str, Any]
    fn: Any
    write: bool = False
    #: Permissions the caller needs *somewhere* for the tool to be listed (it is re-checked against the real workspace on every call).
    needs: tuple[str, ...] = ()
    #: Platform service the tool depends on; the tool is neither listed nor callable while that module is not installed.  ``"service.attr"`` also requires the optional
    #: package the service carries in that attribute (``"intelligence.governance"``).
    service: str | None = None
    annotations: dict[str, Any] = field(default_factory=dict)

    def descriptor(self, version: str) -> dict[str, Any]:
        d: dict[str, Any] = {"name": self.name, "description": self.description, "inputSchema": self.schema}
        if version >= "2025-06-18":
            d["title"] = self.title
        d["annotations"] = {"title": self.title, "readOnlyHint": not self.write, "destructiveHint": False, "idempotentHint": not self.write, "openWorldHint": False,
                            **self.annotations}
        return d


def obj(props: dict[str, Any], required: list[str] | None = None) -> dict[str, Any]:
    """Compact JSON Schema object builder (``additionalProperties`` is always false so typos are reported, not ignored)."""
    s: dict[str, Any] = {"type": "object", "properties": props, "additionalProperties": False}
    if required:
        s["required"] = required
    return s
