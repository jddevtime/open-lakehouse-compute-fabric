# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Helpers shared by the command modules."""

from __future__ import annotations

import functools
import json
import os
import sys
from collections.abc import Callable
from pathlib import Path
from typing import Any

import click

from olcf.cli.state import State

pass_state = click.make_pass_decorator(State)


def data_dir_option(f: Callable[..., Any]) -> Callable[..., Any]:
    """``--data-dir`` for commands that work on the local installation (overrides the global option and ``OLCF_DATA_DIR``)."""

    @click.option("--data-dir", type=click.Path(file_okay=False), default=None, envvar="OLCF_DATA_DIR", help="OLCF data directory (default ~/.olcf).")
    @functools.wraps(f)
    def wrapper(*a: Any, data_dir: str | None = None, **kw: Any) -> Any:
        ctx = click.get_current_context()
        st = ctx.find_object(State)
        if st is not None and data_dir:
            st.data_dir = data_dir
        return f(*a, **kw)

    return wrapper


def workspace_option(f: Callable[..., Any]) -> Callable[..., Any]:
    return click.option("-w", "--workspace", "workspace", default=None, help="Workspace name or id (default: $OLCF_WORKSPACE or the context default).")(f)


def parse_value(raw: str) -> Any:
    """``5`` -> 5, ``true`` -> True, ``[1,2]`` -> list, anything else stays a string."""
    try:
        return json.loads(raw)
    except ValueError:
        return raw


def parse_kv(pairs: tuple[str, ...] | list[str], *, what: str = "key=value") -> dict[str, Any]:
    out: dict[str, Any] = {}
    for p in pairs:
        if "=" not in p:
            raise click.BadParameter(f"'{p}' must look like {what}")
        k, v = p.split("=", 1)
        out[k.strip()] = parse_value(v)
    return out


def read_input(path: str | None, *, binary: bool = False) -> Any:
    """Read a file, or stdin for ``-``."""
    if path in (None, "-"):
        data = sys.stdin.buffer.read() if binary else sys.stdin.read()
        return data
    p = Path(path)
    return p.read_bytes() if binary else p.read_text(encoding="utf-8")


def load_json_arg(path: str | None) -> Any:
    text = read_input(path)
    try:
        return json.loads(text)
    except ValueError as exc:
        raise click.BadParameter(f"not valid JSON: {exc}") from exc


def confirm(state: State, question: str, yes: bool) -> None:
    """Ask before a destructive action; ``--yes`` skips, a non-interactive session without ``--yes`` refuses."""
    if yes:
        return
    if not sys.stdin.isatty():
        raise click.ClickException(f"{question} - refusing in a non-interactive session; pass --yes to confirm")
    if not click.confirm(question, default=False):
        raise click.Abort()


def yes_option(f: Callable[..., Any]) -> Callable[..., Any]:
    return click.option("-y", "--yes", is_flag=True, help="Do not ask for confirmation.")(f)


def write_or_print(state: State, data: bytes, output: str | None, *, label: str = "") -> None:
    if output and output != "-":
        Path(output).write_bytes(data)
        state.out.success(f"wrote {output} ({len(data)} bytes)")
    else:
        state.out.raw_bytes(data)


def resolve_item(state: State, workspace: str | None, ref: str, type_: str | None = None) -> dict[str, Any]:
    """An item by id (``itm_...``) or by exact name inside the workspace."""
    c = state.client
    if ref.startswith("itm_"):
        return c.items.get(ref)
    ws = state.default_workspace(workspace)
    matches = [i for i in c.paginate(c.items.list, ws, type=type_, q=ref) if i.get("name") == ref]
    if not matches:
        raise click.ClickException(f"no {type_ or 'item'} named '{ref}' in workspace '{ws}'")
    return matches[0]


def secret_prompt(label: str, *, from_env: str | None = None, stdin: bool = False) -> str:
    """Read a secret without echo (or from an environment variable / stdin for automation)."""
    if from_env and os.environ.get(from_env):
        return os.environ[from_env]
    if stdin or not sys.stdin.isatty():
        return sys.stdin.readline().rstrip("\n")
    return click.prompt(label, hide_input=True)
