# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Deterministic ``%run`` inlining.

A Fabric ``%run <notebook> [{json parameters}]`` executes another notebook *in the caller's session*. A capsule ships exactly one source
document, so the SDK bundles the referenced notebooks next to it (``olcf.run_remote(bundle_run_targets=True)``) and this module replaces
each ``%run`` line by the referenced notebook's cells. That is the same session semantics (shared globals, shared Spark session), it is
reproducible, and it is recorded: every inlined cell carries ``origin = {"notebook", "cell"}`` and the analyzer report lists each expansion.

Rules:

* only a ``%run`` at the start of a line outside strings/brackets is expanded (``split_ipython`` guarantees that);
* the parameters JSON becomes plain assignments placed after the child's parameters cell (or at its top), exactly what Fabric does;
* a target that is not bundled, or a ``%run`` cycle, is left untouched and reported: the analyzer then blocks with ``FAB-MAGIC-RUN``;
* the child's ``parameters`` cell is demoted to an ordinary cell so the caller keeps a single parameters cell.
"""

from __future__ import annotations

import copy
import json
import re
import shlex
from typing import Any

from olcf.fabric import notebook as nbm
from olcf.fabric.analyzer.pyfront import cell_magic, split_ipython

MAX_DEPTH = 10


def _split_target(args: str) -> tuple[str, dict[str, Any]]:
    """``Name {"a": 1}`` -> ``("Name", {"a": 1})``; leading ``-b/--builtin`` flags are ignored."""
    args = args.strip()
    params: dict[str, Any] = {}
    brace = args.find("{")
    head = args if brace < 0 else args[:brace]
    if brace >= 0:
        try:
            val = json.loads(args[brace:])
            if isinstance(val, dict):
                params = val
        except ValueError:
            params = {}
    try:
        toks = [t for t in shlex.split(head) if not t.startswith("-")]
    except ValueError:
        toks = [t for t in head.split() if not t.startswith("-")]
    return (toks[0] if toks else ""), params


def _synthetic_params(params: dict[str, Any], origin: dict[str, Any]) -> nbm.Cell | None:
    lines = [f"{k} = {v!r}" for k, v in params.items() if isinstance(k, str) and k.isidentifier()]
    if not lines:
        return None
    return nbm.Cell(cell_type="code", source="\n".join(lines), language="python", origin=dict(origin, synthetic="run-parameters"))


def inline_runs(nb: nbm.Notebook, bundled: dict[str, str], *, main_name: str | None = None) -> list[dict[str, Any]]:
    """Expand ``%run`` lines of ``nb`` in place. Returns events ``{status, target, cell, line, cells}`` (status: inlined | missing | recursive | too_deep)."""
    events: list[dict[str, Any]] = []
    parsed: dict[str, nbm.Notebook] = {}

    def load(name: str) -> nbm.Notebook | None:
        if name not in bundled:
            return None
        if name not in parsed:
            parsed[name] = nbm.parse(bundled[name])
        return copy.deepcopy(parsed[name])

    def expand(cells: list[nbm.Cell], stack: tuple[str, ...], owner: str | None, top: bool) -> list[nbm.Cell]:
        out: list[nbm.Cell] = []
        for cell in cells:
            base_origin = {"notebook": owner, "cell": cell.index}
            if cell.cell_type != "code" or cell.language not in ("python",) or cell_magic(cell.source):
                c = copy.copy(cell)
                c.origin = cell.origin or base_origin
                out.append(c)
                continue
            _, magics = split_ipython(cell.source)
            runs = [m for m in magics if m.kind == "magic" and m.name == "run" and m.col == 0]
            if not runs:
                c = copy.copy(cell)
                c.origin = cell.origin or base_origin
                out.append(c)
                continue
            lines = cell.source.split("\n")
            decisions: dict[int, tuple[str, dict[str, Any], nbm.Notebook | None, str]] = {}
            for m in runs:
                target, params = _split_target(m.args)
                child = load(target) if target else None
                status = "inlined"
                if child is None:
                    status = "missing"
                elif target in stack or (owner is not None and target == owner):
                    status = "recursive"
                elif len(stack) >= MAX_DEPTH:
                    status = "too_deep"
                decisions[m.line] = (target, params, child if status == "inlined" else None, status)
            if not any(d[3] == "inlined" for d in decisions.values()):
                for ln, (target, _p, _c, status) in decisions.items():
                    events.append({"target": target, "cell": cell.index, "line": ln, "notebook": owner, "status": status, "cells": 0})
                c = copy.copy(cell)
                c.origin = cell.origin or base_origin
                out.append(c)
                continue
            pending: list[str] = []
            pending_start = 1

            def flush(cell: nbm.Cell = cell, base_origin: dict[str, Any] = base_origin) -> None:  # noqa: B023 - runs inside the iteration that defines it
                body = "\n".join(pending).strip("\n")  # noqa: B023
                if body.strip():
                    lead = len("\n".join(pending)) - len("\n".join(pending).lstrip("\n"))  # noqa: B023
                    c = copy.copy(cell)
                    c.source = body
                    c.is_parameters = cell.is_parameters and not any(x.is_parameters for x in out)
                    c.origin = dict(cell.origin or base_origin, line_offset=pending_start - 1 + lead)  # noqa: B023
                    out.append(c)
                pending.clear()  # noqa: B023

            for n, line in enumerate(lines, start=1):
                d = decisions.get(n)
                if d is None or d[3] != "inlined":
                    if d is not None:
                        events.append({"target": d[0], "cell": cell.index, "line": n, "notebook": owner, "status": d[3], "cells": 0})
                    if not pending:
                        pending_start = n
                    pending.append(line)
                    continue
                target, params, child, _status = d
                assert child is not None
                flush()
                inner = expand(child.cells, stack + (target,), target, False)
                for ic in inner:
                    ic.is_parameters = False
                    if ic.metadata.get("tags"):
                        ic.metadata = dict(ic.metadata, tags=[t for t in ic.metadata["tags"] if t != "parameters"])
                syn = _synthetic_params(params, {"notebook": owner, "cell": cell.index})
                if syn is not None:
                    child_params = next((i for i, c in enumerate(child.cells) if c.is_parameters), None)
                    pos = 0 if child_params is None else min(len(inner), child_params + 1)
                    inner.insert(pos, syn)
                for ic in inner:
                    ic.origin = dict(ic.origin or {}, via=[*((ic.origin or {}).get("via") or []), {"notebook": owner, "cell": cell.index, "line": n}])
                out.extend(inner)
                events.append({"target": target, "cell": cell.index, "line": n, "notebook": owner, "status": "inlined", "cells": len(inner)})
                pending_start = n + 1
            flush()
        return out

    nb.cells = expand(nb.cells, (), main_name, True)
    if not any(e["status"] == "inlined" for e in events):
        for c in nb.cells:
            c.origin = None  # nothing was restructured: cell numbers are the original ones
    nb.renumber()
    return events


_RUN = re.compile(r"(?m)^%run\b")


def mentions_run(text: str) -> bool:
    """Cheap pre-check used by the SDK to decide which notebooks to bundle."""
    return bool(_RUN.search(text))


def run_targets(nb: nbm.Notebook) -> list[str]:
    """Names referenced by ``%run`` lines of ``nb`` (in order, de-duplicated)."""
    seen: list[str] = []
    for c in nb.cells:
        if c.cell_type != "code" or c.language != "python" or cell_magic(c.source):
            continue
        for m in split_ipython(c.source)[1]:
            if m.kind == "magic" and m.name == "run":
                t, _ = _split_target(m.args)
                if t and t not in seen:
                    seen.append(t)
    return seen
