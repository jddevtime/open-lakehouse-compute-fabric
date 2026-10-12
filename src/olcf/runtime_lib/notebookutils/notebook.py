# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""``notebookutils.notebook``: ``exit``, ``run`` and ``runMultiple`` (stdlib only).

``exit(value)`` ends the notebook successfully; the value becomes the job's ``exit_value`` (reserved display-data MIME, see
:func:`olcf.runtime_lib.bootstrap.exit_notebook`). ``run`` submits a **child job** for an OLCF notebook item of the given name through the job-scoped token
in ``spec["callbacks"]`` (the child records ``parent_run_id``), waits for it and returns its exit value; a failing child raises, like Fabric.

Guards: nesting depth (``MAX_DEPTH``) and name loops are tracked through the child's environment (``NBU_RUN_DEPTH`` / ``NBU_RUN_CHAIN``), so a
notebook that runs itself - directly or through others - fails immediately with a clear message instead of exhausting the queue.
"""

from __future__ import annotations

import os
import time
import urllib.parse
from typing import Any

from olcf.runtime_lib.bootstrap import exit_notebook
from olcf.runtime_lib.notebookutils import context as ctxmod
from olcf.runtime_lib.notebookutils.api import ApiError, Callbacks
from olcf.runtime_lib.notebookutils.errors import NotSupportedError

MAX_DEPTH = 5
_TERMINAL = {"succeeded", "failed", "cancelled", "timed_out", "canceled"}
_MGMT = "managing Fabric notebook items needs the Fabric workspace APIs of the notebook session, which a remote run does not have"
_MGMT_FIX = "Perform notebook item management in the Fabric notebook before or after olcf.run_remote(), or use the Fabric REST API from a pipeline."


class NotebookNS:
    """The ``notebookutils.notebook`` namespace."""

    def __init__(self, state: dict[str, Any]) -> None:
        self._state = state
        self._counter = 0

    def exit(self, value: Any = None) -> None:
        exit_notebook(value)

    # ------------------------------------------------------------------ run
    def run(self, name: str, timeout_seconds: int = 90, arguments: dict[str, Any] | None = None, workspace: str | None = None, **_: Any) -> Any:
        """Run notebook ``name`` as a child job and return its exit value."""
        if workspace:
            raise NotSupportedError("notebookutils.notebook.run(workspace=...)", "child runs stay inside the job's OLCF workspace",
                                    "Run the notebook of another workspace through its own capsule.")
        spec = self._state["spec"]
        cb = Callbacks(spec)
        if not cb.available:
            raise NotSupportedError("notebookutils.notebook.run", "this job has no callback credentials", "Submit the job through the OLCF API so it receives a job token.")
        depth = int(os.environ.get("NBU_RUN_DEPTH", "0") or 0)
        chain = [c for c in os.environ.get("NBU_RUN_CHAIN", "").split("|") if c]
        current = str(ctxmod.build(spec).get("currentNotebookName") or "")
        if depth >= MAX_DEPTH:
            raise RuntimeError(f"notebookutils.notebook.run: maximum nesting depth {MAX_DEPTH} reached while running '{name}'")
        if name == current or name in chain:
            raise RuntimeError(f"notebookutils.notebook.run: '{name}' would run itself ({' -> '.join([*chain, current, name])}); loops are not allowed")
        ws_id = (spec.get("workspace") or {}).get("id")
        try:
            listing = cb.request("GET", f"/v1/workspaces/{urllib.parse.quote(str(ws_id))}/items?type=notebook&q={urllib.parse.quote(name)}&limit=50")
        except ApiError as exc:
            raise RuntimeError(f"notebookutils.notebook.run: cannot look up notebook '{name}': {exc}") from None
        match = next((i for i in (listing or {}).get("items", []) if i.get("name") == name), None)
        if match is None:
            raise RuntimeError(f"notebookutils.notebook.run: no OLCF notebook named '{name}' in workspace '{(spec.get('workspace') or {}).get('name')}'. "
                               "Import it as a notebook item, or inline it with %run (olcf.run_remote bundles %run targets).")
        lake = spec.get("default_lakehouse") or {}
        self._counter += 1
        body: dict[str, Any] = {
            "kind": "notebook", "workspace_id": ws_id, "item_id": match["id"], "name": name, "parameters": dict(arguments or {}),
            "parent_run_id": spec.get("job_id"), "origin": {"kind": "fabric", "details": {"parent_job_id": spec.get("job_id"), "via": "notebook.run"}},
            "env": {"NBU_RUN_DEPTH": str(depth + 1), "NBU_RUN_CHAIN": "|".join([*chain, current])},
            "idempotency_key": f"{spec.get('job_id')}:{spec.get('attempt', 1)}:run:{self._counter}:{name}",
            "policy": {"timeout_seconds": max(1, int(timeout_seconds or 90))},
        }
        if lake.get("id"):
            body["default_lakehouse"] = {"lakehouse_id": lake["id"]}
        try:
            job = cb.request("POST", "/v1/jobs", body)
        except ApiError as exc:
            raise RuntimeError(f"notebookutils.notebook.run: submitting '{name}' failed: {exc}") from None
        return self._wait(cb, job["id"], name, float(timeout_seconds or 90))

    def _wait(self, cb: Callbacks, job_id: str, name: str, timeout: float) -> Any:
        deadline = time.monotonic() + timeout + 30.0  # the child enforces its own timeout; allow queueing slack
        delay = 0.25
        while True:
            row = cb.request("GET", f"/v1/jobs/{job_id}")
            state = str(row.get("state"))
            if state in _TERMINAL:
                if state == "succeeded":
                    return row.get("exit_value")
                fail = row.get("failure") or {}
                raise RuntimeError(f"child notebook '{name}' ({job_id}) {state}: {fail.get('message') or 'no failure message'}")
            if time.monotonic() > deadline:
                try:
                    cb.request("POST", f"/v1/jobs/{job_id}/cancel")
                except ApiError:
                    pass
                raise TimeoutError(f"child notebook '{name}' ({job_id}) did not finish within {int(timeout)} seconds")
            time.sleep(delay)
            delay = min(delay * 1.5, 2.0)

    def runMultiple(self, DAG: Any, config: dict[str, Any] | None = None) -> dict[str, Any]:  # noqa: N802, N803 - Fabric names
        """Run several notebooks in dependency order (sequentially). Returns ``{name: {"exitVal": ..., "exception": ...}}``."""
        acts = DAG.get("activities") if isinstance(DAG, dict) else [{"name": n, "path": n} for n in DAG]
        if not isinstance(acts, list) or not acts:
            raise ValueError("runMultiple: provide a list of notebook names or a DAG dict with 'activities'")
        by_name = {str(a.get("name") or a.get("path")): a for a in acts}
        done: dict[str, dict[str, Any]] = {}
        pending = list(by_name)
        while pending:
            progressed = False
            for n in list(pending):
                a = by_name[n]
                deps = [str(d) for d in a.get("dependencies") or []]
                if any(d not in by_name for d in deps):
                    raise ValueError(f"runMultiple: activity '{n}' depends on an unknown activity")
                if not all(d in done for d in deps):
                    continue
                pending.remove(n)
                progressed = True
                if any(done[d].get("exception") for d in deps):
                    done[n] = {"exitVal": None, "exception": "skipped: a dependency failed"}
                    continue
                try:
                    out = self.run(str(a.get("path") or n), int(a.get("timeoutPerCellInSeconds", 90) or 90), a.get("args") or {})
                    done[n] = {"exitVal": out, "exception": None}
                except Exception as exc:  # noqa: BLE001 - reported per activity, like Fabric
                    done[n] = {"exitVal": None, "exception": f"{type(exc).__name__}: {exc}"}
            if not progressed:
                raise ValueError(f"runMultiple: dependency cycle among {pending}")
        return done

    # ------------------------------------------------------------------ unsupported
    def _unsupported(self, api: str) -> Any:
        def _raise(*args: Any, **kwargs: Any) -> Any:
            raise NotSupportedError(f"notebookutils.notebook.{api}", _MGMT, _MGMT_FIX)

        return _raise

    def __getattr__(self, name: str) -> Any:
        if name in {"create", "get", "getDefinition", "update", "updateDefinition", "delete", "list", "validateDAG"}:
            return self._unsupported(name)
        if name.startswith("_"):
            raise AttributeError(name)
        raise NotSupportedError(f"notebookutils.notebook.{name}")

    def help(self, method_name: str = "") -> None:
        print("notebookutils.notebook (OLCF):\n  exit(value)\n  run(name, timeout_seconds=90, arguments=None) -> exit value of the child job\n"
              "  runMultiple(DAG_or_list) -> {name: {exitVal, exception}} (sequential, dependency order)\n"
              "  create/get/getDefinition/update/updateDefinition/delete/list/validateDAG: not supported in OLCF")
