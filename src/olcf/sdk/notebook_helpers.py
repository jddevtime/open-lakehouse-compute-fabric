# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Conveniences for code that runs *inside* an OLCF (or Fabric) notebook and wants to talk to the control plane.

::

    from olcf.sdk import Client
    nh = Client.from_env().notebook_helpers          # needs OLCF_URL and OLCF_TOKEN in the notebook environment
    df = nh.df("SELECT region, sum(revenue) r FROM retail_lakehouse.orders GROUP BY 1", workspace="Sample Retail")
    result = nh.run_notebook("Clean data", {"year": 2025}, workspace="Sample Retail", timeout=900)

The helpers return plain Python values (lists of dicts, bytes, the child notebook's exit value); ``df`` returns a pandas DataFrame when
pandas is installed in the runtime.  Credentials come from the client: never hard-code a token in a notebook - store it as an OLCF secret and
inject it with ``secret_refs`` (or use a short-lived personal access token with scope ``mcp``).
"""

from __future__ import annotations

import os
from typing import Any

from olcf.sdk.errors import ValidationError


class NotebookHelpers:
    """Small facade over :class:`olcf.sdk.Client` with notebook-friendly names."""

    def __init__(self, client: Any) -> None:
        self.client = client
        self._ws_cache: dict[str, str] = {}

    # ------------------------------------------------------------------ names
    def workspace_id(self, workspace: str | None = None) -> str:
        """Resolve a workspace name/id; defaults to ``$OLCF_WORKSPACE_ID`` or ``$OLCF_WORKSPACE`` (set by the platform for notebook runs)."""
        ref = workspace or os.environ.get("OLCF_WORKSPACE_ID") or os.environ.get("OLCF_WORKSPACE")
        if not ref:
            raise ValidationError("no workspace given and $OLCF_WORKSPACE_ID is not set", status=422, code="validation_failed",
                                  remediation="pass workspace='name or id'")
        if ref.startswith("wks_"):
            return ref
        if ref not in self._ws_cache:
            self._ws_cache[ref] = str(self.client.workspaces.get(ref)["id"])
        return self._ws_cache[ref]

    # ------------------------------------------------------------------ SQL
    def sql(self, query: str, *, workspace: str | None = None, lakehouse: str | None = None, engine: str = "duckdb",
            parameters: dict[str, Any] | None = None, max_rows: int | None = None) -> dict[str, Any]:
        """Run SQL; returns ``{columns, rows, row_count, truncated, ...}``."""
        return self.client.sql.execute(query, engine=engine, workspace_id=self.workspace_id(workspace), lakehouse=lakehouse,
                                       parameters=parameters, max_rows=max_rows)

    def rows(self, query: str, **kw: Any) -> list[dict[str, Any]]:
        """Result rows as a list of ``{column: value}`` dicts."""
        res = self.sql(query, **kw)
        cols = [c["name"] if isinstance(c, dict) else c for c in res.get("columns", [])]
        out = []
        for r in res.get("rows", []):
            out.append(dict(zip(cols, r, strict=False)) if isinstance(r, (list, tuple)) else r)
        return out

    def df(self, query: str, **kw: Any) -> Any:
        """Result as a pandas DataFrame."""
        try:
            import pandas as pd
        except ImportError as exc:  # pragma: no cover - environment dependent
            raise ImportError("pandas is not installed in this environment; use .rows() or add pandas to the environment") from exc
        return pd.DataFrame(self.rows(query, **kw))

    # ------------------------------------------------------------------ lakehouse files
    def read_file(self, lakehouse: str, path: str) -> bytes:
        return self.client.lakehouses.read_file(lakehouse, path)

    def write_file(self, lakehouse: str, path: str, data: bytes | str, *, overwrite: bool = True) -> dict[str, Any]:
        return self.client.lakehouses.write_file(lakehouse, path, data.encode() if isinstance(data, str) else data, overwrite=overwrite)

    def tables(self, lakehouse: str) -> list[dict[str, Any]]:
        return list(self.client.lakehouses.list_tables(lakehouse))

    # ------------------------------------------------------------------ child runs
    def run_notebook(self, notebook: str, parameters: dict[str, Any] | None = None, *, workspace: str | None = None, timeout: float | None = 3600.0,
                     **run_kw: Any) -> Any:
        """Run another notebook as a job and return its exit value (``notebookutils.notebook.exit``); raises ``JobFailedError`` on failure."""
        ws = self.workspace_id(workspace)
        item_id = notebook
        if not notebook.startswith("itm_"):
            found = [i for i in self.client.items.list(ws, type="notebook", q=notebook) if i.get("name") == notebook]
            if not found:
                raise ValidationError(f"notebook '{notebook}' not found in workspace {ws}", status=404, code="not_found")
            item_id = found[0]["id"]
        job = self.client.notebooks.run(item_id, parameters=parameters, **run_kw)
        done = self.client.jobs.wait(job["id"], timeout=timeout, raise_on_failure=True)
        return done.get("exit_value")

    def job_log(self, job_id: str, *, tail_bytes: int = 8192) -> str:
        return str(self.client.jobs.logs_page(job_id, limit=tail_bytes, tail=True).get("data", ""))
