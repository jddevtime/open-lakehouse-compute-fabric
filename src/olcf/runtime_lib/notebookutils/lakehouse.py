# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""``notebookutils.lakehouse``: ``get`` and ``list``, limited to the lakehouses attached to the job (stdlib only)."""

from __future__ import annotations

from typing import Any

from olcf.runtime_lib.notebookutils.errors import LakehouseNotFound, NotSupportedError

_MGMT = "creating or changing lakehouse items (and the Fabric load-to-table service) is a Fabric control-plane operation"
_MGMT_FIX = 'Manage lakehouses in OLCF or Fabric outside the remote run; write tables with Spark (df.write.format("delta").saveAsTable(...)).'


def _describe(lh: dict[str, Any]) -> dict[str, Any]:
    fab = lh.get("fabric") or {}
    return {"id": fab.get("lakehouse_id") or lh.get("id"), "type": "Lakehouse", "displayName": lh.get("name"), "description": "",
            "workspaceId": fab.get("workspace_id") or lh.get("workspace_id"),
            "properties": {"oneLakeTablesPath": lh.get("tables_uri"), "oneLakeFilesPath": lh.get("files_uri"), "abfsPath": lh.get("root_uri"),
                           "olcfId": lh.get("id"), "defaultFormat": lh.get("format")}}


class LakehouseNS:
    """The ``notebookutils.lakehouse`` namespace."""

    def __init__(self, state: dict[str, Any]) -> None:
        self._state = state

    def _known(self) -> list[dict[str, Any]]:
        spec = self._state["spec"]
        out, seen = [], set()
        for lh in [spec.get("default_lakehouse"), *(spec.get("lakehouses") or [])]:
            if lh and lh.get("id") not in seen:
                seen.add(lh.get("id"))
                out.append(lh)
        return out

    def get(self, name: str | None = None, workspaceId: str = "") -> dict[str, Any]:  # noqa: N803
        known = self._known()
        if name is None:
            if not known:
                raise LakehouseNotFound("this job has no default lakehouse")
            return _describe(known[0])
        for lh in known:
            fab = lh.get("fabric") or {}
            if name in (lh.get("name"), lh.get("id"), fab.get("lakehouse_id")) and (not workspaceId or workspaceId in (lh.get("workspace_id"), fab.get("workspace_id"))):
                return _describe(lh)
        raise LakehouseNotFound(f"lakehouse '{name}' is not attached to this job (known: {', '.join(str(k.get('name')) for k in known) or 'none'})")

    def list(self, workspaceId: str = "", maxResults: int = 1000) -> list[dict[str, Any]]:  # noqa: N803
        return [_describe(lh) for lh in self._known()
                if not workspaceId or workspaceId in (lh.get("workspace_id"), (lh.get("fabric") or {}).get("workspace_id"))][: int(maxResults)]

    def _unsupported(self, api: str) -> Any:
        def _raise(*args: Any, **kwargs: Any) -> Any:
            raise NotSupportedError(f"notebookutils.lakehouse.{api}", _MGMT, _MGMT_FIX)

        return _raise

    def __getattr__(self, name: str) -> Any:
        if name in {"create", "update", "delete", "getWithProperties", "listTables", "loadTable"}:
            return self._unsupported(name)
        if name.startswith("_"):
            raise AttributeError(name)
        raise NotSupportedError(f"notebookutils.lakehouse.{name}")

    def help(self, method_name: str = "") -> None:
        print("notebookutils.lakehouse (OLCF):\n  get(name=None, workspaceId='') -> dict   (lakehouses attached to this job only)\n  list(workspaceId='') -> list[dict]\n"
              "  create/update/delete/getWithProperties/listTables/loadTable: not supported in OLCF")
