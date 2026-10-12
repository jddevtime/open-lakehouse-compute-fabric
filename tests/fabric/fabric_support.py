# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Support code of the Fabric adapter tests (import by qualified name: ``from tests.fabric.fabric_support import ipynb``).

``E2E`` bundles the control plane, a real worker agent (fake Spark-less runtime), the fake Microsoft world and a Fabric-scoped service principal; ``ipynb`` /
``notebook_context`` build notebooks and Fabric ``notebookutils`` contexts.  The pytest fixtures that use them are in ``conftest.py``.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from tests.fabric.fabric_fakes import FABRIC_WS, NOTEBOOK, FakeFabricWorld
from tests.fabric.fabric_stack import FabricStack

HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[1]


@dataclass
class E2E:
    """Control plane + real worker agent (fake Spark-less runtime) + fake Microsoft world + a Fabric-scoped service principal."""

    st: FabricStack
    world: FakeFabricWorld
    ws: str                                  # OLCF workspace the Fabric workspace maps to
    headers: dict[str, str]                  # Fabric-scoped platform token of a service principal bound to ``ws``
    token: str
    broker_secret_id: str = ""
    extra: dict[str, Any] = field(default_factory=dict)

    @property
    def url(self) -> str:
        return self.st.url


def notebook_context(**over: Any) -> dict[str, Any]:
    ctx = {"currentNotebookName": "etl_orders", "currentNotebookId": NOTEBOOK, "currentWorkspaceName": "Sales", "currentWorkspaceId": FABRIC_WS, "defaultLakehouseName": None,
           "defaultLakehouseId": None, "defaultLakehouseWorkspaceName": None, "defaultLakehouseWorkspaceId": None, "currentRunId": "run-0001", "parentRunId": None,
           "rootRunId": "run-0001", "isForPipeline": False, "isReferenceRun": False, "userName": "alice@contoso.example", "userId": "00000000-0000-4000-8000-0000000000aa",
           "productType": "Fabric"}
    ctx.update(over)
    return ctx


def ipynb(cells: list[tuple[str, str] | tuple[str, str, list[str]]], metadata: dict[str, Any] | None = None) -> str:
    """Build an ipynb text from ``(cell_type, source[, tags])`` tuples."""
    out = []
    for c in cells:
        tags = c[2] if len(c) > 2 else []
        cell: dict[str, Any] = {"cell_type": c[0], "metadata": {"tags": tags} if tags else {}, "source": c[1]}
        if c[0] == "code":
            cell.update({"execution_count": None, "outputs": []})
        out.append(cell)
    return json.dumps({"nbformat": 4, "nbformat_minor": 5, "metadata": {"kernelspec": {"name": "python3", "display_name": "Python 3", "language": "python"}, **(metadata or {})},
                       "cells": out})


SUBMIT_CELL = "import olcf_fabric as olcf\nolcf.run_remote()\n"
