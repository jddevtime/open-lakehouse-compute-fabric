# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""The in-memory notebook model shared by the service, the serializers and other packages (``NotebookService.load``)."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass
class Notebook:
    """A normalised nbformat v4 notebook plus the OLCF item settings (CONTRACTS section 10).

    ``nb`` is a plain ``dict`` (never a ``NotebookNode``): ``{"nbformat": 4, "nbformat_minor": 5, "metadata": {...}, "cells": [...]}``.
    ``settings`` are the item's ``definition["settings"]`` (engine, runtime, default lakehouse, environment ...).
    """

    nb: dict[str, Any]
    settings: dict[str, Any] = field(default_factory=dict)
    name: str | None = None
    item_id: str | None = None
    workspace_id: str | None = None
    version: int | None = None

    @property
    def cells(self) -> list[dict[str, Any]]:
        return self.nb["cells"]

    @property
    def metadata(self) -> dict[str, Any]:
        return self.nb["metadata"]

    def definition(self) -> dict[str, Any]:
        """The item definition (``{"nbformat": ..., "settings": ...}``)."""
        out: dict[str, Any] = {"nbformat": self.nb}
        if self.settings:
            out["settings"] = self.settings
        return out
