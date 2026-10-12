# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""uvicorn application factory used by ``olcf serve`` (``olcf.cli.factory:create``).

``olcf serve --host/--port/--public-url/...`` must reach the application without environment variables of the ``OLCF_SERVER__*`` family: the
settings loader treats any such variable as replacing the *whole* ``[server]`` table of ``config.toml``.  The flags are therefore handed over as
one JSON document in ``OLCF_SERVE_OVERRIDES`` and applied with :func:`olcf.core.config.load_settings` overrides, which merge key by key.
Every worker process of ``--workers N`` calls this factory, so they all see identical settings.
"""

from __future__ import annotations

import json
import os
from typing import Any

from fastapi import FastAPI

OVERRIDES_ENV = "OLCF_SERVE_OVERRIDES"


def overrides_from_env() -> dict[str, Any]:
    raw = os.environ.get(OVERRIDES_ENV)
    return json.loads(raw) if raw else {}


def create() -> FastAPI:
    from olcf.api.app import create_app
    from olcf.core.config import load_settings

    settings = load_settings(os.environ.get("OLCF_DATA_DIR") or None, **overrides_from_env())
    return create_app(settings)
