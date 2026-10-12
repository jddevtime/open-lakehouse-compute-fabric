# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""One control-plane replica for the HA test: ``python hanode.py <data_dir> <port>`` (database and keys come from the environment, as in a real deployment)."""

from __future__ import annotations

import sys

import uvicorn

from olcf.api.app import create_app
from olcf.core.config import load_settings


def main() -> None:
    data_dir, port = sys.argv[1], int(sys.argv[2])
    settings = load_settings(data_dir, environment="test")
    settings.server.host, settings.server.port = "127.0.0.1", port
    settings.compute.embedded_worker = False
    settings.server.serve_ui = False
    uvicorn.run(create_app(settings), host="127.0.0.1", port=port, log_level="warning", access_log=False)


if __name__ == "__main__":
    main()
