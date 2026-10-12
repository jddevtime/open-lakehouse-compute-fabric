# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Structured, redacted logging."""

from __future__ import annotations

import json
import logging
import sys
from logging.handlers import RotatingFileHandler
from pathlib import Path

from olcf.core.redaction import RedactingFilter


class JsonFormatter(logging.Formatter):
    """Logging formatter that renders each record as one JSON object (timestamp, level, logger, message and exception text)."""
    def format(self, record: logging.LogRecord) -> str:
        out = {"ts": self.formatTime(record, "%Y-%m-%dT%H:%M:%S%z"), "level": record.levelname, "logger": record.name, "msg": record.getMessage()}
        if record.exc_text:
            out["exc"] = record.exc_text
        return json.dumps(out)


def configure_logging(level: str = "INFO", *, json_logs: bool = False, log_dir: Path | None = None, name: str = "olcf") -> None:
    root = logging.getLogger()
    for h in list(root.handlers):
        if getattr(h, "_olcf", False):
            root.removeHandler(h)
    fmt: logging.Formatter = JsonFormatter() if json_logs else logging.Formatter("%(asctime)s %(levelname)-7s %(name)s: %(message)s")
    handlers: list[logging.Handler] = [logging.StreamHandler(sys.stderr)]
    if log_dir:
        log_dir.mkdir(parents=True, exist_ok=True)
        handlers.append(RotatingFileHandler(log_dir / f"{name}.log", maxBytes=20 * 1024 * 1024, backupCount=10, encoding="utf-8"))
    for h in handlers:
        h.setFormatter(fmt)
        h.addFilter(RedactingFilter())
        h._olcf = True  # type: ignore[attr-defined]
        root.addHandler(h)
    root.setLevel(level.upper())
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("httpcore").setLevel(logging.WARNING)
