# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Dependency-free terminal output for the ``olcf`` CLI: tables, colour, progress lines, JSON / YAML / CSV.

The CLI is deliberately *rich-free* so it behaves the same in a minimal container, over SSH, in CI logs and on a Windows console.
Colour is used only when stdout is a terminal, ``NO_COLOR`` is unset and ``--no-color`` was not given.  Every value that is printed passes
:func:`scrub`, which masks credentials that slipped into API answers; the only deliberate exceptions are the one-time displays of a freshly
created token or password (``reveal=`` argument).
"""

from __future__ import annotations

import csv
import io
import json
import os
import shutil
import sys
import time
from collections.abc import Callable, Iterable, Sequence
from typing import Any, TextIO

from olcf.core.redaction import REDACTED, redact_text

#: Keys whose values are never printed (they are write-only in the API, but a bug must not turn into a leak).
SECRET_KEYS = frozenset({"password", "secret", "client_secret", "private_key", "access_token", "refresh_token", "signing_secret", "api_key", "token", "worker_token",
                         "enrollment_token", "authorization", "generated_password", "passphrase"})


def scrub(obj: Any, *, reveal: Iterable[str] = ()) -> Any:
    """Deep copy of ``obj`` with secrets masked (keys in :data:`SECRET_KEYS` unless listed in ``reveal``; credentials inside strings)."""
    allow = set(reveal)
    if isinstance(obj, dict):
        return {k: (v if k in allow else REDACTED if (str(k).lower() in SECRET_KEYS and v not in (None, "", False)) else scrub(v, reveal=allow)) for k, v in obj.items()}
    if isinstance(obj, (list, tuple)):
        return [scrub(v, reveal=allow) for v in obj]
    if isinstance(obj, str):
        return redact_text(obj)
    return obj


def get_path(obj: Any, path: str, default: Any = None) -> Any:
    """``get_path(job, "failure.category")``."""
    cur = obj
    for part in path.split("."):
        if isinstance(cur, dict) and part in cur:
            cur = cur[part]
        else:
            return default
    return cur


def fmt_cell(value: Any, width: int | None = None) -> str:
    if value is None:
        text = "-"
    elif isinstance(value, bool):
        text = "yes" if value else "no"
    elif isinstance(value, (list, tuple)):
        text = ", ".join(fmt_cell(v) for v in value) or "-"
    elif isinstance(value, dict):
        text = json.dumps(value, separators=(",", ":"), default=str)
    elif isinstance(value, float):
        text = f"{value:.2f}".rstrip("0").rstrip(".")
    else:
        text = str(value)
    text = text.replace("\n", " ").replace("\r", " ")
    if width and len(text) > width:
        text = text[: max(1, width - 1)] + "…"
    return text


def human_bytes(n: float | int | None) -> str:
    if n is None:
        return "-"
    n = float(n)
    for unit in ("B", "KiB", "MiB", "GiB", "TiB"):
        if abs(n) < 1024 or unit == "TiB":
            return f"{n:.0f} {unit}" if unit == "B" else f"{n:.1f} {unit}"
        n /= 1024
    return f"{n:.1f} TiB"


def human_age(iso: str | None) -> str:
    """'3m ago' for an ISO-8601 UTC timestamp."""
    if not iso:
        return "-"
    from datetime import UTC, datetime

    try:
        t = datetime.fromisoformat(iso.replace("Z", "+00:00"))
    except ValueError:
        return iso
    secs = max(0, int((datetime.now(UTC) - t).total_seconds()))
    for limit, unit in ((60, "s"), (3600, "m"), (86400, "h")):
        if secs < limit:
            return f"{secs // (limit // 60) if unit != 's' else secs}{unit} ago"
    return f"{secs // 86400}d ago"


class Output:
    """Printer configured once per invocation."""

    def __init__(self, fmt: str = "table", *, color: bool | None = None, quiet: bool = False, out: TextIO | None = None, err: TextIO | None = None) -> None:
        self.fmt = fmt
        self.quiet = quiet
        self.out = out or sys.stdout
        self.err = err or sys.stderr
        if color is None:
            color = bool(getattr(self.out, "isatty", lambda: False)()) and "NO_COLOR" not in os.environ and os.environ.get("TERM") != "dumb"
        self.color = color
        if self.color and os.name == "nt":
            os.system("")  # noqa: S605,S607 - enables ANSI escape processing on Windows 10+ consoles

    # ------------------------------------------------------------------ styling
    @property
    def json_mode(self) -> bool:
        return self.fmt == "json"

    def style(self, text: str, *, fg: str | None = None, bold: bool = False, dim: bool = False) -> str:
        if not self.color:
            return text
        codes = {"red": 31, "green": 32, "yellow": 33, "blue": 34, "magenta": 35, "cyan": 36, "grey": 90}
        seq = []
        if bold:
            seq.append("1")
        if dim:
            seq.append("2")
        if fg:
            seq.append(str(codes[fg]))
        return f"\x1b[{';'.join(seq)}m{text}\x1b[0m" if seq else text

    def state(self, value: str) -> str:
        """Colour a status word (job state, health, ...)."""
        v = (value or "").lower()
        good = {"ok", "online", "ready", "succeeded", "healthy", "up", "active", "installed", "verified", "running", "idle", "pass"}
        bad = {"failed", "down", "offline", "error", "timed_out", "revoked", "fail", "dead", "critical"}
        warn = {"degraded", "queued", "cordoned", "draining", "warn", "pending", "cancelled", "cancel_requested", "unknown", "installing"}
        fg = "green" if v in good else "red" if v in bad else "yellow" if v in warn else None
        return self.style(value, fg=fg)

    # ------------------------------------------------------------------ messages (stderr so stdout stays machine-readable)
    def info(self, text: str = "") -> None:
        if not self.quiet:
            print(text, file=self.err if self.json_mode else self.out, flush=True)

    def step(self, text: str) -> None:
        if not self.quiet:
            print(self.style("==> ", fg="blue", bold=True) + text, file=self.err, flush=True)

    def detail(self, text: str) -> None:
        if not self.quiet:
            print("    " + self.style(text, dim=True), file=self.err, flush=True)

    def success(self, text: str) -> None:
        if not self.quiet:
            print(self.style("✓ " if self.color else "OK: ", fg="green", bold=True) + text, file=self.err if self.json_mode else self.out, flush=True)

    def warn(self, text: str) -> None:
        print(self.style("! " if self.color else "warning: ", fg="yellow", bold=True) + text, file=self.err, flush=True)

    def error(self, text: str, hint: str | None = None) -> None:
        print(self.style("error: ", fg="red", bold=True) + text, file=self.err, flush=True)
        if hint:
            print(self.style("hint:  ", fg="cyan", bold=True) + hint, file=self.err, flush=True)

    def progress(self, step: str, detail: str = "") -> None:
        """Progress callback compatible with ``olcf.runtime`` (``(step, detail)``)."""
        if not self.quiet:
            print(f"  {self.style('[' + step + ']', fg='cyan')} {detail}", file=self.err, flush=True)

    # ------------------------------------------------------------------ data
    def emit(self, data: Any, human: Callable[[Any], None] | None = None, *, reveal: Iterable[str] = ()) -> None:
        """Print ``data`` as JSON/YAML when requested, otherwise through the ``human`` renderer."""
        safe = scrub(data, reveal=reveal)
        if self.fmt == "json":
            print(json.dumps(safe, indent=2, default=str, ensure_ascii=False), file=self.out)
        elif self.fmt == "yaml":
            import yaml

            print(yaml.safe_dump(json.loads(json.dumps(safe, default=str)), sort_keys=False, allow_unicode=True).rstrip(), file=self.out)
        elif human is not None:
            human(data if reveal else safe)
        else:
            print(json.dumps(safe, indent=2, default=str, ensure_ascii=False), file=self.out)

    def table(self, rows: Sequence[dict[str, Any]], columns: Sequence[tuple[Any, ...]], *, empty: str = "(none)",
              raw_headers: bool = False) -> None:
        """Render ``rows`` with ``(header, key_path[, max_width])`` columns in the chosen format (csv / table; JSON/YAML print the rows)."""
        rows = list(rows)
        if self.fmt in ("json", "yaml"):
            self.emit(rows)
            return
        if self.fmt == "csv":
            buf = io.StringIO()
            w = csv.writer(buf, lineterminator="\n")
            w.writerow([c[0] for c in columns])
            for r in rows:
                w.writerow([_csv_cell(get_path(r, c[1])) for c in columns])
            self.out.write(redact_text(buf.getvalue()))
            return
        if not rows:
            print(self.style(empty, dim=True), file=self.out)
            return
        term = shutil.get_terminal_size((120, 24)).columns
        cells = [[fmt_cell(scrub_value(get_path(r, c[1])), c[2] if len(c) > 2 else 60) for c in columns] for r in rows]  # type: ignore[misc]
        widths = [max(len(c[0]), *(len(row[i]) for row in cells)) for i, c in enumerate(columns)]
        while sum(widths) + 2 * (len(widths) - 1) > term and max(widths) > 12:
            widths[widths.index(max(widths))] -= 1
        line = "  ".join(self.style((c[0] if raw_headers else c[0].upper()).ljust(widths[i]), bold=True) for i, c in enumerate(columns))
        print(line.rstrip(), file=self.out)
        for row in cells:
            out_cells = []
            for i, cell in enumerate(row):
                cell = cell if len(cell) <= widths[i] else cell[: widths[i] - 1] + "…"
                padded = cell.ljust(widths[i])
                key = columns[i][1]
                out_cells.append(padded.replace(cell, self.state(cell), 1) if key.rsplit(".", 1)[-1] in ("state", "status", "scheduling") else padded)
            print("  ".join(out_cells).rstrip(), file=self.out)

    def kv(self, pairs: Sequence[tuple[str, Any]] | dict[str, Any], *, indent: int = 0) -> None:
        """Aligned ``key: value`` block."""
        items = list(pairs.items()) if isinstance(pairs, dict) else list(pairs)
        if not items:
            return
        w = max(len(str(k)) for k, _ in items)
        for k, v in items:
            print(" " * indent + self.style(f"{k}:".ljust(w + 1), bold=True) + " " + fmt_cell(scrub_value(v), 400), file=self.out)

    def text(self, text: str) -> None:
        print(text, file=self.out)

    def raw_bytes(self, data: bytes) -> None:
        buf = getattr(self.out, "buffer", None)
        if buf is not None:
            buf.write(data)
            buf.flush()
        else:
            self.out.write(data.decode("utf-8", errors="replace"))


def scrub_value(v: Any) -> Any:
    return scrub(v)


def _csv_cell(v: Any) -> str:
    if v is None:
        return ""
    if isinstance(v, (dict, list)):
        return json.dumps(v, separators=(",", ":"), default=str)
    return str(v)


class Spinner:
    """Minimal activity indicator for waits (only animates on a TTY; otherwise prints one line every ``every`` seconds)."""

    def __init__(self, out: Output, label: str, every: float = 15.0) -> None:
        self.out, self.label, self.every = out, label, every
        self._t0 = time.monotonic()
        self._last = 0.0

    def tick(self, extra: str = "") -> None:
        now = time.monotonic()
        if self.out.quiet or now - self._last < (0.2 if self._tty else self.every):
            return
        self._last = now
        el = int(now - self._t0)
        msg = f"{self.label}{' - ' + extra if extra else ''} ({el}s)"
        if self._tty:
            print("\r" + msg.ljust(shutil.get_terminal_size((80, 24)).columns - 1), end="", file=self.out.err, flush=True)
        else:
            print(msg, file=self.out.err, flush=True)

    @property
    def _tty(self) -> bool:
        return bool(getattr(self.out.err, "isatty", lambda: False)())

    def done(self) -> None:
        if self._tty and not self.out.quiet:
            print("\r" + " " * (shutil.get_terminal_size((80, 24)).columns - 1) + "\r", end="", file=self.out.err, flush=True)
