# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Multi-context credential store shared by the CLI and the SDK (``Client.from_context``).

A *context* names a control plane (``url``) plus the credentials used against it (``token`` and/or a ``refresh_token`` obtained by
``olcf login``).  They live in one JSON file in the per-user configuration directory:

=========  ===============================================
Linux      ``$XDG_CONFIG_HOME/olcf`` or ``~/.config/olcf``
macOS      ``~/Library/Application Support/olcf``
Windows    ``%APPDATA%\\olcf``
=========  ===============================================

``$OLCF_CONFIG_DIR`` overrides the location.  The file is written atomically with mode ``0600`` (the directory ``0700``); on
Windows the per-user profile ACLs apply.  When the optional ``keyring`` package is installed *and* ``OLCF_USE_KEYRING=1``, secrets are
kept in the OS keychain instead and the file only holds a marker.  Nothing here ever prints a token.
"""

from __future__ import annotations

import contextlib
import json
import os
import sys
import tempfile
from pathlib import Path
from typing import Any

SECRET_FIELDS = ("token", "refresh_token")
KEYRING_SERVICE = "olcf"


def config_dir() -> Path:
    override = os.environ.get("OLCF_CONFIG_DIR")
    if override:
        return Path(override).expanduser()
    if sys.platform == "win32":
        return Path(os.environ.get("APPDATA") or Path.home() / "AppData" / "Roaming") / "olcf"
    if sys.platform == "darwin":
        return Path.home() / "Library" / "Application Support" / "olcf"
    return Path(os.environ.get("XDG_CONFIG_HOME") or Path.home() / ".config") / "olcf"


def contexts_path() -> Path:
    return config_dir() / "contexts.json"


def _use_keyring() -> bool:
    if os.environ.get("OLCF_USE_KEYRING") not in ("1", "true", "yes"):
        return False
    try:
        import keyring  # noqa: F401
    except ImportError:
        return False
    return True


def _write_private(path: Path, text: str) -> None:
    """Atomic write with 0600 permissions (temp file in the same directory, then ``os.replace``)."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with contextlib.suppress(OSError):
        os.chmod(path.parent, 0o700)
    fd, tmp = tempfile.mkstemp(prefix=".contexts-", dir=str(path.parent))
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as fh:
            fh.write(text)
        with contextlib.suppress(OSError):
            os.chmod(tmp, 0o600)
        os.replace(tmp, path)
    finally:
        with contextlib.suppress(OSError):
            os.unlink(tmp)


class ContextStore:
    """Read / modify / save the contexts file."""

    def __init__(self, path: Path | None = None) -> None:
        self.path = path or contexts_path()
        self.current: str | None = None
        self.contexts: dict[str, dict[str, Any]] = {}
        self.load()

    def load(self) -> None:
        try:
            doc = json.loads(self.path.read_text(encoding="utf-8"))
        except (OSError, ValueError):
            doc = {}
        self.current = doc.get("current")
        self.contexts = {k: dict(v) for k, v in (doc.get("contexts") or {}).items() if isinstance(v, dict)}

    def save(self) -> None:
        contexts = {}
        for name, ctx in self.contexts.items():
            ctx = dict(ctx)
            if _use_keyring():
                import keyring

                for f in SECRET_FIELDS:
                    if ctx.get(f) and ctx[f] != "@keyring":
                        keyring.set_password(KEYRING_SERVICE, f"{name}:{f}", ctx[f])
                        ctx[f] = "@keyring"
            contexts[name] = ctx
        _write_private(self.path, json.dumps({"current": self.current, "contexts": contexts}, indent=2, sort_keys=True) + "\n")

    def get(self, name: str | None = None) -> dict[str, Any] | None:
        """The named (default: current) context with secrets resolved, or ``None``."""
        name = name or os.environ.get("OLCF_CONTEXT") or self.current
        if not name or name not in self.contexts:
            return None
        ctx = dict(self.contexts[name])
        ctx["name"] = name
        for f in SECRET_FIELDS:
            if ctx.get(f) == "@keyring":
                try:
                    import keyring

                    ctx[f] = keyring.get_password(KEYRING_SERVICE, f"{name}:{f}")
                except Exception:  # noqa: BLE001
                    ctx[f] = None
        return ctx

    def set(self, name: str, *, make_current: bool = True, **values: Any) -> None:
        ctx = self.contexts.setdefault(name, {})
        ctx.update({k: v for k, v in values.items() if v is not None})
        if make_current or not self.current:
            self.current = name
        self.save()

    def clear_credentials(self, name: str | None = None) -> bool:
        name = name or self.current
        if not name or name not in self.contexts:
            return False
        for f in SECRET_FIELDS + ("username",):
            self.contexts[name].pop(f, None)
        if _use_keyring():
            import keyring

            for f in SECRET_FIELDS:
                with contextlib.suppress(Exception):
                    keyring.delete_password(KEYRING_SERVICE, f"{name}:{f}")
        self.save()
        return True

    def delete(self, name: str) -> bool:
        if name not in self.contexts:
            return False
        self.clear_credentials(name)
        self.contexts.pop(name, None)
        if self.current == name:
            self.current = next(iter(self.contexts), None)
        self.save()
        return True

    def use(self, name: str) -> None:
        if name not in self.contexts:
            raise KeyError(name)
        self.current = name
        self.save()

    def names(self) -> list[str]:
        return sorted(self.contexts)
