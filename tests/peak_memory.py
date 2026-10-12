# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Peak resident memory of the current process in BYTES, on Linux, macOS and Windows (review finding F15).

``resource`` exists on POSIX only (importing it on Windows fails, so it is imported lazily here, never at module level of a test file) and ``ru_maxrss`` has no common unit:
Linux (and the BSDs) report KiB, macOS reports bytes.  On Windows the peak working set comes from ``psutil``.  :func:`peak_rss_bytes` returns ``None`` when the platform offers no
peak figure; a caller then skips the peak assertion with a stated reason rather than guessing.
"""

from __future__ import annotations

import sys


def maxrss_to_bytes(raw: int, platform: str) -> int:
    """Normalise ``resource.getrusage(...).ru_maxrss`` to bytes: macOS (``darwin``) already reports bytes, every other POSIX platform reports KiB."""
    if platform == "win32":
        raise ValueError("ru_maxrss does not exist on Windows")
    return int(raw) if platform == "darwin" else int(raw) * 1024


def peak_rss_bytes(platform: str | None = None) -> int | None:
    """The peak RSS of this process in bytes, or ``None`` when neither ``resource`` nor psutil's ``peak_wset`` is available here."""
    plat = platform or sys.platform
    if plat != "win32":
        try:
            import resource
        except ImportError:
            pass
        else:
            return maxrss_to_bytes(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss, plat)
    try:
        import psutil
    except ImportError:
        return None
    peak = getattr(psutil.Process().memory_info(), "peak_wset", None)      # Windows only
    return int(peak) if peak else None
